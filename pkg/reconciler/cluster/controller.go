package cluster

import (
	"context"
	"fmt"
	"io/ioutil"
	"log"
	"strconv"
	"time"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	"k8s.io/client-go/dynamic/dynamicinformer"

	"k8s.io/client-go/dynamic"

	"k8s.io/apimachinery/pkg/runtime/schema"

	"github.com/kcp-dev/kcp/pkg/apis/cluster/v1alpha1"
	clusterclient "github.com/kcp-dev/kcp/pkg/client/clientset/versioned"
	clusterv1alpha1 "github.com/kcp-dev/kcp/pkg/client/clientset/versioned/typed/cluster/v1alpha1"
	"github.com/kcp-dev/kcp/pkg/client/informers/externalversions"
	"github.com/kcp-dev/kcp/pkg/syncer"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	apiextensionsv1client "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset/typed/apiextensions/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/klog"
	"sigs.k8s.io/yaml"
)

const resyncPeriod = 10 * time.Hour

type SyncerMode int

const (
	SyncerModePull SyncerMode = iota
	SyncerModePush
	SyncerModeNone
)

// NewController returns a new Controller which reconciles Cluster resources in the API
// server it reaches using the REST client.
//
// When new Clusters are found, the syncer will be run there using the given image.
func NewController(cfg *rest.Config, syncerImage string, kubeconfig clientcmdapi.Config, resourcesToSync []string, syncerMode SyncerMode) *Controller {

	var (
		client          = clusterv1alpha1.NewForConfigOrDie(cfg)
		queue           = workqueue.NewRateLimitingQueue(workqueue.DefaultControllerRateLimiter())
		hypershiftQueue = workqueue.NewRateLimitingQueue(workqueue.DefaultControllerRateLimiter())

		stopCh = make(chan struct{}) // TODO: hook this up to SIGTERM/SIGINT

		dynamicClient = dynamic.NewForConfigOrDie(cfg)
		crdClient     = apiextensionsv1client.NewForConfigOrDie(cfg)
		c             = &Controller{
			queue:           queue,
			hyperShiftQueue: hypershiftQueue,
			client:          client,
			crdClient:       crdClient,
			syncerImage:     syncerImage,
			kubeconfig:      kubeconfig,
			stopCh:          stopCh,
			resourcesToSync: resourcesToSync,
			syncerMode:      syncerMode,
			syncers:         map[string]*syncer.Controller{},
		}
	)
	c.dynamicClient = dynamicClient

	sif := externalversions.NewSharedInformerFactoryWithOptions(clusterclient.NewForConfigOrDie(cfg), resyncPeriod)
	sif.Cluster().V1alpha1().Clusters().Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    func(obj interface{}) { c.enqueue(obj) },
		UpdateFunc: func(_, obj interface{}) { c.enqueue(obj) },
		DeleteFunc: func(obj interface{}) { c.deletedCluster(obj) },
	})
	c.indexer = sif.Cluster().V1alpha1().Clusters().Informer().GetIndexer()
	sif.WaitForCacheSync(stopCh)
	sif.Start(stopCh)

	gvrstr := "hostedclusters.v1alpha1.hypershift.openshift.io"
	gvr, _ := schema.ParseResourceArg(gvrstr)

	cockpitSIF := dynamicinformer.NewFilteredDynamicSharedInformerFactory(c.dynamicClient, resyncPeriod, metav1.NamespaceAll, func(o *metav1.ListOptions) {
		//o.LabelSelector = fmt.Sprintf("kcp.dev/cluster=%s", clusterID)
	})
	cockpitSIF.ForResource(*gvr).Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    func(obj interface{}) { c.AddToQueue(*gvr, obj) },
		UpdateFunc: func(_, obj interface{}) { c.AddToQueue(*gvr, obj) },
		DeleteFunc: func(obj interface{}) { c.AddToQueue(*gvr, obj) },
	})

	cockpitSIF.WaitForCacheSync(stopCh)
	cockpitSIF.Start(stopCh)
	c.cockpitSIF = cockpitSIF

	return c
}

type Controller struct {
	queue           workqueue.RateLimitingInterface
	hyperShiftQueue workqueue.RateLimitingInterface
	client          clusterv1alpha1.ClusterV1alpha1Interface
	dynamicClient   dynamic.Interface
	cockpitSIF      dynamicinformer.DynamicSharedInformerFactory
	indexer         cache.Indexer
	crdClient       apiextensionsv1client.ApiextensionsV1Interface
	syncerImage     string
	kubeconfig      clientcmdapi.Config
	stopCh          chan struct{}
	resourcesToSync []string
	syncerMode      SyncerMode
	syncers         map[string]*syncer.Controller
}

type holder struct {
	gvr schema.GroupVersionResource
	obj interface{}
}

func (c *Controller) AddToQueue(gvr schema.GroupVersionResource, obj interface{}) {
	c.hyperShiftQueue.AddRateLimited(holder{gvr: gvr, obj: obj})
}

func (c *Controller) enqueue(obj interface{}) {
	key, err := cache.MetaNamespaceKeyFunc(obj)
	if err != nil {
		runtime.HandleError(err)
		return
	}
	c.queue.AddRateLimited(key)
}

func (c *Controller) Start(numThreads int) {
	defer c.queue.ShutDown()
	for i := 0; i < numThreads; i++ {
		go wait.Until(c.startWorker, time.Second, c.stopCh)
		go wait.Until(c.startWorkerHyperShift, time.Second, c.stopCh)
	}
	log.Println("Starting workers")
	<-c.stopCh
	log.Println("Stopping workers")
}

func (c *Controller) startWorker() {
	for c.processNextWorkItem() {
	}
}

func (c *Controller) startWorkerHyperShift() {
	for c.processNextWorkItemHyperShift() {
	}
}

func (c *Controller) processNextWorkItemHyperShift() bool {
	// Wait until there is a new item in the working queue
	i, quit := c.hyperShiftQueue.Get()
	if quit {
		return false
	}

	h := i.(holder)

	// No matter what, tell the queue we're done with this key, to unblock
	// other workers.
	defer c.queue.Done(i)
	c.processHyperShift(h.gvr, h.obj)
	//c.handleErrHyperShift(, i)
	return true
}

func (c *Controller) processNextWorkItem() bool {
	// Wait until there is a new item in the working queue
	k, quit := c.queue.Get()
	if quit {
		return false
	}
	key := k.(string)

	// No matter what, tell the queue we're done with this key, to unblock
	// other workers.
	defer c.queue.Done(key)

	err := c.process(key)
	c.handleErr(err, key)
	return true
}

func (c *Controller) handleErr(err error, key string) {
	// Reconcile worked, nothing else to do for this workqueue item.
	if err == nil {
		log.Println("Successfully reconciled", key)
		c.queue.Forget(key)
		return
	}

	// Re-enqueue up to 5 times.
	num := c.queue.NumRequeues(key)
	if num < 5 {
		log.Printf("Error reconciling key %q, retrying... (#%d): %v", key, num, err)
		c.queue.AddRateLimited(key)
		return
	}

	// Give up and report error elsewhere.
	c.queue.Forget(key)
	runtime.HandleError(err)
	klog.V(2).Infof("Dropping key %q after failed retries: %v", key, err)
}

func (c *Controller) handleErrHyperShift(quit bool, i interface{}) {
	// Reconcile worked, nothing else to do for this workqueue item.
	if quit {
		c.queue.Forget(i)
		return
	}

	// Re-enqueue up to 5 times.
	num := c.queue.NumRequeues(i)
	if num < 5 {
		klog.Errorf("Error reconciling key %q, retrying... (#%d)", i, num)
		c.queue.AddRateLimited(i)
		return
	}

	// Give up and report error elsewhere.
	c.queue.Forget(i)
	klog.Errorf("Dropping key %q after failed retries")
}

func (c *Controller) processHyperShift(gvr schema.GroupVersionResource, obj interface{}) error {
	klog.Infoln("processHyperShift", gvr)
	obj, _, err := c.cockpitSIF.ForResource(gvr).Informer().GetIndexer().Get(obj)
	if err != nil {
		klog.Error(err)
		return err
	}

	unstructuredObject, isUnstructured := obj.(*unstructured.Unstructured)
	if !isUnstructured {
		err := fmt.Errorf("object to synchronize is expected to be Unstructured, but is %T", obj)
		klog.Error(err)
		return err
	}

	return c.reconcileHostedCluster(context.TODO(), unstructuredObject)
}

func (c *Controller) process(key string) error {
	obj, exists, err := c.indexer.GetByKey(key)
	if err != nil {
		return err
	}

	if !exists {
		log.Printf("Object with key %q was deleted", key)
		return nil
	}
	current := obj.(*v1alpha1.Cluster)
	previous := current.DeepCopy()

	budget, err := strconv.Atoi(current.GetAnnotations()[budgetKey])
	if err != nil {
		return err
	}

	klog.Infof("Budget for %s is %d", current.Name, budget)
	ctx := context.TODO()

	if err := c.reconcileSyncer(ctx, current); err != nil {
		return err
	}

	// If the object being reconciled changed as a result, update it.
	if !equality.Semantic.DeepEqual(previous.Status, current.Status) {
		log.Println("saw update")
		_, uerr := c.client.Clusters().UpdateStatus(ctx, current, metav1.UpdateOptions{})
		return uerr
	}
	log.Println("no update")

	return nil
}

func (c *Controller) deletedCluster(obj interface{}) {
	castObj, ok := obj.(*v1alpha1.Cluster)
	if !ok {
		tombstone, ok := obj.(cache.DeletedFinalStateUnknown)
		if !ok {
			klog.Errorf("Couldn't get object from tombstone %#v", obj)
			return
		}
		castObj, ok = tombstone.Obj.(*v1alpha1.Cluster)
		if !ok {
			klog.Errorf("Tombstone contained object that is not expected %#v", obj)
			return
		}
	}
	klog.V(4).Infof("Deleting cluster %q", castObj.Name)
	ctx := context.TODO()
	c.cleanup(ctx, castObj)
}

func RegisterClusterCRD(cfg *rest.Config) error {
	bytes, err := ioutil.ReadFile("config/cluster.example.dev_clusters.yaml")

	crdClient := apiextensionsv1client.NewForConfigOrDie(cfg)

	crd := &apiextensionsv1.CustomResourceDefinition{}
	err = yaml.Unmarshal(bytes, crd)
	if err != nil {
		return err
	}

	_, err = crdClient.CustomResourceDefinitions().Create(context.TODO(), crd, metav1.CreateOptions{})
	if err != nil && !errors.IsAlreadyExists(err) {
		return err
	}

	return nil
}
