package syncer

import (
	"context"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"strings"
	"time"

	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/json"

	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime/schema"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/dynamic/dynamicinformer"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/klog"
)

const (
	resyncPeriod = 10 * time.Hour
	owner        = "owner"
)

var budgetExceededError = errors.New("cluster scheduling Budget Exceeded")

type Controller struct {
	queue workqueue.RateLimitingInterface

	// Upstream
	fromDSIF dynamicinformer.DynamicSharedInformerFactory

	// Downstream
	toClient, fromClient dynamic.Interface

	clusterID string
	stopCh    chan struct{}
}

// New returns a new syncer Controller syncing spec from "from" to "to".
func New(from, to *rest.Config, syncedResourceTypes []string, clusterID string) (*Controller, error) {
	var (
		queue  = workqueue.NewRateLimitingQueue(workqueue.DefaultControllerRateLimiter())
		stopCh = make(chan struct{})
	)

	klog.Infoln("Info: ", syncedResourceTypes, clusterID)
	c := Controller{
		// TODO: should we have separate upstream and downstream sync workqueues?
		queue: queue,

		toClient:   dynamic.NewForConfigOrDie(to),
		fromClient: dynamic.NewForConfigOrDie(from),
		stopCh:     stopCh,
		clusterID:  clusterID,
	}

	fromDSIF := dynamicinformer.NewFilteredDynamicSharedInformerFactory(c.fromClient, resyncPeriod, metav1.NamespaceAll, func(o *metav1.ListOptions) {
		//o.LabelSelector = fmt.Sprintf("kcp.dev/cluster=%s", clusterID)
	})

	// Get all types the upstream API server knows about.
	// TODO: watch this and learn about new types, or forget about old ones.
	gvrstrs, err := getAllGVRs(from, syncedResourceTypes...)
	if err != nil {
		return nil, err
	}

	klog.Infoln("GVRs: ", gvrstrs)
	for _, gvrstr := range gvrstrs {
		gvr, _ := schema.ParseResourceArg(gvrstr)
		klog.Infoln("gvr: ", gvr.String())

		ret, err := fromDSIF.ForResource(*gvr).Lister().List(labels.Everything())
		if err != nil {
			klog.Infof("Failed to list all %q: %v", gvrstr, err)
			continue
		}

		klog.Infoln("INFO: ", ret)

		fromDSIF.ForResource(*gvr).Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
			AddFunc:    func(obj interface{}) { c.AddToQueue(*gvr, obj) },
			UpdateFunc: func(_, obj interface{}) { c.AddToQueue(*gvr, obj) },
			DeleteFunc: func(obj interface{}) { c.AddToQueue(*gvr, obj) },
		})
		klog.Infof("Set up informer for %v", gvr)
	}
	fromDSIF.WaitForCacheSync(stopCh)
	fromDSIF.Start(stopCh)
	c.fromDSIF = fromDSIF

	return &c, nil
}

func contains(ss []string, s string) bool {
	for _, n := range ss {
		if n == s {
			return true
		}
	}
	return false
}

func getAllGVRs(config *rest.Config, resourcesToSync ...string) ([]string, error) {
	toSyncSet := sets.NewString(resourcesToSync...)
	willBeSyncedSet := sets.NewString()
	dc, err := discovery.NewDiscoveryClientForConfig(config)
	if err != nil {
		return nil, err
	}
	rs, err := dc.ServerPreferredResources()
	if err != nil {
		return nil, err
	}
	var gvrstrs []string
	for _, r := range rs {
		// v1 -> v1.
		// apps/v1 -> v1.apps
		// tekton.dev/v1beta1 -> v1beta1.tekton.dev
		parts := strings.SplitN(r.GroupVersion, "/", 2)
		vr := parts[0] + "."
		if len(parts) == 2 {
			vr = parts[1] + "." + parts[0]
		}
		for _, ai := range r.APIResources {
			if !toSyncSet.Has(ai.Name) {
				// We're not interested in this resource type
				continue
			}
			if strings.Contains(ai.Name, "/") {
				// foo/status, pods/exec, namespace/finalize, etc.
				continue
			}
			if !ai.Namespaced {
				// Ignore cluster-scoped things.
				continue
			}
			if !contains(ai.Verbs, "watch") {
				klog.Infof("resource %s %s is not watchable: %v", vr, ai.Name, ai.Verbs)
				continue
			}
			gvrstrs = append(gvrstrs, fmt.Sprintf("%s.%s", ai.Name, vr))
			willBeSyncedSet.Insert(ai.Name)
		}
	}

	notFoundResourceTypes := toSyncSet.Difference(willBeSyncedSet)
	if notFoundResourceTypes.Len() != 0 {
		return nil, fmt.Errorf("The following resource types were requested to be synced, but were not found in the KCP logical cluster: %v", notFoundResourceTypes.List())
	}
	return gvrstrs, nil
}

type holder struct {
	gvr schema.GroupVersionResource
	obj interface{}
}

func (c *Controller) AddToQueue(gvr schema.GroupVersionResource, obj interface{}) {
	c.queue.AddRateLimited(holder{gvr: gvr, obj: obj})
}

// Start starts N worker processes processing work items.
func (c *Controller) Start(numThreads int) {
	for i := 0; i < numThreads; i++ {
		go c.startWorker()
	}
}

// startWorker processes work items until stopCh is closed.
func (c *Controller) startWorker() {
	klog.Infoln("Starting")
	for {
		select {
		case <-c.stopCh:
			log.Println("stopping syncer worker")
			return
		default:
			c.processNextWorkItem()
		}
	}
}

// Stop stops the syncer.
func (c *Controller) Stop() {
	c.queue.ShutDown()
	close(c.stopCh)
}

// Done returns a channel that's closed when the syncer is stopped.
func (c *Controller) Done() <-chan struct{} { return c.stopCh }

func (c *Controller) processNextWorkItem() bool {
	klog.Infoln("processNextWorkItem")
	// Wait until there is a new item in the working queue
	i, quit := c.queue.Get()
	if quit {
		return false
	}
	h := i.(holder)

	// No matter what, tell the queue we're done with this key, to unblock
	// other workers.
	defer c.queue.Done(i)

	err := c.process(h.gvr, h.obj)
	c.handleErr(err, i)
	return true
}

func (c *Controller) handleErr(err error, i interface{}) {
	// Reconcile worked, nothing else to do for this workqueue item.
	if err == nil {
		c.queue.Forget(i)
		return
	}

	// Re-enqueue up to 5 times.
	num := c.queue.NumRequeues(i)
	if num < 5 {
		klog.Errorf("Error reconciling key %q, retrying... (#%d): %v", i, num, err)
		c.queue.AddRateLimited(i)
		return
	}

	// Give up and report error elsewhere.
	c.queue.Forget(i)
	utilruntime.HandleError(err)
	klog.Errorf("Dropping key %q after failed retries: %v", i, err)
}

func (c *Controller) process(gvr schema.GroupVersionResource, obj interface{}) error {
	klog.V(2).Infof("Process object of type: %T : %v", obj, obj)
	meta, isMeta := obj.(metav1.Object)
	if !isMeta {
		if tombstone, isTombstone := obj.(cache.DeletedFinalStateUnknown); isTombstone {
			meta, isMeta = tombstone.Obj.(metav1.Object)
			if !isMeta {
				err := fmt.Errorf("Tombstone contained object is expected to be a metav1.Object, but is %T: %#v", obj, obj)
				klog.Error(err)
				return err
			}
		} else {
			err := fmt.Errorf("Object to synchronize is expected to be a metav1.Object, but is %T", obj)
			klog.Error(err)
			return err
		}
	}
	namespace, name := meta.GetNamespace(), meta.GetName()

	ctx := context.TODO()

	obj, exists, err := c.fromDSIF.ForResource(gvr).Informer().GetIndexer().Get(obj)
	if err != nil {
		klog.Error(err)
		return err
	}

	if !exists {
		klog.Infof("Object with gvr=%q was deleted : %s/%s", gvr, namespace, name)
		c.delete(ctx, gvr, namespace, name)
		return nil
	}

	unstructuredObject, isUnstructured := obj.(*unstructured.Unstructured)
	if !isUnstructured {
		err := fmt.Errorf("Object to synchronize is expected to be Unstructured, but is %T", obj)
		klog.Error(err)
		return err
	}

	if err := c.ensureNamespaceExists(namespace); err != nil {
		klog.Error(err)
		return err
	}

	if err := c.upsert(ctx, gvr, namespace, unstructuredObject); err != nil {
		return err
	}

	return err
}

type BudgetExceededError struct {
}

func (e *BudgetExceededError) Error() string {
	return fmt.Sprintf("Available Cluster Budget Exceeded")
}

func (c *Controller) ensureClusterSchedulingBudget(ctx context.Context, budget int) error {
	// check the number of clusters created on this management cluster
	nsList, err := c.toClient.Resource(schema.GroupVersionResource{
		Group:    "",
		Version:  "v1",
		Resource: "namespaces",
	}).List(ctx, metav1.ListOptions{})

	if err != nil {
		return err
	}

	budgeSize := len(nsList.Items)
	if budgeSize >= budget {
		return budgetExceededError
	}
	klog.Infof("Size of namespace  list %d", budgeSize)
	return nil
}

func (c *Controller) ensureNamespaceExists(namespace string) error {
	namespaces := c.toClient.Resource(schema.GroupVersionResource{
		Group:    "",
		Version:  "v1",
		Resource: "namespaces",
	})
	newNamespace := &unstructured.Unstructured{}
	newNamespace.SetAPIVersion("v1")
	newNamespace.SetKind("Namespace")
	newNamespace.SetName(namespace)
	if _, err := namespaces.Create(context.TODO(), newNamespace, metav1.CreateOptions{}); err != nil {
		if !k8serrors.IsAlreadyExists(err) {
			klog.Infof("Error while creating namespace %s: %v", namespace, err)
			return err
		}
	}
	return nil
}

// getToClient gets a dynamic client for the GVR, scoped to namespace if the namespace is not "".
func (c *Controller) getToClient(gvr schema.GroupVersionResource, namespace string) dynamic.ResourceInterface {
	nri := c.toClient.Resource(gvr)
	if namespace != "" {
		return nri.Namespace(namespace)
	}
	return nri
}

// getToClient gets a dynamic client for the GVR, scoped to namespace if the namespace is not "".
func (c *Controller) getFromClient(gvr schema.GroupVersionResource, namespace string) dynamic.ResourceInterface {
	nri := c.fromClient.Resource(gvr)
	if namespace != "" {
		return nri.Namespace(namespace)
	}
	return nri
}

func (c *Controller) delete(ctx context.Context, gvr schema.GroupVersionResource, namespace, name string) error {
	// TODO: get UID of just-deleted object and pass it as a precondition on this delete.
	// This would avoid races where an object is deleted and another object with the same name is created immediately after.

	return c.getToClient(gvr, namespace).Delete(ctx, name, metav1.DeleteOptions{})
}

func (c *Controller) upsert(ctx context.Context, gvr schema.GroupVersionResource, namespace string, unstrob *unstructured.Unstructured) error {

	var (
		toClient           = c.getToClient(gvr, namespace)
		fromClient         = c.getFromClient(gvr, namespace)
		unstrobAnnotations = unstrob.GetAnnotations()
	)

	// random wait to avoid race conditions with other cluster syncers
	rand.Seed(time.Now().UnixNano())
	n := rand.Intn(10) // n will be between 0 and 10
	klog.Infof("Sleeping %d seconds...\n", n)
	time.Sleep(time.Duration(n) * time.Second)
	klog.Infoln("Done")

	// Attempt to create the object; if the object already exists, update it.
	unstrob.SetUID("")
	unstrob.SetResourceVersion("")

	if len(unstrob.GetAnnotations()) == 0 {
		unstrobAnnotations = make(map[string]string)
	}
	ownerValue, hasOwnerAnnotation := unstrobAnnotations[owner]

	// upsert only if we are the owner of the resource or if the resource does not have an owner yet
	klog.Infoln("Owner values", hasOwnerAnnotation, ownerValue, c.clusterID)
	if ownerValue == c.clusterID || !hasOwnerAnnotation {
		klog.Infof("Updating ownership on %s", gvr)
		// add owner
		unstrobAnnotations[owner] = c.clusterID
		unstrob.SetAnnotations(unstrobAnnotations)

		data, err := json.Marshal(unstrob)
		if err != nil {
			return err
		}
		if _, err := fromClient.Patch(ctx, unstrob.GetName(), types.MergePatchType, data, metav1.PatchOptions{
			FieldManager: fmt.Sprintf("syncer-%s", c.clusterID),
		}); err != nil {
			klog.Errorf("Updating resource %s/%s: %v", namespace, unstrob.GetName(), err)
			return err
		}

		klog.Infof("Ownership updated on %s:%s", gvr, unstrob.GetName())
		if c.ensureClusterSchedulingBudget(ctx, 8) == budgetExceededError {
			return budgetExceededError
		}
		if _, err := toClient.Create(ctx, unstrob, metav1.CreateOptions{}); err != nil {
			if !k8serrors.IsAlreadyExists(err) {
				klog.Error("Creating resource %s/%s: %v", namespace, unstrob.GetName(), err)
				return err
			}

			existing, err := toClient.Get(ctx, unstrob.GetName(), metav1.GetOptions{})
			if err != nil {
				klog.Errorf("Getting resource %s/%s: %v", namespace, unstrob.GetName(), err)
				return err
			}
			klog.Infof("Object %s/%s already exists: update it", gvr.Resource, unstrob.GetName())

			unstrob.SetResourceVersion(existing.GetResourceVersion())
			if _, err := toClient.Update(ctx, unstrob, metav1.UpdateOptions{}); err != nil {
				klog.Errorf("Updating resource %s/%s: %v", namespace, unstrob.GetName(), err)
				return err
			}
		} else {
			klog.Infof("Created object %s/%s", gvr.Resource, unstrob.GetName())
		}

	} else {
		klog.Infoln("I don't own this resource")
	}

	return nil
}
