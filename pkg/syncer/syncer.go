package syncer

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strconv"
	"strings"
	"time"

	"k8s.io/client-go/informers"

	"k8s.io/client-go/kubernetes"

	clusterv1alpha1 "github.com/kcp-dev/kcp/pkg/client/clientset/versioned/typed/cluster/v1alpha1"

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
	budgetKey    = "kcp.dev/budget"
)

var budgetExceededError = errors.New("cluster scheduling Budget Exceeded")

type Controller struct {
	queue, namespacesQueue workqueue.RateLimitingInterface

	// Upstream
	fromDSIF          dynamicinformer.DynamicSharedInformerFactory
	toSHIF            informers.SharedInformerFactory
	fromClusterClient clusterv1alpha1.ClusterV1alpha1Client
	// Downstream
	toClient, fromClient dynamic.Interface

	toKubeClient kubernetes.Interface
	clusterID    string
	stopCh       chan struct{}
}

// New returns a new syncer Controller syncing spec from "from" to "to".
func New(from, to *rest.Config, syncedResourceTypes []string, clusterID string) (*Controller, error) {
	var (
		queue          = workqueue.NewRateLimitingQueue(workqueue.DefaultControllerRateLimiter())
		namespaceQueue = workqueue.NewNamedRateLimitingQueue(workqueue.DefaultControllerRateLimiter(), "namespace")

		stopCh = make(chan struct{})
	)
	//client := clusterv1alpha1.NewForConfigOrDie(c.)
	toKubeClient, err := kubernetes.NewForConfig(to)

	klog.Infoln("Info: ", syncedResourceTypes, clusterID)
	c := Controller{
		// TODO: should we have separate upstream and downstream sync workqueues?
		queue:             queue,
		namespacesQueue:   namespaceQueue,
		fromClusterClient: *clusterv1alpha1.NewForConfigOrDie(from),
		toClient:          dynamic.NewForConfigOrDie(to),
		fromClient:        dynamic.NewForConfigOrDie(from),

		stopCh:    stopCh,
		clusterID: clusterID,
	}

	var (
		fromDSIF = dynamicinformer.NewFilteredDynamicSharedInformerFactory(c.fromClient, resyncPeriod, metav1.NamespaceAll, func(o *metav1.ListOptions) {
			//o.LabelSelector = fmt.Sprintf("kcp.dev/cluster=%s", clusterID)
		})
		toSHIF = informers.NewSharedInformerFactory(toKubeClient, resyncPeriod)
	)

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

	toSHIF.Core().V1().Namespaces().Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    c.namespaceAdd,
		UpdateFunc: c.namespaceUpdate,
		DeleteFunc: c.namespaceDelete,
	})

	klog.Infof("Set up informer for namespaces")

	toSHIF.WaitForCacheSync(stopCh)
	toSHIF.Start(stopCh)
	c.toSHIF = toSHIF
	return &c, nil
}

func (c *Controller) namespaceAdd(obj interface{}) {
	key, err := cache.MetaNamespaceKeyFunc(obj)
	if err != nil {
		klog.Errorf("Couldn't get key for object %+v: %v", obj, err)
		return
	}
	c.namespacesQueue.Add(key)
}

func (c *Controller) namespaceUpdate(_, newObj interface{}) {
	c.namespaceAdd(newObj)
}

func (c *Controller) namespaceDelete(obj interface{}) {
	key, err := cache.DeletionHandlingMetaNamespaceKeyFunc(obj)
	if err != nil {
		klog.Errorf("Couldn't get key for object %+v: %v", obj, err)
		return
	}
	c.namespacesQueue.Add(key)
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
			//if !ai.Namespaced {
			//	// Ignore cluster-scoped things.
			//	continue
			//}
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

	klog.Infoln(gvrstrs)
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
		go c.startWorkerNamespaces()
	}
}

// startWorker processes work items until stopCh is closed.
func (c *Controller) startWorkerNamespaces() {
	for {
		select {
		case <-c.stopCh:
			log.Println("stopping syncer worker")
			return
		default:
			c.processNextWorkItemNamespaces()
		}
	}
}

// startWorker processes work items until stopCh is closed.
func (c *Controller) startWorker() {
	for {
		select {
		case <-c.stopCh:
			log.Println("stopping syncer worker")
			return
		default:
			c.processNextWorkItemGVRs()
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

func (c *Controller) processNextWorkItemGVRs() bool {
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

func (c *Controller) processNextWorkItemNamespaces() bool {
	klog.Infoln("processNextWorkItemNamespaces")
	// Wait until there is a new item in the working queue
	i, quit := c.namespacesQueue.Get()
	if quit {
		return false
	}

	// No matter what, tell the queue we're done with this key, to unblock
	// other workers.
	defer c.queue.Done(i)

	err := c.processNamespaces()
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

func (c *Controller) processNamespaces() error {
	namespaceList, err := c.toSHIF.Core().V1().Namespaces().Lister().List(labels.NewSelector())
	if err != nil {
		return err
	}

	klog.Infoln("Namespace List:", len(namespaceList))
	return c.upsertNamespaces(context.TODO(), len(namespaceList))
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

	//if err := c.ensureNamespaceExists(namespace); err != nil {
	//	klog.Error(err)
	//	return err
	//}

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

func (c *Controller) ensureClusterSchedulingBudgetNamespaces(ctx context.Context, budget int) error {
	cluster, err := c.fromClusterClient.Clusters().Get(ctx, c.clusterID, metav1.GetOptions{})
	if err != nil {
		return err
	}

	patchCluster := cluster.DeepCopy()
	patchCluster.SetAnnotations(map[string]string{
		budgetKey: strconv.Itoa(budget),
	})

	data, err := json.Marshal(patchCluster)
	if err != nil {
		return err
	}
	if _, err := c.fromClusterClient.Clusters().Patch(ctx, c.clusterID, types.MergePatchType, data, metav1.PatchOptions{
		DryRun:       nil,
		Force:        nil,
		FieldManager: fmt.Sprintf("syncer-%s", c.clusterID),
	}); err != nil {
		klog.Errorf("Updating resource %s: %v", c.clusterID, err)
		return err
	}

	return nil
}
func (c *Controller) ensureClusterSchedulingBudget(ctx context.Context, gvr schema.GroupVersionResource, unstrobj *unstructured.Unstructured) error {
	//klog.Infoln("GVR: ", gvr)
	// check the number of clusters created on this management cluster
	nsList, err := c.toClient.Resource(schema.GroupVersionResource{
		Group:    "",
		Version:  "v1",
		Resource: "namespaces",
	}).List(ctx, metav1.ListOptions{})

	if err != nil {
		return err
	}

	if unstrobj.GetName() == c.clusterID {
		unstrobj.SetAnnotations(map[string]string{
			budgetKey: strconv.Itoa(len(nsList.Items)),
		})

		data, err := json.Marshal(unstrobj)
		if err != nil {
			return err
		}

		if _, err := c.fromClient.Resource(schema.GroupVersionResource{
			Group:    "cluster.example.dev",
			Version:  "v1alpha1",
			Resource: "clusters",
		}).Patch(ctx, c.clusterID, types.MergePatchType, data, metav1.PatchOptions{
			DryRun:       nil,
			Force:        nil,
			FieldManager: fmt.Sprintf("syncer-%s", c.clusterID),
		}); err != nil {
			klog.Errorf("Updating resource %s: %v", c.clusterID, err)
			return err
		}
	}

	klog.Infof("Budget Updated")
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

func (c *Controller) upsertNamespaces(ctx context.Context, budget int) error {
	return c.ensureClusterSchedulingBudgetNamespaces(ctx, budget)
}

func (c *Controller) upsert(ctx context.Context, gvr schema.GroupVersionResource, namespace string, unstrob *unstructured.Unstructured) error {
	return c.ensureClusterSchedulingBudget(ctx, gvr, unstrob)
}

var retryCounter = 0

//func (c *Controller) upsert(ctx context.Context, gvr schema.GroupVersionResource, namespace string, unstrob *unstructured.Unstructured) error {
//
//	var (
//		toClient           = c.getToClient(gvr, namespace)
//		fromClient         = c.getFromClient(gvr, namespace)
//		unstrobAnnotations = unstrob.GetAnnotations()
//	)
//
//	// Attempt to create the object; if the object already exists, update it.
//	unstrob.SetUID("")
//	unstrob.SetResourceVersion("")
//
//	if len(unstrob.GetAnnotations()) == 0 {
//		unstrobAnnotations = make(map[string]string)
//	}
//	ownerValue, hasOwnerAnnotation := unstrobAnnotations[owner]
//
//	// upsert only if we are the owner of the resource or if the resource does not have an owner yet
//	klog.Infof("Has Owner Annotation: %t, Owner Value %s, Cluster ID: %s", hasOwnerAnnotation, ownerValue, c.clusterID)
//
//	if ownerValue == c.clusterID || !hasOwnerAnnotation {
//		klog.Infof("Updating ownership on %s", gvr)
//		// add owner
//		unstrobAnnotations[owner] = c.clusterID
//		unstrob.SetAnnotations(unstrobAnnotations)
//
//		data, err := json.Marshal(unstrob)
//		if err != nil {
//			return err
//		}
//
//		if _, err := fromClient.Patch(ctx, unstrob.GetName(), types.MergePatchType, data, metav1.PatchOptions{
//			FieldManager: fmt.Sprintf("syncer-%s", c.clusterID),
//		}); err != nil {
//			klog.Errorf("Updating resource %s/%s: %v", namespace, unstrob.GetName(), err)
//			return err
//		}
//
//		klog.Infof("Ownership updated on %s:%s", gvr, unstrob.GetName())
//		if c.ensureClusterSchedulingBudget(ctx, 8) == budgetExceededError {
//			// Just return a warning now
//			klog.Warning(budgetExceededError.Error())
//			return nil
//		}
//		if _, err := toClient.Create(ctx, unstrob, metav1.CreateOptions{}); err != nil {
//			if !k8serrors.IsAlreadyExists(err) {
//				klog.Error("Creating resource %s/%s: %v", namespace, unstrob.GetName(), err)
//				return err
//			}
//
//			existing, err := toClient.Get(ctx, unstrob.GetName(), metav1.GetOptions{})
//			if err != nil {
//				klog.Errorf("Getting resource %s/%s: %v", namespace, unstrob.GetName(), err)
//				return err
//			}
//			klog.Infof("Object %s/%s already exists: update it", gvr.Resource, unstrob.GetName())
//
//			unstrob.SetResourceVersion(existing.GetResourceVersion())
//			if _, err := toClient.Update(ctx, unstrob, metav1.UpdateOptions{}); err != nil {
//				klog.Errorf("Updating resource %s/%s: %v", namespace, unstrob.GetName(), err)
//				return err
//			}
//		} else {
//			klog.Infof("Created object %s/%s", gvr.Resource, unstrob.GetName())
//		}
//
//	} else {
//		// Remove from queue to avoid actuating upon it again.
//		c.queue.Forget(holder{
//			gvr: gvr,
//			obj: unstrob,
//		})
//		klog.Infoln("I don't own this resource, removing from queue!")
//	}
//
//	return nil
//}
