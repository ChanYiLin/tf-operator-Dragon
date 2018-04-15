// Copyright 2018 The Kubeflow Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package controller provides a Kubernetes controller for a TensorFlow job resource.
package controller

import (
	"errors"
	"fmt"
	"sort"
	"time"

	log "github.com/sirupsen/logrus"
	"k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/util/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	typedcorev1 "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/workqueue"

	"github.com/juju/ratelimit"
	tfv1alpha1 "github.com/kubeflow/tf-operator/pkg/apis/tensorflow/v1alpha1"
	tfjobclient "github.com/kubeflow/tf-operator/pkg/client/clientset/versioned"
	kubeflowscheme "github.com/kubeflow/tf-operator/pkg/client/clientset/versioned/scheme"
	informers "github.com/kubeflow/tf-operator/pkg/client/informers/externalversions"
	listers "github.com/kubeflow/tf-operator/pkg/client/listers/kubeflow/v1alpha1"
	"github.com/kubeflow/tf-operator/pkg/trainer"

)

const (
	controllerName = "kubeflow"
)


/*** Jack Lin  ***/
// ClusterResource is the resource of a cluster
type ClusterResource struct {
	NodeCount int

	GPURequest int
	GPULimit   int
	GPUTotal   int

	CPURequestMilli int64
	CPULimitMilli   int64
	CPUTotalMilli   int64

	MemoryRequestMega int64
	MemoryLimitMega   int64
	MemoryTotalMega   int64

	NodeInfos NodeInfos
}

// NodeInfos is the information of all nodes.
type NodeInfos struct {
	NodesCPUIdleMilli   map[string]int64
	NodesMemoryFreeMega map[string]int64
}
/*** Jack Lin  ***/

var (
	// ErrVersionOutdated is a exported var to capture the error in apiserver
	ErrVersionOutdated = errors.New("requested version is outdated in apiserver")

	// IndexerInformer uses a delta queue, therefore for deletes we have to use this
	// key function but it should be just fine for non delete events.
	keyFunc = cache.DeletionHandlingMetaNamespaceKeyFunc

	// DefaultJobBackOff is the max backoff period, exported for the e2e test
	DefaultJobBackOff = 10 * time.Second
	// MaxJobBackOff is the max backoff period, exported for the e2e test
	MaxJobBackOff = 360 * time.Second
)

// Controller is structure to manage various service clients
type Controller struct {
	KubeClient  kubernetes.Interface
	TFJobClient tfjobclient.Interface

	config tfv1alpha1.ControllerConfig
	jobs   map[string]*trainer.TrainingJob

	TFJobLister listers.TFJobLister
	TFJobSynced cache.InformerSynced

	// WorkQueue is a rate limited work queue. This is used to queue work to be
	// processed instead of performing it as soon as a change happens. This
	// means we can ensure we only process a fixed amount of resources at a
	// time, and makes it easy to ensure we are never processing the same item
	// simultaneously in two different workers.
	//
	// Items in the work queue correspond to the name of the job.
	// In response to various events (e.g. Add, Update, Delete), the informer
	// is configured to add events to the queue. Since the item in the queue
	// represents a job and not a particular event, we end up aggregating events for
	// a job and ensure that a particular job isn't being processed by multiple
	// workers simultaneously.
	//
	// We rely on the informer to periodically generate Update events. This ensures
	// we regularly check on each TFJob and take any action needed.
	//
	// If there is a problem processing a job, processNextWorkItem just requeues
	// the work item. This ensures that we end up retrying it. In this case
	// we rely on the rateLimiter in the worker queue to retry with exponential
	// backoff.
	WorkQueue workqueue.RateLimitingInterface

	// recorder is an event recorder for recording Event resources to the
	// Kubernetes API.
	recorder record.EventRecorder

	syncHandler func(jobKey string) (bool, error)

	enableGangScheduling bool

	/*** Jack Lin ***/
	cluster        *Cluster
	/*** Jack Lin ***/
	
}

/*** Jack Lin  ***/

// elastic job filter.
//func elastic(j job) bool {
//	return j.Config.Elastic()
//}

// gpu job filter.
//func gpu(j job) bool {
//	return j.Config.NeedGPU()
//}




//type ComparedJobs []*trainer.TrainingJob

type ComparedJobs struct {
	Key string
  	Value *trainer.TrainingJob
}

type ComparedJobsList []ComparedJobs

func Fulfillment(j ComparedJobs) float64 {
	//minInstance := j.Config.Spec.Trainer.MinInstance
	//maxInstance := j.Config.Spec.Trainer.MaxInstance

	jobA := j.Value.GetJob()
	minInstance := jobA.Spec.MinInstance
	maxInstance := jobA.Spec.MaxInstance

	if minInstance == maxInstance {
		return 1
	}

	var curInstance int

	jobAReplicasSetList := j.Value.GetJobReplicasSetList()


	for _, t := range jobAReplicasSetList {
		if t.Spec.TFReplicaType == tfv1alpha1.WORKER {
			tReplicasSetSpec := t.GetReplicasSetSpec()
			curInstance = int(*tReplicasSetSpec.Replicas)
			log.Info("in Fulfillment: curInstance: %v", curInstance)
			break
		}
	}


	//curInstance := int(*j.TrainerJob.Spec.Parallelism)

	return float64(curInstance-minInstance) / float64(maxInstance-minInstance)
}


func (j ComparedJobsList) Len() int {
	return len(j)
}

func (j ComparedJobsList) Less(a int, b int) bool {
	scoreA := Fulfillment(j[a])
	scoreB := Fulfillment(j[b])

	jobAReplicasSetList := j[a].Value.GetJobReplicasSetList()
	jobBReplicasSetList := j[b].Value.GetJobReplicasSetList()

	var jAReplicasSet *trainer.TFReplicaSet
	var jBReplicasSet *trainer.TFReplicaSet

	log.Info("in Sort Less: scoreA: %v, scoreB: %v", scoreA, scoreB)
	if scoreA == scoreB {

		for _, t := range jobAReplicasSetList {
			if t.Spec.TFReplicaType == tfv1alpha1.WORKER {
				jAReplicasSet = t
				break
			}
		}


		for _, k := range jobBReplicasSetList {
			if k.Spec.TFReplicaType == tfv1alpha1.WORKER {
				jBReplicasSet = k
				break
			}
		}

		jAReplicasSetSpec := jAReplicasSet.GetReplicasSetSpec()
		jBReplicasSetSpec := jBReplicasSet.GetReplicasSetSpec()

		//resA := j[a].Config.Spec.Trainer.Resources
		//resB := j[b].Config.Spec.Trainer.Resources
		//log.Info("resA has GPU limits %v")
		var resARequestsCPU resource.Quantity
		var resARequestsMem resource.Quantity
		var resALimitsGPU resource.Quantity

		var resBRequestsCPU resource.Quantity
		var resBRequestsMem resource.Quantity
		var resBLimitsGPU resource.Quantity

		for _, container := range jAReplicasSetSpec.Template.Spec.Containers {
			log.Info("In sort jAReplicasSet Container: %v", container.Name)
			tmpCPU := *container.Resources.Requests.Cpu()
			resARequestsCPU.Add(tmpCPU)

			tmpMem := *container.Resources.Requests.Memory()
			resARequestsMem.Add(tmpMem)
			
			tmpGpu := container.Resources.Requests[ResourceNvidiaGPU]
			resALimitsGPU.Add(tmpGpu)
			
		}

		for _, container := range jBReplicasSetSpec.Template.Spec.Containers {
			log.Info("In sort jBReplicasSet Container: %v", container.Name)
			tmpCPU := *container.Resources.Requests.Cpu()
			resBRequestsCPU.Add(tmpCPU)

			tmpMem := *container.Resources.Requests.Memory()
			resBRequestsMem.Add(tmpMem)
			
			tmpGpu := container.Resources.Requests[ResourceNvidiaGPU]
			resBLimitsGPU.Add(tmpGpu)
			
		}

		//resALimitsGPU := jA.Template.Spec.Containers[0].Resources.Requests[ResourceNvidiaGPU]
		//resBLimitsGPU := jB.Template.Spec.Containers[0].Resources.Requests[ResourceNvidiaGPU]
		//log.Info("resA resB has GPU limits %v / %v", int(resALimitsGPU.Value()), int(resBLimitsGPU.Value()))
		
		if resALimitsGPU.Cmp(resBLimitsGPU) == 0 {
			if resARequestsCPU.Cmp(resBRequestsCPU) == 0 {
				return resARequestsMem.Cmp(resBRequestsMem) == -1
			}
			return resARequestsCPU.Cmp(resBRequestsCPU) == -1
		}
		return resALimitsGPU.Cmp(resBLimitsGPU) == -1
	}
	return scoreA < scoreB
}

func (j ComparedJobsList) Swap(a int, b int) {
	j[a], j[b] = j[b], j[a]
}


// sortedJobs return the names of sorted jobs by fulfillment and
// tiebreakers in ascending order.
func sortedJobs(j map[string]*trainer.TrainingJob) ComparedJobsList {
	//var js jobs
	js := make(ComparedJobsList, len(j))
	i := 0
	for k, v := range j {
		js[i] = ComparedJobs{k, v}
	    i++
	}
/*nextJob:
	for _, v := range j {
		for _, f := range filters {
			if !f(v) {
				continue nextJob
			}
		}
		js = append(js, v)
	}
*/
	sort.Sort(js)
	log.Info("in sortedJobs sorted js: %v", js)
	return js
}

func searchAssignableNode(r *ClusterResource, jA tfv1alpha1.TFReplicaSpec) (nodeName string) {
	for name, idle := range r.NodeInfos.NodesCPUIdleMilli {
		if jA.Template.Spec.Containers[0].Resources.Requests.Cpu().ScaledValue(resource.Milli) <= idle &&
			jA.Template.Spec.Containers[0].Resources.Requests.Memory().ScaledValue(resource.Mega) <= r.NodeInfos.NodesMemoryFreeMega[name] {
			return name
		}
	}
	return ""
}


func scaleDryRun(r *ClusterResource, j *trainer.TrainingJob, curDiff int, scaleDown bool) (additional int) {
	additionalGPUInstance := 0
	additionalCPUInstance := 0


	jobAReplicasSetList := j.GetJobReplicasSetList()
	var jAReplicasSetSpec tfv1alpha1.TFReplicaSpec

	log.Info("jobAReplicasSetList len: %v", len(jobAReplicasSetList))
	workerFlag := false
	for _, t := range jobAReplicasSetList {
		log.Info("in scaleDryRun t.Spec.TFReplicaType: %v", t.Spec.TFReplicaType)
		if t.Spec.TFReplicaType == tfv1alpha1.WORKER {
			workerFlag = true
			jAReplicasSetSpec = t.Spec
			break
		}
	}

	if (workerFlag == false){
		return
	}

	// tfv1alpha1.TFReplicaSpec
	//jAReplicasSetSpec := jAReplicasSet.GetReplicasSetSpec()


	log.Info("jAReplicasSetSpec TFReplicaType: %v", jAReplicasSetSpec.TFReplicaType)
	
	//if(jAReplicasSet.Spec.Template.Containers == nil){
	//	return
	//}
	var cpuRequestMilli int64
	var memRequestMega int64
	var gpuLimitNum resource.Quantity

	for _, container := range jAReplicasSetSpec.Template.Spec.Containers {
		log.Info("in scaleDryRun container info: %v", container)
		cpuRequestMilli += container.Resources.Requests.Cpu().ScaledValue(resource.Milli)
		memRequestMega  += container.Resources.Requests.Memory().ScaledValue(resource.Mega)
		tmpGpu			:= container.Resources.Requests[ResourceNvidiaGPU]
		gpuLimitNum.Add(tmpGpu)
	}
	//cpuRequestMilli := jAReplicasSet.Spec.Template.Spec.Containers[0].Resources.Requests.Cpu().ScaledValue(resource.Milli)
	//memRequestMega := jA.Template.Spec.Containers[0].Resources.Requests.Memory().ScaledValue(resource.Mega)
	//gpuLimitNum := jA.Template.Spec.Containers[0].Resources.Requests[ResourceNvidiaGPU]
	
	gpuLimit := int(gpuLimitNum.Value())

	log.Info("pod need CPU %v, Mem %v, GPU %v", cpuRequestMilli, memRequestMega, gpuLimit)

	nodeName := ""

	// Adjust resource upon return.
	defer func() {
		r.GPULimit += gpuLimit * additional
		r.CPURequestMilli += cpuRequestMilli * int64(additional)
		r.MemoryRequestMega += memRequestMega * int64(additional)
		if nodeName != "" {
			r.NodeInfos.NodesCPUIdleMilli[nodeName] += cpuRequestMilli * int64(additional)
			r.NodeInfos.NodesMemoryFreeMega[nodeName] += memRequestMega * int64(additional)
		}
	}()

	jobA := j.GetJob()

	// TODO(helin): j.TrainerJob.Spec.Parallelism may not reflect
	// the actual pod running for the trainer job. We need to
	// count the pod manually. And calculate the additional value
	// based on the running pod count,
	// j.TrainerJob.Spec.Parallelism, and curDiff.
	plannedInstance := int(*jAReplicasSetSpec.Replicas) + curDiff
	instanceMax := jobA.Spec.MaxInstance
	instanceMin := jobA.Spec.MinInstance

	// TODO(typhoonzero): refine below code to remove direction
	// ======================= scaleDown ======================
	if scaleDown {
		if plannedInstance > instanceMax {
			additional = -1
			return
		}
		//gpuCondition := r.GPULimit > int(float64(r.GPUTotal)*maxLoadDesired)
		cpuCondition := r.CPURequestMilli > int64(float64(r.CPUTotalMilli)*0.97)
		if cpuCondition {
			if plannedInstance > instanceMin {
				additional = -1
				return
			}

			// can not scale down further
			additional = 0
			return
		}
		// do not try to scale up
		return
	}
	// ======================= scaleUp ==========================

	if plannedInstance >= instanceMax {
		// Do not scale or scale down, don't need to check if
		// there are available free resources.
		additional = instanceMax - plannedInstance
		return
	}

	if r.MemoryTotalMega-r.MemoryRequestMega <= memRequestMega {
		// insufficient memory, do not scale
		additional = 0
		return
	}
	if nodeName = searchAssignableNode(r, jAReplicasSetSpec); nodeName == "" {
		additional = 0
		return
	}

	// NOTE: do not scale up to use full cluster resource of CPU
	//       but we do scale up for GPU.
	if int64(float64(r.CPUTotalMilli)*0.97)-r.CPURequestMilli >= cpuRequestMilli {
		additionalCPUInstance = 1
	}

	needGPU := gpuLimit > 0
	if needGPU && r.GPUTotal-r.GPULimit >= gpuLimit {
		additionalGPUInstance = 1
	}

	if needGPU {
		if additionalGPUInstance < additionalCPUInstance {
			additional = additionalGPUInstance
		} else {
			additional = additionalCPUInstance
		}
	} else {
		additional = additionalCPUInstance
	}

	return
}

/*
sorted = []ComparedJobs = ComparedJobsList
type ComparedJobs struct {
	Key string
  	Value *trainer.TrainingJob
}
*/

func (c *Controller) scaleAllDryRun(r ClusterResource, jobs map[string]*trainer.TrainingJob) map[string]int {

	diff := make(map[string]int)

	for {
		noChange := true
		sorted := sortedJobs(jobs)
		dryRun := func(j *trainer.TrainingJob, isScaleDown bool) {
			name := j.GetJob().ObjectMeta.GetName()
			log.Info("scale dry run job: %v", name)

			additional := scaleDryRun(&r, j, diff[name], isScaleDown)

			log.Info(
				"dry run scale job ",
				" name ", name, " current scale difference ", diff[name],
				" scale up number of instances ", additional, " cluster resource ", r,
			)
			diff[name] += additional

			if additional != 0 {
				noChange = false
			}
		}
		// scale up from the ones that need scaling up the
		// most.
		for _, j := range sorted {
			dryRun(j.Value, false)
		}

		// scale down from the ones that need scaling up the
		// least.
		for i := len(sorted) - 1; i >= 0; i-- {
			dryRun(sorted[i].Value, true)
		}

		if noChange {
			break
		}
	}

	return diff

}


/*** Jack Lin  ***/





// New method sets up service client handles and returns controller object
func New(cluster *Cluster, kubeClient kubernetes.Interface, tfJobClient tfjobclient.Interface,
	config tfv1alpha1.ControllerConfig, tfJobInformerFactory informers.SharedInformerFactory,
	enableGangScheduling bool) (*Controller, error) {
	tfJobInformer := tfJobInformerFactory.Kubeflow().V1alpha1().TFJobs()

	kubeflowscheme.AddToScheme(scheme.Scheme)
	log.Debug("Creating event broadcaster")
	eventBroadcaster := record.NewBroadcaster()
	eventBroadcaster.StartLogging(log.Infof)
	eventBroadcaster.StartRecordingToSink(&typedcorev1.EventSinkImpl{Interface: kubeClient.CoreV1().Events("")})
	recorder := eventBroadcaster.NewRecorder(scheme.Scheme, v1.EventSource{Component: controllerName})

	// Use a ratelimiter with overall  and per-item rate limitting.
	// The overall is a token bucket and the per-item is exponential
	// For the per item
	rateLimiter := workqueue.NewMaxOfRateLimiter(
		workqueue.NewItemExponentialFailureRateLimiter(5*time.Millisecond, 1000*time.Second),
		// 10 qps, 100 bucket size.  This is only for retry speed and its only the overall factor (not per item)
		&workqueue.BucketRateLimiter{Bucket: ratelimit.NewBucketWithRate(float64(10), int64(100))},
	)

	controller := &Controller{
		KubeClient:  kubeClient,
		TFJobClient: tfJobClient,
		WorkQueue:   workqueue.NewNamedRateLimitingQueue(rateLimiter, "TFjobs"),
		recorder:    recorder,
		// TODO(jlewi)): What to do about cluster.Cluster?
		jobs:                 make(map[string]*trainer.TrainingJob),
		config:               config,
		enableGangScheduling: enableGangScheduling,
		/*** Jack Lin ***/
		cluster:        cluster,
		/*** Jack Lin ***/
	}

	log.Info("Setting up event handlers")
	// Set up an event handler for when Foo resources change
	tfJobInformer.Informer().AddEventHandler(
		cache.FilteringResourceEventHandler{
			FilterFunc: func(obj interface{}) bool {
				switch t := obj.(type) {
				case *tfv1alpha1.TFJob:
					log.Debugf("filter tfjob name: %v", t.Name)
					return true
				default:
					return false
				}
			},
			Handler: cache.ResourceEventHandlerFuncs{
				AddFunc: controller.enqueueController,
				UpdateFunc: func(oldObj, newObj interface{}) {
					controller.enqueueController(newObj)
				},
				DeleteFunc: controller.enqueueController,
			},
		})

	controller.TFJobLister = tfJobInformer.Lister()
	controller.TFJobSynced = tfJobInformer.Informer().HasSynced
	controller.syncHandler = controller.syncTFJob

	return controller, nil
}

// Run will set up the event handlers for types we are interested in, as well
// as syncing informer caches and starting workers. It will block until stopCh
// is closed, at which point it will shutdown the workqueue and wait for
// workers to finish processing their current work items.
func (c *Controller) Run(threadiness int, stopCh <-chan struct{}) error {
	defer runtime.HandleCrash()
	defer c.WorkQueue.ShutDown()

	// Start the informer factories to begin populating the informer caches
	log.Info("Starting TFJob controller")

	// Wait for the caches to be synced before starting workers
	log.Info("Waiting for informer caches to sync")
	if ok := cache.WaitForCacheSync(stopCh, c.TFJobSynced); !ok {
		return fmt.Errorf("failed to wait for caches to sync")
	}

	log.Infof("Starting %v workers", threadiness)
	// Launch workers to process TFJob resources
	for i := 0; i < threadiness; i++ {
		go wait.Until(c.runWorker, time.Second, stopCh)
	}

	log.Info("Started workers")
	<-stopCh
	log.Info("Shutting down workers")

	return nil
}

// runWorker is a long-running function that will continually call the
// processNextWorkItem function in order to read and process a message on the
// workqueue.
func (c *Controller) runWorker() {
	for c.processNextWorkItem() {
	}
}

// processNextWorkItem will read a single work item off the workqueue and
// attempt to process it, by calling the syncHandler.
func (c *Controller) processNextWorkItem() bool {
	key, quit := c.WorkQueue.Get()
	if quit {
		return false
	}

	defer c.WorkQueue.Done(key)

	_, err := c.syncHandler(key.(string))
	if err == nil {
		// Calling forget resets the rate limiter for this item.
		// Since the sync was processed successfully we want to reset the ratelimiter
		// so that future events can be processed immediately.
		log.WithFields(log.Fields{
			"job": key,
		}).Infof("WorkQueue forgetting key %v", key)
		c.WorkQueue.Forget(key)
		return true
	}

	// There was an error processing the key so to retry we requeue it.
	// The WorkQueue uses a rate limiter to control when the key gets retried.
	utilruntime.HandleError(fmt.Errorf("Error syncing job: %v", err))
	c.WorkQueue.AddRateLimited(key)

	return true
}

// syncTFJob will sync the job with the given. This function is not meant to be invoked
// concurrently with the same key.
//
// When a job is completely processed it will return true indicating that its ok to forget about this job since
// no more processing will occur for it.
func (c *Controller) syncTFJob(key string) (bool, error) {
	startTime := time.Now()
	defer func() {
		log.WithFields(log.Fields{
			"job": key,
		}).Infof("Finished syncing job %q (%v)", key, time.Since(startTime))
	}()

	ns, name, err := cache.SplitMetaNamespaceKey(key)
	if err != nil {
		return false, err
	}
	if len(ns) == 0 || len(name) == 0 {
		return false, fmt.Errorf("invalid job key %q: either namespace or name is missing", key)
	}
	if ns != "jack-kubeflow" {
		return false, nil
	}

	tfJob, err := c.TFJobLister.TFJobs(ns).Get(name)

	if err != nil {
		if apierrors.IsNotFound(err) {
			log.WithFields(log.Fields{
				"job": key,
			}).Infof("Job has been deleted: %v", key)
			return true, nil
		}
		return false, err
	}

	// Create a new TrainingJob if there is no TrainingJob stored for it in the jobs map or if the UID's don't match.
	// The UID's won't match in the event we deleted the job and then recreated the job with the same name.
	if cJob, ok := c.jobs[key]; !ok || cJob.UID() != tfJob.UID {
		log.WithFields(log.Fields{
			"job": key,
		}).Infof("Creating new job %v", key)
		nc, err := trainer.NewJob(c.KubeClient, c.TFJobClient, c.recorder, tfJob, &c.config)

		if err != nil {
			log.WithFields(log.Fields{
				"job": key,
			}).Errorf("There was a problem creating NewJob %v; Error: %v", key, err)
			return false, err
		}
		c.jobs[key] = nc
	} else {
		// Replace the TFJob stored inside TrainingJob with the latest job.
		// We need to do this to pull in the latest changes to the spec/status.

		// Update replaces the TFJob corresponding to TrainingJob with the provided job.
		// This function is used when the Spec/Status of the job is modified outside the controller.
		// For example, if the user issues a delete request. This will update the metadata on the object
		// so we need to replace the spec.
		c.jobs[key].Update(tfJob)
	}

	/*** Jack Lin  ***/
	/***  Monitor the cluster resclusterource ***/
	r, err := c.cluster.SyncResource()
	if err != nil {
		return false, err
	}
	log.Info("sync cluster resource done", "resource", r)
	/*** Jack Lin  ***/


	nc := c.jobs[key]

	if err := nc.Reconcile(&c.config, c.enableGangScheduling); err != nil {
		return false, err
	}


	/*** Jack Lin  ***/
	/***  Get all jobs status ***/

	havePending := false
	for _, j := range c.jobs {
		// k8s job for trainers may not be created immediently
		// try sync it here
		/*if j.TrainerJob == nil {
			tj, err := a.cluster.GetTrainerJob(j.Config)
			if err != nil {
				log.Error(
					"Error getting the trainer k8s job, will sync later.",
					"name", j.Config.ObjectMeta.Name,
					"error", err,
				)
				continue
			}
			j.TrainerJob = tj
			a.jobs[key] = j
		}*/

		// j is a *trainer.TrainingJob a pointer to the trainer.TrainingJob
		total, _, pending, err := c.cluster.JobPods(j)

		if err != nil {
			log.Error("check if job is running failed", "error", err)
			continue
		}

		if total == pending {
			havePending = true
			break
		}
	}

	js := make(map[string]*trainer.TrainingJob)
	for key, j := range c.jobs {
		// k8s job for trainers may not be created immediently
		// try sync it here
		/*if j.TrainerJob == nil {
			tj, err := a.cluster.GetTrainerJob(j.Config)
			if err != nil {
				log.Error(
					"Error getting the trainer k8s job, will sync later.",
					"name", j.Config.ObjectMeta.Name,
					"error", err,
				)
				continue
			}
			j.TrainerJob = tj
			a.jobs[key] = j
		}*/

		total, running, pending, err:= c.cluster.JobPods(j)
		if err != nil {
			log.Error("check if job is running failed", "error", err)
			continue
		}

		log.Info(
			"job info ",
			"name ", key,
			"running ", running,
			"pending ", pending,
			"total ", total,
		)

		// Scale jobs only when all pods' "Phase" are
		// running, or some job is pending.
		if total == running || havePending {
			log.Info("jog", key, "is append into js")
			js[key] = j
			//js = append(js, j)
		}
	}



	/***  Scale Plan ***/
	diff := c.scaleAllDryRun(r, js)

	log.Info("Scale Plan %v", diff)
	log.Info("Congrats!!!")
	/*** Jack Lin  ***/	


	// TODO(jlewi): Why do we issue a get request again here?
	tfJob, err = c.TFJobClient.KubeflowV1alpha1().TFJobs(tfJob.ObjectMeta.Namespace).Get(tfJob.ObjectMeta.Name, metav1.GetOptions{})

	if err != nil {
		return false, err
	}

	// TODO(jlewi): This logic will need to change when/if we get rid of phases and move to conditions. At that
	// case we should forget about a job when the appropriate condition is reached.
	if tfJob.Status.Phase == tfv1alpha1.TFJobPhaseCleanUp {
		return true, nil
	}
	return false, nil

}

// obj could be an *batch.Job, or a DeletionFinalStateUnknown marker item.
func (c *Controller) enqueueController(obj interface{}) {
	key, err := keyFunc(obj)
	if err != nil {
		utilruntime.HandleError(fmt.Errorf("Couldn't get key for object %+v: %v", obj, err))
		return
	}

	c.WorkQueue.AddRateLimited(key)
}
