import (
	"sort"
	"tf-operator/pkg/trainer"
)

/*** Jack Lin  ***/

// elastic job filter.
//func elastic(j job) bool {
//  return j.Config.Elastic()
//}

// gpu job filter.
//func gpu(j job) bool {
//  return j.Config.NeedGPU()
//}

//type ComparedJobs []*trainer.TrainingJob

type ComparedJobs struct {
	Key   string
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

type Pair struct {
	Key   string
	Value int
}

type PairList []Pair

func (p PairList) Len() int           { return len(p) }
func (p PairList) Less(i, j int) bool { return p[i].Value < p[j].Value }
func (p PairList) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }

func sortedJobSufficiency(jobSufficiency map[string]int) PairList {
	pl := make(PairList, len(jobSufficiency))
	i := 0
	for k, v := range jobSufficiency {
		pl[i] = Pair{k, v}
		i++
	}
	sort.Sort(sort.Reverse(pl))
	return pl
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

	if workerFlag == false {
		return
	}

	// tfv1alpha1.TFReplicaSpec
	//jAReplicasSetSpec := jAReplicasSet.GetReplicasSetSpec()

	log.Info("jAReplicasSetSpec TFReplicaType: %v", jAReplicasSetSpec.TFReplicaType)

	//if(jAReplicasSet.Spec.Template.Containers == nil){
	//  return
	//}
	var cpuRequestMilli int64
	var memRequestMega int64
	var gpuLimitNum resource.Quantity

	for _, container := range jAReplicasSetSpec.Template.Spec.Containers {
		log.Info("in scaleDryRun container info: %v", container)
		cpuRequestMilli += container.Resources.Requests.Cpu().ScaledValue(resource.Milli)
		memRequestMega += container.Resources.Requests.Memory().ScaledValue(resource.Mega)
		tmpGpu := container.Resources.Requests[ResourceNvidiaGPU]
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
	// scaleDown when there is pending job.
	// ======================= scaleDown ======================
	if scaleDown {
		if plannedInstance > instanceMin { // we want to remove extra pod for runnning job for the pending job
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

type Pair struct {
  Key string
  Value int
}
type PairList []Pair
jobSufficiencySorted = []Pair
jobSufficientSorted = []Pair

*/

func (c *Controller) scaleAllDryRun(r ClusterResource, jobs map[string]*trainer.TrainingJob, jobNotSufficient map[string]int, jobSufficient map[string]int) map[string]int {

	diff := make(map[string]int)

	sorted := sortedJobs(jobs)
	jobNotSufficientSorted := sortedJobSufficiency(jobNotSufficient)

	// to indicate the current job which is pending and we are giving the resource for it by scaling down running jobs.
	currentAddJob := 0

	for {

		jobSufficientSorted := sortedJobSufficiency(jobSufficient)
		noChange := true
		surplus := 0

		for _, j := range jobSufficientSorted {
			surplus += j.Value // current surplus
		}

		log.Info("currentAddJob: ", currentAddJob, "& jobNotSufficientSorted: ", len(jobNotSufficientSorted))

		if len(jobNotSufficientSorted) > 0 {
			if currentAddJob >= len(jobNotSufficientSorted) {
				break
			}
			// if the current surplus is not enough for the next job then
			if surplus < (-jobNotSufficientSorted[currentAddJob].Value) {
				break
			}
		}

		dryRun := func(j *trainer.TrainingJob, isScaleDown bool) {
			name := j.GetJob().ObjectMeta.GetName()
			log.Info("scale dry run job: %v", name)

			additional := scaleDryRun(&r, j, diff[name], isScaleDown)

			log.Info(
				"dry run scale job ",
				" name ", name, " current scale difference ", diff[name],
				" scale up number of instances ", additional, " cluster resource ", r,
			)

			jobSufficient[name] += additional
			if additional == -1 {
				if len(jobNotSufficientSorted) > 0 {
					jobNotSufficientSorted[currentAddJob].Value += 1
					// if the currentAddJob is sufficient then move to next job.
					if jobNotSufficientSorted[currentAddJob].Value >= 0 {
						currentAddJob += 1
					}
				}
			}

			diff[name] += additional

			if additional != 0 {
				noChange = false
			}
		}

		log.Info("in scaleAllDryRun sorted: %v ", sorted)

		// scale up from the ones that need scaling up the
		// most.

		for _, j := range sorted {
			log.Info("in scaleAllDryRun scale up %v ", j.Key)
			dryRun(j.Value, false)
		}

		if len(jobNotSufficientSorted) > 0 {
			// scale down from the ones that need scaling up the
			// least.
			for i := len(sorted) - 1; i >= 0; i-- {
				dryRun(sorted[i].Value, true)
			}
		}

		if noChange {
			break
		}
	}

	return diff

}

func (c *Controller) autoScale(r ClusterResource) map[string]int {

	/***  Get all jobs status ***/
	havePending := false
	for _, j := range c.jobs {

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
	jobSufficient := make(map[string]int)
	jobNotSufficient := make(map[string]int)
	for key, j := range c.jobs {

		total, running, pending, err := c.cluster.JobPods(j)
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

		jobTmp := c.jobs[key].GetJob()

		if running-jobTmp.Spec.MinInstance < 0 {
			jobNotSufficient[key] = running - jobTmp.Spec.MinInstance
		}

		if running-jobTmp.Spec.MinInstance > 0 {
			jobSufficient[key] = running - jobTmp.Spec.MinInstance
		}

		// Scale jobs only when all pods' "Phase" are
		// running, or some job is pending.
		if total == running || havePending {
			log.Info("jog", key, "is append into js")
			js[key] = j
			//js = append(js, j)
		}
	}

	/***  Scale Plan ***/
	diff := c.scaleAllDryRun(r, js, jobNotSufficient, jobSufficient)

	return diff
}

/*** Jack Lin  ***/