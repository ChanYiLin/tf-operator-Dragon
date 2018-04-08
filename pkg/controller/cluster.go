/*
Copyright 2018 Jack Lin (jacklin@laslab.cs.nthu.edu)

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at
  
    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package controller


import (
    //"fmt"
    log "github.com/sirupsen/logrus"
    "k8s.io/apimachinery/pkg/api/resource"
    metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
    "k8s.io/apimachinery/pkg/fields"
    "k8s.io/client-go/kubernetes"
    //"k8s.io/client-go/pkg/api/v1"
    //"k8s.io/kubernetes/pkg/api/v1"
    corev1 "k8s.io/api/core/v1"
    //batchv1 "k8s.io/client-go/pkg/apis/batch/v1"
    //"k8s.io/client-go/pkg/apis/extensions/v1beta1"
    "k8s.io/kubernetes/pkg/api"
)

const (
    // ResourceNvidiaGPU is the name of the Nvidia GPU resource.
    ResourceNvidiaGPU = "nvidia.com/gpu"
)




// Cluster resprensents a Kubernetes cluster.
type Cluster struct {
    clientset *kubernetes.Clientset
}

// NewCluster create a new instance of K8sCluster.
func NewCluster(clientset *kubernetes.Clientset) *Cluster {
    return &Cluster{
        clientset: clientset,
    }
}


// AddResourceList add another v1.ResourceList to first's inner
// quantity.  v1.ResourceList is equal to map[string]Quantity
func AddResourceList(a corev1.ResourceList, b corev1.ResourceList) {
    for resname, q := range b {
        v, ok := a[resname]
        if !ok {
            a[resname] = q.DeepCopy()
        }
        v.Add(q)
        a[resname] = v
    }

    return
}


// getPodsTotalRequestsAndLimits accumulate resource requests and
// limits from all pods containers.
func getPodsTotalRequestsAndLimits(podList *corev1.PodList) (reqs corev1.ResourceList, limits corev1.ResourceList, err error) {
    reqs, limits = corev1.ResourceList{}, corev1.ResourceList{}
    for _, pod := range podList.Items {
        for _, container := range pod.Spec.Containers {
            AddResourceList(reqs, container.Resources.Requests)
            AddResourceList(limits, container.Resources.Limits)
        }

        for _, container := range pod.Spec.InitContainers {
            AddResourceList(reqs, container.Resources.Requests)
            AddResourceList(limits, container.Resources.Limits)
        }
    }
    return
}

func updateNodesIdleResource(podList *corev1.PodList, nodesCPUIdleMilli map[string]int64, nodesMemoryFreeMega map[string]int64) (err error) {
    for _, pod := range podList.Items {
        nodeName := pod.Spec.NodeName
        if nodeName == "" {
            continue
        }
        for _, container := range pod.Spec.Containers {
            nodesCPUIdleMilli[nodeName] -= container.Resources.Requests.Cpu().ScaledValue(resource.Milli)
            nodesMemoryFreeMega[nodeName] -= container.Resources.Requests.Memory().ScaledValue(resource.Mega)
        }

        for _, container := range pod.Spec.InitContainers {
            nodesCPUIdleMilli[nodeName] -= container.Resources.Requests.Cpu().ScaledValue(resource.Milli)
            nodesMemoryFreeMega[nodeName] -= container.Resources.Requests.Memory().ScaledValue(resource.Mega)
        }
    }
    return
}

/*** from kubernetes/autoscaler 
// GpuRequestInfo contains an information about a set of pods requesting a GPU.
type GpuRequestInfo struct {
    // MaxRequest is maximum GPU request among pods
    MaxRequest resource.Quantity
    // Pods is a list of pods requesting GPU
    Pods []*apiv1.Pod
    // SystemLabels is a set of system labels corresponding to selected GPU
    // that needs to be passed to cloudprovider
    SystemLabels map[string]string
}
***/

/*** from kubernetes/autoscaler ***/
// GetGpuRequests returns a GpuRequestInfo for each type of GPU requested by
// any pod in pods argument. If the pod requests GPU, but doesn't specify what
// type of GPU it wants (via NodeSelector) it assumes it's DefaultGPUType.
func GetGpuRequests(podList *corev1.PodList) (GpuReqLimNum resource.Quantity){
    //result := make(map[string]GpuRequestInfo)
    //var GpuReqLimNum resource.Quantity

    for _, pod := range podList.Items {
        for _, container := range pod.Spec.Containers {
            if container.Resources.Requests != nil {
                containerGpu := container.Resources.Requests[ResourceNvidiaGPU]
                GpuReqLimNum.Add(containerGpu)
            }
        }
    }

    /*
    for _, pod := range pods {
        var podGpu resource.Quantity
        for _, container := range pod.Spec.Containers {
            if container.Resources.Requests != nil {
                containerGpu := container.Resources.Requests[ResourceNvidiaGPU]
                podGpu.Add(containerGpu)
            }
        }
        if podGpu.Value() == 0 {
            continue
        }

        gpuType := DefaultGPUType
        if gpuTypeFromSelector, found := pod.Spec.NodeSelector[GPULabel]; found {
            gpuType = gpuTypeFromSelector
        }

        requestInfo, found := result[gpuType]
        if !found {
            requestInfo = GpuRequestInfo{
                MaxRequest: podGpu,
                Pods:       make([]*apiv1.Pod, 0),
                SystemLabels: map[string]string{
                    GPULabel: gpuType,
                },
            }
        }
        if podGpu.Cmp(requestInfo.MaxRequest) > 0 {
            requestInfo.MaxRequest = podGpu
        }
        requestInfo.Pods = append(requestInfo.Pods, pod)
        result[gpuType] = requestInfo
    }*/
    return
}



// SyncResource will update free and total resources in k8s cluster.
func (c *Cluster) SyncResource() (res ClusterResource, err error) {

    nodes := c.clientset.CoreV1().Nodes()
    nodeList, err := nodes.List(metav1.ListOptions{})
    if err != nil {
        return ClusterResource{}, err
    }

    allocatable := make(corev1.ResourceList)
    nodesCPUIdleMilli := make(map[string]int64)
    nodesMemoryFreeMega := make(map[string]int64)

    for _, node := range nodeList.Items {
        nodesCPUIdleMilli[node.GetObjectMeta().GetName()] =
            node.Status.Allocatable.Cpu().ScaledValue(resource.Milli)
        nodesMemoryFreeMega[node.GetObjectMeta().GetName()] =
            node.Status.Allocatable.Memory().ScaledValue(resource.Mega)
        AddResourceList(allocatable, node.Status.Allocatable)
    }

    // Get non-terminated pods from all namespaces.
    namespace := ""

    // FIXME(typhoonzero): scan all pods is not a efficient way.
    // NOTE: pending pods need to be caculated for scale down.
    // NOTE: "terminating" pods' status is still running, do not
    // scale up/down the job if job is still at last scaling
    // process.
    fieldSelector, err := fields.ParseSelector("status.phase!=" + string(api.PodSucceeded) + ",status.phase!=" + string(api.PodFailed))
    if err != nil {
        return ClusterResource{}, err
    }

    allPodsList, err := c.clientset.CoreV1().Pods(namespace).List(metav1.ListOptions{FieldSelector: fieldSelector.String()})

    if err != nil {
        return ClusterResource{}, err
    }

    allReqs, allLimits, err := getPodsTotalRequestsAndLimits(allPodsList)
    if err != nil {
        return ClusterResource{}, err
    }

    /*** 待修改 換方法計算GPU數量 ***/
    var GPUTotalNum resource.Quantity

    for _, node := range nodeList.Items {
        nodeGpu := node.Status.Allocatable[ResourceNvidiaGPU]
        GPUTotalNum.Add(nodeGpu)
    }

    GPUReqLimNum := GetGpuRequests(allPodsList)
    
    log.Info("Cluster total GPU: %v",   int(GPUTotalNum.Value()))
    log.Info("Cluster all Req GPU: %v", int(GPUReqLimNum.Value()))

    // node.Status.Allocatable[ResourceNvidiaGPU]
    /*** 待修改 換方法計算GPU數量 ***/





    err = updateNodesIdleResource(allPodsList, nodesCPUIdleMilli, nodesMemoryFreeMega)

    if err != nil {
        return ClusterResource{}, err
    }



    res = ClusterResource{
        NodeCount:       len(nodeList.Items),
        GPUTotal:        int(GPUTotalNum.Value()), //int(allocatable.NvidiaGPU().Value())
        CPUTotalMilli:   allocatable.Cpu().ScaledValue(resource.Milli),
        MemoryTotalMega: allocatable.Memory().ScaledValue(resource.Mega),

        GPURequest:        int(GPUReqLimNum.Value()),   //int(allReqs.NvidiaGPU().Value())
        CPURequestMilli:   allReqs.Cpu().ScaledValue(resource.Milli),
        MemoryRequestMega: allReqs.Memory().ScaledValue(resource.Mega),

        GPULimit:        int(allReqs.NvidiaGPU().Value()),  //int(allLimits.NvidiaGPU().Value())
        CPULimitMilli:   allLimits.Cpu().ScaledValue(resource.Milli),
        MemoryLimitMega: allLimits.Memory().ScaledValue(resource.Mega),

        NodeInfos: NodeInfos{
            NodesCPUIdleMilli:   nodesCPUIdleMilli,
            NodesMemoryFreeMega: nodesMemoryFreeMega,
        },
    }
    return
}





