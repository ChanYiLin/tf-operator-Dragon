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
    "fmt"

    "k8s.io/apimachinery/pkg/api/resource"
    metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
    "k8s.io/apimachinery/pkg/fields"
    "k8s.io/client-go/kubernetes"
    "k8s.io/client-go/pkg/api/v1"
    batchv1 "k8s.io/client-go/pkg/apis/batch/v1"
    "k8s.io/client-go/pkg/apis/extensions/v1beta1"
    "k8s.io/kubernetes/pkg/api"
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
func AddResourceList(a v1.ResourceList, b v1.ResourceList) {
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


// SyncResource will update free and total resources in k8s cluster.
func (c *Cluster) SyncResource() (res ClusterResource, err error) {

    nodes := c.clientset.CoreV1().Nodes()
    nodeList, err := nodes.List(metav1.ListOptions{})
    if err != nil {
        return ClusterResource{}, err
    }

    allocatable := make(v1.ResourceList)
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

    err = updateNodesIdleResource(allPodsList, nodesCPUIdleMilli, nodesMemoryFreeMega)
    if err != nil {
        return ClusterResource{}, err
    }

    res = ClusterResource{
        NodeCount:       len(nodeList.Items),
        GPUTotal:        int(allocatable.NvidiaGPU().Value()),
        CPUTotalMilli:   allocatable.Cpu().ScaledValue(resource.Milli),
        MemoryTotalMega: allocatable.Memory().ScaledValue(resource.Mega),

        GPURequest:        int(allReqs.NvidiaGPU().Value()),
        CPURequestMilli:   allReqs.Cpu().ScaledValue(resource.Milli),
        MemoryRequestMega: allReqs.Memory().ScaledValue(resource.Mega),

        GPULimit:        int(allLimits.NvidiaGPU().Value()),
        CPULimitMilli:   allLimits.Cpu().ScaledValue(resource.Milli),
        MemoryLimitMega: allLimits.Memory().ScaledValue(resource.Mega),

        NodeInfos: NodeInfos{
            NodesCPUIdleMilli:   nodesCPUIdleMilli,
            NodesMemoryFreeMega: nodesMemoryFreeMega,
        },
    }
    return
}






