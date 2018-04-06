# K8s Custom Resource and Operator For TensorFlow jobs

[![Build Status](https://travis-ci.org/kubeflow/tf-operator.svg?branch=master)](https://travis-ci.org/kubeflow/tf-operator)
[![Coverage Status](https://coveralls.io/repos/github/kubeflow/tf-operator/badge.svg?branch=master)](https://coveralls.io/github/kubeflow/tf-operator?branch=master)
[![Go Report Card](https://goreportcard.com/badge/github.com/kubeflow/tf-operator)](https://goreportcard.com/report/github.com/kubeflow/tf-operator)

## Quick Links

* [Prow test dashboard](https://k8s-testgrid.appspot.com/sig-big-data)
* [Prow jobs dashboard](https://prow.k8s.io/?repo=kubeflow%2Ftf-operator)
* [Argo UI for E2E tests](http://testing-argo.kubeflow.org)

## Overview

TFJob provides a Kubernetes custom resource that makes it easy to
run distributed or non-distributed TensorFlow jobs on Kubernetes.

Using a Custom Resource Definition (CRD) gives users the ability to create and manage TF Jobs just like builtin K8s resources. For example to
create a job

```
kubectl create -f examples/tf_job.yaml
```

To list jobs

```
kubectl get tfjobs

NAME          KINDS
example-job   TFJob.v1alpha.kubeflow.org
```

For additional information about motivation and design for the
CRD please refer to
[tf_job_design_doc.md](tf_job_design_doc.md).

### Requirements

TFJob requires Kubernetes >= 1.8
 * CRDs required Kubernetes >= 1.7
 * TFJob depends on Garbage Collection for CRDs which is only supported
   in >= 1.8
 * GPU support is evolving quickly and its best to use Kubernetes 1.8
   to get the latest features.

## Installing the TFJob CRD and operator on your k8s cluster

Please refer to the [Kubeflow user guide](https://github.com/kubeflow/kubeflow/blob/master/user_guide.md).

We recommend deploying Kubeflow in order to use the TFJob operator.

## Creating a job

You create a job by defining a TFJob and then creating it with.

```
kubectl create -f https://raw.githubusercontent.com/kubeflow/tf-operator/master/examples/tf_job.yaml
```

In this case the job spec looks like the following

```
apiVersion: "kubeflow.org/v1alpha1"
kind: "TFJob"
metadata:
  name: "example-job"
spec:
  replicaSpecs:
    - replicas: 1
      tfReplicaType: MASTER
      template:
        spec:
          containers:
            - image: gcr.io/tf-on-k8s-dogfood/tf_sample:dc944ff
              name: tensorflow
          restartPolicy: OnFailure
    - replicas: 1
      tfReplicaType: WORKER
      template:
        spec:
          containers:
            - image: gcr.io/tf-on-k8s-dogfood/tf_sample:dc944ff
              name: tensorflow
          restartPolicy: OnFailure
    - replicas: 2
      tfReplicaType: PS
```

Each replicaSpec defines a set of TensorFlow processes.
The tfReplicaType defines the semantics for the set of processes.
The semantics are as follows

**master**
  * A job must have 1 and only 1 master
  * The pod must contain a container named tensorflow
  * The overall status of the TFJob is determined by the exit code of the
    tensorflow container
      * 0 = success
      * 1-127 = permanent error
      * 128-255 = retryable error

**worker**
  * A job can have 0 to N workers
  * The pod must contain a container named tensorflow
  * Workers are automatically restarted if they exit

**ps**
  * A job can have 0 to N parameter servers
  * parameter servers are automatically restarted if they exit
  * If you do not specify a container named tensorflow the TFJob
    will automatically add a container to the pod that starts a
    standard TensorFlow gRPC server for each PS.


For each replica you define a **template** which is a K8s
[PodTemplateSpec](https://kubernetes.io/docs/api-reference/v1.8/#podtemplatespec-v1-core).
The template allows you to specify the containers, volumes, etc... that
should be created for each replica.


### Using GPUs

**Note** The use of GPUs and K8s is still in flux.
The following works with GKE & K8s 1.7.2. If this doesn't work on
your setup please consider opening an issue.

Ensure your K8s cluster is properly configured to use GPUs
  * Nodes must have GPUs attached
  * K8s cluster must recognize the nvidia-gpu resource type
  * GPU drivers must be installed on the cluster.
  * Your TFJob controller must be configured to properly attach
    volumes and set environment variables needed for GPUs.

To attach GPUs specify the GPU resource on the container e.g.

```
apiVersion: "kubeflow.org/v1alpha1"
kind: "TFJob"
metadata:
  name: "tf-smoke-gpu"
spec:
  replica_specs:
    - replicas: 1
      tfPort: 2222
      tfReplicaType: MASTER
      template:
        spec:
          containers:
            - image: gcr.io/tf-on-k8s-dogfood/tf_sample_gpu:latest
              name: tensorflow
              resources:
                limits:
                  alpha.kubernetes.io/nvidia-gpu: 1
          restartPolicy: OnFailure
```

Follow TensorFlow's [instructions](https://www.kubeflow.org/tutorials/using_gpu)
for using GPUs.

## Monitoring your job

To get the status of your job

```
kubectl get -o yaml tfjobs $JOB
```

Here is sample output for an example job

```
apiVersion: kubeflow.org/v1alpha1
kind: TFJob
metadata:
  clusterName: ""
  creationTimestamp: 2017-10-20T22:27:38Z
  generation: 0
  name: example-job
  namespace: default
  resourceVersion: "1881"
  selfLink: /apis/kubeflow.org/v1alpha1/namespaces/default/tfjobs/example-job
  uid: e11f9577-b5e5-11e7-8522-42010a8e01a4
spec:
  RuntimeId: 76no
  replicaSpecs:
  - replicas: 1
    template:
      metadata:
        creationTimestamp: null
      spec:
        containers:
        - image: gcr.io/tf-on-k8s-dogfood/tf_sample:dc944ff
          name: tensorflow
          resources: {}
        restartPolicy: OnFailure
    tfPort: 2222
    tfReplicaType: MASTER
  - replicas: 1
    template:
      metadata:
        creationTimestamp: null
      spec:
        containers:
        - image: gcr.io/tf-on-k8s-dogfood/tf_sample:dc944ff
          name: tensorflow
          resources: {}
        restartPolicy: OnFailure
    tfPort: 2222
    tfReplicaType: WORKER
  - replicas: 2
    template:
      metadata:
        creationTimestamp: null
      spec:
        containers:
        - image: tensorflow/tensorflow:1.3.0
          name: tensorflow
          resources: {}
          volumeMounts:
          - mountPath: /ps-server
            name: ps-config-volume
        restartPolicy: OnFailure
    tfPort: 2222
    tfReplicaType: PS
  tfImage: tensorflow/tensorflow:1.3.0
status:
  conditions: null
  controlPaused: false
  phase: Done
  reason: ""
  replicaStatuses:
  - ReplicasStates:
      Succeeded: 1
    state: Succeeded
    tf_replica_type: MASTER
  - ReplicasStates:
      Running: 1
    state: Running
    tf_replica_type: WORKER
  - ReplicasStates:
      Running: 2
    state: Running
    tf_replica_type: PS
  state: Succeeded

```

The first thing to note is the **RuntimeId**. This is a random unique
string which is used to give names to all the K8s resouces
(e.g Job controllers & services) that are created by the TFJob.

As with other K8s resources status provides information about the state
of the resource.

**phase** - Indicates the phase of a job and will be one of
 - Creating
 - Running
 - CleanUp
 - Failed
 - Done

**state** - Provides the overall status of the job and will be one of
  - Running
  - Succeeded
  - Failed

For each replica type in the job, there will be a ReplicaStatus that
provides the number of replicas of that type in each state.

For each replica type, the job creates a set of K8s
[Job Controllers](https://kubernetes.io/docs/concepts/workloads/controllers/jobs-run-to-completion/)
named

```
${REPLICA-TYPE}-${RUNTIME_ID}-${INDEX}
```

For example, if you have 2 parameter servers and runtime id 76n0 TFJob
will create the jobs

```
ps-76no-0
ps-76no-1
```

### TensorFlow Logs

Logging follows standard K8s logging practices.

You can use kubectl to get standard output/error for any of
your containers.

First find the pod created by the job controller for the replica of
index. Pods will be named

```
${REPLICA-TYPE}-${RUNTIME_ID}-${INDEX}-${RANDOM}
```

where RANDOM is a unique id generated by K8s to uniquely identify each
pod.

Once you've identified your pod you can get the logs using kubectl.

```
kubectl logs ${REPLICA-TYPE}-${RUNTIME_ID}-${INDEX}-${RADNOM}
```

If your cluster takes advantage of K8s
[logging infrastructure](https://kubernetes.io/docs/concepts/cluster-administration/logging/)
then your logs may also be shipped to an appropriate data store for
further analysis.

#### GKE

The default on GKE is send logs to
[Stackdriver logging](https://cloud.google.com/logging/docs/).

To get the logs for a particular pod you can use the following
advanced filter in Stackdriver logging's search UI.

```
resource.type="container"
resource.labels.pod_id=${POD_NAME}
```

where ${POD_NAME} is the name of the pod.

**Tip** If you don't know the id of the pod, just enter the RuntimeId
for your job into the Stackdriver logging search UI. This will find all
log entries with the RuntimeId anywhere in the log entry. Since the
RuntimeId is a random string, the only matches will be the log entries
for your job.

**Tip** If your program outputs an easily searchable log message with
the replica type and index then you can search for this log message
and use it to determine the ${POD_NAME} for a particular pod; e.g

```
cluster_json = os.getenv('TF_CONFIG')
cluster = json.loads(cluster)
logging.info("REPLICA_TYPE=%s,REPLICA_INDEX=%s", cluster["task"]["type"], cluster["task"]["index"])
```

This would log a message like

```
REPLICA_TYPE=worker,REPLICA_INDEX=0
```

which you could then search for in the StackDriver UI. Once you find
the entry you can expand it to see **resource.labels.pod_id**.


## Contributing

Please refer to the [developer_guide](developer_guide.md)

## Community

This is a part of Kubeflow, so please see [readme in kubeflow/kubeflow](https://github.com/kubeflow/kubeflow#get-involved) to get in touch with the community.
# tf-operator-extend
