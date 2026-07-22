/*
Copyright The Kubernetes Authors.

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

package logicaltas

import (
	"fmt"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta2"
	"sigs.k8s.io/kueue/pkg/util/logicaltas"
	utiltestingapi "sigs.k8s.io/kueue/pkg/util/testing/v1beta2"
	testingjob "sigs.k8s.io/kueue/pkg/util/testingjobs/job"
	workloadjob "sigs.k8s.io/kueue/pkg/controller/jobs/job"
	"sigs.k8s.io/kueue/test/util"
)

const (
	tasNodeGroupLabel = "cloud.provider.com/node-group"
	instanceType      = "tas-node"

	passThroughCPU    = "10000"
	passThroughMemory = "10000Gi"
)

type workerClusterClients struct {
	client client.Client
}

type logicalTASFixture struct {
	managerNs *corev1.Namespace
	worker1Ns *corev1.Namespace
	worker2Ns *corev1.Namespace

	workerCluster1   *kueue.MultiKueueCluster
	workerCluster2   *kueue.MultiKueueCluster
	multiKueueConfig *kueue.MultiKueueConfig
	multiKueueAc     *kueue.AdmissionCheck

	managerTopology *kueue.Topology
	managerFlavor   *kueue.ResourceFlavor
	worker1Flavor   *kueue.ResourceFlavor
	worker2Flavor   *kueue.ResourceFlavor

	managerCQs []*kueue.ClusterQueue
	workers    map[string]workerClusterClients
}

type managerCQSpec struct {
	name             string
	generated        bool
	cpu              string
	memory           string
	enablePreemption bool
}

func setupLogicalTASFixture(cqs ...managerCQSpec) *logicalTASFixture {
	ginkgo.GinkgoHelper()
	if len(cqs) == 0 {
		cqs = []managerCQSpec{{generated: true, cpu: "8", memory: "8Gi"}}
	}

	f := &logicalTASFixture{
		managerNs: util.CreateNamespaceFromPrefixWithLog(ctx, k8sManagerClient, "ltas-"),
		workers: map[string]workerClusterClients{
			"": {client: k8sManagerClient},
		},
	}
	f.worker1Ns = util.CreateNamespaceWithLog(ctx, k8sWorker1Client, f.managerNs.Name)
	f.worker2Ns = util.CreateNamespaceWithLog(ctx, k8sWorker2Client, f.managerNs.Name)

	f.workerCluster1 = utiltestingapi.MakeMultiKueueClusterWithGeneratedName("worker1-").KubeConfig(kueue.SecretLocationType, "multikueue1").Obj()
	util.MustCreate(ctx, k8sManagerClient, f.workerCluster1)
	f.workerCluster2 = utiltestingapi.MakeMultiKueueClusterWithGeneratedName("worker2-").KubeConfig(kueue.SecretLocationType, "multikueue2").Obj()
	util.MustCreate(ctx, k8sManagerClient, f.workerCluster2)

	f.multiKueueConfig = utiltestingapi.MakeMultiKueueConfigWithGeneratedName("mkcfg-").
		Clusters(f.workerCluster1.Name, f.workerCluster2.Name).Obj()
	util.MustCreate(ctx, k8sManagerClient, f.multiKueueConfig)
	waitForMultiKueueClustersActive(f.workerCluster1, f.workerCluster2)

	f.multiKueueAc = utiltestingapi.MakeAdmissionCheck("").
		GeneratedName("mkac-").
		ControllerName(kueue.MultiKueueControllerName).
		Parameters(kueue.SchemeGroupVersion.Group, "MultiKueueConfig", f.multiKueueConfig.Name).
		Obj()
	util.CreateAdmissionChecksAndWaitForActive(ctx, k8sManagerClient, f.multiKueueAc)

	f.managerTopology = utiltestingapi.MakeTopology("cluster-" + f.managerNs.Name).
		Levels(logicaltas.ClusterLabel).
		Obj()
	util.MustCreate(ctx, k8sManagerClient, f.managerTopology)

	f.managerFlavor = utiltestingapi.MakeResourceFlavor("").
		GeneratedName("tas-flavor-").
		NodeLabel(tasNodeGroupLabel, instanceType).
		TopologyName(f.managerTopology.Name).
		Obj()
	util.MustCreate(ctx, k8sManagerClient, f.managerFlavor)

	f.worker1Flavor = utiltestingapi.MakeResourceFlavor(f.managerFlavor.Name).
		NodeLabel(tasNodeGroupLabel, instanceType).
		Obj()
	util.MustCreate(ctx, k8sWorker1Client, f.worker1Flavor)
	f.worker2Flavor = utiltestingapi.MakeResourceFlavor(f.managerFlavor.Name).
		NodeLabel(tasNodeGroupLabel, instanceType).
		Obj()
	util.MustCreate(ctx, k8sWorker2Client, f.worker2Flavor)

	f.workers[f.workerCluster1.Name] = workerClusterClients{client: k8sWorker1Client}
	f.workers[f.workerCluster2.Name] = workerClusterClients{client: k8sWorker2Client}

	for _, spec := range cqs {
		var cq *kueue.ClusterQueue
		if spec.generated {
			cq = utiltestingapi.MakeClusterQueue("").
				GeneratedName("cq-").
				ResourceGroup(*utiltestingapi.MakeFlavorQuotas(f.managerFlavor.Name).
					Resource(corev1.ResourceCPU, spec.cpu).
					Resource(corev1.ResourceMemory, spec.memory).
					Obj()).
				AdmissionChecks(kueue.AdmissionCheckReference(f.multiKueueAc.Name)).
				Obj()
		} else {
			cq = utiltestingapi.MakeClusterQueue(spec.name).
				ResourceGroup(*utiltestingapi.MakeFlavorQuotas(f.managerFlavor.Name).
					Resource(corev1.ResourceCPU, spec.cpu).
					Resource(corev1.ResourceMemory, spec.memory).
					Obj()).
				AdmissionChecks(kueue.AdmissionCheckReference(f.multiKueueAc.Name)).
				Obj()
		}
		if spec.enablePreemption {
			cq.Spec.Preemption = &kueue.ClusterQueuePreemption{
				WithinClusterQueue: kueue.PreemptionPolicyLowerPriority,
			}
		}
		util.CreateClusterQueuesAndWaitForActive(ctx, k8sManagerClient, cq)
		f.managerCQs = append(f.managerCQs, cq)
		createPassThroughWorkerCQ(k8sWorker1Client, cq.Name, f.worker1Flavor.Name)
		createPassThroughWorkerCQ(k8sWorker2Client, cq.Name, f.worker2Flavor.Name)
	}

	return f
}

func createPassThroughWorkerCQ(k8sClient client.Client, cqName, flavorName string) {
	ginkgo.GinkgoHelper()
	cq := utiltestingapi.MakeClusterQueue(cqName).
		ResourceGroup(*utiltestingapi.MakeFlavorQuotas(flavorName).
			Resource(corev1.ResourceCPU, passThroughCPU).
			Resource(corev1.ResourceMemory, passThroughMemory).
			Obj()).
		Obj()
	util.CreateClusterQueuesAndWaitForActive(ctx, k8sClient, cq)
}

func createManagerLQ(f *logicalTASFixture, cqName, lqName string) *kueue.LocalQueue {
	ginkgo.GinkgoHelper()
	lq := utiltestingapi.MakeLocalQueue(lqName, f.managerNs.Name).ClusterQueue(cqName).Obj()
	util.CreateLocalQueuesAndWaitForActive(ctx, k8sManagerClient, lq)
	createPassThroughLQ(f, cqName, lqName)
	return lq
}

func createPassThroughLQ(f *logicalTASFixture, cqName, lqName string) {
	ginkgo.GinkgoHelper()
	for _, wc := range []struct {
		client client.Client
		ns     *corev1.Namespace
	}{
		{k8sWorker1Client, f.worker1Ns},
		{k8sWorker2Client, f.worker2Ns},
	} {
		lq := utiltestingapi.MakeLocalQueue(lqName, wc.ns.Name).ClusterQueue(cqName).Obj()
		util.CreateLocalQueuesAndWaitForActive(ctx, wc.client, lq)
	}
}

func waitForMultiKueueClustersActive(clusters ...*kueue.MultiKueueCluster) {
	ginkgo.GinkgoHelper()
	for _, cluster := range clusters {
		gomega.Eventually(func(g gomega.Gomega) {
			updated := &kueue.MultiKueueCluster{}
			g.Expect(k8sManagerClient.Get(ctx, client.ObjectKeyFromObject(cluster), updated)).To(gomega.Succeed())
			cond := apimeta.FindStatusCondition(updated.Status.Conditions, kueue.MultiKueueClusterActive)
			g.Expect(cond).NotTo(gomega.BeNil())
			g.Expect(cond.Status).To(gomega.Equal(metav1.ConditionTrue))
		}, util.LongTimeout, util.Interval).Should(gomega.Succeed())
	}
}

func getTASWorkerNode(k8sClient client.Client) *corev1.Node {
	ginkgo.GinkgoHelper()
	nodes := &corev1.NodeList{}
	gomega.Expect(k8sClient.List(ctx, nodes, client.MatchingLabels{tasNodeGroupLabel: instanceType})).To(gomega.Succeed())
	gomega.Expect(nodes.Items).NotTo(gomega.BeEmpty())
	return &nodes.Items[0]
}

func cpuRequestToLeave(node *corev1.Node, reserve resource.Quantity) string {
	alloc := node.Status.Allocatable[corev1.ResourceCPU]
	alloc.Sub(reserve)
	if alloc.Sign() <= 0 {
		return "1"
	}
	return alloc.String()
}

func createTASJobWithWPC(name, ns, lqName, wpc string, parallelism int32, cpu string) *batchv1.Job {
	ginkgo.GinkgoHelper()
	job := testingjob.MakeJob(name, ns).
		Queue(kueue.LocalQueueName(lqName)).
		Parallelism(parallelism).
		Completions(parallelism).
		RequestAndLimit(corev1.ResourceCPU, cpu).
		RequestAndLimit(corev1.ResourceMemory, "128Mi").
		TerminationGracePeriod(1).
		Image(util.GetAgnHostImage(), util.BehaviorWaitForDeletion).
		Obj()
	w := &testingjob.JobWrapper{Job: *job}
	if wpc != "" {
		w = w.WorkloadPriorityClass(wpc)
	}
	return w.PodAnnotation(kueue.PodSetRequiredTopologyAnnotation, logicaltas.ClusterLabel).Obj()
}

func createWorkloadPriorityClasses(f *logicalTASFixture) (high, low *kueue.WorkloadPriorityClass) {
	ginkgo.GinkgoHelper()
	high = utiltestingapi.MakeWorkloadPriorityClass("high-" + f.managerNs.Name).
		PriorityValue(100).
		Obj()
	low = utiltestingapi.MakeWorkloadPriorityClass("low-" + f.managerNs.Name).
		PriorityValue(10).
		Obj()
	for _, cl := range []client.Client{k8sManagerClient, k8sWorker1Client, k8sWorker2Client} {
		util.MustCreate(ctx, cl, high.DeepCopy())
		util.MustCreate(ctx, cl, low.DeepCopy())
	}
	return high, low
}

func workloadKeyForJob(job *batchv1.Job) types.NamespacedName {
	return types.NamespacedName{
		Name:      workloadjob.GetWorkloadNameForJob(job.Name, job.UID),
		Namespace: job.Namespace,
	}
}

func waitForManagerLogicalAdmission(wlKey types.NamespacedName) (clusterName string, levels []string) {
	ginkgo.GinkgoHelper()
	gomega.Eventually(func(g gomega.Gomega) {
		wl := &kueue.Workload{}
		g.Expect(k8sManagerClient.Get(ctx, wlKey, wl)).To(gomega.Succeed())
		g.Expect(wl.Status.Admission).NotTo(gomega.BeNil())
		g.Expect(wl.Status.ClusterName).NotTo(gomega.BeNil())
		g.Expect(wl.Status.Admission.PodSetAssignments).To(gomega.HaveLen(1))
		ta := wl.Status.Admission.PodSetAssignments[0].TopologyAssignment
		g.Expect(ta).NotTo(gomega.BeNil())
		g.Expect(ta.Levels).To(gomega.Equal([]string{logicaltas.ClusterLabel}))
		g.Expect(wl.Status.Admission.PodSetAssignments[0].DelayedTopologyRequest).To(gomega.BeNil())
		clusterName = *wl.Status.ClusterName
		levels = ta.Levels
	}, util.LongTimeout, util.Interval).Should(gomega.Succeed())
	return clusterName, levels
}

func waitForJobManagedByMultiKueue(job *batchv1.Job) {
	ginkgo.GinkgoHelper()
	gomega.Eventually(func(g gomega.Gomega) {
		created := &batchv1.Job{}
		g.Expect(k8sManagerClient.Get(ctx, client.ObjectKeyFromObject(job), created)).To(gomega.Succeed())
		g.Expect(ptr.Deref(created.Spec.ManagedBy, "")).To(gomega.BeEquivalentTo(kueue.MultiKueueControllerName))
	}, util.Timeout, util.Interval).Should(gomega.Succeed())
}

func expectWorkerJobPodsRunning(workerClient client.Client, ns, jobName string, count int) {
	ginkgo.GinkgoHelper()
	listOpts := util.GetListOptsFromLabel(fmt.Sprintf("%s=%s", batchv1.JobNameLabel, jobName))
	gomega.Eventually(func(g gomega.Gomega) {
		pods := &corev1.PodList{}
		g.Expect(workerClient.List(ctx, pods, client.InNamespace(ns), listOpts)).To(gomega.Succeed())
		g.Expect(pods.Items).To(gomega.HaveLen(count))
		for _, pod := range pods.Items {
			g.Expect(pod.Spec.SchedulingGates).To(gomega.BeEmpty())
		}
	}, util.LongTimeout, util.Interval).Should(gomega.Succeed())
}

func cleanupLogicalTASFixture(f *logicalTASFixture) {
	ginkgo.GinkgoHelper()
	if f == nil {
		return
	}
	gomega.Expect(util.DeleteNamespace(ctx, k8sManagerClient, f.managerNs)).To(gomega.Succeed())
	gomega.Expect(util.DeleteNamespace(ctx, k8sWorker1Client, f.worker1Ns)).To(gomega.Succeed())
	gomega.Expect(util.DeleteNamespace(ctx, k8sWorker2Client, f.worker2Ns)).To(gomega.Succeed())

	for _, cq := range f.managerCQs {
		util.ExpectObjectToBeDeletedWithTimeout(ctx, k8sWorker1Client, &kueue.ClusterQueue{ObjectMeta: metav1.ObjectMeta{Name: cq.Name}}, true, util.MediumTimeout)
		util.ExpectObjectToBeDeletedWithTimeout(ctx, k8sWorker2Client, &kueue.ClusterQueue{ObjectMeta: metav1.ObjectMeta{Name: cq.Name}}, true, util.MediumTimeout)
		util.ExpectObjectToBeDeletedWithTimeout(ctx, k8sManagerClient, cq, true, util.MediumTimeout)
	}
	util.ExpectObjectToBeDeletedWithTimeout(ctx, k8sManagerClient, f.managerFlavor, true, util.MediumTimeout)
	util.ExpectObjectToBeDeletedWithTimeout(ctx, k8sManagerClient, f.managerTopology, true, util.MediumTimeout)
	util.ExpectObjectToBeDeletedWithTimeout(ctx, k8sWorker1Client, f.worker1Flavor, true, util.MediumTimeout)
	util.ExpectObjectToBeDeletedWithTimeout(ctx, k8sWorker2Client, f.worker2Flavor, true, util.MediumTimeout)
	util.ExpectObjectToBeDeletedWithTimeout(ctx, k8sManagerClient, f.multiKueueAc, true, util.MediumTimeout)
	util.ExpectObjectToBeDeletedWithTimeout(ctx, k8sManagerClient, f.multiKueueConfig, true, util.MediumTimeout)
	util.ExpectObjectToBeDeletedWithTimeout(ctx, k8sManagerClient, f.workerCluster1, true, util.MediumTimeout)
	util.ExpectObjectToBeDeletedWithTimeout(ctx, k8sManagerClient, f.workerCluster2, true, util.MediumTimeout)
}
