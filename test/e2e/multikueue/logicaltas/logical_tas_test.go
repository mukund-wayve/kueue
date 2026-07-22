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
	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta2"
	"sigs.k8s.io/kueue/pkg/util/logicaltas"
	workloadevict "sigs.k8s.io/kueue/pkg/workload/evict"
	"sigs.k8s.io/kueue/test/util"
)

var _ = ginkgo.Describe("Logical TAS spike", ginkgo.Label("area:multikueue", "feature:logicaltas", "feature:tas"), func() {
	var (
		fixture   *logicalTASFixture
		managerLq *kueue.LocalQueue
	)

	ginkgo.BeforeEach(func() {
		fixture = setupLogicalTASFixture(managerCQSpec{generated: true, cpu: "8", memory: "8Gi"})
		managerLq = createManagerLQ(fixture, fixture.managerCQs[0].Name, "user-queue")
	})

	ginkgo.AfterEach(func() {
		cleanupLogicalTASFixture(fixture)
	})

	ginkgo.It("should compute manager topology with a single cluster level before dispatch", func() {
		job := createTASJobWithWPC("topology-levels", fixture.managerNs.Name, managerLq.Name, "", 1, "500m")
		util.MustCreate(ctx, k8sManagerClient, job)
		waitForJobManagedByMultiKueue(job)

		wlKey := workloadKeyForJob(job)
		clusterName, levels := waitForManagerLogicalAdmission(wlKey)
		gomega.Expect(levels).To(gomega.Equal([]string{logicaltas.ClusterLabel}))
		gomega.Expect(clusterName).To(gomega.Or(gomega.Equal(fixture.workerCluster1.Name), gomega.Equal(fixture.workerCluster2.Name)))

		wc := fixture.workers[clusterName].client
		expectWorkerJobPodsRunning(wc, fixture.managerNs.Name, job.Name, 1)
	})
})

var _ = ginkgo.Describe("Logical TAS preemption", ginkgo.Label("area:multikueue", "feature:logicaltas", "feature:tas"), func() {
	var (
		fixture   *logicalTASFixture
		managerLq *kueue.LocalQueue
		highWPC   *kueue.WorkloadPriorityClass
		lowWPC    *kueue.WorkloadPriorityClass
	)

	ginkgo.BeforeEach(func() {
		fixture = setupLogicalTASFixture(managerCQSpec{
			generated:        true,
			cpu:              "20",
			memory:           "20Gi",
			enablePreemption: true,
		})
		managerLq = createManagerLQ(fixture, fixture.managerCQs[0].Name, "user-queue")
		highWPC, lowWPC = createWorkloadPriorityClasses(fixture)
	})

	ginkgo.AfterEach(func() {
		if highWPC != nil && lowWPC != nil {
			for _, cl := range []client.Client{k8sManagerClient, k8sWorker1Client, k8sWorker2Client} {
				util.ExpectObjectToBeDeletedWithTimeout(ctx, cl, highWPC, true, util.MediumTimeout)
				util.ExpectObjectToBeDeletedWithTimeout(ctx, cl, lowWPC, true, util.MediumTimeout)
			}
		}
		cleanupLogicalTASFixture(fixture)
	})

	ginkgo.It("should preempt only the low-priority job on the chosen worker cluster", func() {
		const (
			lowParallelism  int32 = 1
			highParallelism int32 = 2
			highPodCPU            = "1"
		)
		highGangCPU := resource.MustParse(highPodCPU)
		for i := int32(1); i < highParallelism; i++ {
			highGangCPU.Add(resource.MustParse(highPodCPU))
		}

		worker1Node := getTASWorkerNode(k8sWorker1Client)
		worker2Node := getTASWorkerNode(k8sWorker2Client)
		lowCPU1 := cpuRequestToLeave(worker1Node, highGangCPU)
		lowCPU2 := cpuRequestToLeave(worker2Node, highGangCPU)

		lowJob1 := createTASJobWithWPC("low-a", fixture.managerNs.Name, managerLq.Name, lowWPC.Name, lowParallelism, lowCPU1)
		util.MustCreate(ctx, k8sManagerClient, lowJob1)
		waitForJobManagedByMultiKueue(lowJob1)
		lowWlKey1 := workloadKeyForJob(lowJob1)

		var lowCluster1 string
		ginkgo.By("waiting for the first low job to land on a worker", func() {
			gomega.Eventually(func(g gomega.Gomega) {
				wl := &kueue.Workload{}
				g.Expect(k8sManagerClient.Get(ctx, lowWlKey1, wl)).To(gomega.Succeed())
				g.Expect(wl.Status.ClusterName).NotTo(gomega.BeNil())
				g.Expect(apimeta.FindStatusCondition(wl.Status.Conditions, kueue.WorkloadAdmitted).Status).To(gomega.Equal(metav1.ConditionTrue))
				lowCluster1 = *wl.Status.ClusterName
			}, util.LongTimeout, util.Interval).Should(gomega.Succeed())
		})

		lowJob2 := createTASJobWithWPC("low-b", fixture.managerNs.Name, managerLq.Name, lowWPC.Name, lowParallelism, lowCPU2)
		util.MustCreate(ctx, k8sManagerClient, lowJob2)
		waitForJobManagedByMultiKueue(lowJob2)
		lowWlKey2 := workloadKeyForJob(lowJob2)

		var lowCluster2 string
		ginkgo.By("waiting for the second low job to land on the other worker", func() {
			gomega.Eventually(func(g gomega.Gomega) {
				wl := &kueue.Workload{}
				g.Expect(k8sManagerClient.Get(ctx, lowWlKey2, wl)).To(gomega.Succeed())
				g.Expect(wl.Status.ClusterName).NotTo(gomega.BeNil())
				g.Expect(*wl.Status.ClusterName).NotTo(gomega.Equal(lowCluster1))
				lowCluster2 = *wl.Status.ClusterName
			}, util.LongTimeout, util.Interval).Should(gomega.Succeed())
		})

		highJob := createTASJobWithWPC("high-gang", fixture.managerNs.Name, managerLq.Name, highWPC.Name, highParallelism, highPodCPU)
		util.MustCreate(ctx, k8sManagerClient, highJob)
		waitForJobManagedByMultiKueue(highJob)
		highWlKey := workloadKeyForJob(highJob)

		var (
			chosenCluster      string
			evictedWlKey       types.NamespacedName
			unaffectedWlKey    types.NamespacedName
			unaffectedWorker   client.Client
			chosenWorkerClient client.Client
		)

		lowByCluster := map[string]types.NamespacedName{
			lowCluster1: lowWlKey1,
			lowCluster2: lowWlKey2,
		}

		ginkgo.By("waiting for the high-priority gang to be admitted on exactly one worker", func() {
			gomega.Eventually(func(g gomega.Gomega) {
				var admittedOn []string
				for clusterName, wc := range fixture.workers {
					if clusterName == "" {
						continue
					}
					wl := &kueue.Workload{}
					g.Expect(wc.client.Get(ctx, highWlKey, wl)).To(gomega.Succeed())
					if apimeta.FindStatusCondition(wl.Status.Conditions, kueue.WorkloadAdmitted).Status == metav1.ConditionTrue {
						admittedOn = append(admittedOn, clusterName)
					}
				}
				g.Expect(admittedOn).To(gomega.HaveLen(1))
				chosenCluster = admittedOn[0]
				chosenWorkerClient = fixture.workers[chosenCluster].client
				evictedWlKey = lowByCluster[chosenCluster]
				for clusterName, wlKey := range lowByCluster {
					if clusterName != chosenCluster {
						unaffectedWlKey = wlKey
						unaffectedWorker = fixture.workers[clusterName].client
						break
					}
				}
			}, util.LongTimeout, util.Interval).Should(gomega.Succeed())
		})

		ginkgo.By("checking only the victim on the chosen worker was preempted", func() {
			gomega.Eventually(func(g gomega.Gomega) {
				evictedWl := &kueue.Workload{}
				g.Expect(k8sManagerClient.Get(ctx, evictedWlKey, evictedWl)).To(gomega.Succeed())
				cond := apimeta.FindStatusCondition(evictedWl.Status.Conditions, kueue.WorkloadEvicted)
				g.Expect(cond).NotTo(gomega.BeNil())
				g.Expect(cond.Status).To(gomega.Equal(metav1.ConditionTrue))
				g.Expect(cond.Reason).To(gomega.Equal(kueue.WorkloadPreempted))
			}, util.LongTimeout, util.Interval).Should(gomega.Succeed())
			gomega.Consistently(func(g gomega.Gomega) {
				unaffectedWl := &kueue.Workload{}
				g.Expect(k8sManagerClient.Get(ctx, unaffectedWlKey, unaffectedWl)).To(gomega.Succeed())
				g.Expect(workloadevict.IsEvicted(unaffectedWl)).To(gomega.BeFalse())
				g.Expect(unaffectedWorker.Get(ctx, unaffectedWlKey, unaffectedWl)).To(gomega.Succeed())
				g.Expect(workloadevict.IsEvicted(unaffectedWl)).To(gomega.BeFalse())
			}, util.ShortConsistentDuration, util.ShortInterval).Should(gomega.Succeed())
		})

		ginkgo.By("checking the high gang is running on the chosen worker", func() {
			expectWorkerJobPodsRunning(chosenWorkerClient, fixture.managerNs.Name, highJob.Name, int(highParallelism))
		})
	})
})
