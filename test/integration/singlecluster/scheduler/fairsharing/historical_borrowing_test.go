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

package fairsharing

import (
	"time"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	config "sigs.k8s.io/kueue/apis/config/v1beta2"
	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta2"
	"sigs.k8s.io/kueue/pkg/features"
	utiltestingapi "sigs.k8s.io/kueue/pkg/util/testing/v1beta2"
	"sigs.k8s.io/kueue/test/util/behavioral"
	"sigs.k8s.io/kueue/test/util/behavioral/integration"
)

// hbFairSharing builds a HistoricalBorrowing FairSharing config. An optional k
// (BorrowingHistoryWeight) may be passed; when omitted it is left unset and
// defaults to 1, matching the production default.
func hbFairSharing(samplingInterval, halfLife time.Duration, k ...float64) *config.FairSharing {
	hb := &config.HistoricalBorrowingConfig{
		BorrowingSamplingInterval: metav1.Duration{Duration: samplingInterval},
		BorrowingHalfLifeTime:     metav1.Duration{Duration: halfLife},
	}
	if len(k) > 0 {
		hb.BorrowingHistoryWeight = &k[0]
	}
	return &config.FairSharing{
		PreemptionStrategies: []config.PreemptionStrategy{
			config.LessThanOrEqualToFinalShare,
			config.LessThanInitialShare,
		},
		Strategy:            config.DRSStrategyHistoricalBorrowing,
		HistoricalBorrowing: hb,
	}
}

var _ = ginkgo.Describe("Scheduler Historical Borrowing Fair Sharing", ginkgo.Label("feature:fairsharing"), func() {
	var (
		defaultFlavor *kueue.ResourceFlavor
		ns            *corev1.Namespace
		cohorts       []*kueue.Cohort
		cqs           []*kueue.ClusterQueue
		lqs           []*kueue.LocalQueue
		wls           []*kueue.Workload
	)

	createFlavor := func() {
		defaultFlavor = utiltestingapi.MakeResourceFlavor("default").Obj()
		behavioral.MustCreate(ctx, k8sClient, defaultFlavor)
	}

	// startHB enables the feature gate and starts a manager configured for the
	// HistoricalBorrowing strategy. The gate must be set before the manager
	// starts because the cache only registers the sampling controllers while
	// HistoricalBorrowingActive() is true. It is safe to call twice in one spec
	// (stop then start) to simulate a controller restart; the flavor is created
	// separately so the restart does not recreate it.
	startHB := func(fs *config.FairSharing) {
		features.SetFeatureGateDuringTest(ginkgo.GinkgoTB(), features.HistoricalBorrowingFairSharing, true)
		fwk.StartManager(ctx, cfg, managerAndSchedulerSetupWithFairSharing(fs, nil))
	}

	createCohort := func(c *kueue.Cohort) *kueue.Cohort {
		behavioral.MustCreate(ctx, k8sClient, c)
		cohorts = append(cohorts, c)
		return c
	}

	createQueue := func(cq *kueue.ClusterQueue) *kueue.ClusterQueue {
		behavioral.MustCreate(ctx, k8sClient, cq)
		behavioral.ExpectClusterQueuesToBeActive(ctx, k8sClient, cq)
		cqs = append(cqs, cq)

		lq := utiltestingapi.MakeLocalQueue(cq.Name, ns.Name).ClusterQueue(cq.Name).Obj()
		behavioral.MustCreate(ctx, k8sClient, lq)
		behavioral.ExpectLocalQueuesToBeActive(ctx, k8sClient, lq)
		lqs = append(lqs, lq)
		return cq
	}

	// cpuQueue builds a single-flavor ("default") cpu ClusterQueue wrapper, so
	// callers can append options (e.g. Preemption) before passing it to
	// createQueue.
	cpuQueue := func(name string, cohort kueue.CohortReference, nominal string) *utiltestingapi.ClusterQueueWrapper {
		return utiltestingapi.MakeClusterQueue(name).
			Cohort(cohort).
			ResourceGroup(*utiltestingapi.MakeFlavorQuotas("default").Resource(corev1.ResourceCPU, nominal).Obj())
	}

	createWorkloads := func(queue string, count int, cpu string) {
		for range count {
			wl := utiltestingapi.MakeWorkloadWithGeneratedName("hb-", ns.Name).
				Queue(kueue.LocalQueueName(queue)).
				Request(corev1.ResourceCPU, cpu).Obj()
			wls = append(wls, wl)
			behavioral.MustCreate(ctx, k8sClient, wl)
		}
	}

	expectReserving := func(cqName string, n int32) {
		ginkgo.GinkgoHelper()
		gomega.Eventually(func(g gomega.Gomega) {
			cq := &kueue.ClusterQueue{}
			g.Expect(k8sClient.Get(ctx, client.ObjectKey{Name: cqName}, cq)).To(gomega.Succeed())
			g.Expect(cq.Status.ReservingWorkloads).To(gomega.Equal(n))
		}, behavioral.Timeout, behavioral.Interval).Should(gomega.Succeed())
	}

	expectPending := func(cqName string, n int32) {
		ginkgo.GinkgoHelper()
		gomega.Eventually(func(g gomega.Gomega) {
			cq := &kueue.ClusterQueue{}
			g.Expect(k8sClient.Get(ctx, client.ObjectKey{Name: cqName}, cq)).To(gomega.Succeed())
			g.Expect(cq.Status.PendingWorkloads).To(gomega.Equal(n))
		}, behavioral.Timeout, behavioral.Interval).Should(gomega.Succeed())
	}

	// queueBehindLender lets lender borrow all 6 cpu of cohort capacity, then
	// queues 3 one-cpu workloads on each contender. Freeing lender capacity one
	// cpu at a time afterwards makes the contenders compete for each slot, so
	// the scheduler orders them by fair share rather than by arrival.
	queueBehindLender := func(lender *kueue.ClusterQueue, contenders ...string) {
		ginkgo.GinkgoHelper()
		createWorkloads(lender.Name, 6, "1")
		expectReserving(lender.Name, 6)
		for _, cq := range contenders {
			createWorkloads(cq, 3, "1")
			expectPending(cq, 3)
		}
	}

	// borrowedCPU returns the persisted cpu historical borrowing from a
	// FairSharingStatus, or the zero Quantity when no history is recorded.
	borrowedCPU := func(fs *kueue.FairSharingStatus) resource.Quantity {
		if fs == nil || fs.HistoricalBorrowingStatus == nil {
			return resource.Quantity{}
		}
		return fs.HistoricalBorrowingStatus.BorrowedResources[corev1.ResourceCPU]
	}

	// hbStatus builds a persisted status with a known cpu value and lastUpdate;
	// pairing a past lastUpdate with a restart drives the seed-then-decay path.
	hbStatus := func(cpu string, lastUpdate time.Time) *kueue.HistoricalBorrowingStatus {
		return &kueue.HistoricalBorrowingStatus{
			BorrowedResources: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse(cpu)},
			LastUpdate:        metav1.NewTime(lastUpdate),
		}
	}

	cqBorrowedCPU := func(g gomega.Gomega, cqName string) resource.Quantity {
		cq := &kueue.ClusterQueue{}
		g.Expect(k8sClient.Get(ctx, client.ObjectKey{Name: cqName}, cq)).To(gomega.Succeed())
		return borrowedCPU(cq.Status.FairSharing)
	}

	cohortBorrowedCPU := func(g gomega.Gomega, cohortName string) resource.Quantity {
		c := &kueue.Cohort{}
		g.Expect(k8sClient.Get(ctx, client.ObjectKey{Name: cohortName}, c)).To(gomega.Succeed())
		return borrowedCPU(c.Status.FairSharing)
	}

	seedBorrowedCPU := func(cqName, cpu string, lastUpdate time.Time) {
		ginkgo.GinkgoHelper()
		cq := &kueue.ClusterQueue{}
		gomega.Expect(k8sClient.Get(ctx, client.ObjectKey{Name: cqName}, cq)).To(gomega.Succeed())
		if cq.Status.FairSharing == nil {
			cq.Status.FairSharing = &kueue.FairSharingStatus{}
		}
		cq.Status.FairSharing.HistoricalBorrowingStatus = hbStatus(cpu, lastUpdate)
		gomega.Expect(k8sClient.Status().Update(ctx, cq)).To(gomega.Succeed())
	}

	// clearBorrowedCPU wipes a ClusterQueue's persisted historical borrowing, so
	// that a restart re-seeds it via the no-status (seed-from-zero) path.
	clearBorrowedCPU := func(cqName string) {
		ginkgo.GinkgoHelper()
		cq := &kueue.ClusterQueue{}
		gomega.Expect(k8sClient.Get(ctx, client.ObjectKey{Name: cqName}, cq)).To(gomega.Succeed())
		if cq.Status.FairSharing == nil {
			return
		}
		cq.Status.FairSharing.HistoricalBorrowingStatus = nil
		gomega.Expect(k8sClient.Status().Update(ctx, cq)).To(gomega.Succeed())
	}

	seedCohortBorrowedCPU := func(cohortName, cpu string, lastUpdate time.Time) {
		ginkgo.GinkgoHelper()
		c := &kueue.Cohort{}
		gomega.Expect(k8sClient.Get(ctx, client.ObjectKey{Name: cohortName}, c)).To(gomega.Succeed())
		if c.Status.FairSharing == nil {
			c.Status.FairSharing = &kueue.FairSharingStatus{}
		}
		c.Status.FairSharing.HistoricalBorrowingStatus = hbStatus(cpu, lastUpdate)
		gomega.Expect(k8sClient.Status().Update(ctx, c)).To(gomega.Succeed())
	}

	// eventuallyBorrowedCPU asserts on the persisted historical borrowing for a
	// ClusterQueue until the check passes within the spec timeout.
	eventuallyBorrowedCPU := func(cqName string, check func(g gomega.Gomega, borrowed resource.Quantity)) {
		ginkgo.GinkgoHelper()
		gomega.Eventually(func(g gomega.Gomega) {
			check(g, cqBorrowedCPU(g, cqName))
		}, behavioral.Timeout, behavioral.Interval).Should(gomega.Succeed())
	}

	eventuallyCohortBorrowedCPU := func(cohortName string, timeout time.Duration, check func(g gomega.Gomega, borrowed resource.Quantity)) {
		ginkgo.GinkgoHelper()
		gomega.Eventually(func(g gomega.Gomega) {
			check(g, cohortBorrowedCPU(g, cohortName))
		}, timeout, behavioral.Interval).Should(gomega.Succeed())
	}

	// seedAndRestart pins a known remembered-borrowing value on a ClusterQueue
	// (with a lastUpdate one interval in the past) and restarts the manager with
	// a fresh cache, so the reconciler restores it via the seed-from-status path.
	seedAndRestart := func(cqName, cpu string, fs *config.FairSharing) {
		ginkgo.GinkgoHelper()
		fwk.StopManager(ctx)
		seedBorrowedCPU(cqName, cpu, time.Now().Add(-2*time.Second))
		startHB(fs)
		eventuallyBorrowedCPU(cqName, func(g gomega.Gomega, borrowed resource.Quantity) {
			g.Expect(borrowed.Cmp(resource.MustParse("1"))).To(gomega.Equal(1))
		})
	}

	// seedCohortAndRestart is the cohort analogue of seedAndRestart: it pins a
	// known remembered-borrowing value on a Cohort and restarts the manager with
	// a fresh cache, so the reconciler restores it via the seed-from-status path.
	seedCohortAndRestart := func(cohortName, cpu string, fs *config.FairSharing) {
		ginkgo.GinkgoHelper()
		fwk.StopManager(ctx)
		seedCohortBorrowedCPU(cohortName, cpu, time.Now().Add(-2*time.Second))
		startHB(fs)
		eventuallyCohortBorrowedCPU(cohortName, behavioral.Timeout, func(g gomega.Gomega, borrowed resource.Quantity) {
			g.Expect(borrowed.Cmp(resource.MustParse("1"))).To(gomega.Equal(1))
		})
	}

	ginkgo.BeforeEach(func() {
		cohorts = nil
		cqs = nil
		lqs = nil
		wls = nil
		defaultFlavor = nil
		ns = behavioral.CreateNamespaceFromPrefixWithLog(ctx, k8sClient, "hb-")
	})

	ginkgo.AfterEach(func() {
		for _, wl := range wls {
			behavioral.ExpectObjectToBeDeleted(ctx, k8sClient, wl, true)
		}
		for _, lq := range lqs {
			behavioral.ExpectObjectToBeDeleted(ctx, k8sClient, lq, true)
		}
		gomega.Expect(behavioral.DeleteNamespace(ctx, k8sClient, ns)).To(gomega.Succeed())
		for _, cq := range cqs {
			behavioral.ExpectObjectToBeDeleted(ctx, k8sClient, cq, true)
		}
		for _, cohort := range cohorts {
			behavioral.ExpectObjectToBeDeleted(ctx, k8sClient, cohort, true)
		}
		if defaultFlavor != nil {
			behavioral.ExpectObjectToBeDeleted(ctx, k8sClient, defaultFlavor, true)
		}
		fwk.StopManager(ctx)
	})

	ginkgo.It("records historical borrowing on the ClusterQueue and Cohort status", func() {
		// half-life == sampling interval (alpha 0.5) so the EMA builds within the
		// spec timeout from live sampling rather than warming up slowly.
		startHB(hbFairSharing(time.Second, time.Second))
		createFlavor()

		// root lends 8 cpu; ch1 has no quota of its own, so a borrowing CQ under
		// ch1 forces ch1 to borrow from root, exercising cohort-level history.
		createCohort(utiltestingapi.MakeCohort("root").
			ResourceGroup(*utiltestingapi.MakeFlavorQuotas("default").Resource(corev1.ResourceCPU, "8").Obj()).
			Obj())
		createCohort(utiltestingapi.MakeCohort("ch1").Parent("root").Obj())
		createQueue(cpuQueue("a", "ch1", "1").Obj())

		ginkgo.By("Saturating a so it borrows 5 cpu from the cohort")
		createWorkloads("a", 6, "1")
		expectReserving("a", 6)

		ginkgo.By("Expecting the ClusterQueue history to be sampled and persisted")
		gomega.Eventually(func(g gomega.Gomega) {
			cq := &kueue.ClusterQueue{}
			g.Expect(k8sClient.Get(ctx, client.ObjectKey{Name: "a"}, cq)).To(gomega.Succeed())
			g.Expect(cq.Status.FairSharing).ToNot(gomega.BeNil())
			g.Expect(cq.Status.FairSharing.HistoricalBorrowingStatus).ToNot(gomega.BeNil())
			g.Expect(cq.Status.FairSharing.HistoricalBorrowingStatus.BorrowedResources).To(gomega.HaveKey(corev1.ResourceCPU))
			g.Expect(cq.Status.FairSharing.HistoricalBorrowingStatus.LastUpdate.IsZero()).To(gomega.BeFalse())
		}, behavioral.Timeout, behavioral.Interval).Should(gomega.Succeed())

		ginkgo.By("Expecting the Cohort history to be sampled and persisted")
		gomega.Eventually(func(g gomega.Gomega) {
			cohort := &kueue.Cohort{}
			g.Expect(k8sClient.Get(ctx, client.ObjectKey{Name: "ch1"}, cohort)).To(gomega.Succeed())
			g.Expect(cohort.Status.FairSharing).ToNot(gomega.BeNil())
			g.Expect(cohort.Status.FairSharing.HistoricalBorrowingStatus).ToNot(gomega.BeNil())
			g.Expect(cohort.Status.FairSharing.HistoricalBorrowingStatus.BorrowedResources).To(gomega.HaveKey(corev1.ResourceCPU))
		}, behavioral.Timeout, behavioral.Interval).Should(gomega.Succeed())
	})

	ginkgo.It("penalizes a remembered borrower in admission order", func() {
		startHB(hbFairSharing(time.Second, time.Hour))
		createFlavor()

		createQueue(cpuQueue("a", "all", "1").Obj())
		createQueue(cpuQueue("b", "all", "1").Obj())
		lender := createQueue(cpuQueue("lender", "all", "4").Obj())

		// The long half-life keeps a's seeded memory effectively constant for
		// the spec.
		ginkgo.By("Seeding a with remembered borrowing and restarting the manager")
		seedAndRestart("a", "5", hbFairSharing(time.Second, time.Hour))

		ginkgo.By("Queueing equal demand on a and b while lender holds all capacity")
		queueBehindLender(lender, "b", "a")

		ginkgo.By("Freeing 2 cpu one at a time; the idle b wins both")
		integration.FinishRunningWorkloadsInCQ(ctx, k8sClient, lender, 1)
		expectReserving("b", 1)
		integration.FinishRunningWorkloadsInCQ(ctx, k8sClient, lender, 1)
		gomega.Eventually(func(g gomega.Gomega) {
			cqAStatus := &kueue.ClusterQueue{}
			cqBStatus := &kueue.ClusterQueue{}
			g.Expect(k8sClient.Get(ctx, client.ObjectKey{Name: "a"}, cqAStatus)).To(gomega.Succeed())
			g.Expect(k8sClient.Get(ctx, client.ObjectKey{Name: "b"}, cqBStatus)).To(gomega.Succeed())
			g.Expect(cqBStatus.Status.ReservingWorkloads).To(gomega.Equal(int32(2)))
			g.Expect(cqAStatus.Status.ReservingWorkloads).To(gomega.Equal(int32(0)))
		}, behavioral.Timeout, behavioral.Interval).Should(gomega.Succeed())
		gomega.Consistently(func(g gomega.Gomega) {
			cq := &kueue.ClusterQueue{}
			g.Expect(k8sClient.Get(ctx, client.ObjectKey{Name: "a"}, cq)).To(gomega.Succeed())
			g.Expect(cq.Status.ReservingWorkloads).To(gomega.Equal(int32(0)))
		}, behavioral.ConsistentDuration, behavioral.Interval).Should(gomega.Succeed())
	})

	ginkgo.It("penalizes a remembered borrowing cohort in admission order", func() {
		// Cohort analogue of the ClusterQueue ordering spec: the seeded history
		// lives on a child cohort (ch-a), so its inflated dominant resource share
		// must shut out ch-a's member CQ in favor of the idle sibling cohort's CQ.
		startHB(hbFairSharing(time.Second, time.Hour))
		createFlavor()

		// root's lendable pool is the sum of member nominal quotas (lender 4 +
		// cq-a 1 + cq-b 1 = 6). ch-a and ch-b own no quota, so any over-nominal
		// usage by their member CQ makes the cohort borrow.
		createCohort(utiltestingapi.MakeCohort("root").Obj())
		createCohort(utiltestingapi.MakeCohort("ch-a").Parent("root").Obj())
		createCohort(utiltestingapi.MakeCohort("ch-b").Parent("root").Obj())
		createQueue(cpuQueue("cq-a", "ch-a", "1").Obj())
		createQueue(cpuQueue("cq-b", "ch-b", "1").Obj())
		lender := createQueue(cpuQueue("lender", "root", "4").Obj())

		ginkgo.By("Seeding ch-a with remembered borrowing and restarting the manager")
		seedCohortAndRestart("ch-a", "5", hbFairSharing(time.Second, time.Hour))

		ginkgo.By("Queueing equal demand on cq-a and cq-b while lender holds all capacity")
		queueBehindLender(lender, "cq-b", "cq-a")

		ginkgo.By("Freeing 2 cpu one at a time; the idle cohort's cq-b wins both")
		integration.FinishRunningWorkloadsInCQ(ctx, k8sClient, lender, 1)
		expectReserving("cq-b", 1)
		integration.FinishRunningWorkloadsInCQ(ctx, k8sClient, lender, 1)
		gomega.Eventually(func(g gomega.Gomega) {
			cqA := &kueue.ClusterQueue{}
			cqB := &kueue.ClusterQueue{}
			g.Expect(k8sClient.Get(ctx, client.ObjectKey{Name: "cq-a"}, cqA)).To(gomega.Succeed())
			g.Expect(k8sClient.Get(ctx, client.ObjectKey{Name: "cq-b"}, cqB)).To(gomega.Succeed())
			g.Expect(cqB.Status.ReservingWorkloads).To(gomega.Equal(int32(2)))
			g.Expect(cqA.Status.ReservingWorkloads).To(gomega.Equal(int32(0)))
		}, behavioral.Timeout, behavioral.Interval).Should(gomega.Succeed())
		gomega.Consistently(func(g gomega.Gomega) {
			cq := &kueue.ClusterQueue{}
			g.Expect(k8sClient.Get(ctx, client.ObjectKey{Name: "cq-a"}, cq)).To(gomega.Succeed())
			g.Expect(cq.Status.ReservingWorkloads).To(gomega.Equal(int32(0)))
		}, behavioral.ConsistentDuration, behavioral.Interval).Should(gomega.Succeed())
	})

	ginkgo.It("does not penalize a remembered borrower when borrowingHistoryWeight is zero", func() {
		// k=0 disables the historical contribution to the DRS score, validating
		// the new knob end to end (config -> cache -> snapshot). Contrast with the
		// k>0 spec above, where the remembered borrower a is shut out.
		startHB(hbFairSharing(time.Second, time.Hour, 0))
		createFlavor()

		createQueue(cpuQueue("a", "all", "1").Obj())
		createQueue(cpuQueue("b", "all", "1").Obj())
		lender := createQueue(cpuQueue("lender", "all", "4").Obj())

		ginkgo.By("Seeding a with remembered borrowing and restarting the manager")
		seedAndRestart("a", "5", hbFairSharing(time.Second, time.Hour, 0))

		ginkgo.By("Queueing equal demand on a and b while lender holds all capacity")
		queueBehindLender(lender, "b", "a")

		ginkgo.By("Freeing 2 cpu; with k=0 a and b split it equally")
		integration.FinishRunningWorkloadsInCQ(ctx, k8sClient, lender, 2)
		expectReserving("a", 1)
		expectReserving("b", 1)
	})

	ginkgo.It("seeds a fresh borrower's history from zero rather than its current borrowing", func() {
		// Long half-life so the EMA warms up slowly and observably from zero. A
		// fresh node (no saved status) seeded from its current borrowing would
		// jump to ~5 cpu; seeding from zero keeps the recorded history far below.
		startHB(hbFairSharing(time.Second, 10*time.Minute))
		createFlavor()

		createQueue(cpuQueue("a", "all", "1").Obj())
		createQueue(cpuQueue("lender", "all", "5").Obj())

		ginkgo.By("a borrows 5 cpu and persists some history")
		createWorkloads("a", 6, "1")
		expectReserving("a", 6)
		eventuallyBorrowedCPU("a", func(g gomega.Gomega, borrowed resource.Quantity) {
			g.Expect(borrowed.CmpInt64(0)).To(gomega.Equal(1))
		})

		// Clear the saved status while a keeps borrowing, then restart so the
		// fresh cache re-seeds a (no status) while it actively borrows 5 cpu.
		ginkgo.By("Clearing the saved status and restarting while a still borrows")
		fwk.StopManager(ctx)
		clearBorrowedCPU("a")
		startHB(hbFairSharing(time.Second, 10*time.Minute))
		expectReserving("a", 6)

		ginkgo.By("Expecting history to warm up from zero, staying well below current borrowing")
		eventuallyBorrowedCPU("a", func(g gomega.Gomega, borrowed resource.Quantity) {
			// Recorded (> 0) but far below the current 5 cpu (< 1): warmed up from
			// zero, not seeded from current borrowing.
			g.Expect(borrowed.CmpInt64(0)).To(gomega.Equal(1))
			g.Expect(borrowed.Cmp(resource.MustParse("1"))).To(gomega.Equal(-1))
		})
	})

	ginkgo.It("does not record history when the strategy is Borrowing", func() {
		// Default FairSharing (Borrowing strategy) and feature gate left off: the
		// sampling controllers must not run and no history must be written.
		fwk.StartManager(ctx, cfg, managerAndSchedulerSetup(nil))
		createFlavor()

		createQueue(cpuQueue("a", "all", "1").Obj())
		createQueue(cpuQueue("lender", "all", "5").Obj())

		ginkgo.By("a borrows, but no historical borrowing status is written")
		createWorkloads("a", 6, "1")
		expectReserving("a", 6)
		gomega.Consistently(func(g gomega.Gomega) {
			cq := &kueue.ClusterQueue{}
			g.Expect(k8sClient.Get(ctx, client.ObjectKey{Name: "a"}, cq)).To(gomega.Succeed())
			if cq.Status.FairSharing != nil {
				g.Expect(cq.Status.FairSharing.HistoricalBorrowingStatus).To(gomega.BeNil())
			}
		}, behavioral.ConsistentDuration, behavioral.Interval).Should(gomega.Succeed())
	})

	ginkgo.It("decays remembered borrowing to zero and clears the status once borrowing stops", func() {
		// Short half-life so the recorded borrowing measurably decays within the
		// spec timeout, demonstrating the penalty fading back toward neutral.
		startHB(hbFairSharing(time.Second, time.Second))
		createFlavor()

		cqA := createQueue(cpuQueue("a", "all", "1").Obj())
		createQueue(cpuQueue("lender", "all", "5").Obj())

		ginkgo.By("a borrows 5 cpu and records history")
		createWorkloads("a", 6, "1")
		expectReserving("a", 6)
		eventuallyBorrowedCPU("a", func(g gomega.Gomega, borrowed resource.Quantity) {
			g.Expect(borrowed.CmpInt64(0)).To(gomega.Equal(1))
		})

		ginkgo.By("Releasing a's workloads and expecting the history to decay")
		integration.FinishRunningWorkloadsInCQ(ctx, k8sClient, cqA, 6)
		expectReserving("a", 0)
		eventuallyBorrowedCPU("a", func(g gomega.Gomega, borrowed resource.Quantity) {
			g.Expect(borrowed.Cmp(resource.MustParse("1"))).To(gomega.Equal(-1))
		})

		// Once the EMA fully decays below the epsilon, the controller writes a nil
		// status to clear it, so a stale value is not resurrected by seeding on a
		// later restart. MediumTimeout because reaching zero takes many half-lives.
		ginkgo.By("Expecting the fully decayed history to clear the persisted status")
		gomega.Eventually(func(g gomega.Gomega) {
			cq := &kueue.ClusterQueue{}
			g.Expect(k8sClient.Get(ctx, client.ObjectKey{Name: "a"}, cq)).To(gomega.Succeed())
			g.Expect(cq.Status.FairSharing).ToNot(gomega.BeNil())
			g.Expect(cq.Status.FairSharing.HistoricalBorrowingStatus).To(gomega.BeNil())
		}, behavioral.MediumTimeout, behavioral.Interval).Should(gomega.Succeed())
	})

	ginkgo.It("re-seeds from saved status and corrects the value on restart", func() {
		// Short sampling interval so the post-restart sample runs within the spec
		// timeout; half-life stays at 30s for the downtime decay assertion.
		startHB(hbFairSharing(time.Second, 30*time.Second))
		createFlavor()

		createQueue(cpuQueue("a", "all", "1").Obj())
		createQueue(cpuQueue("lender", "all", "5").Obj())

		ginkgo.By("a borrows and persists history")
		createWorkloads("a", 6, "1")
		expectReserving("a", 6)
		eventuallyBorrowedCPU("a", func(g gomega.Gomega, borrowed resource.Quantity) {
			g.Expect(borrowed.CmpInt64(0)).To(gomega.Equal(1))
		})

		// Delete (not finish) the workloads so the fresh cache rebuilt on restart
		// has zero current borrowing, isolating the seed-from-status decay.
		ginkgo.By("Deleting a's workloads so post-restart borrowing is zero")
		for _, wl := range wls {
			behavioral.ExpectObjectToBeDeleted(ctx, k8sClient, wl, true)
		}
		wls = nil
		expectReserving("a", 0)

		ginkgo.By("Stopping the manager to discard the in-memory cache")
		fwk.StopManager(ctx)

		// Simulate downtime deterministically: pin a known borrowed value and a
		// lastUpdate one half-life in the past. The first post-restart sample
		// computes elapsed from this timestamp, so the value should halve.
		ginkgo.By("Patching the saved status to simulate one half-life of downtime")
		seedBorrowedCPU("a", "4", time.Now().Add(-30*time.Second))

		ginkgo.By("Restarting the manager with a fresh cache")
		startHB(hbFairSharing(time.Second, 30*time.Second))

		ginkgo.By("Expecting the restored value to be halved by the first sample")
		eventuallyBorrowedCPU("a", func(g gomega.Gomega, borrowed resource.Quantity) {
			// Restored from saved status (> 1 cpu) but decayed by ~one half-life
			// (< the saved 4 cpu): demonstrates seed-from-status + downtime decay.
			g.Expect(borrowed.Cmp(resource.MustParse("4"))).To(gomega.Equal(-1))
			g.Expect(borrowed.Cmp(resource.MustParse("1"))).To(gomega.Equal(1))
		})
	})

	ginkgo.It("re-seeds a Cohort from saved status and corrects the value on restart", func() {
		// Mirrors the ClusterQueue restart spec at cohort granularity: the cohort
		// reconciler must restore a cohort's history from its saved status and the
		// first post-restart sample must run (proving reconcile happened) by
		// decaying the restored value by ~one half-life of simulated downtime.
		startHB(hbFairSharing(time.Second, 30*time.Second))
		createFlavor()

		// A member CQ (with no workloads) is required for ch1 to be realized in
		// the scheduler cache; its zero current borrowing lets the seeded value
		// decay cleanly without being blended toward live borrowing.
		createCohort(utiltestingapi.MakeCohort("root").
			ResourceGroup(*utiltestingapi.MakeFlavorQuotas("default").Resource(corev1.ResourceCPU, "8").Obj()).
			Obj())
		createCohort(utiltestingapi.MakeCohort("ch1").Parent("root").Obj())
		createQueue(cpuQueue("a", "ch1", "1").Obj())

		ginkgo.By("Stopping the manager and seeding ch1 with one half-life of downtime")
		fwk.StopManager(ctx)
		seedCohortBorrowedCPU("ch1", "4", time.Now().Add(-30*time.Second))

		ginkgo.By("Restarting the manager with a fresh cache")
		startHB(hbFairSharing(time.Second, 30*time.Second))

		ginkgo.By("Expecting the cohort's restored value to be halved by the first sample")
		eventuallyCohortBorrowedCPU("ch1", behavioral.Timeout, func(g gomega.Gomega, borrowed resource.Quantity) {
			// ch1 has no current borrowing, so the restored 4 cpu only decays: a
			// value in (1, 4) proves both seed-from-status and that a post-restart
			// sample (reconcile) ran on the cohort path.
			g.Expect(borrowed.Cmp(resource.MustParse("4"))).To(gomega.Equal(-1))
			g.Expect(borrowed.Cmp(resource.MustParse("1"))).To(gomega.Equal(1))
		})
	})

	ginkgo.It("preempts a remembered borrower to reclaim cohort capacity", func() {
		// Long half-life keeps a's seeded history effectively constant, so its
		// inflated dominant resource share drives fair-sharing preemption end to
		// end, not just admission ordering.
		startHB(hbFairSharing(time.Second, time.Hour))
		createFlavor()

		preemption := kueue.ClusterQueuePreemption{
			ReclaimWithinCohort: kueue.PreemptionPolicyAny,
			WithinClusterQueue:  kueue.PreemptionPolicyLowerPriority,
		}
		cqA := createQueue(cpuQueue("a", "all", "1").Preemption(preemption).Obj())
		createQueue(cpuQueue("b", "all", "1").Preemption(preemption).Obj())
		createQueue(cpuQueue("lender", "all", "4").Obj())

		// Seed a with remembered borrowing deterministically via a restart, so
		// its share is high before any contention begins.
		ginkgo.By("Seeding a with remembered borrowing and restarting the manager")
		seedAndRestart("a", "5", hbFairSharing(time.Second, time.Hour))

		// lender leaves 1 cpu of its nominal quota idle for a to borrow, and b
		// runs within its nominal quota, filling the cohort.
		ginkgo.By("Filling the cohort: lender 3, a 2 (borrowing 1), b 1")
		createWorkloads("lender", 3, "1")
		expectReserving("lender", 3)
		createWorkloads("a", 2, "1")
		expectReserving("a", 2)
		createWorkloads("b", 1, "1")
		expectReserving("b", 1)

		// b's next workload makes it borrow 1 cpu, as much as a borrows now, so
		// neither fair-sharing strategy would preempt a on current borrowing
		// alone; a's remembered borrowing makes it the victim.
		ginkgo.By("b borrows and preempts a via fair sharing")
		createWorkloads("b", 1, "1")
		behavioral.ExpectPreemptedWorkloadsTotalMetric("b", "InCohortFairSharing", 1)
		integration.FinishEvictionOfWorkloadsInCQ(ctx, k8sClient, cqA, 1)
		expectReserving("b", 2)
		expectReserving("a", 1)
	})

	ginkgo.It("preempts a remembered borrowing cohort to reclaim capacity", func() {
		// Cohort analogue of the ClusterQueue preemption spec: the seeded history
		// lives on ch-a, so ch-a's inflated dominant resource share must make its
		// member CQ the fair-sharing victim when the sibling cohort borrows.
		startHB(hbFairSharing(time.Second, time.Hour))
		createFlavor()

		preemption := kueue.ClusterQueuePreemption{
			ReclaimWithinCohort: kueue.PreemptionPolicyAny,
			WithinClusterQueue:  kueue.PreemptionPolicyLowerPriority,
		}
		createCohort(utiltestingapi.MakeCohort("root").Obj())
		createCohort(utiltestingapi.MakeCohort("ch-a").Parent("root").Obj())
		createCohort(utiltestingapi.MakeCohort("ch-b").Parent("root").Obj())
		cqA := createQueue(cpuQueue("cq-a", "ch-a", "1").Preemption(preemption).Obj())
		createQueue(cpuQueue("cq-b", "ch-b", "1").Preemption(preemption).Obj())
		createQueue(cpuQueue("lender", "root", "4").Obj())

		ginkgo.By("Seeding ch-a with remembered borrowing and restarting the manager")
		seedCohortAndRestart("ch-a", "5", hbFairSharing(time.Second, time.Hour))

		ginkgo.By("Filling the cohort: lender 3, cq-a 2 (borrowing 1), cq-b 1")
		createWorkloads("lender", 3, "1")
		expectReserving("lender", 3)
		createWorkloads("cq-a", 2, "1")
		expectReserving("cq-a", 2)
		createWorkloads("cq-b", 1, "1")
		expectReserving("cq-b", 1)

		ginkgo.By("cq-b borrows and preempts cq-a via fair sharing")
		createWorkloads("cq-b", 1, "1")
		behavioral.ExpectPreemptedWorkloadsTotalMetric("cq-b", "InCohortFairSharing", 1)
		integration.FinishEvictionOfWorkloadsInCQ(ctx, k8sClient, cqA, 1)
		expectReserving("cq-b", 2)
		expectReserving("cq-a", 1)
	})
})
