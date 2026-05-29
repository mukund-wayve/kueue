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

package core

import (
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	testingclock "k8s.io/utils/clock/testing"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	configapi "sigs.k8s.io/kueue/apis/config/v1beta2"
	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta2"
	schdcache "sigs.k8s.io/kueue/pkg/cache/scheduler"
	"sigs.k8s.io/kueue/pkg/features"
	utiltesting "sigs.k8s.io/kueue/pkg/util/testing"
	utiltestingapi "sigs.k8s.io/kueue/pkg/util/testing/v1beta2"
)

const (
	hbInterval = time.Minute
	hbHalfLife = time.Hour
)

func hbControllerCfg() *configapi.HistoricalBorrowingConfig {
	return &configapi.HistoricalBorrowingConfig{
		BorrowingHalfLifeTime:     metav1.Duration{Duration: hbHalfLife},
		BorrowingSamplingInterval: metav1.Duration{Duration: hbInterval},
	}
}

// hbStatus builds a FairSharing status carrying a single cpu historical
// borrowing entry last updated lastUpdate ago.
func hbStatus(cpuCores string, lastUpdate time.Time) *kueue.FairSharingStatus {
	return &kueue.FairSharingStatus{
		HistoricalBorrowingStatus: &kueue.HistoricalBorrowingStatus{
			BorrowedResources: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse(cpuCores)},
			LastUpdate:        metav1.NewTime(lastUpdate),
		},
	}
}

// newHBCQFixture wires a cohort plus a CQ object (with its status) into a fake
// client and an active historical-borrowing cache, returning a reconciler
// driven by clk.
func newHBCQFixture(t *testing.T, clk *testingclock.FakeClock, cohort *kueue.Cohort, cq *kueue.ClusterQueue, active bool) (*historicalBorrowingReconciler[*kueue.ClusterQueue], client.Client) {
	t.Helper()
	ctx, _ := utiltesting.ContextWithLog(t)
	cl := utiltesting.NewClientBuilder().WithObjects(cq).WithStatusSubresource(cq).Build()
	cfg := hbControllerCfg()
	opts := []schdcache.Option{schdcache.WithFairSharing(true)}
	if active {
		features.SetFeatureGateDuringTest(t, features.HistoricalBorrowingFairSharing, true)
		opts = append(opts, schdcache.WithHistoricalBorrowing(cfg))
	}
	cache := schdcache.New(cl, opts...)
	if err := cache.AddOrUpdateCohort(cohort); err != nil {
		t.Fatalf("AddOrUpdateCohort: %v", err)
	}
	if err := cache.AddClusterQueue(ctx, cq); err != nil {
		t.Fatalf("AddClusterQueue: %v", err)
	}
	r := newHistoricalBorrowingCQReconciler(cl, cache, cfg)
	r.clock = clk
	return r, cl
}

// cpuFlavorQuotas builds one FlavorQuotas per flavor, each with the given
// nominal cpu.
func cpuFlavorQuotas(nominal string, flavors ...string) []kueue.FlavorQuotas {
	fqs := make([]kueue.FlavorQuotas, 0, len(flavors))
	for _, f := range flavors {
		fqs = append(fqs, *utiltestingapi.MakeFlavorQuotas(f).
			ResourceQuotaWrapper("cpu").NominalQuota(nominal).Append().Obj())
	}
	return fqs
}

// cohortWithCPU builds a cohort providing the given nominal cpu on each flavor.
func cohortWithCPU(nominal string, flavors ...string) *kueue.Cohort {
	return utiltestingapi.MakeCohort("cohort").ResourceGroup(cpuFlavorQuotas(nominal, flavors...)...).Obj()
}

func defaultHBCohort() *kueue.Cohort { return cohortWithCPU("20", "default") }

// cqWithCPU builds a CQ in cohort "cohort" providing nominal cpu on each flavor.
func cqWithCPU(name, nominal string, flavors ...string) *utiltestingapi.ClusterQueueWrapper {
	return utiltestingapi.MakeClusterQueue(name).ResourceGroup(cpuFlavorQuotas(nominal, flavors...)...).Cohort("cohort")
}

func makeCQWithCohort(name string) *utiltestingapi.ClusterQueueWrapper {
	return cqWithCPU(name, "10", "default")
}

func reconcileCQ(t *testing.T, r *historicalBorrowingReconciler[*kueue.ClusterQueue], cq *kueue.ClusterQueue) reconcile.Result {
	t.Helper()
	ctx, _ := utiltesting.ContextWithLog(t)
	res, err := r.Reconcile(ctx, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(cq)})
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	return res
}

func getCQHistorical(t *testing.T, cl client.Client, name string) *kueue.HistoricalBorrowingStatus {
	t.Helper()
	ctx, _ := utiltesting.ContextWithLog(t)
	got := &kueue.ClusterQueue{}
	if err := cl.Get(ctx, client.ObjectKey{Name: name}, got); err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.Status.FairSharing == nil {
		return nil
	}
	return got.Status.FairSharing.HistoricalBorrowingStatus
}

func TestHBControllerInactiveNoOp(t *testing.T) {
	now := time.Now().Truncate(time.Second)
	clk := testingclock.NewFakeClock(now)
	cq := makeCQWithCohort("cq").Obj()
	r, cl := newHBCQFixture(t, clk, defaultHBCohort(), cq, false)

	res := reconcileCQ(t, r, cq)
	if res != (reconcile.Result{}) {
		t.Fatalf("expected empty result when inactive, got %+v", res)
	}
	if hb := getCQHistorical(t, cl, "cq"); hb != nil {
		t.Fatalf("expected no status write when inactive, got %v", hb)
	}
}

func TestHBControllerNotFound(t *testing.T) {
	now := time.Now().Truncate(time.Second)
	clk := testingclock.NewFakeClock(now)
	cq := makeCQWithCohort("cq").Obj()
	r, _ := newHBCQFixture(t, clk, defaultHBCohort(), cq, true)

	ctx, _ := utiltesting.ContextWithLog(t)
	res, err := r.Reconcile(ctx, reconcile.Request{NamespacedName: client.ObjectKey{Name: "absent"}})
	if err != nil {
		t.Fatalf("expected no error for missing object, got %v", err)
	}
	if res != (reconcile.Result{}) {
		t.Fatalf("expected empty result for missing object, got %+v", res)
	}
}

func TestHBControllerRequeueBeforeInterval(t *testing.T) {
	now := time.Now().Truncate(time.Second)
	clk := testingclock.NewFakeClock(now)
	cq := makeCQWithCohort("cq").Obj()
	cq.Status.FairSharing = hbStatus("5", now.Add(-30*time.Second))
	r, cl := newHBCQFixture(t, clk, defaultHBCohort(), cq, true)

	res := reconcileCQ(t, r, cq)
	if res.RequeueAfter != hbInterval-30*time.Second {
		t.Fatalf("expected requeue after %v, got %v", hbInterval-30*time.Second, res.RequeueAfter)
	}
	// No new sample taken: persisted LastUpdate is unchanged.
	if hb := getCQHistorical(t, cl, "cq"); hb == nil || !hb.LastUpdate.Equal(&metav1.Time{Time: now.Add(-30 * time.Second)}) {
		t.Fatalf("expected status unchanged before interval, got %v", hb)
	}
}

// TestHBControllerSamplesAndPersists covers the save path: a seeded 5-core
// history is sampled, decayed by the elapsed time, and persisted. With one
// interval elapsed it barely decays; with a full half-life of downtime (the
// restart case) it halves.
func TestHBControllerSamplesAndPersists(t *testing.T) {
	cases := map[string]struct {
		elapsed             time.Duration
		wantLoMilli, wantHi int64
	}{
		"one interval barely decays": {elapsed: hbInterval, wantLoMilli: 4901, wantHi: 5000},
		"half-life downtime halves":  {elapsed: hbHalfLife, wantLoMilli: 2500, wantHi: 2500},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			now := time.Now().Truncate(time.Second)
			clk := testingclock.NewFakeClock(now)
			cq := makeCQWithCohort("cq").Obj()
			cq.Status.FairSharing = hbStatus("5", now.Add(-tc.elapsed))
			r, cl := newHBCQFixture(t, clk, defaultHBCohort(), cq, true)

			res := reconcileCQ(t, r, cq)
			if res.RequeueAfter != hbInterval {
				t.Fatalf("expected requeue after %v, got %v", hbInterval, res.RequeueAfter)
			}
			hb := getCQHistorical(t, cl, "cq")
			if hb == nil {
				t.Fatal("expected persisted historical borrowing status")
			}
			if milli := hb.BorrowedResources.Cpu().MilliValue(); milli < tc.wantLoMilli || milli > tc.wantHi {
				t.Fatalf("persisted cpu = %dm, want [%d,%d]", milli, tc.wantLoMilli, tc.wantHi)
			}
		})
	}
}

func TestHBControllerRoundTrip(t *testing.T) {
	now := time.Now().Truncate(time.Second)
	clk := testingclock.NewFakeClock(now)
	// CQ provides cpu on two flavors; persistence must stay per-resource (no
	// fan-out/amplification across flavors).
	cq := cqWithCPU("cq", "10", "f1", "f2").Obj()
	cq.Status.FairSharing = hbStatus("5", now.Add(-hbInterval))
	r, cl := newHBCQFixture(t, clk, cohortWithCPU("20", "f1", "f2"), cq, true)

	reconcileCQ(t, r, cq)

	hb := getCQHistorical(t, cl, "cq")
	if hb == nil || len(hb.BorrowedResources) != 1 {
		t.Fatalf("expected single per-resource entry, got %v", hb)
	}
	if milli := hb.BorrowedResources.Cpu().MilliValue(); milli <= 4900 || milli > 5000 {
		t.Fatalf("round-trip amplified or lost borrowing across flavors: got %dm, want ~5000m", milli)
	}
}

// TestHBControllerClearsDecayedStatus verifies that once the in-memory EMA has
// decayed to empty (no current borrowing, a full half-life elapsed from a tiny
// seed), the persisted status is cleared rather than left stale. Otherwise the
// stale value would be resurrected by seeding on restart.
func TestHBControllerClearsDecayedStatus(t *testing.T) {
	now := time.Now().Truncate(time.Second)
	clk := testingclock.NewFakeClock(now)
	cq := makeCQWithCohort("cq").Obj()
	// Seed a sub-epsilon historical value with no current borrowing; a single
	// sample after one half-life drops it below the epsilon and deletes it.
	cq.Status.FairSharing = hbStatus("1m", now.Add(-hbHalfLife))
	r, cl := newHBCQFixture(t, clk, defaultHBCohort(), cq, true)

	res := reconcileCQ(t, r, cq)
	if res.RequeueAfter != hbInterval {
		t.Fatalf("expected requeue after %v, got %v", hbInterval, res.RequeueAfter)
	}
	if hb := getCQHistorical(t, cl, "cq"); hb != nil {
		t.Fatalf("expected decayed status to be cleared, got %v", hb)
	}
}

// TestHBControllerNodeNotInCacheKeepsStatus is a regression test for the restart
// race: the ClusterQueue exists in the API with a persisted EMA and a full
// interval has elapsed, but it is not yet in the scheduler cache. The reconcile
// must requeue without wiping the persisted status (seeding restores it once the
// node is cached), rather than mistaking the missing node for a decayed value.
func TestHBControllerNodeNotInCacheKeepsStatus(t *testing.T) {
	now := time.Now().Truncate(time.Second)
	clk := testingclock.NewFakeClock(now)
	cq := makeCQWithCohort("cq").Obj()
	cq.Status.FairSharing = hbStatus("5", now.Add(-hbInterval))

	cl := utiltesting.NewClientBuilder().WithObjects(cq).WithStatusSubresource(cq).Build()
	features.SetFeatureGateDuringTest(t, features.HistoricalBorrowingFairSharing, true)
	cache := schdcache.New(cl, schdcache.WithFairSharing(true), schdcache.WithHistoricalBorrowing(hbControllerCfg()))
	// Intentionally do NOT add the CQ to the cache, simulating the window after a
	// restart before the ClusterQueue reconciler has populated it.
	r := newHistoricalBorrowingCQReconciler(cl, cache, hbControllerCfg())
	r.clock = clk

	res := reconcileCQ(t, r, cq)
	if res.RequeueAfter != hbInterval {
		t.Fatalf("expected requeue after %v, got %v", hbInterval, res.RequeueAfter)
	}
	hb := getCQHistorical(t, cl, "cq")
	if hb == nil || hb.BorrowedResources.Cpu().MilliValue() != 5000 {
		t.Fatalf("expected persisted EMA preserved (5000m) while node uncached, got %v", hb)
	}
}

// TestHBControllerClampsNegativeElapsed verifies that a LastUpdate in the future
// (clock skew) does not sample with a negative elapsed; the reconcile requeues
// for a full interval and leaves the persisted value unchanged.
func TestHBControllerClampsNegativeElapsed(t *testing.T) {
	now := time.Now().Truncate(time.Second)
	clk := testingclock.NewFakeClock(now)
	cq := makeCQWithCohort("cq").Obj()
	future := now.Add(time.Hour)
	cq.Status.FairSharing = hbStatus("5", future)
	r, cl := newHBCQFixture(t, clk, defaultHBCohort(), cq, true)

	res := reconcileCQ(t, r, cq)
	if res.RequeueAfter != hbInterval {
		t.Fatalf("expected requeue after %v, got %v", hbInterval, res.RequeueAfter)
	}
	hb := getCQHistorical(t, cl, "cq")
	if hb == nil || !hb.LastUpdate.Equal(&metav1.Time{Time: future}) {
		t.Fatalf("expected unchanged status with future LastUpdate, got %v", hb)
	}
}

func TestHBControllerCohortSamplesAndPersists(t *testing.T) {
	now := time.Now().Truncate(time.Second)
	clk := testingclock.NewFakeClock(now)
	cohort := utiltestingapi.MakeCohort("cohort").
		ResourceGroup(
			*utiltestingapi.MakeFlavorQuotas("default").
				ResourceQuotaWrapper("cpu").NominalQuota("20").Append().Obj(),
		).Obj()
	cohort.Status.FairSharing = hbStatus("5", now.Add(-hbHalfLife))

	ctx, _ := utiltesting.ContextWithLog(t)
	cl := utiltesting.NewClientBuilder().WithObjects(cohort).WithStatusSubresource(cohort).Build()
	features.SetFeatureGateDuringTest(t, features.HistoricalBorrowingFairSharing, true)
	cfg := hbControllerCfg()
	cache := schdcache.New(cl, schdcache.WithFairSharing(true), schdcache.WithHistoricalBorrowing(cfg))
	if err := cache.AddOrUpdateCohort(cohort); err != nil {
		t.Fatalf("AddOrUpdateCohort: %v", err)
	}
	r := newHistoricalBorrowingCohortReconciler(cl, cache, cfg)
	r.clock = clk

	res, err := r.Reconcile(ctx, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(cohort)})
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if res.RequeueAfter != hbInterval {
		t.Fatalf("expected requeue after %v, got %v", hbInterval, res.RequeueAfter)
	}
	got := &kueue.Cohort{}
	if err := cl.Get(ctx, client.ObjectKeyFromObject(cohort), got); err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.Status.FairSharing == nil || got.Status.FairSharing.HistoricalBorrowingStatus == nil {
		t.Fatal("expected persisted cohort historical borrowing status")
	}
	if milli := got.Status.FairSharing.HistoricalBorrowingStatus.BorrowedResources.Cpu().MilliValue(); milli != 2500 {
		t.Fatalf("expected cohort decay 5000m -> 2500m, got %dm", milli)
	}
}
