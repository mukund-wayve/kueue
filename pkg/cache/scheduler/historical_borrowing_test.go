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

package scheduler

import (
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	featuregatetesting "k8s.io/component-base/featuregate/testing"

	configapi "sigs.k8s.io/kueue/apis/config/v1beta2"
	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta2"
	"sigs.k8s.io/kueue/pkg/features"
	"sigs.k8s.io/kueue/pkg/resources"
	utiltesting "sigs.k8s.io/kueue/pkg/util/testing"
)

// TestHistoricalBorrowingRaisesShareWithoutFlaggingBorrowing verifies that once
// only remembered borrowing remains (no instantaneous borrowing), the node has a
// non-zero DRS but is not flagged as borrowing, so workloads within its nominal
// quota keep their priority and it is not a fair-sharing preemption target.
// Covered for a CQ seeded via live sampling and for a remembered-then-idle
// sibling CQ.
func TestHistoricalBorrowingRaisesShareWithoutFlaggingBorrowing(t *testing.T) {
	cpuReq := resources.FlavorResourceQuantities{cpuFR: resources.NewAmount(1)}
	cases := map[string]func(t *testing.T) DRS{
		"single CQ idle after sampling": func(t *testing.T) DRS {
			cache := newHistoricalBorrowingCache(t)
			cqImpl := cache.hm.ClusterQueue("cq")
			addUsage(cqImpl, cpuFR, cpuCores(15))
			sampleCQAfter(cache, "cq", time.Minute)
			// Drop usage below quota so there is no instantaneous borrowing.
			removeUsage(cqImpl, cpuFR, cpuCores(6))
			if instant := currentBorrowingByResource(cqImpl.getResourceNode()); len(instant) != 0 {
				t.Fatalf("expected no instantaneous borrowing, got %v", instant)
			}
			return dominantResourceShare(cqImpl, nil)
		},
		"remembered then idle sibling": func(t *testing.T) DRS {
			cache := newTwoCQCohortCache(t)
			return dominantResourceShare(rememberCPUBorrowing(cache, "cqA"), nil)
		},
	}
	for name, setup := range cases {
		t.Run(name, func(t *testing.T) {
			drs := setup(t)
			if drs.IsZero() {
				t.Fatal("expected non-zero DRS from historical borrowing")
			}
			if drs.IsBorrowing() || drs.IsBorrowingOn(cpuReq) {
				t.Fatal("expected history alone not to flag the node as borrowing")
			}
		})
	}
}

// TestHistoricalBorrowingFlagsOnlyCurrentFlavors verifies that remembered
// borrowing does not mark any flavor as borrowed: only the flavor the node
// currently borrows on is.
func TestHistoricalBorrowingFlagsOnlyCurrentFlavors(t *testing.T) {
	f1FR := resources.FlavorResource{Flavor: "f1", Resource: corev1.ResourceCPU}
	f2FR := resources.FlavorResource{Flavor: "f2", Resource: corev1.ResourceCPU}

	cache := newTwoFlavorCache(t)
	cqImpl := cache.hm.ClusterQueue("cq")

	// Currently borrowing only 2 cores on f1 (usage 12, nominal 10).
	addUsage(cqImpl, f1FR, cpuCores(12))
	loadCQ(cache, "cq", cpuHistoryStatus(8000))

	drs := dominantResourceShare(cqImpl, nil)
	if !drs.IsBorrowingOn(resources.FlavorResourceQuantities{f1FR: cpuCores(1)}) {
		t.Fatal("expected node to borrow on f1")
	}
	if drs.IsBorrowingOn(resources.FlavorResourceQuantities{f2FR: cpuCores(1)}) {
		t.Fatal("expected no borrowing on f2 from history alone")
	}
}

// TestHistoricalBorrowingSkipsZeroWeight verifies a node with zero fair weight
// ignores history: within quota its share stays 0 instead of becoming +Inf.
func TestHistoricalBorrowingSkipsZeroWeight(t *testing.T) {
	cache := newCatchupCache(t, historicalBorrowingCfg(), 1, 1)
	cqA := cache.hm.ClusterQueue("cqA")
	seedCPUHistory(&cqA.resourceNode, cpuCores(5))
	cqA.FairWeight = 0

	if drs := dominantResourceShare(cqA, nil); !drs.IsZero() {
		t.Fatalf("expected zero DRS for a zero-weight node with only history, got %+v", drs)
	}
}

func TestBorrowingAlphaRate(t *testing.T) {
	cases := map[string]struct {
		elapsed, halfLife float64
		// wantInUnitInterval asserts 0 < alpha < 1; otherwise want is the exact value.
		wantInUnitInterval bool
		want               float64
	}{
		"positive elapsed and half-life": {elapsed: 300, halfLife: 3600, wantInUnitInterval: true},
		"zero half-life":                 {elapsed: 300, halfLife: 0, want: 0},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			got := borrowingAlphaRate(tc.elapsed, tc.halfLife)
			if tc.wantInUnitInterval {
				if got <= 0 || got >= 1 {
					t.Fatalf("expected alpha in (0,1), got %v", got)
				}
				return
			}
			if got != tc.want {
				t.Fatalf("borrowingAlphaRate(%v, %v) = %v, want %v", tc.elapsed, tc.halfLife, got, tc.want)
			}
		})
	}
}

func TestHistoricalBorrowingDecaysToZero(t *testing.T) {
	cache := newHistoricalBorrowingCache(t)
	cqImpl := cache.hm.ClusterQueue("cq")
	addUsage(cqImpl, cpuFR, cpuCores(15))
	sampleCQAfter(cache, "cq", time.Minute)

	// Stop borrowing, then sample for much longer than the half-life: the EMA
	// must decay all the way to zero and the entry must be dropped.
	removeUsage(cqImpl, cpuFR, cpuCores(15))
	sampleCQAfter(cache, "cq", 30*halfLife)

	if len(cqImpl.getResourceNode().HistoricalBorrowing) != 0 {
		t.Fatalf("expected historical borrowing to be cleared, got %v", cqImpl.getResourceNode().HistoricalBorrowing)
	}
	if status := historicalBorrowingStatus(cqImpl, time.Now()); status != nil {
		t.Fatalf("expected nil status after decay, got %v", status)
	}
	if drs := dominantResourceShare(cqImpl, nil); !drs.IsZero() {
		t.Fatalf("expected zero DRS after decay, got %v", drs)
	}
}

// TestHistoricalBorrowingLoadsOnAdd verifies that adding a ClusterQueue or
// Cohort to the cache restores its EMA from the saved status, so the scheduler
// sees history before the first reconcile after a restart.
func TestHistoricalBorrowingLoadsOnAdd(t *testing.T) {
	withStatus := func(fs **kueue.FairSharingStatus) {
		*fs = &kueue.FairSharingStatus{HistoricalBorrowingStatus: cpuHistoryStatus(5000)}
	}
	cases := map[string]func(t *testing.T, c *Cache) resourceNode{
		"clusterqueue": func(t *testing.T, c *Cache) resourceNode {
			cq := singleFlavorCQ()
			cq.Name = "cq2"
			withStatus(&cq.Status.FairSharing)
			if err := c.AddClusterQueue(t.Context(), cq); err != nil {
				t.Fatalf("AddClusterQueue: %v", err)
			}
			return c.hm.ClusterQueue("cq2").getResourceNode()
		},
		"cohort": func(t *testing.T, c *Cache) resourceNode {
			cohort := singleFlavorCohort()
			cohort.Name = "cohort2"
			withStatus(&cohort.Status.FairSharing)
			if err := c.AddOrUpdateCohort(cohort); err != nil {
				t.Fatalf("AddOrUpdateCohort: %v", err)
			}
			return c.hm.Cohort("cohort2").getResourceNode()
		},
	}
	for name, add := range cases {
		t.Run(name, func(t *testing.T) {
			cache := newHistoricalBorrowingCache(t)
			if got := cpuHistory(add(t, cache)); got != 5000 {
				t.Fatalf("loaded historical borrowing = %v, want 5000", got)
			}
		})
	}
}

// TestHistoricalBorrowingNewerStatusWins verifies the in-memory EMA follows the
// saved status only when the status is newer than memory: a newer write from
// another replica is loaded, while an older or missing status is ignored.
func TestHistoricalBorrowingNewerStatusWins(t *testing.T) {
	for name, load := range map[string]struct {
		do   func(c *Cache, status *kueue.HistoricalBorrowingStatus)
		node func(c *Cache) resourceNode
	}{
		"clusterqueue": {
			do:   func(c *Cache, s *kueue.HistoricalBorrowingStatus) { loadCQ(c, "cq", s) },
			node: func(c *Cache) resourceNode { return c.hm.ClusterQueue("cq").getResourceNode() },
		},
		"cohort": {
			do:   func(c *Cache, s *kueue.HistoricalBorrowingStatus) { loadCohort(c, "cohort", s) },
			node: func(c *Cache) resourceNode { return c.hm.Cohort("cohort").getResourceNode() },
		},
	} {
		t.Run(name, func(t *testing.T) {
			cache := newHistoricalBorrowingCache(t)
			first := cpuHistoryStatus(5000)
			load.do(cache, first)
			if got := cpuHistory(load.node(cache)); got != 5000 {
				t.Fatalf("after first load = %v, want 5000", got)
			}

			older := cpuHistoryStatus(9000)
			older.LastUpdate = metav1.NewTime(first.LastUpdate.Add(-time.Second))
			load.do(cache, older)
			load.do(cache, nil)
			if got := cpuHistory(load.node(cache)); got != 5000 {
				t.Fatalf("older or missing status replaced memory: %v, want 5000", got)
			}

			newer := &kueue.HistoricalBorrowingStatus{LastUpdate: metav1.NewTime(first.LastUpdate.Add(time.Second))}
			load.do(cache, newer)
			if got := load.node(cache).HistoricalBorrowing; len(got) != 0 {
				t.Fatalf("newer empty status should clear memory, got %v", got)
			}
		})
	}
}

// TestHistoricalBorrowingLeaderIgnoresOwnWrite verifies the leader does not
// reload the status it just wrote: the persisted LastUpdate equals its
// in-memory sample time, so the next sync does not reset the sampling clock.
func TestHistoricalBorrowingLeaderIgnoresOwnWrite(t *testing.T) {
	cache := newHistoricalBorrowingCache(t)
	cqImpl := cache.hm.ClusterQueue("cq")
	addUsage(cqImpl, cpuFR, cpuCores(15))
	now := time.Now().Truncate(time.Second).Add(time.Hour)
	written, _, _ := cache.SyncCQHistoricalBorrowing("cq", nil, now, true)
	if written == nil {
		t.Fatal("expected a sample to be written")
	}
	want := cpuHistory(cqImpl.getResourceNode())
	// Same LastUpdate with a different value: a reload would be visible.
	echoed := cpuHistoryStatus(1)
	echoed.LastUpdate = written.LastUpdate
	interval := historicalBorrowingCfg().BorrowingSamplingInterval.Duration
	if _, wait, _ := cache.SyncCQHistoricalBorrowing("cq", echoed, now.Add(interval/2), true); wait != interval/2 {
		t.Fatalf("wait after own write = %v, want %v", wait, interval/2)
	}
	if got := cpuHistory(cqImpl.getResourceNode()); got != want {
		t.Fatalf("leader reloaded its own write: %v, want %v", got, want)
	}
}

// TestHistoricalBorrowingReloadsAfterRecreate verifies that deleting a
// ClusterQueue forgets its history, so a later ClusterQueue with the same name
// is loaded from its own saved status.
func TestHistoricalBorrowingReloadsAfterRecreate(t *testing.T) {
	cache := newHistoricalBorrowingCache(t)
	loadCQ(cache, "cq", cpuHistoryStatus(5000))

	cache.DeleteClusterQueue(singleFlavorCQ())
	if _, loaded := cache.hb.lastSample[historicalSeededKey("cq", "cq")]; loaded {
		t.Fatal("expected sampling clock to be cleared on delete")
	}

	cq := singleFlavorCQ()
	cq.Status.FairSharing = &kueue.FairSharingStatus{HistoricalBorrowingStatus: cpuHistoryStatus(3000)}
	if err := cache.AddClusterQueue(t.Context(), cq); err != nil {
		t.Fatalf("re-AddClusterQueue: %v", err)
	}
	if got := cpuHistory(cache.hm.ClusterQueue("cq").getResourceNode()); got != 3000 {
		t.Fatalf("reloaded historical borrowing = %v, want 3000", got)
	}
}

// TestHistoricalBorrowingDeleteCohortClearsImplicitNode verifies that deleting
// a Cohort still referenced by a ClusterQueue drops its history, so the
// surviving implicit node does not keep stale borrowing.
func TestHistoricalBorrowingDeleteCohortClearsImplicitNode(t *testing.T) {
	cache := newHistoricalBorrowingCache(t)
	loadCohort(cache, "cohort", cpuHistoryStatus(5000))

	cache.DeleteCohort("cohort")
	cohort := cache.hm.Cohort("cohort")
	if cohort == nil {
		t.Fatal("expected the cohort to survive as an implicit node")
	}
	if got := cohort.getResourceNode().HistoricalBorrowing; len(got) != 0 {
		t.Fatalf("expected implicit cohort history cleared, got %v", got)
	}

	if err := cache.AddOrUpdateCohort(singleFlavorCohort()); err != nil {
		t.Fatalf("re-AddOrUpdateCohort: %v", err)
	}
	if got := cache.hm.Cohort("cohort").getResourceNode().HistoricalBorrowing; len(got) != 0 {
		t.Fatalf("expected recreated cohort without status to start empty, got %v", got)
	}
}

// TestHistoricalBorrowingRestoreRoundTrip is a regression test for the original
// amplification bug: restoring persisted status and persisting it again must be
// an identity round-trip, even when the resource is provided by multiple
// flavors. Storing the EMA per resource name (not per flavor) guarantees this.
func TestHistoricalBorrowingRestoreRoundTrip(t *testing.T) {
	cache := newTwoFlavorCache(t)
	loadCQ(cache, "cq", cpuHistoryStatus(5000))

	persisted := historicalBorrowingStatus(cache.hm.ClusterQueue("cq"), time.Now())
	if persisted == nil {
		t.Fatal("expected persisted status, got nil")
	}
	if got := persisted.BorrowedResources.Cpu().MilliValue(); got != 5000 {
		t.Fatalf("round-trip amplified borrowing: got %d milli, want 5000 (no fan-out across 2 flavors)", got)
	}
}

// TestHistoricalBorrowingOption verifies HistoricalBorrowingOption only activates
// the strategy when fairSharing selects HistoricalBorrowing and supplies its
// config; every other case must yield an inactive (no-op) cache.
func TestHistoricalBorrowingOption(t *testing.T) {
	featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.HistoricalBorrowingFairSharing, true)

	cases := map[string]struct {
		fs         *configapi.FairSharing
		wantActive bool
	}{
		"nil fairSharing": {
			fs:         nil,
			wantActive: false,
		},
		"borrowing strategy": {
			fs:         &configapi.FairSharing{Strategy: configapi.DRSStrategyBorrowing},
			wantActive: false,
		},
		"historical strategy without config": {
			fs:         &configapi.FairSharing{Strategy: configapi.DRSStrategyHistoricalBorrowing},
			wantActive: false,
		},
		"historical strategy with config": {
			fs: &configapi.FairSharing{
				Strategy:            configapi.DRSStrategyHistoricalBorrowing,
				HistoricalBorrowing: historicalBorrowingCfg(),
			},
			wantActive: true,
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			cache := New(utiltesting.NewFakeClient(), HistoricalBorrowingOption(tc.fs))
			if got := cache.HistoricalBorrowingActive(); got != tc.wantActive {
				t.Fatalf("HistoricalBorrowingActive() = %v, want %v", got, tc.wantActive)
			}
		})
	}
}

// TestHistoricalBorrowingInactiveNoop verifies the public sync methods and the
// add hooks are no-ops while the strategy is inactive (feature gate disabled),
// returning nil and leaving nodes unloaded with no EMA map allocated.
func TestHistoricalBorrowingInactiveNoop(t *testing.T) {
	// Feature gate explicitly disabled, so the strategy is inactive even though
	// the historical borrowing config is supplied.
	featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.HistoricalBorrowingFairSharing, false)

	cache := New(utiltesting.NewFakeClient(), WithFairSharing(true), WithHistoricalBorrowing(historicalBorrowingCfg()))
	if err := cache.AddOrUpdateCohort(singleFlavorCohort()); err != nil {
		t.Fatalf("AddOrUpdateCohort: %v", err)
	}
	if err := cache.AddClusterQueue(t.Context(), singleFlavorCQ()); err != nil {
		t.Fatalf("AddClusterQueue: %v", err)
	}

	if cache.HistoricalBorrowingActive() {
		t.Fatal("expected strategy inactive with feature gate disabled")
	}

	now := time.Now()
	if status, _, found := cache.SyncCQHistoricalBorrowing("cq", cpuHistoryStatus(5000), now, true); status != nil || found {
		t.Fatalf("SyncCQHistoricalBorrowing on inactive cache = (%v, %v), want (nil, false)", status, found)
	}
	if status, _, found := cache.SyncCohortHistoricalBorrowing("cohort", cpuHistoryStatus(5000), now, true); status != nil || found {
		t.Fatalf("SyncCohortHistoricalBorrowing on inactive cache = (%v, %v), want (nil, false)", status, found)
	}
	if len(cache.hb.lastSample) != 0 {
		t.Fatalf("expected no nodes loaded while inactive, got %d", len(cache.hb.lastSample))
	}
	if got := cache.hm.ClusterQueue("cq").getResourceNode().HistoricalBorrowing; got != nil {
		t.Fatalf("expected nil historical borrowing map while inactive, got %v", got)
	}
}

// TestHistoricalBorrowingMissingObjectNoop verifies syncing a name that is not
// in the cache is a safe no-op (the cq/cohort == nil guard).
func TestHistoricalBorrowingMissingObjectNoop(t *testing.T) {
	cache := newHistoricalBorrowingCache(t)

	now := time.Now()
	if status, _, found := cache.SyncCQHistoricalBorrowing("missing", cpuHistoryStatus(5000), now, true); status != nil || found {
		t.Fatalf("SyncCQHistoricalBorrowing on missing CQ = (%v, %v), want (nil, false)", status, found)
	}
	if status, _, found := cache.SyncCohortHistoricalBorrowing("missing", cpuHistoryStatus(5000), now, true); status != nil || found {
		t.Fatalf("SyncCohortHistoricalBorrowing on missing cohort = (%v, %v), want (nil, false)", status, found)
	}
}
