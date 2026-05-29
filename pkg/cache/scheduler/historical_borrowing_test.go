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
	"context"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	featuregatetesting "k8s.io/component-base/featuregate/testing"

	configapi "sigs.k8s.io/kueue/apis/config/v1beta2"
	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta2"
	"sigs.k8s.io/kueue/pkg/features"
	"sigs.k8s.io/kueue/pkg/resources"
	utiltesting "sigs.k8s.io/kueue/pkg/util/testing"
)

// TestHistoricalBorrowingFlagsBorrowingFromHistory verifies that once only
// remembered borrowing remains (no instantaneous borrowing), the node still has
// a non-zero DRS and is flagged as borrowing on the resource's flavor so
// preemption can target it. Covered for a CQ seeded via live sampling and for a
// remembered-then-idle sibling CQ.
func TestHistoricalBorrowingFlagsBorrowingFromHistory(t *testing.T) {
	cpuReq := resources.FlavorResourceQuantities{cpuFR: resources.NewAmount(1)}
	cases := map[string]func(t *testing.T) DRS{
		"single CQ idle after sampling": func(t *testing.T) DRS {
			cache := newHistoricalBorrowingCache(t)
			cqImpl := cache.hm.ClusterQueue("cq")
			addUsage(cqImpl, cpuFR, cpuCores(15))
			cache.SampleCQHistoricalBorrowing("cq", time.Minute)
			// Drop usage below quota so there is no instantaneous borrowing.
			removeUsage(cqImpl, cpuFR, cpuCores(6))
			if instant := currentBorrowingByResource(cqImpl.getResourceNode(), nil); len(instant) != 0 {
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
			if !drs.IsBorrowing() {
				t.Fatal("expected node flagged as borrowing from history")
			}
			if !drs.IsBorrowingOn(cpuReq) {
				t.Fatal("expected node to borrow on the requested cpu flavor for preemption targeting")
			}
		})
	}
}

// TestHistoricalBorrowingMarksAllFlavorsWhenHistoryDominates verifies Option A:
// when remembered borrowing for a resource exceeds current borrowing, every
// flavor of that resource is reported as borrowed, even a flavor with no current
// borrowing. The per-resource EMA has lost the originating flavor, so DRS treats
// the whole resource as borrowed for ordering consistency with the score.
func TestHistoricalBorrowingMarksAllFlavorsWhenHistoryDominates(t *testing.T) {
	f1FR := resources.FlavorResource{Flavor: "f1", Resource: corev1.ResourceCPU}
	f2FR := resources.FlavorResource{Flavor: "f2", Resource: corev1.ResourceCPU}

	cache := newTwoFlavorCache(t)
	cqImpl := cache.hm.ClusterQueue("cq")

	// Currently borrowing only 2 cores on f1 (usage 12, nominal 10).
	addUsage(cqImpl, f1FR, cpuCores(12))
	// Remembered borrowing of 8 cores on cpu dominates the 2-core instant.
	cache.EnsureCQHistoricalBorrowingSeeded("cq", cpuHistoryStatus(8000))

	drs := dominantResourceShare(cqImpl, nil)
	if !drs.IsBorrowingOn(resources.FlavorResourceQuantities{f1FR: cpuCores(1)}) {
		t.Fatal("expected node to borrow on f1 (current + history)")
	}
	if !drs.IsBorrowingOn(resources.FlavorResourceQuantities{f2FR: cpuCores(1)}) {
		t.Fatal("expected node to borrow on f2 from dominating history despite no current f2 borrowing")
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
	cache.SampleCQHistoricalBorrowing("cq", time.Minute)

	// Stop borrowing, then sample for much longer than the half-life: the EMA
	// must decay all the way to zero and the entry must be dropped.
	removeUsage(cqImpl, cpuFR, cpuCores(15))
	cache.SampleCQHistoricalBorrowing("cq", 10*halfLife)

	if len(cqImpl.getResourceNode().HistoricalBorrowing) != 0 {
		t.Fatalf("expected historical borrowing to be cleared, got %v", cqImpl.getResourceNode().HistoricalBorrowing)
	}
	if status := cache.historicalBorrowingStatusFromNode(cqImpl); status != nil {
		t.Fatalf("expected nil status after decay, got %v", status)
	}
	if drs := dominantResourceShare(cqImpl, nil); !drs.IsZero() {
		t.Fatalf("expected zero DRS after decay, got %v", drs)
	}
}

// TestHistoricalBorrowingSeeding verifies that both CQ and cohort nodes restore
// their EMA from a persisted status and are flagged seeded (the restart path).
func TestHistoricalBorrowingSeeding(t *testing.T) {
	cases := map[string]struct {
		seed func(c *Cache, status *kueue.HistoricalBorrowingStatus)
		node func(c *Cache) resourceNode
		key  string
	}{
		"clusterqueue": {
			seed: func(c *Cache, status *kueue.HistoricalBorrowingStatus) {
				c.EnsureCQHistoricalBorrowingSeeded("cq", status)
			},
			node: func(c *Cache) resourceNode { return c.hm.ClusterQueue("cq").getResourceNode() },
			key:  historicalSeededKey("cq", "cq"),
		},
		"cohort": {
			seed: func(c *Cache, status *kueue.HistoricalBorrowingStatus) {
				c.EnsureCohortHistoricalBorrowingSeeded("cohort", status)
			},
			node: func(c *Cache) resourceNode { return c.hm.Cohort("cohort").getResourceNode() },
			key:  historicalSeededKey("cohort", "cohort"),
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			cache := newHistoricalBorrowingCache(t)
			tc.seed(cache, cpuHistoryStatus(5000))

			if got := tc.node(cache).HistoricalBorrowing[corev1.ResourceCPU]; amountI64(got) != 5000 {
				t.Fatalf("seeded historical borrowing = %d, want 5000", amountI64(got))
			}
			if !cache.hb.seeded.Has(tc.key) {
				t.Fatal("expected node to be marked seeded")
			}
		})
	}
}

// TestHistoricalBorrowingReseedsAfterRecreate verifies that deleting a
// ClusterQueue forgets its seeded marker, so a later ClusterQueue with the same
// name is re-seeded from persisted status instead of starting empty.
func TestHistoricalBorrowingReseedsAfterRecreate(t *testing.T) {
	cache := newHistoricalBorrowingCache(t)
	ctx := context.Background()
	status := cpuHistoryStatus(5000)

	cache.EnsureCQHistoricalBorrowingSeeded("cq", status)
	if !cache.hb.seeded.Has(historicalSeededKey("cq", "cq")) {
		t.Fatal("expected CQ to be marked seeded")
	}

	cache.DeleteClusterQueue(singleFlavorCQ())
	if cache.hb.seeded.Has(historicalSeededKey("cq", "cq")) {
		t.Fatal("expected seeded marker to be cleared on delete")
	}

	// Recreate under the same name: a fresh node must re-seed from status.
	if err := cache.AddClusterQueue(ctx, singleFlavorCQ()); err != nil {
		t.Fatalf("re-AddClusterQueue: %v", err)
	}
	cache.EnsureCQHistoricalBorrowingSeeded("cq", status)
	got := cache.hm.ClusterQueue("cq").getResourceNode().HistoricalBorrowing[corev1.ResourceCPU]
	if amountI64(got) != 5000 {
		t.Fatalf("re-seeded historical borrowing = %d, want 5000", amountI64(got))
	}
}

// TestHistoricalBorrowingRestoreRoundTrip is a regression test for the original
// amplification bug: restoring persisted status and persisting it again must be
// an identity round-trip, even when the resource is provided by multiple
// flavors. Storing the EMA per resource name (not per flavor) guarantees this.
func TestHistoricalBorrowingRestoreRoundTrip(t *testing.T) {
	cache := newTwoFlavorCache(t)
	cache.EnsureCQHistoricalBorrowingSeeded("cq", cpuHistoryStatus(5000))

	persisted := cache.historicalBorrowingStatusFromNode(cache.hm.ClusterQueue("cq"))
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

// TestHistoricalBorrowingInactiveNoop verifies the public seeding/sampling
// methods are no-ops while the strategy is inactive (feature gate disabled),
// returning nil and leaving nodes unseeded with no EMA map allocated.
func TestHistoricalBorrowingInactiveNoop(t *testing.T) {
	// Feature gate explicitly disabled, so the strategy is inactive even though
	// the historical borrowing config is supplied.
	featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.HistoricalBorrowingFairSharing, false)

	cache := New(utiltesting.NewFakeClient(), WithFairSharing(true), WithHistoricalBorrowing(historicalBorrowingCfg()))
	if err := cache.AddOrUpdateCohort(singleFlavorCohort()); err != nil {
		t.Fatalf("AddOrUpdateCohort: %v", err)
	}
	if err := cache.AddClusterQueue(context.Background(), singleFlavorCQ()); err != nil {
		t.Fatalf("AddClusterQueue: %v", err)
	}

	if cache.HistoricalBorrowingActive() {
		t.Fatal("expected strategy inactive with feature gate disabled")
	}

	cache.EnsureCQHistoricalBorrowingSeeded("cq", cpuHistoryStatus(5000))
	cache.EnsureCohortHistoricalBorrowingSeeded("cohort", cpuHistoryStatus(5000))
	if status, found := cache.SampleCQHistoricalBorrowing("cq", time.Minute); status != nil || found {
		t.Fatalf("SampleCQHistoricalBorrowing on inactive cache = (%v, %v), want (nil, false)", status, found)
	}
	if status, found := cache.SampleCohortHistoricalBorrowing("cohort", time.Minute); status != nil || found {
		t.Fatalf("SampleCohortHistoricalBorrowing on inactive cache = (%v, %v), want (nil, false)", status, found)
	}
	if cache.hb.seeded.Len() != 0 {
		t.Fatalf("expected no nodes seeded while inactive, got %d", cache.hb.seeded.Len())
	}
	if got := cache.hm.ClusterQueue("cq").getResourceNode().HistoricalBorrowing; got != nil {
		t.Fatalf("expected nil historical borrowing map while inactive, got %v", got)
	}
}

// TestHistoricalBorrowingMissingObjectNoop verifies seeding/sampling a name that
// is not in the cache is a safe no-op (the cq/cohort == nil guard).
func TestHistoricalBorrowingMissingObjectNoop(t *testing.T) {
	cache := newHistoricalBorrowingCache(t)

	cache.EnsureCQHistoricalBorrowingSeeded("missing", cpuHistoryStatus(5000))
	cache.EnsureCohortHistoricalBorrowingSeeded("missing", cpuHistoryStatus(5000))
	if status, found := cache.SampleCQHistoricalBorrowing("missing", time.Minute); status != nil || found {
		t.Fatalf("SampleCQHistoricalBorrowing on missing CQ = (%v, %v), want (nil, false)", status, found)
	}
	if status, found := cache.SampleCohortHistoricalBorrowing("missing", time.Minute); status != nil || found {
		t.Fatalf("SampleCohortHistoricalBorrowing on missing cohort = (%v, %v), want (nil, false)", status, found)
	}
}

// TestHistoricalBorrowingSeedOnceIdempotent verifies a second seed call for an
// already-seeded node is a no-op (the seeded-set short-circuit), so a later
// status cannot overwrite the live EMA.
func TestHistoricalBorrowingSeedOnceIdempotent(t *testing.T) {
	cache := newHistoricalBorrowingCache(t)

	cache.EnsureCQHistoricalBorrowingSeeded("cq", cpuHistoryStatus(5000))
	// Second call with a different status must leave the already-seeded EMA intact.
	cache.EnsureCQHistoricalBorrowingSeeded("cq", cpuHistoryStatus(9000))

	got := cache.hm.ClusterQueue("cq").getResourceNode().HistoricalBorrowing[corev1.ResourceCPU]
	if amountI64(got) != 5000 {
		t.Fatalf("second seed overwrote EMA: got %d, want 5000 (seed once)", amountI64(got))
	}
}
