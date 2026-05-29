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

	utilfeature "k8s.io/apiserver/pkg/util/feature"
	featuregatetesting "k8s.io/component-base/featuregate/testing"

	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta2"
	"sigs.k8s.io/kueue/pkg/features"
	"sigs.k8s.io/kueue/pkg/resources"
	utiltesting "sigs.k8s.io/kueue/pkg/util/testing"
	utiltestingapi "sigs.k8s.io/kueue/pkg/util/testing/v1beta2"
)

// TestHistoricalBorrowingPenalisesRememberedBorrower verifies that a node with
// remembered borrowing ranks worse than a lower-history peer: higher DRS
// (preempted first) and higher weighted share (admitted last). It covers a CQ
// against an idle sibling, two CQs with equal current borrowing differing only
// in history (the additive-vs-max distinguisher), and the same at cohort
// granularity.
func TestHistoricalBorrowingPenalisesRememberedBorrower(t *testing.T) {
	cases := map[string]struct {
		setup        func(t *testing.T) (worse, better DRS)
		betterIsZero bool
	}{
		"clusterqueue vs idle sibling": {
			setup: func(t *testing.T) (DRS, DRS) {
				cache := newTwoCQCohortCache(t)
				cqA := rememberCPUBorrowing(cache, "cqA")
				cqB := cache.hm.ClusterQueue("cqB")
				return dominantResourceShare(cqA, nil), dominantResourceShare(cqB, nil)
			},
			betterIsZero: true,
		},
		// Two equal-weight CQs that currently borrow the same amount but differ
		// in history must be ordered by history. Under the old max() fold both
		// would tie (max(X,X)=X); additive (X + k*EMA) prefers the lower-EMA CQ.
		"equal current borrowing differ by history": {
			setup: func(t *testing.T) (DRS, DRS) {
				cache := newCatchupCache(t, historicalBorrowingCfg(), 1, 1)
				cqA := cache.hm.ClusterQueue("cqA")
				cqB := cache.hm.ClusterQueue("cqB")
				addUsage(cqA, cpuFR, cpuCores(20))
				addUsage(cqB, cpuFR, cpuCores(20))
				seedCPUHistoryK(&cqA.resourceNode, cpuCores(20), 1)
				seedCPUHistoryK(&cqB.resourceNode, resources.NewAmount(0), 1)
				return dominantResourceShare(cqA, nil), dominantResourceShare(cqB, nil)
			},
		},
		"cohort vs idle sibling": {
			setup:        cohortOrderingDRS,
			betterIsZero: true,
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			worse, better := tc.setup(t)
			if tc.betterIsZero && !better.IsZero() {
				t.Fatalf("idle node should have zero DRS, got %v", better)
			}
			if worse.IsZero() {
				t.Fatal("remembered borrower should have non-zero DRS")
			}
			assertRanksWorse(t, worse, better)
		})
	}
}

// cohortOrderingDRS builds a child cohort ("ca") with remembered borrowing and
// an idle sibling ("cb") under a "root" cohort, returning their dominant
// resource shares.
func cohortOrderingDRS(t *testing.T) (DRS, DRS) {
	t.Helper()
	featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.HistoricalBorrowingFairSharing, true)

	root := utiltestingapi.MakeCohort("root").
		ResourceGroup(
			*utiltestingapi.MakeFlavorQuotas("default").
				ResourceQuotaWrapper("cpu").NominalQuota("100").Append().
				Obj(),
		).
		Obj()
	cache := New(utiltesting.NewFakeClient(), WithFairSharing(true), WithHistoricalBorrowing(historicalBorrowingCfg()))
	if err := cache.AddOrUpdateCohort(root); err != nil {
		t.Fatalf("AddOrUpdateCohort root: %v", err)
	}
	for _, name := range []kueue.CohortReference{"ca", "cb"} {
		child := utiltestingapi.MakeCohort(name).
			Parent("root").
			ResourceGroup(
				*utiltestingapi.MakeFlavorQuotas("default").
					ResourceQuotaWrapper("cpu").NominalQuota("10").Append().
					Obj(),
			).
			Obj()
		if err := cache.AddOrUpdateCohort(child); err != nil {
			t.Fatalf("AddOrUpdateCohort %s: %v", name, err)
		}
	}

	ca := cache.hm.Cohort("ca")
	seedCPUHistory(&ca.resourceNode, cpuCores(5))
	return dominantResourceShare(ca, nil), dominantResourceShare(cache.hm.Cohort("cb"), nil)
}

// TestHistoricalBorrowingDecayFlipsOrdering verifies that as a heavy past
// borrower goes idle while a light borrower keeps borrowing, decay eventually
// flips which CQ ranks worse.
func TestHistoricalBorrowingDecayFlipsOrdering(t *testing.T) {
	cache := newTwoCQCohortCache(t)
	cqA := cache.hm.ClusterQueue("cqA")
	cqB := cache.hm.ClusterQueue("cqB")

	addUsage(cqA, cpuFR, cpuCores(10)) // borrow 5
	addUsage(cqB, cpuFR, cpuCores(6))  // borrow 1
	cache.SampleCQHistoricalBorrowing("cqA", halfLife)
	cache.SampleCQHistoricalBorrowing("cqB", halfLife)

	if CompareDRS(dominantResourceShare(cqA, nil), dominantResourceShare(cqB, nil)) <= 0 {
		t.Fatal("expected heavy borrower cqA to initially rank higher than cqB")
	}

	// cqA goes idle, cqB keeps borrowing; A's history decays below B.
	removeUsage(cqA, cpuFR, cpuCores(10))
	for range 3 {
		cache.SampleCQHistoricalBorrowing("cqA", halfLife)
		cache.SampleCQHistoricalBorrowing("cqB", halfLife)
	}

	if CompareDRS(dominantResourceShare(cqA, nil), dominantResourceShare(cqB, nil)) >= 0 {
		t.Fatal("expected ordering to flip: idle cqA should rank below still-borrowing cqB after decay")
	}
}

// TestHistoricalBorrowingSurvivesSnapshot verifies resourceNode.Clone copies the
// EMA so the scheduling/preemption path (which runs on snapshots) sees it.
func TestHistoricalBorrowingSurvivesSnapshot(t *testing.T) {
	cache := newTwoCQCohortCache(t)
	rememberCPUBorrowing(cache, "cqA")

	ctx := context.Background()
	snapshot, err := cache.Snapshot(ctx)
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	if drs := snapshot.ClusterQueue("cqA").DominantResourceShare(); drs.IsZero() {
		t.Fatal("expected snapshot DRS to reflect remembered historical borrowing")
	}
}
