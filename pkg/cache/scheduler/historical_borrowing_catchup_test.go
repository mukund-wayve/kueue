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
	"math"
	"testing"

	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta2"
	"sigs.k8s.io/kueue/pkg/resources"
)

// catchupPoolCores is the size of the contended over-quota pool the two CQs
// compete for. Both CQs have zero nominal quota, so every allocated core counts
// as borrowing.
const catchupPoolCores = 100

// bothCQs are the two competing ClusterQueues used by the catch-up simulations.
var bothCQs = []kueue.ClusterQueueReference{"cqA", "cqB"}

// allocateContendedPool resets both CQs' cpu usage, then greedily assigns the
// pool one core at a time to the demanding CQ whose resulting DRS is lowest
// (mirroring admission, which prefers the lowest dominant resource share). It
// returns the cores allocated to each CQ.
func allocateContendedPool(cache *Cache, demanders []kueue.ClusterQueueReference) map[kueue.ClusterQueueReference]int64 {
	for _, ref := range bothCQs {
		cq := cache.hm.ClusterQueue(ref)
		if used := cq.getResourceNode().Usage[cpuFR]; used.CmpInt64(0) > 0 {
			removeUsage(cq, cpuFR, used)
		}
	}
	candReq := resources.FlavorResourceQuantities{cpuFR: cpuCores(1)}
	alloc := make(map[kueue.ClusterQueueReference]int64, len(demanders))
	for range catchupPoolCores {
		var best kueue.ClusterQueueReference
		var bestDRS DRS
		for _, ref := range demanders {
			cand := dominantResourceShare(cache.hm.ClusterQueue(ref), candReq)
			if best == "" || CompareDRS(cand, bestDRS) < 0 {
				best, bestDRS = ref, cand
			}
		}
		addUsage(cache.hm.ClusterQueue(best), cpuFR, cpuCores(1))
		alloc[best]++
	}
	return alloc
}

// runRound allocates the pool among the demanders, then folds the resulting
// borrowing into both CQs' EMAs (an idle CQ decays). It returns the allocation.
func runRound(cache *Cache, demanders ...kueue.ClusterQueueReference) map[kueue.ClusterQueueReference]int64 {
	alloc := allocateContendedPool(cache, demanders)
	for _, ref := range bothCQs {
		sampleCQAfter(cache, ref, halfLife)
	}
	return alloc
}

func emaCores(cache *Cache, ref kueue.ClusterQueueReference) float64 {
	return cpuHistory(cache.hm.ClusterQueue(ref).getResourceNode()) / 1000.0
}

func weightProportional(w, wTotal float64) float64 {
	return w / wTotal * catchupPoolCores
}

// convergeAndExpectWeightProportional warms the system to steady state, then
// asserts the goal two ways: the realized time share (cores actually allocated
// per CQ, summed over a measurement window) is weight-proportional, and the
// decayed history (weighted EMAs) has equalized.
func convergeAndExpectWeightProportional(t *testing.T, cache *Cache, wA, wB float64) {
	t.Helper()
	for range 40 { // warm up to steady state.
		runRound(cache, bothCQs...)
	}

	const window = 10
	var shareA, shareB int64
	for range window {
		alloc := runRound(cache, bothCQs...)
		shareA += alloc["cqA"]
		shareB += alloc["cqB"]
	}
	total := float64(shareA + shareB)
	wantA, wantB := wA/(wA+wB)*total, wB/(wA+wB)*total
	if math.Abs(float64(shareA)-wantA) > window || math.Abs(float64(shareB)-wantB) > window {
		t.Fatalf("time share over %d rounds = {cqA:%d, cqB:%d}, want weight-proportional ~{%.0f, %.0f}",
			window, shareA, shareB, wantA, wantB)
	}

	if gap := math.Abs(emaCores(cache, "cqA")/wA - emaCores(cache, "cqB")/wB); gap > 1.0 {
		t.Fatalf("weighted EMAs did not equalize: gap %.2f (EMA_A/wA=%.2f EMA_B/wB=%.2f)",
			gap, emaCores(cache, "cqA")/wA, emaCores(cache, "cqB")/wB)
	}
}

// weightCases are the fair-weight splits exercised by the simulation scenarios.
var weightCases = map[string]struct{ wA, wB float64 }{
	"equal 50/50": {wA: 1, wB: 1},
	"split 30/70": {wA: 3, wB: 7},
}

// TestHistoricalBorrowingCatchUp: cqA borrows the whole pool alone for a while
// (builds history), then both contend. The additive fold must let cqB reclaim
// above its weight-proportional share in the first contended round (this is what
// fails under the old max() fold), then converge to weight-proportional shares.
func TestHistoricalBorrowingCatchUp(t *testing.T) {
	for name, tc := range weightCases {
		t.Run(name, func(t *testing.T) {
			cache := newCatchupCache(t, historicalBorrowingCfg(), tc.wA, tc.wB)

			for range 12 { // cqA borrows alone, building its EMA toward the pool.
				runRound(cache, "cqA")
			}

			wpB := weightProportional(tc.wB, tc.wA+tc.wB)
			if got := runRound(cache, bothCQs...)["cqB"]; float64(got) <= wpB {
				t.Fatalf("first contended round: cqB share %d should exceed weight-proportional %.1f (reclaim)", got, wpB)
			}

			convergeAndExpectWeightProportional(t, cache, tc.wA, tc.wB)
		})
	}
}

// TestHistoricalBorrowingNetHistoryOrdering: cqA borrows first, then goes idle
// while cqB borrows (so cqA's history decays below cqB's), then both contend.
// The lower-EMA cqA must reclaim first despite having borrowed first - net
// decayed history, not borrowing order, drives the ordering.
func TestHistoricalBorrowingNetHistoryOrdering(t *testing.T) {
	for name, tc := range weightCases {
		t.Run(name, func(t *testing.T) {
			cache := newCatchupCache(t, historicalBorrowingCfg(), tc.wA, tc.wB)

			for range 8 {
				runRound(cache, "cqA")
			}
			for range 8 {
				runRound(cache, "cqB")
			}
			if emaA, emaB := emaCores(cache, "cqA"), emaCores(cache, "cqB"); emaA >= emaB {
				t.Fatalf("expected cqA history to have decayed below cqB: EMA_A=%.2f EMA_B=%.2f", emaA, emaB)
			}

			wpA := weightProportional(tc.wA, tc.wA+tc.wB)
			if got := runRound(cache, bothCQs...)["cqA"]; float64(got) <= wpA {
				t.Fatalf("first contended round: lower-EMA cqA share %d should exceed weight-proportional %.1f", got, wpA)
			}

			convergeAndExpectWeightProportional(t, cache, tc.wA, tc.wB)
		})
	}
}

// TestHistoricalBorrowingKSensitivity verifies k tunes the reclaim: k=0
// reproduces the plain Borrowing score (cqB reaches only parity, no reclaim),
// and a larger k yields a larger reclaim for the same remembered history.
func TestHistoricalBorrowingKSensitivity(t *testing.T) {
	reclaim := func(k float64) int64 {
		cache := newCatchupCache(t, historicalBorrowingCfgK(k), 1, 1)
		// cqA remembers half the pool; cqB has no history. Single contended round.
		seedCPUHistoryK(&cache.hm.ClusterQueue("cqA").resourceNode, cpuCores(catchupPoolCores/2), k)
		return allocateContendedPool(cache, bothCQs)["cqB"]
	}

	k0, k1, k2 := reclaim(0), reclaim(1), reclaim(2)
	if parity := int64(catchupPoolCores / 2); k0 < parity-2 || k0 > parity+2 {
		t.Fatalf("k=0 should give parity (~%d) with no reclaim, got %d", parity, k0)
	}
	if k1 <= k0 {
		t.Fatalf("k=1 reclaim %d should exceed k=0 reclaim %d", k1, k0)
	}
	if k2 <= k1 {
		t.Fatalf("k=2 reclaim %d should exceed k=1 reclaim %d", k2, k1)
	}
}

// TestHistoricalBorrowingFreshBorrowerReclaims guards the seed-to-zero fix: a CQ
// that is already borrowing when first seeded (no persisted status) starts from
// an empty EMA, so against a remembered sibling it still reclaims. Had it been
// seeded from current borrowing, it would start penalized and not reclaim.
func TestHistoricalBorrowingFreshBorrowerReclaims(t *testing.T) {
	cache := newCatchupCache(t, historicalBorrowingCfg(), 1, 1)

	for range 12 { // cqA is a remembered heavy borrower.
		runRound(cache, "cqA")
	}

	// cqB is brand new: it is borrowing when first seeded with no status.
	cqB := cache.hm.ClusterQueue("cqB")
	addUsage(cqB, cpuFR, cpuCores(catchupPoolCores))
	loadCQ(cache, "cqB", nil)
	if got := cqB.getResourceNode().HistoricalBorrowing; len(got) != 0 {
		t.Fatalf("fresh cqB should seed an empty EMA, got %v", got)
	}

	if got := allocateContendedPool(cache, bothCQs)["cqB"]; float64(got) <= weightProportional(1, 2) {
		t.Fatalf("fresh cqB should reclaim above parity against remembered cqA, got %d", got)
	}
}
