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

	"sigs.k8s.io/kueue/pkg/resources"
)

// TestSampleHistoricalBorrowingZeroHalfLifeNoOp guards against a zero half-life
// producing NaN/Inf: a sample with a zero half-life is a no-op. The alpha rate
// itself is covered by TestBorrowingAlphaRate.
func TestSampleHistoricalBorrowingZeroHalfLifeNoOp(t *testing.T) {
	cache := newHistoricalBorrowingCache(t)
	cqImpl := cache.hm.ClusterQueue("cq")
	addUsage(cqImpl, cpuFR, cpuCores(15))
	sampleHistoricalBorrowing(cqImpl, time.Minute, 0)
	if len(cqImpl.getResourceNode().HistoricalBorrowing) != 0 {
		t.Fatalf("expected no-op sample with zero half-life, got %v", cqImpl.getResourceNode().HistoricalBorrowing)
	}
}

// TestHistoricalBorrowingEMABlendDeterministic pins the blend math: sampling
// with elapsed == half-life gives alpha == 0.5, so an existing EMA with no
// current borrowing is exactly halved.
func TestHistoricalBorrowingEMABlendDeterministic(t *testing.T) {
	cache := newHistoricalBorrowingCache(t)
	cqImpl := cache.hm.ClusterQueue("cq")
	seedCPUHistory(&cqImpl.resourceNode, cpuCores(8))

	cache.SampleCQHistoricalBorrowing("cq", halfLife)

	got := cqImpl.getResourceNode().HistoricalBorrowing[corev1.ResourceCPU]
	if amountI64(got) != amountI64(cpuCores(4)) {
		t.Fatalf("EMA after half-life sample = %d, want %d (halved)", amountI64(got), amountI64(cpuCores(4)))
	}
}

// TestHistoricalBorrowingSeedZeroWhenNoStatus verifies a fresh object (no
// persisted status) seeds its EMA to zero, not from current borrowing, so a new
// or returning team keeps its catch-up credit under the additive fold.
func TestHistoricalBorrowingSeedZeroWhenNoStatus(t *testing.T) {
	cache := newHistoricalBorrowingCache(t)
	cqImpl := cache.hm.ClusterQueue("cq")
	addUsage(cqImpl, cpuFR, cpuCores(15))

	cache.EnsureCQHistoricalBorrowingSeeded("cq", nil)

	if got := cqImpl.getResourceNode().HistoricalBorrowing; len(got) != 0 {
		t.Fatalf("expected empty EMA when seeding without status, got %v", got)
	}
}

// TestHistoricalBorrowingAdditiveOfInstantAndHistory checks the DRS borrowing is
// the additive blend current + k*EMA per resource (k defaults to 1).
func TestHistoricalBorrowingAdditiveOfInstantAndHistory(t *testing.T) {
	cases := map[string]struct {
		history       int64
		usage         int64
		wantBorrowing int64
	}{
		"history adds to smaller instantaneous borrowing": {
			history:       5,
			usage:         12, // borrows 2, plus history 5
			wantBorrowing: 7,
		},
		"history adds to larger instantaneous borrowing": {
			history:       5,
			usage:         18, // borrows 8, plus history 5
			wantBorrowing: 13,
		},
		"history and instantaneous match": {
			history:       5,
			usage:         15, // borrows 5, plus history 5
			wantBorrowing: 10,
		},
		"no instantaneous borrowing": {
			history:       5,
			usage:         8, // borrows 0 (below quota 10), plus history 5
			wantBorrowing: 5,
		},
		"no history, instantaneous only": {
			history:       0,
			usage:         14, // borrows 4
			wantBorrowing: 4,
		},
		"no history, no instantaneous borrowing": {
			history:       0,
			usage:         10, // borrows 0 (exactly at quota 10)
			wantBorrowing: 0,
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			cache := newHistoricalBorrowingCache(t)
			cqImpl := cache.hm.ClusterQueue("cq")
			if tc.history > 0 {
				seedCPUHistory(&cqImpl.resourceNode, cpuCores(tc.history))
			}

			addUsage(cqImpl, cpuFR, cpuCores(tc.usage))
			if got := borrowingForDRSAmount(cqImpl); got != amountI64(cpuCores(tc.wantBorrowing)) {
				t.Fatalf("got %d, want %d", got, amountI64(cpuCores(tc.wantBorrowing)))
			}
		})
	}
}

func borrowingForDRSAmount(node dominantResourceShareNode) int64 {
	borrowing := currentBorrowingByResource(node.getResourceNode(), nil)
	applyHistoricalBorrowing(node, nil, borrowing, nil)
	return amountI64(borrowing[corev1.ResourceCPU])
}

// TestSampleAggregatesMultipleFlavors verifies borrowing on multiple flavors of
// the same resource aggregates into a single per-resource EMA entry.
func TestSampleAggregatesMultipleFlavors(t *testing.T) {
	cache := newTwoFlavorCache(t)
	cqImpl := cache.hm.ClusterQueue("cq")
	f1 := resources.FlavorResource{Flavor: "f1", Resource: corev1.ResourceCPU}
	f2 := resources.FlavorResource{Flavor: "f2", Resource: corev1.ResourceCPU}
	addUsage(cqImpl, f1, cpuCores(15)) // borrow 5 on f1
	addUsage(cqImpl, f2, cpuCores(13)) // borrow 3 on f2

	current := currentBorrowingByResource(cqImpl.getResourceNode(), nil)
	if len(current) != 1 || amountI64(current[corev1.ResourceCPU]) != amountI64(cpuCores(8)) {
		t.Fatalf("expected single aggregated cpu entry of 8 cores, got %v", current)
	}

	cache.SampleCQHistoricalBorrowing("cq", halfLife)
	hb := cqImpl.getResourceNode().HistoricalBorrowing
	if len(hb) != 1 || amountI64(hb[corev1.ResourceCPU]) != amountI64(cpuCores(4)) {
		t.Fatalf("expected single aggregated cpu EMA of 4 cores after half-life sample, got %v", hb)
	}
}

// TestHistoricalBorrowingConvergesUnderSustainedBorrowing verifies the EMA rises
// monotonically toward the sustained instantaneous borrowing (convergence).
func TestHistoricalBorrowingConvergesUnderSustainedBorrowing(t *testing.T) {
	cache := newHistoricalBorrowingCache(t)
	cqImpl := cache.hm.ClusterQueue("cq")
	addUsage(cqImpl, cpuFR, cpuCores(15)) // sustained borrow of 5 cores

	tick := halfLife / 4
	var prev int64
	for range 100 {
		cache.SampleCQHistoricalBorrowing("cq", tick)
		got := amountI64(cqImpl.getResourceNode().HistoricalBorrowing[corev1.ResourceCPU])
		if got < prev {
			t.Fatalf("EMA decreased under sustained borrowing: %d -> %d", prev, got)
		}
		prev = got
	}
	target := amountI64(cpuCores(5))
	if prev <= target*99/100 || prev > target {
		t.Fatalf("EMA did not converge to ~%d cores: got %d", target, prev)
	}
}

// TestSampleCohortHistoricalBorrowing verifies the cohort node EMA is populated
// from cohort-level borrowing.
func TestSampleCohortHistoricalBorrowing(t *testing.T) {
	cache := newHistoricalBorrowingCache(t)
	cohortImpl := cache.hm.Cohort("cohort")
	quota := cohortImpl.getResourceNode().SubtreeQuota[cpuFR]
	addUsage(cohortImpl, cpuFR, quota.Add(cpuCores(5))) // cohort borrows 5 cores

	status, found := cache.SampleCohortHistoricalBorrowing("cohort", halfLife)
	if !found {
		t.Fatal("expected cohort to be found in cache")
	}

	// 0.5 * 5 cores = 2.5 cores = 2500 milli.
	got := cohortImpl.getResourceNode().HistoricalBorrowing[corev1.ResourceCPU]
	if amountI64(got) != 2500 {
		t.Fatalf("cohort EMA after half-life sample = %d, want 2500", amountI64(got))
	}
	if status == nil || status.BorrowedResources.Cpu().MilliValue() != 2500 {
		t.Fatalf("expected persisted cohort status of 2500m, got %v", status)
	}
}
