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
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	configapi "sigs.k8s.io/kueue/apis/config/v1beta2"
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

	sampleCQAfter(cache, "cq", halfLife)

	if got := cpuHistory(cqImpl.getResourceNode()); got != 4000 {
		t.Fatalf("EMA after half-life sample = %v, want 4000 (halved)", got)
	}
}

// TestHistoricalBorrowingSeedZeroWhenNoStatus verifies a fresh object (no
// persisted status) seeds its EMA to zero, not from current borrowing, so a new
// or returning team keeps its catch-up credit under the additive fold.
func TestHistoricalBorrowingSeedZeroWhenNoStatus(t *testing.T) {
	cache := newHistoricalBorrowingCache(t)
	cqImpl := cache.hm.ClusterQueue("cq")
	addUsage(cqImpl, cpuFR, cpuCores(15))

	loadCQ(cache, "cq", nil)

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
			if got, want := borrowingForDRS(cqImpl), float64(amountI64(cpuCores(tc.wantBorrowing))); got != want {
				t.Fatalf("got %v, want %v", got, want)
			}
		})
	}
}

// borrowingForDRS returns the effective cpu borrowing (current + k*EMA) in
// milliCPU, as folded into the DRS ratio.
func borrowingForDRS(node dominantResourceShareNode) float64 {
	borrowing := currentBorrowingByResource(node.getResourceNode())
	terms := historicalBorrowingTerms(node, borrowing)
	return amountFloat64(borrowing[corev1.ResourceCPU]) + terms[corev1.ResourceCPU]
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

	current := currentBorrowingByResource(cqImpl.getResourceNode())
	if len(current) != 1 || amountI64(current[corev1.ResourceCPU]) != amountI64(cpuCores(8)) {
		t.Fatalf("expected single aggregated cpu entry of 8 cores, got %v", current)
	}

	sampleCQAfter(cache, "cq", halfLife)
	hb := cqImpl.getResourceNode().HistoricalBorrowing
	if len(hb) != 1 || hb[corev1.ResourceCPU] != 4000 {
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
	var prev float64
	for range 100 {
		sampleCQAfter(cache, "cq", tick)
		got := cpuHistory(cqImpl.getResourceNode())
		if got < prev {
			t.Fatalf("EMA decreased under sustained borrowing: %v -> %v", prev, got)
		}
		prev = got
	}
	const target = 5000.0
	if prev <= target*0.99 || prev > target+1e-6 {
		t.Fatalf("EMA did not converge to ~%v milliCPU: got %v", target, prev)
	}
}

// TestSampleCohortHistoricalBorrowing verifies the cohort node EMA is populated
// from cohort-level borrowing.
func TestSampleCohortHistoricalBorrowing(t *testing.T) {
	cache := newHistoricalBorrowingCache(t)
	cohortImpl := cache.hm.Cohort("cohort")
	quota := cohortImpl.getResourceNode().SubtreeQuota[cpuFR]
	addUsage(cohortImpl, cpuFR, quota.Add(cpuCores(5))) // cohort borrows 5 cores

	status, found := sampleCohortAfter(cache, "cohort", halfLife)
	if !found {
		t.Fatal("expected cohort to be found in cache")
	}

	// 0.5 * 5 cores = 2.5 cores = 2500 milli.
	if got := cpuHistory(cohortImpl.getResourceNode()); got != 2500 {
		t.Fatalf("cohort EMA after half-life sample = %v, want 2500", got)
	}
	if status == nil || status.BorrowedResources.Cpu().MilliValue() != 2500 {
		t.Fatalf("expected persisted cohort status of 2500m, got %v", status)
	}
}

// dayHalfLifeCfg samples every minute with a one-day half-life, so a single
// tick's alpha (~4.8e-4) times a whole-unit borrowing is far below one unit.
func dayHalfLifeCfg() *configapi.HistoricalBorrowingConfig {
	cfg := historicalBorrowingCfg()
	cfg.BorrowingHalfLifeTime = metav1.Duration{Duration: 24 * time.Hour}
	return cfg
}

// TestHistoricalBorrowingKeepsSubUnitContributions verifies a one-GPU borrowing
// contributes alpha*1 GPU to the EMA, and that this sub-unit history still
// raises the CQ's share once it stops. An integer EMA truncated it to zero.
func TestHistoricalBorrowingKeepsSubUnitContributions(t *testing.T) {
	cache := newGPUCache(t, dayHalfLifeCfg())
	cqImpl := cache.hm.ClusterQueue("cq")
	addUsage(cqImpl, hbGPUFR, resources.NewAmount(1))

	sampleCQAfter(cache, "cq", time.Minute)
	want := borrowingAlphaRate(time.Minute.Seconds(), (24 * time.Hour).Seconds())
	if got := cqImpl.getResourceNode().HistoricalBorrowing[hbGPU]; math.Abs(got-want) > 1e-12 {
		t.Fatalf("GPU EMA after one tick = %v, want %v", got, want)
	}

	removeUsage(cqImpl, hbGPUFR, resources.NewAmount(1))
	if drs := dominantResourceShare(cqImpl, nil); drs.IsZero() {
		t.Fatalf("expected sub-unit GPU history to raise the share, got %+v", drs)
	}
}

// TestHistoricalBorrowingDecaysAtHalfLife verifies a small whole-unit history
// halves after one half-life of per-minute ticks. Truncating each tick would
// lose a whole GPU per minute and clear it within 100 ticks.
func TestHistoricalBorrowingDecaysAtHalfLife(t *testing.T) {
	cache := newGPUCache(t, dayHalfLifeCfg())
	cqImpl := cache.hm.ClusterQueue("cq")
	ensureHistoricalBorrowingMap(&cqImpl.resourceNode)
	cqImpl.resourceNode.HistoricalBorrowing[hbGPU] = 100

	for range 24 * 60 {
		sampleCQAfter(cache, "cq", time.Minute)
	}
	if got := cqImpl.getResourceNode().HistoricalBorrowing[hbGPU]; math.Abs(got-50) > 1e-6 {
		t.Fatalf("GPU EMA after one half-life = %v, want 50", got)
	}
}

// TestHistoricalBorrowingSubUnitRoundTrip verifies sub-milli cpu and sub-unit
// GPU history survive persisting to status and restoring from it.
func TestHistoricalBorrowingSubUnitRoundTrip(t *testing.T) {
	src := newGPUCache(t, dayHalfLifeCfg()).hm.ClusterQueue("cq")
	ensureHistoricalBorrowingMap(&src.resourceNode)
	src.resourceNode.HistoricalBorrowing[corev1.ResourceCPU] = 0.5
	src.resourceNode.HistoricalBorrowing[hbGPU] = 0.25

	status := historicalBorrowingStatus(src, time.Now())
	dst := newGPUCache(t, dayHalfLifeCfg())
	status.LastUpdate = metav1.NewTime(time.Now().Add(time.Second))
	loadCQ(dst, "cq", status)

	got := dst.hm.ClusterQueue("cq").getResourceNode().HistoricalBorrowing
	if len(got) != 2 || math.Abs(got[corev1.ResourceCPU]-0.5) > 1e-9 || math.Abs(got[hbGPU]-0.25) > 1e-9 {
		t.Fatalf("restored history = %v, want cpu 0.5 milli and GPU 0.25 (status %v)", got, status.BorrowedResources)
	}
}

// TestHistoricalBorrowingSamplingClock verifies sampling is timed from the
// in-memory clock: repeated calls within an interval (e.g. a burst of object
// events) sample once, a restored LastUpdate drives the first elapsed, and a
// LastUpdate in the future waits instead of sampling.
func TestHistoricalBorrowingSamplingClock(t *testing.T) {
	t0 := time.Now()
	interval := historicalBorrowingCfg().BorrowingSamplingInterval.Duration
	key := historicalSeededKey("cq", "cq")
	sample := func(c *Cache, at time.Time) time.Duration {
		_, wait, _ := c.SyncCQHistoricalBorrowing("cq", nil, at, true)
		return wait
	}

	t.Run("burst within an interval samples once", func(t *testing.T) {
		cache := newHistoricalBorrowingCache(t)
		cqImpl := cache.hm.ClusterQueue("cq")
		addUsage(cqImpl, cpuFR, cpuCores(15))
		cache.hb.lastSample[key] = t0

		if wait := sample(cache, t0.Add(interval/2)); wait != interval/2 {
			t.Fatalf("wait before the first interval = %v, want %v", wait, interval/2)
		}
		if got := cpuHistory(cqImpl.getResourceNode()); got != 0 {
			t.Fatalf("EMA sampled before the interval elapsed: %v", got)
		}
		at := t0.Add(interval)
		if wait := sample(cache, at); wait != 0 {
			t.Fatalf("expected a sample once the interval elapsed, got wait %v", wait)
		}
		want := cpuHistory(cqImpl.getResourceNode())
		for range 5 {
			if wait := sample(cache, at); wait != interval {
				t.Fatalf("repeated sample at the same time: wait %v, want %v", wait, interval)
			}
		}
		if got := cpuHistory(cqImpl.getResourceNode()); got != want {
			t.Fatalf("repeated calls re-sampled the EMA: %v, want %v", got, want)
		}
	})

	t.Run("restored LastUpdate drives the first elapsed", func(t *testing.T) {
		cache := newHistoricalBorrowingCache(t)
		status := cpuHistoryStatus(8000)
		status.LastUpdate = metav1.NewTime(t0.Add(-halfLife))
		delete(cache.hb.lastSample, key)
		cache.SyncCQHistoricalBorrowing("cq", status, t0, false)

		if wait := sample(cache, t0); wait != 0 {
			t.Fatalf("expected an immediate sample after a restart, got wait %v", wait)
		}
		if got := cpuHistory(cache.hm.ClusterQueue("cq").getResourceNode()); got != 4000 {
			t.Fatalf("EMA after a half-life of downtime = %v, want 4000", got)
		}
	})

	t.Run("future LastUpdate waits", func(t *testing.T) {
		cache := newHistoricalBorrowingCache(t)
		status := cpuHistoryStatus(8000)
		status.LastUpdate = metav1.NewTime(t0.Add(time.Hour))
		loadCQ(cache, "cq", status)

		if wait := sample(cache, t0.Add(2*interval)); wait != interval {
			t.Fatalf("wait with a future LastUpdate = %v, want %v", wait, interval)
		}
		if got := cpuHistory(cache.hm.ClusterQueue("cq").getResourceNode()); got != 8000 {
			t.Fatalf("EMA moved with a future LastUpdate: %v, want 8000", got)
		}
	})
}
