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
	utiltestingapi "sigs.k8s.io/kueue/pkg/util/testing/v1beta2"
)

// Shared fixtures for the historical-borrowing unit tests. Keeping the cache
// builders, seed helpers, and assertions here gives every test file one source
// of truth and avoids the near-identical per-file copies.

const halfLife = time.Hour

// cpuFR is declared in oversized_limits_test.go within this package.

// amountI64 returns the int64 value of a. Test amounts are small, so the
// out-of-range case never occurs.
func amountI64(a resources.Amount) int64 {
	v, _ := a.Int64()
	return v
}

// cpuHistory returns the node's remembered cpu borrowing, in milliCPU.
func cpuHistory(rn resourceNode) float64 {
	return rn.HistoricalBorrowing[corev1.ResourceCPU]
}

// sampleCQAfter samples the CQ as if elapsed had passed since its previous
// sample, by backdating the in-memory sampling clock.
func sampleCQAfter(c *Cache, name kueue.ClusterQueueReference, elapsed time.Duration) (*kueue.HistoricalBorrowingStatus, bool) {
	now := time.Now()
	c.initHistoricalBorrowingLastSampleLocked()
	c.hb.lastSample[historicalSeededKey("cq", string(name))] = now.Add(-elapsed)
	status, _, found := c.SyncCQHistoricalBorrowing(name, nil, now, true)
	return status, found
}

// sampleCohortAfter is sampleCQAfter for a Cohort.
func sampleCohortAfter(c *Cache, name kueue.CohortReference, elapsed time.Duration) (*kueue.HistoricalBorrowingStatus, bool) {
	now := time.Now()
	c.initHistoricalBorrowingLastSampleLocked()
	c.hb.lastSample[historicalSeededKey("cohort", string(name))] = now.Add(-elapsed)
	status, _, found := c.SyncCohortHistoricalBorrowing(name, nil, now, true)
	return status, found
}

// loadCQ loads a persisted status into the CQ as a non-leader replica does.
func loadCQ(c *Cache, name kueue.ClusterQueueReference, status *kueue.HistoricalBorrowingStatus) {
	c.SyncCQHistoricalBorrowing(name, status, time.Now(), false)
}

// loadCohort is loadCQ for a Cohort.
func loadCohort(c *Cache, name kueue.CohortReference, status *kueue.HistoricalBorrowingStatus) {
	c.SyncCohortHistoricalBorrowing(name, status, time.Now(), false)
}

func historicalBorrowingCfg() *configapi.HistoricalBorrowingConfig {
	return &configapi.HistoricalBorrowingConfig{
		BorrowingHalfLifeTime:     metav1.Duration{Duration: halfLife},
		BorrowingSamplingInterval: metav1.Duration{Duration: time.Minute},
	}
}

// historicalBorrowingCfgK returns the default config with an explicit weight k
// (BorrowingHistoryWeight).
func historicalBorrowingCfgK(k float64) *configapi.HistoricalBorrowingConfig {
	cfg := historicalBorrowingCfg()
	cfg.BorrowingHistoryWeight = &k
	return cfg
}

// cpuCores returns the internal Amount for n CPU cores (stored as milli).
func cpuCores(n int64) resources.Amount {
	return resources.NewAmount(n * 1000)
}

// cpuHistoryStatus builds a persisted status carrying milli cpu of remembered
// borrowing, used to drive the load/restore paths. Its LastUpdate is a second
// ahead so it is newer than the load the cache does when adding the node.
func cpuHistoryStatus(milli int64) *kueue.HistoricalBorrowingStatus {
	return &kueue.HistoricalBorrowingStatus{
		BorrowedResources: corev1.ResourceList{
			corev1.ResourceCPU: resources.NewResourceFormatter().ResourceQuantity(corev1.ResourceCPU, milli),
		},
		LastUpdate: metav1.NewTime(time.Now().Add(time.Second)),
	}
}

// singleFlavorCQ builds the CQ ("cq", nominal cpu 10, flavor "default") used by
// the single-flavor tests. It is a function (not a value) so callers that
// delete and recreate the CQ get a fresh object each time.
func singleFlavorCQ() *kueue.ClusterQueue {
	return utiltestingapi.MakeClusterQueue("cq").
		ResourceGroup(
			*utiltestingapi.MakeFlavorQuotas("default").
				ResourceQuotaWrapper("cpu").NominalQuota("10").Append().
				Obj(),
		).
		Cohort("cohort").
		Obj()
}

// singleFlavorCohort builds the Cohort ("cohort", nominal cpu 20) parent.
func singleFlavorCohort() *kueue.Cohort {
	return utiltestingapi.MakeCohort("cohort").
		ResourceGroup(
			*utiltestingapi.MakeFlavorQuotas("default").
				ResourceQuotaWrapper("cpu").NominalQuota("20").Append().
				Obj(),
		).
		Obj()
}

// cqSpec describes a ClusterQueue for newSingleFlavorCache: its name, nominal
// cpu quota, and (optional) fair weight. A zero weight leaves the default.
type cqSpec struct {
	name    string
	nominal string
	weight  float64
}

// newSingleFlavorCache builds an active historical-borrowing cache with one
// cohort ("cohort") providing cohortNominal cpu on the "default" flavor and the
// given sibling ClusterQueues under it. It backs every single-flavor fixture so
// the builders below differ only in their cohort/CQ parameters.
func newSingleFlavorCache(t *testing.T, cfg *configapi.HistoricalBorrowingConfig, cohortNominal string, cqs ...cqSpec) *Cache {
	t.Helper()
	featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.HistoricalBorrowingFairSharing, true)

	ctx, log := utiltesting.ContextWithLog(t)
	cohort := utiltestingapi.MakeCohort("cohort").
		ResourceGroup(
			*utiltestingapi.MakeFlavorQuotas("default").
				ResourceQuotaWrapper("cpu").NominalQuota(cohortNominal).Append().
				Obj(),
		).
		Obj()
	cache := New(utiltesting.NewFakeClient(), WithFairSharing(true), WithHistoricalBorrowing(cfg))
	cache.AddOrUpdateResourceFlavor(log, utiltestingapi.MakeResourceFlavor("default").Obj())
	if err := cache.AddOrUpdateCohort(cohort); err != nil {
		t.Fatalf("AddOrUpdateCohort: %v", err)
	}
	for _, spec := range cqs {
		cq := utiltestingapi.MakeClusterQueue(spec.name).
			ResourceGroup(
				*utiltestingapi.MakeFlavorQuotas("default").
					ResourceQuotaWrapper("cpu").NominalQuota(spec.nominal).Append().
					Obj(),
			).
			Cohort("cohort").
			Obj()
		if err := cache.AddClusterQueue(ctx, cq); err != nil {
			t.Fatalf("AddClusterQueue %s: %v", spec.name, err)
		}
		if spec.weight != 0 {
			cache.hm.ClusterQueue(kueue.ClusterQueueReference(spec.name)).FairWeight = spec.weight
		}
	}
	return cache
}

// newHistoricalBorrowingCache returns a cache with one CQ ("cq", nominal cpu 10)
// nested in one Cohort ("cohort", nominal cpu 20).
func newHistoricalBorrowingCache(t *testing.T) *Cache {
	return newSingleFlavorCache(t, historicalBorrowingCfg(), "20", cqSpec{name: "cq", nominal: "10"})
}

// newTwoCQCohortCache returns a cache with two sibling CQs ("cqA", "cqB"), each
// with cpu nominal 5, under one cohort ("cohort", cpu nominal 20).
func newTwoCQCohortCache(t *testing.T) *Cache {
	return newSingleFlavorCache(t, historicalBorrowingCfg(), "20",
		cqSpec{name: "cqA", nominal: "5"}, cqSpec{name: "cqB", nominal: "5"})
}

// newCatchupCache builds a cache with two sibling CQs ("cqA", "cqB") of zero
// nominal cpu quota under a cohort that owns the whole cpu pool, with the given
// fair weights and historical borrowing config.
func newCatchupCache(t *testing.T, cfg *configapi.HistoricalBorrowingConfig, wA, wB float64) *Cache {
	return newSingleFlavorCache(t, cfg, "100",
		cqSpec{name: "cqA", nominal: "0", weight: wA}, cqSpec{name: "cqB", nominal: "0", weight: wB})
}

const hbGPU = corev1.ResourceName("example.com/gpu")

var hbGPUFR = resources.FlavorResource{Flavor: "default", Resource: hbGPU}

// newGPUCache returns a cache with one CQ ("cq", nominal 0 GPUs, so all its
// usage is borrowed) in one Cohort ("cohort", nominal 8 GPUs). GPUs are
// accounted in whole units, unlike milliCPU.
func newGPUCache(t *testing.T, cfg *configapi.HistoricalBorrowingConfig) *Cache {
	t.Helper()
	featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.HistoricalBorrowingFairSharing, true)

	ctx, log := utiltesting.ContextWithLog(t)
	gpuQuotas := func(nominal string) kueue.FlavorQuotas {
		return *utiltestingapi.MakeFlavorQuotas("default").
			ResourceQuotaWrapper(hbGPU).NominalQuota(nominal).Append().
			Obj()
	}
	cache := New(utiltesting.NewFakeClient(), WithFairSharing(true), WithHistoricalBorrowing(cfg))
	cache.AddOrUpdateResourceFlavor(log, utiltestingapi.MakeResourceFlavor("default").Obj())
	if err := cache.AddOrUpdateCohort(utiltestingapi.MakeCohort("cohort").ResourceGroup(gpuQuotas("8")).Obj()); err != nil {
		t.Fatalf("AddOrUpdateCohort: %v", err)
	}
	cq := utiltestingapi.MakeClusterQueue("cq").ResourceGroup(gpuQuotas("0")).Cohort("cohort").Obj()
	if err := cache.AddClusterQueue(ctx, cq); err != nil {
		t.Fatalf("AddClusterQueue: %v", err)
	}
	return cache
}

// newTwoFlavorCache returns a cache with a single CQ providing cpu on two
// flavors (f1, f2), used to exercise per-resource aggregation.
func newTwoFlavorCache(t *testing.T) *Cache {
	t.Helper()
	featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.HistoricalBorrowingFairSharing, true)

	ctx, log := utiltesting.ContextWithLog(t)
	cq := utiltestingapi.MakeClusterQueue("cq").
		ResourceGroup(
			*utiltestingapi.MakeFlavorQuotas("f1").
				ResourceQuotaWrapper("cpu").NominalQuota("10").Append().
				Obj(),
			*utiltestingapi.MakeFlavorQuotas("f2").
				ResourceQuotaWrapper("cpu").NominalQuota("10").Append().
				Obj(),
		).
		Cohort("cohort").
		Obj()
	cohort := utiltestingapi.MakeCohort("cohort").
		ResourceGroup(
			*utiltestingapi.MakeFlavorQuotas("f1").
				ResourceQuotaWrapper("cpu").NominalQuota("20").Append().
				Obj(),
			*utiltestingapi.MakeFlavorQuotas("f2").
				ResourceQuotaWrapper("cpu").NominalQuota("20").Append().
				Obj(),
		).
		Obj()

	cache := New(utiltesting.NewFakeClient(), WithFairSharing(true), WithHistoricalBorrowing(historicalBorrowingCfg()))
	cache.AddOrUpdateResourceFlavor(log, utiltestingapi.MakeResourceFlavor("f1").Obj())
	cache.AddOrUpdateResourceFlavor(log, utiltestingapi.MakeResourceFlavor("f2").Obj())
	if err := cache.AddOrUpdateCohort(cohort); err != nil {
		t.Fatalf("AddOrUpdateCohort: %v", err)
	}
	if err := cache.AddClusterQueue(ctx, cq); err != nil {
		t.Fatalf("AddClusterQueue: %v", err)
	}
	return cache
}

// seedCPUHistoryK directly sets a node's remembered cpu borrowing and weight k,
// bypassing the sampling path, so a single contended allocation can be measured
// for a known history at a chosen k.
func seedCPUHistoryK(rn *resourceNode, amount resources.Amount, k float64) {
	ensureHistoricalBorrowingMap(rn)
	rn.HistoricalBorrowing[corev1.ResourceCPU] = amountFloat64(amount)
	rn.BorrowingHistoryWeight = k
}

// seedCPUHistory directly sets a node's remembered cpu borrowing at the default
// weight k=1, mirroring the cache funnel so DRS computed directly on the live
// node folds in history.
func seedCPUHistory(rn *resourceNode, amount resources.Amount) {
	seedCPUHistoryK(rn, amount, defaultBorrowingHistoryWeight)
}

// rememberCPUBorrowing makes the named CQ borrow 5 cpu cores, folds it into the
// historical EMA, then drops the usage so only remembered borrowing remains.
func rememberCPUBorrowing(cache *Cache, name kueue.ClusterQueueReference) *clusterQueue {
	cq := cache.hm.ClusterQueue(name)
	addUsage(cq, cpuFR, cpuCores(10))
	sampleCQAfter(cache, name, halfLife)
	removeUsage(cq, cpuFR, cpuCores(10))
	return cq
}

// assertRanksWorse asserts that worse is ranked behind better: a higher
// dominant resource share (preempted first) and a larger weighted share
// (admitted last).
func assertRanksWorse(t *testing.T, worse, better DRS) {
	t.Helper()
	if CompareDRS(worse, better) <= 0 {
		t.Fatalf("expected worse node to rank higher for preemption: CompareDRS=%d", CompareDRS(worse, better))
	}
	if better.PreciseWeightedShare() >= worse.PreciseWeightedShare() {
		t.Fatalf("expected better node to have the smaller share: better=%v worse=%v",
			better.PreciseWeightedShare(), worse.PreciseWeightedShare())
	}
}
