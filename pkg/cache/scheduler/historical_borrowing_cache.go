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
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	config "sigs.k8s.io/kueue/apis/config/v1beta2"
	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta2"
	"sigs.k8s.io/kueue/pkg/features"
)

// defaultBorrowingHistoryWeight is the value of k (BorrowingHistoryWeight) used
// when the config leaves it unset: history contributes with weight 1.
const defaultBorrowingHistoryWeight = 1.0

// historicalBorrowingState holds all cache-level data for the HistoricalBorrowing
// DRS strategy. Keeping it in one struct, referenced by a single Cache field,
// limits the feature's footprint in the shared cache.go to a single line.
type historicalBorrowingState struct {
	// cfg is set only when fairSharing.strategy is HistoricalBorrowing, so a
	// non-nil value means the strategy is selected.
	cfg *config.HistoricalBorrowingConfig
	// lastSample holds, per seeded ClusterQueue/Cohort, the time of its last EMA
	// sample. A present key means the node was seeded, so seeding happens at
	// most once each. Sampling is timed from this in-memory clock rather than
	// the persisted status, so a failed or lagging status write cannot make the
	// next tick re-apply an interval the EMA already absorbed.
	lastSample map[string]time.Time
}

// WithHistoricalBorrowing sets the historical borrowing parameters. It is only
// passed when fairSharing.strategy is HistoricalBorrowing, so a non-nil value
// means the strategy is selected.
func WithHistoricalBorrowing(cfg *config.HistoricalBorrowingConfig) Option {
	return func(c *Cache) {
		c.hb.cfg = cfg
	}
}

// HistoricalBorrowingOption returns a cache Option that enables historical
// borrowing when fairSharing selects the HistoricalBorrowing strategy and
// provides its config; otherwise it is a no-op. Centralizing the strategy check
// here keeps the cmd/kueue wiring to a single line.
func HistoricalBorrowingOption(fs *config.FairSharing) Option {
	if fs == nil || fs.Strategy != config.DRSStrategyHistoricalBorrowing || fs.HistoricalBorrowing == nil {
		return func(*Cache) {}
	}
	return WithHistoricalBorrowing(fs.HistoricalBorrowing)
}

// HistoricalBorrowingActive reports whether the historical borrowing DRS
// strategy is in use for this cache: the feature gate is enabled AND the
// strategy is selected. The config is only set (via WithHistoricalBorrowing)
// when the strategy is HistoricalBorrowing, so a non-nil config plus an enabled
// feature gate captures both conditions.
func (c *Cache) HistoricalBorrowingActive() bool {
	return features.Enabled(features.HistoricalBorrowingFairSharing) && c.hb.cfg != nil
}

func historicalSeededKey(kind, name string) string {
	return kind + ":" + name
}

// borrowingHistoryWeight returns k (BorrowingHistoryWeight), used by the DRS
// fold currentBorrowing + k*HistoricalBorrowing. It defaults to 1 when the
// strategy is active but the knob is unset, and is 0 when the strategy is
// inactive (the HistoricalBorrowing maps are empty then, so k is unused).
func (c *Cache) borrowingHistoryWeight() float64 {
	if c.hb.cfg == nil {
		return 0
	}
	if c.hb.cfg.BorrowingHistoryWeight != nil {
		return *c.hb.cfg.BorrowingHistoryWeight
	}
	return defaultBorrowingHistoryWeight
}

// ensureHistoricalBorrowingMap lazily allocates the per-resource EMA map on the
// concrete node. It must be called on the live node (not a getResourceNode copy)
// before the seeding/sampling helpers write to the map.
func ensureHistoricalBorrowingMap(rn *resourceNode) {
	if rn.HistoricalBorrowing == nil {
		rn.HistoricalBorrowing = make(map[corev1.ResourceName]float64)
	}
}

// ensureHistoricalBorrowingNodeLocked allocates the EMA map and records the
// configured weight k on the live node. Recording k here (the single funnel
// before any EMA write) keeps the invariant that every node with a non-empty EMA
// carries the configured k, so DRS computed directly on the live node (metrics
// and stats) and on snapshot clones fold in history identically.
func (c *Cache) ensureHistoricalBorrowingNodeLocked(rn *resourceNode) {
	ensureHistoricalBorrowingMap(rn)
	rn.BorrowingHistoryWeight = c.borrowingHistoryWeight()
}

func (c *Cache) initHistoricalBorrowingLastSampleLocked() {
	if c.hb.lastSample == nil {
		c.hb.lastSample = make(map[string]time.Time)
	}
}

// ensureHistoricalBorrowingSeededLocked seeds node from its persisted status
// once, and starts its sampling clock at the persisted LastUpdate (so the next
// sample also decays across a controller restart) or at now for a fresh node.
func (c *Cache) ensureHistoricalBorrowingSeededLocked(key string, node hierarchicalResourceNode, status *kueue.HistoricalBorrowingStatus, now time.Time) {
	c.initHistoricalBorrowingLastSampleLocked()
	if _, seeded := c.hb.lastSample[key]; seeded {
		return
	}
	seedHistoricalBorrowingNode(node, status)
	c.hb.lastSample[key] = now
	if status != nil && !status.LastUpdate.IsZero() {
		c.hb.lastSample[key] = status.LastUpdate.Time
	}
}

// clearHistoricalBorrowingSeededLocked forgets that a ClusterQueue or Cohort was
// seeded, so that a later object with the same name is re-seeded from its
// persisted status instead of starting from an empty in-memory average. The
// caller must hold the cache write lock.
func (c *Cache) clearHistoricalBorrowingSeededLocked(kind, name string) {
	delete(c.hb.lastSample, historicalSeededKey(kind, name))
}

// sampleHistoricalBorrowingLocked applies an EMA sample to node, which callers
// must have verified is non-nil and present in the cache, once a sampling
// interval has passed since its previous sample. Otherwise it returns the
// remaining wait and leaves the EMA untouched.
func (c *Cache) sampleHistoricalBorrowingLocked(key string, node hierarchicalResourceNode, now time.Time) (*kueue.HistoricalBorrowingStatus, time.Duration) {
	c.initHistoricalBorrowingLastSampleLocked()
	interval := c.hb.cfg.BorrowingSamplingInterval.Duration
	last, seeded := c.hb.lastSample[key]
	if !seeded {
		c.hb.lastSample[key] = now
		return nil, interval
	}
	// A last sample in the future (clock skew in a restored LastUpdate) would
	// give a negative alpha and move the EMA the wrong way.
	elapsed := max(0, now.Sub(last))
	if elapsed < interval {
		return nil, interval - elapsed
	}
	sampleHistoricalBorrowing(node, elapsed, c.hb.cfg.BorrowingHalfLifeTime.Duration)
	c.hb.lastSample[key] = now
	return historicalBorrowingStatus(node, now), 0
}

// EnsureCQHistoricalBorrowingSeeded seeds the CQ's historical borrowing once.
func (c *Cache) EnsureCQHistoricalBorrowingSeeded(name kueue.ClusterQueueReference, status *kueue.HistoricalBorrowingStatus, now time.Time) {
	if !c.HistoricalBorrowingActive() {
		return
	}
	c.Lock()
	defer c.Unlock()
	cq := c.hm.ClusterQueue(name)
	if cq == nil {
		return
	}
	c.ensureHistoricalBorrowingNodeLocked(&cq.resourceNode)
	c.ensureHistoricalBorrowingSeededLocked(historicalSeededKey("cq", string(name)), cq, status, now)
}

// EnsureCohortHistoricalBorrowingSeeded seeds the Cohort's historical borrowing once.
func (c *Cache) EnsureCohortHistoricalBorrowingSeeded(name kueue.CohortReference, status *kueue.HistoricalBorrowingStatus, now time.Time) {
	if !c.HistoricalBorrowingActive() {
		return
	}
	c.Lock()
	defer c.Unlock()
	cohort := c.hm.Cohort(name)
	if cohort == nil {
		return
	}
	c.ensureHistoricalBorrowingNodeLocked(&cohort.resourceNode)
	c.ensureHistoricalBorrowingSeededLocked(historicalSeededKey("cohort", string(name)), cohort, status, now)
}

// SampleCQHistoricalBorrowing applies an EMA sample for the ClusterQueue if a
// sampling interval has passed since its previous sample. A positive wait means
// no sample was taken yet and is the time until the next one is due. The
// returned found reports whether the ClusterQueue is present in the cache; it is
// false when the strategy is inactive or the node is not (yet) cached, which
// lets callers tell "not sampled" apart from "sampled and decayed to empty"
// (the latter returns a nil status, zero wait and found==true).
func (c *Cache) SampleCQHistoricalBorrowing(name kueue.ClusterQueueReference, now time.Time) (status *kueue.HistoricalBorrowingStatus, wait time.Duration, found bool) {
	if !c.HistoricalBorrowingActive() {
		return nil, 0, false
	}
	c.Lock()
	defer c.Unlock()
	cq := c.hm.ClusterQueue(name)
	if cq == nil {
		return nil, 0, false
	}
	c.ensureHistoricalBorrowingNodeLocked(&cq.resourceNode)
	status, wait = c.sampleHistoricalBorrowingLocked(historicalSeededKey("cq", string(name)), cq, now)
	return status, wait, true
}

// SampleCohortHistoricalBorrowing applies an EMA sample for the Cohort; see
// SampleCQHistoricalBorrowing for the wait and found semantics.
func (c *Cache) SampleCohortHistoricalBorrowing(name kueue.CohortReference, now time.Time) (status *kueue.HistoricalBorrowingStatus, wait time.Duration, found bool) {
	if !c.HistoricalBorrowingActive() {
		return nil, 0, false
	}
	c.Lock()
	defer c.Unlock()
	cohort := c.hm.Cohort(name)
	if cohort == nil {
		return nil, 0, false
	}
	c.ensureHistoricalBorrowingNodeLocked(&cohort.resourceNode)
	status, wait = c.sampleHistoricalBorrowingLocked(historicalSeededKey("cohort", string(name)), cohort, now)
	return status, wait, true
}

func historicalBorrowingStatus(node hierarchicalResourceNode, now time.Time) *kueue.HistoricalBorrowingStatus {
	hb := node.getResourceNode().HistoricalBorrowing
	if len(hb) == 0 {
		return nil
	}
	return &kueue.HistoricalBorrowingStatus{
		BorrowedResources: historicalBorrowingToResourceList(hb),
		LastUpdate:        metav1.NewTime(now),
	}
}
