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
	// lastSample holds, per loaded ClusterQueue/Cohort, the time of its last EMA
	// sample (or of the persisted status it was loaded from). A present key
	// means the node was loaded. Sampling is timed from this in-memory clock
	// rather than the persisted status, so a failed or lagging status write
	// cannot make the next tick re-apply an interval the EMA already absorbed.
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

// loadIfNewerLocked replaces the node's in-memory history with status when the
// node has not been loaded yet, or when status is newer than the last in-memory
// sample (another replica sampled and wrote it). The sampling clock restarts at
// the persisted LastUpdate, so the next sample also decays across a restart or
// failover, or at now for a node without saved state. The leader never reloads
// its own writes: it persists LastUpdate equal to its in-memory sample time.
func (c *Cache) loadIfNewerLocked(key string, node hierarchicalResourceNode, status *kueue.HistoricalBorrowingStatus, now time.Time) {
	c.initHistoricalBorrowingLastSampleLocked()
	last, loaded := c.hb.lastSample[key]
	if loaded && (status == nil || !status.LastUpdate.After(last)) {
		return
	}
	clear(node.getResourceNode().HistoricalBorrowing)
	seedHistoricalBorrowingNode(node, status)
	c.hb.lastSample[key] = now
	if status != nil && !status.LastUpdate.IsZero() {
		c.hb.lastSample[key] = status.LastUpdate.Time
	}
}

// loadHistoricalBorrowingOnAddLocked loads saved history when the cache first
// sees a ClusterQueue or Cohort, so the scheduler never snapshots the node
// without it after a restart.
func (c *Cache) loadHistoricalBorrowingOnAddLocked(key string, node hierarchicalResourceNode, rn *resourceNode, fs *kueue.FairSharingStatus) {
	if !c.HistoricalBorrowingActive() {
		return
	}
	var status *kueue.HistoricalBorrowingStatus
	if fs != nil {
		status = fs.HistoricalBorrowingStatus
	}
	c.ensureHistoricalBorrowingNodeLocked(rn)
	c.loadIfNewerLocked(key, node, status, time.Now().Truncate(time.Second))
}

// forgetCohortHistoricalBorrowingLocked drops a deleted Cohort's history. A
// Cohort still referenced by children survives deletion as an implicit node, so
// its map is cleared too.
func (c *Cache) forgetCohortHistoricalBorrowingLocked(name kueue.CohortReference) {
	delete(c.hb.lastSample, historicalSeededKey("cohort", string(name)))
	if cohort := c.hm.Cohort(name); cohort != nil {
		clear(cohort.resourceNode.HistoricalBorrowing)
	}
}

// syncHistoricalBorrowingLocked loads status if it is newer than memory and,
// on the leader, applies an EMA sample once a sampling interval has passed since
// the previous one. Otherwise it returns the remaining wait (zero for
// non-leaders) and leaves the EMA untouched.
func (c *Cache) syncHistoricalBorrowingLocked(
	key string,
	node hierarchicalResourceNode,
	rn *resourceNode,
	status *kueue.HistoricalBorrowingStatus,
	now time.Time,
	leader bool,
) (*kueue.HistoricalBorrowingStatus, time.Duration) {
	c.ensureHistoricalBorrowingNodeLocked(rn)
	c.loadIfNewerLocked(key, node, status, now)
	if !leader {
		return nil, 0
	}
	interval := c.hb.cfg.BorrowingSamplingInterval.Duration
	// A last sample in the future (clock skew in a restored LastUpdate) would
	// give a negative alpha and move the EMA the wrong way.
	elapsed := max(0, now.Sub(c.hb.lastSample[key]))
	if elapsed < interval {
		return nil, interval - elapsed
	}
	sampleHistoricalBorrowing(node, elapsed, c.hb.cfg.BorrowingHalfLifeTime.Duration)
	c.hb.lastSample[key] = now
	return historicalBorrowingStatus(node, now), 0
}

// SyncCQHistoricalBorrowing loads the ClusterQueue's persisted history if it is
// newer than memory and, when leader, samples it. A positive wait means no
// sample was taken yet and is the time until the next one is due. The returned
// found reports whether the ClusterQueue is present in the cache; it is false
// when the strategy is inactive or the node is not (yet) cached, which lets
// callers tell "not sampled" apart from "sampled and decayed to empty" (the
// latter returns a nil status, zero wait and found==true).
func (c *Cache) SyncCQHistoricalBorrowing(
	name kueue.ClusterQueueReference,
	status *kueue.HistoricalBorrowingStatus,
	now time.Time,
	leader bool,
) (newStatus *kueue.HistoricalBorrowingStatus, wait time.Duration, found bool) {
	if !c.HistoricalBorrowingActive() {
		return nil, 0, false
	}
	c.Lock()
	defer c.Unlock()
	cq := c.hm.ClusterQueue(name)
	if cq == nil {
		return nil, 0, false
	}
	newStatus, wait = c.syncHistoricalBorrowingLocked(historicalSeededKey("cq", string(name)), cq, &cq.resourceNode, status, now, leader)
	return newStatus, wait, true
}

// SyncCohortHistoricalBorrowing is SyncCQHistoricalBorrowing for a Cohort.
func (c *Cache) SyncCohortHistoricalBorrowing(
	name kueue.CohortReference,
	status *kueue.HistoricalBorrowingStatus,
	now time.Time,
	leader bool,
) (newStatus *kueue.HistoricalBorrowingStatus, wait time.Duration, found bool) {
	if !c.HistoricalBorrowingActive() {
		return nil, 0, false
	}
	c.Lock()
	defer c.Unlock()
	cohort := c.hm.Cohort(name)
	if cohort == nil {
		return nil, 0, false
	}
	newStatus, wait = c.syncHistoricalBorrowingLocked(historicalSeededKey("cohort", string(name)), cohort, &cohort.resourceNode, status, now, leader)
	return newStatus, wait, true
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
