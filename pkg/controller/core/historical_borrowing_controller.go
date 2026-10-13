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
	"context"
	"time"

	"k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/utils/clock"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	configapi "sigs.k8s.io/kueue/apis/config/v1beta2"
	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta2"
	schdcache "sigs.k8s.io/kueue/pkg/cache/scheduler"
)

// historicalBorrowingReconciler samples and persists historical borrowing for a
// ClusterQueue or Cohort. The two object kinds share the identical reconcile
// algorithm; only the typed accessors and cache calls differ, supplied via the
// adapter fields below.
type historicalBorrowingReconciler[T client.Object] struct {
	client  client.Client
	cache   *schdcache.Cache
	clock   clock.Clock
	cfg     *configapi.HistoricalBorrowingConfig
	elected <-chan struct{}

	newObj         func() T
	fairSharing    func(T) *kueue.FairSharingStatus
	setFairSharing func(T, *kueue.FairSharingStatus)
	sync           func(c *schdcache.Cache, name string, status *kueue.HistoricalBorrowingStatus, now time.Time, leader bool) (*kueue.HistoricalBorrowingStatus, time.Duration, bool)
}

// Reconcile runs one sampling tick for a ClusterQueue or Cohort on every
// replica:
//  1. Load the object's saved status into memory if it is newer than memory,
//     so non-leaders follow the leader and a new leader resumes from the last
//     write.
//  2. Only the elected leader continues. If less than one sampling interval has
//     passed since the last in-memory sample, requeue for the remaining time.
//  3. Otherwise take a new EMA sample and patch it into the object's status,
//     then requeue for the next interval.
//
// The in-memory EMA is authoritative on the leader: if the status write fails,
// the persisted status lags by one interval and the next tick writes the
// current value.
//
// It is a no-op while historical borrowing is disabled.
func (r *historicalBorrowingReconciler[T]) Reconcile(ctx context.Context, req reconcile.Request) (reconcile.Result, error) {
	if !r.cache.HistoricalBorrowingActive() {
		return reconcile.Result{}, nil
	}
	obj := r.newObj()
	if err := r.client.Get(ctx, req.NamespacedName, obj); err != nil {
		return reconcile.Result{}, client.IgnoreNotFound(err)
	}

	interval := r.cfg.BorrowingSamplingInterval.Duration
	// LastUpdate is persisted with second precision; sampling on the same clock
	// keeps the persisted time equal to the in-memory one.
	now := r.clock.Now().Truncate(time.Second)
	leader := r.leader()
	newStatus, wait, found := r.sync(r.cache, obj.GetName(), r.historicalStatus(obj), now, leader)
	if !found || !leader {
		// Either the node is not in the scheduler cache yet (e.g. shortly after
		// a restart) or this replica is not the leader. Requeue without writing,
		// so a saved EMA is not wiped and a replica that becomes leader starts
		// sampling within one interval.
		return reconcile.Result{RequeueAfter: interval}, nil
	}
	if wait > 0 {
		return reconcile.Result{RequeueAfter: wait}, nil
	}
	// Nothing sampled and nothing persisted: no status update needed.
	if newStatus == nil && r.historicalStatus(obj) == nil {
		return reconcile.Result{RequeueAfter: interval}, nil
	}
	orig := obj.DeepCopyObject().(T)
	fs := r.fairSharing(obj)
	if fs == nil {
		fs = &kueue.FairSharingStatus{}
		r.setFairSharing(obj, fs)
	}
	// newStatus may be nil when the EMA has fully decayed; writing it clears the
	// persisted status so the stale value is not resurrected on restart.
	fs.HistoricalBorrowingStatus = newStatus
	if err := r.client.Status().Patch(ctx, obj, client.MergeFrom(orig)); err != nil {
		return reconcile.Result{}, client.IgnoreNotFound(err)
	}
	return reconcile.Result{RequeueAfter: interval}, nil
}

func (r *historicalBorrowingReconciler[T]) historicalStatus(obj T) *kueue.HistoricalBorrowingStatus {
	if fs := r.fairSharing(obj); fs != nil {
		return fs.HistoricalBorrowingStatus
	}
	return nil
}

// leader reports whether this replica is the elected leader. The channel is
// closed on election, or immediately when leader election is disabled.
func (r *historicalBorrowingReconciler[T]) leader() bool {
	select {
	case <-r.elected:
		return true
	default:
		return false
	}
}

// historicalStatusChanged filters update events: spec updates do not affect
// history, and only status changes written by the leader need to be loaded by
// the other replicas.
func (r *historicalBorrowingReconciler[T]) historicalStatusChanged(e event.UpdateEvent) bool {
	return !equality.Semantic.DeepEqual(r.historicalStatus(e.ObjectOld.(T)), r.historicalStatus(e.ObjectNew.(T)))
}

func (r *historicalBorrowingReconciler[T]) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(r.newObj(), builder.WithPredicates(predicate.Funcs{UpdateFunc: r.historicalStatusChanged})).
		WithOptions(controller.Options{NeedLeaderElection: new(false)}).
		Complete(r)
}

func newHistoricalBorrowingCQReconciler(
	c client.Client,
	cache *schdcache.Cache,
	cfg *configapi.HistoricalBorrowingConfig,
	elected <-chan struct{},
) *historicalBorrowingReconciler[*kueue.ClusterQueue] {
	return &historicalBorrowingReconciler[*kueue.ClusterQueue]{
		client:  c,
		cache:   cache,
		clock:   clock.RealClock{},
		cfg:     cfg,
		elected: elected,
		newObj:  func() *kueue.ClusterQueue { return &kueue.ClusterQueue{} },
		fairSharing: func(cq *kueue.ClusterQueue) *kueue.FairSharingStatus {
			return cq.Status.FairSharing
		},
		setFairSharing: func(cq *kueue.ClusterQueue, fs *kueue.FairSharingStatus) {
			cq.Status.FairSharing = fs
		},
		sync: func(cc *schdcache.Cache, name string, status *kueue.HistoricalBorrowingStatus, now time.Time, leader bool) (*kueue.HistoricalBorrowingStatus, time.Duration, bool) {
			return cc.SyncCQHistoricalBorrowing(kueue.ClusterQueueReference(name), status, now, leader)
		},
	}
}

func newHistoricalBorrowingCohortReconciler(c client.Client, cache *schdcache.Cache, cfg *configapi.HistoricalBorrowingConfig, elected <-chan struct{}) *historicalBorrowingReconciler[*kueue.Cohort] {
	return &historicalBorrowingReconciler[*kueue.Cohort]{
		client:  c,
		cache:   cache,
		clock:   clock.RealClock{},
		cfg:     cfg,
		elected: elected,
		newObj:  func() *kueue.Cohort { return &kueue.Cohort{} },
		fairSharing: func(cohort *kueue.Cohort) *kueue.FairSharingStatus {
			return cohort.Status.FairSharing
		},
		setFairSharing: func(cohort *kueue.Cohort, fs *kueue.FairSharingStatus) {
			cohort.Status.FairSharing = fs
		},
		sync: func(cc *schdcache.Cache, name string, status *kueue.HistoricalBorrowingStatus, now time.Time, leader bool) (*kueue.HistoricalBorrowingStatus, time.Duration, bool) {
			return cc.SyncCohortHistoricalBorrowing(kueue.CohortReference(name), status, now, leader)
		},
	}
}

func setupHistoricalBorrowingControllers(mgr ctrl.Manager, cc *schdcache.Cache, cfg *configapi.Configuration) (string, error) {
	if !cc.HistoricalBorrowingActive() {
		return "", nil
	}
	if cfg.FairSharing == nil || cfg.FairSharing.HistoricalBorrowing == nil {
		return "", nil
	}
	hbCfg := cfg.FairSharing.HistoricalBorrowing
	if err := newHistoricalBorrowingCQReconciler(mgr.GetClient(), cc, hbCfg, mgr.Elected()).SetupWithManager(mgr); err != nil {
		return "HistoricalBorrowingClusterQueue", err
	}
	if err := newHistoricalBorrowingCohortReconciler(mgr.GetClient(), cc, hbCfg, mgr.Elected()).SetupWithManager(mgr); err != nil {
		return "HistoricalBorrowingCohort", err
	}
	return "", nil
}
