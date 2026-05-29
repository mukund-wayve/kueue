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

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/utils/clock"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
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
	client client.Client
	cache  *schdcache.Cache
	clock  clock.Clock
	cfg    *configapi.HistoricalBorrowingConfig

	newObj         func() T
	fairSharing    func(T) *kueue.FairSharingStatus
	setFairSharing func(T, *kueue.FairSharingStatus)
	ensureSeeded   func(*schdcache.Cache, string, *kueue.HistoricalBorrowingStatus)
	sample         func(*schdcache.Cache, string, time.Duration) (*kueue.HistoricalBorrowingStatus, bool)
}

// Reconcile runs one sampling tick for a ClusterQueue or Cohort:
//  1. Seed the in-memory history from the object's saved status (once).
//  2. If less than one sampling interval has passed since the last update,
//     requeue for the remaining time and do nothing else.
//  3. Otherwise take a new EMA sample and write it back to the object's status,
//     then requeue for the next interval.
//
// It is a no-op while historical borrowing is disabled.
func (r *historicalBorrowingReconciler[T]) Reconcile(ctx context.Context, req reconcile.Request) (reconcile.Result, error) {
	if !r.cache.HistoricalBorrowingActive() {
		return reconcile.Result{}, nil
	}
	log := ctrl.LoggerFrom(ctx)
	obj := r.newObj()
	if err := r.client.Get(ctx, req.NamespacedName, obj); err != nil {
		return reconcile.Result{}, client.IgnoreNotFound(err)
	}

	interval := r.cfg.BorrowingSamplingInterval.Duration
	var histStatus *kueue.HistoricalBorrowingStatus
	if fs := r.fairSharing(obj); fs != nil {
		histStatus = fs.HistoricalBorrowingStatus
	}
	r.ensureSeeded(r.cache, obj.GetName(), histStatus)

	elapsed := interval
	if histStatus != nil && !histStatus.LastUpdate.IsZero() {
		elapsed = r.clock.Now().Sub(histStatus.LastUpdate.Time)
	}
	// Guard against a LastUpdate in the future (clock skew), which would yield a
	// negative alpha and move the EMA the wrong way.
	if elapsed < 0 {
		elapsed = 0
	}
	if elapsed < interval {
		return reconcile.Result{RequeueAfter: interval - elapsed}, nil
	}

	newStatus, found := r.sample(r.cache, obj.GetName(), elapsed)
	if !found {
		// The node is not in the scheduler cache yet (e.g. shortly after a
		// restart, before the ClusterQueue/Cohort has been added). Requeue
		// without touching the persisted status, so a saved EMA is not wiped and
		// is restored via seeding once the node appears.
		return reconcile.Result{RequeueAfter: interval}, nil
	}

	fs := r.fairSharing(obj)
	// Nothing sampled and nothing persisted: no status update needed.
	if newStatus == nil && (fs == nil || fs.HistoricalBorrowingStatus == nil) {
		return reconcile.Result{RequeueAfter: interval}, nil
	}
	if fs == nil {
		fs = &kueue.FairSharingStatus{}
		r.setFairSharing(obj, fs)
	}
	// newStatus may be nil when the EMA has fully decayed; writing it clears the
	// persisted status so the stale value is not resurrected via seeding on
	// restart.
	fs.HistoricalBorrowingStatus = newStatus
	if err := r.client.Status().Update(ctx, obj); err != nil {
		if apierrors.IsConflict(err) {
			log.V(3).Info("Conflict updating historical borrowing status", "object", obj.GetName())
			return reconcile.Result{RequeueAfter: interval}, nil
		}
		return reconcile.Result{}, err
	}
	return reconcile.Result{RequeueAfter: interval}, nil
}

func (r *historicalBorrowingReconciler[T]) SetupWithManager(mgr ctrl.Manager, cfg *configapi.Configuration) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(r.newObj()).
		Complete(WithLeadingManager(mgr, r, r.newObj(), cfg))
}

func newHistoricalBorrowingCQReconciler(c client.Client, cache *schdcache.Cache, cfg *configapi.HistoricalBorrowingConfig) *historicalBorrowingReconciler[*kueue.ClusterQueue] {
	return &historicalBorrowingReconciler[*kueue.ClusterQueue]{
		client: c,
		cache:  cache,
		clock:  clock.RealClock{},
		cfg:    cfg,
		newObj: func() *kueue.ClusterQueue { return &kueue.ClusterQueue{} },
		fairSharing: func(cq *kueue.ClusterQueue) *kueue.FairSharingStatus {
			return cq.Status.FairSharing
		},
		setFairSharing: func(cq *kueue.ClusterQueue, fs *kueue.FairSharingStatus) {
			cq.Status.FairSharing = fs
		},
		ensureSeeded: func(cc *schdcache.Cache, name string, status *kueue.HistoricalBorrowingStatus) {
			cc.EnsureCQHistoricalBorrowingSeeded(kueue.ClusterQueueReference(name), status)
		},
		sample: func(cc *schdcache.Cache, name string, elapsed time.Duration) (*kueue.HistoricalBorrowingStatus, bool) {
			return cc.SampleCQHistoricalBorrowing(kueue.ClusterQueueReference(name), elapsed)
		},
	}
}

func newHistoricalBorrowingCohortReconciler(c client.Client, cache *schdcache.Cache, cfg *configapi.HistoricalBorrowingConfig) *historicalBorrowingReconciler[*kueue.Cohort] {
	return &historicalBorrowingReconciler[*kueue.Cohort]{
		client: c,
		cache:  cache,
		clock:  clock.RealClock{},
		cfg:    cfg,
		newObj: func() *kueue.Cohort { return &kueue.Cohort{} },
		fairSharing: func(cohort *kueue.Cohort) *kueue.FairSharingStatus {
			return cohort.Status.FairSharing
		},
		setFairSharing: func(cohort *kueue.Cohort, fs *kueue.FairSharingStatus) {
			cohort.Status.FairSharing = fs
		},
		ensureSeeded: func(cc *schdcache.Cache, name string, status *kueue.HistoricalBorrowingStatus) {
			cc.EnsureCohortHistoricalBorrowingSeeded(kueue.CohortReference(name), status)
		},
		sample: func(cc *schdcache.Cache, name string, elapsed time.Duration) (*kueue.HistoricalBorrowingStatus, bool) {
			return cc.SampleCohortHistoricalBorrowing(kueue.CohortReference(name), elapsed)
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
	if err := newHistoricalBorrowingCQReconciler(mgr.GetClient(), cc, hbCfg).SetupWithManager(mgr, cfg); err != nil {
		return "HistoricalBorrowingClusterQueue", err
	}
	if err := newHistoricalBorrowingCohortReconciler(mgr.GetClient(), cc, hbCfg).SetupWithManager(mgr, cfg); err != nil {
		return "HistoricalBorrowingCohort", err
	}
	return "", nil
}
