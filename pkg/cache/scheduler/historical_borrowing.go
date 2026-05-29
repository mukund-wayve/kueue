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
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/sets"

	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta2"
	"sigs.k8s.io/kueue/pkg/resources"
)

// historicalBorrowingEpsilon is the per-resource amount below which a decayed
// EMA entry is dropped. Without it a truncated EMA can stick at a small residual
// forever, keeping a node flagged as borrowing long after it stopped.
const historicalBorrowingEpsilon = 1

func borrowingAlphaRate(elapsedSeconds, halfLifeSeconds float64) float64 {
	if halfLifeSeconds == 0 {
		return 0
	}
	return 1.0 - math.Pow(0.5, elapsedSeconds/halfLifeSeconds)
}

// currentBorrowingFR returns the node's instantaneous per-FlavorResource
// borrowing: usage (optionally including a candidate workload request) above
// quota. During scheduling the candidate workload is already added to Usage via
// SimulateUsageAddition, so wlReq is usually nil there.
func currentBorrowingFR(rn resourceNode, wlReq resources.FlavorResourceQuantities) resources.FlavorResourceQuantities {
	borrowing := make(resources.FlavorResourceQuantities, len(rn.SubtreeQuota))
	for fr, quota := range rn.SubtreeQuota {
		amountBorrowed := wlReq[fr].Add(rn.Usage[fr]).Sub(quota)
		if amountBorrowed.CmpInt64(0) > 0 {
			borrowing[fr] = amountBorrowed
		}
	}
	return borrowing
}

// currentBorrowingByResource aggregates instantaneous borrowing across flavors
// into per-resource-name amounts. This matches the granularity of both the
// HistoricalBorrowing EMA and the per-resource DRS ratio.
func currentBorrowingByResource(rn resourceNode, wlReq resources.FlavorResourceQuantities) map[corev1.ResourceName]resources.Amount {
	byResource := make(map[corev1.ResourceName]resources.Amount, len(rn.SubtreeQuota))
	for fr, amount := range currentBorrowingFR(rn, wlReq) {
		byResource[fr.Resource] = byResource[fr.Resource].Add(amount)
	}
	return byResource
}

// applyHistoricalBorrowing augments the instantaneous borrowing already computed
// by dominantResourceShare with remembered borrowing. It is a no-op unless the
// node carries historical data (only populated while the HistoricalBorrowing
// strategy is active) and k > 0, so the default DRS path is unchanged.
//
// For each resource it adds k*decayedBorrowing on top of the current borrowing,
// i.e. effectiveBorrowing[r] = current[r] + k*EMA[r], where k is the node's
// BorrowingHistoryWeight. Unlike a max() fold (which can only raise a node to
// its own instantaneous borrowing), the additive term keeps inflating a heavy
// past borrower's share even when its current borrowing already matches its
// history, so a light or new borrower (EMA=0) is preferred by CompareDRS at
// equal current borrowing and can catch up. k=0 reproduces the plain Borrowing
// score. Every flavor of an affected resource that is not already counted as
// currently borrowing is reported as borrowed (the per-resource EMA does not
// record which flavor the borrowing came from). borrowing is mutated in place;
// the extended borrowedFRs is returned.
func applyHistoricalBorrowing(node dominantResourceShareNode, wlReq resources.FlavorResourceQuantities, borrowing map[corev1.ResourceName]resources.Amount, borrowedFRs []resources.FlavorResource) []resources.FlavorResource {
	rn := node.getResourceNode()
	if len(rn.HistoricalBorrowing) == 0 || rn.BorrowingHistoryWeight == 0 {
		return borrowedFRs
	}
	currentFR := currentBorrowingFR(rn, wlReq)
	for r, hist := range rn.HistoricalBorrowing {
		weighted := int64(rn.BorrowingHistoryWeight * amountFloat64(hist))
		if weighted <= 0 {
			continue
		}
		borrowing[r] = borrowing[r].Add(resources.NewAmount(weighted))
		for fr := range rn.SubtreeQuota {
			if fr.Resource != r {
				continue
			}
			if _, ok := currentFR[fr]; ok {
				continue
			}
			borrowedFRs = append(borrowedFRs, fr)
		}
	}
	return borrowedFRs
}

// sampleHistoricalBorrowing folds the node's current borrowing into its
// HistoricalBorrowing EMA, per resource name. It is called once per sampling
// tick by the historical borrowing controllers; elapsed is the time since the
// previous sample and drives how much weight the new reading gets (alpha).
// Resources no longer borrowed decay toward zero and are dropped once they fall
// below historicalBorrowingEpsilon.
func sampleHistoricalBorrowing(node hierarchicalResourceNode, elapsed, halfLife time.Duration) {
	alpha := borrowingAlphaRate(elapsed.Seconds(), halfLife.Seconds())
	if alpha == 0 {
		return
	}
	rn := node.getResourceNode()
	current := currentBorrowingByResource(rn, nil)
	// Iterate the union of currently-borrowed and previously-tracked resources so
	// each is updated exactly once.
	resourceNames := sets.New[corev1.ResourceName]()
	for r := range current {
		resourceNames.Insert(r)
	}
	for r := range rn.HistoricalBorrowing {
		resourceNames.Insert(r)
	}
	for r := range resourceNames {
		// old/cur are resource Amounts converted to float64 for the blend. The
		// 53-bit mantissa represents these exactly up to ~9e15 (e.g. ~9 PB of
		// memory in bytes, ~9e12 cpu millicores), which is far above any
		// realistic quota, so the conversion is lossless in practice.
		old := amountFloat64(rn.HistoricalBorrowing[r])
		cur := amountFloat64(current[r])
		// Truncate toward zero so a resource with no current borrowing strictly
		// decays each tick and eventually reaches zero (rounding could stall).
		blended := int64((1-alpha)*old + alpha*cur)
		if blended < historicalBorrowingEpsilon {
			delete(rn.HistoricalBorrowing, r)
			continue
		}
		rn.HistoricalBorrowing[r] = resources.NewAmount(blended)
	}
}

// seedHistoricalBorrowingNode initializes a node's HistoricalBorrowing once,
// when the controller first sees the ClusterQueue or Cohort (including after a
// restart). It does not blend like sampling does:
//
//   - If a persisted status exists, restore it as-is. Because both the status
//     and the EMA are keyed per resource name, this is an identity round-trip
//     (no fan-out across flavors). The decay for the time the controller was
//     down is applied by the next sample via elapsed since status.LastUpdate.
//   - Otherwise (a fresh object with no saved state), start the EMA at zero and
//     let it warm up from sampling. Seeding to current borrowing instead would
//     deny a brand-new or returning team its catch-up credit: with the additive
//     fold its EMA would already equal its current borrowing, so it would be
//     scored as a heavy past borrower from the first tick.
func seedHistoricalBorrowingNode(node hierarchicalResourceNode, status *kueue.HistoricalBorrowingStatus) {
	rn := node.getResourceNode()
	if status != nil && len(status.BorrowedResources) > 0 {
		for r, q := range status.BorrowedResources {
			rn.HistoricalBorrowing[r] = resources.AmountFromQuantity(r, q)
		}
	}
}

// historicalBorrowingToResourceList converts the per-resource historical
// borrowing EMA into a ResourceList for persistence. It is a direct copy, so
// persist/restore round-trips without amplification.
func historicalBorrowingToResourceList(hb map[corev1.ResourceName]resources.Amount, formatter *resources.ResourceFormatter) corev1.ResourceList {
	if len(hb) == 0 {
		return nil
	}
	result := make(corev1.ResourceList, len(hb))
	for r, amount := range hb {
		result[r] = formatter.AmountQuantity(r, amount)
	}
	return result
}

// amountFloat64 returns a as a float64 in its internal accounting units, for the
// EMA blend. Historical borrowing amounts stay far below the int64 range, so the
// out-of-range fallback (0) is never hit in practice.
func amountFloat64(a resources.Amount) float64 {
	v, _ := a.Int64()
	return float64(v)
}
