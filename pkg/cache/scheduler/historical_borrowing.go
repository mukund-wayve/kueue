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
	"strconv"
	"time"

	"gopkg.in/inf.v0"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/util/sets"

	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta2"
	"sigs.k8s.io/kueue/pkg/resources"
)

// historicalBorrowingEpsilon is the per-resource amount, in accounting units,
// below which an EMA entry no longer fed by current borrowing is dropped. The
// EMA is kept in float64 so that small per-tick contributions (alpha*borrowing
// is often far below one unit for whole-unit resources such as GPUs) are not
// truncated away; the geometric decay then needs a floor to ever end.
const historicalBorrowingEpsilon = 1e-3

// historicalBorrowingScale is the number of decimal places, in the resource's
// API unit, kept when the EMA is persisted.
const historicalBorrowingScale = 9

func borrowingAlphaRate(elapsedSeconds, halfLifeSeconds float64) float64 {
	if halfLifeSeconds == 0 {
		return 0
	}
	return 1.0 - math.Pow(0.5, elapsedSeconds/halfLifeSeconds)
}

// currentBorrowingByResource returns the node's instantaneous borrowing (usage
// above quota) summed across flavors per resource name. This matches the
// granularity of both the HistoricalBorrowing EMA and the per-resource DRS ratio.
func currentBorrowingByResource(rn resourceNode) map[corev1.ResourceName]resources.Amount {
	byResource := make(map[corev1.ResourceName]resources.Amount, len(rn.SubtreeQuota))
	for fr, quota := range rn.SubtreeQuota {
		if amountBorrowed := rn.Usage[fr].Sub(quota); amountBorrowed.CmpInt64(0) > 0 {
			byResource[fr.Resource] = byResource[fr.Resource].Add(amountBorrowed)
		}
	}
	return byResource
}

// historicalBorrowingTerms returns the remembered borrowing that
// dominantResourceShare adds to the instantaneous borrowing ratio, as k*EMA per
// resource in accounting units. It returns nil unless the node carries
// historical data (only populated while the HistoricalBorrowing strategy is
// active) and k > 0, so the default DRS path is unchanged.
//
// The effective borrowing is current[r] + k*EMA[r], where k is the node's
// BorrowingHistoryWeight. Unlike a max() fold (which can only raise a node to
// its own instantaneous borrowing), the additive term keeps inflating a heavy
// past borrower's share even when its current borrowing already matches its
// history, so a light or new borrower (EMA=0) is preferred by CompareDRS at
// equal current borrowing and can catch up. k=0 reproduces the plain Borrowing
// score. The terms stay in float64 so sub-unit history still moves the ratio.
//
// History only raises the share: it never marks the node as borrowing, so
// workloads within the node's nominal quota keep their priority. Each resource
// with history is added to borrowing (as zero when not currently borrowed) so
// the caller's ratio loop visits it. A node with zero fair weight is skipped:
// its share is already +Inf when borrowing and 0 otherwise, and history would
// only turn the latter into +Inf.
func historicalBorrowingTerms(node dominantResourceShareNode, borrowing map[corev1.ResourceName]resources.Amount) map[corev1.ResourceName]float64 {
	rn := node.getResourceNode()
	if len(rn.HistoricalBorrowing) == 0 || rn.BorrowingHistoryWeight == 0 || node.fairWeight() == 0 {
		return nil
	}
	terms := make(map[corev1.ResourceName]float64, len(rn.HistoricalBorrowing))
	for r, hist := range rn.HistoricalBorrowing {
		weighted := rn.BorrowingHistoryWeight * hist
		if !(weighted > 0) {
			continue
		}
		terms[r] = weighted
		if _, ok := borrowing[r]; !ok {
			borrowing[r] = resources.Amount{}
		}
	}
	return terms
}

// historicalPerThousand returns the history term h over lendable in thousandths,
// on the same scale as Amount.PerThousandOf. lendable must be positive. A
// lendable past int64 makes any realistic history negligible.
func historicalPerThousand(h float64, lendable resources.Amount) float64 {
	l, ok := lendable.Int64()
	if h == 0 || !ok {
		return 0
	}
	return h * 1000 / float64(l)
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
	current := currentBorrowingByResource(rn)
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
		cur := amountFloat64(current[r])
		blended := (1-alpha)*rn.HistoricalBorrowing[r] + alpha*cur
		if cur == 0 && blended < historicalBorrowingEpsilon {
			delete(rn.HistoricalBorrowing, r)
			continue
		}
		rn.HistoricalBorrowing[r] = blended
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
	if status == nil {
		return
	}
	for r, q := range status.BorrowedResources {
		if v := quantityToHistoricalBorrowing(r, q); v > 0 {
			rn.HistoricalBorrowing[r] = v
		}
	}
}

// historicalBorrowingToResourceList converts the per-resource historical
// borrowing EMA into a ResourceList for persistence. Values keep
// historicalBorrowingScale decimal places, rounded down, so persist/restore
// round-trips sub-unit history without amplification.
func historicalBorrowingToResourceList(hb map[corev1.ResourceName]float64) corev1.ResourceList {
	if len(hb) == 0 {
		return nil
	}
	result := make(corev1.ResourceList, len(hb))
	for r, v := range hb {
		if r == corev1.ResourceCPU {
			v /= 1000
		}
		d, ok := new(inf.Dec).SetString(strconv.FormatFloat(v, 'f', -1, 64))
		if !ok {
			continue
		}
		d.Round(d, historicalBorrowingScale, inf.RoundDown)
		result[r] = *resource.NewDecimalQuantity(*d, resource.DecimalSI)
	}
	return result
}

// quantityToHistoricalBorrowing converts a persisted quantity back to the EMA's
// accounting units (milliCPU for cpu, whole units otherwise).
func quantityToHistoricalBorrowing(r corev1.ResourceName, q resource.Quantity) float64 {
	v := q.AsApproximateFloat64()
	if r == corev1.ResourceCPU {
		v *= 1000
	}
	return v
}

// amountFloat64 returns a as a float64 in its internal accounting units, for the
// EMA blend. Historical borrowing amounts stay far below the int64 range, so the
// out-of-range fallback (0) is never hit in practice.
func amountFloat64(a resources.Amount) float64 {
	v, _ := a.Int64()
	return float64(v)
}
