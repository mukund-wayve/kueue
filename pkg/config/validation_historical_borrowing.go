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

package config

import (
	"k8s.io/apimachinery/pkg/util/validation/field"

	configapi "sigs.k8s.io/kueue/apis/config/v1beta2"
	"sigs.k8s.io/kueue/pkg/features"
)

var (
	fsStrategyPath            = field.NewPath("fairSharing", "strategy")
	fsHistoricalBorrowingPath = field.NewPath("fairSharing", "historicalBorrowing")
)

// validateFairSharingStrategy validates the DRS strategy and its historical
// borrowing parameters.
func validateFairSharingStrategy(fs *configapi.FairSharing) field.ErrorList {
	var allErrs field.ErrorList
	switch fs.Strategy {
	case "", configapi.DRSStrategyBorrowing:
		if fs.HistoricalBorrowing != nil {
			allErrs = append(allErrs, field.Forbidden(fsHistoricalBorrowingPath,
				"historicalBorrowing has no effect when strategy is Borrowing"))
		}
	case configapi.DRSStrategyHistoricalBorrowing:
		if !features.Enabled(features.HistoricalBorrowingFairSharing) {
			allErrs = append(allErrs, field.Forbidden(fsStrategyPath,
				"requires the HistoricalBorrowingFairSharing feature gate to be enabled"))
		}
		if fs.HistoricalBorrowing == nil {
			allErrs = append(allErrs, field.Required(fsHistoricalBorrowingPath,
				"required when strategy is HistoricalBorrowing"))
		} else {
			hb := fs.HistoricalBorrowing
			if hb.BorrowingHalfLifeTime.Duration <= 0 {
				allErrs = append(allErrs, field.Required(fsHistoricalBorrowingPath.Child("borrowingHalfLifeTime"),
					"required when strategy is HistoricalBorrowing"))
			}
			if hb.BorrowingSamplingInterval.Duration <= 0 {
				allErrs = append(allErrs, field.Required(fsHistoricalBorrowingPath.Child("borrowingSamplingInterval"),
					"required when strategy is HistoricalBorrowing"))
			}
			if hb.BorrowingHistoryWeight != nil && *hb.BorrowingHistoryWeight < 0 {
				allErrs = append(allErrs, field.Invalid(fsHistoricalBorrowingPath.Child("borrowingHistoryWeight"),
					*hb.BorrowingHistoryWeight, "must be greater than or equal to zero"))
			}
		}
	default:
		allErrs = append(allErrs, field.NotSupported(fsStrategyPath, fs.Strategy,
			[]string{string(configapi.DRSStrategyBorrowing), string(configapi.DRSStrategyHistoricalBorrowing)}))
	}
	return allErrs
}
