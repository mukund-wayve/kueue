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

package v1beta2

import (
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// FairSharing contains the properties of the ClusterQueue or Cohort,
// when participating in FairSharing.
type FairSharing struct {
	// weight gives a comparative advantage to this ClusterQueue
	// or Cohort when competing for unused resources in the
	// Cohort.  The share is based on the dominant resource usage
	// above nominal quotas for each resource, divided by the
	// weight.  Admission prioritizes scheduling workloads from
	// ClusterQueues and Cohorts with the lowest share and
	// preempting workloads from the ClusterQueues and Cohorts
	// with the highest share.  A zero weight implies infinite
	// share value, meaning that this Node will always be at
	// disadvantage against other ClusterQueues and Cohorts.
	// When not 0, Weight must be greater than 10^-9.
	// +kubebuilder:default=1
	// +optional
	Weight *resource.Quantity `json:"weight,omitempty"`
}

// FairSharingStatus contains the information about the current status of Fair Sharing.
type FairSharingStatus struct {
	// weightedShare represents the maximum of the ratios of usage
	// above nominal quota to the lendable resources in the
	// Cohort, among all the resources provided by the Node, and
	// divided by the weight.  If zero, it means that the usage of
	// the Node is below the nominal quota.  If the Node has a
	// weight of zero and is borrowing, this will return
	// 9223372036854775807, the maximum possible share value.
	// +required
	WeightedShare int64 `json:"weightedShare"`

	// historicalBorrowingStatus holds the decayed borrowing average used by the
	// HistoricalBorrowing fair sharing strategy. It is managed by Kueue and only
	// populated when the HistoricalBorrowingFairSharing feature gate is enabled and
	// fairSharing.strategy is HistoricalBorrowing. It is persisted so the decayed
	// average survives controller restarts and should not be edited by clients.
	// +optional
	HistoricalBorrowingStatus *HistoricalBorrowingStatus `json:"historicalBorrowingStatus,omitempty"`
}

// HistoricalBorrowingStatus holds the persisted, half-life decayed borrowing
// average per resource name.
//
// After a gap without samples (for example while the controller is down), the
// next sample weighs the borrowing observed at that moment by the whole gap, as
// admission fair sharing does for usage. An object deleted and recreated with
// the same name in quick succession may inherit the previous object's history.
type HistoricalBorrowingStatus struct {
	// borrowedResources is the decayed borrowing average per resource name, in the
	// same units as the resource's quota.
	// +required
	BorrowedResources corev1.ResourceList `json:"borrowedResources"`

	// lastUpdate is the time borrowedResources was last sampled. It is used to
	// apply decay for the elapsed time since the previous sample, including time
	// the controller was not running.
	// +required
	LastUpdate metav1.Time `json:"lastUpdate"`
}

type AdmissionScope struct {
	// admissionMode indicates which mode for AdmissionFairSharing should be used
	// in the AdmissionScope. Possible values are:
	// - UsageBasedAdmissionFairSharing
	// - NoAdmissionFairSharing
	//
	// +kubebuilder:validation:Enum=UsageBasedAdmissionFairSharing;NoAdmissionFairSharing
	// +required
	AdmissionMode AdmissionMode `json:"admissionMode,omitempty"`
}

type AdmissionMode string

const (
	// AdmissionFairSharing based on usage, with QueuingStrategy as defined in CQ.
	UsageBasedAdmissionFairSharing AdmissionMode = "UsageBasedAdmissionFairSharing"

	// AdmissionFairSharing is disabled for this CQ
	NoAdmissionFairSharing AdmissionMode = "NoAdmissionFairSharing"
)
