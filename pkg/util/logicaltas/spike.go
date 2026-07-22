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

// Package logicaltas holds the opt-in switch and shared constants for the
// Logical TAS (cluster-as-node) MultiKueue spike. The manager treats each worker
// cluster as a single topology domain for TAS-based cluster selection and
// cluster-bounded preemption, without syncing physical nodes or pods.
package logicaltas

import "os"

const (
	// envVar opts a manager process into the Logical TAS spike.
	envVar = "KUEUE_LOGICAL_TAS_SPIKE"

	// ClusterLabel is the single topology level stamped onto each fake Node
	// representing a worker cluster in the manager TAS cache.
	ClusterLabel = "kueue.x-k8s.io/multikueue-cluster"
)

// Enabled reports whether the Logical TAS spike is on for this process.
func Enabled() bool {
	return os.Getenv(envVar) == "true"
}
