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

package multikueue

import (
	"testing"

	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta2"
	"sigs.k8s.io/kueue/pkg/util/logicaltas"
	utiltas "sigs.k8s.io/kueue/pkg/util/tas"
)

func managerLogicalAssignment(domains []utiltas.TopologyDomainAssignment) *kueue.TopologyAssignment {
	return utiltas.V1Beta2From(&utiltas.TopologyAssignment{
		Levels:  []string{logicaltas.ClusterLabel},
		Domains: domains,
	})
}

func TestProjectLogicalTASAdmissionForWorker(t *testing.T) {
	cases := map[string]struct {
		admission   *kueue.Admission
		wantOK      bool
		wantCluster string
	}{
		"no admission yet": {
			admission: nil,
			wantOK:    false,
		},
		"delayed (no topology assignment yet)": {
			admission: &kueue.Admission{
				PodSetAssignments: []kueue.PodSetAssignment{{Name: "main"}},
			},
			wantOK: false,
		},
		"single cluster strips topology": {
			admission: &kueue.Admission{
				ClusterQueue: "central-cq",
				PodSetAssignments: []kueue.PodSetAssignment{{
					Name: "main",
					TopologyAssignment: managerLogicalAssignment([]utiltas.TopologyDomainAssignment{
						{Values: []string{"worker1"}, Count: 2},
					}),
				}},
			},
			wantOK:      true,
			wantCluster: "worker1",
		},
		"assignment spanning two clusters is rejected": {
			admission: &kueue.Admission{
				PodSetAssignments: []kueue.PodSetAssignment{{
					Name: "main",
					TopologyAssignment: managerLogicalAssignment([]utiltas.TopologyDomainAssignment{
						{Values: []string{"worker1"}, Count: 1},
						{Values: []string{"worker2"}, Count: 1},
					}),
				}},
			},
			wantOK: false,
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			wl := &kueue.Workload{}
			wl.Status.Admission = tc.admission

			gotCluster, gotAdmission, gotOK := projectLogicalTASAdmissionForWorker(wl)
			if gotOK != tc.wantOK {
				t.Fatalf("ok = %v, want %v", gotOK, tc.wantOK)
			}
			if !tc.wantOK {
				return
			}
			if gotCluster != tc.wantCluster {
				t.Errorf("cluster = %q, want %q", gotCluster, tc.wantCluster)
			}
			psa := gotAdmission.PodSetAssignments[0]
			if psa.TopologyAssignment != nil {
				t.Errorf("topology should be stripped, got %#v", psa.TopologyAssignment)
			}
			if psa.DelayedTopologyRequest != nil {
				t.Errorf("DelayedTopologyRequest should be cleared, got %v", *psa.DelayedTopologyRequest)
			}
		})
	}
}
