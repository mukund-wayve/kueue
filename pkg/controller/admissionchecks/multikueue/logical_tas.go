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
	"context"
	"fmt"
	"maps"
	"slices"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta2"
	"sigs.k8s.io/kueue/pkg/util/logicaltas"
	utiltas "sigs.k8s.io/kueue/pkg/util/tas"
	"sigs.k8s.io/kueue/pkg/workload"
	workloadpatching "sigs.k8s.io/kueue/pkg/workload/patching"
)

func (rc *remoteClient) syncLogicalTASFakeNode(ctx context.Context) {
	if !logicaltas.Enabled() || rc.schedulerCache == nil {
		return
	}
	log := ctrl.LoggerFrom(ctx).WithValues("clusterName", rc.clusterName)

	remoteCl := rc.getClient()
	if remoteCl == nil {
		return
	}

	allocatable, labels, err := rc.logicalTASNodeCapacity(ctx, remoteCl)
	if err != nil {
		log.Error(err, "Failed to compute worker cluster capacity")
		return
	}
	if len(allocatable) == 0 {
		rc.schedulerCache.TASCache().DeleteNodeByName(rc.clusterName)
		return
	}

	rc.schedulerCache.TASCache().SyncNode(&corev1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: rc.clusterName, Labels: labels},
		Status: corev1.NodeStatus{
			Capacity:    allocatable,
			Allocatable: allocatable,
			Conditions:  []corev1.NodeCondition{{Type: corev1.NodeReady, Status: corev1.ConditionTrue}},
		},
	})
}

func (rc *remoteClient) logicalTASNodeCapacity(ctx context.Context, remoteCl client.Client) (corev1.ResourceList, map[string]string, error) {
	nodes := &corev1.NodeList{}
	if err := remoteCl.List(ctx, nodes); err != nil {
		return nil, nil, err
	}

	total := corev1.ResourceList{}
	var labels map[string]string
	for i := range nodes.Items {
		node := &nodes.Items[i]
		if node.Spec.Unschedulable || !utiltas.IsNodeStatusConditionTrue(node.Status.Conditions, corev1.NodeReady) {
			continue
		}
		if labels == nil {
			labels = maps.Clone(node.Labels)
		}
		for res, qty := range node.Status.Allocatable {
			curr := total[res]
			curr.Add(qty)
			total[res] = curr
		}
	}
	if len(total) == 0 {
		return nil, nil, nil
	}
	if labels == nil {
		labels = map[string]string{}
	}
	labels[logicaltas.ClusterLabel] = rc.clusterName
	return total, labels, nil
}

// projectLogicalTASAdmissionForWorker returns the chosen worker cluster and an
// admission with topology stripped so the worker schedules normally.
func projectLogicalTASAdmissionForWorker(local *kueue.Workload) (string, *kueue.Admission, bool) {
	if local.Status.Admission == nil {
		return "", nil, false
	}
	out := local.Status.Admission.DeepCopy()
	clusterName := ""
	sawTopology := false
	for i := range out.PodSetAssignments {
		psa := &out.PodSetAssignments[i]
		if psa.TopologyAssignment == nil {
			continue
		}
		internal := utiltas.InternalFrom(psa.TopologyAssignment)
		if len(internal.Levels) != 1 || internal.Levels[0] != logicaltas.ClusterLabel {
			return "", nil, false
		}
		sawTopology = true
		for _, d := range internal.Domains {
			if len(d.Values) != 1 {
				return "", nil, false
			}
			if clusterName == "" {
				clusterName = d.Values[0]
			} else if clusterName != d.Values[0] {
				return "", nil, false
			}
		}
		psa.TopologyAssignment = nil
		psa.DelayedTopologyRequest = nil
	}
	if !sawTopology || clusterName == "" {
		return "", nil, false
	}
	return clusterName, out, true
}

func (w *wlReconciler) logicalTASNominate(ctx context.Context, group *wlGroup) (reconcile.Result, bool, error) {
	clusterName, admission, ok := projectLogicalTASAdmissionForWorker(group.local)
	if !ok {
		return reconcile.Result{}, false, nil
	}
	log := ctrl.LoggerFrom(ctx).WithValues("op", "logicalTASNominate", "chosenCluster", clusterName)

	rc, found := group.remoteClients[clusterName]
	if !found {
		return reconcile.Result{}, true, fmt.Errorf("logical-TAS: chosen cluster %q is not an available worker", clusterName)
	}
	if !slices.Equal(group.local.Status.NominatedClusterNames, []string{clusterName}) {
		if err := workloadpatching.PatchAdmissionStatus(ctx, w.client, group.local, w.clock, func(wl *kueue.Workload) (bool, error) {
			wl.Status.NominatedClusterNames = []string{clusterName}
			return true, nil
		}); err != nil {
			return reconcile.Result{}, true, err
		}
	}
	if _, err := w.syncToSingleCluster(ctx, log, group, clusterName); err != nil {
		return reconcile.Result{}, true, err
	}

	remoteWl := &kueue.Workload{}
	if err := rc.getClient().Get(ctx, client.ObjectKeyFromObject(group.local), remoteWl); err != nil {
		return reconcile.Result{}, true, client.IgnoreNotFound(err)
	}
	if workload.IsAdmitted(remoteWl) {
		return reconcile.Result{RequeueAfter: w.workerLostTimeout}, true, nil
	}
	workload.SetQuotaReservation(remoteWl, admission, w.clock)
	workload.SetAdmittedCondition(remoteWl, w.clock.Now(), "LogicalTAS", "Admitted by Logical TAS manager")
	if err := rc.getClient().Status().Update(ctx, remoteWl); err != nil {
		return reconcile.Result{}, true, fmt.Errorf("logical-TAS: stamping remote admission: %w", err)
	}
	return reconcile.Result{RequeueAfter: w.workerLostTimeout}, true, nil
}
