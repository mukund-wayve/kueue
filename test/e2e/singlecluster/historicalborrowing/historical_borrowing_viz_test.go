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

package historicalborrowing

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta2"
	utiltestingapi "sigs.k8s.io/kueue/pkg/util/testing/v1beta2"
	testingjob "sigs.k8s.io/kueue/pkg/util/testingjobs/job"
	"sigs.k8s.io/kueue/test/util/behavioral"
	"sigs.k8s.io/kueue/test/util/behavioral/e2e"
)

// Scenario tuning. Two ClusterQueues share a lending Cohort. Their own nominal
// quota is tiny, so their dominant resource share is driven almost entirely by
// borrowing, which is what historical borrowing remembers and decays.
const (
	cohortName = "hb-root"
	cqAName    = "hb-a"
	cqBName    = "hb-b"

	jobCPU    = "200m" // per workload
	cqCPU     = "200m" // minimal nominal quota per ClusterQueue
	cohortCPU = "2"    // lendable capacity shared by the cohort

	slotsPerQueue = 12 // concurrent in-flight jobs each queue tries to keep

	minRunSeconds = 2
	maxRunSeconds = 5

	pollInterval = time.Second
)

// sample is one polled data point of the two ClusterQueues' status.
type sample struct {
	T          float64 `json:"t"`
	AReserving int     `json:"aReserving"`
	BReserving int     `json:"bReserving"`
	AHist      float64 `json:"aHist"`
	BHist      float64 `json:"bHist"`
	AShare     int64   `json:"aShare"`
	BShare     int64   `json:"bShare"`
	// ACumCPU/BCumCPU are cumulative admitted cpu-seconds (reserving usage
	// integrated over time). They measure long-term fairness: equal cumulative
	// usage means each queue got an equal share of capacity over the whole run.
	ACumCPU float64 `json:"aCum"`
	BCumCPU float64 `json:"bCum"`
}

// cpuCores parses a cpu quantity string into cores (e.g. "200m" -> 0.2).
func cpuCores(s string) float64 {
	q := resource.MustParse(s)
	return q.AsApproximateFloat64()
}

var _ = ginkgo.Describe("Historical Borrowing visualization", ginkgo.Label("feature:fairsharing"), func() {
	ginkgo.It("drives two ClusterQueues and charts historical usage over time", func() {
		warmup := envDuration("HBVIZ_WARMUP_SECONDS", 20*time.Second)
		total := envDuration("HBVIZ_TOTAL_SECONDS", 60*time.Second)

		ns := behavioral.CreateNamespaceFromPrefixWithLog(ctx, k8sClient, "hb-viz-")
		ginkgo.DeferCleanup(func() {
			_ = behavioral.DeleteNamespace(ctx, k8sClient, ns)
		})

		flavor := utiltestingapi.MakeResourceFlavor("hb-default").Obj()
		gomega.Expect(k8sClient.Create(ctx, flavor)).To(gomega.Succeed())

		cohort := utiltestingapi.MakeCohort(cohortName).
			ResourceGroup(*utiltestingapi.MakeFlavorQuotas("hb-default").Resource(corev1.ResourceCPU, cohortCPU).Obj()).
			Obj()
		gomega.Expect(k8sClient.Create(ctx, cohort)).To(gomega.Succeed())

		cqA := newCQ(cqAName)
		cqB := newCQ(cqBName)
		gomega.Expect(k8sClient.Create(ctx, cqA)).To(gomega.Succeed())
		gomega.Expect(k8sClient.Create(ctx, cqB)).To(gomega.Succeed())
		behavioral.ExpectClusterQueuesToBeActive(ctx, k8sClient, cqA, cqB)

		lqA := utiltestingapi.MakeLocalQueue(cqAName, ns.Name).ClusterQueue(cqAName).Obj()
		lqB := utiltestingapi.MakeLocalQueue(cqBName, ns.Name).ClusterQueue(cqBName).Obj()
		gomega.Expect(k8sClient.Create(ctx, lqA)).To(gomega.Succeed())
		gomega.Expect(k8sClient.Create(ctx, lqB)).To(gomega.Succeed())

		ginkgo.DeferCleanup(func() {
			bg := context.Background()
			for _, o := range []client.Object{cqA, cqB, cohort, flavor} {
				_ = k8sClient.Delete(bg, o, client.PropagationPolicy(metav1.DeletePropagationBackground))
			}
		})

		start := time.Now()
		warmEnd := start.Add(warmup)
		deadline := start.Add(total)

		var (
			samples []sample
			mu      sync.Mutex
			wg      sync.WaitGroup
		)

		ginkgo.By(fmt.Sprintf("running for %s (hb-b joins after %s)", total, warmup))

		wg.Add(1)
		go pollStatus(&wg, start, deadline, &samples, &mu, cqAName, cqBName)

		// hb-a borrows from the start; hb-b joins only after the warm-up, by which
		// point hb-a has built up historical borrowing.
		for i := 0; i < slotsPerQueue; i++ {
			wg.Add(1)
			go runWorker(&wg, cqAName, ns.Name, start, deadline)
			wg.Add(1)
			go runWorker(&wg, cqBName, ns.Name, warmEnd, deadline)
		}

		wg.Wait()

		path, err := writeHTML(samples, warmup.Seconds())
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		ginkgo.GinkgoLogr.Info("historical borrowing visualization written", "path", path, "samples", len(samples))
	})

	// This case probes long-term (cumulative) fairness. hb-a runs ALONE for many
	// half-lives, building a large head start, then both queues saturate. Because
	// max(instantaneous, EMA) only remembers ~one half-life of borrowing, the
	// make-up hb-b receives in phase 2 is bounded and cannot recover hb-a's
	// multi-half-life lead: hb-a keeps a permanent cumulative advantage. The
	// chart and the final assertion make that gap explicit.
	ginkgo.It("shows max(instantaneous, EMA) is not long-term fair: hb-a keeps a permanent cumulative lead", func() {
		// Half-life is 10s (see the e2e config), comparable to the phases, so the
		// EMA penalty persists into phase 2 and hb-b gets a catch-up window.
		phase1 := envDuration("HBFAIR_PHASE1_SECONDS", 20*time.Second)
		phase2 := envDuration("HBFAIR_PHASE2_SECONDS", 40*time.Second)

		const (
			fairCohort = "hbf-root"
			fairFlavor = "hbf-default"
			fairA      = "hbf-a"
			fairB      = "hbf-b"
		)

		ns := behavioral.CreateNamespaceFromPrefixWithLog(ctx, k8sClient, "hb-fair-")
		ginkgo.DeferCleanup(func() {
			_ = behavioral.DeleteNamespace(ctx, k8sClient, ns)
		})

		flavor := utiltestingapi.MakeResourceFlavor(fairFlavor).Obj()
		gomega.Expect(k8sClient.Create(ctx, flavor)).To(gomega.Succeed())

		cohort := utiltestingapi.MakeCohort(fairCohort).
			ResourceGroup(*utiltestingapi.MakeFlavorQuotas(fairFlavor).Resource(corev1.ResourceCPU, cohortCPU).Obj()).
			Obj()
		gomega.Expect(k8sClient.Create(ctx, cohort)).To(gomega.Succeed())

		cqA := newCQNamed(fairA, fairCohort, fairFlavor)
		cqB := newCQNamed(fairB, fairCohort, fairFlavor)
		gomega.Expect(k8sClient.Create(ctx, cqA)).To(gomega.Succeed())
		gomega.Expect(k8sClient.Create(ctx, cqB)).To(gomega.Succeed())
		behavioral.ExpectClusterQueuesToBeActive(ctx, k8sClient, cqA, cqB)

		lqA := utiltestingapi.MakeLocalQueue(fairA, ns.Name).ClusterQueue(fairA).Obj()
		lqB := utiltestingapi.MakeLocalQueue(fairB, ns.Name).ClusterQueue(fairB).Obj()
		gomega.Expect(k8sClient.Create(ctx, lqA)).To(gomega.Succeed())
		gomega.Expect(k8sClient.Create(ctx, lqB)).To(gomega.Succeed())

		ginkgo.DeferCleanup(func() {
			bg := context.Background()
			for _, o := range []client.Object{cqA, cqB, cohort, flavor} {
				_ = k8sClient.Delete(bg, o, client.PropagationPolicy(metav1.DeletePropagationBackground))
			}
		})

		start := time.Now()
		phase1End := start.Add(phase1)
		deadline := start.Add(phase1 + phase2)

		var (
			samples []sample
			mu      sync.Mutex
			wg      sync.WaitGroup
		)

		ginkgo.By(fmt.Sprintf("phase1=%s (hb-a alone), phase2=%s (both saturate)", phase1, phase2))

		wg.Add(1)
		go pollStatus(&wg, start, deadline, &samples, &mu, fairA, fairB)

		// hb-a borrows alone for all of phase 1; hb-b only starts demanding in
		// phase 2.
		for i := 0; i < slotsPerQueue; i++ {
			wg.Add(1)
			go runWorker(&wg, fairA, ns.Name, start, deadline)
			wg.Add(1)
			go runWorker(&wg, fairB, ns.Name, phase1End, deadline)
		}

		wg.Wait()

		path, err := writeFairnessHTML(samples, phase1.Seconds())
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		gomega.Expect(samples).NotTo(gomega.BeEmpty())
		last := samples[len(samples)-1]
		ginkgo.GinkgoLogr.Info("long-term fairness result",
			"path", path, "samples", len(samples),
			"aCumCPUSeconds", last.ACumCPU, "bCumCPUSeconds", last.BCumCPU,
			"gapCPUSeconds", last.ACumCPU-last.BCumCPU)

		// The headline result: even after phase 2, hb-a's cumulative usage still
		// exceeds hb-b's. A bounded-memory EMA gives no credit for the long time
		// hb-b was starved, so the head start is never repaid.
		gomega.Expect(last.ACumCPU).To(gomega.BeNumerically(">", last.BCumCPU),
			"expected hb-a to retain a cumulative lead: max(instantaneous, EMA) is not long-term fair")
	})

	// This case shows that historical borrowing is recency-weighted. team-a
	// accrues alone, then stops; team-b accrues alone, then stops; finally both
	// compete. By the time they compete, team-a's EMA has decayed for a whole
	// phase while team-b's is fresh, so team-b carries the larger remembered
	// borrowing and team-a is favored for the contested capacity at first.
	ginkgo.It("is recency-weighted: team-a accrues, then team-b, then both compete and team-b's fresher history is penalized", func() {
		p1 := envDuration("HBREC_PHASE1_SECONDS", 20*time.Second) // team-a alone
		p2 := envDuration("HBREC_PHASE2_SECONDS", 20*time.Second) // team-b alone
		p3 := envDuration("HBREC_PHASE3_SECONDS", 20*time.Second) // both compete

		const (
			recCohort = "hbr-root"
			recFlavor = "hbr-default"
			recA      = "hbr-a"
			recB      = "hbr-b"
		)

		ns := behavioral.CreateNamespaceFromPrefixWithLog(ctx, k8sClient, "hb-rec-")
		ginkgo.DeferCleanup(func() {
			_ = behavioral.DeleteNamespace(ctx, k8sClient, ns)
		})

		flavor := utiltestingapi.MakeResourceFlavor(recFlavor).Obj()
		gomega.Expect(k8sClient.Create(ctx, flavor)).To(gomega.Succeed())

		cohort := utiltestingapi.MakeCohort(recCohort).
			ResourceGroup(*utiltestingapi.MakeFlavorQuotas(recFlavor).Resource(corev1.ResourceCPU, cohortCPU).Obj()).
			Obj()
		gomega.Expect(k8sClient.Create(ctx, cohort)).To(gomega.Succeed())

		cqA := newCQNamed(recA, recCohort, recFlavor)
		cqB := newCQNamed(recB, recCohort, recFlavor)
		gomega.Expect(k8sClient.Create(ctx, cqA)).To(gomega.Succeed())
		gomega.Expect(k8sClient.Create(ctx, cqB)).To(gomega.Succeed())
		behavioral.ExpectClusterQueuesToBeActive(ctx, k8sClient, cqA, cqB)

		lqA := utiltestingapi.MakeLocalQueue(recA, ns.Name).ClusterQueue(recA).Obj()
		lqB := utiltestingapi.MakeLocalQueue(recB, ns.Name).ClusterQueue(recB).Obj()
		gomega.Expect(k8sClient.Create(ctx, lqA)).To(gomega.Succeed())
		gomega.Expect(k8sClient.Create(ctx, lqB)).To(gomega.Succeed())

		ginkgo.DeferCleanup(func() {
			bg := context.Background()
			for _, o := range []client.Object{cqA, cqB, cohort, flavor} {
				_ = k8sClient.Delete(bg, o, client.PropagationPolicy(metav1.DeletePropagationBackground))
			}
		})

		start := time.Now()
		p1End := start.Add(p1)
		p2End := start.Add(p1 + p2)
		deadline := start.Add(p1 + p2 + p3)

		var (
			samples []sample
			mu      sync.Mutex
			wg      sync.WaitGroup
		)

		ginkgo.By(fmt.Sprintf("phase1=%s (team-a alone), phase2=%s (team-b alone), phase3=%s (both compete)", p1, p2, p3))

		wg.Add(1)
		go pollStatus(&wg, start, deadline, &samples, &mu, recA, recB)

		// team-a borrows only in phase 1 (then stops); team-b borrows only in
		// phase 2 (then stops); both borrow in phase 3.
		for i := 0; i < slotsPerQueue; i++ {
			wg.Add(1)
			go runWorker(&wg, recA, ns.Name, start, p1End) // phase 1
			wg.Add(1)
			go runWorker(&wg, recB, ns.Name, p1End, p2End) // phase 2
			wg.Add(1)
			go runWorker(&wg, recA, ns.Name, p2End, deadline) // phase 3
			wg.Add(1)
			go runWorker(&wg, recB, ns.Name, p2End, deadline) // phase 3
		}

		wg.Wait()

		path, err := writeRecencyHTML(samples, p1.Seconds(), (p1 + p2).Seconds())
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		gomega.Expect(samples).NotTo(gomega.BeEmpty())

		// At the start of phase 3, team-a's history has decayed for a full phase
		// while team-b's is fresh, so team-b carries the larger remembered
		// borrowing. Find the first sample at/after the phase-3 boundary.
		var atCompete *sample
		for i := range samples {
			if samples[i].T >= (p1 + p2).Seconds() {
				atCompete = &samples[i]
				break
			}
		}
		gomega.Expect(atCompete).NotTo(gomega.BeNil())
		ginkgo.GinkgoLogr.Info("recency result at compete start",
			"path", path, "samples", len(samples),
			"aHist", atCompete.AHist, "bHist", atCompete.BHist,
			"aShare", atCompete.AShare, "bShare", atCompete.BShare)

		gomega.Expect(atCompete.BHist).To(gomega.BeNumerically(">", atCompete.AHist),
			"expected team-b's fresher history to exceed team-a's decayed history at the start of competition")
	})
})

func newCQ(name string) *kueue.ClusterQueue {
	return newCQNamed(name, cohortName, "hb-default")
}

func newCQNamed(name, cohort, flavor string) *kueue.ClusterQueue {
	return utiltestingapi.MakeClusterQueue(name).
		Cohort(kueue.CohortReference(cohort)).
		ResourceGroup(*utiltestingapi.MakeFlavorQuotas(flavor).Resource(corev1.ResourceCPU, cqCPU).Obj()).
		// Enable fair-sharing preemption so a queue with a higher (worse) share
		// actively reclaims borrowed capacity from cohort peers, instead of
		// waiting for their workloads to finish naturally.
		Preemption(kueue.ClusterQueuePreemption{
			ReclaimWithinCohort: kueue.PreemptionPolicyAny,
			WithinClusterQueue:  kueue.PreemptionPolicyLowerPriority,
		}).
		Obj()
}

// runWorker keeps one job in flight at a time for the given LocalQueue: it
// creates a job, waits until Kueue admits it (unsuspends), lets it "run" for a
// random 2-5s, then deletes it to free quota. Repeats until the deadline.
func runWorker(wg *sync.WaitGroup, lqName, nsName string, startAt, deadline time.Time) {
	defer wg.Done()
	defer ginkgo.GinkgoRecover()

	if d := time.Until(startAt); d > 0 {
		select {
		case <-ctx.Done():
			return
		case <-time.After(d):
		}
	}

	for time.Now().Before(deadline) && ctx.Err() == nil {
		job := testingjob.MakeJob("", nsName).
			GeneratedName(lqName+"-").
			Queue(kueue.LocalQueueName(lqName)).
			Image(e2e.GetAgnHostImage(), e2e.BehaviorWaitForDeletion).
			RequestAndLimit(corev1.ResourceCPU, jobCPU).
			TerminationGracePeriod(1).
			BackoffLimit(0).
			Suspend(true).
			Obj()
		if err := k8sClient.Create(ctx, job); err != nil {
			time.Sleep(time.Second)
			continue
		}

		waitUntilAdmitted(job, deadline)

		run := time.Duration(minRunSeconds+rand.Intn(maxRunSeconds-minRunSeconds+1)) * time.Second
		if rem := time.Until(deadline); rem < run {
			run = rem
		}
		if run > 0 {
			select {
			case <-ctx.Done():
			case <-time.After(run):
			}
		}

		// Use a background context so the job is always cleaned up, even once the
		// spec context is done.
		_ = k8sClient.Delete(context.Background(), job, client.PropagationPolicy(metav1.DeletePropagationBackground))
	}
}

// waitUntilAdmitted polls until Kueue unsuspends the job (admission) or the
// deadline passes.
func waitUntilAdmitted(job *batchv1.Job, deadline time.Time) {
	key := client.ObjectKeyFromObject(job)
	for time.Now().Before(deadline) && ctx.Err() == nil {
		cur := &batchv1.Job{}
		if err := k8sClient.Get(ctx, key, cur); err == nil {
			if cur.Spec.Suspend != nil && !*cur.Spec.Suspend {
				return
			}
		}
		time.Sleep(500 * time.Millisecond)
	}
}

func pollStatus(wg *sync.WaitGroup, start, deadline time.Time, out *[]sample, mu *sync.Mutex, aName, bName string) {
	defer wg.Done()
	defer ginkgo.GinkgoRecover()

	jobCores := cpuCores(jobCPU)
	prev := start
	var aCum, bCum float64

	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			if now.After(deadline) {
				return
			}
			aRes, aHist, aShare := readCQ(aName)
			bRes, bHist, bShare := readCQ(bName)
			// Integrate instantaneous usage (reserving workloads * cpu/job) over
			// the elapsed interval to accumulate cpu-seconds per queue.
			dt := now.Sub(prev).Seconds()
			prev = now
			aCum += float64(aRes) * jobCores * dt
			bCum += float64(bRes) * jobCores * dt
			mu.Lock()
			*out = append(*out, sample{
				T:          now.Sub(start).Seconds(),
				AReserving: aRes, BReserving: bRes,
				AHist: aHist, BHist: bHist,
				AShare: aShare, BShare: bShare,
				ACumCPU: aCum, BCumCPU: bCum,
			})
			mu.Unlock()
		}
	}
}

// readCQ reads the live, prioritization-relevant signals from a ClusterQueue's
// status: instantaneous reserving workloads, the decayed historical borrowing
// EMA (cpu cores), and the weighted share that drives admission ordering.
func readCQ(name string) (reserving int, histCPU float64, share int64) {
	cq := &kueue.ClusterQueue{}
	if err := k8sClient.Get(ctx, client.ObjectKey{Name: name}, cq); err != nil {
		return 0, 0, 0
	}
	reserving = int(cq.Status.ReservingWorkloads)
	if fs := cq.Status.FairSharing; fs != nil {
		share = fs.WeightedShare
		if hb := fs.HistoricalBorrowingStatus; hb != nil {
			if q, ok := hb.BorrowedResources[corev1.ResourceCPU]; ok {
				histCPU = q.AsApproximateFloat64()
			}
		}
	}
	return reserving, histCPU, share
}

func envDuration(name string, def time.Duration) time.Duration {
	if v := os.Getenv(name); v != "" {
		if secs, err := strconv.Atoi(v); err == nil && secs > 0 {
			return time.Duration(secs) * time.Second
		}
	}
	return def
}

func writeHTML(samples []sample, warmupSeconds float64) (string, error) {
	data, err := json.Marshal(samples)
	if err != nil {
		return "", err
	}
	// lendable is the cohort's total lendable cpu (cohort nominal + both CQs'
	// nominal). The weighted share is borrowing/lendable*1000, so dividing the
	// share back by 1000 and multiplying by lendable reconstructs the exact
	// borrowing (max of instantaneous and EMA) that fed the DRS score.
	lendable := cpuCores(cohortCPU) + 2*cpuCores(cqCPU)

	html := strings.NewReplacer(
		"__DATA__", string(data),
		"__WARMUP__", strconv.FormatFloat(warmupSeconds, 'f', 0, 64),
		"__LENDABLE__", strconv.FormatFloat(lendable, 'f', 3, 64),
	).Replace(htmlTemplate)

	dir := os.Getenv("ARTIFACTS")
	if dir == "" {
		dir = "."
	}
	path := filepath.Join(dir, "historical-borrowing-viz.html")
	return path, os.WriteFile(path, []byte(html), 0o644)
}

func writeFairnessHTML(samples []sample, phase1Seconds float64) (string, error) {
	data, err := json.Marshal(samples)
	if err != nil {
		return "", err
	}
	html := strings.NewReplacer(
		"__DATA__", string(data),
		"__PHASE1__", strconv.FormatFloat(phase1Seconds, 'f', 0, 64),
	).Replace(fairnessTemplate)

	dir := os.Getenv("ARTIFACTS")
	if dir == "" {
		dir = "."
	}
	path := filepath.Join(dir, "historical-borrowing-fairness.html")
	return path, os.WriteFile(path, []byte(html), 0o644)
}

func writeRecencyHTML(samples []sample, p1Seconds, p2Seconds float64) (string, error) {
	data, err := json.Marshal(samples)
	if err != nil {
		return "", err
	}
	html := strings.NewReplacer(
		"__DATA__", string(data),
		"__PHASES__", fmt.Sprintf("[%s,%s]",
			strconv.FormatFloat(p1Seconds, 'f', 0, 64),
			strconv.FormatFloat(p2Seconds, 'f', 0, 64)),
	).Replace(recencyTemplate)

	dir := os.Getenv("ARTIFACTS")
	if dir == "" {
		dir = "."
	}
	path := filepath.Join(dir, "historical-borrowing-recency.html")
	return path, os.WriteFile(path, []byte(html), 0o644)
}

const recencyTemplate = `<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>Historical Borrowing: recency weighting</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4"></script>
<style>
  body { font-family: system-ui, sans-serif; margin: 24px; color: #222; }
  .chart { max-width: 1000px; margin: 0 0 40px; }
  p { max-width: 1000px; }
</style>
</head>
<body>
<h1>Historical Borrowing is recency-weighted</h1>
<p><b>team-a</b> accrues alone in phase 1 (left of the first dashed line), then
stops. <b>team-b</b> accrues alone in phase 2 (between the dashed lines), then
stops. In phase 3 (right of the second line) both compete. By then team-a's
history has decayed for a whole phase while team-b's is fresh, so team-b carries
the larger remembered borrowing and team-a is favored for the contested capacity
at first.</p>
<div class="chart"><canvas id="hist"></canvas></div>
<div class="chart"><canvas id="res"></canvas></div>
<div class="chart"><canvas id="share"></canvas></div>
<div class="chart"><canvas id="cum"></canvas></div>
<script>
const DATA = __DATA__;
const PHASES = __PHASES__;
const labels = DATA.map(d => d.t.toFixed(0));
const pt = { pointRadius: 0, borderWidth: 2, tension: 0.2 };
const phaseLines = {
  id: 'phaseLines',
  afterDraw(chart) {
    const c = chart.ctx, a = chart.chartArea;
    PHASES.forEach(p => {
      const idx = labels.findIndex(l => Number(l) >= p);
      if (idx < 0) return;
      const px = chart.scales.x.getPixelForValue(idx);
      c.save();
      c.strokeStyle = '#888';
      c.setLineDash([5, 5]);
      c.beginPath();
      c.moveTo(px, a.top);
      c.lineTo(px, a.bottom);
      c.stroke();
      c.restore();
    });
  }
};
function mk(id, title, series) {
  new Chart(document.getElementById(id), {
    type: 'line',
    data: { labels: labels, datasets: series },
    options: {
      responsive: true,
      interaction: { intersect: false, mode: 'index' },
      plugins: { title: { display: true, text: title } },
      scales: { x: { title: { display: true, text: 'seconds' } } }
    },
    plugins: [phaseLines]
  });
}
mk('hist', 'Historical borrowing (decayed EMA, cpu cores)', [
  { label: 'team-a', data: DATA.map(d => d.aHist), borderColor: '#d62728', ...pt },
  { label: 'team-b', data: DATA.map(d => d.bHist), borderColor: '#1f77b4', ...pt }
]);
mk('res', 'Instantaneous reserving workloads', [
  { label: 'team-a', data: DATA.map(d => d.aReserving), borderColor: '#d62728', ...pt },
  { label: 'team-b', data: DATA.map(d => d.bReserving), borderColor: '#1f77b4', ...pt }
]);
mk('share', 'Weighted share (lower wins admission)', [
  { label: 'team-a', data: DATA.map(d => d.aShare), borderColor: '#d62728', ...pt },
  { label: 'team-b', data: DATA.map(d => d.bShare), borderColor: '#1f77b4', ...pt }
]);
mk('cum', 'Cumulative usage (cpu-seconds)', [
  { label: 'team-a', data: DATA.map(d => d.aCum), borderColor: '#d62728', ...pt },
  { label: 'team-b', data: DATA.map(d => d.bCum), borderColor: '#1f77b4', ...pt }
]);
</script>
</body>
</html>
`

const fairnessTemplate = `<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>Historical Borrowing: long-term fairness</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4"></script>
<style>
  body { font-family: system-ui, sans-serif; margin: 24px; color: #222; }
  .chart { max-width: 1000px; margin: 0 0 40px; }
  p { max-width: 1000px; }
</style>
</head>
<body>
<h1>Is max(instantaneous, EMA) long-term fair?</h1>
<p><b>hb-a</b> borrows alone for the whole first phase (left of the dashed line),
many half-lives long, building a large cumulative head start. In the second phase
<b>hb-b</b> also demands continuously. Because the EMA only remembers ~one
half-life of borrowing, hb-b gets a bounded make-up burst and then the two queues
split capacity evenly &mdash; so hb-a's head start is <b>never repaid</b>. The
cumulative-usage gap opens during phase 1 and then plateaus instead of closing.</p>
<div class="chart"><canvas id="cum"></canvas></div>
<div class="chart"><canvas id="gap"></canvas></div>
<div class="chart"><canvas id="res"></canvas></div>
<div class="chart"><canvas id="share"></canvas></div>
<script>
const DATA = __DATA__;
const PHASE1 = __PHASE1__;
const labels = DATA.map(d => d.t.toFixed(0));
const pt = { pointRadius: 0, borderWidth: 2, tension: 0.2 };
const phaseLine = {
  id: 'phaseLine',
  afterDraw(chart) {
    const idx = labels.findIndex(l => Number(l) >= PHASE1);
    if (idx < 0) return;
    const px = chart.scales.x.getPixelForValue(idx);
    const c = chart.ctx, a = chart.chartArea;
    c.save();
    c.strokeStyle = '#888';
    c.setLineDash([5, 5]);
    c.beginPath();
    c.moveTo(px, a.top);
    c.lineTo(px, a.bottom);
    c.stroke();
    c.restore();
  }
};
function mk(id, title, series) {
  new Chart(document.getElementById(id), {
    type: 'line',
    data: { labels: labels, datasets: series },
    options: {
      responsive: true,
      interaction: { intersect: false, mode: 'index' },
      plugins: { title: { display: true, text: title } },
      scales: { x: { title: { display: true, text: 'seconds' } } }
    },
    plugins: [phaseLine]
  });
}
mk('cum', 'Cumulative usage (cpu-seconds) - equal lines would mean long-term fair', [
  { label: 'hb-a', data: DATA.map(d => d.aCum), borderColor: '#d62728', ...pt },
  { label: 'hb-b', data: DATA.map(d => d.bCum), borderColor: '#1f77b4', ...pt }
]);
mk('gap', 'Cumulative usage gap hb-a - hb-b (cpu-seconds); stays > 0 = head start never repaid', [
  { label: 'gap', data: DATA.map(d => d.aCum - d.bCum), borderColor: '#9467bd', ...pt }
]);
mk('res', 'Instantaneous reserving workloads', [
  { label: 'hb-a', data: DATA.map(d => d.aReserving), borderColor: '#d62728', ...pt },
  { label: 'hb-b', data: DATA.map(d => d.bReserving), borderColor: '#1f77b4', ...pt }
]);
mk('share', 'Weighted share (lower wins admission)', [
  { label: 'hb-a', data: DATA.map(d => d.aShare), borderColor: '#d62728', ...pt },
  { label: 'hb-b', data: DATA.map(d => d.bShare), borderColor: '#1f77b4', ...pt }
]);
</script>
</body>
</html>
`

const htmlTemplate = `<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>Historical Borrowing Fair Sharing</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4"></script>
<style>
  body { font-family: system-ui, sans-serif; margin: 24px; color: #222; }
  .chart { max-width: 1000px; margin: 0 0 40px; }
  p { max-width: 1000px; }
</style>
</head>
<body>
<h1>Historical Borrowing Fair Sharing over time</h1>
<p><b>hb-a</b> borrows heavily on its own during the warm-up, building up historical
borrowing. After the dashed line <b>hb-b</b> joins with equal demand. Because hb-a's
remembered borrowing keeps its weighted share high, hb-b is prioritized for the free
capacity while hb-a's historical usage decays back down over time.</p>
<div class="chart"><canvas id="hist"></canvas></div>
<div class="chart"><canvas id="eff"></canvas></div>
<div class="chart"><canvas id="res"></canvas></div>
<div class="chart"><canvas id="share"></canvas></div>
<script>
const DATA = __DATA__;
const WARMUP = __WARMUP__;
const LENDABLE = __LENDABLE__;
const labels = DATA.map(d => d.t.toFixed(0));
const pt = { pointRadius: 0, borderWidth: 2, tension: 0.2 };
const warmupLine = {
  id: 'warmupLine',
  afterDraw(chart) {
    const idx = labels.findIndex(l => Number(l) >= WARMUP);
    if (idx < 0) return;
    const px = chart.scales.x.getPixelForValue(idx);
    const c = chart.ctx, a = chart.chartArea;
    c.save();
    c.strokeStyle = '#888';
    c.setLineDash([5, 5]);
    c.beginPath();
    c.moveTo(px, a.top);
    c.lineTo(px, a.bottom);
    c.stroke();
    c.restore();
  }
};
function mk(id, title, series) {
  new Chart(document.getElementById(id), {
    type: 'line',
    data: { labels: labels, datasets: series },
    options: {
      responsive: true,
      interaction: { intersect: false, mode: 'index' },
      plugins: { title: { display: true, text: title } },
      scales: { x: { title: { display: true, text: 'seconds' } } }
    },
    plugins: [warmupLine]
  });
}
mk('hist', 'Historical borrowing (decayed EMA, cpu cores)', [
  { label: 'hb-a', data: DATA.map(d => d.aHist), borderColor: '#d62728', ...pt },
  { label: 'hb-b', data: DATA.map(d => d.bHist), borderColor: '#1f77b4', ...pt }
]);
// Borrowing that actually feeds the DRS score, reconstructed from the weighted
// share (share = borrowing/lendable*1000). It equals max(instantaneous, EMA):
// it tracks live borrowing while a queue is active and falls back to the EMA
// once the queue stops borrowing, which is what keeps a recent borrower's share
// elevated. Dashed overlay is the EMA from the first chart for comparison.
mk('eff', 'Borrowing feeding DRS = max(instantaneous, EMA) (cpu cores)', [
  { label: 'hb-a (DRS)', data: DATA.map(d => d.aShare / 1000 * LENDABLE), borderColor: '#d62728', ...pt },
  { label: 'hb-b (DRS)', data: DATA.map(d => d.bShare / 1000 * LENDABLE), borderColor: '#1f77b4', ...pt },
  { label: 'hb-a (EMA)', data: DATA.map(d => d.aHist), borderColor: '#d62728', borderDash: [4, 4], ...pt },
  { label: 'hb-b (EMA)', data: DATA.map(d => d.bHist), borderColor: '#1f77b4', borderDash: [4, 4], ...pt }
]);
mk('res', 'Instantaneous reserving workloads', [
  { label: 'hb-a', data: DATA.map(d => d.aReserving), borderColor: '#d62728', ...pt },
  { label: 'hb-b', data: DATA.map(d => d.bReserving), borderColor: '#1f77b4', ...pt }
]);
mk('share', 'Weighted share (drives prioritization; lower wins admission)', [
  { label: 'hb-a', data: DATA.map(d => d.aShare), borderColor: '#d62728', ...pt },
  { label: 'hb-b', data: DATA.map(d => d.bShare), borderColor: '#1f77b4', ...pt }
]);
</script>
</body>
</html>
`
