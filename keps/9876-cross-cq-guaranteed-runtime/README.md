# KEP-9876: Preemption Protection — Guaranteed Minimum Runtime

<!-- toc -->
- [Summary](#summary)
- [Motivation](#motivation)
  - [Goals](#goals)
  - [Non-Goals](#non-goals)
- [Proposal](#proposal)
  - [User Stories](#user-stories)
    - [Story 1: Guaranteeing Minimum Runtime for GPU Training Jobs](#story-1-guaranteeing-minimum-runtime-for-gpu-training-jobs)
    - [Story 2: Grace Period for Nominal Quota Reclaim](#story-2-grace-period-for-nominal-quota-reclaim)
    - [Story 3: Protecting Opportunistically Admitted Workloads Within a ClusterQueue](#story-3-protecting-opportunistically-admitted-workloads-within-a-clusterqueue)
  - [Notes/Constraints/Caveats](#notesconstraintscaveats)
  - [Risks and Mitigations](#risks-and-mitigations)
- [Design Details](#design-details)
  - [API Changes](#api-changes)
    - [Cross-CQ Protection (Configuration)](#cross-cq-protection-configuration)
    - [Within-CQ Protection (ClusterQueuePreemption)](#within-cq-protection-clusterqueuepreemption)
  - [Admission Time](#admission-time)
  - [Preemption Eligibility](#preemption-eligibility)
  - [Validation](#validation)
  - [Test Plan](#test-plan)
    - [Unit Tests](#unit-tests)
    - [Integration tests](#integration-tests)
  - [Graduation Criteria](#graduation-criteria)
    - [Alpha](#alpha)
    - [Beta](#beta)
    - [GA](#ga)
- [Implementation History](#implementation-history)
- [Drawbacks](#drawbacks)
- [Alternatives](#alternatives)
  - [Rely on gracefulTerminationPeriod or terminationGracePeriodSeconds](#rely-on-gracefulterminationperiod-or-terminationgraceperiodseconds)
  - [Per-ClusterQueue Cross-CQ Configuration](#per-clusterqueue-cross-cq-configuration)
  - [Per-Workload Overrides](#per-workload-overrides)
  - [Separate Duration for Each Reclaim Type](#separate-duration-for-each-reclaim-type)
  - [Rely on Within-ClusterQueue Time-Based Preemption Alone](#rely-on-within-clusterqueue-time-based-preemption-alone)
  - [Scatter Fields Across Configuration](#scatter-fields-across-configuration)
  - [List-Based preemptionProtection API](#list-based-preemptionprotection-api)
<!-- /toc -->

## Summary

This KEP introduces preemption protection: configuration fields that allow
administrators to guarantee a minimum runtime before workloads become eligible
for preemption. Protection is supported for both cross-ClusterQueue preemption
(fair sharing rebalancing and reclaim) and within-ClusterQueue preemption.

## Motivation

Kueue can preempt workloads for several reasons: fair sharing rebalancing
across a cohort, nominal quota reclamation by an owner ClusterQueue, and
within-ClusterQueue priority enforcement. In all cases, a workload that was
just admitted may be preempted before it has had enough time to make meaningful
progress — for example, before reaching a useful checkpoint or completing a
meaningful unit of work.

Administrators need a way to guarantee a minimum runtime so that preemption
only targets workloads that have already had a reasonable opportunity to make
progress. Because different preemption types carry different strengths of
claim, administrators may want different thresholds — for example, a longer
protection window for fair sharing (where neither side has priority) and a
shorter one for reclaim (where the owner has a legitimate entitlement).

This is analogous to Slurm's `PreemptExemptTime`, which provides a guaranteed
minimum runtime before a job becomes preemptible.

### Goals

- Allow administrators to configure a global minimum runtime guarantee for
  workloads before they become eligible for fair sharing preemption
- Allow administrators to independently configure a global minimum runtime
  guarantee for workloads before they become eligible for cross-CQ reclaim
  preemption (nominal quota reclaim and reclaim while borrowing)
- Allow administrators to configure per-ClusterQueue minimum runtime
  guarantees for within-CQ preemption, with separate thresholds for
  incumbent and opportunistic workloads
- Maintain backward compatibility: existing configurations without these
  settings behave exactly as before

### Non-Goals

- Per-CQ overrides for cross-CQ durations (deferred to future iterations)
- Per-workload minimum runtime overrides
- Workload priority that decays over time based on runtime
- Precise CPU/GPU time accounting

## Proposal

This KEP adds preemption protection at two levels:

1. **Cross-CQ protection** (global, on Configuration): protects workloads
   from fair sharing rebalancing and nominal quota reclamation across
   ClusterQueues within a cohort.
2. **Within-CQ protection** (per-CQ, on ClusterQueuePreemption): protects
   workloads from preemption by other workloads within the same
   ClusterQueue, with separate thresholds for incumbent and opportunistic
   workloads.

Both are gated behind the `PreemptionProtection` feature gate and use the
workload's `Admitted` condition timestamp for timing.

### User Stories

#### Story 1: Guaranteeing Minimum Runtime for GPU Training Jobs

As a cluster administrator managing GPU resources shared across multiple teams
via ClusterQueues in a cohort, I want to ensure that when fair sharing
preemption rebalances resources between ClusterQueues, the affected workloads
have had enough time to reach a useful checkpoint.

By setting `preemptionProtection.fairSharing.minAdmitDuration: 2h` in the
Configuration, workloads are guaranteed at least 2 hours of runtime before
they can be preempted by fair sharing rebalancing. Separately, setting
`preemptionProtection.reclaimWithinCohort.minAdmitDuration: 10m` gives
borrowing workloads a short grace period when the owner CQ reclaims its quota.

#### Story 2: Grace Period for Nominal Quota Reclaim

As a cluster administrator, I want to give borrowing workloads a short grace
period before a ClusterQueue reclaims its own nominal quota, so that workloads
are not killed immediately when the owner's demand increases.

By setting `preemptionProtection.reclaimWithinCohort.minAdmitDuration: 10m`,
borrowing workloads get at least 10 minutes to make progress before they can
be reclaimed. This is shorter than the fair sharing protection because the
owner has a legitimate entitlement to its quota.

#### Story 3: Protecting Opportunistically Admitted Workloads Within a ClusterQueue

As a platform team running ML training workloads with BestEffortFIFO, I want
to guarantee that workloads which were admitted opportunistically (queued
after the preemptor but admitted first because they fit) get enough time to
checkpoint before being preempted by a same-priority peer.

By setting `preemptionProtection.withinClusterQueue.opportunisticMinAdmitDuration: 30m`
on a ClusterQueue, opportunistically admitted workloads get at least 30 minutes
of uninterrupted runtime.

### Notes/Constraints/Caveats

This feature is complementary to the `kueue-priority-booster` experimental
controller and the `SchedulerTimestampPreemptionBuffer` feature gate.
`kueue-priority-booster` implements time-sharing via priority annotation
manipulation, while this KEP provides first-class API support for minimum
runtime guarantees. The `SchedulerTimestampPreemptionBuffer` adds a 5-minute
buffer to prevent near-simultaneous workloads from preempting each other;
preemption protection provides an administrator-configurable, longer-term
guarantee.

**Interaction with Fair Sharing strategies**: Workloads that have not yet
reached their `minAdmitDuration` are removed from the preemption candidate
set. This applies regardless of which fair sharing strategy is configured.

**Re-admission after preemption**: If a workload is preempted and later
re-admitted, the protection timer resets based on the new admission time.

### Risks and Mitigations

- **Risk**: If `minAdmitDuration` is set too high for fair sharing, resources
  may remain imbalanced across the cohort for extended periods. **Mitigation**:
  document recommended ranges; administrators can set different durations for
  fair sharing vs. reclaim.
- **Risk**: Within-CQ protection could delay higher-priority workloads if
  `minAdmitDuration` is large. **Mitigation**: administrators must opt in via
  the feature gate and explicitly configure durations per ClusterQueue.

## Design Details

### API Changes

#### Cross-CQ Protection (Configuration)

Cross-CQ protection is configured globally in the Kueue `Configuration`
resource. The fields are grouped under a `preemptionProtection` struct,
keeping related rules together and providing a clear extensibility point.

```go
// In apis/config/v1beta2/configuration_types.go

type Configuration struct {
	// ... existing fields ...

	// preemptionProtection configures minimum runtime guarantees that
	// protect admitted workloads from cross-ClusterQueue preemption.
	// +optional
	PreemptionProtection *CrossCQPreemptionProtection `json:"preemptionProtection,omitempty"`
}

type CrossCQPreemptionProtection struct {
	// fairSharing configures protection from fair sharing rebalancing
	// preemption (InCohortFairSharing). Only applies when fair sharing
	// is enabled.
	// +optional
	FairSharing *FairSharingProtection `json:"fairSharing,omitempty"`

	// reclaimWithinCohort configures protection from cross-CQ reclaim
	// preemption (InCohortReclamation and InCohortReclaimWhileBorrowing).
	// Applies in both classical and fair sharing preemption modes.
	// +optional
	ReclaimWithinCohort *ReclaimProtection `json:"reclaimWithinCohort,omitempty"`
}

type FairSharingProtection struct {
	// minAdmitDuration is the minimum time a workload must be admitted
	// before it becomes eligible for fair sharing preemption. A workload
	// admitted less than this duration ago is skipped as a preemption
	// candidate. When nil, no minimum is enforced.
	// +optional
	MinAdmitDuration *metav1.Duration `json:"minAdmitDuration,omitempty"`
}

type ReclaimProtection struct {
	// minAdmitDuration is the minimum time a workload must be admitted
	// before it becomes eligible for cross-CQ reclaim preemption. A
	// workload admitted less than this duration ago is skipped as a
	// preemption candidate. When nil, no minimum is enforced.
	// +optional
	MinAdmitDuration *metav1.Duration `json:"minAdmitDuration,omitempty"`
}
```

YAML example:

```yaml
apiVersion: config.kueue.x-k8s.io/v1beta2
kind: Configuration
preemptionProtection:
  fairSharing:
    minAdmitDuration: 1h
  reclaimWithinCohort:
    minAdmitDuration: 10m
fairSharing:
  preemptionStrategies:
    - LessThanOrEqualToFinalShare
    - LessThanInitialShare
```

#### Within-CQ Protection (ClusterQueuePreemption)

Within-CQ protection is configured per-ClusterQueue on the
`ClusterQueuePreemption` struct. This allows different ClusterQueues to have
different protection policies.

```go
// In apis/kueue/v1beta2/clusterqueue_types.go

type ClusterQueuePreemption struct {
	// ... existing fields (reclaimWithinCohort, borrowWithinCohort, withinClusterQueue) ...

	// preemptionProtection configures minimum runtime guarantees that
	// protect admitted workloads from preemption within this ClusterQueue.
	// Only valid when withinClusterQueue is not Never.
	// +optional
	PreemptionProtection *WithinCQPreemptionProtection `json:"preemptionProtection,omitempty"`
}

type WithinCQPreemptionProtection struct {
	// withinClusterQueue configures protection for workloads preempted
	// within the same ClusterQueue.
	// +optional
	WithinClusterQueue *WithinCQProtection `json:"withinClusterQueue,omitempty"`
}

type WithinCQProtection struct {
	// minAdmitDuration is the minimum time an incumbent workload (one
	// that was queued before the preemptor) must be admitted before it
	// becomes eligible for preemption within the ClusterQueue.
	// When nil, no minimum is enforced for incumbent workloads.
	// +optional
	MinAdmitDuration *metav1.Duration `json:"minAdmitDuration,omitempty"`

	// opportunisticMinAdmitDuration is the minimum time an opportunistic
	// workload (one that was queued after the preemptor but admitted first,
	// e.g., via BestEffortFIFO) must be admitted before it becomes eligible
	// for preemption. When nil, no minimum is enforced for opportunistic
	// workloads.
	// +optional
	OpportunisticMinAdmitDuration *metav1.Duration `json:"opportunisticMinAdmitDuration,omitempty"`
}
```

YAML example:

```yaml
apiVersion: kueue.x-k8s.io/v1beta2
kind: ClusterQueue
metadata:
  name: team-a
spec:
  preemption:
    withinClusterQueue: LowerOrNewerEqualPriority
    preemptionProtection:
      withinClusterQueue:
        minAdmitDuration: 20m
        opportunisticMinAdmitDuration: 10m
```

### Admission Time

Protection timing is based on the `LastTransitionTime` of the workload's
`Admitted` condition (when `Status=True`). This is when the workload actually
starts running, not when quota is reserved.

Using `Admitted` rather than `QuotaReserved` is important because:
- In 2-phase admission, AdmissionChecks (e.g., ProvisioningRequest for
  Cluster Autoscaler) can take significant time between QuotaReserved and
  Admitted.
- In 2-pass scheduling (e.g., TAS), the workload may get QuotaReserved
  in the first pass but only becomes Admitted after topology assignment in
  the second pass.

If the `Admitted` condition is not yet set, the workload is not protected
(it hasn't started running yet and cannot have made progress).

### Preemption Eligibility

When the `PreemptionProtection` feature gate is enabled, preemption candidates
are filtered based on their preemption type and the corresponding duration:

- **`InCohortFairSharing`** candidates: filtered by
  `Configuration.preemptionProtection.fairSharing.minAdmitDuration`. Only
  applies when fair sharing is enabled.
- **`InCohortReclamation`** and **`InCohortReclaimWhileBorrowing`**
  candidates: filtered by
  `Configuration.preemptionProtection.reclaimWithinCohort.minAdmitDuration`.
  Applies in both classical and fair sharing preemption paths.
- **`InClusterQueue`** candidates: filtered by the ClusterQueue's
  `preemptionProtection.withinClusterQueue.minAdmitDuration` (for incumbent
  workloads) or `opportunisticMinAdmitDuration` (for opportunistic
  workloads).

If no eligible candidates remain after filtering for a given preemption type,
that type of preemption is skipped for the current scheduling cycle.

### Validation

- All duration fields are optional (`nil` means no protection).
- No minimum duration is enforced — any positive duration is valid.
- Within-CQ `preemptionProtection` requires `withinClusterQueue` to not be
  `Never`.
- Cross-CQ `preemptionProtection.fairSharing` is only effective when fair
  sharing is enabled in the Configuration.
- All fields are gated behind the `PreemptionProtection` feature gate.
  Setting them when the gate is disabled is rejected by validation.

### Test Plan

[x] I/we understand the owners of the involved components may require updates to
existing tests to make this code solid enough prior to committing the changes
necessary to implement this enhancement.

#### Unit Tests

- `pkg/scheduler/preemption`: Test preemption candidate filtering:
  - Cross-CQ: recently admitted candidates skipped for `InCohortFairSharing`
    when `fairSharing.minAdmitDuration` is set; candidates beyond the
    duration are eligible
  - Cross-CQ: recently admitted candidates skipped for
    `InCohortReclamation` when `reclaimWithinCohort.minAdmitDuration` is set
  - Within-CQ: incumbent workloads protected by `minAdmitDuration`
  - Within-CQ: opportunistic workloads protected by
    `opportunisticMinAdmitDuration`
  - When all candidates for a given type are protected, that type of
    preemption does not occur
  - When a duration is `nil`, no filtering occurs for that type
- `pkg/config`: Test validation for both cross-CQ and within-CQ fields
- `pkg/webhooks`: Test ClusterQueue webhook validation

#### Integration tests

1. Two ClusterQueues in a cohort with fair sharing enabled; fair sharing
   preemption candidates protected within `minAdmitDuration`
2. Nominal quota reclaim candidates protected within
   `reclaimWithinCohort.minAdmitDuration`
3. Within-CQ preemption: incumbent workloads protected
4. Within-CQ preemption: opportunistic workloads protected
5. One field set, other `nil`: only the configured preemption type is filtered
6. All fields `nil`: existing behavior preserved
7. Controller restart: workload timestamps persist correctly

### Graduation Criteria

#### Alpha

- Feature behind `PreemptionProtection` feature gate (disabled by default)
- Cross-CQ protection fields on Configuration
- Within-CQ protection fields on ClusterQueuePreemption
- Preemption logic updated to filter candidates based on admission time
- Unit and integration tests

#### Beta

- Feature gate enabled by default
- E2E tests
- Documentation and tuning guidance

#### GA

- Feature gate removed
- Stable for at least 2 releases
- All reported bugs addressed

## Implementation History

- 2026-03-15: Initial KEP proposal (cross-CQ only)
- 2026-05-26: Revised KEP incorporating reviewer feedback; expanded scope
  to include within-CQ protection; unified API under `preemptionProtection`

## Drawbacks

- Adds configuration knobs to the preemption system at two levels
- If protection durations are set too high, preemption becomes less
  effective at rebalancing resources or enforcing priority

## Alternatives

### Rely on gracefulTerminationPeriod or terminationGracePeriodSeconds

Kubernetes' `terminationGracePeriodSeconds` on Pods controls how long the
kubelet waits between SIGTERM and SIGKILL — it governs shutdown behavior
*after* a preemption decision has been made. Preemption protection, by
contrast, governs *eligibility* for preemption: whether a workload can be
chosen as a preemption candidate at all. These are complementary mechanisms.
Kueue does not have a `gracefulTerminationPeriod` field; the closest
concept is `WaitForPodsReady.Timeout`, which controls eviction of workloads
that fail to become ready and is unrelated to preemption timing.

### Per-ClusterQueue Cross-CQ Configuration

Instead of global duration fields on Configuration, each ClusterQueue could
define its own cross-CQ protection durations. While this offers finer-grained
control, it multiplies interacting knobs and makes it harder to reason about
fair sharing behavior. Global values keep the system predictable. Per-CQ
overrides can be added in future iterations under the same
`preemptionProtection` struct.

### Per-Workload Overrides

A per-workload `minAdmitDuration` override (e.g., via an annotation) was
considered but deferred. Allowing individual users to set their own minimum
duration introduces the risk of gaming the system by requesting arbitrarily
long protection windows. Mitigating this would require additional policy
controls. This may be revisited if demand arises.

### Separate Duration for Each Reclaim Type

A third cross-CQ field for `InCohortReclaimWhileBorrowing` could give
administrators independent control over all three cross-CQ preemption types.
However, three duration knobs is too many to reason about. Grouping both
reclaim types under a single `reclaimWithinCohort` configuration keeps the
surface manageable.

### Rely on Within-ClusterQueue Time-Based Preemption Alone

The `kueue-priority-booster` experimental controller implements time-sharing
via priority annotation manipulation. However, it is not a first-class API
and requires deploying a separate controller. This KEP provides native
support integrated directly into the preemption logic.

### Scatter Fields Across Configuration

The original KEP proposal placed `fairSharing.minAdmitDuration` inside the
existing `FairSharing` struct and `reclaimMinAdmitDuration` at the top level
of Configuration. Grouping them under a dedicated `preemptionProtection`
struct keeps related rules together, avoids scattering configuration, and
provides a clear place for future extensions.

### List-Based preemptionProtection API

A list-based API mapping protection rules to preemption reason enums was
considered:

```yaml
preemptionProtection:
  - reason: InCohortFairSharing
    minAdmitDuration: 1h
  - reason: InCohortReclaim
    minAdmitDuration: 30min
```

While maximally extensible, this feels over-engineered for 2-3 entries and
makes validation and configuration merging more complex. The struct-based
approach was preferred for clarity.
