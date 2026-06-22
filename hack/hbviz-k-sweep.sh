#!/usr/bin/env bash
# Sweep borrowingHistoryWeight (k) over the recency e2e spec against an already
# running kind cluster. For each k it patches the deployed Kueue config,
# restarts the manager, runs only the recency spec, and saves a k-labelled HTML.
set -o errexit
set -o pipefail
set -o nounset

cd "$(git rev-parse --show-toplevel)"

export PATH="/tmp/hbviz-bin:/opt/homebrew/bin:$PATH"
export KIND_EXPERIMENTAL_PROVIDER=podman

SWEEP_DIR="artifacts/hbviz-k-sweep"
mkdir -p "$SWEEP_DIR"

KS=(1 4 9)

# Shorten the phases so each run is quick but the EMA (half-life 10s) still
# reaches near steady state within a 20s phase.
export HBREC_PHASE1_SECONDS=20 HBREC_PHASE2_SECONDS=20 HBREC_PHASE3_SECONDS=20

config_yaml() {
  local k="$1"
  cat <<EOF
apiVersion: config.kueue.x-k8s.io/v1beta2
kind: Configuration
metrics:
  enableClusterQueueResources: true
leaderElection:
  leaderElect: true
controller:
  groupKindConcurrency:
    Job.batch: 5
    Pod: 5
    Workload.kueue.x-k8s.io: 5
    LocalQueue.kueue.x-k8s.io: 1
    ClusterQueue.kueue.x-k8s.io: 1
    ResourceFlavor.kueue.x-k8s.io: 1
fairSharing:
  preemptionStrategies: ["LessThanOrEqualToFinalShare", "LessThanInitialShare"]
  strategy: HistoricalBorrowing
  historicalBorrowing:
    borrowingHalfLifeTime: 10s
    borrowingSamplingInterval: 1s
    borrowingHistoryWeight: ${k}.0
clientConnection:
  qps: 50
  burst: 100
integrations:
  frameworks:
  - "batch/job"
featureGates:
  HistoricalBorrowingFairSharing: true
EOF
}

for k in "${KS[@]}"; do
  echo "=================================================================="
  echo "=== k=${k}: patching config ==="
  tmp="$(mktemp)"
  config_yaml "$k" > "$tmp"
  kubectl create configmap kueue-manager-config -n kueue-system \
    --from-file=controller_manager_config.yaml="$tmp" \
    --dry-run=client -o yaml | kubectl apply -f -
  rm -f "$tmp"

  echo "=== k=${k}: restarting manager ==="
  kubectl rollout restart deploy/kueue-controller-manager -n kueue-system
  kubectl rollout status deploy/kueue-controller-manager -n kueue-system --timeout=120s
  # Give the leader election + cache a moment to settle before driving load.
  sleep 8

  echo "=== k=${k}: running recency spec ==="
  run_dir="$PWD/$SWEEP_DIR/k${k}"
  mkdir -p "$run_dir"
  ARTIFACTS="$run_dir" bin/ginkgo run \
    --focus="recency-weighted" \
    --timeout=15m -v \
    ./test/e2e/singlecluster/historicalborrowing/... 2>&1 | tee "$SWEEP_DIR/k${k}.log"

  cp "$run_dir/historical-borrowing-recency.html" "$PWD/$SWEEP_DIR/recency-k${k}.html"
  echo "=== k=${k}: saved $SWEEP_DIR/recency-k${k}.html ==="
done

echo "=================================================================="
echo "Sweep complete. HTML files:"
ls -la "$SWEEP_DIR"/recency-k*.html
