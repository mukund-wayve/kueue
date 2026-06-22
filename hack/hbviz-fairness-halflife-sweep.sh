#!/usr/bin/env bash
# Sweep borrowingHalfLifeTime over the long-term fairness e2e spec against the
# running (colima/docker) kind cluster. k is held at 4. For each half-life it
# patches the deployed Kueue config, restarts the manager, runs only the
# fairness spec, and saves a half-life-labelled HTML.
set -o errexit
set -o pipefail
set -o nounset

cd "$(git rev-parse --show-toplevel)"

export PATH="$PWD/bin:/usr/local/go/bin:/opt/homebrew/bin:/usr/bin:/bin"

SWEEP_DIR="artifacts/hbviz-fairness-halflife-sweep"
mkdir -p "$SWEEP_DIR"

HALFLIVES=(10s 20s 60s)

# hb-a borrows alone in phase1, then both saturate in phase2.
export HBFAIR_PHASE1_SECONDS=20 HBFAIR_PHASE2_SECONDS=40

config_yaml() {
  local hl="$1"
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
    borrowingHalfLifeTime: ${hl}
    borrowingSamplingInterval: 1s
    borrowingHistoryWeight: 4.0
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

for hl in "${HALFLIVES[@]}"; do
  echo "=================================================================="
  echo "=== halflife=${hl}: patching config ==="
  tmp="$(mktemp)"
  config_yaml "$hl" > "$tmp"
  kubectl create configmap kueue-manager-config -n kueue-system \
    --from-file=controller_manager_config.yaml="$tmp" \
    --dry-run=client -o yaml | kubectl apply -f -
  rm -f "$tmp"

  echo "=== halflife=${hl}: restarting manager ==="
  kubectl rollout restart deploy/kueue-controller-manager -n kueue-system
  kubectl rollout status deploy/kueue-controller-manager -n kueue-system --timeout=120s
  sleep 8

  echo "=== halflife=${hl}: running fairness spec ==="
  run_dir="$PWD/$SWEEP_DIR/hl${hl}"
  mkdir -p "$run_dir"
  ARTIFACTS="$run_dir" bin/ginkgo run \
    --focus="is not long-term fair" \
    --timeout=15m -v \
    ./test/e2e/singlecluster/historicalborrowing/... 2>&1 | tee "$SWEEP_DIR/hl${hl}.log"

  cp "$run_dir/historical-borrowing-fairness.html" "$PWD/$SWEEP_DIR/fairness-hl${hl}.html"
  echo "=== halflife=${hl}: saved $SWEEP_DIR/fairness-hl${hl}.html ==="
done

echo "=================================================================="
echo "Sweep complete. HTML files:"
ls -la "$SWEEP_DIR"/fairness-hl*.html
