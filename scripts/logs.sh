#!/usr/bin/env bash

# Fetches Spark logs, in case we run in CLUSTER mode


# Show a Spark driver pod's application output (println/show/BENCH), with the
# Spark INFO/WARN log noise stripped. In cluster mode results go to the driver
# pod, not to spark-submit's console.
#
#   ./scripts/logs.sh           # latest driver pod
#   ./scripts/logs.sh q2df      # latest driver whose name matches "q2df"
#   ./scripts/logs.sh -r        # raw logs (no filtering), latest driver
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."
source scripts/env.sh

RAW=false
[[ ${1:-} == -r ]] && { RAW=true; shift; }
MATCH=${1:-}

PODS=$(kubectl get pods --sort-by=.metadata.creationTimestamp -o name 2>/dev/null | grep -- '-driver')
[[ -n "$MATCH" ]] && PODS=$(echo "$PODS" | grep -- "$MATCH" || true)
P=$(echo "$PODS" | tail -1)
[[ -n "$P" ]] || { echo "No matching driver pod found."; exit 1; }

echo "==> $P"
if $RAW; then
  kubectl logs "$P"
else
  # Drop standard Spark log lines; keep application stdout.
  kubectl logs "$P" 2>&1 | grep -vE "^[0-9]{2}/[0-9]{2}/[0-9]{2} [0-9:]+ (INFO|WARN|ERROR) "
fi
