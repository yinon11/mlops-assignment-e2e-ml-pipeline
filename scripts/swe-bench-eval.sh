#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

DATASET_NAME="${DATASET_NAME:-princeton-nlp/SWE-bench_Verified}"
PREDICTIONS_PATH="${PREDICTIONS_PATH:-trajectories/preds.json}"
MAX_WORKERS="${MAX_WORKERS:-5}"
RUN_ID="${RUN_ID:-test}"

run_eval() {
  "$@" -m swebench.harness.run_evaluation \
      --dataset_name "$DATASET_NAME" \
      --predictions_path "$PREDICTIONS_PATH" \
      --max_workers "$MAX_WORKERS" \
      --run_id "$RUN_ID"
}

if [[ -x "$ROOT/.venv/bin/python" ]]; then
  run_eval "$ROOT/.venv/bin/python"
elif command -v uv >/dev/null 2>&1; then
  run_eval uv run python
else
  run_eval python
fi
