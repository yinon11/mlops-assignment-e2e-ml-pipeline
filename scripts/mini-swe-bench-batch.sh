#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

SUBSET="${SUBSET:-verified}"
SPLIT="${SPLIT:-test}"
MODEL="${MODEL:-nebius/moonshotai/Kimi-K2.6}"
TASK_SLICE="${TASK_SLICE:-0:3}"
WORKERS="${WORKERS:-5}"
OUTPUT_DIR="${OUTPUT_DIR:-trajectories}"

CONFIG_ARGS=()
if [[ -f "$ROOT/mini-swe-agent/src/minisweagent/config/benchmarks/swebench.yaml" ]]; then
  CONFIG_ARGS=(--config "$ROOT/mini-swe-agent/src/minisweagent/config/benchmarks/swebench.yaml")
fi

run_mini() {
  # Note: --cost-limit is only on swebench-single; batch uses config defaults.
  MSWEA_COST_TRACKING='ignore_errors' "$@" swebench \
      --subset "$SUBSET" \
      --split "$SPLIT" \
      --model "$MODEL" \
      --slice "$TASK_SLICE" \
      "${CONFIG_ARGS[@]}" \
      --workers "$WORKERS" \
      -o "$OUTPUT_DIR"
}

if [[ -x "$ROOT/.venv/bin/mini-extra" ]]; then
  run_mini "$ROOT/.venv/bin/mini-extra"
elif command -v uv >/dev/null 2>&1; then
  run_mini uv run mini-extra
else
  run_mini mini-extra
fi
