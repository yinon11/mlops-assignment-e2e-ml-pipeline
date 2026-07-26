#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

run_mini() {
  MSWEA_COST_TRACKING='ignore_errors' "$@" swebench-single \
      --subset verified \
      --split test \
      --model nebius/moonshotai/Kimi-K2.6 \
      --yolo \
      --cost-limit 0 \
      -i sympy__sympy-15599 \
      -o trajectory.json
}

if [[ -x "$ROOT/.venv/bin/mini-extra" ]]; then
  run_mini "$ROOT/.venv/bin/mini-extra"
elif command -v uv >/dev/null 2>&1; then
  run_mini uv run mini-extra
else
  run_mini mini-extra
fi
