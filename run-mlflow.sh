#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "$0")" && pwd)"
cd "$ROOT"
export PATH="${HOME}/.local/bin:${PATH}"
PORT="${MLFLOW_PORT:-5000}"
BACKEND_URI="${MLFLOW_BACKEND_STORE_URI:-sqlite:///${ROOT}/mlflow.db}"
ARTIFACT_ROOT="${MLFLOW_ARTIFACT_ROOT:-${ROOT}/mlartifacts}"
mkdir -p "$ARTIFACT_ROOT"
echo "Starting MLflow UI on http://127.0.0.1:${PORT}"
echo "  backend: ${BACKEND_URI}"
echo "  artifacts: ${ARTIFACT_ROOT}"
exec uv run mlflow ui --host 127.0.0.1 --port "$PORT" \
  --backend-store-uri "$BACKEND_URI" \
  --default-artifact-root "$ARTIFACT_ROOT"
