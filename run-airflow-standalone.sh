#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")" && pwd)"
cd "$ROOT"
export PATH="${HOME}/.local/bin:${PATH}"

# Load NEBIUS_API_KEY and friends for worker tasks.
if [[ -f "$ROOT/.env" ]]; then
  set -a
  # shellcheck disable=SC1091
  source "$ROOT/.env"
  set +a
fi

export AIRFLOW_HOME="${AIRFLOW_HOME:-$HOME/airflow}"
export AIRFLOW__CORE__DAGS_FOLDER="$ROOT/dags"
export AIRFLOW__CORE__LOAD_EXAMPLES=false
export AIRFLOW__CORE__EXECUTOR=LocalExecutor
export PYTHONPATH="$ROOT${PYTHONPATH:+:$PYTHONPATH}"
export MLFLOW_TRACKING_URI="${MLFLOW_TRACKING_URI:-sqlite:///${ROOT}/mlflow.db}"
export MLFLOW_EXPERIMENT_NAME="${MLFLOW_EXPERIMENT_NAME:-evaluate-agent}"
export MLFLOW_BACKEND_STORE_URI="${MLFLOW_BACKEND_STORE_URI:-sqlite:///${ROOT}/mlflow.db}"

mkdir -p "$AIRFLOW_HOME"
echo '{"admin": "admin"}' > "$AIRFLOW_HOME/simple_auth_manager_passwords.json.generated"

# Extra deps so tasks can log to MLflow, upload to S3, and use DockerOperator.
AIRFLOW_CMD="uv tool run --with mlflow --with boto3 --with apache-airflow-providers-docker apache-airflow standalone"

# Ensure docker group is available for SWE-bench containers when possible.
if groups | grep -q '\bdocker\b'; then
  exec $AIRFLOW_CMD
else
  exec sg docker -c "$AIRFLOW_CMD"
fi
