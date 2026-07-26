# REPORT — End-to-end coding-agent evaluation pipeline

**Course**: Nebius Academy — AI Performance Engineering, MLOps module  
**Assignment**: [mlops-assignment-e2e-ml-pipeline](https://github.com/minotru/mlops-assignment-e2e-ml-pipeline)  
**Mode**: Easy-mode (subprocess tasks + Airflow standalone + local MLflow)

## Architecture

```
Airflow DAG evaluate_agent
  prepare_run  ->  run_agent  ->  run_eval  ->  summarize_and_log
       |              |              |                 |
       v              v              v                 v
  runs/<id>/     mini-swe-agent   SWE-bench       metrics.json
  config.json    -> preds.json    harness         + MLflow
                 + trajectories   -> logs/reports
```

Helpers live in `pipeline/run_helpers.py`. The DAG is `dags/evaluate_agent.py`.  
A CLI mirror (same helpers) is `scripts/run_evaluate_pipeline.py` for debugging without the UI.

**Configurable Airflow params**: `split`, `subset`, `workers`, `model`, `task_slice`, `run_id`, `cost_limit`.

## VM setup

| Item | Value |
|------|--------|
| Instance | `mlops-e2e-pipeline` (`computeinstance-e00x1468v6hhn2k2dw`) |
| Platform | `cpu-d3` / `8vcpu-32gb`, Ubuntu 24.04 driverless |
| Public IP | `89.169.112.133` |
| Project path | `~/work/mlops-assignment-e2e-ml-pipeline` |

SSH + port forwards:

```bash
ssh -i ~/.ssh/nebius_mlops \
  -L 8080:localhost:8080 \
  -L 5000:localhost:5000 \
  ubuntu@89.169.112.133
```

Then open Airflow at http://localhost:8080 and MLflow at http://localhost:5000.

## How to trigger

### 1) Airflow UI / CLI

```bash
cd ~/work/mlops-assignment-e2e-ml-pipeline
# terminals (separate):
./run-mlflow.sh
./run-airflow-standalone.sh

# trigger
airflow dags unpause evaluate_agent
airflow dags trigger evaluate_agent --conf '{
  "split": "test",
  "subset": "verified",
  "workers": 1,
  "model": "nebius/moonshotai/Kimi-K2.6",
  "task_slice": "0:1",
  "run_id": "my-run",
  "cost_limit": 0
}'
```

Standalone SimpleAuthManager credentials (this VM): user `admin` / password `admin`.

### 2) CLI (same pipeline as the DAG)

```bash
set -a; source .env; set +a
export MLFLOW_TRACKING_URI="sqlite:///$PWD/mlflow.db"
sg docker -c 'uv run python scripts/run_evaluate_pipeline.py \
  --subset verified --split test --workers 1 \
  --task-slice 0:1 --run-id demo-slice-0-1'
```

## Artifact layout

```
runs/<run-id>/
  config.json
  metrics.json
  manifest.json
  run-agent/
    preds.json
    trajectories/
  run-eval/
    logs/
    reports/
```

`manifest.json` points at the important files. Easy-mode keeps artifacts local; remote Object Storage upload is documented in the manifest (`remote_artifact_uri: null`) and would be a natural next step (`aws s3 sync runs/<run-id> s3://...` then log the URI to MLflow).

## Completed runs

Both used model `nebius/moonshotai/Kimi-K2.6`, SWE-bench Verified / test / slice `0:1`, instance `astropy__astropy-12907`.

| Run id | How | Resolved | Artifacts |
|--------|-----|----------|-----------|
| `demo-slice-0-1` | CLI (`scripts/run_evaluate_pipeline.py`) | **1 / 1** | `runs/demo-slice-0-1/` |
| `airflow-demo-0-1` | Airflow DAG `evaluate_agent` (`manual__2026-07-25T11:32:28.705836+00:00`, state **success**) | **1 / 1** | `runs/airflow-demo-0-1/` |

MLflow experiment: `evaluate-agent` (SQLite backend `mlflow.db` — required by MLflow 3.x).  
Example MLflow run id from CLI: `c64283edd6724447ae9348ad1666186f`.

## Rerun by run-id

1. Re-trigger with the same `run_id` (or copy `runs/<run-id>/config.json` params).
2. Agent outputs land in `runs/<run-id>/run-agent/`; evaluation in `run-eval/`.
3. Compare runs in MLflow UI (experiment `evaluate-agent`) by `model`, `task_slice`, `resolve_rate`.

To reuse predictions only:

```bash
uv run python scripts/run_evaluate_pipeline.py --skip-agent --run-id demo-slice-0-1
```

## Notes / trade-offs

- **Easy-mode**: Airflow tasks call `uv run mini-extra` / `swebench` via subprocess (not `DockerOperator`). Execution is still isolated per SWE-bench instance container.
- Batch CLI does not accept `--cost-limit` (only `swebench-single` does); the param is still logged to MLflow for provenance.
- Production-style follow-ups: `DockerOperator` + `docker-compose` for Airflow/MLflow, and S3 upload of `runs/<run-id>/`.
