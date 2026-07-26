# Screenshots

Grading evidence, captured from a local run of the full stack (Airflow standalone + MLflow server, SQLite backend).

| File | What it shows |
|------|---------------|
| `airflow_dag.png` | Airflow graph view of the `evaluate_agent` run `local-demo-0-1`: all four tasks (`prepare_run → run_agent → run_eval → summarize_and_log`) green, run state **Success**, duration 00:03:22 |
| `mlflow_runs.png` | MLflow experiment `evaluate-agent` with three runs (`local-demo-0-1`, `airflow-demo-0-1`, `demo-slice-0-1`) and their logged metrics (`resolve_rate`, `resolved_instances`, `unresolved_instances`) |
| `airflow_dag_docker.png` | Run `docker-demo-0-1` with `use_docker=true`: DockerOperator tasks (`run_agent_docker`, `run_eval_docker`) green, subprocess tasks skipped, run **Success** in 00:02:38 |
| `object_storage_artifacts.png` | MinIO console: bucket `mlops-runs` with the `s3-demo-0-1/` run folder (14 objects) uploaded by `upload_run_to_s3` |

Reproduce with:

```bash
./run-mlflow.sh          # MLflow UI on :5000
./run-airflow-standalone.sh  # Airflow UI on :8080
# trigger the DAG, e.g.:
uv tool run apache-airflow dags trigger evaluate_agent \
  --conf '{"task_slice":"0:1","run_id":"local-demo-0-1","workers":1}'
```
