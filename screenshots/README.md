# Screenshots

Add grading evidence here after bringing the stack up:

| File | What to capture |
|------|-----------------|
| `airflow_dag.png` | Airflow UI showing `evaluate_agent` graph with a successful run |
| `mlflow_runs.png` | MLflow experiment `evaluate-agent` with logged params/metrics |

```bash
# Compose path
docker compose up -d
# then open :8080 and :5000 via SSH tunnels if on a VM
```
