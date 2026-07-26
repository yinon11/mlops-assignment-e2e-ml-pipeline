"""Configurable Airflow DAG: prepare_run -> run_agent -> run_eval -> summarize_and_log."""

from __future__ import annotations

import json
import os
import sys
from datetime import datetime, timedelta
from pathlib import Path

from airflow.decorators import dag, task
from airflow.models.param import Param

PROJECT_ROOT = Path(os.environ.get("MLOPS_PROJECT_ROOT") or Path(__file__).resolve().parents[1])
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from pipeline.run_helpers import (  # noqa: E402
    build_run_config,
    collect_metrics,
    log_mlflow_run,
    prepare_run_dir,
    run_agent_batch,
    run_swebench_eval,
    write_manifest,
)


@dag(
    dag_id="evaluate_agent",
    description="Run mini-swe-agent on a SWE-bench slice, evaluate, and log to MLflow",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["mlops", "swe-bench", "mini-swe-agent"],
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=1),
        "retry_exponential_backoff": True,
    },
    params={
        "split": Param("test", type="string", description="Dataset split"),
        "subset": Param("verified", type="string", description="SWE-bench subset: verified|lite|full"),
        "workers": Param(2, type="integer", description="Parallel workers for agent and eval"),
        "model": Param(
            "nebius/moonshotai/Kimi-K2.6",
            type="string",
            description="Model id for mini-swe-agent",
        ),
        "task_slice": Param("0:1", type="string", description="Instance slice, e.g. 0:1 or 0:3"),
        "run_id": Param("", type="string", description="Optional stable run id; empty => auto"),
        "cost_limit": Param(0.0, type="number", description="Per-instance cost limit (0 = unlimited)"),
    },
)
def evaluate_agent():
    @task(execution_timeout=timedelta(minutes=15))
    def prepare_run(**context) -> dict:
        params = context["params"]
        run_config = build_run_config(params, PROJECT_ROOT)
        run_dir = prepare_run_dir(run_config)
        print(f"Prepared run dir: {run_dir}")
        return {**run_config, "run_dir": str(run_dir)}

    @task(execution_timeout=timedelta(hours=2))
    def run_agent(run_config: dict) -> dict:
        run_dir = Path(run_config["run_dir"])
        preds_path = run_agent_batch(run_config, run_dir)
        print(f"Agent finished. preds={preds_path}")
        return {**run_config, "preds_path": str(preds_path)}

    @task(execution_timeout=timedelta(hours=1))
    def run_eval(run_config: dict) -> dict:
        run_dir = Path(run_config["run_dir"])
        preds_path = Path(run_config["preds_path"])
        eval_dir = run_swebench_eval(run_config, preds_path, run_dir)
        print(f"Eval finished. eval_dir={eval_dir}")
        return run_config

    @task(execution_timeout=timedelta(minutes=15))
    def summarize_and_log(run_config: dict) -> dict:
        run_dir = Path(run_config["run_dir"])
        eval_dir = run_dir / "run-eval"
        metrics = collect_metrics(eval_dir, run_id=run_config["run_id"])
        (run_dir / "metrics.json").write_text(json.dumps(metrics, indent=2) + "\n", encoding="utf-8")
        write_manifest(run_dir, run_config, metrics)
        try:
            mlflow_run_id = log_mlflow_run(run_config, metrics, run_dir)
        except Exception as exc:  # noqa: BLE001 - keep pipeline durable if MLflow is down
            print(f"MLflow logging failed (continuing): {exc}")
            mlflow_run_id = None
        result = {
            "run_id": run_config["run_id"],
            "run_dir": str(run_dir),
            "metrics": metrics,
            "mlflow_run_id": mlflow_run_id,
        }
        print(json.dumps(result, indent=2, default=str))
        return result

    cfg = prepare_run()
    after_agent = run_agent(cfg)
    after_eval = run_eval(after_agent)
    summarize_and_log(after_eval)


evaluate_agent()
