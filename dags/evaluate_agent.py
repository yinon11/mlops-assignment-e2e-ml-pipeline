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
    upload_run_to_s3,
    write_manifest,
)

# DockerOperator path is optional: only available when the docker provider is
# installed (it is in the Compose image; the uv-tool standalone env may lack it).
try:
    from airflow.providers.docker.operators.docker import DockerOperator
    from docker.types import Mount

    DOCKER_PROVIDER_AVAILABLE = True
except ImportError:
    DOCKER_PROVIDER_AVAILABLE = False

AGENT_IMAGE = os.environ.get("MLOPS_AGENT_IMAGE", "mlops-agent:latest")
# Host-side path of the project root: DockerOperator talks to the *host* Docker
# daemon, so bind-mount sources must be host paths (not container paths).
HOST_PROJECT_ROOT = os.environ.get("MLOPS_HOST_PROJECT_ROOT", str(PROJECT_ROOT))


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
        "use_docker": Param(
            False,
            type="boolean",
            description="Run agent/eval via DockerOperator (requires docker provider + mlops-agent image)",
        ),
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
        artifact_uri = None
        try:
            artifact_uri = upload_run_to_s3(run_dir, run_config)
        except Exception as exc:  # noqa: BLE001 - keep pipeline durable without object storage
            print(f"S3 upload failed (continuing): {exc}")
        if artifact_uri:
            # Re-write manifest so the local copy records where artifacts live remotely.
            write_manifest(run_dir, run_config, metrics, artifact_uri=artifact_uri)
        try:
            mlflow_run_id = log_mlflow_run(run_config, metrics, run_dir, artifact_uri=artifact_uri)
        except Exception as exc:  # noqa: BLE001 - keep pipeline durable if MLflow is down
            print(f"MLflow logging failed (continuing): {exc}")
            mlflow_run_id = None
        result = {
            "run_id": run_config["run_id"],
            "run_dir": str(run_dir),
            "metrics": metrics,
            "mlflow_run_id": mlflow_run_id,
            "remote_artifact_uri": artifact_uri,
        }
        print(json.dumps(result, indent=2, default=str))
        return result

    cfg = prepare_run()
    after_agent = run_agent(cfg)
    after_eval = run_eval(after_agent)

    if DOCKER_PROVIDER_AVAILABLE:

        @task.branch
        def choose_execution_path(**context) -> list[str]:
            if context["params"].get("use_docker"):
                return ["run_agent_docker"]
            return ["run_agent"]

        run_id_tpl = "{{ ti.xcom_pull(task_ids='prepare_run')['run_id'] }}"
        common_docker_kwargs = {
            "image": AGENT_IMAGE,
            "auto_remove": "success",
            "mount_tmp_dir": False,
            "mounts": [
                Mount(source=HOST_PROJECT_ROOT, target="/opt/mlops", type="bind"),
                # SWE-bench / mini-swe-agent launch sibling containers.
                Mount(source="/var/run/docker.sock", target="/var/run/docker.sock", type="bind"),
            ],
            "environment": {
                "NEBIUS_API_KEY": os.environ.get("NEBIUS_API_KEY", ""),
                "MSWEA_COST_TRACKING": "ignore_errors",
            },
            "retries": 2,
        }

        run_agent_docker = DockerOperator(
            task_id="run_agent_docker",
            execution_timeout=timedelta(hours=2),
            command=[
                "bash",
                "-c",
                (
                    "set -e && "
                    f"cd /opt/mlops/runs/{run_id_tpl}/run-agent && "
                    "rm -rf trajectories && mkdir -p trajectories && "
                    "mini-extra swebench"
                    " --subset {{ params.subset }} --split {{ params.split }}"
                    " --model '{{ params.model }}' --slice {{ params.task_slice }}"
                    " --workers {{ params.workers }} -o trajectories && "
                    "cp trajectories/preds.json preds.json"
                ),
            ],
            **common_docker_kwargs,
        )

        run_eval_docker = DockerOperator(
            task_id="run_eval_docker",
            execution_timeout=timedelta(hours=1),
            command=[
                "bash",
                "-c",
                (
                    "set -e && "
                    f"cd /opt/mlops/runs/{run_id_tpl}/run-eval && "
                    "rm -rf logs reports && mkdir -p logs reports && "
                    "python -m swebench.harness.run_evaluation"
                    " --dataset_name '{{ ti.xcom_pull(task_ids='prepare_run')['dataset_name'] }}'"
                    " --predictions_path ../run-agent/preds.json"
                    " --max_workers {{ params.workers }}"
                    f" --run_id {run_id_tpl} && "
                    "mv ./*.json reports/ 2>/dev/null || true"
                ),
            ],
            **common_docker_kwargs,
        )

        branch = choose_execution_path()
        summary = summarize_and_log.override(trigger_rule="none_failed_min_one_success")(cfg)
        cfg >> branch
        branch >> after_agent
        branch >> run_agent_docker >> run_eval_docker
        [after_eval, run_eval_docker] >> summary
    else:
        after_eval >> summarize_and_log(cfg)


evaluate_agent()
