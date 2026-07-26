"""Reusable helpers for the evaluate_agent Airflow pipeline."""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

DEFAULT_MODEL = "nebius/moonshotai/Kimi-K2.6"
DEFAULT_SUBSET = "verified"
DEFAULT_SPLIT = "test"
DEFAULT_WORKERS = 2
DEFAULT_TASK_SLICE = "0:1"
DEFAULT_COST_LIMIT = 0.0

SUBSET_TO_DATASET = {
    "verified": "princeton-nlp/SWE-bench_Verified",
    "lite": "princeton-nlp/SWE-bench_Lite",
    "full": "princeton-nlp/SWE-bench",
}


def build_run_config(params: dict[str, Any], project_root: Path) -> dict[str, Any]:
    """Normalize Airflow params into a durable run config."""
    run_id = (params.get("run_id") or "").strip()
    if not run_id:
        stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        run_id = f"eval-{stamp}-{uuid.uuid4().hex[:8]}"

    subset = str(params.get("subset") or DEFAULT_SUBSET)
    split = str(params.get("split") or DEFAULT_SPLIT)
    workers = int(params.get("workers") or DEFAULT_WORKERS)
    model = str(params.get("model") or DEFAULT_MODEL)
    task_slice = str(params.get("task_slice") or DEFAULT_TASK_SLICE)
    cost_limit = float(params.get("cost_limit") if params.get("cost_limit") is not None else DEFAULT_COST_LIMIT)

    dataset_name = SUBSET_TO_DATASET.get(subset, SUBSET_TO_DATASET[DEFAULT_SUBSET])

    return {
        "run_id": run_id,
        "subset": subset,
        "split": split,
        "workers": workers,
        "model": model,
        "task_slice": task_slice,
        "cost_limit": cost_limit,
        "dataset_name": dataset_name,
        "project_root": str(project_root),
        "created_at": datetime.now(timezone.utc).isoformat(),
        "mlflow_tracking_uri": os.environ.get(
            "MLFLOW_TRACKING_URI",
            f"sqlite:///{project_root / 'mlflow.db'}",
        ),
        "mlflow_experiment": os.environ.get("MLFLOW_EXPERIMENT_NAME", "evaluate-agent"),
    }


def prepare_run_dir(run_config: dict[str, Any]) -> Path:
    """Create runs/<run-id>/ layout and write config.json."""
    project_root = Path(run_config["project_root"])
    run_dir = project_root / "runs" / run_config["run_id"]
    (run_dir / "run-agent").mkdir(parents=True, exist_ok=True)
    (run_dir / "run-eval" / "logs").mkdir(parents=True, exist_ok=True)
    (run_dir / "run-eval" / "reports").mkdir(parents=True, exist_ok=True)

    config_path = run_dir / "config.json"
    config_path.write_text(json.dumps(run_config, indent=2) + "\n", encoding="utf-8")
    return run_dir


def _load_dotenv(project_root: Path) -> dict[str, str]:
    env = os.environ.copy()
    env_file = project_root / ".env"
    if not env_file.exists():
        return env
    for line in env_file.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        env.setdefault(key, value)
    return env


def run_agent_batch(run_config: dict[str, Any], run_dir: Path) -> Path:
    """Run mini-swe-agent batch and materialize outputs under run-agent/."""
    project_root = Path(run_config["project_root"])
    agent_dir = run_dir / "run-agent"
    trajectories_dir = agent_dir / "trajectories"
    if trajectories_dir.exists():
        shutil.rmtree(trajectories_dir)
    trajectories_dir.mkdir(parents=True, exist_ok=True)

    env = _load_dotenv(project_root)
    env["MSWEA_COST_TRACKING"] = "ignore_errors"

    cmd = [
        "uv",
        "run",
        "mini-extra",
        "swebench",
        "--subset",
        run_config["subset"],
        "--split",
        run_config["split"],
        "--model",
        run_config["model"],
        "--slice",
        run_config["task_slice"],
        "--workers",
        str(run_config["workers"]),
        "-o",
        str(trajectories_dir),
    ]
    # cost-limit is supported by swebench-single; batch uses harness defaults / config.

    # Prefer local cloned config if present; otherwise let mini-swe-agent defaults apply.
    config_candidate = project_root / "mini-swe-agent/src/minisweagent/config/benchmarks/swebench.yaml"
    if config_candidate.exists():
        cmd.extend(["--config", str(config_candidate)])

    subprocess.run(cmd, cwd=project_root, env=env, check=True)

    preds_src = trajectories_dir / "preds.json"
    if not preds_src.exists():
        raise FileNotFoundError(f"Expected predictions at {preds_src}")
    preds_dst = agent_dir / "preds.json"
    shutil.copy2(preds_src, preds_dst)
    return preds_dst


def run_swebench_eval(run_config: dict[str, Any], preds_path: Path, run_dir: Path) -> Path:
    """Run SWE-bench evaluation against preds.json into run-eval/."""
    project_root = Path(run_config["project_root"])
    eval_dir = run_dir / "run-eval"
    logs_dir = eval_dir / "logs"
    reports_dir = eval_dir / "reports"
    logs_dir.mkdir(parents=True, exist_ok=True)
    reports_dir.mkdir(parents=True, exist_ok=True)

    env = _load_dotenv(project_root)
    # Keep evaluation artifacts under the run directory.
    env["SWEBENCH_LOG_DIR"] = str(logs_dir)

    cmd = [
        "uv",
        "run",
        "python",
        "-m",
        "swebench.harness.run_evaluation",
        "--dataset_name",
        run_config["dataset_name"],
        "--predictions_path",
        str(preds_path),
        "--max_workers",
        str(run_config["workers"]),
        "--run_id",
        run_config["run_id"],
    ]
    subprocess.run(cmd, cwd=project_root, env=env, check=True)

    # Collect aggregate report JSON written to CWD by swebench (includes run_id in name).
    for path in project_root.glob(f"*{run_config['run_id']}*.json"):
        if path.is_file() and path.parent == project_root:
            shutil.copy2(path, reports_dir / path.name)

    # Also copy default logs/run_evaluation tree if present in project root.
    default_logs = project_root / "logs" / "run_evaluation"
    if default_logs.exists():
        dest = logs_dir / "run_evaluation"
        if dest.exists():
            shutil.rmtree(dest)
        shutil.copytree(default_logs, dest)

    return eval_dir


def collect_metrics(eval_dir: Path) -> dict[str, Any]:
    """Parse SWE-bench report JSON into a compact metrics dict."""
    reports_dir = eval_dir / "reports"
    report_files = sorted(reports_dir.glob("*.json"))
    if not report_files:
        # Fall back to nested report.json files under logs.
        report_files = sorted((eval_dir / "logs").rglob("report.json"))

    metrics: dict[str, Any] = {
        "total_instances": 0,
        "submitted_instances": 0,
        "completed_instances": 0,
        "resolved_instances": 0,
        "unresolved_instances": 0,
        "empty_patch_instances": 0,
        "error_instances": 0,
        "resolve_rate": 0.0,
        "report_files": [str(p) for p in report_files],
    }

    # Prefer aggregate reports (contain total_instances) over per-instance report.json.
    aggregate = None
    for path in report_files:
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            continue
        if isinstance(data, dict) and "resolved_instances" in data:
            aggregate = data
            break

    if aggregate:
        for key in (
            "total_instances",
            "submitted_instances",
            "completed_instances",
            "resolved_instances",
            "unresolved_instances",
            "empty_patch_instances",
            "error_instances",
        ):
            if key in aggregate:
                metrics[key] = aggregate[key]
        completed = metrics.get("completed_instances") or 0
        resolved = metrics.get("resolved_instances") or 0
        metrics["resolve_rate"] = (resolved / completed) if completed else 0.0
        metrics["resolved_ids"] = aggregate.get("resolved_ids", [])
        metrics["completed_ids"] = aggregate.get("completed_ids", [])
    else:
        # Per-instance fallback
        resolved = 0
        completed = 0
        for path in report_files:
            try:
                data = json.loads(path.read_text(encoding="utf-8"))
            except json.JSONDecodeError:
                continue
            if isinstance(data, dict):
                completed += 1
                # SWE-bench instance reports often use {"instance_id": {"resolved": bool}}
                if any(
                    isinstance(v, dict) and v.get("resolved") is True for v in data.values()
                ) or data.get("resolved") is True:
                    resolved += 1
        metrics["completed_instances"] = completed
        metrics["submitted_instances"] = completed
        metrics["resolved_instances"] = resolved
        metrics["unresolved_instances"] = max(completed - resolved, 0)
        metrics["resolve_rate"] = (resolved / completed) if completed else 0.0

    return metrics


def write_manifest(run_dir: Path, run_config: dict[str, Any], metrics: dict[str, Any]) -> Path:
    """Write manifest.json pointing at key artifacts."""
    agent_dir = run_dir / "run-agent"
    eval_dir = run_dir / "run-eval"
    manifest = {
        "run_id": run_config["run_id"],
        "created_at": run_config.get("created_at"),
        "config": "config.json",
        "metrics": "metrics.json",
        "predictions": "run-agent/preds.json",
        "trajectories": "run-agent/trajectories",
        "eval_logs": "run-eval/logs",
        "eval_reports": "run-eval/reports",
        "exists": {
            "preds.json": (agent_dir / "preds.json").exists(),
            "trajectories": (agent_dir / "trajectories").exists(),
            "metrics.json": (run_dir / "metrics.json").exists(),
            "eval_logs": eval_dir.joinpath("logs").exists(),
            "eval_reports": eval_dir.joinpath("reports").exists(),
        },
        "summary": {
            "model": run_config["model"],
            "subset": run_config["subset"],
            "split": run_config["split"],
            "task_slice": run_config["task_slice"],
            "resolved_instances": metrics.get("resolved_instances"),
            "completed_instances": metrics.get("completed_instances"),
            "resolve_rate": metrics.get("resolve_rate"),
        },
        "remote_artifact_uri": None,
        "note": "Remote Object Storage upload not enabled in easy-mode; local runs/<run-id>/ is the source of truth.",
    }
    path = run_dir / "manifest.json"
    path.write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
    return path


def log_mlflow_run(
    run_config: dict[str, Any],
    metrics: dict[str, Any],
    run_dir: Path,
    artifact_uri: str | None = None,
) -> str:
    """Log params/metrics/artifact path to MLflow; return MLflow run id."""
    import mlflow

    tracking_uri = run_config.get("mlflow_tracking_uri") or "http://127.0.0.1:5000"
    experiment = run_config.get("mlflow_experiment") or "evaluate-agent"
    mlflow.set_tracking_uri(tracking_uri)
    mlflow.set_experiment(experiment)

    with mlflow.start_run(run_name=run_config["run_id"]) as active:
        mlflow.log_params(
            {
                "run_id": run_config["run_id"],
                "subset": run_config["subset"],
                "split": run_config["split"],
                "workers": run_config["workers"],
                "model": run_config["model"],
                "task_slice": run_config["task_slice"],
                "cost_limit": run_config["cost_limit"],
                "dataset_name": run_config["dataset_name"],
            }
        )
        numeric_keys = [
            "total_instances",
            "submitted_instances",
            "completed_instances",
            "resolved_instances",
            "unresolved_instances",
            "empty_patch_instances",
            "error_instances",
            "resolve_rate",
        ]
        for key in numeric_keys:
            if key in metrics and isinstance(metrics[key], (int, float)):
                mlflow.log_metric(key, float(metrics[key]))

        local_uri = f"file://{run_dir.resolve()}"
        mlflow.log_param("artifact_path", str(run_dir.resolve()))
        mlflow.log_param("artifact_uri", artifact_uri or local_uri)
        mlflow.set_tag("pipeline", "evaluate_agent")
        # Log compact JSON artifacts for UI inspection.
        metrics_path = run_dir / "metrics.json"
        config_path = run_dir / "config.json"
        manifest_path = run_dir / "manifest.json"
        for path in (metrics_path, config_path, manifest_path):
            if path.exists():
                mlflow.log_artifact(str(path))
        return active.info.run_id
