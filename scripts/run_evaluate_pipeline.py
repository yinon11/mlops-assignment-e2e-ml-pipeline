#!/usr/bin/env python3
"""CLI entrypoint that mirrors the evaluate_agent Airflow DAG (for smoke / debug)."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from pipeline.run_helpers import (
    build_run_config,
    collect_metrics,
    log_mlflow_run,
    prepare_run_dir,
    run_agent_batch,
    run_swebench_eval,
    upload_run_to_s3,
    write_manifest,
)


def main() -> int:
    parser = argparse.ArgumentParser(description="Run evaluate_agent pipeline once")
    parser.add_argument("--split", default="test")
    parser.add_argument("--subset", default="verified")
    parser.add_argument("--workers", type=int, default=1)
    parser.add_argument("--model", default="nebius/moonshotai/Kimi-K2.6")
    parser.add_argument("--task-slice", default="0:1")
    parser.add_argument("--run-id", default="")
    parser.add_argument("--cost-limit", type=float, default=0.0)
    parser.add_argument("--skip-agent", action="store_true", help="Reuse existing preds.json")
    parser.add_argument("--skip-eval", action="store_true")
    parser.add_argument("--skip-mlflow", action="store_true")
    args = parser.parse_args()

    params = {
        "split": args.split,
        "subset": args.subset,
        "workers": args.workers,
        "model": args.model,
        "task_slice": args.task_slice,
        "run_id": args.run_id,
        "cost_limit": args.cost_limit,
    }
    run_config = build_run_config(params, ROOT)
    run_dir = prepare_run_dir(run_config)
    print(f"run_dir={run_dir}")

    if args.skip_agent:
        preds_path = run_dir / "run-agent" / "preds.json"
        if not preds_path.exists():
            raise SystemExit(f"--skip-agent requires {preds_path}")
    else:
        preds_path = run_agent_batch(run_config, run_dir)

    if not args.skip_eval:
        run_swebench_eval(run_config, preds_path, run_dir)

    metrics = collect_metrics(run_dir / "run-eval", run_id=run_config["run_id"])
    (run_dir / "metrics.json").write_text(json.dumps(metrics, indent=2) + "\n", encoding="utf-8")
    write_manifest(run_dir, run_config, metrics)

    artifact_uri = None
    try:
        artifact_uri = upload_run_to_s3(run_dir, run_config)
    except Exception as exc:  # noqa: BLE001
        print(f"S3 upload failed (continuing): {exc}")
    if artifact_uri:
        write_manifest(run_dir, run_config, metrics, artifact_uri=artifact_uri)

    mlflow_run_id = None
    if not args.skip_mlflow:
        try:
            mlflow_run_id = log_mlflow_run(run_config, metrics, run_dir, artifact_uri=artifact_uri)
        except Exception as exc:  # noqa: BLE001
            print(f"MLflow logging failed: {exc}")

    print(
        json.dumps(
            {
                "run_id": run_config["run_id"],
                "metrics": metrics,
                "mlflow_run_id": mlflow_run_id,
                "remote_artifact_uri": artifact_uri,
            },
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
