"""Reproduce ArcadeDB GraphAnalyticalView reopen/restore failures.

This script creates an indexed OLAP ArcadeDB fixture, waits for the persisted
GraphAnalyticalView to become READY, then closes and reopens the database in a
loop while waiting for the view to restore each time.
"""

from __future__ import annotations

import argparse
import os
from datetime import datetime, timezone
from pathlib import Path

from scripts.benchmarks.common.runtime_shared import _prepare_generated_graph_fixture
from scripts.benchmarks.common.shared import _build_graph_schema, _progress
from scripts.benchmarks.runtime.arcadedb_embedded import (
    ARCADEDB_GAV_NAME,
    _fetch_arcadedb_gav_metadata,
    _open_arcadedb,
    _prepare_arcadedb_fixture,
    _write_json_atomic,
    _wait_for_arcadedb_gav_status,
)
from scripts.benchmarks.runtime.matrix import SCALE_PRESETS


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Create an indexed ArcadeDB OLAP fixture with a persisted "
            "GraphAnalyticalView, then reopen the database repeatedly while "
            "waiting for the view to restore."
        )
    )
    parser.add_argument(
        "--scale",
        choices=tuple(SCALE_PRESETS),
        default="medium",
        help="Benchmark scale preset to use for the repro fixture.",
    )
    parser.add_argument(
        "--reopen-count",
        type=int,
        default=5,
        help="How many close/reopen cycles to run after the initial GAV build.",
    )
    parser.add_argument(
        "--gav-ready-timeout-s",
        type=float,
        default=180.0,
        help="Seconds to wait for the GraphAnalyticalView to reach READY.",
    )
    parser.add_argument(
        "--db-root-dir",
        type=Path,
        help=(
            "Optional root directory under which generated CSV and ArcadeDB "
            "artifacts should be persisted."
        ),
    )
    parser.add_argument(
        "--output",
        type=Path,
        help="Optional JSON path for a cycle-by-cycle repro summary.",
    )
    return parser.parse_args()


def _metadata_summary(metadata: dict[str, object] | None) -> dict[str, object] | None:
    if metadata is None:
        return None
    return {
        "name": metadata.get("name"),
        "status": metadata.get("status"),
        "updateMode": metadata.get("updateMode"),
        "nodeCount": metadata.get("nodeCount"),
        "edgeCount": metadata.get("edgeCount"),
        "buildDurationMs": metadata.get("buildDurationMs"),
    }


def main() -> int:
    args = _parse_args()
    if args.reopen_count <= 0:
        raise ValueError("--reopen-count must be positive.")
    if args.gav_ready_timeout_s <= 0:
        raise ValueError("--gav-ready-timeout-s must be positive.")
    if args.db_root_dir is not None:
        args.db_root_dir.mkdir(parents=True, exist_ok=True)

    scale_preset = SCALE_PRESETS[args.scale]
    os.environ.setdefault("ARCADEDB_JVM_ARGS", scale_preset.arcadedb_jvm_args)

    _progress(
        f"arcadedb-gav-reopen-demo: scale={args.scale} "
        f"reopen_count={args.reopen_count} "
        f"jvm_args={os.environ['ARCADEDB_JVM_ARGS']}"
    )

    graph_schema, edge_plans = _build_graph_schema(scale_preset.scale)
    generated_fixture = _prepare_generated_graph_fixture(
        scale=scale_preset.scale,
        graph_schema=graph_schema,
        edge_plans=edge_plans,
        index_mode="unindexed",
        db_root_dir=args.db_root_dir,
    )

    fixture = None
    summary: dict[str, object] = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "scale": args.scale,
        "jvm_args": os.environ["ARCADEDB_JVM_ARGS"],
        "gav_name": ARCADEDB_GAV_NAME,
        "reopen_count": args.reopen_count,
        "cycles": [],
    }

    try:
        fixture = _prepare_arcadedb_fixture(
            workload="olap",
            index_mode="indexed",
            graph_schema=graph_schema,
            sqlite_source=generated_fixture,
            ingest_batch_size=scale_preset.scale.ingest_batch_size,
            db_root_dir=args.db_root_dir,
        )
        summary["db_path"] = str(fixture.db_path)
        summary["initial_gav"] = _metadata_summary(
            _fetch_arcadedb_gav_metadata(fixture.database, ARCADEDB_GAV_NAME)
        )

        fixture.database.close()
        fixture.database = None

        for cycle in range(1, args.reopen_count + 1):
            _progress(
                f"arcadedb-gav-reopen-demo: reopen cycle {cycle}/{args.reopen_count}"
            )
            database = _open_arcadedb(fixture.db_path)
            try:
                metadata = _wait_for_arcadedb_gav_status(
                    database,
                    ARCADEDB_GAV_NAME,
                    {"READY"},
                    timeout_sec=args.gav_ready_timeout_s,
                )
                cycle_summary = {
                    "cycle": cycle,
                    "status": "ready",
                    "gav": _metadata_summary(metadata),
                }
                summary["cycles"].append(cycle_summary)
                _progress(
                    f"arcadedb-gav-reopen-demo: cycle {cycle} READY "
                    f"metadata={cycle_summary['gav']}"
                )
            except Exception as exc:
                cycle_summary = {
                    "cycle": cycle,
                    "status": "failed",
                    "error_type": type(exc).__name__,
                    "error_message": str(exc),
                    "gav": _metadata_summary(
                        _fetch_arcadedb_gav_metadata(database, ARCADEDB_GAV_NAME)
                    ),
                }
                summary["cycles"].append(cycle_summary)
                summary["failure"] = cycle_summary
                if args.output is not None:
                    _write_json_atomic(args.output, summary)
                raise
            finally:
                database.close()

    finally:
        if fixture is not None:
            if fixture.database is not None:
                fixture.database.close()
                fixture.database = None
            fixture.work_dir.close()
        generated_fixture.close()

    if args.output is not None:
        _write_json_atomic(args.output, summary)
    _progress("arcadedb-gav-reopen-demo: completed without reopen failures")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())