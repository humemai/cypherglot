"""Run repeated schema-shape benchmarks through a shuffled worker queue."""

from __future__ import annotations

import argparse
import json
import queue
import random
import subprocess
import sys
import threading
import time
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any


REPO_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_OUTPUT_ROOT = REPO_ROOT / "scripts" / "benchmarks" / "results" / "schema"
DEFAULT_SESSION_ROOT = (
    REPO_ROOT / "scripts" / "benchmarks" / "results" / "schema-matrix"
)


@dataclass(frozen=True, slots=True)
class SchemaScalePreset:
    name: str
    node_type_count: int
    edge_type_count: int
    nodes_per_type: int
    edges_per_source: int
    multi_hop_length: int
    node_numeric_property_count: int
    node_text_property_count: int
    node_boolean_property_count: int
    edge_numeric_property_count: int
    edge_text_property_count: int
    edge_boolean_property_count: int
    batch_size: int


@dataclass(frozen=True, slots=True)
class MatrixJob:
    sequence: int
    repeat: int
    scale_name: str
    output_path: Path
    log_path: Path

    @property
    def slug(self) -> str:
        return f"schema-{self.scale_name}-r{self.repeat:02d}"


@dataclass(slots=True)
class JobStatus:
    job: MatrixJob
    status: str = "pending"
    worker_id: int | None = None
    started_at: str | None = None
    completed_at: str | None = None
    duration_s: float | None = None
    exit_code: int | None = None
    command: list[str] | None = None
    error: str | None = None


SCALE_PRESETS: dict[str, SchemaScalePreset] = {
    "small": SchemaScalePreset(
        name="small",
        node_type_count=4,
        edge_type_count=4,
        nodes_per_type=1_000,
        edges_per_source=3,
        multi_hop_length=4,
        node_numeric_property_count=10,
        node_text_property_count=2,
        node_boolean_property_count=2,
        edge_numeric_property_count=6,
        edge_text_property_count=2,
        edge_boolean_property_count=1,
        batch_size=1_000,
    ),
    "medium": SchemaScalePreset(
        name="medium",
        node_type_count=6,
        edge_type_count=8,
        nodes_per_type=5_000,
        edges_per_source=4,
        multi_hop_length=5,
        node_numeric_property_count=10,
        node_text_property_count=2,
        node_boolean_property_count=2,
        edge_numeric_property_count=6,
        edge_text_property_count=2,
        edge_boolean_property_count=1,
        batch_size=5_000,
    ),
    "large": SchemaScalePreset(
        name="large",
        node_type_count=10,
        edge_type_count=10,
        nodes_per_type=100_000,
        edges_per_source=4,
        multi_hop_length=5,
        node_numeric_property_count=10,
        node_text_property_count=2,
        node_boolean_property_count=2,
        edge_numeric_property_count=6,
        edge_text_property_count=2,
        edge_boolean_property_count=1,
        batch_size=5_000,
    ),
}


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Run repeated SQLite schema-shape benchmark jobs through a shuffled "
            "worker queue, writing one JSON result per run."
        )
    )
    parser.add_argument("--scale", choices=tuple(SCALE_PRESETS), required=True)
    parser.add_argument("--workers", type=int, default=1)
    parser.add_argument("--repeats", type=int, default=1)
    parser.add_argument("--iterations", type=int, default=20)
    parser.add_argument("--warmup", type=int, default=3)
    parser.add_argument(
        "--batch-size",
        type=int,
        help="Override the preset batch size used for ingestion.",
    )
    parser.add_argument(
        "--schema",
        action="append",
        choices=("json", "typed", "typeaware"),
        help="Optional schema shape to run. Repeat the flag to select a subset.",
    )
    parser.add_argument(
        "--no-shuffle",
        action="store_true",
        help="Preserve repeat order instead of shuffling queued jobs.",
    )
    parser.add_argument(
        "--shuffle-seed",
        type=int,
        help="Deterministic seed to use when shuffling the job queue.",
    )
    parser.add_argument(
        "--output-root",
        type=Path,
        default=DEFAULT_OUTPUT_ROOT,
        help="Directory where per-run benchmark JSON files will be written.",
    )
    parser.add_argument(
        "--session-root",
        type=Path,
        default=DEFAULT_SESSION_ROOT,
        help="Directory where logs and the run manifest will be written.",
    )
    parser.add_argument(
        "--run-stamp",
        help="Optional run stamp. Defaults to the current UTC timestamp.",
    )
    parser.add_argument(
        "--fail-fast",
        action="store_true",
        help="Stop assigning new jobs after the first failure.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print the queued jobs and commands without executing them.",
    )
    parser.add_argument(
        "--list-scales",
        action="store_true",
        help="Print the available scale presets and exit.",
    )
    return parser.parse_args()


def _resolve_run_stamp(run_stamp: str | None) -> str:
    if run_stamp:
        return run_stamp
    return datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")


def _build_jobs(
    *,
    repeats: int,
    scale_name: str,
    run_stamp: str,
    output_root: Path,
    session_dir: Path,
    shuffle: bool,
    shuffle_seed: int | None,
) -> list[MatrixJob]:
    jobs = [
        MatrixJob(
            sequence=repeat,
            repeat=repeat,
            scale_name=scale_name,
            output_path=(
                output_root / f"schema-{scale_name}-r{repeat:02d}-{run_stamp}.json"
            ),
            log_path=session_dir / "logs" / f"schema-{scale_name}-r{repeat:02d}.log",
        )
        for repeat in range(1, repeats + 1)
    ]
    if shuffle:
        rng = random.Random(shuffle_seed)
        rng.shuffle(jobs)
    return jobs


def _build_command(
    args: argparse.Namespace,
    *,
    job: MatrixJob,
    preset: SchemaScalePreset,
) -> list[str]:
    batch_size = args.batch_size if args.batch_size is not None else preset.batch_size
    command = [
        sys.executable,
        "-m",
        "scripts.benchmarks.schema.sqlite_shapes",
        "--output",
        str(job.output_path),
        "--iterations",
        str(args.iterations),
        "--warmup",
        str(args.warmup),
        "--batch-size",
        str(batch_size),
        "--node-type-count",
        str(preset.node_type_count),
        "--edge-type-count",
        str(preset.edge_type_count),
        "--nodes-per-type",
        str(preset.nodes_per_type),
        "--edges-per-source",
        str(preset.edges_per_source),
        "--multi-hop-length",
        str(preset.multi_hop_length),
        "--node-numeric-property-count",
        str(preset.node_numeric_property_count),
        "--node-text-property-count",
        str(preset.node_text_property_count),
        "--node-boolean-property-count",
        str(preset.node_boolean_property_count),
        "--edge-numeric-property-count",
        str(preset.edge_numeric_property_count),
        "--edge-text-property-count",
        str(preset.edge_text_property_count),
        "--edge-boolean-property-count",
        str(preset.edge_boolean_property_count),
    ]
    if args.schema:
        for schema_name in args.schema:
            command.extend(["--schema", schema_name])
    return command


def _relay_process_output(
    *,
    worker_id: int,
    status: JobStatus,
    log_file: Any,
    line: str,
) -> None:
    log_file.write(line)
    print(f"[worker {worker_id}] {status.job.slug} {line.rstrip()}", flush=True)


def _progress_counts(statuses: list[JobStatus]) -> dict[str, int]:
    counts = {"completed": 0, "failed": 0, "running": 0, "pending": 0}
    for status in statuses:
        if status.status in counts:
            counts[status.status] += 1
    counts["finished"] = counts["completed"] + counts["failed"]
    counts["total"] = len(statuses)
    return counts


def _format_progress_snapshot(statuses: list[JobStatus]) -> str:
    counts = _progress_counts(statuses)
    return (
        f"progress={counts['finished']}/{counts['total']} "
        f"completed={counts['completed']} failed={counts['failed']} "
        f"running={counts['running']} pending={counts['pending']}"
    )


def _worker_loop(
    worker_id: int,
    *,
    args: argparse.Namespace,
    preset: SchemaScalePreset,
    job_queue: queue.Queue[JobStatus],
    statuses: list[JobStatus],
    status_lock: threading.Lock,
    stop_event: threading.Event,
) -> None:
    while True:
        if stop_event.is_set() and args.fail_fast:
            return
        try:
            status = job_queue.get_nowait()
        except queue.Empty:
            return

        command = _build_command(args, job=status.job, preset=preset)
        started_at = datetime.now(UTC)
        with status_lock:
            status.status = "running"
            status.worker_id = worker_id
            status.started_at = started_at.isoformat()
            status.command = command
            print(
                f"[worker {worker_id}] starting {status.job.slug} | "
                f"{_format_progress_snapshot(statuses)}",
                flush=True,
            )

        status.job.log_path.parent.mkdir(parents=True, exist_ok=True)
        start_time = time.perf_counter()
        exit_code = 1
        error: str | None = None
        try:
            with status.job.log_path.open("w", encoding="utf-8") as log_file:
                process = subprocess.Popen(
                    command,
                    cwd=REPO_ROOT,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.STDOUT,
                    text=True,
                    bufsize=1,
                )
                assert process.stdout is not None
                for line in process.stdout:
                    _relay_process_output(
                        worker_id=worker_id,
                        status=status,
                        log_file=log_file,
                        line=line,
                    )
                exit_code = process.wait()
        except (OSError, subprocess.SubprocessError) as exc:
            error = str(exc)

        completed_at = datetime.now(UTC)
        duration_s = time.perf_counter() - start_time
        with status_lock:
            status.completed_at = completed_at.isoformat()
            status.duration_s = duration_s
            status.exit_code = exit_code
            status.error = error
            status.status = (
                "completed" if exit_code == 0 and error is None else "failed"
            )
            print(
                f"[worker {worker_id}] finished {status.job.slug} "
                f"status={status.status} exit={exit_code} duration={duration_s:.1f}s | "
                f"{_format_progress_snapshot(statuses)}",
                flush=True,
            )
            if status.status == "failed" and args.fail_fast:
                stop_event.set()

        job_queue.task_done()


def _manifest_payload(
    *,
    args: argparse.Namespace,
    preset: SchemaScalePreset,
    run_stamp: str,
    statuses: list[JobStatus],
) -> dict[str, Any]:
    return {
        "runner": "scripts.benchmarks.schema.matrix",
        "generated_at": datetime.now(UTC).isoformat(),
        "run_stamp": run_stamp,
        "scale": preset.name,
        "scale_preset": asdict(preset),
        "controls": {
            "repeats": args.repeats,
            "workers": args.workers,
            "iterations": args.iterations,
            "warmup": args.warmup,
            "batch_size": (
                args.batch_size
                if args.batch_size is not None
                else preset.batch_size
            ),
            "schemas": args.schema or ["json", "typed", "typeaware"],
            "shuffle": not args.no_shuffle,
            "shuffle_seed": args.shuffle_seed,
        },
        "jobs": [
            {
                "slug": status.job.slug,
                "repeat": status.job.repeat,
                "output_path": str(status.job.output_path),
                "log_path": str(status.job.log_path),
                "status": status.status,
                "worker_id": status.worker_id,
                "started_at": status.started_at,
                "completed_at": status.completed_at,
                "duration_s": status.duration_s,
                "exit_code": status.exit_code,
                "command": status.command,
                "error": status.error,
            }
            for status in statuses
        ],
    }


def _print_scale_presets() -> None:
    for preset in SCALE_PRESETS.values():
        print(
            f"{preset.name}: node-types={preset.node_type_count} "
            f"edge-types={preset.edge_type_count} "
            f"nodes/type={preset.nodes_per_type} "
            f"edges/source={preset.edges_per_source} batch={preset.batch_size}"
        )


def main() -> int:
    args = _parse_args()
    if args.list_scales:
        _print_scale_presets()
        return 0
    if args.workers <= 0:
        raise ValueError("--workers must be positive.")
    if args.repeats <= 0:
        raise ValueError("--repeats must be positive.")
    if args.iterations <= 0:
        raise ValueError("--iterations must be positive.")
    if args.warmup < 0:
        raise ValueError("--warmup must be zero or positive.")
    if args.batch_size is not None and args.batch_size <= 0:
        raise ValueError("--batch-size must be positive when provided.")

    preset = SCALE_PRESETS[args.scale]
    run_stamp = _resolve_run_stamp(args.run_stamp)
    session_dir = args.session_root / run_stamp
    session_dir.mkdir(parents=True, exist_ok=True)
    (session_dir / "logs").mkdir(parents=True, exist_ok=True)
    args.output_root.mkdir(parents=True, exist_ok=True)

    jobs = _build_jobs(
        repeats=args.repeats,
        scale_name=args.scale,
        run_stamp=run_stamp,
        output_root=args.output_root,
        session_dir=session_dir,
        shuffle=not args.no_shuffle,
        shuffle_seed=args.shuffle_seed,
    )

    if args.dry_run:
        for job in jobs:
            command = _build_command(args, job=job, preset=preset)
            print(f"{job.slug}: {' '.join(command)}")
        return 0

    statuses = [JobStatus(job=job) for job in jobs]
    job_queue: queue.Queue[JobStatus] = queue.Queue()
    for status in statuses:
        job_queue.put(status)

    status_lock = threading.Lock()
    stop_event = threading.Event()
    threads = [
        threading.Thread(
            target=_worker_loop,
            kwargs={
                "worker_id": worker_id,
                "args": args,
                "preset": preset,
                "job_queue": job_queue,
                "statuses": statuses,
                "status_lock": status_lock,
                "stop_event": stop_event,
            },
            daemon=True,
        )
        for worker_id in range(1, args.workers + 1)
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    manifest_path = session_dir / "manifest.json"
    manifest_path.write_text(
        json.dumps(
            _manifest_payload(
                args=args,
                preset=preset,
                run_stamp=run_stamp,
                statuses=statuses,
            ),
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )
    print(f"Wrote schema-matrix manifest to {manifest_path}")

    failures = [status for status in statuses if status.status == "failed"]
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
