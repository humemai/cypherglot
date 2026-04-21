"""Run the runtime benchmark matrix through a shuffled worker queue."""

from __future__ import annotations

import argparse
import json
import os
import queue
import random
import socket
import subprocess
import sys
import threading
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from _benchmark_common import RuntimeScale


REPO_ROOT = Path(__file__).resolve().parents[2]
SCRIPTS_DIR = REPO_ROOT / "scripts" / "benchmarks"
DEFAULT_OUTPUT_ROOT = REPO_ROOT / "scripts" / "benchmarks" / "results" / "runtime"
DEFAULT_SESSION_ROOT = (
    REPO_ROOT / "scripts" / "benchmarks" / "results" / "runtime-matrix"
)
DEFAULT_DB_ROOT = REPO_ROOT / "my_test_databases"


@dataclass(frozen=True, slots=True)
class ScalePreset:
    name: str
    scale: RuntimeScale
    arcadedb_jvm_args: str


@dataclass(frozen=True, slots=True)
class VariantSpec:
    name: str
    script_name: str
    backend: str
    index_mode: str | None
    uses_db_root_dir: bool
    uses_neo4j_docker: bool = False
    uses_arcadedb_env: bool = False


@dataclass(frozen=True, slots=True)
class MatrixJob:
    sequence: int
    variant: VariantSpec
    repeat: int
    output_path: Path
    log_path: Path
    db_root_dir: Path | None

    @property
    def slug(self) -> str:
        return f"{self.variant.name}-r{self.repeat:02d}"


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
    env_overrides: dict[str, str] | None = None
    neo4j_bolt_port: int | None = None
    neo4j_http_port: int | None = None
    neo4j_container_name: str | None = None
    error: str | None = None


SCALE_PRESETS: dict[str, ScalePreset] = {
    "small": ScalePreset(
        name="small",
        scale=RuntimeScale(
            node_type_count=4,
            edge_type_count=4,
            nodes_per_type=1_000,
            edges_per_source=3,
            edge_degree_profile="uniform",
            node_extra_text_property_count=2,
            node_extra_numeric_property_count=6,
            node_extra_boolean_property_count=2,
            edge_extra_text_property_count=1,
            edge_extra_numeric_property_count=3,
            edge_extra_boolean_property_count=1,
            ingest_batch_size=1_000,
            variable_hop_max=2,
        ),
        arcadedb_jvm_args="-Xmx4g",
    ),
    "medium": ScalePreset(
        name="medium",
        scale=RuntimeScale(
            node_type_count=6,
            edge_type_count=8,
            nodes_per_type=100_000,
            edges_per_source=4,
            edge_degree_profile="skewed",
            node_extra_text_property_count=4,
            node_extra_numeric_property_count=10,
            node_extra_boolean_property_count=4,
            edge_extra_text_property_count=2,
            edge_extra_numeric_property_count=6,
            edge_extra_boolean_property_count=2,
            ingest_batch_size=5_000,
            variable_hop_max=5,
        ),
        arcadedb_jvm_args="-Xmx8g",
    ),
    "large": ScalePreset(
        name="large",
        scale=RuntimeScale(
            node_type_count=10,
            edge_type_count=10,
            nodes_per_type=1_000_000,
            edges_per_source=8,
            edge_degree_profile="skewed",
            node_extra_text_property_count=8,
            node_extra_numeric_property_count=18,
            node_extra_boolean_property_count=8,
            edge_extra_text_property_count=4,
            edge_extra_numeric_property_count=10,
            edge_extra_boolean_property_count=4,
            ingest_batch_size=10_000,
            variable_hop_max=8,
        ),
        arcadedb_jvm_args="-Xmx32g",
    ),
}

VARIANTS: tuple[VariantSpec, ...] = (
    VariantSpec(
        name="sqlite-indexed",
        script_name="benchmark_sqlite_runtime.py",
        backend="sqlite",
        index_mode="indexed",
        uses_db_root_dir=True,
    ),
    VariantSpec(
        name="sqlite-unindexed",
        script_name="benchmark_sqlite_runtime.py",
        backend="sqlite",
        index_mode="unindexed",
        uses_db_root_dir=True,
    ),
    VariantSpec(
        name="duckdb-unindexed",
        script_name="benchmark_duckdb_runtime.py",
        backend="duckdb",
        index_mode="unindexed",
        uses_db_root_dir=True,
    ),
    VariantSpec(
        name="postgresql-indexed",
        script_name="benchmark_postgresql_runtime.py",
        backend="postgresql",
        index_mode="indexed",
        uses_db_root_dir=True,
    ),
    VariantSpec(
        name="postgresql-unindexed",
        script_name="benchmark_postgresql_runtime.py",
        backend="postgresql",
        index_mode="unindexed",
        uses_db_root_dir=True,
    ),
    VariantSpec(
        name="neo4j-indexed",
        script_name="benchmark_neo4j_runtime.py",
        backend="neo4j",
        index_mode="indexed",
        uses_db_root_dir=False,
        uses_neo4j_docker=True,
    ),
    VariantSpec(
        name="neo4j-unindexed",
        script_name="benchmark_neo4j_runtime.py",
        backend="neo4j",
        index_mode="unindexed",
        uses_db_root_dir=False,
        uses_neo4j_docker=True,
    ),
    VariantSpec(
        name="arcadedb-indexed",
        script_name="benchmark_arcadedb_embedded_runtime.py",
        backend="arcadedb_embedded",
        index_mode="indexed",
        uses_db_root_dir=True,
        uses_arcadedb_env=True,
    ),
    VariantSpec(
        name="arcadedb-unindexed",
        script_name="benchmark_arcadedb_embedded_runtime.py",
        backend="arcadedb_embedded",
        index_mode="unindexed",
        uses_db_root_dir=True,
        uses_arcadedb_env=True,
    ),
    VariantSpec(
        name="ladybug-unindexed",
        script_name="benchmark_ladybug_runtime.py",
        backend="ladybug",
        index_mode=None,
        uses_db_root_dir=True,
    ),
)

VARIANT_BY_NAME = {variant.name: variant for variant in VARIANTS}


class PortReservationPool:
    def __init__(self, *, bolt_base: int, http_base: int, scan_limit: int) -> None:
        self._bolt_base = bolt_base
        self._http_base = http_base
        self._scan_limit = scan_limit
        self._reserved: set[int] = set()
        self._lock = threading.Lock()

    def reserve_pair(self) -> tuple[int, int]:
        with self._lock:
            bolt_port = self._reserve_port(self._bolt_base)
            try:
                http_port = self._reserve_port(self._http_base)
            except RuntimeError:
                self._reserved.remove(bolt_port)
                raise
            return bolt_port, http_port

    def release_pair(self, pair: tuple[int, int]) -> None:
        with self._lock:
            self._reserved.discard(pair[0])
            self._reserved.discard(pair[1])

    def _reserve_port(self, base_port: int) -> int:
        for offset in range(self._scan_limit):
            candidate = base_port + offset
            if candidate in self._reserved:
                continue
            if _port_is_available(candidate):
                self._reserved.add(candidate)
                return candidate
        raise RuntimeError(
            f"Unable to reserve a free port near {base_port}; increase the scan range."
        )


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Run the 10-variant runtime benchmark matrix through a shuffled "
            "worker queue, with optional repeated runs per variant."
        )
    )
    parser.add_argument("--scale", choices=tuple(SCALE_PRESETS), required=True)
    parser.add_argument("--workers", type=int, default=1)
    parser.add_argument("--repeats", type=int, default=1)
    parser.add_argument("--iterations", type=int, default=1000)
    parser.add_argument("--warmup", type=int, default=10)
    parser.add_argument("--oltp-iterations", type=int)
    parser.add_argument("--oltp-warmup", type=int)
    parser.add_argument("--olap-iterations", type=int)
    parser.add_argument("--olap-warmup", type=int)
    parser.add_argument(
        "--variant",
        action="append",
        dest="variants",
        choices=tuple(VARIANT_BY_NAME),
        help="Optional variant to run. Repeat the flag to select a subset.",
    )
    parser.add_argument(
        "--no-shuffle",
        action="store_true",
        help="Preserve the declared job order instead of shuffling the queue.",
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
        help="Directory where per-job benchmark JSON files will be written.",
    )
    parser.add_argument(
        "--session-root",
        type=Path,
        default=DEFAULT_SESSION_ROOT,
        help="Directory where the runner will write logs and its manifest.",
    )
    parser.add_argument(
        "--db-root-base",
        type=Path,
        default=DEFAULT_DB_ROOT,
        help="Base directory under which per-job database artifacts will be stored.",
    )
    parser.add_argument(
        "--run-stamp",
        help="Optional run stamp. Defaults to the current UTC timestamp.",
    )
    parser.add_argument(
        "--arcadedb-jvm-args",
        help=(
            "Override ARCADEDB_JVM_ARGS for ArcadeDB jobs. Defaults to -Xmx4g, "
            "-Xmx8g, or -Xmx32g for small, medium, and large respectively."
        ),
    )
    parser.add_argument("--postgres-dsn", help="Optional PostgreSQL DSN override.")
    parser.add_argument(
        "--neo4j-password",
        default=os.environ.get("NEO4J_PASSWORD", ""),
        help="Neo4j password for Docker-backed jobs. Defaults to NEO4J_PASSWORD.",
    )
    parser.add_argument("--neo4j-user", default="neo4j")
    parser.add_argument("--neo4j-database", default="neo4j")
    parser.add_argument(
        "--neo4j-docker-image",
        default="neo4j:5.26.24-community",
    )
    parser.add_argument("--neo4j-docker-startup-timeout", type=int, default=120)
    parser.add_argument("--neo4j-bolt-port-base", type=int, default=8788)
    parser.add_argument("--neo4j-http-port-base", type=int, default=8575)
    parser.add_argument(
        "--neo4j-port-scan-limit",
        type=int,
        default=200,
        help="How many sequential ports to scan from each Neo4j base port.",
    )
    parser.add_argument(
        "--neo4j-keep-container",
        action="store_true",
        help="Keep Docker Neo4j containers after each job exits.",
    )
    parser.add_argument(
        "--iteration-progress",
        action="store_true",
        help="Forward per-iteration progress output to the underlying scripts.",
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
        "--list-variants",
        action="store_true",
        help="Print the available matrix variants and exit.",
    )
    return parser.parse_args()


def _resolve_run_stamp(run_stamp: str | None) -> str:
    if run_stamp:
        return run_stamp
    return datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")


def _resolve_arcadedb_jvm_args(scale_name: str, override: str | None) -> str:
    if override:
        return override
    return SCALE_PRESETS[scale_name].arcadedb_jvm_args


def _selected_variants(names: list[str] | None) -> list[VariantSpec]:
    if not names:
        return list(VARIANTS)
    return [VARIANT_BY_NAME[name] for name in names]


def _build_jobs(
    *,
    variants: list[VariantSpec],
    repeats: int,
    scale_name: str,
    run_stamp: str,
    output_root: Path,
    session_dir: Path,
    db_root_base: Path,
    shuffle: bool,
    shuffle_seed: int | None,
) -> list[MatrixJob]:
    jobs: list[MatrixJob] = []
    for repeat in range(1, repeats + 1):
        for variant in variants:
            output_path = output_root / (
                f"{variant.name}-{scale_name}-r{repeat:02d}-{run_stamp}.json"
            )
            log_path = session_dir / "logs" / (
                f"{variant.name}-{scale_name}-r{repeat:02d}.log"
            )
            db_root_dir = None
            if variant.uses_db_root_dir:
                db_root_dir = (
                    db_root_base
                    / f"runtime-{scale_name}-{run_stamp}"
                    / variant.name
                    / f"r{repeat:02d}"
                )
            jobs.append(
                MatrixJob(
                    sequence=len(jobs) + 1,
                    variant=variant,
                    repeat=repeat,
                    output_path=output_path,
                    log_path=log_path,
                    db_root_dir=db_root_dir,
                )
            )
    if shuffle:
        rng = random.Random(shuffle_seed)
        rng.shuffle(jobs)
    return jobs


def _common_scale_args(scale: RuntimeScale) -> list[str]:
    return [
        "--node-type-count",
        str(scale.node_type_count),
        "--edge-type-count",
        str(scale.edge_type_count),
        "--nodes-per-type",
        str(scale.nodes_per_type),
        "--edges-per-source",
        str(scale.edges_per_source),
        "--edge-degree-profile",
        scale.edge_degree_profile,
        "--node-extra-text-property-count",
        str(scale.node_extra_text_property_count),
        "--node-extra-numeric-property-count",
        str(scale.node_extra_numeric_property_count),
        "--node-extra-boolean-property-count",
        str(scale.node_extra_boolean_property_count),
        "--edge-extra-text-property-count",
        str(scale.edge_extra_text_property_count),
        "--edge-extra-numeric-property-count",
        str(scale.edge_extra_numeric_property_count),
        "--edge-extra-boolean-property-count",
        str(scale.edge_extra_boolean_property_count),
        "--variable-hop-max",
        str(scale.variable_hop_max),
        "--ingest-batch-size",
        str(scale.ingest_batch_size),
    ]


def _build_command(
    args: argparse.Namespace,
    *,
    job: MatrixJob,
    scale_preset: ScalePreset,
    neo4j_ports: tuple[int, int] | None = None,
    neo4j_container_name: str | None = None,
) -> tuple[list[str], dict[str, str]]:
    command = [
        sys.executable,
        str(SCRIPTS_DIR / job.variant.script_name),
        "--output",
        str(job.output_path),
        "--iterations",
        str(args.iterations),
        "--warmup",
        str(args.warmup),
        *_common_scale_args(scale_preset.scale),
    ]
    if args.oltp_iterations is not None:
        command.extend(["--oltp-iterations", str(args.oltp_iterations)])
    if args.oltp_warmup is not None:
        command.extend(["--oltp-warmup", str(args.oltp_warmup)])
    if args.olap_iterations is not None:
        command.extend(["--olap-iterations", str(args.olap_iterations)])
    if args.olap_warmup is not None:
        command.extend(["--olap-warmup", str(args.olap_warmup)])
    if args.iteration_progress:
        command.append("--iteration-progress")
    if job.variant.index_mode is not None:
        command.extend(["--index-mode", job.variant.index_mode])
    if job.db_root_dir is not None:
        command.extend(["--db-root-dir", str(job.db_root_dir)])
    if job.variant.backend == "postgresql" and args.postgres_dsn:
        command.extend(["--postgres-dsn", args.postgres_dsn])
    if job.variant.uses_neo4j_docker:
        if neo4j_ports is None or neo4j_container_name is None:
            raise ValueError("Neo4j jobs require reserved ports and a container name.")
        command.extend(
            [
                "--neo4j-user",
                args.neo4j_user,
                "--neo4j-database",
                args.neo4j_database,
                "--neo4j-password",
                args.neo4j_password,
                "--docker",
                "--docker-image",
                args.neo4j_docker_image,
                "--docker-container-name",
                neo4j_container_name,
                "--docker-bolt-port",
                str(neo4j_ports[0]),
                "--docker-http-port",
                str(neo4j_ports[1]),
                "--docker-startup-timeout",
                str(args.neo4j_docker_startup_timeout),
            ]
        )
        if args.neo4j_keep_container:
            command.append("--docker-keep-container")

    env = os.environ.copy()
    if job.variant.uses_arcadedb_env:
        env["ARCADEDB_JVM_ARGS"] = _resolve_arcadedb_jvm_args(
            args.scale,
            args.arcadedb_jvm_args,
        )
    return command, env


def _print_job_plan(jobs: list[MatrixJob]) -> None:
    for index, job in enumerate(jobs, start=1):
        print(
            f"[{index:02d}/{len(jobs):02d}] {job.variant.name} repeat={job.repeat} "
            f"-> {job.output_path.name}"
        )


def _initial_manifest(
    *,
    args: argparse.Namespace,
    scale_preset: ScalePreset,
    run_stamp: str,
    session_dir: Path,
    jobs: list[MatrixJob],
) -> dict[str, Any]:
    return {
        "run_stamp": run_stamp,
        "scale": scale_preset.name,
        "scale_preset": {
            "node_type_count": scale_preset.scale.node_type_count,
            "edge_type_count": scale_preset.scale.edge_type_count,
            "nodes_per_type": scale_preset.scale.nodes_per_type,
            "edges_per_source": scale_preset.scale.edges_per_source,
            "edge_degree_profile": scale_preset.scale.edge_degree_profile,
            "node_extra_text_property_count": (
                scale_preset.scale.node_extra_text_property_count
            ),
            "node_extra_numeric_property_count": (
                scale_preset.scale.node_extra_numeric_property_count
            ),
            "node_extra_boolean_property_count": (
                scale_preset.scale.node_extra_boolean_property_count
            ),
            "edge_extra_text_property_count": (
                scale_preset.scale.edge_extra_text_property_count
            ),
            "edge_extra_numeric_property_count": (
                scale_preset.scale.edge_extra_numeric_property_count
            ),
            "edge_extra_boolean_property_count": (
                scale_preset.scale.edge_extra_boolean_property_count
            ),
            "variable_hop_max": scale_preset.scale.variable_hop_max,
            "ingest_batch_size": scale_preset.scale.ingest_batch_size,
            "total_nodes": scale_preset.scale.total_nodes,
            "total_edges": scale_preset.scale.total_edges,
        },
        "workers": args.workers,
        "repeats": args.repeats,
        "iterations": args.iterations,
        "warmup": args.warmup,
        "oltp_iterations": args.oltp_iterations,
        "oltp_warmup": args.oltp_warmup,
        "olap_iterations": args.olap_iterations,
        "olap_warmup": args.olap_warmup,
        "shuffle": not args.no_shuffle,
        "shuffle_seed": args.shuffle_seed,
        "arcadedb_jvm_args": _resolve_arcadedb_jvm_args(
            args.scale,
            args.arcadedb_jvm_args,
        ),
        "output_root": str(args.output_root),
        "session_dir": str(session_dir),
        "db_root_base": str(args.db_root_base),
        "jobs": [
            {
                "sequence": index,
                "variant": job.variant.name,
                "repeat": job.repeat,
                "output_path": str(job.output_path),
                "log_path": str(job.log_path),
                "db_root_dir": str(job.db_root_dir) if job.db_root_dir else None,
                "status": "pending",
            }
            for index, job in enumerate(jobs, start=1)
        ],
    }


def _write_json_atomic(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = path.with_name(f"{path.name}.{os.getpid()}.tmp")
    temp_path.write_text(
        json.dumps(payload, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    temp_path.replace(path)


def _job_status_to_manifest_entries(statuses: list[JobStatus]) -> list[dict[str, Any]]:
    entries: list[dict[str, Any]] = []
    for status in statuses:
        entries.append(
            {
                "sequence": status.job.sequence,
                "variant": status.job.variant.name,
                "repeat": status.job.repeat,
                "output_path": str(status.job.output_path),
                "log_path": str(status.job.log_path),
                "db_root_dir": (
                    str(status.job.db_root_dir) if status.job.db_root_dir else None
                ),
                "status": status.status,
                "worker_id": status.worker_id,
                "started_at": status.started_at,
                "completed_at": status.completed_at,
                "duration_s": status.duration_s,
                "exit_code": status.exit_code,
                "command": status.command,
                "env_overrides": status.env_overrides,
                "neo4j_bolt_port": status.neo4j_bolt_port,
                "neo4j_http_port": status.neo4j_http_port,
                "neo4j_container_name": status.neo4j_container_name,
                "error": status.error,
            }
        )
    return entries


def _update_manifest(
    manifest_path: Path,
    base_manifest: dict[str, Any],
    statuses: list[JobStatus],
    lock: threading.Lock,
) -> None:
    with lock:
        payload = dict(base_manifest)
        payload["jobs"] = _job_status_to_manifest_entries(statuses)
        payload["completed_jobs"] = sum(
            1 for status in statuses if status.status in {"completed", "failed"}
        )
        payload["failed_jobs"] = sum(
            1 for status in statuses if status.status == "failed"
        )
        _write_json_atomic(manifest_path, payload)


def _port_is_available(port: int) -> bool:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            sock.bind(("127.0.0.1", port))
        except OSError:
            return False
    return True


def _validate_args(args: argparse.Namespace, variants: list[VariantSpec]) -> None:
    if args.workers <= 0:
        raise ValueError("--workers must be positive.")
    if args.repeats <= 0:
        raise ValueError("--repeats must be positive.")
    if args.iterations <= 0:
        raise ValueError("--iterations must be positive.")
    if args.warmup < 0:
        raise ValueError("--warmup must be non-negative.")
    if args.oltp_iterations is not None and args.oltp_iterations <= 0:
        raise ValueError("--oltp-iterations must be positive.")
    if args.oltp_warmup is not None and args.oltp_warmup < 0:
        raise ValueError("--oltp-warmup must be non-negative.")
    if args.olap_iterations is not None and args.olap_iterations <= 0:
        raise ValueError("--olap-iterations must be positive.")
    if args.olap_warmup is not None and args.olap_warmup < 0:
        raise ValueError("--olap-warmup must be non-negative.")
    if args.neo4j_docker_startup_timeout <= 0:
        raise ValueError("--neo4j-docker-startup-timeout must be positive.")
    if args.neo4j_port_scan_limit <= 0:
        raise ValueError("--neo4j-port-scan-limit must be positive.")
    requires_neo4j_password = any(
        variant.uses_neo4j_docker for variant in variants
    )
    if requires_neo4j_password and not args.neo4j_password:
        raise ValueError(
            "Neo4j variants require --neo4j-password or NEO4J_PASSWORD "
            "in the environment."
        )


def _neo4j_container_name(run_stamp: str, job: MatrixJob) -> str:
    slug = job.variant.name.replace("_", "-")
    return f"cypherglot-{slug}-{run_stamp}-r{job.repeat:02d}"


def _worker_loop(
    *,
    worker_id: int,
    job_queue: queue.Queue[JobStatus],
    args: argparse.Namespace,
    scale_preset: ScalePreset,
    run_stamp: str,
    port_pool: PortReservationPool,
    manifest_path: Path,
    base_manifest: dict[str, Any],
    statuses: list[JobStatus],
    manifest_lock: threading.Lock,
    stop_event: threading.Event,
) -> None:
    while True:
        if stop_event.is_set() and args.fail_fast:
            return
        try:
            status = job_queue.get_nowait()
        except queue.Empty:
            return

        if stop_event.is_set() and args.fail_fast:
            job_queue.task_done()
            return

        status.worker_id = worker_id
        status.status = "running"
        status.started_at = datetime.now(UTC).isoformat()
        reserved_ports: tuple[int, int] | None = None
        try:
            if status.job.variant.uses_neo4j_docker:
                reserved_ports = port_pool.reserve_pair()
                status.neo4j_bolt_port = reserved_ports[0]
                status.neo4j_http_port = reserved_ports[1]
                status.neo4j_container_name = _neo4j_container_name(
                    run_stamp,
                    status.job,
                )

            command, env = _build_command(
                args,
                job=status.job,
                scale_preset=scale_preset,
                neo4j_ports=reserved_ports,
                neo4j_container_name=status.neo4j_container_name,
            )
            status.command = command
            env_overrides = {}
            if status.job.variant.uses_arcadedb_env:
                env_overrides["ARCADEDB_JVM_ARGS"] = env["ARCADEDB_JVM_ARGS"]
            status.env_overrides = env_overrides or None
            _update_manifest(manifest_path, base_manifest, statuses, manifest_lock)

            status.job.log_path.parent.mkdir(parents=True, exist_ok=True)
            if status.job.db_root_dir is not None:
                status.job.db_root_dir.mkdir(parents=True, exist_ok=True)

            print(
                f"[worker {worker_id}] starting {status.job.slug} -> "
                f"{status.job.output_path.name}"
            )
            start = time.monotonic()
            with status.job.log_path.open("w", encoding="utf-8") as log_file:
                log_file.write("COMMAND:\n")
                log_file.write(" ".join(command))
                log_file.write("\n\n")
                if status.env_overrides:
                    log_file.write("ENV OVERRIDES:\n")
                    for key, value in sorted(status.env_overrides.items()):
                        log_file.write(f"{key}={value}\n")
                    log_file.write("\n")
                log_file.flush()
                completed = subprocess.run(
                    command,
                    cwd=REPO_ROOT,
                    env=env,
                    stdout=log_file,
                    stderr=subprocess.STDOUT,
                    check=False,
                    text=True,
                )
            status.duration_s = round(time.monotonic() - start, 2)
            status.exit_code = completed.returncode
            status.completed_at = datetime.now(UTC).isoformat()
            if completed.returncode == 0:
                status.status = "completed"
                print(
                    f"[worker {worker_id}] completed {status.job.slug} "
                    f"in {status.duration_s:.2f}s"
                )
            else:
                status.status = "failed"
                status.error = f"process exited with code {completed.returncode}"
                print(
                    f"[worker {worker_id}] failed {status.job.slug} "
                    f"with exit code {completed.returncode}"
                )
                if args.fail_fast:
                    stop_event.set()
        except (OSError, RuntimeError, subprocess.SubprocessError, ValueError) as exc:
            status.status = "failed"
            status.completed_at = datetime.now(UTC).isoformat()
            status.error = str(exc)
            print(f"[worker {worker_id}] failed {status.job.slug}: {exc}")
            if args.fail_fast:
                stop_event.set()
        finally:
            if reserved_ports is not None:
                port_pool.release_pair(reserved_ports)
            _update_manifest(manifest_path, base_manifest, statuses, manifest_lock)
            job_queue.task_done()


def _run_jobs(
    *,
    args: argparse.Namespace,
    scale_preset: ScalePreset,
    run_stamp: str,
    session_dir: Path,
    jobs: list[MatrixJob],
) -> int:
    statuses = [JobStatus(job=job) for job in jobs]
    manifest_path = session_dir / "manifest.json"
    manifest_lock = threading.Lock()
    base_manifest = _initial_manifest(
        args=args,
        scale_preset=scale_preset,
        run_stamp=run_stamp,
        session_dir=session_dir,
        jobs=jobs,
    )
    _write_json_atomic(manifest_path, base_manifest)

    job_queue: queue.Queue[JobStatus] = queue.Queue()
    for status in statuses:
        job_queue.put(status)

    stop_event = threading.Event()
    port_pool = PortReservationPool(
        bolt_base=args.neo4j_bolt_port_base,
        http_base=args.neo4j_http_port_base,
        scan_limit=args.neo4j_port_scan_limit,
    )

    threads = [
        threading.Thread(
            target=_worker_loop,
            kwargs={
                "worker_id": worker_id,
                "job_queue": job_queue,
                "args": args,
                "scale_preset": scale_preset,
                "run_stamp": run_stamp,
                "port_pool": port_pool,
                "manifest_path": manifest_path,
                "base_manifest": base_manifest,
                "statuses": statuses,
                "manifest_lock": manifest_lock,
                "stop_event": stop_event,
            },
            daemon=False,
        )
        for worker_id in range(1, args.workers + 1)
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    failures = [status for status in statuses if status.status == "failed"]
    print(
        f"runtime matrix finished: {len(statuses) - len(failures)} succeeded, "
        f"{len(failures)} failed"
    )
    print(f"manifest: {manifest_path}")
    if failures:
        for failure in failures:
            print(
                f"  FAILED {failure.job.slug}: {failure.error or failure.exit_code} "
                f"(log: {failure.job.log_path})"
            )
        return 1
    return 0


def main() -> int:
    args = _parse_args()
    if args.list_variants:
        for variant in VARIANTS:
            print(variant.name)
        return 0

    selected_variants = _selected_variants(args.variants)
    _validate_args(args, selected_variants)

    scale_preset = SCALE_PRESETS[args.scale]
    run_stamp = _resolve_run_stamp(args.run_stamp)
    session_dir = args.session_root / run_stamp
    args.output_root.mkdir(parents=True, exist_ok=True)
    session_dir.mkdir(parents=True, exist_ok=True)
    (session_dir / "logs").mkdir(parents=True, exist_ok=True)

    jobs = _build_jobs(
        variants=selected_variants,
        repeats=args.repeats,
        scale_name=args.scale,
        run_stamp=run_stamp,
        output_root=args.output_root,
        session_dir=session_dir,
        db_root_base=args.db_root_base,
        shuffle=not args.no_shuffle,
        shuffle_seed=args.shuffle_seed,
    )

    print(
        f"runtime matrix queue: scale={args.scale}, workers={args.workers}, "
        f"variants={len(selected_variants)}, repeats={args.repeats}, jobs={len(jobs)}"
    )
    print(
        f"ArcadeDB heap for this scale: "
        f"{_resolve_arcadedb_jvm_args(args.scale, args.arcadedb_jvm_args)}"
    )
    _print_job_plan(jobs)

    if args.dry_run:
        for job in jobs:
            neo4j_ports = None
            neo4j_container_name = None
            if job.variant.uses_neo4j_docker:
                neo4j_ports = (args.neo4j_bolt_port_base, args.neo4j_http_port_base)
                neo4j_container_name = _neo4j_container_name(run_stamp, job)
            command, env = _build_command(
                args,
                job=job,
                scale_preset=scale_preset,
                neo4j_ports=neo4j_ports,
                neo4j_container_name=neo4j_container_name,
            )
            print()
            print(job.slug)
            print("  command:")
            print("   ", " ".join(command))
            if job.variant.uses_arcadedb_env:
                print("  env:")
                print("   ", f"ARCADEDB_JVM_ARGS={env['ARCADEDB_JVM_ARGS']}")
        return 0

    return _run_jobs(
        args=args,
        scale_preset=scale_preset,
        run_stamp=run_stamp,
        session_dir=session_dir,
        jobs=jobs,
    )


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
