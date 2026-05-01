from __future__ import annotations

import argparse
import importlib.util
import io
import queue
import sys
import tempfile
import threading
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from unittest.mock import patch


REPO_ROOT = Path(__file__).resolve().parents[1]
SCRIPT_PATH = REPO_ROOT / "scripts" / "benchmarks" / "runtime/matrix.py"
MODULE_SPEC = importlib.util.spec_from_file_location(
    "run_runtime_matrix",
    SCRIPT_PATH,
)
if MODULE_SPEC is None or MODULE_SPEC.loader is None:
    raise RuntimeError(f"Unable to load benchmark script from {SCRIPT_PATH}")
run_runtime_matrix = importlib.util.module_from_spec(MODULE_SPEC)
sys.modules[MODULE_SPEC.name] = run_runtime_matrix
MODULE_SPEC.loader.exec_module(run_runtime_matrix)

resolve_arcadedb_jvm_args = getattr(
    run_runtime_matrix,
    "_resolve_arcadedb_jvm_args",
)
build_jobs = getattr(run_runtime_matrix, "_build_jobs")
build_command = getattr(run_runtime_matrix, "_build_command")
validate_args = getattr(run_runtime_matrix, "_validate_args")
format_progress_snapshot = getattr(
    run_runtime_matrix,
    "_format_progress_snapshot",
)
job_detail_suffix = getattr(run_runtime_matrix, "_job_detail_suffix")
parse_args = getattr(run_runtime_matrix, "_parse_args")
format_relayed_progress_line = getattr(
    run_runtime_matrix,
    "_format_relayed_progress_line",
)
relay_process_output = getattr(run_runtime_matrix, "_relay_process_output")
cleanup_job_db_root_dir = getattr(run_runtime_matrix, "_cleanup_job_db_root_dir")
worker_loop = getattr(run_runtime_matrix, "_worker_loop")


class RunRuntimeMatrixTests(unittest.TestCase):
    def test_format_relayed_progress_line_only_relays_progress_messages(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            status = run_runtime_matrix.JobStatus(
                job=run_runtime_matrix.MatrixJob(
                    sequence=1,
                    variant=run_runtime_matrix.VARIANT_BY_NAME["duckdb-unindexed"],
                    repeat=1,
                    output_path=temp_path / "result.json",
                    log_path=temp_path / "job.log",
                    db_root_dir=temp_path / "db",
                )
            )

            relayed = format_relayed_progress_line(
                2,
                status,
                "[progress 14:10:12] oltp/duckdb_unindexed: query 3/10 foo\n",
            )
            skipped = format_relayed_progress_line(2, status, "plain child output\n")

        self.assertEqual(
            relayed,
            (
                "[worker 2] duckdb-unindexed-r01 [progress 14:10:12] "
                "oltp/duckdb_unindexed: query 3/10 foo"
            ),
        )
        self.assertIsNone(skipped)

    def test_relay_process_output_writes_log_and_surfaces_progress(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            status = run_runtime_matrix.JobStatus(
                job=run_runtime_matrix.MatrixJob(
                    sequence=1,
                    variant=run_runtime_matrix.VARIANT_BY_NAME["neo4j-indexed"],
                    repeat=1,
                    output_path=temp_path / "result.json",
                    log_path=temp_path / "job.log",
                    db_root_dir=None,
                )
            )
            stream = io.StringIO(
                "[progress 14:10:12] neo4j/indexed: creating query indexes\n"
                "non-progress line\n"
                "[progress 14:10:13] neo4j/indexed: query 4/10 foo\n"
            )
            log_file = io.StringIO()
            stdout = io.StringIO()

            with redirect_stdout(stdout):
                relay_process_output(3, status, stream=stream, log_file=log_file)

        self.assertIn("creating query indexes", stdout.getvalue())
        self.assertIn("query 4/10 foo", stdout.getvalue())
        self.assertNotIn("non-progress line", stdout.getvalue())
        self.assertIn("non-progress line", log_file.getvalue())

    def test_parse_args_enables_iteration_progress_by_default(self) -> None:
        with patch.object(
            sys,
            "argv",
            ["runtime/matrix.py", "--scale", "small"],
        ):
            args = parse_args()

        self.assertTrue(args.iteration_progress)
        self.assertEqual(args.oltp_timeout_ms, 1000.0)
        self.assertEqual(args.olap_timeout_ms, 10000.0)

    def test_parse_args_supports_no_iteration_progress_opt_out(self) -> None:
        with patch.object(
            sys,
            "argv",
            ["runtime/matrix.py", "--scale", "small", "--no-iteration-progress"],
        ):
            args = parse_args()

        self.assertFalse(args.iteration_progress)

    def test_format_progress_snapshot_includes_counts_and_eta(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            completed_job = run_runtime_matrix.MatrixJob(
                sequence=1,
                variant=run_runtime_matrix.VARIANT_BY_NAME["sqlite-indexed"],
                repeat=1,
                output_path=temp_path / "completed.json",
                log_path=temp_path / "completed.log",
                db_root_dir=temp_path / "db-completed",
            )
            running_job = run_runtime_matrix.MatrixJob(
                sequence=2,
                variant=run_runtime_matrix.VARIANT_BY_NAME["neo4j-indexed"],
                repeat=1,
                output_path=temp_path / "running.json",
                log_path=temp_path / "running.log",
                db_root_dir=None,
            )
            pending_job = run_runtime_matrix.MatrixJob(
                sequence=3,
                variant=run_runtime_matrix.VARIANT_BY_NAME["duckdb-unindexed"],
                repeat=1,
                output_path=temp_path / "pending.json",
                log_path=temp_path / "pending.log",
                db_root_dir=temp_path / "db-pending",
            )

            statuses = [
                run_runtime_matrix.JobStatus(
                    job=completed_job,
                    status="completed",
                    duration_s=12.5,
                ),
                run_runtime_matrix.JobStatus(job=running_job, status="running"),
                run_runtime_matrix.JobStatus(job=pending_job, status="pending"),
            ]

            progress = format_progress_snapshot(statuses)

        self.assertIn("progress=1/3", progress)
        self.assertIn("completed=1", progress)
        self.assertIn("failed=0", progress)
        self.assertIn("running=1", progress)
        self.assertIn("pending=1", progress)
        self.assertIn("eta~25s", progress)

    def test_job_detail_suffix_includes_output_log_db_and_ports(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            job = run_runtime_matrix.MatrixJob(
                sequence=1,
                variant=run_runtime_matrix.VARIANT_BY_NAME["neo4j-indexed"],
                repeat=2,
                output_path=temp_path / "result.json",
                log_path=temp_path / "job.log",
                db_root_dir=temp_path / "db-root",
            )
            status = run_runtime_matrix.JobStatus(
                job=job,
                neo4j_bolt_port=8788,
                neo4j_http_port=8575,
            )

            details = job_detail_suffix(status)

        self.assertIn("output=result.json", details)
        self.assertIn("log=job.log", details)
        self.assertIn("db=", details)
        self.assertIn("neo4j_ports=8788/8575", details)

    def test_resolve_arcadedb_jvm_args_uses_scale_defaults(self) -> None:
        self.assertEqual(
            resolve_arcadedb_jvm_args("small", None),
            "-Xmx4g",
        )
        self.assertEqual(
            resolve_arcadedb_jvm_args("medium", None),
            "-Xmx16g",
        )
        self.assertEqual(
            resolve_arcadedb_jvm_args("large", None),
            "-Xmx32g",
        )
        self.assertEqual(
            resolve_arcadedb_jvm_args("medium", "-Xmx12g"),
            "-Xmx12g",
        )

    def test_build_jobs_expands_repeats_and_shuffles_deterministically(self) -> None:
        variants = [
            run_runtime_matrix.VARIANT_BY_NAME["sqlite-indexed"],
            run_runtime_matrix.VARIANT_BY_NAME["duckdb-unindexed"],
        ]
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            jobs = build_jobs(
                variants=variants,
                repeats=2,
                scale_name="small",
                run_stamp="20260421T120000Z",
                output_root=temp_path / "results",
                session_dir=temp_path / "session",
                db_root_base=temp_path / "db",
                shuffle=True,
                shuffle_seed=7,
            )

        self.assertEqual(len(jobs), 4)
        self.assertEqual(
            [job.slug for job in jobs],
            [
                "duckdb-unindexed-r02",
                "duckdb-unindexed-r01",
                "sqlite-indexed-r01",
                "sqlite-indexed-r02",
            ],
        )

    def test_build_command_for_arcadedb_sets_heap_and_scale_flags(self) -> None:
        args = argparse.Namespace(
            scale="medium",
            iterations=1000,
            warmup=10,
            oltp_iterations=250,
            oltp_warmup=5,
            olap_iterations=50,
            olap_warmup=2,
            iteration_progress=True,
            postgres_dsn=None,
            neo4j_user="neo4j",
            neo4j_database="neo4j",
            neo4j_password="secret",
            neo4j_docker_image="neo4j:5.26.24-community",
            neo4j_docker_startup_timeout=120,
            neo4j_keep_container=False,
            arcadedb_jvm_args=None,
        )
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            job = run_runtime_matrix.MatrixJob(
                sequence=1,
                variant=run_runtime_matrix.VARIANT_BY_NAME["arcadedb-indexed"],
                repeat=1,
                output_path=temp_path / "result.json",
                log_path=temp_path / "job.log",
                db_root_dir=temp_path / "db",
            )
            command, env = build_command(
                args,
                job=job,
                scale_preset=run_runtime_matrix.SCALE_PRESETS["medium"],
            )

        self.assertIn("--index-mode", command)
        self.assertIn("indexed", command)
        self.assertIn("--iteration-progress", command)
        self.assertIn("--oltp-iterations", command)
        self.assertIn("250", command)
        self.assertIn("--olap-iterations", command)
        self.assertIn("50", command)
        self.assertEqual(env["ARCADEDB_JVM_ARGS"], "-Xmx16g")

    def test_build_command_for_neo4j_adds_docker_isolation(self) -> None:
        args = argparse.Namespace(
            scale="small",
            iterations=1000,
            warmup=10,
            oltp_iterations=None,
            oltp_warmup=None,
            olap_iterations=None,
            olap_warmup=None,
            iteration_progress=True,
            postgres_dsn=None,
            neo4j_user="neo4j",
            neo4j_database="neo4j",
            neo4j_password="secret",
            neo4j_docker_image="neo4j:5.26.24-community",
            neo4j_docker_startup_timeout=180,
            neo4j_keep_container=True,
            arcadedb_jvm_args=None,
        )
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            job = run_runtime_matrix.MatrixJob(
                sequence=1,
                variant=run_runtime_matrix.VARIANT_BY_NAME["neo4j-indexed"],
                repeat=2,
                output_path=temp_path / "result.json",
                log_path=temp_path / "job.log",
                db_root_dir=None,
            )
            command, env = build_command(
                args,
                job=job,
                scale_preset=run_runtime_matrix.SCALE_PRESETS["small"],
                neo4j_ports=(8788, 8575),
                neo4j_container_name="cypherglot-neo4j-indexed-test-r02",
            )

        self.assertIn("--docker", command)
        self.assertIn("--iteration-progress", command)
        self.assertIn("--neo4j-password", command)
        self.assertIn("secret", command)
        self.assertIn("--docker-container-name", command)
        self.assertIn("cypherglot-neo4j-indexed-test-r02", command)
        self.assertIn("--docker-bolt-port", command)
        self.assertIn("8788", command)
        self.assertIn("--docker-http-port", command)
        self.assertIn("8575", command)
        self.assertIn("--docker-keep-container", command)
        self.assertNotIn("ARCADEDB_JVM_ARGS", env)

    def test_build_command_forwards_timeout_flags(self) -> None:
        args = argparse.Namespace(
            scale="medium",
            iterations=1000,
            warmup=10,
            oltp_iterations=None,
            oltp_warmup=None,
            olap_iterations=None,
            olap_warmup=None,
            oltp_timeout_ms=750.0,
            olap_timeout_ms=9000.0,
            iteration_progress=True,
            postgres_dsn=None,
            neo4j_user="neo4j",
            neo4j_database="neo4j",
            neo4j_password="secret",
            neo4j_docker_image="neo4j:5.26.24-community",
            neo4j_docker_startup_timeout=120,
            neo4j_keep_container=False,
            arcadedb_jvm_args=None,
        )
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            job = run_runtime_matrix.MatrixJob(
                sequence=1,
                variant=run_runtime_matrix.VARIANT_BY_NAME["sqlite-indexed"],
                repeat=1,
                output_path=temp_path / "result.json",
                log_path=temp_path / "job.log",
                db_root_dir=temp_path / "db",
            )
            command, _env = build_command(
                args,
                job=job,
                scale_preset=run_runtime_matrix.SCALE_PRESETS["medium"],
            )

        self.assertIn("--oltp-timeout-ms", command)
        self.assertIn("750.0", command)
        self.assertIn("--olap-timeout-ms", command)
        self.assertIn("9000.0", command)

    def test_validate_args_requires_neo4j_password_when_selected(self) -> None:
        args = argparse.Namespace(
            workers=2,
            repeats=1,
            iterations=1000,
            warmup=10,
            oltp_iterations=None,
            oltp_warmup=None,
            olap_iterations=None,
            olap_warmup=None,
            neo4j_docker_startup_timeout=120,
            neo4j_port_scan_limit=10,
            neo4j_password="",
        )

        with self.assertRaisesRegex(ValueError, "Neo4j variants require"):
            validate_args(
                args,
                [run_runtime_matrix.VARIANT_BY_NAME["neo4j-indexed"]],
            )

    def test_build_command_skips_iteration_progress_when_disabled(self) -> None:
        args = argparse.Namespace(
            scale="small",
            iterations=1000,
            warmup=10,
            oltp_iterations=None,
            oltp_warmup=None,
            olap_iterations=None,
            olap_warmup=None,
            iteration_progress=False,
            postgres_dsn=None,
            neo4j_user="neo4j",
            neo4j_database="neo4j",
            neo4j_password="secret",
            neo4j_docker_image="neo4j:5.26.24-community",
            neo4j_docker_startup_timeout=120,
            neo4j_keep_container=False,
            arcadedb_jvm_args=None,
        )
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            job = run_runtime_matrix.MatrixJob(
                sequence=1,
                variant=run_runtime_matrix.VARIANT_BY_NAME["sqlite-indexed"],
                repeat=1,
                output_path=temp_path / "result.json",
                log_path=temp_path / "job.log",
                db_root_dir=temp_path / "db",
            )
            command, _ = build_command(
                args,
                job=job,
                scale_preset=run_runtime_matrix.SCALE_PRESETS["small"],
            )

        self.assertNotIn("--iteration-progress", command)

    def test_cleanup_job_db_root_dir_removes_existing_job_directory(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            db_root_dir = temp_path / "db"
            db_root_dir.mkdir()
            (db_root_dir / "artifact.txt").write_text("ok", encoding="utf-8")
            job = run_runtime_matrix.MatrixJob(
                sequence=1,
                variant=run_runtime_matrix.VARIANT_BY_NAME["sqlite-indexed"],
                repeat=1,
                output_path=temp_path / "result.json",
                log_path=temp_path / "job.log",
                db_root_dir=db_root_dir,
            )

            cleanup_error = cleanup_job_db_root_dir(job)

        self.assertIsNone(cleanup_error)
        self.assertFalse(db_root_dir.exists())

    def test_cleanup_job_db_root_dir_keeps_missing_directory_as_noop(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            db_root_dir = temp_path / "missing-db"
            job = run_runtime_matrix.MatrixJob(
                sequence=1,
                variant=run_runtime_matrix.VARIANT_BY_NAME["sqlite-indexed"],
                repeat=1,
                output_path=temp_path / "result.json",
                log_path=temp_path / "job.log",
                db_root_dir=db_root_dir,
            )

            cleanup_error = cleanup_job_db_root_dir(job)

        self.assertIsNone(cleanup_error)

    def test_worker_loop_removes_db_root_dir_after_failed_job(self) -> None:
        args = argparse.Namespace(
            scale="small",
            fail_fast=False,
            iterations=1000,
            warmup=10,
            oltp_iterations=None,
            oltp_warmup=None,
            olap_iterations=None,
            olap_warmup=None,
            oltp_timeout_ms=1000.0,
            olap_timeout_ms=10000.0,
            iteration_progress=True,
            postgres_dsn=None,
            neo4j_user="neo4j",
            neo4j_database="neo4j",
            neo4j_password="secret",
            neo4j_docker_image="neo4j:5.26.24-community",
            neo4j_docker_startup_timeout=120,
            neo4j_keep_container=False,
            arcadedb_jvm_args=None,
        )

        class _FakeProcess:
            def __init__(self) -> None:
                self.stdout = io.StringIO("")

            def wait(self) -> int:
                return 1

        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            db_root_dir = temp_path / "db"
            db_root_dir.mkdir()
            (db_root_dir / "artifact.txt").write_text("ok", encoding="utf-8")
            job = run_runtime_matrix.MatrixJob(
                sequence=1,
                variant=run_runtime_matrix.VARIANT_BY_NAME["sqlite-indexed"],
                repeat=1,
                output_path=temp_path / "result.json",
                log_path=temp_path / "job.log",
                db_root_dir=db_root_dir,
            )
            status = run_runtime_matrix.JobStatus(job=job)
            job_queue: queue.Queue[run_runtime_matrix.JobStatus] = queue.Queue()
            job_queue.put(status)
            statuses = [status]
            manifest_path = temp_path / "manifest.json"
            base_manifest = {"jobs": []}
            port_pool = run_runtime_matrix.PortReservationPool(
                bolt_base=8788,
                http_base=8575,
                scan_limit=10,
            )

            with patch.object(
                run_runtime_matrix,
                "_build_command",
                return_value=([sys.executable, "-c", "raise SystemExit(1)"], {}),
            ), patch.object(
                run_runtime_matrix.subprocess,
                "Popen",
                return_value=_FakeProcess(),
            ):
                worker_loop(
                    worker_id=1,
                    job_queue=job_queue,
                    args=args,
                    scale_preset=run_runtime_matrix.SCALE_PRESETS["small"],
                    run_stamp="20260501T000000Z",
                    port_pool=port_pool,
                    manifest_path=manifest_path,
                    base_manifest=base_manifest,
                    statuses=statuses,
                    manifest_lock=threading.Lock(),
                    stop_event=threading.Event(),
                )

        self.assertEqual(status.status, "failed")
        self.assertEqual(status.exit_code, 1)
        self.assertFalse(db_root_dir.exists())
