from __future__ import annotations

import argparse
import importlib.util
import sys
import tempfile
import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
SCRIPT_PATH = REPO_ROOT / "scripts" / "benchmarks" / "run_runtime_matrix.py"
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


class RunRuntimeMatrixTests(unittest.TestCase):
    def test_resolve_arcadedb_jvm_args_uses_scale_defaults(self) -> None:
        self.assertEqual(
            resolve_arcadedb_jvm_args("small", None),
            "-Xmx4g",
        )
        self.assertEqual(
            resolve_arcadedb_jvm_args("medium", None),
            "-Xmx8g",
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
        self.assertIn("--oltp-iterations", command)
        self.assertIn("250", command)
        self.assertIn("--olap-iterations", command)
        self.assertIn("50", command)
        self.assertEqual(env["ARCADEDB_JVM_ARGS"], "-Xmx8g")

    def test_build_command_for_neo4j_adds_docker_isolation(self) -> None:
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
