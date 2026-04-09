from __future__ import annotations

import importlib.util
import sys
import tempfile
import unittest
from io import StringIO
from pathlib import Path
from unittest import mock


REPO_ROOT = Path(__file__).resolve().parents[1]
SCRIPT_PATH = REPO_ROOT / "scripts" / "benchmarks" / "benchmark_sqlite_runtime.py"
MODULE_SPEC = importlib.util.spec_from_file_location(
    "benchmark_sqlite_runtime",
    SCRIPT_PATH,
)
if MODULE_SPEC is None or MODULE_SPEC.loader is None:
    raise RuntimeError(f"Unable to load benchmark script from {SCRIPT_PATH}")
benchmark_sqlite_runtime = importlib.util.module_from_spec(MODULE_SPEC)
sys.modules[MODULE_SPEC.name] = benchmark_sqlite_runtime
MODULE_SPEC.loader.exec_module(benchmark_sqlite_runtime)

SMALL_SCALE = benchmark_sqlite_runtime.GraphScale(
    user_count=100,
    company_count=10,
    knows_edges_per_user=3,
)


class BenchmarkSQLiteRuntimeScriptTests(unittest.TestCase):
    def test_workload_catalog_has_expected_sizes(self) -> None:
        self.assertEqual(len(benchmark_sqlite_runtime.OLTP_QUERIES), 13)
        self.assertEqual(len(benchmark_sqlite_runtime.OLAP_QUERIES), 10)
        self.assertEqual(len(benchmark_sqlite_runtime.ALL_QUERIES), 23)
        self.assertEqual(benchmark_sqlite_runtime.GraphScale().user_count, 100_000)

    def test_select_queries_filters_named_entries(self) -> None:
        select_queries = getattr(benchmark_sqlite_runtime, "_select_queries")

        selected = select_queries(
            benchmark_sqlite_runtime.ALL_QUERIES,
            ["oltp_user_point_lookup", "olap_user_count"],
        )

        self.assertEqual(
            [query.name for query in selected],
            ["oltp_user_point_lookup", "olap_user_count"],
        )

    def test_select_queries_rejects_unknown_names(self) -> None:
        select_queries = getattr(benchmark_sqlite_runtime, "_select_queries")

        with self.assertRaisesRegex(ValueError, "Unknown benchmark query"):
            select_queries(
                benchmark_sqlite_runtime.ALL_QUERIES,
                ["not-a-real-query"],
            )

    def test_create_sqlite_connection_applies_profile(self) -> None:
        create_connection = getattr(
            benchmark_sqlite_runtime,
            "_create_sqlite_connection",
        )

        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "runtime.sqlite3"
            conn = create_connection(db_path)
            try:
                journal_mode = conn.execute("PRAGMA journal_mode").fetchone()
                synchronous = conn.execute("PRAGMA synchronous").fetchone()
                foreign_keys = conn.execute("PRAGMA foreign_keys").fetchone()
            finally:
                conn.close()

        self.assertEqual(journal_mode, ("wal",))
        self.assertEqual(synchronous, (1,))
        self.assertEqual(foreign_keys, (1,))

    def test_backend_runner_sqlite_builds_seeded_graph(self) -> None:
        backend_runner = getattr(benchmark_sqlite_runtime, "_BackendRunner")

        temp_dir: tempfile.TemporaryDirectory[str] = tempfile.TemporaryDirectory(
            prefix="sqlite-bench-test-"
        )
        runner = backend_runner(
            "sqlite",
            temp_dir,
            scale=SMALL_SCALE,
            ingest_batch_size=50,
        )
        try:
            self.assertEqual(runner.row_counts["node_count"], 110)
            self.assertEqual(runner.row_counts["label_count"], 110)
            self.assertEqual(runner.row_counts["edge_count"], 400)
            self.assertIn("analyze_ns", runner.setup_metrics)
            count = runner.sqlite.execute("SELECT COUNT(*) FROM nodes").fetchone()
        finally:
            runner.close()

        self.assertEqual(count, (110,))

    def test_measure_query_compiles_each_iteration(self) -> None:
        backend_runner = getattr(benchmark_sqlite_runtime, "_BackendRunner")
        measure_query = getattr(benchmark_sqlite_runtime, "_measure_query")

        temp_dir: tempfile.TemporaryDirectory[str] = tempfile.TemporaryDirectory(
            prefix="sqlite-bench-test-"
        )
        runner = backend_runner(
            "sqlite",
            temp_dir,
            scale=SMALL_SCALE,
            ingest_batch_size=50,
        )
        query = benchmark_sqlite_runtime.OLTP_QUERIES[0]
        try:
            with mock.patch.object(
                benchmark_sqlite_runtime.cypherglot,
                "to_sql",
                wraps=benchmark_sqlite_runtime.cypherglot.to_sql,
            ) as to_sql:
                result = measure_query(
                    runner,
                    query,
                    iterations=2,
                    warmup=0,
                )
        finally:
            runner.close()

        self.assertEqual(to_sql.call_count, 2)
        self.assertGreaterEqual(result["compile"]["p50_ms"], 0.0)
        self.assertGreaterEqual(result["execute"]["p50_ms"], 0.0)
        self.assertGreaterEqual(result["end_to_end"]["p50_ms"], 0.0)

    def test_mutation_iteration_rolls_back_state(self) -> None:
        backend_runner = getattr(benchmark_sqlite_runtime, "_BackendRunner")
        run_iteration = getattr(benchmark_sqlite_runtime, "_run_iteration")

        temp_dir: tempfile.TemporaryDirectory[str] = tempfile.TemporaryDirectory(
            prefix="sqlite-bench-test-"
        )
        runner = backend_runner(
            "sqlite",
            temp_dir,
            scale=SMALL_SCALE,
            ingest_batch_size=50,
        )
        query = next(
            benchmark_query
            for benchmark_query in benchmark_sqlite_runtime.OLTP_QUERIES
            if benchmark_query.name == "oltp_delete_knows_relationship"
        )
        try:
            before = runner.sqlite.execute(
                "SELECT COUNT(*) FROM edges WHERE type = 'KNOWS'"
            ).fetchone()
            metrics = run_iteration(runner, query)
            after = runner.sqlite.execute(
                "SELECT COUNT(*) FROM edges WHERE type = 'KNOWS'"
            ).fetchone()
        finally:
            runner.close()

        self.assertEqual(before, after)
        self.assertGreaterEqual(metrics["reset_ns"], 0)

    def test_benchmark_result_reports_filtered_workloads(self) -> None:
        benchmark_result = getattr(benchmark_sqlite_runtime, "_benchmark_result")

        result = benchmark_result(
            [
                benchmark_sqlite_runtime.OLTP_QUERIES[0],
                benchmark_sqlite_runtime.OLAP_QUERIES[0],
            ],
            iterations=1,
            warmup=0,
            include_duckdb=False,
            scale=SMALL_SCALE,
            ingest_batch_size=50,
        )

        workloads = result["workloads"]
        self.assertIn("oltp", workloads)
        self.assertIn("olap", workloads)
        self.assertEqual(workloads["oltp"]["sqlite"]["query_count"], 1)
        self.assertEqual(workloads["olap"]["sqlite"]["query_count"], 1)
        self.assertNotIn("duckdb", workloads["olap"])

    def test_print_suite_includes_percentiles(self) -> None:
        print_suite = getattr(benchmark_sqlite_runtime, "_print_suite")
        suite = {
            "setup": {
                "connect_ms": 1.0,
                "schema_ms": 2.0,
                "index_ms": 3.0,
                "ingest_ms": 4.0,
                "analyze_ms": 5.0,
            },
            "compile": {
                "mean_of_mean_ms": 1.1,
                "mean_of_p50_ms": 1.0,
                "mean_of_p95_ms": 1.2,
                "mean_of_p99_ms": 1.3,
            },
            "execute": {
                "mean_of_mean_ms": 2.1,
                "mean_of_p50_ms": 2.0,
                "mean_of_p95_ms": 2.2,
                "mean_of_p99_ms": 2.3,
            },
            "end_to_end": {
                "mean_of_mean_ms": 3.1,
                "mean_of_p50_ms": 3.0,
                "mean_of_p95_ms": 3.2,
                "mean_of_p99_ms": 3.3,
            },
            "queries": [
                {
                    "name": "example_query",
                    "category": "example",
                    "compile": {
                        "mean_ms": 1.1,
                        "p50_ms": 1.0,
                        "p95_ms": 1.2,
                        "p99_ms": 1.3,
                    },
                    "execute": {
                        "mean_ms": 2.1,
                        "p50_ms": 2.0,
                        "p95_ms": 2.2,
                        "p99_ms": 2.3,
                    },
                    "end_to_end": {
                        "mean_ms": 3.1,
                        "p50_ms": 3.0,
                        "p95_ms": 3.2,
                        "p99_ms": 3.3,
                    },
                }
            ],
        }

        with mock.patch("sys.stdout", new_callable=StringIO) as stdout:
            print_suite("oltp/sqlite", suite)

        rendered = stdout.getvalue()
        self.assertIn("analyze=5.00 ms", rendered)
        self.assertIn("p50=1.00 ms", rendered)
        self.assertIn("p95=1.20 ms", rendered)
        self.assertIn("compile_p99=1.30 ms", rendered)
        self.assertIn("end_to_end_p99=3.30 ms", rendered)

    @unittest.skipIf(
        benchmark_sqlite_runtime.duckdb is None,
        "duckdb is not installed",
    )
    def test_benchmark_result_includes_duckdb_olap_backend(self) -> None:
        benchmark_result = getattr(benchmark_sqlite_runtime, "_benchmark_result")

        result = benchmark_result(
            [benchmark_sqlite_runtime.OLAP_QUERIES[0]],
            iterations=1,
            warmup=0,
            include_duckdb=True,
            scale=SMALL_SCALE,
            ingest_batch_size=50,
        )

        workloads = result["workloads"]
        self.assertIn("olap", workloads)
        self.assertIn("duckdb", workloads["olap"])
        self.assertEqual(workloads["olap"]["duckdb"]["query_count"], 1)
        self.assertIn("analyze_ms", workloads["olap"]["sqlite"]["setup"])
        self.assertEqual(workloads["olap"]["duckdb"]["setup"]["ingest_ms"], 0.0)
        self.assertEqual(
            workloads["olap"]["duckdb"]["row_counts"],
            workloads["olap"]["sqlite"]["row_counts"],
        )


if __name__ == "__main__":
    unittest.main()
