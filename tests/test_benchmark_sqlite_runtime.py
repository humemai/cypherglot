from __future__ import annotations

import importlib.util
import json
import sys
import tempfile
import unittest
from io import StringIO
from pathlib import Path
from unittest import mock

import cypherglot


REPO_ROOT = Path(__file__).resolve().parents[1]
SCRIPT_PATH = REPO_ROOT / "scripts" / "benchmarks" / "benchmark_sqlite_runtime.py"
CORPUS_PATH = (
    REPO_ROOT
    / "scripts"
    / "benchmarks"
    / "corpora"
    / "sqlite_runtime_benchmark_corpus.json"
)
MODULE_SPEC = importlib.util.spec_from_file_location(
    "benchmark_sqlite_runtime",
    SCRIPT_PATH,
)
if MODULE_SPEC is None or MODULE_SPEC.loader is None:
    raise RuntimeError(f"Unable to load benchmark script from {SCRIPT_PATH}")
benchmark_sqlite_runtime = importlib.util.module_from_spec(MODULE_SPEC)
sys.modules[MODULE_SPEC.name] = benchmark_sqlite_runtime
MODULE_SPEC.loader.exec_module(benchmark_sqlite_runtime)

SMALL_SCALE = benchmark_sqlite_runtime.RuntimeScale(
    node_type_count=3,
    edge_type_count=3,
    nodes_per_type=20,
    edges_per_source=2,
    ingest_batch_size=10,
    variable_hop_max=2,
)

SKEWED_SCALE = benchmark_sqlite_runtime.RuntimeScale(
    node_type_count=3,
    edge_type_count=3,
    nodes_per_type=1_000,
    edges_per_source=8,
    edge_degree_profile="skewed",
    ingest_batch_size=100,
    variable_hop_max=2,
)


class BenchmarkSQLiteRuntimeScriptTests(unittest.TestCase):
    def test_load_corpus_has_expected_shape(self) -> None:
        load_corpus = getattr(benchmark_sqlite_runtime, "_load_corpus")

        queries = load_corpus(CORPUS_PATH)

        self.assertEqual(len(queries), 20)
        self.assertEqual(
            len([query for query in queries if query.workload == "oltp"]),
            10,
        )
        self.assertEqual(
            len([query for query in queries if query.workload == "olap"]),
            10,
        )
        self.assertTrue(all(query.backends for query in queries))
        self.assertEqual(benchmark_sqlite_runtime.RuntimeScale().total_nodes, 100_000)

    def test_select_queries_filters_named_entries(self) -> None:
        select_queries = getattr(benchmark_sqlite_runtime, "_select_queries")
        load_corpus = getattr(benchmark_sqlite_runtime, "_load_corpus")

        selected = select_queries(
            load_corpus(CORPUS_PATH),
            ["oltp_type1_point_lookup", "olap_type1_age_rollup"],
        )

        self.assertEqual(
            [query.name for query in selected],
            ["oltp_type1_point_lookup", "olap_type1_age_rollup"],
        )

    def test_select_queries_rejects_unknown_names(self) -> None:
        select_queries = getattr(benchmark_sqlite_runtime, "_select_queries")
        load_corpus = getattr(benchmark_sqlite_runtime, "_load_corpus")

        with self.assertRaisesRegex(ValueError, "Unknown benchmark query"):
            select_queries(load_corpus(CORPUS_PATH), ["not-a-real-query"])

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

    def test_prepare_shared_fixture_supports_index_modes(self) -> None:
        build_graph_schema = getattr(benchmark_sqlite_runtime, "_build_graph_schema")
        prepare_fixture = getattr(
            benchmark_sqlite_runtime,
            "_prepare_shared_sqlite_fixture",
        )
        create_connection = getattr(
            benchmark_sqlite_runtime,
            "_create_sqlite_connection",
        )

        graph_schema, edge_plans = build_graph_schema(SMALL_SCALE)
        indexed = prepare_fixture(
            scale=SMALL_SCALE,
            graph_schema=graph_schema,
            edge_plans=edge_plans,
            index_mode="indexed",
        )
        unindexed = prepare_fixture(
            scale=SMALL_SCALE,
            graph_schema=graph_schema,
            edge_plans=edge_plans,
            index_mode="unindexed",
        )
        try:
            self.assertEqual(indexed.row_counts["node_count"], 60)
            self.assertEqual(indexed.row_counts["edge_count"], 120)
            self.assertIn("after_analyze", indexed.rss_snapshots_mib)
            self.assertGreater(indexed.db_size_mib, 0.0)

            indexed_conn = create_connection(indexed.db_path)
            unindexed_conn = create_connection(unindexed.db_path)
            try:
                indexed_custom_indexes = indexed_conn.execute(
                    "SELECT COUNT(*) FROM sqlite_master "
                    "WHERE type = 'index' AND name LIKE 'idx_%'"
                ).fetchone()
                unindexed_custom_indexes = unindexed_conn.execute(
                    "SELECT COUNT(*) FROM sqlite_master "
                    "WHERE type = 'index' AND name LIKE 'idx_%'"
                ).fetchone()
            finally:
                indexed_conn.close()
                unindexed_conn.close()
        finally:
            indexed.close()
            unindexed.close()

        self.assertGreater(indexed_custom_indexes[0], 0)
        self.assertEqual(unindexed_custom_indexes[0], 0)

    def test_skewed_degree_profile_matches_requested_bucket_mix(self) -> None:
        edge_out_degree = getattr(benchmark_sqlite_runtime, "_edge_out_degree")

        degrees = [
            edge_out_degree(SKEWED_SCALE, source_local_index)
            for source_local_index in range(1, 1_001)
        ]

        low_bucket = [degree for degree in degrees if 2 <= degree <= 5]
        medium_bucket = [degree for degree in degrees if 6 <= degree <= 15]
        high_bucket = [degree for degree in degrees if 20 <= degree <= 200]

        self.assertEqual(len(low_bucket), 700)
        self.assertEqual(len(medium_bucket), 250)
        self.assertEqual(len(high_bucket), 50)
        self.assertGreaterEqual(max(high_bucket), 180)
        self.assertGreater(sum(degrees) / len(degrees), 7.5)
        self.assertLess(sum(degrees) / len(degrees), 8.5)

    def test_prepare_shared_fixture_supports_skewed_degree_profile(self) -> None:
        build_graph_schema = getattr(benchmark_sqlite_runtime, "_build_graph_schema")
        prepare_fixture = getattr(
            benchmark_sqlite_runtime,
            "_prepare_shared_sqlite_fixture",
        )

        graph_schema, edge_plans = build_graph_schema(SKEWED_SCALE)
        indexed = prepare_fixture(
            scale=SKEWED_SCALE,
            graph_schema=graph_schema,
            edge_plans=edge_plans,
            index_mode="indexed",
        )
        try:
            self.assertEqual(indexed.row_counts["node_count"], 3_000)
            self.assertEqual(indexed.row_counts["edge_count"], SKEWED_SCALE.total_edges)
            self.assertGreater(indexed.row_counts["edge_count"], 20_000)
        finally:
            indexed.close()

    def test_prepare_shared_fixture_can_persist_under_root_dir(self) -> None:
        build_graph_schema = getattr(benchmark_sqlite_runtime, "_build_graph_schema")
        prepare_fixture = getattr(
            benchmark_sqlite_runtime,
            "_prepare_shared_sqlite_fixture",
        )

        graph_schema, edge_plans = build_graph_schema(SMALL_SCALE)
        with tempfile.TemporaryDirectory() as temp_dir:
            root_dir = Path(temp_dir) / "my_test_databases"
            root_dir.mkdir()
            fixture = prepare_fixture(
                scale=SMALL_SCALE,
                graph_schema=graph_schema,
                edge_plans=edge_plans,
                index_mode="indexed",
                db_root_dir=root_dir,
            )
            try:
                self.assertTrue(fixture.db_path.exists())
                self.assertEqual(fixture.db_path.parent, root_dir / "sqlite-indexed")
            finally:
                fixture.close()

            self.assertTrue((root_dir / "sqlite-indexed" / "runtime.sqlite3").exists())

    def test_measure_query_compiles_each_iteration(self) -> None:
        build_graph_schema = getattr(benchmark_sqlite_runtime, "_build_graph_schema")
        prepare_fixture = getattr(
            benchmark_sqlite_runtime,
            "_prepare_shared_sqlite_fixture",
        )
        backend_runner = getattr(benchmark_sqlite_runtime, "_BackendRunner")
        measure_query = getattr(benchmark_sqlite_runtime, "_measure_query")
        load_corpus = getattr(benchmark_sqlite_runtime, "_load_corpus")
        render_corpus_queries = getattr(
            benchmark_sqlite_runtime,
            "_render_corpus_queries",
        )
        token_map_fn = getattr(benchmark_sqlite_runtime, "_token_map")

        graph_schema, edge_plans = build_graph_schema(SMALL_SCALE)
        schema_context = cypherglot.CompilerSchemaContext.type_aware(graph_schema)
        token_map = token_map_fn(SMALL_SCALE, graph_schema, edge_plans)
        query = next(
            query
            for query in render_corpus_queries(load_corpus(CORPUS_PATH), token_map)
            if query.name == "oltp_type1_point_lookup"
        )
        fixture = prepare_fixture(
            scale=SMALL_SCALE,
            graph_schema=graph_schema,
            edge_plans=edge_plans,
            index_mode="indexed",
        )
        managed_directory = getattr(benchmark_sqlite_runtime, "ManagedDirectory")
        temp_dir: tempfile.TemporaryDirectory[str] = tempfile.TemporaryDirectory(
            prefix="sqlite-bench-test-"
        )
        runner = backend_runner(
            "sqlite",
            managed_directory(path=Path(temp_dir.name), temp_dir=temp_dir),
            graph_schema=graph_schema,
            schema_context=schema_context,
            sqlite_source=fixture,
        )
        try:
            with mock.patch.object(
                benchmark_sqlite_runtime.cypherglot,
                "to_sql",
                wraps=benchmark_sqlite_runtime.cypherglot.to_sql,
            ) as to_sql:
                result = measure_query(runner, query, iterations=2, warmup=0)
        finally:
            runner.close()
            fixture.close()

        self.assertEqual(to_sql.call_count, 2)
        self.assertGreaterEqual(result["compile"]["p50_ms"], 0.0)
        self.assertGreaterEqual(result["execute"]["p50_ms"], 0.0)
        self.assertGreaterEqual(result["end_to_end"]["p50_ms"], 0.0)

    def test_mutation_iteration_rolls_back_state(self) -> None:
        build_graph_schema = getattr(benchmark_sqlite_runtime, "_build_graph_schema")
        prepare_fixture = getattr(
            benchmark_sqlite_runtime,
            "_prepare_shared_sqlite_fixture",
        )
        backend_runner = getattr(benchmark_sqlite_runtime, "_BackendRunner")
        run_iteration = getattr(benchmark_sqlite_runtime, "_run_iteration")
        load_corpus = getattr(benchmark_sqlite_runtime, "_load_corpus")
        render_corpus_queries = getattr(
            benchmark_sqlite_runtime,
            "_render_corpus_queries",
        )
        token_map_fn = getattr(benchmark_sqlite_runtime, "_token_map")

        graph_schema, edge_plans = build_graph_schema(SMALL_SCALE)
        schema_context = cypherglot.CompilerSchemaContext.type_aware(graph_schema)
        token_map = token_map_fn(SMALL_SCALE, graph_schema, edge_plans)
        query = next(
            query
            for query in render_corpus_queries(load_corpus(CORPUS_PATH), token_map)
            if query.name == "oltp_delete_type1_edge"
        )
        fixture = prepare_fixture(
            scale=SMALL_SCALE,
            graph_schema=graph_schema,
            edge_plans=edge_plans,
            index_mode="indexed",
        )
        managed_directory = getattr(benchmark_sqlite_runtime, "ManagedDirectory")
        temp_dir: tempfile.TemporaryDirectory[str] = tempfile.TemporaryDirectory(
            prefix="sqlite-bench-test-"
        )
        runner = backend_runner(
            "sqlite",
            managed_directory(path=Path(temp_dir.name), temp_dir=temp_dir),
            graph_schema=graph_schema,
            schema_context=schema_context,
            sqlite_source=fixture,
        )
        edge_table = graph_schema.edge_types[0].table_name
        try:
            before = runner.sqlite.execute(
                f"SELECT COUNT(*) FROM {edge_table}"
            ).fetchone()
            metrics = run_iteration(runner, query)
            after = runner.sqlite.execute(
                f"SELECT COUNT(*) FROM {edge_table}"
            ).fetchone()
        finally:
            runner.close()
            fixture.close()

        self.assertEqual(before, after)
        self.assertGreaterEqual(metrics["reset_ns"], 0)

    def test_benchmark_result_reports_indexed_and_unindexed_sqlite(self) -> None:
        benchmark_result = getattr(benchmark_sqlite_runtime, "_benchmark_result")
        load_corpus = getattr(benchmark_sqlite_runtime, "_load_corpus")
        select_queries = getattr(benchmark_sqlite_runtime, "_select_queries")

        result = benchmark_result(
            select_queries(
                load_corpus(CORPUS_PATH),
                ["oltp_type1_point_lookup", "olap_type1_age_rollup"],
            ),
            iterations=1,
            warmup=0,
            include_duckdb=False,
            scale=SMALL_SCALE,
            index_mode="both",
        )

        workloads = result["workloads"]
        self.assertIn("oltp", workloads)
        self.assertIn("olap", workloads)
        self.assertEqual(workloads["oltp"]["sqlite_indexed"]["query_count"], 1)
        self.assertEqual(workloads["oltp"]["sqlite_unindexed"]["query_count"], 1)
        self.assertEqual(workloads["olap"]["sqlite_indexed"]["query_count"], 1)
        self.assertEqual(workloads["olap"]["sqlite_unindexed"]["query_count"], 1)
        self.assertNotIn("duckdb", workloads["olap"])

    def test_benchmark_result_supports_workload_specific_controls(self) -> None:
        benchmark_result = getattr(benchmark_sqlite_runtime, "_benchmark_result")
        load_corpus = getattr(benchmark_sqlite_runtime, "_load_corpus")
        select_queries = getattr(benchmark_sqlite_runtime, "_select_queries")

        result = benchmark_result(
            select_queries(
                load_corpus(CORPUS_PATH),
                ["oltp_type1_point_lookup", "olap_type1_age_rollup"],
            ),
            iterations=7,
            warmup=3,
            oltp_iterations=11,
            oltp_warmup=2,
            olap_iterations=5,
            olap_warmup=1,
            include_duckdb=False,
            scale=SMALL_SCALE,
            index_mode="both",
        )

        workloads = result["workloads"]
        self.assertEqual(workloads["oltp"]["sqlite_indexed"]["iterations"], 11)
        self.assertEqual(workloads["oltp"]["sqlite_indexed"]["warmup"], 2)
        self.assertEqual(workloads["oltp"]["sqlite_unindexed"]["iterations"], 11)
        self.assertEqual(workloads["olap"]["sqlite_indexed"]["iterations"], 5)
        self.assertEqual(workloads["olap"]["sqlite_indexed"]["warmup"], 1)
        self.assertEqual(workloads["olap"]["sqlite_unindexed"]["iterations"], 5)

    def test_benchmark_result_persists_distinct_suite_dirs(
        self,
    ) -> None:
        benchmark_result = getattr(benchmark_sqlite_runtime, "_benchmark_result")
        load_corpus = getattr(benchmark_sqlite_runtime, "_load_corpus")
        select_queries = getattr(benchmark_sqlite_runtime, "_select_queries")

        with tempfile.TemporaryDirectory() as temp_dir:
            root_dir = Path(temp_dir) / "my_test_databases"
            root_dir.mkdir()

            result = benchmark_result(
                select_queries(
                    load_corpus(CORPUS_PATH),
                    ["oltp_type1_point_lookup", "olap_type1_age_rollup"],
                ),
                iterations=1,
                warmup=0,
                include_duckdb=False,
                scale=SMALL_SCALE,
                index_mode="both",
                db_root_dir=root_dir,
            )

            workloads = result["workloads"]
            self.assertEqual(
                Path(workloads["oltp"]["sqlite_indexed"]["db_path"]).parent,
                root_dir / "sqlite-indexed",
            )
            self.assertEqual(
                Path(workloads["oltp"]["sqlite_unindexed"]["db_path"]).parent,
                root_dir / "sqlite-unindexed",
            )
            self.assertTrue((root_dir / "oltp-sqlite-indexed-suite").exists())
            self.assertTrue((root_dir / "oltp-sqlite-unindexed-suite").exists())
            self.assertTrue((root_dir / "olap-sqlite-indexed-suite").exists())
            self.assertTrue((root_dir / "olap-sqlite-unindexed-suite").exists())

    def test_benchmark_result_reports_incremental_progress(self) -> None:
        benchmark_result = getattr(benchmark_sqlite_runtime, "_benchmark_result")
        load_corpus = getattr(benchmark_sqlite_runtime, "_load_corpus")
        select_queries = getattr(benchmark_sqlite_runtime, "_select_queries")

        snapshots: list[dict[str, object]] = []

        result = benchmark_result(
            select_queries(
                load_corpus(CORPUS_PATH),
                ["oltp_type1_point_lookup", "olap_type1_age_rollup"],
            ),
            iterations=1,
            warmup=0,
            include_duckdb=False,
            scale=SMALL_SCALE,
            index_mode="both",
            progress_callback=lambda partial: snapshots.append(
                json.loads(json.dumps(partial))
            ),
        )

        self.assertGreaterEqual(len(snapshots), 5)
        self.assertEqual(snapshots[0]["workloads"], {})
        self.assertIn("oltp", snapshots[1]["workloads"])
        self.assertIn("sqlite_indexed", snapshots[1]["workloads"]["oltp"])
        self.assertIn("sqlite_unindexed", snapshots[2]["workloads"]["oltp"])
        self.assertIn("olap", snapshots[3]["workloads"])
        self.assertIn("sqlite_indexed", snapshots[3]["workloads"]["olap"])
        self.assertIn("sqlite_unindexed", snapshots[4]["workloads"]["olap"])
        self.assertEqual(snapshots[-1], result)

    def test_benchmark_result_reports_postgresql_suites_when_configured(self) -> None:
        benchmark_result = getattr(benchmark_sqlite_runtime, "_benchmark_result")
        load_corpus = getattr(benchmark_sqlite_runtime, "_load_corpus")
        select_queries = getattr(benchmark_sqlite_runtime, "_select_queries")

        backend_calls: list[tuple[str, str, str]] = []

        class FakeFixture:
            def __init__(self, index_mode: str) -> None:
                self.index_mode = index_mode
                self.db_path = Path("/tmp/runtime.sqlite3")
                self.row_counts = {"node_count": 1, "edge_count": 1}
                self.db_size_mib = 1.0
                self.wal_size_mib = 0.5
                self.rss_snapshots_mib = {}
                self.setup_metrics = {
                    "connect_ns": 1,
                    "schema_ns": 1,
                    "index_ns": 1,
                    "ingest_ns": 1,
                    "analyze_ns": 1,
                }

            def close(self) -> None:
                return None

        def fake_prepare_fixture(
            *,
            scale: object,
            graph_schema: object,
            edge_plans: object,
            index_mode: str,
            db_root_dir: Path | None = None,
        ) -> FakeFixture:
            self.assertIsNotNone(scale)
            self.assertIsNotNone(graph_schema)
            self.assertIsNotNone(edge_plans)
            self.assertIsNone(db_root_dir)
            return FakeFixture(index_mode)

        def fake_run_backend_suite(
            backend: str,
            queries: list[object],
            *,
            workload: str,
            iterations: int,
            warmup: int,
            graph_schema: object,
            schema_context: object,
            sqlite_source: FakeFixture,
            postgres_dsn: str | None = None,
            db_root_dir: Path | None = None,
            iteration_progress: bool = False,
        ) -> dict[str, object]:
            self.assertFalse(iteration_progress)
            self.assertIsNotNone(graph_schema)
            self.assertIsNotNone(schema_context)
            self.assertIsNone(db_root_dir)
            backend_calls.append((workload, backend, sqlite_source.index_mode))
            if backend == "postgresql":
                self.assertEqual(postgres_dsn, "postgresql://bench")
            return {
                "backend": backend,
                "index_mode": sqlite_source.index_mode,
                "iterations": iterations,
                "warmup": warmup,
                "query_count": len(queries),
                "setup": {
                    "connect_ms": 0.1,
                    "schema_ms": 0.2,
                    "index_ms": 0.3,
                    "ingest_ms": 0.4,
                    "analyze_ms": 0.5,
                },
                "row_counts": sqlite_source.row_counts,
                "rss_snapshots_mib": {"after_connect": 1.0},
                "storage": {"db_size_mib": 1.0, "wal_size_mib": 0.0},
                "db_path": "/tmp/runtime.sqlite3",
                "compile": {
                    "mean_of_mean_ms": 1.0,
                    "mean_of_p50_ms": 1.0,
                    "mean_of_p95_ms": 1.0,
                    "mean_of_p99_ms": 1.0,
                },
                "execute": {
                    "mean_of_mean_ms": 2.0,
                    "mean_of_p50_ms": 2.0,
                    "mean_of_p95_ms": 2.0,
                    "mean_of_p99_ms": 2.0,
                },
                "reset": {
                    "mean_of_mean_ms": 0.5,
                    "mean_of_p50_ms": 0.5,
                    "mean_of_p95_ms": 0.5,
                    "mean_of_p99_ms": 0.5,
                },
                "end_to_end": {
                    "mean_of_mean_ms": 3.0,
                    "mean_of_p50_ms": 3.0,
                    "mean_of_p95_ms": 3.0,
                    "mean_of_p99_ms": 3.0,
                },
                "queries": [
                    {
                        "name": query.name,
                        "category": query.category,
                        "compile": {
                            "mean_ms": 1.0,
                            "p50_ms": 1.0,
                            "p95_ms": 1.0,
                            "p99_ms": 1.0,
                        },
                        "execute": {
                            "mean_ms": 2.0,
                            "p50_ms": 2.0,
                            "p95_ms": 2.0,
                            "p99_ms": 2.0,
                        },
                        "reset": {
                            "mean_ms": 0.5,
                            "p50_ms": 0.5,
                            "p95_ms": 0.5,
                            "p99_ms": 0.5,
                        },
                        "end_to_end": {
                            "mean_ms": 3.0,
                            "p50_ms": 3.0,
                            "p95_ms": 3.0,
                            "p99_ms": 3.0,
                        },
                    }
                    for query in queries
                ],
            }

        with mock.patch.object(
            benchmark_sqlite_runtime,
            "_prepare_shared_sqlite_fixture",
            side_effect=fake_prepare_fixture,
        ), mock.patch.object(
            benchmark_sqlite_runtime,
            "_run_backend_suite",
            side_effect=fake_run_backend_suite,
        ):
            result = benchmark_result(
                select_queries(
                    load_corpus(CORPUS_PATH),
                    ["oltp_type1_point_lookup", "olap_type1_age_rollup"],
                ),
                iterations=1,
                warmup=0,
                include_duckdb=False,
                postgres_dsn="postgresql://bench",
                scale=SMALL_SCALE,
                index_mode="both",
            )

        workloads = result["workloads"]
        self.assertIn("postgresql_indexed", workloads["oltp"])
        self.assertIn("postgresql_unindexed", workloads["oltp"])
        self.assertIn("postgresql_indexed", workloads["olap"])
        self.assertIn("postgresql_unindexed", workloads["olap"])
        self.assertEqual(
            backend_calls,
            [
                ("oltp", "sqlite", "indexed"),
                ("oltp", "postgresql", "indexed"),
                ("oltp", "sqlite", "unindexed"),
                ("oltp", "postgresql", "unindexed"),
                ("olap", "sqlite", "indexed"),
                ("olap", "postgresql", "indexed"),
                ("olap", "sqlite", "unindexed"),
                ("olap", "postgresql", "unindexed"),
            ],
        )

    def test_filter_duckdb_olap_queries_skips_variable_length_reachability(
        self,
    ) -> None:
        filter_duckdb_olap_queries = getattr(
            benchmark_sqlite_runtime,
            "_filter_duckdb_olap_queries",
        )
        load_corpus = getattr(benchmark_sqlite_runtime, "_load_corpus")
        select_queries = getattr(benchmark_sqlite_runtime, "_select_queries")

        queries = select_queries(
            load_corpus(CORPUS_PATH),
            [
                "olap_variable_length_reachability",
                "olap_cross_type_edge_rollup",
            ],
        )

        filtered = filter_duckdb_olap_queries(queries)

        self.assertEqual(
            [query.name for query in filtered],
            ["olap_cross_type_edge_rollup"],
        )

    def test_write_json_atomic_replaces_destination(self) -> None:
        write_json_atomic = getattr(benchmark_sqlite_runtime, "_write_json_atomic")

        with tempfile.TemporaryDirectory() as temp_dir:
            output_path = Path(temp_dir) / "runtime.json"

            write_json_atomic(output_path, {"status": "running", "value": 1})
            first_payload = json.loads(output_path.read_text(encoding="utf-8"))

            write_json_atomic(output_path, {"status": "completed", "value": 2})
            second_payload = json.loads(output_path.read_text(encoding="utf-8"))

            self.assertEqual(first_payload, {"status": "running", "value": 1})
            self.assertEqual(second_payload, {"status": "completed", "value": 2})
            self.assertEqual(list(Path(temp_dir).glob("runtime.json.*.tmp")), [])

    def test_print_suite_includes_rss_and_storage(self) -> None:
        print_suite = getattr(benchmark_sqlite_runtime, "_print_suite")
        suite = {
            "setup": {
                "connect_ms": 1.0,
                "schema_ms": 2.0,
                "index_ms": 3.0,
                "ingest_ms": 4.0,
                "analyze_ms": 5.0,
            },
            "rss_snapshots_mib": {
                "after_connect": 10.0,
                "after_ingest": 20.0,
            },
            "storage": {
                "db_size_mib": 12.5,
                "wal_size_mib": 3.0,
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
            "reset": {
                "mean_of_mean_ms": 0.4,
                "mean_of_p50_ms": 0.4,
                "mean_of_p95_ms": 0.5,
                "mean_of_p99_ms": 0.6,
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
                    "reset": {
                        "mean_ms": 0.4,
                        "p50_ms": 0.4,
                        "p95_ms": 0.5,
                        "p99_ms": 0.6,
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
            print_suite("oltp/sqlite_indexed", suite)

        rendered = stdout.getvalue()
        self.assertIn("analyze=5.00 ms", rendered)
        self.assertIn("after_ingest=20.00 MiB", rendered)
        self.assertIn("db=12.50 MiB", rendered)
        self.assertIn("p99=1.30 ms", rendered)
        self.assertIn("reset_p99=0.60 ms", rendered)
        self.assertIn("end_to_end_p95=3.20 ms", rendered)


if __name__ == "__main__":
    unittest.main()
