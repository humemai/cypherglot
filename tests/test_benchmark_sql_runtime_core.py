from __future__ import annotations

import importlib.util
import json
import sys
import tempfile
import time
import unittest
from io import StringIO
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

import cypherglot


REPO_ROOT = Path(__file__).resolve().parents[1]
SCRIPT_PATH = REPO_ROOT / "scripts" / "benchmarks" / "_benchmark_sql_runtime_core.py"
CORPUS_PATH = (
    REPO_ROOT
    / "scripts"
    / "benchmarks"
    / "corpora"
    / "sqlite_runtime_benchmark_corpus.json"
)
MODULE_SPEC = importlib.util.spec_from_file_location(
    "benchmark_sql_runtime_core",
    SCRIPT_PATH,
)
if MODULE_SPEC is None or MODULE_SPEC.loader is None:
    raise RuntimeError(f"Unable to load benchmark script from {SCRIPT_PATH}")
benchmark_sql_runtime_core = importlib.util.module_from_spec(MODULE_SPEC)
sys.modules[MODULE_SPEC.name] = benchmark_sql_runtime_core
MODULE_SPEC.loader.exec_module(benchmark_sql_runtime_core)
DUCKDB_AVAILABLE = getattr(benchmark_sql_runtime_core, "_duckdb_available")()

SMALL_SCALE = benchmark_sql_runtime_core.RuntimeScale(
    node_type_count=3,
    edge_type_count=3,
    nodes_per_type=20,
    edges_per_source=2,
    ingest_batch_size=10,
    variable_hop_max=2,
)

SKEWED_SCALE = benchmark_sql_runtime_core.RuntimeScale(
    node_type_count=3,
    edge_type_count=3,
    nodes_per_type=1_000,
    edges_per_source=8,
    edge_degree_profile="skewed",
    ingest_batch_size=100,
    variable_hop_max=2,
)


class BenchmarkSQLRuntimeCoreTests(unittest.TestCase):
    @unittest.skipIf(
        not DUCKDB_AVAILABLE,
        "duckdb is not installed",
    )
    def test_duckdb_backend_runner_uses_native_ingest_setup_stages(self) -> None:
        build_graph_schema = getattr(benchmark_sql_runtime_core, "_build_graph_schema")
        prepare_fixture = getattr(
            benchmark_sql_runtime_core,
            "_prepare_shared_sqlite_fixture",
        )
        backend_runner = getattr(benchmark_sql_runtime_core, "_BackendRunner")
        managed_directory = getattr(benchmark_sql_runtime_core, "ManagedDirectory")

        graph_schema, edge_plans = build_graph_schema(SMALL_SCALE)
        schema_context = cypherglot.CompilerSchemaContext.type_aware(graph_schema)
        fixture = prepare_fixture(
            scale=SMALL_SCALE,
            graph_schema=graph_schema,
            edge_plans=edge_plans,
            index_mode="indexed",
        )
        temp_dir: tempfile.TemporaryDirectory[str] = tempfile.TemporaryDirectory(
            prefix="duckdb-bench-test-"
        )
        runner = backend_runner(
            "duckdb",
            managed_directory(path=Path(temp_dir.name), temp_dir=temp_dir),
            graph_schema=graph_schema,
            schema_context=schema_context,
            sqlite_source=fixture,
        )
        try:
            self.assertGreater(runner.setup_metrics["schema_ns"], 0)
            self.assertGreater(runner.setup_metrics["index_ns"], 0)
            self.assertGreater(runner.setup_metrics["ingest_ns"], 0)
            self.assertGreater(runner.setup_metrics["analyze_ns"], 0)
            self.assertEqual(runner.row_counts, fixture.row_counts)
            self.assertIn("after_index", runner.rss_snapshots_mib)
            self.assertIn("after_ingest", runner.rss_snapshots_mib)
            self.assertIn("after_analyze", runner.rss_snapshots_mib)

            first_node_table = graph_schema.node_types[0].table_name
            first_edge_table = graph_schema.edge_types[0].table_name
            user_count = runner.duck.execute(
                f"SELECT COUNT(*) FROM {first_node_table}"
            ).fetchone()
            edge_count = runner.duck.execute(
                f"SELECT COUNT(*) FROM {first_edge_table}"
            ).fetchone()
        finally:
            runner.close()
            fixture.close()

        self.assertIsNotNone(user_count)
        self.assertIsNotNone(edge_count)
        self.assertGreater(user_count[0], 0)
        self.assertGreater(edge_count[0], 0)

    def test_filter_postgresql_queries_requires_explicit_postgresql_backend(
        self,
    ) -> None:
        filter_postgresql_queries = getattr(
            benchmark_sql_runtime_core,
            "_filter_postgresql_queries",
        )
        corpus_query = benchmark_sql_runtime_core.CorpusQuery

        queries = [
            corpus_query(
                name="sqlite_only",
                workload="oltp",
                category="example",
                query="MATCH (n) RETURN n",
                backends=("sqlite",),
                mode="statement",
                mutation=False,
            ),
            corpus_query(
                name="postgresql_explicit",
                workload="oltp",
                category="example",
                query="MATCH (n) RETURN n",
                backends=("sqlite", "postgresql"),
                mode="statement",
                mutation=False,
            ),
        ]

        filtered = filter_postgresql_queries(queries)

        self.assertEqual(
            [query.name for query in filtered],
            ["postgresql_explicit"],
        )

    def test_load_corpus_has_expected_shape(self) -> None:
        load_corpus = getattr(benchmark_sql_runtime_core, "_load_corpus")

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
        self.assertEqual(benchmark_sql_runtime_core.RuntimeScale().total_nodes, 100_000)

    def test_select_queries_filters_named_entries(self) -> None:
        select_queries = getattr(benchmark_sql_runtime_core, "_select_queries")
        load_corpus = getattr(benchmark_sql_runtime_core, "_load_corpus")

        selected = select_queries(
            load_corpus(CORPUS_PATH),
            ["oltp_type1_point_lookup", "olap_type1_age_rollup"],
        )

        self.assertEqual(
            [query.name for query in selected],
            ["oltp_type1_point_lookup", "olap_type1_age_rollup"],
        )

    def test_select_queries_rejects_unknown_names(self) -> None:
        select_queries = getattr(benchmark_sql_runtime_core, "_select_queries")
        load_corpus = getattr(benchmark_sql_runtime_core, "_load_corpus")

        with self.assertRaisesRegex(ValueError, "Unknown benchmark query"):
            select_queries(load_corpus(CORPUS_PATH), ["not-a-real-query"])

    def test_create_sqlite_connection_applies_profile(self) -> None:
        create_connection = getattr(
            benchmark_sql_runtime_core,
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
        build_graph_schema = getattr(benchmark_sql_runtime_core, "_build_graph_schema")
        prepare_fixture = getattr(
            benchmark_sql_runtime_core,
            "_prepare_shared_sqlite_fixture",
        )
        create_connection = getattr(
            benchmark_sql_runtime_core,
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
        edge_out_degree = getattr(benchmark_sql_runtime_core, "_edge_out_degree")

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
        build_graph_schema = getattr(benchmark_sql_runtime_core, "_build_graph_schema")
        prepare_fixture = getattr(
            benchmark_sql_runtime_core,
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
        build_graph_schema = getattr(benchmark_sql_runtime_core, "_build_graph_schema")
        prepare_fixture = getattr(
            benchmark_sql_runtime_core,
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
        build_graph_schema = getattr(benchmark_sql_runtime_core, "_build_graph_schema")
        prepare_fixture = getattr(
            benchmark_sql_runtime_core,
            "_prepare_shared_sqlite_fixture",
        )
        backend_runner = getattr(benchmark_sql_runtime_core, "_BackendRunner")
        measure_query = getattr(benchmark_sql_runtime_core, "_measure_query")
        load_corpus = getattr(benchmark_sql_runtime_core, "_load_corpus")
        render_corpus_queries = getattr(
            benchmark_sql_runtime_core,
            "_render_corpus_queries",
        )
        token_map_fn = getattr(benchmark_sql_runtime_core, "_token_map")

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
        managed_directory = getattr(benchmark_sql_runtime_core, "ManagedDirectory")
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
                benchmark_sql_runtime_core.cypherglot,
                "to_sql",
                wraps=benchmark_sql_runtime_core.cypherglot.to_sql,
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
        build_graph_schema = getattr(benchmark_sql_runtime_core, "_build_graph_schema")
        prepare_fixture = getattr(
            benchmark_sql_runtime_core,
            "_prepare_shared_sqlite_fixture",
        )
        backend_runner = getattr(benchmark_sql_runtime_core, "_BackendRunner")
        run_iteration = getattr(benchmark_sql_runtime_core, "_run_iteration")
        load_corpus = getattr(benchmark_sql_runtime_core, "_load_corpus")
        render_corpus_queries = getattr(
            benchmark_sql_runtime_core,
            "_render_corpus_queries",
        )
        token_map_fn = getattr(benchmark_sql_runtime_core, "_token_map")

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
        managed_directory = getattr(benchmark_sql_runtime_core, "ManagedDirectory")
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

    def test_postgresql_end_to_end_excludes_rss_sampling_overhead(self) -> None:
        run_postgresql_iteration = getattr(
            benchmark_sql_runtime_core,
            "_run_postgresql_iteration",
        )

        def capture_rss_snapshot() -> dict[str, float | None]:
            time.sleep(0.02)
            return {
                "client_mib": 1.0,
                "server_mib": 2.0,
                "combined_mib": 3.0,
            }

        runner = SimpleNamespace(
            capture_rss_snapshot=capture_rss_snapshot,
            compile_query=lambda query: "SELECT 1",
            execute_query=lambda artifact: None,
            postgresql=SimpleNamespace(rollback=lambda: None),
        )
        query = SimpleNamespace(mutation=False)

        metrics = run_postgresql_iteration(runner, query)

        self.assertLess(metrics["end_to_end_ns"], 10_000_000)
        self.assertGreater(metrics["compile_ns"] + metrics["execute_ns"], 0)
        self.assertEqual(
            metrics["end_to_end_ns"],
            metrics["compile_ns"] + metrics["execute_ns"],
        )

    def test_postgresql_iteration_uses_lightweight_rss_snapshots(self) -> None:
        run_postgresql_iteration = getattr(
            benchmark_sql_runtime_core,
            "_run_postgresql_iteration",
        )

        full_snapshot = mock.Mock(side_effect=AssertionError("full RSS should not run"))
        lightweight_snapshot = mock.Mock(
            return_value={
                "client_mib": 1.0,
                "server_mib": None,
                "combined_mib": None,
            }
        )

        runner = SimpleNamespace(
            capture_rss_snapshot=full_snapshot,
            capture_lightweight_rss_snapshot=lightweight_snapshot,
            compile_query=lambda query: "SELECT 1",
            execute_query=lambda artifact: None,
            postgresql=SimpleNamespace(rollback=lambda: None),
        )
        query = SimpleNamespace(mutation=False)

        metrics = run_postgresql_iteration(runner, query)

        self.assertEqual(lightweight_snapshot.call_count, 4)
        self.assertEqual(
            metrics["rss_stages_mib"]["before_compile"]["server_mib"],
            None,
        )

    def test_benchmark_result_reports_indexed_and_unindexed_sqlite(self) -> None:
        benchmark_result = getattr(benchmark_sql_runtime_core, "_benchmark_result")
        load_corpus = getattr(benchmark_sql_runtime_core, "_load_corpus")
        select_queries = getattr(benchmark_sql_runtime_core, "_select_queries")

        result = benchmark_result(
            select_queries(
                load_corpus(CORPUS_PATH),
                ["oltp_type1_point_lookup", "olap_type1_age_rollup"],
            ),
            iterations=1,
            warmup=0,
            entrypoint=benchmark_sql_runtime_core.SQLITE_ENTRYPOINT,
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

    def test_run_backend_suite_reports_postgresql_index_mode(self) -> None:
        build_graph_schema = getattr(benchmark_sql_runtime_core, "_build_graph_schema")
        prepare_fixture = getattr(
            benchmark_sql_runtime_core,
            "_prepare_shared_sqlite_fixture",
        )
        run_backend_suite = getattr(benchmark_sql_runtime_core, "_run_backend_suite")
        load_corpus = getattr(benchmark_sql_runtime_core, "_load_corpus")
        select_queries = getattr(benchmark_sql_runtime_core, "_select_queries")
        render_corpus_queries = getattr(
            benchmark_sql_runtime_core,
            "_render_corpus_queries",
        )
        token_map_fn = getattr(benchmark_sql_runtime_core, "_token_map")

        graph_schema, edge_plans = build_graph_schema(SMALL_SCALE)
        schema_context = cypherglot.CompilerSchemaContext.type_aware(graph_schema)
        fixture = prepare_fixture(
            scale=SMALL_SCALE,
            graph_schema=graph_schema,
            edge_plans=edge_plans,
            index_mode="unindexed",
        )
        token_map = token_map_fn(SMALL_SCALE, graph_schema, edge_plans)
        queries = render_corpus_queries(
            select_queries(load_corpus(CORPUS_PATH), ["oltp_type1_point_lookup"]),
            token_map,
        )
        try:
            with mock.patch.object(
                benchmark_sql_runtime_core,
                "_BackendRunner",
            ) as backend_runner_cls:
                runner = mock.MagicMock()
                runner.backend = "postgresql"
                runner.index_mode = fixture.index_mode
                runner.setup_metrics = {
                    "connect_ns": 1,
                    "schema_ns": 2,
                    "index_ns": 3,
                    "ingest_ns": 4,
                    "analyze_ns": 5,
                }
                runner.row_counts = dict(fixture.row_counts)
                runner.rss_snapshots_mib = {}
                runner.db_size_mib = 0.0
                runner.wal_size_mib = 0.0
                runner.artifact_path = None
                runner.capture_rss_snapshot.return_value = {
                    "client_mib": 1.0,
                    "server_mib": 2.0,
                    "combined_mib": 3.0,
                }
                runner.close.return_value = None
                backend_runner_cls.return_value = runner

                with mock.patch.object(
                    benchmark_sql_runtime_core,
                    "_measure_query",
                    return_value={
                        "name": queries[0].name,
                        "workload": queries[0].workload,
                        "category": queries[0].category,
                        "backend": "postgresql",
                        "index_mode": fixture.index_mode,
                        "mode": queries[0].mode,
                        "mutation": queries[0].mutation,
                        "compile": {
                            "mean_ms": 0.0,
                            "p50_ms": 0.0,
                            "p95_ms": 0.0,
                            "p99_ms": 0.0,
                        },
                        "execute": {
                            "mean_ms": 0.0,
                            "p50_ms": 0.0,
                            "p95_ms": 0.0,
                            "p99_ms": 0.0,
                        },
                        "end_to_end": {
                            "mean_ms": 0.0,
                            "p50_ms": 0.0,
                            "p95_ms": 0.0,
                            "p99_ms": 0.0,
                        },
                        "reset": {
                            "mean_ms": 0.0,
                            "p50_ms": 0.0,
                            "p95_ms": 0.0,
                            "p99_ms": 0.0,
                        },
                        "rss_stages_mib": {},
                    },
                ):
                    result = run_backend_suite(
                        "postgresql",
                        queries,
                        workload="oltp",
                        iterations=1,
                        warmup=0,
                        graph_schema=graph_schema,
                        schema_context=schema_context,
                        sqlite_source=fixture,
                        postgres_dsn="postgresql://example",
                    )
        finally:
            fixture.close()

        self.assertEqual(result["index_mode"], "unindexed")
        self.assertEqual(result["queries"][0]["index_mode"], "unindexed")

    def test_benchmark_result_supports_workload_specific_controls(self) -> None:
        benchmark_result = getattr(benchmark_sql_runtime_core, "_benchmark_result")
        load_corpus = getattr(benchmark_sql_runtime_core, "_load_corpus")
        select_queries = getattr(benchmark_sql_runtime_core, "_select_queries")

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
            entrypoint=benchmark_sql_runtime_core.SQLITE_ENTRYPOINT,
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
        benchmark_result = getattr(benchmark_sql_runtime_core, "_benchmark_result")
        load_corpus = getattr(benchmark_sql_runtime_core, "_load_corpus")
        select_queries = getattr(benchmark_sql_runtime_core, "_select_queries")

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
                entrypoint=benchmark_sql_runtime_core.SQLITE_ENTRYPOINT,
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
        benchmark_result = getattr(benchmark_sql_runtime_core, "_benchmark_result")
        load_corpus = getattr(benchmark_sql_runtime_core, "_load_corpus")
        select_queries = getattr(benchmark_sql_runtime_core, "_select_queries")

        snapshots: list[dict[str, object]] = []

        result = benchmark_result(
            select_queries(
                load_corpus(CORPUS_PATH),
                ["oltp_type1_point_lookup", "olap_type1_age_rollup"],
            ),
            iterations=1,
            warmup=0,
            entrypoint=benchmark_sql_runtime_core.SQLITE_ENTRYPOINT,
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
        benchmark_result = getattr(benchmark_sql_runtime_core, "_benchmark_result")
        load_corpus = getattr(benchmark_sql_runtime_core, "_load_corpus")
        select_queries = getattr(benchmark_sql_runtime_core, "_select_queries")

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
                "rss_snapshots_mib": {
                    "after_connect": {
                        "client_mib": 1.0,
                        "server_mib": 2.0 if backend == "postgresql" else None,
                        "combined_mib": 3.0 if backend == "postgresql" else 1.0,
                    }
                },
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
                        "rss_stages_mib": {
                            "before_compile": {
                                "client_mean_mib": 1.0,
                                "client_peak_mib": 1.0,
                                "server_mean_mib": None,
                                "server_peak_mib": None,
                                "combined_mean_mib": 1.0,
                                "combined_peak_mib": 1.0,
                            }
                        },
                    }
                    for query in queries
                ],
            }

        with mock.patch.object(
            benchmark_sql_runtime_core,
            "_prepare_shared_sqlite_fixture",
            side_effect=fake_prepare_fixture,
        ), mock.patch.object(
            benchmark_sql_runtime_core,
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
                entrypoint=benchmark_sql_runtime_core.POSTGRESQL_ENTRYPOINT,
                postgres_dsn="postgresql://bench",
                scale=SMALL_SCALE,
                index_mode="both",
            )

        workloads = result["workloads"]
        self.assertIn("postgresql_indexed", workloads["oltp"])
        self.assertIn("postgresql_unindexed", workloads["oltp"])
        self.assertIn("postgresql_indexed", workloads["olap"])
        self.assertIn("postgresql_unindexed", workloads["olap"])
        self.assertNotIn("sqlite_indexed", workloads["oltp"])
        self.assertNotIn("sqlite_unindexed", workloads["oltp"])
        self.assertNotIn("sqlite_indexed", workloads["olap"])
        self.assertNotIn("sqlite_unindexed", workloads["olap"])
        self.assertEqual(
            backend_calls,
            [
                ("oltp", "postgresql", "indexed"),
                ("oltp", "postgresql", "unindexed"),
                ("olap", "postgresql", "indexed"),
                ("olap", "postgresql", "unindexed"),
            ],
        )

    def test_resolve_postgresql_runtime_dsn_uses_configured_value(self) -> None:
        resolve_postgresql_runtime_dsn = getattr(
            benchmark_sql_runtime_core,
            "_resolve_postgresql_runtime_dsn",
        )

        with mock.patch.object(
            benchmark_sql_runtime_core,
            "acquire_postgresql_benchmark_dsn",
        ) as acquire_postgresql_benchmark_dsn:
            resolved_dsn, acquired = resolve_postgresql_runtime_dsn(
                benchmark_sql_runtime_core.POSTGRESQL_ENTRYPOINT,
                "postgresql://configured",
            )

        self.assertEqual(resolved_dsn, "postgresql://configured")
        self.assertFalse(acquired)
        acquire_postgresql_benchmark_dsn.assert_not_called()

    def test_resolve_postgresql_runtime_dsn_acquires_disposable_runtime(self) -> None:
        resolve_postgresql_runtime_dsn = getattr(
            benchmark_sql_runtime_core,
            "_resolve_postgresql_runtime_dsn",
        )

        with mock.patch.object(
            benchmark_sql_runtime_core,
            "acquire_postgresql_benchmark_dsn",
            return_value="postgresql://auto",
        ) as acquire_postgresql_benchmark_dsn:
            resolved_dsn, acquired = resolve_postgresql_runtime_dsn(
                benchmark_sql_runtime_core.POSTGRESQL_ENTRYPOINT,
                "",
            )

        self.assertEqual(resolved_dsn, "postgresql://auto")
        self.assertTrue(acquired)
        acquire_postgresql_benchmark_dsn.assert_called_once_with()

    def test_main_releases_acquired_postgresql_runtime(self) -> None:
        main = getattr(benchmark_sql_runtime_core, "main")

        args = mock.Mock(
            corpus=CORPUS_PATH,
            output=Path("/tmp/runtime.json"),
            iterations=1,
            warmup=0,
            oltp_iterations=None,
            oltp_warmup=None,
            olap_iterations=None,
            olap_warmup=None,
            query_names=["oltp_type1_point_lookup"],
            iteration_progress=False,
            postgres_dsn="",
            index_mode="both",
            node_type_count=3,
            edge_type_count=3,
            nodes_per_type=20,
            edges_per_source=2,
            edge_degree_profile="uniform",
            node_extra_text_property_count=2,
            node_extra_numeric_property_count=6,
            node_extra_boolean_property_count=2,
            edge_extra_text_property_count=1,
            edge_extra_numeric_property_count=3,
            edge_extra_boolean_property_count=1,
            variable_hop_max=2,
            ingest_batch_size=10,
            db_root_dir=None,
        )

        with mock.patch.object(
            benchmark_sql_runtime_core,
            "_parse_args",
            return_value=args,
        ), mock.patch.object(
            benchmark_sql_runtime_core,
            "_resolve_postgresql_runtime_dsn",
            return_value=("postgresql://auto", True),
        ), mock.patch.object(
            benchmark_sql_runtime_core,
            "_load_corpus",
            return_value=[],
        ), mock.patch.object(
            benchmark_sql_runtime_core,
            "_select_queries",
            return_value=[],
        ), mock.patch.object(
            benchmark_sql_runtime_core,
            "_build_graph_schema",
            return_value=(mock.sentinel.graph_schema, mock.sentinel.edge_plans),
        ), mock.patch.object(
            benchmark_sql_runtime_core,
            "_benchmark_result",
            return_value={"workloads": {}},
        ), mock.patch.object(
            benchmark_sql_runtime_core,
            "_build_payload",
            return_value={"status": "ok"},
        ), mock.patch.object(
            benchmark_sql_runtime_core,
            "_write_json_atomic",
        ), mock.patch.object(
            benchmark_sql_runtime_core,
            "_progress",
        ), mock.patch.object(
            benchmark_sql_runtime_core,
            "release_postgresql_benchmark_dsn",
        ) as release_postgresql_benchmark_dsn, mock.patch(
            "builtins.print"
        ):
            result = main(benchmark_sql_runtime_core.POSTGRESQL_ENTRYPOINT)

        self.assertEqual(result, 0)
        release_postgresql_benchmark_dsn.assert_called_once_with()

    def test_filter_duckdb_olap_queries_keeps_duckdb_admitted_queries(
        self,
    ) -> None:
        filter_duckdb_queries = getattr(
            benchmark_sql_runtime_core,
            "_filter_duckdb_queries",
        )
        load_corpus = getattr(benchmark_sql_runtime_core, "_load_corpus")
        select_queries = getattr(benchmark_sql_runtime_core, "_select_queries")

        queries = select_queries(
            load_corpus(CORPUS_PATH),
            [
                "olap_variable_length_reachability",
                "olap_cross_type_edge_rollup",
            ],
        )

        filtered = filter_duckdb_queries(queries)

        self.assertCountEqual(
            [query.name for query in filtered],
            [
                "olap_variable_length_reachability",
                "olap_cross_type_edge_rollup",
            ],
        )

    def test_write_json_atomic_replaces_destination(self) -> None:
        write_json_atomic = getattr(benchmark_sql_runtime_core, "_write_json_atomic")

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
        print_suite = getattr(benchmark_sql_runtime_core, "_print_suite")
        suite = {
            "setup": {
                "connect_ms": 1.0,
                "schema_ms": 2.0,
                "index_ms": 3.0,
                "ingest_ms": 4.0,
                "analyze_ms": 5.0,
            },
            "rss_snapshots_mib": {
                "after_connect": {
                    "client_mib": 10.0,
                    "server_mib": None,
                    "combined_mib": 10.0,
                },
                "after_ingest": {
                    "client_mib": 20.0,
                    "server_mib": 30.0,
                    "combined_mib": 50.0,
                },
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
        self.assertIn(
            "after_ingest(client=20.00 MiB, server=30.00 MiB, combined=50.00 MiB)",
            rendered,
        )
        self.assertIn("db=12.50 MiB", rendered)
        self.assertIn("p99=1.30 ms", rendered)
        self.assertIn("reset_p99=0.60 ms", rendered)
        self.assertIn("end_to_end_p95=3.20 ms", rendered)


if __name__ == "__main__":
    unittest.main()
