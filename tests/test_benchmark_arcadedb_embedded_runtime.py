from __future__ import annotations

import importlib.util
import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock


REPO_ROOT = Path(__file__).resolve().parents[1]
SCRIPT_PATH = (
    REPO_ROOT
    / "scripts"
    / "benchmarks"
    / "runtime/arcadedb_embedded.py"
)
CORPUS_PATH = (
    REPO_ROOT
    / "scripts"
    / "benchmarks"
    / "corpora"
    / "sqlite_runtime_benchmark_corpus.json"
)
MODULE_SPEC = importlib.util.spec_from_file_location(
    "benchmark_arcadedb_embedded_runtime",
    SCRIPT_PATH,
)
if MODULE_SPEC is None or MODULE_SPEC.loader is None:
    raise RuntimeError(f"Unable to load benchmark script from {SCRIPT_PATH}")
benchmark_arcadedb_embedded_runtime = importlib.util.module_from_spec(MODULE_SPEC)
sys.modules[MODULE_SPEC.name] = benchmark_arcadedb_embedded_runtime
MODULE_SPEC.loader.exec_module(benchmark_arcadedb_embedded_runtime)

SMALL_SCALE = benchmark_arcadedb_embedded_runtime.RuntimeScale(
    node_type_count=3,
    edge_type_count=3,
    nodes_per_type=20,
    edges_per_source=2,
    ingest_batch_size=10,
    variable_hop_max=2,
)


class BenchmarkArcadeDBEmbeddedRuntimeScriptTests(unittest.TestCase):
    def test_write_json_atomic_replaces_destination(self) -> None:
        write_json_atomic = getattr(
            benchmark_arcadedb_embedded_runtime,
            "_write_json_atomic",
        )

        with tempfile.TemporaryDirectory() as temp_dir:
            output_path = Path(temp_dir) / "runtime.json"

            write_json_atomic(output_path, {"status": "running", "value": 1})
            first_payload = json.loads(output_path.read_text(encoding="utf-8"))

            write_json_atomic(output_path, {"status": "completed", "value": 2})
            second_payload = json.loads(output_path.read_text(encoding="utf-8"))

            self.assertEqual(first_payload, {"status": "running", "value": 1})
            self.assertEqual(second_payload, {"status": "completed", "value": 2})
            self.assertEqual(list(Path(temp_dir).glob("runtime.json.*.tmp")), [])

    def test_arcadedb_version_prefers_installed_distribution(self) -> None:
        arcadedb_version = getattr(
            benchmark_arcadedb_embedded_runtime,
            "_arcadedb_version",
        )
        with mock.patch.object(
            benchmark_arcadedb_embedded_runtime.importlib.metadata,
            "version",
            return_value="26.4.1.dev3",
        ):
            self.assertEqual(arcadedb_version(), "26.4.1.dev3")

    def test_open_arcadedb_opens_existing_database(self) -> None:
        open_arcadedb = getattr(
            benchmark_arcadedb_embedded_runtime,
            "_open_arcadedb",
        )

        with mock.patch.object(
            benchmark_arcadedb_embedded_runtime.arcadedb,
            "database_exists",
            return_value=True,
        ), mock.patch.object(
            benchmark_arcadedb_embedded_runtime.arcadedb,
            "open_database",
            return_value="opened-db",
        ) as open_database, mock.patch.object(
            benchmark_arcadedb_embedded_runtime.arcadedb,
            "create_database",
        ) as create_database:
            result = open_arcadedb(Path("/tmp/runtime.arcadedb"))

        self.assertEqual(result, "opened-db")
        open_database.assert_called_once_with("/tmp/runtime.arcadedb")
        create_database.assert_not_called()

    def test_open_arcadedb_creates_missing_database(self) -> None:
        open_arcadedb = getattr(
            benchmark_arcadedb_embedded_runtime,
            "_open_arcadedb",
        )

        with mock.patch.object(
            benchmark_arcadedb_embedded_runtime.arcadedb,
            "database_exists",
            return_value=False,
        ), mock.patch.object(
            benchmark_arcadedb_embedded_runtime.arcadedb,
            "create_database",
            return_value="created-db",
        ) as create_database, mock.patch.object(
            benchmark_arcadedb_embedded_runtime.arcadedb,
            "open_database",
        ) as open_database:
            result = open_arcadedb(Path("/tmp/runtime.arcadedb"))

        self.assertEqual(result, "created-db")
        create_database.assert_called_once_with("/tmp/runtime.arcadedb")
        open_database.assert_not_called()

    def test_rewrite_arcadedb_query_injects_ids_for_mutations(self) -> None:
        rewrite_query = getattr(
            benchmark_arcadedb_embedded_runtime,
            "_rewrite_arcadedb_query",
        )
        fixture = benchmark_arcadedb_embedded_runtime.ArcadeDBFixture(
            work_dir=mock.Mock(),
            db_path=Path("/tmp/runtime.arcadedb"),
            database=mock.Mock(),
            setup_metrics={},
            row_counts={"node_count": 60, "edge_count": 120},
            rss_snapshots_mib={},
            db_size_mib=0.0,
            wal_size_mib=0.0,
            index_mode="indexed",
        )

        create_query = benchmark_arcadedb_embedded_runtime.CorpusQuery(
            name="oltp_create_type1_node",
            workload="oltp",
            category="create-node",
            query=(
                "CREATE (n:NodeType01 {name: 'created', age: 44, "
                "score: 99.5, active: true})"
            ),
            backends=("sqlite",),
            mode="program",
            mutation=True,
        )
        link_query = benchmark_arcadedb_embedded_runtime.CorpusQuery(
            name="oltp_program_create_and_link",
            workload="oltp",
            category="create-program",
            query=(
                "MATCH (a:NodeType01 {name: 'n1'}) "
                "CREATE (a)-[:EdgeType01]->(:NodeType01 "
                "{name: 'peer', age: 28, score: 4.5, active: true})"
            ),
            backends=("sqlite",),
            mode="program",
            mutation=True,
        )

        self.assertIn(
            "{id: 61, name:",
            rewrite_query(
                fixture,
                create_query,
            ),
        )
        self.assertIn(
            "{id: 62, name:",
            rewrite_query(
                fixture,
                link_query,
            ),
        )

    def test_build_payload_includes_database_versions_and_index_mode(self) -> None:
        build_payload = getattr(
            benchmark_arcadedb_embedded_runtime,
            "_build_payload",
        )
        graph_schema, _ = getattr(
            benchmark_arcadedb_embedded_runtime,
            "_build_graph_schema",
        )(SMALL_SCALE)

        payload = build_payload(
            started_at=benchmark_arcadedb_embedded_runtime.datetime.now(
                benchmark_arcadedb_embedded_runtime.UTC
            ),
            database_versions={"arcadedb-embedded": "26.4.1.dev3"},
            corpus_path=CORPUS_PATH,
            queries=[],
            scale=SMALL_SCALE,
            graph_schema=graph_schema,
            index_mode="both",
            default_iterations=1,
            default_warmup=0,
            oltp_iterations=1,
            oltp_warmup=0,
            olap_iterations=1,
            olap_warmup=0,
            db_root_dir=None,
            result={"workloads": {}},
            failure_count=0,
            status="running",
        )

        self.assertEqual(
            payload["database_versions"],
            {"arcadedb-embedded": "26.4.1.dev3"},
        )
        self.assertEqual(payload["index_mode"], "both")

    def test_arcadedb_timeout_result_uses_next_iteration_after_progress(self) -> None:
        timeout_result = getattr(
            benchmark_arcadedb_embedded_runtime,
            "_arcadedb_timeout_result",
        )
        query = benchmark_arcadedb_embedded_runtime.CorpusQuery(
            name="olap_variable_length_grouped_rollup",
            workload="olap",
            category="path-rollup",
            query="MATCH (n) RETURN n",
            backends=("arcadedb-embedded",),
        )

        result = timeout_result(
            query=query,
            index_mode="indexed",
            timeout_ms=10000.0,
            last_progress_event={"phase": "warmup", "iteration": 2},
            warmup=2,
        )

        self.assertEqual(result["status"], "timed_out")
        self.assertEqual(result["query_timeout"]["phase"], "iteration")
        self.assertEqual(result["query_timeout"]["iteration"], 1)

    def test_arcadedb_timeout_result_treats_ready_as_before_warmup(self) -> None:
        timeout_result = getattr(
            benchmark_arcadedb_embedded_runtime,
            "_arcadedb_timeout_result",
        )
        query = benchmark_arcadedb_embedded_runtime.CorpusQuery(
            name="oltp_type1_point_lookup",
            workload="oltp",
            category="read-point",
            query="MATCH (n) RETURN n",
            backends=("arcadedb-embedded",),
        )

        result = timeout_result(
            query=query,
            index_mode="indexed",
            timeout_ms=100.0,
            last_progress_event={"phase": "ready", "iteration": 0},
            warmup=5,
        )

        self.assertEqual(result["status"], "timed_out")
        self.assertEqual(result["query_timeout"]["phase"], "warmup")
        self.assertEqual(result["query_timeout"]["iteration"], 1)

    def test_arcadedb_worker_writes_passed_result_and_progress(self) -> None:
        run_worker = getattr(
            benchmark_arcadedb_embedded_runtime,
            "_run_arcadedb_query_worker",
        )
        query = benchmark_arcadedb_embedded_runtime.CorpusQuery(
            name="oltp_type1_point_lookup",
            workload="oltp",
            category="read-point",
            query="MATCH (n) RETURN n",
            backends=("arcadedb-embedded",),
        )

        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            spec_path = temp_path / "spec.json"
            progress_path = temp_path / "progress.jsonl"
            result_path = temp_path / "result.json"
            spec_path.write_text(
                json.dumps(
                    {
                        "db_path": str(temp_path / "runtime.arcadedb"),
                        "row_counts": {"node_count": 60, "edge_count": 120},
                        "index_mode": "indexed",
                        "query": {
                            "name": query.name,
                            "workload": query.workload,
                            "category": query.category,
                            "query": query.query,
                            "backends": list(query.backends),
                            "mode": query.mode,
                            "mutation": query.mutation,
                        },
                        "warmup": 2,
                        "iterations": 3,
                        "progress_path": str(progress_path),
                        "result_path": str(result_path),
                    }
                ),
                encoding="utf-8",
            )

            fixture = mock.Mock(database=mock.Mock(close=mock.Mock()))
            with mock.patch.object(
                benchmark_arcadedb_embedded_runtime,
                "_arcadedb_worker_fixture",
                return_value=fixture,
            ), mock.patch.object(
                benchmark_arcadedb_embedded_runtime,
                "_run_query_once",
                return_value={
                    "execute_ns": 1_000_000,
                    "end_to_end_ns": 1_000_000,
                    "reset_ns": 0,
                },
            ):
                exit_code = run_worker(spec_path)

            self.assertEqual(exit_code, 0)
            events = [
                json.loads(line)
                for line in progress_path.read_text(encoding="utf-8").splitlines()
                if line.strip()
            ]
            self.assertEqual(
                events,
                [
                    {"phase": "ready", "iteration": 0},
                    {"phase": "warmup", "iteration": 1},
                    {"phase": "warmup", "iteration": 2},
                    {"phase": "iteration", "iteration": 1},
                    {"phase": "iteration", "iteration": 2},
                    {"phase": "iteration", "iteration": 3},
                ],
            )
            result = json.loads(result_path.read_text(encoding="utf-8"))
            self.assertEqual(result["status"], "passed")
            self.assertEqual(result["execute"]["p50_ms"], 1.0)

    def test_arcadedb_worker_process_launches_from_repo_root(self) -> None:
        measure_query = getattr(
            benchmark_arcadedb_embedded_runtime,
            "_measure_query_in_arcadedb_worker",
        )
        query = benchmark_arcadedb_embedded_runtime.CorpusQuery(
            name="oltp_type1_point_lookup",
            workload="oltp",
            category="read-point",
            query="MATCH (n) RETURN n",
            backends=("arcadedb-embedded",),
        )

        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            fixture = benchmark_arcadedb_embedded_runtime.ArcadeDBFixture(
                work_dir=mock.Mock(path=temp_path),
                db_path=temp_path / "runtime.arcadedb",
                database=mock.Mock(),
                setup_metrics={},
                row_counts={"node_count": 60, "edge_count": 120},
                rss_snapshots_mib={},
                db_size_mib=0.0,
                wal_size_mib=0.0,
                index_mode="indexed",
            )
            result_path = temp_path / f"{query.name}.worker-result.json"
            result_path.write_text(
                json.dumps(
                    {
                        "name": query.name,
                        "workload": query.workload,
                        "category": query.category,
                        "backend": "arcadedb-embedded",
                        "index_mode": "indexed",
                        "mode": query.mode,
                        "mutation": query.mutation,
                        "status": "passed",
                        "execute": {"min_ms": 1.0, "mean_ms": 1.0, "p50_ms": 1.0, "p95_ms": 1.0, "p99_ms": 1.0, "max_ms": 1.0},
                        "end_to_end": {"min_ms": 1.0, "mean_ms": 1.0, "p50_ms": 1.0, "p95_ms": 1.0, "p99_ms": 1.0, "max_ms": 1.0},
                        "reset": {"min_ms": 0.0, "mean_ms": 0.0, "p50_ms": 0.0, "p95_ms": 0.0, "p99_ms": 0.0, "max_ms": 0.0},
                    }
                ),
                encoding="utf-8",
            )
            process = mock.Mock()
            process.poll.return_value = 0
            process.communicate.return_value = ("", "")

            with mock.patch.object(
                benchmark_arcadedb_embedded_runtime.subprocess,
                "Popen",
                return_value=process,
            ) as popen:
                result = measure_query(
                    fixture,
                    query=query,
                    iterations=1,
                    warmup=0,
                    timeout_ms=1000.0,
                )

            self.assertEqual(result["status"], "passed")
            popen.assert_called_once()
            self.assertEqual(
                popen.call_args.args[0],
                [
                    sys.executable,
                    "-m",
                    "scripts.benchmarks.runtime.arcadedb_embedded",
                    "--query-worker-spec",
                    str(temp_path / f"{query.name}.worker-spec.json"),
                ],
            )
            self.assertEqual(
                popen.call_args.kwargs["cwd"],
                str(benchmark_arcadedb_embedded_runtime.REPO_ROOT),
            )

    def test_arcadedb_worker_returns_result_before_process_exit(self) -> None:
        measure_query = getattr(
            benchmark_arcadedb_embedded_runtime,
            "_measure_query_in_arcadedb_worker",
        )
        query = benchmark_arcadedb_embedded_runtime.CorpusQuery(
            name="oltp_type1_point_lookup",
            workload="oltp",
            category="read-point",
            query="MATCH (n) RETURN n",
            backends=("arcadedb-embedded",),
        )

        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            fixture = benchmark_arcadedb_embedded_runtime.ArcadeDBFixture(
                work_dir=mock.Mock(path=temp_path),
                db_path=temp_path / "runtime.arcadedb",
                database=mock.Mock(),
                setup_metrics={},
                row_counts={"node_count": 60, "edge_count": 120},
                rss_snapshots_mib={},
                db_size_mib=0.0,
                wal_size_mib=0.0,
                index_mode="indexed",
            )
            result_payload = {
                "name": query.name,
                "workload": query.workload,
                "category": query.category,
                "backend": "arcadedb-embedded",
                "index_mode": "indexed",
                "mode": query.mode,
                "mutation": query.mutation,
                "status": "passed",
                "execute": {"min_ms": 1.0, "mean_ms": 1.0, "p50_ms": 1.0, "p95_ms": 1.0, "p99_ms": 1.0, "max_ms": 1.0},
                "end_to_end": {"min_ms": 1.0, "mean_ms": 1.0, "p50_ms": 1.0, "p95_ms": 1.0, "p99_ms": 1.0, "max_ms": 1.0},
                "reset": {"min_ms": 0.0, "mean_ms": 0.0, "p50_ms": 0.0, "p95_ms": 0.0, "p99_ms": 0.0, "max_ms": 0.0},
            }
            result_path = temp_path / f"{query.name}.worker-result.json"
            result_path.write_text(json.dumps(result_payload), encoding="utf-8")
            process = mock.Mock()
            process.poll.return_value = None
            process.wait.return_value = 0

            with mock.patch.object(
                benchmark_arcadedb_embedded_runtime.subprocess,
                "Popen",
                return_value=process,
            ), mock.patch.object(
                benchmark_arcadedb_embedded_runtime,
                "_read_arcadedb_worker_progress",
                return_value=([{"phase": "ready", "iteration": 0}], 0),
            ):
                result = measure_query(
                    fixture,
                    query=query,
                    iterations=1,
                    warmup=0,
                    timeout_ms=1000.0,
                )

            self.assertEqual(result["status"], "passed")
            process.wait.assert_called_once_with(
                timeout=benchmark_arcadedb_embedded_runtime.ARCADEDB_WORKER_SHUTDOWN_TIMEOUT_S
            )
            process.kill.assert_not_called()

    def test_arcadedb_gav_statement_covers_runtime_schema(self) -> None:
        gav_statement = getattr(
            benchmark_arcadedb_embedded_runtime,
            "_arcadedb_gav_statement",
        )
        graph_schema, _ = getattr(
            benchmark_arcadedb_embedded_runtime,
            "_build_graph_schema",
        )(SMALL_SCALE)

        statement = gav_statement(graph_schema)

        self.assertIn("CREATE GRAPH ANALYTICAL VIEW cypherglot_olap", statement)
        self.assertIn("VERTEX TYPES (NodeType01, NodeType02, NodeType03)", statement)
        self.assertIn("EDGE TYPES (EdgeType01, EdgeType02, EdgeType03)", statement)
        self.assertIn("PROPERTIES (id, name, age, active, score)", statement)
        self.assertIn("EDGE PROPERTIES (rank, active, score)", statement)
        self.assertTrue(statement.endswith("UPDATE MODE OFF"))

    def test_benchmark_result_reports_incremental_progress(self) -> None:
        benchmark_result = getattr(
            benchmark_arcadedb_embedded_runtime,
            "_benchmark_result",
        )
        load_corpus = getattr(benchmark_arcadedb_embedded_runtime, "_load_corpus")
        select_queries = getattr(
            benchmark_arcadedb_embedded_runtime,
            "_select_queries",
        )

        suite_calls: list[tuple[str, str]] = []
        snapshots: list[tuple[dict[str, object], int]] = []
        sqlite_source_mock = mock.Mock()

        def fake_run_workload_suite(
            *,
            workload: str,
            index_mode: str,
            queries: list[object],
            iterations: int,
            warmup: int,
            graph_schema: object,
            sqlite_source: object,
            ingest_batch_size: int,
            db_root_dir: Path | None,
            iteration_progress: bool,
        ) -> dict[str, object]:
            self.assertIsNotNone(graph_schema)
            self.assertIs(sqlite_source, sqlite_source_mock)
            self.assertEqual(ingest_batch_size, SMALL_SCALE.ingest_batch_size)
            self.assertFalse(iteration_progress)
            self.assertIsNone(db_root_dir)
            suite_calls.append((workload, index_mode))
            return {
                "backend": "arcadedb-embedded",
                "index_mode": index_mode,
                "iterations": iterations,
                "warmup": warmup,
                "query_count": len(queries),
                "pass_count": len(queries),
                "fail_count": 0,
                "setup": {
                    "connect_ms": 1.0,
                    "schema_ms": 2.0,
                    "ingest_ms": 3.0,
                    "index_ms": 4.0 if index_mode == "indexed" else 0.0,
                    "gav_ms": 5.0 if workload == "olap" else 0.0,
                    "checkpoint_ms": 0.0,
                },
                "row_counts": {
                    "node_count": 60,
                    "edge_count": 120,
                    "node_type_count": 3,
                    "edge_type_count": 3,
                },
                "rss_snapshots_mib": {
                    "after_connect": {
                        "client_mib": 10.0,
                        "server_mib": None,
                        "combined_mib": 10.0,
                    }
                },
                "storage": {"db_size_mib": 5.0, "wal_size_mib": 0.0},
                "db_path": "/tmp/runtime.arcadedb",
                "execute": {
                    "mean_of_mean_ms": 1.0,
                    "mean_of_p50_ms": 1.0,
                    "mean_of_p95_ms": 1.0,
                    "mean_of_p99_ms": 1.0,
                },
                "end_to_end": {
                    "mean_of_mean_ms": 2.0,
                    "mean_of_p50_ms": 2.0,
                    "mean_of_p95_ms": 2.0,
                    "mean_of_p99_ms": 2.0,
                },
                "reset": {
                    "mean_of_mean_ms": 0.0,
                    "mean_of_p50_ms": 0.0,
                    "mean_of_p95_ms": 0.0,
                    "mean_of_p99_ms": 0.0,
                },
                "queries": [
                    {
                        "name": query.name,
                        "workload": query.workload,
                        "category": query.category,
                        "backend": "arcadedb-embedded",
                        "index_mode": index_mode,
                        "mode": query.mode,
                        "mutation": query.mutation,
                        "status": "passed",
                        "execute": {
                            "min_ms": 1.0,
                            "mean_ms": 1.0,
                            "p50_ms": 1.0,
                            "p95_ms": 1.0,
                            "p99_ms": 1.0,
                            "max_ms": 1.0,
                        },
                        "end_to_end": {
                            "min_ms": 2.0,
                            "mean_ms": 2.0,
                            "p50_ms": 2.0,
                            "p95_ms": 2.0,
                            "p99_ms": 2.0,
                            "max_ms": 2.0,
                        },
                        "reset": {
                            "min_ms": 0.0,
                            "mean_ms": 0.0,
                            "p50_ms": 0.0,
                            "p95_ms": 0.0,
                            "p99_ms": 0.0,
                            "max_ms": 0.0,
                        },
                    }
                    for query in queries
                ],
            }

        with mock.patch.object(
            benchmark_arcadedb_embedded_runtime,
            "_prepare_generated_graph_fixture",
            return_value=sqlite_source_mock,
        ), mock.patch.object(
            benchmark_arcadedb_embedded_runtime,
            "_run_workload_suite",
            side_effect=fake_run_workload_suite,
        ):
            result, failure_count = benchmark_result(
                queries=select_queries(
                    load_corpus(CORPUS_PATH),
                    ["oltp_type1_point_lookup", "olap_type1_age_rollup"],
                ),
                iterations=1,
                warmup=0,
                oltp_iterations=None,
                oltp_warmup=None,
                olap_iterations=None,
                olap_warmup=None,
                scale=SMALL_SCALE,
                index_mode="both",
                db_root_dir=None,
                iteration_progress=False,
                progress_callback=lambda partial, failures: snapshots.append(
                    (json.loads(json.dumps(partial)), failures)
                ),
            )

        sqlite_source_mock.close.assert_called_once_with()
        self.assertEqual(
            suite_calls,
            [
                ("oltp", "indexed"),
                ("olap", "indexed"),
                ("oltp", "unindexed"),
                ("olap", "unindexed"),
            ],
        )
        self.assertEqual(failure_count, 0)
        self.assertGreaterEqual(len(snapshots), 5)
        self.assertEqual(snapshots[0][0]["workloads"], {})
        self.assertTrue(snapshots[0][0]["token_map"])
        self.assertIn("arcadedb_embedded_indexed", snapshots[1][0]["workloads"]["oltp"])
        self.assertIn("arcadedb_embedded_indexed", snapshots[2][0]["workloads"]["olap"])
        self.assertIn(
            "arcadedb_embedded_unindexed",
            snapshots[3][0]["workloads"]["oltp"],
        )
        self.assertIn(
            "arcadedb_embedded_unindexed",
            snapshots[4][0]["workloads"]["olap"],
        )
        self.assertEqual(snapshots[-1], (result, 0))

    def test_run_workload_suite_records_suite_rss_snapshots(self) -> None:
        run_workload_suite = getattr(
            benchmark_arcadedb_embedded_runtime,
            "_run_workload_suite",
        )

        query = benchmark_arcadedb_embedded_runtime.CorpusQuery(
            name="q",
            workload="oltp",
            category="point_lookup",
            query="MATCH (n) RETURN n LIMIT 1",
            backends=("arcadedb-embedded",),
            mode="statement",
            mutation=False,
        )
        fixture = mock.Mock(
            setup_metrics={
                "connect_ns": 1_000_000,
                "schema_ns": 2_000_000,
                "ingest_ns": 3_000_000,
                "index_ns": 4_000_000,
                "gav_ns": 5_000_000,
                "checkpoint_ns": 0,
            },
            row_counts={
                "node_count": 1,
                "edge_count": 0,
                "node_type_count": 1,
                "edge_type_count": 0,
            },
            rss_snapshots_mib={
                "after_connect": {
                    "client_mib": 10.0,
                    "server_mib": None,
                    "combined_mib": 10.0,
                }
            },
            db_size_mib=5.0,
            wal_size_mib=0.0,
            db_path=Path("/tmp/runtime.arcadedb"),
            index_mode="indexed",
        )
        rss_snapshots = iter(
            [
                {"client_mib": 11.0, "server_mib": None, "combined_mib": 11.0},
                {"client_mib": 12.0, "server_mib": None, "combined_mib": 12.0},
            ]
        )

        with mock.patch.object(
            benchmark_arcadedb_embedded_runtime,
            "_prepare_arcadedb_fixture",
            return_value=fixture,
        ), mock.patch.object(
            benchmark_arcadedb_embedded_runtime,
            "_measure_query",
            return_value={
                "name": query.name,
                "workload": query.workload,
                "category": query.category,
                "backend": "arcadedb-embedded",
                "index_mode": "indexed",
                "mode": query.mode,
                "mutation": query.mutation,
                "status": "passed",
                "execute": {
                    "min_ms": 1.0,
                    "mean_ms": 1.0,
                    "p50_ms": 1.0,
                    "p95_ms": 1.0,
                    "p99_ms": 1.0,
                    "max_ms": 1.0,
                },
                "end_to_end": {
                    "min_ms": 2.0,
                    "mean_ms": 2.0,
                    "p50_ms": 2.0,
                    "p95_ms": 2.0,
                    "p99_ms": 2.0,
                    "max_ms": 2.0,
                },
                "reset": {
                    "min_ms": 0.0,
                    "mean_ms": 0.0,
                    "p50_ms": 0.0,
                    "p95_ms": 0.0,
                    "p99_ms": 0.0,
                    "max_ms": 0.0,
                },
            },
        ), mock.patch.object(
            benchmark_arcadedb_embedded_runtime,
            "_capture_rss_snapshot",
            side_effect=lambda *, backend: next(rss_snapshots),
        ):
            result = run_workload_suite(
                workload="oltp",
                index_mode="indexed",
                queries=[query],
                iterations=3,
                warmup=1,
                graph_schema=mock.Mock(),
                sqlite_source=mock.Mock(),
                ingest_batch_size=10,
                db_root_dir=None,
                iteration_progress=False,
            )

        fixture.close.assert_called_once_with()
        self.assertNotIn("suite_start", fixture.rss_snapshots_mib)
        self.assertNotIn("suite_complete", fixture.rss_snapshots_mib)
        self.assertEqual(
            result["rss_snapshots_mib"]["suite_start"],
            {"client_mib": 11.0, "server_mib": None, "combined_mib": 11.0},
        )
        self.assertEqual(
            result["rss_snapshots_mib"]["suite_complete"],
            {"client_mib": 12.0, "server_mib": None, "combined_mib": 12.0},
        )
        self.assertEqual(result["setup"]["gav_ms"], 5.0)


if __name__ == "__main__":
    unittest.main()
