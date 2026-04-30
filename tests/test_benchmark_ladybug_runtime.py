from __future__ import annotations

import importlib.util
import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock


REPO_ROOT = Path(__file__).resolve().parents[1]
SCRIPT_PATH = REPO_ROOT / "scripts" / "benchmarks" / "runtime/ladybug.py"
CORPUS_PATH = (
    REPO_ROOT
    / "scripts"
    / "benchmarks"
    / "corpora"
    / "sqlite_runtime_benchmark_corpus.json"
)
MODULE_SPEC = importlib.util.spec_from_file_location(
    "benchmark_ladybug_runtime",
    SCRIPT_PATH,
)
if MODULE_SPEC is None or MODULE_SPEC.loader is None:
    raise RuntimeError(f"Unable to load benchmark script from {SCRIPT_PATH}")
benchmark_ladybug_runtime = importlib.util.module_from_spec(MODULE_SPEC)
sys.modules[MODULE_SPEC.name] = benchmark_ladybug_runtime
MODULE_SPEC.loader.exec_module(benchmark_ladybug_runtime)

SMALL_SCALE = benchmark_ladybug_runtime.RuntimeScale(
    node_type_count=3,
    edge_type_count=3,
    nodes_per_type=20,
    edges_per_source=2,
    ingest_batch_size=10,
    variable_hop_max=2,
)


class BenchmarkLadybugRuntimeScriptTests(unittest.TestCase):
    def test_write_json_atomic_replaces_destination(self) -> None:
        write_json_atomic = getattr(benchmark_ladybug_runtime, "_write_json_atomic")

        with tempfile.TemporaryDirectory() as temp_dir:
            output_path = Path(temp_dir) / "runtime.json"

            write_json_atomic(output_path, {"status": "running", "value": 1})
            first_payload = json.loads(output_path.read_text(encoding="utf-8"))

            write_json_atomic(output_path, {"status": "completed", "value": 2})
            second_payload = json.loads(output_path.read_text(encoding="utf-8"))

            self.assertEqual(first_payload, {"status": "running", "value": 1})
            self.assertEqual(second_payload, {"status": "completed", "value": 2})
            self.assertEqual(list(Path(temp_dir).glob("runtime.json.*.tmp")), [])

    def test_ladybug_copy_column_names_maps_edge_columns(self) -> None:
        copy_column_names = getattr(
            benchmark_ladybug_runtime,
            "_ladybug_copy_column_names",
        )

        self.assertEqual(
            copy_column_names(["from_id", "to_id", "rank", "active"]),
            ["from", "to", "rank", "active"],
        )

    def test_build_payload_includes_database_versions(self) -> None:
        build_payload = getattr(benchmark_ladybug_runtime, "_build_payload")
        graph_schema, _ = getattr(benchmark_ladybug_runtime, "_build_graph_schema")(
            SMALL_SCALE
        )

        payload = build_payload(
            started_at=benchmark_ladybug_runtime.datetime.now(
                benchmark_ladybug_runtime.UTC
            ),
            database_versions={"ladybug": "0.15.3"},
            corpus_path=CORPUS_PATH,
            queries=[],
            scale=SMALL_SCALE,
            graph_schema=graph_schema,
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

        self.assertEqual(payload["database_versions"], {"ladybug": "0.15.3"})
        self.assertEqual(payload["index_mode"], "unindexed")

    def test_run_query_once_sets_native_ladybug_query_timeout(self) -> None:
        run_query_once = getattr(benchmark_ladybug_runtime, "_run_query_once")
        query = benchmark_ladybug_runtime.CorpusQuery(
            name="oltp_type1_point_lookup",
            workload="oltp",
            category="read-point",
            query="MATCH (n) RETURN n",
            backends=("ladybug",),
        )
        connection = mock.MagicMock()
        connection.execute.return_value = [object()]
        fixture = mock.Mock(connection=connection)

        with mock.patch.object(
            benchmark_ladybug_runtime,
            "_rewrite_ladybug_query",
            return_value=query.query,
        ):
            run_query_once(
                fixture,
                query=query,
                timeout_ms=2500.0,
            )

        connection.set_query_timeout.assert_called_once_with(2500)

    def test_measure_query_classifies_native_ladybug_timeout(self) -> None:
        measure_query = getattr(benchmark_ladybug_runtime, "_measure_query")
        query = benchmark_ladybug_runtime.CorpusQuery(
            name="olap_variable_length_grouped_rollup",
            workload="olap",
            category="path-rollup",
            query="MATCH (n) RETURN n",
            backends=("ladybug",),
        )

        with mock.patch.object(
            benchmark_ladybug_runtime,
            "_run_query_once",
            side_effect=RuntimeError("query timed out"),
        ):
            result = measure_query(
                mock.Mock(),
                query=query,
                iterations=5,
                warmup=1,
                progress_label="ladybug/olap",
                iteration_progress=False,
                timeout_ms=10000.0,
            )

        self.assertEqual(result["status"], "timed_out")
        self.assertEqual(result["query_timeout"]["phase"], "warmup")
        self.assertEqual(result["query_timeout"]["iteration"], 1)

    def test_benchmark_result_reports_incremental_progress(self) -> None:
        benchmark_result = getattr(benchmark_ladybug_runtime, "_benchmark_result")
        load_corpus = getattr(benchmark_ladybug_runtime, "_load_corpus")
        select_queries = getattr(benchmark_ladybug_runtime, "_select_queries")

        suite_calls: list[str] = []
        snapshots: list[tuple[dict[str, object], int]] = []
        sqlite_source_mock = mock.Mock()

        def fake_run_workload_suite(
            *,
            workload: str,
            queries: list[object],
            iterations: int,
            warmup: int,
            graph_schema: object,
            sqlite_source: object,
            db_root_dir: Path | None,
            iteration_progress: bool,
        ) -> dict[str, object]:
            self.assertIsNotNone(graph_schema)
            self.assertIs(sqlite_source, sqlite_source_mock)
            self.assertFalse(iteration_progress)
            self.assertIsNone(db_root_dir)
            suite_calls.append(workload)
            return {
                "backend": "ladybug",
                "index_mode": "unindexed",
                "iterations": iterations,
                "warmup": warmup,
                "query_count": len(queries),
                "pass_count": len(queries),
                "fail_count": 0,
                "setup": {
                    "connect_ms": 1.0,
                    "schema_ms": 2.0,
                    "ingest_ms": 3.0,
                    "index_ms": 0.0,
                    "checkpoint_ms": 4.0,
                },
                "row_counts": {"node_count": 60, "edge_count": 120},
                "rss_snapshots_mib": {
                    "after_connect": {
                        "client_mib": 10.0,
                        "server_mib": None,
                        "combined_mib": 10.0,
                    }
                },
                "storage": {"db_size_mib": 5.0, "wal_size_mib": 0.0},
                "db_path": "/tmp/runtime.lbug",
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
                        "backend": "ladybug",
                        "index_mode": "unindexed",
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
            benchmark_ladybug_runtime,
            "_prepare_generated_graph_fixture",
            return_value=sqlite_source_mock,
        ), mock.patch.object(
            benchmark_ladybug_runtime,
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
                db_root_dir=None,
                iteration_progress=False,
                progress_callback=lambda partial, failures: snapshots.append(
                    (json.loads(json.dumps(partial)), failures)
                ),
            )

        sqlite_source_mock.close.assert_called_once_with()
        self.assertEqual(suite_calls, ["oltp", "olap"])
        self.assertEqual(failure_count, 0)
        self.assertGreaterEqual(len(snapshots), 3)
        self.assertEqual(snapshots[0][0]["workloads"], {})
        self.assertTrue(snapshots[0][0]["token_map"])
        self.assertIn("ladybug_unindexed", snapshots[1][0]["workloads"]["oltp"])
        self.assertIn("ladybug_unindexed", snapshots[2][0]["workloads"]["olap"])
        self.assertEqual(snapshots[-1], (result, 0))

    def test_run_workload_suite_records_suite_rss_snapshots(self) -> None:
        run_workload_suite = getattr(benchmark_ladybug_runtime, "_run_workload_suite")

        query = benchmark_ladybug_runtime.CorpusQuery(
            name="q",
            workload="oltp",
            category="point_lookup",
            query="MATCH (n) RETURN n LIMIT 1",
            backends=("ladybug",),
            mode="statement",
            mutation=False,
        )
        fixture = mock.Mock(
            setup_metrics={
                "connect_ns": 1_000_000,
                "schema_ns": 2_000_000,
                "ingest_ns": 3_000_000,
                "index_ns": 0,
                "checkpoint_ns": 4_000_000,
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
            db_size_mib=12.0,
            wal_size_mib=0.0,
            db_path=Path("/tmp/runtime.lbug"),
        )
        rss_snapshots = iter(
            [
                {"client_mib": 11.0, "server_mib": None, "combined_mib": 11.0},
                {"client_mib": 12.0, "server_mib": None, "combined_mib": 12.0},
            ]
        )

        with mock.patch.object(
            benchmark_ladybug_runtime,
            "_prepare_ladybug_fixture",
            return_value=fixture,
        ), mock.patch.object(
            benchmark_ladybug_runtime,
            "_measure_query",
            return_value={
                "name": query.name,
                "workload": query.workload,
                "category": query.category,
                "backend": "ladybug",
                "index_mode": "unindexed",
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
            benchmark_ladybug_runtime,
            "_capture_rss_snapshot",
            side_effect=lambda **_kwargs: next(rss_snapshots),
        ):
            result = run_workload_suite(
                workload="oltp",
                queries=[query],
                iterations=3,
                warmup=1,
                graph_schema=mock.sentinel.graph_schema,
                sqlite_source=mock.sentinel.sqlite_source,
                db_root_dir=None,
                iteration_progress=False,
            )

        fixture.close.assert_called_once_with()
        self.assertEqual(result["pass_count"], 1)
        self.assertEqual(result["fail_count"], 0)
        self.assertIn("suite_start", result["rss_snapshots_mib"])
        self.assertIn("suite_complete", result["rss_snapshots_mib"])
        self.assertEqual(result["setup"]["connect_ms"], 1.0)
        self.assertEqual(result["setup"]["checkpoint_ms"], 4.0)
        self.assertEqual(result["storage"]["db_size_mib"], 12.0)


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
