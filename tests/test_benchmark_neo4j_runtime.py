from __future__ import annotations

import importlib.util
import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock


REPO_ROOT = Path(__file__).resolve().parents[1]
SCRIPT_PATH = REPO_ROOT / "scripts" / "benchmarks" / "runtime/neo4j.py"
CORPUS_PATH = (
    REPO_ROOT
    / "scripts"
    / "benchmarks"
    / "corpora"
    / "sqlite_runtime_benchmark_corpus.json"
)
MODULE_SPEC = importlib.util.spec_from_file_location(
    "benchmark_neo4j_runtime",
    SCRIPT_PATH,
)
if MODULE_SPEC is None or MODULE_SPEC.loader is None:
    raise RuntimeError(f"Unable to load benchmark script from {SCRIPT_PATH}")
benchmark_neo4j_runtime = importlib.util.module_from_spec(MODULE_SPEC)
sys.modules[MODULE_SPEC.name] = benchmark_neo4j_runtime
MODULE_SPEC.loader.exec_module(benchmark_neo4j_runtime)

SMALL_SCALE = benchmark_neo4j_runtime.RuntimeScale(
    node_type_count=3,
    edge_type_count=3,
    nodes_per_type=20,
    edges_per_source=2,
    ingest_batch_size=10,
    variable_hop_max=2,
)


class BenchmarkNeo4jRuntimeScriptTests(unittest.TestCase):
    def test_write_json_atomic_replaces_destination(self) -> None:
        write_json_atomic = getattr(benchmark_neo4j_runtime, "_write_json_atomic")

        with tempfile.TemporaryDirectory() as temp_dir:
            output_path = Path(temp_dir) / "runtime.json"

            write_json_atomic(output_path, {"status": "running", "value": 1})
            first_payload = json.loads(output_path.read_text(encoding="utf-8"))

            write_json_atomic(output_path, {"status": "completed", "value": 2})
            second_payload = json.loads(output_path.read_text(encoding="utf-8"))

            self.assertEqual(first_payload, {"status": "running", "value": 1})
            self.assertEqual(second_payload, {"status": "completed", "value": 2})
            self.assertEqual(list(Path(temp_dir).glob("runtime.json.*.tmp")), [])

    def test_build_payload_includes_database_versions(self) -> None:
        build_payload = getattr(benchmark_neo4j_runtime, "_build_payload")
        graph_schema, _ = getattr(benchmark_neo4j_runtime, "_build_graph_schema")(
            SMALL_SCALE
        )

        payload = build_payload(
            started_at=benchmark_neo4j_runtime.datetime.now(
                benchmark_neo4j_runtime.UTC
            ),
            database_versions={"neo4j": "5.26.0"},
            neo4j_uri="bolt://127.0.0.1:7687",
            neo4j_database="neo4j",
            neo4j_user="neo4j",
            docker_config=None,
            corpus_path=CORPUS_PATH,
            queries=[],
            scale=SMALL_SCALE,
            graph_schema=graph_schema,
            index_mode="indexed",
            default_iterations=1,
            default_warmup=0,
            oltp_iterations=1,
            oltp_warmup=0,
            olap_iterations=1,
            olap_warmup=0,
            connect_ms=None,
            connect_rss_mib=None,
            result={"workloads": {}},
            failure_count=0,
            status="running",
        )

        self.assertEqual(payload["database_versions"], {"neo4j": "5.26.0"})

    def test_run_query_once_uses_native_neo4j_transaction_timeout(self) -> None:
        run_query_once = getattr(benchmark_neo4j_runtime, "_run_query_once")
        query = benchmark_neo4j_runtime.CorpusQuery(
            name="oltp_type1_point_lookup",
            workload="oltp",
            category="read-point",
            query="MATCH (n) RETURN n",
            backends=("neo4j",),
        )
        transaction = mock.MagicMock()
        transaction.run.return_value = [object()]
        session = mock.MagicMock()
        session.begin_transaction.return_value = transaction
        session.__enter__.return_value = session
        session.__exit__.return_value = None
        driver = mock.MagicMock()
        driver.session.return_value = session

        run_query_once(
            driver,
            database="neo4j",
            query=query,
            timeout_ms=2500.0,
        )

        driver.session.assert_called_once_with(database="neo4j")
        session.begin_transaction.assert_called_once_with(timeout=2.5)
        self.assertEqual(transaction.run.call_args.args[0], query.query)

    def test_measure_query_classifies_neo4j_timeout_errors(self) -> None:
        measure_query = getattr(benchmark_neo4j_runtime, "_measure_query")
        class TimeoutNeo4jError(benchmark_neo4j_runtime.Neo4jError):
            @property
            def code(self) -> str:
                return (
                    "Neo.ClientError.Transaction."
                    "TransactionTimedOutClientConfiguration"
                )

        timeout_exc = TimeoutNeo4jError("transaction timed out")
        query = benchmark_neo4j_runtime.CorpusQuery(
            name="olap_variable_length_grouped_rollup",
            workload="olap",
            category="path-rollup",
            query="MATCH (n) RETURN n",
            backends=("neo4j",),
        )

        with mock.patch.object(
            benchmark_neo4j_runtime,
            "_run_query_once",
            side_effect=timeout_exc,
        ):
            result = measure_query(
                object(),
                database="neo4j",
                index_mode="indexed",
                query=query,
                iterations=5,
                warmup=1,
                progress_label="neo4j/olap",
                iteration_progress=False,
                timeout_ms=10000.0,
            )

        self.assertEqual(result["status"], "timed_out")
        self.assertEqual(result["query_timeout"]["phase"], "warmup")
        self.assertEqual(result["query_timeout"]["iteration"], 1)

    def test_benchmark_result_reports_incremental_progress(self) -> None:
        benchmark_result = getattr(benchmark_neo4j_runtime, "_benchmark_result")
        load_corpus = getattr(benchmark_neo4j_runtime, "_load_corpus")
        select_queries = getattr(benchmark_neo4j_runtime, "_select_queries")

        setup_calls: list[str] = []
        suite_calls: list[tuple[str, str]] = []
        snapshots: list[tuple[dict[str, object], int]] = []

        def fake_setup_mode(
            _driver: object,
            *,
            database: str,
            index_mode: str,
            scale: object,
            graph_schema: object,
            edge_plans: object,
            docker_config: object,
        ) -> dict[str, object]:
            self.assertEqual(database, "neo4j")
            self.assertIsNotNone(scale)
            self.assertIsNotNone(graph_schema)
            self.assertIsNotNone(edge_plans)
            self.assertIsNone(docker_config)
            setup_calls.append(index_mode)
            return {
                "setup_metrics": {
                    "reset_ns": 1,
                    "seed_constraints_ns": 2,
                    "ingest_ns": 3,
                    "index_ns": 4,
                },
                "row_counts": {
                    "node_count": 60,
                    "edge_count": 120,
                    "node_type_count": 3,
                    "edge_type_count": 3,
                },
                "rss_snapshots_mib": {
                    "after_reset": {
                        "client_mib": 10.0,
                        "server_mib": 20.0,
                        "combined_mib": 30.0,
                    },
                    "after_ingest": {
                        "client_mib": 11.0,
                        "server_mib": 21.0,
                        "combined_mib": 32.0,
                    },
                },
                "index_mode": index_mode,
            }

        def fake_run_workload_suite(
            _driver: object,
            *,
            database: str,
            workload: str,
            index_mode: str,
            queries: list[object],
            iterations: int,
            warmup: int,
            setup: dict[str, object],
            docker_config: object,
            iteration_progress: bool,
        ) -> dict[str, object]:
            self.assertEqual(database, "neo4j")
            self.assertIsNone(docker_config)
            self.assertFalse(iteration_progress)
            suite_calls.append((workload, index_mode))
            return {
                "backend": "neo4j",
                "index_mode": index_mode,
                "iterations": iterations,
                "warmup": warmup,
                "query_count": len(queries),
                "pass_count": len(queries),
                "fail_count": 0,
                "setup": {
                    "reset_ms": 0.001,
                    "seed_constraints_ms": 0.002,
                    "ingest_ms": 0.003,
                    "index_ms": 0.004,
                },
                "row_counts": setup["row_counts"],
                "rss_snapshots_mib": setup["rss_snapshots_mib"],
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
                        "backend": "neo4j",
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
            benchmark_neo4j_runtime,
            "_setup_mode",
            side_effect=fake_setup_mode,
        ), mock.patch.object(
            benchmark_neo4j_runtime,
            "_run_workload_suite",
            side_effect=fake_run_workload_suite,
        ):
            result, failure_count = benchmark_result(
                object(),
                database="neo4j",
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
                docker_config=None,
                iteration_progress=False,
                progress_callback=lambda partial, failures: snapshots.append(
                    (json.loads(json.dumps(partial)), failures)
                ),
            )

        self.assertEqual(setup_calls, ["indexed", "unindexed"])
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
        self.assertEqual(snapshots[0][1], 0)
        self.assertIn("oltp", snapshots[1][0]["workloads"])
        self.assertIn("neo4j_indexed", snapshots[1][0]["workloads"]["oltp"])
        self.assertIn("olap", snapshots[2][0]["workloads"])
        self.assertIn("neo4j_indexed", snapshots[2][0]["workloads"]["olap"])
        self.assertIn("neo4j_unindexed", snapshots[3][0]["workloads"]["oltp"])
        self.assertIn("neo4j_unindexed", snapshots[4][0]["workloads"]["olap"])
        self.assertEqual(snapshots[-1], (result, 0))

    def test_run_workload_suite_records_suite_rss_snapshots(self) -> None:
        run_workload_suite = getattr(benchmark_neo4j_runtime, "_run_workload_suite")

        query = benchmark_neo4j_runtime.CorpusQuery(
            name="q",
            workload="oltp",
            category="point_lookup",
            query="MATCH (n) RETURN n LIMIT 1",
            backends=("neo4j",),
            mode="statement",
            mutation=False,
        )
        setup = {
            "setup_metrics": {
                "reset_ns": 1,
                "seed_constraints_ns": 2,
                "ingest_ns": 3,
                "index_ns": 4,
            },
            "row_counts": {
                "node_count": 1,
                "edge_count": 0,
                "node_type_count": 1,
                "edge_type_count": 0,
            },
            "rss_snapshots_mib": {
                "after_reset": {
                    "client_mib": 10.0,
                    "server_mib": 20.0,
                    "combined_mib": 30.0,
                }
            },
        }
        rss_snapshots = iter(
            [
                {"client_mib": 11.0, "server_mib": 21.0, "combined_mib": 32.0},
                {"client_mib": 12.0, "server_mib": 22.0, "combined_mib": 34.0},
            ]
        )

        with mock.patch.object(
            benchmark_neo4j_runtime,
            "_measure_query",
            return_value={
                "name": query.name,
                "workload": query.workload,
                "category": query.category,
                "backend": "neo4j",
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
            benchmark_neo4j_runtime,
            "_capture_neo4j_rss_snapshot",
            side_effect=lambda _docker_config: next(rss_snapshots),
        ):
            result = run_workload_suite(
                object(),
                database="neo4j",
                workload="oltp",
                index_mode="indexed",
                queries=[query],
                iterations=3,
                warmup=1,
                setup=setup,
                docker_config=None,
                iteration_progress=False,
            )

        self.assertNotIn("suite_start", setup["rss_snapshots_mib"])
        self.assertNotIn("suite_complete", setup["rss_snapshots_mib"])
        self.assertEqual(
            result["rss_snapshots_mib"]["suite_start"],
            {"client_mib": 11.0, "server_mib": 21.0, "combined_mib": 32.0},
        )
        self.assertEqual(
            result["rss_snapshots_mib"]["suite_complete"],
            {"client_mib": 12.0, "server_mib": 22.0, "combined_mib": 34.0},
        )


if __name__ == "__main__":
    unittest.main()
