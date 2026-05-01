from __future__ import annotations

import importlib.util
import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch


REPO_ROOT = Path(__file__).resolve().parents[1]
SCRIPT_PATH = REPO_ROOT / "scripts" / "benchmarks" / "runtime/summarize_results.py"
MODULE_SPEC = importlib.util.spec_from_file_location(
    "summarize_runtime_results",
    SCRIPT_PATH,
)
if MODULE_SPEC is None or MODULE_SPEC.loader is None:
    raise RuntimeError(f"Unable to load benchmark script from {SCRIPT_PATH}")
summarize_runtime_results = importlib.util.module_from_spec(MODULE_SPEC)
sys.modules[MODULE_SPEC.name] = summarize_runtime_results
MODULE_SPEC.loader.exec_module(summarize_runtime_results)

discover_json_files = getattr(summarize_runtime_results, "_discover_json_files")
load_completed_runs = getattr(summarize_runtime_results, "_load_completed_runs")
parse_args = getattr(summarize_runtime_results, "_parse_args")


def _payload(
    *,
    generated_at: str,
    suite_name: str,
    backend: str = "sqlite",
    p50: float,
    p95: float,
    p99: float,
    query_p50: float,
    query_p95: float,
    query_p99: float,
    run_status: str = "completed",
) -> dict[str, object]:
    if backend == "neo4j":
        database_versions = {"neo4j": "5.28.2"}
        top_setup = {
            "connect_ms": 12.0,
            "connect_rss_mib": {
                "client_mib": 100.0,
                "server_mib": 200.0,
                "combined_mib": 300.0,
            },
        }
        setup = {
            "reset_ms": 10.0,
            "seed_constraints_ms": 20.0,
            "ingest_ms": 30.0,
            "index_ms": 40.0,
        }
        rss_snapshots = {
            "after_reset": {
                "client_mib": 110.0,
                "server_mib": 210.0,
                "combined_mib": 320.0,
            },
            "after_seed_constraints": {
                "client_mib": 120.0,
                "server_mib": 220.0,
                "combined_mib": 340.0,
            },
            "after_ingest": {
                "client_mib": 130.0,
                "server_mib": 230.0,
                "combined_mib": 360.0,
            },
            "after_index": {
                "client_mib": 140.0,
                "server_mib": 240.0,
                "combined_mib": 380.0,
            },
            "suite_complete": {
                "client_mib": 150.0,
                "server_mib": 250.0,
                "combined_mib": 400.0,
            },
        }
    else:
        database_versions = {"sqlite": "3.50.4"}
        top_setup = {}
        setup = {
            "connect_ms": 10.0,
            "schema_ms": 20.0,
            "ingest_ms": 30.0,
            "index_ms": 40.0,
            "analyze_ms": 50.0,
        }
        rss_snapshots = {
            "after_connect": {
                "client_mib": 100.0,
                "server_mib": None,
                "combined_mib": 100.0,
            },
            "after_schema": {
                "client_mib": 120.0,
                "server_mib": None,
                "combined_mib": 120.0,
            },
            "after_ingest": {
                "client_mib": 140.0,
                "server_mib": None,
                "combined_mib": 140.0,
            },
            "after_index": {
                "client_mib": 160.0,
                "server_mib": None,
                "combined_mib": 160.0,
            },
            "after_analyze": {
                "client_mib": 180.0,
                "server_mib": None,
                "combined_mib": 180.0,
            },
            "suite_complete": {
                "client_mib": 200.0,
                "server_mib": None,
                "combined_mib": 200.0,
            },
        }

    return {
        "benchmark_entrypoint": backend,
        "enabled_backends": [backend],
        "generated_at": generated_at,
        "updated_at": generated_at,
        "run_status": run_status,
        "cypherglot_version": "0.0.1.dev7",
        "database_versions": database_versions,
        "corpus_path": "/tmp/runtime_corpus.json",
        "graph_scale": {
            "node_type_count": 4,
            "edge_type_count": 4,
            "nodes_per_type": 1000,
            "edges_per_source": 3,
            "total_nodes": 4000,
            "total_edges": 12000,
            "edge_degree_profile": "uniform",
            "node_extra_text_property_count": 2,
            "node_extra_numeric_property_count": 6,
            "node_extra_boolean_property_count": 2,
            "edge_extra_text_property_count": 1,
            "edge_extra_numeric_property_count": 3,
            "edge_extra_boolean_property_count": 1,
            "ingest_batch_size": 1000,
            "variable_hop_max": 2,
        },
        "index_mode": "indexed",
        "workload_controls": {
            "default_iterations": 1000,
            "default_warmup": 10,
            "oltp_iterations": 1000,
            "oltp_warmup": 10,
            "olap_iterations": 25,
            "olap_warmup": 5,
        },
        "setup": top_setup,
        "results": {
            "token_map": {},
            "workloads": {
                "oltp": {
                    suite_name: {
                        "backend": backend,
                        "index_mode": "indexed",
                        "iterations": 1000,
                        "warmup": 10,
                        "query_count": 1,
                        "pass_count": 1,
                        "fail_count": 0,
                        "setup": setup,
                        "rss_snapshots_mib": rss_snapshots,
                        "end_to_end": {
                            "mean_of_mean_ms": p50,
                            "mean_of_p50_ms": p50,
                            "mean_of_p95_ms": p95,
                            "mean_of_p99_ms": p99,
                        },
                        "queries": [
                            {
                                "name": "oltp_type1_point_lookup",
                                "end_to_end": {
                                    "p50_ms": query_p50,
                                    "p95_ms": query_p95,
                                    "p99_ms": query_p99,
                                },
                            }
                        ],
                    }
                },
                "olap": {
                    suite_name: {
                        "backend": backend,
                        "index_mode": "indexed",
                        "iterations": 25,
                        "warmup": 5,
                        "query_count": 1,
                        "pass_count": 1,
                        "fail_count": 0,
                        "setup": setup,
                        "rss_snapshots_mib": rss_snapshots,
                        "end_to_end": {
                            "mean_of_mean_ms": p50 * 10,
                            "mean_of_p50_ms": p50 * 10,
                            "mean_of_p95_ms": p95 * 10,
                            "mean_of_p99_ms": p99 * 10,
                        },
                        "queries": [
                            {
                                "name": "olap_type1_scan",
                                "end_to_end": {
                                    "p50_ms": query_p50 * 10,
                                    "p95_ms": query_p95 * 10,
                                    "p99_ms": query_p99 * 10,
                                },
                            }
                        ],
                    }
                }
            },
        },
    }


def _payload_with_non_passing_query(
    *,
    generated_at: str,
    suite_name: str,
) -> dict[str, object]:
    payload = _payload(
        generated_at=generated_at,
        suite_name=suite_name,
        p50=10.0,
        p95=20.0,
        p99=30.0,
        query_p50=1.0,
        query_p95=2.0,
        query_p99=3.0,
    )
    payload["results"]["workloads"]["oltp"][suite_name]["queries"] = [
        {
            "name": "oltp_type1_point_lookup",
            "status": "timed_out",
            "query_timeout": {
                "phase": "warmup",
                "timeout_ms": 1000.0,
                "iteration": 1,
            },
        }
    ]
    payload["results"]["workloads"]["oltp"][suite_name]["pass_count"] = 0
    payload["results"]["workloads"]["oltp"][suite_name]["timeout_count"] = 1
    return payload


class SummarizeRuntimeResultsTests(unittest.TestCase):
    def test_parse_args_defaults_to_benchmark_results_runtime_dir(self) -> None:
        with patch.object(sys, "argv", ["runtime/summarize_results.py"]):
            args = parse_args()

        self.assertEqual(
            args.inputs,
            [REPO_ROOT / "scripts" / "benchmarks" / "results" / "runtime"],
        )
        self.assertTrue(args.include_queries)

    def test_parse_args_includes_queries_by_default(self) -> None:
        with patch.object(sys, "argv", ["runtime/summarize_results.py"]):
            args = parse_args()

        self.assertTrue(args.include_queries)

    def test_parse_args_supports_no_queries_opt_out(self) -> None:
        with patch.object(
            sys,
            "argv",
            ["runtime/summarize_results.py", "--no-queries"],
        ):
            args = parse_args()

        self.assertFalse(args.include_queries)

    def test_render_summary_groups_repeats_and_reports_mean_std(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            first = temp_path / "sqlite-indexed-small-r01.json"
            second = temp_path / "sqlite-indexed-small-r02.json"
            running = temp_path / "sqlite-indexed-small-r03.json"
            first.write_text(
                json.dumps(
                    _payload(
                        generated_at="2026-04-21T13:00:00+00:00",
                        suite_name="sqlite_indexed",
                        p50=10.0,
                        p95=20.0,
                        p99=30.0,
                        query_p50=1.0,
                        query_p95=2.0,
                        query_p99=3.0,
                    )
                ),
                encoding="utf-8",
            )
            second.write_text(
                json.dumps(
                    _payload(
                        generated_at="2026-04-21T13:10:00+00:00",
                        suite_name="sqlite_indexed",
                        p50=14.0,
                        p95=24.0,
                        p99=34.0,
                        query_p50=5.0,
                        query_p95=6.0,
                        query_p99=7.0,
                    )
                ),
                encoding="utf-8",
            )
            running.write_text(
                json.dumps(
                    _payload(
                        generated_at="2026-04-21T13:20:00+00:00",
                        suite_name="sqlite_indexed",
                        p50=99.0,
                        p95=99.0,
                        p99=99.0,
                        query_p50=99.0,
                        query_p95=99.0,
                        query_p99=99.0,
                        run_status="running",
                    )
                ),
                encoding="utf-8",
            )

            discovered = discover_json_files([temp_path])
            completed, skipped = load_completed_runs(discovered)
            markdown = summarize_runtime_results.render_summary(
                completed,
                skipped=skipped,
                include_queries=False,
            )

        self.assertIn("Completed runs: 2", markdown)
        self.assertIn("Skipped non-completed runs: 1", markdown)
        self.assertIn("Grouped benchmark campaigns: 1", markdown)
        self.assertIn("### Small runtime dataset", markdown)
        self.assertIn("Runtime result artifacts for this run now live under", markdown)
        self.assertIn("Versions used for this summarized run:", markdown)
        self.assertIn("`database_versions` object inside each JSON payload", markdown)
        self.assertIn("- `4` node types", markdown)
        self.assertIn("- `4` edge types", markdown)
        self.assertIn(
            "- `24` property fields across the schema (`14` per node, `10` per edge)",
            markdown,
        )
        self.assertIn("OLTP summary:", markdown)
        self.assertIn("| SQLite Indexed | `10.00 ms +- 0.00` |", markdown)
        self.assertIn("| SQLite Indexed | `100.00 MiB +- 0.00` |", markdown)
        self.assertIn("#### Small runtime suite comparison", markdown)
        self.assertIn("Read these tables with a couple of caveats:", markdown)
        self.assertIn("| `oltp/sqlite_indexed` | `12.00 ms +- 2.83` |", markdown)

    def test_render_summary_includes_query_tables_when_requested(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            result_path = temp_path / "sqlite-indexed-small-r01.json"
            result_path.write_text(
                json.dumps(
                    _payload(
                        generated_at="2026-04-21T13:00:00+00:00",
                        suite_name="sqlite_indexed",
                        p50=10.0,
                        p95=20.0,
                        p99=30.0,
                        query_p50=1.0,
                        query_p95=2.0,
                        query_p99=3.0,
                    )
                ),
                encoding="utf-8",
            )

            discovered = discover_json_files([temp_path])
            completed, skipped = load_completed_runs(discovered)
            markdown = summarize_runtime_results.render_summary(
                completed,
                skipped=skipped,
                include_queries=True,
            )

        self.assertIn("#### Small runtime query breakdowns", markdown)
        self.assertIn("##### OLTP query breakdown, end-to-end `p50`", markdown)
        self.assertIn("oltp_type1_point_lookup", markdown)
        self.assertIn("`1.00 ms +- 0.00`", markdown)

    def test_render_summary_skips_non_passing_query_rows_without_metrics(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            result_path = temp_path / "sqlite-indexed-small-r01.json"
            result_path.write_text(
                json.dumps(
                    _payload_with_non_passing_query(
                        generated_at="2026-04-21T13:00:00+00:00",
                        suite_name="sqlite_indexed",
                    )
                ),
                encoding="utf-8",
            )

            discovered = discover_json_files([temp_path])
            completed, skipped = load_completed_runs(discovered)
            markdown = summarize_runtime_results.render_summary(
                completed,
                skipped=skipped,
                include_queries=True,
            )

        self.assertIn("#### Small runtime query breakdowns", markdown)
        self.assertIn("oltp_type1_point_lookup", markdown)
        self.assertIn("| `oltp_type1_point_lookup` | - |", markdown)

    def test_render_summary_handles_empty_completed_set(self) -> None:
        markdown = summarize_runtime_results.render_summary(
            [],
            skipped=[],
            include_queries=False,
        )

        self.assertIn("No completed runtime result JSON files were found.", markdown)


if __name__ == "__main__":
    unittest.main()
