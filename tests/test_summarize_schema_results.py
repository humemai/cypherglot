from __future__ import annotations

import importlib.util
import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch


REPO_ROOT = Path(__file__).resolve().parents[1]
SCRIPT_PATH = REPO_ROOT / "scripts" / "benchmarks" / "schema/summarize_results.py"
MODULE_SPEC = importlib.util.spec_from_file_location(
    "summarize_schema_results",
    SCRIPT_PATH,
)
if MODULE_SPEC is None or MODULE_SPEC.loader is None:
    raise RuntimeError(f"Unable to load benchmark script from {SCRIPT_PATH}")
summarize_schema_results = importlib.util.module_from_spec(MODULE_SPEC)
sys.modules[MODULE_SPEC.name] = summarize_schema_results
MODULE_SPEC.loader.exec_module(summarize_schema_results)

discover_json_files = getattr(summarize_schema_results, "_discover_json_files")
load_completed_runs = getattr(summarize_schema_results, "_load_completed_runs")
parse_args = getattr(summarize_schema_results, "_parse_args")
render_markdown = getattr(summarize_schema_results, "_render_markdown")


def _schema_payload(
    *, generated_at: str, run_status: str = "completed"
) -> dict[str, object]:
    def _query(
        name: str,
        category: str,
        mean: float,
        p50: float,
        p95: float,
        p99: float,
    ) -> dict[str, object]:
        return {
            "name": name,
            "category": category,
            "execute": {
                "mean_ms": mean,
                "p50_ms": p50,
                "p95_ms": p95,
                "p99_ms": p99,
            },
        }

    def _suite(
        connect: float,
        schema: float,
        ingest: float,
        analyze: float,
        size: float,
        pooled: tuple[float, float, float, float],
        mean_offset: float,
    ) -> dict[str, object]:
        mean_ms, p50_ms, p95_ms, p99_ms = pooled
        return {
            "setup": {
                "connect_ms": connect,
                "schema_ms": schema,
                "ingest_ms": ingest,
                "index_ms": 15.0 + mean_offset,
                "analyze_ms": analyze,
                "database_size_mb": size,
                "rss_mib": {
                    "connect": 10.0 + mean_offset,
                    "schema": 11.0 + mean_offset,
                    "ingest": 12.0 + mean_offset,
                    "index": 12.5 + mean_offset,
                    "analyze": 13.0 + mean_offset,
                },
            },
            "row_counts": {"node_count": 4000, "edge_count": 12000},
            "pooled_execute": {
                "mean_ms": mean_ms,
                "p50_ms": p50_ms,
                "p95_ms": p95_ms,
                "p99_ms": p99_ms,
            },
            "queries": [
                _query(
                    "point_lookup",
                    "point-read",
                    1.0 + mean_offset,
                    0.8 + mean_offset,
                    1.2 + mean_offset,
                    1.4 + mean_offset,
                ),
                _query(
                    "top_active_score",
                    "ordered-top-k",
                    2.0 + mean_offset,
                    1.8 + mean_offset,
                    2.4 + mean_offset,
                    2.8 + mean_offset,
                ),
                _query(
                    "one_hop_neighbors",
                    "adjacency-read",
                    3.0 + mean_offset,
                    2.8 + mean_offset,
                    3.4 + mean_offset,
                    3.8 + mean_offset,
                ),
                _query(
                    "multi_hop_chain",
                    "multi-hop-read",
                    4.0 + mean_offset,
                    3.8 + mean_offset,
                    4.5 + mean_offset,
                    4.9 + mean_offset,
                ),
                _query(
                    "relationship_stats",
                    "relationship-aggregate",
                    5.0 + mean_offset,
                    4.8 + mean_offset,
                    5.5 + mean_offset,
                    5.9 + mean_offset,
                ),
                _query(
                    "relationship_projection",
                    "relationship-projection",
                    6.0 + mean_offset,
                    5.8 + mean_offset,
                    6.6 + mean_offset,
                    7.0 + mean_offset,
                ),
            ],
        }

    return {
        "benchmark_entrypoint": "scripts.benchmarks.schema.sqlite_shapes",
        "generated_at": generated_at,
        "run_status": run_status,
        "environment": {"python": "3.12.11", "sqlite": "3.50.4", "platform": "Linux"},
        "controls": {
            "iterations": 20,
            "warmup": 3,
            "batch_size": 1000,
            "schemas": ["json", "typed", "typeaware"],
        },
        "scale": {
            "node_type_count": 4,
            "edge_type_count": 4,
            "nodes_per_type": 1000,
            "edges_per_source": 3,
            "multi_hop_length": 4,
            "node_numeric_property_count": 10,
            "node_text_property_count": 2,
            "node_boolean_property_count": 2,
            "edge_numeric_property_count": 6,
            "edge_text_property_count": 2,
            "edge_boolean_property_count": 1,
        },
        "schemas": {
            "json": _suite(10.0, 20.0, 30.0, 40.0, 50.0, (6.0, 5.0, 7.0, 8.0), 0.0),
            "typed": _suite(11.0, 21.0, 31.0, 41.0, 51.0, (7.0, 6.0, 8.0, 9.0), 0.5),
            "typeaware": _suite(
                12.0,
                22.0,
                32.0,
                42.0,
                52.0,
                (5.0, 4.0, 6.0, 7.0),
                1.0,
            ),
        },
    }


class SummarizeSchemaResultsTests(unittest.TestCase):
    def test_parse_args_defaults_to_benchmark_results_schema_dir(self) -> None:
        with patch.object(sys, "argv", ["schema/summarize_results.py"]):
            args = parse_args()

        self.assertEqual(
            args.inputs,
            [REPO_ROOT / "scripts" / "benchmarks" / "results" / "schema"],
        )
        self.assertTrue(args.include_queries)

    def test_render_markdown_reports_pooled_percentiles_and_grouped_query_tables(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            first = temp_path / "schema-small-r01.json"
            second = temp_path / "schema-small-r02.json"
            skipped = temp_path / "schema-small-r03.json"
            first.write_text(
                json.dumps(_schema_payload(generated_at="2026-04-22T08:00:00+00:00")),
                encoding="utf-8",
            )
            second.write_text(
                json.dumps(_schema_payload(generated_at="2026-04-22T08:10:00+00:00")),
                encoding="utf-8",
            )
            skipped.write_text(
                json.dumps(
                    _schema_payload(
                        generated_at="2026-04-22T08:20:00+00:00",
                        run_status="running",
                    )
                ),
                encoding="utf-8",
            )

            discovered = discover_json_files([temp_path])
            completed, incomplete = load_completed_runs(discovered)
            markdown = render_markdown(
                [completed],
                include_queries=True,
                results_label="scripts/benchmarks/results/schema",
                skipped=incomplete,
            )

        self.assertIn("Skipped incomplete files: `1`", markdown)
        self.assertIn("Pooled p50", markdown)
        self.assertIn("Pooled p95", markdown)
        self.assertIn("Pooled p99", markdown)
        self.assertIn("RSS Index", markdown)
        self.assertIn("Dataset:", markdown)
        self.assertIn("- node types: `4`", markdown)
        self.assertIn("- edge types: `4`", markdown)
        self.assertIn("- total nodes: `4,000`", markdown)
        self.assertIn("- total edges: `12,000`", markdown)
        self.assertIn(
            "- node properties per node: `text=2`, `numeric=10`, `boolean=2`",
            markdown,
        )
        self.assertIn(
            "- edge properties per edge: `text=2`, `numeric=6`, `boolean=1`",
            markdown,
        )
        self.assertIn("### Query mean summary", markdown)
        self.assertIn("### Query p50 summary", markdown)
        self.assertIn("### Query p95 summary", markdown)
        self.assertIn("### Query p99 summary", markdown)
        self.assertIn("#### OLTP-leaning query mean", markdown)
        self.assertIn("#### OLAP-leaning query p95", markdown)
        self.assertIn("`point_lookup`", markdown)
        self.assertIn("`relationship_projection`", markdown)
        self.assertIn("`point-read`", markdown)
        self.assertIn("`relationship-projection`", markdown)
        self.assertIn("`5.00 ms +- 0.00`", markdown)

    def test_render_markdown_handles_empty_completed_set(self) -> None:
        markdown = render_markdown(
            [],
            include_queries=False,
            results_label="scripts/benchmarks/results/schema",
            skipped=[],
        )

        self.assertIn("No completed schema benchmark JSON files found.", markdown)


if __name__ == "__main__":
    unittest.main()
