from __future__ import annotations

import importlib.util
import sqlite3
import sys
import unittest
from pathlib import Path
from unittest.mock import patch


REPO_ROOT = Path(__file__).resolve().parents[1]
SCRIPT_PATH = REPO_ROOT / "scripts" / "benchmarks" / "schema/sqlite_shapes.py"
MODULE_SPEC = importlib.util.spec_from_file_location(
    "benchmark_schema_sqlite_shapes",
    SCRIPT_PATH,
)
if MODULE_SPEC is None or MODULE_SPEC.loader is None:
    raise RuntimeError(f"Unable to load benchmark script from {SCRIPT_PATH}")
benchmark_schema_sqlite_shapes = importlib.util.module_from_spec(MODULE_SPEC)
sys.modules[MODULE_SPEC.name] = benchmark_schema_sqlite_shapes
MODULE_SPEC.loader.exec_module(benchmark_schema_sqlite_shapes)


class BenchmarkSchemaSqliteShapesTests(unittest.TestCase):
    def test_parse_args_defaults_output_to_schema_results_dir(self) -> None:
        parse_args = getattr(benchmark_schema_sqlite_shapes, "_parse_args")

        with patch.object(sys, "argv", ["schema/sqlite_shapes.py"]):
            args = parse_args()

        self.assertEqual(
            args.output,
            REPO_ROOT
            / "scripts"
            / "benchmarks"
            / "results"
            / "schema"
            / "sqlite_schema_shape_benchmark.json",
        )

    def test_benchmark_query_reports_warmup_and_timed_progress(self) -> None:
        benchmark_query = getattr(benchmark_schema_sqlite_shapes, "_benchmark_query")

        conn = sqlite3.connect(":memory:")
        self.addCleanup(conn.close)
        conn.execute("CREATE TABLE items(id INTEGER PRIMARY KEY, value TEXT)")
        conn.execute("INSERT INTO items(value) VALUES ('a')")

        progress_messages: list[str] = []
        with patch.object(
            benchmark_schema_sqlite_shapes,
            "_progress",
            side_effect=progress_messages.append,
        ):
            benchmark_query(
                conn,
                "SELECT value FROM items",
                warmup=3,
                iterations=10,
                progress_label="schema benchmark: typed query 1/6 point_lookup",
            )

        self.assertIn(
            (
                "schema benchmark: typed query 1/6 point_lookup "
                "warmup start (3 iterations)"
            ),
            progress_messages,
        )
        self.assertIn(
            "schema benchmark: typed query 1/6 point_lookup warmup 3/3 (100%)",
            progress_messages,
        )
        self.assertIn(
            (
                "schema benchmark: typed query 1/6 point_lookup "
                "timed start (10 iterations)"
            ),
            progress_messages,
        )
        self.assertIn(
            "schema benchmark: typed query 1/6 point_lookup timed 10/10 (100%)",
            progress_messages,
        )

    def test_seed_json_schema_reports_node_and_edge_progress(self) -> None:
        create_json_schema = getattr(
            benchmark_schema_sqlite_shapes,
            "_create_json_schema",
        )
        seed_json_schema = getattr(benchmark_schema_sqlite_shapes, "_seed_json_schema")
        schema_scale = getattr(benchmark_schema_sqlite_shapes, "SchemaShapeScale")

        conn = sqlite3.connect(":memory:")
        self.addCleanup(conn.close)
        create_json_schema(conn)
        scale = schema_scale(
            node_type_count=2,
            edge_type_count=2,
            nodes_per_type=5,
            edges_per_source=2,
            multi_hop_length=2,
            node_numeric_property_count=1,
            node_text_property_count=1,
            node_boolean_property_count=1,
            edge_numeric_property_count=1,
            edge_text_property_count=1,
            edge_boolean_property_count=1,
        )

        progress_messages: list[str] = []
        with patch.object(
            benchmark_schema_sqlite_shapes,
            "_progress",
            side_effect=progress_messages.append,
        ):
            row_counts = seed_json_schema(
                conn,
                scale=scale,
                batch_size=2,
                progress_label="schema benchmark: json ingest",
            )

        self.assertEqual(row_counts["node_count"], 10)
        self.assertEqual(row_counts["edge_count"], 20)
        self.assertIn(
            "schema benchmark: json ingest nodes 10/10 (100%)",
            progress_messages,
        )
        self.assertIn(
            "schema benchmark: json ingest edges 20/20 (100%)",
            progress_messages,
        )


if __name__ == "__main__":
    unittest.main()
