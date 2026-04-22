from __future__ import annotations

import importlib.util
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


if __name__ == "__main__":
    unittest.main()