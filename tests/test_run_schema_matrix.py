from __future__ import annotations

import importlib.util
import sys
import unittest
from pathlib import Path
from unittest.mock import patch


REPO_ROOT = Path(__file__).resolve().parents[1]
SCRIPT_PATH = REPO_ROOT / "scripts" / "benchmarks" / "schema/matrix.py"
MODULE_SPEC = importlib.util.spec_from_file_location("run_schema_matrix", SCRIPT_PATH)
if MODULE_SPEC is None or MODULE_SPEC.loader is None:
    raise RuntimeError(f"Unable to load benchmark script from {SCRIPT_PATH}")
run_schema_matrix = importlib.util.module_from_spec(MODULE_SPEC)
sys.modules[MODULE_SPEC.name] = run_schema_matrix
MODULE_SPEC.loader.exec_module(run_schema_matrix)


class RunSchemaMatrixTests(unittest.TestCase):
    def test_parse_args_requires_explicit_scale(self) -> None:
        parse_args = getattr(run_schema_matrix, "_parse_args")

        with patch.object(sys, "argv", ["schema/matrix.py"]):
            with self.assertRaises(SystemExit):
                parse_args()

    def test_scale_presets_follow_runtime_style_progression(self) -> None:
        scale_presets = getattr(run_schema_matrix, "SCALE_PRESETS")

        self.assertEqual(tuple(scale_presets.keys()), ("small", "medium", "large"))
        self.assertEqual(scale_presets["small"].node_type_count, 4)
        self.assertEqual(scale_presets["small"].edge_type_count, 4)
        self.assertEqual(scale_presets["small"].nodes_per_type, 1_000)
        self.assertEqual(scale_presets["small"].batch_size, 1_000)
        self.assertLessEqual(
            scale_presets["small"].multi_hop_length,
            scale_presets["small"].edge_type_count,
        )
        self.assertEqual(scale_presets["medium"].node_type_count, 6)
        self.assertEqual(scale_presets["medium"].edge_type_count, 8)
        self.assertEqual(scale_presets["medium"].nodes_per_type, 100_000)
        self.assertEqual(scale_presets["large"].node_type_count, 10)
        self.assertEqual(scale_presets["large"].edge_type_count, 10)
        self.assertEqual(scale_presets["large"].nodes_per_type, 1_000_000)
        self.assertEqual(scale_presets["large"].edges_per_source, 8)
        self.assertEqual(scale_presets["large"].multi_hop_length, 8)
        self.assertEqual(scale_presets["large"].batch_size, 10_000)


if __name__ == "__main__":
    unittest.main()
