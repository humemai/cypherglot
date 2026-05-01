from __future__ import annotations

import importlib.util
import sys
import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
SCRIPT_PATH = REPO_ROOT / "scripts" / "benchmarks" / "compiler/summarize_results.py"
MODULE_SPEC = importlib.util.spec_from_file_location(
    "summarize_compiler_results",
    SCRIPT_PATH,
)
if MODULE_SPEC is None or MODULE_SPEC.loader is None:
    raise RuntimeError(f"Unable to load summarizer from {SCRIPT_PATH}")
summarize_compiler_results = importlib.util.module_from_spec(MODULE_SPEC)
sys.modules[MODULE_SPEC.name] = summarize_compiler_results
MODULE_SPEC.loader.exec_module(summarize_compiler_results)


class SummarizeCompilerResultsTests(unittest.TestCase):
    def test_render_report_includes_all_compiler_sections(self) -> None:
        render_report = getattr(summarize_compiler_results, "_render_report")
        report = render_report(
            [
                REPO_ROOT
                / "scripts"
                / "benchmarks"
                / "results"
                / "compiler_benchmark.json"
            ]
        )

        self.assertIn("# Compiler Benchmark Summary", report)
        self.assertIn("## Shared Entrypoints", report)
        self.assertIn("parse_cypher_text", report)
        self.assertIn("## Backend Entrypoints", report)
        self.assertIn("to_sqlglot_ast", report)
        self.assertIn("## Backend Lowering", report)
        self.assertIn("Build IR P50", report)
        self.assertIn("## SQLGlot Suites", report)
        self.assertIn("compiled", report)
        self.assertIn("## SQLGlot Per Query", report)
        self.assertIn("SQLGlot `compiled` / `tokenize` per-query", report)
        self.assertIn("simple_filter_limit", report)


if __name__ == "__main__":
    unittest.main()