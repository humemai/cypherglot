from __future__ import annotations

import importlib.util
import sys
import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
SCRIPT_PATH = REPO_ROOT / "scripts" / "benchmarks" / "benchmark_compiler.py"
MODULE_SPEC = importlib.util.spec_from_file_location("benchmark_compiler", SCRIPT_PATH)
if MODULE_SPEC is None or MODULE_SPEC.loader is None:
    raise RuntimeError(f"Unable to load benchmark script from {SCRIPT_PATH}")
benchmark_compiler = importlib.util.module_from_spec(MODULE_SPEC)
sys.modules[MODULE_SPEC.name] = benchmark_compiler
MODULE_SPEC.loader.exec_module(benchmark_compiler)


class BenchmarkCompilerScriptTests(unittest.TestCase):
    def test_sqlglot_corpus_matches_cypherglot_query_count(self) -> None:
        load_corpus = getattr(benchmark_compiler, "_load_corpus")
        load_sql_corpus = getattr(benchmark_compiler, "_load_sql_corpus")
        cypherglot_queries = load_corpus(benchmark_compiler.DEFAULT_CORPUS_PATH)
        sqlglot_queries = load_sql_corpus(
            benchmark_compiler.DEFAULT_SQLGLOT_CORPUS_PATH
        )

        self.assertEqual(len(cypherglot_queries), 20)
        self.assertEqual(len(sqlglot_queries), len(cypherglot_queries))

    def test_sqlglot_pure_python_context_imports_python_modules(self) -> None:
        sqlglot_import_context = getattr(benchmark_compiler, "_sqlglot_import_context")

        with sqlglot_import_context("python"):
            sqlglot = __import__("sqlglot")
            parser_module = __import__("sqlglot.parser", fromlist=["Parser"])
            generator_module = __import__("sqlglot.generator", fromlist=["Generator"])
            tokenizer_core_module = __import__(
                "sqlglot.tokenizer_core",
                fromlist=["TokenizerCore"],
            )

            self.assertTrue(str(Path(sqlglot.__file__)).endswith("__init__.py"))
            self.assertEqual(Path(parser_module.__file__).suffix, ".py")
            self.assertEqual(Path(generator_module.__file__).suffix, ".py")
            self.assertEqual(Path(tokenizer_core_module.__file__).suffix, ".py")

    def test_sqlglot_installed_details_report_known_implementation(self) -> None:
        load_sql_corpus = getattr(benchmark_compiler, "_load_sql_corpus")
        sqlglot_suite_result = getattr(benchmark_compiler, "_sqlglot_suite_result")
        suite = sqlglot_suite_result(
            load_sql_corpus(benchmark_compiler.DEFAULT_SQLGLOT_CORPUS_PATH)[:1],
            iterations=1,
            warmup=0,
            mode="installed",
        )

        self.assertIn(suite["implementation"], {"compiled", "python"})
        self.assertEqual(suite["dialect_pair"]["read"], "postgres")
        self.assertEqual(suite["dialect_pair"]["write"], "sqlite")
        self.assertEqual(len(suite["results"]), 4)


if __name__ == "__main__":
    unittest.main()
