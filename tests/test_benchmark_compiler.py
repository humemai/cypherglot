from __future__ import annotations

import importlib.util
import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch


REPO_ROOT = Path(__file__).resolve().parents[1]
SCRIPT_PATH = REPO_ROOT / "scripts" / "benchmarks" / "compiler/benchmark.py"
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

        self.assertEqual(len(cypherglot_queries), 22)
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

    def test_backend_lowering_result_reports_all_stages(self) -> None:
        load_corpus = getattr(benchmark_compiler, "_load_corpus")
        backend_lowering_result = getattr(
            benchmark_compiler,
            "_backend_lowering_result",
        )

        result = backend_lowering_result(
            load_corpus(benchmark_compiler.DEFAULT_CORPUS_PATH)[:1],
            backend=benchmark_compiler.SQLBackend.SQLITE,
            iterations=1,
            warmup=0,
        )

        self.assertEqual(result["backend"], "sqlite")
        self.assertEqual(result["query_count"], 1)
        self.assertEqual(
            set(result["overall"].keys()),
            {
                "build_ir",
                "bind_backend",
                "lower_backend",
                "render_program",
                "end_to_end",
            },
        )
        self.assertEqual(
            set(result["queries"][0].keys()),
            {
                "name",
                "category",
                "build_ir",
                "bind_backend",
                "lower_backend",
                "render_program",
                "end_to_end",
            },
        )

    def test_sqlglot_subprocess_uses_module_execution(self) -> None:
        run_sqlglot_subprocess = getattr(
            benchmark_compiler,
            "_run_sqlglot_subprocess",
        )

        def _fake_run(command: list[str], *, check: bool, cwd: Path) -> None:
            self.assertTrue(check)
            self.assertEqual(cwd, REPO_ROOT)
            self.assertEqual(command[:3], [
                sys.executable,
                "-m",
                "scripts.benchmarks.compiler.benchmark",
            ])
            output_index = command.index("--_sqlglot-subprocess-output") + 1
            Path(command[output_index]).write_text(
                json.dumps({"implementation": "python", "results": []}),
                encoding="utf-8",
            )

        with tempfile.TemporaryDirectory() as temp_dir:
            sql_corpus = Path(temp_dir) / "sql-corpus.json"
            sql_corpus.write_text("[]", encoding="utf-8")
            with patch.object(benchmark_compiler.subprocess, "run", side_effect=_fake_run):
                result = run_sqlglot_subprocess(
                    sql_corpus=sql_corpus,
                    iterations=1,
                    warmup=0,
                    mode="python",
                )

        self.assertEqual(result["implementation"], "python")
        self.assertEqual(result["results"], [])


if __name__ == "__main__":
    unittest.main()
