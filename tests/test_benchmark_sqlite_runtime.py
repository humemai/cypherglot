from __future__ import annotations

import importlib.util
import sys
import tempfile
import unittest
from unittest import mock
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
SCRIPT_PATH = REPO_ROOT / "scripts" / "benchmarks" / "benchmark_sqlite_runtime.py"
MODULE_SPEC = importlib.util.spec_from_file_location(
    "benchmark_sqlite_runtime",
    SCRIPT_PATH,
)
if MODULE_SPEC is None or MODULE_SPEC.loader is None:
    raise RuntimeError(f"Unable to load benchmark script from {SCRIPT_PATH}")
benchmark_sqlite_runtime = importlib.util.module_from_spec(MODULE_SPEC)
sys.modules[MODULE_SPEC.name] = benchmark_sqlite_runtime
MODULE_SPEC.loader.exec_module(benchmark_sqlite_runtime)


class BenchmarkSQLiteRuntimeScriptTests(unittest.TestCase):
    def test_runtime_query_runner_reuses_seeded_state_for_reads(self) -> None:
        RuntimeCorpusQuery = getattr(benchmark_sqlite_runtime, "RuntimeCorpusQuery")
        RuntimeQueryRunner = getattr(benchmark_sqlite_runtime, "_RuntimeQueryRunner")

        prepared = RuntimeCorpusQuery(
            name="statement_case",
            category="match-read",
            fixture="basic_graph",
            mode="statement",
            query="MATCH (u:User) RETURN u.name ORDER BY u.name",
        )

        runner = RuntimeQueryRunner(prepared)
        try:
            runner.run_once()
            runner.run_once()
            rows = runner.conn.execute(
                "SELECT COUNT(*) FROM nodes"
            ).fetchone()
        finally:
            runner.close()

        self.assertEqual(rows, (2,))

    def test_runtime_query_runner_rolls_back_write_effects(self) -> None:
        RuntimeCorpusQuery = getattr(benchmark_sqlite_runtime, "RuntimeCorpusQuery")
        RuntimeQueryRunner = getattr(benchmark_sqlite_runtime, "_RuntimeQueryRunner")

        prepared = RuntimeCorpusQuery(
            name="write_case",
            category="write-single-statement",
            fixture="basic_graph",
            mode="statement",
            query=(
                "MATCH (u:User {name: 'Alice'}) "
                "SET u.age = 31, u.active = true"
            ),
        )

        runner = RuntimeQueryRunner(prepared)
        try:
            runner.run_once()
            runner.run_once()
            properties = runner.conn.execute(
                "SELECT properties FROM nodes WHERE id = 1"
            ).fetchone()
        finally:
            runner.close()

        self.assertEqual(
            properties,
            ('{"name":"Alice","age":30,"score":9}',),
        )

    def test_select_queries_filters_named_entries(self) -> None:
        load_corpus = getattr(benchmark_sqlite_runtime, "_load_corpus")
        select_queries = getattr(benchmark_sqlite_runtime, "_select_queries")

        selected = select_queries(
            load_corpus(benchmark_sqlite_runtime.DEFAULT_CORPUS_PATH),
            ["simple_match_return", "delete_relationship"],
        )

        self.assertEqual(
            [query.name for query in selected],
            ["simple_match_return", "delete_relationship"],
        )

    def test_select_queries_rejects_unknown_names(self) -> None:
        load_corpus = getattr(benchmark_sqlite_runtime, "_load_corpus")
        select_queries = getattr(benchmark_sqlite_runtime, "_select_queries")

        with self.assertRaisesRegex(ValueError, "Unknown runtime benchmark"):
            select_queries(
                load_corpus(benchmark_sqlite_runtime.DEFAULT_CORPUS_PATH),
                ["not_a_real_query"],
            )

    def test_runtime_query_runner_compiles_statement_each_run(self) -> None:
        RuntimeCorpusQuery = getattr(benchmark_sqlite_runtime, "RuntimeCorpusQuery")
        RuntimeQueryRunner = getattr(benchmark_sqlite_runtime, "_RuntimeQueryRunner")

        query = RuntimeCorpusQuery(
            name="statement_case",
            category="match-read",
            fixture="basic_graph",
            mode="statement",
            query="MATCH (u:User) RETURN u.name ORDER BY u.name",
        )

        runner = RuntimeQueryRunner(query)
        try:
            with mock.patch.object(
                benchmark_sqlite_runtime.cypherglot,
                "to_sql",
                wraps=benchmark_sqlite_runtime.cypherglot.to_sql,
            ) as to_sql:
                runner.run_once()
                runner.run_once()
        finally:
            runner.close()

        self.assertEqual(to_sql.call_count, 2)

    def test_runtime_query_runner_compiles_program_each_run(self) -> None:
        RuntimeCorpusQuery = getattr(benchmark_sqlite_runtime, "RuntimeCorpusQuery")
        RuntimeQueryRunner = getattr(benchmark_sqlite_runtime, "_RuntimeQueryRunner")

        query = RuntimeCorpusQuery(
            name="program_case",
            category="write-program",
            fixture="basic_graph",
            mode="program",
            query=(
                "MATCH (a:User)-[r:KNOWS]->(b:User) "
                "CREATE (a)-[:INTRODUCED]->(:Person {name: 'Cara'})"
            ),
        )

        runner = RuntimeQueryRunner(query)
        try:
            with mock.patch.object(
                benchmark_sqlite_runtime.cypherglot,
                "render_cypher_program_text",
                wraps=benchmark_sqlite_runtime.cypherglot.render_cypher_program_text,
            ) as render_program:
                runner.run_once()
                runner.run_once()
        finally:
            runner.close()

        self.assertEqual(render_program.call_count, 2)

    def test_create_connection_applies_wal_normal_profile(self) -> None:
        create_connection = getattr(benchmark_sqlite_runtime, "_create_connection")

        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "runtime.sqlite3"
            conn = create_connection(db_path)
            try:
                journal_mode = conn.execute("PRAGMA journal_mode").fetchone()
                synchronous = conn.execute("PRAGMA synchronous").fetchone()
                foreign_keys = conn.execute("PRAGMA foreign_keys").fetchone()
            finally:
                conn.close()

        self.assertEqual(journal_mode, ("wal",))
        self.assertEqual(synchronous, (1,))
        self.assertEqual(foreign_keys, (1,))

    def test_runtime_corpus_has_expected_size(self) -> None:
        load_corpus = getattr(benchmark_sqlite_runtime, "_load_corpus")
        queries = load_corpus(benchmark_sqlite_runtime.DEFAULT_CORPUS_PATH)

        self.assertEqual(len(queries), 9)
        self.assertEqual({query.mode for query in queries}, {"statement", "program"})

    def test_runtime_suite_result_reports_batch_and_query_summaries(self) -> None:
        load_corpus = getattr(benchmark_sqlite_runtime, "_load_corpus")
        runtime_suite_result = getattr(
            benchmark_sqlite_runtime,
            "_runtime_suite_result",
        )
        result = runtime_suite_result(
            load_corpus(benchmark_sqlite_runtime.DEFAULT_CORPUS_PATH)[:2],
            iterations=1,
            warmup=0,
        )

        self.assertEqual(result["query_count"], 2)
        self.assertIn("overall", result)
        self.assertIn("batch", result)
        self.assertIn("queries", result)
        self.assertEqual(len(result["queries"]), 2)

        overall = result["overall"]
        self.assertGreaterEqual(overall["p50_us"], 0.0)
        batch = result["batch"]
        self.assertGreaterEqual(batch["p50_us"], 0.0)
        first_query = result["queries"][0]
        self.assertIn(
            first_query["fixture"],
            {
                "basic_graph",
                "chain_graph",
                "user_chain_graph",
                "duplicate_name_graph",
            },
        )
        self.assertGreaterEqual(first_query["summary"]["p50_us"], 0.0)


if __name__ == "__main__":
    unittest.main()
