"""Transparent compile cache at the string-rendering entrypoints (M3)."""

from __future__ import annotations

import os
import threading
import unittest

import cypherglot
from cypherglot import CompilerSchemaContext, GraphSchema
from cypherglot.schema import EdgeTypeSpec, NodeTypeSpec, PropertyField


def _schema(node_name: str = "User") -> GraphSchema:
    return GraphSchema(
        node_types=(
            NodeTypeSpec(
                name=node_name,
                properties=(PropertyField(name="name", logical_type="string"),),
            ),
        ),
        edge_types=(
            EdgeTypeSpec(
                name="KNOWS",
                source_type=node_name,
                target_type=node_name,
                properties=(),
            ),
        ),
    )


_QUERY = "MATCH (a:User)-[r:KNOWS]->(b:User) RETURN a.name, b.name"


class CompileCacheTests(unittest.TestCase):
    def setUp(self) -> None:
        self.ctx = CompilerSchemaContext.type_aware(_schema())
        cypherglot.set_compile_cache(enabled=True, maxsize=512)
        cypherglot.clear_compile_cache()

    def tearDown(self) -> None:
        cypherglot.set_compile_cache(enabled=True, maxsize=512)
        cypherglot.clear_compile_cache()
        os.environ.pop("CYPHERGLOT_COMPILE_CACHE", None)

    def _to_sql(self, query: str = _QUERY, **kwargs) -> str:
        kwargs.setdefault("backend", "sqlite")
        kwargs.setdefault("schema_context", self.ctx)
        return cypherglot.to_sql(query, **kwargs)

    def test_hit_returns_identical_string(self) -> None:
        first = self._to_sql()
        second = self._to_sql()
        self.assertIs(first, second)
        info = cypherglot.compile_cache_info()
        self.assertEqual(info.hits, 1)
        self.assertEqual(info.misses, 1)
        self.assertEqual(info.currsize, 1)

    def test_cached_output_matches_uncached_output(self) -> None:
        cached = self._to_sql()
        cypherglot.set_compile_cache(enabled=False)
        uncached = self._to_sql()
        self.assertEqual(cached, uncached)

    def test_disabled_cache_never_stores(self) -> None:
        cypherglot.set_compile_cache(enabled=False)
        first = self._to_sql()
        second = self._to_sql()
        self.assertEqual(first, second)
        self.assertIsNot(first, second)
        info = cypherglot.compile_cache_info()
        self.assertEqual(info.currsize, 0)
        self.assertFalse(info.enabled)

    def test_env_flag_bypasses_without_code_changes(self) -> None:
        os.environ["CYPHERGLOT_COMPILE_CACHE"] = "0"
        self._to_sql()
        self._to_sql()
        info = cypherglot.compile_cache_info()
        self.assertEqual(info.currsize, 0)
        self.assertFalse(info.enabled)

    def test_schema_contexts_do_not_share_entries(self) -> None:
        other_ctx = CompilerSchemaContext.type_aware(_schema("Person"))
        query = "MATCH (a:User) RETURN a.name"
        other_query = "MATCH (a:Person) RETURN a.name"
        sql_user = self._to_sql(query)
        sql_person = self._to_sql(other_query, schema_context=other_ctx)
        self.assertNotEqual(sql_user, sql_person)
        self.assertEqual(cypherglot.compile_cache_info().currsize, 2)

    def test_render_options_get_separate_entries(self) -> None:
        plain = self._to_sql()
        pretty = self._to_sql(pretty=True)
        unroll = self._to_sql(variable_length_strategy="unroll")
        rcte = self._to_sql(variable_length_strategy="recursive_cte")
        self.assertNotEqual(plain, pretty)
        self.assertEqual(unroll, rcte)  # non-VL query: same SQL...
        # ...but distinct cache slots (plain==unroll shares one).
        self.assertEqual(cypherglot.compile_cache_info().currsize, 3)

    def test_backends_get_separate_entries(self) -> None:
        self._to_sql(backend="sqlite")
        self._to_sql(backend="postgresql")
        self.assertEqual(cypherglot.compile_cache_info().currsize, 2)

    def test_lru_eviction_at_maxsize(self) -> None:
        cypherglot.set_compile_cache(maxsize=2)
        q1 = "MATCH (a:User) RETURN a.name"
        q2 = "MATCH (a:User) RETURN a.name LIMIT 1"
        q3 = "MATCH (a:User) RETURN a.name LIMIT 2"
        self._to_sql(q1)
        self._to_sql(q2)
        self._to_sql(q1)  # refresh q1 so q2 is now least-recently-used
        self._to_sql(q3)  # evicts q2
        info = cypherglot.compile_cache_info()
        self.assertEqual(info.currsize, 2)
        misses_before = info.misses
        self._to_sql(q1)  # still cached
        self._to_sql(q2)  # evicted -> miss
        info = cypherglot.compile_cache_info()
        self.assertEqual(info.misses, misses_before + 1)

    def test_shrinking_maxsize_evicts_down(self) -> None:
        self._to_sql("MATCH (a:User) RETURN a.name")
        self._to_sql("MATCH (a:User) RETURN a.name LIMIT 1")
        self._to_sql("MATCH (a:User) RETURN a.name LIMIT 2")
        cypherglot.set_compile_cache(maxsize=1)
        self.assertEqual(cypherglot.compile_cache_info().currsize, 1)

    def test_invalid_maxsize_rejected(self) -> None:
        with self.assertRaises(ValueError):
            cypherglot.set_compile_cache(maxsize=0)

    def test_clear_resets_entries_and_stats(self) -> None:
        self._to_sql()
        self._to_sql()
        cypherglot.clear_compile_cache()
        info = cypherglot.compile_cache_info()
        self.assertEqual((info.hits, info.misses, info.currsize), (0, 0, 0))

    def test_failed_compiles_are_not_cached(self) -> None:
        bad = "MATCH (a:User) RETURN shortestPath(a)"
        for _ in range(2):
            with self.assertRaises(ValueError):
                self._to_sql(bad)
        self.assertEqual(cypherglot.compile_cache_info().currsize, 0)

    def test_program_rendering_is_cached(self) -> None:
        query = "CREATE (a:User {name: 'A'})"
        first = cypherglot.render_cypher_program_text(
            query, backend="sqlite", schema_context=self.ctx
        )
        second = cypherglot.render_cypher_program_text(
            query, backend="sqlite", schema_context=self.ctx
        )
        self.assertIs(first, second)

    def test_sql_and_program_entries_do_not_collide(self) -> None:
        self._to_sql()
        program = cypherglot.render_cypher_program_text(
            _QUERY, backend="sqlite", schema_context=self.ctx
        )
        self.assertEqual(cypherglot.compile_cache_info().currsize, 2)
        self.assertEqual(program.steps[0].sql, self._to_sql())

    def test_thread_hammer_returns_consistent_results(self) -> None:
        results: list[str] = []
        errors: list[BaseException] = []

        def worker() -> None:
            try:
                for i in range(20):
                    results.append(self._to_sql(f"{_QUERY} LIMIT {i % 5 + 1}"))
            except BaseException as exc:  # noqa: BLE001 - surfaced below
                errors.append(exc)

        threads = [threading.Thread(target=worker) for _ in range(8)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        self.assertEqual(errors, [])
        expected = {self._to_sql(f"{_QUERY} LIMIT {n}") for n in range(1, 6)}
        self.assertEqual(set(results), expected)


if __name__ == "__main__":
    unittest.main()
