"""Structured diagnostics: CypherCompileError, reject(), construct scanner (M2)."""

from __future__ import annotations

import unittest

import cypherglot
from cypherglot import CompilerSchemaContext, CypherCompileError, GraphSchema
from cypherglot.errors import reject
from cypherglot.schema import EdgeTypeSpec, NodeTypeSpec, PropertyField

_SCHEMA = GraphSchema(
    node_types=(
        NodeTypeSpec(
            name="User",
            properties=(PropertyField(name="name", logical_type="string"),),
        ),
    ),
    edge_types=(
        EdgeTypeSpec(
            name="KNOWS",
            source_type="User",
            target_type="User",
            properties=(),
        ),
    ),
)


class CypherCompileErrorTests(unittest.TestCase):
    def test_is_value_error(self) -> None:
        error = CypherCompileError("nope")
        self.assertIsInstance(error, ValueError)

    def test_fields_default_to_none(self) -> None:
        error = CypherCompileError("nope")
        self.assertEqual(error.message, "nope")
        self.assertIsNone(error.line)
        self.assertIsNone(error.column)
        self.assertIsNone(error.construct)
        self.assertIsNone(error.hint)

    def test_rendering_includes_location_and_hint(self) -> None:
        error = CypherCompileError(
            "nope.", line=3, column=7, construct="thing", hint="Do it differently."
        )
        self.assertEqual(str(error), "nope. (line 3, column 7) Hint: Do it differently.")
        self.assertEqual(error.message, "nope.")

    def test_reject_extracts_antlr_location(self) -> None:
        class Token:
            line = 5
            column = 12

        class Ctx:
            start = Token()

        with self.assertRaises(CypherCompileError) as caught:
            reject("nope", ctx=Ctx(), construct="thing")
        self.assertEqual(caught.exception.line, 5)
        self.assertEqual(caught.exception.column, 12)
        self.assertEqual(caught.exception.construct, "thing")

    def test_reject_without_ctx(self) -> None:
        with self.assertRaises(CypherCompileError) as caught:
            reject("nope")
        self.assertIsNone(caught.exception.line)


class DiagnosticSurfaceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.ctx = CompilerSchemaContext.type_aware(_SCHEMA)

    def _compile_error(self, query: str) -> CypherCompileError:
        with self.assertRaises(CypherCompileError) as caught:
            cypherglot.to_sql(query, backend="sqlite", schema_context=self.ctx)
        return caught.exception

    def test_syntax_error_carries_location(self) -> None:
        error = self._compile_error("MATCH (a:User RETURN a.name")
        self.assertEqual(error.construct, "syntax_error")
        self.assertEqual(error.line, 1)
        self.assertIsNotNone(error.column)

    def test_scanner_named_construct_rejections(self) -> None:
        cases = {
            "MATCH (a:User) RETURN [x IN [1,2] | x] AS xs": "list_comprehension",
            "MATCH (a:User) RETURN [(a)-[:KNOWS]->(b) | b.name] AS names": (
                "pattern_comprehension"
            ),
            "MATCH (a:User) WHERE EXISTS { (a)-[:KNOWS]->(:User) } RETURN a.name": (
                "exists_subquery"
            ),
            "CALL db.labels()": "call_procedure",
            "MATCH (a:User) WHERE all(x IN [1] WHERE x > 0) RETURN a.name": (
                "quantified_predicate"
            ),
            "MATCH p = (a:User)-[:KNOWS]->(b:User) RETURN a.name": "named_path",
            "MATCH (a:User) RETURN shortestPath((a)-[:KNOWS]->(a)) AS p": (
                "shortest_path"
            ),
            # reduce()'s initializer syntax is not in the grammar at all, so it
            # surfaces as a located syntax error rather than a scanner hit.
            "MATCH (a:User) RETURN reduce(s = 0, x IN [1,2] | s + x) AS total": (
                "syntax_error"
            ),
        }
        for query, construct in cases.items():
            with self.subTest(construct=construct):
                error = self._compile_error(query)
                self.assertEqual(error.construct, construct)
                self.assertIsNotNone(error.line)

    def test_multi_label_rejected_with_hint(self) -> None:
        error = self._compile_error("MATCH (a:User:Admin) RETURN a.name")
        self.assertEqual(error.construct, "multi_label")
        self.assertIn("Hint:", str(error))

    def test_gate_rejections_carry_construct(self) -> None:
        error = self._compile_error(
            "MATCH (a:User)-[:KNOWS*]->(b:User) RETURN b.name"
        )
        self.assertEqual(error.construct, "unbounded_variable_length")
        self.assertIn("upper bound", str(error))

    def test_admitted_queries_still_compile(self) -> None:
        sql = cypherglot.to_sql(
            "MATCH (a:User)-[r:KNOWS]->(b:User) RETURN a.name, b.name",
            backend="sqlite",
            schema_context=self.ctx,
        )
        self.assertIn("SELECT", sql)


if __name__ == "__main__":
    unittest.main()
