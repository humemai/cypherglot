# pyright: reportPrivateUsage=false
# pylint: disable=protected-access
"""Pure-logic unit tests for the Apache AGE runtime helpers.

These run without Docker. The end-to-end native-Cypher comparison against a real
AGE server lives in tests/test_cypher_conformance_age.py (Docker-gated)."""

from __future__ import annotations

import unittest

from cypherglot.schema import (
    CompilerSchemaContext,
    EdgeTypeSpec,
    GraphSchema,
    NodeTypeSpec,
    PropertyField,
)
from scripts.benchmarks.runtime.age import (
    AGE_UNSUPPORTED_QUERIES,
    _age_uses_union,
    _agtype_property_map,
    age_column_clause,
    age_return_columns,
    parse_agtype,
)


_SCHEMA = GraphSchema(
    node_types=(
        NodeTypeSpec(
            "User",
            (
                PropertyField("name", "string"),
                PropertyField("age", "integer"),
                PropertyField("active", "boolean"),
            ),
        ),
        NodeTypeSpec("Company", (PropertyField("name", "string"),)),
    ),
    edge_types=(
        EdgeTypeSpec("KNOWS", "User", "User"),
        EdgeTypeSpec("WORKS_AT", "User", "Company"),
    ),
)


class AgeReturnColumnTests(unittest.TestCase):
    def setUp(self) -> None:
        self.ctx = CompilerSchemaContext.type_aware(_SCHEMA)

    def test_scalar_and_predicate_projection_columns(self) -> None:
        columns = age_return_columns(
            "MATCH (u:User) RETURN u.name AS name, u.age >= 30 AS senior ORDER BY name",
            self.ctx,
        )
        self.assertEqual(columns, ["name", "senior"])

    def test_aggregate_columns(self) -> None:
        columns = age_return_columns(
            "MATCH (u:User) RETURN min(u.age) AS mn, max(u.age) AS mx", self.ctx
        )
        self.assertEqual(columns, ["mn", "mx"])

    def test_collect_columns(self) -> None:
        columns = age_return_columns(
            "MATCH (a:User)-[:KNOWS]->(b:User) "
            "RETURN a.name AS name, collect(b.name) AS friends ORDER BY name",
            self.ctx,
        )
        self.assertEqual(columns, ["name", "friends"])

    def test_column_clause_quotes_each_column_as_agtype(self) -> None:
        self.assertEqual(
            age_column_clause(["name", "friends"]),
            '"name" agtype, "friends" agtype',
        )


class AgtypeParsingTests(unittest.TestCase):
    def test_parse_scalar_agtype_strings(self) -> None:
        self.assertEqual(parse_agtype('"Alice"'), "Alice")
        self.assertEqual(parse_agtype("30"), 30)
        self.assertEqual(parse_agtype("true"), True)
        self.assertIsNone(parse_agtype("null"))

    def test_parse_list_agtype(self) -> None:
        self.assertEqual(parse_agtype('["Bob", "Carol"]'), ["Bob", "Carol"])

    def test_passthrough_native_values(self) -> None:
        self.assertEqual(parse_agtype(42), 42)
        self.assertIsNone(parse_agtype(None))

    def test_non_json_string_passthrough(self) -> None:
        self.assertEqual(parse_agtype("plain text"), "plain text")


class AgtypePropertyMapTests(unittest.TestCase):
    def test_property_map_renders_typed_literals(self) -> None:
        rendered = _agtype_property_map(
            {"id": 1, "name": "Alice", "active": True, "score": None}
        )
        self.assertEqual(
            rendered, "{id: 1, name: 'Alice', active: true, score: null}"
        )

    def test_property_map_escapes_quotes(self) -> None:
        self.assertEqual(
            _agtype_property_map({"name": "O'Brien"}), "{name: 'O\\'Brien'}"
        )


class AgeUnsupportedTests(unittest.TestCase):
    def test_union_queries_flagged(self) -> None:
        self.assertTrue(_age_uses_union("MATCH (u:User) RETURN u.name UNION MATCH ..."))
        self.assertFalse(_age_uses_union("MATCH (u:User) RETURN u.name"))

    def test_unsupported_set_covers_union_conformance_queries(self) -> None:
        # The UNION conformance queries must always be flagged.
        self.assertTrue(
            {"union_distinct", "union_dedup", "union_all_keeps"}.issubset(
                AGE_UNSUPPORTED_QUERIES
            )
        )

    def test_lower_upper_queries_no_longer_skipped(self) -> None:
        # The lower()/toLower() function-name gap is closed: CypherGlot accepts the
        # standard Cypher names and the corpus uses them, so these run on AGE.
        self.assertNotIn("olap_with_where_lower_projection", AGE_UNSUPPORTED_QUERIES)
        self.assertNotIn(
            "olap_relationship_function_projection", AGE_UNSUPPORTED_QUERIES
        )


if __name__ == "__main__":
    unittest.main()


class AgeBulkLoadCsvTests(unittest.TestCase):
    def test_agtype_cells_are_typed_literals(self) -> None:
        from scripts.benchmarks.runtime.age import _agtype_csv_cell

        self.assertEqual(_agtype_csv_cell("54", "integer"), "54")
        self.assertEqual(_agtype_csv_cell("83.2", "float"), "83.2")
        self.assertEqual(_agtype_csv_cell("1", "boolean"), "true")
        self.assertEqual(_agtype_csv_cell("0", "boolean"), "false")
        self.assertEqual(_agtype_csv_cell("plain", "string"), '"plain"')
        self.assertEqual(
            _agtype_csv_cell('say "hi"', "string"), '"say \\"hi\\""'
        )

    def test_write_age_load_csvs_shapes(self) -> None:
        import csv as _csv
        import tempfile
        from pathlib import Path as _Path

        from scripts.benchmarks.common.shared import (
            RuntimeScale,
            _build_graph_schema,
        )
        from scripts.benchmarks.common.runtime_shared import (
            _prepare_generated_graph_fixture,
        )
        from scripts.benchmarks.runtime.age import _write_age_load_csvs

        scale = RuntimeScale(
            node_type_count=3, edge_type_count=3, nodes_per_type=5,
            edges_per_source=1, ingest_batch_size=5,
        )
        schema, plans = _build_graph_schema(scale)
        fixture = _prepare_generated_graph_fixture(
            scale=scale, graph_schema=schema, edge_plans=plans,
            index_mode="indexed",
        )
        try:
            with tempfile.TemporaryDirectory() as tmp:
                out = _Path(tmp) / "load"
                paths = _write_age_load_csvs(fixture, schema, out)
                node_rows = list(
                    _csv.reader(paths[schema.node_types[0].table_name].open())
                )
                self.assertEqual(node_rows[0][0], "id")
                self.assertEqual(node_rows[1][0], "1")  # id stays plain
                self.assertTrue(node_rows[1][1].startswith('"'))  # name quoted
                edge_rows = list(
                    _csv.reader(paths[schema.edge_types[0].table_name].open())
                )
                self.assertEqual(
                    edge_rows[0][:4],
                    ["start_id", "start_vertex_type", "end_id", "end_vertex_type"],
                )
                self.assertEqual(edge_rows[1][1], schema.edge_types[0].source_type)
                self.assertIn(edge_rows[1][7], ("true", "false"))  # active typed
        finally:
            fixture.close()
