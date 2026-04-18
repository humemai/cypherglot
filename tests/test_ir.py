from __future__ import annotations

import unittest

from cypherglot.ir import (
    GraphRelationalCreateNodeWriteIR,
    GraphRelationalCreateRelationshipWriteIR,
    GraphRelationalMergeNodeWriteIR,
    GraphRelationalReadIR,
    SQLBackend,
    build_graph_relational_ir,
)
from cypherglot.normalize import normalize_cypher_text
from cypherglot.schema import (
    CompilerSchemaContext,
    GraphSchema,
    NodeTypeSpec,
    PropertyField,
)


class GraphRelationalIRTests(unittest.TestCase):
    def setUp(self) -> None:
        graph_schema = GraphSchema(
            node_types=(
                NodeTypeSpec(
                    name="User",
                    properties=(
                        PropertyField("name", "string"),
                        PropertyField("age", "integer"),
                    ),
                ),
            ),
            edge_types=(),
        )
        self.schema_context = CompilerSchemaContext.type_aware(graph_schema)

    def test_build_graph_relational_ir_for_match_node(self) -> None:
        statement = normalize_cypher_text(
            "MATCH (u:User) RETURN u.name ORDER BY u.name"
        )

        ir_program = build_graph_relational_ir(
            statement,
            schema_context=self.schema_context,
        )

        self.assertEqual(ir_program.statement.family, "match-node")
        self.assertFalse(ir_program.statement.is_write)
        self.assertFalse(ir_program.statement.uses_variable_length)
        self.assertIn(SQLBackend.SQLITE, ir_program.backend_capabilities)
        self.assertIn(SQLBackend.DUCKDB, ir_program.backend_capabilities)
        self.assertIn(SQLBackend.POSTGRESQL, ir_program.backend_capabilities)
        sqlite_capabilities = ir_program.backend_capabilities[SQLBackend.SQLITE]
        duckdb_capabilities = ir_program.backend_capabilities[SQLBackend.DUCKDB]
        postgresql_capabilities = ir_program.backend_capabilities[
            SQLBackend.POSTGRESQL
        ]
        self.assertIsNone(sqlite_capabilities.render_dialect)
        self.assertIsNone(sqlite_capabilities.numeric_coercion_sql_type)
        self.assertFalse(sqlite_capabilities.numeric_coercion_is_tolerant)
        self.assertFalse(sqlite_capabilities.integer_cast_requires_truncation)
        self.assertEqual(duckdb_capabilities.render_dialect, "duckdb")
        self.assertEqual(duckdb_capabilities.numeric_coercion_sql_type, "DOUBLE")
        self.assertTrue(duckdb_capabilities.numeric_coercion_is_tolerant)
        self.assertTrue(duckdb_capabilities.integer_cast_requires_truncation)
        self.assertEqual(postgresql_capabilities.render_dialect, "postgres")
        self.assertIsNone(postgresql_capabilities.numeric_coercion_sql_type)
        self.assertTrue(postgresql_capabilities.integer_cast_requires_truncation)

    def test_build_graph_relational_ir_for_create_node(self) -> None:
        statement = normalize_cypher_text("CREATE (:User {name: 'Alice'})")

        ir_program = build_graph_relational_ir(
            statement,
            schema_context=self.schema_context,
        )

        self.assertEqual(ir_program.statement.family, "create-node")
        self.assertTrue(ir_program.statement.is_write)
        self.assertIsInstance(
            ir_program.statement.write_query,
            GraphRelationalCreateNodeWriteIR,
        )

    def test_build_graph_relational_ir_for_create_relationship(self) -> None:
        statement = normalize_cypher_text(
            "CREATE (:User {name: 'Alice'})-[:KNOWS]->(:User {name: 'Bob'})"
        )

        ir_program = build_graph_relational_ir(
            statement,
            schema_context=self.schema_context,
        )

        self.assertEqual(ir_program.statement.family, "create-relationship")
        self.assertTrue(ir_program.statement.is_write)
        self.assertIsInstance(
            ir_program.statement.write_query,
            GraphRelationalCreateRelationshipWriteIR,
        )

    def test_build_graph_relational_ir_for_merge_node(self) -> None:
        statement = normalize_cypher_text("MERGE (:User {name: 'Alice'})")

        ir_program = build_graph_relational_ir(
            statement,
            schema_context=self.schema_context,
        )

        self.assertEqual(ir_program.statement.family, "merge-node")
        self.assertTrue(ir_program.statement.is_write)
        self.assertIsInstance(
            ir_program.statement.write_query,
            GraphRelationalMergeNodeWriteIR,
        )

    def test_build_graph_relational_ir_for_traversal_backed_write_uses_read_ir(
        self,
    ) -> None:
        statement = normalize_cypher_text(
            "MATCH (u:User)-[:KNOWS]->(v:User) "
            "CREATE (u)-[:KNOWS]->(:User {name: 'Cleo'})"
        )

        ir_program = build_graph_relational_ir(
            statement,
            schema_context=self.schema_context,
        )

        write_query = ir_program.statement.write_query
        self.assertIsNotNone(write_query)
        assert write_query is not None
        self.assertEqual(
            ir_program.statement.family,
            "match-create-relationship-from-traversal",
        )
        self.assertIsInstance(write_query.source, GraphRelationalReadIR)
        self.assertEqual(write_query.source.source_kind, "relationship")


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
