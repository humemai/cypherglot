from __future__ import annotations

import unittest

from cypherglot.schema import (
    CompilerSchemaContext,
    EdgeTypeSpec,
    GraphSchema,
    NodeTypeSpec,
    PropertyField,
    SchemaContractError,
    edge_table_name,
    node_table_name,
)


class SchemaContractTests(unittest.TestCase):
    def test_type_aware_compiler_schema_context_requires_graph_schema(self) -> None:
        with self.assertRaises(SchemaContractError):
            CompilerSchemaContext().validate()

    def test_type_aware_compiler_schema_context_accepts_valid_graph_schema(
        self,
    ) -> None:
        context = CompilerSchemaContext.type_aware(
            GraphSchema(
                node_types=(NodeTypeSpec(name="User"),),
                edge_types=(),
            )
        )

        self.assertEqual(context.layout, "type-aware")

    def test_type_aware_compiler_schema_context_defaults_to_type_aware_layout(
        self,
    ) -> None:
        schema = GraphSchema(
            node_types=(NodeTypeSpec(name="User"),),
            edge_types=(),
        )

        context = CompilerSchemaContext(graph_schema=schema)

        context.validate()

        self.assertEqual(context.layout, "type-aware")

    def test_table_name_helpers_normalize_type_names(self) -> None:
        self.assertEqual(node_table_name("User Profile"), "cg_node_user_profile")
        self.assertEqual(edge_table_name("WORKS-WITH"), "cg_edge_works_with")

    def test_graph_schema_sqlite_ddl_builds_type_aware_tables(self) -> None:
        schema = GraphSchema(
            node_types=(
                NodeTypeSpec(
                    name="User",
                    properties=(
                        PropertyField("name", "string", nullable=False),
                        PropertyField("age", "integer"),
                    ),
                ),
                NodeTypeSpec(
                    name="Company",
                    properties=(PropertyField("name", "string", nullable=False),),
                ),
            ),
            edge_types=(
                EdgeTypeSpec(
                    name="WORKS_AT",
                    source_type="User",
                    target_type="Company",
                    properties=(PropertyField("since", "integer"),),
                ),
            ),
        )

        ddl = "\n".join(schema.sqlite_ddl())

        self.assertIn("CREATE TABLE cg_node_user", ddl)
        self.assertIn("name TEXT NOT NULL", ddl)
        self.assertIn("age INTEGER", ddl)
        self.assertIn("CREATE TABLE cg_node_company", ddl)
        self.assertIn("CREATE TABLE cg_edge_works_at", ddl)
        self.assertIn(
            "FOREIGN KEY (from_id) REFERENCES cg_node_user(id) ON DELETE CASCADE",
            ddl,
        )
        self.assertIn(
            "FOREIGN KEY (to_id) REFERENCES cg_node_company(id) ON DELETE CASCADE",
            ddl,
        )
        self.assertIn(
            (
                "CREATE INDEX idx_cg_edge_works_at_from_to ON "
                "cg_edge_works_at(from_id, to_id);"
            ),
            ddl,
        )

    def test_graph_schema_rejects_unknown_edge_endpoint_types(self) -> None:
        schema = GraphSchema(
            node_types=(NodeTypeSpec(name="User"),),
            edge_types=(
                EdgeTypeSpec(
                    name="KNOWS",
                    source_type="User",
                    target_type="Person",
                ),
            ),
        )

        with self.assertRaises(SchemaContractError):
            schema.validate()

    def test_graph_schema_rejects_identifier_collisions(self) -> None:
        schema = GraphSchema(
            node_types=(
                NodeTypeSpec(name="User-Profile"),
                NodeTypeSpec(name="User Profile"),
            ),
            edge_types=(),
        )

        with self.assertRaises(SchemaContractError):
            schema.validate()
