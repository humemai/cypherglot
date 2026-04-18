from __future__ import annotations

import unittest

from cypherglot.schema import (
    CompilerSchemaContext,
    EdgeTypeSpec,
    GraphSchema,
    NodeTypeSpec,
    PropertyIndexSpec,
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

        ddl = "\n".join(schema.ddl("sqlite"))

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
            "CREATE INDEX idx_cg_edge_works_at_from_id ON cg_edge_works_at(from_id);",
            ddl,
        )
        self.assertIn(
            "CREATE INDEX idx_cg_edge_works_at_to_id ON cg_edge_works_at(to_id);",
            ddl,
        )
        self.assertIn(
            (
                "CREATE INDEX idx_cg_edge_works_at_from_to ON "
                "cg_edge_works_at(from_id, to_id);"
            ),
            ddl,
        )
        self.assertIn(
            (
                "CREATE INDEX idx_cg_edge_works_at_to_from ON "
                "cg_edge_works_at(to_id, from_id);"
            ),
            ddl,
        )

    def test_graph_schema_duckdb_ddl_builds_backend_specific_tables(self) -> None:
        schema = GraphSchema(
            node_types=(
                NodeTypeSpec(
                    name="User",
                    properties=(
                        PropertyField("name", "string", nullable=False),
                        PropertyField("active", "boolean"),
                        PropertyField("nickname", "string"),
                    ),
                ),
            ),
            edge_types=(
                EdgeTypeSpec(
                    name="KNOWS",
                    source_type="User",
                    target_type="User",
                ),
            ),
        )

        ddl = "\n".join(schema.ddl("duckdb"))
        self.assertIn("CREATE SEQUENCE cg_node_user_id_seq START 1;", ddl)
        self.assertIn("CREATE TABLE cg_node_user", ddl)
        self.assertIn(
            "id BIGINT PRIMARY KEY DEFAULT nextval('cg_node_user_id_seq')",
            ddl,
        )
        self.assertIn("name VARCHAR NOT NULL", ddl)
        self.assertIn("active BOOLEAN", ddl)
        self.assertIn("nickname VARCHAR", ddl)
        self.assertNotIn("STRICT", ddl)
        self.assertNotIn("PRAGMA foreign_keys = ON;", ddl)
        self.assertNotIn("FOREIGN KEY", ddl)
        self.assertIn(
            "CREATE INDEX idx_cg_edge_knows_from_id ON cg_edge_knows(from_id);",
            ddl,
        )
        self.assertIn(
            "CREATE INDEX idx_cg_edge_knows_to_id ON cg_edge_knows(to_id);",
            ddl,
        )
        self.assertIn(
            "CREATE INDEX idx_cg_edge_knows_from_to ON cg_edge_knows(from_id, to_id);",
            ddl,
        )
        self.assertIn(
            "CREATE INDEX idx_cg_edge_knows_to_from ON cg_edge_knows(to_id, from_id);",
            ddl,
        )

    def test_graph_schema_postgresql_ddl_builds_backend_specific_tables(self) -> None:
        schema = GraphSchema(
            node_types=(
                NodeTypeSpec(
                    name="User",
                    properties=(PropertyField("score", "float"),),
                ),
                NodeTypeSpec(name="Company"),
            ),
            edge_types=(
                EdgeTypeSpec(
                    name="WORKS_AT",
                    source_type="User",
                    target_type="Company",
                    properties=(PropertyField("note", "string"),),
                ),
            ),
        )

        ddl = "\n".join(schema.ddl("postgresql"))

        self.assertIn("CREATE SEQUENCE cg_node_user_id_seq START 1;", ddl)
        self.assertIn("score DOUBLE PRECISION", ddl)
        self.assertIn("note TEXT", ddl)
        self.assertIn(
            "id BIGINT PRIMARY KEY DEFAULT nextval('cg_node_user_id_seq')",
            ddl,
        )
        self.assertIn(
            "FOREIGN KEY (from_id) REFERENCES cg_node_user(id) ON DELETE CASCADE",
            ddl,
        )
        self.assertIn(
            "CREATE INDEX idx_cg_edge_works_at_from_id ON cg_edge_works_at(from_id);",
            ddl,
        )
        self.assertIn(
            "CREATE INDEX idx_cg_edge_works_at_to_id ON cg_edge_works_at(to_id);",
            ddl,
        )
        self.assertIn(
            (
                "CREATE INDEX idx_cg_edge_works_at_from_to ON "
                "cg_edge_works_at(from_id, to_id);"
            ),
            ddl,
        )
        self.assertIn(
            (
                "CREATE INDEX idx_cg_edge_works_at_to_from ON "
                "cg_edge_works_at(to_id, from_id);"
            ),
            ddl,
        )

    def test_property_field_rejects_removed_json_logical_type(self) -> None:
        with self.assertRaises(SchemaContractError) as raised:
            PropertyField("profile", "json").column_sql("sqlite")

        self.assertIn("Unsupported logical type 'json'", str(raised.exception))

    def test_graph_schema_ddl_emits_explicit_property_indexes(self) -> None:
        schema = GraphSchema(
            node_types=(
                NodeTypeSpec(
                    name="User",
                    properties=(
                        PropertyField("name", "string"),
                        PropertyField("age", "integer"),
                    ),
                ),
                NodeTypeSpec(name="Company"),
            ),
            edge_types=(
                EdgeTypeSpec(
                    name="WORKS_AT",
                    source_type="User",
                    target_type="Company",
                    properties=(
                        PropertyField("since", "integer"),
                        PropertyField("active", "boolean"),
                    ),
                ),
            ),
            property_indexes=(
                PropertyIndexSpec(
                    name="user_name_age_idx",
                    target_kind="node",
                    target_type="User",
                    property_names=("name", "age"),
                ),
                PropertyIndexSpec(
                    name="works_at_since_idx",
                    target_kind="edge",
                    target_type="WORKS_AT",
                    property_names=("since",),
                ),
            ),
        )

        ddl = "\n".join(schema.ddl("sqlite"))

        self.assertIn(
            "CREATE INDEX user_name_age_idx ON cg_node_user(name, age);",
            ddl,
        )
        self.assertIn(
            "CREATE INDEX works_at_since_idx ON cg_edge_works_at(since);",
            ddl,
        )

    def test_graph_schema_rejects_invalid_property_indexes(self) -> None:
        unknown_target_schema = GraphSchema(
            node_types=(NodeTypeSpec(name="User"),),
            edge_types=(),
            property_indexes=(
                PropertyIndexSpec(
                    name="missing_idx",
                    target_kind="node",
                    target_type="Company",
                    property_names=("name",),
                ),
            ),
        )

        with self.assertRaisesRegex(SchemaContractError, "unknown node type"):
            unknown_target_schema.validate()

        unknown_property_schema = GraphSchema(
            node_types=(
                NodeTypeSpec(
                    name="User",
                    properties=(PropertyField("name", "string"),),
                ),
            ),
            edge_types=(),
            property_indexes=(
                PropertyIndexSpec(
                    name="missing_property_idx",
                    target_kind="node",
                    target_type="User",
                    property_names=("age",),
                ),
            ),
        )

        with self.assertRaisesRegex(SchemaContractError, "unknown node property"):
            unknown_property_schema.validate()

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
