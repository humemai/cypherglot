from __future__ import annotations

import unittest

from cypherglot.schema import SchemaContractError
from cypherglot.schema_commands import graph_schema_from_text, schema_ddl_from_text


class SchemaCommandTests(unittest.TestCase):
    def test_graph_schema_from_text_builds_graph_schema(self) -> None:
        schema = graph_schema_from_text(
            """
            CREATE NODE User (name STRING NOT NULL, age INTEGER, active BOOLEAN);
            CREATE NODE Company (name STRING NOT NULL);
            CREATE EDGE WORKS_AT FROM User TO Company (since INTEGER NOT NULL);
            CREATE INDEX user_name_idx ON NODE User(name);
            CREATE INDEX works_at_since_idx ON EDGE WORKS_AT(since);
            """
        )

        self.assertEqual(
            [node.name for node in schema.node_types],
            ["User", "Company"],
        )
        self.assertEqual([edge.name for edge in schema.edge_types], ["WORKS_AT"])
        self.assertEqual(
            [field.name for field in schema.node_type("User").properties],
            ["name", "age", "active"],
        )
        self.assertFalse(schema.node_type("User").properties[0].nullable)
        self.assertTrue(schema.node_type("User").properties[1].nullable)
        self.assertEqual(schema.edge_type("WORKS_AT").source_type, "User")
        self.assertEqual(schema.edge_type("WORKS_AT").target_type, "Company")
        self.assertFalse(schema.edge_type("WORKS_AT").properties[0].nullable)
        self.assertEqual(
            [property_index.name for property_index in schema.property_indexes],
            ["user_name_idx", "works_at_since_idx"],
        )

    def test_schema_ddl_from_text_lowers_schema_commands_to_backend_ddl(self) -> None:
        ddl = "\n".join(
            schema_ddl_from_text(
                """
                CREATE NODE User (name STRING NOT NULL);
                CREATE NODE Company (name STRING NOT NULL);
                CREATE EDGE WORKS_AT FROM User TO Company (since INTEGER);
                CREATE INDEX user_name_idx ON NODE User(name);
                """,
                "postgresql",
            )
        )

        self.assertIn("CREATE TABLE cg_node_user", ddl)
        self.assertIn("CREATE TABLE cg_edge_works_at", ddl)
        self.assertIn(
            "CREATE INDEX idx_cg_edge_works_at_from_id ON cg_edge_works_at(from_id);",
            ddl,
        )
        self.assertIn(
            (
                "CREATE INDEX idx_cg_edge_works_at_to_from ON "
                "cg_edge_works_at(to_id, from_id);"
            ),
            ddl,
        )
        self.assertIn(
            "CREATE INDEX user_name_idx ON cg_node_user(name);",
            ddl,
        )

    def test_graph_schema_from_text_rejects_unknown_commands(self) -> None:
        with self.assertRaisesRegex(
            SchemaContractError,
            "Unsupported schema command",
        ):
            graph_schema_from_text("DROP NODE User;")

    def test_graph_schema_from_text_rejects_invalid_create_index_commands(self) -> None:
        with self.assertRaisesRegex(
            SchemaContractError,
            "Unsupported schema command",
        ):
            graph_schema_from_text(
                """
                CREATE NODE User (name STRING NOT NULL);
                CREATE INDEX user_name_idx ON User(name);
                """
            )

        with self.assertRaisesRegex(
            SchemaContractError,
            "requires at least one property name",
        ):
            graph_schema_from_text(
                """
                CREATE NODE User (name STRING NOT NULL);
                CREATE INDEX user_name_idx ON NODE User();
                """
            )

    def test_graph_schema_from_text_rejects_invalid_property_declarations(self) -> None:
        with self.assertRaisesRegex(
            SchemaContractError,
            "Invalid property declaration",
        ):
            graph_schema_from_text("CREATE NODE User (name);")

        with self.assertRaisesRegex(
            SchemaContractError,
            "Invalid property declaration",
        ):
            graph_schema_from_text("CREATE NODE User (profile JSON);")

    def test_graph_schema_from_text_rejects_invalid_or_empty_input(self) -> None:
        with self.assertRaisesRegex(SchemaContractError, "must not be empty"):
            graph_schema_from_text("  ")

        with self.assertRaisesRegex(
            SchemaContractError,
            "references unknown source node type",
        ):
            graph_schema_from_text(
                "CREATE EDGE WORKS_AT FROM User TO Company (since INTEGER);"
            )
