from __future__ import annotations

import sqlite3
import unittest

import cypherglot
from cypherglot.schema import (
    CompilerSchemaContext,
    EdgeTypeSpec,
    GraphSchema,
    NodeTypeSpec,
    PropertyField,
)


class TypeAwareSQLiteRuntimeTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self.conn = sqlite3.connect(":memory:")
        self.graph_schema = GraphSchema(
            node_types=(
                NodeTypeSpec(
                    name="User",
                    properties=(
                        PropertyField("name", "string"),
                        PropertyField("age", "integer"),
                    ),
                ),
                NodeTypeSpec(
                    name="Company",
                    properties=(PropertyField("name", "string"),),
                ),
                NodeTypeSpec(
                    name="Person",
                    properties=(PropertyField("name", "string"),),
                ),
            ),
            edge_types=(
                EdgeTypeSpec(
                    name="KNOWS",
                    source_type="User",
                    target_type="User",
                ),
                EdgeTypeSpec(
                    name="WORKS_AT",
                    source_type="User",
                    target_type="Company",
                    properties=(PropertyField("since", "integer"),),
                ),
                EdgeTypeSpec(
                    name="INTRODUCED",
                    source_type="User",
                    target_type="Person",
                ),
            ),
        )
        self.conn.executescript("\n".join(self.graph_schema.ddl("sqlite")))

    def tearDown(self) -> None:
        self.conn.close()

    def _type_aware_schema_context(
        self,
        graph_schema: GraphSchema | None = None,
    ) -> CompilerSchemaContext:
        return CompilerSchemaContext.type_aware(
            self.graph_schema if graph_schema is None else graph_schema,
        )

    def _execute_program(self, program: cypherglot.RenderedCypherProgram) -> None:
        bindings: dict[str, object] = {}
        for step in program.steps:
            if isinstance(step, cypherglot.RenderedCypherLoop):
                rows = self.conn.execute(step.source, bindings).fetchall()
                for row in rows:
                    loop_bindings = bindings | dict(
                        zip(step.row_bindings, row, strict=True)
                    )
                    for statement in step.body:
                        cursor = self.conn.execute(statement.sql, loop_bindings)
                        if statement.bind_columns:
                            returned = cursor.fetchone()
                            self.assertIsNotNone(returned)
                            assert returned is not None
                            loop_bindings |= dict(
                                zip(statement.bind_columns, returned, strict=True)
                            )
                continue

            cursor = self.conn.execute(step.sql, bindings)
            if step.bind_columns:
                returned = cursor.fetchone()
                self.assertIsNotNone(returned)
                assert returned is not None
                bindings |= dict(zip(step.bind_columns, returned, strict=True))

        self.conn.commit()

    def _seed_type_aware_graph(self) -> None:
        self.conn.execute(
            "INSERT INTO cg_node_user (id, name, age) VALUES (?, ?, ?)",
            (1, "Alice", 30),
        )
        self.conn.execute(
            "INSERT INTO cg_node_user (id, name, age) VALUES (?, ?, ?)",
            (2, "Bob", 25),
        )
        self.conn.execute(
            "INSERT INTO cg_node_company (id, name) VALUES (?, ?)",
            (10, "Acme"),
        )
        self.conn.execute(
            "INSERT INTO cg_node_company (id, name) VALUES (?, ?)",
            (11, "Bravo"),
        )
        self.conn.execute(
            "INSERT INTO cg_edge_knows (id, from_id, to_id) VALUES (?, ?, ?)",
            (90, 1, 2),
        )
        self.conn.execute(
            (
                "INSERT INTO cg_edge_works_at (id, from_id, to_id, since) "
                "VALUES (?, ?, ?, ?)"
            ),
            (100, 1, 10, 2020),
        )
        self.conn.execute(
            (
                "INSERT INTO cg_edge_works_at (id, from_id, to_id, since) "
                "VALUES (?, ?, ?, ?)"
            ),
            (101, 2, 11, 2021),
        )
        self.conn.commit()

    def _seed_type_aware_variable_length_graph(self) -> None:
        self._seed_type_aware_graph()
        self.conn.execute(
            "INSERT INTO cg_node_user (id, name, age) VALUES (?, ?, ?)",
            (3, "Cara", 20),
        )
        self.conn.execute(
            "INSERT INTO cg_edge_knows (id, from_id, to_id) VALUES (?, ?, ?)",
            (91, 2, 3),
        )
        self.conn.commit()
