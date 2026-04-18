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


class SQLiteRuntimeTests(unittest.TestCase):
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
                    properties=(
                        PropertyField("note", "string"),
                        PropertyField("since", "integer"),
                        PropertyField("strength", "integer"),
                    ),
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
                    target_type="User",
                ),
                EdgeTypeSpec(
                    name="INTRODUCED_TO_COMPANY",
                    source_type="User",
                    target_type="Company",
                ),
                EdgeTypeSpec(
                    name="INTRODUCED_TO_PERSON",
                    source_type="User",
                    target_type="Person",
                ),
            ),
        )
        self.schema_context = CompilerSchemaContext.type_aware(self.graph_schema)
        self.conn.executescript("\n".join(self.graph_schema.ddl("sqlite")))

    def tearDown(self) -> None:
        self.conn.close()

    def test_compiled_match_return_executes_on_sqlite(self) -> None:
        self._seed_graph()

        rows = self.conn.execute(
            cypherglot.to_sql(
                "MATCH (u:User) RETURN u.name ORDER BY u.name",
                backend="sqlite",
                schema_context=self.schema_context,
            )
        ).fetchall()

        self.assertEqual(rows, [("Alice",), ("Bob",)])

    def test_compiled_entity_return_executes_on_sqlite(self) -> None:
        self._seed_graph()

        rows = self.conn.execute(
            cypherglot.to_sql(
                "MATCH (u:User) RETURN u ORDER BY u.name",
                backend="sqlite",
                schema_context=self.schema_context,
            )
        ).fetchall()

        self.assertEqual(rows, [(1, "User", "Alice", 30), (2, "User", "Bob", 25)])

    def test_compiled_match_with_return_executes_on_sqlite(self) -> None:
        self._seed_graph()

        rows = self.conn.execute(
            cypherglot.to_sql(
                "MATCH (u:User) WITH u.name AS name RETURN name ORDER BY name",
                backend="sqlite",
                schema_context=self.schema_context,
            )
        ).fetchall()

        self.assertEqual(rows, [("Alice",), ("Bob",)])

    def test_compiled_variable_length_match_executes_on_sqlite(self) -> None:
        self._seed_user_chain_graph()

        rows = self.conn.execute(
            cypherglot.to_sql(
                "MATCH (a:User)-[:KNOWS*1..2]->(b:User) "
                "WHERE a.name = 'Alice' RETURN b.name AS friend ORDER BY friend",
                backend="sqlite",
                schema_context=self.schema_context,
            )
        ).fetchall()

        self.assertEqual(rows, [("Bob",), ("Cara",)])

    def test_compiled_optional_match_return_executes_on_sqlite(self) -> None:
        self._seed_graph()

        rows = self.conn.execute(
            cypherglot.to_sql(
                "OPTIONAL MATCH (u:User) WHERE u.name = 'Alice' RETURN u.name AS name",
                backend="sqlite",
                schema_context=self.schema_context,
            )
        ).fetchall()

        self.assertEqual(rows, [("Alice",)])

    def test_compiled_optional_match_missing_row_executes_on_sqlite(self) -> None:
        self._seed_graph()

        rows = self.conn.execute(
            cypherglot.to_sql(
                "OPTIONAL MATCH (u:User) WHERE u.name = 'Cara' RETURN u.name AS name",
                backend="sqlite",
                schema_context=self.schema_context,
            )
        ).fetchall()

        self.assertEqual(rows, [(None,)])

    def test_compiled_match_count_executes_on_sqlite(self) -> None:
        self._seed_graph()

        rows = self.conn.execute(
            cypherglot.to_sql(
                "MATCH (u:User) RETURN count(*) AS total",
                backend="sqlite",
                schema_context=self.schema_context,
            )
        ).fetchall()

        self.assertEqual(rows, [(2,)])

    def test_compiled_grouped_aggregate_executes_on_sqlite(self) -> None:
        self._seed_duplicate_name_graph()

        rows = self.conn.execute(
            cypherglot.to_sql(
                "MATCH (u:User) RETURN u.name AS name, count(*) AS total "
                "ORDER BY total DESC, name ASC",
                backend="sqlite",
                schema_context=self.schema_context,
            )
        ).fetchall()

        self.assertEqual(rows, [("Alice", 2), ("Bob", 1)])

    def test_compiled_graph_introspection_returns_execute_on_sqlite(self) -> None:
        self._seed_graph()

        rows = self.conn.execute(
            cypherglot.to_sql(
                "MATCH (a:User)-[r:KNOWS]->(b:User) "
                "RETURN id(a) AS uid, type(r) AS rel_type, "
                "startNode(r).id AS start_id, "
                "endNode(r).id AS end_id ORDER BY uid",
                backend="sqlite",
                schema_context=self.schema_context,
            )
        ).fetchall()

        self.assertEqual(rows, [(1, "KNOWS", 1, 2)])

    def test_compiled_properties_and_labels_returns_execute_on_sqlite(self) -> None:
        self._seed_graph()

        with self.assertRaisesRegex(
            ValueError,
            (
                "relational output mode does not yet support whole-entity or "
                "introspection returns"
            ),
        ):
            cypherglot.to_sql(
                "MATCH (u:User) RETURN properties(u) AS props, labels(u) AS labels "
                "ORDER BY u.name",
                backend="sqlite",
                schema_context=self.schema_context,
            )

    def test_compiled_keys_return_executes_on_sqlite(self) -> None:
        self._seed_graph()

        with self.assertRaisesRegex(
            ValueError,
            (
                "relational output mode does not yet support whole-entity or "
                "introspection returns"
            ),
        ):
            cypherglot.to_sql(
                "MATCH (a:User)-[r:KNOWS]->(b:User) RETURN keys(r) AS rel_keys",
                backend="sqlite",
                schema_context=self.schema_context,
            )

    def test_compiled_create_program_executes_on_sqlite(self) -> None:
        program = cypherglot.render_cypher_program_text(
            "CREATE (:User {name: 'Alice'})",
            backend="sqlite",
            schema_context=self.schema_context,
        )

        self._execute_program(program)

        rows = self.conn.execute(
            cypherglot.to_sql(
                "MATCH (u:User) RETURN u.name ORDER BY u.name",
                backend="sqlite",
                schema_context=self.schema_context,
            )
        ).fetchall()

        self.assertEqual(rows, [("Alice",)])

    def test_compiled_match_set_node_executes_on_sqlite(self) -> None:
        self._seed_graph()

        self.conn.execute(
            cypherglot.to_sql(
                "MATCH (u:User {name: 'Alice'}) SET u.age = 31",
                backend="sqlite",
                schema_context=self.schema_context,
            )
        )
        self.conn.commit()

        row = self.conn.execute(
            "SELECT name, age FROM cg_node_user WHERE id = 1"
        ).fetchone()

        self.assertEqual(row, ("Alice", 31))

    def test_compiled_match_set_relationship_executes_on_sqlite(self) -> None:
        self._seed_graph()

        self.conn.execute(
            cypherglot.to_sql(
                "MATCH (a:User)-[r:KNOWS]->(b:User) "
                "WHERE a.name = $name SET r.since = 2021, r.strength = 2",
                backend="sqlite",
                schema_context=self.schema_context,
            ),
            {"name": "Alice"},
        )
        self.conn.commit()

        row = self.conn.execute(
            "SELECT note, since, strength FROM cg_edge_knows WHERE id = 10"
        ).fetchone()

        self.assertEqual(row, ("met", 2021, 2))

    def test_compiled_detach_delete_node_cascades_edges_on_sqlite(self) -> None:
        self._seed_graph()

        self.conn.execute(
            cypherglot.to_sql(
                "MATCH (u:User {name: 'Alice'}) DETACH DELETE u",
                backend="sqlite",
                schema_context=self.schema_context,
            )
        )
        self.conn.commit()

        counts = self.conn.execute(
            "SELECT (SELECT COUNT(*) FROM cg_node_user), "
            "(SELECT COUNT(*) FROM cg_edge_knows)"
        ).fetchone()

        self.assertEqual(counts, (1, 0))

    def test_compiled_delete_relationship_executes_on_sqlite(self) -> None:
        self._seed_graph()

        self.conn.execute(
            cypherglot.to_sql(
                "MATCH (a:User)-[r:KNOWS]->(b:User) WHERE a.name = 'Alice' DELETE r",
                backend="sqlite",
                schema_context=self.schema_context,
            )
        )
        self.conn.commit()

        counts = self.conn.execute(
            "SELECT (SELECT COUNT(*) FROM cg_node_user), "
            "(SELECT COUNT(*) FROM cg_edge_knows)"
        ).fetchone()

        self.assertEqual(counts, (2, 0))

    def test_compiled_match_create_from_relationship_source_executes_on_sqlite(
        self,
    ) -> None:
        self._seed_graph()

        self.conn.execute(
            cypherglot.to_sql(
                "MATCH (a:User)-[r:KNOWS]->(b:User) CREATE (a)-[:INTRODUCED]->(b)",
                backend="sqlite",
                schema_context=self.schema_context,
            )
        )
        self.conn.commit()

        rows = self.conn.execute(
            "SELECT from_id, to_id FROM cg_edge_introduced ORDER BY id"
        ).fetchall()

        self.assertEqual(rows, [(1, 2)])

    def test_compiled_match_merge_from_chain_source_executes_on_sqlite(self) -> None:
        self._seed_graph()

        sql = cypherglot.to_sql(
            "MATCH (a:User)-[r:KNOWS]->(b:User) "
            "MERGE (a)-[:INTRODUCED]->(b)",
            backend="sqlite",
            schema_context=self.schema_context,
        )

        self.conn.execute(sql)
        self.conn.execute(sql)
        self.conn.commit()

        counts = self.conn.execute(
            "SELECT COUNT(*) FROM cg_edge_introduced"
        ).fetchone()
        row = self.conn.execute(
            "SELECT from_id, to_id FROM cg_edge_introduced"
        ).fetchone()

        self.assertEqual(counts, (1,))
        self.assertEqual(row, (1, 2))

    def test_rendered_program_traversal_match_create_executes_on_sqlite(self) -> None:
        self._seed_graph()

        program = cypherglot.render_cypher_program_text(
            "MATCH (a:User)-[r:KNOWS]->(b:User) "
            "CREATE (a)-[:INTRODUCED_TO_PERSON]->(:Person {name: 'Cara'})",
            backend="sqlite",
            schema_context=self.schema_context,
        )

        self._execute_program(program)

        counts = self.conn.execute(
            "SELECT "
            "(SELECT COUNT(*) FROM cg_node_user), "
            "(SELECT COUNT(*) FROM cg_node_person), "
            "(SELECT COUNT(*) FROM cg_edge_knows), "
            "(SELECT COUNT(*) FROM cg_edge_introduced_to_person)"
        ).fetchone()
        person_row = self.conn.execute(
            "SELECT id, name FROM cg_node_person"
        ).fetchone()
        introduced_row = self.conn.execute(
            "SELECT from_id, to_id FROM cg_edge_introduced_to_person"
        ).fetchone()

        self.assertEqual(counts, (2, 1, 1, 1))
        self.assertEqual(person_row, (1, "Cara"))
        self.assertEqual(introduced_row, (1, 1))

    def test_rendered_program_traversal_match_merge_executes_on_sqlite(self) -> None:
        self._seed_graph()

        program = cypherglot.render_cypher_program_text(
            "MATCH (a:User)-[r:KNOWS]->(b:User) "
            "MERGE (a)-[:INTRODUCED_TO_PERSON]->(:Person {name: 'Cara'})",
            backend="sqlite",
            schema_context=self.schema_context,
        )

        self._execute_program(program)
        self._execute_program(program)

        person_count = self.conn.execute(
            "SELECT COUNT(*) FROM cg_node_person WHERE name = 'Cara'"
        ).fetchone()
        introduced_count = self.conn.execute(
            "SELECT COUNT(*) FROM cg_edge_introduced_to_person"
        ).fetchone()

        self.assertEqual(person_count, (1,))
        self.assertEqual(introduced_count, (1,))

    def _seed_graph(self) -> None:
        self.conn.execute(
            "INSERT INTO cg_node_user (id, name, age) VALUES (?, ?, ?)",
            (1, "Alice", 30),
        )
        self.conn.execute(
            "INSERT INTO cg_node_user (id, name, age) VALUES (?, ?, ?)",
            (2, "Bob", 25),
        )
        self.conn.execute(
            "INSERT INTO cg_edge_knows (id, from_id, to_id, note) VALUES (?, ?, ?, ?)",
            (10, 1, 2, "met"),
        )
        self.conn.commit()

    def _seed_chain_graph(self) -> None:
        self._seed_graph()
        self.conn.execute(
            "INSERT INTO cg_node_company (id, name) VALUES (?, ?)",
            (3, "Acme"),
        )
        self.conn.execute(
            "INSERT INTO cg_edge_works_at (id, from_id, to_id) VALUES (?, ?, ?)",
            (11, 2, 3),
        )
        self.conn.commit()

    def _seed_duplicate_name_graph(self) -> None:
        self._seed_graph()
        self.conn.execute(
            "INSERT INTO cg_node_user (id, name, age) VALUES (?, ?, ?)",
            (3, "Alice", 22),
        )
        self.conn.commit()

    def _seed_user_chain_graph(self) -> None:
        self._seed_graph()
        self.conn.execute(
            "INSERT INTO cg_node_user (id, name, age) VALUES (?, ?, ?)",
            (3, "Cara", 28),
        )
        self.conn.execute(
            "INSERT INTO cg_edge_knows (id, from_id, to_id, note) VALUES (?, ?, ?, ?)",
            (11, 2, 3, "coworker"),
        )
        self.conn.commit()

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
