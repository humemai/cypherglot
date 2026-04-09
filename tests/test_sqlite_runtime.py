from __future__ import annotations

import json
import sqlite3
import unittest

import cypherglot


class SQLiteRuntimeTests(unittest.TestCase):
    def setUp(self) -> None:
        self.conn = sqlite3.connect(":memory:")
        self.conn.execute("PRAGMA foreign_keys = ON")
        self.conn.executescript(
            """
            CREATE TABLE nodes (
              id INTEGER PRIMARY KEY,
              properties TEXT NOT NULL DEFAULT '{}',
              CHECK (json_valid(properties)),
              CHECK (json_type(properties) = 'object')
            ) STRICT;

            CREATE TABLE edges (
              id INTEGER PRIMARY KEY,
              type TEXT NOT NULL,
              from_id INTEGER NOT NULL,
              to_id INTEGER NOT NULL,
              properties TEXT NOT NULL DEFAULT '{}',
              CHECK (json_valid(properties)),
              CHECK (json_type(properties) = 'object'),
              FOREIGN KEY (from_id) REFERENCES nodes(id) ON DELETE CASCADE,
              FOREIGN KEY (to_id) REFERENCES nodes(id) ON DELETE CASCADE
            ) STRICT;

            CREATE TABLE node_labels (
              node_id INTEGER NOT NULL,
              label TEXT NOT NULL,
              PRIMARY KEY (node_id, label),
              FOREIGN KEY (node_id) REFERENCES nodes(id) ON DELETE CASCADE
            ) STRICT;

            CREATE INDEX idx_node_labels_label_node_id ON node_labels(label, node_id);
            CREATE INDEX idx_node_labels_node_id_label ON node_labels(node_id, label);
            CREATE INDEX idx_edges_from_id ON edges(from_id);
            CREATE INDEX idx_edges_to_id ON edges(to_id);
            CREATE INDEX idx_edges_type ON edges(type);
            CREATE INDEX idx_edges_type_from_id ON edges(type, from_id);
            CREATE INDEX idx_edges_type_to_id ON edges(type, to_id);
            """
        )

    def tearDown(self) -> None:
        self.conn.close()

    def test_compiled_match_return_executes_on_sqlite(self) -> None:
        self._seed_graph()

        sql = cypherglot.to_sql(
            "MATCH (u:User) RETURN u.name ORDER BY u.name"
        )

        rows = self.conn.execute(sql).fetchall()

        self.assertEqual(rows, [("Alice",), ("Bob",)])

    def test_compiled_entity_return_executes_on_sqlite(self) -> None:
        self._seed_graph()

        sql = cypherglot.to_sql(
            "MATCH (u:User) RETURN u ORDER BY u.name"
        )

        rows = self.conn.execute(sql).fetchall()

        self.assertEqual(
            [json.loads(row[0]) for row in rows],
            [
                {"id": 1, "label": "User", "properties": {"name": "Alice", "age": 30}},
                {"id": 2, "label": "User", "properties": {"name": "Bob", "age": 25}},
            ],
        )

    def test_compiled_match_with_return_executes_on_sqlite(self) -> None:
        self._seed_graph()

        sql = cypherglot.to_sql(
            "MATCH (u:User) WITH u.name AS name RETURN name ORDER BY name"
        )

        rows = self.conn.execute(sql).fetchall()

        self.assertEqual(rows, [("Alice",), ("Bob",)])

    def test_compiled_variable_length_match_executes_on_sqlite(self) -> None:
        self._seed_user_chain_graph()

        sql = cypherglot.to_sql(
            "MATCH (a:User)-[:KNOWS*1..2]->(b:User) "
            "WHERE a.name = 'Alice' RETURN b.name AS friend ORDER BY friend"
        )

        rows = self.conn.execute(sql).fetchall()

        self.assertEqual(rows, [("Bob",), ("Cara",)])

    def test_compiled_optional_match_return_executes_on_sqlite(self) -> None:
        self._seed_graph()

        sql = cypherglot.to_sql(
            "OPTIONAL MATCH (u:User) WHERE u.name = 'Alice' RETURN u.name AS name"
        )

        rows = self.conn.execute(sql).fetchall()

        self.assertEqual(rows, [("Alice",)])

    def test_compiled_optional_match_missing_row_executes_on_sqlite(self) -> None:
        self._seed_graph()

        sql = cypherglot.to_sql(
            "OPTIONAL MATCH (u:User) WHERE u.name = 'Cara' RETURN u.name AS name"
        )

        rows = self.conn.execute(sql).fetchall()

        self.assertEqual(rows, [(None,)])

    def test_compiled_match_count_executes_on_sqlite(self) -> None:
        self._seed_graph()

        sql = cypherglot.to_sql(
            "MATCH (u:User) RETURN count(*) AS total"
        )

        rows = self.conn.execute(sql).fetchall()

        self.assertEqual(rows, [(2,)])

    def test_compiled_grouped_aggregate_executes_on_sqlite(self) -> None:
        self._seed_duplicate_name_graph()

        sql = cypherglot.to_sql(
            "MATCH (u:User) RETURN u.name AS name, count(*) AS total "
            "ORDER BY total DESC, name ASC"
        )

        rows = self.conn.execute(sql).fetchall()

        self.assertEqual(rows, [("Alice", 2), ("Bob", 1)])

    def test_compiled_graph_introspection_returns_execute_on_sqlite(self) -> None:
        self._seed_graph()

        sql = cypherglot.to_sql(
            "MATCH (a:User)-[r:KNOWS]->(b:User) "
            "RETURN id(a) AS uid, type(r) AS rel_type, startNode(r).id AS start_id, "
            "endNode(r).id AS end_id ORDER BY uid"
        )

        rows = self.conn.execute(sql).fetchall()

        self.assertEqual(rows, [(1, "KNOWS", 1, 2)])

    def test_compiled_properties_and_labels_returns_execute_on_sqlite(self) -> None:
        self._seed_graph()

        sql = cypherglot.to_sql(
            "MATCH (u:User) RETURN properties(u) AS props, labels(u) AS labels "
            "ORDER BY u.name"
        )

        rows = self.conn.execute(sql).fetchall()

        self.assertEqual(
            [(json.loads(props), json.loads(labels)) for props, labels in rows],
            [
                ({"name": "Alice", "age": 30}, ["User"]),
                ({"name": "Bob", "age": 25}, ["User"]),
            ],
        )

    def test_compiled_keys_return_executes_on_sqlite(self) -> None:
        self._seed_graph()

        sql = cypherglot.to_sql(
            "MATCH (a:User)-[r:KNOWS]->(b:User) RETURN keys(r) AS rel_keys"
        )

        rows = self.conn.execute(sql).fetchall()

        self.assertEqual([json.loads(row[0]) for row in rows], [["note"]])

    def test_compiled_create_program_executes_on_sqlite(self) -> None:
        program = cypherglot.render_cypher_program_text(
            "CREATE (:User {name: 'Alice'})"
        )

        self._execute_program(program)

        rows = self.conn.execute(
            cypherglot.to_sql("MATCH (u:User) RETURN u.name ORDER BY u.name")
        ).fetchall()

        self.assertEqual(rows, [("Alice",)])

    def test_compiled_match_set_node_executes_on_sqlite(self) -> None:
        self._seed_graph()

        sql = cypherglot.to_sql(
            "MATCH (u:User {name: 'Alice'}) SET u.age = 31, u.active = true"
        )

        self.conn.execute(sql)
        self.conn.commit()
        row = self.conn.execute(
            "SELECT properties FROM nodes WHERE id = 1"
        ).fetchone()

        self.assertIsNotNone(row)
        assert row is not None
        self.assertEqual(
            json.loads(row[0]),
            {"name": "Alice", "age": 31, "active": 1},
        )

    def test_compiled_match_set_relationship_executes_on_sqlite(self) -> None:
        self._seed_graph()

        sql = cypherglot.to_sql(
            "MATCH (a:User)-[r:KNOWS]->(b:User) "
            "WHERE a.name = $name SET r.since = 2021, r.strength = 2"
        )

        self.conn.execute(sql, {"name": "Alice"})
        self.conn.commit()

        row = self.conn.execute(
            "SELECT properties FROM edges WHERE id = 10"
        ).fetchone()

        self.assertIsNotNone(row)
        assert row is not None
        self.assertEqual(
            json.loads(row[0]),
            {"note": "met", "since": 2021, "strength": 2},
        )

    def test_compiled_detach_delete_node_cascades_edges_on_sqlite(self) -> None:
        self._seed_graph()

        sql = cypherglot.to_sql(
            "MATCH (u:User {name: 'Alice'}) DETACH DELETE u"
        )

        self.conn.execute(sql)
        self.conn.commit()

        counts = self.conn.execute(
            "SELECT "
            "(SELECT COUNT(*) FROM nodes), "
            "(SELECT COUNT(*) FROM edges), "
            "(SELECT COUNT(*) FROM node_labels)"
        ).fetchone()

        self.assertEqual(counts, (1, 0, 1))

    def test_compiled_delete_relationship_executes_on_sqlite(self) -> None:
        self._seed_graph()

        sql = cypherglot.to_sql(
            "MATCH (a:User)-[r:KNOWS]->(b:User) WHERE a.name = 'Alice' DELETE r"
        )

        self.conn.execute(sql)
        self.conn.commit()

        counts = self.conn.execute(
            "SELECT "
            "(SELECT COUNT(*) FROM nodes), "
            "(SELECT COUNT(*) FROM edges), "
            "(SELECT COUNT(*) FROM node_labels)"
        ).fetchone()

        self.assertEqual(counts, (2, 0, 2))

    def test_compiled_match_create_from_relationship_source_executes_on_sqlite(
        self,
    ) -> None:
        self._seed_graph()

        sql = cypherglot.to_sql(
            "MATCH (a:User)-[r:KNOWS]->(b:User) CREATE (a)-[:INTRODUCED]->(b)"
        )

        self.conn.execute(sql)
        self.conn.commit()

        rows = self.conn.execute(
            "SELECT type, from_id, to_id, properties FROM edges ORDER BY id"
        ).fetchall()

        self.assertEqual(
            rows,
            [
                ("KNOWS", 1, 2, '{"note":"met"}'),
                ("INTRODUCED", 1, 2, "{}"),
            ],
        )

    def test_compiled_match_merge_from_chain_source_executes_on_sqlite(self) -> None:
        self._seed_chain_graph()

        sql = cypherglot.to_sql(
            "MATCH (a:User)-[r:KNOWS]->(b:User)-[s:WORKS_AT]->(c:Company) "
            "MERGE (a)-[:INTRODUCED]->(c)"
        )

        self.conn.execute(sql)
        self.conn.execute(sql)
        self.conn.commit()

        counts = self.conn.execute(
            "SELECT COUNT(*) FROM edges WHERE type = 'INTRODUCED'"
        ).fetchone()
        row = self.conn.execute(
            "SELECT type, from_id, to_id, properties FROM edges "
            "WHERE type = 'INTRODUCED'"
        ).fetchone()

        self.assertEqual(counts, (1,))
        self.assertEqual(row, ("INTRODUCED", 1, 3, "{}"))

    def test_rendered_program_traversal_match_create_executes_on_sqlite(self) -> None:
        self._seed_graph()

        program = cypherglot.render_cypher_program_text(
            "MATCH (a:User)-[r:KNOWS]->(b:User) "
            "CREATE (a)-[:INTRODUCED]->(:Person {name: 'Cara'})"
        )

        self._execute_program(program)

        counts = self.conn.execute(
            "SELECT "
            "(SELECT COUNT(*) FROM nodes), "
            "(SELECT COUNT(*) FROM edges), "
            "(SELECT COUNT(*) FROM node_labels)"
        ).fetchone()
        person_row = self.conn.execute(
            "SELECT n.id, json_extract(n.properties, '$.name') "
            "FROM nodes AS n "
            "JOIN node_labels AS l ON l.node_id = n.id "
            "WHERE l.label = 'Person'"
        ).fetchone()

        self.assertEqual(counts, (3, 2, 3))
        self.assertIsNotNone(person_row)
        assert person_row is not None
        self.assertEqual(person_row[1], "Cara")
        self.assertEqual(
            self.conn.execute(
                "SELECT type, from_id, to_id FROM edges WHERE type = 'INTRODUCED'"
            ).fetchone(),
            ("INTRODUCED", 1, person_row[0]),
        )

    def test_rendered_program_traversal_match_merge_executes_on_sqlite(self) -> None:
        self._seed_graph()

        program = cypherglot.render_cypher_program_text(
            "MATCH (a:User)-[r:KNOWS]->(b:User) "
            "MERGE (a)-[:INTRODUCED]->(:Person {name: 'Cara'})"
        )

        self._execute_program(program)
        self._execute_program(program)

        person_count = self.conn.execute(
            "SELECT COUNT(*) FROM nodes AS n "
            "JOIN node_labels AS l ON l.node_id = n.id "
            "WHERE l.label = 'Person' AND json_extract(n.properties, '$.name') = 'Cara'"
        ).fetchone()
        introduced_count = self.conn.execute(
            "SELECT COUNT(*) FROM edges WHERE type = 'INTRODUCED'"
        ).fetchone()

        self.assertEqual(person_count, (1,))
        self.assertEqual(introduced_count, (1,))

    def _seed_graph(self) -> None:
        self.conn.execute(
            "INSERT INTO nodes (id, properties) VALUES (?, ?)",
            (1, '{"name":"Alice","age":30}'),
        )
        self.conn.execute(
            "INSERT INTO nodes (id, properties) VALUES (?, ?)",
            (2, '{"name":"Bob","age":25}'),
        )
        self.conn.execute(
            "INSERT INTO node_labels (node_id, label) VALUES (?, ?)",
            (1, "User"),
        )
        self.conn.execute(
            "INSERT INTO node_labels (node_id, label) VALUES (?, ?)",
            (2, "User"),
        )
        self.conn.execute(
            (
                "INSERT INTO edges (id, type, from_id, to_id, properties) "
                "VALUES (?, ?, ?, ?, ?)"
            ),
            (10, "KNOWS", 1, 2, '{"note":"met"}'),
        )
        self.conn.commit()

    def _seed_chain_graph(self) -> None:
        self._seed_graph()
        self.conn.execute(
            "INSERT INTO nodes (id, properties) VALUES (?, ?)",
            (3, '{"name":"Acme"}'),
        )
        self.conn.execute(
            "INSERT INTO node_labels (node_id, label) VALUES (?, ?)",
            (3, "Company"),
        )
        self.conn.execute(
            (
                "INSERT INTO edges (id, type, from_id, to_id, properties) "
                "VALUES (?, ?, ?, ?, ?)"
            ),
            (11, "WORKS_AT", 2, 3, "{}"),
        )
        self.conn.commit()

    def _seed_duplicate_name_graph(self) -> None:
        self._seed_graph()
        self.conn.execute(
            "INSERT INTO nodes (id, properties) VALUES (?, ?)",
            (3, '{"name":"Alice","age":22}'),
        )
        self.conn.execute(
            "INSERT INTO node_labels (node_id, label) VALUES (?, ?)",
            (3, "User"),
        )
        self.conn.commit()

    def _seed_user_chain_graph(self) -> None:
        self._seed_graph()
        self.conn.execute(
            "INSERT INTO nodes (id, properties) VALUES (?, ?)",
            (3, '{"name":"Cara","age":28}'),
        )
        self.conn.execute(
            "INSERT INTO node_labels (node_id, label) VALUES (?, ?)",
            (3, "User"),
        )
        self.conn.execute(
            (
                "INSERT INTO edges (id, type, from_id, to_id, properties) "
                "VALUES (?, ?, ?, ?, ?)"
            ),
            (11, "KNOWS", 2, 3, '{"note":"coworker"}'),
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
