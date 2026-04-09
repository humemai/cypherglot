from __future__ import annotations

import json
import unittest

import cypherglot

try:
    import duckdb
except ImportError:  # pragma: no cover - optional test dependency
    duckdb = None


@unittest.skipIf(duckdb is None, "duckdb is not installed")
class DuckDBRuntimeTests(unittest.TestCase):
    def setUp(self) -> None:
        assert duckdb is not None
        self.conn = duckdb.connect()
        self.conn.execute("CREATE TABLE nodes (id BIGINT, properties VARCHAR)")
        self.conn.execute(
            "CREATE TABLE edges (id BIGINT, type VARCHAR, from_id BIGINT, to_id BIGINT, properties VARCHAR)"
        )
        self.conn.execute("CREATE TABLE node_labels (node_id BIGINT, label VARCHAR)")

    def tearDown(self) -> None:
        self.conn.close()

    def test_compiled_match_return_executes_on_duckdb(self) -> None:
        self._seed_graph()

        sql = cypherglot.to_sql(
            "MATCH (u:User) RETURN u.name ORDER BY u.name",
            dialect="duckdb",
        )

        rows = self.conn.execute(sql).fetchall()

        self.assertEqual(rows, [("Alice",), ("Bob",)])

    def test_compiled_match_where_string_executes_on_duckdb(self) -> None:
        self._seed_graph()

        sql = cypherglot.to_sql(
            "MATCH (u:User) WHERE u.name = 'Alice' RETURN u.name",
            dialect="duckdb",
        )

        rows = self.conn.execute(sql).fetchall()

        self.assertEqual(rows, [("Alice",)])

    def test_compiled_sum_executes_on_duckdb(self) -> None:
        self._seed_graph()

        sql = cypherglot.to_sql(
            "MATCH (u:User) RETURN sum(u.age) AS total",
            dialect="duckdb",
        )

        rows = self.conn.execute(sql).fetchall()

        self.assertEqual(rows, [(55.0,)])

    def test_compiled_entity_return_executes_on_duckdb(self) -> None:
        self._seed_graph()

        sql = cypherglot.to_sql(
            "MATCH (u:User) RETURN u AS user ORDER BY u.name",
            dialect="duckdb",
        )

        rows = self.conn.execute(sql).fetchall()

        self.assertEqual(
            [json.loads(row[0]) for row in rows],
            [
                {"id": 1, "label": "User", "properties": {"name": "Alice", "age": 30}},
                {"id": 2, "label": "User", "properties": {"name": "Bob", "age": 25}},
            ],
        )

    def test_compiled_numeric_ordering_executes_on_duckdb(self) -> None:
        self.conn.execute(
            "INSERT INTO nodes VALUES (3, '{\"name\":\"Cara\",\"age\":4}')"
        )
        self.conn.execute("INSERT INTO node_labels VALUES (3, 'User')")
        self._seed_graph()

        sql = cypherglot.to_sql(
            "MATCH (u:User) RETURN u.age AS age ORDER BY age ASC",
            dialect="duckdb",
        )

        rows = self.conn.execute(sql).fetchall()

        self.assertEqual(rows, [("4",), ("25",), ("30",)])

    def test_compiled_min_max_execute_on_duckdb(self) -> None:
        self.conn.execute(
            "INSERT INTO nodes VALUES (3, '{\"name\":\"Cara\",\"age\":4}')"
        )
        self.conn.execute("INSERT INTO node_labels VALUES (3, 'User')")
        self._seed_graph()

        min_sql = cypherglot.to_sql(
            "MATCH (u:User) RETURN min(u.age) AS min_age",
            dialect="duckdb",
        )
        max_sql = cypherglot.to_sql(
            "MATCH (u:User) RETURN max(u.age) AS max_age",
            dialect="duckdb",
        )

        self.assertEqual(self.conn.execute(min_sql).fetchall(), [("4",)])
        self.assertEqual(self.conn.execute(max_sql).fetchall(), [("30",)])

    def _seed_graph(self) -> None:
        self.conn.execute(
            "INSERT INTO nodes VALUES (1, '{\"name\":\"Alice\",\"age\":30}')"
        )
        self.conn.execute(
            "INSERT INTO nodes VALUES (2, '{\"name\":\"Bob\",\"age\":25}')"
        )
        self.conn.execute("INSERT INTO node_labels VALUES (1, 'User')")
        self.conn.execute("INSERT INTO node_labels VALUES (2, 'User')")
