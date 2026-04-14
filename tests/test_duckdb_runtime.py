from __future__ import annotations

import unittest

import cypherglot
from cypherglot.schema import (
    CompilerSchemaContext,
    GraphSchema,
    NodeTypeSpec,
    PropertyField,
)

try:
    import duckdb
except ImportError:  # pragma: no cover - optional test dependency
    duckdb = None


@unittest.skipIf(duckdb is None, "duckdb is not installed")
class DuckDBRuntimeTests(unittest.TestCase):
    def setUp(self) -> None:
        assert duckdb is not None
        self.conn = duckdb.connect()
        self.graph_schema = GraphSchema(
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
        self.schema_context = CompilerSchemaContext.type_aware(self.graph_schema)
        self.conn.execute(
            "CREATE TABLE cg_node_user (id BIGINT, name VARCHAR, age BIGINT)"
        )

    def tearDown(self) -> None:
        self.conn.close()

    def test_compiled_match_return_executes_on_duckdb(self) -> None:
        self._seed_graph()

        sql = cypherglot.to_sql(
            "MATCH (u:User) RETURN u.name ORDER BY u.name",
            dialect="duckdb",
            schema_context=self.schema_context,
        )

        rows = self.conn.execute(sql).fetchall()

        self.assertEqual(rows, [("Alice",), ("Bob",)])

    def test_compiled_match_where_string_executes_on_duckdb(self) -> None:
        self._seed_graph()

        sql = cypherglot.to_sql(
            "MATCH (u:User) WHERE u.name = 'Alice' RETURN u.name",
            dialect="duckdb",
            schema_context=self.schema_context,
        )

        rows = self.conn.execute(sql).fetchall()

        self.assertEqual(rows, [("Alice",)])

    def test_compiled_sum_executes_on_duckdb(self) -> None:
        self._seed_graph()

        sql = cypherglot.to_sql(
            "MATCH (u:User) RETURN sum(u.age) AS total",
            dialect="duckdb",
            schema_context=self.schema_context,
        )

        rows = self.conn.execute(sql).fetchall()

        self.assertEqual(int(rows[0][0]), 55)

    def test_compiled_entity_return_executes_on_duckdb(self) -> None:
        self._seed_graph()

        sql = cypherglot.to_sql(
            "MATCH (u:User) RETURN u AS user ORDER BY u.name",
            dialect="duckdb",
            schema_context=self.schema_context,
        )

        rows = self.conn.execute(sql).fetchall()

        self.assertEqual(rows, [(1, "User", "Alice", 30), (2, "User", "Bob", 25)])

    def test_compiled_numeric_ordering_executes_on_duckdb(self) -> None:
        self.conn.execute("INSERT INTO cg_node_user VALUES (3, 'Cara', 4)")
        self._seed_graph()

        sql = cypherglot.to_sql(
            "MATCH (u:User) RETURN u.age AS age ORDER BY age ASC",
            dialect="duckdb",
            schema_context=self.schema_context,
        )

        rows = self.conn.execute(sql).fetchall()

        self.assertEqual(rows, [(4,), (25,), (30,)])

    def test_compiled_min_max_execute_on_duckdb(self) -> None:
        self.conn.execute("INSERT INTO cg_node_user VALUES (3, 'Cara', 4)")
        self._seed_graph()

        min_sql = cypherglot.to_sql(
            "MATCH (u:User) RETURN min(u.age) AS min_age",
            dialect="duckdb",
            schema_context=self.schema_context,
        )
        max_sql = cypherglot.to_sql(
            "MATCH (u:User) RETURN max(u.age) AS max_age",
            dialect="duckdb",
            schema_context=self.schema_context,
        )

        self.assertEqual(self.conn.execute(min_sql).fetchall(), [(4,)])
        self.assertEqual(self.conn.execute(max_sql).fetchall(), [(30,)])

    def _seed_graph(self) -> None:
        self.conn.execute("INSERT INTO cg_node_user VALUES (1, 'Alice', 30)")
        self.conn.execute("INSERT INTO cg_node_user VALUES (2, 'Bob', 25)")
