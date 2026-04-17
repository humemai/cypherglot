from __future__ import annotations

import unittest

import cypherglot
from cypherglot.render import RenderedCypherLoop, RenderedCypherProgram
from cypherglot.schema import (
    CompilerSchemaContext,
    EdgeTypeSpec,
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
                NodeTypeSpec(
                    name="Company",
                    properties=(PropertyField("name", "string"),),
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
        self.schema_context = CompilerSchemaContext.type_aware(self.graph_schema)
        for statement in self.graph_schema.ddl("duckdb"):
            self.conn.execute(statement)

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

    def test_compiled_create_node_program_executes_on_duckdb(self) -> None:
        program = cypherglot.render_cypher_program_text(
            "CREATE (:User {name: 'Cara', age: 4})",
            dialect="duckdb",
            backend="duckdb",
            schema_context=self.schema_context,
        )

        self._execute_program(program)

        rows = self.conn.execute(
            "SELECT id, name, age FROM cg_node_user ORDER BY id"
        ).fetchall()

        self.assertEqual(rows, [(1, "Cara", 4)])

    def test_compiled_create_relationship_program_executes_on_duckdb(self) -> None:
        program = cypherglot.render_cypher_program_text(
            (
                "CREATE (:User {name: 'Dana', age: 41})"
                "-[:WORKS_AT {since: 2024}]->"
                "(:Company {name: 'Bravo'})"
            ),
            dialect="duckdb",
            backend="duckdb",
            schema_context=self.schema_context,
        )

        self._execute_program(program)

        users = self.conn.execute(
            "SELECT id, name, age FROM cg_node_user ORDER BY id"
        ).fetchall()
        companies = self.conn.execute(
            "SELECT id, name FROM cg_node_company ORDER BY id"
        ).fetchall()
        relationships = self.conn.execute(
            "SELECT from_id, to_id, since FROM cg_edge_works_at ORDER BY id"
        ).fetchall()

        self.assertEqual(users, [(1, "Dana", 41)])
        self.assertEqual(companies, [(1, "Bravo")])
        self.assertEqual(relationships, [(1, 1, 2024)])

    def test_compiled_match_set_program_executes_on_duckdb(self) -> None:
        self._seed_graph()

        program = cypherglot.render_cypher_program_text(
            "MATCH (u:User {name: 'Alice'}) SET u.age = 31",
            dialect="duckdb",
            backend="duckdb",
            schema_context=self.schema_context,
        )

        self._execute_program(program)

        rows = self.conn.execute(
            "SELECT id, name, age FROM cg_node_user ORDER BY id"
        ).fetchall()

        self.assertEqual(rows, [(1, "Alice", 31), (2, "Bob", 25)])

    def test_compiled_merge_node_program_executes_on_duckdb(self) -> None:
        self._seed_graph()

        program = cypherglot.render_cypher_program_text(
            "MERGE (u:User {name: 'Alice'})",
            dialect="duckdb",
            backend="duckdb",
            schema_context=self.schema_context,
        )

        self._execute_program(program)
        self._execute_program(program)

        rows = self.conn.execute(
            "SELECT id, name, age FROM cg_node_user ORDER BY id"
        ).fetchall()

        self.assertEqual(rows, [(1, "Alice", 30), (2, "Bob", 25)])

    def test_compiled_merge_relationship_program_executes_on_duckdb(self) -> None:
        self.conn.execute("INSERT INTO cg_node_user VALUES (1, 'Alice', 30)")
        self.conn.execute("INSERT INTO cg_node_company VALUES (10, 'Acme')")

        program = cypherglot.render_cypher_program_text(
            (
                "MERGE (:User {name: 'Alice'})"
                "-[:WORKS_AT {since: 2020}]->"
                "(:Company {name: 'Acme'})"
            ),
            dialect="duckdb",
            backend="duckdb",
            schema_context=self.schema_context,
        )

        self._execute_program(program)
        self._execute_program(program)

        users = self.conn.execute(
            "SELECT id, name, age FROM cg_node_user ORDER BY id"
        ).fetchall()
        companies = self.conn.execute(
            "SELECT id, name FROM cg_node_company ORDER BY id"
        ).fetchall()
        works_at = self.conn.execute(
            "SELECT from_id, to_id, since FROM cg_edge_works_at ORDER BY id"
        ).fetchall()

        self.assertEqual(users, [(1, "Alice", 30)])
        self.assertEqual(companies, [(10, "Acme")])
        self.assertEqual(works_at, [(1, 10, 2020)])

    def test_duckdb_schema_ddl_supports_generated_ids(self) -> None:
        self.conn.execute("INSERT INTO cg_node_user (name, age) VALUES ('Cara', 4)")

        rows = self.conn.execute(
            "SELECT id, name, age FROM cg_node_user ORDER BY id"
        ).fetchall()

        self.assertEqual(rows, [(1, 'Cara', 4)])

    def _execute_program(self, program: RenderedCypherProgram) -> None:
        bindings: dict[str, object] = {}

        for step in program.steps:
            if isinstance(step, RenderedCypherLoop):
                loop_source = step.source
                for name, value in bindings.items():
                    loop_source = loop_source.replace(f"${name}", str(value))
                    loop_source = loop_source.replace(f":{name}", str(value))

                rows = self.conn.execute(loop_source).fetchall()
                for row in rows:
                    loop_bindings = bindings | dict(
                        zip(step.row_bindings, row, strict=True)
                    )
                    for statement in step.body:
                        sql = statement.sql
                        for name, value in loop_bindings.items():
                            sql = sql.replace(f"${name}", str(value))
                            sql = sql.replace(f":{name}", str(value))

                        cursor = self.conn.execute(sql)
                        if statement.bind_columns:
                            returned = cursor.fetchone()
                            self.assertIsNotNone(returned)
                            assert returned is not None
                            loop_bindings |= dict(
                                zip(statement.bind_columns, returned, strict=True)
                            )
                continue

            sql = step.sql
            for name, value in bindings.items():
                sql = sql.replace(f"${name}", str(value))
                sql = sql.replace(f":{name}", str(value))

            cursor = self.conn.execute(sql)
            if step.bind_columns:
                row = cursor.fetchone()
                self.assertIsNotNone(row)
                assert row is not None
                bindings |= dict(zip(step.bind_columns, row, strict=True))

        self.conn.commit()

    def _seed_graph(self) -> None:
        self.conn.execute("INSERT INTO cg_node_user VALUES (1, 'Alice', 30)")
        self.conn.execute("INSERT INTO cg_node_user VALUES (2, 'Bob', 25)")
