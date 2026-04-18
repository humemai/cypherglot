from __future__ import annotations

import re
import unittest

import cypherglot
from tests._postgres_runtime_support import (
    acquire_postgresql_test_dsn,
    release_postgresql_test_dsn,
)
from cypherglot.render import RenderedCypherLoop, RenderedCypherProgram
from cypherglot.schema import (
    CompilerSchemaContext,
    EdgeTypeSpec,
    GraphSchema,
    NodeTypeSpec,
    PropertyField,
)

try:
    import psycopg2
    import psycopg2.extras
except ImportError:  # pragma: no cover - optional test dependency
    psycopg2 = None  # type: ignore[assignment]


class PostgreSQLRuntimeTests(unittest.TestCase):
    _postgres_dsn: str

    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        cls._postgres_dsn = acquire_postgresql_test_dsn()

    @classmethod
    def tearDownClass(cls) -> None:
        release_postgresql_test_dsn()
        super().tearDownClass()

    def setUp(self) -> None:
        assert psycopg2 is not None
        self.conn = psycopg2.connect(self._postgres_dsn)
        self.conn.autocommit = False
        self.graph_schema = GraphSchema(
            node_types=(
                NodeTypeSpec(
                    name="User",
                    properties=(
                        PropertyField("name", "string"),
                        PropertyField("age", "integer"),
                        PropertyField("score", "float"),
                    ),
                ),
                NodeTypeSpec(
                    name="Company",
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
            ),
        )
        self.schema_context = CompilerSchemaContext.type_aware(self.graph_schema)
        self._reset_schema()
        with self.conn.cursor() as cur:
            for statement in self.graph_schema.ddl("postgresql"):
                cur.execute(statement)
        self.conn.commit()

    def tearDown(self) -> None:
        conn = getattr(self, "conn", None)
        if conn is None or conn.closed:
            return
        try:
            self._reset_schema()
        finally:
            conn.close()

    def test_compiled_match_return_executes_on_postgresql(self) -> None:
        self._seed_graph()

        sql = cypherglot.to_sql(
            "MATCH (u:User) RETURN u.name ORDER BY u.name",
            schema_context=self.schema_context,
            dialect="postgres",
            backend="postgresql",
        )

        with self.conn.cursor() as cur:
            cur.execute(sql)
            rows = cur.fetchall()

        self.assertEqual(rows, [("Alice",), ("Bob",)])

    def test_compiled_derived_with_return_executes_on_postgresql(self) -> None:
        self._seed_graph()

        sql = cypherglot.to_sql(
            (
                "MATCH (u:User) WITH lower(u.name) AS lowered "
                "RETURN lowered ORDER BY lowered"
            ),
            schema_context=self.schema_context,
            dialect="postgres",
            backend="postgresql",
        )

        with self.conn.cursor() as cur:
            cur.execute(sql)
            rows = cur.fetchall()

        self.assertEqual(rows, [("alice",), ("bob",)])

    def test_compiled_endpoint_derived_with_return_executes_on_postgresql(
        self,
    ) -> None:
        self._seed_employment_graph()

        sql = cypherglot.to_sql(
            (
                "MATCH (a:User)-[r:WORKS_AT]->(b:Company) "
                "WITH startNode(r).name AS employee, endNode(r).name AS employer "
                "RETURN employee, employer ORDER BY employer, employee"
            ),
            schema_context=self.schema_context,
            dialect="postgres",
            backend="postgresql",
        )

        with self.conn.cursor() as cur:
            cur.execute(sql)
            rows = cur.fetchall()

        self.assertEqual(rows, [("Alice", "Acme")])

    def test_compiled_variable_length_derived_with_return_executes_on_postgresql(
        self,
    ) -> None:
        self._seed_knows_graph()

        sql = cypherglot.to_sql(
            (
                "MATCH (a:User)-[:KNOWS*1..2]->(b:User) "
                "WITH lower(b.name) AS lowered "
                "RETURN lowered ORDER BY lowered"
            ),
            schema_context=self.schema_context,
            dialect="postgres",
            backend="postgresql",
        )

        with self.conn.cursor() as cur:
            cur.execute(sql)
            rows = cur.fetchall()

        self.assertEqual(rows, [("bob",), ("cara",), ("cara",)])

    def test_compiled_grouped_derived_with_aggregate_executes_on_postgresql(
        self,
    ) -> None:
        self._seed_grouped_graph()

        sql = cypherglot.to_sql(
            (
                "MATCH (u:User) WITH lower(u.name) AS lowered, "
                "u.score AS score RETURN lowered, avg(score) AS mean "
                "ORDER BY mean DESC, lowered"
            ),
            schema_context=self.schema_context,
            dialect="postgres",
            backend="postgresql",
        )

        with self.conn.cursor() as cur:
            cur.execute(sql)
            rows = cur.fetchall()

        self.assertEqual(rows, [("bob", 2.8), ("alice", 2.1)])

    def test_compiled_numeric_predicates_execute_on_postgresql(self) -> None:
        self._seed_grouped_graph()

        age_sql = cypherglot.to_sql(
            (
                "MATCH (u:User) WHERE u.age >= 18 "
                "RETURN u.name AS name ORDER BY name"
            ),
            schema_context=self.schema_context,
            dialect="postgres",
            backend="postgresql",
        )
        with_sql = cypherglot.to_sql(
            (
                "MATCH (u:User) WITH toInteger(u.score) AS score_int "
                "RETURN score_int >= 2 AS ge_two ORDER BY ge_two"
            ),
            schema_context=self.schema_context,
            dialect="postgres",
            backend="postgresql",
        )

        with self.conn.cursor() as cur:
            cur.execute(age_sql)
            age_rows = cur.fetchall()
            cur.execute(with_sql)
            predicate_rows = cur.fetchall()

        self.assertEqual(age_rows, [("Alice",), ("Alice",), ("Bob",)])
        self.assertEqual(predicate_rows, [(False,), (True,), (True,)])

    def test_compiled_merge_node_program_executes_on_postgresql(self) -> None:
        self._seed_graph()

        program = cypherglot.render_cypher_program_text(
            "MERGE (u:User {name: 'Alice'})",
            dialect="postgres",
            backend="postgresql",
            schema_context=self.schema_context,
        )

        self._execute_program(program)
        self._execute_program(program)

        with self.conn.cursor() as cur:
            cur.execute("SELECT id, name, age FROM cg_node_user ORDER BY id")
            users = cur.fetchall()

        self.assertEqual(users, [(1, "Alice", 30), (2, "Bob", 25)])

    def test_compiled_create_node_program_executes_on_postgresql(self) -> None:
        program = cypherglot.render_cypher_program_text(
            "CREATE (:User {name: 'Cara', age: 4})",
            dialect="postgres",
            backend="postgresql",
            schema_context=self.schema_context,
        )

        self._execute_program(program)

        with self.conn.cursor() as cur:
            cur.execute("SELECT id, name, age FROM cg_node_user ORDER BY id")
            rows = cur.fetchall()

        self.assertEqual(rows, [(1, "Cara", 4)])

    def test_compiled_create_relationship_program_executes_on_postgresql(self) -> None:
        program = cypherglot.render_cypher_program_text(
            (
                "CREATE (:User {name: 'Dana', age: 41})"
                "-[:WORKS_AT {since: 2024}]->"
                "(:Company {name: 'Bravo'})"
            ),
            dialect="postgres",
            backend="postgresql",
            schema_context=self.schema_context,
        )

        self._execute_program(program)

        with self.conn.cursor() as cur:
            cur.execute("SELECT id, name, age FROM cg_node_user ORDER BY id")
            users = cur.fetchall()
            cur.execute("SELECT id, name FROM cg_node_company ORDER BY id")
            companies = cur.fetchall()
            cur.execute(
                "SELECT from_id, to_id, since FROM cg_edge_works_at ORDER BY id"
            )
            relationships = cur.fetchall()

        self.assertEqual(users, [(1, "Dana", 41)])
        self.assertEqual(companies, [(1, "Bravo")])
        self.assertEqual(relationships, [(1, 1, 2024)])

    def test_compiled_merge_relationship_program_executes_on_postgresql(self) -> None:
        self._seed_employment_graph()

        program = cypherglot.render_cypher_program_text(
            (
                "MERGE (:User {name: 'Alice'})"
                "-[:WORKS_AT {since: 2020}]->"
                "(:Company {name: 'Acme'})"
            ),
            dialect="postgres",
            backend="postgresql",
            schema_context=self.schema_context,
        )

        self._execute_program(program)
        self._execute_program(program)

        with self.conn.cursor() as cur:
            cur.execute("SELECT id, name, age FROM cg_node_user ORDER BY id")
            users = cur.fetchall()
            cur.execute("SELECT id, name FROM cg_node_company ORDER BY id")
            companies = cur.fetchall()
            cur.execute(
                "SELECT from_id, to_id, since FROM cg_edge_works_at ORDER BY id"
            )
            works_at = cur.fetchall()

        self.assertEqual(users, [(1, "Alice", 30), (2, "Bob", 25)])
        self.assertEqual(companies, [(10, "Acme")])
        self.assertEqual(works_at, [(1, 10, 2020)])

    def test_compiled_match_create_program_executes_on_postgresql(self) -> None:
        self._seed_employment_graph()

        program = cypherglot.render_cypher_program_text(
            (
                "MATCH (u:User {name: 'Alice'}) "
                "CREATE (u)-[:WORKS_AT {since: 2024}]->(:Company {name: 'Cypher'})"
            ),
            dialect="postgres",
            backend="postgresql",
            schema_context=self.schema_context,
        )

        self._execute_program(program)

        with self.conn.cursor() as cur:
            cur.execute("SELECT id, name FROM cg_node_company ORDER BY id")
            companies = cur.fetchall()
            cur.execute(
                "SELECT from_id, to_id, since FROM cg_edge_works_at ORDER BY id"
            )
            works_at = cur.fetchall()

        self.assertEqual(companies, [(10, "Acme"), (11, "Cypher")])
        self.assertEqual(works_at, [(1, 10, 2020), (1, 11, 2024)])

    def test_compiled_match_set_program_executes_on_postgresql(self) -> None:
        self._seed_graph()

        program = cypherglot.render_cypher_program_text(
            "MATCH (u:User {name: 'Alice'}) SET u.age = 31",
            dialect="postgres",
            backend="postgresql",
            schema_context=self.schema_context,
        )

        self._execute_program(program)

        with self.conn.cursor() as cur:
            cur.execute("SELECT id, name, age FROM cg_node_user ORDER BY id")
            rows = cur.fetchall()

        self.assertEqual(rows, [(1, "Alice", 31), (2, "Bob", 25)])

    def test_compiled_match_relationship_set_program_executes_on_postgresql(
        self,
    ) -> None:
        self._seed_employment_graph()

        program = cypherglot.render_cypher_program_text(
            "MATCH (u:User)-[r:WORKS_AT {since: 2020}]->(c:Company) SET r.since = 2021",
            dialect="postgres",
            backend="postgresql",
            schema_context=self.schema_context,
        )

        self._execute_program(program)

        with self.conn.cursor() as cur:
            cur.execute(
                "SELECT id, from_id, to_id, since FROM cg_edge_works_at ORDER BY id"
            )
            rows = cur.fetchall()

        self.assertEqual(rows, [(1, 1, 10, 2021)])

    def test_compiled_match_relationship_delete_program_executes_on_postgresql(
        self,
    ) -> None:
        self._seed_employment_graph()

        program = cypherglot.render_cypher_program_text(
            "MATCH (u:User)-[r:WORKS_AT {since: 2020}]->(c:Company) DELETE r",
            dialect="postgres",
            backend="postgresql",
            schema_context=self.schema_context,
        )

        self._execute_program(program)

        with self.conn.cursor() as cur:
            cur.execute(
                "SELECT id, from_id, to_id, since FROM cg_edge_works_at ORDER BY id"
            )
            rows = cur.fetchall()

        self.assertEqual(rows, [])

    def test_postgresql_schema_ddl_supports_sequences(self) -> None:
        with self.conn.cursor() as cur:
            cur.execute(
                "INSERT INTO cg_node_user (name, age) VALUES ('Cara', 4) RETURNING id"
            )
            row = cur.fetchone()

        self.assertIsNotNone(row)
        assert row is not None
        self.assertEqual(row[0], 1)

    def _execute_program(self, program: RenderedCypherProgram) -> None:
        bindings: dict[str, object] = {}
        with self.conn.cursor() as cur:
            for step in program.steps:
                if isinstance(step, RenderedCypherLoop):
                    self._execute_bound_sql(cur, step.source, bindings)
                    rows = cur.fetchall()
                    for row in rows:
                        loop_bindings = bindings | dict(
                            zip(step.row_bindings, row, strict=True)
                        )
                        for statement in step.body:
                            self._execute_bound_sql(cur, statement.sql, loop_bindings)
                            if statement.bind_columns:
                                returned = cur.fetchone()
                                self.assertIsNotNone(returned)
                                assert returned is not None
                                loop_bindings |= dict(
                                    zip(statement.bind_columns, returned, strict=True)
                                )
                    continue

                self._execute_bound_sql(cur, step.sql, bindings)
                if step.bind_columns:
                    returned = cur.fetchone()
                    self.assertIsNotNone(returned)
                    assert returned is not None
                    bindings |= dict(zip(step.bind_columns, returned, strict=True))

        self.conn.commit()

    def _seed_graph(self) -> None:
        with self.conn.cursor() as cur:
            cur.execute(
                (
                    "INSERT INTO cg_node_user (id, name, age, score) "
                    "VALUES (%s, %s, %s, %s)"
                ),
                (1, "Alice", 30, 1.2),
            )
            cur.execute(
                (
                    "INSERT INTO cg_node_user (id, name, age, score) "
                    "VALUES (%s, %s, %s, %s)"
                ),
                (2, "Bob", 25, 2.8),
            )
            cur.execute("SELECT setval('cg_node_user_id_seq', %s)", (2,))
        self.conn.commit()

    def _seed_knows_graph(self) -> None:
        with self.conn.cursor() as cur:
            cur.executemany(
                (
                    "INSERT INTO cg_node_user (id, name, age, score) "
                    "VALUES (%s, %s, %s, %s)"
                ),
                [
                    (1, "Alice", 30, 1.2),
                    (2, "Bob", 25, 2.8),
                    (3, "Cara", 4, 4.4),
                ],
            )
            cur.execute("SELECT setval('cg_node_user_id_seq', %s)", (3,))
            cur.executemany(
                "INSERT INTO cg_edge_knows (id, from_id, to_id) VALUES (%s, %s, %s)",
                [(1, 1, 2), (2, 2, 3)],
            )
            cur.execute("SELECT setval('cg_edge_knows_id_seq', %s)", (2,))

        self.conn.commit()

    def _seed_grouped_graph(self) -> None:
        with self.conn.cursor() as cur:
            cur.executemany(
                (
                    "INSERT INTO cg_node_user (id, name, age, score) "
                    "VALUES (%s, %s, %s, %s)"
                ),
                [
                    (1, "Alice", 30, 1.2),
                    (2, "Bob", 25, 2.8),
                    (3, "Alice", 22, 3.0),
                ],
            )
            cur.execute("SELECT setval('cg_node_user_id_seq', %s)", (3,))

        self.conn.commit()

    def _seed_employment_graph(self) -> None:
        self._seed_graph()

        with self.conn.cursor() as cur:
            cur.execute(
                "INSERT INTO cg_node_company (id, name) VALUES (%s, %s)",
                (10, "Acme"),
            )
            cur.execute("SELECT setval('cg_node_company_id_seq', %s)", (10,))
            cur.execute(
                "INSERT INTO cg_edge_works_at (id, from_id, to_id, since) "
                "VALUES (%s, %s, %s, %s)",
                (1, 1, 10, 2020),
            )
            cur.execute("SELECT setval('cg_edge_works_at_id_seq', %s)", (1,))

        self.conn.commit()

    def _reset_schema(self) -> None:
        try:
            self.conn.rollback()
        except psycopg2.Error:
            pass

        with self.conn.cursor() as cur:
            cur.execute("DROP TABLE IF EXISTS cg_edge_works_at CASCADE")
            cur.execute("DROP TABLE IF EXISTS cg_edge_knows CASCADE")
            cur.execute("DROP TABLE IF EXISTS cg_node_user CASCADE")
            cur.execute("DROP TABLE IF EXISTS cg_node_company CASCADE")
            cur.execute("DROP SEQUENCE IF EXISTS cg_node_user_id_seq CASCADE")
            cur.execute("DROP SEQUENCE IF EXISTS cg_node_company_id_seq CASCADE")
            cur.execute("DROP SEQUENCE IF EXISTS cg_edge_knows_id_seq CASCADE")
            cur.execute("DROP SEQUENCE IF EXISTS cg_edge_works_at_id_seq CASCADE")

        self.conn.commit()

    def _execute_bound_sql(
        self,
        cur: psycopg2.extensions.cursor,
        sql: str,
        bindings: dict[str, object],
    ) -> None:
        bound_sql = sql
        for name in bindings:
            pyformat_token = f"%({name})s"
            bound_sql = bound_sql.replace(f"${name}", pyformat_token)
            bound_sql = bound_sql.replace(f":{name}", pyformat_token)

        if re.search(r"%\([A-Za-z_][A-Za-z0-9_]*\)s", bound_sql):
            cur.execute(bound_sql, bindings)
            return

        cur.execute(bound_sql)
