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


class TypeAwareSQLiteRuntimeTests(unittest.TestCase):
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
        self.conn.executescript("\n".join(self.graph_schema.sqlite_ddl()))

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

    def test_type_aware_match_return_executes_on_sqlite(self) -> None:
        self._seed_type_aware_graph()

        sql = cypherglot.to_sql(
            "MATCH (u:User) RETURN u.name ORDER BY u.name",
            schema_context=self._type_aware_schema_context(),
        )

        rows = self.conn.execute(sql).fetchall()

        self.assertEqual(rows, [("Alice",), ("Bob",)])

    def test_type_aware_merge_node_program_executes_on_sqlite(self) -> None:
        self._seed_type_aware_graph()

        program = cypherglot.render_cypher_program_text(
            "MERGE (u:User {name: 'Alice'})",
            schema_context=self._type_aware_schema_context(),
        )

        self._execute_program(program)
        self._execute_program(program)

        users = self.conn.execute(
            "SELECT id, name, age FROM cg_node_user ORDER BY id"
        ).fetchall()

        self.assertEqual(users, [(1, "Alice", 30), (2, "Bob", 25)])

    def test_type_aware_merge_relationship_program_executes_on_sqlite(self) -> None:
        self._seed_type_aware_graph()

        program = cypherglot.render_cypher_program_text(
            "MERGE (:User {name: 'Alice'})-[:WORKS_AT {since: 2020}]->(:Company {name: 'Acme'})",
            schema_context=self._type_aware_schema_context(),
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

        self.assertEqual(users, [(1, "Alice", 30), (2, "Bob", 25)])
        self.assertEqual(companies, [(10, "Acme"), (11, "Bravo")])
        self.assertEqual(works_at, [(1, 10, 2020), (2, 11, 2021)])

    def test_type_aware_merge_relationship_self_loop_program_executes_on_sqlite(
        self,
    ) -> None:
        self._seed_type_aware_graph()

        program = cypherglot.render_cypher_program_text(
            "MERGE (u:User {name: 'Alice'})-[:KNOWS]->(u:User {name: 'Alice'})",
            schema_context=CompilerSchemaContext.type_aware(self.graph_schema),
        )

        self._execute_program(program)
        self._execute_program(program)

        users = self.conn.execute(
            "SELECT id, name, age FROM cg_node_user ORDER BY id"
        ).fetchall()
        knows = self.conn.execute(
            "SELECT from_id, to_id FROM cg_edge_knows ORDER BY from_id, to_id"
        ).fetchall()

        self.assertEqual(users, [(1, "Alice", 30), (2, "Bob", 25)])
        self.assertEqual(knows, [(1, 1), (1, 2)])

    def test_type_aware_one_hop_match_executes_on_sqlite(self) -> None:
        self._seed_type_aware_graph()

        sql = cypherglot.to_sql(
            (
                "MATCH (a:User)-[r:WORKS_AT]->(b:Company) "
                "RETURN a.name AS user_name, r.since AS since, b.name AS company "
                "ORDER BY company"
            ),
            schema_context=self._type_aware_schema_context(),
        )

        rows = self.conn.execute(sql).fetchall()

        self.assertEqual(rows, [("Alice", 2020, "Acme"), ("Bob", 2021, "Bravo")])

    def test_type_aware_one_hop_self_loop_match_executes_on_sqlite(self) -> None:
        self._seed_type_aware_graph()
        self.conn.execute(
            "INSERT INTO cg_edge_knows (id, from_id, to_id) VALUES (?, ?, ?)",
            (91, 1, 1),
        )
        self.conn.commit()

        sql = cypherglot.to_sql(
            (
                "MATCH (u:User)-[r:KNOWS]->(u:User) "
                "RETURN u.name AS user_name ORDER BY user_name"
            ),
            schema_context=CompilerSchemaContext.type_aware(self.graph_schema),
        )

        rows = self.conn.execute(sql).fetchall()

        self.assertEqual(rows, [("Alice",)])

    def test_type_aware_match_create_program_executes_on_sqlite(self) -> None:
        self._seed_type_aware_graph()

        program = cypherglot.render_cypher_program_text(
            (
                "MATCH (u:User {name: 'Alice'}) "
                "CREATE (u)-[:WORKS_AT {since: 2024}]->(:Company {name: 'Cypher'})"
            ),
            schema_context=self._type_aware_schema_context(),
        )

        self._execute_program(program)

        companies = self.conn.execute(
            "SELECT id, name FROM cg_node_company ORDER BY id"
        ).fetchall()
        works_at = self.conn.execute(
            "SELECT from_id, to_id, since FROM cg_edge_works_at ORDER BY id"
        ).fetchall()

        self.assertEqual(companies, [(10, "Acme"), (11, "Bravo"), (12, "Cypher")])
        self.assertEqual(
            works_at,
            [(1, 10, 2020), (2, 11, 2021), (1, 12, 2024)],
        )

    def test_type_aware_match_create_program_with_new_left_endpoint_executes_on_sqlite(
        self,
    ) -> None:
        self._seed_type_aware_graph()

        program = cypherglot.render_cypher_program_text(
            "MATCH (b:Company {name: 'Acme'}) CREATE (:User {name: 'Cara'})-[:WORKS_AT {since: 2024}]->(b)",
            schema_context=self._type_aware_schema_context(),
        )

        self._execute_program(program)

        users = self.conn.execute(
            "SELECT id, name, age FROM cg_node_user ORDER BY id"
        ).fetchall()
        works_at = self.conn.execute(
            "SELECT from_id, to_id, since FROM cg_edge_works_at ORDER BY from_id, to_id, since"
        ).fetchall()

        self.assertEqual(users, [(1, "Alice", 30), (2, "Bob", 25), (3, "Cara", None)])
        self.assertEqual(works_at, [(1, 10, 2020), (2, 11, 2021), (3, 10, 2024)])

    def test_type_aware_match_create_sql_executes_on_sqlite(self) -> None:
        self._seed_type_aware_graph()

        sql = cypherglot.to_sql(
            (
                "MATCH (a:User {name: 'Alice'}), (b:Company {name: 'Acme'}) "
                "CREATE (a)-[:WORKS_AT {since: 2024}]->(b)"
            ),
            schema_context=CompilerSchemaContext.type_aware(self.graph_schema),
        )

        self.conn.execute(sql)
        self.conn.commit()

        works_at = self.conn.execute(
            "SELECT from_id, to_id, since FROM cg_edge_works_at ORDER BY id"
        ).fetchall()

        self.assertEqual(
            works_at,
            [(1, 10, 2020), (2, 11, 2021), (1, 10, 2024)],
        )

        filtered_sql = cypherglot.to_sql(
            (
                "MATCH (a:User), (b:Company) WHERE a.name = 'Alice' "
                "AND b.name = 'Acme' CREATE (a)-[:WORKS_AT {since: 2025}]->(b)"
            ),
            schema_context=CompilerSchemaContext.type_aware(self.graph_schema),
        )

        self.conn.execute(filtered_sql)
        self.conn.commit()

        works_at = self.conn.execute(
            "SELECT from_id, to_id, since FROM cg_edge_works_at ORDER BY id"
        ).fetchall()

        self.assertEqual(
            works_at,
            [(1, 10, 2020), (2, 11, 2021), (1, 10, 2024), (1, 10, 2025)],
        )

        right_filtered_sql = cypherglot.to_sql(
            (
                "MATCH (a:User), (b:Company) WHERE b.name = 'Acme' "
                "CREATE (a)-[:WORKS_AT {since: 2026}]->(b)"
            ),
            schema_context=CompilerSchemaContext.type_aware(self.graph_schema),
        )

        self.conn.execute(right_filtered_sql)
        self.conn.commit()

        works_at = self.conn.execute(
            "SELECT from_id, to_id, since FROM cg_edge_works_at ORDER BY id"
        ).fetchall()

        self.assertEqual(
            works_at,
            [
                (1, 10, 2020),
                (2, 11, 2021),
                (1, 10, 2024),
                (1, 10, 2025),
                (1, 10, 2026),
                (2, 10, 2026),
            ],
        )

    def test_type_aware_match_create_with_right_endpoint_filter_executes_on_sqlite(
        self,
    ) -> None:
        self._seed_type_aware_graph()

        sql = cypherglot.to_sql(
            "MATCH (a:User)-[r:WORKS_AT]->(b:Company {name: 'Acme'}) CREATE (a)-[:WORKS_AT {since: 2024}]->(b)",
            schema_context=CompilerSchemaContext.type_aware(self.graph_schema),
        )

        self.conn.execute(sql)
        self.conn.commit()

        works_at = self.conn.execute(
            "SELECT from_id, to_id, since FROM cg_edge_works_at ORDER BY id"
        ).fetchall()

        self.assertEqual(works_at, [(1, 10, 2020), (2, 11, 2021), (1, 10, 2024)])

    def test_type_aware_match_create_with_relationship_filter_executes_on_sqlite(
        self,
    ) -> None:
        self._seed_type_aware_graph()

        sql = cypherglot.to_sql(
            "MATCH (a:User)-[r:WORKS_AT]->(b:Company) WHERE r.since = 2020 CREATE (a)-[:WORKS_AT {since: 2024}]->(b)",
            schema_context=self._type_aware_schema_context(),
        )

        self.conn.execute(sql)
        self.conn.commit()

        works_at = self.conn.execute(
            "SELECT from_id, to_id, since FROM cg_edge_works_at ORDER BY id"
        ).fetchall()

        self.assertEqual(works_at, [(1, 10, 2020), (2, 11, 2021), (1, 10, 2024)])

    def test_type_aware_match_create_with_left_and_relationship_filters_executes_on_sqlite(
        self,
    ) -> None:
        self._seed_type_aware_graph()

        sql = cypherglot.to_sql(
            "MATCH (a:User)-[r:WORKS_AT]->(b:Company) WHERE a.name = 'Alice' AND r.since = 2020 CREATE (a)-[:WORKS_AT {since: 2024}]->(b)",
            schema_context=CompilerSchemaContext.type_aware(self.graph_schema),
        )

        self.conn.execute(sql)
        self.conn.commit()

        works_at = self.conn.execute(
            "SELECT from_id, to_id, since FROM cg_edge_works_at ORDER BY id"
        ).fetchall()

        self.assertEqual(works_at, [(1, 10, 2020), (2, 11, 2021), (1, 10, 2024)])

    def test_type_aware_match_create_with_both_endpoint_filters_executes_on_sqlite(
        self,
    ) -> None:
        self._seed_type_aware_graph()

        sql = cypherglot.to_sql(
            "MATCH (a:User {name: 'Alice'})-[r:WORKS_AT]->(b:Company {name: 'Acme'}) CREATE (a)-[:WORKS_AT {since: 2024}]->(b)",
            schema_context=CompilerSchemaContext.type_aware(self.graph_schema),
        )

        self.conn.execute(sql)
        self.conn.commit()

        works_at = self.conn.execute(
            "SELECT from_id, to_id, since FROM cg_edge_works_at ORDER BY id"
        ).fetchall()

        self.assertEqual(works_at, [(1, 10, 2020), (2, 11, 2021), (1, 10, 2024)])

    def test_type_aware_match_create_with_all_filters_executes_on_sqlite(self) -> None:
        self._seed_type_aware_graph()

        sql = cypherglot.to_sql(
            "MATCH (a:User {name: 'Alice'})-[r:WORKS_AT]->(b:Company {name: 'Acme'}) WHERE r.since = 2020 CREATE (a)-[:WORKS_AT {since: 2024}]->(b)",
            schema_context=CompilerSchemaContext.type_aware(self.graph_schema),
        )

        self.conn.execute(sql)
        self.conn.commit()

        works_at = self.conn.execute(
            "SELECT from_id, to_id, since FROM cg_edge_works_at ORDER BY id"
        ).fetchall()

        self.assertEqual(works_at, [(1, 10, 2020), (2, 11, 2021), (1, 10, 2024)])

    def test_type_aware_match_create_self_loop_sql_executes_on_sqlite(self) -> None:
        self._seed_type_aware_graph()

        sql = cypherglot.to_sql(
            "MATCH (a:User {name: 'Alice'}) CREATE (a)-[:KNOWS]->(a)",
            schema_context=CompilerSchemaContext.type_aware(self.graph_schema),
        )

        self.conn.execute(sql)
        self.conn.commit()

        knows = self.conn.execute(
            "SELECT from_id, to_id FROM cg_edge_knows ORDER BY from_id, to_id"
        ).fetchall()

        self.assertEqual(knows, [(1, 1), (1, 2)])

    def test_type_aware_match_merge_sql_executes_on_sqlite(self) -> None:
        self._seed_type_aware_graph()

        sql = cypherglot.to_sql(
            (
                "MATCH (a:User {name: 'Alice'}), (b:Company {name: 'Acme'}) "
                "MERGE (a)-[:WORKS_AT {since: 2020}]->(b)"
            ),
            schema_context=CompilerSchemaContext.type_aware(self.graph_schema),
        )

        self.conn.execute(sql)
        self.conn.execute(sql)
        self.conn.commit()

        works_at = self.conn.execute(
            "SELECT from_id, to_id, since FROM cg_edge_works_at ORDER BY id"
        ).fetchall()

        self.assertEqual(
            works_at,
            [(1, 10, 2020), (2, 11, 2021)],
        )

        right_filtered_sql = cypherglot.to_sql(
            (
                "MATCH (a:User), (b:Company) WHERE b.name = 'Acme' "
                "MERGE (a)-[:WORKS_AT {since: 2020}]->(b)"
            ),
            schema_context=CompilerSchemaContext.type_aware(self.graph_schema),
        )

        self.conn.execute(right_filtered_sql)
        self.conn.execute(right_filtered_sql)
        self.conn.commit()

        works_at = self.conn.execute(
            "SELECT from_id, to_id, since FROM cg_edge_works_at ORDER BY id"
        ).fetchall()

        self.assertEqual(
            works_at,
            [(1, 10, 2020), (2, 11, 2021), (2, 10, 2020)],
        )

        filtered_sql = cypherglot.to_sql(
            (
                "MATCH (a:User), (b:Company) WHERE a.name = 'Alice' "
                "AND b.name = 'Acme' MERGE (a)-[:WORKS_AT {since: 2020}]->(b)"
            ),
            schema_context=CompilerSchemaContext.type_aware(self.graph_schema),
        )

        self.conn.execute(filtered_sql)
        self.conn.execute(filtered_sql)
        self.conn.commit()

        works_at = self.conn.execute(
            "SELECT from_id, to_id, since FROM cg_edge_works_at ORDER BY id"
        ).fetchall()

        self.assertEqual(
            works_at,
            [(1, 10, 2020), (2, 11, 2021), (2, 10, 2020)],
        )

    def test_type_aware_match_merge_with_right_endpoint_filter_executes_on_sqlite(
        self,
    ) -> None:
        self._seed_type_aware_graph()

        sql = cypherglot.to_sql(
            "MATCH (a:User)-[r:WORKS_AT]->(b:Company {name: 'Acme'}) MERGE (a)-[:WORKS_AT {since: 2024}]->(b)",
            schema_context=CompilerSchemaContext.type_aware(self.graph_schema),
        )

        self.conn.execute(sql)
        self.conn.execute(sql)
        self.conn.commit()

        works_at = self.conn.execute(
            "SELECT from_id, to_id, since FROM cg_edge_works_at ORDER BY id"
        ).fetchall()

        self.assertEqual(works_at, [(1, 10, 2020), (2, 11, 2021), (1, 10, 2024)])

    def test_type_aware_match_merge_with_relationship_filter_executes_on_sqlite(
        self,
    ) -> None:
        self._seed_type_aware_graph()

        sql = cypherglot.to_sql(
            "MATCH (a:User)-[r:WORKS_AT]->(b:Company) WHERE r.since = 2020 MERGE (a)-[:WORKS_AT {since: 2024}]->(b)",
            schema_context=CompilerSchemaContext.type_aware(self.graph_schema),
        )

        self.conn.execute(sql)
        self.conn.execute(sql)
        self.conn.commit()

        works_at = self.conn.execute(
            "SELECT from_id, to_id, since FROM cg_edge_works_at ORDER BY id"
        ).fetchall()

        self.assertEqual(works_at, [(1, 10, 2020), (2, 11, 2021), (1, 10, 2024)])

    def test_type_aware_match_merge_with_all_filters_executes_on_sqlite(self) -> None:
        self._seed_type_aware_graph()

        sql = cypherglot.to_sql(
            "MATCH (a:User {name: 'Alice'})-[r:WORKS_AT]->(b:Company {name: 'Acme'}) WHERE r.since = 2020 MERGE (a)-[:WORKS_AT {since: 2024}]->(b)",
            schema_context=CompilerSchemaContext.type_aware(self.graph_schema),
        )

        self.conn.execute(sql)
        self.conn.execute(sql)
        self.conn.commit()

        works_at = self.conn.execute(
            "SELECT from_id, to_id, since FROM cg_edge_works_at ORDER BY id"
        ).fetchall()

        self.assertEqual(works_at, [(1, 10, 2020), (2, 11, 2021), (1, 10, 2024)])

    def test_type_aware_match_merge_with_left_and_relationship_filters_executes_on_sqlite(
        self,
    ) -> None:
        self._seed_type_aware_graph()

        sql = cypherglot.to_sql(
            "MATCH (a:User)-[r:WORKS_AT]->(b:Company) WHERE a.name = 'Alice' AND r.since = 2020 MERGE (a)-[:WORKS_AT {since: 2024}]->(b)",
            schema_context=CompilerSchemaContext.type_aware(self.graph_schema),
        )

        self.conn.execute(sql)
        self.conn.execute(sql)
        self.conn.commit()

        works_at = self.conn.execute(
            "SELECT from_id, to_id, since FROM cg_edge_works_at ORDER BY id"
        ).fetchall()

        self.assertEqual(works_at, [(1, 10, 2020), (2, 11, 2021), (1, 10, 2024)])

    def test_type_aware_match_merge_with_both_endpoint_filters_executes_on_sqlite(
        self,
    ) -> None:
        self._seed_type_aware_graph()

        sql = cypherglot.to_sql(
            "MATCH (a:User {name: 'Alice'})-[r:WORKS_AT]->(b:Company {name: 'Acme'}) MERGE (a)-[:WORKS_AT {since: 2024}]->(b)",
            schema_context=CompilerSchemaContext.type_aware(self.graph_schema),
        )

        self.conn.execute(sql)
        self.conn.execute(sql)
        self.conn.commit()

        works_at = self.conn.execute(
            "SELECT from_id, to_id, since FROM cg_edge_works_at ORDER BY id"
        ).fetchall()

        self.assertEqual(works_at, [(1, 10, 2020), (2, 11, 2021), (1, 10, 2024)])

    def test_type_aware_match_merge_self_loop_sql_executes_on_sqlite(self) -> None:
        self._seed_type_aware_graph()

        sql = cypherglot.to_sql(
            "MATCH (a:User {name: 'Alice'}) MERGE (a)-[:KNOWS]->(a)",
            schema_context=CompilerSchemaContext.type_aware(self.graph_schema),
        )

        self.conn.execute(sql)
        self.conn.execute(sql)
        self.conn.commit()

        knows = self.conn.execute(
            "SELECT from_id, to_id FROM cg_edge_knows ORDER BY from_id, to_id"
        ).fetchall()

        self.assertEqual(knows, [(1, 1), (1, 2)])

    def test_type_aware_match_merge_program_with_new_right_endpoint_executes_on_sqlite(
        self,
    ) -> None:
        self._seed_type_aware_graph()

        program = cypherglot.render_cypher_program_text(
            "MATCH (a:User {name: 'Alice'}) MERGE (a)-[:INTRODUCED]->(:Person {name: 'Cara'})",
            schema_context=CompilerSchemaContext.type_aware(self.graph_schema),
        )

        self._execute_program(program)
        self._execute_program(program)

        people = self.conn.execute(
            "SELECT id, name FROM cg_node_person ORDER BY id"
        ).fetchall()
        introduced = self.conn.execute(
            "SELECT from_id, to_id FROM cg_edge_introduced ORDER BY from_id, to_id"
        ).fetchall()

        self.assertEqual(people, [(1, "Cara")])
        self.assertEqual(introduced, [(1, 1)])

        filtered_left_program = cypherglot.render_cypher_program_text(
            (
                "MATCH (a:User {name: 'Alice'})-[r:KNOWS]->(b:User) "
                "CREATE (a)-[:INTRODUCED]->(:Person {name: 'Dana'})"
            ),
            schema_context=CompilerSchemaContext.type_aware(self.graph_schema),
        )
        filtered_right_program = cypherglot.render_cypher_program_text(
            (
                "MATCH (a:User)-[r:KNOWS]->(b:User {name: 'Bob'}) "
                "CREATE (a)-[:INTRODUCED]->(:Person {name: 'Erin'})"
            ),
            schema_context=CompilerSchemaContext.type_aware(self.graph_schema),
        )

        self._execute_program(filtered_left_program)
        self._execute_program(filtered_right_program)

        people = self.conn.execute(
            "SELECT id, name FROM cg_node_person ORDER BY id"
        ).fetchall()
        introduced = self.conn.execute(
            "SELECT from_id, to_id FROM cg_edge_introduced ORDER BY id"
        ).fetchall()

        self.assertEqual(people, [(1, "Cara"), (2, "Dana"), (3, "Erin")])
        self.assertEqual(introduced, [(1, 1), (1, 2), (1, 3)])

    def test_type_aware_match_merge_program_with_new_left_endpoint_executes_on_sqlite(
        self,
    ) -> None:
        self._seed_type_aware_graph()

        program = cypherglot.render_cypher_program_text(
            "MATCH (b:Company {name: 'Acme'}) MERGE (:User {name: 'Cara'})-[:WORKS_AT {since: 2024}]->(b)",
            schema_context=CompilerSchemaContext.type_aware(self.graph_schema),
        )

        self._execute_program(program)
        self._execute_program(program)

        users = self.conn.execute(
            "SELECT id, name, age FROM cg_node_user ORDER BY id"
        ).fetchall()
        works_at = self.conn.execute(
            "SELECT from_id, to_id, since FROM cg_edge_works_at ORDER BY from_id, to_id, since"
        ).fetchall()

        self.assertEqual(users, [(1, "Alice", 30), (2, "Bob", 25), (3, "Cara", None)])
        self.assertEqual(works_at, [(1, 10, 2020), (2, 11, 2021), (3, 10, 2024)])

    def test_type_aware_match_set_node_sql_executes_on_sqlite(self) -> None:
        self._seed_type_aware_graph()

        sql = cypherglot.to_sql(
            "MATCH (u:User {name: 'Alice'}) SET u.age = 31",
            schema_context=CompilerSchemaContext.type_aware(self.graph_schema),
        )

        self.conn.execute(sql)
        self.conn.commit()

        users = self.conn.execute(
            "SELECT id, name, age FROM cg_node_user ORDER BY id"
        ).fetchall()

        self.assertEqual(users, [(1, "Alice", 31), (2, "Bob", 25)])

    def test_type_aware_match_set_relationship_sql_executes_on_sqlite(self) -> None:
        self._seed_type_aware_graph()

        sql = cypherglot.to_sql(
            (
                "MATCH (a:User)-[r:WORKS_AT]->(b:Company) "
                "WHERE a.name = 'Alice' SET r.since = 2024"
            ),
            schema_context=CompilerSchemaContext.type_aware(self.graph_schema),
        )

        self.conn.execute(sql)
        self.conn.commit()

        works_at = self.conn.execute(
            "SELECT from_id, to_id, since FROM cg_edge_works_at ORDER BY id"
        ).fetchall()

        self.assertEqual(
            works_at,
            [(1, 10, 2024), (2, 11, 2021)],
        )

    def test_type_aware_match_set_relationship_with_right_endpoint_filter_executes_on_sqlite(
        self,
    ) -> None:
        self._seed_type_aware_graph()

        sql = cypherglot.to_sql(
            "MATCH (a:User)-[r:WORKS_AT]->(b:Company {name: 'Acme'}) SET r.since = 2025",
            schema_context=CompilerSchemaContext.type_aware(self.graph_schema),
        )

        self.conn.execute(sql)
        self.conn.commit()

        works_at = self.conn.execute(
            "SELECT from_id, to_id, since FROM cg_edge_works_at ORDER BY id"
        ).fetchall()

        self.assertEqual(works_at, [(1, 10, 2025), (2, 11, 2021)])

    def test_type_aware_match_set_relationship_with_relationship_filter_executes_on_sqlite(
        self,
    ) -> None:
        self._seed_type_aware_graph()

        sql = cypherglot.to_sql(
            "MATCH (a:User)-[r:WORKS_AT]->(b:Company) WHERE r.since = 2020 SET r.since = 2025",
            schema_context=CompilerSchemaContext.type_aware(self.graph_schema),
        )

        self.conn.execute(sql)
        self.conn.commit()

        works_at = self.conn.execute(
            "SELECT from_id, to_id, since FROM cg_edge_works_at ORDER BY id"
        ).fetchall()

        self.assertEqual(works_at, [(1, 10, 2025), (2, 11, 2021)])

    def test_type_aware_match_set_relationship_with_combined_filters_executes_on_sqlite(
        self,
    ) -> None:
        self._seed_type_aware_graph()

        sql = cypherglot.to_sql(
            "MATCH (a:User)-[r:WORKS_AT]->(b:Company {name: 'Acme'}) WHERE r.since = 2020 SET r.since = 2025",
            schema_context=CompilerSchemaContext.type_aware(self.graph_schema),
        )

        self.conn.execute(sql)
        self.conn.commit()

        works_at = self.conn.execute(
            "SELECT from_id, to_id, since FROM cg_edge_works_at ORDER BY id"
        ).fetchall()

        self.assertEqual(works_at, [(1, 10, 2025), (2, 11, 2021)])

    def test_type_aware_match_set_relationship_with_left_and_relationship_filters_executes_on_sqlite(
        self,
    ) -> None:
        self._seed_type_aware_graph()

        sql = cypherglot.to_sql(
            "MATCH (a:User)-[r:WORKS_AT]->(b:Company) WHERE a.name = 'Alice' AND r.since = 2020 SET r.since = 2025",
            schema_context=CompilerSchemaContext.type_aware(self.graph_schema),
        )

        self.conn.execute(sql)
        self.conn.commit()

        works_at = self.conn.execute(
            "SELECT from_id, to_id, since FROM cg_edge_works_at ORDER BY id"
        ).fetchall()

        self.assertEqual(works_at, [(1, 10, 2025), (2, 11, 2021)])

    def test_type_aware_match_set_relationship_with_both_endpoint_filters_executes_on_sqlite(
        self,
    ) -> None:
        self._seed_type_aware_graph()

        sql = cypherglot.to_sql(
            "MATCH (a:User {name: 'Alice'})-[r:WORKS_AT]->(b:Company {name: 'Acme'}) SET r.since = 2025",
            schema_context=CompilerSchemaContext.type_aware(self.graph_schema),
        )

        self.conn.execute(sql)
        self.conn.commit()

        works_at = self.conn.execute(
            "SELECT from_id, to_id, since FROM cg_edge_works_at ORDER BY id"
        ).fetchall()

        self.assertEqual(works_at, [(1, 10, 2025), (2, 11, 2021)])

    def test_type_aware_match_set_relationship_with_all_filters_executes_on_sqlite(
        self,
    ) -> None:
        self._seed_type_aware_graph()

        sql = cypherglot.to_sql(
            "MATCH (a:User {name: 'Alice'})-[r:WORKS_AT]->(b:Company {name: 'Acme'}) WHERE r.since = 2020 SET r.since = 2025",
            schema_context=CompilerSchemaContext.type_aware(self.graph_schema),
        )

        self.conn.execute(sql)
        self.conn.commit()

        works_at = self.conn.execute(
            "SELECT from_id, to_id, since FROM cg_edge_works_at ORDER BY id"
        ).fetchall()

        self.assertEqual(works_at, [(1, 10, 2025), (2, 11, 2021)])

    def test_type_aware_match_set_relationship_self_loop_sql_executes_on_sqlite(
        self,
    ) -> None:
        local_conn = sqlite3.connect(":memory:")
        local_schema = GraphSchema(
            node_types=(
                NodeTypeSpec(
                    name="User",
                    properties=(PropertyField("name", "string"),),
                ),
            ),
            edge_types=(
                EdgeTypeSpec(
                    name="KNOWS",
                    source_type="User",
                    target_type="User",
                    properties=(PropertyField("since", "integer"),),
                ),
            ),
        )
        local_conn.executescript("\n".join(local_schema.sqlite_ddl()))
        local_conn.executemany(
            "INSERT INTO cg_node_user (id, name) VALUES (?, ?)",
            ((1, "Alice"), (2, "Bob")),
        )
        local_conn.executemany(
            "INSERT INTO cg_edge_knows (id, from_id, to_id, since) VALUES (?, ?, ?, ?)",
            ((91, 1, 2, None), (92, 1, 1, 2020)),
        )
        local_conn.commit()

        sql = cypherglot.to_sql(
            "MATCH (a:User)-[r:KNOWS]->(a:User) WHERE a.name = 'Alice' SET r.since = 2021",
            schema_context=CompilerSchemaContext.type_aware(local_schema),
        )

        local_conn.execute(sql)
        local_conn.commit()

        knows = local_conn.execute(
            "SELECT from_id, to_id, since FROM cg_edge_knows ORDER BY from_id, to_id"
        ).fetchall()
        local_conn.close()

        self.assertEqual(knows, [(1, 1, 2021), (1, 2, None)])

    def test_type_aware_match_delete_node_sql_executes_on_sqlite(self) -> None:
        self._seed_type_aware_graph()

        sql = cypherglot.to_sql(
            "MATCH (u:User {name: 'Alice'}) DETACH DELETE u",
            schema_context=self._type_aware_schema_context(),
        )

        self.conn.execute(sql)
        self.conn.commit()

        users = self.conn.execute(
            "SELECT id, name, age FROM cg_node_user ORDER BY id"
        ).fetchall()
        works_at = self.conn.execute(
            "SELECT from_id, to_id, since FROM cg_edge_works_at ORDER BY id"
        ).fetchall()
        knows = self.conn.execute(
            "SELECT from_id, to_id FROM cg_edge_knows ORDER BY id"
        ).fetchall()

        self.assertEqual(users, [(2, "Bob", 25)])
        self.assertEqual(works_at, [(2, 11, 2021)])
        self.assertEqual(knows, [])

    def test_type_aware_match_delete_relationship_sql_executes_on_sqlite(self) -> None:
        self._seed_type_aware_graph()

        sql = cypherglot.to_sql(
            "MATCH (a:User)-[r:WORKS_AT]->(b:Company) WHERE a.name = 'Alice' DELETE r",
            schema_context=CompilerSchemaContext.type_aware(self.graph_schema),
        )

        self.conn.execute(sql)
        self.conn.commit()

        works_at = self.conn.execute(
            "SELECT from_id, to_id, since FROM cg_edge_works_at ORDER BY id"
        ).fetchall()

        self.assertEqual(works_at, [(2, 11, 2021)])

    def test_type_aware_match_delete_relationship_with_right_endpoint_filter_executes_on_sqlite(
        self,
    ) -> None:
        self._seed_type_aware_graph()

        sql = cypherglot.to_sql(
            "MATCH (a:User)-[r:WORKS_AT]->(b:Company {name: 'Acme'}) DELETE r",
            schema_context=CompilerSchemaContext.type_aware(self.graph_schema),
        )

        self.conn.execute(sql)
        self.conn.commit()

        works_at = self.conn.execute(
            "SELECT from_id, to_id, since FROM cg_edge_works_at ORDER BY id"
        ).fetchall()

        self.assertEqual(works_at, [(2, 11, 2021)])

    def test_type_aware_match_delete_relationship_with_relationship_filter_executes_on_sqlite(
        self,
    ) -> None:
        self._seed_type_aware_graph()

        sql = cypherglot.to_sql(
            "MATCH (a:User)-[r:WORKS_AT]->(b:Company) WHERE r.since = 2020 DELETE r",
            schema_context=CompilerSchemaContext.type_aware(self.graph_schema),
        )

        self.conn.execute(sql)
        self.conn.commit()

        works_at = self.conn.execute(
            "SELECT from_id, to_id, since FROM cg_edge_works_at ORDER BY id"
        ).fetchall()

        self.assertEqual(works_at, [(2, 11, 2021)])

    def test_type_aware_match_delete_relationship_with_combined_filters_executes_on_sqlite(
        self,
    ) -> None:
        self._seed_type_aware_graph()

        sql = cypherglot.to_sql(
            "MATCH (a:User)-[r:WORKS_AT]->(b:Company {name: 'Acme'}) WHERE r.since = 2020 DELETE r",
            schema_context=CompilerSchemaContext.type_aware(self.graph_schema),
        )

        self.conn.execute(sql)
        self.conn.commit()

        works_at = self.conn.execute(
            "SELECT from_id, to_id, since FROM cg_edge_works_at ORDER BY id"
        ).fetchall()

        self.assertEqual(works_at, [(2, 11, 2021)])

    def test_type_aware_match_delete_relationship_with_left_and_relationship_filters_executes_on_sqlite(
        self,
    ) -> None:
        self._seed_type_aware_graph()

        sql = cypherglot.to_sql(
            "MATCH (a:User)-[r:WORKS_AT]->(b:Company) WHERE a.name = 'Alice' AND r.since = 2020 DELETE r",
            schema_context=CompilerSchemaContext.type_aware(self.graph_schema),
        )

        self.conn.execute(sql)
        self.conn.commit()

        works_at = self.conn.execute(
            "SELECT from_id, to_id, since FROM cg_edge_works_at ORDER BY id"
        ).fetchall()

        self.assertEqual(works_at, [(2, 11, 2021)])

    def test_type_aware_match_delete_relationship_with_both_endpoint_filters_executes_on_sqlite(
        self,
    ) -> None:
        self._seed_type_aware_graph()

        sql = cypherglot.to_sql(
            "MATCH (a:User {name: 'Alice'})-[r:WORKS_AT]->(b:Company {name: 'Acme'}) DELETE r",
            schema_context=CompilerSchemaContext.type_aware(self.graph_schema),
        )

        self.conn.execute(sql)
        self.conn.commit()

        works_at = self.conn.execute(
            "SELECT from_id, to_id, since FROM cg_edge_works_at ORDER BY id"
        ).fetchall()

        self.assertEqual(works_at, [(2, 11, 2021)])

    def test_type_aware_match_delete_relationship_with_all_filters_executes_on_sqlite(
        self,
    ) -> None:
        self._seed_type_aware_graph()

        sql = cypherglot.to_sql(
            "MATCH (a:User {name: 'Alice'})-[r:WORKS_AT]->(b:Company {name: 'Acme'}) WHERE r.since = 2020 DELETE r",
            schema_context=CompilerSchemaContext.type_aware(self.graph_schema),
        )

        self.conn.execute(sql)
        self.conn.commit()

        works_at = self.conn.execute(
            "SELECT from_id, to_id, since FROM cg_edge_works_at ORDER BY id"
        ).fetchall()

        self.assertEqual(works_at, [(2, 11, 2021)])

    def test_type_aware_match_delete_relationship_self_loop_sql_executes_on_sqlite(
        self,
    ) -> None:
        self._seed_type_aware_graph()
        self.conn.execute(
            "INSERT INTO cg_edge_knows (id, from_id, to_id) VALUES (?, ?, ?)",
            (92, 1, 1),
        )
        self.conn.commit()

        sql = cypherglot.to_sql(
            "MATCH (a:User)-[r:KNOWS]->(a:User) WHERE a.name = 'Alice' DELETE r",
            schema_context=CompilerSchemaContext.type_aware(self.graph_schema),
        )

        self.conn.execute(sql)
        self.conn.commit()

        knows = self.conn.execute(
            "SELECT from_id, to_id FROM cg_edge_knows ORDER BY from_id, to_id"
        ).fetchall()

        self.assertEqual(knows, [(1, 2)])

    def test_type_aware_traversal_match_create_program_executes_on_sqlite(self) -> None:
        self._seed_type_aware_graph()

        program = cypherglot.render_cypher_program_text(
            (
                "MATCH (a:User)-[r:KNOWS]->(b:User) "
                "CREATE (a)-[:INTRODUCED]->(:Person {name: 'Cara'})"
            ),
            schema_context=CompilerSchemaContext.type_aware(self.graph_schema),
        )

        self._execute_program(program)

        people = self.conn.execute(
            "SELECT id, name FROM cg_node_person ORDER BY id"
        ).fetchall()
        introduced = self.conn.execute(
            "SELECT from_id, to_id FROM cg_edge_introduced ORDER BY id"
        ).fetchall()

        self.assertEqual(people, [(1, "Cara")])
        self.assertEqual(introduced, [(1, 1)])

        filtered_left_program = cypherglot.render_cypher_program_text(
            (
                "MATCH (a:User {name: 'Alice'})-[r:KNOWS]->(b:User) "
                "MERGE (a)-[:INTRODUCED]->(:Person {name: 'Dana'})"
            ),
            schema_context=CompilerSchemaContext.type_aware(self.graph_schema),
        )
        filtered_right_program = cypherglot.render_cypher_program_text(
            (
                "MATCH (a:User)-[r:KNOWS]->(b:User {name: 'Bob'}) "
                "MERGE (a)-[:INTRODUCED]->(:Person {name: 'Erin'})"
            ),
            schema_context=CompilerSchemaContext.type_aware(self.graph_schema),
        )

        self._execute_program(filtered_left_program)
        self._execute_program(filtered_left_program)
        self._execute_program(filtered_right_program)
        self._execute_program(filtered_right_program)

        people = self.conn.execute(
            "SELECT id, name FROM cg_node_person ORDER BY id"
        ).fetchall()
        introduced = self.conn.execute(
            "SELECT from_id, to_id FROM cg_edge_introduced ORDER BY id"
        ).fetchall()

        self.assertEqual(people, [(1, "Cara"), (2, "Dana"), (3, "Erin")])
        self.assertEqual(introduced, [(1, 1), (1, 2), (1, 3)])

    def test_type_aware_traversal_self_loop_match_create_program_executes_on_sqlite(
        self,
    ) -> None:
        self._seed_type_aware_graph()
        self.conn.execute(
            "INSERT INTO cg_edge_knows (id, from_id, to_id) VALUES (?, ?, ?)",
            (91, 1, 1),
        )
        self.conn.commit()

        program = cypherglot.render_cypher_program_text(
            (
                "MATCH (a:User)-[r:KNOWS]->(a:User) "
                "CREATE (a)-[:INTRODUCED]->(:Person {name: 'Cara'})"
            ),
            schema_context=CompilerSchemaContext.type_aware(self.graph_schema),
        )

        self._execute_program(program)

        people = self.conn.execute(
            "SELECT id, name FROM cg_node_person ORDER BY id"
        ).fetchall()
        introduced = self.conn.execute(
            "SELECT from_id, to_id FROM cg_edge_introduced ORDER BY from_id, to_id"
        ).fetchall()

        self.assertEqual(people, [(1, "Cara")])
        self.assertEqual(introduced, [(1, 1)])

    def test_type_aware_traversal_match_create_program_with_new_left_endpoint_executes_on_sqlite(
        self,
    ) -> None:
        self._seed_type_aware_graph()

        program = cypherglot.render_cypher_program_text(
            "MATCH (a:User)-[r:WORKS_AT]->(b:Company {name: 'Acme'}) CREATE (:User {name: 'Cara'})-[:WORKS_AT {since: 2024}]->(b)",
            schema_context=CompilerSchemaContext.type_aware(self.graph_schema),
        )

        self._execute_program(program)

        users = self.conn.execute(
            "SELECT id, name, age FROM cg_node_user ORDER BY id"
        ).fetchall()
        works_at = self.conn.execute(
            "SELECT from_id, to_id, since FROM cg_edge_works_at ORDER BY from_id, to_id, since"
        ).fetchall()

        self.assertEqual(users, [(1, "Alice", 30), (2, "Bob", 25), (3, "Cara", None)])
        self.assertEqual(works_at, [(1, 10, 2020), (2, 11, 2021), (3, 10, 2024)])

        filtered_left_program = cypherglot.render_cypher_program_text(
            "MATCH (a:User {name: 'Alice'})-[r:WORKS_AT]->(b:Company) CREATE (:User {name: 'Dana'})-[:WORKS_AT {since: 2025}]->(b)",
            schema_context=CompilerSchemaContext.type_aware(self.graph_schema),
        )
        filtered_right_program = cypherglot.render_cypher_program_text(
            "MATCH (a:User)-[r:WORKS_AT]->(b:Company {name: 'Bravo'}) CREATE (:User {name: 'Erin'})-[:WORKS_AT {since: 2026}]->(b)",
            schema_context=CompilerSchemaContext.type_aware(self.graph_schema),
        )

        self._execute_program(filtered_left_program)
        self._execute_program(filtered_right_program)

        users = self.conn.execute(
            "SELECT id, name, age FROM cg_node_user ORDER BY id"
        ).fetchall()
        works_at = self.conn.execute(
            "SELECT from_id, to_id, since FROM cg_edge_works_at ORDER BY from_id, to_id, since"
        ).fetchall()

        self.assertEqual(
            users,
            [(1, "Alice", 30), (2, "Bob", 25), (3, "Cara", None), (4, "Dana", None), (5, "Erin", None)],
        )
        self.assertEqual(
            works_at,
            [(1, 10, 2020), (2, 11, 2021), (3, 10, 2024), (4, 10, 2025), (5, 11, 2026)],
        )

        relationship_filtered_program = cypherglot.render_cypher_program_text(
            "MATCH (a:User)-[r:WORKS_AT]->(b:Company) WHERE r.since = 2020 CREATE (:User {name: 'Fiona'})-[:WORKS_AT {since: 2027}]->(b)",
            schema_context=CompilerSchemaContext.type_aware(self.graph_schema),
        )
        combined_filtered_program = cypherglot.render_cypher_program_text(
            "MATCH (a:User {name: 'Alice'})-[r:WORKS_AT]->(b:Company) WHERE r.since = 2020 CREATE (:User {name: 'Gina'})-[:WORKS_AT {since: 2028}]->(b)",
            schema_context=CompilerSchemaContext.type_aware(self.graph_schema),
        )

        self._execute_program(relationship_filtered_program)
        self._execute_program(combined_filtered_program)

        users = self.conn.execute(
            "SELECT id, name, age FROM cg_node_user ORDER BY id"
        ).fetchall()
        works_at = self.conn.execute(
            "SELECT from_id, to_id, since FROM cg_edge_works_at ORDER BY from_id, to_id, since"
        ).fetchall()

        self.assertEqual(
            users,
            [(1, "Alice", 30), (2, "Bob", 25), (3, "Cara", None), (4, "Dana", None), (5, "Erin", None), (6, "Fiona", None), (7, "Gina", None)],
        )
        self.assertEqual(
            works_at,
            [(1, 10, 2020), (2, 11, 2021), (3, 10, 2024), (4, 10, 2025), (5, 11, 2026), (6, 10, 2027), (7, 10, 2028)],
        )

        right_and_relationship_program = cypherglot.render_cypher_program_text(
            "MATCH (a:User)-[r:WORKS_AT]->(b:Company {name: 'Bravo'}) WHERE r.since = 2021 CREATE (:User {name: 'Hana'})-[:WORKS_AT {since: 2029}]->(b)",
            schema_context=CompilerSchemaContext.type_aware(self.graph_schema),
        )

        self._execute_program(right_and_relationship_program)

        users = self.conn.execute(
            "SELECT id, name, age FROM cg_node_user ORDER BY id"
        ).fetchall()
        works_at = self.conn.execute(
            "SELECT from_id, to_id, since FROM cg_edge_works_at ORDER BY from_id, to_id, since"
        ).fetchall()

        self.assertEqual(
            users,
            [(1, "Alice", 30), (2, "Bob", 25), (3, "Cara", None), (4, "Dana", None), (5, "Erin", None), (6, "Fiona", None), (7, "Gina", None), (8, "Hana", None)],
        )
        self.assertEqual(
            works_at,
            [(1, 10, 2020), (2, 11, 2021), (3, 10, 2024), (4, 10, 2025), (5, 11, 2026), (6, 10, 2027), (7, 10, 2028), (8, 11, 2029)],
        )

        all_filters_program = cypherglot.render_cypher_program_text(
            "MATCH (a:User {name: 'Bob'})-[r:WORKS_AT]->(b:Company {name: 'Bravo'}) WHERE r.since = 2021 CREATE (:User {name: 'Iris'})-[:WORKS_AT {since: 2030}]->(b)",
            schema_context=CompilerSchemaContext.type_aware(self.graph_schema),
        )

        self._execute_program(all_filters_program)

        users = self.conn.execute(
            "SELECT id, name, age FROM cg_node_user ORDER BY id"
        ).fetchall()
        works_at = self.conn.execute(
            "SELECT from_id, to_id, since FROM cg_edge_works_at ORDER BY from_id, to_id, since"
        ).fetchall()

        self.assertEqual(
            users,
            [(1, "Alice", 30), (2, "Bob", 25), (3, "Cara", None), (4, "Dana", None), (5, "Erin", None), (6, "Fiona", None), (7, "Gina", None), (8, "Hana", None), (9, "Iris", None)],
        )
        self.assertEqual(
            works_at,
            [(1, 10, 2020), (2, 11, 2021), (3, 10, 2024), (4, 10, 2025), (5, 11, 2026), (6, 10, 2027), (7, 10, 2028), (8, 11, 2029), (9, 11, 2030)],
        )

    def test_type_aware_traversal_match_create_existing_endpoints_sql_executes_on_sqlite(
        self,
    ) -> None:
        self._seed_type_aware_graph()

        sql = cypherglot.to_sql(
            (
                "MATCH (a:User)-[r:WORKS_AT]->(b:Company) "
                "WHERE a.name = 'Alice' CREATE (a)-[:WORKS_AT {since: 2024}]->(b)"
            ),
            schema_context=CompilerSchemaContext.type_aware(self.graph_schema),
        )

        self.conn.execute(sql)
        self.conn.commit()

        works_at = self.conn.execute(
            "SELECT from_id, to_id, since FROM cg_edge_works_at ORDER BY id"
        ).fetchall()

        self.assertEqual(works_at, [(1, 10, 2020), (2, 11, 2021), (1, 10, 2024)])

    def test_type_aware_traversal_self_loop_match_create_existing_endpoints_sql_executes_on_sqlite(
        self,
    ) -> None:
        self._seed_type_aware_graph()
        self.conn.execute(
            "INSERT INTO cg_edge_knows (id, from_id, to_id) VALUES (?, ?, ?)",
            (91, 1, 1),
        )
        self.conn.commit()

        sql = cypherglot.to_sql(
            "MATCH (a:User)-[r:KNOWS]->(a:User) CREATE (a)-[:KNOWS]->(a)",
            schema_context=CompilerSchemaContext.type_aware(self.graph_schema),
        )

        self.conn.execute(sql)
        self.conn.commit()

        self_loop_count = self.conn.execute(
            "SELECT COUNT(*) FROM cg_edge_knows WHERE from_id = 1 AND to_id = 1"
        ).fetchone()
        knows_count = self.conn.execute(
            "SELECT COUNT(*) FROM cg_edge_knows"
        ).fetchone()

        self.assertEqual(self_loop_count, (2,))
        self.assertEqual(knows_count, (3,))

    def test_type_aware_traversal_match_merge_program_executes_on_sqlite(self) -> None:
        self._seed_type_aware_graph()

        program = cypherglot.render_cypher_program_text(
            (
                "MATCH (a:User)-[r:KNOWS]->(b:User) "
                "MERGE (a)-[:INTRODUCED]->(:Person {name: 'Cara'})"
            ),
            schema_context=CompilerSchemaContext.type_aware(self.graph_schema),
        )

        self._execute_program(program)
        self._execute_program(program)

        people = self.conn.execute(
            "SELECT id, name FROM cg_node_person ORDER BY id"
        ).fetchall()
        introduced = self.conn.execute(
            "SELECT from_id, to_id FROM cg_edge_introduced ORDER BY id"
        ).fetchall()

        self.assertEqual(people, [(1, "Cara")])
        self.assertEqual(introduced, [(1, 1)])

    def test_type_aware_traversal_self_loop_match_merge_program_executes_on_sqlite(
        self,
    ) -> None:
        self._seed_type_aware_graph()
        self.conn.execute(
            "INSERT INTO cg_edge_knows (id, from_id, to_id) VALUES (?, ?, ?)",
            (91, 1, 1),
        )
        self.conn.commit()

        program = cypherglot.render_cypher_program_text(
            (
                "MATCH (a:User)-[r:KNOWS]->(a:User) "
                "MERGE (a)-[:INTRODUCED]->(:Person {name: 'Cara'})"
            ),
            schema_context=CompilerSchemaContext.type_aware(self.graph_schema),
        )

        self._execute_program(program)
        self._execute_program(program)

        people = self.conn.execute(
            "SELECT id, name FROM cg_node_person ORDER BY id"
        ).fetchall()
        introduced = self.conn.execute(
            "SELECT from_id, to_id FROM cg_edge_introduced ORDER BY from_id, to_id"
        ).fetchall()

        self.assertEqual(people, [(1, "Cara")])
        self.assertEqual(introduced, [(1, 1)])

    def test_type_aware_traversal_match_merge_program_with_new_left_endpoint_executes_on_sqlite(
        self,
    ) -> None:
        self._seed_type_aware_graph()

        program = cypherglot.render_cypher_program_text(
            "MATCH (a:User)-[r:WORKS_AT]->(b:Company {name: 'Acme'}) MERGE (:User {name: 'Cara'})-[:WORKS_AT {since: 2024}]->(b)",
            schema_context=CompilerSchemaContext.type_aware(self.graph_schema),
        )

        self._execute_program(program)
        self._execute_program(program)

        users = self.conn.execute(
            "SELECT id, name, age FROM cg_node_user ORDER BY id"
        ).fetchall()
        works_at = self.conn.execute(
            "SELECT from_id, to_id, since FROM cg_edge_works_at ORDER BY from_id, to_id, since"
        ).fetchall()

        self.assertEqual(users, [(1, "Alice", 30), (2, "Bob", 25), (3, "Cara", None)])
        self.assertEqual(works_at, [(1, 10, 2020), (2, 11, 2021), (3, 10, 2024)])

        filtered_left_program = cypherglot.render_cypher_program_text(
            "MATCH (a:User {name: 'Alice'})-[r:WORKS_AT]->(b:Company) MERGE (:User {name: 'Dana'})-[:WORKS_AT {since: 2025}]->(b)",
            schema_context=CompilerSchemaContext.type_aware(self.graph_schema),
        )
        filtered_right_program = cypherglot.render_cypher_program_text(
            "MATCH (a:User)-[r:WORKS_AT]->(b:Company {name: 'Bravo'}) MERGE (:User {name: 'Erin'})-[:WORKS_AT {since: 2026}]->(b)",
            schema_context=CompilerSchemaContext.type_aware(self.graph_schema),
        )

        self._execute_program(filtered_left_program)
        self._execute_program(filtered_left_program)
        self._execute_program(filtered_right_program)
        self._execute_program(filtered_right_program)

        users = self.conn.execute(
            "SELECT id, name, age FROM cg_node_user ORDER BY id"
        ).fetchall()
        works_at = self.conn.execute(
            "SELECT from_id, to_id, since FROM cg_edge_works_at ORDER BY from_id, to_id, since"
        ).fetchall()

        self.assertEqual(
            users,
            [(1, "Alice", 30), (2, "Bob", 25), (3, "Cara", None), (4, "Dana", None), (5, "Erin", None)],
        )
        self.assertEqual(
            works_at,
            [(1, 10, 2020), (2, 11, 2021), (3, 10, 2024), (4, 10, 2025), (5, 11, 2026)],
        )

        relationship_filtered_program = cypherglot.render_cypher_program_text(
            "MATCH (a:User)-[r:WORKS_AT]->(b:Company) WHERE r.since = 2020 MERGE (:User {name: 'Fiona'})-[:WORKS_AT {since: 2027}]->(b)",
            schema_context=CompilerSchemaContext.type_aware(self.graph_schema),
        )
        combined_filtered_program = cypherglot.render_cypher_program_text(
            "MATCH (a:User {name: 'Alice'})-[r:WORKS_AT]->(b:Company) WHERE r.since = 2020 MERGE (:User {name: 'Gina'})-[:WORKS_AT {since: 2028}]->(b)",
            schema_context=CompilerSchemaContext.type_aware(self.graph_schema),
        )

        self._execute_program(relationship_filtered_program)
        self._execute_program(relationship_filtered_program)
        self._execute_program(combined_filtered_program)
        self._execute_program(combined_filtered_program)

        users = self.conn.execute(
            "SELECT id, name, age FROM cg_node_user ORDER BY id"
        ).fetchall()
        works_at = self.conn.execute(
            "SELECT from_id, to_id, since FROM cg_edge_works_at ORDER BY from_id, to_id, since"
        ).fetchall()

        self.assertEqual(
            users,
            [(1, "Alice", 30), (2, "Bob", 25), (3, "Cara", None), (4, "Dana", None), (5, "Erin", None), (6, "Fiona", None), (7, "Gina", None)],
        )
        self.assertEqual(
            works_at,
            [(1, 10, 2020), (2, 11, 2021), (3, 10, 2024), (4, 10, 2025), (5, 11, 2026), (6, 10, 2027), (7, 10, 2028)],
        )

        right_and_relationship_program = cypherglot.render_cypher_program_text(
            "MATCH (a:User)-[r:WORKS_AT]->(b:Company {name: 'Bravo'}) WHERE r.since = 2021 MERGE (:User {name: 'Hana'})-[:WORKS_AT {since: 2029}]->(b)",
            schema_context=CompilerSchemaContext.type_aware(self.graph_schema),
        )

        self._execute_program(right_and_relationship_program)
        self._execute_program(right_and_relationship_program)

        users = self.conn.execute(
            "SELECT id, name, age FROM cg_node_user ORDER BY id"
        ).fetchall()
        works_at = self.conn.execute(
            "SELECT from_id, to_id, since FROM cg_edge_works_at ORDER BY from_id, to_id, since"
        ).fetchall()

        self.assertEqual(
            users,
            [(1, "Alice", 30), (2, "Bob", 25), (3, "Cara", None), (4, "Dana", None), (5, "Erin", None), (6, "Fiona", None), (7, "Gina", None), (8, "Hana", None)],
        )
        self.assertEqual(
            works_at,
            [(1, 10, 2020), (2, 11, 2021), (3, 10, 2024), (4, 10, 2025), (5, 11, 2026), (6, 10, 2027), (7, 10, 2028), (8, 11, 2029)],
        )

        all_filters_program = cypherglot.render_cypher_program_text(
            "MATCH (a:User {name: 'Bob'})-[r:WORKS_AT]->(b:Company {name: 'Bravo'}) WHERE r.since = 2021 MERGE (:User {name: 'Iris'})-[:WORKS_AT {since: 2030}]->(b)",
            schema_context=CompilerSchemaContext.type_aware(self.graph_schema),
        )

        self._execute_program(all_filters_program)
        self._execute_program(all_filters_program)

        users = self.conn.execute(
            "SELECT id, name, age FROM cg_node_user ORDER BY id"
        ).fetchall()
        works_at = self.conn.execute(
            "SELECT from_id, to_id, since FROM cg_edge_works_at ORDER BY from_id, to_id, since"
        ).fetchall()

        self.assertEqual(
            users,
            [(1, "Alice", 30), (2, "Bob", 25), (3, "Cara", None), (4, "Dana", None), (5, "Erin", None), (6, "Fiona", None), (7, "Gina", None), (8, "Hana", None), (9, "Iris", None)],
        )
        self.assertEqual(
            works_at,
            [(1, 10, 2020), (2, 11, 2021), (3, 10, 2024), (4, 10, 2025), (5, 11, 2026), (6, 10, 2027), (7, 10, 2028), (8, 11, 2029), (9, 11, 2030)],
        )

    def test_type_aware_traversal_self_loop_match_merge_program_with_new_left_endpoint_executes_on_sqlite(
        self,
    ) -> None:
        self._seed_type_aware_graph()
        self.conn.execute(
            "INSERT INTO cg_edge_knows (id, from_id, to_id) VALUES (?, ?, ?)",
            (91, 1, 1),
        )
        self.conn.commit()

        program = cypherglot.render_cypher_program_text(
            "MATCH (a:User)-[r:KNOWS]->(a:User) MERGE (:User {name: 'Cara'})-[:KNOWS]->(a)",
            schema_context=CompilerSchemaContext.type_aware(self.graph_schema),
        )

        self._execute_program(program)
        self._execute_program(program)

        users = self.conn.execute(
            "SELECT id, name, age FROM cg_node_user ORDER BY id"
        ).fetchall()
        knows = self.conn.execute(
            "SELECT from_id, to_id FROM cg_edge_knows ORDER BY from_id, to_id"
        ).fetchall()

        self.assertEqual(users, [(1, "Alice", 30), (2, "Bob", 25), (3, "Cara", None)])
        self.assertEqual(knows, [(1, 1), (1, 2), (3, 1)])

    def test_type_aware_traversal_self_loop_match_create_program_with_new_left_endpoint_executes_on_sqlite(
        self,
    ) -> None:
        self._seed_type_aware_graph()
        self.conn.execute(
            "INSERT INTO cg_edge_knows (id, from_id, to_id) VALUES (?, ?, ?)",
            (91, 1, 1),
        )
        self.conn.commit()

        program = cypherglot.render_cypher_program_text(
            "MATCH (a:User)-[r:KNOWS]->(a:User) CREATE (:User {name: 'Cara'})-[:KNOWS]->(a)",
            schema_context=CompilerSchemaContext.type_aware(self.graph_schema),
        )

        self._execute_program(program)

        users = self.conn.execute(
            "SELECT id, name, age FROM cg_node_user ORDER BY id"
        ).fetchall()
        knows = self.conn.execute(
            "SELECT from_id, to_id FROM cg_edge_knows ORDER BY from_id, to_id"
        ).fetchall()

        self.assertEqual(users, [(1, "Alice", 30), (2, "Bob", 25), (3, "Cara", None)])
        self.assertEqual(knows, [(1, 1), (1, 2), (3, 1)])

    def test_type_aware_traversal_match_merge_existing_endpoints_sql_executes_on_sqlite(
        self,
    ) -> None:
        self._seed_type_aware_graph()

        sql = cypherglot.to_sql(
            (
                "MATCH (a:User)-[r:WORKS_AT]->(b:Company) "
                "WHERE a.name = 'Alice' MERGE (a)-[:WORKS_AT {since: 2020}]->(b)"
            ),
            schema_context=CompilerSchemaContext.type_aware(self.graph_schema),
        )

        self.conn.execute(sql)
        self.conn.execute(sql)
        self.conn.commit()

        works_at = self.conn.execute(
            "SELECT from_id, to_id, since FROM cg_edge_works_at ORDER BY id"
        ).fetchall()

        self.assertEqual(works_at, [(1, 10, 2020), (2, 11, 2021)])

    def test_type_aware_traversal_existing_endpoints_filtered_create_merge_with_distinct_target_edge_type_executes_on_sqlite(
        self,
    ) -> None:
        local_conn = sqlite3.connect(":memory:")
        local_schema = GraphSchema(
            node_types=(
                NodeTypeSpec(
                    name="User",
                    properties=(PropertyField("name", "string"),),
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
                EdgeTypeSpec(
                    name="INTRODUCED",
                    source_type="User",
                    target_type="Company",
                ),
            ),
        )
        local_conn.executescript("\n".join(local_schema.sqlite_ddl()))
        local_conn.executemany(
            "INSERT INTO cg_node_user (id, name) VALUES (?, ?)",
            ((1, "Alice"), (2, "Bob")),
        )
        local_conn.executemany(
            "INSERT INTO cg_node_company (id, name) VALUES (?, ?)",
            ((10, "Acme"), (11, "Bravo")),
        )
        local_conn.executemany(
            "INSERT INTO cg_edge_works_at (id, from_id, to_id, since) VALUES (?, ?, ?, ?)",
            ((100, 1, 10, 2020), (101, 2, 11, 2021)),
        )
        local_conn.commit()

        create_sql = cypherglot.to_sql(
            "MATCH (a:User {name: 'Alice'})-[r:WORKS_AT]->(b:Company {name: 'Acme'}) WHERE r.since = 2020 CREATE (a)-[:INTRODUCED]->(b)",
            schema_context=CompilerSchemaContext.type_aware(local_schema),
        )
        merge_sql = cypherglot.to_sql(
            "MATCH (a:User {name: 'Alice'})-[r:WORKS_AT]->(b:Company {name: 'Acme'}) WHERE r.since = 2020 MERGE (a)-[:INTRODUCED]->(b)",
            schema_context=CompilerSchemaContext.type_aware(local_schema),
        )

        local_conn.execute(create_sql)
        local_conn.execute(merge_sql)
        local_conn.execute(merge_sql)
        local_conn.commit()

        introduced = local_conn.execute(
            "SELECT from_id, to_id FROM cg_edge_introduced ORDER BY from_id, to_id"
        ).fetchall()
        local_conn.close()

        self.assertEqual(introduced, [(1, 10)])

    def test_type_aware_traversal_self_loop_match_merge_existing_endpoints_sql_executes_on_sqlite(
        self,
    ) -> None:
        self._seed_type_aware_graph()
        self.conn.execute(
            "INSERT INTO cg_edge_knows (id, from_id, to_id) VALUES (?, ?, ?)",
            (91, 1, 1),
        )
        self.conn.commit()

        sql = cypherglot.to_sql(
            "MATCH (a:User)-[r:KNOWS]->(a:User) MERGE (a)-[:KNOWS]->(a)",
            schema_context=CompilerSchemaContext.type_aware(self.graph_schema),
        )

        self.conn.execute(sql)
        self.conn.execute(sql)
        self.conn.commit()

        self_loop_count = self.conn.execute(
            "SELECT COUNT(*) FROM cg_edge_knows WHERE from_id = 1 AND to_id = 1"
        ).fetchone()
        knows_count = self.conn.execute(
            "SELECT COUNT(*) FROM cg_edge_knows"
        ).fetchone()

        self.assertEqual(self_loop_count, (1,))
        self.assertEqual(knows_count, (2,))

    def test_type_aware_fixed_length_multi_hop_match_executes_on_sqlite(self) -> None:
        self._seed_type_aware_graph()

        sql = cypherglot.to_sql(
            (
                "MATCH (a:User)-[r:KNOWS]->(b:User)-[s:WORKS_AT]->(c:Company) "
                "RETURN a.name AS user_name, c.name AS company ORDER BY company"
            ),
            schema_context=CompilerSchemaContext.type_aware(self.graph_schema),
        )

        rows = self.conn.execute(sql).fetchall()

        self.assertEqual(rows, [("Alice", "Bravo")])

    def test_type_aware_bounded_variable_length_match_executes_on_sqlite(self) -> None:
        self._seed_type_aware_variable_length_graph()

        sql = cypherglot.to_sql(
            (
                "MATCH (a:User)-[:KNOWS*0..2]->(b:User) "
                "RETURN a.name AS user_name, b.name AS friend ORDER BY friend, user_name"
            ),
            schema_context=CompilerSchemaContext.type_aware(self.graph_schema),
        )

        rows = self.conn.execute(sql).fetchall()

        self.assertEqual(
            rows,
            [
                ("Alice", "Alice"),
                ("Alice", "Bob"),
                ("Bob", "Bob"),
                ("Alice", "Cara"),
                ("Bob", "Cara"),
                ("Cara", "Cara"),
            ],
        )

        aggregate_sql = cypherglot.to_sql(
            "MATCH (a:User)-[:KNOWS*0..2]->(b:User) RETURN sum(b.age) AS total_age",
            schema_context=CompilerSchemaContext.type_aware(self.graph_schema),
        )

        aggregate_rows = self.conn.execute(aggregate_sql).fetchall()

        self.assertEqual(aggregate_rows, [(140,)])

        with self.assertRaisesRegex(
            ValueError,
            "relational output mode does not yet support whole-entity or introspection returns",
        ):
            cypherglot.to_sql(
                (
                    "MATCH (a:User)-[:KNOWS*0..2]->(b:User) RETURN b AS friend_node, properties(b) AS friend_props, "
                    "labels(b) AS friend_labels, keys(b) AS friend_keys, b.name AS friend ORDER BY friend"
                ),
                schema_context=self._type_aware_schema_context(),
            )

        scalar_function_sql = cypherglot.to_sql(
            (
                "MATCH (a:User)-[:KNOWS*0..2]->(b:User) "
                "RETURN lower(b.name) AS lower_friend, toString(b.age) AS age_text "
                "ORDER BY age_text, lower_friend"
            ),
            schema_context=CompilerSchemaContext.type_aware(self.graph_schema),
        )

        scalar_function_rows = self.conn.execute(scalar_function_sql).fetchall()

        self.assertEqual(
            scalar_function_rows,
            [
                ("cara", "20"),
                ("cara", "20"),
                ("cara", "20"),
                ("bob", "25"),
                ("bob", "25"),
                ("alice", "30"),
            ],
        )

        id_sql = cypherglot.to_sql(
            "MATCH (a:User)-[:KNOWS*0..2]->(b:User) RETURN id(b) AS friend_id ORDER BY friend_id",
            schema_context=CompilerSchemaContext.type_aware(self.graph_schema),
        )

        id_rows = self.conn.execute(id_sql).fetchall()

        self.assertEqual(id_rows, [(1,), (2,), (2,), (3,), (3,), (3,)])

    def test_type_aware_bounded_variable_length_match_grouped_count_executes_on_sqlite(
        self,
    ) -> None:
        self._seed_type_aware_variable_length_graph()

        sql = cypherglot.to_sql(
            (
                "MATCH (a:User)-[:KNOWS*0..2]->(b:User) "
                "RETURN b.name AS friend, count(b) AS total ORDER BY total DESC, friend"
            ),
            schema_context=CompilerSchemaContext.type_aware(self.graph_schema),
        )

        rows = self.conn.execute(sql).fetchall()

        self.assertEqual(rows, [("Cara", 3), ("Bob", 2), ("Alice", 1)])

        with self.assertRaisesRegex(
            ValueError,
            "relational output mode does not yet support whole-entity or introspection returns",
        ):
            cypherglot.to_sql(
                (
                    "MATCH (a:User)-[:KNOWS*0..2]->(b:User) "
                    "RETURN labels(b) AS friend_labels, count(b) AS total ORDER BY total DESC, friend_labels"
                ),
                schema_context=self._type_aware_schema_context(),
            )

        with self.assertRaisesRegex(
            ValueError,
            "relational output mode does not yet support whole-entity or introspection returns",
        ):
            self.conn.execute(
                cypherglot.to_sql(
                    (
                        "MATCH (a:User)-[:KNOWS*0..2]->(b:User) "
                        "RETURN keys(b) AS friend_keys, count(b) AS total ORDER BY total DESC, friend_keys"
                    ),
                    schema_context=self._type_aware_schema_context(),
                )
            ).fetchall()

        lowered_sql = cypherglot.to_sql(
            (
                "MATCH (a:User)-[:KNOWS*0..2]->(b:User) "
                "RETURN lower(b.name) AS lowered_name, count(b) AS total ORDER BY total DESC, lowered_name"
            ),
            schema_context=CompilerSchemaContext.type_aware(self.graph_schema),
        )

        lowered_rows = self.conn.execute(lowered_sql).fetchall()

        self.assertEqual(lowered_rows, [("cara", 3), ("bob", 2), ("alice", 1)])

        age_text_sql = cypherglot.to_sql(
            (
                "MATCH (a:User)-[:KNOWS*0..2]->(b:User) "
                "RETURN toString(b.age) AS age_text, count(b) AS total ORDER BY total DESC, age_text"
            ),
            schema_context=CompilerSchemaContext.type_aware(self.graph_schema),
        )

        age_text_rows = self.conn.execute(age_text_sql).fetchall()

        self.assertEqual(age_text_rows, [("20", 3), ("25", 2), ("30", 1)])

        id_sql = cypherglot.to_sql(
            (
                "MATCH (a:User)-[:KNOWS*0..2]->(b:User) "
                "RETURN id(b) AS friend_id, count(b) AS total ORDER BY total DESC, friend_id"
            ),
            schema_context=CompilerSchemaContext.type_aware(self.graph_schema),
        )

        id_rows = self.conn.execute(id_sql).fetchall()

        self.assertEqual(id_rows, [(3, 3), (2, 2), (1, 1)])

        relational_entity_sql = cypherglot.to_sql(
            (
                "MATCH (a:User)-[:KNOWS*0..2]->(b:User) "
                "RETURN b AS friend, count(b) AS total ORDER BY friend, total"
            ),
            schema_context=CompilerSchemaContext.type_aware(
                self.graph_schema,
            ),
        )

        relational_entity_rows = self.conn.execute(relational_entity_sql).fetchall()

        self.assertEqual(
            relational_entity_rows,
            [
                (1, "User", "Alice", 30, 1),
                (2, "User", "Bob", 25, 2),
                (3, "User", "Cara", 20, 3),
            ],
        )

        relational_properties_sql = cypherglot.to_sql(
            (
                "MATCH (a:User)-[:KNOWS*0..2]->(b:User) "
                "RETURN properties(b) AS props, count(b) AS total ORDER BY props, total"
            ),
            schema_context=CompilerSchemaContext.type_aware(
                self.graph_schema,
            ),
        )

        relational_properties_rows = self.conn.execute(
            relational_properties_sql
        ).fetchall()

        self.assertEqual(
            relational_properties_rows,
            [
                ("Alice", 30, 1),
                ("Bob", 25, 2),
                ("Cara", 20, 3),
            ],
        )

    def test_type_aware_fixed_length_multi_hop_introspection_executes_on_sqlite(self) -> None:
        self._seed_type_aware_graph()

        sql = cypherglot.to_sql(
            (
                "MATCH (a:User)-[r:KNOWS]->(b:User)-[s:WORKS_AT]->(c:Company) "
                "RETURN type(r) AS first_rel_type, startNode(s).name AS employee, "
                "endNode(s) AS employer ORDER BY first_rel_type, employee"
            ),
            schema_context=self._type_aware_schema_context(),
        )

        rows = self.conn.execute(sql).fetchall()

        self.assertEqual(rows, [("KNOWS", "Bob", 11, "Company", "Bravo")])

    def test_type_aware_fixed_length_multi_hop_helper_returns_execute_on_sqlite(self) -> None:
        self._seed_type_aware_graph()

        with self.assertRaisesRegex(
            ValueError,
            "relational output mode does not yet support whole-entity or introspection returns",
        ):
            cypherglot.to_sql(
                (
                    "MATCH (a:User)-[r:KNOWS]->(b:User)-[s:WORKS_AT]->(c:Company) "
                    "RETURN properties(b) AS friend_props, labels(b) AS friend_labels, "
                    "keys(s) AS rel_keys, startNode(s).name AS employee, endNode(s).id AS company_id "
                    "ORDER BY friend_props, friend_labels, rel_keys, employee, company_id"
                ),
                schema_context=self._type_aware_schema_context(),
            )

    def test_type_aware_fixed_length_multi_hop_complementary_helper_returns_execute_on_sqlite(
        self,
    ) -> None:
        self._seed_type_aware_graph()

        with self.assertRaisesRegex(
            ValueError,
            "relational output mode does not yet support whole-entity or introspection returns",
        ):
            cypherglot.to_sql(
                (
                    "MATCH (a:User)-[r:KNOWS]->(b:User)-[s:WORKS_AT]->(c:Company) "
                    "RETURN properties(s) AS rel_props, keys(b) AS friend_keys, "
                    "labels(c) AS company_labels, startNode(s).id AS employee_id, endNode(s).name AS company_name "
                    "ORDER BY rel_props, friend_keys, company_labels, employee_id, company_name"
                ),
                schema_context=self._type_aware_schema_context(),
            )

    def test_type_aware_fixed_length_multi_hop_grouped_helper_returns_execute_on_sqlite(
        self,
    ) -> None:
        self._seed_type_aware_graph()

        with self.assertRaisesRegex(
            ValueError,
            "relational output mode does not yet support whole-entity or introspection returns",
        ):
            cypherglot.to_sql(
                (
                    "MATCH (a:User)-[r:KNOWS]->(b:User)-[s:WORKS_AT]->(c:Company) "
                    "RETURN properties(b) AS friend_props, labels(b) AS friend_labels, "
                    "keys(s) AS rel_keys, startNode(s).name AS employee, endNode(s).id AS company_id, "
                    "count(s) AS total ORDER BY total DESC"
                ),
                schema_context=self._type_aware_schema_context(),
            )

    def test_type_aware_fixed_length_multi_hop_grouped_complementary_helper_returns_execute_on_sqlite(
        self,
    ) -> None:
        self._seed_type_aware_graph()

        with self.assertRaisesRegex(
            ValueError,
            "relational output mode does not yet support whole-entity or introspection returns",
        ):
            cypherglot.to_sql(
                (
                    "MATCH (a:User)-[r:KNOWS]->(b:User)-[s:WORKS_AT]->(c:Company) "
                    "RETURN properties(s) AS rel_props, keys(b) AS friend_keys, "
                    "labels(c) AS company_labels, startNode(s).id AS employee_id, endNode(s).name AS company_name, "
                    "count(s) AS total ORDER BY total DESC"
                ),
                schema_context=self._type_aware_schema_context(),
            )

    def test_type_aware_optional_match_missing_row_executes_on_sqlite(self) -> None:
        self._seed_type_aware_graph()

        sql = cypherglot.to_sql(
            "OPTIONAL MATCH (u:User) WHERE u.name = 'Cara' RETURN u.name AS name",
            schema_context=CompilerSchemaContext.type_aware(self.graph_schema),
        )

        rows = self.conn.execute(sql).fetchall()

        self.assertEqual(rows, [(None,)])

    def test_type_aware_with_return_executes_on_sqlite(self) -> None:
        self._seed_type_aware_graph()

        sql = cypherglot.to_sql(
            (
                "MATCH (u:User) WITH u AS person, u.name AS name "
                "WHERE name = 'Alice' "
                "RETURN person.name AS display_name, id(person) AS person_id "
                "ORDER BY display_name, person_id"
            ),
            schema_context=CompilerSchemaContext.type_aware(self.graph_schema),
        )

        rows = self.conn.execute(sql).fetchall()

        self.assertEqual(rows, [("Alice", 1)])

    def test_type_aware_match_with_chain_source_executes_on_sqlite(self) -> None:
        self._seed_type_aware_graph()

        sql = cypherglot.to_sql(
            (
                "MATCH (a:User)-[r:KNOWS]->(b:User)-[s:WORKS_AT]->(c:Company) "
                "WITH b AS friend, c.name AS company "
                "RETURN friend.name AS friend_name, company ORDER BY company"
            ),
            schema_context=CompilerSchemaContext.type_aware(self.graph_schema),
        )

        rows = self.conn.execute(sql).fetchall()

        self.assertEqual(rows, [("Bob", "Bravo")])

    def test_type_aware_bounded_variable_length_match_with_return_executes_on_sqlite(
        self,
    ) -> None:
        self._seed_type_aware_variable_length_graph()

        sql = cypherglot.to_sql(
            (
                "MATCH (a:User)-[:KNOWS*0..2]->(b:User) "
                "WITH b AS friend RETURN friend.name AS name ORDER BY name"
            ),
            schema_context=CompilerSchemaContext.type_aware(self.graph_schema),
        )

        rows = self.conn.execute(sql).fetchall()

        self.assertEqual(
            rows,
            [("Alice",), ("Bob",), ("Bob",), ("Cara",), ("Cara",), ("Cara",)],
        )

        with self.assertRaisesRegex(
            ValueError,
            "relational output mode does not yet support whole-entity or introspection returns",
        ):
            cypherglot.to_sql(
                (
                    "MATCH (a:User)-[:KNOWS*0..2]->(b:User) WITH b AS friend "
                    "RETURN friend AS friend_node, properties(friend) AS friend_props, "
                    "labels(friend) AS friend_labels, keys(friend) AS friend_keys, friend.name AS name ORDER BY name"
                ),
                schema_context=self._type_aware_schema_context(),
            )

        scalar_function_sql = cypherglot.to_sql(
            (
                "MATCH (a:User)-[:KNOWS*0..2]->(b:User) WITH b AS friend "
                "RETURN lower(friend.name) AS lower_name, toString(friend.age) AS age_text "
                "ORDER BY age_text, lower_name"
            ),
            schema_context=CompilerSchemaContext.type_aware(self.graph_schema),
        )

        scalar_function_rows = self.conn.execute(scalar_function_sql).fetchall()

        self.assertEqual(
            scalar_function_rows,
            [
                ("cara", "20"),
                ("cara", "20"),
                ("cara", "20"),
                ("bob", "25"),
                ("bob", "25"),
                ("alice", "30"),
            ],
        )

        id_sql = cypherglot.to_sql(
            (
                "MATCH (a:User)-[:KNOWS*0..2]->(b:User) WITH b AS friend "
                "RETURN id(friend) AS friend_id ORDER BY friend_id"
            ),
            schema_context=CompilerSchemaContext.type_aware(self.graph_schema),
        )

        id_rows = self.conn.execute(id_sql).fetchall()

        self.assertEqual(id_rows, [(1,), (2,), (2,), (3,), (3,), (3,)])

    def test_type_aware_bounded_variable_length_match_with_grouped_count_executes_on_sqlite(
        self,
    ) -> None:
        self._seed_type_aware_variable_length_graph()

        sql = cypherglot.to_sql(
            (
                "MATCH (a:User)-[:KNOWS*0..2]->(b:User) WITH b AS friend "
                "RETURN friend.name AS name, count(friend) AS total ORDER BY total DESC, name"
            ),
            schema_context=CompilerSchemaContext.type_aware(self.graph_schema),
        )

        rows = self.conn.execute(sql).fetchall()

        self.assertEqual(rows, [("Cara", 3), ("Bob", 2), ("Alice", 1)])

        aggregate_sql = cypherglot.to_sql(
            (
                "MATCH (a:User)-[:KNOWS*0..2]->(b:User) WITH b AS friend "
                "RETURN friend.name AS name, sum(friend.age) AS total_age ORDER BY total_age DESC, name"
            ),
            schema_context=CompilerSchemaContext.type_aware(self.graph_schema),
        )

        aggregate_rows = self.conn.execute(aggregate_sql).fetchall()

        self.assertEqual(aggregate_rows, [("Cara", 60), ("Bob", 50), ("Alice", 30)])

        with self.assertRaisesRegex(
            ValueError,
            "relational output mode does not yet support whole-entity or introspection returns",
        ):
            cypherglot.to_sql(
                (
                    "MATCH (a:User)-[:KNOWS*0..2]->(b:User) WITH b AS friend "
                    "RETURN labels(friend) AS friend_labels, count(friend) AS total ORDER BY total DESC, friend_labels"
                ),
                schema_context=self._type_aware_schema_context(),
            )

        with self.assertRaisesRegex(
            ValueError,
            "relational output mode does not yet support whole-entity or introspection returns",
        ):
            cypherglot.to_sql(
                (
                    "MATCH (a:User)-[:KNOWS*0..2]->(b:User) WITH b AS friend "
                    "RETURN keys(friend) AS friend_keys, count(friend) AS total ORDER BY total DESC, friend_keys"
                ),
                schema_context=self._type_aware_schema_context(),
            )

        lowered_sql = cypherglot.to_sql(
            (
                "MATCH (a:User)-[:KNOWS*0..2]->(b:User) WITH b AS friend "
                "RETURN lower(friend.name) AS lowered_name, count(friend) AS total ORDER BY total DESC, lowered_name"
            ),
            schema_context=CompilerSchemaContext.type_aware(self.graph_schema),
        )

        lowered_rows = self.conn.execute(lowered_sql).fetchall()

        self.assertEqual(lowered_rows, [("cara", 3), ("bob", 2), ("alice", 1)])

        age_text_sql = cypherglot.to_sql(
            (
                "MATCH (a:User)-[:KNOWS*0..2]->(b:User) WITH b AS friend "
                "RETURN toString(friend.age) AS age_text, count(friend) AS total ORDER BY total DESC, age_text"
            ),
            schema_context=CompilerSchemaContext.type_aware(self.graph_schema),
        )

        age_text_rows = self.conn.execute(age_text_sql).fetchall()

        self.assertEqual(age_text_rows, [("20", 3), ("25", 2), ("30", 1)])

        id_sql = cypherglot.to_sql(
            (
                "MATCH (a:User)-[:KNOWS*0..2]->(b:User) WITH b AS friend "
                "RETURN id(friend) AS friend_id, count(friend) AS total ORDER BY total DESC, friend_id"
            ),
            schema_context=CompilerSchemaContext.type_aware(self.graph_schema),
        )

        id_rows = self.conn.execute(id_sql).fetchall()

        self.assertEqual(id_rows, [(3, 3), (2, 2), (1, 1)])

        relational_entity_sql = cypherglot.to_sql(
            (
                "MATCH (a:User)-[:KNOWS*0..2]->(b:User) WITH b AS friend "
                "RETURN friend AS user, count(friend) AS total ORDER BY user, total"
            ),
            schema_context=CompilerSchemaContext.type_aware(
                self.graph_schema,
            ),
        )

        relational_entity_rows = self.conn.execute(relational_entity_sql).fetchall()

        self.assertEqual(
            relational_entity_rows,
            [
                (1, "User", "Alice", 30, 1),
                (2, "User", "Bob", 25, 2),
                (3, "User", "Cara", 20, 3),
            ],
        )

        relational_properties_sql = cypherglot.to_sql(
            (
                "MATCH (a:User)-[:KNOWS*0..2]->(b:User) WITH b AS friend "
                "RETURN properties(friend) AS props, count(friend) AS total "
                "ORDER BY props, total"
            ),
            schema_context=CompilerSchemaContext.type_aware(
                self.graph_schema,
            ),
        )

        relational_properties_rows = self.conn.execute(
            relational_properties_sql
        ).fetchall()

        self.assertEqual(
            relational_properties_rows,
            [
                ("Alice", 30, 1),
                ("Bob", 25, 2),
                ("Cara", 20, 3),
            ],
        )

    def test_type_aware_match_with_chain_relationship_introspection_executes_on_sqlite(
        self,
    ) -> None:
        self._seed_type_aware_graph()

        sql = cypherglot.to_sql(
            (
                "MATCH (a:User)-[r:KNOWS]->(b:User)-[s:WORKS_AT]->(c:Company) "
                "WITH b AS friend, c AS company, s AS rel "
                "RETURN startNode(rel).name AS employee, endNode(rel) AS employer, "
                "type(rel) AS rel_type ORDER BY employee, rel_type"
            ),
            schema_context=self._type_aware_schema_context(),
        )

        rows = self.conn.execute(sql).fetchall()

        self.assertEqual(rows, [("Bob", 11, "Company", "Bravo", "WORKS_AT")])

    def test_type_aware_match_with_chain_helper_introspection_executes_on_sqlite(
        self,
    ) -> None:
        self._seed_type_aware_graph()

        with self.assertRaisesRegex(
            ValueError,
            "relational output mode does not yet support whole-entity or introspection returns",
        ):
            cypherglot.to_sql(
                (
                    "MATCH (a:User)-[r:KNOWS]->(b:User)-[s:WORKS_AT]->(c:Company) "
                    "WITH b AS friend, s AS rel, c AS company "
                    "RETURN properties(friend) AS friend_props, labels(friend) AS friend_labels, "
                    "keys(rel) AS rel_keys, startNode(rel).name AS employee, endNode(rel).id AS company_id "
                    "ORDER BY friend_props, friend_labels, rel_keys, employee, company_id"
                ),
                schema_context=self._type_aware_schema_context(),
            )

    def test_type_aware_match_with_chain_complementary_helper_introspection_executes_on_sqlite(
        self,
    ) -> None:
        self._seed_type_aware_graph()

        with self.assertRaisesRegex(
            ValueError,
            "relational output mode does not yet support whole-entity or introspection returns",
        ):
            cypherglot.to_sql(
                (
                    "MATCH (a:User)-[r:KNOWS]->(b:User)-[s:WORKS_AT]->(c:Company) "
                    "WITH b AS friend, s AS rel, c AS company "
                    "RETURN properties(rel) AS rel_props, keys(friend) AS friend_keys, "
                    "labels(company) AS company_labels, startNode(rel).id AS employee_id, endNode(rel).name AS company_name "
                    "ORDER BY rel_props, friend_keys, company_labels, employee_id, company_name"
                ),
                schema_context=self._type_aware_schema_context(),
            )

    def test_type_aware_match_with_chain_grouped_helper_introspection_executes_on_sqlite(
        self,
    ) -> None:
        self._seed_type_aware_graph()

        with self.assertRaisesRegex(
            ValueError,
            "relational output mode does not yet support whole-entity or introspection returns",
        ):
            cypherglot.to_sql(
                (
                    "MATCH (a:User)-[r:KNOWS]->(b:User)-[s:WORKS_AT]->(c:Company) "
                    "WITH b AS friend, s AS rel, c AS company "
                    "RETURN properties(friend) AS friend_props, labels(friend) AS friend_labels, "
                    "keys(rel) AS rel_keys, startNode(rel).name AS employee, endNode(rel).id AS company_id, "
                    "count(rel) AS total ORDER BY total DESC"
                ),
                schema_context=self._type_aware_schema_context(),
            )

    def test_type_aware_match_with_chain_grouped_complementary_helper_introspection_executes_on_sqlite(
        self,
    ) -> None:
        self._seed_type_aware_graph()

        with self.assertRaisesRegex(
            ValueError,
            "relational output mode does not yet support whole-entity or introspection returns",
        ):
            cypherglot.to_sql(
                (
                    "MATCH (a:User)-[r:KNOWS]->(b:User)-[s:WORKS_AT]->(c:Company) "
                    "WITH b AS friend, s AS rel, c AS company "
                    "RETURN properties(rel) AS rel_props, keys(friend) AS friend_keys, "
                    "labels(company) AS company_labels, startNode(rel).id AS employee_id, endNode(rel).name AS company_name, "
                    "count(rel) AS total ORDER BY total DESC"
                ),
                schema_context=self._type_aware_schema_context(),
            )

    def test_type_aware_fixed_length_multi_hop_grouped_aggregate_executes_on_sqlite(
        self,
    ) -> None:
        self._seed_type_aware_graph()

        sql = cypherglot.to_sql(
            (
                "MATCH (a:User)-[r:KNOWS]->(b:User)-[s:WORKS_AT]->(c:Company) "
                "RETURN c.name AS company, count(s) AS total ORDER BY total DESC, company"
            ),
            schema_context=CompilerSchemaContext.type_aware(self.graph_schema),
        )

        rows = self.conn.execute(sql).fetchall()

        self.assertEqual(rows, [("Bravo", 1)])

    def test_type_aware_fixed_length_multi_hop_ungrouped_sum_aggregate_executes_on_sqlite(
        self,
    ) -> None:
        self._seed_type_aware_graph()

        sql = cypherglot.to_sql(
            (
                "MATCH (a:User)-[r:KNOWS]->(b:User)-[s:WORKS_AT]->(c:Company) "
                "RETURN sum(s.since) AS total_since"
            ),
            schema_context=CompilerSchemaContext.type_aware(self.graph_schema),
        )

        rows = self.conn.execute(sql).fetchall()

        self.assertEqual(rows, [(2021,)])

    def test_type_aware_fixed_length_multi_hop_ungrouped_count_star_executes_on_sqlite(
        self,
    ) -> None:
        self._seed_type_aware_graph()

        sql = cypherglot.to_sql(
            (
                "MATCH (a:User)-[r:KNOWS]->(b:User)-[s:WORKS_AT]->(c:Company) "
                "RETURN count(*) AS total"
            ),
            schema_context=CompilerSchemaContext.type_aware(self.graph_schema),
        )

        rows = self.conn.execute(sql).fetchall()

        self.assertEqual(rows, [(1,)])

    def test_type_aware_fixed_length_multi_hop_ungrouped_count_rel_executes_on_sqlite(
        self,
    ) -> None:
        self._seed_type_aware_graph()

        sql = cypherglot.to_sql(
            (
                "MATCH (a:User)-[r:KNOWS]->(b:User)-[s:WORKS_AT]->(c:Company) "
                "RETURN count(s) AS total"
            ),
            schema_context=CompilerSchemaContext.type_aware(self.graph_schema),
        )

        rows = self.conn.execute(sql).fetchall()

        self.assertEqual(rows, [(1,)])

    def test_type_aware_fixed_length_multi_hop_grouped_count_star_executes_on_sqlite(
        self,
    ) -> None:
        self._seed_type_aware_graph()

        sql = cypherglot.to_sql(
            (
                "MATCH (a:User)-[r:KNOWS]->(b:User)-[s:WORKS_AT]->(c:Company) "
                "RETURN c.name AS company, count(*) AS total ORDER BY total DESC, company"
            ),
            schema_context=CompilerSchemaContext.type_aware(self.graph_schema),
        )

        rows = self.conn.execute(sql).fetchall()

        self.assertEqual(rows, [("Bravo", 1)])

    def test_type_aware_fixed_length_multi_hop_grouped_sum_aggregate_executes_on_sqlite(
        self,
    ) -> None:
        self._seed_type_aware_graph()

        sql = cypherglot.to_sql(
            (
                "MATCH (a:User)-[r:KNOWS]->(b:User)-[s:WORKS_AT]->(c:Company) "
                "RETURN c.name AS company, sum(s.since) AS total_since "
                "ORDER BY total_since DESC, company"
            ),
            schema_context=CompilerSchemaContext.type_aware(self.graph_schema),
        )

        rows = self.conn.execute(sql).fetchall()

        self.assertEqual(rows, [("Bravo", 2021)])

    def test_type_aware_fixed_length_multi_hop_ungrouped_min_aggregate_executes_on_sqlite(
        self,
    ) -> None:
        self._seed_type_aware_graph()

        sql = cypherglot.to_sql(
            (
                "MATCH (a:User)-[r:KNOWS]->(b:User)-[s:WORKS_AT]->(c:Company) "
                "RETURN min(s.since) AS first_since"
            ),
            schema_context=CompilerSchemaContext.type_aware(self.graph_schema),
        )

        rows = self.conn.execute(sql).fetchall()

        self.assertEqual(rows, [(2021,)])

    def test_type_aware_fixed_length_multi_hop_grouped_max_aggregate_executes_on_sqlite(
        self,
    ) -> None:
        self._seed_type_aware_graph()

        sql = cypherglot.to_sql(
            (
                "MATCH (a:User)-[r:KNOWS]->(b:User)-[s:WORKS_AT]->(c:Company) "
                "RETURN c.name AS company, max(s.since) AS latest_since "
                "ORDER BY latest_since DESC, company"
            ),
            schema_context=CompilerSchemaContext.type_aware(self.graph_schema),
        )

        rows = self.conn.execute(sql).fetchall()

        self.assertEqual(rows, [("Bravo", 2021)])

    def test_type_aware_match_with_chain_grouped_aggregate_executes_on_sqlite(
        self,
    ) -> None:
        self._seed_type_aware_graph()

        sql = cypherglot.to_sql(
            (
                "MATCH (a:User)-[r:KNOWS]->(b:User)-[s:WORKS_AT]->(c:Company) "
                "WITH c.name AS company, s AS rel "
                "RETURN company, avg(rel.since) AS mean_since ORDER BY mean_since DESC, company"
            ),
            schema_context=CompilerSchemaContext.type_aware(self.graph_schema),
        )

        rows = self.conn.execute(sql).fetchall()

        self.assertEqual(rows, [("Bravo", 2021.0)])

    def test_type_aware_match_with_chain_ungrouped_max_aggregate_executes_on_sqlite(
        self,
    ) -> None:
        self._seed_type_aware_graph()

        sql = cypherglot.to_sql(
            (
                "MATCH (a:User)-[r:KNOWS]->(b:User)-[s:WORKS_AT]->(c:Company) "
                "WITH s AS rel RETURN max(rel.since) AS latest_since"
            ),
            schema_context=CompilerSchemaContext.type_aware(self.graph_schema),
        )

        rows = self.conn.execute(sql).fetchall()

        self.assertEqual(rows, [(2021,)])

    def test_type_aware_match_with_chain_ungrouped_count_star_executes_on_sqlite(
        self,
    ) -> None:
        self._seed_type_aware_graph()

        sql = cypherglot.to_sql(
            (
                "MATCH (a:User)-[r:KNOWS]->(b:User)-[s:WORKS_AT]->(c:Company) "
                "WITH s AS rel RETURN count(*) AS total"
            ),
            schema_context=CompilerSchemaContext.type_aware(self.graph_schema),
        )

        rows = self.conn.execute(sql).fetchall()

        self.assertEqual(rows, [(1,)])

    def test_type_aware_match_with_chain_ungrouped_count_rel_executes_on_sqlite(
        self,
    ) -> None:
        self._seed_type_aware_graph()

        sql = cypherglot.to_sql(
            (
                "MATCH (a:User)-[r:KNOWS]->(b:User)-[s:WORKS_AT]->(c:Company) "
                "WITH s AS rel RETURN count(rel) AS total"
            ),
            schema_context=CompilerSchemaContext.type_aware(self.graph_schema),
        )

        rows = self.conn.execute(sql).fetchall()

        self.assertEqual(rows, [(1,)])

    def test_type_aware_match_with_chain_grouped_count_star_executes_on_sqlite(
        self,
    ) -> None:
        self._seed_type_aware_graph()

        sql = cypherglot.to_sql(
            (
                "MATCH (a:User)-[r:KNOWS]->(b:User)-[s:WORKS_AT]->(c:Company) "
                "WITH c.name AS company, s AS rel "
                "RETURN company, count(*) AS total ORDER BY total DESC, company"
            ),
            schema_context=CompilerSchemaContext.type_aware(self.graph_schema),
        )

        rows = self.conn.execute(sql).fetchall()

        self.assertEqual(rows, [("Bravo", 1)])

    def test_type_aware_match_with_chain_grouped_count_rel_executes_on_sqlite(
        self,
    ) -> None:
        self._seed_type_aware_graph()

        sql = cypherglot.to_sql(
            (
                "MATCH (a:User)-[r:KNOWS]->(b:User)-[s:WORKS_AT]->(c:Company) "
                "WITH c.name AS company, s AS rel "
                "RETURN company, count(rel) AS total ORDER BY total DESC, company"
            ),
            schema_context=CompilerSchemaContext.type_aware(self.graph_schema),
        )

        rows = self.conn.execute(sql).fetchall()

        self.assertEqual(rows, [("Bravo", 1)])

    def test_type_aware_match_with_chain_ungrouped_sum_aggregate_executes_on_sqlite(
        self,
    ) -> None:
        self._seed_type_aware_graph()

        sql = cypherglot.to_sql(
            (
                "MATCH (a:User)-[r:KNOWS]->(b:User)-[s:WORKS_AT]->(c:Company) "
                "WITH s AS rel RETURN sum(rel.since) AS total_since"
            ),
            schema_context=CompilerSchemaContext.type_aware(self.graph_schema),
        )

        rows = self.conn.execute(sql).fetchall()

        self.assertEqual(rows, [(2021,)])

    def test_type_aware_match_with_chain_grouped_min_aggregate_executes_on_sqlite(
        self,
    ) -> None:
        self._seed_type_aware_graph()

        sql = cypherglot.to_sql(
            (
                "MATCH (a:User)-[r:KNOWS]->(b:User)-[s:WORKS_AT]->(c:Company) "
                "WITH c.name AS company, s AS rel "
                "RETURN company, min(rel.since) AS first_since ORDER BY first_since DESC, company"
            ),
            schema_context=CompilerSchemaContext.type_aware(self.graph_schema),
        )

        rows = self.conn.execute(sql).fetchall()

        self.assertEqual(rows, [("Bravo", 2021)])

    def test_type_aware_relational_chain_endpoint_output_executes_on_sqlite(self) -> None:
        self._seed_type_aware_graph()

        sql = cypherglot.to_sql(
            (
                "MATCH (a:User)-[r:KNOWS]->(b:User)-[s:WORKS_AT]->(c:Company) "
                "RETURN startNode(s) AS employee, endNode(s) AS employer, c.name AS company "
                "ORDER BY company"
            ),
            schema_context=CompilerSchemaContext.type_aware(
                self.graph_schema,
            ),
        )

        rows = self.conn.execute(sql).fetchall()

        self.assertEqual(
            rows,
            [(2, "User", "Bob", 25, 11, "Company", "Bravo", "Bravo")],
        )

    def test_type_aware_relational_chain_entities_and_properties_execute_on_sqlite(
        self,
    ) -> None:
        self._seed_type_aware_graph()

        sql = cypherglot.to_sql(
            (
                "MATCH (a:User)-[r:KNOWS]->(b:User)-[s:WORKS_AT]->(c:Company) "
                "RETURN b AS friend, properties(b) AS friend_props, "
                "s AS rel, properties(s) AS rel_props, c.name AS company_name "
                "ORDER BY company_name"
            ),
            schema_context=CompilerSchemaContext.type_aware(
                self.graph_schema,
            ),
        )

        rows = self.conn.execute(sql).fetchall()

        self.assertEqual(
            rows,
            [(2, "User", "Bob", 25, "Bob", 25, 101, "WORKS_AT", 2, 11, 2021, 2021, "Bravo")],
        )

    def test_type_aware_relational_with_chain_endpoint_output_executes_on_sqlite(
        self,
    ) -> None:
        self._seed_type_aware_graph()

        sql = cypherglot.to_sql(
            (
                "MATCH (a:User)-[r:KNOWS]->(b:User)-[s:WORKS_AT]->(c:Company) "
                "WITH b AS friend, c AS company, s AS rel "
                "RETURN startNode(rel) AS employee, endNode(rel) AS employer, company.name AS company_name "
                "ORDER BY company_name"
            ),
            schema_context=CompilerSchemaContext.type_aware(
                self.graph_schema,
            ),
        )

        rows = self.conn.execute(sql).fetchall()

        self.assertEqual(
            rows,
            [(2, "User", "Bob", 25, 11, "Company", "Bravo", "Bravo")],
        )

    def test_type_aware_relational_with_chain_entities_and_properties_execute_on_sqlite(
        self,
    ) -> None:
        self._seed_type_aware_graph()

        sql = cypherglot.to_sql(
            (
                "MATCH (a:User)-[r:KNOWS]->(b:User)-[s:WORKS_AT]->(c:Company) "
                "WITH b AS friend, s AS rel, c AS company "
                "RETURN friend AS employee, properties(friend) AS employee_props, "
                "rel AS job, properties(rel) AS job_props, company.name AS company_name "
                "ORDER BY company_name"
            ),
            schema_context=CompilerSchemaContext.type_aware(
                self.graph_schema,
            ),
        )

        rows = self.conn.execute(sql).fetchall()

        self.assertEqual(
            rows,
            [(2, "User", "Bob", 25, "Bob", 25, 101, "WORKS_AT", 2, 11, 2021, 2021, "Bravo")],
        )

    def test_type_aware_grouped_relational_chain_entities_and_properties_execute_on_sqlite(
        self,
    ) -> None:
        self._seed_type_aware_graph()

        sql = cypherglot.to_sql(
            (
                "MATCH (a:User)-[r:KNOWS]->(b:User)-[s:WORKS_AT]->(c:Company) "
                "RETURN b AS friend, properties(b) AS friend_props, "
                "s AS rel, properties(s) AS rel_props, count(s) AS total "
                "ORDER BY total DESC"
            ),
            schema_context=CompilerSchemaContext.type_aware(
                self.graph_schema,
            ),
        )

        rows = self.conn.execute(sql).fetchall()

        self.assertEqual(
            rows,
            [(2, "User", "Bob", 25, "Bob", 25, 101, "WORKS_AT", 2, 11, 2021, 2021, 1)],
        )

    def test_type_aware_grouped_relational_with_chain_entities_and_properties_execute_on_sqlite(
        self,
    ) -> None:
        self._seed_type_aware_graph()

        sql = cypherglot.to_sql(
            (
                "MATCH (a:User)-[r:KNOWS]->(b:User)-[s:WORKS_AT]->(c:Company) "
                "WITH b AS friend, s AS rel "
                "RETURN friend AS employee, properties(friend) AS employee_props, "
                "rel AS job, properties(rel) AS job_props, count(rel) AS total "
                "ORDER BY total DESC"
            ),
            schema_context=CompilerSchemaContext.type_aware(
                self.graph_schema,
            ),
        )

        rows = self.conn.execute(sql).fetchall()

        self.assertEqual(
            rows,
            [(2, "User", "Bob", 25, "Bob", 25, 101, "WORKS_AT", 2, 11, 2021, 2021, 1)],
        )

    def test_type_aware_relational_entity_output_executes_on_sqlite(self) -> None:
        self._seed_type_aware_graph()

        sql = cypherglot.to_sql(
            "MATCH (u:User) RETURN u AS user ORDER BY u.name",
            schema_context=CompilerSchemaContext.type_aware(
                self.graph_schema,
            ),
        )

        rows = self.conn.execute(sql).fetchall()

        self.assertEqual(
            rows,
            [
                (1, "User", "Alice", 30),
                (2, "User", "Bob", 25),
            ],
        )

    def test_type_aware_direct_grouped_aggregate_executes_on_sqlite(self) -> None:
        self._seed_type_aware_graph()

        sql = cypherglot.to_sql(
            (
                "MATCH (a:User)-[r:WORKS_AT]->(b:Company) "
                "RETURN b.name AS company, count(r) AS total "
                "ORDER BY total DESC, company"
            ),
            schema_context=CompilerSchemaContext.type_aware(self.graph_schema),
        )

        rows = self.conn.execute(sql).fetchall()

        self.assertEqual(rows, [("Acme", 1), ("Bravo", 1)])

    def test_type_aware_with_grouped_aggregate_executes_on_sqlite(self) -> None:
        self._seed_type_aware_graph()

        sql = cypherglot.to_sql(
            (
                "MATCH (a:User)-[r:WORKS_AT]->(b:Company) "
                "WITH b.name AS company, r AS rel "
                "RETURN company, count(rel) AS total "
                "ORDER BY total DESC, company"
            ),
            schema_context=CompilerSchemaContext.type_aware(self.graph_schema),
        )

        rows = self.conn.execute(sql).fetchall()

        self.assertEqual(rows, [("Acme", 1), ("Bravo", 1)])

    def test_type_aware_grouped_relational_entity_output_executes_on_sqlite(
        self,
    ) -> None:
        self._seed_type_aware_graph()

        sql = cypherglot.to_sql(
            (
                "MATCH (u:User) RETURN u AS user, count(u) AS total "
                "ORDER BY total DESC, u.name"
            ),
            schema_context=CompilerSchemaContext.type_aware(
                self.graph_schema,
            ),
        )

        rows = self.conn.execute(sql).fetchall()

        self.assertEqual(
            rows,
            [
                (1, "User", "Alice", 30, 1),
                (2, "User", "Bob", 25, 1),
            ],
        )

    def test_type_aware_graph_introspection_returns_execute_on_sqlite(self) -> None:
        self._seed_type_aware_graph()

        sql = cypherglot.to_sql(
            (
                "MATCH (a:User)-[r:WORKS_AT]->(b:Company) "
                "RETURN id(a) AS uid, type(r) AS rel_type, "
                "startNode(r).id AS start_id, endNode(r).id AS end_id "
                "ORDER BY uid"
            ),
            schema_context=CompilerSchemaContext.type_aware(self.graph_schema),
        )

        rows = self.conn.execute(sql).fetchall()

        self.assertEqual(rows, [(1, "WORKS_AT", 1, 10), (2, "WORKS_AT", 2, 11)])

    def test_type_aware_properties_labels_and_keys_execute_on_sqlite(self) -> None:
        self._seed_type_aware_graph()

        with self.assertRaisesRegex(
            ValueError,
            "relational output mode does not yet support whole-entity or introspection returns",
        ):
            cypherglot.to_sql(
                (
                    "MATCH (u:User) RETURN properties(u) AS props, labels(u) AS labels, "
                    "keys(u) AS user_keys ORDER BY u.name"
                ),
                schema_context=self._type_aware_schema_context(),
            )

    def test_type_aware_endpoint_entity_returns_execute_on_sqlite(self) -> None:
        self._seed_type_aware_graph()

        sql = cypherglot.to_sql(
            (
                "MATCH (a:User)-[r:WORKS_AT]->(b:Company) "
                "RETURN startNode(r) AS start, endNode(r) AS ending "
                "ORDER BY b.name"
            ),
            schema_context=self._type_aware_schema_context(),
        )

        rows = self.conn.execute(sql).fetchall()

        self.assertEqual(
            rows,
            [
                (1, "User", "Alice", 30, 10, "Company", "Acme"),
                (2, "User", "Bob", 25, 11, "Company", "Bravo"),
            ],
        )

    def test_type_aware_with_introspection_returns_execute_on_sqlite(self) -> None:
        self._seed_type_aware_graph()

        with self.assertRaisesRegex(
            ValueError,
            "relational output mode does not yet support whole-entity or introspection returns",
        ):
            cypherglot.to_sql(
                (
                    "MATCH (a:User)-[r:WORKS_AT]->(b:Company) "
                    "WITH a AS person, r AS rel, b AS company "
                    "RETURN properties(person) AS person_props, keys(rel) AS rel_keys, "
                    "startNode(rel).name AS start_name, endNode(rel).id AS company_id "
                    "ORDER BY start_name"
                ),
                schema_context=self._type_aware_schema_context(),
            )

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
