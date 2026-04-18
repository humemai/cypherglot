from __future__ import annotations

import sqlite3

import cypherglot
from cypherglot.schema import (
    CompilerSchemaContext,
    EdgeTypeSpec,
    GraphSchema,
    NodeTypeSpec,
    PropertyField,
)

from tests._sqlite_runtime_type_aware_support import TypeAwareSQLiteRuntimeTestCase


class TypeAwareSQLiteRuntimeTests(TypeAwareSQLiteRuntimeTestCase):
    def test_type_aware_match_return_executes_on_sqlite(self) -> None:
        self._seed_type_aware_graph()

        sql = cypherglot.to_sql(
            "MATCH (u:User) RETURN u.name ORDER BY u.name",
            backend="sqlite",
            schema_context=self._type_aware_schema_context(),
        )

        rows = self.conn.execute(sql).fetchall()

        self.assertEqual(rows, [("Alice",), ("Bob",)])

    def test_type_aware_merge_node_program_executes_on_sqlite(self) -> None:
        self._seed_type_aware_graph()

        program = cypherglot.render_cypher_program_text(
            "MERGE (u:User {name: 'Alice'})",
            backend="sqlite",
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
            (
                "MERGE (:User {name: 'Alice'})"
                "-[:WORKS_AT {since: 2020}]->(:Company {name: 'Acme'})"
            ),
            backend="sqlite",
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
            backend="sqlite",
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
            backend="sqlite",
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
            backend="sqlite",
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
            backend="sqlite",
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
            (
                "MATCH (b:Company {name: 'Acme'}) "
                "CREATE (:User {name: 'Cara'})-[:WORKS_AT {since: 2024}]->(b)"
            ),
            backend="sqlite",
            schema_context=self._type_aware_schema_context(),
        )

        self._execute_program(program)

        users = self.conn.execute(
            "SELECT id, name, age FROM cg_node_user ORDER BY id"
        ).fetchall()
        works_at = self.conn.execute(
            (
                "SELECT from_id, to_id, since FROM cg_edge_works_at "
                "ORDER BY from_id, to_id, since"
            )
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
            backend="sqlite",
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
            backend="sqlite",
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
            backend="sqlite",
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
            (
                "MATCH (a:User)-[r:WORKS_AT]->(b:Company {name: 'Acme'}) "
                "CREATE (a)-[:WORKS_AT {since: 2024}]->(b)"
            ),
            backend="sqlite",
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
            (
                "MATCH (a:User)-[r:WORKS_AT]->(b:Company) "
                "WHERE r.since = 2020 CREATE (a)-[:WORKS_AT {since: 2024}]->(b)"
            ),
            backend="sqlite",
            schema_context=self._type_aware_schema_context(),
        )

        self.conn.execute(sql)
        self.conn.commit()

        works_at = self.conn.execute(
            "SELECT from_id, to_id, since FROM cg_edge_works_at ORDER BY id"
        ).fetchall()

        self.assertEqual(works_at, [(1, 10, 2020), (2, 11, 2021), (1, 10, 2024)])

    def test_type_aware_match_create_with_left_rel_filters_executes_on_sqlite(
        self,
    ) -> None:
        self._seed_type_aware_graph()

        sql = cypherglot.to_sql(
            (
                "MATCH (a:User)-[r:WORKS_AT]->(b:Company) "
                "WHERE a.name = 'Alice' AND r.since = 2020 "
                "CREATE (a)-[:WORKS_AT {since: 2024}]->(b)"
            ),
            backend="sqlite",
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
            (
                "MATCH (a:User {name: 'Alice'})-[r:WORKS_AT]->"
                "(b:Company {name: 'Acme'}) "
                "CREATE (a)-[:WORKS_AT {since: 2024}]->(b)"
            ),
            backend="sqlite",
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
            (
                "MATCH (a:User {name: 'Alice'})-[r:WORKS_AT]->"
                "(b:Company {name: 'Acme'}) WHERE r.since = 2020 "
                "CREATE (a)-[:WORKS_AT {since: 2024}]->(b)"
            ),
            backend="sqlite",
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
            backend="sqlite",
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
            backend="sqlite",
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
            backend="sqlite",
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
            backend="sqlite",
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
            (
                "MATCH (a:User)-[r:WORKS_AT]->(b:Company {name: 'Acme'}) "
                "MERGE (a)-[:WORKS_AT {since: 2024}]->(b)"
            ),
            backend="sqlite",
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
            (
                "MATCH (a:User)-[r:WORKS_AT]->(b:Company) "
                "WHERE r.since = 2020 MERGE (a)-[:WORKS_AT {since: 2024}]->(b)"
            ),
            backend="sqlite",
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
            (
                "MATCH (a:User {name: 'Alice'})-[r:WORKS_AT]->"
                "(b:Company {name: 'Acme'}) WHERE r.since = 2020 "
                "MERGE (a)-[:WORKS_AT {since: 2024}]->(b)"
            ),
            backend="sqlite",
            schema_context=CompilerSchemaContext.type_aware(self.graph_schema),
        )

        self.conn.execute(sql)
        self.conn.execute(sql)
        self.conn.commit()

        works_at = self.conn.execute(
            "SELECT from_id, to_id, since FROM cg_edge_works_at ORDER BY id"
        ).fetchall()

        self.assertEqual(works_at, [(1, 10, 2020), (2, 11, 2021), (1, 10, 2024)])

    def test_type_aware_match_merge_with_left_rel_filters_executes_on_sqlite(
        self,
    ) -> None:
        self._seed_type_aware_graph()

        sql = cypherglot.to_sql(
            (
                "MATCH (a:User)-[r:WORKS_AT]->(b:Company) "
                "WHERE a.name = 'Alice' AND r.since = 2020 "
                "MERGE (a)-[:WORKS_AT {since: 2024}]->(b)"
            ),
            backend="sqlite",
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
            (
                "MATCH (a:User {name: 'Alice'})-[r:WORKS_AT]->"
                "(b:Company {name: 'Acme'}) "
                "MERGE (a)-[:WORKS_AT {since: 2024}]->(b)"
            ),
            backend="sqlite",
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
            backend="sqlite",
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
            (
                "MATCH (a:User {name: 'Alice'}) "
                "MERGE (a)-[:INTRODUCED]->(:Person {name: 'Cara'})"
            ),
            backend="sqlite",
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
            backend="sqlite",
            schema_context=CompilerSchemaContext.type_aware(self.graph_schema),
        )
        filtered_right_program = cypherglot.render_cypher_program_text(
            (
                "MATCH (a:User)-[r:KNOWS]->(b:User {name: 'Bob'}) "
                "CREATE (a)-[:INTRODUCED]->(:Person {name: 'Erin'})"
            ),
            backend="sqlite",
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
            (
                "MATCH (b:Company {name: 'Acme'}) "
                "MERGE (:User {name: 'Cara'})-[:WORKS_AT {since: 2024}]->(b)"
            ),
            backend="sqlite",
            schema_context=CompilerSchemaContext.type_aware(self.graph_schema),
        )

        self._execute_program(program)
        self._execute_program(program)

        users = self.conn.execute(
            "SELECT id, name, age FROM cg_node_user ORDER BY id"
        ).fetchall()
        works_at = self.conn.execute(
            (
                "SELECT from_id, to_id, since FROM cg_edge_works_at "
                "ORDER BY from_id, to_id, since"
            )
        ).fetchall()

        self.assertEqual(users, [(1, "Alice", 30), (2, "Bob", 25), (3, "Cara", None)])
        self.assertEqual(works_at, [(1, 10, 2020), (2, 11, 2021), (3, 10, 2024)])

    def test_type_aware_match_set_node_sql_executes_on_sqlite(self) -> None:
        self._seed_type_aware_graph()

        sql = cypherglot.to_sql(
            "MATCH (u:User {name: 'Alice'}) SET u.age = 31",
            backend="sqlite",
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
            backend="sqlite",
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

    def test_type_aware_match_set_relationship_with_right_filter_executes_on_sqlite(
        self,
    ) -> None:
        self._seed_type_aware_graph()

        sql = cypherglot.to_sql(
            (
                "MATCH (a:User)-[r:WORKS_AT]->(b:Company {name: 'Acme'}) "
                "SET r.since = 2025"
            ),
            backend="sqlite",
            schema_context=CompilerSchemaContext.type_aware(self.graph_schema),
        )

        self.conn.execute(sql)
        self.conn.commit()

        works_at = self.conn.execute(
            "SELECT from_id, to_id, since FROM cg_edge_works_at ORDER BY id"
        ).fetchall()

        self.assertEqual(works_at, [(1, 10, 2025), (2, 11, 2021)])

    def test_type_aware_match_set_relationship_with_rel_filter_executes_on_sqlite(
        self,
    ) -> None:
        self._seed_type_aware_graph()

        sql = cypherglot.to_sql(
            (
                "MATCH (a:User)-[r:WORKS_AT]->(b:Company) "
                "WHERE r.since = 2020 SET r.since = 2025"
            ),
            backend="sqlite",
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
            (
                "MATCH (a:User)-[r:WORKS_AT]->(b:Company {name: 'Acme'}) "
                "WHERE r.since = 2020 SET r.since = 2025"
            ),
            backend="sqlite",
            schema_context=CompilerSchemaContext.type_aware(self.graph_schema),
        )

        self.conn.execute(sql)
        self.conn.commit()

        works_at = self.conn.execute(
            "SELECT from_id, to_id, since FROM cg_edge_works_at ORDER BY id"
        ).fetchall()

        self.assertEqual(works_at, [(1, 10, 2025), (2, 11, 2021)])

    def test_type_aware_match_set_relationship_with_left_rel_filters_executes_on_sqlite(
        self,
    ) -> None:
        self._seed_type_aware_graph()

        sql = cypherglot.to_sql(
            (
                "MATCH (a:User)-[r:WORKS_AT]->(b:Company) "
                "WHERE a.name = 'Alice' AND r.since = 2020 SET r.since = 2025"
            ),
            backend="sqlite",
            schema_context=CompilerSchemaContext.type_aware(self.graph_schema),
        )

        self.conn.execute(sql)
        self.conn.commit()

        works_at = self.conn.execute(
            "SELECT from_id, to_id, since FROM cg_edge_works_at ORDER BY id"
        ).fetchall()

        self.assertEqual(works_at, [(1, 10, 2025), (2, 11, 2021)])

    def test_type_aware_match_set_relationship_with_both_filters_executes_on_sqlite(
        self,
    ) -> None:
        self._seed_type_aware_graph()

        sql = cypherglot.to_sql(
            (
                "MATCH (a:User {name: 'Alice'})-[r:WORKS_AT]->"
                "(b:Company {name: 'Acme'}) SET r.since = 2025"
            ),
            backend="sqlite",
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
            (
                "MATCH (a:User {name: 'Alice'})-[r:WORKS_AT]->"
                "(b:Company {name: 'Acme'}) WHERE r.since = 2020 "
                "SET r.since = 2025"
            ),
            backend="sqlite",
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
        local_conn.executescript("\n".join(local_schema.ddl("sqlite")))
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
            (
                "MATCH (a:User)-[r:KNOWS]->(a:User) "
                "WHERE a.name = 'Alice' SET r.since = 2021"
            ),
            backend="sqlite",
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
            backend="sqlite",
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
            backend="sqlite",
            schema_context=CompilerSchemaContext.type_aware(self.graph_schema),
        )

        self.conn.execute(sql)
        self.conn.commit()

        works_at = self.conn.execute(
            "SELECT from_id, to_id, since FROM cg_edge_works_at ORDER BY id"
        ).fetchall()

        self.assertEqual(works_at, [(2, 11, 2021)])

    def test_type_aware_match_delete_relationship_with_right_filter_executes_on_sqlite(
        self,
    ) -> None:
        self._seed_type_aware_graph()

        sql = cypherglot.to_sql(
            "MATCH (a:User)-[r:WORKS_AT]->(b:Company {name: 'Acme'}) DELETE r",
            backend="sqlite",
            schema_context=CompilerSchemaContext.type_aware(self.graph_schema),
        )

        self.conn.execute(sql)
        self.conn.commit()

        works_at = self.conn.execute(
            "SELECT from_id, to_id, since FROM cg_edge_works_at ORDER BY id"
        ).fetchall()

        self.assertEqual(works_at, [(2, 11, 2021)])

    def test_type_aware_match_delete_relationship_with_rel_filter_executes_on_sqlite(
        self,
    ) -> None:
        self._seed_type_aware_graph()

        sql = cypherglot.to_sql(
            "MATCH (a:User)-[r:WORKS_AT]->(b:Company) WHERE r.since = 2020 DELETE r",
            backend="sqlite",
            schema_context=CompilerSchemaContext.type_aware(self.graph_schema),
        )

        self.conn.execute(sql)
        self.conn.commit()

        works_at = self.conn.execute(
            "SELECT from_id, to_id, since FROM cg_edge_works_at ORDER BY id"
        ).fetchall()

        self.assertEqual(works_at, [(2, 11, 2021)])

    def test_type_aware_match_delete_rel_with_combined_filters_executes_on_sqlite(
        self,
    ) -> None:
        self._seed_type_aware_graph()

        sql = cypherglot.to_sql(
            (
                "MATCH (a:User)-[r:WORKS_AT]->(b:Company {name: 'Acme'}) "
                "WHERE r.since = 2020 DELETE r"
            ),
            backend="sqlite",
            schema_context=CompilerSchemaContext.type_aware(self.graph_schema),
        )

        self.conn.execute(sql)
        self.conn.commit()

        works_at = self.conn.execute(
            "SELECT from_id, to_id, since FROM cg_edge_works_at ORDER BY id"
        ).fetchall()

        self.assertEqual(works_at, [(2, 11, 2021)])

    def test_type_aware_match_delete_rel_with_left_filters_executes_on_sqlite(
        self,
    ) -> None:
        self._seed_type_aware_graph()

        sql = cypherglot.to_sql(
            (
                "MATCH (a:User)-[r:WORKS_AT]->(b:Company) "
                "WHERE a.name = 'Alice' AND r.since = 2020 DELETE r"
            ),
            backend="sqlite",
            schema_context=CompilerSchemaContext.type_aware(self.graph_schema),
        )

        self.conn.execute(sql)
        self.conn.commit()

        works_at = self.conn.execute(
            "SELECT from_id, to_id, since FROM cg_edge_works_at ORDER BY id"
        ).fetchall()

        self.assertEqual(works_at, [(2, 11, 2021)])

    def test_type_aware_match_delete_relationship_with_both_filters_executes_on_sqlite(
        self,
    ) -> None:
        self._seed_type_aware_graph()

        sql = cypherglot.to_sql(
            (
                "MATCH (a:User {name: 'Alice'})-[r:WORKS_AT]->"
                "(b:Company {name: 'Acme'}) DELETE r"
            ),
            backend="sqlite",
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
            (
                "MATCH (a:User {name: 'Alice'})-[r:WORKS_AT]->"
                "(b:Company {name: 'Acme'}) WHERE r.since = 2020 DELETE r"
            ),
            backend="sqlite",
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
            backend="sqlite",
            schema_context=CompilerSchemaContext.type_aware(self.graph_schema),
        )

        self.conn.execute(sql)
        self.conn.commit()

        knows = self.conn.execute(
            "SELECT from_id, to_id FROM cg_edge_knows ORDER BY from_id, to_id"
        ).fetchall()

        self.assertEqual(knows, [(1, 2)])
