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


class TypeAwareSQLiteTraversalRuntimeTests(TypeAwareSQLiteRuntimeTestCase):
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
        local_conn.executescript("\n".join(local_schema.ddl("sqlite")))
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
