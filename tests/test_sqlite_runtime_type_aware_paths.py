from __future__ import annotations

import cypherglot
from cypherglot.schema import CompilerSchemaContext

from tests._sqlite_runtime_type_aware_support import TypeAwareSQLiteRuntimeTestCase


class TypeAwareSQLitePathRuntimeTests(TypeAwareSQLiteRuntimeTestCase):
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
