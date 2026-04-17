from __future__ import annotations

import unittest

import cypherglot
from cypherglot.schema import (
    CompilerSchemaContext,
    GraphSchema,
    NodeTypeSpec,
    PropertyField,
)


def _public_api_schema_context() -> CompilerSchemaContext:
    return CompilerSchemaContext.type_aware(
        GraphSchema(
            node_types=(
                NodeTypeSpec(
                    name="User",
                    properties=(
                        PropertyField("name", "string"),
                        PropertyField("age", "integer"),
                        PropertyField("score", "float"),
                        PropertyField("active", "boolean"),
                    ),
                ),
                NodeTypeSpec(
                    name="Company",
                    properties=(PropertyField("name", "string"),),
                ),
            ),
            edge_types=(
                cypherglot.EdgeTypeSpec(
                    name="KNOWS",
                    source_type="User",
                    target_type="User",
                    properties=(
                        PropertyField("note", "string"),
                        PropertyField("weight", "float"),
                        PropertyField("score", "float"),
                        PropertyField("active", "boolean"),
                    ),
                ),
                cypherglot.EdgeTypeSpec(
                    name="WORKS_AT",
                    source_type="User",
                    target_type="Company",
                    properties=(PropertyField("since", "integer"),),
                ),
            ),
        )
    )


def _postgres_write_schema_context() -> CompilerSchemaContext:
    return CompilerSchemaContext.type_aware(
        GraphSchema(
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
                cypherglot.EdgeTypeSpec(
                    name="KNOWS",
                    source_type="User",
                    target_type="User",
                    properties=(PropertyField("since", "integer"),),
                ),
                cypherglot.EdgeTypeSpec(
                    name="WORKS_AT",
                    source_type="User",
                    target_type="Company",
                    properties=(PropertyField("since", "integer"),),
                ),
                cypherglot.EdgeTypeSpec(
                    name="INTRODUCED",
                    source_type="User",
                    target_type="Person",
                ),
            ),
        )
    )


class RenderTests(unittest.TestCase):
    def test_render_cypher_program_text_renders_postgresql_write_programs(self) -> None:
        program = cypherglot.render_cypher_program_text(
            "MERGE (u:User {name: 'Alice'})",
            dialect="postgres",
            backend="postgresql",
            schema_context=CompilerSchemaContext.type_aware(
                GraphSchema(
                    node_types=(
                        NodeTypeSpec(
                            name="User",
                            properties=(PropertyField("name", "string"),),
                        ),
                    ),
                    edge_types=(),
                ),
            ),
        )

        self.assertEqual(len(program.steps), 1)
        stmt = program.steps[0]
        self.assertIsInstance(stmt, cypherglot.RenderedCypherStatement)
        assert isinstance(stmt, cypherglot.RenderedCypherStatement)
        self.assertEqual(
            stmt.sql,
            "INSERT INTO cg_node_user (name) SELECT 'Alice' FROM (SELECT 1) AS merge_guard WHERE NOT EXISTS(SELECT 1 FROM cg_node_user AS u WHERE u.name = 'Alice' LIMIT 1)",
        )
        self.assertEqual(stmt.bind_columns, ())

    def test_render_cypher_program_text_renders_postgresql_merge_relationship_program(
        self,
    ) -> None:
        program = cypherglot.render_cypher_program_text(
            (
                "MERGE (:User {name: 'Alice'})"
                "-[:WORKS_AT {since: 2020}]->"
                "(:Company {name: 'Acme'})"
            ),
            dialect="postgres",
            backend="postgresql",
            schema_context=CompilerSchemaContext.type_aware(
                GraphSchema(
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
                        cypherglot.EdgeTypeSpec(
                            name="WORKS_AT",
                            source_type="User",
                            target_type="Company",
                            properties=(PropertyField("since", "integer"),),
                        ),
                    ),
                ),
            ),
        )

        self.assertEqual(len(program.steps), 3)
        self.assertEqual(
            program.steps[0].sql,
            "INSERT INTO cg_node_user (name) SELECT 'Alice' FROM (SELECT 1) AS merge_guard WHERE NOT EXISTS(SELECT 1 FROM cg_node_user AS __humem_merge_left_node WHERE __humem_merge_left_node.name = 'Alice' LIMIT 1)",
        )
        self.assertEqual(
            program.steps[1].sql,
            "INSERT INTO cg_node_company (name) SELECT 'Acme' FROM (SELECT 1) AS merge_guard WHERE NOT EXISTS(SELECT 1 FROM cg_node_company AS __humem_merge_right_node WHERE __humem_merge_right_node.name = 'Acme' LIMIT 1)",
        )
        self.assertEqual(
            program.steps[2].sql,
            "INSERT INTO cg_edge_works_at (from_id, to_id, since) SELECT __humem_merge_left_node.id, __humem_merge_right_node.id, 2020 FROM cg_node_user AS __humem_merge_left_node, cg_node_company AS __humem_merge_right_node WHERE __humem_merge_left_node.name = 'Alice' AND __humem_merge_right_node.name = 'Acme' AND NOT EXISTS(SELECT 1 FROM cg_edge_works_at AS existing_merge_edge WHERE existing_merge_edge.from_id = __humem_merge_left_node.id AND existing_merge_edge.to_id = __humem_merge_right_node.id AND existing_merge_edge.since = 2020)",
        )

    def test_to_sql_renders_postgresql_relationship_set_with_update_from(
        self,
    ) -> None:
        sql = cypherglot.to_sql(
            "MATCH (u:User)-[r:WORKS_AT {since: 2020}]->(c:Company) SET r.since = 2021",
            dialect="postgres",
            backend="postgresql",
            schema_context=_public_api_schema_context(),
        )

        self.assertEqual(
            sql,
            "UPDATE cg_edge_works_at AS r SET since = 2021 FROM cg_node_user AS u, cg_node_company AS c WHERE u.id = r.from_id AND c.id = r.to_id AND r.since = 2020",
        )

    def test_to_sql_renders_postgresql_relationship_delete(self) -> None:
        sql = cypherglot.to_sql(
            "MATCH (u:User)-[r:WORKS_AT {since: 2020}]->(c:Company) DELETE r",
            dialect="postgres",
            backend="postgresql",
            schema_context=_public_api_schema_context(),
        )

        self.assertEqual(
            sql,
            "DELETE FROM cg_edge_works_at WHERE id IN (SELECT r.id FROM cg_edge_works_at AS r JOIN cg_node_user AS u ON u.id = r.from_id JOIN cg_node_company AS c ON c.id = r.to_id WHERE r.since = 2020)",
        )

    def test_render_cypher_program_text_renders_postgresql_match_create_loop(
        self,
    ) -> None:
        program = cypherglot.render_cypher_program_text(
            (
                "MATCH (u:User {name: 'Alice'}) "
                "CREATE (u)-[:WORKS_AT {since: 2024}]->(:Company {name: 'Cypher'})"
            ),
            dialect="postgres",
            backend="postgresql",
            schema_context=_postgres_write_schema_context(),
        )

        self.assertEqual(len(program.steps), 1)
        loop = program.steps[0]
        self.assertIsInstance(loop, cypherglot.RenderedCypherLoop)
        assert isinstance(loop, cypherglot.RenderedCypherLoop)
        self.assertEqual(
            loop.source,
            (
                "SELECT u.id AS match_node_id FROM cg_node_user AS u "
                "WHERE u.name = 'Alice'"
            ),
        )
        self.assertEqual(loop.row_bindings, ("match_node_id",))
        self.assertEqual(len(loop.body), 2)
        self.assertEqual(
            loop.body[0].sql,
            "INSERT INTO cg_node_company (name) VALUES ('Cypher') RETURNING id",
        )
        self.assertEqual(loop.body[0].bind_columns, ("created_node_id",))
        self.assertEqual(
            loop.body[1].sql,
            (
                "INSERT INTO cg_edge_works_at (from_id, to_id, since) VALUES "
                "(%(match_node_id)s, %(created_node_id)s, 2024)"
            ),
        )
        self.assertEqual(loop.body[1].bind_columns, ())

    def test_render_cypher_program_text_renders_postgresql_match_merge_loop(
        self,
    ) -> None:
        program = cypherglot.render_cypher_program_text(
            (
                "MATCH (u:User {name: 'Alice'}) "
                "MERGE (u)-[:WORKS_AT {since: 2024}]->(:Company {name: 'Cypher'})"
            ),
            dialect="postgres",
            backend="postgresql",
            schema_context=_postgres_write_schema_context(),
        )

        self.assertEqual(len(program.steps), 1)
        loop = program.steps[0]
        self.assertIsInstance(loop, cypherglot.RenderedCypherLoop)
        assert isinstance(loop, cypherglot.RenderedCypherLoop)
        self.assertEqual(
            loop.source,
            (
                "SELECT u.id AS match_node_id FROM cg_node_user AS u "
                "WHERE u.name = 'Alice' AND NOT EXISTS(SELECT 1 "
                "FROM cg_edge_works_at AS existing_merge_edge "
                "JOIN cg_node_company AS existing_merge_new_node "
                "ON existing_merge_new_node.id = existing_merge_edge.to_id "
                "WHERE existing_merge_edge.from_id = u.id "
                "AND existing_merge_edge.since = 2024 "
                "AND existing_merge_new_node.name = 'Cypher' LIMIT 1)"
            ),
        )
        self.assertEqual(loop.row_bindings, ("match_node_id",))
        self.assertEqual(
            loop.body[0].sql,
            "INSERT INTO cg_node_company (name) VALUES ('Cypher') RETURNING id",
        )
        self.assertEqual(loop.body[0].bind_columns, ("created_node_id",))
        self.assertEqual(
            loop.body[1].sql,
            (
                "INSERT INTO cg_edge_works_at (from_id, to_id, since) VALUES "
                "(%(match_node_id)s, %(created_node_id)s, 2024)"
            ),
        )
        self.assertEqual(loop.body[1].bind_columns, ())

    def test_render_cypher_program_text_renders_postgresql_traversal_create_loop(
        self,
    ) -> None:
        program = cypherglot.render_cypher_program_text(
            (
                "MATCH (a:User)-[r:KNOWS]->(b:User) "
                "CREATE (a)-[:INTRODUCED]->(:Person {name: 'Cara'})"
            ),
            dialect="postgres",
            backend="postgresql",
            schema_context=_postgres_write_schema_context(),
        )

        self.assertEqual(len(program.steps), 1)
        loop = program.steps[0]
        self.assertIsInstance(loop, cypherglot.RenderedCypherLoop)
        assert isinstance(loop, cypherglot.RenderedCypherLoop)
        self.assertEqual(
            loop.source,
            (
                "SELECT a.id AS match_node_id FROM cg_edge_knows AS r "
                "JOIN cg_node_user AS a ON a.id = r.from_id "
                "JOIN cg_node_user AS b ON b.id = r.to_id"
            ),
        )
        self.assertEqual(loop.row_bindings, ("match_node_id",))
        self.assertEqual(
            loop.body[0].sql,
            "INSERT INTO cg_node_person (name) VALUES ('Cara') RETURNING id",
        )
        self.assertEqual(loop.body[0].bind_columns, ("created_node_id",))
        self.assertEqual(
            loop.body[1].sql,
            (
                "INSERT INTO cg_edge_introduced (from_id, to_id) VALUES "
                "(%(match_node_id)s, %(created_node_id)s)"
            ),
        )
        self.assertEqual(loop.body[1].bind_columns, ())

    def test_to_sql_renders_type_aware_match_node_properties(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            "relational output mode does not yet support whole-entity or introspection returns",
        ):
            cypherglot.to_sql(
                "MATCH (u:User) RETURN properties(u) AS props, labels(u) AS labels",
                schema_context=CompilerSchemaContext.type_aware(
                    GraphSchema(
                        node_types=(
                            NodeTypeSpec(
                                name="User",
                                properties=(PropertyField("name", "string"),),
                            ),
                        ),
                        edge_types=(),
                    )
                ),
            )
    def test_to_sql_renders_type_aware_relational_output_mode_endpoint_entities(
        self,
    ) -> None:
        sql = cypherglot.to_sql(
            (
                "MATCH (a:User)-[r:WORKS_AT]->(b:Company) "
                "RETURN startNode(r) AS start, endNode(r) AS ending ORDER BY b.name"
            ),
            schema_context=CompilerSchemaContext.type_aware(
                GraphSchema(
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
                        cypherglot.EdgeTypeSpec(
                            name="WORKS_AT",
                            source_type="User",
                            target_type="Company",
                        ),
                    ),
                ),
            ),
        )

        self.assertEqual(
            sql,
            'SELECT a.id AS "start.id", \'User\' AS "start.label", '
            'a.name AS "start.name", a.age AS "start.age", '
            'b.id AS "ending.id", \'Company\' AS "ending.label", '
            'b.name AS "ending.name" '
            'FROM cg_edge_works_at AS r '
            'JOIN cg_node_user AS a ON a.id = r.from_id '
            'JOIN cg_node_company AS b ON b.id = r.to_id '
            'ORDER BY b.name ASC',
        )

    def test_to_sql_renders_type_aware_relational_output_mode_match_with_introspection(self) -> None:
        sql = cypherglot.to_sql(
            (
                "MATCH (a:User)-[r:WORKS_AT]->(b:Company) "
                "WITH a AS person, r AS rel, b AS company "
                "RETURN properties(person) AS person_props, startNode(rel).name AS start_name, endNode(rel) AS employer"
            ),
            schema_context=CompilerSchemaContext.type_aware(
                GraphSchema(
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
                        cypherglot.EdgeTypeSpec(
                            name="WORKS_AT",
                            source_type="User",
                            target_type="Company",
                        ),
                    ),
                ),
            ),
        )

        self.assertEqual(
            sql,
            'SELECT with_q."__cg_with_person_prop_name" AS "person_props.name", '
            'with_q."__cg_with_person_prop_name" AS "start_name", '
            'with_q."__cg_with_company_id" AS "employer.id", '
            '\'Company\' AS "employer.label", '
            'with_q."__cg_with_company_prop_name" AS "employer.name" '
            'FROM (SELECT a.id AS "__cg_with_person_id", '
            'a.name AS "__cg_with_person_prop_name", '
            'r.id AS "__cg_with_rel_id", '
            'r.from_id AS "__cg_with_rel_from_id", '
            'r.to_id AS "__cg_with_rel_to_id", '
            'b.id AS "__cg_with_company_id", '
            'b.name AS "__cg_with_company_prop_name" '
            'FROM cg_edge_works_at AS r '
            'JOIN cg_node_user AS a ON a.id = r.from_id '
            'JOIN cg_node_company AS b ON b.id = r.to_id) AS with_q',
        )

    def test_to_sql_renders_type_aware_match_with_aggregate(self) -> None:
        sql = cypherglot.to_sql(
            (
                "MATCH (u:User) WITH u.name AS name, u.age AS age "
                "RETURN name, max(age) AS top_age ORDER BY top_age DESC"
            ),
            schema_context=CompilerSchemaContext.type_aware(
                GraphSchema(
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
            ),
        )

        self.assertEqual(
            sql,
            'SELECT with_q."__cg_with_scalar_name" AS "name", '
            'MAX(with_q."__cg_with_scalar_age") AS "top_age" '
            'FROM (SELECT u.name AS "__cg_with_scalar_name", '
            'u.age AS "__cg_with_scalar_age" FROM cg_node_user AS u) AS with_q '
            'GROUP BY with_q."__cg_with_scalar_name" ORDER BY "top_age" DESC',
        )

    def test_to_sql_renders_type_aware_match_with_entity_field_aggregates(self) -> None:
        node_sql = cypherglot.to_sql(
            (
                "MATCH (u:User) WITH u AS person "
                "RETURN max(person.age) AS top_age ORDER BY top_age DESC"
            ),
            schema_context=CompilerSchemaContext.type_aware(
                GraphSchema(
                    node_types=(
                        NodeTypeSpec(
                            name="User",
                            properties=(PropertyField("age", "integer"),),
                        ),
                    ),
                    edge_types=(),
                )
            ),
        )
        relationship_sql = cypherglot.to_sql(
            (
                "MATCH (a:User)-[r:WORKS_AT]->(b:Company) "
                "WITH a AS person, r AS rel "
                "RETURN person.name AS name, avg(rel.since) AS mean_since "
                "ORDER BY mean_since DESC"
            ),
            schema_context=CompilerSchemaContext.type_aware(
                GraphSchema(
                    node_types=(
                        NodeTypeSpec(
                            name="User",
                            properties=(PropertyField("name", "string"),),
                        ),
                        NodeTypeSpec(name="Company"),
                    ),
                    edge_types=(
                        cypherglot.EdgeTypeSpec(
                            name="WORKS_AT",
                            source_type="User",
                            target_type="Company",
                            properties=(PropertyField("since", "integer"),),
                        ),
                    ),
                )
            ),
        )

        self.assertEqual(
            node_sql,
            'SELECT MAX(with_q."__cg_with_person_prop_age") AS "top_age" '
            'FROM (SELECT u.id AS "__cg_with_person_id", '
            'u.age AS "__cg_with_person_prop_age" '
            'FROM cg_node_user AS u) AS with_q ORDER BY "top_age" DESC',
        )
        self.assertEqual(
            relationship_sql,
            'SELECT with_q."__cg_with_person_prop_name" AS "name", '
            'AVG(with_q."__cg_with_rel_prop_since") AS "mean_since" '
            'FROM (SELECT a.id AS "__cg_with_person_id", '
            'a.name AS "__cg_with_person_prop_name", '
            'r.id AS "__cg_with_rel_id", '
            'r.from_id AS "__cg_with_rel_from_id", '
            'r.to_id AS "__cg_with_rel_to_id", '
            'r.since AS "__cg_with_rel_prop_since" '
            'FROM cg_edge_works_at AS r '
            'JOIN cg_node_user AS a ON a.id = r.from_id '
            'JOIN cg_node_company AS b ON b.id = r.to_id) AS with_q '
            'GROUP BY with_q."__cg_with_person_prop_name" ORDER BY "mean_since" DESC',
        )

    def test_to_sql_renders_type_aware_match_with_node_source_scalar_functions(self) -> None:
        sql = cypherglot.to_sql(
            (
                "MATCH (u:User) WITH u AS person, u.name AS name, u.age AS age "
                "RETURN lower(person.name) AS lower_name, size(name) AS name_len, "
                "toString(age) AS age_text, coalesce(person.name, 'unknown') AS display_name "
                "ORDER BY lower_name, name_len, age_text, display_name"
            ),
            schema_context=CompilerSchemaContext.type_aware(
                GraphSchema(
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
            ),
        )

        self.assertEqual(
            sql,
            'SELECT LOWER(with_q."__cg_with_person_prop_name") AS "lower_name", '
            'LENGTH(with_q."__cg_with_scalar_name") AS "name_len", '
            'CAST(with_q."__cg_with_scalar_age" AS TEXT) AS "age_text", '
            'COALESCE(with_q."__cg_with_person_prop_name", \'unknown\') AS "display_name" '
            'FROM (SELECT u.id AS "__cg_with_person_id", '
            'u.name AS "__cg_with_person_prop_name", '
            'u.age AS "__cg_with_person_prop_age", '
            'u.name AS "__cg_with_scalar_name", '
            'u.age AS "__cg_with_scalar_age" '
            'FROM cg_node_user AS u) AS with_q '
            'ORDER BY LOWER(with_q."__cg_with_person_prop_name") ASC, '
            'LENGTH(with_q."__cg_with_scalar_name") ASC, '
            'CAST(with_q."__cg_with_scalar_age" AS TEXT) ASC, '
            'COALESCE(with_q."__cg_with_person_prop_name", \'unknown\') ASC',
        )

    def test_to_sql_renders_type_aware_match_with_relationship_source_scalar_functions(self) -> None:
        sql = cypherglot.to_sql(
            (
                "MATCH (a:User)-[r:WORKS_AT]->(b:Company) "
                "WITH r AS rel, r.note AS note, r.since AS since "
                "RETURN lower(rel.note) AS lower_note, size(type(rel)) AS type_len, "
                "toString(since) AS since_text, coalesce(note, 'unknown') AS display_note "
                "ORDER BY lower_note, type_len, since_text, display_note"
            ),
            schema_context=CompilerSchemaContext.type_aware(
                GraphSchema(
                    node_types=(
                        NodeTypeSpec(name="User"),
                        NodeTypeSpec(name="Company"),
                    ),
                    edge_types=(
                        cypherglot.EdgeTypeSpec(
                            name="WORKS_AT",
                            source_type="User",
                            target_type="Company",
                            properties=(
                                PropertyField("note", "string"),
                                PropertyField("since", "integer"),
                            ),
                        ),
                    ),
                )
            ),
        )

        self.assertEqual(
            sql,
            'SELECT LOWER(with_q."__cg_with_rel_prop_note") AS "lower_note", '
            'LENGTH(\'WORKS_AT\') AS "type_len", '
            'CAST(with_q."__cg_with_scalar_since" AS TEXT) AS "since_text", '
            'COALESCE(with_q."__cg_with_scalar_note", \'unknown\') AS "display_note" '
            'FROM (SELECT r.id AS "__cg_with_rel_id", '
            'r.from_id AS "__cg_with_rel_from_id", '
            'r.to_id AS "__cg_with_rel_to_id", '
            'r.note AS "__cg_with_rel_prop_note", '
            'r.since AS "__cg_with_rel_prop_since", '
            'r.note AS "__cg_with_scalar_note", '
            'r.since AS "__cg_with_scalar_since" '
            'FROM cg_edge_works_at AS r '
            'JOIN cg_node_user AS a ON a.id = r.from_id '
            'JOIN cg_node_company AS b ON b.id = r.to_id) AS with_q '
            'ORDER BY LOWER(with_q."__cg_with_rel_prop_note") ASC, '
            'LENGTH(\'WORKS_AT\') ASC, '
            'CAST(with_q."__cg_with_scalar_since" AS TEXT) ASC, '
            'COALESCE(with_q."__cg_with_scalar_note", \'unknown\') ASC',
        )

    def test_to_sql_renders_type_aware_match_with_case_returns(self) -> None:
        node_sql = cypherglot.to_sql(
            (
                "MATCH (u:User) WITH u AS person "
                "RETURN CASE WHEN person.age >= 18 THEN person.name ELSE 'minor' END AS label "
                "ORDER BY label"
            ),
            schema_context=CompilerSchemaContext.type_aware(
                GraphSchema(
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
            ),
        )
        relationship_sql = cypherglot.to_sql(
            (
                "MATCH (a:User)-[r:WORKS_AT]->(b:Company) WITH r AS rel "
                "RETURN CASE WHEN rel.since >= 2020 THEN 'recent' ELSE 'legacy' END AS rel_class "
                "ORDER BY rel_class"
            ),
            schema_context=CompilerSchemaContext.type_aware(
                GraphSchema(
                    node_types=(
                        NodeTypeSpec(name="User"),
                        NodeTypeSpec(name="Company"),
                    ),
                    edge_types=(
                        cypherglot.EdgeTypeSpec(
                            name="WORKS_AT",
                            source_type="User",
                            target_type="Company",
                            properties=(PropertyField("since", "integer"),),
                        ),
                    ),
                )
            ),
        )

        self.assertEqual(
            node_sql,
            'SELECT CASE WHEN with_q."__cg_with_person_prop_age" >= 18 THEN '
            'with_q."__cg_with_person_prop_name" ELSE \'minor\' END AS "label" '
            'FROM (SELECT u.id AS "__cg_with_person_id", '
            'u.name AS "__cg_with_person_prop_name", '
            'u.age AS "__cg_with_person_prop_age" '
            'FROM cg_node_user AS u) AS with_q '
            'ORDER BY CASE WHEN with_q."__cg_with_person_prop_age" >= 18 THEN '
            'with_q."__cg_with_person_prop_name" ELSE \'minor\' END ASC',
        )
        self.assertEqual(
            relationship_sql,
            'SELECT CASE WHEN with_q."__cg_with_rel_prop_since" >= 2020 THEN '
            '\'recent\' ELSE \'legacy\' END AS "rel_class" '
            'FROM (SELECT r.id AS "__cg_with_rel_id", '
            'r.from_id AS "__cg_with_rel_from_id", '
            'r.to_id AS "__cg_with_rel_to_id", '
            'r.since AS "__cg_with_rel_prop_since" '
            'FROM cg_edge_works_at AS r '
            'JOIN cg_node_user AS a ON a.id = r.from_id '
            'JOIN cg_node_company AS b ON b.id = r.to_id) AS with_q '
            'ORDER BY CASE WHEN with_q."__cg_with_rel_prop_since" >= 2020 THEN '
            '\'recent\' ELSE \'legacy\' END ASC',
        )

    def test_to_sql_renders_type_aware_match_with_predicate_returns(self) -> None:
        node_sql = cypherglot.to_sql(
            (
                "MATCH (u:User) WITH u AS person, u.name AS name "
                "RETURN person.age >= 18 AS adult, name = 'Alice' AS is_alice, "
                "size(name) >= 3 AS long_name, person.name IS NOT NULL AS has_name "
                "ORDER BY adult, is_alice, long_name, has_name"
            ),
            schema_context=CompilerSchemaContext.type_aware(
                GraphSchema(
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
            ),
        )
        relationship_sql = cypherglot.to_sql(
            (
                "MATCH (a:User)-[r:WORKS_AT]->(b:Company) "
                "WITH r AS rel, r.note AS note "
                "RETURN rel.note STARTS WITH 'Al' AS has_prefix, "
                "note ENDS WITH 'ce' AS has_suffix, "
                "type(rel) = 'WORKS_AT' AS rel_matches, "
                "size(rel.note) >= 3 AS long_note "
                "ORDER BY has_prefix, has_suffix, rel_matches, long_note"
            ),
            schema_context=CompilerSchemaContext.type_aware(
                GraphSchema(
                    node_types=(
                        NodeTypeSpec(name="User"),
                        NodeTypeSpec(name="Company"),
                    ),
                    edge_types=(
                        cypherglot.EdgeTypeSpec(
                            name="WORKS_AT",
                            source_type="User",
                            target_type="Company",
                            properties=(PropertyField("note", "string"),),
                        ),
                    ),
                )
            ),
        )

        self.assertEqual(
            node_sql,
            'SELECT with_q."__cg_with_person_prop_age" >= 18 AS "adult", '
            'with_q."__cg_with_scalar_name" = \'Alice\' AS "is_alice", '
            'LENGTH(with_q."__cg_with_scalar_name") >= 3 AS "long_name", '
            'NOT with_q."__cg_with_person_prop_name" IS NULL AS "has_name" '
            'FROM (SELECT u.id AS "__cg_with_person_id", '
            'u.name AS "__cg_with_person_prop_name", '
            'u.age AS "__cg_with_person_prop_age", '
            'u.name AS "__cg_with_scalar_name" '
            'FROM cg_node_user AS u) AS with_q '
            'ORDER BY with_q."__cg_with_person_prop_age" >= 18 ASC, '
            'with_q."__cg_with_scalar_name" = \'Alice\' ASC, '
            'LENGTH(with_q."__cg_with_scalar_name") >= 3 ASC, '
            'NOT with_q."__cg_with_person_prop_name" IS NULL ASC',
        )
        self.assertEqual(
            relationship_sql,
            'SELECT SUBSTRING(with_q."__cg_with_rel_prop_note", 1, LENGTH(\'Al\')) = \'Al\' AS "has_prefix", '
            'LENGTH(with_q."__cg_with_scalar_note") >= LENGTH(\'ce\') AND '
            'SUBSTRING(with_q."__cg_with_scalar_note", LENGTH(with_q."__cg_with_scalar_note") - LENGTH(\'ce\') + 1) = \'ce\' AS "has_suffix", '
            '\'WORKS_AT\' = \'WORKS_AT\' AS "rel_matches", '
            'LENGTH(with_q."__cg_with_rel_prop_note") >= 3 AS "long_note" '
            'FROM (SELECT r.id AS "__cg_with_rel_id", '
            'r.from_id AS "__cg_with_rel_from_id", '
            'r.to_id AS "__cg_with_rel_to_id", '
            'r.note AS "__cg_with_rel_prop_note", '
            'r.note AS "__cg_with_scalar_note" '
            'FROM cg_edge_works_at AS r '
            'JOIN cg_node_user AS a ON a.id = r.from_id '
            'JOIN cg_node_company AS b ON b.id = r.to_id) AS with_q '
            'ORDER BY SUBSTRING(with_q."__cg_with_rel_prop_note", 1, LENGTH(\'Al\')) = \'Al\' ASC, '
            'LENGTH(with_q."__cg_with_scalar_note") >= LENGTH(\'ce\') AND '
            'SUBSTRING(with_q."__cg_with_scalar_note", LENGTH(with_q."__cg_with_scalar_note") - LENGTH(\'ce\') + 1) = \'ce\' ASC, '
            '\'WORKS_AT\' = \'WORKS_AT\' ASC, '
            'LENGTH(with_q."__cg_with_rel_prop_note") >= 3 ASC',
        )

    def test_to_sql_rejects_type_aware_relational_output_mode_match_with_broader_introspection_returns(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            "relational output mode does not yet support whole-entity or introspection returns",
        ):
            cypherglot.to_sql(
                (
                    "MATCH (a:User)-[r:WORKS_AT]->(b:Company) "
                    "WITH a AS person, r AS rel, b AS company "
                    "RETURN id(person) AS person_id, type(rel) AS rel_type, "
                    "properties(rel) AS rel_props, keys(person) AS person_keys, "
                    "startNode(rel) AS start_person, endNode(rel).id AS company_id "
                    "ORDER BY person_id, rel_type, rel_props, person_keys, start_person, company_id"
                ),
                schema_context=CompilerSchemaContext.type_aware(
                    GraphSchema(
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
                            cypherglot.EdgeTypeSpec(
                                name="WORKS_AT",
                                source_type="User",
                                target_type="Company",
                                properties=(PropertyField("since", "integer"),),
                            ),
                        ),
                    ),
                ),
            )

