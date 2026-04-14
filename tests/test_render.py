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


class RenderTests(unittest.TestCase):
    def test_to_sql_renders_type_aware_relational_output_mode_scalar_returns(self) -> None:
        sql = cypherglot.to_sql(
            "MATCH (u:User) RETURN id(u) AS user_id, u.name AS name ORDER BY user_id, name",
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

        self.assertEqual(
            sql,
            'SELECT u.id AS "user_id", u.name AS "name" FROM cg_node_user AS u '
            'ORDER BY u.id ASC, u.name ASC',
        )

    def test_to_sql_rejects_type_aware_relational_output_mode_json_constructor_returns(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            "relational output mode does not yet support whole-entity or introspection returns",
        ):
            cypherglot.to_sql(
                "MATCH (u:User) RETURN labels(u) AS labels, keys(u) AS keys",
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

    def test_to_sql_renders_type_aware_relational_output_mode_direct_node_entity_and_properties(self) -> None:
        sql = cypherglot.to_sql(
            (
                "MATCH (u:User) RETURN u AS user, properties(u) AS props, u.name AS name "
                "ORDER BY name"
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
                ),
            ),
        )

        self.assertEqual(
            sql,
            'SELECT u.id AS "user.id", \'User\' AS "user.label", '
            'u.name AS "user.name", u.age AS "user.age", '
            'u.name AS "props.name", u.age AS "props.age", '
            'u.name AS "name" FROM cg_node_user AS u ORDER BY u.name ASC',
        )

    def test_to_sql_rejects_type_aware_relational_output_mode_with_labels_and_keys(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            "relational output mode does not yet support whole-entity or introspection returns",
        ):
            cypherglot.to_sql(
                (
                    "MATCH (a:User)-[r:WORKS_AT]->(b:Company) "
                    "WITH a AS person, r AS rel "
                    "RETURN labels(person) AS labels, keys(rel) AS keys"
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
                            ),
                        ),
                    ),
                ),
            )

    def test_to_sql_rejects_type_aware_relational_output_mode_direct_chain_labels_and_keys(
        self,
    ) -> None:
        with self.assertRaisesRegex(
            ValueError,
            "relational output mode does not yet support whole-entity or introspection returns",
        ):
            cypherglot.to_sql(
                (
                    "MATCH (a:User)-[r:KNOWS]->(b:User)-[s:WORKS_AT]->(c:Company) "
                    "RETURN labels(b) AS friend_labels, keys(s) AS rel_keys"
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
                                name="KNOWS",
                                source_type="User",
                                target_type="User",
                            ),
                            cypherglot.EdgeTypeSpec(
                                name="WORKS_AT",
                                source_type="User",
                                target_type="Company",
                            ),
                        ),
                    ),
                ),
            )

    def test_to_sql_rejects_type_aware_relational_output_mode_with_chain_labels_and_keys(
        self,
    ) -> None:
        with self.assertRaisesRegex(
            ValueError,
            "relational output mode does not yet support whole-entity or introspection returns",
        ):
            cypherglot.to_sql(
                (
                    "MATCH (a:User)-[r:KNOWS]->(b:User)-[s:WORKS_AT]->(c:Company) "
                    "WITH b AS friend, s AS rel "
                    "RETURN labels(friend) AS friend_labels, keys(rel) AS rel_keys"
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
                                name="KNOWS",
                                source_type="User",
                                target_type="User",
                            ),
                            cypherglot.EdgeTypeSpec(
                                name="WORKS_AT",
                                source_type="User",
                                target_type="Company",
                            ),
                        ),
                    ),
                ),
            )

    def test_to_sql_renders_type_aware_relational_output_mode_direct_relationship_entity_and_properties(self) -> None:
        sql = cypherglot.to_sql(
            (
                "MATCH (a:User)-[r:WORKS_AT]->(b:Company) "
                "RETURN r AS rel, properties(r) AS props, b.name AS company ORDER BY company"
            ),
            schema_context=CompilerSchemaContext.type_aware(
                GraphSchema(
                    node_types=(
                        NodeTypeSpec(name="User"),
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

        self.assertEqual(
            sql,
            'SELECT r.id AS "rel.id", \'WORKS_AT\' AS "rel.type", '
            'r.from_id AS "rel.from_id", r.to_id AS "rel.to_id", '
            'r.since AS "rel.since", r.since AS "props.since", '
            'b.name AS "company" FROM cg_edge_works_at AS r '
            'JOIN cg_node_user AS a ON a.id = r.from_id '
            'JOIN cg_node_company AS b ON b.id = r.to_id '
            'ORDER BY b.name ASC',
        )

    def test_to_sql_renders_type_aware_relational_output_mode_direct_endpoint_entities(self) -> None:
        sql = cypherglot.to_sql(
            (
                "MATCH (a:User)-[r:WORKS_AT]->(b:Company) "
                "RETURN startNode(r) AS start, endNode(r) AS ending, r.since AS since "
                "ORDER BY since"
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

        self.assertEqual(
            sql,
            'SELECT a.id AS "start.id", \'User\' AS "start.label", '
            'a.name AS "start.name", '
            'b.id AS "ending.id", \'Company\' AS "ending.label", '
            'b.name AS "ending.name", '
            'r.since AS "since" FROM cg_edge_works_at AS r '
            'JOIN cg_node_user AS a ON a.id = r.from_id '
            'JOIN cg_node_company AS b ON b.id = r.to_id '
            'ORDER BY r.since ASC',
        )

    def test_to_sql_renders_type_aware_relational_output_mode_orders_by_projected_entity_aliases(
        self,
    ) -> None:
        sql = cypherglot.to_sql(
            (
                "MATCH (a:User)-[r:WORKS_AT]->(b:Company) "
                "RETURN startNode(r) AS start, endNode(r) AS ending, r AS rel "
                "ORDER BY start, ending, rel"
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

        self.assertEqual(
            sql,
            'SELECT a.id AS "start.id", \'User\' AS "start.label", '
            'a.name AS "start.name", '
            'b.id AS "ending.id", \'Company\' AS "ending.label", '
            'b.name AS "ending.name", '
            'r.id AS "rel.id", \'WORKS_AT\' AS "rel.type", '
            'r.from_id AS "rel.from_id", r.to_id AS "rel.to_id", '
            'r.since AS "rel.since" FROM cg_edge_works_at AS r '
            'JOIN cg_node_user AS a ON a.id = r.from_id '
            'JOIN cg_node_company AS b ON b.id = r.to_id '
            'ORDER BY a.id ASC, \'User\' ASC, a.name ASC, '
            'b.id ASC, \'Company\' ASC, b.name ASC, '
            'r.id ASC, \'WORKS_AT\' ASC, r.from_id ASC, r.to_id ASC, r.since ASC',
        )

    def test_to_sql_renders_type_aware_relational_output_mode_grouped_direct_node_entity(self) -> None:
        sql = cypherglot.to_sql(
            "MATCH (u:User) RETURN u AS user, count(u) AS total ORDER BY total DESC",
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

        self.assertEqual(
            sql,
            'SELECT u.id AS "user.id", \'User\' AS "user.label", '
            'u.name AS "user.name", COUNT(u.id) AS "total" '
            'FROM cg_node_user AS u '
            'GROUP BY u.id, \'User\', u.name ORDER BY "total" DESC',
        )

    def test_to_sql_renders_type_aware_relational_output_mode_grouped_direct_relationship_entity(self) -> None:
        sql = cypherglot.to_sql(
            (
                "MATCH (a:User)-[r:WORKS_AT]->(b:Company) "
                "RETURN r AS rel, count(r) AS total ORDER BY total DESC"
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
                ),
            ),
        )

        self.assertEqual(
            sql,
            'SELECT r.id AS "rel.id", \'WORKS_AT\' AS "rel.type", '
            'r.from_id AS "rel.from_id", r.to_id AS "rel.to_id", '
            'r.since AS "rel.since", COUNT(r.id) AS "total" '
            'FROM cg_edge_works_at AS r '
            'JOIN cg_node_user AS a ON a.id = r.from_id '
            'JOIN cg_node_company AS b ON b.id = r.to_id '
            'GROUP BY r.id, \'WORKS_AT\', r.from_id, r.to_id, r.since '
            'ORDER BY "total" DESC',
        )

    def test_to_sql_renders_type_aware_relational_output_mode_grouped_direct_endpoint_entities(self) -> None:
        sql = cypherglot.to_sql(
            (
                "MATCH (a:User)-[r:WORKS_AT]->(b:Company) "
                "RETURN startNode(r) AS start, endNode(r) AS ending, count(r) AS total "
                "ORDER BY total DESC"
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
            'SELECT a.id AS "start.id", \'User\' AS "start.label", '
            'a.name AS "start.name", b.id AS "ending.id", '
            '\'Company\' AS "ending.label", b.name AS "ending.name", '
            'COUNT(r.id) AS "total" FROM cg_edge_works_at AS r '
            'JOIN cg_node_user AS a ON a.id = r.from_id '
            'JOIN cg_node_company AS b ON b.id = r.to_id '
            'GROUP BY a.id, \'User\', a.name, b.id, \'Company\', b.name '
            'ORDER BY "total" DESC',
        )

    def test_to_sql_renders_type_aware_relational_output_mode_with_node_entity_and_properties(self) -> None:
        sql = cypherglot.to_sql(
            (
                "MATCH (u:User) WITH u AS person "
                "RETURN person AS user, properties(person) AS props, person.name AS name "
                "ORDER BY name"
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
                ),
            ),
        )

        self.assertEqual(
            sql,
            'SELECT with_q."__cg_with_person_id" AS "user.id", \'User\' AS "user.label", '
            'with_q."__cg_with_person_prop_name" AS "user.name", '
            'with_q."__cg_with_person_prop_age" AS "user.age", '
            'with_q."__cg_with_person_prop_name" AS "props.name", '
            'with_q."__cg_with_person_prop_age" AS "props.age", '
            'with_q."__cg_with_person_prop_name" AS "name" '
            'FROM (SELECT u.id AS "__cg_with_person_id", '
            'u.name AS "__cg_with_person_prop_name", '
            'u.age AS "__cg_with_person_prop_age" '
            'FROM cg_node_user AS u) AS with_q '
            'ORDER BY with_q."__cg_with_person_prop_name" ASC',
        )

    def test_to_sql_renders_type_aware_relational_output_mode_with_relationship_entity_and_properties(self) -> None:
        sql = cypherglot.to_sql(
            (
                "MATCH (a:User)-[r:WORKS_AT]->(b:Company) "
                "WITH r AS rel, b.name AS company "
                "RETURN rel AS edge, properties(rel) AS props, company ORDER BY company"
            ),
            schema_context=CompilerSchemaContext.type_aware(
                GraphSchema(
                    node_types=(
                        NodeTypeSpec(name="User"),
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

        self.assertEqual(
            sql,
            'SELECT with_q."__cg_with_rel_id" AS "edge.id", \'WORKS_AT\' AS "edge.type", '
            'with_q."__cg_with_rel_from_id" AS "edge.from_id", '
            'with_q."__cg_with_rel_to_id" AS "edge.to_id", '
            'with_q."__cg_with_rel_prop_since" AS "edge.since", '
            'with_q."__cg_with_rel_prop_since" AS "props.since", '
            'with_q."__cg_with_scalar_company" AS "company" '
            'FROM (SELECT r.id AS "__cg_with_rel_id", '
            'r.from_id AS "__cg_with_rel_from_id", '
            'r.to_id AS "__cg_with_rel_to_id", '
            'r.since AS "__cg_with_rel_prop_since", '
            'b.name AS "__cg_with_scalar_company" '
            'FROM cg_edge_works_at AS r '
            'JOIN cg_node_user AS a ON a.id = r.from_id '
            'JOIN cg_node_company AS b ON b.id = r.to_id) AS with_q '
            'ORDER BY with_q."__cg_with_scalar_company" ASC',
        )

    def test_to_sql_renders_type_aware_relational_output_mode_with_endpoint_entities(self) -> None:
        sql = cypherglot.to_sql(
            (
                "MATCH (a:User)-[r:WORKS_AT]->(b:Company) "
                "WITH a AS person, r AS rel, b AS company "
                "RETURN startNode(rel) AS start_person, endNode(rel) AS employer, rel.since AS since "
                "ORDER BY since"
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

        self.assertEqual(
            sql,
            'SELECT with_q."__cg_with_person_id" AS "start_person.id", '
            '\'User\' AS "start_person.label", '
            'with_q."__cg_with_person_prop_name" AS "start_person.name", '
            'with_q."__cg_with_company_id" AS "employer.id", '
            '\'Company\' AS "employer.label", '
            'with_q."__cg_with_company_prop_name" AS "employer.name", '
            'with_q."__cg_with_rel_prop_since" AS "since" '
            'FROM (SELECT a.id AS "__cg_with_person_id", '
            'a.name AS "__cg_with_person_prop_name", '
            'r.id AS "__cg_with_rel_id", '
            'r.from_id AS "__cg_with_rel_from_id", '
            'r.to_id AS "__cg_with_rel_to_id", '
            'r.since AS "__cg_with_rel_prop_since", '
            'b.id AS "__cg_with_company_id", '
            'b.name AS "__cg_with_company_prop_name" '
            'FROM cg_edge_works_at AS r '
            'JOIN cg_node_user AS a ON a.id = r.from_id '
            'JOIN cg_node_company AS b ON b.id = r.to_id) AS with_q '
            'ORDER BY with_q."__cg_with_rel_prop_since" ASC',
        )

    def test_to_sql_renders_type_aware_relational_output_mode_orders_by_with_projected_entity_aliases(
        self,
    ) -> None:
        sql = cypherglot.to_sql(
            (
                "MATCH (a:User)-[r:WORKS_AT]->(b:Company) "
                "WITH a AS person, r AS rel, b AS company "
                "RETURN startNode(rel) AS start, endNode(rel) AS ending, rel AS edge "
                "ORDER BY start, ending, edge"
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

        self.assertEqual(
            sql,
            'SELECT with_q."__cg_with_person_id" AS "start.id", '
            '\'User\' AS "start.label", '
            'with_q."__cg_with_person_prop_name" AS "start.name", '
            'with_q."__cg_with_company_id" AS "ending.id", '
            '\'Company\' AS "ending.label", '
            'with_q."__cg_with_company_prop_name" AS "ending.name", '
            'with_q."__cg_with_rel_id" AS "edge.id", '
            '\'WORKS_AT\' AS "edge.type", '
            'with_q."__cg_with_rel_from_id" AS "edge.from_id", '
            'with_q."__cg_with_rel_to_id" AS "edge.to_id", '
            'with_q."__cg_with_rel_prop_since" AS "edge.since" '
            'FROM (SELECT a.id AS "__cg_with_person_id", '
            'a.name AS "__cg_with_person_prop_name", '
            'r.id AS "__cg_with_rel_id", '
            'r.from_id AS "__cg_with_rel_from_id", '
            'r.to_id AS "__cg_with_rel_to_id", '
            'r.since AS "__cg_with_rel_prop_since", '
            'b.id AS "__cg_with_company_id", '
            'b.name AS "__cg_with_company_prop_name" '
            'FROM cg_edge_works_at AS r '
            'JOIN cg_node_user AS a ON a.id = r.from_id '
            'JOIN cg_node_company AS b ON b.id = r.to_id) AS with_q '
            'ORDER BY with_q."__cg_with_person_id" ASC, \'User\' ASC, '
            'with_q."__cg_with_person_prop_name" ASC, '
            'with_q."__cg_with_company_id" ASC, \'Company\' ASC, '
            'with_q."__cg_with_company_prop_name" ASC, '
            'with_q."__cg_with_rel_id" ASC, \'WORKS_AT\' ASC, '
            'with_q."__cg_with_rel_from_id" ASC, '
            'with_q."__cg_with_rel_to_id" ASC, '
            'with_q."__cg_with_rel_prop_since" ASC',
        )

    def test_to_sql_renders_type_aware_relational_output_mode_grouped_with_node_entity(self) -> None:
        sql = cypherglot.to_sql(
            (
                "MATCH (u:User) WITH u AS person "
                "RETURN person AS user, count(person) AS total ORDER BY total DESC"
            ),
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

        self.assertEqual(
            sql,
            "SELECT with_q.\"__cg_with_person_id\" AS \"user.id\", 'User' AS \"user.label\", "
            "with_q.\"__cg_with_person_prop_name\" AS \"user.name\", "
            "COUNT(with_q.\"__cg_with_person_id\") AS \"total\" "
            "FROM (SELECT u.id AS \"__cg_with_person_id\", "
            "u.name AS \"__cg_with_person_prop_name\" "
            "FROM cg_node_user AS u) AS with_q "
            "GROUP BY with_q.\"__cg_with_person_id\", 'User', "
            "with_q.\"__cg_with_person_prop_name\" ORDER BY \"total\" DESC",
        )

    def test_to_sql_renders_type_aware_relational_output_mode_grouped_with_relationship_entity(self) -> None:
        sql = cypherglot.to_sql(
            (
                "MATCH (a:User)-[r:WORKS_AT]->(b:Company) "
                "WITH r AS rel "
                "RETURN rel AS edge, count(rel) AS total ORDER BY total DESC"
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
                ),
            ),
        )

        self.assertEqual(
            sql,
            "SELECT with_q.\"__cg_with_rel_id\" AS \"edge.id\", 'WORKS_AT' AS \"edge.type\", "
            "with_q.\"__cg_with_rel_from_id\" AS \"edge.from_id\", "
            "with_q.\"__cg_with_rel_to_id\" AS \"edge.to_id\", "
            "with_q.\"__cg_with_rel_prop_since\" AS \"edge.since\", "
            "COUNT(with_q.\"__cg_with_rel_id\") AS \"total\" "
            "FROM (SELECT r.id AS \"__cg_with_rel_id\", "
            "r.from_id AS \"__cg_with_rel_from_id\", "
            "r.to_id AS \"__cg_with_rel_to_id\", "
            "r.since AS \"__cg_with_rel_prop_since\" "
            "FROM cg_edge_works_at AS r "
            "JOIN cg_node_user AS a ON a.id = r.from_id "
            "JOIN cg_node_company AS b ON b.id = r.to_id) AS with_q "
            "GROUP BY with_q.\"__cg_with_rel_id\", 'WORKS_AT', "
            "with_q.\"__cg_with_rel_from_id\", with_q.\"__cg_with_rel_to_id\", "
            "with_q.\"__cg_with_rel_prop_since\" ORDER BY \"total\" DESC",
        )

    def test_to_sql_renders_type_aware_match_node_field_access(self) -> None:
        sql = cypherglot.to_sql(
            "MATCH (u:User) WHERE u.name = $name RETURN u.name LIMIT 1",
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

        self.assertEqual(
            sql,
            'SELECT u.name AS "u.name" FROM cg_node_user AS u '
            'WHERE u.name = :name LIMIT 1',
        )

    def test_to_sql_renders_type_aware_one_hop_match(self) -> None:
        sql = cypherglot.to_sql(
            (
                "MATCH (a:User)-[r:WORKS_AT]->(b:Company) "
                "WHERE r.since >= $since RETURN b.name AS company"
            ),
            schema_context=CompilerSchemaContext.type_aware(
                GraphSchema(
                    node_types=(
                        NodeTypeSpec(name="User"),
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
                )
            ),
        )

        self.assertEqual(
            sql,
            'SELECT b.name AS "company" FROM cg_edge_works_at AS r '
            'JOIN cg_node_user AS a ON a.id = r.from_id '
            'JOIN cg_node_company AS b ON b.id = r.to_id '
            'WHERE r.since >= :since',
        )

    def test_to_sql_renders_type_aware_direct_node_aggregates(self) -> None:
        sql = cypherglot.to_sql(
            "MATCH (u:User) RETURN u.name AS name, avg(u.score) AS mean ORDER BY mean DESC",
            schema_context=CompilerSchemaContext.type_aware(
                GraphSchema(
                    node_types=(
                        NodeTypeSpec(
                            name="User",
                            properties=(
                                PropertyField("name", "string"),
                                PropertyField("score", "float"),
                            ),
                        ),
                    ),
                    edge_types=(),
                )
            ),
        )

        self.assertEqual(
            sql,
            'SELECT u.name AS "name", AVG(u.score) AS "mean" '
            'FROM cg_node_user AS u GROUP BY u.name ORDER BY "mean" DESC',
        )

    def test_to_sql_renders_type_aware_direct_node_scalar_functions(self) -> None:
        sql = cypherglot.to_sql(
            (
                "MATCH (u:User) RETURN lower(u.name) AS lower_name, "
                "size(u.name) AS name_len, toString(u.age) AS age_text, "
                "coalesce(u.name, 'unknown') AS display_name "
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
            'SELECT LOWER(u.name) AS "lower_name", LENGTH(u.name) AS "name_len", '
            'CAST(u.age AS TEXT) AS "age_text", COALESCE(u.name, \'unknown\') AS "display_name" '
            'FROM cg_node_user AS u ORDER BY LOWER(u.name) ASC, LENGTH(u.name) ASC, '
            'CAST(u.age AS TEXT) ASC, COALESCE(u.name, \'unknown\') ASC',
        )

    def test_to_sql_renders_type_aware_direct_relationship_aggregates(self) -> None:
        sql = cypherglot.to_sql(
            (
                "MATCH (a:User)-[r:WORKS_AT]->(b:Company) "
                "RETURN a.name AS name, count(r) AS total ORDER BY total DESC"
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
                        ),
                    ),
                )
            ),
        )

        self.assertEqual(
            sql,
            'SELECT a.name AS "name", COUNT(r.id) AS "total" '
            'FROM cg_edge_works_at AS r '
            'JOIN cg_node_user AS a ON a.id = r.from_id '
            'JOIN cg_node_company AS b ON b.id = r.to_id '
            'GROUP BY a.name ORDER BY "total" DESC',
        )

    def test_to_sql_renders_type_aware_direct_relationship_scalar_functions(self) -> None:
        sql = cypherglot.to_sql(
            (
                "MATCH (a:User)-[r:WORKS_AT]->(b:Company) "
                "RETURN lower(r.note) AS lower_note, size(type(r)) AS type_len, "
                "toString(r.since) AS since_text ORDER BY lower_note, type_len, since_text"
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
            'SELECT LOWER(r.note) AS "lower_note", LENGTH(\'WORKS_AT\') AS "type_len", '
            'CAST(r.since AS TEXT) AS "since_text" FROM cg_edge_works_at AS r '
            'JOIN cg_node_user AS a ON a.id = r.from_id '
            'JOIN cg_node_company AS b ON b.id = r.to_id '
            'ORDER BY LOWER(r.note) ASC, LENGTH(\'WORKS_AT\') ASC, CAST(r.since AS TEXT) ASC',
        )

    def test_to_sql_renders_type_aware_match_with_return(self) -> None:
        sql = cypherglot.to_sql(
            (
                "MATCH (u:User) WITH u AS person, u.name AS name "
                "WHERE name = $name RETURN person.name AS display_name, name "
                "ORDER BY display_name, name"
            ),
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

        self.assertEqual(
            sql,
            'SELECT with_q."__cg_with_person_prop_name" AS "display_name", '
            'with_q."__cg_with_scalar_name" AS "name" '
            'FROM (SELECT u.id AS "__cg_with_person_id", '
            'u.name AS "__cg_with_person_prop_name", '
            'u.name AS "__cg_with_scalar_name" '
            'FROM cg_node_user AS u) AS with_q '
            'WHERE with_q."__cg_with_scalar_name" = :name '
            'ORDER BY with_q."__cg_with_person_prop_name" ASC, '
            'with_q."__cg_with_scalar_name" ASC',
        )

    def test_to_sql_renders_type_aware_fixed_length_multi_hop_match(self) -> None:
        sql = cypherglot.to_sql(
            (
                "MATCH (a:User)-[r:KNOWS]->(b:User)-[s:WORKS_AT]->(c:Company) "
                "RETURN a.name AS user_name, c.name AS company ORDER BY company"
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
                            name="KNOWS",
                            source_type="User",
                            target_type="User",
                        ),
                        cypherglot.EdgeTypeSpec(
                            name="WORKS_AT",
                            source_type="User",
                            target_type="Company",
                        ),
                    ),
                )
            ),
        )

        self.assertEqual(
            sql,
            'SELECT a.name AS "user_name", c.name AS "company" '
            'FROM cg_edge_knows AS r '
            'JOIN cg_node_user AS a ON a.id = r.from_id '
            'JOIN cg_node_user AS b ON b.id = r.to_id '
            'JOIN cg_edge_works_at AS s ON b.id = s.from_id '
            'JOIN cg_node_company AS c ON c.id = s.to_id '
            'ORDER BY c.name ASC',
        )

    def test_to_sql_renders_type_aware_relational_output_mode_bounded_variable_length_match(
        self,
    ) -> None:
        sql = cypherglot.to_sql(
            (
                "MATCH (a:User)-[:KNOWS*0..2]->(b:User) "
                "RETURN a.name AS user_name, b.name AS friend ORDER BY friend, user_name"
            ),
            schema_context=CompilerSchemaContext.type_aware(
                GraphSchema(
                    node_types=(
                        NodeTypeSpec(
                            name="User",
                            properties=(PropertyField("name", "string"),),
                        ),
                    ),
                    edge_types=(
                        cypherglot.EdgeTypeSpec(
                            name="KNOWS",
                            source_type="User",
                            target_type="User",
                        ),
                    ),
                ),
            ),
        )
        aggregate_sql = cypherglot.to_sql(
            "MATCH (a:User)-[:KNOWS*0..2]->(b:User) RETURN sum(b.age) AS total_age",
            schema_context=CompilerSchemaContext.type_aware(
                GraphSchema(
                    node_types=(
                        NodeTypeSpec(
                            name="User",
                            properties=(PropertyField("age", "integer"),),
                        ),
                    ),
                    edge_types=(
                        cypherglot.EdgeTypeSpec(
                            name="KNOWS",
                            source_type="User",
                            target_type="User",
                        ),
                    ),
                ),
            ),
        )
        scalar_function_sql = cypherglot.to_sql(
            (
                "MATCH (a:User)-[:KNOWS*0..2]->(b:User) "
                "RETURN lower(b.name) AS lower_friend, toString(b.age) AS age_text "
                "ORDER BY age_text, lower_friend"
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
                    edge_types=(
                        cypherglot.EdgeTypeSpec(
                            name="KNOWS",
                            source_type="User",
                            target_type="User",
                        ),
                    ),
                ),
            ),
        )
        id_sql = cypherglot.to_sql(
            "MATCH (a:User)-[:KNOWS*0..2]->(b:User) RETURN id(b) AS friend_id ORDER BY friend_id",
            schema_context=CompilerSchemaContext.type_aware(
                GraphSchema(
                    node_types=(
                        NodeTypeSpec(
                            name="User",
                            properties=(PropertyField("name", "string"),),
                        ),
                    ),
                    edge_types=(
                        cypherglot.EdgeTypeSpec(
                            name="KNOWS",
                            source_type="User",
                            target_type="User",
                        ),
                    ),
                ),
            ),
        )

        self.assertEqual(
            sql,
            'SELECT * FROM (SELECT __cg_zero_hop_node.name AS "user_name", __cg_zero_hop_node.name AS "friend" '
            'FROM cg_node_user AS __cg_zero_hop_node '
            'UNION ALL SELECT a.name AS "user_name", b.name AS "friend" '
            'FROM cg_edge_knows AS __cg_edge_0 '
            'JOIN cg_node_user AS a ON a.id = __cg_edge_0.from_id '
            'JOIN cg_node_user AS b ON b.id = __cg_edge_0.to_id '
            'UNION ALL SELECT a.name AS "user_name", b.name AS "friend" '
            'FROM cg_edge_knows AS __cg_edge_0 '
            'JOIN cg_node_user AS a ON a.id = __cg_edge_0.from_id '
            'JOIN cg_node_user AS __cg_variable_hop_2_node_1 ON __cg_variable_hop_2_node_1.id = __cg_edge_0.to_id '
            'JOIN cg_edge_knows AS __cg_edge_1 ON __cg_variable_hop_2_node_1.id = __cg_edge_1.from_id '
            'JOIN cg_node_user AS b ON b.id = __cg_edge_1.to_id) AS variable_length_q '
            'ORDER BY variable_length_q."friend" ASC, variable_length_q."user_name" ASC',
        )
        self.assertEqual(
            aggregate_sql,
            'SELECT SUM(variable_length_q."__cg_aggregate_0") AS "total_age" '
            'FROM (SELECT __cg_zero_hop_node.age AS "__cg_aggregate_0" '
            'FROM cg_node_user AS __cg_zero_hop_node '
            'UNION ALL SELECT b.age AS "__cg_aggregate_0" '
            'FROM cg_edge_knows AS __cg_edge_0 '
            'JOIN cg_node_user AS a ON a.id = __cg_edge_0.from_id '
            'JOIN cg_node_user AS b ON b.id = __cg_edge_0.to_id '
            'UNION ALL SELECT b.age AS "__cg_aggregate_0" '
            'FROM cg_edge_knows AS __cg_edge_0 '
            'JOIN cg_node_user AS a ON a.id = __cg_edge_0.from_id '
            'JOIN cg_node_user AS __cg_variable_hop_2_node_1 ON __cg_variable_hop_2_node_1.id = __cg_edge_0.to_id '
            'JOIN cg_edge_knows AS __cg_edge_1 ON __cg_variable_hop_2_node_1.id = __cg_edge_1.from_id '
            'JOIN cg_node_user AS b ON b.id = __cg_edge_1.to_id) AS variable_length_q',
        )
        with self.assertRaisesRegex(
            ValueError,
            "relational output mode does not yet support whole-entity or introspection returns",
        ):
            cypherglot.to_sql(
                (
                    "MATCH (a:User)-[:KNOWS*0..2]->(b:User) RETURN b AS friend_node, "
                    "properties(b) AS friend_props, labels(b) AS friend_labels, keys(b) AS friend_keys, "
                    "b.name AS friend ORDER BY friend"
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
                        edge_types=(
                            cypherglot.EdgeTypeSpec(
                                name="KNOWS",
                                source_type="User",
                                target_type="User",
                            ),
                        ),
                    ),
                ),
            )
        self.assertEqual(
            scalar_function_sql,
            'SELECT * FROM (SELECT LOWER(__cg_zero_hop_node.name) AS "lower_friend", CAST(__cg_zero_hop_node.age AS TEXT) AS "age_text" '
            'FROM cg_node_user AS __cg_zero_hop_node '
            'UNION ALL SELECT LOWER(b.name) AS "lower_friend", CAST(b.age AS TEXT) AS "age_text" '
            'FROM cg_edge_knows AS __cg_edge_0 '
            'JOIN cg_node_user AS a ON a.id = __cg_edge_0.from_id '
            'JOIN cg_node_user AS b ON b.id = __cg_edge_0.to_id '
            'UNION ALL SELECT LOWER(b.name) AS "lower_friend", CAST(b.age AS TEXT) AS "age_text" '
            'FROM cg_edge_knows AS __cg_edge_0 '
            'JOIN cg_node_user AS a ON a.id = __cg_edge_0.from_id '
            'JOIN cg_node_user AS __cg_variable_hop_2_node_1 ON __cg_variable_hop_2_node_1.id = __cg_edge_0.to_id '
            'JOIN cg_edge_knows AS __cg_edge_1 ON __cg_variable_hop_2_node_1.id = __cg_edge_1.from_id '
            'JOIN cg_node_user AS b ON b.id = __cg_edge_1.to_id) AS variable_length_q '
            'ORDER BY variable_length_q."age_text" ASC, variable_length_q."lower_friend" ASC',
        )
        self.assertEqual(
            id_sql,
            'SELECT * FROM (SELECT __cg_zero_hop_node.id AS "friend_id" '
            'FROM cg_node_user AS __cg_zero_hop_node '
            'UNION ALL SELECT b.id AS "friend_id" '
            'FROM cg_edge_knows AS __cg_edge_0 '
            'JOIN cg_node_user AS a ON a.id = __cg_edge_0.from_id '
            'JOIN cg_node_user AS b ON b.id = __cg_edge_0.to_id '
            'UNION ALL SELECT b.id AS "friend_id" '
            'FROM cg_edge_knows AS __cg_edge_0 '
            'JOIN cg_node_user AS a ON a.id = __cg_edge_0.from_id '
            'JOIN cg_node_user AS __cg_variable_hop_2_node_1 ON __cg_variable_hop_2_node_1.id = __cg_edge_0.to_id '
            'JOIN cg_edge_knows AS __cg_edge_1 ON __cg_variable_hop_2_node_1.id = __cg_edge_1.from_id '
            'JOIN cg_node_user AS b ON b.id = __cg_edge_1.to_id) AS variable_length_q '
            'ORDER BY variable_length_q."friend_id" ASC',
        )

    def test_to_sql_renders_type_aware_relational_output_mode_bounded_variable_length_match_grouped_count(
        self,
    ) -> None:
        sql = cypherglot.to_sql(
            (
                "MATCH (a:User)-[:KNOWS*0..2]->(b:User) "
                "RETURN b.name AS friend, count(b) AS total ORDER BY total DESC, friend"
            ),
            schema_context=CompilerSchemaContext.type_aware(
                GraphSchema(
                    node_types=(
                        NodeTypeSpec(
                            name="User",
                            properties=(PropertyField("name", "string"),),
                        ),
                    ),
                    edge_types=(
                        cypherglot.EdgeTypeSpec(
                            name="KNOWS",
                            source_type="User",
                            target_type="User",
                        ),
                    ),
                ),
            ),
        )
        node_sql = cypherglot.to_sql(
            (
                "MATCH (a:User)-[:KNOWS*0..2]->(b:User) "
                "RETURN b AS friend_node, count(b) AS total ORDER BY total DESC, friend_node"
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
                    edge_types=(
                        cypherglot.EdgeTypeSpec(
                            name="KNOWS",
                            source_type="User",
                            target_type="User",
                        ),
                    ),
                ),
            ),
        )
        helper_sql = cypherglot.to_sql(
            (
                "MATCH (a:User)-[:KNOWS*0..2]->(b:User) "
                "RETURN properties(b) AS friend_props, count(b) AS total ORDER BY total DESC, friend_props"
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
                    edge_types=(
                        cypherglot.EdgeTypeSpec(
                            name="KNOWS",
                            source_type="User",
                            target_type="User",
                        ),
                    ),
                ),
            ),
        )
        lowered_sql = cypherglot.to_sql(
            (
                "MATCH (a:User)-[:KNOWS*0..2]->(b:User) "
                "RETURN lower(b.name) AS lowered_name, count(b) AS total ORDER BY total DESC, lowered_name"
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
                    edge_types=(
                        cypherglot.EdgeTypeSpec(
                            name="KNOWS",
                            source_type="User",
                            target_type="User",
                        ),
                    ),
                ),
            ),
        )
        age_text_sql = cypherglot.to_sql(
            (
                "MATCH (a:User)-[:KNOWS*0..2]->(b:User) "
                "RETURN toString(b.age) AS age_text, count(b) AS total ORDER BY total DESC, age_text"
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
                    edge_types=(
                        cypherglot.EdgeTypeSpec(
                            name="KNOWS",
                            source_type="User",
                            target_type="User",
                        ),
                    ),
                ),
            ),
        )
        id_sql = cypherglot.to_sql(
            (
                "MATCH (a:User)-[:KNOWS*0..2]->(b:User) "
                "RETURN id(b) AS friend_id, count(b) AS total ORDER BY total DESC, friend_id"
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
                    edge_types=(
                        cypherglot.EdgeTypeSpec(
                            name="KNOWS",
                            source_type="User",
                            target_type="User",
                        ),
                    ),
                ),
            ),
        )

        self.assertEqual(
            sql,
            'SELECT variable_length_q."friend" AS "friend", COUNT(variable_length_q."__cg_aggregate_1") AS "total" '
            'FROM (SELECT __cg_zero_hop_node.name AS "friend", __cg_zero_hop_node.id AS "__cg_aggregate_1" '
            'FROM cg_node_user AS __cg_zero_hop_node '
            'UNION ALL SELECT b.name AS "friend", b.id AS "__cg_aggregate_1" '
            'FROM cg_edge_knows AS __cg_edge_0 '
            'JOIN cg_node_user AS a ON a.id = __cg_edge_0.from_id '
            'JOIN cg_node_user AS b ON b.id = __cg_edge_0.to_id '
            'UNION ALL SELECT b.name AS "friend", b.id AS "__cg_aggregate_1" '
            'FROM cg_edge_knows AS __cg_edge_0 '
            'JOIN cg_node_user AS a ON a.id = __cg_edge_0.from_id '
            'JOIN cg_node_user AS __cg_variable_hop_2_node_1 ON __cg_variable_hop_2_node_1.id = __cg_edge_0.to_id '
            'JOIN cg_edge_knows AS __cg_edge_1 ON __cg_variable_hop_2_node_1.id = __cg_edge_1.from_id '
            'JOIN cg_node_user AS b ON b.id = __cg_edge_1.to_id) AS variable_length_q '
            'GROUP BY variable_length_q."friend" ORDER BY "total" DESC, variable_length_q."friend" ASC',
        )
        self.assertEqual(
            node_sql,
            'SELECT variable_length_q."friend_node.id" AS "friend_node.id", variable_length_q."friend_node.label" AS "friend_node.label", '
            'variable_length_q."friend_node.name" AS "friend_node.name", variable_length_q."friend_node.age" AS "friend_node.age", '
            'COUNT(variable_length_q."__cg_aggregate_1") AS "total" '
            'FROM (SELECT __cg_zero_hop_node.id AS "friend_node.id", \'User\' AS "friend_node.label", __cg_zero_hop_node.name AS "friend_node.name", __cg_zero_hop_node.age AS "friend_node.age", __cg_zero_hop_node.id AS "__cg_aggregate_1" FROM cg_node_user AS __cg_zero_hop_node '
            'UNION ALL SELECT b.id AS "friend_node.id", \'User\' AS "friend_node.label", b.name AS "friend_node.name", b.age AS "friend_node.age", b.id AS "__cg_aggregate_1" FROM cg_edge_knows AS __cg_edge_0 JOIN cg_node_user AS a ON a.id = __cg_edge_0.from_id JOIN cg_node_user AS b ON b.id = __cg_edge_0.to_id '
            'UNION ALL SELECT b.id AS "friend_node.id", \'User\' AS "friend_node.label", b.name AS "friend_node.name", b.age AS "friend_node.age", b.id AS "__cg_aggregate_1" FROM cg_edge_knows AS __cg_edge_0 JOIN cg_node_user AS a ON a.id = __cg_edge_0.from_id JOIN cg_node_user AS __cg_variable_hop_2_node_1 ON __cg_variable_hop_2_node_1.id = __cg_edge_0.to_id JOIN cg_edge_knows AS __cg_edge_1 ON __cg_variable_hop_2_node_1.id = __cg_edge_1.from_id JOIN cg_node_user AS b ON b.id = __cg_edge_1.to_id) AS variable_length_q '
            'GROUP BY variable_length_q."friend_node.id", variable_length_q."friend_node.label", variable_length_q."friend_node.name", variable_length_q."friend_node.age" '
            'ORDER BY "total" DESC, variable_length_q."friend_node.id" ASC, variable_length_q."friend_node.label" ASC, variable_length_q."friend_node.name" ASC, variable_length_q."friend_node.age" ASC',
        )
        self.assertEqual(
            helper_sql,
            'SELECT variable_length_q."friend_props.name" AS "friend_props.name", variable_length_q."friend_props.age" AS "friend_props.age", COUNT(variable_length_q."__cg_aggregate_1") AS "total" '
            'FROM (SELECT __cg_zero_hop_node.name AS "friend_props.name", __cg_zero_hop_node.age AS "friend_props.age", __cg_zero_hop_node.id AS "__cg_aggregate_1" FROM cg_node_user AS __cg_zero_hop_node '
            'UNION ALL SELECT b.name AS "friend_props.name", b.age AS "friend_props.age", b.id AS "__cg_aggregate_1" FROM cg_edge_knows AS __cg_edge_0 JOIN cg_node_user AS a ON a.id = __cg_edge_0.from_id JOIN cg_node_user AS b ON b.id = __cg_edge_0.to_id '
            'UNION ALL SELECT b.name AS "friend_props.name", b.age AS "friend_props.age", b.id AS "__cg_aggregate_1" FROM cg_edge_knows AS __cg_edge_0 JOIN cg_node_user AS a ON a.id = __cg_edge_0.from_id JOIN cg_node_user AS __cg_variable_hop_2_node_1 ON __cg_variable_hop_2_node_1.id = __cg_edge_0.to_id JOIN cg_edge_knows AS __cg_edge_1 ON __cg_variable_hop_2_node_1.id = __cg_edge_1.from_id JOIN cg_node_user AS b ON b.id = __cg_edge_1.to_id) AS variable_length_q '
            'GROUP BY variable_length_q."friend_props.name", variable_length_q."friend_props.age" ORDER BY "total" DESC, variable_length_q."friend_props.name" ASC, variable_length_q."friend_props.age" ASC',
        )
        with self.assertRaisesRegex(
            ValueError,
            "relational output mode does not yet support whole-entity or introspection returns",
        ):
            cypherglot.to_sql(
                (
                    "MATCH (a:User)-[:KNOWS*0..2]->(b:User) "
                    "RETURN labels(b) AS friend_labels, count(b) AS total ORDER BY total DESC, friend_labels"
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
                        edge_types=(
                            cypherglot.EdgeTypeSpec(
                                name="KNOWS",
                                source_type="User",
                                target_type="User",
                            ),
                        ),
                    ),
                ),
            )
        with self.assertRaisesRegex(
            ValueError,
            "relational output mode does not yet support whole-entity or introspection returns",
        ):
            cypherglot.to_sql(
                (
                    "MATCH (a:User)-[:KNOWS*0..2]->(b:User) "
                    "RETURN keys(b) AS friend_keys, count(b) AS total ORDER BY total DESC, friend_keys"
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
                        edge_types=(
                            cypherglot.EdgeTypeSpec(
                                name="KNOWS",
                                source_type="User",
                                target_type="User",
                            ),
                        ),
                    ),
                ),
            )
        self.assertEqual(
            lowered_sql,
            'SELECT variable_length_q."lowered_name" AS "lowered_name", COUNT(variable_length_q."__cg_aggregate_1") AS "total" '
            'FROM (SELECT LOWER(__cg_zero_hop_node.name) AS "lowered_name", __cg_zero_hop_node.id AS "__cg_aggregate_1" FROM cg_node_user AS __cg_zero_hop_node '
            'UNION ALL SELECT LOWER(b.name) AS "lowered_name", b.id AS "__cg_aggregate_1" FROM cg_edge_knows AS __cg_edge_0 JOIN cg_node_user AS a ON a.id = __cg_edge_0.from_id JOIN cg_node_user AS b ON b.id = __cg_edge_0.to_id '
            'UNION ALL SELECT LOWER(b.name) AS "lowered_name", b.id AS "__cg_aggregate_1" FROM cg_edge_knows AS __cg_edge_0 JOIN cg_node_user AS a ON a.id = __cg_edge_0.from_id JOIN cg_node_user AS __cg_variable_hop_2_node_1 ON __cg_variable_hop_2_node_1.id = __cg_edge_0.to_id JOIN cg_edge_knows AS __cg_edge_1 ON __cg_variable_hop_2_node_1.id = __cg_edge_1.from_id JOIN cg_node_user AS b ON b.id = __cg_edge_1.to_id) AS variable_length_q '
            'GROUP BY variable_length_q."lowered_name" ORDER BY "total" DESC, variable_length_q."lowered_name" ASC',
        )
        self.assertEqual(
            age_text_sql,
            'SELECT variable_length_q."age_text" AS "age_text", COUNT(variable_length_q."__cg_aggregate_1") AS "total" '
            'FROM (SELECT CAST(__cg_zero_hop_node.age AS TEXT) AS "age_text", __cg_zero_hop_node.id AS "__cg_aggregate_1" FROM cg_node_user AS __cg_zero_hop_node '
            'UNION ALL SELECT CAST(b.age AS TEXT) AS "age_text", b.id AS "__cg_aggregate_1" FROM cg_edge_knows AS __cg_edge_0 JOIN cg_node_user AS a ON a.id = __cg_edge_0.from_id JOIN cg_node_user AS b ON b.id = __cg_edge_0.to_id '
            'UNION ALL SELECT CAST(b.age AS TEXT) AS "age_text", b.id AS "__cg_aggregate_1" FROM cg_edge_knows AS __cg_edge_0 JOIN cg_node_user AS a ON a.id = __cg_edge_0.from_id JOIN cg_node_user AS __cg_variable_hop_2_node_1 ON __cg_variable_hop_2_node_1.id = __cg_edge_0.to_id JOIN cg_edge_knows AS __cg_edge_1 ON __cg_variable_hop_2_node_1.id = __cg_edge_1.from_id JOIN cg_node_user AS b ON b.id = __cg_edge_1.to_id) AS variable_length_q '
            'GROUP BY variable_length_q."age_text" ORDER BY "total" DESC, variable_length_q."age_text" ASC',
        )
        self.assertEqual(
            id_sql,
            'SELECT variable_length_q."friend_id" AS "friend_id", COUNT(variable_length_q."__cg_aggregate_1") AS "total" '
            'FROM (SELECT __cg_zero_hop_node.id AS "friend_id", __cg_zero_hop_node.id AS "__cg_aggregate_1" FROM cg_node_user AS __cg_zero_hop_node '
            'UNION ALL SELECT b.id AS "friend_id", b.id AS "__cg_aggregate_1" FROM cg_edge_knows AS __cg_edge_0 JOIN cg_node_user AS a ON a.id = __cg_edge_0.from_id JOIN cg_node_user AS b ON b.id = __cg_edge_0.to_id '
            'UNION ALL SELECT b.id AS "friend_id", b.id AS "__cg_aggregate_1" FROM cg_edge_knows AS __cg_edge_0 JOIN cg_node_user AS a ON a.id = __cg_edge_0.from_id JOIN cg_node_user AS __cg_variable_hop_2_node_1 ON __cg_variable_hop_2_node_1.id = __cg_edge_0.to_id JOIN cg_edge_knows AS __cg_edge_1 ON __cg_variable_hop_2_node_1.id = __cg_edge_1.from_id JOIN cg_node_user AS b ON b.id = __cg_edge_1.to_id) AS variable_length_q '
            'GROUP BY variable_length_q."friend_id" ORDER BY "total" DESC, variable_length_q."friend_id" ASC',
        )

    def test_to_sql_renders_type_aware_relational_bounded_variable_length_grouped_entity_and_properties(
        self,
    ) -> None:
        entity_sql = cypherglot.to_sql(
            (
                "MATCH (a:User)-[:KNOWS*0..2]->(b:User) "
                "RETURN b AS friend, count(b) AS total ORDER BY friend, total"
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
                    edge_types=(
                        cypherglot.EdgeTypeSpec(
                            name="KNOWS",
                            source_type="User",
                            target_type="User",
                        ),
                    ),
                ),
            ),
        )
        properties_sql = cypherglot.to_sql(
            (
                "MATCH (a:User)-[:KNOWS*0..2]->(b:User) "
                "RETURN properties(b) AS props, count(b) AS total ORDER BY props, total"
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
                    edge_types=(
                        cypherglot.EdgeTypeSpec(
                            name="KNOWS",
                            source_type="User",
                            target_type="User",
                        ),
                    ),
                ),
            ),
        )

        self.assertEqual(
            entity_sql,
            'SELECT variable_length_q."friend.id" AS "friend.id", variable_length_q."friend.label" AS "friend.label", '
            'variable_length_q."friend.name" AS "friend.name", variable_length_q."friend.age" AS "friend.age", '
            'COUNT(variable_length_q."__cg_aggregate_1") AS "total" '
            'FROM (SELECT __cg_zero_hop_node.id AS "friend.id", \'User\' AS "friend.label", '
            '__cg_zero_hop_node.name AS "friend.name", __cg_zero_hop_node.age AS "friend.age", '
            '__cg_zero_hop_node.id AS "__cg_aggregate_1" FROM cg_node_user AS __cg_zero_hop_node '
            'UNION ALL SELECT b.id AS "friend.id", \'User\' AS "friend.label", b.name AS "friend.name", b.age AS "friend.age", b.id AS "__cg_aggregate_1" '
            'FROM cg_edge_knows AS __cg_edge_0 JOIN cg_node_user AS a ON a.id = __cg_edge_0.from_id JOIN cg_node_user AS b ON b.id = __cg_edge_0.to_id '
            'UNION ALL SELECT b.id AS "friend.id", \'User\' AS "friend.label", b.name AS "friend.name", b.age AS "friend.age", b.id AS "__cg_aggregate_1" '
            'FROM cg_edge_knows AS __cg_edge_0 JOIN cg_node_user AS a ON a.id = __cg_edge_0.from_id JOIN cg_node_user AS __cg_variable_hop_2_node_1 ON __cg_variable_hop_2_node_1.id = __cg_edge_0.to_id JOIN cg_edge_knows AS __cg_edge_1 ON __cg_variable_hop_2_node_1.id = __cg_edge_1.from_id JOIN cg_node_user AS b ON b.id = __cg_edge_1.to_id) AS variable_length_q '
            'GROUP BY variable_length_q."friend.id", variable_length_q."friend.label", variable_length_q."friend.name", variable_length_q."friend.age" '
            'ORDER BY variable_length_q."friend.id" ASC, variable_length_q."friend.label" ASC, variable_length_q."friend.name" ASC, variable_length_q."friend.age" ASC, "total" ASC',
        )
        self.assertEqual(
            properties_sql,
            'SELECT variable_length_q."props.name" AS "props.name", variable_length_q."props.age" AS "props.age", '
            'COUNT(variable_length_q."__cg_aggregate_1") AS "total" '
            'FROM (SELECT __cg_zero_hop_node.name AS "props.name", __cg_zero_hop_node.age AS "props.age", __cg_zero_hop_node.id AS "__cg_aggregate_1" FROM cg_node_user AS __cg_zero_hop_node '
            'UNION ALL SELECT b.name AS "props.name", b.age AS "props.age", b.id AS "__cg_aggregate_1" FROM cg_edge_knows AS __cg_edge_0 JOIN cg_node_user AS a ON a.id = __cg_edge_0.from_id JOIN cg_node_user AS b ON b.id = __cg_edge_0.to_id '
            'UNION ALL SELECT b.name AS "props.name", b.age AS "props.age", b.id AS "__cg_aggregate_1" FROM cg_edge_knows AS __cg_edge_0 JOIN cg_node_user AS a ON a.id = __cg_edge_0.from_id JOIN cg_node_user AS __cg_variable_hop_2_node_1 ON __cg_variable_hop_2_node_1.id = __cg_edge_0.to_id JOIN cg_edge_knows AS __cg_edge_1 ON __cg_variable_hop_2_node_1.id = __cg_edge_1.from_id JOIN cg_node_user AS b ON b.id = __cg_edge_1.to_id) AS variable_length_q '
            'GROUP BY variable_length_q."props.name", variable_length_q."props.age" '
            'ORDER BY variable_length_q."props.name" ASC, variable_length_q."props.age" ASC, "total" ASC',
        )

    def test_to_sql_renders_type_aware_fixed_length_multi_hop_introspection(self) -> None:
        sql = cypherglot.to_sql(
            (
                "MATCH (a:User)-[r:KNOWS]->(b:User)-[s:WORKS_AT]->(c:Company) "
                "RETURN type(r) AS first_rel_type, startNode(s).name AS employee, "
                "endNode(s) AS employer ORDER BY first_rel_type, employee, employer"
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
                            name="KNOWS",
                            source_type="User",
                            target_type="User",
                        ),
                        cypherglot.EdgeTypeSpec(
                            name="WORKS_AT",
                            source_type="User",
                            target_type="Company",
                        ),
                    ),
                )
            ),
        )

        self.assertEqual(
            sql,
            'SELECT \'KNOWS\' AS "first_rel_type", b.name AS "employee", '
            'c.id AS "employer.id", \'Company\' AS "employer.label", '
            'c.name AS "employer.name" '
            'FROM cg_edge_knows AS r '
            'JOIN cg_node_user AS a ON a.id = r.from_id '
            'JOIN cg_node_user AS b ON b.id = r.to_id '
            'JOIN cg_edge_works_at AS s ON b.id = s.from_id '
            'JOIN cg_node_company AS c ON c.id = s.to_id '
            'ORDER BY \'KNOWS\' ASC, b.name ASC, c.id ASC, \'Company\' ASC, c.name ASC',
        )

    def test_to_sql_renders_type_aware_fixed_length_multi_hop_helper_introspection(
        self,
    ) -> None:
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
                                name="KNOWS",
                                source_type="User",
                                target_type="User",
                            ),
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

    def test_to_sql_renders_type_aware_fixed_length_multi_hop_complementary_helper_returns(
        self,
    ) -> None:
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
                                name="KNOWS",
                                source_type="User",
                                target_type="User",
                            ),
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

    def test_to_sql_rejects_type_aware_relational_output_mode_fixed_length_multi_hop_grouped_helper_returns(
        self,
    ) -> None:
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
                                name="KNOWS",
                                source_type="User",
                                target_type="User",
                            ),
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

    def test_to_sql_rejects_type_aware_relational_output_mode_fixed_length_multi_hop_grouped_complementary_helper_returns(
        self,
    ) -> None:
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
                                name="KNOWS",
                                source_type="User",
                                target_type="User",
                            ),
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

    def test_to_sql_renders_type_aware_fixed_length_multi_hop_grouped_aggregates(self) -> None:
        sql = cypherglot.to_sql(
            (
                "MATCH (a:User)-[r:KNOWS]->(b:User)-[s:WORKS_AT]->(c:Company) "
                "RETURN c.name AS company, count(s) AS total ORDER BY total DESC, company"
            ),
            schema_context=CompilerSchemaContext.type_aware(
                GraphSchema(
                    node_types=(
                        NodeTypeSpec(name="User"),
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
                        ),
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
            sql,
            'SELECT c.name AS "company", COUNT(s.id) AS "total" '
            'FROM cg_edge_knows AS r '
            'JOIN cg_node_user AS a ON a.id = r.from_id '
            'JOIN cg_node_user AS b ON b.id = r.to_id '
            'JOIN cg_edge_works_at AS s ON b.id = s.from_id '
            'JOIN cg_node_company AS c ON c.id = s.to_id '
            'GROUP BY c.name ORDER BY "total" DESC, c.name ASC',
        )

    def test_to_sql_renders_type_aware_relational_output_mode_direct_chain_endpoints(
        self,
    ) -> None:
        sql = cypherglot.to_sql(
            (
                "MATCH (a:User)-[r:KNOWS]->(b:User)-[s:WORKS_AT]->(c:Company) "
                "RETURN startNode(s) AS employee, endNode(s) AS employer, c.name AS company "
                "ORDER BY company"
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
                            name="KNOWS",
                            source_type="User",
                            target_type="User",
                        ),
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
            'SELECT b.id AS "employee.id", \'User\' AS "employee.label", '
            'b.name AS "employee.name", c.id AS "employer.id", '
            '\'Company\' AS "employer.label", c.name AS "employer.name", '
            'c.name AS "company" FROM cg_edge_knows AS r '
            'JOIN cg_node_user AS a ON a.id = r.from_id '
            'JOIN cg_node_user AS b ON b.id = r.to_id '
            'JOIN cg_edge_works_at AS s ON b.id = s.from_id '
            'JOIN cg_node_company AS c ON c.id = s.to_id '
            'ORDER BY c.name ASC',
        )

    def test_to_sql_renders_type_aware_relational_output_mode_direct_chain_entities_and_properties(
        self,
    ) -> None:
        sql = cypherglot.to_sql(
            (
                "MATCH (a:User)-[r:KNOWS]->(b:User)-[s:WORKS_AT]->(c:Company) "
                "RETURN b AS friend, properties(b) AS friend_props, "
                "s AS rel, properties(s) AS rel_props, c.name AS company_name "
                "ORDER BY company_name"
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
                            name="KNOWS",
                            source_type="User",
                            target_type="User",
                        ),
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

        self.assertEqual(
            sql,
            'SELECT b.id AS "friend.id", \'User\' AS "friend.label", '
            'b.name AS "friend.name", b.name AS "friend_props.name", '
            's.id AS "rel.id", \'WORKS_AT\' AS "rel.type", '
            's.from_id AS "rel.from_id", s.to_id AS "rel.to_id", '
            's.since AS "rel.since", s.since AS "rel_props.since", '
            'c.name AS "company_name" FROM cg_edge_knows AS r '
            'JOIN cg_node_user AS a ON a.id = r.from_id '
            'JOIN cg_node_user AS b ON b.id = r.to_id '
            'JOIN cg_edge_works_at AS s ON b.id = s.from_id '
            'JOIN cg_node_company AS c ON c.id = s.to_id '
            'ORDER BY c.name ASC',
        )

    def test_to_sql_renders_type_aware_relational_output_mode_grouped_direct_chain_entities_and_properties(
        self,
    ) -> None:
        sql = cypherglot.to_sql(
            (
                "MATCH (a:User)-[r:KNOWS]->(b:User)-[s:WORKS_AT]->(c:Company) "
                "RETURN b AS friend, properties(b) AS friend_props, "
                "s AS rel, properties(s) AS rel_props, count(s) AS total "
                "ORDER BY total DESC"
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
                            name="KNOWS",
                            source_type="User",
                            target_type="User",
                        ),
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

        self.assertEqual(
            sql,
            'SELECT b.id AS "friend.id", \'User\' AS "friend.label", '
            'b.name AS "friend.name", b.name AS "friend_props.name", '
            's.id AS "rel.id", \'WORKS_AT\' AS "rel.type", '
            's.from_id AS "rel.from_id", s.to_id AS "rel.to_id", '
            's.since AS "rel.since", s.since AS "rel_props.since", '
            'COUNT(s.id) AS "total" FROM cg_edge_knows AS r '
            'JOIN cg_node_user AS a ON a.id = r.from_id '
            'JOIN cg_node_user AS b ON b.id = r.to_id '
            'JOIN cg_edge_works_at AS s ON b.id = s.from_id '
            'JOIN cg_node_company AS c ON c.id = s.to_id '
            'GROUP BY b.id, \'User\', b.name, b.name, s.id, \'WORKS_AT\', '
            's.from_id, s.to_id, s.since, s.since ORDER BY "total" DESC',
        )

    def test_to_sql_renders_type_aware_match_with_chain_source(self) -> None:
        sql = cypherglot.to_sql(
            (
                "MATCH (a:User)-[r:KNOWS]->(b:User)-[s:WORKS_AT]->(c:Company) "
                "WITH b AS friend, c.name AS company "
                "RETURN friend.name AS friend_name, company ORDER BY company"
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
                            name="KNOWS",
                            source_type="User",
                            target_type="User",
                        ),
                        cypherglot.EdgeTypeSpec(
                            name="WORKS_AT",
                            source_type="User",
                            target_type="Company",
                        ),
                    ),
                )
            ),
        )

        self.assertEqual(
            sql,
            'SELECT with_q."__cg_with_friend_prop_name" AS "friend_name", '
            'with_q."__cg_with_scalar_company" AS "company" '
            'FROM (SELECT b.id AS "__cg_with_friend_id", '
            'b.name AS "__cg_with_friend_prop_name", '
            'c.name AS "__cg_with_scalar_company" '
            'FROM cg_edge_knows AS r '
            'JOIN cg_node_user AS a ON a.id = r.from_id '
            'JOIN cg_node_user AS b ON b.id = r.to_id '
            'JOIN cg_edge_works_at AS s ON b.id = s.from_id '
            'JOIN cg_node_company AS c ON c.id = s.to_id) AS with_q '
            'ORDER BY with_q."__cg_with_scalar_company" ASC',
        )

    def test_to_sql_renders_type_aware_relational_output_mode_match_with_chain_relationship_introspection(self) -> None:
        sql = cypherglot.to_sql(
            (
                "MATCH (a:User)-[r:KNOWS]->(b:User)-[s:WORKS_AT]->(c:Company) "
                "WITH b AS friend, c AS company, s AS rel "
                "RETURN startNode(rel).name AS employee, endNode(rel) AS employer, "
                "type(rel) AS rel_type ORDER BY employee, employer, rel_type"
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
                            name="KNOWS",
                            source_type="User",
                            target_type="User",
                        ),
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
            'SELECT with_q."__cg_with_friend_prop_name" AS "employee", '
            'with_q."__cg_with_company_id" AS "employer.id", '
            '\'Company\' AS "employer.label", '
            'with_q."__cg_with_company_prop_name" AS "employer.name", '
            '\'WORKS_AT\' AS "rel_type" '
            'FROM (SELECT b.id AS "__cg_with_friend_id", '
            'b.name AS "__cg_with_friend_prop_name", '
            'c.id AS "__cg_with_company_id", '
            'c.name AS "__cg_with_company_prop_name", '
            's.id AS "__cg_with_rel_id", '
            's.from_id AS "__cg_with_rel_from_id", '
            's.to_id AS "__cg_with_rel_to_id" '
            'FROM cg_edge_knows AS r '
            'JOIN cg_node_user AS a ON a.id = r.from_id '
            'JOIN cg_node_user AS b ON b.id = r.to_id '
            'JOIN cg_edge_works_at AS s ON b.id = s.from_id '
            'JOIN cg_node_company AS c ON c.id = s.to_id) AS with_q '
            'ORDER BY with_q."__cg_with_friend_prop_name" ASC, '
            'with_q."__cg_with_company_id" ASC, \'Company\' ASC, '
            'with_q."__cg_with_company_prop_name" ASC, '
            '\'WORKS_AT\' ASC',
        )

    def test_to_sql_rejects_type_aware_relational_output_mode_match_with_chain_helper_introspection(
        self,
    ) -> None:
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
                                name="KNOWS",
                                source_type="User",
                                target_type="User",
                            ),
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

    def test_to_sql_renders_type_aware_match_with_chain_complementary_helper_introspection(
        self,
    ) -> None:
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
                                name="KNOWS",
                                source_type="User",
                                target_type="User",
                            ),
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

    def test_to_sql_renders_type_aware_match_with_chain_grouped_helper_introspection(
        self,
    ) -> None:
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
                                name="KNOWS",
                                source_type="User",
                                target_type="User",
                            ),
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

    def test_to_sql_rejects_type_aware_relational_output_mode_match_with_chain_grouped_complementary_helper_introspection(
        self,
    ) -> None:
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
                                name="KNOWS",
                                source_type="User",
                                target_type="User",
                            ),
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

    def test_to_sql_renders_type_aware_match_with_chain_grouped_aggregates(self) -> None:
        sql = cypherglot.to_sql(
            (
                "MATCH (a:User)-[r:KNOWS]->(b:User)-[s:WORKS_AT]->(c:Company) "
                "WITH c.name AS company, s AS rel "
                "RETURN company, avg(rel.since) AS mean_since ORDER BY mean_since DESC, company"
            ),
            schema_context=CompilerSchemaContext.type_aware(
                GraphSchema(
                    node_types=(
                        NodeTypeSpec(name="User"),
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
                        ),
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
            sql,
            'SELECT with_q."__cg_with_scalar_company" AS "company", '
            'AVG(with_q."__cg_with_rel_prop_since") AS "mean_since" '
            'FROM (SELECT c.name AS "__cg_with_scalar_company", '
            's.id AS "__cg_with_rel_id", '
            's.from_id AS "__cg_with_rel_from_id", '
            's.to_id AS "__cg_with_rel_to_id", '
            's.since AS "__cg_with_rel_prop_since" '
            'FROM cg_edge_knows AS r '
            'JOIN cg_node_user AS a ON a.id = r.from_id '
            'JOIN cg_node_user AS b ON b.id = r.to_id '
            'JOIN cg_edge_works_at AS s ON b.id = s.from_id '
            'JOIN cg_node_company AS c ON c.id = s.to_id) AS with_q '
            'GROUP BY with_q."__cg_with_scalar_company" '
            'ORDER BY "mean_since" DESC, with_q."__cg_with_scalar_company" ASC',
        )

    def test_to_sql_renders_type_aware_relational_output_mode_bounded_variable_length_match_with_return(
        self,
    ) -> None:
        sql = cypherglot.to_sql(
            (
                "MATCH (a:User)-[:KNOWS*0..2]->(b:User) "
                "WITH b AS friend RETURN friend.name AS name ORDER BY name"
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
                    edge_types=(
                        cypherglot.EdgeTypeSpec(
                            name="KNOWS",
                            source_type="User",
                            target_type="User",
                        ),
                    ),
                ),
            ),
        )
        scalar_function_sql = cypherglot.to_sql(
            (
                "MATCH (a:User)-[:KNOWS*0..2]->(b:User) WITH b AS friend "
                "RETURN lower(friend.name) AS lower_name, toString(friend.age) AS age_text "
                "ORDER BY age_text, lower_name"
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
                    edge_types=(
                        cypherglot.EdgeTypeSpec(
                            name="KNOWS",
                            source_type="User",
                            target_type="User",
                        ),
                    ),
                ),
            ),
        )
        id_sql = cypherglot.to_sql(
            (
                "MATCH (a:User)-[:KNOWS*0..2]->(b:User) WITH b AS friend "
                "RETURN id(friend) AS friend_id ORDER BY friend_id"
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
                    edge_types=(
                        cypherglot.EdgeTypeSpec(
                            name="KNOWS",
                            source_type="User",
                            target_type="User",
                        ),
                    ),
                ),
            ),
        )

        self.assertEqual(
            sql,
            'SELECT with_q."__cg_with_friend_prop_name" AS "name" '
            'FROM (SELECT __cg_zero_hop_node.id AS "__cg_with_friend_id", '
            '__cg_zero_hop_node.name AS "__cg_with_friend_prop_name", '
            '__cg_zero_hop_node.age AS "__cg_with_friend_prop_age" '
            'FROM cg_node_user AS __cg_zero_hop_node '
            'UNION ALL SELECT b.id AS "__cg_with_friend_id", '
            'b.name AS "__cg_with_friend_prop_name", '
            'b.age AS "__cg_with_friend_prop_age" '
            'FROM cg_edge_knows AS __cg_edge_0 '
            'JOIN cg_node_user AS a ON a.id = __cg_edge_0.from_id '
            'JOIN cg_node_user AS b ON b.id = __cg_edge_0.to_id '
            'UNION ALL SELECT b.id AS "__cg_with_friend_id", '
            'b.name AS "__cg_with_friend_prop_name", '
            'b.age AS "__cg_with_friend_prop_age" '
            'FROM cg_edge_knows AS __cg_edge_0 '
            'JOIN cg_node_user AS a ON a.id = __cg_edge_0.from_id '
            'JOIN cg_node_user AS __cg_variable_hop_2_node_1 ON __cg_variable_hop_2_node_1.id = __cg_edge_0.to_id '
            'JOIN cg_edge_knows AS __cg_edge_1 ON __cg_variable_hop_2_node_1.id = __cg_edge_1.from_id '
            'JOIN cg_node_user AS b ON b.id = __cg_edge_1.to_id) AS with_q '
            'ORDER BY with_q."__cg_with_friend_prop_name" ASC',
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
                        edge_types=(
                            cypherglot.EdgeTypeSpec(
                                name="KNOWS",
                                source_type="User",
                                target_type="User",
                            ),
                        ),
                    ),
                ),
            )
        self.assertEqual(
            scalar_function_sql,
            'SELECT LOWER(with_q."__cg_with_friend_prop_name") AS "lower_name", CAST(with_q."__cg_with_friend_prop_age" AS TEXT) AS "age_text" '
            'FROM (SELECT __cg_zero_hop_node.id AS "__cg_with_friend_id", '
            '__cg_zero_hop_node.name AS "__cg_with_friend_prop_name", '
            '__cg_zero_hop_node.age AS "__cg_with_friend_prop_age" '
            'FROM cg_node_user AS __cg_zero_hop_node '
            'UNION ALL SELECT b.id AS "__cg_with_friend_id", '
            'b.name AS "__cg_with_friend_prop_name", '
            'b.age AS "__cg_with_friend_prop_age" '
            'FROM cg_edge_knows AS __cg_edge_0 '
            'JOIN cg_node_user AS a ON a.id = __cg_edge_0.from_id '
            'JOIN cg_node_user AS b ON b.id = __cg_edge_0.to_id '
            'UNION ALL SELECT b.id AS "__cg_with_friend_id", '
            'b.name AS "__cg_with_friend_prop_name", '
            'b.age AS "__cg_with_friend_prop_age" '
            'FROM cg_edge_knows AS __cg_edge_0 '
            'JOIN cg_node_user AS a ON a.id = __cg_edge_0.from_id '
            'JOIN cg_node_user AS __cg_variable_hop_2_node_1 ON __cg_variable_hop_2_node_1.id = __cg_edge_0.to_id '
            'JOIN cg_edge_knows AS __cg_edge_1 ON __cg_variable_hop_2_node_1.id = __cg_edge_1.from_id '
            'JOIN cg_node_user AS b ON b.id = __cg_edge_1.to_id) AS with_q '
            'ORDER BY CAST(with_q."__cg_with_friend_prop_age" AS TEXT) ASC, LOWER(with_q."__cg_with_friend_prop_name") ASC',
        )
        self.assertEqual(
            id_sql,
            'SELECT with_q."__cg_with_friend_id" AS "friend_id" '
            'FROM (SELECT __cg_zero_hop_node.id AS "__cg_with_friend_id", '
            '__cg_zero_hop_node.name AS "__cg_with_friend_prop_name", '
            '__cg_zero_hop_node.age AS "__cg_with_friend_prop_age" '
            'FROM cg_node_user AS __cg_zero_hop_node '
            'UNION ALL SELECT b.id AS "__cg_with_friend_id", '
            'b.name AS "__cg_with_friend_prop_name", '
            'b.age AS "__cg_with_friend_prop_age" '
            'FROM cg_edge_knows AS __cg_edge_0 '
            'JOIN cg_node_user AS a ON a.id = __cg_edge_0.from_id '
            'JOIN cg_node_user AS b ON b.id = __cg_edge_0.to_id '
            'UNION ALL SELECT b.id AS "__cg_with_friend_id", '
            'b.name AS "__cg_with_friend_prop_name", '
            'b.age AS "__cg_with_friend_prop_age" '
            'FROM cg_edge_knows AS __cg_edge_0 '
            'JOIN cg_node_user AS a ON a.id = __cg_edge_0.from_id '
            'JOIN cg_node_user AS __cg_variable_hop_2_node_1 ON __cg_variable_hop_2_node_1.id = __cg_edge_0.to_id '
            'JOIN cg_edge_knows AS __cg_edge_1 ON __cg_variable_hop_2_node_1.id = __cg_edge_1.from_id '
            'JOIN cg_node_user AS b ON b.id = __cg_edge_1.to_id) AS with_q '
            'ORDER BY with_q."__cg_with_friend_id" ASC',
        )

    def test_to_sql_renders_type_aware_relational_output_mode_bounded_variable_length_match_with_grouped_count(
        self,
    ) -> None:
        sql = cypherglot.to_sql(
            (
                "MATCH (a:User)-[:KNOWS*0..2]->(b:User) WITH b AS friend "
                "RETURN friend.name AS name, count(friend) AS total ORDER BY total DESC, name"
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
                    edge_types=(
                        cypherglot.EdgeTypeSpec(
                            name="KNOWS",
                            source_type="User",
                            target_type="User",
                        ),
                    ),
                ),
            ),
        )
        node_sql = cypherglot.to_sql(
            (
                "MATCH (a:User)-[:KNOWS*0..2]->(b:User) WITH b AS friend "
                "RETURN friend AS friend_node, count(friend) AS total ORDER BY total DESC, friend_node"
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
                    edge_types=(
                        cypherglot.EdgeTypeSpec(
                            name="KNOWS",
                            source_type="User",
                            target_type="User",
                        ),
                    ),
                ),
            ),
        )
        helper_sql = cypherglot.to_sql(
            (
                "MATCH (a:User)-[:KNOWS*0..2]->(b:User) WITH b AS friend "
                "RETURN properties(friend) AS friend_props, count(friend) AS total ORDER BY total DESC, friend_props"
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
                    edge_types=(
                        cypherglot.EdgeTypeSpec(
                            name="KNOWS",
                            source_type="User",
                            target_type="User",
                        ),
                    ),
                ),
            ),
        )
        lowered_sql = cypherglot.to_sql(
            (
                "MATCH (a:User)-[:KNOWS*0..2]->(b:User) WITH b AS friend "
                "RETURN lower(friend.name) AS lowered_name, count(friend) AS total ORDER BY total DESC, lowered_name"
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
                    edge_types=(
                        cypherglot.EdgeTypeSpec(
                            name="KNOWS",
                            source_type="User",
                            target_type="User",
                        ),
                    ),
                ),
            ),
        )
        age_text_sql = cypherglot.to_sql(
            (
                "MATCH (a:User)-[:KNOWS*0..2]->(b:User) WITH b AS friend "
                "RETURN toString(friend.age) AS age_text, count(friend) AS total ORDER BY total DESC, age_text"
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
                    edge_types=(
                        cypherglot.EdgeTypeSpec(
                            name="KNOWS",
                            source_type="User",
                            target_type="User",
                        ),
                    ),
                ),
            ),
        )
        id_sql = cypherglot.to_sql(
            (
                "MATCH (a:User)-[:KNOWS*0..2]->(b:User) WITH b AS friend "
                "RETURN id(friend) AS friend_id, count(friend) AS total ORDER BY total DESC, friend_id"
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
                    edge_types=(
                        cypherglot.EdgeTypeSpec(
                            name="KNOWS",
                            source_type="User",
                            target_type="User",
                        ),
                    ),
                ),
            ),
        )
        aggregate_sql = cypherglot.to_sql(
            (
                "MATCH (a:User)-[:KNOWS*0..2]->(b:User) WITH b AS friend "
                "RETURN friend.name AS name, sum(friend.age) AS total_age ORDER BY total_age DESC, name"
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
                    edge_types=(
                        cypherglot.EdgeTypeSpec(
                            name="KNOWS",
                            source_type="User",
                            target_type="User",
                        ),
                    ),
                ),
            ),
        )

        self.assertEqual(
            sql,
            'SELECT with_q."__cg_with_friend_prop_name" AS "name", COUNT(with_q."__cg_with_friend_id") AS "total" '
            'FROM (SELECT __cg_zero_hop_node.id AS "__cg_with_friend_id", '
            '__cg_zero_hop_node.name AS "__cg_with_friend_prop_name", '
            '__cg_zero_hop_node.age AS "__cg_with_friend_prop_age" '
            'FROM cg_node_user AS __cg_zero_hop_node '
            'UNION ALL SELECT b.id AS "__cg_with_friend_id", '
            'b.name AS "__cg_with_friend_prop_name", '
            'b.age AS "__cg_with_friend_prop_age" '
            'FROM cg_edge_knows AS __cg_edge_0 '
            'JOIN cg_node_user AS a ON a.id = __cg_edge_0.from_id '
            'JOIN cg_node_user AS b ON b.id = __cg_edge_0.to_id '
            'UNION ALL SELECT b.id AS "__cg_with_friend_id", '
            'b.name AS "__cg_with_friend_prop_name", '
            'b.age AS "__cg_with_friend_prop_age" '
            'FROM cg_edge_knows AS __cg_edge_0 '
            'JOIN cg_node_user AS a ON a.id = __cg_edge_0.from_id '
            'JOIN cg_node_user AS __cg_variable_hop_2_node_1 ON __cg_variable_hop_2_node_1.id = __cg_edge_0.to_id '
            'JOIN cg_edge_knows AS __cg_edge_1 ON __cg_variable_hop_2_node_1.id = __cg_edge_1.from_id '
            'JOIN cg_node_user AS b ON b.id = __cg_edge_1.to_id) AS with_q '
            'GROUP BY with_q."__cg_with_friend_prop_name" ORDER BY "total" DESC, with_q."__cg_with_friend_prop_name" ASC',
        )
        self.assertEqual(
            aggregate_sql,
            'SELECT with_q."__cg_with_friend_prop_name" AS "name", SUM(with_q."__cg_with_friend_prop_age") AS "total_age" '
            'FROM (SELECT __cg_zero_hop_node.id AS "__cg_with_friend_id", '
            '__cg_zero_hop_node.name AS "__cg_with_friend_prop_name", '
            '__cg_zero_hop_node.age AS "__cg_with_friend_prop_age" '
            'FROM cg_node_user AS __cg_zero_hop_node '
            'UNION ALL SELECT b.id AS "__cg_with_friend_id", '
            'b.name AS "__cg_with_friend_prop_name", '
            'b.age AS "__cg_with_friend_prop_age" '
            'FROM cg_edge_knows AS __cg_edge_0 '
            'JOIN cg_node_user AS a ON a.id = __cg_edge_0.from_id '
            'JOIN cg_node_user AS b ON b.id = __cg_edge_0.to_id '
            'UNION ALL SELECT b.id AS "__cg_with_friend_id", '
            'b.name AS "__cg_with_friend_prop_name", '
            'b.age AS "__cg_with_friend_prop_age" '
            'FROM cg_edge_knows AS __cg_edge_0 '
            'JOIN cg_node_user AS a ON a.id = __cg_edge_0.from_id '
            'JOIN cg_node_user AS __cg_variable_hop_2_node_1 ON __cg_variable_hop_2_node_1.id = __cg_edge_0.to_id '
            'JOIN cg_edge_knows AS __cg_edge_1 ON __cg_variable_hop_2_node_1.id = __cg_edge_1.from_id '
            'JOIN cg_node_user AS b ON b.id = __cg_edge_1.to_id) AS with_q '
            'GROUP BY with_q."__cg_with_friend_prop_name" ORDER BY "total_age" DESC, with_q."__cg_with_friend_prop_name" ASC',
        )
        self.assertEqual(
            node_sql,
            'SELECT with_q."__cg_with_friend_id" AS "friend_node.id", \'User\' AS "friend_node.label", with_q."__cg_with_friend_prop_name" AS "friend_node.name", with_q."__cg_with_friend_prop_age" AS "friend_node.age", COUNT(with_q."__cg_with_friend_id") AS "total" '
            'FROM (SELECT __cg_zero_hop_node.id AS "__cg_with_friend_id", __cg_zero_hop_node.name AS "__cg_with_friend_prop_name", __cg_zero_hop_node.age AS "__cg_with_friend_prop_age" FROM cg_node_user AS __cg_zero_hop_node '
            'UNION ALL SELECT b.id AS "__cg_with_friend_id", b.name AS "__cg_with_friend_prop_name", b.age AS "__cg_with_friend_prop_age" FROM cg_edge_knows AS __cg_edge_0 JOIN cg_node_user AS a ON a.id = __cg_edge_0.from_id JOIN cg_node_user AS b ON b.id = __cg_edge_0.to_id '
            'UNION ALL SELECT b.id AS "__cg_with_friend_id", b.name AS "__cg_with_friend_prop_name", b.age AS "__cg_with_friend_prop_age" FROM cg_edge_knows AS __cg_edge_0 JOIN cg_node_user AS a ON a.id = __cg_edge_0.from_id JOIN cg_node_user AS __cg_variable_hop_2_node_1 ON __cg_variable_hop_2_node_1.id = __cg_edge_0.to_id JOIN cg_edge_knows AS __cg_edge_1 ON __cg_variable_hop_2_node_1.id = __cg_edge_1.from_id JOIN cg_node_user AS b ON b.id = __cg_edge_1.to_id) AS with_q '
            'GROUP BY with_q."__cg_with_friend_id", \'User\', with_q."__cg_with_friend_prop_name", with_q."__cg_with_friend_prop_age" '
            'ORDER BY "total" DESC, with_q."__cg_with_friend_id" ASC, \'User\' ASC, with_q."__cg_with_friend_prop_name" ASC, with_q."__cg_with_friend_prop_age" ASC',
        )
        self.assertEqual(
            helper_sql,
            'SELECT with_q."__cg_with_friend_prop_name" AS "friend_props.name", with_q."__cg_with_friend_prop_age" AS "friend_props.age", COUNT(with_q."__cg_with_friend_id") AS "total" '
            'FROM (SELECT __cg_zero_hop_node.id AS "__cg_with_friend_id", __cg_zero_hop_node.name AS "__cg_with_friend_prop_name", __cg_zero_hop_node.age AS "__cg_with_friend_prop_age" FROM cg_node_user AS __cg_zero_hop_node '
            'UNION ALL SELECT b.id AS "__cg_with_friend_id", b.name AS "__cg_with_friend_prop_name", b.age AS "__cg_with_friend_prop_age" FROM cg_edge_knows AS __cg_edge_0 JOIN cg_node_user AS a ON a.id = __cg_edge_0.from_id JOIN cg_node_user AS b ON b.id = __cg_edge_0.to_id '
            'UNION ALL SELECT b.id AS "__cg_with_friend_id", b.name AS "__cg_with_friend_prop_name", b.age AS "__cg_with_friend_prop_age" FROM cg_edge_knows AS __cg_edge_0 JOIN cg_node_user AS a ON a.id = __cg_edge_0.from_id JOIN cg_node_user AS __cg_variable_hop_2_node_1 ON __cg_variable_hop_2_node_1.id = __cg_edge_0.to_id JOIN cg_edge_knows AS __cg_edge_1 ON __cg_variable_hop_2_node_1.id = __cg_edge_1.from_id JOIN cg_node_user AS b ON b.id = __cg_edge_1.to_id) AS with_q '
            'GROUP BY with_q."__cg_with_friend_prop_name", with_q."__cg_with_friend_prop_age" ORDER BY "total" DESC, with_q."__cg_with_friend_prop_name" ASC, with_q."__cg_with_friend_prop_age" ASC',
        )
        with self.assertRaisesRegex(
            ValueError,
            "relational output mode does not yet support whole-entity or introspection returns",
        ):
            cypherglot.to_sql(
                (
                    "MATCH (a:User)-[:KNOWS*0..2]->(b:User) WITH b AS friend "
                    "RETURN labels(friend) AS friend_labels, count(friend) AS total ORDER BY total DESC, friend_labels"
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
                        edge_types=(
                            cypherglot.EdgeTypeSpec(
                                name="KNOWS",
                                source_type="User",
                                target_type="User",
                            ),
                        ),
                    ),
                ),
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
                        edge_types=(
                            cypherglot.EdgeTypeSpec(
                                name="KNOWS",
                                source_type="User",
                                target_type="User",
                            ),
                        ),
                    ),
                ),
            )
        self.assertEqual(
            lowered_sql,
            'SELECT LOWER(with_q."__cg_with_friend_prop_name") AS "lowered_name", COUNT(with_q."__cg_with_friend_id") AS "total" '
            'FROM (SELECT __cg_zero_hop_node.id AS "__cg_with_friend_id", '
            '__cg_zero_hop_node.name AS "__cg_with_friend_prop_name", '
            '__cg_zero_hop_node.age AS "__cg_with_friend_prop_age" '
            'FROM cg_node_user AS __cg_zero_hop_node '
            'UNION ALL SELECT b.id AS "__cg_with_friend_id", '
            'b.name AS "__cg_with_friend_prop_name", '
            'b.age AS "__cg_with_friend_prop_age" '
            'FROM cg_edge_knows AS __cg_edge_0 '
            'JOIN cg_node_user AS a ON a.id = __cg_edge_0.from_id '
            'JOIN cg_node_user AS b ON b.id = __cg_edge_0.to_id '
            'UNION ALL SELECT b.id AS "__cg_with_friend_id", '
            'b.name AS "__cg_with_friend_prop_name", '
            'b.age AS "__cg_with_friend_prop_age" '
            'FROM cg_edge_knows AS __cg_edge_0 '
            'JOIN cg_node_user AS a ON a.id = __cg_edge_0.from_id '
            'JOIN cg_node_user AS __cg_variable_hop_2_node_1 ON __cg_variable_hop_2_node_1.id = __cg_edge_0.to_id '
            'JOIN cg_edge_knows AS __cg_edge_1 ON __cg_variable_hop_2_node_1.id = __cg_edge_1.from_id '
            'JOIN cg_node_user AS b ON b.id = __cg_edge_1.to_id) AS with_q '
            'GROUP BY LOWER(with_q."__cg_with_friend_prop_name") '
            'ORDER BY "total" DESC, LOWER(with_q."__cg_with_friend_prop_name") ASC',
        )
        self.assertEqual(
            age_text_sql,
            'SELECT CAST(with_q."__cg_with_friend_prop_age" AS TEXT) AS "age_text", COUNT(with_q."__cg_with_friend_id") AS "total" '
            'FROM (SELECT __cg_zero_hop_node.id AS "__cg_with_friend_id", '
            '__cg_zero_hop_node.name AS "__cg_with_friend_prop_name", '
            '__cg_zero_hop_node.age AS "__cg_with_friend_prop_age" '
            'FROM cg_node_user AS __cg_zero_hop_node '
            'UNION ALL SELECT b.id AS "__cg_with_friend_id", '
            'b.name AS "__cg_with_friend_prop_name", '
            'b.age AS "__cg_with_friend_prop_age" '
            'FROM cg_edge_knows AS __cg_edge_0 '
            'JOIN cg_node_user AS a ON a.id = __cg_edge_0.from_id '
            'JOIN cg_node_user AS b ON b.id = __cg_edge_0.to_id '
            'UNION ALL SELECT b.id AS "__cg_with_friend_id", '
            'b.name AS "__cg_with_friend_prop_name", '
            'b.age AS "__cg_with_friend_prop_age" '
            'FROM cg_edge_knows AS __cg_edge_0 '
            'JOIN cg_node_user AS a ON a.id = __cg_edge_0.from_id '
            'JOIN cg_node_user AS __cg_variable_hop_2_node_1 ON __cg_variable_hop_2_node_1.id = __cg_edge_0.to_id '
            'JOIN cg_edge_knows AS __cg_edge_1 ON __cg_variable_hop_2_node_1.id = __cg_edge_1.from_id '
            'JOIN cg_node_user AS b ON b.id = __cg_edge_1.to_id) AS with_q '
            'GROUP BY CAST(with_q."__cg_with_friend_prop_age" AS TEXT) '
            'ORDER BY "total" DESC, CAST(with_q."__cg_with_friend_prop_age" AS TEXT) ASC',
        )
        self.assertEqual(
            id_sql,
            'SELECT with_q."__cg_with_friend_id" AS "friend_id", COUNT(with_q."__cg_with_friend_id") AS "total" '
            'FROM (SELECT __cg_zero_hop_node.id AS "__cg_with_friend_id", '
            '__cg_zero_hop_node.name AS "__cg_with_friend_prop_name", '
            '__cg_zero_hop_node.age AS "__cg_with_friend_prop_age" '
            'FROM cg_node_user AS __cg_zero_hop_node '
            'UNION ALL SELECT b.id AS "__cg_with_friend_id", '
            'b.name AS "__cg_with_friend_prop_name", '
            'b.age AS "__cg_with_friend_prop_age" '
            'FROM cg_edge_knows AS __cg_edge_0 '
            'JOIN cg_node_user AS a ON a.id = __cg_edge_0.from_id '
            'JOIN cg_node_user AS b ON b.id = __cg_edge_0.to_id '
            'UNION ALL SELECT b.id AS "__cg_with_friend_id", '
            'b.name AS "__cg_with_friend_prop_name", '
            'b.age AS "__cg_with_friend_prop_age" '
            'FROM cg_edge_knows AS __cg_edge_0 '
            'JOIN cg_node_user AS a ON a.id = __cg_edge_0.from_id '
            'JOIN cg_node_user AS __cg_variable_hop_2_node_1 ON __cg_variable_hop_2_node_1.id = __cg_edge_0.to_id '
            'JOIN cg_edge_knows AS __cg_edge_1 ON __cg_variable_hop_2_node_1.id = __cg_edge_1.from_id '
            'JOIN cg_node_user AS b ON b.id = __cg_edge_1.to_id) AS with_q '
            'GROUP BY with_q."__cg_with_friend_id" '
            'ORDER BY "total" DESC, with_q."__cg_with_friend_id" ASC',
        )

    def test_to_sql_renders_type_aware_fixed_length_multi_hop_ungrouped_sum_aggregate(
        self,
    ) -> None:
        sql = cypherglot.to_sql(
            (
                "MATCH (a:User)-[r:KNOWS]->(b:User)-[s:WORKS_AT]->(c:Company) "
                "RETURN sum(s.since) AS total_since"
            ),
            schema_context=CompilerSchemaContext.type_aware(
                GraphSchema(
                    node_types=(
                        NodeTypeSpec(name="User"),
                        NodeTypeSpec(name="Company"),
                    ),
                    edge_types=(
                        cypherglot.EdgeTypeSpec(
                            name="KNOWS",
                            source_type="User",
                            target_type="User",
                        ),
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
            sql,
            'SELECT SUM(s.since) AS "total_since" '
            'FROM cg_edge_knows AS r '
            'JOIN cg_node_user AS a ON a.id = r.from_id '
            'JOIN cg_node_user AS b ON b.id = r.to_id '
            'JOIN cg_edge_works_at AS s ON b.id = s.from_id '
            'JOIN cg_node_company AS c ON c.id = s.to_id',
        )

    def test_to_sql_renders_type_aware_fixed_length_multi_hop_ungrouped_count_star(
        self,
    ) -> None:
        sql = cypherglot.to_sql(
            (
                "MATCH (a:User)-[r:KNOWS]->(b:User)-[s:WORKS_AT]->(c:Company) "
                "RETURN count(*) AS total"
            ),
            schema_context=CompilerSchemaContext.type_aware(
                GraphSchema(
                    node_types=(
                        NodeTypeSpec(name="User"),
                        NodeTypeSpec(name="Company"),
                    ),
                    edge_types=(
                        cypherglot.EdgeTypeSpec(
                            name="KNOWS",
                            source_type="User",
                            target_type="User",
                        ),
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
            sql,
            'SELECT COUNT(*) AS "total" '
            'FROM cg_edge_knows AS r '
            'JOIN cg_node_user AS a ON a.id = r.from_id '
            'JOIN cg_node_user AS b ON b.id = r.to_id '
            'JOIN cg_edge_works_at AS s ON b.id = s.from_id '
            'JOIN cg_node_company AS c ON c.id = s.to_id',
        )

    def test_to_sql_renders_type_aware_fixed_length_multi_hop_ungrouped_count_rel(
        self,
    ) -> None:
        sql = cypherglot.to_sql(
            (
                "MATCH (a:User)-[r:KNOWS]->(b:User)-[s:WORKS_AT]->(c:Company) "
                "RETURN count(s) AS total"
            ),
            schema_context=CompilerSchemaContext.type_aware(
                GraphSchema(
                    node_types=(
                        NodeTypeSpec(name="User"),
                        NodeTypeSpec(name="Company"),
                    ),
                    edge_types=(
                        cypherglot.EdgeTypeSpec(
                            name="KNOWS",
                            source_type="User",
                            target_type="User",
                        ),
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
            sql,
            'SELECT COUNT(s.id) AS "total" '
            'FROM cg_edge_knows AS r '
            'JOIN cg_node_user AS a ON a.id = r.from_id '
            'JOIN cg_node_user AS b ON b.id = r.to_id '
            'JOIN cg_edge_works_at AS s ON b.id = s.from_id '
            'JOIN cg_node_company AS c ON c.id = s.to_id',
        )

    def test_to_sql_renders_type_aware_fixed_length_multi_hop_grouped_count_star(
        self,
    ) -> None:
        sql = cypherglot.to_sql(
            (
                "MATCH (a:User)-[r:KNOWS]->(b:User)-[s:WORKS_AT]->(c:Company) "
                "RETURN c.name AS company, count(*) AS total ORDER BY total DESC, company"
            ),
            schema_context=CompilerSchemaContext.type_aware(
                GraphSchema(
                    node_types=(
                        NodeTypeSpec(name="User"),
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
                        ),
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
            sql,
            'SELECT c.name AS "company", COUNT(*) AS "total" '
            'FROM cg_edge_knows AS r '
            'JOIN cg_node_user AS a ON a.id = r.from_id '
            'JOIN cg_node_user AS b ON b.id = r.to_id '
            'JOIN cg_edge_works_at AS s ON b.id = s.from_id '
            'JOIN cg_node_company AS c ON c.id = s.to_id '
            'GROUP BY c.name ORDER BY "total" DESC, c.name ASC',
        )

    def test_to_sql_renders_type_aware_fixed_length_multi_hop_grouped_sum_aggregate(
        self,
    ) -> None:
        sql = cypherglot.to_sql(
            (
                "MATCH (a:User)-[r:KNOWS]->(b:User)-[s:WORKS_AT]->(c:Company) "
                "RETURN c.name AS company, sum(s.since) AS total_since "
                "ORDER BY total_since DESC, company"
            ),
            schema_context=CompilerSchemaContext.type_aware(
                GraphSchema(
                    node_types=(
                        NodeTypeSpec(name="User"),
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
                        ),
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
            sql,
            'SELECT c.name AS "company", SUM(s.since) AS "total_since" '
            'FROM cg_edge_knows AS r '
            'JOIN cg_node_user AS a ON a.id = r.from_id '
            'JOIN cg_node_user AS b ON b.id = r.to_id '
            'JOIN cg_edge_works_at AS s ON b.id = s.from_id '
            'JOIN cg_node_company AS c ON c.id = s.to_id '
            'GROUP BY c.name ORDER BY "total_since" DESC, c.name ASC',
        )

    def test_to_sql_renders_type_aware_fixed_length_multi_hop_ungrouped_min_aggregate(
        self,
    ) -> None:
        sql = cypherglot.to_sql(
            (
                "MATCH (a:User)-[r:KNOWS]->(b:User)-[s:WORKS_AT]->(c:Company) "
                "RETURN min(s.since) AS first_since"
            ),
            schema_context=CompilerSchemaContext.type_aware(
                GraphSchema(
                    node_types=(
                        NodeTypeSpec(name="User"),
                        NodeTypeSpec(name="Company"),
                    ),
                    edge_types=(
                        cypherglot.EdgeTypeSpec(
                            name="KNOWS",
                            source_type="User",
                            target_type="User",
                        ),
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
            sql,
            'SELECT MIN(s.since) AS "first_since" '
            'FROM cg_edge_knows AS r '
            'JOIN cg_node_user AS a ON a.id = r.from_id '
            'JOIN cg_node_user AS b ON b.id = r.to_id '
            'JOIN cg_edge_works_at AS s ON b.id = s.from_id '
            'JOIN cg_node_company AS c ON c.id = s.to_id',
        )

    def test_to_sql_renders_type_aware_fixed_length_multi_hop_grouped_max_aggregate(
        self,
    ) -> None:
        sql = cypherglot.to_sql(
            (
                "MATCH (a:User)-[r:KNOWS]->(b:User)-[s:WORKS_AT]->(c:Company) "
                "RETURN c.name AS company, max(s.since) AS latest_since "
                "ORDER BY latest_since DESC, company"
            ),
            schema_context=CompilerSchemaContext.type_aware(
                GraphSchema(
                    node_types=(
                        NodeTypeSpec(name="User"),
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
                        ),
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
            sql,
            'SELECT c.name AS "company", MAX(s.since) AS "latest_since" '
            'FROM cg_edge_knows AS r '
            'JOIN cg_node_user AS a ON a.id = r.from_id '
            'JOIN cg_node_user AS b ON b.id = r.to_id '
            'JOIN cg_edge_works_at AS s ON b.id = s.from_id '
            'JOIN cg_node_company AS c ON c.id = s.to_id '
            'GROUP BY c.name ORDER BY "latest_since" DESC, c.name ASC',
        )

    def test_to_sql_renders_type_aware_match_with_chain_ungrouped_max_aggregate(
        self,
    ) -> None:
        sql = cypherglot.to_sql(
            (
                "MATCH (a:User)-[r:KNOWS]->(b:User)-[s:WORKS_AT]->(c:Company) "
                "WITH s AS rel RETURN max(rel.since) AS latest_since"
            ),
            schema_context=CompilerSchemaContext.type_aware(
                GraphSchema(
                    node_types=(
                        NodeTypeSpec(name="User"),
                        NodeTypeSpec(name="Company"),
                    ),
                    edge_types=(
                        cypherglot.EdgeTypeSpec(
                            name="KNOWS",
                            source_type="User",
                            target_type="User",
                        ),
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
            sql,
            'SELECT MAX(with_q."__cg_with_rel_prop_since") AS "latest_since" '
            'FROM (SELECT s.id AS "__cg_with_rel_id", '
            's.from_id AS "__cg_with_rel_from_id", '
            's.to_id AS "__cg_with_rel_to_id", '
            's.since AS "__cg_with_rel_prop_since" '
            'FROM cg_edge_knows AS r '
            'JOIN cg_node_user AS a ON a.id = r.from_id '
            'JOIN cg_node_user AS b ON b.id = r.to_id '
            'JOIN cg_edge_works_at AS s ON b.id = s.from_id '
            'JOIN cg_node_company AS c ON c.id = s.to_id) AS with_q',
        )

    def test_to_sql_renders_type_aware_match_with_chain_ungrouped_count_star(
        self,
    ) -> None:
        sql = cypherglot.to_sql(
            (
                "MATCH (a:User)-[r:KNOWS]->(b:User)-[s:WORKS_AT]->(c:Company) "
                "WITH s AS rel RETURN count(*) AS total"
            ),
            schema_context=CompilerSchemaContext.type_aware(
                GraphSchema(
                    node_types=(
                        NodeTypeSpec(name="User"),
                        NodeTypeSpec(name="Company"),
                    ),
                    edge_types=(
                        cypherglot.EdgeTypeSpec(
                            name="KNOWS",
                            source_type="User",
                            target_type="User",
                        ),
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
            sql,
            'SELECT COUNT(*) AS "total" '
            'FROM (SELECT s.id AS "__cg_with_rel_id", '
            's.from_id AS "__cg_with_rel_from_id", '
            's.to_id AS "__cg_with_rel_to_id", '
            's.since AS "__cg_with_rel_prop_since" '
            'FROM cg_edge_knows AS r '
            'JOIN cg_node_user AS a ON a.id = r.from_id '
            'JOIN cg_node_user AS b ON b.id = r.to_id '
            'JOIN cg_edge_works_at AS s ON b.id = s.from_id '
            'JOIN cg_node_company AS c ON c.id = s.to_id) AS with_q',
        )

    def test_to_sql_renders_type_aware_match_with_chain_ungrouped_count_rel(
        self,
    ) -> None:
        sql = cypherglot.to_sql(
            (
                "MATCH (a:User)-[r:KNOWS]->(b:User)-[s:WORKS_AT]->(c:Company) "
                "WITH s AS rel RETURN count(rel) AS total"
            ),
            schema_context=CompilerSchemaContext.type_aware(
                GraphSchema(
                    node_types=(
                        NodeTypeSpec(name="User"),
                        NodeTypeSpec(name="Company"),
                    ),
                    edge_types=(
                        cypherglot.EdgeTypeSpec(
                            name="KNOWS",
                            source_type="User",
                            target_type="User",
                        ),
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
            sql,
            'SELECT COUNT(with_q."__cg_with_rel_id") AS "total" '
            'FROM (SELECT s.id AS "__cg_with_rel_id", '
            's.from_id AS "__cg_with_rel_from_id", '
            's.to_id AS "__cg_with_rel_to_id", '
            's.since AS "__cg_with_rel_prop_since" '
            'FROM cg_edge_knows AS r '
            'JOIN cg_node_user AS a ON a.id = r.from_id '
            'JOIN cg_node_user AS b ON b.id = r.to_id '
            'JOIN cg_edge_works_at AS s ON b.id = s.from_id '
            'JOIN cg_node_company AS c ON c.id = s.to_id) AS with_q',
        )

    def test_to_sql_renders_type_aware_match_with_chain_grouped_count_star(
        self,
    ) -> None:
        sql = cypherglot.to_sql(
            (
                "MATCH (a:User)-[r:KNOWS]->(b:User)-[s:WORKS_AT]->(c:Company) "
                "WITH c.name AS company, s AS rel "
                "RETURN company, count(*) AS total ORDER BY total DESC, company"
            ),
            schema_context=CompilerSchemaContext.type_aware(
                GraphSchema(
                    node_types=(
                        NodeTypeSpec(name="User"),
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
                        ),
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
            sql,
            'SELECT with_q."__cg_with_scalar_company" AS "company", COUNT(*) AS "total" '
            'FROM (SELECT c.name AS "__cg_with_scalar_company", '
            's.id AS "__cg_with_rel_id", '
            's.from_id AS "__cg_with_rel_from_id", '
            's.to_id AS "__cg_with_rel_to_id", '
            's.since AS "__cg_with_rel_prop_since" '
            'FROM cg_edge_knows AS r '
            'JOIN cg_node_user AS a ON a.id = r.from_id '
            'JOIN cg_node_user AS b ON b.id = r.to_id '
            'JOIN cg_edge_works_at AS s ON b.id = s.from_id '
            'JOIN cg_node_company AS c ON c.id = s.to_id) AS with_q '
            'GROUP BY with_q."__cg_with_scalar_company" '
            'ORDER BY "total" DESC, with_q."__cg_with_scalar_company" ASC',
        )

    def test_to_sql_renders_type_aware_match_with_chain_grouped_count_rel(
        self,
    ) -> None:
        sql = cypherglot.to_sql(
            (
                "MATCH (a:User)-[r:KNOWS]->(b:User)-[s:WORKS_AT]->(c:Company) "
                "WITH c.name AS company, s AS rel "
                "RETURN company, count(rel) AS total ORDER BY total DESC, company"
            ),
            schema_context=CompilerSchemaContext.type_aware(
                GraphSchema(
                    node_types=(
                        NodeTypeSpec(name="User"),
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
                        ),
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
            sql,
            'SELECT with_q."__cg_with_scalar_company" AS "company", '
            'COUNT(with_q."__cg_with_rel_id") AS "total" '
            'FROM (SELECT c.name AS "__cg_with_scalar_company", '
            's.id AS "__cg_with_rel_id", '
            's.from_id AS "__cg_with_rel_from_id", '
            's.to_id AS "__cg_with_rel_to_id", '
            's.since AS "__cg_with_rel_prop_since" '
            'FROM cg_edge_knows AS r '
            'JOIN cg_node_user AS a ON a.id = r.from_id '
            'JOIN cg_node_user AS b ON b.id = r.to_id '
            'JOIN cg_edge_works_at AS s ON b.id = s.from_id '
            'JOIN cg_node_company AS c ON c.id = s.to_id) AS with_q '
            'GROUP BY with_q."__cg_with_scalar_company" '
            'ORDER BY "total" DESC, with_q."__cg_with_scalar_company" ASC',
        )

    def test_to_sql_renders_type_aware_match_with_chain_ungrouped_sum_aggregate(
        self,
    ) -> None:
        sql = cypherglot.to_sql(
            (
                "MATCH (a:User)-[r:KNOWS]->(b:User)-[s:WORKS_AT]->(c:Company) "
                "WITH s AS rel RETURN sum(rel.since) AS total_since"
            ),
            schema_context=CompilerSchemaContext.type_aware(
                GraphSchema(
                    node_types=(
                        NodeTypeSpec(name="User"),
                        NodeTypeSpec(name="Company"),
                    ),
                    edge_types=(
                        cypherglot.EdgeTypeSpec(
                            name="KNOWS",
                            source_type="User",
                            target_type="User",
                        ),
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
            sql,
            'SELECT SUM(with_q."__cg_with_rel_prop_since") AS "total_since" '
            'FROM (SELECT s.id AS "__cg_with_rel_id", '
            's.from_id AS "__cg_with_rel_from_id", '
            's.to_id AS "__cg_with_rel_to_id", '
            's.since AS "__cg_with_rel_prop_since" '
            'FROM cg_edge_knows AS r '
            'JOIN cg_node_user AS a ON a.id = r.from_id '
            'JOIN cg_node_user AS b ON b.id = r.to_id '
            'JOIN cg_edge_works_at AS s ON b.id = s.from_id '
            'JOIN cg_node_company AS c ON c.id = s.to_id) AS with_q',
        )

    def test_to_sql_renders_type_aware_match_with_chain_grouped_min_aggregate(
        self,
    ) -> None:
        sql = cypherglot.to_sql(
            (
                "MATCH (a:User)-[r:KNOWS]->(b:User)-[s:WORKS_AT]->(c:Company) "
                "WITH c.name AS company, s AS rel "
                "RETURN company, min(rel.since) AS first_since ORDER BY first_since DESC, company"
            ),
            schema_context=CompilerSchemaContext.type_aware(
                GraphSchema(
                    node_types=(
                        NodeTypeSpec(name="User"),
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
                        ),
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
            sql,
            'SELECT with_q."__cg_with_scalar_company" AS "company", '
            'MIN(with_q."__cg_with_rel_prop_since") AS "first_since" '
            'FROM (SELECT c.name AS "__cg_with_scalar_company", '
            's.id AS "__cg_with_rel_id", '
            's.from_id AS "__cg_with_rel_from_id", '
            's.to_id AS "__cg_with_rel_to_id", '
            's.since AS "__cg_with_rel_prop_since" '
            'FROM cg_edge_knows AS r '
            'JOIN cg_node_user AS a ON a.id = r.from_id '
            'JOIN cg_node_user AS b ON b.id = r.to_id '
            'JOIN cg_edge_works_at AS s ON b.id = s.from_id '
            'JOIN cg_node_company AS c ON c.id = s.to_id) AS with_q '
            'GROUP BY with_q."__cg_with_scalar_company" '
            'ORDER BY "first_since" DESC, with_q."__cg_with_scalar_company" ASC',
        )

    def test_to_sql_renders_type_aware_relational_output_mode_with_chain_endpoints(
        self,
    ) -> None:
        sql = cypherglot.to_sql(
            (
                "MATCH (a:User)-[r:KNOWS]->(b:User)-[s:WORKS_AT]->(c:Company) "
                "WITH b AS friend, c AS company, s AS rel "
                "RETURN startNode(rel) AS employee, endNode(rel) AS employer, company.name AS company_name "
                "ORDER BY company_name"
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
                            name="KNOWS",
                            source_type="User",
                            target_type="User",
                        ),
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
            'SELECT with_q."__cg_with_friend_id" AS "employee.id", '
            '\'User\' AS "employee.label", '
            'with_q."__cg_with_friend_prop_name" AS "employee.name", '
            'with_q."__cg_with_company_id" AS "employer.id", '
            '\'Company\' AS "employer.label", '
            'with_q."__cg_with_company_prop_name" AS "employer.name", '
            'with_q."__cg_with_company_prop_name" AS "company_name" '
            'FROM (SELECT b.id AS "__cg_with_friend_id", '
            'b.name AS "__cg_with_friend_prop_name", '
            'c.id AS "__cg_with_company_id", '
            'c.name AS "__cg_with_company_prop_name", '
            's.id AS "__cg_with_rel_id", '
            's.from_id AS "__cg_with_rel_from_id", '
            's.to_id AS "__cg_with_rel_to_id" '
            'FROM cg_edge_knows AS r '
            'JOIN cg_node_user AS a ON a.id = r.from_id '
            'JOIN cg_node_user AS b ON b.id = r.to_id '
            'JOIN cg_edge_works_at AS s ON b.id = s.from_id '
            'JOIN cg_node_company AS c ON c.id = s.to_id) AS with_q '
            'ORDER BY with_q."__cg_with_company_prop_name" ASC',
        )

    def test_to_sql_renders_type_aware_relational_output_mode_with_chain_entities_and_properties(
        self,
    ) -> None:
        sql = cypherglot.to_sql(
            (
                "MATCH (a:User)-[r:KNOWS]->(b:User)-[s:WORKS_AT]->(c:Company) "
                "WITH b AS friend, s AS rel, c AS company "
                "RETURN friend AS employee, properties(friend) AS employee_props, "
                "rel AS job, properties(rel) AS job_props, company.name AS company_name "
                "ORDER BY company_name"
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
                            name="KNOWS",
                            source_type="User",
                            target_type="User",
                        ),
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

        self.assertEqual(
            sql,
            'SELECT with_q."__cg_with_friend_id" AS "employee.id", '
            '\'User\' AS "employee.label", '
            'with_q."__cg_with_friend_prop_name" AS "employee.name", '
            'with_q."__cg_with_friend_prop_name" AS "employee_props.name", '
            'with_q."__cg_with_rel_id" AS "job.id", '
            '\'WORKS_AT\' AS "job.type", '
            'with_q."__cg_with_rel_from_id" AS "job.from_id", '
            'with_q."__cg_with_rel_to_id" AS "job.to_id", '
            'with_q."__cg_with_rel_prop_since" AS "job.since", '
            'with_q."__cg_with_rel_prop_since" AS "job_props.since", '
            'with_q."__cg_with_company_prop_name" AS "company_name" '
            'FROM (SELECT b.id AS "__cg_with_friend_id", '
            'b.name AS "__cg_with_friend_prop_name", '
            's.id AS "__cg_with_rel_id", '
            's.from_id AS "__cg_with_rel_from_id", '
            's.to_id AS "__cg_with_rel_to_id", '
            's.since AS "__cg_with_rel_prop_since", '
            'c.id AS "__cg_with_company_id", '
            'c.name AS "__cg_with_company_prop_name" '
            'FROM cg_edge_knows AS r '
            'JOIN cg_node_user AS a ON a.id = r.from_id '
            'JOIN cg_node_user AS b ON b.id = r.to_id '
            'JOIN cg_edge_works_at AS s ON b.id = s.from_id '
            'JOIN cg_node_company AS c ON c.id = s.to_id) AS with_q '
            'ORDER BY with_q."__cg_with_company_prop_name" ASC',
        )

    def test_to_sql_renders_type_aware_relational_output_mode_grouped_with_chain_entities_and_properties(
        self,
    ) -> None:
        sql = cypherglot.to_sql(
            (
                "MATCH (a:User)-[r:KNOWS]->(b:User)-[s:WORKS_AT]->(c:Company) "
                "WITH b AS friend, s AS rel "
                "RETURN friend AS employee, properties(friend) AS employee_props, "
                "rel AS job, properties(rel) AS job_props, count(rel) AS total "
                "ORDER BY total DESC"
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
                            name="KNOWS",
                            source_type="User",
                            target_type="User",
                        ),
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

        self.assertEqual(
            sql,
            'SELECT with_q."__cg_with_friend_id" AS "employee.id", '
            '\'User\' AS "employee.label", '
            'with_q."__cg_with_friend_prop_name" AS "employee.name", '
            'with_q."__cg_with_friend_prop_name" AS "employee_props.name", '
            'with_q."__cg_with_rel_id" AS "job.id", '
            '\'WORKS_AT\' AS "job.type", '
            'with_q."__cg_with_rel_from_id" AS "job.from_id", '
            'with_q."__cg_with_rel_to_id" AS "job.to_id", '
            'with_q."__cg_with_rel_prop_since" AS "job.since", '
            'with_q."__cg_with_rel_prop_since" AS "job_props.since", '
            'COUNT(with_q."__cg_with_rel_id") AS "total" '
            'FROM (SELECT b.id AS "__cg_with_friend_id", '
            'b.name AS "__cg_with_friend_prop_name", '
            's.id AS "__cg_with_rel_id", '
            's.from_id AS "__cg_with_rel_from_id", '
            's.to_id AS "__cg_with_rel_to_id", '
            's.since AS "__cg_with_rel_prop_since" '
            'FROM cg_edge_knows AS r '
            'JOIN cg_node_user AS a ON a.id = r.from_id '
            'JOIN cg_node_user AS b ON b.id = r.to_id '
            'JOIN cg_edge_works_at AS s ON b.id = s.from_id '
            'JOIN cg_node_company AS c ON c.id = s.to_id) AS with_q '
            'GROUP BY with_q."__cg_with_friend_id", \'User\', '
            'with_q."__cg_with_friend_prop_name", '
            'with_q."__cg_with_friend_prop_name", '
            'with_q."__cg_with_rel_id", \'WORKS_AT\', '
            'with_q."__cg_with_rel_from_id", with_q."__cg_with_rel_to_id", '
            'with_q."__cg_with_rel_prop_since", '
            'with_q."__cg_with_rel_prop_since" ORDER BY "total" DESC',
        )

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

    def test_to_sql_renders_type_aware_optional_match_node_scalar_return(self) -> None:
        sql = cypherglot.to_sql(
            (
                "OPTIONAL MATCH (u:User) WHERE u.name = 'Alice' "
                "RETURN u.name AS name ORDER BY name LIMIT 1"
            ),
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

        self.assertEqual(
            sql,
            'SELECT u.name AS "name" '
            'FROM (SELECT 1 AS __cg_seed) AS seed '
            'LEFT JOIN cg_node_user AS u ON 1 = 1 AND u.name = \'Alice\' '
            'ORDER BY u.name ASC LIMIT 1',
        )

    def test_to_sql_renders_type_aware_optional_match_node_relational_entity_return(self) -> None:
        sql = cypherglot.to_sql(
            (
                "OPTIONAL MATCH (u:User) RETURN u AS user, count(u) AS total "
                "ORDER BY total DESC"
            ),
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

        self.assertEqual(
            sql,
            'SELECT u.id AS "user.id", \'User\' AS "user.label", '
            'u.name AS "user.name", COUNT(u.id) AS "total" '
            'FROM (SELECT 1 AS __cg_seed) AS seed '
            'LEFT JOIN cg_node_user AS u ON 1 = 1 '
            'GROUP BY u.id, \'User\', u.name ORDER BY "total" DESC',
        )

    def test_to_sqlglot_ast_returns_expression(self) -> None:
        expression = cypherglot.to_sqlglot_ast(
            "MATCH (u:User) RETURN u.name ORDER BY u.name LIMIT 1",
            schema_context=_public_api_schema_context(),
        )

        self.assertEqual(
            expression.sql(),
            'SELECT u.name AS "u.name" FROM cg_node_user AS u ORDER BY u.name ASC LIMIT 1',
        )

    def test_to_sql_renders_single_statement_shape(self) -> None:
        sql = cypherglot.to_sql(
            "MATCH (u:User) WHERE u.name = $name RETURN u.name LIMIT 1",
            schema_context=_public_api_schema_context(),
        )

        self.assertEqual(
            sql,
            'SELECT u.name AS "u.name" FROM cg_node_user AS u WHERE u.name = :name LIMIT 1',
        )

    def test_to_sql_defaults_to_type_aware_relational_entity_rendering(self) -> None:
        sql = cypherglot.to_sql(
            "MATCH (u:User) RETURN u AS user",
            schema_context=_public_api_schema_context(),
        )

        self.assertEqual(
            sql,
            'SELECT u.id AS "user.id", \'User\' AS "user.label", '
            'u.name AS "user.name", u.age AS "user.age", '
            'u.score AS "user.score", u.active AS "user.active" '
            'FROM cg_node_user AS u',
        )

    def test_to_sql_renders_duckdb_string_property_access(self) -> None:
        sql = cypherglot.to_sql(
            "MATCH (u:User) RETURN u.name ORDER BY u.name",
            dialect="duckdb",
            schema_context=_public_api_schema_context(),
        )

        self.assertEqual(
            sql,
            'SELECT u.name AS "u.name" FROM cg_node_user AS u ORDER BY u.name ASC NULLS FIRST',
        )

    def test_to_sql_renders_duckdb_numeric_aggregate_cast(self) -> None:
        sql = cypherglot.to_sql(
            "MATCH (u:User) RETURN sum(u.age) AS total",
            dialect="duckdb",
            schema_context=_public_api_schema_context(),
        )

        self.assertEqual(
            sql,
            'SELECT SUM(TRY_CAST(u.age AS DOUBLE)) AS "total" FROM cg_node_user AS u',
        )

    def test_to_sql_renders_duckdb_numeric_ordering_dual_keys(self) -> None:
        sql = cypherglot.to_sql(
            "MATCH (u:User) RETURN u.age AS age ORDER BY age ASC",
            dialect="duckdb",
            schema_context=_public_api_schema_context(),
        )

        self.assertEqual(
            sql,
            'SELECT u.age AS "age" FROM cg_node_user AS u ORDER BY u.age ASC NULLS FIRST',
        )

    def test_to_sql_renders_duckdb_min_over_property_with_ordered_first(self) -> None:
        sql = cypherglot.to_sql(
            "MATCH (u:User) RETURN min(u.age) AS min_age",
            dialect="duckdb",
            schema_context=_public_api_schema_context(),
        )

        self.assertEqual(
            sql,
            'SELECT MIN(u.age) AS "min_age" FROM cg_node_user AS u',
        )

    def test_to_sql_renders_duckdb_without_constant_order_by(self) -> None:
        sql = cypherglot.to_sql(
            "OPTIONAL MATCH (u:User) RETURN $value AS value ORDER BY value",
            dialect="duckdb",
            schema_context=_public_api_schema_context(),
        )

        self.assertEqual(
            sql,
            'SELECT $value AS "value" FROM (SELECT 1 AS __cg_seed) AS seed LEFT JOIN cg_node_user AS u ON 1 = 1',
        )

    def test_to_sql_renders_duckdb_without_mixed_constant_order_by(self) -> None:
        sql = cypherglot.to_sql(
            "MATCH (u:User) WITH u AS person RETURN 'tag' AS tag, $value AS value ORDER BY tag, value",
            dialect="duckdb",
            schema_context=_public_api_schema_context(),
        )

        self.assertEqual(
            sql,
            'SELECT \'tag\' AS "tag", $value AS "value" FROM (SELECT u.id AS "__cg_with_person_id", u.name AS "__cg_with_person_prop_name", u.age AS "__cg_with_person_prop_age", u.score AS "__cg_with_person_prop_score", u.active AS "__cg_with_person_prop_active" FROM cg_node_user AS u) AS with_q',
        )

    def test_to_sql_renders_duckdb_numeric_cast_on_rebound_alias(self) -> None:
        sql = cypherglot.to_sql(
            "MATCH (u:User) WITH u AS person, u.age AS age RETURN abs(person.age) AS magnitude, abs(age) AS rebound, abs(-3) AS lit ORDER BY magnitude, rebound, lit",
            dialect="duckdb",
            schema_context=_public_api_schema_context(),
        )

        self.assertEqual(
            sql,
            'SELECT ABS(TRY_CAST(with_q."__cg_with_person_prop_age" AS DOUBLE)) AS "magnitude", ABS(TRY_CAST(with_q."__cg_with_scalar_age" AS DOUBLE)) AS "rebound", ABS(-3) AS "lit" FROM (SELECT u.id AS "__cg_with_person_id", u.name AS "__cg_with_person_prop_name", u.age AS "__cg_with_person_prop_age", u.score AS "__cg_with_person_prop_score", u.active AS "__cg_with_person_prop_active", u.age AS "__cg_with_scalar_age" FROM cg_node_user AS u) AS with_q ORDER BY ABS(TRY_CAST(with_q."__cg_with_person_prop_age" AS DOUBLE)) ASC NULLS FIRST, ABS(TRY_CAST(with_q."__cg_with_scalar_age" AS DOUBLE)) ASC NULLS FIRST, ABS(-3) ASC NULLS FIRST',
        )

    def test_to_sql_renders_duckdb_length_cast_for_id_size(self) -> None:
        sql = cypherglot.to_sql(
            "MATCH (u:User) RETURN size(id(u)) AS id_len ORDER BY id_len",
            dialect="duckdb",
            schema_context=_public_api_schema_context(),
        )

        self.assertEqual(
            sql,
            'SELECT LENGTH(CAST(u.id AS TEXT)) AS "id_len" FROM cg_node_user AS u ORDER BY LENGTH(CAST(u.id AS TEXT)) ASC NULLS FIRST',
        )

    def test_to_sql_renders_duckdb_avg_cast_on_with_scalar_binding(self) -> None:
        sql = cypherglot.to_sql(
            "MATCH (u:User) WITH u.name AS name, u.score AS score RETURN name, avg(score) AS mean ORDER BY mean DESC, name ASC",
            dialect="duckdb",
            schema_context=_public_api_schema_context(),
        )

        self.assertEqual(
            sql,
            'SELECT with_q."__cg_with_scalar_name" AS "name", AVG(TRY_CAST(with_q."__cg_with_scalar_score" AS DOUBLE)) AS "mean" FROM (SELECT u.name AS "__cg_with_scalar_name", u.score AS "__cg_with_scalar_score" FROM cg_node_user AS u) AS with_q GROUP BY with_q."__cg_with_scalar_name" ORDER BY "mean" DESC, with_q."__cg_with_scalar_name" ASC NULLS FIRST',
        )

    def test_to_sql_renders_with_order_by_projected_expression_without_alias(self) -> None:
        sql = cypherglot.to_sql(
            "MATCH (u:User) WITH u AS person, u.name AS name RETURN left(person.name, 2), right(name, 2) ORDER BY left(person.name, 2), right(name, 2)",
            schema_context=_public_api_schema_context(),
        )

        self.assertEqual(
            sql,
            'SELECT LEFT(with_q."__cg_with_person_prop_name", 2) AS "left(person.name, 2)", RIGHT(with_q."__cg_with_scalar_name", 2) AS "right(name, 2)" FROM (SELECT u.id AS "__cg_with_person_id", u.name AS "__cg_with_person_prop_name", u.age AS "__cg_with_person_prop_age", u.score AS "__cg_with_person_prop_score", u.active AS "__cg_with_person_prop_active", u.name AS "__cg_with_scalar_name" FROM cg_node_user AS u) AS with_q ORDER BY LEFT(with_q."__cg_with_person_prop_name", 2) ASC, RIGHT(with_q."__cg_with_scalar_name", 2) ASC',
        )

    def test_to_sql_renders_duckdb_truncating_integer_cast(self) -> None:
        sql = cypherglot.to_sql(
            "MATCH (a:User)-[r:KNOWS]->(b:User) RETURN toInteger(r.score) AS score_int ORDER BY score_int",
            dialect="duckdb",
            schema_context=_public_api_schema_context(),
        )

        self.assertEqual(
            sql,
            'SELECT CAST(TRUNC(TRY_CAST(r.score AS DOUBLE)) AS INT) AS "score_int" FROM cg_edge_knows AS r JOIN cg_node_user AS a ON a.id = r.from_id JOIN cg_node_user AS b ON b.id = r.to_id ORDER BY CAST(TRUNC(TRY_CAST(r.score AS DOUBLE)) AS INT) ASC NULLS FIRST',
        )

    def test_to_sql_rejects_multi_step_shape(self) -> None:
        with self.assertRaisesRegex(ValueError, "multi-step SQL program"):
            cypherglot.to_sql(
                "CREATE (:User {name: 'Alice'})",
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

    def test_to_sqlglot_program_returns_program(self) -> None:
        program = cypherglot.to_sqlglot_program(
            "CREATE (:User {name: 'Alice'})",
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

        self.assertEqual(len(program.steps), 1)
        self.assertIsInstance(program, cypherglot.CompiledCypherProgram)

    def test_render_cypher_program_text_preserves_structure(self) -> None:
        rendered = cypherglot.render_cypher_program_text(
            "MATCH (x:Begin) CREATE (x)-[:TYPE]->(:End {name: 'finish'})",
            schema_context=CompilerSchemaContext.type_aware(
                GraphSchema(
                    node_types=(
                        NodeTypeSpec(name="Begin"),
                        NodeTypeSpec(
                            name="End",
                            properties=(PropertyField("name", "string"),),
                        ),
                    ),
                    edge_types=(
                        cypherglot.EdgeTypeSpec(
                            name="TYPE",
                            source_type="Begin",
                            target_type="End",
                        ),
                    ),
                )
            ),
        )
        type_aware_rendered = cypherglot.render_cypher_program_text(
            "MATCH (x:Begin) CREATE (x)-[:TYPE]->(:End {name: 'finish'})",
            schema_context=CompilerSchemaContext.type_aware(
                GraphSchema(
                    node_types=(
                        NodeTypeSpec(name="Begin"),
                        NodeTypeSpec(
                            name="End",
                            properties=(PropertyField("name", "string"),),
                        ),
                    ),
                    edge_types=(
                        cypherglot.EdgeTypeSpec(
                            name="TYPE",
                            source_type="Begin",
                            target_type="End",
                        ),
                    ),
                )
            ),
        )

        self.assertEqual(len(rendered.steps), 1)
        loop = rendered.steps[0]
        self.assertIsInstance(loop, cypherglot.RenderedCypherLoop)
        assert isinstance(loop, cypherglot.RenderedCypherLoop)

        self.assertEqual(loop.row_bindings, ("match_node_id",))
        self.assertEqual(loop.source, "SELECT x.id AS match_node_id FROM cg_node_begin AS x")
        self.assertEqual(loop.body[0].bind_columns, ("created_node_id",))
        self.assertEqual(
            loop.body[1].sql,
            "INSERT INTO cg_edge_type (from_id, to_id) VALUES (:match_node_id, :created_node_id)",
        )

        type_aware_loop = type_aware_rendered.steps[0]
        self.assertIsInstance(type_aware_loop, cypherglot.RenderedCypherLoop)
        assert isinstance(type_aware_loop, cypherglot.RenderedCypherLoop)
        self.assertEqual(
            type_aware_loop.source,
            "SELECT x.id AS match_node_id FROM cg_node_begin AS x",
        )
        self.assertEqual(
            type_aware_loop.body[0].sql,
            "INSERT INTO cg_node_end (name) VALUES ('finish') RETURNING id",
        )
        self.assertEqual(
            type_aware_loop.body[1].sql,
            "INSERT INTO cg_edge_type (from_id, to_id) VALUES (:match_node_id, :created_node_id)",
        )
