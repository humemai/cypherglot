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
            backend="sqlite",
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

    def test_to_sql_rejects_non_scalar_packaging_returns(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            (
                "relational output mode does not yet support whole-entity or "
                "introspection returns"
            ),
        ):
            cypherglot.to_sql(
                "MATCH (u:User) RETURN labels(u) AS labels, keys(u) AS keys",
                backend="sqlite",
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
            backend="sqlite",
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
                backend="sqlite",
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
                backend="sqlite",
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
                backend="sqlite",
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
            backend="sqlite",
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
            backend="sqlite",
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
            backend="sqlite",
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
            backend="sqlite",
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
            backend="sqlite",
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
            backend="sqlite",
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
            backend="sqlite",
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
            backend="sqlite",
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
            backend="sqlite",
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
            backend="sqlite",
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
            backend="sqlite",
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
            backend="sqlite",
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
            backend="sqlite",
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
            backend="sqlite",
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
            backend="sqlite",
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
            backend="sqlite",
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
            'SELECT LOWER(u.name) AS "lower_name", LENGTH(CAST(u.name AS TEXT)) AS "name_len", '
            'CAST(u.age AS TEXT) AS "age_text", COALESCE(u.name, \'unknown\') AS "display_name" '
            'FROM cg_node_user AS u ORDER BY LOWER(u.name) ASC, LENGTH(CAST(u.name AS TEXT)) ASC, '
            'CAST(u.age AS TEXT) ASC, COALESCE(u.name, \'unknown\') ASC',
        )

    def test_to_sql_renders_type_aware_direct_relationship_aggregates(self) -> None:
        sql = cypherglot.to_sql(
            (
                "MATCH (a:User)-[r:WORKS_AT]->(b:Company) "
                "RETURN a.name AS name, count(r) AS total ORDER BY total DESC"
            ),
            backend="sqlite",
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
            backend="sqlite",
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
            'SELECT LOWER(r.note) AS "lower_note", LENGTH(CAST(\'WORKS_AT\' AS TEXT)) AS "type_len", '
            'CAST(r.since AS TEXT) AS "since_text" FROM cg_edge_works_at AS r '
            'JOIN cg_node_user AS a ON a.id = r.from_id '
            'JOIN cg_node_company AS b ON b.id = r.to_id '
            'ORDER BY LOWER(r.note) ASC, LENGTH(CAST(\'WORKS_AT\' AS TEXT)) ASC, CAST(r.since AS TEXT) ASC',
        )

    def test_to_sql_renders_type_aware_match_with_return(self) -> None:
        sql = cypherglot.to_sql(
            (
                "MATCH (u:User) WITH u AS person, u.name AS name "
                "WHERE name = $name RETURN person.name AS display_name, name "
                "ORDER BY display_name, name"
            ),
            backend="sqlite",
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

