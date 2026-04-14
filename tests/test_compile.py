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


class CompileTests(unittest.TestCase):

    def test_compile_type_aware_relational_output_mode_rejects_with_labels_and_keys(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            "relational output mode does not yet support whole-entity or introspection returns",
        ):
            cypherglot.compile_cypher_text(
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

    def test_compile_type_aware_relational_output_mode_rejects_direct_chain_labels_and_keys(
        self,
    ) -> None:
        with self.assertRaisesRegex(
            ValueError,
            "relational output mode does not yet support whole-entity or introspection returns",
        ):
            cypherglot.compile_cypher_text(
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

    def test_compile_type_aware_relational_output_mode_rejects_with_chain_labels_and_keys(
        self,
    ) -> None:
        with self.assertRaisesRegex(
            ValueError,
            "relational output mode does not yet support whole-entity or introspection returns",
        ):
            cypherglot.compile_cypher_text(
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

    def test_compile_type_aware_relational_output_mode_expands_direct_relationship_entity_and_properties(self) -> None:
        expression = cypherglot.compile_cypher_text(
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
            expression.sql(),
            'SELECT r.id AS "rel.id", \'WORKS_AT\' AS "rel.type", '
            'r.from_id AS "rel.from_id", r.to_id AS "rel.to_id", '
            'r.since AS "rel.since", r.since AS "props.since", '
            'b.name AS "company" FROM cg_edge_works_at AS r '
            'JOIN cg_node_user AS a ON a.id = r.from_id '
            'JOIN cg_node_company AS b ON b.id = r.to_id '
            'ORDER BY b.name ASC',
        )

    def test_compile_type_aware_relational_output_mode_expands_direct_endpoint_entities(self) -> None:
        expression = cypherglot.compile_cypher_text(
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
            expression.sql(),
            'SELECT a.id AS "start.id", \'User\' AS "start.label", '
            'a.name AS "start.name", '
            'b.id AS "ending.id", \'Company\' AS "ending.label", '
            'b.name AS "ending.name", '
            'r.since AS "since" FROM cg_edge_works_at AS r '
            'JOIN cg_node_user AS a ON a.id = r.from_id '
            'JOIN cg_node_company AS b ON b.id = r.to_id '
            'ORDER BY r.since ASC',
        )

    def test_compile_type_aware_relational_output_mode_groups_direct_node_entity(self) -> None:
        expression = cypherglot.compile_cypher_text(
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
            expression.sql(),
            'SELECT u.id AS "user.id", \'User\' AS "user.label", '
            'u.name AS "user.name", COUNT(u.id) AS "total" '
            'FROM cg_node_user AS u '
            'GROUP BY u.id, \'User\', u.name ORDER BY "total" DESC',
        )

    def test_compile_type_aware_relational_output_mode_groups_direct_relationship_entity(self) -> None:
        expression = cypherglot.compile_cypher_text(
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
            expression.sql(),
            'SELECT r.id AS "rel.id", \'WORKS_AT\' AS "rel.type", '
            'r.from_id AS "rel.from_id", r.to_id AS "rel.to_id", '
            'r.since AS "rel.since", COUNT(r.id) AS "total" '
            'FROM cg_edge_works_at AS r '
            'JOIN cg_node_user AS a ON a.id = r.from_id '
            'JOIN cg_node_company AS b ON b.id = r.to_id '
            'GROUP BY r.id, \'WORKS_AT\', r.from_id, r.to_id, r.since '
            'ORDER BY "total" DESC',
        )

    def test_compile_type_aware_relational_output_mode_groups_direct_endpoint_entities(self) -> None:
        expression = cypherglot.compile_cypher_text(
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
            expression.sql(),
            'SELECT a.id AS "start.id", \'User\' AS "start.label", '
            'a.name AS "start.name", b.id AS "ending.id", '
            '\'Company\' AS "ending.label", b.name AS "ending.name", '
            'COUNT(r.id) AS "total" FROM cg_edge_works_at AS r '
            'JOIN cg_node_user AS a ON a.id = r.from_id '
            'JOIN cg_node_company AS b ON b.id = r.to_id '
            'GROUP BY a.id, \'User\', a.name, b.id, \'Company\', b.name '
            'ORDER BY "total" DESC',
        )

    def test_compile_type_aware_relational_output_mode_expands_with_node_entity_and_properties(self) -> None:
        expression = cypherglot.compile_cypher_text(
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
            expression.sql(),
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

    def test_compile_type_aware_relational_output_mode_expands_with_relationship_entity_and_properties(self) -> None:
        expression = cypherglot.compile_cypher_text(
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
            expression.sql(),
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

    def test_compile_type_aware_relational_output_mode_expands_with_endpoint_entities(self) -> None:
        expression = cypherglot.compile_cypher_text(
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
            expression.sql(),
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

    def test_compile_type_aware_relational_output_mode_orders_by_with_projected_entity_aliases(
        self,
    ) -> None:
        expression = cypherglot.compile_cypher_text(
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
            expression.sql(),
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

    def test_compile_type_aware_relational_output_mode_groups_with_node_entity(self) -> None:
        expression = cypherglot.compile_cypher_text(
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
            expression.sql(),
            "SELECT with_q.\"__cg_with_person_id\" AS \"user.id\", 'User' AS \"user.label\", "
            "with_q.\"__cg_with_person_prop_name\" AS \"user.name\", "
            "COUNT(with_q.\"__cg_with_person_id\") AS \"total\" "
            "FROM (SELECT u.id AS \"__cg_with_person_id\", "
            "u.name AS \"__cg_with_person_prop_name\" "
            "FROM cg_node_user AS u) AS with_q "
            "GROUP BY with_q.\"__cg_with_person_id\", 'User', "
            "with_q.\"__cg_with_person_prop_name\" ORDER BY \"total\" DESC",
        )

    def test_compile_type_aware_relational_output_mode_groups_with_relationship_entity(self) -> None:
        expression = cypherglot.compile_cypher_text(
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
            expression.sql(),
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
    def test_compile_type_aware_match_node_return_field(self) -> None:
        expression = cypherglot.compile_cypher_text(
            "MATCH (u:User) RETURN u.name ORDER BY u.name LIMIT 1",
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
            expression.sql(),
            'SELECT u.name AS "u.name" FROM cg_node_user AS u '
            'ORDER BY u.name ASC LIMIT 1',
        )

    def test_compile_type_aware_match_node_direct_aggregates(self) -> None:
        count_expression = cypherglot.compile_cypher_text(
            "MATCH (u:User) RETURN count(u) AS total",
            schema_context=CompilerSchemaContext.type_aware(
                GraphSchema(
                    node_types=(NodeTypeSpec(name="User"),),
                    edge_types=(),
                )
            ),
        )
        grouped_expression = cypherglot.compile_cypher_text(
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
            count_expression.sql(),
            'SELECT COUNT(u.id) AS "total" FROM cg_node_user AS u',
        )
        self.assertEqual(
            grouped_expression.sql(),
            'SELECT u.name AS "name", AVG(u.score) AS "mean" '
            'FROM cg_node_user AS u GROUP BY u.name ORDER BY "mean" DESC',
        )

    def test_compile_type_aware_match_node_direct_scalar_functions(self) -> None:
        expression = cypherglot.compile_cypher_text(
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
            expression.sql(),
            'SELECT LOWER(u.name) AS "lower_name", LENGTH(u.name) AS "name_len", '
            'CAST(u.age AS TEXT) AS "age_text", COALESCE(u.name, \'unknown\') AS "display_name" '
            'FROM cg_node_user AS u ORDER BY LOWER(u.name) ASC, LENGTH(u.name) ASC, '
            'CAST(u.age AS TEXT) ASC, COALESCE(u.name, \'unknown\') ASC',
        )

    def test_compile_type_aware_match_relationship_return_fields(self) -> None:
        expression = cypherglot.compile_cypher_text(
            (
                "MATCH (a:User)-[r:WORKS_AT]->(b:Company) "
                "RETURN a.name AS user_name, r.since AS since, b.name AS company "
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
            expression.sql(),
            'SELECT a.name AS "user_name", r.since AS "since", '
            'b.name AS "company" FROM cg_edge_works_at AS r '
            'JOIN cg_node_user AS a ON a.id = r.from_id '
            'JOIN cg_node_company AS b ON b.id = r.to_id '
            'ORDER BY b.name ASC',
        )

    def test_compile_type_aware_match_relationship_self_loop_return_fields(self) -> None:
        expression = cypherglot.compile_cypher_text(
            (
                "MATCH (u:User)-[r:KNOWS]->(u:User) "
                "RETURN u.name AS user_name ORDER BY user_name"
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
                )
            ),
        )

        self.assertEqual(
            expression.sql(),
            'SELECT u.name AS "user_name" FROM cg_edge_knows AS r '
            'JOIN cg_node_user AS u ON u.id = r.from_id '
            'WHERE r.from_id = r.to_id ORDER BY u.name ASC',
        )

    def test_compile_type_aware_match_relationship_direct_aggregates(self) -> None:
        grouped_expression = cypherglot.compile_cypher_text(
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
            grouped_expression.sql(),
            'SELECT a.name AS "name", COUNT(r.id) AS "total" '
            'FROM cg_edge_works_at AS r '
            'JOIN cg_node_user AS a ON a.id = r.from_id '
            'JOIN cg_node_company AS b ON b.id = r.to_id '
            'GROUP BY a.name ORDER BY "total" DESC',
        )

    def test_compile_type_aware_match_relationship_direct_scalar_functions(self) -> None:
        expression = cypherglot.compile_cypher_text(
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
            expression.sql(),
            'SELECT LOWER(r.note) AS "lower_note", LENGTH(\'WORKS_AT\') AS "type_len", '
            'CAST(r.since AS TEXT) AS "since_text" FROM cg_edge_works_at AS r '
            'JOIN cg_node_user AS a ON a.id = r.from_id '
            'JOIN cg_node_company AS b ON b.id = r.to_id '
            'ORDER BY LOWER(r.note) ASC, LENGTH(\'WORKS_AT\') ASC, CAST(r.since AS TEXT) ASC',
        )

    def test_compile_type_aware_fixed_length_multi_hop_match(self) -> None:
        expression = cypherglot.compile_cypher_text(
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
            expression.sql(),
            'SELECT a.name AS "user_name", c.name AS "company" '
            'FROM cg_edge_knows AS r '
            'JOIN cg_node_user AS a ON a.id = r.from_id '
            'JOIN cg_node_user AS b ON b.id = r.to_id '
            'JOIN cg_edge_works_at AS s ON b.id = s.from_id '
            'JOIN cg_node_company AS c ON c.id = s.to_id '
            'ORDER BY c.name ASC',
        )

    def test_compile_type_aware_relational_output_mode_bounded_variable_length_match(
        self,
    ) -> None:
        expression = cypherglot.compile_cypher_text(
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
        aggregate_expression = cypherglot.compile_cypher_text(
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
        scalar_function_expression = cypherglot.compile_cypher_text(
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
        id_expression = cypherglot.compile_cypher_text(
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
            expression.sql(),
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
            aggregate_expression.sql(),
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
            cypherglot.compile_cypher_text(
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
            scalar_function_expression.sql(),
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
            id_expression.sql(),
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

    def test_compile_type_aware_relational_output_mode_bounded_variable_length_match_grouped_count(
        self,
    ) -> None:
        expression = cypherglot.compile_cypher_text(
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
        node_expression = cypherglot.compile_cypher_text(
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
        helper_expression = cypherglot.compile_cypher_text(
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
        lowered_expression = cypherglot.compile_cypher_text(
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
        age_text_expression = cypherglot.compile_cypher_text(
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
        id_expression = cypherglot.compile_cypher_text(
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
            expression.sql(),
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
            node_expression.sql(),
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
            helper_expression.sql(),
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
            cypherglot.compile_cypher_text(
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
            cypherglot.compile_cypher_text(
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
            lowered_expression.sql(),
            'SELECT variable_length_q."lowered_name" AS "lowered_name", COUNT(variable_length_q."__cg_aggregate_1") AS "total" '
            'FROM (SELECT LOWER(__cg_zero_hop_node.name) AS "lowered_name", __cg_zero_hop_node.id AS "__cg_aggregate_1" FROM cg_node_user AS __cg_zero_hop_node '
            'UNION ALL SELECT LOWER(b.name) AS "lowered_name", b.id AS "__cg_aggregate_1" FROM cg_edge_knows AS __cg_edge_0 JOIN cg_node_user AS a ON a.id = __cg_edge_0.from_id JOIN cg_node_user AS b ON b.id = __cg_edge_0.to_id '
            'UNION ALL SELECT LOWER(b.name) AS "lowered_name", b.id AS "__cg_aggregate_1" FROM cg_edge_knows AS __cg_edge_0 JOIN cg_node_user AS a ON a.id = __cg_edge_0.from_id JOIN cg_node_user AS __cg_variable_hop_2_node_1 ON __cg_variable_hop_2_node_1.id = __cg_edge_0.to_id JOIN cg_edge_knows AS __cg_edge_1 ON __cg_variable_hop_2_node_1.id = __cg_edge_1.from_id JOIN cg_node_user AS b ON b.id = __cg_edge_1.to_id) AS variable_length_q '
            'GROUP BY variable_length_q."lowered_name" ORDER BY "total" DESC, variable_length_q."lowered_name" ASC',
        )
        self.assertEqual(
            age_text_expression.sql(),
            'SELECT variable_length_q."age_text" AS "age_text", COUNT(variable_length_q."__cg_aggregate_1") AS "total" '
            'FROM (SELECT CAST(__cg_zero_hop_node.age AS TEXT) AS "age_text", __cg_zero_hop_node.id AS "__cg_aggregate_1" FROM cg_node_user AS __cg_zero_hop_node '
            'UNION ALL SELECT CAST(b.age AS TEXT) AS "age_text", b.id AS "__cg_aggregate_1" FROM cg_edge_knows AS __cg_edge_0 JOIN cg_node_user AS a ON a.id = __cg_edge_0.from_id JOIN cg_node_user AS b ON b.id = __cg_edge_0.to_id '
            'UNION ALL SELECT CAST(b.age AS TEXT) AS "age_text", b.id AS "__cg_aggregate_1" FROM cg_edge_knows AS __cg_edge_0 JOIN cg_node_user AS a ON a.id = __cg_edge_0.from_id JOIN cg_node_user AS __cg_variable_hop_2_node_1 ON __cg_variable_hop_2_node_1.id = __cg_edge_0.to_id JOIN cg_edge_knows AS __cg_edge_1 ON __cg_variable_hop_2_node_1.id = __cg_edge_1.from_id JOIN cg_node_user AS b ON b.id = __cg_edge_1.to_id) AS variable_length_q '
            'GROUP BY variable_length_q."age_text" ORDER BY "total" DESC, variable_length_q."age_text" ASC',
        )
        self.assertEqual(
            id_expression.sql(),
            'SELECT variable_length_q."friend_id" AS "friend_id", COUNT(variable_length_q."__cg_aggregate_1") AS "total" '
            'FROM (SELECT __cg_zero_hop_node.id AS "friend_id", __cg_zero_hop_node.id AS "__cg_aggregate_1" FROM cg_node_user AS __cg_zero_hop_node '
            'UNION ALL SELECT b.id AS "friend_id", b.id AS "__cg_aggregate_1" FROM cg_edge_knows AS __cg_edge_0 JOIN cg_node_user AS a ON a.id = __cg_edge_0.from_id JOIN cg_node_user AS b ON b.id = __cg_edge_0.to_id '
            'UNION ALL SELECT b.id AS "friend_id", b.id AS "__cg_aggregate_1" FROM cg_edge_knows AS __cg_edge_0 JOIN cg_node_user AS a ON a.id = __cg_edge_0.from_id JOIN cg_node_user AS __cg_variable_hop_2_node_1 ON __cg_variable_hop_2_node_1.id = __cg_edge_0.to_id JOIN cg_edge_knows AS __cg_edge_1 ON __cg_variable_hop_2_node_1.id = __cg_edge_1.from_id JOIN cg_node_user AS b ON b.id = __cg_edge_1.to_id) AS variable_length_q '
            'GROUP BY variable_length_q."friend_id" ORDER BY "total" DESC, variable_length_q."friend_id" ASC',
        )

    def test_compile_type_aware_relational_bounded_variable_length_grouped_entity_and_properties(
        self,
    ) -> None:
        entity_expression = cypherglot.compile_cypher_text(
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
        properties_expression = cypherglot.compile_cypher_text(
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
            entity_expression.sql(),
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
            properties_expression.sql(),
            'SELECT variable_length_q."props.name" AS "props.name", variable_length_q."props.age" AS "props.age", '
            'COUNT(variable_length_q."__cg_aggregate_1") AS "total" '
            'FROM (SELECT __cg_zero_hop_node.name AS "props.name", __cg_zero_hop_node.age AS "props.age", __cg_zero_hop_node.id AS "__cg_aggregate_1" FROM cg_node_user AS __cg_zero_hop_node '
            'UNION ALL SELECT b.name AS "props.name", b.age AS "props.age", b.id AS "__cg_aggregate_1" FROM cg_edge_knows AS __cg_edge_0 JOIN cg_node_user AS a ON a.id = __cg_edge_0.from_id JOIN cg_node_user AS b ON b.id = __cg_edge_0.to_id '
            'UNION ALL SELECT b.name AS "props.name", b.age AS "props.age", b.id AS "__cg_aggregate_1" FROM cg_edge_knows AS __cg_edge_0 JOIN cg_node_user AS a ON a.id = __cg_edge_0.from_id JOIN cg_node_user AS __cg_variable_hop_2_node_1 ON __cg_variable_hop_2_node_1.id = __cg_edge_0.to_id JOIN cg_edge_knows AS __cg_edge_1 ON __cg_variable_hop_2_node_1.id = __cg_edge_1.from_id JOIN cg_node_user AS b ON b.id = __cg_edge_1.to_id) AS variable_length_q '
            'GROUP BY variable_length_q."props.name", variable_length_q."props.age" '
            'ORDER BY variable_length_q."props.name" ASC, variable_length_q."props.age" ASC, "total" ASC',
        )

    def test_compile_type_aware_fixed_length_multi_hop_introspection_returns(self) -> None:
        expression = cypherglot.compile_cypher_text(
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
            expression.sql(),
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

    def test_compile_type_aware_fixed_length_multi_hop_helper_returns(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            "relational output mode does not yet support whole-entity or introspection returns",
        ):
            cypherglot.compile_cypher_text(
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

    def test_compile_type_aware_fixed_length_multi_hop_complementary_helper_returns(
        self,
    ) -> None:
        with self.assertRaisesRegex(
            ValueError,
            "relational output mode does not yet support whole-entity or introspection returns",
        ):
            cypherglot.compile_cypher_text(
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

    def test_compile_type_aware_relational_output_mode_rejects_fixed_length_multi_hop_grouped_helper_returns(
        self,
    ) -> None:
        with self.assertRaisesRegex(
            ValueError,
            "relational output mode does not yet support whole-entity or introspection returns",
        ):
            cypherglot.compile_cypher_text(
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

    def test_compile_type_aware_relational_output_mode_rejects_fixed_length_multi_hop_grouped_complementary_helper_returns(
        self,
    ) -> None:
        with self.assertRaisesRegex(
            ValueError,
            "relational output mode does not yet support whole-entity or introspection returns",
        ):
            cypherglot.compile_cypher_text(
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

    def test_compile_type_aware_fixed_length_multi_hop_grouped_aggregates(self) -> None:
        expression = cypherglot.compile_cypher_text(
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
            expression.sql(),
            'SELECT c.name AS "company", COUNT(s.id) AS "total" '
            'FROM cg_edge_knows AS r '
            'JOIN cg_node_user AS a ON a.id = r.from_id '
            'JOIN cg_node_user AS b ON b.id = r.to_id '
            'JOIN cg_edge_works_at AS s ON b.id = s.from_id '
            'JOIN cg_node_company AS c ON c.id = s.to_id '
            'GROUP BY c.name ORDER BY "total" DESC, c.name ASC',
        )

    def test_compile_type_aware_fixed_length_multi_hop_ungrouped_sum_aggregate(
        self,
    ) -> None:
        expression = cypherglot.compile_cypher_text(
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
            expression.sql(),
            'SELECT SUM(s.since) AS "total_since" '
            'FROM cg_edge_knows AS r '
            'JOIN cg_node_user AS a ON a.id = r.from_id '
            'JOIN cg_node_user AS b ON b.id = r.to_id '
            'JOIN cg_edge_works_at AS s ON b.id = s.from_id '
            'JOIN cg_node_company AS c ON c.id = s.to_id',
        )

    def test_compile_type_aware_fixed_length_multi_hop_ungrouped_count_star(
        self,
    ) -> None:
        expression = cypherglot.compile_cypher_text(
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
            expression.sql(),
            'SELECT COUNT(*) AS "total" '
            'FROM cg_edge_knows AS r '
            'JOIN cg_node_user AS a ON a.id = r.from_id '
            'JOIN cg_node_user AS b ON b.id = r.to_id '
            'JOIN cg_edge_works_at AS s ON b.id = s.from_id '
            'JOIN cg_node_company AS c ON c.id = s.to_id',
        )

    def test_compile_type_aware_fixed_length_multi_hop_ungrouped_count_rel(
        self,
    ) -> None:
        expression = cypherglot.compile_cypher_text(
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
            expression.sql(),
            'SELECT COUNT(s.id) AS "total" '
            'FROM cg_edge_knows AS r '
            'JOIN cg_node_user AS a ON a.id = r.from_id '
            'JOIN cg_node_user AS b ON b.id = r.to_id '
            'JOIN cg_edge_works_at AS s ON b.id = s.from_id '
            'JOIN cg_node_company AS c ON c.id = s.to_id',
        )

    def test_compile_type_aware_fixed_length_multi_hop_grouped_count_star(
        self,
    ) -> None:
        expression = cypherglot.compile_cypher_text(
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
            expression.sql(),
            'SELECT c.name AS "company", COUNT(*) AS "total" '
            'FROM cg_edge_knows AS r '
            'JOIN cg_node_user AS a ON a.id = r.from_id '
            'JOIN cg_node_user AS b ON b.id = r.to_id '
            'JOIN cg_edge_works_at AS s ON b.id = s.from_id '
            'JOIN cg_node_company AS c ON c.id = s.to_id '
            'GROUP BY c.name ORDER BY "total" DESC, c.name ASC',
        )

    def test_compile_type_aware_fixed_length_multi_hop_grouped_sum_aggregate(
        self,
    ) -> None:
        expression = cypherglot.compile_cypher_text(
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
            expression.sql(),
            'SELECT c.name AS "company", SUM(s.since) AS "total_since" '
            'FROM cg_edge_knows AS r '
            'JOIN cg_node_user AS a ON a.id = r.from_id '
            'JOIN cg_node_user AS b ON b.id = r.to_id '
            'JOIN cg_edge_works_at AS s ON b.id = s.from_id '
            'JOIN cg_node_company AS c ON c.id = s.to_id '
            'GROUP BY c.name ORDER BY "total_since" DESC, c.name ASC',
        )

    def test_compile_type_aware_fixed_length_multi_hop_ungrouped_min_aggregate(
        self,
    ) -> None:
        expression = cypherglot.compile_cypher_text(
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
            expression.sql(),
            'SELECT MIN(s.since) AS "first_since" '
            'FROM cg_edge_knows AS r '
            'JOIN cg_node_user AS a ON a.id = r.from_id '
            'JOIN cg_node_user AS b ON b.id = r.to_id '
            'JOIN cg_edge_works_at AS s ON b.id = s.from_id '
            'JOIN cg_node_company AS c ON c.id = s.to_id',
        )

    def test_compile_type_aware_fixed_length_multi_hop_grouped_max_aggregate(
        self,
    ) -> None:
        expression = cypherglot.compile_cypher_text(
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
            expression.sql(),
            'SELECT c.name AS "company", MAX(s.since) AS "latest_since" '
            'FROM cg_edge_knows AS r '
            'JOIN cg_node_user AS a ON a.id = r.from_id '
            'JOIN cg_node_user AS b ON b.id = r.to_id '
            'JOIN cg_edge_works_at AS s ON b.id = s.from_id '
            'JOIN cg_node_company AS c ON c.id = s.to_id '
            'GROUP BY c.name ORDER BY "latest_since" DESC, c.name ASC',
        )

    def test_compile_type_aware_relational_output_mode_expands_direct_chain_endpoints(
        self,
    ) -> None:
        expression = cypherglot.compile_cypher_text(
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
            expression.sql(),
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

    def test_compile_type_aware_relational_output_mode_expands_direct_chain_entities_and_properties(
        self,
    ) -> None:
        expression = cypherglot.compile_cypher_text(
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
            expression.sql(),
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

    def test_compile_type_aware_relational_output_mode_groups_direct_chain_entities_and_properties(
        self,
    ) -> None:
        expression = cypherglot.compile_cypher_text(
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
            expression.sql(),
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

    def test_compile_type_aware_match_with_return_node_source(self) -> None:
        expression = cypherglot.compile_cypher_text(
            (
                "MATCH (u:User) WITH u AS person, u.name AS name "
                "WHERE name = 'Alice' AND person.age >= 18 "
                "RETURN person.name AS display_name, id(person) AS person_id, name "
                "ORDER BY display_name, person_id, name"
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
            expression.sql(),
            'SELECT with_q."__cg_with_person_prop_name" AS "display_name", '
            'with_q."__cg_with_person_id" AS "person_id", '
            'with_q."__cg_with_scalar_name" AS "name" '
            'FROM (SELECT u.id AS "__cg_with_person_id", '
            'u.name AS "__cg_with_person_prop_name", '
            'u.age AS "__cg_with_person_prop_age", '
            'u.name AS "__cg_with_scalar_name" '
            'FROM cg_node_user AS u) AS with_q '
            'WHERE with_q."__cg_with_scalar_name" = \'Alice\' '
            'AND with_q."__cg_with_person_prop_age" >= 18 '
            'ORDER BY with_q."__cg_with_person_prop_name" ASC, '
            'with_q."__cg_with_person_id" ASC, '
            'with_q."__cg_with_scalar_name" ASC',
        )

    def test_compile_type_aware_match_with_return_relationship_source(self) -> None:
        expression = cypherglot.compile_cypher_text(
            (
                "MATCH (a:User)-[r:WORKS_AT]->(b:Company) "
                "WITH a AS person, r AS rel, b.name AS company "
                "WHERE rel.since >= 2020 "
                "RETURN person.name AS user_name, type(rel) AS rel_type, company "
                "ORDER BY user_name, rel_type, company"
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
                )
            ),
        )

        self.assertEqual(
            expression.sql(),
            'SELECT with_q."__cg_with_person_prop_name" AS "user_name", '
            '\'WORKS_AT\' AS "rel_type", '
            'with_q."__cg_with_scalar_company" AS "company" '
            'FROM (SELECT a.id AS "__cg_with_person_id", '
            'a.name AS "__cg_with_person_prop_name", '
            'r.id AS "__cg_with_rel_id", '
            'r.from_id AS "__cg_with_rel_from_id", '
            'r.to_id AS "__cg_with_rel_to_id", '
            'r.since AS "__cg_with_rel_prop_since", '
            'b.name AS "__cg_with_scalar_company" '
            'FROM cg_edge_works_at AS r '
            'JOIN cg_node_user AS a ON a.id = r.from_id '
            'JOIN cg_node_company AS b ON b.id = r.to_id) AS with_q '
            'WHERE with_q."__cg_with_rel_prop_since" >= 2020 '
            'ORDER BY with_q."__cg_with_person_prop_name" ASC, '
            '\'WORKS_AT\' ASC, with_q."__cg_with_scalar_company" ASC',
        )

    def test_compile_type_aware_match_with_return_chain_source(self) -> None:
        expression = cypherglot.compile_cypher_text(
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
            expression.sql(),
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

    def test_compile_type_aware_relational_output_mode_match_with_return_chain_relationship_introspection(self) -> None:
        expression = cypherglot.compile_cypher_text(
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
            expression.sql(),
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

    def test_compile_type_aware_relational_output_mode_rejects_match_with_return_chain_helper_introspection(
        self,
    ) -> None:
        with self.assertRaisesRegex(
            ValueError,
            "relational output mode does not yet support whole-entity or introspection returns",
        ):
            cypherglot.compile_cypher_text(
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

    def test_compile_type_aware_relational_output_mode_rejects_match_with_return_chain_complementary_helper_introspection(
        self,
    ) -> None:
        with self.assertRaisesRegex(
            ValueError,
            "relational output mode does not yet support whole-entity or introspection returns",
        ):
            cypherglot.compile_cypher_text(
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
                    ),
                ),
            )

    def test_compile_type_aware_relational_output_mode_rejects_match_with_return_chain_grouped_helper_introspection(
        self,
    ) -> None:
        with self.assertRaisesRegex(
            ValueError,
            "relational output mode does not yet support whole-entity or introspection returns",
        ):
            cypherglot.compile_cypher_text(
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
                    ),
                ),
            )

    def test_compile_type_aware_relational_output_mode_rejects_match_with_return_chain_grouped_complementary_helper_introspection(
        self,
    ) -> None:
        with self.assertRaisesRegex(
            ValueError,
            "relational output mode does not yet support whole-entity or introspection returns",
        ):
            cypherglot.compile_cypher_text(
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

    def test_compile_type_aware_match_with_return_chain_grouped_aggregates(self) -> None:
        expression = cypherglot.compile_cypher_text(
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
            expression.sql(),
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

    def test_compile_type_aware_relational_output_mode_bounded_variable_length_match_with_return(
        self,
    ) -> None:
        expression = cypherglot.compile_cypher_text(
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
        scalar_function_expression = cypherglot.compile_cypher_text(
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
        id_expression = cypherglot.compile_cypher_text(
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
            expression.sql(),
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
            cypherglot.compile_cypher_text(
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
            scalar_function_expression.sql(),
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
            id_expression.sql(),
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

    def test_compile_type_aware_relational_output_mode_bounded_variable_length_match_with_grouped_count(
        self,
    ) -> None:
        expression = cypherglot.compile_cypher_text(
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
        node_expression = cypherglot.compile_cypher_text(
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
        helper_expression = cypherglot.compile_cypher_text(
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
        lowered_expression = cypherglot.compile_cypher_text(
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
        age_text_expression = cypherglot.compile_cypher_text(
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
        id_expression = cypherglot.compile_cypher_text(
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
        aggregate_expression = cypherglot.compile_cypher_text(
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
            expression.sql(),
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
            'GROUP BY with_q."__cg_with_friend_prop_name" '
            'ORDER BY "total" DESC, with_q."__cg_with_friend_prop_name" ASC',
        )
        self.assertEqual(
            aggregate_expression.sql(),
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
            'GROUP BY with_q."__cg_with_friend_prop_name" '
            'ORDER BY "total_age" DESC, with_q."__cg_with_friend_prop_name" ASC',
        )
        self.assertEqual(
            node_expression.sql(),
            'SELECT with_q."__cg_with_friend_id" AS "friend_node.id", \'User\' AS "friend_node.label", with_q."__cg_with_friend_prop_name" AS "friend_node.name", with_q."__cg_with_friend_prop_age" AS "friend_node.age", COUNT(with_q."__cg_with_friend_id") AS "total" '
            'FROM (SELECT __cg_zero_hop_node.id AS "__cg_with_friend_id", __cg_zero_hop_node.name AS "__cg_with_friend_prop_name", __cg_zero_hop_node.age AS "__cg_with_friend_prop_age" FROM cg_node_user AS __cg_zero_hop_node '
            'UNION ALL SELECT b.id AS "__cg_with_friend_id", b.name AS "__cg_with_friend_prop_name", b.age AS "__cg_with_friend_prop_age" FROM cg_edge_knows AS __cg_edge_0 JOIN cg_node_user AS a ON a.id = __cg_edge_0.from_id JOIN cg_node_user AS b ON b.id = __cg_edge_0.to_id '
            'UNION ALL SELECT b.id AS "__cg_with_friend_id", b.name AS "__cg_with_friend_prop_name", b.age AS "__cg_with_friend_prop_age" FROM cg_edge_knows AS __cg_edge_0 JOIN cg_node_user AS a ON a.id = __cg_edge_0.from_id JOIN cg_node_user AS __cg_variable_hop_2_node_1 ON __cg_variable_hop_2_node_1.id = __cg_edge_0.to_id JOIN cg_edge_knows AS __cg_edge_1 ON __cg_variable_hop_2_node_1.id = __cg_edge_1.from_id JOIN cg_node_user AS b ON b.id = __cg_edge_1.to_id) AS with_q '
            'GROUP BY with_q."__cg_with_friend_id", \'User\', with_q."__cg_with_friend_prop_name", with_q."__cg_with_friend_prop_age" '
            'ORDER BY "total" DESC, with_q."__cg_with_friend_id" ASC, \'User\' ASC, with_q."__cg_with_friend_prop_name" ASC, with_q."__cg_with_friend_prop_age" ASC',
        )
        self.assertEqual(
            helper_expression.sql(),
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
            cypherglot.compile_cypher_text(
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
            cypherglot.compile_cypher_text(
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
            lowered_expression.sql(),
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
            age_text_expression.sql(),
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
            id_expression.sql(),
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

    def test_compile_type_aware_match_with_return_chain_ungrouped_max_aggregate(
        self,
    ) -> None:
        expression = cypherglot.compile_cypher_text(
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
            expression.sql(),
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

    def test_compile_type_aware_match_with_return_chain_ungrouped_count_star(
        self,
    ) -> None:
        expression = cypherglot.compile_cypher_text(
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
            expression.sql(),
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

    def test_compile_type_aware_match_with_return_chain_ungrouped_count_rel(
        self,
    ) -> None:
        expression = cypherglot.compile_cypher_text(
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
            expression.sql(),
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

    def test_compile_type_aware_match_with_return_chain_grouped_count_star(
        self,
    ) -> None:
        expression = cypherglot.compile_cypher_text(
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
            expression.sql(),
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

    def test_compile_type_aware_match_with_return_chain_grouped_count_rel(
        self,
    ) -> None:
        expression = cypherglot.compile_cypher_text(
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
            expression.sql(),
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

    def test_compile_type_aware_match_with_return_chain_ungrouped_sum_aggregate(
        self,
    ) -> None:
        expression = cypherglot.compile_cypher_text(
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
            expression.sql(),
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

    def test_compile_type_aware_match_with_return_chain_grouped_min_aggregate(
        self,
    ) -> None:
        expression = cypherglot.compile_cypher_text(
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
            expression.sql(),
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

    def test_compile_type_aware_relational_output_mode_expands_with_chain_endpoints(
        self,
    ) -> None:
        expression = cypherglot.compile_cypher_text(
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
            expression.sql(),
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

    def test_compile_type_aware_relational_output_mode_expands_with_chain_entities_and_properties(
        self,
    ) -> None:
        expression = cypherglot.compile_cypher_text(
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
            expression.sql(),
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

    def test_compile_type_aware_relational_output_mode_groups_with_chain_entities_and_properties(
        self,
    ) -> None:
        expression = cypherglot.compile_cypher_text(
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
            expression.sql(),
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

    def test_compile_type_aware_match_with_introspection_returns(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            "relational output mode does not yet support whole-entity or introspection returns",
        ):
            cypherglot.compile_cypher_text(
                (
                    "MATCH (a:User)-[r:WORKS_AT]->(b:Company) "
                    "WITH a AS person, r AS rel, b AS company "
                    "RETURN properties(person) AS person_props, labels(person) AS person_labels, "
                    "keys(rel) AS rel_keys, startNode(rel).name AS start_name, "
                    "endNode(rel) AS employer ORDER BY person_props, person_labels, rel_keys, start_name, employer"
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
                    )
                ),
            )

    def test_compile_type_aware_match_with_start_node_requires_endpoint_binding(self) -> None:
        with self.assertRaisesRegex(ValueError, "explicit rebound endpoint node bindings"):
            cypherglot.compile_cypher_text(
                (
                    "MATCH (a:User)-[r:WORKS_AT]->(b:Company) "
                    "WITH r AS rel RETURN startNode(rel).name AS start_name"
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

    def test_compile_type_aware_match_with_grouped_aggregates(self) -> None:
        expression = cypherglot.compile_cypher_text(
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
        relationship_expression = cypherglot.compile_cypher_text(
            (
                "MATCH (a:User)-[r:WORKS_AT]->(b:Company) "
                "WITH r AS rel RETURN rel AS edge, count(rel) AS total "
                "ORDER BY total DESC"
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
                        ),
                    ),
                )
            ),
        )

        self.assertEqual(
            expression.sql(),
            'SELECT with_q."__cg_with_scalar_name" AS "name", '
            'MAX(with_q."__cg_with_scalar_age") AS "top_age" '
            'FROM (SELECT u.name AS "__cg_with_scalar_name", '
            'u.age AS "__cg_with_scalar_age" FROM cg_node_user AS u) AS with_q '
            'GROUP BY with_q."__cg_with_scalar_name" ORDER BY "top_age" DESC',
        )
        self.assertEqual(
            relationship_expression.sql(),
            'SELECT with_q."__cg_with_rel_id" AS "edge.id", '
            '\'WORKS_AT\' AS "edge.type", '
            'with_q."__cg_with_rel_from_id" AS "edge.from_id", '
            'with_q."__cg_with_rel_to_id" AS "edge.to_id", '
            'COUNT(with_q."__cg_with_rel_id") AS "total" '
            'FROM (SELECT r.id AS "__cg_with_rel_id", '
            'r.from_id AS "__cg_with_rel_from_id", '
            'r.to_id AS "__cg_with_rel_to_id" '
            'FROM cg_edge_works_at AS r '
            'JOIN cg_node_user AS a ON a.id = r.from_id '
            'JOIN cg_node_company AS b ON b.id = r.to_id) AS with_q '
            'GROUP BY with_q."__cg_with_rel_id", \'WORKS_AT\', '
            'with_q."__cg_with_rel_from_id", with_q."__cg_with_rel_to_id" '
            'ORDER BY "total" DESC',
        )

    def test_compile_type_aware_match_with_entity_field_aggregates(self) -> None:
        node_expression = cypherglot.compile_cypher_text(
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
        relationship_expression = cypherglot.compile_cypher_text(
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
            node_expression.sql(),
            'SELECT MAX(with_q."__cg_with_person_prop_age") AS "top_age" '
            'FROM (SELECT u.id AS "__cg_with_person_id", '
            'u.age AS "__cg_with_person_prop_age" '
            'FROM cg_node_user AS u) AS with_q ORDER BY "top_age" DESC',
        )
        self.assertEqual(
            relationship_expression.sql(),
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

    def test_compile_type_aware_match_with_node_source_scalar_functions(self) -> None:
        expression = cypherglot.compile_cypher_text(
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
            expression.sql(),
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

    def test_compile_type_aware_match_with_relationship_source_scalar_functions(self) -> None:
        expression = cypherglot.compile_cypher_text(
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
            expression.sql(),
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

    def test_compile_type_aware_match_with_case_returns(self) -> None:
        node_expression = cypherglot.compile_cypher_text(
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
        relationship_expression = cypherglot.compile_cypher_text(
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
            node_expression.sql(),
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
            relationship_expression.sql(),
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

    def test_compile_type_aware_match_with_predicate_returns(self) -> None:
        node_expression = cypherglot.compile_cypher_text(
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
        relationship_expression = cypherglot.compile_cypher_text(
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
            node_expression.sql(),
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
            relationship_expression.sql(),
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

    def test_compile_type_aware_relational_output_mode_rejects_match_with_broader_introspection_returns(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            "relational output mode does not yet support whole-entity or introspection returns",
        ):
            cypherglot.compile_cypher_text(
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

    def test_compile_type_aware_match_node_introspection_returns(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            "relational output mode does not yet support whole-entity or introspection returns",
        ):
            cypherglot.compile_cypher_text(
                "MATCH (u:User) RETURN u AS user, properties(u) AS props, labels(u) AS labels, keys(u) AS keys ORDER BY user, props, labels, keys",
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

    def test_compile_type_aware_match_relationship_introspection_returns(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            "relational output mode does not yet support whole-entity or introspection returns",
        ):
            cypherglot.compile_cypher_text(
                (
                    "MATCH (a:User)-[r:WORKS_AT]->(b:Company) "
                    "RETURN r AS rel, properties(r) AS props, keys(r) AS keys, "
                    "startNode(r) AS start, endNode(r).name AS company "
                    "ORDER BY rel, props, keys, start, company"
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
                    )
                ),
            )

    def test_compile_type_aware_optional_match_node_scalar_return(self) -> None:
        expression = cypherglot.compile_cypher_text(
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
            expression.sql(),
            'SELECT u.name AS "name" '
            'FROM (SELECT 1 AS __cg_seed) AS seed '
            'LEFT JOIN cg_node_user AS u ON 1 = 1 AND u.name = \'Alice\' '
            'ORDER BY u.name ASC LIMIT 1',
        )

    def test_compile_type_aware_optional_match_node_relational_entity_return(self) -> None:
        expression = cypherglot.compile_cypher_text(
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
            expression.sql(),
            'SELECT u.id AS "user.id", \'User\' AS "user.label", '
            'u.name AS "user.name", COUNT(u.id) AS "total" '
            'FROM (SELECT 1 AS __cg_seed) AS seed '
            'LEFT JOIN cg_node_user AS u ON 1 = 1 '
            'GROUP BY u.id, \'User\', u.name ORDER BY "total" DESC',
        )

    def test_compile_rejects_vector_aware_query_nodes_for_now(self) -> None:
        with self.assertRaisesRegex(ValueError, "does not yet compile vector-aware CALL queries"):
            cypherglot.compile_cypher_program_text(
                "CALL db.index.vector.queryNodes('user_embedding_idx', 1, $query) "
                "YIELD node, score RETURN node.id, score",
                schema_context=_public_api_schema_context(),
            )

    def test_compile_single_statement_api_rejects_vector_aware_query_nodes(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            "carries vector intent forward for host runtimes, but it does not yet compile vector-aware CALL queries into SQLGlot output",
        ):
            cypherglot.compile_cypher_text(
                "CALL db.index.vector.queryNodes('user_embedding_idx', 3, $query) "
                "YIELD node, score WHERE node.region = 'west' RETURN node.id, score",
                schema_context=_public_api_schema_context(),
            )

    def test_compile_match_with_scalar_rebinding(self) -> None:
        expression = cypherglot.compile_cypher_text(
            "MATCH (u:User) WITH u.name AS name RETURN name ORDER BY name",
            schema_context=_public_api_schema_context(),
        )

        self.assertEqual(
            expression.sql(),
            'SELECT with_q."__cg_with_scalar_name" AS "name" '
            'FROM (SELECT u.name AS "__cg_with_scalar_name" FROM cg_node_user AS u) AS with_q '
            'ORDER BY with_q."__cg_with_scalar_name" ASC',
        )

    def test_compile_match_with_entity_passthrough_return(self) -> None:
        expression = cypherglot.compile_cypher_text(
            "MATCH (u:User) WITH u AS person, u.name AS name RETURN person, name ORDER BY name",
            schema_context=_public_api_schema_context(),
        )
        ordered_expression = cypherglot.compile_cypher_text(
            "MATCH (u:User) WITH u AS person RETURN person AS user ORDER BY user",
            schema_context=_public_api_schema_context(),
        )
        relationship_ordered_expression = cypherglot.compile_cypher_text(
            "MATCH (a:User)-[r:KNOWS]->(b:User) WITH r AS rel RETURN rel AS edge ORDER BY edge",
            schema_context=_public_api_schema_context(),
        )

        self.assertEqual(
            expression.sql(),
            'SELECT with_q."__cg_with_person_id" AS "person.id", '\
            "'User' AS \"person.label\", "
            'with_q."__cg_with_person_prop_name" AS "person.name", '\
            'with_q."__cg_with_person_prop_age" AS "person.age", '\
            'with_q."__cg_with_person_prop_score" AS "person.score", '\
            'with_q."__cg_with_person_prop_active" AS "person.active", '\
            'with_q."__cg_with_scalar_name" AS "name" '
            'FROM (SELECT u.id AS "__cg_with_person_id", '
            'u.name AS "__cg_with_person_prop_name", '
            'u.age AS "__cg_with_person_prop_age", '
            'u.score AS "__cg_with_person_prop_score", '
            'u.active AS "__cg_with_person_prop_active", '
            'u.name AS "__cg_with_scalar_name" FROM cg_node_user AS u) AS with_q '
            'ORDER BY with_q."__cg_with_scalar_name" ASC',
        )
        self.assertEqual(
            ordered_expression.sql(),
            'SELECT with_q."__cg_with_person_id" AS "user.id", '\
            "'User' AS \"user.label\", "
            'with_q."__cg_with_person_prop_name" AS "user.name", '\
            'with_q."__cg_with_person_prop_age" AS "user.age", '\
            'with_q."__cg_with_person_prop_score" AS "user.score", '\
            'with_q."__cg_with_person_prop_active" AS "user.active" '
            'FROM (SELECT u.id AS "__cg_with_person_id", u.name AS "__cg_with_person_prop_name", '
            'u.age AS "__cg_with_person_prop_age", u.score AS "__cg_with_person_prop_score", '
            'u.active AS "__cg_with_person_prop_active" FROM cg_node_user AS u) AS with_q '
            'ORDER BY with_q."__cg_with_person_id" ASC, \'User\' ASC, with_q."__cg_with_person_prop_name" ASC, '
            'with_q."__cg_with_person_prop_age" ASC, with_q."__cg_with_person_prop_score" ASC, '
            'with_q."__cg_with_person_prop_active" ASC',
        )
        self.assertEqual(
            relationship_ordered_expression.sql(),
            'SELECT with_q."__cg_with_rel_id" AS "edge.id", '\
            "'KNOWS' AS \"edge.type\", "
            'with_q."__cg_with_rel_from_id" AS "edge.from_id", '\
            'with_q."__cg_with_rel_to_id" AS "edge.to_id", '\
            'with_q."__cg_with_rel_prop_note" AS "edge.note", '\
            'with_q."__cg_with_rel_prop_weight" AS "edge.weight", '\
            'with_q."__cg_with_rel_prop_score" AS "edge.score", '\
            'with_q."__cg_with_rel_prop_active" AS "edge.active" '
            'FROM (SELECT r.id AS "__cg_with_rel_id", r.from_id AS "__cg_with_rel_from_id", '
            'r.to_id AS "__cg_with_rel_to_id", r.note AS "__cg_with_rel_prop_note", '
            'r.weight AS "__cg_with_rel_prop_weight", r.score AS "__cg_with_rel_prop_score", '
            'r.active AS "__cg_with_rel_prop_active" FROM cg_edge_knows AS r '
            'JOIN cg_node_user AS a ON a.id = r.from_id JOIN cg_node_user AS b ON b.id = r.to_id) AS with_q '
            'ORDER BY with_q."__cg_with_rel_id" ASC, \'KNOWS\' ASC, with_q."__cg_with_rel_from_id" ASC, '
            'with_q."__cg_with_rel_to_id" ASC, with_q."__cg_with_rel_prop_note" ASC, '
            'with_q."__cg_with_rel_prop_weight" ASC, with_q."__cg_with_rel_prop_score" ASC, '
            'with_q."__cg_with_rel_prop_active" ASC',
        )

    def test_compile_fixed_length_multi_hop_match(self) -> None:
        expression = cypherglot.compile_cypher_text(
            "MATCH (a:User)-[r:KNOWS]->(b:User)-[s:WORKS_AT]->(c:Company) RETURN a.name AS user_name, c.name AS company ORDER BY company",
            schema_context=_public_api_schema_context(),
        )

        self.assertEqual(
            expression.sql(),
            'SELECT a.name AS "user_name", c.name AS "company" FROM cg_edge_knows AS r '
            'JOIN cg_node_user AS a ON a.id = r.from_id JOIN cg_node_user AS b ON b.id = r.to_id '
            'JOIN cg_edge_works_at AS s ON b.id = s.from_id JOIN cg_node_company AS c ON c.id = s.to_id '
            'ORDER BY c.name ASC',
        )

    def test_compile_fixed_length_multi_hop_match_with_return(self) -> None:
        expression = cypherglot.compile_cypher_text(
            "MATCH (a:User)-[r:KNOWS]->(b:User)-[s:WORKS_AT]->(c:Company) WITH b AS friend, c.name AS company RETURN friend.name, company ORDER BY company",
            schema_context=_public_api_schema_context(),
        )

        self.assertEqual(
            expression.sql(),
            'SELECT with_q."__cg_with_friend_prop_name" AS "friend.name", with_q."__cg_with_scalar_company" AS "company" '
            'FROM (SELECT b.id AS "__cg_with_friend_id", b.name AS "__cg_with_friend_prop_name", '
            'b.age AS "__cg_with_friend_prop_age", b.score AS "__cg_with_friend_prop_score", '
            'b.active AS "__cg_with_friend_prop_active", c.name AS "__cg_with_scalar_company" '
            'FROM cg_edge_knows AS r JOIN cg_node_user AS a ON a.id = r.from_id '
            'JOIN cg_node_user AS b ON b.id = r.to_id JOIN cg_edge_works_at AS s ON b.id = s.from_id '
            'JOIN cg_node_company AS c ON c.id = s.to_id) AS with_q '
            'ORDER BY with_q."__cg_with_scalar_company" ASC',
        )

    def test_compile_bounded_variable_length_match(self) -> None:
        expression = cypherglot.compile_cypher_text(
            "MATCH (a:User)-[:KNOWS*1..2]->(b:User) RETURN a.name AS user_name, b.name AS friend ORDER BY friend",
            schema_context=_public_api_schema_context(),
        )
        zero_hop_expression = cypherglot.compile_cypher_text(
            "MATCH (a:User)-[:KNOWS*0..2]->(b:User) RETURN a.name AS user_name, b.name AS friend ORDER BY friend",
            schema_context=_public_api_schema_context(),
        )
        alias_expression = cypherglot.compile_cypher_text(
            "MATCH (a:User)-[r:KNOWS*1..2]->(b:User) RETURN a.name AS user_name, b.name AS friend ORDER BY friend",
            schema_context=_public_api_schema_context(),
        )

        self.assertEqual(
            expression.sql(),
            'SELECT * FROM (SELECT a.name AS "user_name", b.name AS "friend" '
            'FROM cg_edge_knows AS __cg_edge_0 JOIN cg_node_user AS a ON a.id = __cg_edge_0.from_id '
            'JOIN cg_node_user AS b ON b.id = __cg_edge_0.to_id UNION ALL '
            'SELECT a.name AS "user_name", b.name AS "friend" FROM cg_edge_knows AS __cg_edge_0 '
            'JOIN cg_node_user AS a ON a.id = __cg_edge_0.from_id '
            'JOIN cg_node_user AS __cg_variable_hop_2_node_1 ON __cg_variable_hop_2_node_1.id = __cg_edge_0.to_id '
            'JOIN cg_edge_knows AS __cg_edge_1 ON __cg_variable_hop_2_node_1.id = __cg_edge_1.from_id '
            'JOIN cg_node_user AS b ON b.id = __cg_edge_1.to_id) AS variable_length_q '
            'ORDER BY variable_length_q."friend" ASC',
        )
        self.assertEqual(alias_expression.sql(), expression.sql())
        self.assertEqual(
            zero_hop_expression.sql(),
            'SELECT * FROM (SELECT __cg_zero_hop_node.name AS "user_name", __cg_zero_hop_node.name AS "friend" '
            'FROM cg_node_user AS __cg_zero_hop_node UNION ALL '
            'SELECT a.name AS "user_name", b.name AS "friend" FROM cg_edge_knows AS __cg_edge_0 '
            'JOIN cg_node_user AS a ON a.id = __cg_edge_0.from_id JOIN cg_node_user AS b ON b.id = __cg_edge_0.to_id UNION ALL '
            'SELECT a.name AS "user_name", b.name AS "friend" FROM cg_edge_knows AS __cg_edge_0 '
            'JOIN cg_node_user AS a ON a.id = __cg_edge_0.from_id '
            'JOIN cg_node_user AS __cg_variable_hop_2_node_1 ON __cg_variable_hop_2_node_1.id = __cg_edge_0.to_id '
            'JOIN cg_edge_knows AS __cg_edge_1 ON __cg_variable_hop_2_node_1.id = __cg_edge_1.from_id '
            'JOIN cg_node_user AS b ON b.id = __cg_edge_1.to_id) AS variable_length_q '
            'ORDER BY variable_length_q."friend" ASC',
        )

    def test_compile_bounded_variable_length_match_with_return(self) -> None:
        expression = cypherglot.compile_cypher_text(
            "MATCH (a:User)-[:KNOWS*..2]->(b:User) WITH b AS friend RETURN friend.name ORDER BY friend.name",
            schema_context=_public_api_schema_context(),
        )
        zero_hop_expression = cypherglot.compile_cypher_text(
            "MATCH (a:User)-[:KNOWS*0..2]->(b:User) WITH b AS friend RETURN friend.name ORDER BY friend.name",
            schema_context=_public_api_schema_context(),
        )

        self.assertEqual(
            expression.sql(),
            'SELECT with_q."__cg_with_friend_prop_name" AS "friend.name" FROM '
            '(SELECT b.id AS "__cg_with_friend_id", b.name AS "__cg_with_friend_prop_name", '
            'b.age AS "__cg_with_friend_prop_age", b.score AS "__cg_with_friend_prop_score", '
            'b.active AS "__cg_with_friend_prop_active" FROM cg_edge_knows AS __cg_edge_0 '
            'JOIN cg_node_user AS a ON a.id = __cg_edge_0.from_id JOIN cg_node_user AS b ON b.id = __cg_edge_0.to_id UNION ALL '
            'SELECT b.id AS "__cg_with_friend_id", b.name AS "__cg_with_friend_prop_name", '
            'b.age AS "__cg_with_friend_prop_age", b.score AS "__cg_with_friend_prop_score", '
            'b.active AS "__cg_with_friend_prop_active" FROM cg_edge_knows AS __cg_edge_0 '
            'JOIN cg_node_user AS a ON a.id = __cg_edge_0.from_id '
            'JOIN cg_node_user AS __cg_variable_hop_2_node_1 ON __cg_variable_hop_2_node_1.id = __cg_edge_0.to_id '
            'JOIN cg_edge_knows AS __cg_edge_1 ON __cg_variable_hop_2_node_1.id = __cg_edge_1.from_id '
            'JOIN cg_node_user AS b ON b.id = __cg_edge_1.to_id) AS with_q '
            'ORDER BY with_q."__cg_with_friend_prop_name" ASC',
        )
        self.assertEqual(
            zero_hop_expression.sql(),
            'SELECT with_q."__cg_with_friend_prop_name" AS "friend.name" FROM '
            '(SELECT __cg_zero_hop_node.id AS "__cg_with_friend_id", __cg_zero_hop_node.name AS "__cg_with_friend_prop_name", '
            '__cg_zero_hop_node.age AS "__cg_with_friend_prop_age", __cg_zero_hop_node.score AS "__cg_with_friend_prop_score", '
            '__cg_zero_hop_node.active AS "__cg_with_friend_prop_active" FROM cg_node_user AS __cg_zero_hop_node UNION ALL '
            'SELECT b.id AS "__cg_with_friend_id", b.name AS "__cg_with_friend_prop_name", '
            'b.age AS "__cg_with_friend_prop_age", b.score AS "__cg_with_friend_prop_score", '
            'b.active AS "__cg_with_friend_prop_active" FROM cg_edge_knows AS __cg_edge_0 '
            'JOIN cg_node_user AS a ON a.id = __cg_edge_0.from_id JOIN cg_node_user AS b ON b.id = __cg_edge_0.to_id UNION ALL '
            'SELECT b.id AS "__cg_with_friend_id", b.name AS "__cg_with_friend_prop_name", '
            'b.age AS "__cg_with_friend_prop_age", b.score AS "__cg_with_friend_prop_score", '
            'b.active AS "__cg_with_friend_prop_active" FROM cg_edge_knows AS __cg_edge_0 '
            'JOIN cg_node_user AS a ON a.id = __cg_edge_0.from_id '
            'JOIN cg_node_user AS __cg_variable_hop_2_node_1 ON __cg_variable_hop_2_node_1.id = __cg_edge_0.to_id '
            'JOIN cg_edge_knows AS __cg_edge_1 ON __cg_variable_hop_2_node_1.id = __cg_edge_1.from_id '
            'JOIN cg_node_user AS b ON b.id = __cg_edge_1.to_id) AS with_q '
            'ORDER BY with_q."__cg_with_friend_prop_name" ASC',
        )


    def test_compile_rejects_multi_step_create_node_in_single_expression_api(
        self,
    ) -> None:
        with self.assertRaisesRegex(ValueError, "multi-step SQL program"):
            cypherglot.compile_cypher_text(
                "CREATE (:User {name: 'Alice'})",
                schema_context=_public_api_schema_context(),
            )

    def test_compile_program_create_node(self) -> None:
        program = cypherglot.compile_cypher_program_text(
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
        self.assertIsInstance(program.steps[0], cypherglot.CompiledCypherStatement)
        self.assertEqual(
            program.steps[0].sql.sql(),
            "INSERT INTO cg_node_user (name) VALUES ('Alice') RETURNING id",
        )
        self.assertEqual(program.steps[0].bind_columns, ("created_node_id",))

    def test_compile_program_create_relationship(self) -> None:
        program = cypherglot.compile_cypher_program_text(
            "CREATE (a:User {name: 'Alice'})"
            "-[r:KNOWS {since: 2020}]->"
            "(b:User {name: 'Bob'})",
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
                            properties=(PropertyField("since", "integer"),),
                        ),
                    ),
                )
            ),
        )

        self.assertEqual(len(program.steps), 3)
        self.assertEqual(
            program.steps[0].sql.sql(),
            "INSERT INTO cg_node_user (name) VALUES ('Alice') RETURNING id",
        )
        self.assertEqual(program.steps[0].bind_columns, ("left_node_id",))
        self.assertEqual(
            program.steps[1].sql.sql(),
            "INSERT INTO cg_node_user (name) VALUES ('Bob') RETURNING id",
        )
        self.assertEqual(program.steps[1].bind_columns, ("right_node_id",))
        self.assertEqual(
            program.steps[2].sql.sql(),
            "INSERT INTO cg_edge_knows (from_id, to_id, since) VALUES (:left_node_id, :right_node_id, 2020)",
        )

    def test_compile_program_create_relationship_self_loop(self) -> None:
        program = cypherglot.compile_cypher_program_text(
            "CREATE (root:Root)-[:LINK]->(root:Root)",
            schema_context=CompilerSchemaContext.type_aware(
                GraphSchema(
                    node_types=(NodeTypeSpec(name="Root"),),
                    edge_types=(
                        cypherglot.EdgeTypeSpec(
                            name="LINK",
                            source_type="Root",
                            target_type="Root",
                        ),
                    ),
                )
            ),
        )

        self.assertEqual(len(program.steps), 2)
        self.assertEqual(program.steps[0].bind_columns, ("left_node_id",))
        self.assertEqual(
            program.steps[0].sql.sql(),
            "INSERT INTO cg_node_root DEFAULT VALUES RETURNING id",
        )
        self.assertEqual(
            program.steps[1].sql.sql(),
            "INSERT INTO cg_edge_link (from_id, to_id) VALUES (:left_node_id, :left_node_id)",
        )

    def test_compile_program_create_relationship_from_separate_patterns(self) -> None:
        program = cypherglot.compile_cypher_program_text(
            "CREATE (a:A {name: 'Alice'}), (b:B {name: 'Bob'}), (a:A)-[:R]->(b:B)",
            schema_context=CompilerSchemaContext.type_aware(
                GraphSchema(
                    node_types=(
                        NodeTypeSpec(
                            name="A",
                            properties=(PropertyField("name", "string"),),
                        ),
                        NodeTypeSpec(
                            name="B",
                            properties=(PropertyField("name", "string"),),
                        ),
                    ),
                    edge_types=(
                        cypherglot.EdgeTypeSpec(
                            name="R",
                            source_type="A",
                            target_type="B",
                        ),
                    ),
                )
            ),
        )

        self.assertEqual(len(program.steps), 3)
        self.assertEqual(program.steps[0].bind_columns, ("first_node_id",))
        self.assertEqual(program.steps[1].bind_columns, ("second_node_id",))
        self.assertEqual(
            program.steps[0].sql.sql(),
            "INSERT INTO cg_node_a (name) VALUES ('Alice') RETURNING id",
        )
        self.assertEqual(
            program.steps[1].sql.sql(),
            "INSERT INTO cg_node_b (name) VALUES ('Bob') RETURNING id",
        )
        self.assertEqual(
            program.steps[2].sql.sql(),
            "INSERT INTO cg_edge_r (from_id, to_id) VALUES (:first_node_id, :second_node_id)",
        )

    def test_compile_rejects_merge_program_from_single_statement_api(self) -> None:
        with self.assertRaisesRegex(ValueError, "multi-step SQL program"):
            cypherglot.compile_cypher_text(
                "MERGE (u:User {name: 'Alice'})",
                schema_context=_public_api_schema_context(),
            )

    def test_compile_program_merge_node(self) -> None:
        program = cypherglot.compile_cypher_program_text(
            "MERGE (u:User {name: 'Alice'})",
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
        loop = program.steps[0]
        self.assertIsInstance(loop, cypherglot.CompiledCypherLoop)
        assert isinstance(loop, cypherglot.CompiledCypherLoop)

        self.assertEqual(loop.row_bindings, ("merge_guard",))
        self.assertEqual(
            loop.source.sql(),
            "SELECT 1 AS merge_guard WHERE NOT EXISTS(SELECT 1 FROM cg_node_user AS u WHERE u.name = 'Alice' LIMIT 1)",
        )
        self.assertEqual(len(loop.body), 1)
        self.assertEqual(
            loop.body[0].sql.sql(),
            "INSERT INTO cg_node_user (name) VALUES ('Alice') RETURNING id",
        )
        self.assertEqual(loop.body[0].bind_columns, ("merged_node_id",))

    def test_compile_program_merge_relationship(self) -> None:
        program = cypherglot.compile_cypher_program_text(
            "MERGE (:User {name: 'Alice'})-[:WORKS_AT {since: 2020}]->(:Company {name: 'Acme'})",
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
                )
            ),
        )

        self.assertEqual(len(program.steps), 1)
        loop = program.steps[0]
        self.assertIsInstance(loop, cypherglot.CompiledCypherLoop)
        assert isinstance(loop, cypherglot.CompiledCypherLoop)

        self.assertEqual(loop.row_bindings, ("merge_guard",))
        self.assertEqual(
            loop.source.sql(),
            "SELECT 1 AS merge_guard WHERE NOT EXISTS(SELECT 1 FROM cg_edge_works_at AS merge_edge JOIN cg_node_user AS __humem_merge_left_node ON __humem_merge_left_node.id = merge_edge.from_id JOIN cg_node_company AS __humem_merge_right_node ON __humem_merge_right_node.id = merge_edge.to_id WHERE __humem_merge_left_node.name = 'Alice' AND merge_edge.since = 2020 AND __humem_merge_right_node.name = 'Acme' LIMIT 1)",
        )
        self.assertEqual(len(loop.body), 3)
        self.assertEqual(
            loop.body[0].sql.sql(),
            "INSERT INTO cg_node_user (name) VALUES ('Alice') RETURNING id",
        )
        self.assertEqual(
            loop.body[1].sql.sql(),
            "INSERT INTO cg_node_company (name) VALUES ('Acme') RETURNING id",
        )
        self.assertEqual(
            loop.body[2].sql.sql(),
            "INSERT INTO cg_edge_works_at (from_id, to_id, since) VALUES (:left_node_id, :right_node_id, 2020)",
        )

    def test_compile_program_merge_relationship_self_loop(self) -> None:
        program = cypherglot.compile_cypher_program_text(
            "MERGE (u:User {name: 'Alice'})-[:KNOWS]->(u:User {name: 'Alice'})",
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
                )
            ),
        )

        self.assertEqual(len(program.steps), 2)
        loop = program.steps[0]
        self.assertIsInstance(loop, cypherglot.CompiledCypherLoop)
        assert isinstance(loop, cypherglot.CompiledCypherLoop)
        self.assertEqual(loop.row_bindings, ("merge_node_id",))
        self.assertEqual(
            loop.source.sql(),
            "SELECT u.id AS merge_node_id FROM cg_node_user AS u WHERE u.name = 'Alice' LIMIT 1",
        )
        self.assertEqual(len(loop.body), 1)
        self.assertEqual(
            loop.body[0].sql.sql(),
            "INSERT INTO cg_edge_knows (from_id, to_id) SELECT :merge_node_id, :merge_node_id WHERE NOT EXISTS(SELECT 1 FROM cg_edge_knows AS existing_merge_edge WHERE existing_merge_edge.from_id = :merge_node_id AND existing_merge_edge.to_id = :merge_node_id)",
        )

        create_loop = program.steps[1]
        self.assertIsInstance(create_loop, cypherglot.CompiledCypherLoop)
        assert isinstance(create_loop, cypherglot.CompiledCypherLoop)
        self.assertEqual(create_loop.row_bindings, ("merge_guard",))
        self.assertEqual(
            create_loop.source.sql(),
            "SELECT 1 AS merge_guard WHERE NOT EXISTS(SELECT 1 FROM cg_node_user AS u WHERE u.name = 'Alice' LIMIT 1)",
        )
        self.assertEqual(len(create_loop.body), 2)
        self.assertEqual(
            create_loop.body[1].sql.sql(),
            "INSERT INTO cg_edge_knows (from_id, to_id) SELECT :created_node_id, :created_node_id WHERE NOT EXISTS(SELECT 1 FROM cg_edge_knows AS existing_merge_edge WHERE existing_merge_edge.from_id = :created_node_id AND existing_merge_edge.to_id = :created_node_id)",
        )

    def test_compile_traversal_self_loop_existing_endpoint_write_sql(self) -> None:
        create_sql = cypherglot.to_sql(
            "MATCH (a:User)-[r:KNOWS]->(a:User) CREATE (a)-[:KNOWS]->(a)",
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
                )
            ),
        )
        merge_sql = cypherglot.to_sql(
            "MATCH (a:User)-[r:KNOWS]->(a:User) MERGE (a)-[:KNOWS]->(a)",
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
                )
            ),
        )

        self.assertEqual(
            create_sql,
            "INSERT INTO cg_edge_knows (from_id, to_id) SELECT a.id, a.id FROM cg_edge_knows AS r JOIN cg_node_user AS a ON a.id = r.from_id WHERE r.from_id = r.to_id",
        )
        self.assertEqual(
            merge_sql,
            "INSERT INTO cg_edge_knows (from_id, to_id) SELECT a.id, a.id FROM cg_edge_knows AS r JOIN cg_node_user AS a ON a.id = r.from_id WHERE r.from_id = r.to_id AND NOT EXISTS(SELECT 1 FROM cg_edge_knows AS existing_merge_edge WHERE existing_merge_edge.from_id = a.id AND existing_merge_edge.to_id = a.id)",
        )

    def test_compile_traversal_self_loop_relationship_set_delete_sql(self) -> None:
        set_sql = cypherglot.to_sql(
            "MATCH (a:User)-[r:KNOWS]->(a:User) WHERE a.name = 'Alice' SET r.since = 2021",
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
                            properties=(PropertyField("since", "integer"),),
                        ),
                    ),
                )
            ),
        )
        delete_sql = cypherglot.to_sql(
            "MATCH (a:User)-[r:KNOWS]->(a:User) WHERE a.name = 'Alice' DELETE r",
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
                            properties=(PropertyField("since", "integer"),),
                        ),
                    ),
                )
            ),
        )

        self.assertEqual(
            set_sql,
            "UPDATE cg_edge_knows AS r SET since = 2021 FROM cg_node_user AS a WHERE a.id = r.from_id AND r.from_id = r.to_id AND a.name = 'Alice'",
        )
        self.assertEqual(
            delete_sql,
            "DELETE FROM cg_edge_knows WHERE id IN (SELECT r.id FROM cg_edge_knows AS r JOIN cg_node_user AS a ON a.id = r.from_id WHERE r.from_id = r.to_id AND a.name = 'Alice')",
        )

    def test_compile_match_relationship_set_delete_with_right_endpoint_filter(self) -> None:
        set_sql = cypherglot.to_sql(
            "MATCH (a:User)-[r:WORKS_AT]->(b:Company {name: 'Acme'}) SET r.since = 2025",
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
                )
            ),
        )
        delete_sql = cypherglot.to_sql(
            "MATCH (a:User)-[r:WORKS_AT]->(b:Company {name: 'Acme'}) DELETE r",
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
                )
            ),
        )

        self.assertEqual(
            set_sql,
            "UPDATE cg_edge_works_at AS r SET since = 2025 FROM cg_node_user AS a, cg_node_company AS b WHERE a.id = r.from_id AND b.id = r.to_id AND b.name = 'Acme'",
        )
        self.assertEqual(
            delete_sql,
            "DELETE FROM cg_edge_works_at WHERE id IN (SELECT r.id FROM cg_edge_works_at AS r JOIN cg_node_user AS a ON a.id = r.from_id JOIN cg_node_company AS b ON b.id = r.to_id WHERE b.name = 'Acme')",
        )

    def test_compile_match_relationship_set_delete_with_relationship_filter(self) -> None:
        set_sql = cypherglot.to_sql(
            "MATCH (a:User)-[r:WORKS_AT]->(b:Company) WHERE r.since = 2020 SET r.since = 2025",
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
                )
            ),
        )
        delete_sql = cypherglot.to_sql(
            "MATCH (a:User)-[r:WORKS_AT]->(b:Company) WHERE r.since = 2020 DELETE r",
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
                )
            ),
        )

        self.assertEqual(
            set_sql,
            "UPDATE cg_edge_works_at AS r SET since = 2025 FROM cg_node_user AS a, cg_node_company AS b WHERE a.id = r.from_id AND b.id = r.to_id AND r.since = 2020",
        )
        self.assertEqual(
            delete_sql,
            "DELETE FROM cg_edge_works_at WHERE id IN (SELECT r.id FROM cg_edge_works_at AS r JOIN cg_node_user AS a ON a.id = r.from_id JOIN cg_node_company AS b ON b.id = r.to_id WHERE r.since = 2020)",
        )

    def test_compile_match_relationship_set_delete_with_combined_filters(self) -> None:
        set_sql = cypherglot.to_sql(
            "MATCH (a:User)-[r:WORKS_AT]->(b:Company {name: 'Acme'}) WHERE r.since = 2020 SET r.since = 2025",
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
                )
            ),
        )
        delete_sql = cypherglot.to_sql(
            "MATCH (a:User)-[r:WORKS_AT]->(b:Company {name: 'Acme'}) WHERE r.since = 2020 DELETE r",
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
                )
            ),
        )

        self.assertEqual(
            set_sql,
            "UPDATE cg_edge_works_at AS r SET since = 2025 FROM cg_node_user AS a, cg_node_company AS b WHERE a.id = r.from_id AND b.id = r.to_id AND b.name = 'Acme' AND r.since = 2020",
        )
        self.assertEqual(
            delete_sql,
            "DELETE FROM cg_edge_works_at WHERE id IN (SELECT r.id FROM cg_edge_works_at AS r JOIN cg_node_user AS a ON a.id = r.from_id JOIN cg_node_company AS b ON b.id = r.to_id WHERE b.name = 'Acme' AND r.since = 2020)",
        )

    def test_compile_match_relationship_set_delete_with_additional_filter_combinations(
        self,
    ) -> None:
        left_and_relationship_set_sql = cypherglot.to_sql(
            "MATCH (a:User)-[r:WORKS_AT]->(b:Company) WHERE a.name = 'Alice' AND r.since = 2020 SET r.since = 2025",
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
                )
            ),
        )
        left_and_relationship_delete_sql = cypherglot.to_sql(
            "MATCH (a:User)-[r:WORKS_AT]->(b:Company) WHERE a.name = 'Alice' AND r.since = 2020 DELETE r",
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
                )
            ),
        )
        both_endpoints_set_sql = cypherglot.to_sql(
            "MATCH (a:User {name: 'Alice'})-[r:WORKS_AT]->(b:Company {name: 'Acme'}) SET r.since = 2025",
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
                )
            ),
        )
        both_endpoints_delete_sql = cypherglot.to_sql(
            "MATCH (a:User {name: 'Alice'})-[r:WORKS_AT]->(b:Company {name: 'Acme'}) DELETE r",
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
                )
            ),
        )

        self.assertEqual(
            left_and_relationship_set_sql,
            "UPDATE cg_edge_works_at AS r SET since = 2025 FROM cg_node_user AS a, cg_node_company AS b WHERE a.id = r.from_id AND b.id = r.to_id AND a.name = 'Alice' AND r.since = 2020",
        )
        self.assertEqual(
            left_and_relationship_delete_sql,
            "DELETE FROM cg_edge_works_at WHERE id IN (SELECT r.id FROM cg_edge_works_at AS r JOIN cg_node_user AS a ON a.id = r.from_id JOIN cg_node_company AS b ON b.id = r.to_id WHERE a.name = 'Alice' AND r.since = 2020)",
        )
        self.assertEqual(
            both_endpoints_set_sql,
            "UPDATE cg_edge_works_at AS r SET since = 2025 FROM cg_node_user AS a, cg_node_company AS b WHERE a.id = r.from_id AND b.id = r.to_id AND a.name = 'Alice' AND b.name = 'Acme'",
        )
        self.assertEqual(
            both_endpoints_delete_sql,
            "DELETE FROM cg_edge_works_at WHERE id IN (SELECT r.id FROM cg_edge_works_at AS r JOIN cg_node_user AS a ON a.id = r.from_id JOIN cg_node_company AS b ON b.id = r.to_id WHERE a.name = 'Alice' AND b.name = 'Acme')",
        )

    def test_compile_match_relationship_set_delete_with_all_filters(self) -> None:
        set_sql = cypherglot.to_sql(
            "MATCH (a:User {name: 'Alice'})-[r:WORKS_AT]->(b:Company {name: 'Acme'}) WHERE r.since = 2020 SET r.since = 2025",
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
                )
            ),
        )
        delete_sql = cypherglot.to_sql(
            "MATCH (a:User {name: 'Alice'})-[r:WORKS_AT]->(b:Company {name: 'Acme'}) WHERE r.since = 2020 DELETE r",
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
                )
            ),
        )

        self.assertEqual(
            set_sql,
            "UPDATE cg_edge_works_at AS r SET since = 2025 FROM cg_node_user AS a, cg_node_company AS b WHERE a.id = r.from_id AND b.id = r.to_id AND a.name = 'Alice' AND b.name = 'Acme' AND r.since = 2020",
        )
        self.assertEqual(
            delete_sql,
            "DELETE FROM cg_edge_works_at WHERE id IN (SELECT r.id FROM cg_edge_works_at AS r JOIN cg_node_user AS a ON a.id = r.from_id JOIN cg_node_company AS b ON b.id = r.to_id WHERE a.name = 'Alice' AND b.name = 'Acme' AND r.since = 2020)",
        )

    def test_compile_program_traversal_match_create_with_new_right_endpoint(self) -> None:
        program = cypherglot.compile_cypher_program_text(
            "MATCH (a:User)-[r:KNOWS]->(b:User) CREATE (a)-[:INTRODUCED]->(:Person {name: 'Cara'})",
            schema_context=CompilerSchemaContext.type_aware(
                GraphSchema(
                    node_types=(
                        NodeTypeSpec(
                            name="User",
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
                        ),
                        cypherglot.EdgeTypeSpec(
                            name="INTRODUCED",
                            source_type="User",
                            target_type="Person",
                        ),
                    ),
                )
            ),
        )
        self.assertEqual(len(program.steps), 1)
        loop = program.steps[0]
        self.assertIsInstance(loop, cypherglot.CompiledCypherLoop)
        assert isinstance(loop, cypherglot.CompiledCypherLoop)
        self.assertEqual(loop.row_bindings, ("match_node_id",))
        self.assertEqual(
            loop.source.sql(),
            "SELECT a.id AS match_node_id FROM cg_edge_knows AS r JOIN cg_node_user AS a ON a.id = r.from_id JOIN cg_node_user AS b ON b.id = r.to_id",
        )
        self.assertEqual(len(loop.body), 2)
        self.assertEqual(
            loop.body[0].sql.sql(),
            "INSERT INTO cg_node_person (name) VALUES ('Cara') RETURNING id",
        )
        self.assertEqual(
            loop.body[1].sql.sql(),
            "INSERT INTO cg_edge_introduced (from_id, to_id) VALUES (:match_node_id, :created_node_id)",
        )

        filtered_left_program = cypherglot.compile_cypher_program_text(
            "MATCH (a:User {name: 'Alice'})-[r:KNOWS]->(b:User) CREATE (a)-[:INTRODUCED]->(:Person {name: 'Dana'})",
            schema_context=CompilerSchemaContext.type_aware(
                GraphSchema(
                    node_types=(
                        NodeTypeSpec(
                            name="User",
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
                        ),
                        cypherglot.EdgeTypeSpec(
                            name="INTRODUCED",
                            source_type="User",
                            target_type="Person",
                        ),
                    ),
                )
            ),
        )
        filtered_right_program = cypherglot.compile_cypher_program_text(
            "MATCH (a:User)-[r:KNOWS]->(b:User {name: 'Bob'}) CREATE (a)-[:INTRODUCED]->(:Person {name: 'Erin'})",
            schema_context=CompilerSchemaContext.type_aware(
                GraphSchema(
                    node_types=(
                        NodeTypeSpec(
                            name="User",
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
                        ),
                        cypherglot.EdgeTypeSpec(
                            name="INTRODUCED",
                            source_type="User",
                            target_type="Person",
                        ),
                    ),
                )
            ),
        )

        filtered_left_loop = filtered_left_program.steps[0]
        assert isinstance(filtered_left_loop, cypherglot.CompiledCypherLoop)
        self.assertEqual(
            filtered_left_loop.source.sql(),
            "SELECT a.id AS match_node_id FROM cg_edge_knows AS r JOIN cg_node_user AS a ON a.id = r.from_id JOIN cg_node_user AS b ON b.id = r.to_id WHERE a.name = 'Alice'",
        )
        filtered_right_loop = filtered_right_program.steps[0]
        assert isinstance(filtered_right_loop, cypherglot.CompiledCypherLoop)
        self.assertEqual(
            filtered_right_loop.source.sql(),
            "SELECT a.id AS match_node_id FROM cg_edge_knows AS r JOIN cg_node_user AS a ON a.id = r.from_id JOIN cg_node_user AS b ON b.id = r.to_id WHERE b.name = 'Bob'",
        )

    def test_compile_program_traversal_self_loop_match_create_with_new_right_endpoint(
        self,
    ) -> None:
        type_aware_program = cypherglot.compile_cypher_program_text(
            "MATCH (a:User)-[r:KNOWS]->(a:User) CREATE (a)-[:INTRODUCED]->(:Person {name: 'Cara'})",
            schema_context=CompilerSchemaContext.type_aware(
                GraphSchema(
                    node_types=(
                        NodeTypeSpec(
                            name="User",
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
                        ),
                        cypherglot.EdgeTypeSpec(
                            name="INTRODUCED",
                            source_type="User",
                            target_type="Person",
                        ),
                    ),
                )
            ),
        )

        type_aware_loop = type_aware_program.steps[0]
        self.assertIsInstance(type_aware_loop, cypherglot.CompiledCypherLoop)
        assert isinstance(type_aware_loop, cypherglot.CompiledCypherLoop)
        self.assertEqual(type_aware_loop.row_bindings, ("match_node_id",))
        self.assertEqual(
            type_aware_loop.source.sql(),
            "SELECT a.id AS match_node_id FROM cg_edge_knows AS r JOIN cg_node_user AS a ON a.id = r.from_id WHERE r.from_id = r.to_id",
        )
        self.assertEqual(len(type_aware_loop.body), 2)
        self.assertEqual(
            type_aware_loop.body[0].sql.sql(),
            "INSERT INTO cg_node_person (name) VALUES ('Cara') RETURNING id",
        )
        self.assertEqual(
            type_aware_loop.body[1].sql.sql(),
            "INSERT INTO cg_edge_introduced (from_id, to_id) VALUES (:match_node_id, :created_node_id)",
        )

        filtered_left_program = cypherglot.compile_cypher_program_text(
            "MATCH (a:User {name: 'Alice'})-[r:KNOWS]->(b:User) MERGE (a)-[:INTRODUCED]->(:Person {name: 'Dana'})",
            schema_context=CompilerSchemaContext.type_aware(
                GraphSchema(
                    node_types=(
                        NodeTypeSpec(
                            name="User",
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
                        ),
                        cypherglot.EdgeTypeSpec(
                            name="INTRODUCED",
                            source_type="User",
                            target_type="Person",
                        ),
                    ),
                )
            ),
        )
        filtered_right_program = cypherglot.compile_cypher_program_text(
            "MATCH (a:User)-[r:KNOWS]->(b:User {name: 'Bob'}) MERGE (a)-[:INTRODUCED]->(:Person {name: 'Erin'})",
            schema_context=CompilerSchemaContext.type_aware(
                GraphSchema(
                    node_types=(
                        NodeTypeSpec(
                            name="User",
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
                        ),
                        cypherglot.EdgeTypeSpec(
                            name="INTRODUCED",
                            source_type="User",
                            target_type="Person",
                        ),
                    ),
                )
            ),
        )

        filtered_left_loop = filtered_left_program.steps[0]
        assert isinstance(filtered_left_loop, cypherglot.CompiledCypherLoop)
        self.assertEqual(
            filtered_left_loop.source.sql(),
            "SELECT a.id AS match_node_id FROM cg_edge_knows AS r JOIN cg_node_user AS a ON a.id = r.from_id JOIN cg_node_user AS b ON b.id = r.to_id WHERE a.name = 'Alice' AND NOT EXISTS(SELECT 1 FROM cg_edge_introduced AS existing_merge_edge JOIN cg_node_person AS existing_merge_new_node ON existing_merge_new_node.id = existing_merge_edge.to_id WHERE existing_merge_edge.from_id = a.id AND existing_merge_new_node.name = 'Dana' LIMIT 1)",
        )
        filtered_right_loop = filtered_right_program.steps[0]
        assert isinstance(filtered_right_loop, cypherglot.CompiledCypherLoop)
        self.assertEqual(
            filtered_right_loop.source.sql(),
            "SELECT a.id AS match_node_id FROM cg_edge_knows AS r JOIN cg_node_user AS a ON a.id = r.from_id JOIN cg_node_user AS b ON b.id = r.to_id WHERE b.name = 'Bob' AND NOT EXISTS(SELECT 1 FROM cg_edge_introduced AS existing_merge_edge JOIN cg_node_person AS existing_merge_new_node ON existing_merge_new_node.id = existing_merge_edge.to_id WHERE existing_merge_edge.from_id = a.id AND existing_merge_new_node.name = 'Erin' LIMIT 1)",
        )

    def test_compile_program_match_create_with_new_left_endpoint(self) -> None:
        type_aware_program = cypherglot.compile_cypher_program_text(
            "MATCH (b:Company {name: 'Acme'}) CREATE (:User {name: 'Cara'})-[:WORKS_AT {since: 2024}]->(b)",
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
                )
            ),
        )

        self.assertEqual(len(type_aware_program.steps), 1)
        type_aware_loop = type_aware_program.steps[0]
        self.assertIsInstance(type_aware_loop, cypherglot.CompiledCypherLoop)
        assert isinstance(type_aware_loop, cypherglot.CompiledCypherLoop)
        self.assertEqual(type_aware_loop.row_bindings, ("match_node_id",))
        self.assertEqual(
            type_aware_loop.source.sql(),
            "SELECT b.id AS match_node_id FROM cg_node_company AS b WHERE b.name = 'Acme'",
        )
        self.assertEqual(len(type_aware_loop.body), 2)
        self.assertEqual(
            type_aware_loop.body[0].sql.sql(),
            "INSERT INTO cg_node_user (name) VALUES ('Cara') RETURNING id",
        )
        self.assertEqual(
            type_aware_loop.body[1].sql.sql(),
            "INSERT INTO cg_edge_works_at (from_id, to_id, since) VALUES (:created_node_id, :match_node_id, 2024)",
        )

        filtered_left_program = cypherglot.compile_cypher_program_text(
            "MATCH (a:User {name: 'Alice'})-[r:WORKS_AT]->(b:Company) CREATE (:User {name: 'Dana'})-[:WORKS_AT {since: 2025}]->(b)",
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
                )
            ),
        )
        filtered_right_program = cypherglot.compile_cypher_program_text(
            "MATCH (a:User)-[r:WORKS_AT]->(b:Company {name: 'Bravo'}) CREATE (:User {name: 'Erin'})-[:WORKS_AT {since: 2026}]->(b)",
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
                )
            ),
        )

        filtered_left_loop = filtered_left_program.steps[0]
        assert isinstance(filtered_left_loop, cypherglot.CompiledCypherLoop)
        self.assertEqual(
            filtered_left_loop.source.sql(),
            "SELECT b.id AS match_node_id FROM cg_edge_works_at AS r JOIN cg_node_user AS a ON a.id = r.from_id JOIN cg_node_company AS b ON b.id = r.to_id WHERE a.name = 'Alice'",
        )
        filtered_right_loop = filtered_right_program.steps[0]
        assert isinstance(filtered_right_loop, cypherglot.CompiledCypherLoop)
        self.assertEqual(
            filtered_right_loop.source.sql(),
            "SELECT b.id AS match_node_id FROM cg_edge_works_at AS r JOIN cg_node_user AS a ON a.id = r.from_id JOIN cg_node_company AS b ON b.id = r.to_id WHERE b.name = 'Bravo'",
        )

        relationship_filtered_program = cypherglot.compile_cypher_program_text(
            "MATCH (a:User)-[r:WORKS_AT]->(b:Company) WHERE r.since = 2020 CREATE (:User {name: 'Fiona'})-[:WORKS_AT {since: 2027}]->(b)",
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
                )
            ),
        )
        combined_filtered_program = cypherglot.compile_cypher_program_text(
            "MATCH (a:User {name: 'Alice'})-[r:WORKS_AT]->(b:Company) WHERE r.since = 2020 CREATE (:User {name: 'Gina'})-[:WORKS_AT {since: 2028}]->(b)",
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
                )
            ),
        )

        relationship_filtered_loop = relationship_filtered_program.steps[0]
        assert isinstance(relationship_filtered_loop, cypherglot.CompiledCypherLoop)
        self.assertEqual(
            relationship_filtered_loop.source.sql(),
            "SELECT b.id AS match_node_id FROM cg_edge_works_at AS r JOIN cg_node_user AS a ON a.id = r.from_id JOIN cg_node_company AS b ON b.id = r.to_id WHERE r.since = 2020",
        )
        combined_filtered_loop = combined_filtered_program.steps[0]
        assert isinstance(combined_filtered_loop, cypherglot.CompiledCypherLoop)
        self.assertEqual(
            combined_filtered_loop.source.sql(),
            "SELECT b.id AS match_node_id FROM cg_edge_works_at AS r JOIN cg_node_user AS a ON a.id = r.from_id JOIN cg_node_company AS b ON b.id = r.to_id WHERE a.name = 'Alice' AND r.since = 2020",
        )

        right_and_relationship_program = cypherglot.compile_cypher_program_text(
            "MATCH (a:User)-[r:WORKS_AT]->(b:Company {name: 'Bravo'}) WHERE r.since = 2021 CREATE (:User {name: 'Hana'})-[:WORKS_AT {since: 2029}]->(b)",
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
                )
            ),
        )

        right_and_relationship_loop = right_and_relationship_program.steps[0]
        assert isinstance(right_and_relationship_loop, cypherglot.CompiledCypherLoop)
        self.assertEqual(
            right_and_relationship_loop.source.sql(),
            "SELECT b.id AS match_node_id FROM cg_edge_works_at AS r JOIN cg_node_user AS a ON a.id = r.from_id JOIN cg_node_company AS b ON b.id = r.to_id WHERE b.name = 'Bravo' AND r.since = 2021",
        )

        all_filters_program = cypherglot.compile_cypher_program_text(
            "MATCH (a:User {name: 'Bob'})-[r:WORKS_AT]->(b:Company {name: 'Bravo'}) WHERE r.since = 2021 CREATE (:User {name: 'Iris'})-[:WORKS_AT {since: 2030}]->(b)",
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
                )
            ),
        )

        all_filters_loop = all_filters_program.steps[0]
        assert isinstance(all_filters_loop, cypherglot.CompiledCypherLoop)
        self.assertEqual(
            all_filters_loop.source.sql(),
            "SELECT b.id AS match_node_id FROM cg_edge_works_at AS r JOIN cg_node_user AS a ON a.id = r.from_id JOIN cg_node_company AS b ON b.id = r.to_id WHERE a.name = 'Bob' AND b.name = 'Bravo' AND r.since = 2021",
        )

    def test_compile_program_traversal_match_merge_with_new_right_endpoint(self) -> None:
        program = cypherglot.compile_cypher_program_text(
            "MATCH (a:User)-[r:KNOWS]->(b:User) MERGE (a)-[:INTRODUCED]->(:Person {name: 'Cara'})",
            schema_context=CompilerSchemaContext.type_aware(
                GraphSchema(
                    node_types=(
                        NodeTypeSpec(
                            name="User",
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
                        ),
                        cypherglot.EdgeTypeSpec(
                            name="INTRODUCED",
                            source_type="User",
                            target_type="Person",
                        ),
                    ),
                )
            ),
        )
        self.assertEqual(len(program.steps), 1)
        loop = program.steps[0]
        self.assertIsInstance(loop, cypherglot.CompiledCypherLoop)
        assert isinstance(loop, cypherglot.CompiledCypherLoop)
        self.assertEqual(loop.row_bindings, ("match_node_id",))
        self.assertEqual(
            loop.source.sql(),
            "SELECT a.id AS match_node_id FROM cg_edge_knows AS r JOIN cg_node_user AS a ON a.id = r.from_id JOIN cg_node_user AS b ON b.id = r.to_id WHERE NOT EXISTS(SELECT 1 FROM cg_edge_introduced AS existing_merge_edge JOIN cg_node_person AS existing_merge_new_node ON existing_merge_new_node.id = existing_merge_edge.to_id WHERE existing_merge_edge.from_id = a.id AND existing_merge_new_node.name = 'Cara' LIMIT 1)",
        )
        self.assertEqual(len(loop.body), 2)
        self.assertEqual(
            loop.body[0].sql.sql(),
            "INSERT INTO cg_node_person (name) VALUES ('Cara') RETURNING id",
        )
        self.assertEqual(
            loop.body[1].sql.sql(),
            "INSERT INTO cg_edge_introduced (from_id, to_id) VALUES (:match_node_id, :created_node_id)",
        )

    def test_compile_program_traversal_self_loop_match_merge_with_new_right_endpoint(
        self,
    ) -> None:
        type_aware_program = cypherglot.compile_cypher_program_text(
            "MATCH (a:User)-[r:KNOWS]->(a:User) MERGE (a)-[:INTRODUCED]->(:Person {name: 'Cara'})",
            schema_context=CompilerSchemaContext.type_aware(
                GraphSchema(
                    node_types=(
                        NodeTypeSpec(
                            name="User",
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
                        ),
                        cypherglot.EdgeTypeSpec(
                            name="INTRODUCED",
                            source_type="User",
                            target_type="Person",
                        ),
                    ),
                )
            ),
        )

        type_aware_loop = type_aware_program.steps[0]
        self.assertIsInstance(type_aware_loop, cypherglot.CompiledCypherLoop)
        assert isinstance(type_aware_loop, cypherglot.CompiledCypherLoop)
        self.assertEqual(type_aware_loop.row_bindings, ("match_node_id",))
        self.assertEqual(
            type_aware_loop.source.sql(),
            "SELECT a.id AS match_node_id FROM cg_edge_knows AS r JOIN cg_node_user AS a ON a.id = r.from_id WHERE r.from_id = r.to_id AND NOT EXISTS(SELECT 1 FROM cg_edge_introduced AS existing_merge_edge JOIN cg_node_person AS existing_merge_new_node ON existing_merge_new_node.id = existing_merge_edge.to_id WHERE existing_merge_edge.from_id = a.id AND existing_merge_new_node.name = 'Cara' LIMIT 1)",
        )
        self.assertEqual(len(type_aware_loop.body), 2)
        self.assertEqual(
            type_aware_loop.body[0].sql.sql(),
            "INSERT INTO cg_node_person (name) VALUES ('Cara') RETURNING id",
        )
        self.assertEqual(
            type_aware_loop.body[1].sql.sql(),
            "INSERT INTO cg_edge_introduced (from_id, to_id) VALUES (:match_node_id, :created_node_id)",
        )

    def test_compile_program_traversal_match_merge_with_new_left_endpoint(self) -> None:
        type_aware_program = cypherglot.compile_cypher_program_text(
            "MATCH (a:User)-[r:WORKS_AT]->(b:Company {name: 'Acme'}) MERGE (:User {name: 'Cara'})-[:WORKS_AT {since: 2024}]->(b)",
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
                )
            ),
        )

        type_aware_loop = type_aware_program.steps[0]
        assert isinstance(type_aware_loop, cypherglot.CompiledCypherLoop)
        self.assertEqual(
            type_aware_loop.source.sql(),
            "SELECT b.id AS match_node_id FROM cg_edge_works_at AS r JOIN cg_node_user AS a ON a.id = r.from_id JOIN cg_node_company AS b ON b.id = r.to_id WHERE b.name = 'Acme' AND NOT EXISTS(SELECT 1 FROM cg_edge_works_at AS existing_merge_edge JOIN cg_node_user AS existing_merge_new_node ON existing_merge_new_node.id = existing_merge_edge.from_id WHERE existing_merge_edge.to_id = b.id AND existing_merge_edge.since = 2024 AND existing_merge_new_node.name = 'Cara' LIMIT 1)",
        )

        filtered_left_program = cypherglot.compile_cypher_program_text(
            "MATCH (a:User {name: 'Alice'})-[r:WORKS_AT]->(b:Company) MERGE (:User {name: 'Dana'})-[:WORKS_AT {since: 2025}]->(b)",
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
                )
            ),
        )
        filtered_right_program = cypherglot.compile_cypher_program_text(
            "MATCH (a:User)-[r:WORKS_AT]->(b:Company {name: 'Bravo'}) MERGE (:User {name: 'Erin'})-[:WORKS_AT {since: 2026}]->(b)",
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
                )
            ),
        )

        filtered_left_loop = filtered_left_program.steps[0]
        assert isinstance(filtered_left_loop, cypherglot.CompiledCypherLoop)
        self.assertEqual(
            filtered_left_loop.source.sql(),
            "SELECT b.id AS match_node_id FROM cg_edge_works_at AS r JOIN cg_node_user AS a ON a.id = r.from_id JOIN cg_node_company AS b ON b.id = r.to_id WHERE a.name = 'Alice' AND NOT EXISTS(SELECT 1 FROM cg_edge_works_at AS existing_merge_edge JOIN cg_node_user AS existing_merge_new_node ON existing_merge_new_node.id = existing_merge_edge.from_id WHERE existing_merge_edge.to_id = b.id AND existing_merge_edge.since = 2025 AND existing_merge_new_node.name = 'Dana' LIMIT 1)",
        )
        filtered_right_loop = filtered_right_program.steps[0]
        assert isinstance(filtered_right_loop, cypherglot.CompiledCypherLoop)
        self.assertEqual(
            filtered_right_loop.source.sql(),
            "SELECT b.id AS match_node_id FROM cg_edge_works_at AS r JOIN cg_node_user AS a ON a.id = r.from_id JOIN cg_node_company AS b ON b.id = r.to_id WHERE b.name = 'Bravo' AND NOT EXISTS(SELECT 1 FROM cg_edge_works_at AS existing_merge_edge JOIN cg_node_user AS existing_merge_new_node ON existing_merge_new_node.id = existing_merge_edge.from_id WHERE existing_merge_edge.to_id = b.id AND existing_merge_edge.since = 2026 AND existing_merge_new_node.name = 'Erin' LIMIT 1)",
        )

        relationship_filtered_program = cypherglot.compile_cypher_program_text(
            "MATCH (a:User)-[r:WORKS_AT]->(b:Company) WHERE r.since = 2020 MERGE (:User {name: 'Fiona'})-[:WORKS_AT {since: 2027}]->(b)",
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
                )
            ),
        )
        combined_filtered_program = cypherglot.compile_cypher_program_text(
            "MATCH (a:User {name: 'Alice'})-[r:WORKS_AT]->(b:Company) WHERE r.since = 2020 MERGE (:User {name: 'Gina'})-[:WORKS_AT {since: 2028}]->(b)",
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
                )
            ),
        )

        relationship_filtered_loop = relationship_filtered_program.steps[0]
        assert isinstance(relationship_filtered_loop, cypherglot.CompiledCypherLoop)
        self.assertEqual(
            relationship_filtered_loop.source.sql(),
            "SELECT b.id AS match_node_id FROM cg_edge_works_at AS r JOIN cg_node_user AS a ON a.id = r.from_id JOIN cg_node_company AS b ON b.id = r.to_id WHERE r.since = 2020 AND NOT EXISTS(SELECT 1 FROM cg_edge_works_at AS existing_merge_edge JOIN cg_node_user AS existing_merge_new_node ON existing_merge_new_node.id = existing_merge_edge.from_id WHERE existing_merge_edge.to_id = b.id AND existing_merge_edge.since = 2027 AND existing_merge_new_node.name = 'Fiona' LIMIT 1)",
        )
        combined_filtered_loop = combined_filtered_program.steps[0]
        assert isinstance(combined_filtered_loop, cypherglot.CompiledCypherLoop)
        self.assertEqual(
            combined_filtered_loop.source.sql(),
            "SELECT b.id AS match_node_id FROM cg_edge_works_at AS r JOIN cg_node_user AS a ON a.id = r.from_id JOIN cg_node_company AS b ON b.id = r.to_id WHERE a.name = 'Alice' AND r.since = 2020 AND NOT EXISTS(SELECT 1 FROM cg_edge_works_at AS existing_merge_edge JOIN cg_node_user AS existing_merge_new_node ON existing_merge_new_node.id = existing_merge_edge.from_id WHERE existing_merge_edge.to_id = b.id AND existing_merge_edge.since = 2028 AND existing_merge_new_node.name = 'Gina' LIMIT 1)",
        )

        right_and_relationship_program = cypherglot.compile_cypher_program_text(
            "MATCH (a:User)-[r:WORKS_AT]->(b:Company {name: 'Bravo'}) WHERE r.since = 2021 MERGE (:User {name: 'Hana'})-[:WORKS_AT {since: 2029}]->(b)",
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
                )
            ),
        )

        right_and_relationship_loop = right_and_relationship_program.steps[0]
        assert isinstance(right_and_relationship_loop, cypherglot.CompiledCypherLoop)
        self.assertEqual(
            right_and_relationship_loop.source.sql(),
            "SELECT b.id AS match_node_id FROM cg_edge_works_at AS r JOIN cg_node_user AS a ON a.id = r.from_id JOIN cg_node_company AS b ON b.id = r.to_id WHERE b.name = 'Bravo' AND r.since = 2021 AND NOT EXISTS(SELECT 1 FROM cg_edge_works_at AS existing_merge_edge JOIN cg_node_user AS existing_merge_new_node ON existing_merge_new_node.id = existing_merge_edge.from_id WHERE existing_merge_edge.to_id = b.id AND existing_merge_edge.since = 2029 AND existing_merge_new_node.name = 'Hana' LIMIT 1)",
        )

        all_filters_program = cypherglot.compile_cypher_program_text(
            "MATCH (a:User {name: 'Bob'})-[r:WORKS_AT]->(b:Company {name: 'Bravo'}) WHERE r.since = 2021 MERGE (:User {name: 'Iris'})-[:WORKS_AT {since: 2030}]->(b)",
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
                )
            ),
        )

        all_filters_loop = all_filters_program.steps[0]
        assert isinstance(all_filters_loop, cypherglot.CompiledCypherLoop)
        self.assertEqual(
            all_filters_loop.source.sql(),
            "SELECT b.id AS match_node_id FROM cg_edge_works_at AS r JOIN cg_node_user AS a ON a.id = r.from_id JOIN cg_node_company AS b ON b.id = r.to_id WHERE a.name = 'Bob' AND b.name = 'Bravo' AND r.since = 2021 AND NOT EXISTS(SELECT 1 FROM cg_edge_works_at AS existing_merge_edge JOIN cg_node_user AS existing_merge_new_node ON existing_merge_new_node.id = existing_merge_edge.from_id WHERE existing_merge_edge.to_id = b.id AND existing_merge_edge.since = 2030 AND existing_merge_new_node.name = 'Iris' LIMIT 1)",
        )

    def test_compile_program_traversal_self_loop_match_merge_with_new_left_endpoint(
        self,
    ) -> None:
        type_aware_program = cypherglot.compile_cypher_program_text(
            "MATCH (a:User)-[r:KNOWS]->(a:User) MERGE (:User {name: 'Cara'})-[:KNOWS]->(a)",
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
                )
            ),
        )

        type_aware_loop = type_aware_program.steps[0]
        assert isinstance(type_aware_loop, cypherglot.CompiledCypherLoop)
        self.assertEqual(
            type_aware_loop.source.sql(),
            "SELECT a.id AS match_node_id FROM cg_edge_knows AS r JOIN cg_node_user AS a ON a.id = r.from_id WHERE r.from_id = r.to_id AND NOT EXISTS(SELECT 1 FROM cg_edge_knows AS existing_merge_edge JOIN cg_node_user AS existing_merge_new_node ON existing_merge_new_node.id = existing_merge_edge.from_id WHERE existing_merge_edge.to_id = a.id AND existing_merge_new_node.name = 'Cara' LIMIT 1)",
        )
        self.assertEqual(
            type_aware_loop.body[0].sql.sql(),
            "INSERT INTO cg_node_user (name) VALUES ('Cara') RETURNING id",
        )
        self.assertEqual(
            type_aware_loop.body[1].sql.sql(),
            "INSERT INTO cg_edge_knows (from_id, to_id) VALUES (:created_node_id, :match_node_id)",
        )

    def test_compile_program_traversal_match_create_with_new_left_endpoint(self) -> None:
        type_aware_program = cypherglot.compile_cypher_program_text(
            "MATCH (a:User)-[r:WORKS_AT]->(b:Company {name: 'Acme'}) CREATE (:User {name: 'Cara'})-[:WORKS_AT {since: 2024}]->(b)",
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
                )
            ),
        )

        type_aware_loop = type_aware_program.steps[0]
        assert isinstance(type_aware_loop, cypherglot.CompiledCypherLoop)
        self.assertEqual(
            type_aware_loop.source.sql(),
            "SELECT b.id AS match_node_id FROM cg_edge_works_at AS r JOIN cg_node_user AS a ON a.id = r.from_id JOIN cg_node_company AS b ON b.id = r.to_id WHERE b.name = 'Acme'",
        )
        self.assertEqual(len(type_aware_loop.body), 2)
        self.assertEqual(
            type_aware_loop.body[0].sql.sql(),
            "INSERT INTO cg_node_user (name) VALUES ('Cara') RETURNING id",
        )
        self.assertEqual(
            type_aware_loop.body[1].sql.sql(),
            "INSERT INTO cg_edge_works_at (from_id, to_id, since) VALUES (:created_node_id, :match_node_id, 2024)",
        )

    def test_compile_program_traversal_self_loop_match_create_with_new_left_endpoint(
        self,
    ) -> None:
        type_aware_program = cypherglot.compile_cypher_program_text(
            "MATCH (a:User)-[r:KNOWS]->(a:User) CREATE (:User {name: 'Cara'})-[:KNOWS]->(a)",
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
                )
            ),
        )

        type_aware_loop = type_aware_program.steps[0]
        assert isinstance(type_aware_loop, cypherglot.CompiledCypherLoop)
        self.assertEqual(
            type_aware_loop.source.sql(),
            "SELECT a.id AS match_node_id FROM cg_edge_knows AS r JOIN cg_node_user AS a ON a.id = r.from_id WHERE r.from_id = r.to_id",
        )
        self.assertEqual(
            type_aware_loop.body[0].sql.sql(),
            "INSERT INTO cg_node_user (name) VALUES ('Cara') RETURNING id",
        )
        self.assertEqual(
            type_aware_loop.body[1].sql.sql(),
            "INSERT INTO cg_edge_knows (from_id, to_id) VALUES (:created_node_id, :match_node_id)",
        )

    def test_compile_program_match_create_with_new_right_endpoint(self) -> None:
        program = cypherglot.compile_cypher_program_text(
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

        self.assertEqual(len(program.steps), 1)
        loop = program.steps[0]
        self.assertIsInstance(loop, cypherglot.CompiledCypherLoop)
        assert isinstance(loop, cypherglot.CompiledCypherLoop)

        self.assertEqual(loop.row_bindings, ("match_node_id",))
        self.assertEqual(
            loop.source.sql(),
            "SELECT x.id AS match_node_id FROM cg_node_begin AS x",
        )
        self.assertEqual(len(loop.body), 2)
        self.assertEqual(
            loop.body[0].sql.sql(),
            "INSERT INTO cg_node_end (name) VALUES ('finish') RETURNING id",
        )
        self.assertEqual(loop.body[0].bind_columns, ("created_node_id",))
        self.assertEqual(
            loop.body[1].sql.sql(),
            "INSERT INTO cg_edge_type (from_id, to_id) VALUES (:match_node_id, :created_node_id)",
        )

        filtered_type_aware_program = cypherglot.compile_cypher_program_text(
            "MATCH (x:Begin {name: 'start'}) CREATE (x)-[:TYPE]->(:End {name: 'finish'})",
            schema_context=CompilerSchemaContext.type_aware(
                GraphSchema(
                    node_types=(
                        NodeTypeSpec(
                            name="Begin",
                            properties=(PropertyField("name", "string"),),
                        ),
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

        filtered_type_aware_loop = filtered_type_aware_program.steps[0]
        assert isinstance(filtered_type_aware_loop, cypherglot.CompiledCypherLoop)
        self.assertEqual(
            filtered_type_aware_loop.source.sql(),
            "SELECT x.id AS match_node_id FROM cg_node_begin AS x WHERE x.name = 'start'",
        )

    def test_compile_program_match_merge_with_new_right_endpoint(self) -> None:
        program = cypherglot.compile_cypher_program_text(
            "MATCH (x:Begin) MERGE (x)-[:TYPE]->(:End {name: 'finish'})",
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

        self.assertEqual(len(program.steps), 1)
        loop = program.steps[0]
        self.assertIsInstance(loop, cypherglot.CompiledCypherLoop)
        assert isinstance(loop, cypherglot.CompiledCypherLoop)
        self.assertEqual(loop.row_bindings, ("match_node_id",))
        self.assertEqual(
            loop.source.sql(),
            "SELECT x.id AS match_node_id FROM cg_node_begin AS x WHERE NOT EXISTS(SELECT 1 FROM cg_edge_type AS existing_merge_edge JOIN cg_node_end AS existing_merge_new_node ON existing_merge_new_node.id = existing_merge_edge.to_id WHERE existing_merge_edge.from_id = x.id AND existing_merge_new_node.name = 'finish' LIMIT 1)",
        )
        self.assertEqual(len(loop.body), 2)
        self.assertEqual(
            loop.body[0].sql.sql(),
            "INSERT INTO cg_node_end (name) VALUES ('finish') RETURNING id",
        )
        self.assertEqual(
            loop.body[1].sql.sql(),
            "INSERT INTO cg_edge_type (from_id, to_id) VALUES (:match_node_id, :created_node_id)",
        )

        filtered_type_aware_program = cypherglot.compile_cypher_program_text(
            "MATCH (x:Begin {name: 'start'}) MERGE (x)-[:TYPE]->(:End {name: 'finish'})",
            schema_context=CompilerSchemaContext.type_aware(
                GraphSchema(
                    node_types=(
                        NodeTypeSpec(
                            name="Begin",
                            properties=(PropertyField("name", "string"),),
                        ),
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

        filtered_type_aware_loop = filtered_type_aware_program.steps[0]
        assert isinstance(filtered_type_aware_loop, cypherglot.CompiledCypherLoop)
        self.assertEqual(
            filtered_type_aware_loop.source.sql(),
            "SELECT x.id AS match_node_id FROM cg_node_begin AS x WHERE x.name = 'start' AND NOT EXISTS(SELECT 1 FROM cg_edge_type AS existing_merge_edge JOIN cg_node_end AS existing_merge_new_node ON existing_merge_new_node.id = existing_merge_edge.to_id WHERE existing_merge_edge.from_id = x.id AND existing_merge_new_node.name = 'finish' LIMIT 1)",
        )

    def test_compile_program_match_merge_with_new_left_endpoint(self) -> None:
        program = cypherglot.compile_cypher_program_text(
            "MATCH (x:End) MERGE (:Begin {name: 'start'})-[:TYPE]->(x)",
            schema_context=CompilerSchemaContext.type_aware(
                GraphSchema(
                    node_types=(
                        NodeTypeSpec(
                            name="Begin",
                            properties=(PropertyField("name", "string"),),
                        ),
                        NodeTypeSpec(name="End"),
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

        loop = program.steps[0]
        assert isinstance(loop, cypherglot.CompiledCypherLoop)
        self.assertEqual(
            loop.source.sql(),
            "SELECT x.id AS match_node_id FROM cg_node_end AS x WHERE NOT EXISTS(SELECT 1 FROM cg_edge_type AS existing_merge_edge JOIN cg_node_begin AS existing_merge_new_node ON existing_merge_new_node.id = existing_merge_edge.from_id WHERE existing_merge_edge.to_id = x.id AND existing_merge_new_node.name = 'start' LIMIT 1)",
        )
        self.assertEqual(
            loop.body[1].sql.sql(),
            "INSERT INTO cg_edge_type (from_id, to_id) VALUES (:created_node_id, :match_node_id)",
        )

        filtered_type_aware_program = cypherglot.compile_cypher_program_text(
            "MATCH (x:End {name: 'finish'}) MERGE (:Begin {name: 'start'})-[:TYPE]->(x)",
            schema_context=CompilerSchemaContext.type_aware(
                GraphSchema(
                    node_types=(
                        NodeTypeSpec(
                            name="Begin",
                            properties=(PropertyField("name", "string"),),
                        ),
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

        filtered_type_aware_loop = filtered_type_aware_program.steps[0]
        assert isinstance(filtered_type_aware_loop, cypherglot.CompiledCypherLoop)
        self.assertEqual(
            filtered_type_aware_loop.source.sql(),
            "SELECT x.id AS match_node_id FROM cg_node_end AS x WHERE x.name = 'finish' AND NOT EXISTS(SELECT 1 FROM cg_edge_type AS existing_merge_edge JOIN cg_node_begin AS existing_merge_new_node ON existing_merge_new_node.id = existing_merge_edge.from_id WHERE existing_merge_edge.to_id = x.id AND existing_merge_new_node.name = 'start' LIMIT 1)",
        )

    def test_compile_program_match_create_with_new_left_endpoint(self) -> None:
        program = cypherglot.compile_cypher_program_text(
            "MATCH (x:End) CREATE (:Begin {name: 'start'})-[:TYPE]->(x)",
            schema_context=CompilerSchemaContext.type_aware(
                GraphSchema(
                    node_types=(
                        NodeTypeSpec(
                            name="Begin",
                            properties=(PropertyField("name", "string"),),
                        ),
                        NodeTypeSpec(name="End"),
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

        loop = program.steps[0]
        assert isinstance(loop, cypherglot.CompiledCypherLoop)
        self.assertEqual(
            loop.source.sql(),
            "SELECT x.id AS match_node_id FROM cg_node_end AS x",
        )
        self.assertEqual(
            loop.body[0].sql.sql(),
            "INSERT INTO cg_node_begin (name) VALUES ('start') RETURNING id",
        )
        self.assertEqual(
            loop.body[1].sql.sql(),
            "INSERT INTO cg_edge_type (from_id, to_id) VALUES (:created_node_id, :match_node_id)",
        )

        filtered_type_aware_program = cypherglot.compile_cypher_program_text(
            "MATCH (x:End {name: 'finish'}) CREATE (:Begin {name: 'start'})-[:TYPE]->(x)",
            schema_context=CompilerSchemaContext.type_aware(
                GraphSchema(
                    node_types=(
                        NodeTypeSpec(
                            name="Begin",
                            properties=(PropertyField("name", "string"),),
                        ),
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

        filtered_type_aware_loop = filtered_type_aware_program.steps[0]
        assert isinstance(filtered_type_aware_loop, cypherglot.CompiledCypherLoop)
        self.assertEqual(
            filtered_type_aware_loop.source.sql(),
            "SELECT x.id AS match_node_id FROM cg_node_end AS x WHERE x.name = 'finish'",
        )

    def test_compile_rejects_non_match_statement(self) -> None:
        with self.assertRaisesRegex(ValueError, "multi-step SQL program"):
            cypherglot.compile_cypher_text(
                "CREATE (:User {name: 'Alice'})",
                schema_context=_public_api_schema_context(),
            )
