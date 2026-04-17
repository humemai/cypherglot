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

