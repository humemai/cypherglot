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
        
            backend="sqlite",)

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
        
            backend="sqlite",)

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
        
            backend="sqlite",)

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
        
            backend="sqlite",)

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
            
            backend="sqlite",)

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
            
            backend="sqlite",)

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
            
            backend="sqlite",)

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
            
            backend="sqlite",)

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
        
            backend="sqlite",)

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
        
            backend="sqlite",)
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
        
            backend="sqlite",)
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
        
            backend="sqlite",)

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
            
            backend="sqlite",)
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
        
            backend="sqlite",)
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
        
            backend="sqlite",)
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
        
            backend="sqlite",)
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
        
            backend="sqlite",)
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
        
            backend="sqlite",)
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
        
            backend="sqlite",)
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
        
            backend="sqlite",)

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
            
            backend="sqlite",)
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
            
            backend="sqlite",)
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
        
            backend="sqlite",)

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
        
            backend="sqlite",)

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
        
            backend="sqlite",)

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
        
            backend="sqlite",)

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
        
            backend="sqlite",)

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
        
            backend="sqlite",)

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
        
            backend="sqlite",)

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

