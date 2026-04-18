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
        
            backend="sqlite",)

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
        
            backend="sqlite",)
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
        
            backend="sqlite",)
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
        
            backend="sqlite",)
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
        
            backend="sqlite",)

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
            
            backend="sqlite",)
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
        
            backend="sqlite",)
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
        
            backend="sqlite",)
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
        
            backend="sqlite",)
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
        
            backend="sqlite",)
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
        
            backend="sqlite",)
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
        
            backend="sqlite",)

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
            
            backend="sqlite",)
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
            
            backend="sqlite",)
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
        
            backend="sqlite",)
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
        
            backend="sqlite",)

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
        
            backend="sqlite",)

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
            
            backend="sqlite",)

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
            
            backend="sqlite",)

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
            
            backend="sqlite",)

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
            
            backend="sqlite",)

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
        
            backend="sqlite",)

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
        
            backend="sqlite",)

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
        
            backend="sqlite",)

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
        
            backend="sqlite",)

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
        
            backend="sqlite",)

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
        
            backend="sqlite",)

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
        
            backend="sqlite",)

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
        
            backend="sqlite",)

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
        
            backend="sqlite",)

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
        
            backend="sqlite",)

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
        
            backend="sqlite",)

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

