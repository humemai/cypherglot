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
    def test_to_sql_renders_type_aware_fixed_length_multi_hop_match(self) -> None:
        sql = cypherglot.to_sql(
            (
                "MATCH (a:User)-[r:KNOWS]->(b:User)-[s:WORKS_AT]->(c:Company) "
                "RETURN a.name AS user_name, c.name AS company ORDER BY company"
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
            backend="sqlite",
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
            backend="sqlite",
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
            backend="sqlite",
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
            backend="sqlite",
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

