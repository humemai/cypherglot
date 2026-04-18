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
    def test_to_sql_renders_type_aware_relational_output_mode_bounded_variable_length_match_with_return(
        self,
    ) -> None:
        sql = cypherglot.to_sql(
            (
                "MATCH (a:User)-[:KNOWS*0..2]->(b:User) "
                "WITH b AS friend RETURN friend.name AS name ORDER BY name"
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
        scalar_function_sql = cypherglot.to_sql(
            (
                "MATCH (a:User)-[:KNOWS*0..2]->(b:User) WITH b AS friend "
                "RETURN lower(friend.name) AS lower_name, toString(friend.age) AS age_text "
                "ORDER BY age_text, lower_name"
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
                "MATCH (a:User)-[:KNOWS*0..2]->(b:User) WITH b AS friend "
                "RETURN id(friend) AS friend_id ORDER BY friend_id"
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
        derived_with_sql = cypherglot.to_sql(
            (
                "MATCH (a:User)-[:KNOWS*0..2]->(b:User) "
                "WITH lower(b.name) AS lowered "
                "RETURN lowered ORDER BY lowered"
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
            derived_with_sql,
            'SELECT with_q."__cg_with_scalar_lowered" AS "lowered" FROM (SELECT LOWER(__cg_zero_hop_node.name) AS "__cg_with_scalar_lowered" FROM cg_node_user AS __cg_zero_hop_node UNION ALL SELECT LOWER(b.name) AS "__cg_with_scalar_lowered" FROM cg_edge_knows AS __cg_edge_0 JOIN cg_node_user AS a ON a.id = __cg_edge_0.from_id JOIN cg_node_user AS b ON b.id = __cg_edge_0.to_id UNION ALL SELECT LOWER(b.name) AS "__cg_with_scalar_lowered" FROM cg_edge_knows AS __cg_edge_0 JOIN cg_node_user AS a ON a.id = __cg_edge_0.from_id JOIN cg_node_user AS __cg_variable_hop_2_node_1 ON __cg_variable_hop_2_node_1.id = __cg_edge_0.to_id JOIN cg_edge_knows AS __cg_edge_1 ON __cg_variable_hop_2_node_1.id = __cg_edge_1.from_id JOIN cg_node_user AS b ON b.id = __cg_edge_1.to_id) AS with_q ORDER BY with_q."__cg_with_scalar_lowered" ASC',
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
        node_sql = cypherglot.to_sql(
            (
                "MATCH (a:User)-[:KNOWS*0..2]->(b:User) WITH b AS friend "
                "RETURN friend AS friend_node, count(friend) AS total ORDER BY total DESC, friend_node"
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
                "MATCH (a:User)-[:KNOWS*0..2]->(b:User) WITH b AS friend "
                "RETURN properties(friend) AS friend_props, count(friend) AS total ORDER BY total DESC, friend_props"
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
                "MATCH (a:User)-[:KNOWS*0..2]->(b:User) WITH b AS friend "
                "RETURN lower(friend.name) AS lowered_name, count(friend) AS total ORDER BY total DESC, lowered_name"
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
                "MATCH (a:User)-[:KNOWS*0..2]->(b:User) WITH b AS friend "
                "RETURN toString(friend.age) AS age_text, count(friend) AS total ORDER BY total DESC, age_text"
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
                "MATCH (a:User)-[:KNOWS*0..2]->(b:User) WITH b AS friend "
                "RETURN id(friend) AS friend_id, count(friend) AS total ORDER BY total DESC, friend_id"
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
        aggregate_sql = cypherglot.to_sql(
            (
                "MATCH (a:User)-[:KNOWS*0..2]->(b:User) WITH b AS friend "
                "RETURN friend.name AS name, sum(friend.age) AS total_age ORDER BY total_age DESC, name"
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
                    "MATCH (a:User)-[:KNOWS*0..2]->(b:User) WITH b AS friend "
                    "RETURN keys(friend) AS friend_keys, count(friend) AS total ORDER BY total DESC, friend_keys"
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
            backend="sqlite",
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
            backend="sqlite",
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
            backend="sqlite",
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
            backend="sqlite",
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
            'SELECT c.name AS "company", MAX(s.since) AS "latest_since" '
            'FROM cg_edge_knows AS r '
            'JOIN cg_node_user AS a ON a.id = r.from_id '
            'JOIN cg_node_user AS b ON b.id = r.to_id '
            'JOIN cg_edge_works_at AS s ON b.id = s.from_id '
            'JOIN cg_node_company AS c ON c.id = s.to_id '
            'GROUP BY c.name ORDER BY "latest_since" DESC, c.name ASC',
        )

