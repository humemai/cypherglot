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

