from __future__ import annotations

import unittest

import cypherglot


class CompileTests(unittest.TestCase):
    def test_compile_rejects_vector_aware_query_nodes_for_now(self) -> None:
        with self.assertRaisesRegex(ValueError, "does not yet compile vector-aware CALL queries"):
            cypherglot.compile_cypher_program_text(
                "CALL db.index.vector.queryNodes('user_embedding_idx', 1, $query) "
                "YIELD node, score RETURN node.id, score"
            )

    def test_compile_single_statement_api_rejects_vector_aware_query_nodes(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            "carries vector intent forward for host runtimes, but it does not yet compile vector-aware CALL queries into SQLGlot output",
        ):
            cypherglot.compile_cypher_text(
                "CALL db.index.vector.queryNodes('user_embedding_idx', 3, $query) "
                "YIELD node, score WHERE node.region = 'west' RETURN node.id, score"
            )

    def test_compile_match_with_return(self) -> None:
        expression = cypherglot.compile_cypher_text(
            "MATCH (u:User) WITH u AS person RETURN person.name ORDER BY person.name"
        )

        self.assertEqual(
            expression.sql(),
            "SELECT JSON_EXTRACT(with_q.\"__cg_with_person_properties\", '$.name') AS \"person.name\" "
            "FROM (SELECT u.id AS \"__cg_with_person_id\", u.properties AS \"__cg_with_person_properties\" "
            "FROM nodes AS u JOIN node_labels AS u_label_0 ON u_label_0.node_id = u.id "
            "AND u_label_0.label = 'User') AS with_q "
            "ORDER BY JSON_EXTRACT(with_q.\"__cg_with_person_properties\", '$.name') ASC",
        )

    def test_compile_match_with_scalar_rebinding(self) -> None:
        expression = cypherglot.compile_cypher_text(
            "MATCH (u:User) WITH u.name AS name RETURN name ORDER BY name"
        )

        self.assertEqual(
            expression.sql(),
            "SELECT with_q.\"__cg_with_scalar_name\" AS \"name\" "
            "FROM (SELECT JSON_EXTRACT(u.properties, '$.name') AS \"__cg_with_scalar_name\" "
            "FROM nodes AS u JOIN node_labels AS u_label_0 ON u_label_0.node_id = u.id "
            "AND u_label_0.label = 'User') AS with_q "
            "ORDER BY with_q.\"__cg_with_scalar_name\" ASC",
        )

    def test_compile_match_with_entity_passthrough_return(self) -> None:
        expression = cypherglot.compile_cypher_text(
            "MATCH (u:User) WITH u AS person, u.name AS name RETURN person, name ORDER BY name"
        )
        ordered_expression = cypherglot.compile_cypher_text(
            "MATCH (u:User) WITH u AS person RETURN person AS user ORDER BY user"
        )
        relationship_ordered_expression = cypherglot.compile_cypher_text(
            "MATCH (a:User)-[r:KNOWS]->(b:User) WITH r AS rel RETURN rel AS edge ORDER BY edge"
        )

        self.assertEqual(
            expression.sql(),
            "SELECT JSON_OBJECT('id': with_q.\"__cg_with_person_id\", 'label': "
            "(SELECT person_label_return.label FROM node_labels AS person_label_return "
            "WHERE person_label_return.node_id = with_q.\"__cg_with_person_id\" LIMIT 1), "
            "'properties': JSON(COALESCE(with_q.\"__cg_with_person_properties\", '{}'))) AS \"person\", "
            "with_q.\"__cg_with_scalar_name\" AS \"name\" "
            "FROM (SELECT u.id AS \"__cg_with_person_id\", u.properties AS \"__cg_with_person_properties\", "
            "JSON_EXTRACT(u.properties, '$.name') AS \"__cg_with_scalar_name\" "
            "FROM nodes AS u JOIN node_labels AS u_label_0 ON u_label_0.node_id = u.id "
            "AND u_label_0.label = 'User') AS with_q "
            "ORDER BY with_q.\"__cg_with_scalar_name\" ASC",
        )
        self.assertEqual(
            ordered_expression.sql(),
            "SELECT JSON_OBJECT('id': with_q.\"__cg_with_person_id\", 'label': "
            "(SELECT person_label_return.label FROM node_labels AS person_label_return "
            "WHERE person_label_return.node_id = with_q.\"__cg_with_person_id\" LIMIT 1), "
            "'properties': JSON(COALESCE(with_q.\"__cg_with_person_properties\", '{}'))) AS \"user\" "
            "FROM (SELECT u.id AS \"__cg_with_person_id\", u.properties AS \"__cg_with_person_properties\" "
            "FROM nodes AS u JOIN node_labels AS u_label_0 ON u_label_0.node_id = u.id "
            "AND u_label_0.label = 'User') AS with_q "
            "ORDER BY JSON_OBJECT('id': with_q.\"__cg_with_person_id\", 'label': "
            "(SELECT person_label_return.label FROM node_labels AS person_label_return "
            "WHERE person_label_return.node_id = with_q.\"__cg_with_person_id\" LIMIT 1), "
            "'properties': JSON(COALESCE(with_q.\"__cg_with_person_properties\", '{}'))) ASC",
        )
        self.assertEqual(
            relationship_ordered_expression.sql(),
            "SELECT JSON_OBJECT('id': with_q.\"__cg_with_rel_id\", 'type': with_q.\"__cg_with_rel_type\", 'properties': JSON(COALESCE(with_q.\"__cg_with_rel_properties\", '{}'))) AS \"edge\" "
            "FROM (SELECT r.id AS \"__cg_with_rel_id\", r.type AS \"__cg_with_rel_type\", r.properties AS \"__cg_with_rel_properties\" "
            "FROM edges AS r JOIN nodes AS a ON a.id = r.from_id JOIN nodes AS b ON b.id = r.to_id "
            "JOIN node_labels AS a_label_0 ON a_label_0.node_id = a.id AND a_label_0.label = 'User' "
            "JOIN node_labels AS b_label_1 ON b_label_1.node_id = b.id AND b_label_1.label = 'User' "
            "WHERE r.type = 'KNOWS') AS with_q "
            "ORDER BY JSON_OBJECT('id': with_q.\"__cg_with_rel_id\", 'type': with_q.\"__cg_with_rel_type\", 'properties': JSON(COALESCE(with_q.\"__cg_with_rel_properties\", '{}'))) ASC",
        )

    def test_compile_fixed_length_multi_hop_match(self) -> None:
        expression = cypherglot.compile_cypher_text(
            "MATCH (a:User)-[r:KNOWS]->(b:User)-[s:WORKS_AT]->(c:Company) RETURN a.name AS user_name, c.name AS company ORDER BY company"
        )

        self.assertEqual(
            expression.sql(),
            "SELECT JSON_EXTRACT(a.properties, '$.name') AS \"user_name\", JSON_EXTRACT(c.properties, '$.name') AS \"company\" "
            "FROM edges AS r JOIN nodes AS a ON a.id = r.from_id JOIN nodes AS b ON b.id = r.to_id "
            "JOIN edges AS s ON b.id = s.from_id JOIN nodes AS c ON c.id = s.to_id "
            "JOIN node_labels AS a_label_0 ON a_label_0.node_id = a.id AND a_label_0.label = 'User' "
            "JOIN node_labels AS b_label_1 ON b_label_1.node_id = b.id AND b_label_1.label = 'User' "
            "JOIN node_labels AS c_label_2 ON c_label_2.node_id = c.id AND c_label_2.label = 'Company' "
            "WHERE r.type = 'KNOWS' AND s.type = 'WORKS_AT' "
            "ORDER BY JSON_EXTRACT(c.properties, '$.name') ASC",
        )

    def test_compile_fixed_length_multi_hop_match_with_return(self) -> None:
        expression = cypherglot.compile_cypher_text(
            "MATCH (a:User)-[r:KNOWS]->(b:User)-[s:WORKS_AT]->(c:Company) WITH b AS friend, c.name AS company RETURN friend.name, company ORDER BY company"
        )

        self.assertEqual(
            expression.sql(),
            "SELECT JSON_EXTRACT(with_q.\"__cg_with_friend_properties\", '$.name') AS \"friend.name\", with_q.\"__cg_with_scalar_company\" AS \"company\" "
            "FROM (SELECT b.id AS \"__cg_with_friend_id\", b.properties AS \"__cg_with_friend_properties\", "
            "JSON_EXTRACT(c.properties, '$.name') AS \"__cg_with_scalar_company\" "
            "FROM edges AS r JOIN nodes AS a ON a.id = r.from_id JOIN nodes AS b ON b.id = r.to_id "
            "JOIN edges AS s ON b.id = s.from_id JOIN nodes AS c ON c.id = s.to_id "
            "JOIN node_labels AS a_label_0 ON a_label_0.node_id = a.id AND a_label_0.label = 'User' "
            "JOIN node_labels AS b_label_1 ON b_label_1.node_id = b.id AND b_label_1.label = 'User' "
            "JOIN node_labels AS c_label_2 ON c_label_2.node_id = c.id AND c_label_2.label = 'Company' "
            "WHERE r.type = 'KNOWS' AND s.type = 'WORKS_AT') AS with_q "
            "ORDER BY with_q.\"__cg_with_scalar_company\" ASC",
        )

    def test_compile_bounded_variable_length_match(self) -> None:
        expression = cypherglot.compile_cypher_text(
            "MATCH (a:User)-[:KNOWS*1..2]->(b:User) RETURN a.name AS user_name, b.name AS friend ORDER BY friend"
        )
        zero_hop_expression = cypherglot.compile_cypher_text(
            "MATCH (a:User)-[:KNOWS*0..2]->(b:User) RETURN a.name AS user_name, b.name AS friend ORDER BY friend"
        )
        alias_expression = cypherglot.compile_cypher_text(
            "MATCH (a:User)-[r:KNOWS*1..2]->(b:User) RETURN a.name AS user_name, b.name AS friend ORDER BY friend"
        )

        self.assertEqual(
            expression.sql(),
            "SELECT * FROM (SELECT JSON_EXTRACT(a.properties, '$.name') AS \"user_name\", JSON_EXTRACT(b.properties, '$.name') AS \"friend\" "
            "FROM edges AS __cg_edge_0 JOIN nodes AS a ON a.id = __cg_edge_0.from_id JOIN nodes AS b ON b.id = __cg_edge_0.to_id "
            "JOIN node_labels AS a_label_0 ON a_label_0.node_id = a.id AND a_label_0.label = 'User' "
            "JOIN node_labels AS b_label_1 ON b_label_1.node_id = b.id AND b_label_1.label = 'User' "
            "WHERE __cg_edge_0.type = 'KNOWS' UNION ALL SELECT JSON_EXTRACT(a.properties, '$.name') AS \"user_name\", JSON_EXTRACT(b.properties, '$.name') AS \"friend\" "
            "FROM edges AS __cg_edge_0 JOIN nodes AS a ON a.id = __cg_edge_0.from_id JOIN nodes AS __cg_variable_hop_2_node_1 ON __cg_variable_hop_2_node_1.id = __cg_edge_0.to_id "
            "JOIN edges AS __cg_edge_1 ON __cg_variable_hop_2_node_1.id = __cg_edge_1.from_id JOIN nodes AS b ON b.id = __cg_edge_1.to_id "
            "JOIN node_labels AS a_label_0 ON a_label_0.node_id = a.id AND a_label_0.label = 'User' "
            "JOIN node_labels AS b_label_2 ON b_label_2.node_id = b.id AND b_label_2.label = 'User' "
            "WHERE __cg_edge_0.type = 'KNOWS' AND __cg_edge_1.type = 'KNOWS') AS variable_length_q "
            "ORDER BY variable_length_q.\"friend\" ASC",
        )
        self.assertEqual(alias_expression.sql(), expression.sql())
        self.assertEqual(
            zero_hop_expression.sql(),
            "SELECT * FROM (SELECT JSON_EXTRACT(__cg_zero_hop_node.properties, '$.name') AS \"user_name\", JSON_EXTRACT(__cg_zero_hop_node.properties, '$.name') AS \"friend\" "
            "FROM nodes AS __cg_zero_hop_node "
            "JOIN node_labels AS __cg_zero_hop_node_left_label_0 ON __cg_zero_hop_node_left_label_0.node_id = __cg_zero_hop_node.id AND __cg_zero_hop_node_left_label_0.label = 'User' "
            "JOIN node_labels AS __cg_zero_hop_node_right_label_1 ON __cg_zero_hop_node_right_label_1.node_id = __cg_zero_hop_node.id AND __cg_zero_hop_node_right_label_1.label = 'User' "
            "UNION ALL SELECT JSON_EXTRACT(a.properties, '$.name') AS \"user_name\", JSON_EXTRACT(b.properties, '$.name') AS \"friend\" "
            "FROM edges AS __cg_edge_0 JOIN nodes AS a ON a.id = __cg_edge_0.from_id JOIN nodes AS b ON b.id = __cg_edge_0.to_id "
            "JOIN node_labels AS a_label_0 ON a_label_0.node_id = a.id AND a_label_0.label = 'User' "
            "JOIN node_labels AS b_label_1 ON b_label_1.node_id = b.id AND b_label_1.label = 'User' "
            "WHERE __cg_edge_0.type = 'KNOWS' UNION ALL SELECT JSON_EXTRACT(a.properties, '$.name') AS \"user_name\", JSON_EXTRACT(b.properties, '$.name') AS \"friend\" "
            "FROM edges AS __cg_edge_0 JOIN nodes AS a ON a.id = __cg_edge_0.from_id JOIN nodes AS __cg_variable_hop_2_node_1 ON __cg_variable_hop_2_node_1.id = __cg_edge_0.to_id "
            "JOIN edges AS __cg_edge_1 ON __cg_variable_hop_2_node_1.id = __cg_edge_1.from_id JOIN nodes AS b ON b.id = __cg_edge_1.to_id "
            "JOIN node_labels AS a_label_0 ON a_label_0.node_id = a.id AND a_label_0.label = 'User' "
            "JOIN node_labels AS b_label_2 ON b_label_2.node_id = b.id AND b_label_2.label = 'User' "
            "WHERE __cg_edge_0.type = 'KNOWS' AND __cg_edge_1.type = 'KNOWS') AS variable_length_q "
            "ORDER BY variable_length_q.\"friend\" ASC",
        )

    def test_compile_bounded_variable_length_match_with_return(self) -> None:
        expression = cypherglot.compile_cypher_text(
            "MATCH (a:User)-[:KNOWS*..2]->(b:User) WITH b AS friend RETURN friend.name ORDER BY friend.name"
        )
        zero_hop_expression = cypherglot.compile_cypher_text(
            "MATCH (a:User)-[:KNOWS*0..2]->(b:User) WITH b AS friend RETURN friend.name ORDER BY friend.name"
        )

        self.assertEqual(
            expression.sql(),
            "SELECT JSON_EXTRACT(with_q.\"__cg_with_friend_properties\", '$.name') AS \"friend.name\" "
            "FROM (SELECT b.id AS \"__cg_with_friend_id\", b.properties AS \"__cg_with_friend_properties\" "
            "FROM edges AS __cg_edge_0 JOIN nodes AS a ON a.id = __cg_edge_0.from_id JOIN nodes AS b ON b.id = __cg_edge_0.to_id "
            "JOIN node_labels AS a_label_0 ON a_label_0.node_id = a.id AND a_label_0.label = 'User' "
            "JOIN node_labels AS b_label_1 ON b_label_1.node_id = b.id AND b_label_1.label = 'User' "
            "WHERE __cg_edge_0.type = 'KNOWS' UNION ALL SELECT b.id AS \"__cg_with_friend_id\", b.properties AS \"__cg_with_friend_properties\" "
            "FROM edges AS __cg_edge_0 JOIN nodes AS a ON a.id = __cg_edge_0.from_id JOIN nodes AS __cg_variable_hop_2_node_1 ON __cg_variable_hop_2_node_1.id = __cg_edge_0.to_id "
            "JOIN edges AS __cg_edge_1 ON __cg_variable_hop_2_node_1.id = __cg_edge_1.from_id JOIN nodes AS b ON b.id = __cg_edge_1.to_id "
            "JOIN node_labels AS a_label_0 ON a_label_0.node_id = a.id AND a_label_0.label = 'User' "
            "JOIN node_labels AS b_label_2 ON b_label_2.node_id = b.id AND b_label_2.label = 'User' "
            "WHERE __cg_edge_0.type = 'KNOWS' AND __cg_edge_1.type = 'KNOWS') AS with_q "
            "ORDER BY JSON_EXTRACT(with_q.\"__cg_with_friend_properties\", '$.name') ASC",
        )
        self.assertEqual(
            zero_hop_expression.sql(),
            "SELECT JSON_EXTRACT(with_q.\"__cg_with_friend_properties\", '$.name') AS \"friend.name\" "
            "FROM (SELECT __cg_zero_hop_node.id AS \"__cg_with_friend_id\", __cg_zero_hop_node.properties AS \"__cg_with_friend_properties\" "
            "FROM nodes AS __cg_zero_hop_node "
            "JOIN node_labels AS __cg_zero_hop_node_left_label_0 ON __cg_zero_hop_node_left_label_0.node_id = __cg_zero_hop_node.id AND __cg_zero_hop_node_left_label_0.label = 'User' "
            "JOIN node_labels AS __cg_zero_hop_node_right_label_1 ON __cg_zero_hop_node_right_label_1.node_id = __cg_zero_hop_node.id AND __cg_zero_hop_node_right_label_1.label = 'User' "
            "UNION ALL SELECT b.id AS \"__cg_with_friend_id\", b.properties AS \"__cg_with_friend_properties\" "
            "FROM edges AS __cg_edge_0 JOIN nodes AS a ON a.id = __cg_edge_0.from_id JOIN nodes AS b ON b.id = __cg_edge_0.to_id "
            "JOIN node_labels AS a_label_0 ON a_label_0.node_id = a.id AND a_label_0.label = 'User' "
            "JOIN node_labels AS b_label_1 ON b_label_1.node_id = b.id AND b_label_1.label = 'User' "
            "WHERE __cg_edge_0.type = 'KNOWS' UNION ALL SELECT b.id AS \"__cg_with_friend_id\", b.properties AS \"__cg_with_friend_properties\" "
            "FROM edges AS __cg_edge_0 JOIN nodes AS a ON a.id = __cg_edge_0.from_id JOIN nodes AS __cg_variable_hop_2_node_1 ON __cg_variable_hop_2_node_1.id = __cg_edge_0.to_id "
            "JOIN edges AS __cg_edge_1 ON __cg_variable_hop_2_node_1.id = __cg_edge_1.from_id JOIN nodes AS b ON b.id = __cg_edge_1.to_id "
            "JOIN node_labels AS a_label_0 ON a_label_0.node_id = a.id AND a_label_0.label = 'User' "
            "JOIN node_labels AS b_label_2 ON b_label_2.node_id = b.id AND b_label_2.label = 'User' "
            "WHERE __cg_edge_0.type = 'KNOWS' AND __cg_edge_1.type = 'KNOWS') AS with_q "
            "ORDER BY JSON_EXTRACT(with_q.\"__cg_with_friend_properties\", '$.name') ASC",
        )

    def test_compile_bounded_variable_length_match_with_plain_read_count(self) -> None:
        expression = cypherglot.compile_cypher_text(
            "MATCH (a:User)-[:KNOWS*1..2]->(b:User) RETURN count(b) AS total"
        )
        zero_hop_expression = cypherglot.compile_cypher_text(
            "MATCH (a:User)-[:KNOWS*0..2]->(b:User) RETURN count(b) AS total"
        )
        star_expression = cypherglot.compile_cypher_text(
            "MATCH (a:User)-[:KNOWS*1..2]->(b:User) RETURN count(*) AS total"
        )

        self.assertEqual(
            expression.sql(),
            'SELECT COUNT(variable_length_q."__cg_aggregate_0") AS "total" '
            'FROM (SELECT b.id AS "__cg_aggregate_0" '
            'FROM edges AS __cg_edge_0 JOIN nodes AS a ON a.id = __cg_edge_0.from_id JOIN nodes AS b ON b.id = __cg_edge_0.to_id '
            "JOIN node_labels AS a_label_0 ON a_label_0.node_id = a.id AND a_label_0.label = 'User' "
            "JOIN node_labels AS b_label_1 ON b_label_1.node_id = b.id AND b_label_1.label = 'User' "
            "WHERE __cg_edge_0.type = 'KNOWS' UNION ALL SELECT b.id AS \"__cg_aggregate_0\" "
            "FROM edges AS __cg_edge_0 JOIN nodes AS a ON a.id = __cg_edge_0.from_id JOIN nodes AS __cg_variable_hop_2_node_1 ON __cg_variable_hop_2_node_1.id = __cg_edge_0.to_id "
            "JOIN edges AS __cg_edge_1 ON __cg_variable_hop_2_node_1.id = __cg_edge_1.from_id JOIN nodes AS b ON b.id = __cg_edge_1.to_id "
            "JOIN node_labels AS a_label_0 ON a_label_0.node_id = a.id AND a_label_0.label = 'User' "
            "JOIN node_labels AS b_label_2 ON b_label_2.node_id = b.id AND b_label_2.label = 'User' "
            "WHERE __cg_edge_0.type = 'KNOWS' AND __cg_edge_1.type = 'KNOWS') AS variable_length_q",
        )
        self.assertEqual(
            zero_hop_expression.sql(),
            'SELECT COUNT(variable_length_q."__cg_aggregate_0") AS "total" '
            'FROM (SELECT __cg_zero_hop_node.id AS "__cg_aggregate_0" '
            'FROM nodes AS __cg_zero_hop_node '
            "JOIN node_labels AS __cg_zero_hop_node_left_label_0 ON __cg_zero_hop_node_left_label_0.node_id = __cg_zero_hop_node.id AND __cg_zero_hop_node_left_label_0.label = 'User' "
            "JOIN node_labels AS __cg_zero_hop_node_right_label_1 ON __cg_zero_hop_node_right_label_1.node_id = __cg_zero_hop_node.id AND __cg_zero_hop_node_right_label_1.label = 'User' "
            "UNION ALL SELECT b.id AS \"__cg_aggregate_0\" "
            "FROM edges AS __cg_edge_0 JOIN nodes AS a ON a.id = __cg_edge_0.from_id JOIN nodes AS b ON b.id = __cg_edge_0.to_id "
            "JOIN node_labels AS a_label_0 ON a_label_0.node_id = a.id AND a_label_0.label = 'User' "
            "JOIN node_labels AS b_label_1 ON b_label_1.node_id = b.id AND b_label_1.label = 'User' "
            "WHERE __cg_edge_0.type = 'KNOWS' UNION ALL SELECT b.id AS \"__cg_aggregate_0\" "
            "FROM edges AS __cg_edge_0 JOIN nodes AS a ON a.id = __cg_edge_0.from_id JOIN nodes AS __cg_variable_hop_2_node_1 ON __cg_variable_hop_2_node_1.id = __cg_edge_0.to_id "
            "JOIN edges AS __cg_edge_1 ON __cg_variable_hop_2_node_1.id = __cg_edge_1.from_id JOIN nodes AS b ON b.id = __cg_edge_1.to_id "
            "JOIN node_labels AS a_label_0 ON a_label_0.node_id = a.id AND a_label_0.label = 'User' "
            "JOIN node_labels AS b_label_2 ON b_label_2.node_id = b.id AND b_label_2.label = 'User' "
            "WHERE __cg_edge_0.type = 'KNOWS' AND __cg_edge_1.type = 'KNOWS') AS variable_length_q",
        )
        self.assertEqual(
            star_expression.sql(),
            'SELECT COUNT(*) AS "total" '
            'FROM (SELECT 1 AS "__cg_aggregate_0" '
            'FROM edges AS __cg_edge_0 JOIN nodes AS a ON a.id = __cg_edge_0.from_id JOIN nodes AS b ON b.id = __cg_edge_0.to_id '
            "JOIN node_labels AS a_label_0 ON a_label_0.node_id = a.id AND a_label_0.label = 'User' "
            "JOIN node_labels AS b_label_1 ON b_label_1.node_id = b.id AND b_label_1.label = 'User' "
            "WHERE __cg_edge_0.type = 'KNOWS' UNION ALL SELECT 1 AS \"__cg_aggregate_0\" "
            "FROM edges AS __cg_edge_0 JOIN nodes AS a ON a.id = __cg_edge_0.from_id JOIN nodes AS __cg_variable_hop_2_node_1 ON __cg_variable_hop_2_node_1.id = __cg_edge_0.to_id "
            "JOIN edges AS __cg_edge_1 ON __cg_variable_hop_2_node_1.id = __cg_edge_1.from_id JOIN nodes AS b ON b.id = __cg_edge_1.to_id "
            "JOIN node_labels AS a_label_0 ON a_label_0.node_id = a.id AND a_label_0.label = 'User' "
            "JOIN node_labels AS b_label_2 ON b_label_2.node_id = b.id AND b_label_2.label = 'User' "
            "WHERE __cg_edge_0.type = 'KNOWS' AND __cg_edge_1.type = 'KNOWS') AS variable_length_q",
        )

    def test_compile_bounded_variable_length_match_with_plain_read_field_aggregate(self) -> None:
        expression = cypherglot.compile_cypher_text(
            "MATCH (a:User)-[:KNOWS*1..2]->(b:User) RETURN sum(b.score) AS total"
        )
        zero_hop_expression = cypherglot.compile_cypher_text(
            "MATCH (a:User)-[:KNOWS*0..2]->(b:User) RETURN max(b.score) AS top ORDER BY top DESC"
        )

        self.assertEqual(
            expression.sql(),
            "SELECT SUM(variable_length_q.\"__cg_aggregate_0\") AS \"total\" "
            "FROM (SELECT JSON_EXTRACT(b.properties, '$.score') AS \"__cg_aggregate_0\" "
            "FROM edges AS __cg_edge_0 JOIN nodes AS a ON a.id = __cg_edge_0.from_id JOIN nodes AS b ON b.id = __cg_edge_0.to_id "
            "JOIN node_labels AS a_label_0 ON a_label_0.node_id = a.id AND a_label_0.label = 'User' "
            "JOIN node_labels AS b_label_1 ON b_label_1.node_id = b.id AND b_label_1.label = 'User' "
            "WHERE __cg_edge_0.type = 'KNOWS' UNION ALL SELECT JSON_EXTRACT(b.properties, '$.score') AS \"__cg_aggregate_0\" "
            "FROM edges AS __cg_edge_0 JOIN nodes AS a ON a.id = __cg_edge_0.from_id JOIN nodes AS __cg_variable_hop_2_node_1 ON __cg_variable_hop_2_node_1.id = __cg_edge_0.to_id "
            "JOIN edges AS __cg_edge_1 ON __cg_variable_hop_2_node_1.id = __cg_edge_1.from_id JOIN nodes AS b ON b.id = __cg_edge_1.to_id "
            "JOIN node_labels AS a_label_0 ON a_label_0.node_id = a.id AND a_label_0.label = 'User' "
            "JOIN node_labels AS b_label_2 ON b_label_2.node_id = b.id AND b_label_2.label = 'User' "
            "WHERE __cg_edge_0.type = 'KNOWS' AND __cg_edge_1.type = 'KNOWS') AS variable_length_q",
        )
        self.assertEqual(
            zero_hop_expression.sql(),
            "SELECT MAX(variable_length_q.\"__cg_aggregate_0\") AS \"top\" "
            "FROM (SELECT JSON_EXTRACT(__cg_zero_hop_node.properties, '$.score') AS \"__cg_aggregate_0\" "
            "FROM nodes AS __cg_zero_hop_node "
            "JOIN node_labels AS __cg_zero_hop_node_left_label_0 ON __cg_zero_hop_node_left_label_0.node_id = __cg_zero_hop_node.id AND __cg_zero_hop_node_left_label_0.label = 'User' "
            "JOIN node_labels AS __cg_zero_hop_node_right_label_1 ON __cg_zero_hop_node_right_label_1.node_id = __cg_zero_hop_node.id AND __cg_zero_hop_node_right_label_1.label = 'User' "
            "UNION ALL SELECT JSON_EXTRACT(b.properties, '$.score') AS \"__cg_aggregate_0\" "
            "FROM edges AS __cg_edge_0 JOIN nodes AS a ON a.id = __cg_edge_0.from_id JOIN nodes AS b ON b.id = __cg_edge_0.to_id "
            "JOIN node_labels AS a_label_0 ON a_label_0.node_id = a.id AND a_label_0.label = 'User' "
            "JOIN node_labels AS b_label_1 ON b_label_1.node_id = b.id AND b_label_1.label = 'User' "
            "WHERE __cg_edge_0.type = 'KNOWS' UNION ALL SELECT JSON_EXTRACT(b.properties, '$.score') AS \"__cg_aggregate_0\" "
            "FROM edges AS __cg_edge_0 JOIN nodes AS a ON a.id = __cg_edge_0.from_id JOIN nodes AS __cg_variable_hop_2_node_1 ON __cg_variable_hop_2_node_1.id = __cg_edge_0.to_id "
            "JOIN edges AS __cg_edge_1 ON __cg_variable_hop_2_node_1.id = __cg_edge_1.from_id JOIN nodes AS b ON b.id = __cg_edge_1.to_id "
            "JOIN node_labels AS a_label_0 ON a_label_0.node_id = a.id AND a_label_0.label = 'User' "
            "JOIN node_labels AS b_label_2 ON b_label_2.node_id = b.id AND b_label_2.label = 'User' "
            "WHERE __cg_edge_0.type = 'KNOWS' AND __cg_edge_1.type = 'KNOWS') AS variable_length_q "
            "ORDER BY \"top\" DESC",
        )

    def test_compile_bounded_variable_length_match_with_grouped_plain_read_aggregate(self) -> None:
        expression = cypherglot.compile_cypher_text(
            "MATCH (a:User)-[:KNOWS*1..2]->(b:User) RETURN b.name AS friend, count(b) AS total ORDER BY total DESC"
        )
        field_aggregate_expression = cypherglot.compile_cypher_text(
            "MATCH (a:User)-[:KNOWS*0..2]->(b:User) RETURN b.name AS friend, max(b.score) AS top ORDER BY top DESC"
        )

        self.assertEqual(
            expression.sql(),
            "SELECT variable_length_q.\"friend\" AS \"friend\", COUNT(variable_length_q.\"__cg_aggregate_1\") AS \"total\" "
            "FROM (SELECT JSON_EXTRACT(b.properties, '$.name') AS \"friend\", b.id AS \"__cg_aggregate_1\" "
            "FROM edges AS __cg_edge_0 JOIN nodes AS a ON a.id = __cg_edge_0.from_id JOIN nodes AS b ON b.id = __cg_edge_0.to_id "
            "JOIN node_labels AS a_label_0 ON a_label_0.node_id = a.id AND a_label_0.label = 'User' "
            "JOIN node_labels AS b_label_1 ON b_label_1.node_id = b.id AND b_label_1.label = 'User' "
            "WHERE __cg_edge_0.type = 'KNOWS' UNION ALL SELECT JSON_EXTRACT(b.properties, '$.name') AS \"friend\", b.id AS \"__cg_aggregate_1\" "
            "FROM edges AS __cg_edge_0 JOIN nodes AS a ON a.id = __cg_edge_0.from_id JOIN nodes AS __cg_variable_hop_2_node_1 ON __cg_variable_hop_2_node_1.id = __cg_edge_0.to_id "
            "JOIN edges AS __cg_edge_1 ON __cg_variable_hop_2_node_1.id = __cg_edge_1.from_id JOIN nodes AS b ON b.id = __cg_edge_1.to_id "
            "JOIN node_labels AS a_label_0 ON a_label_0.node_id = a.id AND a_label_0.label = 'User' "
            "JOIN node_labels AS b_label_2 ON b_label_2.node_id = b.id AND b_label_2.label = 'User' "
            "WHERE __cg_edge_0.type = 'KNOWS' AND __cg_edge_1.type = 'KNOWS') AS variable_length_q "
            "GROUP BY variable_length_q.\"friend\" ORDER BY \"total\" DESC",
        )
        self.assertEqual(
            field_aggregate_expression.sql(),
            "SELECT variable_length_q.\"friend\" AS \"friend\", MAX(variable_length_q.\"__cg_aggregate_1\") AS \"top\" "
            "FROM (SELECT JSON_EXTRACT(__cg_zero_hop_node.properties, '$.name') AS \"friend\", JSON_EXTRACT(__cg_zero_hop_node.properties, '$.score') AS \"__cg_aggregate_1\" "
            "FROM nodes AS __cg_zero_hop_node "
            "JOIN node_labels AS __cg_zero_hop_node_left_label_0 ON __cg_zero_hop_node_left_label_0.node_id = __cg_zero_hop_node.id AND __cg_zero_hop_node_left_label_0.label = 'User' "
            "JOIN node_labels AS __cg_zero_hop_node_right_label_1 ON __cg_zero_hop_node_right_label_1.node_id = __cg_zero_hop_node.id AND __cg_zero_hop_node_right_label_1.label = 'User' "
            "UNION ALL SELECT JSON_EXTRACT(b.properties, '$.name') AS \"friend\", JSON_EXTRACT(b.properties, '$.score') AS \"__cg_aggregate_1\" "
            "FROM edges AS __cg_edge_0 JOIN nodes AS a ON a.id = __cg_edge_0.from_id JOIN nodes AS b ON b.id = __cg_edge_0.to_id "
            "JOIN node_labels AS a_label_0 ON a_label_0.node_id = a.id AND a_label_0.label = 'User' "
            "JOIN node_labels AS b_label_1 ON b_label_1.node_id = b.id AND b_label_1.label = 'User' "
            "WHERE __cg_edge_0.type = 'KNOWS' UNION ALL SELECT JSON_EXTRACT(b.properties, '$.name') AS \"friend\", JSON_EXTRACT(b.properties, '$.score') AS \"__cg_aggregate_1\" "
            "FROM edges AS __cg_edge_0 JOIN nodes AS a ON a.id = __cg_edge_0.from_id JOIN nodes AS __cg_variable_hop_2_node_1 ON __cg_variable_hop_2_node_1.id = __cg_edge_0.to_id "
            "JOIN edges AS __cg_edge_1 ON __cg_variable_hop_2_node_1.id = __cg_edge_1.from_id JOIN nodes AS b ON b.id = __cg_edge_1.to_id "
            "JOIN node_labels AS a_label_0 ON a_label_0.node_id = a.id AND a_label_0.label = 'User' "
            "JOIN node_labels AS b_label_2 ON b_label_2.node_id = b.id AND b_label_2.label = 'User' "
            "WHERE __cg_edge_0.type = 'KNOWS' AND __cg_edge_1.type = 'KNOWS') AS variable_length_q "
            "GROUP BY variable_length_q.\"friend\" ORDER BY \"top\" DESC",
        )

    def test_compile_match_node_with_entity_alias_order(self) -> None:
        expression = cypherglot.compile_cypher_text(
            "MATCH (u:User) RETURN u AS user ORDER BY user"
        )

        self.assertEqual(
            expression.sql(),
            "SELECT JSON_OBJECT('id': u.id, 'label': (SELECT u_label_return.label FROM node_labels AS u_label_return WHERE u_label_return.node_id = u.id LIMIT 1), 'properties': JSON(COALESCE(u.properties, '{}'))) AS \"user\" FROM nodes AS u JOIN node_labels AS u_label_0 ON u_label_0.node_id = u.id AND u_label_0.label = 'User' ORDER BY JSON_OBJECT('id': u.id, 'label': (SELECT u_label_return.label FROM node_labels AS u_label_return WHERE u_label_return.node_id = u.id LIMIT 1), 'properties': JSON(COALESCE(u.properties, '{}'))) ASC",
        )

    def test_compile_match_with_grouped_count_return(self) -> None:
        expression = cypherglot.compile_cypher_text(
            "MATCH (u:User) WITH u AS person, u.name AS name RETURN name, count(person) AS total ORDER BY total DESC"
        )
        star_expression = cypherglot.compile_cypher_text(
            "MATCH (u:User) WITH u.name AS name RETURN name, count(*) AS total ORDER BY total DESC"
        )
        relationship_expression = cypherglot.compile_cypher_text(
            "MATCH (a:User)-[r:KNOWS]->(b:User) WITH r AS rel RETURN rel AS edge, count(rel) AS total ORDER BY total DESC"
        )
        relationship_star_expression = cypherglot.compile_cypher_text(
            "MATCH (a:User)-[r:KNOWS]->(b:User) WITH r AS rel RETURN rel AS edge, count(*) AS total ORDER BY total DESC"
        )

        self.assertEqual(
            expression.sql(),
            "SELECT with_q.\"__cg_with_scalar_name\" AS \"name\", COUNT(with_q.\"__cg_with_person_id\") AS \"total\" "
            "FROM (SELECT u.id AS \"__cg_with_person_id\", u.properties AS \"__cg_with_person_properties\", "
            "JSON_EXTRACT(u.properties, '$.name') AS \"__cg_with_scalar_name\" "
            "FROM nodes AS u JOIN node_labels AS u_label_0 ON u_label_0.node_id = u.id "
            "AND u_label_0.label = 'User') AS with_q "
            "GROUP BY with_q.\"__cg_with_scalar_name\" ORDER BY \"total\" DESC",
        )
        self.assertEqual(
            star_expression.sql(),
            "SELECT with_q.\"__cg_with_scalar_name\" AS \"name\", COUNT(*) AS \"total\" "
            "FROM (SELECT JSON_EXTRACT(u.properties, '$.name') AS \"__cg_with_scalar_name\" "
            "FROM nodes AS u JOIN node_labels AS u_label_0 ON u_label_0.node_id = u.id "
            "AND u_label_0.label = 'User') AS with_q "
            "GROUP BY with_q.\"__cg_with_scalar_name\" ORDER BY \"total\" DESC",
        )
        self.assertEqual(
            relationship_expression.sql(),
            "SELECT JSON_OBJECT('id': with_q.\"__cg_with_rel_id\", 'type': with_q.\"__cg_with_rel_type\", 'properties': JSON(COALESCE(with_q.\"__cg_with_rel_properties\", '{}'))) AS \"edge\", COUNT(with_q.\"__cg_with_rel_id\") AS \"total\" "
            "FROM (SELECT r.id AS \"__cg_with_rel_id\", r.type AS \"__cg_with_rel_type\", r.properties AS \"__cg_with_rel_properties\" "
            "FROM edges AS r JOIN nodes AS a ON a.id = r.from_id JOIN nodes AS b ON b.id = r.to_id "
            "JOIN node_labels AS a_label_0 ON a_label_0.node_id = a.id AND a_label_0.label = 'User' "
            "JOIN node_labels AS b_label_1 ON b_label_1.node_id = b.id AND b_label_1.label = 'User' "
            "WHERE r.type = 'KNOWS') AS with_q "
            "GROUP BY JSON_OBJECT('id': with_q.\"__cg_with_rel_id\", 'type': with_q.\"__cg_with_rel_type\", 'properties': JSON(COALESCE(with_q.\"__cg_with_rel_properties\", '{}'))) ORDER BY \"total\" DESC",
        )
        self.assertEqual(
            relationship_star_expression.sql(),
            "SELECT JSON_OBJECT('id': with_q.\"__cg_with_rel_id\", 'type': with_q.\"__cg_with_rel_type\", 'properties': JSON(COALESCE(with_q.\"__cg_with_rel_properties\", '{}'))) AS \"edge\", COUNT(*) AS \"total\" "
            "FROM (SELECT r.id AS \"__cg_with_rel_id\", r.type AS \"__cg_with_rel_type\", r.properties AS \"__cg_with_rel_properties\" "
            "FROM edges AS r JOIN nodes AS a ON a.id = r.from_id JOIN nodes AS b ON b.id = r.to_id "
            "JOIN node_labels AS a_label_0 ON a_label_0.node_id = a.id AND a_label_0.label = 'User' "
            "JOIN node_labels AS b_label_1 ON b_label_1.node_id = b.id AND b_label_1.label = 'User' "
            "WHERE r.type = 'KNOWS') AS with_q "
            "GROUP BY JSON_OBJECT('id': with_q.\"__cg_with_rel_id\", 'type': with_q.\"__cg_with_rel_type\", 'properties': JSON(COALESCE(with_q.\"__cg_with_rel_properties\", '{}'))) ORDER BY \"total\" DESC",
        )

    def test_compile_match_with_grouped_numeric_aggregates_return(self) -> None:
        expression = cypherglot.compile_cypher_text(
            "MATCH (u:User) WITH u.name AS name, u.score AS score RETURN name, max(score) AS top ORDER BY top DESC"
        )

        self.assertEqual(
            expression.sql(),
            "SELECT with_q.\"__cg_with_scalar_name\" AS \"name\", MAX(with_q.\"__cg_with_scalar_score\") AS \"top\" "
            "FROM (SELECT JSON_EXTRACT(u.properties, '$.name') AS \"__cg_with_scalar_name\", "
            "JSON_EXTRACT(u.properties, '$.score') AS \"__cg_with_scalar_score\" "
            "FROM nodes AS u JOIN node_labels AS u_label_0 ON u_label_0.node_id = u.id "
            "AND u_label_0.label = 'User') AS with_q "
            "GROUP BY with_q.\"__cg_with_scalar_name\" ORDER BY \"top\" DESC",
        )

    def test_compile_match_plain_read_no_alias_aggregates(self) -> None:
        count_expression = cypherglot.compile_cypher_text(
            "MATCH (u:User) RETURN count(*)"
        )
        sum_expression = cypherglot.compile_cypher_text(
            "MATCH (u:User) RETURN sum(u.score)"
        )

        self.assertEqual(
            count_expression.sql(),
            "SELECT COUNT(*) AS \"count(*)\" FROM nodes AS u JOIN node_labels AS u_label_0 ON u_label_0.node_id = u.id AND u_label_0.label = 'User'",
        )
        self.assertEqual(
            sum_expression.sql(),
            "SELECT SUM(JSON_EXTRACT(u.properties, '$.score')) AS \"sum(u.score)\" FROM nodes AS u JOIN node_labels AS u_label_0 ON u_label_0.node_id = u.id AND u_label_0.label = 'User'",
        )

    def test_compile_match_with_searched_case_return(self) -> None:
        expression = cypherglot.compile_cypher_text(
            "MATCH (u:User) WITH u.age AS age, u.name AS name RETURN CASE WHEN age >= 18 THEN name ELSE 'minor' END AS label ORDER BY label"
        )

        self.assertEqual(
            expression.sql(),
            "SELECT CASE WHEN with_q.\"__cg_with_scalar_age\" >= 18 THEN with_q.\"__cg_with_scalar_name\" ELSE 'minor' END AS \"label\" "
            "FROM (SELECT JSON_EXTRACT(u.properties, '$.age') AS \"__cg_with_scalar_age\", "
            "JSON_EXTRACT(u.properties, '$.name') AS \"__cg_with_scalar_name\" "
            "FROM nodes AS u JOIN node_labels AS u_label_0 ON u_label_0.node_id = u.id "
            "AND u_label_0.label = 'User') AS with_q "
            "ORDER BY CASE WHEN with_q.\"__cg_with_scalar_age\" >= 18 THEN with_q.\"__cg_with_scalar_name\" ELSE 'minor' END ASC",
        )

    def test_compile_match_with_properties_return(self) -> None:
        expression = cypherglot.compile_cypher_text(
            "MATCH (a:User)-[r:KNOWS]->(b:User) WITH a AS person, r AS rel RETURN properties(person) AS user_props, properties(rel) AS rel_props ORDER BY user_props, rel_props"
        )
        no_alias_expression = cypherglot.compile_cypher_text(
            "MATCH (u:User) RETURN properties(u)"
        )

        self.assertEqual(
            expression.sql(),
            "SELECT COALESCE(with_q.\"__cg_with_person_properties\", '{}') AS \"user_props\", COALESCE(with_q.\"__cg_with_rel_properties\", '{}') AS \"rel_props\" "
            "FROM (SELECT a.id AS \"__cg_with_person_id\", a.properties AS \"__cg_with_person_properties\", r.id AS \"__cg_with_rel_id\", r.type AS \"__cg_with_rel_type\", r.properties AS \"__cg_with_rel_properties\" "
            "FROM edges AS r JOIN nodes AS a ON a.id = r.from_id JOIN nodes AS b ON b.id = r.to_id "
            "JOIN node_labels AS a_label_0 ON a_label_0.node_id = a.id AND a_label_0.label = 'User' "
            "JOIN node_labels AS b_label_1 ON b_label_1.node_id = b.id AND b_label_1.label = 'User' "
            "WHERE r.type = 'KNOWS') AS with_q ORDER BY COALESCE(with_q.\"__cg_with_person_properties\", '{}') ASC, COALESCE(with_q.\"__cg_with_rel_properties\", '{}') ASC",
        )
        self.assertEqual(
            no_alias_expression.sql(),
            "SELECT COALESCE(u.properties, '{}') AS \"properties(u)\" FROM nodes AS u JOIN node_labels AS u_label_0 ON u_label_0.node_id = u.id AND u_label_0.label = 'User'",
        )

    def test_compile_match_with_labels_return(self) -> None:
        expression = cypherglot.compile_cypher_text(
            "MATCH (a:User)-[r:KNOWS]->(b:User) WITH a AS person RETURN labels(person) AS user_labels ORDER BY user_labels"
        )

        self.assertEqual(
            expression.sql(),
            "SELECT COALESCE((SELECT JSON_GROUP_ARRAY(person_label_values.label) FROM node_labels AS person_label_values WHERE person_label_values.node_id = with_q.\"__cg_with_person_id\"), '[]') AS \"user_labels\" "
            "FROM (SELECT a.id AS \"__cg_with_person_id\", a.properties AS \"__cg_with_person_properties\" "
            "FROM edges AS r JOIN nodes AS a ON a.id = r.from_id JOIN nodes AS b ON b.id = r.to_id "
            "JOIN node_labels AS a_label_0 ON a_label_0.node_id = a.id AND a_label_0.label = 'User' "
            "JOIN node_labels AS b_label_1 ON b_label_1.node_id = b.id AND b_label_1.label = 'User' "
            "WHERE r.type = 'KNOWS') AS with_q ORDER BY COALESCE((SELECT JSON_GROUP_ARRAY(person_label_values.label) FROM node_labels AS person_label_values WHERE person_label_values.node_id = with_q.\"__cg_with_person_id\"), '[]') ASC",
        )

    def test_compile_match_with_keys_return(self) -> None:
        expression = cypherglot.compile_cypher_text(
            "MATCH (a:User)-[r:KNOWS]->(b:User) WITH a AS person, r AS rel RETURN keys(person) AS user_keys, keys(rel) AS rel_keys ORDER BY user_keys, rel_keys"
        )

        self.assertEqual(
            expression.sql(),
            "SELECT COALESCE((SELECT JSON_GROUP_ARRAY(person_property_keys.key) FROM JSON_EACH(COALESCE(with_q.\"__cg_with_person_properties\", '{}')) AS person_property_keys), '[]') AS \"user_keys\", COALESCE((SELECT JSON_GROUP_ARRAY(rel_property_keys.key) FROM JSON_EACH(COALESCE(with_q.\"__cg_with_rel_properties\", '{}')) AS rel_property_keys), '[]') AS \"rel_keys\" "
            "FROM (SELECT a.id AS \"__cg_with_person_id\", a.properties AS \"__cg_with_person_properties\", r.id AS \"__cg_with_rel_id\", r.type AS \"__cg_with_rel_type\", r.properties AS \"__cg_with_rel_properties\" "
            "FROM edges AS r JOIN nodes AS a ON a.id = r.from_id JOIN nodes AS b ON b.id = r.to_id "
            "JOIN node_labels AS a_label_0 ON a_label_0.node_id = a.id AND a_label_0.label = 'User' "
            "JOIN node_labels AS b_label_1 ON b_label_1.node_id = b.id AND b_label_1.label = 'User' "
            "WHERE r.type = 'KNOWS') AS with_q ORDER BY COALESCE((SELECT JSON_GROUP_ARRAY(person_property_keys.key) FROM JSON_EACH(COALESCE(with_q.\"__cg_with_person_properties\", '{}')) AS person_property_keys), '[]') ASC, COALESCE((SELECT JSON_GROUP_ARRAY(rel_property_keys.key) FROM JSON_EACH(COALESCE(with_q.\"__cg_with_rel_properties\", '{}')) AS rel_property_keys), '[]') ASC",
        )

    def test_compile_match_with_start_and_end_node_return(self) -> None:
        expression = cypherglot.compile_cypher_text(
            "MATCH (a:User)-[r:KNOWS]->(b:User) WITH r AS rel RETURN startNode(rel) AS start, endNode(rel) AS ending ORDER BY start, ending"
        )
        field_expression = cypherglot.compile_cypher_text(
            "MATCH (a:User)-[r:KNOWS]->(b:User) WITH r AS rel RETURN startNode(rel).id AS start_id, endNode(rel).id AS end_id ORDER BY start_id, end_id"
        )
        no_alias_expression = cypherglot.compile_cypher_text(
            "MATCH (a:User)-[r:KNOWS]->(b:User) WITH a AS person, r AS rel RETURN id(person), type(rel), startNode(rel).id"
        )

        self.assertEqual(
            expression.sql(),
            "SELECT JSON_OBJECT('id': (SELECT rel_start_edge.from_id FROM edges AS rel_start_edge WHERE rel_start_edge.id = with_q.\"__cg_with_rel_id\"), 'label': (SELECT rel_start_label_return.label FROM node_labels AS rel_start_label_return WHERE rel_start_label_return.node_id = (SELECT rel_start_edge.from_id FROM edges AS rel_start_edge WHERE rel_start_edge.id = with_q.\"__cg_with_rel_id\") LIMIT 1), 'properties': JSON(COALESCE((SELECT rel_start_node.properties FROM nodes AS rel_start_node WHERE rel_start_node.id = (SELECT rel_start_edge.from_id FROM edges AS rel_start_edge WHERE rel_start_edge.id = with_q.\"__cg_with_rel_id\")), '{}'))) AS \"start\", JSON_OBJECT('id': (SELECT rel_end_edge.to_id FROM edges AS rel_end_edge WHERE rel_end_edge.id = with_q.\"__cg_with_rel_id\"), 'label': (SELECT rel_end_label_return.label FROM node_labels AS rel_end_label_return WHERE rel_end_label_return.node_id = (SELECT rel_end_edge.to_id FROM edges AS rel_end_edge WHERE rel_end_edge.id = with_q.\"__cg_with_rel_id\") LIMIT 1), 'properties': JSON(COALESCE((SELECT rel_end_node.properties FROM nodes AS rel_end_node WHERE rel_end_node.id = (SELECT rel_end_edge.to_id FROM edges AS rel_end_edge WHERE rel_end_edge.id = with_q.\"__cg_with_rel_id\")), '{}'))) AS \"ending\" "
            "FROM (SELECT r.id AS \"__cg_with_rel_id\", r.type AS \"__cg_with_rel_type\", r.properties AS \"__cg_with_rel_properties\" "
            "FROM edges AS r JOIN nodes AS a ON a.id = r.from_id JOIN nodes AS b ON b.id = r.to_id "
            "JOIN node_labels AS a_label_0 ON a_label_0.node_id = a.id AND a_label_0.label = 'User' "
            "JOIN node_labels AS b_label_1 ON b_label_1.node_id = b.id AND b_label_1.label = 'User' "
            "WHERE r.type = 'KNOWS') AS with_q ORDER BY JSON_OBJECT('id': (SELECT rel_start_edge.from_id FROM edges AS rel_start_edge WHERE rel_start_edge.id = with_q.\"__cg_with_rel_id\"), 'label': (SELECT rel_start_label_return.label FROM node_labels AS rel_start_label_return WHERE rel_start_label_return.node_id = (SELECT rel_start_edge.from_id FROM edges AS rel_start_edge WHERE rel_start_edge.id = with_q.\"__cg_with_rel_id\") LIMIT 1), 'properties': JSON(COALESCE((SELECT rel_start_node.properties FROM nodes AS rel_start_node WHERE rel_start_node.id = (SELECT rel_start_edge.from_id FROM edges AS rel_start_edge WHERE rel_start_edge.id = with_q.\"__cg_with_rel_id\")), '{}'))) ASC, JSON_OBJECT('id': (SELECT rel_end_edge.to_id FROM edges AS rel_end_edge WHERE rel_end_edge.id = with_q.\"__cg_with_rel_id\"), 'label': (SELECT rel_end_label_return.label FROM node_labels AS rel_end_label_return WHERE rel_end_label_return.node_id = (SELECT rel_end_edge.to_id FROM edges AS rel_end_edge WHERE rel_end_edge.id = with_q.\"__cg_with_rel_id\") LIMIT 1), 'properties': JSON(COALESCE((SELECT rel_end_node.properties FROM nodes AS rel_end_node WHERE rel_end_node.id = (SELECT rel_end_edge.to_id FROM edges AS rel_end_edge WHERE rel_end_edge.id = with_q.\"__cg_with_rel_id\")), '{}'))) ASC",
        )
        self.assertEqual(
            field_expression.sql(),
            "SELECT (SELECT rel_start_edge.from_id FROM edges AS rel_start_edge WHERE rel_start_edge.id = with_q.\"__cg_with_rel_id\") AS \"start_id\", (SELECT rel_end_edge.to_id FROM edges AS rel_end_edge WHERE rel_end_edge.id = with_q.\"__cg_with_rel_id\") AS \"end_id\" "
            "FROM (SELECT r.id AS \"__cg_with_rel_id\", r.type AS \"__cg_with_rel_type\", r.properties AS \"__cg_with_rel_properties\" "
            "FROM edges AS r JOIN nodes AS a ON a.id = r.from_id JOIN nodes AS b ON b.id = r.to_id "
            "JOIN node_labels AS a_label_0 ON a_label_0.node_id = a.id AND a_label_0.label = 'User' "
            "JOIN node_labels AS b_label_1 ON b_label_1.node_id = b.id AND b_label_1.label = 'User' "
            "WHERE r.type = 'KNOWS') AS with_q ORDER BY (SELECT rel_start_edge.from_id FROM edges AS rel_start_edge WHERE rel_start_edge.id = with_q.\"__cg_with_rel_id\") ASC, (SELECT rel_end_edge.to_id FROM edges AS rel_end_edge WHERE rel_end_edge.id = with_q.\"__cg_with_rel_id\") ASC",
        )
        self.assertEqual(
            no_alias_expression.sql(),
            "SELECT with_q.\"__cg_with_person_id\" AS \"id(person)\", with_q.\"__cg_with_rel_type\" AS \"type(rel)\", (SELECT rel_start_edge.from_id FROM edges AS rel_start_edge WHERE rel_start_edge.id = with_q.\"__cg_with_rel_id\") AS \"startNode(rel).id\" "
            "FROM (SELECT a.id AS \"__cg_with_person_id\", a.properties AS \"__cg_with_person_properties\", r.id AS \"__cg_with_rel_id\", r.type AS \"__cg_with_rel_type\", r.properties AS \"__cg_with_rel_properties\" "
            "FROM edges AS r JOIN nodes AS a ON a.id = r.from_id JOIN nodes AS b ON b.id = r.to_id "
            "JOIN node_labels AS a_label_0 ON a_label_0.node_id = a.id AND a_label_0.label = 'User' "
            "JOIN node_labels AS b_label_1 ON b_label_1.node_id = b.id AND b_label_1.label = 'User' "
            "WHERE r.type = 'KNOWS') AS with_q",
        )

    def test_compile_match_with_where(self) -> None:
        expression = cypherglot.compile_cypher_text(
            "MATCH (u:User) WITH u AS person, u.name AS name WHERE name = 'Alice' AND person.id > 1 RETURN person, name ORDER BY name"
        )

        self.assertEqual(
            expression.sql(),
            "SELECT JSON_OBJECT('id': with_q.\"__cg_with_person_id\", 'label': "
            "(SELECT person_label_return.label FROM node_labels AS person_label_return "
            "WHERE person_label_return.node_id = with_q.\"__cg_with_person_id\" LIMIT 1), "
            "'properties': JSON(COALESCE(with_q.\"__cg_with_person_properties\", '{}'))) AS \"person\", "
            "with_q.\"__cg_with_scalar_name\" AS \"name\" "
            "FROM (SELECT u.id AS \"__cg_with_person_id\", u.properties AS \"__cg_with_person_properties\", "
            "JSON_EXTRACT(u.properties, '$.name') AS \"__cg_with_scalar_name\" "
            "FROM nodes AS u JOIN node_labels AS u_label_0 ON u_label_0.node_id = u.id "
            "AND u_label_0.label = 'User') AS with_q "
            "WHERE with_q.\"__cg_with_scalar_name\" = 'Alice' AND with_q.\"__cg_with_person_id\" > 1 "
            "ORDER BY with_q.\"__cg_with_scalar_name\" ASC",
        )

    def test_compile_match_with_where_id_and_type_filters(self) -> None:
        id_expression = cypherglot.compile_cypher_text(
            "MATCH (u:User) WITH u AS person, u.name AS name WHERE id(person) >= 1 RETURN person, name ORDER BY name"
        )
        type_expression = cypherglot.compile_cypher_text(
            "MATCH (a:User)-[r:KNOWS]->(b:User) WITH a AS person, r AS rel WHERE type(rel) = 'KNOWS' RETURN person, rel"
        )

        self.assertEqual(
            id_expression.sql(),
            "SELECT JSON_OBJECT('id': with_q.\"__cg_with_person_id\", 'label': (SELECT person_label_return.label FROM node_labels AS person_label_return WHERE person_label_return.node_id = with_q.\"__cg_with_person_id\" LIMIT 1), 'properties': JSON(COALESCE(with_q.\"__cg_with_person_properties\", '{}'))) AS \"person\", with_q.\"__cg_with_scalar_name\" AS \"name\" FROM (SELECT u.id AS \"__cg_with_person_id\", u.properties AS \"__cg_with_person_properties\", JSON_EXTRACT(u.properties, '$.name') AS \"__cg_with_scalar_name\" FROM nodes AS u JOIN node_labels AS u_label_0 ON u_label_0.node_id = u.id AND u_label_0.label = 'User') AS with_q WHERE with_q.\"__cg_with_person_id\" >= 1 ORDER BY with_q.\"__cg_with_scalar_name\" ASC",
        )
        self.assertEqual(
            type_expression.sql(),
            "SELECT JSON_OBJECT('id': with_q.\"__cg_with_person_id\", 'label': (SELECT person_label_return.label FROM node_labels AS person_label_return WHERE person_label_return.node_id = with_q.\"__cg_with_person_id\" LIMIT 1), 'properties': JSON(COALESCE(with_q.\"__cg_with_person_properties\", '{}'))) AS \"person\", JSON_OBJECT('id': with_q.\"__cg_with_rel_id\", 'type': with_q.\"__cg_with_rel_type\", 'properties': JSON(COALESCE(with_q.\"__cg_with_rel_properties\", '{}'))) AS \"rel\" FROM (SELECT a.id AS \"__cg_with_person_id\", a.properties AS \"__cg_with_person_properties\", r.id AS \"__cg_with_rel_id\", r.type AS \"__cg_with_rel_type\", r.properties AS \"__cg_with_rel_properties\" FROM edges AS r JOIN nodes AS a ON a.id = r.from_id JOIN nodes AS b ON b.id = r.to_id JOIN node_labels AS a_label_0 ON a_label_0.node_id = a.id AND a_label_0.label = 'User' JOIN node_labels AS b_label_1 ON b_label_1.node_id = b.id AND b_label_1.label = 'User' WHERE r.type = 'KNOWS') AS with_q WHERE with_q.\"__cg_with_rel_type\" = 'KNOWS'",
        )

    def test_compile_match_with_where_null_filters(self) -> None:
        expression = cypherglot.compile_cypher_text(
            "MATCH (a:User)-[r:KNOWS]->(b:User) WITH a AS person, r AS rel, a.name AS name WHERE name IS NOT NULL AND person.name IS NULL AND rel.note IS NOT NULL RETURN person, rel"
        )

        self.assertEqual(
            expression.sql(),
            "SELECT JSON_OBJECT('id': with_q.\"__cg_with_person_id\", 'label': (SELECT person_label_return.label FROM node_labels AS person_label_return WHERE person_label_return.node_id = with_q.\"__cg_with_person_id\" LIMIT 1), 'properties': JSON(COALESCE(with_q.\"__cg_with_person_properties\", '{}'))) AS \"person\", JSON_OBJECT('id': with_q.\"__cg_with_rel_id\", 'type': with_q.\"__cg_with_rel_type\", 'properties': JSON(COALESCE(with_q.\"__cg_with_rel_properties\", '{}'))) AS \"rel\" FROM (SELECT a.id AS \"__cg_with_person_id\", a.properties AS \"__cg_with_person_properties\", r.id AS \"__cg_with_rel_id\", r.type AS \"__cg_with_rel_type\", r.properties AS \"__cg_with_rel_properties\", JSON_EXTRACT(a.properties, '$.name') AS \"__cg_with_scalar_name\" FROM edges AS r JOIN nodes AS a ON a.id = r.from_id JOIN nodes AS b ON b.id = r.to_id JOIN node_labels AS a_label_0 ON a_label_0.node_id = a.id AND a_label_0.label = 'User' JOIN node_labels AS b_label_1 ON b_label_1.node_id = b.id AND b_label_1.label = 'User' WHERE r.type = 'KNOWS') AS with_q WHERE NOT with_q.\"__cg_with_scalar_name\" IS NULL AND (JSON_TYPE(with_q.\"__cg_with_person_properties\", '$.name') IS NULL OR JSON_TYPE(with_q.\"__cg_with_person_properties\", '$.name') = 'null') AND (NOT JSON_TYPE(with_q.\"__cg_with_rel_properties\", '$.note') IS NULL AND JSON_TYPE(with_q.\"__cg_with_rel_properties\", '$.note') <> 'null')",
        )

    def test_compile_match_with_where_string_and_null_filters(self) -> None:
        expression = cypherglot.compile_cypher_text(
            "MATCH (a:User)-[r:KNOWS]->(b:User) WITH a AS person, r AS rel, a.name AS name, r.note AS note WHERE person.name STARTS WITH 'Al' AND name CONTAINS 'li' AND rel.note ENDS WITH 'ce' AND note IS NOT NULL RETURN person, rel"
        )

        self.assertEqual(
            expression.sql(),
            "SELECT JSON_OBJECT('id': with_q.\"__cg_with_person_id\", 'label': (SELECT person_label_return.label FROM node_labels AS person_label_return WHERE person_label_return.node_id = with_q.\"__cg_with_person_id\" LIMIT 1), 'properties': JSON(COALESCE(with_q.\"__cg_with_person_properties\", '{}'))) AS \"person\", JSON_OBJECT('id': with_q.\"__cg_with_rel_id\", 'type': with_q.\"__cg_with_rel_type\", 'properties': JSON(COALESCE(with_q.\"__cg_with_rel_properties\", '{}'))) AS \"rel\" FROM (SELECT a.id AS \"__cg_with_person_id\", a.properties AS \"__cg_with_person_properties\", r.id AS \"__cg_with_rel_id\", r.type AS \"__cg_with_rel_type\", r.properties AS \"__cg_with_rel_properties\", JSON_EXTRACT(a.properties, '$.name') AS \"__cg_with_scalar_name\", JSON_EXTRACT(r.properties, '$.note') AS \"__cg_with_scalar_note\" FROM edges AS r JOIN nodes AS a ON a.id = r.from_id JOIN nodes AS b ON b.id = r.to_id JOIN node_labels AS a_label_0 ON a_label_0.node_id = a.id AND a_label_0.label = 'User' JOIN node_labels AS b_label_1 ON b_label_1.node_id = b.id AND b_label_1.label = 'User' WHERE r.type = 'KNOWS') AS with_q WHERE SUBSTRING(JSON_EXTRACT(with_q.\"__cg_with_person_properties\", '$.name'), 1, LENGTH('Al')) = 'Al' AND STR_POSITION(with_q.\"__cg_with_scalar_name\", 'li') > 0 AND LENGTH(JSON_EXTRACT(with_q.\"__cg_with_rel_properties\", '$.note')) >= LENGTH('ce') AND SUBSTRING(JSON_EXTRACT(with_q.\"__cg_with_rel_properties\", '$.note'), LENGTH(JSON_EXTRACT(with_q.\"__cg_with_rel_properties\", '$.note')) - LENGTH('ce') + 1) = 'ce' AND NOT with_q.\"__cg_with_scalar_note\" IS NULL",
        )

    def test_compile_match_with_where_size_filters(self) -> None:
        expression = cypherglot.compile_cypher_text(
            "MATCH (a:User)-[r:KNOWS]->(b:User) WITH a AS person, r AS rel, a.name AS name, r.note AS note WHERE size(person.name) >= 3 AND size(name) >= 3 AND size(rel.note) IS NOT NULL AND size(note) IS NOT NULL RETURN person, rel"
        )

        self.assertEqual(
            expression.sql(),
            "SELECT JSON_OBJECT('id': with_q.\"__cg_with_person_id\", 'label': (SELECT person_label_return.label FROM node_labels AS person_label_return WHERE person_label_return.node_id = with_q.\"__cg_with_person_id\" LIMIT 1), 'properties': JSON(COALESCE(with_q.\"__cg_with_person_properties\", '{}'))) AS \"person\", JSON_OBJECT('id': with_q.\"__cg_with_rel_id\", 'type': with_q.\"__cg_with_rel_type\", 'properties': JSON(COALESCE(with_q.\"__cg_with_rel_properties\", '{}'))) AS \"rel\" FROM (SELECT a.id AS \"__cg_with_person_id\", a.properties AS \"__cg_with_person_properties\", r.id AS \"__cg_with_rel_id\", r.type AS \"__cg_with_rel_type\", r.properties AS \"__cg_with_rel_properties\", JSON_EXTRACT(a.properties, '$.name') AS \"__cg_with_scalar_name\", JSON_EXTRACT(r.properties, '$.note') AS \"__cg_with_scalar_note\" FROM edges AS r JOIN nodes AS a ON a.id = r.from_id JOIN nodes AS b ON b.id = r.to_id JOIN node_labels AS a_label_0 ON a_label_0.node_id = a.id AND a_label_0.label = 'User' JOIN node_labels AS b_label_1 ON b_label_1.node_id = b.id AND b_label_1.label = 'User' WHERE r.type = 'KNOWS') AS with_q WHERE LENGTH(JSON_EXTRACT(with_q.\"__cg_with_person_properties\", '$.name')) >= 3 AND LENGTH(with_q.\"__cg_with_scalar_name\") >= 3 AND NOT LENGTH(JSON_EXTRACT(with_q.\"__cg_with_rel_properties\", '$.note')) IS NULL AND NOT LENGTH(with_q.\"__cg_with_scalar_note\") IS NULL",
        )

    def test_compile_match_with_aliased_returns(self) -> None:
        expression = cypherglot.compile_cypher_text(
            "MATCH (u:User) WITH u AS person, u.name AS name RETURN person AS user, person.name AS display_name, name AS raw_name ORDER BY display_name, raw_name"
        )

        self.assertEqual(
            expression.sql(),
            "SELECT JSON_OBJECT('id': with_q.\"__cg_with_person_id\", 'label': "
            "(SELECT person_label_return.label FROM node_labels AS person_label_return "
            "WHERE person_label_return.node_id = with_q.\"__cg_with_person_id\" LIMIT 1), "
            "'properties': JSON(COALESCE(with_q.\"__cg_with_person_properties\", '{}'))) AS \"user\", "
            "JSON_EXTRACT(with_q.\"__cg_with_person_properties\", '$.name') AS \"display_name\", "
            "with_q.\"__cg_with_scalar_name\" AS \"raw_name\" "
            "FROM (SELECT u.id AS \"__cg_with_person_id\", u.properties AS \"__cg_with_person_properties\", "
            "JSON_EXTRACT(u.properties, '$.name') AS \"__cg_with_scalar_name\" "
            "FROM nodes AS u JOIN node_labels AS u_label_0 ON u_label_0.node_id = u.id "
            "AND u_label_0.label = 'User') AS with_q "
            "ORDER BY JSON_EXTRACT(with_q.\"__cg_with_person_properties\", '$.name') ASC, with_q.\"__cg_with_scalar_name\" ASC",
        )

    def test_compile_match_with_id_and_type_returns(self) -> None:
        expression = cypherglot.compile_cypher_text(
            "MATCH (a:User)-[r:KNOWS]->(b:User) WITH a AS user, r AS rel RETURN id(user) AS uid, type(rel) AS rel_type ORDER BY uid, rel_type"
        )

        self.assertEqual(
            expression.sql(),
            "SELECT with_q.\"__cg_with_user_id\" AS \"uid\", with_q.\"__cg_with_rel_type\" AS \"rel_type\" "
            "FROM (SELECT a.id AS \"__cg_with_user_id\", a.properties AS \"__cg_with_user_properties\", "
            "r.id AS \"__cg_with_rel_id\", r.type AS \"__cg_with_rel_type\", r.properties AS \"__cg_with_rel_properties\" "
            "FROM edges AS r JOIN nodes AS a ON a.id = r.from_id JOIN nodes AS b ON b.id = r.to_id "
            "JOIN node_labels AS a_label_0 ON a_label_0.node_id = a.id AND a_label_0.label = 'User' "
            "JOIN node_labels AS b_label_1 ON b_label_1.node_id = b.id AND b_label_1.label = 'User' "
            "WHERE r.type = 'KNOWS') AS with_q ORDER BY with_q.\"__cg_with_user_id\" ASC, with_q.\"__cg_with_rel_type\" ASC",
        )

    def test_compile_match_with_size_returns(self) -> None:
        expression = cypherglot.compile_cypher_text(
            "MATCH (a:User)-[r:KNOWS]->(b:User) WITH a AS person, r AS rel, a.name AS name RETURN size(person.name) AS name_len, size(name) AS rebound_len, size(id(person)) AS person_id_len, size(type(rel)) AS rel_type_len ORDER BY name_len, rebound_len, person_id_len, rel_type_len"
        )

        self.assertEqual(
            expression.sql(),
            "SELECT LENGTH(JSON_EXTRACT(with_q.\"__cg_with_person_properties\", '$.name')) AS \"name_len\", LENGTH(with_q.\"__cg_with_scalar_name\") AS \"rebound_len\", LENGTH(with_q.\"__cg_with_person_id\") AS \"person_id_len\", LENGTH(with_q.\"__cg_with_rel_type\") AS \"rel_type_len\" "
            "FROM (SELECT a.id AS \"__cg_with_person_id\", a.properties AS \"__cg_with_person_properties\", "
            "r.id AS \"__cg_with_rel_id\", r.type AS \"__cg_with_rel_type\", r.properties AS \"__cg_with_rel_properties\", "
            "JSON_EXTRACT(a.properties, '$.name') AS \"__cg_with_scalar_name\" "
            "FROM edges AS r JOIN nodes AS a ON a.id = r.from_id JOIN nodes AS b ON b.id = r.to_id "
            "JOIN node_labels AS a_label_0 ON a_label_0.node_id = a.id AND a_label_0.label = 'User' "
            "JOIN node_labels AS b_label_1 ON b_label_1.node_id = b.id AND b_label_1.label = 'User' "
            "WHERE r.type = 'KNOWS') AS with_q "
            "ORDER BY LENGTH(JSON_EXTRACT(with_q.\"__cg_with_person_properties\", '$.name')) ASC, LENGTH(with_q.\"__cg_with_scalar_name\") ASC, LENGTH(with_q.\"__cg_with_person_id\") ASC, LENGTH(with_q.\"__cg_with_rel_type\") ASC",
        )

    def test_compile_match_with_size_literal_and_parameter_returns(self) -> None:
        expression = cypherglot.compile_cypher_text(
            "MATCH (u:User) WITH u AS person RETURN size('tag') AS tag_len, size($value) AS value_len ORDER BY tag_len, value_len"
        )

        self.assertEqual(
            expression.sql(),
            "SELECT LENGTH('tag') AS \"tag_len\", LENGTH(:value) AS \"value_len\" "
            "FROM (SELECT u.id AS \"__cg_with_person_id\", u.properties AS \"__cg_with_person_properties\" "
            "FROM nodes AS u JOIN node_labels AS u_label_0 ON u_label_0.node_id = u.id "
            "AND u_label_0.label = 'User') AS with_q "
            "ORDER BY LENGTH('tag') ASC, LENGTH(:value) ASC",
        )

    def test_compile_match_with_lower_and_upper_returns(self) -> None:
        expression = cypherglot.compile_cypher_text(
            "MATCH (u:User) WITH u AS person, u.name AS name RETURN lower(person.name) AS lower_name, upper(name) AS upper_name, lower('tag') AS lower_tag, upper($value) AS upper_value, trim(name) AS trimmed, ltrim(' tag') AS left_trimmed, rtrim('tag ') AS right_trimmed ORDER BY lower_name, upper_name, lower_tag, upper_value, trimmed, left_trimmed, right_trimmed"
        )
        reverse_expression = cypherglot.compile_cypher_text(
            "MATCH (u:User) WITH u AS person, u.name AS name RETURN reverse(person.name) AS reversed_name, reverse(name) AS rebound_reverse, reverse('tag') AS lit_reverse ORDER BY reversed_name, rebound_reverse, lit_reverse"
        )

        self.assertEqual(
            expression.sql(),
            "SELECT LOWER(JSON_EXTRACT(with_q.\"__cg_with_person_properties\", '$.name')) AS \"lower_name\", UPPER(with_q.\"__cg_with_scalar_name\") AS \"upper_name\", LOWER('tag') AS \"lower_tag\", UPPER(:value) AS \"upper_value\", TRIM(with_q.\"__cg_with_scalar_name\") AS \"trimmed\", LTRIM(' tag') AS \"left_trimmed\", RTRIM('tag ') AS \"right_trimmed\" "
            "FROM (SELECT u.id AS \"__cg_with_person_id\", u.properties AS \"__cg_with_person_properties\", JSON_EXTRACT(u.properties, '$.name') AS \"__cg_with_scalar_name\" "
            "FROM nodes AS u JOIN node_labels AS u_label_0 ON u_label_0.node_id = u.id "
            "AND u_label_0.label = 'User') AS with_q "
            "ORDER BY LOWER(JSON_EXTRACT(with_q.\"__cg_with_person_properties\", '$.name')) ASC, UPPER(with_q.\"__cg_with_scalar_name\") ASC, LOWER('tag') ASC, UPPER(:value) ASC, TRIM(with_q.\"__cg_with_scalar_name\") ASC, LTRIM(' tag') ASC, RTRIM('tag ') ASC",
        )
        self.assertEqual(
            reverse_expression.sql(),
            "SELECT REVERSE(JSON_EXTRACT(with_q.\"__cg_with_person_properties\", '$.name')) AS \"reversed_name\", REVERSE(with_q.\"__cg_with_scalar_name\") AS \"rebound_reverse\", REVERSE('tag') AS \"lit_reverse\" "
            "FROM (SELECT u.id AS \"__cg_with_person_id\", u.properties AS \"__cg_with_person_properties\", JSON_EXTRACT(u.properties, '$.name') AS \"__cg_with_scalar_name\" "
            "FROM nodes AS u JOIN node_labels AS u_label_0 ON u_label_0.node_id = u.id "
            "AND u_label_0.label = 'User') AS with_q "
            "ORDER BY REVERSE(JSON_EXTRACT(with_q.\"__cg_with_person_properties\", '$.name')) ASC, REVERSE(with_q.\"__cg_with_scalar_name\") ASC, REVERSE('tag') ASC",
        )

    def test_compile_match_with_coalesce_returns(self) -> None:
        expression = cypherglot.compile_cypher_text(
            "MATCH (u:User) WITH u AS person, u.name AS name RETURN coalesce(person.name, 'unknown') AS display_name, coalesce(name, $fallback) AS rebound_name ORDER BY display_name, rebound_name"
        )
        no_alias_expression = cypherglot.compile_cypher_text(
            "MATCH (u:User) WITH u AS person, u.name AS name RETURN coalesce(person.name, 'unknown'), coalesce(name, $fallback)"
        )

        self.assertEqual(
            expression.sql(),
            "SELECT COALESCE(JSON_EXTRACT(with_q.\"__cg_with_person_properties\", '$.name'), 'unknown') AS \"display_name\", COALESCE(with_q.\"__cg_with_scalar_name\", :fallback) AS \"rebound_name\" "
            "FROM (SELECT u.id AS \"__cg_with_person_id\", u.properties AS \"__cg_with_person_properties\", JSON_EXTRACT(u.properties, '$.name') AS \"__cg_with_scalar_name\" "
            "FROM nodes AS u JOIN node_labels AS u_label_0 ON u_label_0.node_id = u.id "
            "AND u_label_0.label = 'User') AS with_q "
            "ORDER BY COALESCE(JSON_EXTRACT(with_q.\"__cg_with_person_properties\", '$.name'), 'unknown') ASC, COALESCE(with_q.\"__cg_with_scalar_name\", :fallback) ASC",
        )
        self.assertEqual(
            no_alias_expression.sql(),
            "SELECT COALESCE(JSON_EXTRACT(with_q.\"__cg_with_person_properties\", '$.name'), 'unknown') AS \"coalesce(person.name, 'unknown')\", COALESCE(with_q.\"__cg_with_scalar_name\", :fallback) AS \"coalesce(name, $fallback)\" FROM (SELECT u.id AS \"__cg_with_person_id\", u.properties AS \"__cg_with_person_properties\", JSON_EXTRACT(u.properties, '$.name') AS \"__cg_with_scalar_name\" FROM nodes AS u JOIN node_labels AS u_label_0 ON u_label_0.node_id = u.id AND u_label_0.label = 'User') AS with_q",
        )

    def test_compile_match_with_replace_returns(self) -> None:
        expression = cypherglot.compile_cypher_text(
            "MATCH (u:User) WITH u AS person, u.name AS name RETURN replace(person.name, 'A', 'B') AS display_name, replace(name, $needle, $replacement) AS rebound_name, replace('Alice', 'l', 'x') AS lit_name ORDER BY display_name, rebound_name, lit_name"
        )
        no_alias_expression = cypherglot.compile_cypher_text(
            "MATCH (u:User) WITH u AS person, u.name AS name RETURN replace(person.name, 'A', 'B'), replace(name, $needle, $replacement)"
        )

        self.assertEqual(
            expression.sql(),
            "SELECT REPLACE(JSON_EXTRACT(with_q.\"__cg_with_person_properties\", '$.name'), 'A', 'B') AS \"display_name\", REPLACE(with_q.\"__cg_with_scalar_name\", :needle, :replacement) AS \"rebound_name\", REPLACE('Alice', 'l', 'x') AS \"lit_name\" "
            "FROM (SELECT u.id AS \"__cg_with_person_id\", u.properties AS \"__cg_with_person_properties\", JSON_EXTRACT(u.properties, '$.name') AS \"__cg_with_scalar_name\" "
            "FROM nodes AS u JOIN node_labels AS u_label_0 ON u_label_0.node_id = u.id "
            "AND u_label_0.label = 'User') AS with_q "
            "ORDER BY REPLACE(JSON_EXTRACT(with_q.\"__cg_with_person_properties\", '$.name'), 'A', 'B') ASC, REPLACE(with_q.\"__cg_with_scalar_name\", :needle, :replacement) ASC, REPLACE('Alice', 'l', 'x') ASC",
        )
        self.assertEqual(
            no_alias_expression.sql(),
            "SELECT REPLACE(JSON_EXTRACT(with_q.\"__cg_with_person_properties\", '$.name'), 'A', 'B') AS \"replace(person.name, 'A', 'B')\", REPLACE(with_q.\"__cg_with_scalar_name\", :needle, :replacement) AS \"replace(name, $needle, $replacement)\" FROM (SELECT u.id AS \"__cg_with_person_id\", u.properties AS \"__cg_with_person_properties\", JSON_EXTRACT(u.properties, '$.name') AS \"__cg_with_scalar_name\" FROM nodes AS u JOIN node_labels AS u_label_0 ON u_label_0.node_id = u.id AND u_label_0.label = 'User') AS with_q",
        )

    def test_compile_match_with_left_and_right_returns(self) -> None:
        expression = cypherglot.compile_cypher_text(
            "MATCH (u:User) WITH u AS person, u.name AS name RETURN left(person.name, 2) AS prefix, right(name, $count) AS rebound_suffix, left('Alice', 3) AS lit_prefix ORDER BY prefix, rebound_suffix, lit_prefix"
        )
        no_alias_expression = cypherglot.compile_cypher_text(
            "MATCH (u:User) WITH u AS person, u.name AS name RETURN left(person.name, 2), right(name, $count)"
        )

        self.assertEqual(
            expression.sql(),
            "SELECT LEFT(JSON_EXTRACT(with_q.\"__cg_with_person_properties\", '$.name'), 2) AS \"prefix\", RIGHT(with_q.\"__cg_with_scalar_name\", :count) AS \"rebound_suffix\", LEFT('Alice', 3) AS \"lit_prefix\" "
            "FROM (SELECT u.id AS \"__cg_with_person_id\", u.properties AS \"__cg_with_person_properties\", JSON_EXTRACT(u.properties, '$.name') AS \"__cg_with_scalar_name\" "
            "FROM nodes AS u JOIN node_labels AS u_label_0 ON u_label_0.node_id = u.id "
            "AND u_label_0.label = 'User') AS with_q "
            "ORDER BY LEFT(JSON_EXTRACT(with_q.\"__cg_with_person_properties\", '$.name'), 2) ASC, RIGHT(with_q.\"__cg_with_scalar_name\", :count) ASC, LEFT('Alice', 3) ASC",
        )
        self.assertEqual(
            no_alias_expression.sql(),
            "SELECT LEFT(JSON_EXTRACT(with_q.\"__cg_with_person_properties\", '$.name'), 2) AS \"left(person.name, 2)\", RIGHT(with_q.\"__cg_with_scalar_name\", :count) AS \"right(name, $count)\" FROM (SELECT u.id AS \"__cg_with_person_id\", u.properties AS \"__cg_with_person_properties\", JSON_EXTRACT(u.properties, '$.name') AS \"__cg_with_scalar_name\" FROM nodes AS u JOIN node_labels AS u_label_0 ON u_label_0.node_id = u.id AND u_label_0.label = 'User') AS with_q",
        )

    def test_compile_match_with_split_returns(self) -> None:
        expression = cypherglot.compile_cypher_text(
            "MATCH (u:User) WITH u AS person, u.name AS name RETURN split(person.name, ' ') AS parts, split(name, $delimiter) AS rebound_parts, split('Alice Bob', ' ') AS lit_parts ORDER BY parts, rebound_parts, lit_parts"
        )
        no_alias_expression = cypherglot.compile_cypher_text(
            "MATCH (u:User) WITH u AS person, u.name AS name RETURN split(person.name, ' '), split(name, $delimiter)"
        )

        self.assertEqual(
            expression.sql(),
            "SELECT SPLIT(JSON_EXTRACT(with_q.\"__cg_with_person_properties\", '$.name'), ' ') AS \"parts\", SPLIT(with_q.\"__cg_with_scalar_name\", :delimiter) AS \"rebound_parts\", SPLIT('Alice Bob', ' ') AS \"lit_parts\" "
            "FROM (SELECT u.id AS \"__cg_with_person_id\", u.properties AS \"__cg_with_person_properties\", JSON_EXTRACT(u.properties, '$.name') AS \"__cg_with_scalar_name\" "
            "FROM nodes AS u JOIN node_labels AS u_label_0 ON u_label_0.node_id = u.id "
            "AND u_label_0.label = 'User') AS with_q "
            "ORDER BY SPLIT(JSON_EXTRACT(with_q.\"__cg_with_person_properties\", '$.name'), ' ') ASC, SPLIT(with_q.\"__cg_with_scalar_name\", :delimiter) ASC, SPLIT('Alice Bob', ' ') ASC",
        )
        self.assertEqual(
            no_alias_expression.sql(),
            "SELECT SPLIT(JSON_EXTRACT(with_q.\"__cg_with_person_properties\", '$.name'), ' ') AS \"split(person.name, ' ')\", SPLIT(with_q.\"__cg_with_scalar_name\", :delimiter) AS \"split(name, $delimiter)\" FROM (SELECT u.id AS \"__cg_with_person_id\", u.properties AS \"__cg_with_person_properties\", JSON_EXTRACT(u.properties, '$.name') AS \"__cg_with_scalar_name\" FROM nodes AS u JOIN node_labels AS u_label_0 ON u_label_0.node_id = u.id AND u_label_0.label = 'User') AS with_q",
        )

    def test_compile_match_with_abs_returns(self) -> None:
        expression = cypherglot.compile_cypher_text(
            "MATCH (u:User) WITH u AS person, u.age AS age RETURN abs(person.age) AS magnitude, abs(age) AS rebound, abs(-3) AS lit ORDER BY magnitude, rebound, lit"
        )
        no_alias_expression = cypherglot.compile_cypher_text(
            "MATCH (u:User) WITH u AS person, u.age AS age RETURN abs(person.age), abs(age), sign(age)"
        )

        self.assertEqual(
            expression.sql(),
            "SELECT ABS(JSON_EXTRACT(with_q.\"__cg_with_person_properties\", '$.age')) AS \"magnitude\", ABS(with_q.\"__cg_with_scalar_age\") AS \"rebound\", ABS(-3) AS \"lit\" "
            "FROM (SELECT u.id AS \"__cg_with_person_id\", u.properties AS \"__cg_with_person_properties\", JSON_EXTRACT(u.properties, '$.age') AS \"__cg_with_scalar_age\" "
            "FROM nodes AS u JOIN node_labels AS u_label_0 ON u_label_0.node_id = u.id "
            "AND u_label_0.label = 'User') AS with_q "
            "ORDER BY ABS(JSON_EXTRACT(with_q.\"__cg_with_person_properties\", '$.age')) ASC, ABS(with_q.\"__cg_with_scalar_age\") ASC, ABS(-3) ASC",
        )
        self.assertEqual(
            no_alias_expression.sql(),
            "SELECT ABS(JSON_EXTRACT(with_q.\"__cg_with_person_properties\", '$.age')) AS \"abs(person.age)\", ABS(with_q.\"__cg_with_scalar_age\") AS \"abs(age)\", SIGN(with_q.\"__cg_with_scalar_age\") AS \"sign(age)\" FROM (SELECT u.id AS \"__cg_with_person_id\", u.properties AS \"__cg_with_person_properties\", JSON_EXTRACT(u.properties, '$.age') AS \"__cg_with_scalar_age\" FROM nodes AS u JOIN node_labels AS u_label_0 ON u_label_0.node_id = u.id AND u_label_0.label = 'User') AS with_q",
        )

    def test_compile_match_with_sign_returns(self) -> None:
        expression = cypherglot.compile_cypher_text(
            "MATCH (u:User) WITH u AS person, u.age AS age RETURN sign(person.age) AS age_sign, sign(age) AS rebound_sign, sign(-3.2) AS lit_sign ORDER BY age_sign, rebound_sign, lit_sign"
        )

        self.assertEqual(
            expression.sql(),
            "SELECT SIGN(JSON_EXTRACT(with_q.\"__cg_with_person_properties\", '$.age')) AS \"age_sign\", SIGN(with_q.\"__cg_with_scalar_age\") AS \"rebound_sign\", SIGN(-3.2) AS \"lit_sign\" "
            "FROM (SELECT u.id AS \"__cg_with_person_id\", u.properties AS \"__cg_with_person_properties\", JSON_EXTRACT(u.properties, '$.age') AS \"__cg_with_scalar_age\" "
            "FROM nodes AS u JOIN node_labels AS u_label_0 ON u_label_0.node_id = u.id "
            "AND u_label_0.label = 'User') AS with_q "
            "ORDER BY SIGN(JSON_EXTRACT(with_q.\"__cg_with_person_properties\", '$.age')) ASC, SIGN(with_q.\"__cg_with_scalar_age\") ASC, SIGN(-3.2) ASC",
        )

    def test_compile_match_with_round_returns(self) -> None:
        expression = cypherglot.compile_cypher_text(
            "MATCH (u:User) WITH u AS person, u.score AS score RETURN round(person.score) AS value, round(score) AS rebound, round(-3.2) AS lit ORDER BY value, rebound, lit"
        )

        self.assertEqual(
            expression.sql(),
            "SELECT ROUND(JSON_EXTRACT(with_q.\"__cg_with_person_properties\", '$.score')) AS \"value\", ROUND(with_q.\"__cg_with_scalar_score\") AS \"rebound\", ROUND(-3.2) AS \"lit\" "
            "FROM (SELECT u.id AS \"__cg_with_person_id\", u.properties AS \"__cg_with_person_properties\", JSON_EXTRACT(u.properties, '$.score') AS \"__cg_with_scalar_score\" "
            "FROM nodes AS u JOIN node_labels AS u_label_0 ON u_label_0.node_id = u.id "
            "AND u_label_0.label = 'User') AS with_q "
            "ORDER BY ROUND(JSON_EXTRACT(with_q.\"__cg_with_person_properties\", '$.score')) ASC, ROUND(with_q.\"__cg_with_scalar_score\") ASC, ROUND(-3.2) ASC",
        )

    def test_compile_match_with_ceil_returns(self) -> None:
        expression = cypherglot.compile_cypher_text(
            "MATCH (u:User) WITH u AS person, u.score AS score RETURN ceil(person.score) AS value, ceil(score) AS rebound, ceil(-3.2) AS lit ORDER BY value, rebound, lit"
        )

        self.assertEqual(
            expression.sql(),
            "SELECT CEIL(JSON_EXTRACT(with_q.\"__cg_with_person_properties\", '$.score')) AS \"value\", CEIL(with_q.\"__cg_with_scalar_score\") AS \"rebound\", CEIL(-3.2) AS \"lit\" "
            "FROM (SELECT u.id AS \"__cg_with_person_id\", u.properties AS \"__cg_with_person_properties\", JSON_EXTRACT(u.properties, '$.score') AS \"__cg_with_scalar_score\" "
            "FROM nodes AS u JOIN node_labels AS u_label_0 ON u_label_0.node_id = u.id "
            "AND u_label_0.label = 'User') AS with_q "
            "ORDER BY CEIL(JSON_EXTRACT(with_q.\"__cg_with_person_properties\", '$.score')) ASC, CEIL(with_q.\"__cg_with_scalar_score\") ASC, CEIL(-3.2) ASC",
        )

    def test_compile_match_with_floor_returns(self) -> None:
        expression = cypherglot.compile_cypher_text(
            "MATCH (u:User) WITH u AS person, u.score AS score RETURN floor(person.score) AS value, floor(score) AS rebound, floor(-3.2) AS lit ORDER BY value, rebound, lit"
        )

        self.assertEqual(
            expression.sql(),
            "SELECT FLOOR(JSON_EXTRACT(with_q.\"__cg_with_person_properties\", '$.score')) AS \"value\", FLOOR(with_q.\"__cg_with_scalar_score\") AS \"rebound\", FLOOR(-3.2) AS \"lit\" "
            "FROM (SELECT u.id AS \"__cg_with_person_id\", u.properties AS \"__cg_with_person_properties\", JSON_EXTRACT(u.properties, '$.score') AS \"__cg_with_scalar_score\" "
            "FROM nodes AS u JOIN node_labels AS u_label_0 ON u_label_0.node_id = u.id "
            "AND u_label_0.label = 'User') AS with_q "
            "ORDER BY FLOOR(JSON_EXTRACT(with_q.\"__cg_with_person_properties\", '$.score')) ASC, FLOOR(with_q.\"__cg_with_scalar_score\") ASC, FLOOR(-3.2) ASC",
        )

    def test_compile_match_with_sqrt_returns(self) -> None:
        expression = cypherglot.compile_cypher_text(
            "MATCH (u:User) WITH u AS person, u.score AS score RETURN sqrt(person.score) AS value, sqrt(score) AS rebound, sqrt(-3.2) AS lit ORDER BY value, rebound, lit"
        )

        self.assertEqual(
            expression.sql(),
            "SELECT SQRT(JSON_EXTRACT(with_q.\"__cg_with_person_properties\", '$.score')) AS \"value\", SQRT(with_q.\"__cg_with_scalar_score\") AS \"rebound\", SQRT(-3.2) AS \"lit\" "
            "FROM (SELECT u.id AS \"__cg_with_person_id\", u.properties AS \"__cg_with_person_properties\", JSON_EXTRACT(u.properties, '$.score') AS \"__cg_with_scalar_score\" "
            "FROM nodes AS u JOIN node_labels AS u_label_0 ON u_label_0.node_id = u.id "
            "AND u_label_0.label = 'User') AS with_q "
            "ORDER BY SQRT(JSON_EXTRACT(with_q.\"__cg_with_person_properties\", '$.score')) ASC, SQRT(with_q.\"__cg_with_scalar_score\") ASC, SQRT(-3.2) ASC",
        )

    def test_compile_match_with_exp_returns(self) -> None:
        expression = cypherglot.compile_cypher_text(
            "MATCH (u:User) WITH u AS person, u.score AS score RETURN exp(person.score) AS value, exp(score) AS rebound, exp(-3.2) AS lit ORDER BY value, rebound, lit"
        )

        self.assertEqual(
            expression.sql(),
            "SELECT EXP(JSON_EXTRACT(with_q.\"__cg_with_person_properties\", '$.score')) AS \"value\", EXP(with_q.\"__cg_with_scalar_score\") AS \"rebound\", EXP(-3.2) AS \"lit\" "
            "FROM (SELECT u.id AS \"__cg_with_person_id\", u.properties AS \"__cg_with_person_properties\", JSON_EXTRACT(u.properties, '$.score') AS \"__cg_with_scalar_score\" "
            "FROM nodes AS u JOIN node_labels AS u_label_0 ON u_label_0.node_id = u.id "
            "AND u_label_0.label = 'User') AS with_q "
            "ORDER BY EXP(JSON_EXTRACT(with_q.\"__cg_with_person_properties\", '$.score')) ASC, EXP(with_q.\"__cg_with_scalar_score\") ASC, EXP(-3.2) ASC",
        )

    def test_compile_match_with_sin_returns(self) -> None:
        expression = cypherglot.compile_cypher_text(
            "MATCH (u:User) WITH u AS person, u.score AS score RETURN sin(person.score) AS value, sin(score) AS rebound, sin(-3.2) AS lit ORDER BY value, rebound, lit"
        )

        self.assertEqual(
            expression.sql(),
            "SELECT SIN(JSON_EXTRACT(with_q.\"__cg_with_person_properties\", '$.score')) AS \"value\", SIN(with_q.\"__cg_with_scalar_score\") AS \"rebound\", SIN(-3.2) AS \"lit\" "
            "FROM (SELECT u.id AS \"__cg_with_person_id\", u.properties AS \"__cg_with_person_properties\", JSON_EXTRACT(u.properties, '$.score') AS \"__cg_with_scalar_score\" "
            "FROM nodes AS u JOIN node_labels AS u_label_0 ON u_label_0.node_id = u.id "
            "AND u_label_0.label = 'User') AS with_q "
            "ORDER BY SIN(JSON_EXTRACT(with_q.\"__cg_with_person_properties\", '$.score')) ASC, SIN(with_q.\"__cg_with_scalar_score\") ASC, SIN(-3.2) ASC",
        )

    def test_compile_match_with_cos_returns(self) -> None:
        expression = cypherglot.compile_cypher_text(
            "MATCH (u:User) WITH u AS person, u.score AS score RETURN cos(person.score) AS value, cos(score) AS rebound, cos(-3.2) AS lit ORDER BY value, rebound, lit"
        )

        self.assertEqual(
            expression.sql(),
            "SELECT COS(JSON_EXTRACT(with_q.\"__cg_with_person_properties\", '$.score')) AS \"value\", COS(with_q.\"__cg_with_scalar_score\") AS \"rebound\", COS(-3.2) AS \"lit\" "
            "FROM (SELECT u.id AS \"__cg_with_person_id\", u.properties AS \"__cg_with_person_properties\", JSON_EXTRACT(u.properties, '$.score') AS \"__cg_with_scalar_score\" "
            "FROM nodes AS u JOIN node_labels AS u_label_0 ON u_label_0.node_id = u.id "
            "AND u_label_0.label = 'User') AS with_q "
            "ORDER BY COS(JSON_EXTRACT(with_q.\"__cg_with_person_properties\", '$.score')) ASC, COS(with_q.\"__cg_with_scalar_score\") ASC, COS(-3.2) ASC",
        )

    def test_compile_match_with_tan_returns(self) -> None:
        expression = cypherglot.compile_cypher_text(
            "MATCH (u:User) WITH u AS person, u.score AS score RETURN tan(person.score) AS value, tan(score) AS rebound, tan(-3.2) AS lit ORDER BY value, rebound, lit"
        )

        self.assertEqual(
            expression.sql(),
            "SELECT TAN(JSON_EXTRACT(with_q.\"__cg_with_person_properties\", '$.score')) AS \"value\", TAN(with_q.\"__cg_with_scalar_score\") AS \"rebound\", TAN(-3.2) AS \"lit\" "
            "FROM (SELECT u.id AS \"__cg_with_person_id\", u.properties AS \"__cg_with_person_properties\", JSON_EXTRACT(u.properties, '$.score') AS \"__cg_with_scalar_score\" "
            "FROM nodes AS u JOIN node_labels AS u_label_0 ON u_label_0.node_id = u.id "
            "AND u_label_0.label = 'User') AS with_q "
            "ORDER BY TAN(JSON_EXTRACT(with_q.\"__cg_with_person_properties\", '$.score')) ASC, TAN(with_q.\"__cg_with_scalar_score\") ASC, TAN(-3.2) ASC",
        )

    def test_compile_match_with_asin_returns(self) -> None:
        expression = cypherglot.compile_cypher_text(
            "MATCH (u:User) WITH u AS person, u.score AS score RETURN asin(person.score) AS value, asin(score) AS rebound, asin(-0.5) AS lit ORDER BY value, rebound, lit"
        )

        self.assertEqual(
            expression.sql(),
            "SELECT ASIN(JSON_EXTRACT(with_q.\"__cg_with_person_properties\", '$.score')) AS \"value\", ASIN(with_q.\"__cg_with_scalar_score\") AS \"rebound\", ASIN(-0.5) AS \"lit\" "
            "FROM (SELECT u.id AS \"__cg_with_person_id\", u.properties AS \"__cg_with_person_properties\", JSON_EXTRACT(u.properties, '$.score') AS \"__cg_with_scalar_score\" "
            "FROM nodes AS u JOIN node_labels AS u_label_0 ON u_label_0.node_id = u.id "
            "AND u_label_0.label = 'User') AS with_q "
            "ORDER BY ASIN(JSON_EXTRACT(with_q.\"__cg_with_person_properties\", '$.score')) ASC, ASIN(with_q.\"__cg_with_scalar_score\") ASC, ASIN(-0.5) ASC",
        )

    def test_compile_match_with_acos_returns(self) -> None:
        expression = cypherglot.compile_cypher_text(
            "MATCH (u:User) WITH u AS person, u.score AS score RETURN acos(person.score) AS value, acos(score) AS rebound, acos(-0.5) AS lit ORDER BY value, rebound, lit"
        )

        self.assertEqual(
            expression.sql(),
            "SELECT ACOS(JSON_EXTRACT(with_q.\"__cg_with_person_properties\", '$.score')) AS \"value\", ACOS(with_q.\"__cg_with_scalar_score\") AS \"rebound\", ACOS(-0.5) AS \"lit\" "
            "FROM (SELECT u.id AS \"__cg_with_person_id\", u.properties AS \"__cg_with_person_properties\", JSON_EXTRACT(u.properties, '$.score') AS \"__cg_with_scalar_score\" "
            "FROM nodes AS u JOIN node_labels AS u_label_0 ON u_label_0.node_id = u.id "
            "AND u_label_0.label = 'User') AS with_q "
            "ORDER BY ACOS(JSON_EXTRACT(with_q.\"__cg_with_person_properties\", '$.score')) ASC, ACOS(with_q.\"__cg_with_scalar_score\") ASC, ACOS(-0.5) ASC",
        )

    def test_compile_match_with_atan_returns(self) -> None:
        expression = cypherglot.compile_cypher_text(
            "MATCH (u:User) WITH u AS person, u.score AS score RETURN atan(person.score) AS value, atan(score) AS rebound, atan(-0.5) AS lit ORDER BY value, rebound, lit"
        )

        self.assertEqual(
            expression.sql(),
            "SELECT ATAN(JSON_EXTRACT(with_q.\"__cg_with_person_properties\", '$.score')) AS \"value\", ATAN(with_q.\"__cg_with_scalar_score\") AS \"rebound\", ATAN(-0.5) AS \"lit\" "
            "FROM (SELECT u.id AS \"__cg_with_person_id\", u.properties AS \"__cg_with_person_properties\", JSON_EXTRACT(u.properties, '$.score') AS \"__cg_with_scalar_score\" "
            "FROM nodes AS u JOIN node_labels AS u_label_0 ON u_label_0.node_id = u.id "
            "AND u_label_0.label = 'User') AS with_q "
            "ORDER BY ATAN(JSON_EXTRACT(with_q.\"__cg_with_person_properties\", '$.score')) ASC, ATAN(with_q.\"__cg_with_scalar_score\") ASC, ATAN(-0.5) ASC",
        )

    def test_compile_match_with_ln_returns(self) -> None:
        expression = cypherglot.compile_cypher_text(
            "MATCH (u:User) WITH u AS person, u.score AS score RETURN ln(person.score) AS value, ln(score) AS rebound, ln(0.5) AS lit ORDER BY value, rebound, lit"
        )

        self.assertEqual(
            expression.sql(),
            "SELECT LN(JSON_EXTRACT(with_q.\"__cg_with_person_properties\", '$.score')) AS \"value\", LN(with_q.\"__cg_with_scalar_score\") AS \"rebound\", LN(0.5) AS \"lit\" "
            "FROM (SELECT u.id AS \"__cg_with_person_id\", u.properties AS \"__cg_with_person_properties\", JSON_EXTRACT(u.properties, '$.score') AS \"__cg_with_scalar_score\" "
            "FROM nodes AS u JOIN node_labels AS u_label_0 ON u_label_0.node_id = u.id "
            "AND u_label_0.label = 'User') AS with_q "
            "ORDER BY LN(JSON_EXTRACT(with_q.\"__cg_with_person_properties\", '$.score')) ASC, LN(with_q.\"__cg_with_scalar_score\") ASC, LN(0.5) ASC",
        )

    def test_compile_match_with_log_returns(self) -> None:
        expression = cypherglot.compile_cypher_text(
            "MATCH (u:User) WITH u AS person, u.score AS score RETURN log(person.score) AS value, log(score) AS rebound, log(0.5) AS lit ORDER BY value, rebound, lit"
        )

        self.assertEqual(
            expression.sql(),
            "SELECT LOG(JSON_EXTRACT(with_q.\"__cg_with_person_properties\", '$.score')) AS \"value\", LOG(with_q.\"__cg_with_scalar_score\") AS \"rebound\", LOG(0.5) AS \"lit\" "
            "FROM (SELECT u.id AS \"__cg_with_person_id\", u.properties AS \"__cg_with_person_properties\", JSON_EXTRACT(u.properties, '$.score') AS \"__cg_with_scalar_score\" "
            "FROM nodes AS u JOIN node_labels AS u_label_0 ON u_label_0.node_id = u.id "
            "AND u_label_0.label = 'User') AS with_q "
            "ORDER BY LOG(JSON_EXTRACT(with_q.\"__cg_with_person_properties\", '$.score')) ASC, LOG(with_q.\"__cg_with_scalar_score\") ASC, LOG(0.5) ASC",
        )

    def test_compile_match_with_radians_returns(self) -> None:
        expression = cypherglot.compile_cypher_text(
            "MATCH (u:User) WITH u AS person, u.score AS score RETURN radians(person.score) AS value, radians(score) AS rebound, radians(180) AS lit ORDER BY value, rebound, lit"
        )

        self.assertEqual(
            expression.sql(),
            "SELECT RADIANS(JSON_EXTRACT(with_q.\"__cg_with_person_properties\", '$.score')) AS \"value\", RADIANS(with_q.\"__cg_with_scalar_score\") AS \"rebound\", RADIANS(180) AS \"lit\" "
            "FROM (SELECT u.id AS \"__cg_with_person_id\", u.properties AS \"__cg_with_person_properties\", JSON_EXTRACT(u.properties, '$.score') AS \"__cg_with_scalar_score\" "
            "FROM nodes AS u JOIN node_labels AS u_label_0 ON u_label_0.node_id = u.id "
            "AND u_label_0.label = 'User') AS with_q "
            "ORDER BY RADIANS(JSON_EXTRACT(with_q.\"__cg_with_person_properties\", '$.score')) ASC, RADIANS(with_q.\"__cg_with_scalar_score\") ASC, RADIANS(180) ASC",
        )

    def test_compile_match_with_degrees_returns(self) -> None:
        expression = cypherglot.compile_cypher_text(
            "MATCH (u:User) WITH u AS person, u.score AS score RETURN degrees(person.score) AS value, degrees(score) AS rebound, degrees(3.14159) AS lit ORDER BY value, rebound, lit"
        )

        self.assertEqual(
            expression.sql(),
            "SELECT DEGREES(JSON_EXTRACT(with_q.\"__cg_with_person_properties\", '$.score')) AS \"value\", DEGREES(with_q.\"__cg_with_scalar_score\") AS \"rebound\", DEGREES(3.14159) AS \"lit\" "
            "FROM (SELECT u.id AS \"__cg_with_person_id\", u.properties AS \"__cg_with_person_properties\", JSON_EXTRACT(u.properties, '$.score') AS \"__cg_with_scalar_score\" "
            "FROM nodes AS u JOIN node_labels AS u_label_0 ON u_label_0.node_id = u.id "
            "AND u_label_0.label = 'User') AS with_q "
            "ORDER BY DEGREES(JSON_EXTRACT(with_q.\"__cg_with_person_properties\", '$.score')) ASC, DEGREES(with_q.\"__cg_with_scalar_score\") ASC, DEGREES(3.14159) ASC",
        )

    def test_compile_match_with_log10_returns(self) -> None:
        expression = cypherglot.compile_cypher_text(
            "MATCH (u:User) WITH u AS person, u.score AS score RETURN log10(person.score) AS value, log10(score) AS rebound, log10(0.5) AS lit ORDER BY value, rebound, lit"
        )

        self.assertEqual(
            expression.sql(),
            "SELECT LOG(10, JSON_EXTRACT(with_q.\"__cg_with_person_properties\", '$.score')) AS \"value\", LOG(10, with_q.\"__cg_with_scalar_score\") AS \"rebound\", LOG(10, 0.5) AS \"lit\" "
            "FROM (SELECT u.id AS \"__cg_with_person_id\", u.properties AS \"__cg_with_person_properties\", JSON_EXTRACT(u.properties, '$.score') AS \"__cg_with_scalar_score\" "
            "FROM nodes AS u JOIN node_labels AS u_label_0 ON u_label_0.node_id = u.id "
            "AND u_label_0.label = 'User') AS with_q "
            "ORDER BY LOG(10, JSON_EXTRACT(with_q.\"__cg_with_person_properties\", '$.score')) ASC, LOG(10, with_q.\"__cg_with_scalar_score\") ASC, LOG(10, 0.5) ASC",
        )

    def test_compile_match_with_to_string_returns(self) -> None:
        expression = cypherglot.compile_cypher_text(
            "MATCH (u:User) WITH u AS person, u.age AS age RETURN toString(person.age) AS text, toString(age) AS rebound, toString(-3) AS lit ORDER BY text, rebound, lit"
        )
        no_alias_expression = cypherglot.compile_cypher_text(
            "MATCH (u:User) WITH u AS person, u.age AS age, u.active AS active RETURN toString(person.age), toString(age), toBoolean(active)"
        )

        self.assertEqual(
            expression.sql(),
            "SELECT CAST(JSON_EXTRACT(with_q.\"__cg_with_person_properties\", '$.age') AS TEXT) AS \"text\", CAST(with_q.\"__cg_with_scalar_age\" AS TEXT) AS \"rebound\", CAST(-3 AS TEXT) AS \"lit\" "
            "FROM (SELECT u.id AS \"__cg_with_person_id\", u.properties AS \"__cg_with_person_properties\", JSON_EXTRACT(u.properties, '$.age') AS \"__cg_with_scalar_age\" "
            "FROM nodes AS u JOIN node_labels AS u_label_0 ON u_label_0.node_id = u.id "
            "AND u_label_0.label = 'User') AS with_q "
            "ORDER BY CAST(JSON_EXTRACT(with_q.\"__cg_with_person_properties\", '$.age') AS TEXT) ASC, CAST(with_q.\"__cg_with_scalar_age\" AS TEXT) ASC, CAST(-3 AS TEXT) ASC",
        )
        self.assertEqual(
            no_alias_expression.sql(),
            "SELECT CAST(JSON_EXTRACT(with_q.\"__cg_with_person_properties\", '$.age') AS TEXT) AS \"toString(person.age)\", CAST(with_q.\"__cg_with_scalar_age\" AS TEXT) AS \"toString(age)\", CAST(with_q.\"__cg_with_scalar_active\" AS BOOLEAN) AS \"toBoolean(active)\" FROM (SELECT u.id AS \"__cg_with_person_id\", u.properties AS \"__cg_with_person_properties\", JSON_EXTRACT(u.properties, '$.age') AS \"__cg_with_scalar_age\", JSON_EXTRACT(u.properties, '$.active') AS \"__cg_with_scalar_active\" FROM nodes AS u JOIN node_labels AS u_label_0 ON u_label_0.node_id = u.id AND u_label_0.label = 'User') AS with_q",
        )

    def test_compile_match_with_to_integer_returns(self) -> None:
        expression = cypherglot.compile_cypher_text(
            "MATCH (u:User) WITH u AS person, u.age AS age RETURN toInteger(person.age) AS age_int, toInteger(age) AS rebound, toInteger(-3.2) AS lit ORDER BY age_int, rebound, lit"
        )

        self.assertEqual(
            expression.sql(),
            "SELECT CAST(JSON_EXTRACT(with_q.\"__cg_with_person_properties\", '$.age') AS INT) AS \"age_int\", CAST(with_q.\"__cg_with_scalar_age\" AS INT) AS \"rebound\", CAST(-3.2 AS INT) AS \"lit\" "
            "FROM (SELECT u.id AS \"__cg_with_person_id\", u.properties AS \"__cg_with_person_properties\", JSON_EXTRACT(u.properties, '$.age') AS \"__cg_with_scalar_age\" "
            "FROM nodes AS u JOIN node_labels AS u_label_0 ON u_label_0.node_id = u.id "
            "AND u_label_0.label = 'User') AS with_q "
            "ORDER BY CAST(JSON_EXTRACT(with_q.\"__cg_with_person_properties\", '$.age') AS INT) ASC, CAST(with_q.\"__cg_with_scalar_age\" AS INT) ASC, CAST(-3.2 AS INT) ASC",
        )

    def test_compile_match_node_with_to_float_returns(self) -> None:
        field_expression = cypherglot.compile_cypher_text(
            "MATCH (u:User) RETURN toFloat(u.age) AS age_float ORDER BY age_float"
        )
        literal_expression = cypherglot.compile_cypher_text(
            "MATCH (u:User) RETURN toFloat(-3) AS age_float ORDER BY age_float"
        )

        self.assertEqual(
            field_expression.sql(),
            "SELECT CAST(JSON_EXTRACT(u.properties, '$.age') AS FLOAT) AS \"age_float\" FROM nodes AS u JOIN node_labels AS u_label_0 ON u_label_0.node_id = u.id AND u_label_0.label = 'User' ORDER BY CAST(JSON_EXTRACT(u.properties, '$.age') AS FLOAT) ASC",
        )
        self.assertEqual(
            literal_expression.sql(),
            "SELECT CAST(-3 AS FLOAT) AS \"age_float\" FROM nodes AS u JOIN node_labels AS u_label_0 ON u_label_0.node_id = u.id AND u_label_0.label = 'User' ORDER BY CAST(-3 AS FLOAT) ASC",
        )

    def test_compile_match_node_with_to_boolean_returns(self) -> None:
        field_expression = cypherglot.compile_cypher_text(
            "MATCH (u:User) RETURN toBoolean(u.active) AS is_active ORDER BY is_active"
        )
        literal_expression = cypherglot.compile_cypher_text(
            "MATCH (u:User) RETURN toBoolean(true) AS is_active ORDER BY is_active"
        )

        self.assertEqual(
            field_expression.sql(),
            "SELECT CAST(JSON_EXTRACT(u.properties, '$.active') AS BOOLEAN) AS \"is_active\" FROM nodes AS u JOIN node_labels AS u_label_0 ON u_label_0.node_id = u.id AND u_label_0.label = 'User' ORDER BY CAST(JSON_EXTRACT(u.properties, '$.active') AS BOOLEAN) ASC",
        )
        self.assertEqual(
            literal_expression.sql(),
            "SELECT CAST(TRUE AS BOOLEAN) AS \"is_active\" FROM nodes AS u JOIN node_labels AS u_label_0 ON u_label_0.node_id = u.id AND u_label_0.label = 'User' ORDER BY CAST(TRUE AS BOOLEAN) ASC",
        )

    def test_compile_match_node_with_substring_returns(self) -> None:
        field_expression = cypherglot.compile_cypher_text(
            "MATCH (u:User) RETURN substring(u.name, 0, 2) AS prefix ORDER BY prefix"
        )
        no_alias_expression = cypherglot.compile_cypher_text(
            "MATCH (u:User) RETURN substring(u.name, 0, 2)"
        )
        field_two_arg_expression = cypherglot.compile_cypher_text(
            "MATCH (u:User) RETURN substring(u.name, 1) AS suffix ORDER BY suffix"
        )
        literal_expression = cypherglot.compile_cypher_text(
            "MATCH (u:User) RETURN substring('Alice', 1, 3) AS prefix ORDER BY prefix"
        )
        literal_two_arg_expression = cypherglot.compile_cypher_text(
            "MATCH (u:User) RETURN substring('Alice', 2) AS suffix ORDER BY suffix"
        )

        self.assertEqual(
            field_expression.sql(),
            "SELECT SUBSTRING(JSON_EXTRACT(u.properties, '$.name'), (0 + 1), 2) AS \"prefix\" FROM nodes AS u JOIN node_labels AS u_label_0 ON u_label_0.node_id = u.id AND u_label_0.label = 'User' ORDER BY SUBSTRING(JSON_EXTRACT(u.properties, '$.name'), (0 + 1), 2) ASC",
        )
        self.assertEqual(
            no_alias_expression.sql(),
            "SELECT SUBSTRING(JSON_EXTRACT(u.properties, '$.name'), (0 + 1), 2) AS \"substring(u.name, 0, 2)\" FROM nodes AS u JOIN node_labels AS u_label_0 ON u_label_0.node_id = u.id AND u_label_0.label = 'User'",
        )
        self.assertEqual(
            field_two_arg_expression.sql(),
            "SELECT SUBSTRING(JSON_EXTRACT(u.properties, '$.name'), (1 + 1)) AS \"suffix\" FROM nodes AS u JOIN node_labels AS u_label_0 ON u_label_0.node_id = u.id AND u_label_0.label = 'User' ORDER BY SUBSTRING(JSON_EXTRACT(u.properties, '$.name'), (1 + 1)) ASC",
        )
        self.assertEqual(
            literal_expression.sql(),
            "SELECT SUBSTRING('Alice', (1 + 1), 3) AS \"prefix\" FROM nodes AS u JOIN node_labels AS u_label_0 ON u_label_0.node_id = u.id AND u_label_0.label = 'User' ORDER BY SUBSTRING('Alice', (1 + 1), 3) ASC",
        )
        self.assertEqual(
            literal_two_arg_expression.sql(),
            "SELECT SUBSTRING('Alice', (2 + 1)) AS \"suffix\" FROM nodes AS u JOIN node_labels AS u_label_0 ON u_label_0.node_id = u.id AND u_label_0.label = 'User' ORDER BY SUBSTRING('Alice', (2 + 1)) ASC",
        )

    def test_compile_match_node_with_replace_returns(self) -> None:
        field_expression = cypherglot.compile_cypher_text(
            "MATCH (u:User) RETURN replace(u.name, 'A', 'B') AS display_name ORDER BY display_name"
        )
        no_alias_expression = cypherglot.compile_cypher_text(
            "MATCH (u:User) RETURN replace(u.name, 'A', 'B')"
        )
        literal_expression = cypherglot.compile_cypher_text(
            "MATCH (u:User) RETURN replace('Alice', 'l', 'x') AS alias_name ORDER BY alias_name"
        )

        self.assertEqual(
            field_expression.sql(),
            "SELECT REPLACE(JSON_EXTRACT(u.properties, '$.name'), 'A', 'B') AS \"display_name\" FROM nodes AS u JOIN node_labels AS u_label_0 ON u_label_0.node_id = u.id AND u_label_0.label = 'User' ORDER BY REPLACE(JSON_EXTRACT(u.properties, '$.name'), 'A', 'B') ASC",
        )
        self.assertEqual(
            no_alias_expression.sql(),
            "SELECT REPLACE(JSON_EXTRACT(u.properties, '$.name'), 'A', 'B') AS \"replace(u.name, 'A', 'B')\" FROM nodes AS u JOIN node_labels AS u_label_0 ON u_label_0.node_id = u.id AND u_label_0.label = 'User'",
        )
        self.assertEqual(
            literal_expression.sql(),
            "SELECT REPLACE('Alice', 'l', 'x') AS \"alias_name\" FROM nodes AS u JOIN node_labels AS u_label_0 ON u_label_0.node_id = u.id AND u_label_0.label = 'User' ORDER BY REPLACE('Alice', 'l', 'x') ASC",
        )

    def test_compile_match_node_with_left_and_right_returns(self) -> None:
        left_expression = cypherglot.compile_cypher_text(
            "MATCH (u:User) RETURN left(u.name, 2) AS prefix ORDER BY prefix"
        )
        no_alias_expression = cypherglot.compile_cypher_text(
            "MATCH (u:User) RETURN left(u.name, 2)"
        )
        right_expression = cypherglot.compile_cypher_text(
            "MATCH (u:User) RETURN right('Alice', 3) AS suffix ORDER BY suffix"
        )

        self.assertEqual(
            left_expression.sql(),
            "SELECT LEFT(JSON_EXTRACT(u.properties, '$.name'), 2) AS \"prefix\" FROM nodes AS u JOIN node_labels AS u_label_0 ON u_label_0.node_id = u.id AND u_label_0.label = 'User' ORDER BY LEFT(JSON_EXTRACT(u.properties, '$.name'), 2) ASC",
        )
        self.assertEqual(
            no_alias_expression.sql(),
            "SELECT LEFT(JSON_EXTRACT(u.properties, '$.name'), 2) AS \"left(u.name, 2)\" FROM nodes AS u JOIN node_labels AS u_label_0 ON u_label_0.node_id = u.id AND u_label_0.label = 'User'",
        )
        self.assertEqual(
            right_expression.sql(),
            "SELECT RIGHT('Alice', 3) AS \"suffix\" FROM nodes AS u JOIN node_labels AS u_label_0 ON u_label_0.node_id = u.id AND u_label_0.label = 'User' ORDER BY RIGHT('Alice', 3) ASC",
        )

    def test_compile_match_node_with_split_returns(self) -> None:
        field_expression = cypherglot.compile_cypher_text(
            "MATCH (u:User) RETURN split(u.name, ' ') AS parts ORDER BY parts"
        )
        no_alias_expression = cypherglot.compile_cypher_text(
            "MATCH (u:User) RETURN split(u.name, ' ')"
        )
        literal_expression = cypherglot.compile_cypher_text(
            "MATCH (u:User) RETURN split('Alice Bob', $delimiter) AS parts ORDER BY parts"
        )

        self.assertEqual(
            field_expression.sql(),
            "SELECT SPLIT(JSON_EXTRACT(u.properties, '$.name'), ' ') AS \"parts\" FROM nodes AS u JOIN node_labels AS u_label_0 ON u_label_0.node_id = u.id AND u_label_0.label = 'User' ORDER BY SPLIT(JSON_EXTRACT(u.properties, '$.name'), ' ') ASC",
        )
        self.assertEqual(
            no_alias_expression.sql(),
            "SELECT SPLIT(JSON_EXTRACT(u.properties, '$.name'), ' ') AS \"split(u.name, ' ')\" FROM nodes AS u JOIN node_labels AS u_label_0 ON u_label_0.node_id = u.id AND u_label_0.label = 'User'",
        )
        self.assertEqual(
            literal_expression.sql(),
            "SELECT SPLIT('Alice Bob', :delimiter) AS \"parts\" FROM nodes AS u JOIN node_labels AS u_label_0 ON u_label_0.node_id = u.id AND u_label_0.label = 'User' ORDER BY SPLIT('Alice Bob', :delimiter) ASC",
        )

    def test_compile_match_with_to_float_returns(self) -> None:
        expression = cypherglot.compile_cypher_text(
            "MATCH (u:User) WITH u AS person, u.age AS age RETURN toFloat(person.age) AS age_float, toFloat(age) AS rebound, toFloat(-3) AS lit ORDER BY age_float, rebound, lit"
        )

        self.assertEqual(
            expression.sql(),
            "SELECT CAST(JSON_EXTRACT(with_q.\"__cg_with_person_properties\", '$.age') AS FLOAT) AS \"age_float\", CAST(with_q.\"__cg_with_scalar_age\" AS FLOAT) AS \"rebound\", CAST(-3 AS FLOAT) AS \"lit\" "
            "FROM (SELECT u.id AS \"__cg_with_person_id\", u.properties AS \"__cg_with_person_properties\", JSON_EXTRACT(u.properties, '$.age') AS \"__cg_with_scalar_age\" "
            "FROM nodes AS u JOIN node_labels AS u_label_0 ON u_label_0.node_id = u.id "
            "AND u_label_0.label = 'User') AS with_q "
            "ORDER BY CAST(JSON_EXTRACT(with_q.\"__cg_with_person_properties\", '$.age') AS FLOAT) ASC, CAST(with_q.\"__cg_with_scalar_age\" AS FLOAT) ASC, CAST(-3 AS FLOAT) ASC",
        )

    def test_compile_match_with_to_boolean_returns(self) -> None:
        expression = cypherglot.compile_cypher_text(
            "MATCH (u:User) WITH u AS person, u.active AS active RETURN toBoolean(person.active) AS is_active, toBoolean(active) AS rebound, toBoolean(true) AS lit ORDER BY is_active, rebound, lit"
        )

        self.assertEqual(
            expression.sql(),
            "SELECT CAST(JSON_EXTRACT(with_q.\"__cg_with_person_properties\", '$.active') AS BOOLEAN) AS \"is_active\", CAST(with_q.\"__cg_with_scalar_active\" AS BOOLEAN) AS \"rebound\", CAST(TRUE AS BOOLEAN) AS \"lit\" "
            "FROM (SELECT u.id AS \"__cg_with_person_id\", u.properties AS \"__cg_with_person_properties\", JSON_EXTRACT(u.properties, '$.active') AS \"__cg_with_scalar_active\" "
            "FROM nodes AS u JOIN node_labels AS u_label_0 ON u_label_0.node_id = u.id "
            "AND u_label_0.label = 'User') AS with_q "
            "ORDER BY CAST(JSON_EXTRACT(with_q.\"__cg_with_person_properties\", '$.active') AS BOOLEAN) ASC, CAST(with_q.\"__cg_with_scalar_active\" AS BOOLEAN) ASC, CAST(TRUE AS BOOLEAN) ASC",
        )

    def test_compile_match_with_substring_returns(self) -> None:
        expression = cypherglot.compile_cypher_text(
            "MATCH (u:User) WITH u AS person, u.name AS name RETURN substring(person.name, 0, 2) AS prefix, substring(name, 1, 3) AS rebound, substring('Alice', 1, 3) AS lit ORDER BY prefix, rebound, lit"
        )
        two_arg_expression = cypherglot.compile_cypher_text(
            "MATCH (u:User) WITH u AS person, u.name AS name RETURN substring(person.name, 1) AS suffix, substring(name, 2) AS rebound_suffix, substring('Alice', 3) AS lit_suffix ORDER BY suffix, rebound_suffix, lit_suffix"
        )

        self.assertEqual(
            expression.sql(),
            "SELECT SUBSTRING(JSON_EXTRACT(with_q.\"__cg_with_person_properties\", '$.name'), (0 + 1), 2) AS \"prefix\", SUBSTRING(with_q.\"__cg_with_scalar_name\", (1 + 1), 3) AS \"rebound\", SUBSTRING('Alice', (1 + 1), 3) AS \"lit\" "
            "FROM (SELECT u.id AS \"__cg_with_person_id\", u.properties AS \"__cg_with_person_properties\", JSON_EXTRACT(u.properties, '$.name') AS \"__cg_with_scalar_name\" "
            "FROM nodes AS u JOIN node_labels AS u_label_0 ON u_label_0.node_id = u.id "
            "AND u_label_0.label = 'User') AS with_q "
            "ORDER BY SUBSTRING(JSON_EXTRACT(with_q.\"__cg_with_person_properties\", '$.name'), (0 + 1), 2) ASC, SUBSTRING(with_q.\"__cg_with_scalar_name\", (1 + 1), 3) ASC, SUBSTRING('Alice', (1 + 1), 3) ASC",
        )
        self.assertEqual(
            two_arg_expression.sql(),
            "SELECT SUBSTRING(JSON_EXTRACT(with_q.\"__cg_with_person_properties\", '$.name'), (1 + 1)) AS \"suffix\", SUBSTRING(with_q.\"__cg_with_scalar_name\", (2 + 1)) AS \"rebound_suffix\", SUBSTRING('Alice', (3 + 1)) AS \"lit_suffix\" "
            "FROM (SELECT u.id AS \"__cg_with_person_id\", u.properties AS \"__cg_with_person_properties\", JSON_EXTRACT(u.properties, '$.name') AS \"__cg_with_scalar_name\" "
            "FROM nodes AS u JOIN node_labels AS u_label_0 ON u_label_0.node_id = u.id "
            "AND u_label_0.label = 'User') AS with_q "
            "ORDER BY SUBSTRING(JSON_EXTRACT(with_q.\"__cg_with_person_properties\", '$.name'), (1 + 1)) ASC, SUBSTRING(with_q.\"__cg_with_scalar_name\", (2 + 1)) ASC, SUBSTRING('Alice', (3 + 1)) ASC",
        )

    def test_compile_match_with_relationship_property_string_returns(self) -> None:
        expression = cypherglot.compile_cypher_text(
            "MATCH (a:User)-[r:KNOWS]->(b:User) WITH r AS rel, r.note AS note RETURN lower(rel.note) AS lower_note, upper(note) AS upper_note, trim(rel.note) AS trimmed_note, ltrim(note) AS left_trimmed_note, rtrim(rel.note) AS right_trimmed_note, reverse(note) AS reversed_note, coalesce(rel.note, 'unknown') AS display_note, replace(note, 'A', 'B') AS replaced_note, left(rel.note, 2) AS prefix, right(note, 2) AS suffix, split(rel.note, ' ') AS parts, substring(note, 1) AS tail ORDER BY lower_note, upper_note, trimmed_note, left_trimmed_note, right_trimmed_note, reversed_note, display_note, replaced_note, prefix, suffix, parts, tail"
        )

        self.assertEqual(
            expression.sql(),
            "SELECT LOWER(JSON_EXTRACT(with_q.\"__cg_with_rel_properties\", '$.note')) AS \"lower_note\", UPPER(with_q.\"__cg_with_scalar_note\") AS \"upper_note\", TRIM(JSON_EXTRACT(with_q.\"__cg_with_rel_properties\", '$.note')) AS \"trimmed_note\", LTRIM(with_q.\"__cg_with_scalar_note\") AS \"left_trimmed_note\", RTRIM(JSON_EXTRACT(with_q.\"__cg_with_rel_properties\", '$.note')) AS \"right_trimmed_note\", REVERSE(with_q.\"__cg_with_scalar_note\") AS \"reversed_note\", COALESCE(JSON_EXTRACT(with_q.\"__cg_with_rel_properties\", '$.note'), 'unknown') AS \"display_note\", REPLACE(with_q.\"__cg_with_scalar_note\", 'A', 'B') AS \"replaced_note\", LEFT(JSON_EXTRACT(with_q.\"__cg_with_rel_properties\", '$.note'), 2) AS \"prefix\", RIGHT(with_q.\"__cg_with_scalar_note\", 2) AS \"suffix\", SPLIT(JSON_EXTRACT(with_q.\"__cg_with_rel_properties\", '$.note'), ' ') AS \"parts\", SUBSTRING(with_q.\"__cg_with_scalar_note\", (1 + 1)) AS \"tail\" "
            "FROM (SELECT r.id AS \"__cg_with_rel_id\", r.type AS \"__cg_with_rel_type\", r.properties AS \"__cg_with_rel_properties\", JSON_EXTRACT(r.properties, '$.note') AS \"__cg_with_scalar_note\" "
            "FROM edges AS r JOIN nodes AS a ON a.id = r.from_id JOIN nodes AS b ON b.id = r.to_id "
            "JOIN node_labels AS a_label_0 ON a_label_0.node_id = a.id AND a_label_0.label = 'User' "
            "JOIN node_labels AS b_label_1 ON b_label_1.node_id = b.id AND b_label_1.label = 'User' "
            "WHERE r.type = 'KNOWS') AS with_q "
            "ORDER BY LOWER(JSON_EXTRACT(with_q.\"__cg_with_rel_properties\", '$.note')) ASC, UPPER(with_q.\"__cg_with_scalar_note\") ASC, TRIM(JSON_EXTRACT(with_q.\"__cg_with_rel_properties\", '$.note')) ASC, LTRIM(with_q.\"__cg_with_scalar_note\") ASC, RTRIM(JSON_EXTRACT(with_q.\"__cg_with_rel_properties\", '$.note')) ASC, REVERSE(with_q.\"__cg_with_scalar_note\") ASC, COALESCE(JSON_EXTRACT(with_q.\"__cg_with_rel_properties\", '$.note'), 'unknown') ASC, REPLACE(with_q.\"__cg_with_scalar_note\", 'A', 'B') ASC, LEFT(JSON_EXTRACT(with_q.\"__cg_with_rel_properties\", '$.note'), 2) ASC, RIGHT(with_q.\"__cg_with_scalar_note\", 2) ASC, SPLIT(JSON_EXTRACT(with_q.\"__cg_with_rel_properties\", '$.note'), ' ') ASC, SUBSTRING(with_q.\"__cg_with_scalar_note\", (1 + 1)) ASC",
        )

    def test_compile_match_with_relationship_property_numeric_and_conversion_returns(
        self,
    ) -> None:
        expression = cypherglot.compile_cypher_text(
            "MATCH (a:User)-[r:KNOWS]->(b:User) WITH r AS rel, r.weight AS weight, r.score AS score, r.active AS active RETURN abs(rel.weight) AS magnitude, sign(weight) AS weight_sign, round(rel.score) AS rounded_score, ceil(score) AS ceil_score, floor(rel.score) AS floor_score, sqrt(rel.score) AS sqrt_score, exp(score) AS exp_score, sin(rel.score) AS sin_score, toString(weight) AS weight_text, toInteger(rel.score) AS score_int, toFloat(weight) AS weight_float, toBoolean(active) AS active_bool ORDER BY magnitude, weight_sign, rounded_score, ceil_score, floor_score, sqrt_score, exp_score, sin_score, weight_text, score_int, weight_float, active_bool"
        )

        self.assertEqual(
            expression.sql(),
            "SELECT ABS(JSON_EXTRACT(with_q.\"__cg_with_rel_properties\", '$.weight')) AS \"magnitude\", SIGN(with_q.\"__cg_with_scalar_weight\") AS \"weight_sign\", ROUND(JSON_EXTRACT(with_q.\"__cg_with_rel_properties\", '$.score')) AS \"rounded_score\", CEIL(with_q.\"__cg_with_scalar_score\") AS \"ceil_score\", FLOOR(JSON_EXTRACT(with_q.\"__cg_with_rel_properties\", '$.score')) AS \"floor_score\", SQRT(JSON_EXTRACT(with_q.\"__cg_with_rel_properties\", '$.score')) AS \"sqrt_score\", EXP(with_q.\"__cg_with_scalar_score\") AS \"exp_score\", SIN(JSON_EXTRACT(with_q.\"__cg_with_rel_properties\", '$.score')) AS \"sin_score\", CAST(with_q.\"__cg_with_scalar_weight\" AS TEXT) AS \"weight_text\", CAST(JSON_EXTRACT(with_q.\"__cg_with_rel_properties\", '$.score') AS INT) AS \"score_int\", CAST(with_q.\"__cg_with_scalar_weight\" AS FLOAT) AS \"weight_float\", CAST(with_q.\"__cg_with_scalar_active\" AS BOOLEAN) AS \"active_bool\" "
            "FROM (SELECT r.id AS \"__cg_with_rel_id\", r.type AS \"__cg_with_rel_type\", r.properties AS \"__cg_with_rel_properties\", JSON_EXTRACT(r.properties, '$.weight') AS \"__cg_with_scalar_weight\", JSON_EXTRACT(r.properties, '$.score') AS \"__cg_with_scalar_score\", JSON_EXTRACT(r.properties, '$.active') AS \"__cg_with_scalar_active\" "
            "FROM edges AS r JOIN nodes AS a ON a.id = r.from_id JOIN nodes AS b ON b.id = r.to_id "
            "JOIN node_labels AS a_label_0 ON a_label_0.node_id = a.id AND a_label_0.label = 'User' "
            "JOIN node_labels AS b_label_1 ON b_label_1.node_id = b.id AND b_label_1.label = 'User' "
            "WHERE r.type = 'KNOWS') AS with_q "
            "ORDER BY ABS(JSON_EXTRACT(with_q.\"__cg_with_rel_properties\", '$.weight')) ASC, SIGN(with_q.\"__cg_with_scalar_weight\") ASC, ROUND(JSON_EXTRACT(with_q.\"__cg_with_rel_properties\", '$.score')) ASC, CEIL(with_q.\"__cg_with_scalar_score\") ASC, FLOOR(JSON_EXTRACT(with_q.\"__cg_with_rel_properties\", '$.score')) ASC, SQRT(JSON_EXTRACT(with_q.\"__cg_with_rel_properties\", '$.score')) ASC, EXP(with_q.\"__cg_with_scalar_score\") ASC, SIN(JSON_EXTRACT(with_q.\"__cg_with_rel_properties\", '$.score')) ASC, CAST(with_q.\"__cg_with_scalar_weight\" AS TEXT) ASC, CAST(JSON_EXTRACT(with_q.\"__cg_with_rel_properties\", '$.score') AS INT) ASC, CAST(with_q.\"__cg_with_scalar_weight\" AS FLOAT) ASC, CAST(with_q.\"__cg_with_scalar_active\" AS BOOLEAN) ASC",
        )

    def test_compile_match_with_scalar_literal_and_parameter_returns(self) -> None:
        expression = cypherglot.compile_cypher_text(
            "MATCH (u:User) WITH u AS person RETURN 'tag' AS tag, $value AS value ORDER BY tag, value"
        )

        self.assertEqual(
            expression.sql(),
            "SELECT 'tag' AS \"tag\", :value AS \"value\" "
            "FROM (SELECT u.id AS \"__cg_with_person_id\", u.properties AS \"__cg_with_person_properties\" "
            "FROM nodes AS u JOIN node_labels AS u_label_0 ON u_label_0.node_id = u.id "
            "AND u_label_0.label = 'User') AS with_q "
            "ORDER BY 'tag' ASC, :value ASC",
        )

    def test_compile_match_with_predicate_returns(self) -> None:
        expression = cypherglot.compile_cypher_text(
            "MATCH (a:User)-[r:KNOWS]->(b:User) WITH a AS person, r AS rel, a.name AS name RETURN person.age >= 18 AS adult, name = 'Alice' AS is_alice, person.name CONTAINS 'a' AS has_a, name CONTAINS 'i' AS rebound_has_i, person.name STARTS WITH 'Al' AS has_prefix, name ENDS WITH 'ce' AS has_suffix, id(person) >= 1 AS has_id, type(rel) = 'KNOWS' AS rel_matches, size(person.name) >= 3 AS long_name, size(name) >= 3 AS rebound_long, size(id(person)) >= 1 AS long_id, size(type(rel)) >= 5 AS long_type ORDER BY adult, is_alice, has_a, rebound_has_i, has_prefix, has_suffix, has_id, rel_matches, long_name, rebound_long, long_id, long_type"
        )

        self.assertEqual(
            expression.sql(),
            "SELECT JSON_EXTRACT(with_q.\"__cg_with_person_properties\", '$.age') >= 18 AS \"adult\", with_q.\"__cg_with_scalar_name\" = 'Alice' AS \"is_alice\", STR_POSITION(JSON_EXTRACT(with_q.\"__cg_with_person_properties\", '$.name'), 'a') > 0 AS \"has_a\", STR_POSITION(with_q.\"__cg_with_scalar_name\", 'i') > 0 AS \"rebound_has_i\", SUBSTRING(JSON_EXTRACT(with_q.\"__cg_with_person_properties\", '$.name'), 1, LENGTH('Al')) = 'Al' AS \"has_prefix\", LENGTH(with_q.\"__cg_with_scalar_name\") >= LENGTH('ce') AND SUBSTRING(with_q.\"__cg_with_scalar_name\", LENGTH(with_q.\"__cg_with_scalar_name\") - LENGTH('ce') + 1) = 'ce' AS \"has_suffix\", with_q.\"__cg_with_person_id\" >= 1 AS \"has_id\", with_q.\"__cg_with_rel_type\" = 'KNOWS' AS \"rel_matches\", LENGTH(JSON_EXTRACT(with_q.\"__cg_with_person_properties\", '$.name')) >= 3 AS \"long_name\", LENGTH(with_q.\"__cg_with_scalar_name\") >= 3 AS \"rebound_long\", LENGTH(with_q.\"__cg_with_person_id\") >= 1 AS \"long_id\", LENGTH(with_q.\"__cg_with_rel_type\") >= 5 AS \"long_type\" "
            "FROM (SELECT a.id AS \"__cg_with_person_id\", a.properties AS \"__cg_with_person_properties\", r.id AS \"__cg_with_rel_id\", r.type AS \"__cg_with_rel_type\", r.properties AS \"__cg_with_rel_properties\", "
            "JSON_EXTRACT(a.properties, '$.name') AS \"__cg_with_scalar_name\" "
            "FROM edges AS r JOIN nodes AS a ON a.id = r.from_id JOIN nodes AS b ON b.id = r.to_id "
            "JOIN node_labels AS a_label_0 ON a_label_0.node_id = a.id AND a_label_0.label = 'User' "
            "JOIN node_labels AS b_label_1 ON b_label_1.node_id = b.id AND b_label_1.label = 'User' WHERE r.type = 'KNOWS') AS with_q "
            "ORDER BY JSON_EXTRACT(with_q.\"__cg_with_person_properties\", '$.age') >= 18 ASC, with_q.\"__cg_with_scalar_name\" = 'Alice' ASC, STR_POSITION(JSON_EXTRACT(with_q.\"__cg_with_person_properties\", '$.name'), 'a') > 0 ASC, STR_POSITION(with_q.\"__cg_with_scalar_name\", 'i') > 0 ASC, SUBSTRING(JSON_EXTRACT(with_q.\"__cg_with_person_properties\", '$.name'), 1, LENGTH('Al')) = 'Al' ASC, LENGTH(with_q.\"__cg_with_scalar_name\") >= LENGTH('ce') AND SUBSTRING(with_q.\"__cg_with_scalar_name\", LENGTH(with_q.\"__cg_with_scalar_name\") - LENGTH('ce') + 1) = 'ce' ASC, with_q.\"__cg_with_person_id\" >= 1 ASC, with_q.\"__cg_with_rel_type\" = 'KNOWS' ASC, LENGTH(JSON_EXTRACT(with_q.\"__cg_with_person_properties\", '$.name')) >= 3 ASC, LENGTH(with_q.\"__cg_with_scalar_name\") >= 3 ASC, LENGTH(with_q.\"__cg_with_person_id\") >= 1 ASC, LENGTH(with_q.\"__cg_with_rel_type\") >= 5 ASC",
        )

    def test_compile_match_with_relationship_property_predicate_returns(self) -> None:
        expression = cypherglot.compile_cypher_text(
            "MATCH (a:User)-[r:KNOWS]->(b:User) WITH r AS rel, r.note AS note, r.weight AS weight RETURN rel.weight >= 1 AS heavy, note CONTAINS 'a' AS has_a, rel.note STARTS WITH 'Al' AS has_prefix, note ENDS WITH 'ce' AS has_suffix, size(rel.note) >= 3 AS long_note, size(note) >= 3 AS rebound_long ORDER BY heavy, has_a, has_prefix, has_suffix, long_note, rebound_long"
        )

        self.assertEqual(
            expression.sql(),
            "SELECT JSON_EXTRACT(with_q.\"__cg_with_rel_properties\", '$.weight') >= 1 AS \"heavy\", STR_POSITION(with_q.\"__cg_with_scalar_note\", 'a') > 0 AS \"has_a\", SUBSTRING(JSON_EXTRACT(with_q.\"__cg_with_rel_properties\", '$.note'), 1, LENGTH('Al')) = 'Al' AS \"has_prefix\", LENGTH(with_q.\"__cg_with_scalar_note\") >= LENGTH('ce') AND SUBSTRING(with_q.\"__cg_with_scalar_note\", LENGTH(with_q.\"__cg_with_scalar_note\") - LENGTH('ce') + 1) = 'ce' AS \"has_suffix\", LENGTH(JSON_EXTRACT(with_q.\"__cg_with_rel_properties\", '$.note')) >= 3 AS \"long_note\", LENGTH(with_q.\"__cg_with_scalar_note\") >= 3 AS \"rebound_long\" "
            "FROM (SELECT r.id AS \"__cg_with_rel_id\", r.type AS \"__cg_with_rel_type\", r.properties AS \"__cg_with_rel_properties\", JSON_EXTRACT(r.properties, '$.note') AS \"__cg_with_scalar_note\", JSON_EXTRACT(r.properties, '$.weight') AS \"__cg_with_scalar_weight\" "
            "FROM edges AS r JOIN nodes AS a ON a.id = r.from_id JOIN nodes AS b ON b.id = r.to_id "
            "JOIN node_labels AS a_label_0 ON a_label_0.node_id = a.id AND a_label_0.label = 'User' "
            "JOIN node_labels AS b_label_1 ON b_label_1.node_id = b.id AND b_label_1.label = 'User' "
            "WHERE r.type = 'KNOWS') AS with_q "
            "ORDER BY JSON_EXTRACT(with_q.\"__cg_with_rel_properties\", '$.weight') >= 1 ASC, STR_POSITION(with_q.\"__cg_with_scalar_note\", 'a') > 0 ASC, SUBSTRING(JSON_EXTRACT(with_q.\"__cg_with_rel_properties\", '$.note'), 1, LENGTH('Al')) = 'Al' ASC, LENGTH(with_q.\"__cg_with_scalar_note\") >= LENGTH('ce') AND SUBSTRING(with_q.\"__cg_with_scalar_note\", LENGTH(with_q.\"__cg_with_scalar_note\") - LENGTH('ce') + 1) = 'ce' ASC, LENGTH(JSON_EXTRACT(with_q.\"__cg_with_rel_properties\", '$.note')) >= 3 ASC, LENGTH(with_q.\"__cg_with_scalar_note\") >= 3 ASC",
        )

    def test_compile_match_with_null_predicate_returns(self) -> None:
        expression = cypherglot.compile_cypher_text(
            "MATCH (a:User)-[r:KNOWS]->(b:User) WITH a AS person, r AS rel, a.name AS name, r.note AS note RETURN person.name IS NULL AS missing_name, name IS NOT NULL AS rebound_present, rel.note IS NULL AS missing_note, note IS NOT NULL AS rebound_note, size(name) IS NULL AS missing_len, size(rel.note) IS NOT NULL AS note_len ORDER BY missing_name, rebound_present, missing_note, rebound_note, missing_len, note_len"
        )

        self.assertEqual(
            expression.sql(),
            "SELECT (JSON_TYPE(with_q.\"__cg_with_person_properties\", '$.name') IS NULL OR JSON_TYPE(with_q.\"__cg_with_person_properties\", '$.name') = 'null') AS \"missing_name\", NOT with_q.\"__cg_with_scalar_name\" IS NULL AS \"rebound_present\", (JSON_TYPE(with_q.\"__cg_with_rel_properties\", '$.note') IS NULL OR JSON_TYPE(with_q.\"__cg_with_rel_properties\", '$.note') = 'null') AS \"missing_note\", NOT with_q.\"__cg_with_scalar_note\" IS NULL AS \"rebound_note\", LENGTH(with_q.\"__cg_with_scalar_name\") IS NULL AS \"missing_len\", NOT LENGTH(JSON_EXTRACT(with_q.\"__cg_with_rel_properties\", '$.note')) IS NULL AS \"note_len\" FROM (SELECT a.id AS \"__cg_with_person_id\", a.properties AS \"__cg_with_person_properties\", r.id AS \"__cg_with_rel_id\", r.type AS \"__cg_with_rel_type\", r.properties AS \"__cg_with_rel_properties\", JSON_EXTRACT(a.properties, '$.name') AS \"__cg_with_scalar_name\", JSON_EXTRACT(r.properties, '$.note') AS \"__cg_with_scalar_note\" FROM edges AS r JOIN nodes AS a ON a.id = r.from_id JOIN nodes AS b ON b.id = r.to_id JOIN node_labels AS a_label_0 ON a_label_0.node_id = a.id AND a_label_0.label = 'User' JOIN node_labels AS b_label_1 ON b_label_1.node_id = b.id AND b_label_1.label = 'User' WHERE r.type = 'KNOWS') AS with_q ORDER BY (JSON_TYPE(with_q.\"__cg_with_person_properties\", '$.name') IS NULL OR JSON_TYPE(with_q.\"__cg_with_person_properties\", '$.name') = 'null') ASC, NOT with_q.\"__cg_with_scalar_name\" IS NULL ASC, (JSON_TYPE(with_q.\"__cg_with_rel_properties\", '$.note') IS NULL OR JSON_TYPE(with_q.\"__cg_with_rel_properties\", '$.note') = 'null') ASC, NOT with_q.\"__cg_with_scalar_note\" IS NULL ASC, LENGTH(with_q.\"__cg_with_scalar_name\") IS NULL ASC, NOT LENGTH(JSON_EXTRACT(with_q.\"__cg_with_rel_properties\", '$.note')) IS NULL ASC",
        )

    def test_compile_unwind_literal_list(self) -> None:
        expression = cypherglot.compile_cypher_text(
            "UNWIND [1, 2, 3] AS x RETURN x AS value ORDER BY value DESC LIMIT 2"
        )

        self.assertEqual(
            expression.sql(),
            "SELECT with_q.\"__cg_with_scalar_x\" AS \"value\" "
            "FROM (SELECT unwind_q.value AS \"__cg_with_scalar_x\" FROM JSON_EACH(JSON_ARRAY(1, 2, 3)) AS unwind_q) AS with_q "
            "ORDER BY with_q.\"__cg_with_scalar_x\" DESC LIMIT 2",
        )

    def test_compile_unwind_parameter_source(self) -> None:
        expression = cypherglot.compile_cypher_text(
            "UNWIND $items AS x RETURN x ORDER BY x"
        )

        self.assertEqual(
            expression.sql(),
            "SELECT with_q.\"__cg_with_scalar_x\" AS \"x\" "
            "FROM (SELECT unwind_q.value AS \"__cg_with_scalar_x\" FROM JSON_EACH(:items) AS unwind_q) AS with_q "
            "ORDER BY with_q.\"__cg_with_scalar_x\" ASC",
        )

    def test_compile_optional_match_node(self) -> None:
        expression = cypherglot.compile_cypher_text(
            "OPTIONAL MATCH (u:User) WHERE u.name = 'Alice' RETURN u.name ORDER BY u.name LIMIT 1"
        )

        self.assertEqual(
            expression.sql(),
            "SELECT JSON_EXTRACT(u.properties, '$.name') AS \"u.name\" "
            "FROM (SELECT 1 AS __cg_seed) AS seed "
            "LEFT JOIN nodes AS u ON 1 = 1 AND EXISTS(SELECT 1 FROM node_labels AS u_label_filter WHERE u_label_filter.node_id = u.id AND u_label_filter.label = 'User') AND JSON_EXTRACT(u.properties, '$.name') = 'Alice' "
            "ORDER BY JSON_EXTRACT(u.properties, '$.name') ASC LIMIT 1",
        )

    def test_compile_match_node_with_aliased_return(self) -> None:
        expression = cypherglot.compile_cypher_text(
            "MATCH (u:User) RETURN u.name AS name ORDER BY name LIMIT 1"
        )

        self.assertEqual(
            expression.sql(),
            "SELECT JSON_EXTRACT(u.properties, '$.name') AS \"name\" "
            "FROM nodes AS u JOIN node_labels AS u_label_0 ON u_label_0.node_id = u.id "
            "AND u_label_0.label = 'User' "
            "ORDER BY JSON_EXTRACT(u.properties, '$.name') ASC LIMIT 1",
        )

    def test_compile_match_node_with_entity_return(self) -> None:
        expression = cypherglot.compile_cypher_text(
            "MATCH (u:User) RETURN u AS user"
        )

        self.assertEqual(
            expression.sql(),
            "SELECT JSON_OBJECT('id': u.id, 'label': (SELECT u_label_return.label FROM node_labels AS u_label_return WHERE u_label_return.node_id = u.id LIMIT 1), 'properties': JSON(COALESCE(u.properties, '{}'))) AS \"user\" "
            "FROM nodes AS u JOIN node_labels AS u_label_0 ON u_label_0.node_id = u.id AND u_label_0.label = 'User'",
        )

    def test_compile_match_relationship_with_entity_returns(self) -> None:
        expression = cypherglot.compile_cypher_text(
            "MATCH (a:User)-[r:KNOWS]->(b:User) RETURN a AS user, r AS rel, b.name AS name ORDER BY name"
        )
        ordered_expression = cypherglot.compile_cypher_text(
            "MATCH (a:User)-[r:KNOWS]->(b:User) RETURN r AS rel ORDER BY rel"
        )

        self.assertEqual(
            expression.sql(),
            "SELECT JSON_OBJECT('id': a.id, 'label': (SELECT a_label_return.label FROM node_labels AS a_label_return WHERE a_label_return.node_id = a.id LIMIT 1), 'properties': JSON(COALESCE(a.properties, '{}'))) AS \"user\", "
            "JSON_OBJECT('id': r.id, 'type': r.type, 'properties': JSON(COALESCE(r.properties, '{}'))) AS \"rel\", "
            "JSON_EXTRACT(b.properties, '$.name') AS \"name\" "
            "FROM edges AS r JOIN nodes AS a ON a.id = r.from_id JOIN nodes AS b ON b.id = r.to_id JOIN node_labels AS a_label_0 ON a_label_0.node_id = a.id AND a_label_0.label = 'User' JOIN node_labels AS b_label_1 ON b_label_1.node_id = b.id AND b_label_1.label = 'User' "
            "WHERE r.type = 'KNOWS' ORDER BY JSON_EXTRACT(b.properties, '$.name') ASC",
        )
        self.assertEqual(
            ordered_expression.sql(),
            "SELECT JSON_OBJECT('id': r.id, 'type': r.type, 'properties': JSON(COALESCE(r.properties, '{}'))) AS \"rel\" "
            "FROM edges AS r JOIN nodes AS a ON a.id = r.from_id JOIN nodes AS b ON b.id = r.to_id JOIN node_labels AS a_label_0 ON a_label_0.node_id = a.id AND a_label_0.label = 'User' JOIN node_labels AS b_label_1 ON b_label_1.node_id = b.id AND b_label_1.label = 'User' "
            "WHERE r.type = 'KNOWS' ORDER BY JSON_OBJECT('id': r.id, 'type': r.type, 'properties': JSON(COALESCE(r.properties, '{}'))) ASC",
        )

    def test_compile_match_node_with_plain_read_count(self) -> None:
        expression = cypherglot.compile_cypher_text(
            "MATCH (u:User) RETURN count(u) AS total"
        )
        star_expression = cypherglot.compile_cypher_text(
            "MATCH (u:User) RETURN count(*) AS total"
        )

        self.assertEqual(
            expression.sql(),
            "SELECT COUNT(u.id) AS \"total\" "
            "FROM nodes AS u JOIN node_labels AS u_label_0 ON u_label_0.node_id = u.id AND u_label_0.label = 'User'",
        )
        self.assertEqual(
            star_expression.sql(),
            "SELECT COUNT(*) AS \"total\" "
            "FROM nodes AS u JOIN node_labels AS u_label_0 ON u_label_0.node_id = u.id AND u_label_0.label = 'User'",
        )

    def test_compile_match_node_with_plain_read_numeric_aggregates(self) -> None:
        expression = cypherglot.compile_cypher_text(
            "MATCH (u:User) RETURN sum(u.score) AS total"
        )
        grouped_expression = cypherglot.compile_cypher_text(
            "MATCH (u:User) RETURN u.name AS name, avg(u.score) AS mean ORDER BY mean DESC"
        )

        self.assertEqual(
            expression.sql(),
            "SELECT SUM(JSON_EXTRACT(u.properties, '$.score')) AS \"total\" "
            "FROM nodes AS u JOIN node_labels AS u_label_0 ON u_label_0.node_id = u.id AND u_label_0.label = 'User'",
        )
        self.assertEqual(
            grouped_expression.sql(),
            "SELECT JSON_EXTRACT(u.properties, '$.name') AS \"name\", AVG(JSON_EXTRACT(u.properties, '$.score')) AS \"mean\" "
            "FROM nodes AS u JOIN node_labels AS u_label_0 ON u_label_0.node_id = u.id AND u_label_0.label = 'User' "
            "GROUP BY JSON_EXTRACT(u.properties, '$.name') ORDER BY \"mean\" DESC",
        )

    def test_compile_match_node_with_searched_case_returns(self) -> None:
        expression = cypherglot.compile_cypher_text(
            "MATCH (u:User) RETURN CASE WHEN u.age >= 18 THEN u.name ELSE 'minor' END AS label ORDER BY label"
        )
        optional_expression = cypherglot.compile_cypher_text(
            "OPTIONAL MATCH (u:User) RETURN CASE WHEN u.age >= 18 THEN u.name ELSE 'minor' END AS label ORDER BY label"
        )

        self.assertEqual(
            expression.sql(),
            "SELECT CASE WHEN JSON_EXTRACT(u.properties, '$.age') >= 18 THEN JSON_EXTRACT(u.properties, '$.name') ELSE 'minor' END AS \"label\" "
            "FROM nodes AS u JOIN node_labels AS u_label_0 ON u_label_0.node_id = u.id AND u_label_0.label = 'User' "
            "ORDER BY CASE WHEN JSON_EXTRACT(u.properties, '$.age') >= 18 THEN JSON_EXTRACT(u.properties, '$.name') ELSE 'minor' END ASC",
        )
        self.assertEqual(
            optional_expression.sql(),
            "SELECT CASE WHEN JSON_EXTRACT(u.properties, '$.age') >= 18 THEN JSON_EXTRACT(u.properties, '$.name') ELSE 'minor' END AS \"label\" "
            "FROM (SELECT 1 AS __cg_seed) AS seed LEFT JOIN nodes AS u ON 1 = 1 AND EXISTS(SELECT 1 FROM node_labels AS u_label_filter WHERE u_label_filter.node_id = u.id AND u_label_filter.label = 'User') "
            "ORDER BY CASE WHEN JSON_EXTRACT(u.properties, '$.age') >= 18 THEN JSON_EXTRACT(u.properties, '$.name') ELSE 'minor' END ASC",
        )

    def test_compile_match_node_with_properties_returns(self) -> None:
        expression = cypherglot.compile_cypher_text(
            "MATCH (u:User) RETURN properties(u) AS props ORDER BY props"
        )
        relationship_expression = cypherglot.compile_cypher_text(
            "MATCH (a:User)-[r:KNOWS]->(b:User) RETURN properties(r) AS props ORDER BY props"
        )
        optional_expression = cypherglot.compile_cypher_text(
            "OPTIONAL MATCH (u:User) RETURN properties(u) AS props ORDER BY props"
        )

        self.assertEqual(
            expression.sql(),
            "SELECT COALESCE(u.properties, '{}') AS \"props\" FROM nodes AS u JOIN node_labels AS u_label_0 ON u_label_0.node_id = u.id AND u_label_0.label = 'User' ORDER BY COALESCE(u.properties, '{}') ASC",
        )
        self.assertEqual(
            relationship_expression.sql(),
            "SELECT COALESCE(r.properties, '{}') AS \"props\" FROM edges AS r JOIN nodes AS a ON a.id = r.from_id JOIN nodes AS b ON b.id = r.to_id JOIN node_labels AS a_label_0 ON a_label_0.node_id = a.id AND a_label_0.label = 'User' JOIN node_labels AS b_label_1 ON b_label_1.node_id = b.id AND b_label_1.label = 'User' WHERE r.type = 'KNOWS' ORDER BY COALESCE(r.properties, '{}') ASC",
        )
        self.assertEqual(
            optional_expression.sql(),
            "SELECT COALESCE(u.properties, '{}') AS \"props\" FROM (SELECT 1 AS __cg_seed) AS seed LEFT JOIN nodes AS u ON 1 = 1 AND EXISTS(SELECT 1 FROM node_labels AS u_label_filter WHERE u_label_filter.node_id = u.id AND u_label_filter.label = 'User') ORDER BY COALESCE(u.properties, '{}') ASC",
        )

    def test_compile_match_node_with_labels_returns(self) -> None:
        expression = cypherglot.compile_cypher_text(
            "MATCH (u:User) RETURN labels(u) AS labels ORDER BY labels"
        )
        optional_expression = cypherglot.compile_cypher_text(
            "OPTIONAL MATCH (u:User) RETURN labels(u) AS labels ORDER BY labels"
        )

        self.assertEqual(
            expression.sql(),
            "SELECT COALESCE((SELECT JSON_GROUP_ARRAY(u_label_values.label) FROM node_labels AS u_label_values WHERE u_label_values.node_id = u.id), '[]') AS \"labels\" FROM nodes AS u JOIN node_labels AS u_label_0 ON u_label_0.node_id = u.id AND u_label_0.label = 'User' ORDER BY COALESCE((SELECT JSON_GROUP_ARRAY(u_label_values.label) FROM node_labels AS u_label_values WHERE u_label_values.node_id = u.id), '[]') ASC",
        )
        self.assertEqual(
            optional_expression.sql(),
            "SELECT COALESCE((SELECT JSON_GROUP_ARRAY(u_label_values.label) FROM node_labels AS u_label_values WHERE u_label_values.node_id = u.id), '[]') AS \"labels\" FROM (SELECT 1 AS __cg_seed) AS seed LEFT JOIN nodes AS u ON 1 = 1 AND EXISTS(SELECT 1 FROM node_labels AS u_label_filter WHERE u_label_filter.node_id = u.id AND u_label_filter.label = 'User') ORDER BY COALESCE((SELECT JSON_GROUP_ARRAY(u_label_values.label) FROM node_labels AS u_label_values WHERE u_label_values.node_id = u.id), '[]') ASC",
        )

    def test_compile_match_node_with_keys_returns(self) -> None:
        expression = cypherglot.compile_cypher_text(
            "MATCH (u:User) RETURN keys(u) AS keys ORDER BY keys"
        )
        relationship_expression = cypherglot.compile_cypher_text(
            "MATCH (a:User)-[r:KNOWS]->(b:User) RETURN keys(r) AS keys ORDER BY keys"
        )
        optional_expression = cypherglot.compile_cypher_text(
            "OPTIONAL MATCH (u:User) RETURN keys(u) AS keys ORDER BY keys"
        )

        self.assertEqual(
            expression.sql(),
            "SELECT COALESCE((SELECT JSON_GROUP_ARRAY(u_property_keys.key) FROM JSON_EACH(COALESCE(u.properties, '{}')) AS u_property_keys), '[]') AS \"keys\" FROM nodes AS u JOIN node_labels AS u_label_0 ON u_label_0.node_id = u.id AND u_label_0.label = 'User' ORDER BY COALESCE((SELECT JSON_GROUP_ARRAY(u_property_keys.key) FROM JSON_EACH(COALESCE(u.properties, '{}')) AS u_property_keys), '[]') ASC",
        )
        self.assertEqual(
            relationship_expression.sql(),
            "SELECT COALESCE((SELECT JSON_GROUP_ARRAY(r_property_keys.key) FROM JSON_EACH(COALESCE(r.properties, '{}')) AS r_property_keys), '[]') AS \"keys\" FROM edges AS r JOIN nodes AS a ON a.id = r.from_id JOIN nodes AS b ON b.id = r.to_id JOIN node_labels AS a_label_0 ON a_label_0.node_id = a.id AND a_label_0.label = 'User' JOIN node_labels AS b_label_1 ON b_label_1.node_id = b.id AND b_label_1.label = 'User' WHERE r.type = 'KNOWS' ORDER BY COALESCE((SELECT JSON_GROUP_ARRAY(r_property_keys.key) FROM JSON_EACH(COALESCE(r.properties, '{}')) AS r_property_keys), '[]') ASC",
        )
        self.assertEqual(
            optional_expression.sql(),
            "SELECT COALESCE((SELECT JSON_GROUP_ARRAY(u_property_keys.key) FROM JSON_EACH(COALESCE(u.properties, '{}')) AS u_property_keys), '[]') AS \"keys\" FROM (SELECT 1 AS __cg_seed) AS seed LEFT JOIN nodes AS u ON 1 = 1 AND EXISTS(SELECT 1 FROM node_labels AS u_label_filter WHERE u_label_filter.node_id = u.id AND u_label_filter.label = 'User') ORDER BY COALESCE((SELECT JSON_GROUP_ARRAY(u_property_keys.key) FROM JSON_EACH(COALESCE(u.properties, '{}')) AS u_property_keys), '[]') ASC",
        )

    def test_compile_match_relationship_with_start_and_end_node_returns(self) -> None:
        expression = cypherglot.compile_cypher_text(
            "MATCH (a:User)-[r:KNOWS]->(b:User) RETURN startNode(r) AS start, endNode(r) AS ending ORDER BY start, ending"
        )
        field_expression = cypherglot.compile_cypher_text(
            "MATCH (a:User)-[r:KNOWS]->(b:User) RETURN startNode(r).id AS start_id, endNode(r).id AS end_id ORDER BY start_id, end_id"
        )

        self.assertEqual(
            expression.sql(),
            "SELECT JSON_OBJECT('id': r.from_id, 'label': (SELECT r_start_label_return.label FROM node_labels AS r_start_label_return WHERE r_start_label_return.node_id = r.from_id LIMIT 1), 'properties': JSON(COALESCE((SELECT r_start_node.properties FROM nodes AS r_start_node WHERE r_start_node.id = r.from_id), '{}'))) AS \"start\", JSON_OBJECT('id': r.to_id, 'label': (SELECT r_end_label_return.label FROM node_labels AS r_end_label_return WHERE r_end_label_return.node_id = r.to_id LIMIT 1), 'properties': JSON(COALESCE((SELECT r_end_node.properties FROM nodes AS r_end_node WHERE r_end_node.id = r.to_id), '{}'))) AS \"ending\" FROM edges AS r JOIN nodes AS a ON a.id = r.from_id JOIN nodes AS b ON b.id = r.to_id JOIN node_labels AS a_label_0 ON a_label_0.node_id = a.id AND a_label_0.label = 'User' JOIN node_labels AS b_label_1 ON b_label_1.node_id = b.id AND b_label_1.label = 'User' WHERE r.type = 'KNOWS' ORDER BY JSON_OBJECT('id': r.from_id, 'label': (SELECT r_start_label_return.label FROM node_labels AS r_start_label_return WHERE r_start_label_return.node_id = r.from_id LIMIT 1), 'properties': JSON(COALESCE((SELECT r_start_node.properties FROM nodes AS r_start_node WHERE r_start_node.id = r.from_id), '{}'))) ASC, JSON_OBJECT('id': r.to_id, 'label': (SELECT r_end_label_return.label FROM node_labels AS r_end_label_return WHERE r_end_label_return.node_id = r.to_id LIMIT 1), 'properties': JSON(COALESCE((SELECT r_end_node.properties FROM nodes AS r_end_node WHERE r_end_node.id = r.to_id), '{}'))) ASC",
        )
        self.assertEqual(
            field_expression.sql(),
            "SELECT r.from_id AS \"start_id\", r.to_id AS \"end_id\" FROM edges AS r JOIN nodes AS a ON a.id = r.from_id JOIN nodes AS b ON b.id = r.to_id JOIN node_labels AS a_label_0 ON a_label_0.node_id = a.id AND a_label_0.label = 'User' JOIN node_labels AS b_label_1 ON b_label_1.node_id = b.id AND b_label_1.label = 'User' WHERE r.type = 'KNOWS' ORDER BY r.from_id ASC, r.to_id ASC",
        )

    def test_compile_match_node_with_grouped_entity_count(self) -> None:
        expression = cypherglot.compile_cypher_text(
            "MATCH (u:User) RETURN u AS user, count(u) AS total ORDER BY total DESC"
        )
        relationship_expression = cypherglot.compile_cypher_text(
            "MATCH (a:User)-[r:KNOWS]->(b:User) RETURN r AS rel, count(r) AS total ORDER BY total DESC"
        )
        relationship_star_expression = cypherglot.compile_cypher_text(
            "MATCH (a:User)-[r:KNOWS]->(b:User) RETURN r AS rel, count(*) AS total ORDER BY total DESC"
        )

        self.assertEqual(
            expression.sql(),
            "SELECT JSON_OBJECT('id': u.id, 'label': (SELECT u_label_return.label FROM node_labels AS u_label_return WHERE u_label_return.node_id = u.id LIMIT 1), 'properties': JSON(COALESCE(u.properties, '{}'))) AS \"user\", COUNT(u.id) AS \"total\" "
            "FROM nodes AS u JOIN node_labels AS u_label_0 ON u_label_0.node_id = u.id AND u_label_0.label = 'User' "
            "GROUP BY JSON_OBJECT('id': u.id, 'label': (SELECT u_label_return.label FROM node_labels AS u_label_return WHERE u_label_return.node_id = u.id LIMIT 1), 'properties': JSON(COALESCE(u.properties, '{}'))) ORDER BY \"total\" DESC",
        )
        self.assertEqual(
            relationship_expression.sql(),
            "SELECT JSON_OBJECT('id': r.id, 'type': r.type, 'properties': JSON(COALESCE(r.properties, '{}'))) AS \"rel\", COUNT(r.id) AS \"total\" "
            "FROM edges AS r JOIN nodes AS a ON a.id = r.from_id JOIN nodes AS b ON b.id = r.to_id JOIN node_labels AS a_label_0 ON a_label_0.node_id = a.id AND a_label_0.label = 'User' JOIN node_labels AS b_label_1 ON b_label_1.node_id = b.id AND b_label_1.label = 'User' "
            "WHERE r.type = 'KNOWS' GROUP BY JSON_OBJECT('id': r.id, 'type': r.type, 'properties': JSON(COALESCE(r.properties, '{}'))) ORDER BY \"total\" DESC",
        )
        self.assertEqual(
            relationship_star_expression.sql(),
            "SELECT JSON_OBJECT('id': r.id, 'type': r.type, 'properties': JSON(COALESCE(r.properties, '{}'))) AS \"rel\", COUNT(*) AS \"total\" "
            "FROM edges AS r JOIN nodes AS a ON a.id = r.from_id JOIN nodes AS b ON b.id = r.to_id JOIN node_labels AS a_label_0 ON a_label_0.node_id = a.id AND a_label_0.label = 'User' JOIN node_labels AS b_label_1 ON b_label_1.node_id = b.id AND b_label_1.label = 'User' "
            "WHERE r.type = 'KNOWS' GROUP BY JSON_OBJECT('id': r.id, 'type': r.type, 'properties': JSON(COALESCE(r.properties, '{}'))) ORDER BY \"total\" DESC",
        )

    def test_compile_match_node_with_scalar_literal_and_parameter_returns(self) -> None:
        literal_expression = cypherglot.compile_cypher_text(
            "MATCH (u:User) RETURN 'tag' AS tag ORDER BY tag"
        )
        parameter_expression = cypherglot.compile_cypher_text(
            "MATCH (u:User) RETURN $value AS value ORDER BY value"
        )

        self.assertEqual(
            literal_expression.sql(),
            "SELECT 'tag' AS \"tag\" FROM nodes AS u JOIN node_labels AS u_label_0 ON u_label_0.node_id = u.id AND u_label_0.label = 'User' ORDER BY 'tag' ASC",
        )
        self.assertEqual(
            parameter_expression.sql(),
            "SELECT :value AS \"value\" FROM nodes AS u JOIN node_labels AS u_label_0 ON u_label_0.node_id = u.id AND u_label_0.label = 'User' ORDER BY :value ASC",
        )

    def test_compile_match_node_with_size_returns(self) -> None:
        field_expression = cypherglot.compile_cypher_text(
            "MATCH (u:User) RETURN size(u.name) AS name_len ORDER BY name_len"
        )
        no_alias_expression = cypherglot.compile_cypher_text(
            "MATCH (u:User) RETURN size(u.name), size(id(u))"
        )
        literal_expression = cypherglot.compile_cypher_text(
            "MATCH (u:User) RETURN size('tag') AS tag_len ORDER BY tag_len"
        )
        id_expression = cypherglot.compile_cypher_text(
            "MATCH (u:User) RETURN size(id(u)) AS id_len ORDER BY id_len"
        )
        type_expression = cypherglot.compile_cypher_text(
            "MATCH (a:User)-[r:KNOWS]->(b:User) RETURN size(type(r)) AS type_len ORDER BY type_len"
        )

        self.assertEqual(
            field_expression.sql(),
            "SELECT LENGTH(JSON_EXTRACT(u.properties, '$.name')) AS \"name_len\" FROM nodes AS u JOIN node_labels AS u_label_0 ON u_label_0.node_id = u.id AND u_label_0.label = 'User' ORDER BY LENGTH(JSON_EXTRACT(u.properties, '$.name')) ASC",
        )
        self.assertEqual(
            no_alias_expression.sql(),
            "SELECT LENGTH(JSON_EXTRACT(u.properties, '$.name')) AS \"size(u.name)\", LENGTH(u.id) AS \"size(id(u))\" FROM nodes AS u JOIN node_labels AS u_label_0 ON u_label_0.node_id = u.id AND u_label_0.label = 'User'",
        )
        self.assertEqual(
            literal_expression.sql(),
            "SELECT LENGTH('tag') AS \"tag_len\" FROM nodes AS u JOIN node_labels AS u_label_0 ON u_label_0.node_id = u.id AND u_label_0.label = 'User' ORDER BY LENGTH('tag') ASC",
        )
        self.assertEqual(
            id_expression.sql(),
            "SELECT LENGTH(u.id) AS \"id_len\" FROM nodes AS u JOIN node_labels AS u_label_0 ON u_label_0.node_id = u.id AND u_label_0.label = 'User' ORDER BY LENGTH(u.id) ASC",
        )
        self.assertEqual(
            type_expression.sql(),
            "SELECT LENGTH(r.type) AS \"type_len\" FROM edges AS r JOIN nodes AS a ON a.id = r.from_id JOIN nodes AS b ON b.id = r.to_id JOIN node_labels AS a_label_0 ON a_label_0.node_id = a.id AND a_label_0.label = 'User' JOIN node_labels AS b_label_1 ON b_label_1.node_id = b.id AND b_label_1.label = 'User' WHERE r.type = 'KNOWS' ORDER BY LENGTH(r.type) ASC",
        )

    def test_compile_match_node_with_lower_and_upper_returns(self) -> None:
        lower_expression = cypherglot.compile_cypher_text(
            "MATCH (u:User) RETURN lower(u.name) AS lower_name ORDER BY lower_name"
        )
        no_alias_expression = cypherglot.compile_cypher_text(
            "MATCH (u:User) RETURN lower(u.name), reverse(u.name)"
        )
        upper_expression = cypherglot.compile_cypher_text(
            "MATCH (u:User) RETURN upper('tag') AS upper_tag ORDER BY upper_tag"
        )
        trim_expression = cypherglot.compile_cypher_text(
            "MATCH (u:User) RETURN trim(u.name) AS trimmed ORDER BY trimmed"
        )
        ltrim_expression = cypherglot.compile_cypher_text(
            "MATCH (u:User) RETURN ltrim(' tag') AS left_trimmed ORDER BY left_trimmed"
        )
        rtrim_expression = cypherglot.compile_cypher_text(
            "MATCH (u:User) RETURN rtrim('tag ') AS right_trimmed ORDER BY right_trimmed"
        )
        reverse_expression = cypherglot.compile_cypher_text(
            "MATCH (u:User) RETURN reverse(u.name) AS reversed_name ORDER BY reversed_name"
        )

        self.assertEqual(
            lower_expression.sql(),
            "SELECT LOWER(JSON_EXTRACT(u.properties, '$.name')) AS \"lower_name\" FROM nodes AS u JOIN node_labels AS u_label_0 ON u_label_0.node_id = u.id AND u_label_0.label = 'User' ORDER BY LOWER(JSON_EXTRACT(u.properties, '$.name')) ASC",
        )
        self.assertEqual(
            no_alias_expression.sql(),
            "SELECT LOWER(JSON_EXTRACT(u.properties, '$.name')) AS \"lower(u.name)\", REVERSE(JSON_EXTRACT(u.properties, '$.name')) AS \"reverse(u.name)\" FROM nodes AS u JOIN node_labels AS u_label_0 ON u_label_0.node_id = u.id AND u_label_0.label = 'User'",
        )
        self.assertEqual(
            upper_expression.sql(),
            "SELECT UPPER('tag') AS \"upper_tag\" FROM nodes AS u JOIN node_labels AS u_label_0 ON u_label_0.node_id = u.id AND u_label_0.label = 'User' ORDER BY UPPER('tag') ASC",
        )
        self.assertEqual(
            trim_expression.sql(),
            "SELECT TRIM(JSON_EXTRACT(u.properties, '$.name')) AS \"trimmed\" FROM nodes AS u JOIN node_labels AS u_label_0 ON u_label_0.node_id = u.id AND u_label_0.label = 'User' ORDER BY TRIM(JSON_EXTRACT(u.properties, '$.name')) ASC",
        )
        self.assertEqual(
            ltrim_expression.sql(),
            "SELECT LTRIM(' tag') AS \"left_trimmed\" FROM nodes AS u JOIN node_labels AS u_label_0 ON u_label_0.node_id = u.id AND u_label_0.label = 'User' ORDER BY LTRIM(' tag') ASC",
        )
        self.assertEqual(
            rtrim_expression.sql(),
            "SELECT RTRIM('tag ') AS \"right_trimmed\" FROM nodes AS u JOIN node_labels AS u_label_0 ON u_label_0.node_id = u.id AND u_label_0.label = 'User' ORDER BY RTRIM('tag ') ASC",
        )
        self.assertEqual(
            reverse_expression.sql(),
            "SELECT REVERSE(JSON_EXTRACT(u.properties, '$.name')) AS \"reversed_name\" FROM nodes AS u JOIN node_labels AS u_label_0 ON u_label_0.node_id = u.id AND u_label_0.label = 'User' ORDER BY REVERSE(JSON_EXTRACT(u.properties, '$.name')) ASC",
        )

    def test_compile_match_relationship_property_string_returns(self) -> None:
        expression = cypherglot.compile_cypher_text(
            "MATCH (a:User)-[r:KNOWS]->(b:User) RETURN lower(r.note) AS lower_note, upper(r.note) AS upper_note, trim(r.note) AS trimmed_note, ltrim(r.note) AS left_trimmed_note, rtrim(r.note) AS right_trimmed_note, reverse(r.note) AS reversed_note, coalesce(r.note, 'unknown') AS display_note, replace(r.note, 'A', 'B') AS replaced_note, left(r.note, 2) AS prefix, right(r.note, 2) AS suffix, split(r.note, ' ') AS parts, substring(r.note, 1) AS tail ORDER BY lower_note, upper_note, trimmed_note, left_trimmed_note, right_trimmed_note, reversed_note, display_note, replaced_note, prefix, suffix, parts, tail"
        )

        self.assertEqual(
            expression.sql(),
            "SELECT LOWER(JSON_EXTRACT(r.properties, '$.note')) AS \"lower_note\", UPPER(JSON_EXTRACT(r.properties, '$.note')) AS \"upper_note\", TRIM(JSON_EXTRACT(r.properties, '$.note')) AS \"trimmed_note\", LTRIM(JSON_EXTRACT(r.properties, '$.note')) AS \"left_trimmed_note\", RTRIM(JSON_EXTRACT(r.properties, '$.note')) AS \"right_trimmed_note\", REVERSE(JSON_EXTRACT(r.properties, '$.note')) AS \"reversed_note\", COALESCE(JSON_EXTRACT(r.properties, '$.note'), 'unknown') AS \"display_note\", REPLACE(JSON_EXTRACT(r.properties, '$.note'), 'A', 'B') AS \"replaced_note\", LEFT(JSON_EXTRACT(r.properties, '$.note'), 2) AS \"prefix\", RIGHT(JSON_EXTRACT(r.properties, '$.note'), 2) AS \"suffix\", SPLIT(JSON_EXTRACT(r.properties, '$.note'), ' ') AS \"parts\", SUBSTRING(JSON_EXTRACT(r.properties, '$.note'), (1 + 1)) AS \"tail\" FROM edges AS r JOIN nodes AS a ON a.id = r.from_id JOIN nodes AS b ON b.id = r.to_id JOIN node_labels AS a_label_0 ON a_label_0.node_id = a.id AND a_label_0.label = 'User' JOIN node_labels AS b_label_1 ON b_label_1.node_id = b.id AND b_label_1.label = 'User' WHERE r.type = 'KNOWS' ORDER BY LOWER(JSON_EXTRACT(r.properties, '$.note')) ASC, UPPER(JSON_EXTRACT(r.properties, '$.note')) ASC, TRIM(JSON_EXTRACT(r.properties, '$.note')) ASC, LTRIM(JSON_EXTRACT(r.properties, '$.note')) ASC, RTRIM(JSON_EXTRACT(r.properties, '$.note')) ASC, REVERSE(JSON_EXTRACT(r.properties, '$.note')) ASC, COALESCE(JSON_EXTRACT(r.properties, '$.note'), 'unknown') ASC, REPLACE(JSON_EXTRACT(r.properties, '$.note'), 'A', 'B') ASC, LEFT(JSON_EXTRACT(r.properties, '$.note'), 2) ASC, RIGHT(JSON_EXTRACT(r.properties, '$.note'), 2) ASC, SPLIT(JSON_EXTRACT(r.properties, '$.note'), ' ') ASC, SUBSTRING(JSON_EXTRACT(r.properties, '$.note'), (1 + 1)) ASC",
        )

    def test_compile_match_relationship_property_numeric_and_conversion_returns(
        self,
    ) -> None:
        expression = cypherglot.compile_cypher_text(
            "MATCH (a:User)-[r:KNOWS]->(b:User) RETURN abs(r.weight) AS magnitude, sign(r.weight) AS weight_sign, round(r.score) AS rounded_score, ceil(r.score) AS ceil_score, floor(r.score) AS floor_score, sqrt(r.score) AS sqrt_score, exp(r.score) AS exp_score, sin(r.score) AS sin_score, toString(r.weight) AS weight_text, toInteger(r.score) AS score_int, toFloat(r.weight) AS weight_float, toBoolean(r.active) AS active_bool ORDER BY magnitude, weight_sign, rounded_score, ceil_score, floor_score, sqrt_score, exp_score, sin_score, weight_text, score_int, weight_float, active_bool"
        )

        self.assertEqual(
            expression.sql(),
            "SELECT ABS(JSON_EXTRACT(r.properties, '$.weight')) AS \"magnitude\", SIGN(JSON_EXTRACT(r.properties, '$.weight')) AS \"weight_sign\", ROUND(JSON_EXTRACT(r.properties, '$.score')) AS \"rounded_score\", CEIL(JSON_EXTRACT(r.properties, '$.score')) AS \"ceil_score\", FLOOR(JSON_EXTRACT(r.properties, '$.score')) AS \"floor_score\", SQRT(JSON_EXTRACT(r.properties, '$.score')) AS \"sqrt_score\", EXP(JSON_EXTRACT(r.properties, '$.score')) AS \"exp_score\", SIN(JSON_EXTRACT(r.properties, '$.score')) AS \"sin_score\", CAST(JSON_EXTRACT(r.properties, '$.weight') AS TEXT) AS \"weight_text\", CAST(JSON_EXTRACT(r.properties, '$.score') AS INT) AS \"score_int\", CAST(JSON_EXTRACT(r.properties, '$.weight') AS FLOAT) AS \"weight_float\", CAST(JSON_EXTRACT(r.properties, '$.active') AS BOOLEAN) AS \"active_bool\" FROM edges AS r JOIN nodes AS a ON a.id = r.from_id JOIN nodes AS b ON b.id = r.to_id JOIN node_labels AS a_label_0 ON a_label_0.node_id = a.id AND a_label_0.label = 'User' JOIN node_labels AS b_label_1 ON b_label_1.node_id = b.id AND b_label_1.label = 'User' WHERE r.type = 'KNOWS' ORDER BY ABS(JSON_EXTRACT(r.properties, '$.weight')) ASC, SIGN(JSON_EXTRACT(r.properties, '$.weight')) ASC, ROUND(JSON_EXTRACT(r.properties, '$.score')) ASC, CEIL(JSON_EXTRACT(r.properties, '$.score')) ASC, FLOOR(JSON_EXTRACT(r.properties, '$.score')) ASC, SQRT(JSON_EXTRACT(r.properties, '$.score')) ASC, EXP(JSON_EXTRACT(r.properties, '$.score')) ASC, SIN(JSON_EXTRACT(r.properties, '$.score')) ASC, CAST(JSON_EXTRACT(r.properties, '$.weight') AS TEXT) ASC, CAST(JSON_EXTRACT(r.properties, '$.score') AS INT) ASC, CAST(JSON_EXTRACT(r.properties, '$.weight') AS FLOAT) ASC, CAST(JSON_EXTRACT(r.properties, '$.active') AS BOOLEAN) ASC",
        )

    def test_compile_match_node_with_coalesce_returns(self) -> None:
        field_expression = cypherglot.compile_cypher_text(
            "MATCH (u:User) RETURN coalesce(u.name, 'unknown') AS display_name ORDER BY display_name"
        )
        no_alias_expression = cypherglot.compile_cypher_text(
            "MATCH (u:User) RETURN coalesce(u.name, 'unknown')"
        )

        self.assertEqual(
            field_expression.sql(),
            "SELECT COALESCE(JSON_EXTRACT(u.properties, '$.name'), 'unknown') AS \"display_name\" FROM nodes AS u JOIN node_labels AS u_label_0 ON u_label_0.node_id = u.id AND u_label_0.label = 'User' ORDER BY COALESCE(JSON_EXTRACT(u.properties, '$.name'), 'unknown') ASC",
        )
        self.assertEqual(
            no_alias_expression.sql(),
            "SELECT COALESCE(JSON_EXTRACT(u.properties, '$.name'), 'unknown') AS \"coalesce(u.name, 'unknown')\" FROM nodes AS u JOIN node_labels AS u_label_0 ON u_label_0.node_id = u.id AND u_label_0.label = 'User'",
        )

    def test_compile_match_node_with_abs_returns(self) -> None:
        field_expression = cypherglot.compile_cypher_text(
            "MATCH (u:User) RETURN abs(u.age) AS magnitude ORDER BY magnitude"
        )
        literal_expression = cypherglot.compile_cypher_text(
            "MATCH (u:User) RETURN abs(-3) AS magnitude ORDER BY magnitude"
        )
        sign_expression = cypherglot.compile_cypher_text(
            "MATCH (u:User) RETURN sign(u.age) AS age_sign ORDER BY age_sign"
        )
        sign_literal_expression = cypherglot.compile_cypher_text(
            "MATCH (u:User) RETURN sign(-3.2) AS age_sign ORDER BY age_sign"
        )

        self.assertEqual(
            field_expression.sql(),
            "SELECT ABS(JSON_EXTRACT(u.properties, '$.age')) AS \"magnitude\" FROM nodes AS u JOIN node_labels AS u_label_0 ON u_label_0.node_id = u.id AND u_label_0.label = 'User' ORDER BY ABS(JSON_EXTRACT(u.properties, '$.age')) ASC",
        )
        self.assertEqual(
            literal_expression.sql(),
            "SELECT ABS(-3) AS \"magnitude\" FROM nodes AS u JOIN node_labels AS u_label_0 ON u_label_0.node_id = u.id AND u_label_0.label = 'User' ORDER BY ABS(-3) ASC",
        )
        self.assertEqual(
            sign_expression.sql(),
            "SELECT SIGN(JSON_EXTRACT(u.properties, '$.age')) AS \"age_sign\" FROM nodes AS u JOIN node_labels AS u_label_0 ON u_label_0.node_id = u.id AND u_label_0.label = 'User' ORDER BY SIGN(JSON_EXTRACT(u.properties, '$.age')) ASC",
        )
        self.assertEqual(
            sign_literal_expression.sql(),
            "SELECT SIGN(-3.2) AS \"age_sign\" FROM nodes AS u JOIN node_labels AS u_label_0 ON u_label_0.node_id = u.id AND u_label_0.label = 'User' ORDER BY SIGN(-3.2) ASC",
        )

    def test_compile_match_node_with_round_returns(self) -> None:
        field_expression = cypherglot.compile_cypher_text(
            "MATCH (u:User) RETURN round(u.score) AS value ORDER BY value"
        )
        literal_expression = cypherglot.compile_cypher_text(
            "MATCH (u:User) RETURN round(-3.2) AS value ORDER BY value"
        )

        self.assertEqual(
            field_expression.sql(),
            "SELECT ROUND(JSON_EXTRACT(u.properties, '$.score')) AS \"value\" FROM nodes AS u JOIN node_labels AS u_label_0 ON u_label_0.node_id = u.id AND u_label_0.label = 'User' ORDER BY ROUND(JSON_EXTRACT(u.properties, '$.score')) ASC",
        )
        self.assertEqual(
            literal_expression.sql(),
            "SELECT ROUND(-3.2) AS \"value\" FROM nodes AS u JOIN node_labels AS u_label_0 ON u_label_0.node_id = u.id AND u_label_0.label = 'User' ORDER BY ROUND(-3.2) ASC",
        )

    def test_compile_match_node_with_ceil_returns(self) -> None:
        field_expression = cypherglot.compile_cypher_text(
            "MATCH (u:User) RETURN ceil(u.score) AS value ORDER BY value"
        )
        literal_expression = cypherglot.compile_cypher_text(
            "MATCH (u:User) RETURN ceil(-3.2) AS value ORDER BY value"
        )

        self.assertEqual(
            field_expression.sql(),
            "SELECT CEIL(JSON_EXTRACT(u.properties, '$.score')) AS \"value\" FROM nodes AS u JOIN node_labels AS u_label_0 ON u_label_0.node_id = u.id AND u_label_0.label = 'User' ORDER BY CEIL(JSON_EXTRACT(u.properties, '$.score')) ASC",
        )
        self.assertEqual(
            literal_expression.sql(),
            "SELECT CEIL(-3.2) AS \"value\" FROM nodes AS u JOIN node_labels AS u_label_0 ON u_label_0.node_id = u.id AND u_label_0.label = 'User' ORDER BY CEIL(-3.2) ASC",
        )

    def test_compile_match_node_with_floor_returns(self) -> None:
        field_expression = cypherglot.compile_cypher_text(
            "MATCH (u:User) RETURN floor(u.score) AS value ORDER BY value"
        )
        literal_expression = cypherglot.compile_cypher_text(
            "MATCH (u:User) RETURN floor(-3.2) AS value ORDER BY value"
        )

        self.assertEqual(
            field_expression.sql(),
            "SELECT FLOOR(JSON_EXTRACT(u.properties, '$.score')) AS \"value\" FROM nodes AS u JOIN node_labels AS u_label_0 ON u_label_0.node_id = u.id AND u_label_0.label = 'User' ORDER BY FLOOR(JSON_EXTRACT(u.properties, '$.score')) ASC",
        )
        self.assertEqual(
            literal_expression.sql(),
            "SELECT FLOOR(-3.2) AS \"value\" FROM nodes AS u JOIN node_labels AS u_label_0 ON u_label_0.node_id = u.id AND u_label_0.label = 'User' ORDER BY FLOOR(-3.2) ASC",
        )

    def test_compile_match_node_with_sqrt_returns(self) -> None:
        field_expression = cypherglot.compile_cypher_text(
            "MATCH (u:User) RETURN sqrt(u.score) AS value ORDER BY value"
        )
        literal_expression = cypherglot.compile_cypher_text(
            "MATCH (u:User) RETURN sqrt(-3.2) AS value ORDER BY value"
        )

        self.assertEqual(
            field_expression.sql(),
            "SELECT SQRT(JSON_EXTRACT(u.properties, '$.score')) AS \"value\" FROM nodes AS u JOIN node_labels AS u_label_0 ON u_label_0.node_id = u.id AND u_label_0.label = 'User' ORDER BY SQRT(JSON_EXTRACT(u.properties, '$.score')) ASC",
        )
        self.assertEqual(
            literal_expression.sql(),
            "SELECT SQRT(-3.2) AS \"value\" FROM nodes AS u JOIN node_labels AS u_label_0 ON u_label_0.node_id = u.id AND u_label_0.label = 'User' ORDER BY SQRT(-3.2) ASC",
        )

    def test_compile_match_node_with_exp_returns(self) -> None:
        field_expression = cypherglot.compile_cypher_text(
            "MATCH (u:User) RETURN exp(u.score) AS value ORDER BY value"
        )
        literal_expression = cypherglot.compile_cypher_text(
            "MATCH (u:User) RETURN exp(-3.2) AS value ORDER BY value"
        )

        self.assertEqual(
            field_expression.sql(),
            "SELECT EXP(JSON_EXTRACT(u.properties, '$.score')) AS \"value\" FROM nodes AS u JOIN node_labels AS u_label_0 ON u_label_0.node_id = u.id AND u_label_0.label = 'User' ORDER BY EXP(JSON_EXTRACT(u.properties, '$.score')) ASC",
        )
        self.assertEqual(
            literal_expression.sql(),
            "SELECT EXP(-3.2) AS \"value\" FROM nodes AS u JOIN node_labels AS u_label_0 ON u_label_0.node_id = u.id AND u_label_0.label = 'User' ORDER BY EXP(-3.2) ASC",
        )

    def test_compile_match_node_with_sin_returns(self) -> None:
        field_expression = cypherglot.compile_cypher_text(
            "MATCH (u:User) RETURN sin(u.score) AS value ORDER BY value"
        )
        literal_expression = cypherglot.compile_cypher_text(
            "MATCH (u:User) RETURN sin(-3.2) AS value ORDER BY value"
        )

        self.assertEqual(
            field_expression.sql(),
            "SELECT SIN(JSON_EXTRACT(u.properties, '$.score')) AS \"value\" FROM nodes AS u JOIN node_labels AS u_label_0 ON u_label_0.node_id = u.id AND u_label_0.label = 'User' ORDER BY SIN(JSON_EXTRACT(u.properties, '$.score')) ASC",
        )
        self.assertEqual(
            literal_expression.sql(),
            "SELECT SIN(-3.2) AS \"value\" FROM nodes AS u JOIN node_labels AS u_label_0 ON u_label_0.node_id = u.id AND u_label_0.label = 'User' ORDER BY SIN(-3.2) ASC",
        )

    def test_compile_match_node_with_cos_returns(self) -> None:
        field_expression = cypherglot.compile_cypher_text(
            "MATCH (u:User) RETURN cos(u.score) AS value ORDER BY value"
        )
        literal_expression = cypherglot.compile_cypher_text(
            "MATCH (u:User) RETURN cos(-3.2) AS value ORDER BY value"
        )

        self.assertEqual(
            field_expression.sql(),
            "SELECT COS(JSON_EXTRACT(u.properties, '$.score')) AS \"value\" FROM nodes AS u JOIN node_labels AS u_label_0 ON u_label_0.node_id = u.id AND u_label_0.label = 'User' ORDER BY COS(JSON_EXTRACT(u.properties, '$.score')) ASC",
        )
        self.assertEqual(
            literal_expression.sql(),
            "SELECT COS(-3.2) AS \"value\" FROM nodes AS u JOIN node_labels AS u_label_0 ON u_label_0.node_id = u.id AND u_label_0.label = 'User' ORDER BY COS(-3.2) ASC",
        )

    def test_compile_match_node_with_tan_returns(self) -> None:
        field_expression = cypherglot.compile_cypher_text(
            "MATCH (u:User) RETURN tan(u.score) AS value ORDER BY value"
        )
        literal_expression = cypherglot.compile_cypher_text(
            "MATCH (u:User) RETURN tan(-3.2) AS value ORDER BY value"
        )

        self.assertEqual(
            field_expression.sql(),
            "SELECT TAN(JSON_EXTRACT(u.properties, '$.score')) AS \"value\" FROM nodes AS u JOIN node_labels AS u_label_0 ON u_label_0.node_id = u.id AND u_label_0.label = 'User' ORDER BY TAN(JSON_EXTRACT(u.properties, '$.score')) ASC",
        )
        self.assertEqual(
            literal_expression.sql(),
            "SELECT TAN(-3.2) AS \"value\" FROM nodes AS u JOIN node_labels AS u_label_0 ON u_label_0.node_id = u.id AND u_label_0.label = 'User' ORDER BY TAN(-3.2) ASC",
        )

    def test_compile_match_node_with_asin_returns(self) -> None:
        field_expression = cypherglot.compile_cypher_text(
            "MATCH (u:User) RETURN asin(u.score) AS value ORDER BY value"
        )
        literal_expression = cypherglot.compile_cypher_text(
            "MATCH (u:User) RETURN asin(-0.5) AS value ORDER BY value"
        )

        self.assertEqual(
            field_expression.sql(),
            "SELECT ASIN(JSON_EXTRACT(u.properties, '$.score')) AS \"value\" FROM nodes AS u JOIN node_labels AS u_label_0 ON u_label_0.node_id = u.id AND u_label_0.label = 'User' ORDER BY ASIN(JSON_EXTRACT(u.properties, '$.score')) ASC",
        )
        self.assertEqual(
            literal_expression.sql(),
            "SELECT ASIN(-0.5) AS \"value\" FROM nodes AS u JOIN node_labels AS u_label_0 ON u_label_0.node_id = u.id AND u_label_0.label = 'User' ORDER BY ASIN(-0.5) ASC",
        )

    def test_compile_match_node_with_acos_returns(self) -> None:
        field_expression = cypherglot.compile_cypher_text(
            "MATCH (u:User) RETURN acos(u.score) AS value ORDER BY value"
        )
        literal_expression = cypherglot.compile_cypher_text(
            "MATCH (u:User) RETURN acos(-0.5) AS value ORDER BY value"
        )

        self.assertEqual(
            field_expression.sql(),
            "SELECT ACOS(JSON_EXTRACT(u.properties, '$.score')) AS \"value\" FROM nodes AS u JOIN node_labels AS u_label_0 ON u_label_0.node_id = u.id AND u_label_0.label = 'User' ORDER BY ACOS(JSON_EXTRACT(u.properties, '$.score')) ASC",
        )
        self.assertEqual(
            literal_expression.sql(),
            "SELECT ACOS(-0.5) AS \"value\" FROM nodes AS u JOIN node_labels AS u_label_0 ON u_label_0.node_id = u.id AND u_label_0.label = 'User' ORDER BY ACOS(-0.5) ASC",
        )

    def test_compile_match_node_with_atan_returns(self) -> None:
        field_expression = cypherglot.compile_cypher_text(
            "MATCH (u:User) RETURN atan(u.score) AS value ORDER BY value"
        )
        literal_expression = cypherglot.compile_cypher_text(
            "MATCH (u:User) RETURN atan(-0.5) AS value ORDER BY value"
        )

        self.assertEqual(
            field_expression.sql(),
            "SELECT ATAN(JSON_EXTRACT(u.properties, '$.score')) AS \"value\" FROM nodes AS u JOIN node_labels AS u_label_0 ON u_label_0.node_id = u.id AND u_label_0.label = 'User' ORDER BY ATAN(JSON_EXTRACT(u.properties, '$.score')) ASC",
        )
        self.assertEqual(
            literal_expression.sql(),
            "SELECT ATAN(-0.5) AS \"value\" FROM nodes AS u JOIN node_labels AS u_label_0 ON u_label_0.node_id = u.id AND u_label_0.label = 'User' ORDER BY ATAN(-0.5) ASC",
        )

    def test_compile_match_node_with_ln_returns(self) -> None:
        field_expression = cypherglot.compile_cypher_text(
            "MATCH (u:User) RETURN ln(u.score) AS value ORDER BY value"
        )
        literal_expression = cypherglot.compile_cypher_text(
            "MATCH (u:User) RETURN ln(0.5) AS value ORDER BY value"
        )

        self.assertEqual(
            field_expression.sql(),
            "SELECT LN(JSON_EXTRACT(u.properties, '$.score')) AS \"value\" FROM nodes AS u JOIN node_labels AS u_label_0 ON u_label_0.node_id = u.id AND u_label_0.label = 'User' ORDER BY LN(JSON_EXTRACT(u.properties, '$.score')) ASC",
        )
        self.assertEqual(
            literal_expression.sql(),
            "SELECT LN(0.5) AS \"value\" FROM nodes AS u JOIN node_labels AS u_label_0 ON u_label_0.node_id = u.id AND u_label_0.label = 'User' ORDER BY LN(0.5) ASC",
        )

    def test_compile_match_node_with_log_returns(self) -> None:
        field_expression = cypherglot.compile_cypher_text(
            "MATCH (u:User) RETURN log(u.score) AS value ORDER BY value"
        )
        literal_expression = cypherglot.compile_cypher_text(
            "MATCH (u:User) RETURN log(0.5) AS value ORDER BY value"
        )

        self.assertEqual(
            field_expression.sql(),
            "SELECT LOG(JSON_EXTRACT(u.properties, '$.score')) AS \"value\" FROM nodes AS u JOIN node_labels AS u_label_0 ON u_label_0.node_id = u.id AND u_label_0.label = 'User' ORDER BY LOG(JSON_EXTRACT(u.properties, '$.score')) ASC",
        )
        self.assertEqual(
            literal_expression.sql(),
            "SELECT LOG(0.5) AS \"value\" FROM nodes AS u JOIN node_labels AS u_label_0 ON u_label_0.node_id = u.id AND u_label_0.label = 'User' ORDER BY LOG(0.5) ASC",
        )

    def test_compile_match_node_with_radians_returns(self) -> None:
        field_expression = cypherglot.compile_cypher_text(
            "MATCH (u:User) RETURN radians(u.score) AS value ORDER BY value"
        )
        literal_expression = cypherglot.compile_cypher_text(
            "MATCH (u:User) RETURN radians(180) AS value ORDER BY value"
        )

        self.assertEqual(
            field_expression.sql(),
            "SELECT RADIANS(JSON_EXTRACT(u.properties, '$.score')) AS \"value\" FROM nodes AS u JOIN node_labels AS u_label_0 ON u_label_0.node_id = u.id AND u_label_0.label = 'User' ORDER BY RADIANS(JSON_EXTRACT(u.properties, '$.score')) ASC",
        )
        self.assertEqual(
            literal_expression.sql(),
            "SELECT RADIANS(180) AS \"value\" FROM nodes AS u JOIN node_labels AS u_label_0 ON u_label_0.node_id = u.id AND u_label_0.label = 'User' ORDER BY RADIANS(180) ASC",
        )

    def test_compile_match_node_with_degrees_returns(self) -> None:
        field_expression = cypherglot.compile_cypher_text(
            "MATCH (u:User) RETURN degrees(u.score) AS value ORDER BY value"
        )
        literal_expression = cypherglot.compile_cypher_text(
            "MATCH (u:User) RETURN degrees(3.14159) AS value ORDER BY value"
        )

        self.assertEqual(
            field_expression.sql(),
            "SELECT DEGREES(JSON_EXTRACT(u.properties, '$.score')) AS \"value\" FROM nodes AS u JOIN node_labels AS u_label_0 ON u_label_0.node_id = u.id AND u_label_0.label = 'User' ORDER BY DEGREES(JSON_EXTRACT(u.properties, '$.score')) ASC",
        )
        self.assertEqual(
            literal_expression.sql(),
            "SELECT DEGREES(3.14159) AS \"value\" FROM nodes AS u JOIN node_labels AS u_label_0 ON u_label_0.node_id = u.id AND u_label_0.label = 'User' ORDER BY DEGREES(3.14159) ASC",
        )

    def test_compile_match_node_with_log10_returns(self) -> None:
        field_expression = cypherglot.compile_cypher_text(
            "MATCH (u:User) RETURN log10(u.score) AS value ORDER BY value"
        )
        literal_expression = cypherglot.compile_cypher_text(
            "MATCH (u:User) RETURN log10(0.5) AS value ORDER BY value"
        )

        self.assertEqual(
            field_expression.sql(),
            "SELECT LOG(10, JSON_EXTRACT(u.properties, '$.score')) AS \"value\" FROM nodes AS u JOIN node_labels AS u_label_0 ON u_label_0.node_id = u.id AND u_label_0.label = 'User' ORDER BY LOG(10, JSON_EXTRACT(u.properties, '$.score')) ASC",
        )
        self.assertEqual(
            literal_expression.sql(),
            "SELECT LOG(10, 0.5) AS \"value\" FROM nodes AS u JOIN node_labels AS u_label_0 ON u_label_0.node_id = u.id AND u_label_0.label = 'User' ORDER BY LOG(10, 0.5) ASC",
        )

    def test_compile_match_node_with_to_string_returns(self) -> None:
        field_expression = cypherglot.compile_cypher_text(
            "MATCH (u:User) RETURN toString(u.age) AS text ORDER BY text"
        )
        literal_expression = cypherglot.compile_cypher_text(
            "MATCH (u:User) RETURN toString(-3) AS text ORDER BY text"
        )

        self.assertEqual(
            field_expression.sql(),
            "SELECT CAST(JSON_EXTRACT(u.properties, '$.age') AS TEXT) AS \"text\" FROM nodes AS u JOIN node_labels AS u_label_0 ON u_label_0.node_id = u.id AND u_label_0.label = 'User' ORDER BY CAST(JSON_EXTRACT(u.properties, '$.age') AS TEXT) ASC",
        )
        self.assertEqual(
            literal_expression.sql(),
            "SELECT CAST(-3 AS TEXT) AS \"text\" FROM nodes AS u JOIN node_labels AS u_label_0 ON u_label_0.node_id = u.id AND u_label_0.label = 'User' ORDER BY CAST(-3 AS TEXT) ASC",
        )

    def test_compile_match_node_with_to_integer_returns(self) -> None:
        field_expression = cypherglot.compile_cypher_text(
            "MATCH (u:User) RETURN toInteger(u.age) AS age_int ORDER BY age_int"
        )
        literal_expression = cypherglot.compile_cypher_text(
            "MATCH (u:User) RETURN toInteger(-3.2) AS age_int ORDER BY age_int"
        )

        self.assertEqual(
            field_expression.sql(),
            "SELECT CAST(JSON_EXTRACT(u.properties, '$.age') AS INT) AS \"age_int\" FROM nodes AS u JOIN node_labels AS u_label_0 ON u_label_0.node_id = u.id AND u_label_0.label = 'User' ORDER BY CAST(JSON_EXTRACT(u.properties, '$.age') AS INT) ASC",
        )
        self.assertEqual(
            literal_expression.sql(),
            "SELECT CAST(-3.2 AS INT) AS \"age_int\" FROM nodes AS u JOIN node_labels AS u_label_0 ON u_label_0.node_id = u.id AND u_label_0.label = 'User' ORDER BY CAST(-3.2 AS INT) ASC",
        )

    def test_compile_match_node_with_predicate_returns(self) -> None:
        field_expression = cypherglot.compile_cypher_text(
            "MATCH (u:User) RETURN u.age >= 18 AS adult ORDER BY adult"
        )
        string_expression = cypherglot.compile_cypher_text(
            "MATCH (u:User) RETURN u.name CONTAINS 'a' AS has_a ORDER BY has_a"
        )
        starts_with_expression = cypherglot.compile_cypher_text(
            "MATCH (u:User) RETURN u.name STARTS WITH 'Al' AS has_prefix ORDER BY has_prefix"
        )
        ends_with_expression = cypherglot.compile_cypher_text(
            "MATCH (u:User) RETURN u.name ENDS WITH 'ce' AS has_suffix ORDER BY has_suffix"
        )
        id_expression = cypherglot.compile_cypher_text(
            "MATCH (u:User) RETURN id(u) >= 1 AS has_id ORDER BY has_id"
        )
        type_expression = cypherglot.compile_cypher_text(
            "MATCH (a:User)-[r:KNOWS]->(b:User) RETURN type(r) = 'KNOWS' AS is_knows ORDER BY is_knows"
        )
        size_expression = cypherglot.compile_cypher_text(
            "MATCH (u:User) RETURN size(u.name) >= 3 AS long_name ORDER BY long_name"
        )
        size_id_expression = cypherglot.compile_cypher_text(
            "MATCH (u:User) RETURN size(id(u)) >= 1 AS long_id ORDER BY long_id"
        )
        size_type_expression = cypherglot.compile_cypher_text(
            "MATCH (a:User)-[r:KNOWS]->(b:User) RETURN size(type(r)) >= 5 AS long_type ORDER BY long_type"
        )

        self.assertEqual(
            field_expression.sql(),
            "SELECT JSON_EXTRACT(u.properties, '$.age') >= 18 AS \"adult\" FROM nodes AS u JOIN node_labels AS u_label_0 ON u_label_0.node_id = u.id AND u_label_0.label = 'User' ORDER BY JSON_EXTRACT(u.properties, '$.age') >= 18 ASC",
        )
        self.assertEqual(
            string_expression.sql(),
            "SELECT STR_POSITION(JSON_EXTRACT(u.properties, '$.name'), 'a') > 0 AS \"has_a\" FROM nodes AS u JOIN node_labels AS u_label_0 ON u_label_0.node_id = u.id AND u_label_0.label = 'User' ORDER BY STR_POSITION(JSON_EXTRACT(u.properties, '$.name'), 'a') > 0 ASC",
        )
        self.assertEqual(
            starts_with_expression.sql(),
            "SELECT SUBSTRING(JSON_EXTRACT(u.properties, '$.name'), 1, LENGTH('Al')) = 'Al' AS \"has_prefix\" FROM nodes AS u JOIN node_labels AS u_label_0 ON u_label_0.node_id = u.id AND u_label_0.label = 'User' ORDER BY SUBSTRING(JSON_EXTRACT(u.properties, '$.name'), 1, LENGTH('Al')) = 'Al' ASC",
        )
        self.assertEqual(
            ends_with_expression.sql(),
            "SELECT LENGTH(JSON_EXTRACT(u.properties, '$.name')) >= LENGTH('ce') AND SUBSTRING(JSON_EXTRACT(u.properties, '$.name'), LENGTH(JSON_EXTRACT(u.properties, '$.name')) - LENGTH('ce') + 1) = 'ce' AS \"has_suffix\" FROM nodes AS u JOIN node_labels AS u_label_0 ON u_label_0.node_id = u.id AND u_label_0.label = 'User' ORDER BY LENGTH(JSON_EXTRACT(u.properties, '$.name')) >= LENGTH('ce') AND SUBSTRING(JSON_EXTRACT(u.properties, '$.name'), LENGTH(JSON_EXTRACT(u.properties, '$.name')) - LENGTH('ce') + 1) = 'ce' ASC",
        )
        self.assertEqual(
            id_expression.sql(),
            "SELECT u.id >= 1 AS \"has_id\" FROM nodes AS u JOIN node_labels AS u_label_0 ON u_label_0.node_id = u.id AND u_label_0.label = 'User' ORDER BY u.id >= 1 ASC",
        )
        self.assertEqual(
            type_expression.sql(),
            "SELECT r.type = 'KNOWS' AS \"is_knows\" FROM edges AS r JOIN nodes AS a ON a.id = r.from_id JOIN nodes AS b ON b.id = r.to_id JOIN node_labels AS a_label_0 ON a_label_0.node_id = a.id AND a_label_0.label = 'User' JOIN node_labels AS b_label_1 ON b_label_1.node_id = b.id AND b_label_1.label = 'User' WHERE r.type = 'KNOWS' ORDER BY r.type = 'KNOWS' ASC",
        )
        self.assertEqual(
            size_expression.sql(),
            "SELECT LENGTH(JSON_EXTRACT(u.properties, '$.name')) >= 3 AS \"long_name\" FROM nodes AS u JOIN node_labels AS u_label_0 ON u_label_0.node_id = u.id AND u_label_0.label = 'User' ORDER BY LENGTH(JSON_EXTRACT(u.properties, '$.name')) >= 3 ASC",
        )
        self.assertEqual(
            size_id_expression.sql(),
            "SELECT LENGTH(u.id) >= 1 AS \"long_id\" FROM nodes AS u JOIN node_labels AS u_label_0 ON u_label_0.node_id = u.id AND u_label_0.label = 'User' ORDER BY LENGTH(u.id) >= 1 ASC",
        )
        self.assertEqual(
            size_type_expression.sql(),
            "SELECT LENGTH(r.type) >= 5 AS \"long_type\" FROM edges AS r JOIN nodes AS a ON a.id = r.from_id JOIN nodes AS b ON b.id = r.to_id JOIN node_labels AS a_label_0 ON a_label_0.node_id = a.id AND a_label_0.label = 'User' JOIN node_labels AS b_label_1 ON b_label_1.node_id = b.id AND b_label_1.label = 'User' WHERE r.type = 'KNOWS' ORDER BY LENGTH(r.type) >= 5 ASC",
        )

    def test_compile_match_relationship_property_predicate_returns(self) -> None:
        expression = cypherglot.compile_cypher_text(
            "MATCH (a:User)-[r:KNOWS]->(b:User) RETURN r.weight >= 1 AS heavy, r.note CONTAINS 'a' AS has_a, r.note STARTS WITH 'Al' AS has_prefix, r.note ENDS WITH 'ce' AS has_suffix, size(r.note) >= 3 AS long_note ORDER BY heavy, has_a, has_prefix, has_suffix, long_note"
        )

        self.assertEqual(
            expression.sql(),
            "SELECT JSON_EXTRACT(r.properties, '$.weight') >= 1 AS \"heavy\", STR_POSITION(JSON_EXTRACT(r.properties, '$.note'), 'a') > 0 AS \"has_a\", SUBSTRING(JSON_EXTRACT(r.properties, '$.note'), 1, LENGTH('Al')) = 'Al' AS \"has_prefix\", LENGTH(JSON_EXTRACT(r.properties, '$.note')) >= LENGTH('ce') AND SUBSTRING(JSON_EXTRACT(r.properties, '$.note'), LENGTH(JSON_EXTRACT(r.properties, '$.note')) - LENGTH('ce') + 1) = 'ce' AS \"has_suffix\", LENGTH(JSON_EXTRACT(r.properties, '$.note')) >= 3 AS \"long_note\" FROM edges AS r JOIN nodes AS a ON a.id = r.from_id JOIN nodes AS b ON b.id = r.to_id JOIN node_labels AS a_label_0 ON a_label_0.node_id = a.id AND a_label_0.label = 'User' JOIN node_labels AS b_label_1 ON b_label_1.node_id = b.id AND b_label_1.label = 'User' WHERE r.type = 'KNOWS' ORDER BY JSON_EXTRACT(r.properties, '$.weight') >= 1 ASC, STR_POSITION(JSON_EXTRACT(r.properties, '$.note'), 'a') > 0 ASC, SUBSTRING(JSON_EXTRACT(r.properties, '$.note'), 1, LENGTH('Al')) = 'Al' ASC, LENGTH(JSON_EXTRACT(r.properties, '$.note')) >= LENGTH('ce') AND SUBSTRING(JSON_EXTRACT(r.properties, '$.note'), LENGTH(JSON_EXTRACT(r.properties, '$.note')) - LENGTH('ce') + 1) = 'ce' ASC, LENGTH(JSON_EXTRACT(r.properties, '$.note')) >= 3 ASC",
        )

    def test_compile_match_where_string_and_null_filters(self) -> None:
        expression = cypherglot.compile_cypher_text(
            "MATCH (u:User) WHERE u.name STARTS WITH 'Al' AND u.name CONTAINS 'li' AND u.name ENDS WITH 'ce' RETURN u.name AS name ORDER BY name"
        )
        relationship_expression = cypherglot.compile_cypher_text(
            "MATCH (a:User)-[r:KNOWS]->(b:User) WHERE r.note IS NULL RETURN r.note AS note ORDER BY note"
        )
        optional_expression = cypherglot.compile_cypher_text(
            "OPTIONAL MATCH (u:User) WHERE u.name STARTS WITH 'Al' RETURN u.name AS name ORDER BY name"
        )

        self.assertEqual(
            expression.sql(),
            "SELECT JSON_EXTRACT(u.properties, '$.name') AS \"name\" FROM nodes AS u JOIN node_labels AS u_label_0 ON u_label_0.node_id = u.id AND u_label_0.label = 'User' WHERE SUBSTRING(JSON_EXTRACT(u.properties, '$.name'), 1, LENGTH('Al')) = 'Al' AND STR_POSITION(JSON_EXTRACT(u.properties, '$.name'), 'li') > 0 AND LENGTH(JSON_EXTRACT(u.properties, '$.name')) >= LENGTH('ce') AND SUBSTRING(JSON_EXTRACT(u.properties, '$.name'), LENGTH(JSON_EXTRACT(u.properties, '$.name')) - LENGTH('ce') + 1) = 'ce' ORDER BY JSON_EXTRACT(u.properties, '$.name') ASC",
        )
        self.assertEqual(
            relationship_expression.sql(),
            "SELECT JSON_EXTRACT(r.properties, '$.note') AS \"note\" FROM edges AS r JOIN nodes AS a ON a.id = r.from_id JOIN nodes AS b ON b.id = r.to_id JOIN node_labels AS a_label_0 ON a_label_0.node_id = a.id AND a_label_0.label = 'User' JOIN node_labels AS b_label_1 ON b_label_1.node_id = b.id AND b_label_1.label = 'User' WHERE r.type = 'KNOWS' AND (JSON_TYPE(r.properties, '$.note') IS NULL OR JSON_TYPE(r.properties, '$.note') = 'null') ORDER BY JSON_EXTRACT(r.properties, '$.note') ASC",
        )
        self.assertEqual(
            optional_expression.sql(),
            "SELECT JSON_EXTRACT(u.properties, '$.name') AS \"name\" FROM (SELECT 1 AS __cg_seed) AS seed LEFT JOIN nodes AS u ON 1 = 1 AND EXISTS(SELECT 1 FROM node_labels AS u_label_filter WHERE u_label_filter.node_id = u.id AND u_label_filter.label = 'User') AND SUBSTRING(JSON_EXTRACT(u.properties, '$.name'), 1, LENGTH('Al')) = 'Al' ORDER BY JSON_EXTRACT(u.properties, '$.name') ASC",
        )

    def test_compile_match_where_size_filters(self) -> None:
        expression = cypherglot.compile_cypher_text(
            "MATCH (u:User) WHERE size(u.name) >= 3 RETURN u.name AS name ORDER BY name"
        )
        relationship_expression = cypherglot.compile_cypher_text(
            "MATCH (a:User)-[r:KNOWS]->(b:User) WHERE size(r.note) IS NOT NULL RETURN r.note AS note ORDER BY note"
        )
        optional_expression = cypherglot.compile_cypher_text(
            "OPTIONAL MATCH (u:User) WHERE size(u.name) >= 3 RETURN u.name AS name ORDER BY name"
        )

        self.assertEqual(
            expression.sql(),
            "SELECT JSON_EXTRACT(u.properties, '$.name') AS \"name\" FROM nodes AS u JOIN node_labels AS u_label_0 ON u_label_0.node_id = u.id AND u_label_0.label = 'User' WHERE LENGTH(JSON_EXTRACT(u.properties, '$.name')) >= 3 ORDER BY JSON_EXTRACT(u.properties, '$.name') ASC",
        )
        self.assertEqual(
            relationship_expression.sql(),
            "SELECT JSON_EXTRACT(r.properties, '$.note') AS \"note\" FROM edges AS r JOIN nodes AS a ON a.id = r.from_id JOIN nodes AS b ON b.id = r.to_id JOIN node_labels AS a_label_0 ON a_label_0.node_id = a.id AND a_label_0.label = 'User' JOIN node_labels AS b_label_1 ON b_label_1.node_id = b.id AND b_label_1.label = 'User' WHERE r.type = 'KNOWS' AND NOT LENGTH(JSON_EXTRACT(r.properties, '$.note')) IS NULL ORDER BY JSON_EXTRACT(r.properties, '$.note') ASC",
        )
        self.assertEqual(
            optional_expression.sql(),
            "SELECT JSON_EXTRACT(u.properties, '$.name') AS \"name\" FROM (SELECT 1 AS __cg_seed) AS seed LEFT JOIN nodes AS u ON 1 = 1 AND EXISTS(SELECT 1 FROM node_labels AS u_label_filter WHERE u_label_filter.node_id = u.id AND u_label_filter.label = 'User') AND LENGTH(JSON_EXTRACT(u.properties, '$.name')) >= 3 ORDER BY JSON_EXTRACT(u.properties, '$.name') ASC",
        )

    def test_compile_match_null_predicate_returns(self) -> None:
        expression = cypherglot.compile_cypher_text(
            "MATCH (u:User) RETURN u.name IS NULL AS missing_name, size(u.name) IS NOT NULL AS has_len ORDER BY missing_name, has_len"
        )
        relationship_expression = cypherglot.compile_cypher_text(
            "MATCH (a:User)-[r:KNOWS]->(b:User) RETURN r.note IS NULL AS missing_note, size(r.note) IS NOT NULL AS has_len ORDER BY missing_note, has_len"
        )
        optional_expression = cypherglot.compile_cypher_text(
            "OPTIONAL MATCH (u:User) RETURN u.name IS NULL AS missing_name ORDER BY missing_name"
        )

        self.assertEqual(
            expression.sql(),
            "SELECT (JSON_TYPE(u.properties, '$.name') IS NULL OR JSON_TYPE(u.properties, '$.name') = 'null') AS \"missing_name\", NOT LENGTH(JSON_EXTRACT(u.properties, '$.name')) IS NULL AS \"has_len\" FROM nodes AS u JOIN node_labels AS u_label_0 ON u_label_0.node_id = u.id AND u_label_0.label = 'User' ORDER BY (JSON_TYPE(u.properties, '$.name') IS NULL OR JSON_TYPE(u.properties, '$.name') = 'null') ASC, NOT LENGTH(JSON_EXTRACT(u.properties, '$.name')) IS NULL ASC",
        )
        self.assertEqual(
            relationship_expression.sql(),
            "SELECT (JSON_TYPE(r.properties, '$.note') IS NULL OR JSON_TYPE(r.properties, '$.note') = 'null') AS \"missing_note\", NOT LENGTH(JSON_EXTRACT(r.properties, '$.note')) IS NULL AS \"has_len\" FROM edges AS r JOIN nodes AS a ON a.id = r.from_id JOIN nodes AS b ON b.id = r.to_id JOIN node_labels AS a_label_0 ON a_label_0.node_id = a.id AND a_label_0.label = 'User' JOIN node_labels AS b_label_1 ON b_label_1.node_id = b.id AND b_label_1.label = 'User' WHERE r.type = 'KNOWS' ORDER BY (JSON_TYPE(r.properties, '$.note') IS NULL OR JSON_TYPE(r.properties, '$.note') = 'null') ASC, NOT LENGTH(JSON_EXTRACT(r.properties, '$.note')) IS NULL ASC",
        )
        self.assertEqual(
            optional_expression.sql(),
            "SELECT (JSON_TYPE(u.properties, '$.name') IS NULL OR JSON_TYPE(u.properties, '$.name') = 'null') AS \"missing_name\" FROM (SELECT 1 AS __cg_seed) AS seed LEFT JOIN nodes AS u ON 1 = 1 AND EXISTS(SELECT 1 FROM node_labels AS u_label_filter WHERE u_label_filter.node_id = u.id AND u_label_filter.label = 'User') ORDER BY (JSON_TYPE(u.properties, '$.name') IS NULL OR JSON_TYPE(u.properties, '$.name') = 'null') ASC",
        )

    def test_compile_match_node_with_id_and_type_returns(self) -> None:
        id_expression = cypherglot.compile_cypher_text(
            "MATCH (u:User) RETURN id(u) AS uid ORDER BY uid"
        )
        type_expression = cypherglot.compile_cypher_text(
            "MATCH (a:User)-[r:KNOWS]->(b:User) RETURN type(r) AS rel_type ORDER BY rel_type"
        )

        self.assertEqual(
            id_expression.sql(),
            "SELECT u.id AS \"uid\" FROM nodes AS u JOIN node_labels AS u_label_0 ON u_label_0.node_id = u.id AND u_label_0.label = 'User' ORDER BY u.id ASC",
        )
        self.assertEqual(
            type_expression.sql(),
            "SELECT r.type AS \"rel_type\" FROM edges AS r JOIN nodes AS a ON a.id = r.from_id JOIN nodes AS b ON b.id = r.to_id JOIN node_labels AS a_label_0 ON a_label_0.node_id = a.id AND a_label_0.label = 'User' JOIN node_labels AS b_label_1 ON b_label_1.node_id = b.id AND b_label_1.label = 'User' WHERE r.type = 'KNOWS' ORDER BY r.type ASC",
        )

    def test_compile_match_relationship_with_grouped_plain_read_count(self) -> None:
        expression = cypherglot.compile_cypher_text(
            "MATCH (a:User)-[r:KNOWS]->(b:User) RETURN a.name AS name, count(r) AS total ORDER BY total DESC"
        )

        self.assertEqual(
            expression.sql(),
            "SELECT JSON_EXTRACT(a.properties, '$.name') AS \"name\", COUNT(r.id) AS \"total\" "
            "FROM edges AS r JOIN nodes AS a ON a.id = r.from_id JOIN nodes AS b ON b.id = r.to_id JOIN node_labels AS a_label_0 ON a_label_0.node_id = a.id AND a_label_0.label = 'User' JOIN node_labels AS b_label_1 ON b_label_1.node_id = b.id AND b_label_1.label = 'User' "
            "WHERE r.type = 'KNOWS' GROUP BY JSON_EXTRACT(a.properties, '$.name') ORDER BY \"total\" DESC",
        )

    def test_compile_optional_match_node_with_aliased_return(self) -> None:
        expression = cypherglot.compile_cypher_text(
            "OPTIONAL MATCH (u:User) RETURN u.name AS name ORDER BY name LIMIT 1"
        )

        self.assertEqual(
            expression.sql(),
            "SELECT JSON_EXTRACT(u.properties, '$.name') AS \"name\" "
            "FROM (SELECT 1 AS __cg_seed) AS seed "
            "LEFT JOIN nodes AS u ON 1 = 1 AND EXISTS(SELECT 1 FROM node_labels AS u_label_filter WHERE u_label_filter.node_id = u.id AND u_label_filter.label = 'User') "
            "ORDER BY JSON_EXTRACT(u.properties, '$.name') ASC LIMIT 1",
        )

    def test_compile_optional_match_node_with_entity_return(self) -> None:
        expression = cypherglot.compile_cypher_text(
            "OPTIONAL MATCH (u:User) RETURN u AS user"
        )
        ordered_expression = cypherglot.compile_cypher_text(
            "OPTIONAL MATCH (u:User) RETURN u AS user ORDER BY user"
        )

        self.assertEqual(
            expression.sql(),
            "SELECT JSON_OBJECT('id': u.id, 'label': (SELECT u_label_return.label FROM node_labels AS u_label_return WHERE u_label_return.node_id = u.id LIMIT 1), 'properties': JSON(COALESCE(u.properties, '{}'))) AS \"user\" "
            "FROM (SELECT 1 AS __cg_seed) AS seed "
            "LEFT JOIN nodes AS u ON 1 = 1 AND EXISTS(SELECT 1 FROM node_labels AS u_label_filter WHERE u_label_filter.node_id = u.id AND u_label_filter.label = 'User')",
        )
        self.assertEqual(
            ordered_expression.sql(),
            "SELECT JSON_OBJECT('id': u.id, 'label': (SELECT u_label_return.label FROM node_labels AS u_label_return WHERE u_label_return.node_id = u.id LIMIT 1), 'properties': JSON(COALESCE(u.properties, '{}'))) AS \"user\" "
            "FROM (SELECT 1 AS __cg_seed) AS seed "
            "LEFT JOIN nodes AS u ON 1 = 1 AND EXISTS(SELECT 1 FROM node_labels AS u_label_filter WHERE u_label_filter.node_id = u.id AND u_label_filter.label = 'User') "
            "ORDER BY JSON_OBJECT('id': u.id, 'label': (SELECT u_label_return.label FROM node_labels AS u_label_return WHERE u_label_return.node_id = u.id LIMIT 1), 'properties': JSON(COALESCE(u.properties, '{}'))) ASC",
        )

    def test_compile_optional_match_node_with_grouped_count(self) -> None:
        expression = cypherglot.compile_cypher_text(
            "OPTIONAL MATCH (u:User) RETURN u.name AS name, count(u) AS total ORDER BY total DESC"
        )
        star_expression = cypherglot.compile_cypher_text(
            "OPTIONAL MATCH (u:User) RETURN u.name AS name, count(*) AS total ORDER BY total DESC"
        )

        self.assertEqual(
            expression.sql(),
            "SELECT JSON_EXTRACT(u.properties, '$.name') AS \"name\", COUNT(u.id) AS \"total\" "
            "FROM (SELECT 1 AS __cg_seed) AS seed "
            "LEFT JOIN nodes AS u ON 1 = 1 AND EXISTS(SELECT 1 FROM node_labels AS u_label_filter WHERE u_label_filter.node_id = u.id AND u_label_filter.label = 'User') "
            "GROUP BY JSON_EXTRACT(u.properties, '$.name') ORDER BY \"total\" DESC",
        )
        self.assertEqual(
            star_expression.sql(),
            "SELECT JSON_EXTRACT(u.properties, '$.name') AS \"name\", COUNT(*) AS \"total\" "
            "FROM (SELECT 1 AS __cg_seed) AS seed "
            "LEFT JOIN nodes AS u ON 1 = 1 AND EXISTS(SELECT 1 FROM node_labels AS u_label_filter WHERE u_label_filter.node_id = u.id AND u_label_filter.label = 'User') "
            "GROUP BY JSON_EXTRACT(u.properties, '$.name') ORDER BY \"total\" DESC",
        )

    def test_compile_optional_match_node_with_grouped_entity_count(self) -> None:
        expression = cypherglot.compile_cypher_text(
            "OPTIONAL MATCH (u:User) RETURN u AS user, count(u) AS total ORDER BY total DESC"
        )

        self.assertEqual(
            expression.sql(),
            "SELECT JSON_OBJECT('id': u.id, 'label': (SELECT u_label_return.label FROM node_labels AS u_label_return WHERE u_label_return.node_id = u.id LIMIT 1), 'properties': JSON(COALESCE(u.properties, '{}'))) AS \"user\", COUNT(u.id) AS \"total\" "
            "FROM (SELECT 1 AS __cg_seed) AS seed "
            "LEFT JOIN nodes AS u ON 1 = 1 AND EXISTS(SELECT 1 FROM node_labels AS u_label_filter WHERE u_label_filter.node_id = u.id AND u_label_filter.label = 'User') "
            "GROUP BY JSON_OBJECT('id': u.id, 'label': (SELECT u_label_return.label FROM node_labels AS u_label_return WHERE u_label_return.node_id = u.id LIMIT 1), 'properties': JSON(COALESCE(u.properties, '{}'))) ORDER BY \"total\" DESC",
        )

    def test_compile_optional_match_node_with_scalar_literal_and_parameter_returns(
        self,
    ) -> None:
        literal_expression = cypherglot.compile_cypher_text(
            "OPTIONAL MATCH (u:User) RETURN 'tag' AS tag ORDER BY tag"
        )
        parameter_expression = cypherglot.compile_cypher_text(
            "OPTIONAL MATCH (u:User) RETURN $value AS value ORDER BY value"
        )

        self.assertEqual(
            literal_expression.sql(),
            "SELECT 'tag' AS \"tag\" FROM (SELECT 1 AS __cg_seed) AS seed LEFT JOIN nodes AS u ON 1 = 1 AND EXISTS(SELECT 1 FROM node_labels AS u_label_filter WHERE u_label_filter.node_id = u.id AND u_label_filter.label = 'User') ORDER BY 'tag' ASC",
        )
        self.assertEqual(
            parameter_expression.sql(),
            "SELECT :value AS \"value\" FROM (SELECT 1 AS __cg_seed) AS seed LEFT JOIN nodes AS u ON 1 = 1 AND EXISTS(SELECT 1 FROM node_labels AS u_label_filter WHERE u_label_filter.node_id = u.id AND u_label_filter.label = 'User') ORDER BY :value ASC",
        )

    def test_compile_optional_match_node_with_size_return(self) -> None:
        expression = cypherglot.compile_cypher_text(
            "OPTIONAL MATCH (u:User) RETURN size(u.name) AS name_len ORDER BY name_len"
        )
        id_expression = cypherglot.compile_cypher_text(
            "OPTIONAL MATCH (u:User) RETURN size(id(u)) AS id_len ORDER BY id_len"
        )

        self.assertEqual(
            expression.sql(),
            "SELECT LENGTH(JSON_EXTRACT(u.properties, '$.name')) AS \"name_len\" FROM (SELECT 1 AS __cg_seed) AS seed LEFT JOIN nodes AS u ON 1 = 1 AND EXISTS(SELECT 1 FROM node_labels AS u_label_filter WHERE u_label_filter.node_id = u.id AND u_label_filter.label = 'User') ORDER BY LENGTH(JSON_EXTRACT(u.properties, '$.name')) ASC",
        )
        self.assertEqual(
            id_expression.sql(),
            "SELECT LENGTH(u.id) AS \"id_len\" FROM (SELECT 1 AS __cg_seed) AS seed LEFT JOIN nodes AS u ON 1 = 1 AND EXISTS(SELECT 1 FROM node_labels AS u_label_filter WHERE u_label_filter.node_id = u.id AND u_label_filter.label = 'User') ORDER BY LENGTH(u.id) ASC",
        )

    def test_compile_optional_match_node_with_lower_return(self) -> None:
        expression = cypherglot.compile_cypher_text(
            "OPTIONAL MATCH (u:User) RETURN lower(u.name) AS lower_name ORDER BY lower_name"
        )
        trim_expression = cypherglot.compile_cypher_text(
            "OPTIONAL MATCH (u:User) RETURN trim(u.name) AS trimmed ORDER BY trimmed"
        )
        ltrim_expression = cypherglot.compile_cypher_text(
            "OPTIONAL MATCH (u:User) RETURN ltrim(u.name) AS left_trimmed ORDER BY left_trimmed"
        )
        rtrim_expression = cypherglot.compile_cypher_text(
            "OPTIONAL MATCH (u:User) RETURN rtrim(u.name) AS right_trimmed ORDER BY right_trimmed"
        )
        reverse_expression = cypherglot.compile_cypher_text(
            "OPTIONAL MATCH (u:User) RETURN reverse(u.name) AS reversed_name ORDER BY reversed_name"
        )

        self.assertEqual(
            expression.sql(),
            "SELECT LOWER(JSON_EXTRACT(u.properties, '$.name')) AS \"lower_name\" FROM (SELECT 1 AS __cg_seed) AS seed LEFT JOIN nodes AS u ON 1 = 1 AND EXISTS(SELECT 1 FROM node_labels AS u_label_filter WHERE u_label_filter.node_id = u.id AND u_label_filter.label = 'User') ORDER BY LOWER(JSON_EXTRACT(u.properties, '$.name')) ASC",
        )
        self.assertEqual(
            trim_expression.sql(),
            "SELECT TRIM(JSON_EXTRACT(u.properties, '$.name')) AS \"trimmed\" FROM (SELECT 1 AS __cg_seed) AS seed LEFT JOIN nodes AS u ON 1 = 1 AND EXISTS(SELECT 1 FROM node_labels AS u_label_filter WHERE u_label_filter.node_id = u.id AND u_label_filter.label = 'User') ORDER BY TRIM(JSON_EXTRACT(u.properties, '$.name')) ASC",
        )
        self.assertEqual(
            ltrim_expression.sql(),
            "SELECT LTRIM(JSON_EXTRACT(u.properties, '$.name')) AS \"left_trimmed\" FROM (SELECT 1 AS __cg_seed) AS seed LEFT JOIN nodes AS u ON 1 = 1 AND EXISTS(SELECT 1 FROM node_labels AS u_label_filter WHERE u_label_filter.node_id = u.id AND u_label_filter.label = 'User') ORDER BY LTRIM(JSON_EXTRACT(u.properties, '$.name')) ASC",
        )
        self.assertEqual(
            rtrim_expression.sql(),
            "SELECT RTRIM(JSON_EXTRACT(u.properties, '$.name')) AS \"right_trimmed\" FROM (SELECT 1 AS __cg_seed) AS seed LEFT JOIN nodes AS u ON 1 = 1 AND EXISTS(SELECT 1 FROM node_labels AS u_label_filter WHERE u_label_filter.node_id = u.id AND u_label_filter.label = 'User') ORDER BY RTRIM(JSON_EXTRACT(u.properties, '$.name')) ASC",
        )
        self.assertEqual(
            reverse_expression.sql(),
            "SELECT REVERSE(JSON_EXTRACT(u.properties, '$.name')) AS \"reversed_name\" FROM (SELECT 1 AS __cg_seed) AS seed LEFT JOIN nodes AS u ON 1 = 1 AND EXISTS(SELECT 1 FROM node_labels AS u_label_filter WHERE u_label_filter.node_id = u.id AND u_label_filter.label = 'User') ORDER BY REVERSE(JSON_EXTRACT(u.properties, '$.name')) ASC",
        )

    def test_compile_optional_match_node_with_coalesce_return(self) -> None:
        expression = cypherglot.compile_cypher_text(
            "OPTIONAL MATCH (u:User) RETURN coalesce(u.name, $fallback) AS display_name ORDER BY display_name"
        )

        self.assertEqual(
            expression.sql(),
            "SELECT COALESCE(JSON_EXTRACT(u.properties, '$.name'), :fallback) AS \"display_name\" FROM (SELECT 1 AS __cg_seed) AS seed LEFT JOIN nodes AS u ON 1 = 1 AND EXISTS(SELECT 1 FROM node_labels AS u_label_filter WHERE u_label_filter.node_id = u.id AND u_label_filter.label = 'User') ORDER BY COALESCE(JSON_EXTRACT(u.properties, '$.name'), :fallback) ASC",
        )

    def test_compile_optional_match_node_with_replace_return(self) -> None:
        expression = cypherglot.compile_cypher_text(
            "OPTIONAL MATCH (u:User) RETURN replace(u.name, $needle, $replacement) AS display_name ORDER BY display_name"
        )

        self.assertEqual(
            expression.sql(),
            "SELECT REPLACE(JSON_EXTRACT(u.properties, '$.name'), :needle, :replacement) AS \"display_name\" FROM (SELECT 1 AS __cg_seed) AS seed LEFT JOIN nodes AS u ON 1 = 1 AND EXISTS(SELECT 1 FROM node_labels AS u_label_filter WHERE u_label_filter.node_id = u.id AND u_label_filter.label = 'User') ORDER BY REPLACE(JSON_EXTRACT(u.properties, '$.name'), :needle, :replacement) ASC",
        )

    def test_compile_optional_match_node_with_left_and_right_return(self) -> None:
        expression = cypherglot.compile_cypher_text(
            "OPTIONAL MATCH (u:User) RETURN right(u.name, $count) AS suffix ORDER BY suffix"
        )

        self.assertEqual(
            expression.sql(),
            "SELECT RIGHT(JSON_EXTRACT(u.properties, '$.name'), :count) AS \"suffix\" FROM (SELECT 1 AS __cg_seed) AS seed LEFT JOIN nodes AS u ON 1 = 1 AND EXISTS(SELECT 1 FROM node_labels AS u_label_filter WHERE u_label_filter.node_id = u.id AND u_label_filter.label = 'User') ORDER BY RIGHT(JSON_EXTRACT(u.properties, '$.name'), :count) ASC",
        )

    def test_compile_optional_match_node_with_split_return(self) -> None:
        expression = cypherglot.compile_cypher_text(
            "OPTIONAL MATCH (u:User) RETURN split(u.name, $delimiter) AS parts ORDER BY parts"
        )

        self.assertEqual(
            expression.sql(),
            "SELECT SPLIT(JSON_EXTRACT(u.properties, '$.name'), :delimiter) AS \"parts\" FROM (SELECT 1 AS __cg_seed) AS seed LEFT JOIN nodes AS u ON 1 = 1 AND EXISTS(SELECT 1 FROM node_labels AS u_label_filter WHERE u_label_filter.node_id = u.id AND u_label_filter.label = 'User') ORDER BY SPLIT(JSON_EXTRACT(u.properties, '$.name'), :delimiter) ASC",
        )

    def test_compile_optional_match_node_with_abs_return(self) -> None:
        expression = cypherglot.compile_cypher_text(
            "OPTIONAL MATCH (u:User) RETURN abs(u.age) AS magnitude ORDER BY magnitude"
        )

        self.assertEqual(
            expression.sql(),
            "SELECT ABS(JSON_EXTRACT(u.properties, '$.age')) AS \"magnitude\" FROM (SELECT 1 AS __cg_seed) AS seed LEFT JOIN nodes AS u ON 1 = 1 AND EXISTS(SELECT 1 FROM node_labels AS u_label_filter WHERE u_label_filter.node_id = u.id AND u_label_filter.label = 'User') ORDER BY ABS(JSON_EXTRACT(u.properties, '$.age')) ASC",
        )

    def test_compile_optional_match_node_with_sign_return(self) -> None:
        expression = cypherglot.compile_cypher_text(
            "OPTIONAL MATCH (u:User) RETURN sign(u.age) AS age_sign ORDER BY age_sign"
        )

        self.assertEqual(
            expression.sql(),
            "SELECT SIGN(JSON_EXTRACT(u.properties, '$.age')) AS \"age_sign\" FROM (SELECT 1 AS __cg_seed) AS seed LEFT JOIN nodes AS u ON 1 = 1 AND EXISTS(SELECT 1 FROM node_labels AS u_label_filter WHERE u_label_filter.node_id = u.id AND u_label_filter.label = 'User') ORDER BY SIGN(JSON_EXTRACT(u.properties, '$.age')) ASC",
        )

    def test_compile_optional_match_node_with_round_return(self) -> None:
        expression = cypherglot.compile_cypher_text(
            "OPTIONAL MATCH (u:User) RETURN round(u.score) AS value ORDER BY value"
        )

        self.assertEqual(
            expression.sql(),
            "SELECT ROUND(JSON_EXTRACT(u.properties, '$.score')) AS \"value\" FROM (SELECT 1 AS __cg_seed) AS seed LEFT JOIN nodes AS u ON 1 = 1 AND EXISTS(SELECT 1 FROM node_labels AS u_label_filter WHERE u_label_filter.node_id = u.id AND u_label_filter.label = 'User') ORDER BY ROUND(JSON_EXTRACT(u.properties, '$.score')) ASC",
        )

    def test_compile_optional_match_node_with_ceil_return(self) -> None:
        expression = cypherglot.compile_cypher_text(
            "OPTIONAL MATCH (u:User) RETURN ceil(u.score) AS value ORDER BY value"
        )

        self.assertEqual(
            expression.sql(),
            "SELECT CEIL(JSON_EXTRACT(u.properties, '$.score')) AS \"value\" FROM (SELECT 1 AS __cg_seed) AS seed LEFT JOIN nodes AS u ON 1 = 1 AND EXISTS(SELECT 1 FROM node_labels AS u_label_filter WHERE u_label_filter.node_id = u.id AND u_label_filter.label = 'User') ORDER BY CEIL(JSON_EXTRACT(u.properties, '$.score')) ASC",
        )

    def test_compile_optional_match_node_with_floor_return(self) -> None:
        expression = cypherglot.compile_cypher_text(
            "OPTIONAL MATCH (u:User) RETURN floor(u.score) AS value ORDER BY value"
        )

        self.assertEqual(
            expression.sql(),
            "SELECT FLOOR(JSON_EXTRACT(u.properties, '$.score')) AS \"value\" FROM (SELECT 1 AS __cg_seed) AS seed LEFT JOIN nodes AS u ON 1 = 1 AND EXISTS(SELECT 1 FROM node_labels AS u_label_filter WHERE u_label_filter.node_id = u.id AND u_label_filter.label = 'User') ORDER BY FLOOR(JSON_EXTRACT(u.properties, '$.score')) ASC",
        )

    def test_compile_optional_match_node_with_sqrt_return(self) -> None:
        expression = cypherglot.compile_cypher_text(
            "OPTIONAL MATCH (u:User) RETURN sqrt(u.score) AS value ORDER BY value"
        )

        self.assertEqual(
            expression.sql(),
            "SELECT SQRT(JSON_EXTRACT(u.properties, '$.score')) AS \"value\" FROM (SELECT 1 AS __cg_seed) AS seed LEFT JOIN nodes AS u ON 1 = 1 AND EXISTS(SELECT 1 FROM node_labels AS u_label_filter WHERE u_label_filter.node_id = u.id AND u_label_filter.label = 'User') ORDER BY SQRT(JSON_EXTRACT(u.properties, '$.score')) ASC",
        )

    def test_compile_optional_match_node_with_exp_return(self) -> None:
        expression = cypherglot.compile_cypher_text(
            "OPTIONAL MATCH (u:User) RETURN exp(u.score) AS value ORDER BY value"
        )

        self.assertEqual(
            expression.sql(),
            "SELECT EXP(JSON_EXTRACT(u.properties, '$.score')) AS \"value\" FROM (SELECT 1 AS __cg_seed) AS seed LEFT JOIN nodes AS u ON 1 = 1 AND EXISTS(SELECT 1 FROM node_labels AS u_label_filter WHERE u_label_filter.node_id = u.id AND u_label_filter.label = 'User') ORDER BY EXP(JSON_EXTRACT(u.properties, '$.score')) ASC",
        )

    def test_compile_optional_match_node_with_to_string_return(self) -> None:
        expression = cypherglot.compile_cypher_text(
            "OPTIONAL MATCH (u:User) RETURN toString(u.age) AS text ORDER BY text"
        )

        self.assertEqual(
            expression.sql(),
            "SELECT CAST(JSON_EXTRACT(u.properties, '$.age') AS TEXT) AS \"text\" FROM (SELECT 1 AS __cg_seed) AS seed LEFT JOIN nodes AS u ON 1 = 1 AND EXISTS(SELECT 1 FROM node_labels AS u_label_filter WHERE u_label_filter.node_id = u.id AND u_label_filter.label = 'User') ORDER BY CAST(JSON_EXTRACT(u.properties, '$.age') AS TEXT) ASC",
        )

    def test_compile_optional_match_node_with_to_integer_return(self) -> None:
        expression = cypherglot.compile_cypher_text(
            "OPTIONAL MATCH (u:User) RETURN toInteger(u.age) AS age_int ORDER BY age_int"
        )

        self.assertEqual(
            expression.sql(),
            "SELECT CAST(JSON_EXTRACT(u.properties, '$.age') AS INT) AS \"age_int\" FROM (SELECT 1 AS __cg_seed) AS seed LEFT JOIN nodes AS u ON 1 = 1 AND EXISTS(SELECT 1 FROM node_labels AS u_label_filter WHERE u_label_filter.node_id = u.id AND u_label_filter.label = 'User') ORDER BY CAST(JSON_EXTRACT(u.properties, '$.age') AS INT) ASC",
        )

    def test_compile_optional_match_node_with_to_float_return(self) -> None:
        expression = cypherglot.compile_cypher_text(
            "OPTIONAL MATCH (u:User) RETURN toFloat(u.age) AS age_float ORDER BY age_float"
        )

        self.assertEqual(
            expression.sql(),
            "SELECT CAST(JSON_EXTRACT(u.properties, '$.age') AS FLOAT) AS \"age_float\" FROM (SELECT 1 AS __cg_seed) AS seed LEFT JOIN nodes AS u ON 1 = 1 AND EXISTS(SELECT 1 FROM node_labels AS u_label_filter WHERE u_label_filter.node_id = u.id AND u_label_filter.label = 'User') ORDER BY CAST(JSON_EXTRACT(u.properties, '$.age') AS FLOAT) ASC",
        )

    def test_compile_optional_match_node_with_to_boolean_return(self) -> None:
        expression = cypherglot.compile_cypher_text(
            "OPTIONAL MATCH (u:User) RETURN toBoolean(u.active) AS is_active ORDER BY is_active"
        )

        self.assertEqual(
            expression.sql(),
            "SELECT CAST(JSON_EXTRACT(u.properties, '$.active') AS BOOLEAN) AS \"is_active\" FROM (SELECT 1 AS __cg_seed) AS seed LEFT JOIN nodes AS u ON 1 = 1 AND EXISTS(SELECT 1 FROM node_labels AS u_label_filter WHERE u_label_filter.node_id = u.id AND u_label_filter.label = 'User') ORDER BY CAST(JSON_EXTRACT(u.properties, '$.active') AS BOOLEAN) ASC",
        )

    def test_compile_optional_match_node_with_substring_return(self) -> None:
        expression = cypherglot.compile_cypher_text(
            "OPTIONAL MATCH (u:User) RETURN substring(u.name, 0, 2) AS prefix ORDER BY prefix"
        )
        two_arg_expression = cypherglot.compile_cypher_text(
            "OPTIONAL MATCH (u:User) RETURN substring(u.name, 1) AS suffix ORDER BY suffix"
        )

        self.assertEqual(
            expression.sql(),
            "SELECT SUBSTRING(JSON_EXTRACT(u.properties, '$.name'), (0 + 1), 2) AS \"prefix\" FROM (SELECT 1 AS __cg_seed) AS seed LEFT JOIN nodes AS u ON 1 = 1 AND EXISTS(SELECT 1 FROM node_labels AS u_label_filter WHERE u_label_filter.node_id = u.id AND u_label_filter.label = 'User') ORDER BY SUBSTRING(JSON_EXTRACT(u.properties, '$.name'), (0 + 1), 2) ASC",
        )
        self.assertEqual(
            two_arg_expression.sql(),
            "SELECT SUBSTRING(JSON_EXTRACT(u.properties, '$.name'), (1 + 1)) AS \"suffix\" FROM (SELECT 1 AS __cg_seed) AS seed LEFT JOIN nodes AS u ON 1 = 1 AND EXISTS(SELECT 1 FROM node_labels AS u_label_filter WHERE u_label_filter.node_id = u.id AND u_label_filter.label = 'User') ORDER BY SUBSTRING(JSON_EXTRACT(u.properties, '$.name'), (1 + 1)) ASC",
        )

    def test_compile_optional_match_node_with_predicate_return(self) -> None:
        expression = cypherglot.compile_cypher_text(
            "OPTIONAL MATCH (u:User) RETURN u.age >= 18 AS adult ORDER BY adult"
        )
        contains_expression = cypherglot.compile_cypher_text(
            "OPTIONAL MATCH (u:User) RETURN u.name CONTAINS 'a' AS has_a ORDER BY has_a"
        )
        starts_with_expression = cypherglot.compile_cypher_text(
            "OPTIONAL MATCH (u:User) RETURN u.name STARTS WITH 'Al' AS has_prefix ORDER BY has_prefix"
        )
        ends_with_expression = cypherglot.compile_cypher_text(
            "OPTIONAL MATCH (u:User) RETURN u.name ENDS WITH 'ce' AS has_suffix ORDER BY has_suffix"
        )
        id_expression = cypherglot.compile_cypher_text(
            "OPTIONAL MATCH (u:User) RETURN id(u) >= 1 AS has_id ORDER BY has_id"
        )

        self.assertEqual(
            expression.sql(),
            "SELECT JSON_EXTRACT(u.properties, '$.age') >= 18 AS \"adult\" FROM (SELECT 1 AS __cg_seed) AS seed LEFT JOIN nodes AS u ON 1 = 1 AND EXISTS(SELECT 1 FROM node_labels AS u_label_filter WHERE u_label_filter.node_id = u.id AND u_label_filter.label = 'User') ORDER BY JSON_EXTRACT(u.properties, '$.age') >= 18 ASC",
        )
        self.assertEqual(
            contains_expression.sql(),
            "SELECT STR_POSITION(JSON_EXTRACT(u.properties, '$.name'), 'a') > 0 AS \"has_a\" FROM (SELECT 1 AS __cg_seed) AS seed LEFT JOIN nodes AS u ON 1 = 1 AND EXISTS(SELECT 1 FROM node_labels AS u_label_filter WHERE u_label_filter.node_id = u.id AND u_label_filter.label = 'User') ORDER BY STR_POSITION(JSON_EXTRACT(u.properties, '$.name'), 'a') > 0 ASC",
        )
        self.assertEqual(
            starts_with_expression.sql(),
            "SELECT SUBSTRING(JSON_EXTRACT(u.properties, '$.name'), 1, LENGTH('Al')) = 'Al' AS \"has_prefix\" FROM (SELECT 1 AS __cg_seed) AS seed LEFT JOIN nodes AS u ON 1 = 1 AND EXISTS(SELECT 1 FROM node_labels AS u_label_filter WHERE u_label_filter.node_id = u.id AND u_label_filter.label = 'User') ORDER BY SUBSTRING(JSON_EXTRACT(u.properties, '$.name'), 1, LENGTH('Al')) = 'Al' ASC",
        )
        self.assertEqual(
            ends_with_expression.sql(),
            "SELECT LENGTH(JSON_EXTRACT(u.properties, '$.name')) >= LENGTH('ce') AND SUBSTRING(JSON_EXTRACT(u.properties, '$.name'), LENGTH(JSON_EXTRACT(u.properties, '$.name')) - LENGTH('ce') + 1) = 'ce' AS \"has_suffix\" FROM (SELECT 1 AS __cg_seed) AS seed LEFT JOIN nodes AS u ON 1 = 1 AND EXISTS(SELECT 1 FROM node_labels AS u_label_filter WHERE u_label_filter.node_id = u.id AND u_label_filter.label = 'User') ORDER BY LENGTH(JSON_EXTRACT(u.properties, '$.name')) >= LENGTH('ce') AND SUBSTRING(JSON_EXTRACT(u.properties, '$.name'), LENGTH(JSON_EXTRACT(u.properties, '$.name')) - LENGTH('ce') + 1) = 'ce' ASC",
        )
        self.assertEqual(
            id_expression.sql(),
            "SELECT u.id >= 1 AS \"has_id\" FROM (SELECT 1 AS __cg_seed) AS seed LEFT JOIN nodes AS u ON 1 = 1 AND EXISTS(SELECT 1 FROM node_labels AS u_label_filter WHERE u_label_filter.node_id = u.id AND u_label_filter.label = 'User') ORDER BY u.id >= 1 ASC",
        )

    def test_compile_optional_match_node_with_id_return(self) -> None:
        expression = cypherglot.compile_cypher_text(
            "OPTIONAL MATCH (u:User) RETURN id(u) AS uid ORDER BY uid"
        )

        self.assertEqual(
            expression.sql(),
            "SELECT u.id AS \"uid\" FROM (SELECT 1 AS __cg_seed) AS seed LEFT JOIN nodes AS u ON 1 = 1 AND EXISTS(SELECT 1 FROM node_labels AS u_label_filter WHERE u_label_filter.node_id = u.id AND u_label_filter.label = 'User') ORDER BY u.id ASC",
        )

    def test_compile_rejects_multi_step_create_node_in_single_expression_api(
        self,
    ) -> None:
        with self.assertRaisesRegex(ValueError, "multi-step SQL program"):
            cypherglot.compile_cypher_text("CREATE (:User {name: 'Alice'})")

    def test_compile_program_create_node(self) -> None:
        program = cypherglot.compile_cypher_program_text(
            "CREATE (:User {name: 'Alice'})"
        )

        self.assertEqual(len(program.steps), 2)
        self.assertIsInstance(program.steps[0], cypherglot.CompiledCypherStatement)
        self.assertEqual(
            program.steps[0].sql.sql(),
            "INSERT INTO nodes (properties) VALUES "
            "(JSON_OBJECT('name': 'Alice')) RETURNING id",
        )
        self.assertEqual(program.steps[0].bind_columns, ("created_node_id",))
        self.assertEqual(
            program.steps[1].sql.sql(),
            "INSERT INTO node_labels (node_id, label) VALUES "
            "(:created_node_id, 'User')",
        )

    def test_compile_program_create_relationship(self) -> None:
        program = cypherglot.compile_cypher_program_text(
            "CREATE (a:User {name: 'Alice'})"
            "-[r:KNOWS {since: 2020}]->"
            "(b:User {name: 'Bob'})"
        )

        self.assertEqual(len(program.steps), 5)
        self.assertEqual(
            program.steps[0].sql.sql(),
            "INSERT INTO nodes (properties) VALUES "
            "(JSON_OBJECT('name': 'Alice')) RETURNING id",
        )
        self.assertEqual(program.steps[0].bind_columns, ("left_node_id",))
        self.assertEqual(
            program.steps[1].sql.sql(),
            "INSERT INTO node_labels (node_id, label) VALUES (:left_node_id, 'User')",
        )
        self.assertEqual(
            program.steps[2].sql.sql(),
            "INSERT INTO nodes (properties) VALUES "
            "(JSON_OBJECT('name': 'Bob')) RETURNING id",
        )
        self.assertEqual(program.steps[2].bind_columns, ("right_node_id",))
        self.assertEqual(
            program.steps[3].sql.sql(),
            "INSERT INTO node_labels (node_id, label) VALUES (:right_node_id, 'User')",
        )
        self.assertEqual(
            program.steps[4].sql.sql(),
            "INSERT INTO edges (type, from_id, to_id, properties) VALUES "
            "('KNOWS', :left_node_id, :right_node_id, "
            "JSON_OBJECT('since': 2020))",
        )

    def test_compile_program_create_relationship_self_loop(self) -> None:
        program = cypherglot.compile_cypher_program_text(
            "CREATE (root:Root)-[:LINK]->(root:Root)"
        )

        self.assertEqual(len(program.steps), 3)
        self.assertEqual(program.steps[0].bind_columns, ("left_node_id",))
        self.assertEqual(
            program.steps[2].sql.sql(),
            "INSERT INTO edges (type, from_id, to_id, properties) VALUES "
            "('LINK', :left_node_id, :left_node_id, '{}')",
        )

    def test_compile_program_create_relationship_from_separate_patterns(self) -> None:
        program = cypherglot.compile_cypher_program_text(
            "CREATE (a:A {name: 'Alice'}), (b:B {name: 'Bob'}), (a)-[:R]->(b)"
        )

        self.assertEqual(len(program.steps), 5)
        self.assertEqual(program.steps[0].bind_columns, ("first_node_id",))
        self.assertEqual(program.steps[2].bind_columns, ("second_node_id",))
        self.assertEqual(
            program.steps[4].sql.sql(),
            "INSERT INTO edges (type, from_id, to_id, properties) VALUES "
            "('R', :first_node_id, :second_node_id, '{}')",
        )

    def test_compile_match_node_to_sqlglot_ast(self) -> None:
        expression = cypherglot.compile_cypher_text(
            "MATCH (u:User) WHERE u.name = $name RETURN u.name ORDER BY u.name LIMIT 1"
        )

        self.assertEqual(
            expression.sql(),
            "SELECT JSON_EXTRACT(u.properties, '$.name') AS \"u.name\" "
            "FROM nodes AS u "
            "JOIN node_labels AS u_label_0 ON u_label_0.node_id = u.id "
            "AND u_label_0.label = 'User' "
            "WHERE JSON_EXTRACT(u.properties, '$.name') = :name "
            "ORDER BY JSON_EXTRACT(u.properties, '$.name') ASC "
            "LIMIT 1",
        )

    def test_compile_match_where_id_and_type_filters(self) -> None:
        id_expression = cypherglot.compile_cypher_text(
            "MATCH (u:User) WHERE id(u) >= 1 RETURN u.name ORDER BY u.name"
        )
        type_expression = cypherglot.compile_cypher_text(
            "MATCH (a:User)-[r:KNOWS]->(b:User) WHERE type(r) = 'KNOWS' RETURN b.name ORDER BY b.name"
        )

        self.assertEqual(
            id_expression.sql(),
            "SELECT JSON_EXTRACT(u.properties, '$.name') AS \"u.name\" FROM nodes AS u JOIN node_labels AS u_label_0 ON u_label_0.node_id = u.id AND u_label_0.label = 'User' WHERE u.id >= 1 ORDER BY JSON_EXTRACT(u.properties, '$.name') ASC",
        )
        self.assertEqual(
            type_expression.sql(),
            "SELECT JSON_EXTRACT(b.properties, '$.name') AS \"b.name\" FROM edges AS r JOIN nodes AS a ON a.id = r.from_id JOIN nodes AS b ON b.id = r.to_id JOIN node_labels AS a_label_0 ON a_label_0.node_id = a.id AND a_label_0.label = 'User' JOIN node_labels AS b_label_1 ON b_label_1.node_id = b.id AND b_label_1.label = 'User' WHERE r.type = 'KNOWS' AND r.type = 'KNOWS' ORDER BY JSON_EXTRACT(b.properties, '$.name') ASC",
        )

    def test_compile_match_node_distinct_offset(self) -> None:
        expression = cypherglot.compile_cypher_text(
            "MATCH (u:User) RETURN DISTINCT u.name ORDER BY u.name SKIP 1 LIMIT 2"
        )

        self.assertEqual(
            expression.sql(),
            "SELECT DISTINCT JSON_EXTRACT(u.properties, '$.name') AS \"u.name\" "
            "FROM nodes AS u "
            "JOIN node_labels AS u_label_0 ON u_label_0.node_id = u.id "
            "AND u_label_0.label = 'User' "
            "ORDER BY JSON_EXTRACT(u.properties, '$.name') ASC "
            "LIMIT 2 OFFSET 1",
        )

    def test_compile_match_relationship_to_sqlglot_ast(self) -> None:
        expression = cypherglot.compile_cypher_text(
            (
                "MATCH (a:User)-[r:KNOWS]->(b:User) "
                "WHERE r.since = 2020 "
                "RETURN a.name, b.name ORDER BY a.name LIMIT 1"
            )
        )

        self.assertEqual(
            expression.sql(),
            "SELECT JSON_EXTRACT(a.properties, '$.name') AS \"a.name\", "
            "JSON_EXTRACT(b.properties, '$.name') AS \"b.name\" "
            "FROM edges AS r "
            "JOIN nodes AS a ON a.id = r.from_id "
            "JOIN nodes AS b ON b.id = r.to_id "
            "JOIN node_labels AS a_label_0 ON a_label_0.node_id = a.id "
            "AND a_label_0.label = 'User' "
            "JOIN node_labels AS b_label_1 ON b_label_1.node_id = b.id "
            "AND b_label_1.label = 'User' "
            "WHERE r.type = 'KNOWS' AND JSON_EXTRACT(r.properties, '$.since') = 2020 "
            "ORDER BY JSON_EXTRACT(a.properties, '$.name') ASC "
            "LIMIT 1",
        )

    def test_compile_reverse_relationship_match(self) -> None:
        expression = cypherglot.compile_cypher_text(
            (
                "MATCH (b:User)<-[r:KNOWS]-(a:User) "
                "WHERE r.since = $since "
                "RETURN a.name, r.since, b.name ORDER BY r.since DESC LIMIT 1"
            )
        )

        self.assertEqual(
            expression.sql(),
            "SELECT JSON_EXTRACT(a.properties, '$.name') AS \"a.name\", "
            "JSON_EXTRACT(r.properties, '$.since') AS \"r.since\", "
            "JSON_EXTRACT(b.properties, '$.name') AS \"b.name\" "
            "FROM edges AS r "
            "JOIN nodes AS b ON b.id = r.to_id "
            "JOIN nodes AS a ON a.id = r.from_id "
            "JOIN node_labels AS b_label_0 ON b_label_0.node_id = b.id "
            "AND b_label_0.label = 'User' "
            "JOIN node_labels AS a_label_1 ON a_label_1.node_id = a.id "
            "AND a_label_1.label = 'User' "
            "WHERE r.type = 'KNOWS' AND JSON_EXTRACT(r.properties, '$.since') = :since "
            "ORDER BY JSON_EXTRACT(r.properties, '$.since') DESC "
            "LIMIT 1",
        )

    def test_compile_match_set_node(self) -> None:
        expression = cypherglot.compile_cypher_text(
            "MATCH (u:User {name: 'Alice'}) SET u.age = 31, u.active = true"
        )

        self.assertEqual(
            expression.sql(),
            "UPDATE nodes AS u "
            "SET properties = JSON_SET(COALESCE(u.properties, '{}'), '$.age', 31, "
            "'$.active', TRUE) "
            "WHERE EXISTS(SELECT 1 FROM node_labels AS u_label_filter_0 "
            "WHERE u_label_filter_0.node_id = u.id "
            "AND u_label_filter_0.label = 'User') "
            "AND JSON_EXTRACT(u.properties, '$.name') = 'Alice'",
        )

    def test_compile_match_set_relationship(self) -> None:
        expression = cypherglot.compile_cypher_text(
            (
                "MATCH (a:User)-[r:KNOWS]->(b:User) "
                "WHERE a.name = $name SET r.since = 2021, r.strength = 2"
            )
        )

        self.assertEqual(
            expression.sql(),
            "UPDATE edges AS r "
            "SET properties = JSON_SET(COALESCE(r.properties, '{}'), '$.since', 2021, "
            "'$.strength', 2) "
            "FROM nodes AS a, nodes AS b "
            "WHERE a.id = r.from_id AND b.id = r.to_id "
            "AND EXISTS(SELECT 1 FROM node_labels AS a_label_filter_0 "
            "WHERE a_label_filter_0.node_id = a.id "
            "AND a_label_filter_0.label = 'User') "
            "AND EXISTS(SELECT 1 FROM node_labels AS b_label_filter_1 "
            "WHERE b_label_filter_1.node_id = b.id "
            "AND b_label_filter_1.label = 'User') "
            "AND r.type = 'KNOWS' AND JSON_EXTRACT(a.properties, '$.name') = :name",
        )

    def test_compile_match_delete_node(self) -> None:
        expression = cypherglot.compile_cypher_text(
            "MATCH (u:User {name: 'Alice'}) DETACH DELETE u"
        )

        self.assertEqual(
            expression.sql(),
            "DELETE FROM nodes AS u "
            "WHERE EXISTS(SELECT 1 FROM node_labels AS u_label_filter_0 "
            "WHERE u_label_filter_0.node_id = u.id "
            "AND u_label_filter_0.label = 'User') "
            "AND JSON_EXTRACT(u.properties, '$.name') = 'Alice'",
        )

    def test_compile_match_delete_relationship(self) -> None:
        expression = cypherglot.compile_cypher_text(
            "MATCH (a:User)-[r:KNOWS]->(b:User) WHERE a.name = 'Alice' DELETE r"
        )

        self.assertEqual(
            expression.sql(),
            "DELETE FROM edges WHERE id IN (SELECT r.id FROM edges AS r "
            "JOIN nodes AS a ON a.id = r.from_id JOIN nodes AS b ON b.id = r.to_id "
            "JOIN node_labels AS a_label_0 ON a_label_0.node_id = a.id "
            "AND a_label_0.label = 'User' JOIN node_labels AS b_label_1 "
            "ON b_label_1.node_id = b.id AND b_label_1.label = 'User' "
            "WHERE r.type = 'KNOWS' AND JSON_EXTRACT(a.properties, '$.name') = 'Alice')",
        )

    def test_compile_match_create_self_loop_relationship(self) -> None:
        expression = cypherglot.compile_cypher_text(
            "MATCH (root:Root) CREATE (root)-[:LINK]->(root)"
        )

        self.assertEqual(
            expression.sql(),
            "INSERT INTO edges (type, from_id, to_id, properties) "
            "SELECT 'LINK', root.id, root.id, '{}' "
            "FROM nodes AS root "
            "WHERE EXISTS(SELECT 1 FROM node_labels AS root_label_filter_0 "
            "WHERE root_label_filter_0.node_id = root.id "
            "AND root_label_filter_0.label = 'Root')",
        )

    def test_compile_match_create_between_existing_nodes(self) -> None:
        expression = cypherglot.compile_cypher_text(
            (
                "MATCH (x:Begin), (y:End) WHERE y.name = 'finish' "
                "CREATE (x)-[:TYPE]->(y)"
            )
        )

        self.assertEqual(
            expression.sql(),
            "INSERT INTO edges (type, from_id, to_id, properties) "
            "SELECT 'TYPE', x.id, y.id, '{}' "
            "FROM nodes AS x, nodes AS y "
            "WHERE EXISTS(SELECT 1 FROM node_labels AS x_label_filter_0 "
            "WHERE x_label_filter_0.node_id = x.id "
            "AND x_label_filter_0.label = 'Begin') "
            "AND EXISTS(SELECT 1 FROM node_labels AS y_label_filter_1 "
            "WHERE y_label_filter_1.node_id = y.id AND y_label_filter_1.label = 'End') "
            "AND JSON_EXTRACT(y.properties, '$.name') = 'finish'",
        )

    def test_compile_match_merge_between_existing_nodes(self) -> None:
        expression = cypherglot.compile_cypher_text(
            (
                "MATCH (x:Begin), (y:End) WHERE y.name = 'finish' "
                "MERGE (x)-[:TYPE]->(y)"
            )
        )

        self.assertEqual(
            expression.sql(),
            "INSERT INTO edges (type, from_id, to_id, properties) "
            "SELECT 'TYPE', x.id, y.id, '{}' "
            "FROM nodes AS x, nodes AS y "
            "WHERE EXISTS(SELECT 1 FROM node_labels AS x_label_filter_0 "
            "WHERE x_label_filter_0.node_id = x.id "
            "AND x_label_filter_0.label = 'Begin') "
            "AND EXISTS(SELECT 1 FROM node_labels AS y_label_filter_1 "
            "WHERE y_label_filter_1.node_id = y.id AND y_label_filter_1.label = 'End') "
            "AND JSON_EXTRACT(y.properties, '$.name') = 'finish' "
            "AND NOT EXISTS(SELECT 1 FROM edges AS existing_merge_edge "
            "WHERE existing_merge_edge.from_id = x.id AND existing_merge_edge.to_id = y.id "
            "AND existing_merge_edge.type = 'TYPE')",
        )

    def test_compile_match_create_from_relationship_source(self) -> None:
        expression = cypherglot.compile_cypher_text(
            "MATCH (a:User)-[r:KNOWS]->(b:User) CREATE (a)-[:INTRODUCED]->(b)"
        )

        self.assertEqual(
            expression.sql(),
            "INSERT INTO edges (type, from_id, to_id, properties) "
            "SELECT 'INTRODUCED', a.id, b.id, '{}' "
            "FROM edges AS r JOIN nodes AS a ON a.id = r.from_id JOIN nodes AS b ON b.id = r.to_id "
            "JOIN node_labels AS a_label_0 ON a_label_0.node_id = a.id AND a_label_0.label = 'User' "
            "JOIN node_labels AS b_label_1 ON b_label_1.node_id = b.id AND b_label_1.label = 'User' "
            "WHERE r.type = 'KNOWS'",
        )

    def test_compile_match_merge_from_chain_source(self) -> None:
        expression = cypherglot.compile_cypher_text(
            "MATCH (a:User)-[r:KNOWS]->(b:User)-[s:WORKS_AT]->(c:Company) MERGE (a)-[:INTRODUCED]->(c)"
        )

        self.assertEqual(
            expression.sql(),
            "INSERT INTO edges (type, from_id, to_id, properties) "
            "SELECT 'INTRODUCED', a.id, c.id, '{}' "
            "FROM edges AS r JOIN nodes AS a ON a.id = r.from_id JOIN nodes AS b ON b.id = r.to_id "
            "JOIN edges AS s ON b.id = s.from_id JOIN nodes AS c ON c.id = s.to_id "
            "JOIN node_labels AS a_label_0 ON a_label_0.node_id = a.id AND a_label_0.label = 'User' "
            "JOIN node_labels AS b_label_1 ON b_label_1.node_id = b.id AND b_label_1.label = 'User' "
            "JOIN node_labels AS c_label_2 ON c_label_2.node_id = c.id AND c_label_2.label = 'Company' "
            "WHERE r.type = 'KNOWS' AND s.type = 'WORKS_AT' "
            "AND NOT EXISTS(SELECT 1 FROM edges AS existing_merge_edge WHERE existing_merge_edge.from_id = a.id AND existing_merge_edge.to_id = c.id AND existing_merge_edge.type = 'INTRODUCED')",
        )

    def test_compile_traversal_match_merge_rejects_new_endpoint_from_single_statement_api(self) -> None:
        with self.assertRaisesRegex(ValueError, "multi-step SQL program"):
            cypherglot.compile_cypher_text(
                "MATCH (a:User)-[r:KNOWS]->(b:User) MERGE (a)-[:INTRODUCED]->(:Person {name: 'Cara'})"
            )

    def test_compile_match_create_rejects_new_endpoint_node(self) -> None:
        with self.assertRaisesRegex(ValueError, "multi-step SQL program"):
            cypherglot.compile_cypher_text(
                "MATCH (x:Begin) CREATE (x)-[:TYPE]->(:End {name: 'finish'})"
            )

    def test_compile_traversal_match_create_rejects_new_endpoint_from_single_statement_api(self) -> None:
        with self.assertRaisesRegex(ValueError, "multi-step SQL program"):
            cypherglot.compile_cypher_text(
                "MATCH (a:User)-[r:KNOWS]->(b:User) CREATE (a)-[:INTRODUCED]->(:Person {name: 'Cara'})"
            )

    def test_compile_rejects_merge_program_from_single_statement_api(self) -> None:
        with self.assertRaisesRegex(ValueError, "multi-step SQL program"):
            cypherglot.compile_cypher_text("MERGE (u:User {name: 'Alice'})")

    def test_compile_program_merge_node(self) -> None:
        program = cypherglot.compile_cypher_program_text(
            "MERGE (u:User {name: 'Alice'})"
        )

        self.assertEqual(len(program.steps), 1)
        loop = program.steps[0]
        self.assertIsInstance(loop, cypherglot.CompiledCypherLoop)
        assert isinstance(loop, cypherglot.CompiledCypherLoop)

        self.assertEqual(loop.row_bindings, ("merge_guard",))
        self.assertEqual(
            loop.source.sql(),
            "SELECT 1 AS merge_guard WHERE NOT EXISTS(SELECT 1 FROM nodes AS u "
            "WHERE EXISTS(SELECT 1 FROM node_labels AS u_label_filter_0 "
            "WHERE u_label_filter_0.node_id = u.id AND u_label_filter_0.label = 'User') "
            "AND JSON_EXTRACT(u.properties, '$.name') = 'Alice' LIMIT 1)",
        )
        self.assertEqual(len(loop.body), 2)
        self.assertEqual(
            loop.body[0].sql.sql(),
            "INSERT INTO nodes (properties) VALUES (JSON_OBJECT('name': 'Alice')) RETURNING id",
        )
        self.assertEqual(loop.body[0].bind_columns, ("merged_node_id",))
        self.assertEqual(
            loop.body[1].sql.sql(),
            "INSERT INTO node_labels (node_id, label) VALUES (:merged_node_id, 'User')",
        )

    def test_compile_program_merge_relationship(self) -> None:
        program = cypherglot.compile_cypher_program_text(
            "MERGE (:Begin {name: 'start'})-[:TYPE]->(:End {name: 'finish'})"
        )

        self.assertEqual(len(program.steps), 1)
        loop = program.steps[0]
        self.assertIsInstance(loop, cypherglot.CompiledCypherLoop)
        assert isinstance(loop, cypherglot.CompiledCypherLoop)

        self.assertEqual(loop.row_bindings, ("merge_guard",))
        self.assertEqual(
            loop.source.sql(),
            "SELECT 1 AS merge_guard WHERE NOT EXISTS(SELECT 1 FROM edges AS merge_edge "
            "JOIN nodes AS __humem_merge_left_node ON __humem_merge_left_node.id = merge_edge.from_id "
            "JOIN nodes AS __humem_merge_right_node ON __humem_merge_right_node.id = merge_edge.to_id "
            "JOIN node_labels AS __humem_merge_left_node_label_0 ON __humem_merge_left_node_label_0.node_id = __humem_merge_left_node.id AND __humem_merge_left_node_label_0.label = 'Begin' "
            "JOIN node_labels AS __humem_merge_right_node_label_1 ON __humem_merge_right_node_label_1.node_id = __humem_merge_right_node.id AND __humem_merge_right_node_label_1.label = 'End' "
            "WHERE merge_edge.type = 'TYPE' AND JSON_EXTRACT(__humem_merge_left_node.properties, '$.name') = 'start' "
            "AND JSON_EXTRACT(__humem_merge_right_node.properties, '$.name') = 'finish' LIMIT 1)",
        )
        self.assertEqual(len(loop.body), 5)
        self.assertEqual(
            loop.body[4].sql.sql(),
            "INSERT INTO edges (type, from_id, to_id, properties) VALUES ('TYPE', :left_node_id, :right_node_id, '{}')",
        )

    def test_compile_program_traversal_match_create_with_new_right_endpoint(self) -> None:
        program = cypherglot.compile_cypher_program_text(
            "MATCH (a:User)-[r:KNOWS]->(b:User) CREATE (a)-[:INTRODUCED]->(:Person {name: 'Cara'})"
        )
        unlabeled_program = cypherglot.compile_cypher_program_text(
            "MATCH (a:User)-[r:KNOWS]->(b:User) CREATE (a)-[:INTRODUCED]->({name: 'Cara'})"
        )

        self.assertEqual(len(program.steps), 1)
        loop = program.steps[0]
        self.assertIsInstance(loop, cypherglot.CompiledCypherLoop)
        assert isinstance(loop, cypherglot.CompiledCypherLoop)

        self.assertEqual(loop.row_bindings, ("match_node_id",))
        self.assertEqual(
            loop.source.sql(),
            "SELECT a.id AS match_node_id FROM edges AS r JOIN nodes AS a ON a.id = r.from_id JOIN nodes AS b ON b.id = r.to_id JOIN node_labels AS a_label_0 ON a_label_0.node_id = a.id AND a_label_0.label = 'User' JOIN node_labels AS b_label_1 ON b_label_1.node_id = b.id AND b_label_1.label = 'User' WHERE r.type = 'KNOWS'",
        )
        self.assertEqual(len(loop.body), 3)
        self.assertEqual(
            loop.body[0].sql.sql(),
            "INSERT INTO nodes (properties) VALUES (JSON_OBJECT('name': 'Cara')) RETURNING id",
        )
        self.assertEqual(loop.body[0].bind_columns, ("created_node_id",))
        self.assertEqual(
            loop.body[1].sql.sql(),
            "INSERT INTO node_labels (node_id, label) VALUES (:created_node_id, 'Person')",
        )
        self.assertEqual(
            loop.body[2].sql.sql(),
            "INSERT INTO edges (type, from_id, to_id, properties) VALUES ('INTRODUCED', :match_node_id, :created_node_id, '{}')",
        )

        unlabeled_loop = unlabeled_program.steps[0]
        self.assertIsInstance(unlabeled_loop, cypherglot.CompiledCypherLoop)
        assert isinstance(unlabeled_loop, cypherglot.CompiledCypherLoop)
        self.assertEqual(len(unlabeled_loop.body), 2)
        self.assertEqual(
            unlabeled_loop.body[0].sql.sql(),
            "INSERT INTO nodes (properties) VALUES (JSON_OBJECT('name': 'Cara')) RETURNING id",
        )
        self.assertEqual(
            unlabeled_loop.body[1].sql.sql(),
            "INSERT INTO edges (type, from_id, to_id, properties) VALUES ('INTRODUCED', :match_node_id, :created_node_id, '{}')",
        )

    def test_compile_program_traversal_match_merge_with_new_right_endpoint(self) -> None:
        program = cypherglot.compile_cypher_program_text(
            "MATCH (a:User)-[r:KNOWS]->(b:User) MERGE (a)-[:INTRODUCED]->(:Person {name: 'Cara'})"
        )
        unlabeled_program = cypherglot.compile_cypher_program_text(
            "MATCH (a:User)-[r:KNOWS]->(b:User) MERGE (a)-[:INTRODUCED]->({name: 'Cara'})"
        )

        self.assertEqual(len(program.steps), 1)
        loop = program.steps[0]
        self.assertIsInstance(loop, cypherglot.CompiledCypherLoop)
        assert isinstance(loop, cypherglot.CompiledCypherLoop)

        self.assertEqual(loop.row_bindings, ("match_node_id",))
        self.assertEqual(
            loop.source.sql(),
            "SELECT a.id AS match_node_id FROM edges AS r JOIN nodes AS a ON a.id = r.from_id JOIN nodes AS b ON b.id = r.to_id JOIN node_labels AS a_label_0 ON a_label_0.node_id = a.id AND a_label_0.label = 'User' JOIN node_labels AS b_label_1 ON b_label_1.node_id = b.id AND b_label_1.label = 'User' WHERE r.type = 'KNOWS' AND NOT EXISTS(SELECT 1 FROM edges AS existing_merge_edge JOIN nodes AS existing_merge_new_node ON existing_merge_new_node.id = existing_merge_edge.to_id JOIN node_labels AS existing_merge_new_node_label_0 ON existing_merge_new_node_label_0.node_id = existing_merge_new_node.id AND existing_merge_new_node_label_0.label = 'Person' WHERE existing_merge_edge.from_id = a.id AND existing_merge_edge.type = 'INTRODUCED' AND JSON_EXTRACT(existing_merge_new_node.properties, '$.name') = 'Cara' LIMIT 1)",
        )
        self.assertEqual(len(loop.body), 3)
        self.assertEqual(
            loop.body[0].sql.sql(),
            "INSERT INTO nodes (properties) VALUES (JSON_OBJECT('name': 'Cara')) RETURNING id",
        )
        self.assertEqual(loop.body[0].bind_columns, ("created_node_id",))
        self.assertEqual(
            loop.body[1].sql.sql(),
            "INSERT INTO node_labels (node_id, label) VALUES (:created_node_id, 'Person')",
        )
        self.assertEqual(
            loop.body[2].sql.sql(),
            "INSERT INTO edges (type, from_id, to_id, properties) VALUES ('INTRODUCED', :match_node_id, :created_node_id, '{}')",
        )

        unlabeled_loop = unlabeled_program.steps[0]
        self.assertIsInstance(unlabeled_loop, cypherglot.CompiledCypherLoop)
        assert isinstance(unlabeled_loop, cypherglot.CompiledCypherLoop)
        self.assertEqual(
            unlabeled_loop.source.sql(),
            "SELECT a.id AS match_node_id FROM edges AS r JOIN nodes AS a ON a.id = r.from_id JOIN nodes AS b ON b.id = r.to_id JOIN node_labels AS a_label_0 ON a_label_0.node_id = a.id AND a_label_0.label = 'User' JOIN node_labels AS b_label_1 ON b_label_1.node_id = b.id AND b_label_1.label = 'User' WHERE r.type = 'KNOWS' AND NOT EXISTS(SELECT 1 FROM edges AS existing_merge_edge JOIN nodes AS existing_merge_new_node ON existing_merge_new_node.id = existing_merge_edge.to_id WHERE existing_merge_edge.from_id = a.id AND existing_merge_edge.type = 'INTRODUCED' AND JSON_EXTRACT(existing_merge_new_node.properties, '$.name') = 'Cara' LIMIT 1)",
        )
        self.assertEqual(len(unlabeled_loop.body), 2)
        self.assertEqual(
            unlabeled_loop.body[0].sql.sql(),
            "INSERT INTO nodes (properties) VALUES (JSON_OBJECT('name': 'Cara')) RETURNING id",
        )
        self.assertEqual(
            unlabeled_loop.body[1].sql.sql(),
            "INSERT INTO edges (type, from_id, to_id, properties) VALUES ('INTRODUCED', :match_node_id, :created_node_id, '{}')",
        )

    def test_compile_program_match_create_with_new_right_endpoint(self) -> None:
        program = cypherglot.compile_cypher_program_text(
            "MATCH (x:Begin) CREATE (x)-[:TYPE]->(:End {name: 'finish'})"
        )

        self.assertEqual(len(program.steps), 1)
        loop = program.steps[0]
        self.assertIsInstance(loop, cypherglot.CompiledCypherLoop)
        assert isinstance(loop, cypherglot.CompiledCypherLoop)

        self.assertEqual(loop.row_bindings, ("match_node_id",))
        self.assertEqual(
            loop.source.sql(),
            "SELECT x.id AS match_node_id FROM nodes AS x "
            "WHERE EXISTS(SELECT 1 FROM node_labels AS x_label_filter_0 "
            "WHERE x_label_filter_0.node_id = x.id "
            "AND x_label_filter_0.label = 'Begin')",
        )
        self.assertEqual(len(loop.body), 3)
        self.assertEqual(
            loop.body[0].sql.sql(),
            "INSERT INTO nodes (properties) VALUES "
            "(JSON_OBJECT('name': 'finish')) RETURNING id",
        )
        self.assertEqual(loop.body[0].bind_columns, ("created_node_id",))
        self.assertEqual(
            loop.body[1].sql.sql(),
            "INSERT INTO node_labels (node_id, label) VALUES (:created_node_id, 'End')",
        )
        self.assertEqual(
            loop.body[2].sql.sql(),
            "INSERT INTO edges (type, from_id, to_id, properties) VALUES "
            "('TYPE', :match_node_id, :created_node_id, '{}')",
        )

    def test_compile_program_match_create_with_new_left_endpoint(self) -> None:
        program = cypherglot.compile_cypher_program_text(
            "MATCH (x:End) CREATE (:Begin {name: 'start'})-[:TYPE]->(x)"
        )

        loop = program.steps[0]
        assert isinstance(loop, cypherglot.CompiledCypherLoop)
        self.assertEqual(
            loop.body[0].sql.sql(),
            "INSERT INTO nodes (properties) VALUES "
            "(JSON_OBJECT('name': 'start')) RETURNING id",
        )
        self.assertEqual(
            loop.body[1].sql.sql(),
            "INSERT INTO node_labels (node_id, label) VALUES "
            "(:created_node_id, 'Begin')",
        )
        self.assertEqual(
            loop.body[2].sql.sql(),
            "INSERT INTO edges (type, from_id, to_id, properties) VALUES "
            "('TYPE', :created_node_id, :match_node_id, '{}')",
        )

    def test_compile_rejects_non_match_statement(self) -> None:
        with self.assertRaisesRegex(ValueError, "multi-step SQL program"):
            cypherglot.compile_cypher_text("CREATE (:User {name: 'Alice'})")
