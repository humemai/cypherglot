from __future__ import annotations

import unittest

import cypherglot


class TestNormalize(unittest.TestCase):
    def test_normalize_cypher_text_normalizes_create_node(self) -> None:
        normalized = cypherglot.normalize_cypher_text(
            "CREATE (u:User {name: 'Alice', age: 30})"
        )

        self.assertEqual(type(normalized).__name__, "NormalizedCreateNode")
        self.assertEqual(normalized.node.alias, "u")
        self.assertEqual(normalized.node.label, "User")
        self.assertEqual(normalized.node.properties, (("name", "Alice"), ("age", 30)))

    def test_normalize_cypher_text_normalizes_match_return(self) -> None:
        normalized = cypherglot.normalize_cypher_text(
            "MATCH (u:User) WHERE u.name = $name RETURN u.name ORDER BY u.name LIMIT 1"
        )

        self.assertEqual(type(normalized).__name__, "NormalizedMatchNode")
        self.assertEqual(normalized.node.alias, "u")
        self.assertEqual(normalized.predicates[0].field, "name")
        self.assertEqual(normalized.returns[0].column_name, "u.name")
        self.assertEqual(normalized.limit, 1)

    def test_normalize_cypher_text_normalizes_match_where_id_and_type_filters(self) -> None:
        id_normalized = cypherglot.normalize_cypher_text(
            "MATCH (u:User) WHERE id(u) >= 1 RETURN u.name ORDER BY u.name"
        )
        type_normalized = cypherglot.normalize_cypher_text(
            "MATCH (a:User)-[r:KNOWS]->(b:User) WHERE type(r) = 'KNOWS' RETURN b.name ORDER BY b.name"
        )

        self.assertEqual(id_normalized.predicates[0].field, "id")
        self.assertEqual(id_normalized.predicates[0].operator, ">=")
        self.assertEqual(type_normalized.predicates[0].field, "type")
        self.assertEqual(type_normalized.predicates[0].operator, "=")

    def test_normalize_cypher_text_normalizes_aliased_match_return(self) -> None:
        normalized = cypherglot.normalize_cypher_text(
            "MATCH (u:User) RETURN u.name AS name ORDER BY name LIMIT 1"
        )

        self.assertEqual(type(normalized).__name__, "NormalizedMatchNode")
        self.assertEqual(normalized.returns[0].alias, "u")
        self.assertEqual(normalized.returns[0].field, "name")
        self.assertEqual(normalized.returns[0].column_name, "name")
        self.assertEqual(normalized.order_by[0].alias, "name")
        self.assertEqual(normalized.order_by[0].field, "__value__")

    def test_normalize_cypher_text_normalizes_entity_match_return(self) -> None:
        normalized = cypherglot.normalize_cypher_text(
            "MATCH (u:User) RETURN u AS user"
        )
        ordered_normalized = cypherglot.normalize_cypher_text(
            "MATCH (u:User) RETURN u AS user ORDER BY user"
        )

        self.assertEqual(type(normalized).__name__, "NormalizedMatchNode")
        self.assertEqual(normalized.returns[0].alias, "u")
        self.assertIsNone(normalized.returns[0].field)
        self.assertEqual(normalized.returns[0].column_name, "user")
        self.assertEqual(type(ordered_normalized).__name__, "NormalizedMatchNode")
        self.assertEqual(ordered_normalized.order_by[0].alias, "user")
        self.assertEqual(ordered_normalized.order_by[0].field, "__value__")

    def test_normalize_cypher_text_normalizes_relationship_entity_match_return(
        self,
    ) -> None:
        normalized = cypherglot.normalize_cypher_text(
            "MATCH (a:User)-[r:KNOWS]->(b:User) RETURN a AS user, r AS rel, b.name AS name ORDER BY name"
        )
        ordered_normalized = cypherglot.normalize_cypher_text(
            "MATCH (a:User)-[r:KNOWS]->(b:User) RETURN r AS rel ORDER BY rel"
        )

        self.assertEqual(type(normalized).__name__, "NormalizedMatchRelationship")
        self.assertEqual(normalized.returns[0].alias, "a")
        self.assertIsNone(normalized.returns[0].field)
        self.assertEqual(normalized.returns[0].column_name, "user")
        self.assertEqual(normalized.returns[1].alias, "r")
        self.assertIsNone(normalized.returns[1].field)
        self.assertEqual(normalized.returns[1].column_name, "rel")
        self.assertEqual(normalized.returns[2].column_name, "name")
        self.assertEqual(normalized.order_by[0].alias, "name")
        self.assertEqual(normalized.order_by[0].field, "__value__")
        self.assertEqual(type(ordered_normalized).__name__, "NormalizedMatchRelationship")
        self.assertEqual(ordered_normalized.returns[0].kind, "entity")
        self.assertEqual(ordered_normalized.returns[0].column_name, "rel")
        self.assertEqual(ordered_normalized.order_by[0].alias, "rel")
        self.assertEqual(ordered_normalized.order_by[0].field, "__value__")

    def test_normalize_cypher_text_normalizes_fixed_length_multi_hop_match(self) -> None:
        normalized = cypherglot.normalize_cypher_text(
            "MATCH (a:User)-[r:KNOWS]->(b:User)-[s:WORKS_AT]->(c:Company) WHERE b.name = 'Bob' RETURN a.name AS user_name, c.name AS company ORDER BY company"
        )

        self.assertEqual(type(normalized).__name__, "NormalizedMatchChain")
        self.assertEqual(tuple(node.alias for node in normalized.nodes), ("a", "b", "c"))
        self.assertEqual(tuple(relationship.alias for relationship in normalized.relationships), ("r", "s"))
        self.assertEqual(normalized.relationships[0].type_name, "KNOWS")
        self.assertEqual(normalized.relationships[1].type_name, "WORKS_AT")
        self.assertEqual(normalized.predicates[0].alias, "b")
        self.assertEqual(normalized.returns[0].column_name, "user_name")
        self.assertEqual(normalized.returns[1].column_name, "company")
        self.assertEqual(normalized.order_by[0].alias, "company")

    def test_normalize_cypher_text_normalizes_fixed_length_multi_hop_with_source(self) -> None:
        normalized = cypherglot.normalize_cypher_text(
            "MATCH (a:User)-[r:KNOWS]->(b:User)-[s:WORKS_AT]->(c:Company) WITH b AS friend, c.name AS company RETURN friend.name, company ORDER BY company"
        )

        self.assertEqual(type(normalized).__name__, "NormalizedMatchWithReturn")
        self.assertEqual(type(normalized.source).__name__, "NormalizedMatchChain")
        self.assertEqual(normalized.bindings[0].output_alias, "friend")
        self.assertEqual(normalized.bindings[0].binding_kind, "entity")
        self.assertEqual(normalized.bindings[1].output_alias, "company")
        self.assertEqual(normalized.bindings[1].binding_kind, "scalar")

    def test_normalize_cypher_text_normalizes_bounded_variable_length_match(self) -> None:
        normalized = cypherglot.normalize_cypher_text(
            "MATCH (a:User)-[:KNOWS*1..2]->(b:User) RETURN a.name AS user_name, b.name AS friend ORDER BY friend"
        )
        zero_hop_normalized = cypherglot.normalize_cypher_text(
            "MATCH (a:User)-[:KNOWS*0..2]->(b:User) RETURN a.name AS user_name, b.name AS friend ORDER BY friend"
        )
        count_normalized = cypherglot.normalize_cypher_text(
            "MATCH (a:User)-[:KNOWS*0..2]->(b:User) RETURN count(b) AS total"
        )
        sum_normalized = cypherglot.normalize_cypher_text(
            "MATCH (a:User)-[:KNOWS*1..2]->(b:User) RETURN sum(b.score) AS total"
        )
        grouped_count_normalized = cypherglot.normalize_cypher_text(
            "MATCH (a:User)-[:KNOWS*1..2]->(b:User) RETURN b.name AS friend, count(b) AS total ORDER BY total DESC"
        )
        alias_normalized = cypherglot.normalize_cypher_text(
            "MATCH (a:User)-[r:KNOWS*1..2]->(b:User) RETURN b.name AS friend ORDER BY friend"
        )

        self.assertEqual(type(normalized).__name__, "NormalizedMatchRelationship")
        self.assertEqual(normalized.relationship.min_hops, 1)
        self.assertEqual(normalized.relationship.max_hops, 2)
        self.assertEqual(normalized.relationship.type_name, "KNOWS")
        self.assertEqual(normalized.returns[0].column_name, "user_name")
        self.assertEqual(normalized.order_by[0].alias, "friend")
        self.assertEqual(zero_hop_normalized.relationship.min_hops, 0)
        self.assertEqual(zero_hop_normalized.relationship.max_hops, 2)
        self.assertEqual(count_normalized.returns[0].kind, "count")
        self.assertEqual(count_normalized.returns[0].column_name, "total")
        self.assertEqual(sum_normalized.returns[0].kind, "sum")
        self.assertEqual(sum_normalized.returns[0].field, "score")
        self.assertEqual(grouped_count_normalized.returns[0].column_name, "friend")
        self.assertEqual(grouped_count_normalized.returns[1].kind, "count")
        self.assertEqual(grouped_count_normalized.order_by[0].alias, "total")
        self.assertEqual(alias_normalized.relationship.alias, "r")

    def test_normalize_cypher_text_normalizes_bounded_variable_length_with_source(self) -> None:
        normalized = cypherglot.normalize_cypher_text(
            "MATCH (a:User)-[:KNOWS*..2]->(b:User) WITH b AS friend RETURN friend.name ORDER BY friend.name"
        )
        zero_hop_normalized = cypherglot.normalize_cypher_text(
            "MATCH (a:User)-[:KNOWS*0..2]->(b:User) WITH b AS friend RETURN friend.name ORDER BY friend.name"
        )

        self.assertEqual(type(normalized).__name__, "NormalizedMatchWithReturn")
        self.assertEqual(type(normalized.source).__name__, "NormalizedMatchRelationship")
        self.assertEqual(normalized.source.relationship.min_hops, 1)
        self.assertEqual(normalized.source.relationship.max_hops, 2)
        self.assertEqual(zero_hop_normalized.source.relationship.min_hops, 0)
        self.assertEqual(zero_hop_normalized.source.relationship.max_hops, 2)

    def test_normalize_cypher_text_normalizes_plain_read_count_return(self) -> None:
        normalized = cypherglot.normalize_cypher_text(
            "MATCH (u:User) RETURN count(u) AS total"
        )
        star_normalized = cypherglot.normalize_cypher_text(
            "MATCH (u:User) RETURN count(*) AS total"
        )
        no_alias_normalized = cypherglot.normalize_cypher_text(
            "MATCH (u:User) RETURN count(*)"
        )

        self.assertEqual(type(normalized).__name__, "NormalizedMatchNode")
        self.assertEqual(normalized.returns[0].kind, "count")
        self.assertEqual(normalized.returns[0].alias, "u")
        self.assertEqual(normalized.returns[0].column_name, "total")
        self.assertEqual(type(star_normalized).__name__, "NormalizedMatchNode")
        self.assertEqual(star_normalized.returns[0].kind, "count")
        self.assertEqual(star_normalized.returns[0].alias, "*")
        self.assertEqual(star_normalized.returns[0].column_name, "total")
        self.assertEqual(no_alias_normalized.returns[0].column_name, "count(*)")

    def test_normalize_cypher_text_normalizes_plain_read_numeric_aggregates(
        self,
    ) -> None:
        normalized = cypherglot.normalize_cypher_text(
            "MATCH (u:User) RETURN sum(u.score) AS total"
        )
        grouped_normalized = cypherglot.normalize_cypher_text(
            "MATCH (u:User) RETURN u.name AS name, avg(u.score) AS mean ORDER BY mean DESC"
        )
        no_alias_normalized = cypherglot.normalize_cypher_text(
            "MATCH (u:User) RETURN sum(u.score)"
        )

        self.assertEqual(type(normalized).__name__, "NormalizedMatchNode")
        self.assertEqual(normalized.returns[0].kind, "sum")
        self.assertEqual(normalized.returns[0].alias, "u")
        self.assertEqual(normalized.returns[0].field, "score")
        self.assertEqual(normalized.returns[0].column_name, "total")
        self.assertEqual(type(grouped_normalized).__name__, "NormalizedMatchNode")
        self.assertEqual(grouped_normalized.returns[0].kind, "field")
        self.assertEqual(grouped_normalized.returns[1].kind, "avg")
        self.assertEqual(grouped_normalized.returns[1].field, "score")
        self.assertEqual(grouped_normalized.order_by[0].alias, "mean")
        self.assertEqual(no_alias_normalized.returns[0].column_name, "sum(u.score)")

    def test_normalize_cypher_text_normalizes_searched_case_returns(self) -> None:
        normalized = cypherglot.normalize_cypher_text(
            "MATCH (u:User) RETURN CASE WHEN u.age >= 18 THEN u.name ELSE 'minor' END AS label ORDER BY label"
        )

        self.assertEqual(type(normalized).__name__, "NormalizedMatchNode")
        self.assertEqual(normalized.returns[0].kind, "case")
        case_spec = normalized.returns[0].value
        self.assertEqual(type(case_spec).__name__, "CaseSpec")
        assert case_spec is not None
        self.assertEqual(case_spec.when_items[0].condition.kind, "predicate")
        self.assertEqual(case_spec.when_items[0].condition.alias, "u")
        self.assertEqual(case_spec.when_items[0].condition.field, "age")
        self.assertEqual(case_spec.when_items[0].result.kind, "field")
        self.assertEqual(case_spec.when_items[0].result.alias, "u")
        self.assertEqual(case_spec.when_items[0].result.field, "name")
        self.assertEqual(case_spec.else_item.kind, "scalar")
        self.assertEqual(case_spec.else_item.value, "minor")
        self.assertEqual(normalized.order_by[0].alias, "label")

    def test_normalize_cypher_text_normalizes_properties_returns(self) -> None:
        normalized = cypherglot.normalize_cypher_text(
            "MATCH (a:User)-[r:KNOWS]->(b:User) RETURN properties(r) AS props ORDER BY props"
        )
        no_alias_normalized = cypherglot.normalize_cypher_text(
            "MATCH (u:User) RETURN properties(u)"
        )

        self.assertEqual(type(normalized).__name__, "NormalizedMatchRelationship")
        self.assertEqual(normalized.returns[0].kind, "properties")
        self.assertEqual(normalized.returns[0].alias, "r")
        self.assertEqual(normalized.returns[0].column_name, "props")
        self.assertEqual(normalized.order_by[0].alias, "props")
        self.assertEqual(no_alias_normalized.returns[0].column_name, "properties(u)")

    def test_normalize_cypher_text_normalizes_labels_returns(self) -> None:
        normalized = cypherglot.normalize_cypher_text(
            "MATCH (u:User) RETURN labels(u) AS labels ORDER BY labels"
        )

        self.assertEqual(type(normalized).__name__, "NormalizedMatchNode")
        self.assertEqual(normalized.returns[0].kind, "labels")
        self.assertEqual(normalized.returns[0].alias, "u")
        self.assertEqual(normalized.returns[0].column_name, "labels")
        self.assertEqual(normalized.order_by[0].alias, "labels")

    def test_normalize_cypher_text_normalizes_keys_returns(self) -> None:
        normalized = cypherglot.normalize_cypher_text(
            "MATCH (a:User)-[r:KNOWS]->(b:User) RETURN keys(r) AS keys ORDER BY keys"
        )

        self.assertEqual(type(normalized).__name__, "NormalizedMatchRelationship")
        self.assertEqual(normalized.returns[0].kind, "keys")
        self.assertEqual(normalized.returns[0].alias, "r")
        self.assertEqual(normalized.returns[0].column_name, "keys")
        self.assertEqual(normalized.order_by[0].alias, "keys")

    def test_normalize_cypher_text_normalizes_start_and_end_node_returns(self) -> None:
        normalized = cypherglot.normalize_cypher_text(
            "MATCH (a:User)-[r:KNOWS]->(b:User) RETURN startNode(r) AS start, endNode(r) AS ending ORDER BY start, ending"
        )
        field_normalized = cypherglot.normalize_cypher_text(
            "MATCH (a:User)-[r:KNOWS]->(b:User) RETURN startNode(r).id AS start_id, endNode(r).id AS end_id ORDER BY start_id, end_id"
        )

        self.assertEqual(type(normalized).__name__, "NormalizedMatchRelationship")
        self.assertEqual(normalized.returns[0].kind, "start_node")
        self.assertEqual(normalized.returns[0].alias, "r")
        self.assertEqual(normalized.returns[0].column_name, "start")
        self.assertEqual(normalized.returns[1].kind, "end_node")
        self.assertEqual(normalized.returns[1].alias, "r")
        self.assertEqual(normalized.returns[1].column_name, "ending")
        self.assertEqual(normalized.order_by[0].alias, "start")
        self.assertEqual(normalized.order_by[1].alias, "ending")
        self.assertEqual(field_normalized.returns[0].kind, "start_node")
        self.assertEqual(field_normalized.returns[0].field, "id")
        self.assertEqual(field_normalized.returns[0].column_name, "start_id")
        self.assertEqual(field_normalized.returns[1].kind, "end_node")
        self.assertEqual(field_normalized.returns[1].field, "id")
        self.assertEqual(field_normalized.returns[1].column_name, "end_id")
        self.assertEqual(field_normalized.order_by[0].alias, "start_id")
        self.assertEqual(field_normalized.order_by[1].alias, "end_id")

    def test_normalize_cypher_text_normalizes_grouped_entity_count_return(
        self,
    ) -> None:
        normalized = cypherglot.normalize_cypher_text(
            "MATCH (u:User) RETURN u AS user, count(u) AS total ORDER BY total DESC"
        )
        star_normalized = cypherglot.normalize_cypher_text(
            "MATCH (u:User) RETURN u AS user, count(*) AS total ORDER BY total DESC"
        )
        relationship_normalized = cypherglot.normalize_cypher_text(
            "MATCH (a:User)-[r:KNOWS]->(b:User) RETURN r AS rel, count(r) AS total ORDER BY total DESC"
        )
        relationship_star_normalized = cypherglot.normalize_cypher_text(
            "MATCH (a:User)-[r:KNOWS]->(b:User) RETURN r AS rel, count(*) AS total ORDER BY total DESC"
        )

        self.assertEqual(type(normalized).__name__, "NormalizedMatchNode")
        self.assertEqual(normalized.returns[0].kind, "entity")
        self.assertEqual(normalized.returns[0].column_name, "user")
        self.assertEqual(normalized.returns[1].kind, "count")
        self.assertEqual(normalized.returns[1].column_name, "total")
        self.assertEqual(normalized.order_by[0].alias, "total")
        self.assertEqual(type(star_normalized).__name__, "NormalizedMatchNode")
        self.assertEqual(star_normalized.returns[1].kind, "count")
        self.assertEqual(star_normalized.returns[1].alias, "*")
        self.assertEqual(star_normalized.order_by[0].alias, "total")
        self.assertEqual(type(relationship_normalized).__name__, "NormalizedMatchRelationship")
        self.assertEqual(relationship_normalized.returns[0].kind, "entity")
        self.assertEqual(relationship_normalized.returns[0].column_name, "rel")
        self.assertEqual(relationship_normalized.returns[1].kind, "count")
        self.assertEqual(relationship_normalized.returns[1].alias, "r")
        self.assertEqual(relationship_normalized.order_by[0].alias, "total")
        self.assertEqual(type(relationship_star_normalized).__name__, "NormalizedMatchRelationship")
        self.assertEqual(relationship_star_normalized.returns[1].kind, "count")
        self.assertEqual(relationship_star_normalized.returns[1].alias, "*")
        self.assertEqual(relationship_star_normalized.order_by[0].alias, "total")

    def test_normalize_cypher_text_normalizes_scalar_literal_and_parameter_returns(
        self,
    ) -> None:
        literal_normalized = cypherglot.normalize_cypher_text(
            "MATCH (u:User) RETURN 'tag' AS tag ORDER BY tag"
        )
        parameter_normalized = cypherglot.normalize_cypher_text(
            "MATCH (u:User) RETURN $value AS value ORDER BY value"
        )

        self.assertEqual(literal_normalized.returns[0].kind, "scalar")
        self.assertEqual(literal_normalized.returns[0].column_name, "tag")
        self.assertEqual(literal_normalized.order_by[0].alias, "tag")
        self.assertEqual(parameter_normalized.returns[0].kind, "scalar")
        self.assertEqual(parameter_normalized.returns[0].column_name, "value")
        self.assertEqual(parameter_normalized.order_by[0].alias, "value")

    def test_normalize_cypher_text_normalizes_size_returns(self) -> None:
        field_normalized = cypherglot.normalize_cypher_text(
            "MATCH (u:User) RETURN size(u.name) AS name_len ORDER BY name_len"
        )
        literal_normalized = cypherglot.normalize_cypher_text(
            "MATCH (u:User) RETURN size('tag') AS tag_len ORDER BY tag_len"
        )
        id_normalized = cypherglot.normalize_cypher_text(
            "MATCH (u:User) RETURN size(id(u)) AS id_len ORDER BY id_len"
        )
        type_normalized = cypherglot.normalize_cypher_text(
            "MATCH (a:User)-[r:KNOWS]->(b:User) RETURN size(type(r)) AS type_len ORDER BY type_len"
        )

        self.assertEqual(field_normalized.returns[0].kind, "size")
        self.assertEqual(field_normalized.returns[0].alias, "u")
        self.assertEqual(field_normalized.returns[0].field, "name")
        self.assertEqual(field_normalized.returns[0].column_name, "name_len")
        self.assertEqual(field_normalized.order_by[0].alias, "name_len")
        self.assertEqual(literal_normalized.returns[0].kind, "size")
        self.assertEqual(literal_normalized.returns[0].column_name, "tag_len")
        self.assertEqual(literal_normalized.order_by[0].alias, "tag_len")
        self.assertEqual(id_normalized.returns[0].kind, "size")
        self.assertEqual(id_normalized.returns[0].field, "id")
        self.assertEqual(id_normalized.returns[0].column_name, "id_len")
        self.assertEqual(type_normalized.returns[0].kind, "size")
        self.assertEqual(type_normalized.returns[0].field, "type")
        self.assertEqual(type_normalized.returns[0].column_name, "type_len")

    def test_normalize_cypher_text_normalizes_lower_and_upper_returns(self) -> None:
        lower_normalized = cypherglot.normalize_cypher_text(
            "MATCH (u:User) RETURN lower(u.name) AS lower_name ORDER BY lower_name"
        )
        upper_normalized = cypherglot.normalize_cypher_text(
            "MATCH (u:User) RETURN upper('tag') AS upper_tag ORDER BY upper_tag"
        )
        trim_normalized = cypherglot.normalize_cypher_text(
            "MATCH (u:User) RETURN trim(u.name) AS trimmed ORDER BY trimmed"
        )
        ltrim_normalized = cypherglot.normalize_cypher_text(
            "MATCH (u:User) RETURN ltrim(' tag') AS left_trimmed ORDER BY left_trimmed"
        )
        rtrim_normalized = cypherglot.normalize_cypher_text(
            "MATCH (u:User) RETURN rtrim('tag ') AS right_trimmed ORDER BY right_trimmed"
        )
        reverse_normalized = cypherglot.normalize_cypher_text(
            "MATCH (u:User) RETURN reverse(u.name) AS reversed_name ORDER BY reversed_name"
        )

        self.assertEqual(lower_normalized.returns[0].kind, "lower")
        self.assertEqual(lower_normalized.returns[0].alias, "u")
        self.assertEqual(lower_normalized.returns[0].field, "name")
        self.assertEqual(lower_normalized.returns[0].column_name, "lower_name")
        self.assertEqual(lower_normalized.order_by[0].alias, "lower_name")
        self.assertEqual(upper_normalized.returns[0].kind, "upper")
        self.assertEqual(upper_normalized.returns[0].column_name, "upper_tag")
        self.assertEqual(upper_normalized.returns[0].value, "tag")
        self.assertEqual(upper_normalized.order_by[0].alias, "upper_tag")
        self.assertEqual(trim_normalized.returns[0].kind, "trim")
        self.assertEqual(trim_normalized.returns[0].alias, "u")
        self.assertEqual(trim_normalized.returns[0].field, "name")
        self.assertEqual(trim_normalized.returns[0].column_name, "trimmed")
        self.assertEqual(trim_normalized.order_by[0].alias, "trimmed")
        self.assertEqual(ltrim_normalized.returns[0].kind, "ltrim")
        self.assertEqual(ltrim_normalized.returns[0].column_name, "left_trimmed")
        self.assertEqual(ltrim_normalized.returns[0].value, " tag")
        self.assertEqual(ltrim_normalized.order_by[0].alias, "left_trimmed")
        self.assertEqual(rtrim_normalized.returns[0].kind, "rtrim")
        self.assertEqual(rtrim_normalized.returns[0].column_name, "right_trimmed")
        self.assertEqual(rtrim_normalized.returns[0].value, "tag ")
        self.assertEqual(rtrim_normalized.order_by[0].alias, "right_trimmed")
        self.assertEqual(reverse_normalized.returns[0].kind, "reverse")
        self.assertEqual(reverse_normalized.returns[0].alias, "u")
        self.assertEqual(reverse_normalized.returns[0].field, "name")
        self.assertEqual(reverse_normalized.returns[0].column_name, "reversed_name")
        self.assertEqual(reverse_normalized.order_by[0].alias, "reversed_name")

    def test_normalize_cypher_text_normalizes_relationship_property_string_returns(self) -> None:
        normalized = cypherglot.normalize_cypher_text(
            "MATCH (a:User)-[r:KNOWS]->(b:User) RETURN lower(r.note) AS lower_note, upper(r.note) AS upper_note, trim(r.note) AS trimmed_note, ltrim(r.note) AS left_trimmed_note, rtrim(r.note) AS right_trimmed_note, reverse(r.note) AS reversed_note, coalesce(r.note, 'unknown') AS display_note, replace(r.note, 'A', 'B') AS replaced_note, left(r.note, 2) AS prefix, right(r.note, 2) AS suffix, split(r.note, ' ') AS parts, substring(r.note, 1) AS tail ORDER BY lower_note, upper_note, trimmed_note, left_trimmed_note, right_trimmed_note, reversed_note, display_note, replaced_note, prefix, suffix, parts, tail"
        )

        self.assertEqual(type(normalized).__name__, "NormalizedMatchRelationship")
        self.assertEqual(normalized.returns[0].kind, "lower")
        self.assertEqual(normalized.returns[0].alias, "r")
        self.assertEqual(normalized.returns[0].field, "note")
        self.assertEqual(normalized.returns[5].kind, "reverse")
        self.assertEqual(normalized.returns[6].kind, "coalesce")
        self.assertEqual(normalized.returns[7].kind, "replace")
        self.assertEqual(normalized.returns[8].kind, "left")
        self.assertEqual(normalized.returns[9].kind, "right")
        self.assertEqual(normalized.returns[10].kind, "split")
        self.assertEqual(normalized.returns[11].kind, "substring")
        self.assertEqual(normalized.order_by[0].alias, "lower_note")
        self.assertEqual(normalized.order_by[-1].alias, "tail")

    def test_normalize_cypher_text_normalizes_relationship_property_numeric_and_conversion_returns(
        self,
    ) -> None:
        normalized = cypherglot.normalize_cypher_text(
            "MATCH (a:User)-[r:KNOWS]->(b:User) RETURN abs(r.weight) AS magnitude, sign(r.weight) AS weight_sign, round(r.score) AS rounded_score, ceil(r.score) AS ceil_score, floor(r.score) AS floor_score, sqrt(r.score) AS sqrt_score, exp(r.score) AS exp_score, sin(r.score) AS sin_score, toString(r.weight) AS weight_text, toInteger(r.score) AS score_int, toFloat(r.weight) AS weight_float, toBoolean(r.active) AS active_bool ORDER BY magnitude, weight_sign, rounded_score, ceil_score, floor_score, sqrt_score, exp_score, sin_score, weight_text, score_int, weight_float, active_bool"
        )

        self.assertEqual(type(normalized).__name__, "NormalizedMatchRelationship")
        self.assertEqual(normalized.returns[0].kind, "abs")
        self.assertEqual(normalized.returns[0].alias, "r")
        self.assertEqual(normalized.returns[0].field, "weight")
        self.assertEqual(normalized.returns[1].kind, "sign")
        self.assertEqual(normalized.returns[2].kind, "round")
        self.assertEqual(normalized.returns[3].kind, "ceil")
        self.assertEqual(normalized.returns[4].kind, "floor")
        self.assertEqual(normalized.returns[5].kind, "sqrt")
        self.assertEqual(normalized.returns[6].kind, "exp")
        self.assertEqual(normalized.returns[7].kind, "sin")
        self.assertEqual(normalized.returns[8].kind, "to_string")
        self.assertEqual(normalized.returns[9].kind, "to_integer")
        self.assertEqual(normalized.returns[10].kind, "to_float")
        self.assertEqual(normalized.returns[11].kind, "to_boolean")
        self.assertEqual(normalized.order_by[0].alias, "magnitude")
        self.assertEqual(normalized.order_by[-1].alias, "active_bool")

    def test_normalize_cypher_text_normalizes_coalesce_returns(self) -> None:
        field_normalized = cypherglot.normalize_cypher_text(
            "MATCH (u:User) RETURN coalesce(u.name, 'unknown') AS display_name ORDER BY display_name"
        )
        no_alias_normalized = cypherglot.normalize_cypher_text(
            "MATCH (u:User) RETURN coalesce(u.name, 'unknown')"
        )
        optional_normalized = cypherglot.normalize_cypher_text(
            "OPTIONAL MATCH (u:User) RETURN coalesce(u.name, $fallback) AS display_name ORDER BY display_name"
        )

        self.assertEqual(field_normalized.returns[0].kind, "coalesce")
        self.assertEqual(field_normalized.returns[0].alias, "u")
        self.assertEqual(field_normalized.returns[0].field, "name")
        self.assertEqual(field_normalized.returns[0].value, "unknown")
        self.assertEqual(field_normalized.returns[0].column_name, "display_name")
        self.assertEqual(field_normalized.order_by[0].alias, "display_name")
        self.assertEqual(
            no_alias_normalized.returns[0].column_name,
            "coalesce(u.name, 'unknown')",
        )
        self.assertEqual(optional_normalized.returns[0].kind, "coalesce")
        self.assertEqual(optional_normalized.returns[0].value.name, "fallback")

    def test_normalize_cypher_text_normalizes_replace_returns(self) -> None:
        field_normalized = cypherglot.normalize_cypher_text(
            "MATCH (u:User) RETURN replace(u.name, 'A', 'B') AS display_name ORDER BY display_name"
        )
        no_alias_normalized = cypherglot.normalize_cypher_text(
            "MATCH (u:User) RETURN replace(u.name, 'A', 'B')"
        )
        literal_normalized = cypherglot.normalize_cypher_text(
            "MATCH (u:User) RETURN replace('Alice', 'l', 'x') AS alias_name ORDER BY alias_name"
        )

        self.assertEqual(field_normalized.returns[0].kind, "replace")
        self.assertEqual(field_normalized.returns[0].alias, "u")
        self.assertEqual(field_normalized.returns[0].field, "name")
        self.assertEqual(field_normalized.returns[0].search_value, "A")
        self.assertEqual(field_normalized.returns[0].replace_value, "B")
        self.assertEqual(field_normalized.returns[0].column_name, "display_name")
        self.assertEqual(field_normalized.order_by[0].alias, "display_name")
        self.assertEqual(
            no_alias_normalized.returns[0].column_name,
            "replace(u.name, 'A', 'B')",
        )
        self.assertEqual(literal_normalized.returns[0].kind, "replace")
        self.assertEqual(literal_normalized.returns[0].value, "Alice")
        self.assertEqual(literal_normalized.returns[0].search_value, "l")
        self.assertEqual(literal_normalized.returns[0].replace_value, "x")

    def test_normalize_cypher_text_normalizes_left_and_right_returns(self) -> None:
        left_normalized = cypherglot.normalize_cypher_text(
            "MATCH (u:User) RETURN left(u.name, 2) AS prefix ORDER BY prefix"
        )
        no_alias_normalized = cypherglot.normalize_cypher_text(
            "MATCH (u:User) RETURN left(u.name, 2)"
        )
        right_normalized = cypherglot.normalize_cypher_text(
            "MATCH (u:User) RETURN right('Alice', 3) AS suffix ORDER BY suffix"
        )

        self.assertEqual(left_normalized.returns[0].kind, "left")
        self.assertEqual(left_normalized.returns[0].alias, "u")
        self.assertEqual(left_normalized.returns[0].field, "name")
        self.assertEqual(left_normalized.returns[0].length_value, 2)
        self.assertEqual(left_normalized.order_by[0].alias, "prefix")
        self.assertEqual(no_alias_normalized.returns[0].column_name, "left(u.name, 2)")
        self.assertEqual(right_normalized.returns[0].kind, "right")
        self.assertEqual(right_normalized.returns[0].value, "Alice")
        self.assertEqual(right_normalized.returns[0].length_value, 3)

    def test_normalize_cypher_text_normalizes_split_returns(self) -> None:
        field_normalized = cypherglot.normalize_cypher_text(
            "MATCH (u:User) RETURN split(u.name, ' ') AS parts ORDER BY parts"
        )
        no_alias_normalized = cypherglot.normalize_cypher_text(
            "MATCH (u:User) RETURN split(u.name, ' ')"
        )
        literal_normalized = cypherglot.normalize_cypher_text(
            "MATCH (u:User) RETURN split('Alice Bob', $delimiter) AS parts ORDER BY parts"
        )

        self.assertEqual(field_normalized.returns[0].kind, "split")
        self.assertEqual(field_normalized.returns[0].alias, "u")
        self.assertEqual(field_normalized.returns[0].field, "name")
        self.assertEqual(field_normalized.returns[0].delimiter_value, " ")
        self.assertEqual(field_normalized.order_by[0].alias, "parts")
        self.assertEqual(no_alias_normalized.returns[0].column_name, "split(u.name, ' ')")
        self.assertEqual(literal_normalized.returns[0].kind, "split")
        self.assertEqual(literal_normalized.returns[0].value, "Alice Bob")
        self.assertEqual(literal_normalized.returns[0].delimiter_value.name, "delimiter")

    def test_normalize_cypher_text_normalizes_abs_returns(self) -> None:
        field_normalized = cypherglot.normalize_cypher_text(
            "MATCH (u:User) RETURN abs(u.age) AS magnitude ORDER BY magnitude"
        )
        literal_normalized = cypherglot.normalize_cypher_text(
            "MATCH (u:User) RETURN abs(-3) AS magnitude ORDER BY magnitude"
        )
        sign_normalized = cypherglot.normalize_cypher_text(
            "MATCH (u:User) RETURN sign(u.age) AS age_sign ORDER BY age_sign"
        )
        sign_literal_normalized = cypherglot.normalize_cypher_text(
            "MATCH (u:User) RETURN sign(-3.2) AS age_sign ORDER BY age_sign"
        )

        self.assertEqual(field_normalized.returns[0].kind, "abs")
        self.assertEqual(field_normalized.returns[0].alias, "u")
        self.assertEqual(field_normalized.returns[0].field, "age")
        self.assertEqual(field_normalized.returns[0].column_name, "magnitude")
        self.assertEqual(field_normalized.order_by[0].alias, "magnitude")
        self.assertEqual(literal_normalized.returns[0].kind, "abs")
        self.assertEqual(literal_normalized.returns[0].value, -3)
        self.assertEqual(sign_normalized.returns[0].kind, "sign")
        self.assertEqual(sign_normalized.returns[0].alias, "u")
        self.assertEqual(sign_normalized.returns[0].field, "age")
        self.assertEqual(sign_normalized.returns[0].column_name, "age_sign")
        self.assertEqual(sign_normalized.order_by[0].alias, "age_sign")
        self.assertEqual(sign_literal_normalized.returns[0].kind, "sign")
        self.assertEqual(sign_literal_normalized.returns[0].value, -3.2)

    def test_normalize_cypher_text_normalizes_round_returns(self) -> None:
        field_normalized = cypherglot.normalize_cypher_text(
            "MATCH (u:User) RETURN round(u.score) AS value ORDER BY value"
        )
        literal_normalized = cypherglot.normalize_cypher_text(
            "MATCH (u:User) RETURN round(-3.2) AS value ORDER BY value"
        )

        self.assertEqual(field_normalized.returns[0].kind, "round")
        self.assertEqual(field_normalized.returns[0].alias, "u")
        self.assertEqual(field_normalized.returns[0].field, "score")
        self.assertEqual(field_normalized.returns[0].column_name, "value")
        self.assertEqual(field_normalized.order_by[0].alias, "value")
        self.assertEqual(literal_normalized.returns[0].kind, "round")
        self.assertEqual(literal_normalized.returns[0].value, -3.2)

    def test_normalize_cypher_text_normalizes_ceil_returns(self) -> None:
        field_normalized = cypherglot.normalize_cypher_text(
            "MATCH (u:User) RETURN ceil(u.score) AS value ORDER BY value"
        )
        literal_normalized = cypherglot.normalize_cypher_text(
            "MATCH (u:User) RETURN ceil(-3.2) AS value ORDER BY value"
        )

        self.assertEqual(field_normalized.returns[0].kind, "ceil")
        self.assertEqual(field_normalized.returns[0].alias, "u")
        self.assertEqual(field_normalized.returns[0].field, "score")
        self.assertEqual(field_normalized.returns[0].column_name, "value")
        self.assertEqual(field_normalized.order_by[0].alias, "value")
        self.assertEqual(literal_normalized.returns[0].kind, "ceil")
        self.assertEqual(literal_normalized.returns[0].value, -3.2)

    def test_normalize_cypher_text_normalizes_floor_returns(self) -> None:
        field_normalized = cypherglot.normalize_cypher_text(
            "MATCH (u:User) RETURN floor(u.score) AS value ORDER BY value"
        )
        literal_normalized = cypherglot.normalize_cypher_text(
            "MATCH (u:User) RETURN floor(-3.2) AS value ORDER BY value"
        )

        self.assertEqual(field_normalized.returns[0].kind, "floor")
        self.assertEqual(field_normalized.returns[0].alias, "u")
        self.assertEqual(field_normalized.returns[0].field, "score")
        self.assertEqual(field_normalized.returns[0].column_name, "value")
        self.assertEqual(field_normalized.order_by[0].alias, "value")
        self.assertEqual(literal_normalized.returns[0].kind, "floor")
        self.assertEqual(literal_normalized.returns[0].value, -3.2)

    def test_normalize_cypher_text_normalizes_sqrt_returns(self) -> None:
        field_normalized = cypherglot.normalize_cypher_text(
            "MATCH (u:User) RETURN sqrt(u.score) AS value ORDER BY value"
        )
        literal_normalized = cypherglot.normalize_cypher_text(
            "MATCH (u:User) RETURN sqrt(-3.2) AS value ORDER BY value"
        )

        self.assertEqual(field_normalized.returns[0].kind, "sqrt")
        self.assertEqual(field_normalized.returns[0].alias, "u")
        self.assertEqual(field_normalized.returns[0].field, "score")
        self.assertEqual(field_normalized.returns[0].column_name, "value")
        self.assertEqual(field_normalized.order_by[0].alias, "value")
        self.assertEqual(literal_normalized.returns[0].kind, "sqrt")
        self.assertEqual(literal_normalized.returns[0].value, -3.2)

    def test_normalize_cypher_text_normalizes_exp_returns(self) -> None:
        field_normalized = cypherglot.normalize_cypher_text(
            "MATCH (u:User) RETURN exp(u.score) AS value ORDER BY value"
        )
        literal_normalized = cypherglot.normalize_cypher_text(
            "MATCH (u:User) RETURN exp(-3.2) AS value ORDER BY value"
        )

        self.assertEqual(field_normalized.returns[0].kind, "exp")
        self.assertEqual(field_normalized.returns[0].alias, "u")
        self.assertEqual(field_normalized.returns[0].field, "score")
        self.assertEqual(field_normalized.returns[0].column_name, "value")
        self.assertEqual(field_normalized.order_by[0].alias, "value")
        self.assertEqual(literal_normalized.returns[0].kind, "exp")
        self.assertEqual(literal_normalized.returns[0].value, -3.2)

    def test_normalize_cypher_text_normalizes_sin_returns(self) -> None:
        field_normalized = cypherglot.normalize_cypher_text(
            "MATCH (u:User) RETURN sin(u.score) AS value ORDER BY value"
        )
        literal_normalized = cypherglot.normalize_cypher_text(
            "MATCH (u:User) RETURN sin(-3.2) AS value ORDER BY value"
        )

        self.assertEqual(field_normalized.returns[0].kind, "sin")
        self.assertEqual(field_normalized.returns[0].alias, "u")
        self.assertEqual(field_normalized.returns[0].field, "score")
        self.assertEqual(field_normalized.returns[0].column_name, "value")
        self.assertEqual(field_normalized.order_by[0].alias, "value")
        self.assertEqual(literal_normalized.returns[0].kind, "sin")
        self.assertEqual(literal_normalized.returns[0].value, -3.2)

    def test_normalize_cypher_text_normalizes_cos_returns(self) -> None:
        field_normalized = cypherglot.normalize_cypher_text(
            "MATCH (u:User) RETURN cos(u.score) AS value ORDER BY value"
        )
        literal_normalized = cypherglot.normalize_cypher_text(
            "MATCH (u:User) RETURN cos(-3.2) AS value ORDER BY value"
        )

        self.assertEqual(field_normalized.returns[0].kind, "cos")
        self.assertEqual(field_normalized.returns[0].alias, "u")
        self.assertEqual(field_normalized.returns[0].field, "score")
        self.assertEqual(field_normalized.returns[0].column_name, "value")
        self.assertEqual(field_normalized.order_by[0].alias, "value")
        self.assertEqual(literal_normalized.returns[0].kind, "cos")
        self.assertEqual(literal_normalized.returns[0].value, -3.2)

    def test_normalize_cypher_text_normalizes_tan_returns(self) -> None:
        field_normalized = cypherglot.normalize_cypher_text(
            "MATCH (u:User) RETURN tan(u.score) AS value ORDER BY value"
        )
        literal_normalized = cypherglot.normalize_cypher_text(
            "MATCH (u:User) RETURN tan(-3.2) AS value ORDER BY value"
        )

        self.assertEqual(field_normalized.returns[0].kind, "tan")
        self.assertEqual(field_normalized.returns[0].alias, "u")
        self.assertEqual(field_normalized.returns[0].field, "score")
        self.assertEqual(field_normalized.returns[0].column_name, "value")
        self.assertEqual(field_normalized.order_by[0].alias, "value")
        self.assertEqual(literal_normalized.returns[0].kind, "tan")
        self.assertEqual(literal_normalized.returns[0].value, -3.2)

    def test_normalize_cypher_text_normalizes_asin_returns(self) -> None:
        field_normalized = cypherglot.normalize_cypher_text(
            "MATCH (u:User) RETURN asin(u.score) AS value ORDER BY value"
        )
        literal_normalized = cypherglot.normalize_cypher_text(
            "MATCH (u:User) RETURN asin(-0.5) AS value ORDER BY value"
        )

        self.assertEqual(field_normalized.returns[0].kind, "asin")
        self.assertEqual(field_normalized.returns[0].alias, "u")
        self.assertEqual(field_normalized.returns[0].field, "score")
        self.assertEqual(field_normalized.returns[0].column_name, "value")
        self.assertEqual(field_normalized.order_by[0].alias, "value")
        self.assertEqual(literal_normalized.returns[0].kind, "asin")
        self.assertEqual(literal_normalized.returns[0].value, -0.5)

    def test_normalize_cypher_text_normalizes_acos_returns(self) -> None:
        field_normalized = cypherglot.normalize_cypher_text(
            "MATCH (u:User) RETURN acos(u.score) AS value ORDER BY value"
        )
        literal_normalized = cypherglot.normalize_cypher_text(
            "MATCH (u:User) RETURN acos(-0.5) AS value ORDER BY value"
        )

        self.assertEqual(field_normalized.returns[0].kind, "acos")
        self.assertEqual(field_normalized.returns[0].alias, "u")
        self.assertEqual(field_normalized.returns[0].field, "score")
        self.assertEqual(field_normalized.returns[0].column_name, "value")
        self.assertEqual(field_normalized.order_by[0].alias, "value")
        self.assertEqual(literal_normalized.returns[0].kind, "acos")
        self.assertEqual(literal_normalized.returns[0].value, -0.5)

    def test_normalize_cypher_text_normalizes_atan_returns(self) -> None:
        field_normalized = cypherglot.normalize_cypher_text(
            "MATCH (u:User) RETURN atan(u.score) AS value ORDER BY value"
        )
        literal_normalized = cypherglot.normalize_cypher_text(
            "MATCH (u:User) RETURN atan(-0.5) AS value ORDER BY value"
        )

        self.assertEqual(field_normalized.returns[0].kind, "atan")
        self.assertEqual(field_normalized.returns[0].alias, "u")
        self.assertEqual(field_normalized.returns[0].field, "score")
        self.assertEqual(field_normalized.returns[0].column_name, "value")
        self.assertEqual(field_normalized.order_by[0].alias, "value")
        self.assertEqual(literal_normalized.returns[0].kind, "atan")
        self.assertEqual(literal_normalized.returns[0].value, -0.5)

    def test_normalize_cypher_text_normalizes_ln_returns(self) -> None:
        field_normalized = cypherglot.normalize_cypher_text(
            "MATCH (u:User) RETURN ln(u.score) AS value ORDER BY value"
        )
        literal_normalized = cypherglot.normalize_cypher_text(
            "MATCH (u:User) RETURN ln(0.5) AS value ORDER BY value"
        )

        self.assertEqual(field_normalized.returns[0].kind, "ln")
        self.assertEqual(field_normalized.returns[0].alias, "u")
        self.assertEqual(field_normalized.returns[0].field, "score")
        self.assertEqual(field_normalized.returns[0].column_name, "value")
        self.assertEqual(field_normalized.order_by[0].alias, "value")
        self.assertEqual(literal_normalized.returns[0].kind, "ln")
        self.assertEqual(literal_normalized.returns[0].value, 0.5)

    def test_normalize_cypher_text_normalizes_log_returns(self) -> None:
        field_normalized = cypherglot.normalize_cypher_text(
            "MATCH (u:User) RETURN log(u.score) AS value ORDER BY value"
        )
        literal_normalized = cypherglot.normalize_cypher_text(
            "MATCH (u:User) RETURN log(0.5) AS value ORDER BY value"
        )

        self.assertEqual(field_normalized.returns[0].kind, "log")
        self.assertEqual(field_normalized.returns[0].alias, "u")
        self.assertEqual(field_normalized.returns[0].field, "score")
        self.assertEqual(field_normalized.returns[0].column_name, "value")
        self.assertEqual(field_normalized.order_by[0].alias, "value")
        self.assertEqual(literal_normalized.returns[0].kind, "log")
        self.assertEqual(literal_normalized.returns[0].value, 0.5)

    def test_normalize_cypher_text_normalizes_radians_returns(self) -> None:
        field_normalized = cypherglot.normalize_cypher_text(
            "MATCH (u:User) RETURN radians(u.score) AS value ORDER BY value"
        )
        literal_normalized = cypherglot.normalize_cypher_text(
            "MATCH (u:User) RETURN radians(180) AS value ORDER BY value"
        )

        self.assertEqual(field_normalized.returns[0].kind, "radians")
        self.assertEqual(field_normalized.returns[0].alias, "u")
        self.assertEqual(field_normalized.returns[0].field, "score")
        self.assertEqual(field_normalized.returns[0].column_name, "value")
        self.assertEqual(field_normalized.order_by[0].alias, "value")
        self.assertEqual(literal_normalized.returns[0].kind, "radians")
        self.assertEqual(literal_normalized.returns[0].value, 180)

    def test_normalize_cypher_text_normalizes_degrees_returns(self) -> None:
        field_normalized = cypherglot.normalize_cypher_text(
            "MATCH (u:User) RETURN degrees(u.score) AS value ORDER BY value"
        )
        literal_normalized = cypherglot.normalize_cypher_text(
            "MATCH (u:User) RETURN degrees(3.14159) AS value ORDER BY value"
        )

        self.assertEqual(field_normalized.returns[0].kind, "degrees")
        self.assertEqual(field_normalized.returns[0].alias, "u")
        self.assertEqual(field_normalized.returns[0].field, "score")
        self.assertEqual(field_normalized.returns[0].column_name, "value")
        self.assertEqual(field_normalized.order_by[0].alias, "value")
        self.assertEqual(literal_normalized.returns[0].kind, "degrees")
        self.assertEqual(literal_normalized.returns[0].value, 3.14159)

    def test_normalize_cypher_text_normalizes_log10_returns(self) -> None:
        field_normalized = cypherglot.normalize_cypher_text(
            "MATCH (u:User) RETURN log10(u.score) AS value ORDER BY value"
        )
        literal_normalized = cypherglot.normalize_cypher_text(
            "MATCH (u:User) RETURN log10(0.5) AS value ORDER BY value"
        )

        self.assertEqual(field_normalized.returns[0].kind, "log10")
        self.assertEqual(field_normalized.returns[0].alias, "u")
        self.assertEqual(field_normalized.returns[0].field, "score")
        self.assertEqual(field_normalized.returns[0].column_name, "value")
        self.assertEqual(field_normalized.order_by[0].alias, "value")
        self.assertEqual(literal_normalized.returns[0].kind, "log10")
        self.assertEqual(literal_normalized.returns[0].value, 0.5)

    def test_normalize_cypher_text_normalizes_to_string_returns(self) -> None:
        field_normalized = cypherglot.normalize_cypher_text(
            "MATCH (u:User) RETURN toString(u.age) AS text ORDER BY text"
        )
        literal_normalized = cypherglot.normalize_cypher_text(
            "MATCH (u:User) RETURN toString(-3) AS text ORDER BY text"
        )

        self.assertEqual(field_normalized.returns[0].kind, "to_string")
        self.assertEqual(field_normalized.returns[0].alias, "u")
        self.assertEqual(field_normalized.returns[0].field, "age")
        self.assertEqual(field_normalized.returns[0].column_name, "text")
        self.assertEqual(field_normalized.order_by[0].alias, "text")
        self.assertEqual(literal_normalized.returns[0].kind, "to_string")
        self.assertEqual(literal_normalized.returns[0].value, -3)

    def test_normalize_cypher_text_normalizes_to_integer_returns(self) -> None:
        field_normalized = cypherglot.normalize_cypher_text(
            "MATCH (u:User) RETURN toInteger(u.age) AS age_int ORDER BY age_int"
        )
        literal_normalized = cypherglot.normalize_cypher_text(
            "MATCH (u:User) RETURN toInteger(-3.2) AS age_int ORDER BY age_int"
        )

        self.assertEqual(field_normalized.returns[0].kind, "to_integer")
        self.assertEqual(field_normalized.returns[0].alias, "u")
        self.assertEqual(field_normalized.returns[0].field, "age")
        self.assertEqual(field_normalized.returns[0].column_name, "age_int")
        self.assertEqual(field_normalized.order_by[0].alias, "age_int")
        self.assertEqual(literal_normalized.returns[0].kind, "to_integer")
        self.assertEqual(literal_normalized.returns[0].value, -3.2)

    def test_normalize_cypher_text_normalizes_to_float_returns(self) -> None:
        field_normalized = cypherglot.normalize_cypher_text(
            "MATCH (u:User) RETURN toFloat(u.age) AS age_float ORDER BY age_float"
        )
        literal_normalized = cypherglot.normalize_cypher_text(
            "MATCH (u:User) RETURN toFloat(-3) AS age_float ORDER BY age_float"
        )

        self.assertEqual(field_normalized.returns[0].kind, "to_float")
        self.assertEqual(field_normalized.returns[0].alias, "u")
        self.assertEqual(field_normalized.returns[0].field, "age")
        self.assertEqual(field_normalized.returns[0].column_name, "age_float")
        self.assertEqual(field_normalized.order_by[0].alias, "age_float")
        self.assertEqual(literal_normalized.returns[0].kind, "to_float")
        self.assertEqual(literal_normalized.returns[0].value, -3)

    def test_normalize_cypher_text_normalizes_to_boolean_returns(self) -> None:
        field_normalized = cypherglot.normalize_cypher_text(
            "MATCH (u:User) RETURN toBoolean(u.active) AS is_active ORDER BY is_active"
        )
        literal_normalized = cypherglot.normalize_cypher_text(
            "MATCH (u:User) RETURN toBoolean(true) AS is_active ORDER BY is_active"
        )

        self.assertEqual(field_normalized.returns[0].kind, "to_boolean")
        self.assertEqual(field_normalized.returns[0].alias, "u")
        self.assertEqual(field_normalized.returns[0].field, "active")
        self.assertEqual(field_normalized.returns[0].column_name, "is_active")
        self.assertEqual(field_normalized.order_by[0].alias, "is_active")
        self.assertEqual(literal_normalized.returns[0].kind, "to_boolean")
        self.assertEqual(literal_normalized.returns[0].value, True)

    def test_normalize_cypher_text_normalizes_substring_returns(self) -> None:
        field_normalized = cypherglot.normalize_cypher_text(
            "MATCH (u:User) RETURN substring(u.name, 0, 2) AS prefix ORDER BY prefix"
        )
        no_alias_normalized = cypherglot.normalize_cypher_text(
            "MATCH (u:User) RETURN substring(u.name, 0, 2)"
        )
        field_two_arg_normalized = cypherglot.normalize_cypher_text(
            "MATCH (u:User) RETURN substring(u.name, 1) AS suffix ORDER BY suffix"
        )
        literal_normalized = cypherglot.normalize_cypher_text(
            "MATCH (u:User) RETURN substring('Alice', 1, 3) AS prefix ORDER BY prefix"
        )
        literal_two_arg_normalized = cypherglot.normalize_cypher_text(
            "MATCH (u:User) RETURN substring('Alice', 2) AS suffix ORDER BY suffix"
        )

        self.assertEqual(field_normalized.returns[0].kind, "substring")
        self.assertEqual(field_normalized.returns[0].alias, "u")
        self.assertEqual(field_normalized.returns[0].field, "name")
        self.assertEqual(field_normalized.returns[0].start_value, 0)
        self.assertEqual(field_normalized.returns[0].length_value, 2)
        self.assertEqual(field_normalized.returns[0].column_name, "prefix")
        self.assertEqual(field_normalized.order_by[0].alias, "prefix")
        self.assertEqual(
            no_alias_normalized.returns[0].column_name,
            "substring(u.name, 0, 2)",
        )
        self.assertEqual(field_two_arg_normalized.returns[0].kind, "substring")
        self.assertEqual(field_two_arg_normalized.returns[0].start_value, 1)
        self.assertIsNone(field_two_arg_normalized.returns[0].length_value)
        self.assertEqual(field_two_arg_normalized.returns[0].column_name, "suffix")
        self.assertEqual(literal_normalized.returns[0].kind, "substring")
        self.assertEqual(literal_normalized.returns[0].value, "Alice")
        self.assertEqual(literal_normalized.returns[0].start_value, 1)
        self.assertEqual(literal_normalized.returns[0].length_value, 3)
        self.assertEqual(literal_two_arg_normalized.returns[0].kind, "substring")
        self.assertEqual(literal_two_arg_normalized.returns[0].value, "Alice")
        self.assertEqual(literal_two_arg_normalized.returns[0].start_value, 2)
        self.assertIsNone(literal_two_arg_normalized.returns[0].length_value)

    def test_normalize_cypher_text_normalizes_predicate_returns(self) -> None:
        field_normalized = cypherglot.normalize_cypher_text(
            "MATCH (u:User) RETURN u.age >= 18 AS adult ORDER BY adult"
        )
        string_normalized = cypherglot.normalize_cypher_text(
            "MATCH (u:User) RETURN u.name CONTAINS 'a' AS has_a ORDER BY has_a"
        )
        starts_with_normalized = cypherglot.normalize_cypher_text(
            "MATCH (u:User) RETURN u.name STARTS WITH 'Al' AS has_prefix ORDER BY has_prefix"
        )
        ends_with_normalized = cypherglot.normalize_cypher_text(
            "MATCH (u:User) RETURN u.name ENDS WITH 'ce' AS has_suffix ORDER BY has_suffix"
        )
        id_normalized = cypherglot.normalize_cypher_text(
            "MATCH (u:User) RETURN id(u) >= 1 AS has_id ORDER BY has_id"
        )
        type_normalized = cypherglot.normalize_cypher_text(
            "MATCH (a:User)-[r:KNOWS]->(b:User) RETURN type(r) = 'KNOWS' AS is_knows ORDER BY is_knows"
        )
        size_normalized = cypherglot.normalize_cypher_text(
            "MATCH (u:User) RETURN size(u.name) >= 3 AS long_name ORDER BY long_name"
        )
        size_id_normalized = cypherglot.normalize_cypher_text(
            "MATCH (u:User) RETURN size(id(u)) >= 1 AS long_id ORDER BY long_id"
        )
        size_type_normalized = cypherglot.normalize_cypher_text(
            "MATCH (a:User)-[r:KNOWS]->(b:User) RETURN size(type(r)) >= 5 AS long_type ORDER BY long_type"
        )

        self.assertEqual(field_normalized.returns[0].kind, "predicate")
        self.assertEqual(field_normalized.returns[0].alias, "u")
        self.assertEqual(field_normalized.returns[0].field, "age")
        self.assertEqual(field_normalized.returns[0].operator, ">=")
        self.assertEqual(field_normalized.returns[0].column_name, "adult")
        self.assertEqual(string_normalized.returns[0].kind, "predicate")
        self.assertEqual(string_normalized.returns[0].operator, "CONTAINS")
        self.assertEqual(string_normalized.returns[0].column_name, "has_a")
        self.assertEqual(starts_with_normalized.returns[0].kind, "predicate")
        self.assertEqual(starts_with_normalized.returns[0].operator, "STARTS WITH")
        self.assertEqual(starts_with_normalized.returns[0].column_name, "has_prefix")
        self.assertEqual(ends_with_normalized.returns[0].kind, "predicate")
        self.assertEqual(ends_with_normalized.returns[0].operator, "ENDS WITH")
        self.assertEqual(ends_with_normalized.returns[0].column_name, "has_suffix")
        self.assertEqual(id_normalized.returns[0].kind, "predicate")
        self.assertEqual(id_normalized.returns[0].field, "id")
        self.assertEqual(id_normalized.returns[0].column_name, "has_id")
        self.assertEqual(type_normalized.returns[0].kind, "predicate")
        self.assertEqual(type_normalized.returns[0].field, "type")
        self.assertEqual(type_normalized.returns[0].column_name, "is_knows")
        self.assertEqual(size_normalized.returns[0].kind, "predicate")
        self.assertEqual(size_normalized.returns[0].alias, "u")
        self.assertEqual(size_normalized.returns[0].field, "__size__:name")
        self.assertEqual(size_normalized.returns[0].column_name, "long_name")
        self.assertEqual(size_id_normalized.returns[0].kind, "predicate")
        self.assertEqual(size_id_normalized.returns[0].alias, "u")
        self.assertEqual(size_id_normalized.returns[0].field, "__size__:id")
        self.assertEqual(size_id_normalized.returns[0].column_name, "long_id")
        self.assertEqual(size_type_normalized.returns[0].kind, "predicate")
        self.assertEqual(size_type_normalized.returns[0].alias, "r")
        self.assertEqual(size_type_normalized.returns[0].field, "__size__:type")
        self.assertEqual(size_type_normalized.returns[0].column_name, "long_type")

    def test_normalize_cypher_text_normalizes_relationship_property_predicate_returns(
        self,
    ) -> None:
        normalized = cypherglot.normalize_cypher_text(
            "MATCH (a:User)-[r:KNOWS]->(b:User) RETURN r.weight >= 1 AS heavy, r.note CONTAINS 'a' AS has_a, r.note STARTS WITH 'Al' AS has_prefix, r.note ENDS WITH 'ce' AS has_suffix, size(r.note) >= 3 AS long_note ORDER BY heavy, has_a, has_prefix, has_suffix, long_note"
        )

        self.assertEqual(type(normalized).__name__, "NormalizedMatchRelationship")
        self.assertEqual(normalized.returns[0].kind, "predicate")
        self.assertEqual(normalized.returns[0].alias, "r")
        self.assertEqual(normalized.returns[0].field, "weight")
        self.assertEqual(normalized.returns[1].operator, "CONTAINS")
        self.assertEqual(normalized.returns[2].operator, "STARTS WITH")
        self.assertEqual(normalized.returns[3].operator, "ENDS WITH")
        self.assertEqual(normalized.returns[4].field, "__size__:note")
        self.assertEqual(normalized.order_by[0].alias, "heavy")
        self.assertEqual(normalized.order_by[-1].alias, "long_note")

    def test_normalize_cypher_text_normalizes_where_string_and_null_filters(self) -> None:
        normalized = cypherglot.normalize_cypher_text(
            "MATCH (u:User) WHERE u.name STARTS WITH 'Al' AND u.name CONTAINS 'li' AND u.name ENDS WITH 'ce' RETURN u.name AS name ORDER BY name"
        )
        relationship_normalized = cypherglot.normalize_cypher_text(
            "MATCH (a:User)-[r:KNOWS]->(b:User) WHERE r.note IS NULL RETURN r.note AS note ORDER BY note"
        )
        optional_normalized = cypherglot.normalize_cypher_text(
            "OPTIONAL MATCH (u:User) WHERE u.name STARTS WITH 'Al' RETURN u.name AS name ORDER BY name"
        )

        self.assertEqual(type(normalized).__name__, "NormalizedMatchNode")
        self.assertEqual(len(normalized.predicates), 3)
        self.assertEqual(normalized.predicates[0].operator, "STARTS WITH")
        self.assertEqual(normalized.predicates[1].operator, "CONTAINS")
        self.assertEqual(normalized.predicates[2].operator, "ENDS WITH")
        self.assertEqual(type(relationship_normalized).__name__, "NormalizedMatchRelationship")
        self.assertEqual(relationship_normalized.predicates[0].alias, "r")
        self.assertEqual(relationship_normalized.predicates[0].field, "note")
        self.assertEqual(relationship_normalized.predicates[0].operator, "IS NULL")
        self.assertEqual(type(optional_normalized).__name__, "NormalizedOptionalMatchNode")
        self.assertEqual(optional_normalized.predicates[0].operator, "STARTS WITH")

    def test_normalize_cypher_text_normalizes_where_size_filters(self) -> None:
        normalized = cypherglot.normalize_cypher_text(
            "MATCH (u:User) WHERE size(u.name) >= 3 RETURN u.name AS name ORDER BY name"
        )
        relationship_normalized = cypherglot.normalize_cypher_text(
            "MATCH (a:User)-[r:KNOWS]->(b:User) WHERE size(r.note) IS NOT NULL RETURN r.note AS note ORDER BY note"
        )
        optional_normalized = cypherglot.normalize_cypher_text(
            "OPTIONAL MATCH (u:User) WHERE size(u.name) >= 3 RETURN u.name AS name ORDER BY name"
        )

        self.assertEqual(type(normalized).__name__, "NormalizedMatchNode")
        self.assertEqual(normalized.predicates[0].field, "__size__:name")
        self.assertEqual(normalized.predicates[0].operator, ">=")
        self.assertEqual(type(relationship_normalized).__name__, "NormalizedMatchRelationship")
        self.assertEqual(relationship_normalized.predicates[0].alias, "r")
        self.assertEqual(relationship_normalized.predicates[0].field, "__size__:note")
        self.assertEqual(relationship_normalized.predicates[0].operator, "IS NOT NULL")
        self.assertEqual(type(optional_normalized).__name__, "NormalizedOptionalMatchNode")
        self.assertEqual(optional_normalized.predicates[0].field, "__size__:name")
        self.assertEqual(optional_normalized.predicates[0].operator, ">=")

    def test_normalize_cypher_text_normalizes_null_predicate_returns(self) -> None:
        normalized = cypherglot.normalize_cypher_text(
            "MATCH (u:User) RETURN u.name IS NULL AS missing_name, size(u.name) IS NOT NULL AS has_len ORDER BY missing_name, has_len"
        )
        relationship_normalized = cypherglot.normalize_cypher_text(
            "MATCH (a:User)-[r:KNOWS]->(b:User) RETURN r.note IS NULL AS missing_note, size(r.note) IS NOT NULL AS has_len ORDER BY missing_note, has_len"
        )
        optional_normalized = cypherglot.normalize_cypher_text(
            "OPTIONAL MATCH (u:User) RETURN u.name IS NULL AS missing_name ORDER BY missing_name"
        )

        self.assertEqual(type(normalized).__name__, "NormalizedMatchNode")
        self.assertEqual(normalized.returns[0].kind, "predicate")
        self.assertEqual(normalized.returns[0].alias, "u")
        self.assertEqual(normalized.returns[0].field, "name")
        self.assertEqual(normalized.returns[0].operator, "IS NULL")
        self.assertEqual(normalized.returns[1].field, "__size__:name")
        self.assertEqual(normalized.returns[1].operator, "IS NOT NULL")
        self.assertEqual(type(relationship_normalized).__name__, "NormalizedMatchRelationship")
        self.assertEqual(relationship_normalized.returns[0].alias, "r")
        self.assertEqual(relationship_normalized.returns[0].field, "note")
        self.assertEqual(relationship_normalized.returns[0].operator, "IS NULL")
        self.assertEqual(relationship_normalized.returns[1].field, "__size__:note")
        self.assertEqual(relationship_normalized.returns[1].operator, "IS NOT NULL")
        self.assertEqual(type(optional_normalized).__name__, "NormalizedOptionalMatchNode")
        self.assertEqual(optional_normalized.returns[0].operator, "IS NULL")

    def test_normalize_cypher_text_normalizes_id_and_type_returns(self) -> None:
        id_normalized = cypherglot.normalize_cypher_text(
            "MATCH (u:User) RETURN id(u) AS uid ORDER BY uid"
        )
        type_normalized = cypherglot.normalize_cypher_text(
            "MATCH (a:User)-[r:KNOWS]->(b:User) RETURN type(r) AS rel_type ORDER BY rel_type"
        )

        self.assertEqual(id_normalized.returns[0].kind, "id")
        self.assertEqual(id_normalized.returns[0].alias, "u")
        self.assertEqual(id_normalized.returns[0].column_name, "uid")
        self.assertEqual(type_normalized.returns[0].kind, "type")
        self.assertEqual(type_normalized.returns[0].alias, "r")
        self.assertEqual(type_normalized.returns[0].column_name, "rel_type")


    def test_normalize_cypher_text_normalizes_match_set(self) -> None:
        normalized = cypherglot.normalize_cypher_text(
            "MATCH (u:User) WHERE u.name = $name SET u.age = 31"
        )

        self.assertEqual(type(normalized).__name__, "NormalizedSetNode")
        self.assertEqual(normalized.assignments[0].field, "age")
        self.assertEqual(normalized.assignments[0].value, 31)

    def test_normalize_cypher_text_normalizes_merge_node(self) -> None:
        normalized = cypherglot.normalize_cypher_text(
            "MERGE (u:User {name: 'Alice'})"
        )

        self.assertEqual(type(normalized).__name__, "NormalizedMergeNode")
        self.assertEqual(normalized.node.alias, "u")
        self.assertEqual(normalized.node.label, "User")
        self.assertEqual(normalized.node.properties, (("name", "Alice"),))

    def test_normalize_cypher_text_normalizes_match_merge_relationship(self) -> None:
        normalized = cypherglot.normalize_cypher_text(
            "MATCH (a:User), (b:User {name: 'Bob'}) MERGE (a)-[:KNOWS]->(b)"
        )

        self.assertEqual(type(normalized).__name__, "NormalizedMatchMergeRelationship")
        self.assertEqual(normalized.left_match.alias, "a")
        self.assertEqual(normalized.right_match.alias, "b")
        self.assertEqual(normalized.right_match.properties, (("name", "Bob"),))
        self.assertEqual(normalized.relationship.type_name, "KNOWS")

    def test_normalize_cypher_text_normalizes_traversal_backed_match_create(self) -> None:
        normalized = cypherglot.normalize_cypher_text(
            "MATCH (a:User)-[r:KNOWS]->(b:User) CREATE (a)-[:INTRODUCED]->(b)"
        )

        self.assertEqual(type(normalized).__name__, "NormalizedMatchCreateRelationshipFromTraversal")
        self.assertEqual(type(normalized.source).__name__, "NormalizedMatchRelationship")
        self.assertEqual(normalized.source.relationship.alias, "r")
        self.assertEqual(normalized.relationship.type_name, "INTRODUCED")

    def test_normalize_cypher_text_normalizes_traversal_backed_match_create_with_new_endpoint(self) -> None:
        normalized = cypherglot.normalize_cypher_text(
            "MATCH (a:User)-[r:KNOWS]->(b:User) CREATE (a)-[:INTRODUCED]->(:Person {name: 'Cara'})"
        )
        unlabeled_normalized = cypherglot.normalize_cypher_text(
            "MATCH (a:User)-[r:KNOWS]->(b:User) CREATE (a)-[:INTRODUCED]->({name: 'Cara'})"
        )

        self.assertEqual(type(normalized).__name__, "NormalizedMatchCreateRelationshipFromTraversal")
        self.assertEqual(type(normalized.source).__name__, "NormalizedMatchRelationship")
        self.assertEqual(normalized.right.label, "Person")
        self.assertEqual(normalized.right.properties, (("name", "Cara"),))
        self.assertIsNone(unlabeled_normalized.right.label)
        self.assertEqual(unlabeled_normalized.right.properties, (("name", "Cara"),))

    def test_normalize_cypher_text_normalizes_traversal_backed_match_merge(self) -> None:
        normalized = cypherglot.normalize_cypher_text(
            "MATCH (a:User)-[r:KNOWS]->(b:User)-[s:WORKS_AT]->(c:Company) MERGE (a)-[:INTRODUCED]->(c)"
        )

        self.assertEqual(type(normalized).__name__, "NormalizedMatchMergeRelationshipFromTraversal")
        self.assertEqual(type(normalized.source).__name__, "NormalizedMatchChain")
        self.assertEqual(tuple(node.alias for node in normalized.source.nodes), ("a", "b", "c"))
        self.assertEqual(normalized.relationship.type_name, "INTRODUCED")

    def test_normalize_cypher_text_normalizes_traversal_backed_match_merge_with_new_endpoint(self) -> None:
        normalized = cypherglot.normalize_cypher_text(
            "MATCH (a:User)-[r:KNOWS]->(b:User) MERGE (a)-[:INTRODUCED]->(:Person {name: 'Cara'})"
        )
        unlabeled_normalized = cypherglot.normalize_cypher_text(
            "MATCH (a:User)-[r:KNOWS]->(b:User) MERGE (a)-[:INTRODUCED]->({name: 'Cara'})"
        )

        self.assertEqual(type(normalized).__name__, "NormalizedMatchMergeRelationshipFromTraversal")
        self.assertEqual(type(normalized.source).__name__, "NormalizedMatchRelationship")
        self.assertEqual(normalized.right.label, "Person")
        self.assertEqual(normalized.right.properties, (("name", "Cara"),))
        self.assertIsNone(unlabeled_normalized.right.label)
        self.assertEqual(unlabeled_normalized.right.properties, (("name", "Cara"),))

    def test_normalize_cypher_text_normalizes_match_with_return(self) -> None:
        normalized = cypherglot.normalize_cypher_text(
            "MATCH (u:User) WITH u AS person RETURN person.name ORDER BY person.name"
        )

        self.assertEqual(type(normalized).__name__, "NormalizedMatchWithReturn")
        self.assertEqual(type(normalized.source).__name__, "NormalizedMatchNode")
        self.assertEqual(normalized.bindings[0].source_alias, "u")
        self.assertEqual(normalized.bindings[0].output_alias, "person")
        self.assertEqual(normalized.bindings[0].binding_kind, "entity")
        self.assertEqual(normalized.returns[0].column_name, "person.name")
        self.assertEqual(normalized.order_by[0].alias, "person")

    def test_normalize_cypher_text_normalizes_scalar_with_rebinding(self) -> None:
        normalized = cypherglot.normalize_cypher_text(
            "MATCH (u:User) WITH u.name AS name RETURN name ORDER BY name"
        )

        self.assertEqual(type(normalized).__name__, "NormalizedMatchWithReturn")
        self.assertEqual(normalized.bindings[0].binding_kind, "scalar")
        self.assertEqual(normalized.bindings[0].source_alias, "u")
        self.assertEqual(normalized.bindings[0].source_field, "name")
        self.assertEqual(normalized.bindings[0].output_alias, "name")
        self.assertEqual(normalized.returns[0].column_name, "name")
        self.assertEqual(normalized.order_by[0].kind, "scalar")

    def test_normalize_cypher_text_normalizes_with_entity_passthrough_return(self) -> None:
        normalized = cypherglot.normalize_cypher_text(
            "MATCH (u:User) WITH u AS person, u.name AS name RETURN person, name ORDER BY name"
        )
        ordered_normalized = cypherglot.normalize_cypher_text(
            "MATCH (u:User) WITH u AS person RETURN person AS user ORDER BY user"
        )
        relationship_ordered_normalized = cypherglot.normalize_cypher_text(
            "MATCH (a:User)-[r:KNOWS]->(b:User) WITH r AS rel RETURN rel AS edge ORDER BY edge"
        )

        self.assertEqual(type(normalized).__name__, "NormalizedMatchWithReturn")
        self.assertEqual(normalized.bindings[0].binding_kind, "entity")
        self.assertEqual(normalized.bindings[1].binding_kind, "scalar")
        self.assertEqual(normalized.returns[0].kind, "entity")
        self.assertEqual(normalized.returns[0].column_name, "person")
        self.assertEqual(normalized.returns[1].kind, "scalar")
        self.assertEqual(normalized.returns[1].column_name, "name")
        self.assertEqual(normalized.order_by[0].kind, "scalar")
        self.assertEqual(type(ordered_normalized).__name__, "NormalizedMatchWithReturn")
        self.assertEqual(ordered_normalized.returns[0].kind, "entity")
        self.assertEqual(ordered_normalized.returns[0].column_name, "user")
        self.assertEqual(ordered_normalized.order_by[0].kind, "entity")
        self.assertEqual(ordered_normalized.order_by[0].alias, "person")
        self.assertEqual(type(relationship_ordered_normalized).__name__, "NormalizedMatchWithReturn")
        self.assertEqual(relationship_ordered_normalized.returns[0].kind, "entity")
        self.assertEqual(relationship_ordered_normalized.returns[0].column_name, "edge")
        self.assertEqual(relationship_ordered_normalized.order_by[0].kind, "entity")
        self.assertEqual(relationship_ordered_normalized.order_by[0].alias, "rel")

    def test_normalize_cypher_text_normalizes_with_properties_returns(self) -> None:
        normalized = cypherglot.normalize_cypher_text(
            "MATCH (a:User)-[r:KNOWS]->(b:User) WITH a AS person, r AS rel RETURN properties(person) AS user_props, properties(rel) AS rel_props ORDER BY user_props, rel_props"
        )

        self.assertEqual(type(normalized).__name__, "NormalizedMatchWithReturn")
        self.assertEqual(normalized.returns[0].kind, "properties")
        self.assertEqual(normalized.returns[0].alias, "person")
        self.assertEqual(normalized.returns[0].column_name, "user_props")
        self.assertEqual(normalized.returns[1].kind, "properties")
        self.assertEqual(normalized.returns[1].alias, "rel")
        self.assertEqual(normalized.returns[1].column_name, "rel_props")
        self.assertEqual(normalized.order_by[0].kind, "properties")
        self.assertEqual(normalized.order_by[1].kind, "properties")

    def test_normalize_cypher_text_normalizes_with_labels_returns(self) -> None:
        normalized = cypherglot.normalize_cypher_text(
            "MATCH (a:User)-[r:KNOWS]->(b:User) WITH a AS person RETURN labels(person) AS user_labels ORDER BY user_labels"
        )

        self.assertEqual(type(normalized).__name__, "NormalizedMatchWithReturn")
        self.assertEqual(normalized.returns[0].kind, "labels")
        self.assertEqual(normalized.returns[0].alias, "person")
        self.assertEqual(normalized.returns[0].column_name, "user_labels")
        self.assertEqual(normalized.order_by[0].kind, "labels")

    def test_normalize_cypher_text_normalizes_with_keys_returns(self) -> None:
        normalized = cypherglot.normalize_cypher_text(
            "MATCH (a:User)-[r:KNOWS]->(b:User) WITH a AS person, r AS rel RETURN keys(person) AS user_keys, keys(rel) AS rel_keys ORDER BY user_keys, rel_keys"
        )

        self.assertEqual(type(normalized).__name__, "NormalizedMatchWithReturn")
        self.assertEqual(normalized.returns[0].kind, "keys")
        self.assertEqual(normalized.returns[0].alias, "person")
        self.assertEqual(normalized.returns[0].column_name, "user_keys")
        self.assertEqual(normalized.returns[1].kind, "keys")
        self.assertEqual(normalized.returns[1].alias, "rel")
        self.assertEqual(normalized.returns[1].column_name, "rel_keys")
        self.assertEqual(normalized.order_by[0].kind, "keys")
        self.assertEqual(normalized.order_by[1].kind, "keys")

    def test_normalize_cypher_text_normalizes_with_start_and_end_node_returns(self) -> None:
        normalized = cypherglot.normalize_cypher_text(
            "MATCH (a:User)-[r:KNOWS]->(b:User) WITH r AS rel RETURN startNode(rel) AS start, endNode(rel) AS ending ORDER BY start, ending"
        )
        field_normalized = cypherglot.normalize_cypher_text(
            "MATCH (a:User)-[r:KNOWS]->(b:User) WITH r AS rel RETURN startNode(rel).id AS start_id, endNode(rel).id AS end_id ORDER BY start_id, end_id"
        )
        no_alias_normalized = cypherglot.normalize_cypher_text(
            "MATCH (a:User)-[r:KNOWS]->(b:User) WITH a AS person, r AS rel RETURN id(person), type(rel), startNode(rel).id"
        )

        self.assertEqual(type(normalized).__name__, "NormalizedMatchWithReturn")
        self.assertEqual(normalized.returns[0].kind, "start_node")
        self.assertEqual(normalized.returns[0].alias, "rel")
        self.assertEqual(normalized.returns[0].column_name, "start")
        self.assertEqual(normalized.returns[1].kind, "end_node")
        self.assertEqual(normalized.returns[1].alias, "rel")
        self.assertEqual(normalized.returns[1].column_name, "ending")
        self.assertEqual(normalized.order_by[0].kind, "start_node")
        self.assertEqual(normalized.order_by[1].kind, "end_node")
        self.assertEqual(field_normalized.returns[0].kind, "start_node")
        self.assertEqual(field_normalized.returns[0].field, "id")
        self.assertEqual(field_normalized.returns[0].column_name, "start_id")
        self.assertEqual(field_normalized.returns[1].kind, "end_node")
        self.assertEqual(field_normalized.returns[1].field, "id")
        self.assertEqual(field_normalized.returns[1].column_name, "end_id")
        self.assertEqual(field_normalized.order_by[0].kind, "start_node")
        self.assertEqual(field_normalized.order_by[1].kind, "end_node")
        self.assertEqual(no_alias_normalized.returns[0].column_name, "id(person)")
        self.assertEqual(no_alias_normalized.returns[1].column_name, "type(rel)")
        self.assertEqual(no_alias_normalized.returns[2].column_name, "startNode(rel).id")

    def test_normalize_cypher_text_normalizes_with_id_and_type_returns(self) -> None:
        normalized = cypherglot.normalize_cypher_text(
            "MATCH (a:User)-[r:KNOWS]->(b:User) WITH a AS user, r AS rel RETURN id(user) AS uid, type(rel) AS rel_type ORDER BY uid, rel_type"
        )

        self.assertEqual(type(normalized).__name__, "NormalizedMatchWithReturn")
        self.assertEqual(normalized.returns[0].kind, "id")
        self.assertEqual(normalized.returns[0].alias, "user")
        self.assertEqual(normalized.returns[0].column_name, "uid")
        self.assertEqual(normalized.returns[1].kind, "type")
        self.assertEqual(normalized.returns[1].alias, "rel")
        self.assertEqual(normalized.returns[1].column_name, "rel_type")
        self.assertEqual(normalized.order_by[0].kind, "id")
        self.assertEqual(normalized.order_by[1].kind, "type")

    def test_normalize_cypher_text_normalizes_with_size_returns(self) -> None:
        normalized = cypherglot.normalize_cypher_text(
            "MATCH (a:User)-[r:KNOWS]->(b:User) WITH a AS person, r AS rel, a.name AS name RETURN size(person.name) AS name_len, size(name) AS rebound_len, size(id(person)) AS person_id_len, size(type(rel)) AS rel_type_len ORDER BY name_len, rebound_len, person_id_len, rel_type_len"
        )

        self.assertEqual(type(normalized).__name__, "NormalizedMatchWithReturn")
        self.assertEqual(normalized.returns[0].kind, "size")
        self.assertEqual(normalized.returns[0].alias, "person")
        self.assertEqual(normalized.returns[0].field, "name")
        self.assertEqual(normalized.returns[0].column_name, "name_len")
        self.assertEqual(normalized.returns[1].kind, "size")
        self.assertEqual(normalized.returns[1].alias, "name")
        self.assertIsNone(normalized.returns[1].field)
        self.assertEqual(normalized.returns[1].column_name, "rebound_len")
        self.assertEqual(normalized.returns[2].kind, "size")
        self.assertEqual(normalized.returns[2].field, "id")
        self.assertEqual(normalized.returns[2].column_name, "person_id_len")
        self.assertEqual(normalized.returns[3].kind, "size")
        self.assertEqual(normalized.returns[3].field, "type")
        self.assertEqual(normalized.returns[3].column_name, "rel_type_len")
        self.assertEqual(normalized.order_by[0].kind, "size")
        self.assertEqual(normalized.order_by[1].kind, "size")
        self.assertEqual(normalized.order_by[2].kind, "size")
        self.assertEqual(normalized.order_by[3].kind, "size")

    def test_normalize_cypher_text_normalizes_with_lower_and_upper_returns(self) -> None:
        normalized = cypherglot.normalize_cypher_text(
            "MATCH (u:User) WITH u AS person, u.name AS name RETURN lower(person.name) AS lower_name, upper(name) AS upper_name, lower('tag') AS lower_tag, upper($value) AS upper_value, trim(name) AS trimmed, ltrim(' tag') AS left_trimmed, rtrim('tag ') AS right_trimmed ORDER BY lower_name, upper_name, lower_tag, upper_value, trimmed, left_trimmed, right_trimmed"
        )
        reverse_normalized = cypherglot.normalize_cypher_text(
            "MATCH (u:User) WITH u AS person, u.name AS name RETURN reverse(person.name) AS reversed_name, reverse(name) AS rebound_reverse, reverse('tag') AS lit_reverse ORDER BY reversed_name, rebound_reverse, lit_reverse"
        )

        self.assertEqual(type(normalized).__name__, "NormalizedMatchWithReturn")
        self.assertEqual(normalized.returns[0].kind, "lower")
        self.assertEqual(normalized.returns[0].alias, "person")
        self.assertEqual(normalized.returns[0].field, "name")
        self.assertEqual(normalized.returns[0].column_name, "lower_name")
        self.assertEqual(normalized.returns[1].kind, "upper")
        self.assertEqual(normalized.returns[1].alias, "name")
        self.assertIsNone(normalized.returns[1].field)
        self.assertEqual(normalized.returns[1].column_name, "upper_name")
        self.assertEqual(normalized.returns[2].kind, "lower")
        self.assertEqual(normalized.returns[2].column_name, "lower_tag")
        self.assertEqual(normalized.returns[2].value, "tag")
        self.assertEqual(normalized.returns[3].kind, "upper")
        self.assertEqual(normalized.returns[3].column_name, "upper_value")
        self.assertEqual(normalized.returns[3].value.name, "value")
        self.assertEqual(normalized.returns[4].kind, "trim")
        self.assertEqual(normalized.returns[4].alias, "name")
        self.assertIsNone(normalized.returns[4].field)
        self.assertEqual(normalized.returns[4].column_name, "trimmed")
        self.assertEqual(normalized.returns[5].kind, "ltrim")
        self.assertEqual(normalized.returns[5].column_name, "left_trimmed")
        self.assertEqual(normalized.returns[5].value, " tag")
        self.assertEqual(normalized.returns[6].kind, "rtrim")
        self.assertEqual(normalized.returns[6].column_name, "right_trimmed")
        self.assertEqual(normalized.returns[6].value, "tag ")
        self.assertEqual(normalized.order_by[0].kind, "lower")
        self.assertEqual(normalized.order_by[1].kind, "upper")
        self.assertEqual(normalized.order_by[2].kind, "lower")
        self.assertEqual(normalized.order_by[3].kind, "upper")
        self.assertEqual(normalized.order_by[4].kind, "trim")
        self.assertEqual(normalized.order_by[5].kind, "ltrim")
        self.assertEqual(normalized.order_by[6].kind, "rtrim")
        self.assertEqual(reverse_normalized.returns[0].kind, "reverse")
        self.assertEqual(reverse_normalized.returns[0].alias, "person")
        self.assertEqual(reverse_normalized.returns[0].field, "name")
        self.assertEqual(reverse_normalized.returns[1].kind, "reverse")
        self.assertEqual(reverse_normalized.returns[1].alias, "name")
        self.assertIsNone(reverse_normalized.returns[1].field)
        self.assertEqual(reverse_normalized.returns[2].kind, "reverse")
        self.assertEqual(reverse_normalized.returns[2].value, "tag")
        self.assertEqual(reverse_normalized.order_by[0].kind, "reverse")
        self.assertEqual(reverse_normalized.order_by[1].kind, "reverse")
        self.assertEqual(reverse_normalized.order_by[2].kind, "reverse")

    def test_normalize_cypher_text_normalizes_with_coalesce_returns(self) -> None:
        normalized = cypherglot.normalize_cypher_text(
            "MATCH (u:User) WITH u AS person, u.name AS name RETURN coalesce(person.name, 'unknown') AS display_name, coalesce(name, $fallback) AS rebound_name ORDER BY display_name, rebound_name"
        )
        no_alias_normalized = cypherglot.normalize_cypher_text(
            "MATCH (u:User) WITH u AS person, u.name AS name RETURN coalesce(person.name, 'unknown'), coalesce(name, $fallback)"
        )

        self.assertEqual(type(normalized).__name__, "NormalizedMatchWithReturn")
        self.assertEqual(normalized.returns[0].kind, "coalesce")
        self.assertEqual(normalized.returns[0].alias, "person")
        self.assertEqual(normalized.returns[0].field, "name")
        self.assertEqual(normalized.returns[0].value, "unknown")
        self.assertEqual(normalized.returns[0].column_name, "display_name")
        self.assertEqual(normalized.returns[1].kind, "coalesce")
        self.assertEqual(normalized.returns[1].alias, "name")
        self.assertIsNone(normalized.returns[1].field)
        self.assertEqual(normalized.returns[1].value.name, "fallback")
        self.assertEqual(normalized.returns[1].column_name, "rebound_name")
        self.assertEqual(normalized.order_by[0].kind, "coalesce")
        self.assertEqual(normalized.order_by[1].kind, "coalesce")
        self.assertEqual(
            no_alias_normalized.returns[0].column_name,
            "coalesce(person.name, 'unknown')",
        )
        self.assertEqual(
            no_alias_normalized.returns[1].column_name,
            "coalesce(name, $fallback)",
        )

    def test_normalize_cypher_text_normalizes_with_replace_returns(self) -> None:
        normalized = cypherglot.normalize_cypher_text(
            "MATCH (u:User) WITH u AS person, u.name AS name RETURN replace(person.name, 'A', 'B') AS display_name, replace(name, $needle, $replacement) AS rebound_name, replace('Alice', 'l', 'x') AS lit_name ORDER BY display_name, rebound_name, lit_name"
        )
        no_alias_normalized = cypherglot.normalize_cypher_text(
            "MATCH (u:User) WITH u AS person, u.name AS name RETURN replace(person.name, 'A', 'B'), replace(name, $needle, $replacement)"
        )

        self.assertEqual(type(normalized).__name__, "NormalizedMatchWithReturn")
        self.assertEqual(normalized.returns[0].kind, "replace")
        self.assertEqual(normalized.returns[0].alias, "person")
        self.assertEqual(normalized.returns[0].field, "name")
        self.assertEqual(normalized.returns[0].search_value, "A")
        self.assertEqual(normalized.returns[0].replace_value, "B")
        self.assertEqual(normalized.returns[1].kind, "replace")
        self.assertEqual(normalized.returns[1].alias, "name")
        self.assertIsNone(normalized.returns[1].field)
        self.assertEqual(normalized.returns[1].search_value.name, "needle")
        self.assertEqual(normalized.returns[1].replace_value.name, "replacement")
        self.assertEqual(normalized.returns[2].kind, "replace")
        self.assertEqual(normalized.returns[2].value, "Alice")
        self.assertEqual(normalized.returns[2].search_value, "l")
        self.assertEqual(normalized.returns[2].replace_value, "x")
        self.assertEqual(normalized.order_by[0].kind, "replace")
        self.assertEqual(normalized.order_by[1].kind, "replace")
        self.assertEqual(normalized.order_by[2].kind, "replace")
        self.assertEqual(
            no_alias_normalized.returns[0].column_name,
            "replace(person.name, 'A', 'B')",
        )
        self.assertEqual(
            no_alias_normalized.returns[1].column_name,
            "replace(name, $needle, $replacement)",
        )

    def test_normalize_cypher_text_normalizes_with_left_and_right_returns(self) -> None:
        normalized = cypherglot.normalize_cypher_text(
            "MATCH (u:User) WITH u AS person, u.name AS name RETURN left(person.name, 2) AS prefix, right(name, $count) AS rebound_suffix, left('Alice', 3) AS lit_prefix ORDER BY prefix, rebound_suffix, lit_prefix"
        )
        no_alias_normalized = cypherglot.normalize_cypher_text(
            "MATCH (u:User) WITH u AS person, u.name AS name RETURN left(person.name, 2), right(name, $count)"
        )

        self.assertEqual(type(normalized).__name__, "NormalizedMatchWithReturn")
        self.assertEqual(normalized.returns[0].kind, "left")
        self.assertEqual(normalized.returns[0].alias, "person")
        self.assertEqual(normalized.returns[0].field, "name")
        self.assertEqual(normalized.returns[0].length_value, 2)
        self.assertEqual(normalized.returns[1].kind, "right")
        self.assertEqual(normalized.returns[1].alias, "name")
        self.assertIsNone(normalized.returns[1].field)
        self.assertEqual(normalized.returns[1].length_value.name, "count")
        self.assertEqual(normalized.returns[2].kind, "left")
        self.assertEqual(normalized.returns[2].value, "Alice")
        self.assertEqual(normalized.returns[2].length_value, 3)
        self.assertEqual(normalized.order_by[0].kind, "left")
        self.assertEqual(normalized.order_by[1].kind, "right")
        self.assertEqual(normalized.order_by[2].kind, "left")
        self.assertEqual(no_alias_normalized.returns[0].column_name, "left(person.name, 2)")
        self.assertEqual(no_alias_normalized.returns[1].column_name, "right(name, $count)")

    def test_normalize_cypher_text_normalizes_with_split_returns(self) -> None:
        normalized = cypherglot.normalize_cypher_text(
            "MATCH (u:User) WITH u AS person, u.name AS name RETURN split(person.name, ' ') AS parts, split(name, $delimiter) AS rebound_parts, split('Alice Bob', ' ') AS lit_parts ORDER BY parts, rebound_parts, lit_parts"
        )
        no_alias_normalized = cypherglot.normalize_cypher_text(
            "MATCH (u:User) WITH u AS person, u.name AS name RETURN split(person.name, ' '), split(name, $delimiter)"
        )

        self.assertEqual(type(normalized).__name__, "NormalizedMatchWithReturn")
        self.assertEqual(normalized.returns[0].kind, "split")
        self.assertEqual(normalized.returns[0].alias, "person")
        self.assertEqual(normalized.returns[0].field, "name")
        self.assertEqual(normalized.returns[0].delimiter_value, " ")
        self.assertEqual(normalized.returns[1].kind, "split")
        self.assertEqual(normalized.returns[1].alias, "name")
        self.assertIsNone(normalized.returns[1].field)
        self.assertEqual(normalized.returns[1].delimiter_value.name, "delimiter")
        self.assertEqual(normalized.returns[2].kind, "split")
        self.assertEqual(normalized.returns[2].value, "Alice Bob")
        self.assertEqual(normalized.returns[2].delimiter_value, " ")
        self.assertEqual(normalized.order_by[0].kind, "split")
        self.assertEqual(normalized.order_by[1].kind, "split")
        self.assertEqual(normalized.order_by[2].kind, "split")
        self.assertEqual(no_alias_normalized.returns[0].column_name, "split(person.name, ' ')")
        self.assertEqual(no_alias_normalized.returns[1].column_name, "split(name, $delimiter)")

    def test_normalize_cypher_text_normalizes_with_abs_returns(self) -> None:
        normalized = cypherglot.normalize_cypher_text(
            "MATCH (u:User) WITH u AS person, u.age AS age RETURN abs(person.age) AS magnitude, abs(age) AS rebound, abs(-3) AS lit ORDER BY magnitude, rebound, lit"
        )

        self.assertEqual(type(normalized).__name__, "NormalizedMatchWithReturn")
        self.assertEqual(normalized.returns[0].kind, "abs")
        self.assertEqual(normalized.returns[0].alias, "person")
        self.assertEqual(normalized.returns[0].field, "age")
        self.assertEqual(normalized.returns[0].column_name, "magnitude")
        self.assertEqual(normalized.returns[1].kind, "abs")
        self.assertEqual(normalized.returns[1].alias, "age")
        self.assertIsNone(normalized.returns[1].field)
        self.assertEqual(normalized.returns[1].column_name, "rebound")
        self.assertEqual(normalized.returns[2].kind, "abs")
        self.assertEqual(normalized.returns[2].value, -3)
        self.assertEqual(normalized.returns[2].column_name, "lit")
        self.assertEqual(normalized.order_by[0].kind, "abs")
        self.assertEqual(normalized.order_by[1].kind, "abs")
        self.assertEqual(normalized.order_by[2].kind, "abs")

    def test_normalize_cypher_text_normalizes_with_sign_returns(self) -> None:
        normalized = cypherglot.normalize_cypher_text(
            "MATCH (u:User) WITH u AS person, u.age AS age RETURN sign(person.age) AS age_sign, sign(age) AS rebound_sign, sign(-3.2) AS lit_sign ORDER BY age_sign, rebound_sign, lit_sign"
        )

        self.assertEqual(type(normalized).__name__, "NormalizedMatchWithReturn")
        self.assertEqual(normalized.returns[0].kind, "sign")
        self.assertEqual(normalized.returns[0].alias, "person")
        self.assertEqual(normalized.returns[0].field, "age")
        self.assertEqual(normalized.returns[0].column_name, "age_sign")
        self.assertEqual(normalized.returns[1].kind, "sign")
        self.assertEqual(normalized.returns[1].alias, "age")
        self.assertIsNone(normalized.returns[1].field)
        self.assertEqual(normalized.returns[1].column_name, "rebound_sign")
        self.assertEqual(normalized.returns[2].kind, "sign")
        self.assertEqual(normalized.returns[2].value, -3.2)
        self.assertEqual(normalized.returns[2].column_name, "lit_sign")
        self.assertEqual(normalized.order_by[0].kind, "sign")
        self.assertEqual(normalized.order_by[1].kind, "sign")
        self.assertEqual(normalized.order_by[2].kind, "sign")

    def test_normalize_cypher_text_normalizes_with_round_returns(self) -> None:
        normalized = cypherglot.normalize_cypher_text(
            "MATCH (u:User) WITH u AS person, u.score AS score RETURN round(person.score) AS value, round(score) AS rebound, round(-3.2) AS lit ORDER BY value, rebound, lit"
        )

        self.assertEqual(type(normalized).__name__, "NormalizedMatchWithReturn")
        self.assertEqual(normalized.returns[0].kind, "round")
        self.assertEqual(normalized.returns[0].alias, "person")
        self.assertEqual(normalized.returns[0].field, "score")
        self.assertEqual(normalized.returns[0].column_name, "value")
        self.assertEqual(normalized.returns[1].kind, "round")
        self.assertEqual(normalized.returns[1].alias, "score")
        self.assertIsNone(normalized.returns[1].field)
        self.assertEqual(normalized.returns[1].column_name, "rebound")
        self.assertEqual(normalized.returns[2].kind, "round")
        self.assertEqual(normalized.returns[2].value, -3.2)
        self.assertEqual(normalized.returns[2].column_name, "lit")
        self.assertEqual(normalized.order_by[0].kind, "round")
        self.assertEqual(normalized.order_by[1].kind, "round")
        self.assertEqual(normalized.order_by[2].kind, "round")

    def test_normalize_cypher_text_normalizes_with_ceil_returns(self) -> None:
        normalized = cypherglot.normalize_cypher_text(
            "MATCH (u:User) WITH u AS person, u.score AS score RETURN ceil(person.score) AS value, ceil(score) AS rebound, ceil(-3.2) AS lit ORDER BY value, rebound, lit"
        )

        self.assertEqual(type(normalized).__name__, "NormalizedMatchWithReturn")
        self.assertEqual(normalized.returns[0].kind, "ceil")
        self.assertEqual(normalized.returns[0].alias, "person")
        self.assertEqual(normalized.returns[0].field, "score")
        self.assertEqual(normalized.returns[0].column_name, "value")
        self.assertEqual(normalized.returns[1].kind, "ceil")
        self.assertEqual(normalized.returns[1].alias, "score")
        self.assertIsNone(normalized.returns[1].field)
        self.assertEqual(normalized.returns[1].column_name, "rebound")
        self.assertEqual(normalized.returns[2].kind, "ceil")
        self.assertEqual(normalized.returns[2].value, -3.2)
        self.assertEqual(normalized.returns[2].column_name, "lit")
        self.assertEqual(normalized.order_by[0].kind, "ceil")
        self.assertEqual(normalized.order_by[1].kind, "ceil")
        self.assertEqual(normalized.order_by[2].kind, "ceil")

    def test_normalize_cypher_text_normalizes_with_floor_returns(self) -> None:
        normalized = cypherglot.normalize_cypher_text(
            "MATCH (u:User) WITH u AS person, u.score AS score RETURN floor(person.score) AS value, floor(score) AS rebound, floor(-3.2) AS lit ORDER BY value, rebound, lit"
        )

        self.assertEqual(type(normalized).__name__, "NormalizedMatchWithReturn")
        self.assertEqual(normalized.returns[0].kind, "floor")
        self.assertEqual(normalized.returns[0].alias, "person")
        self.assertEqual(normalized.returns[0].field, "score")
        self.assertEqual(normalized.returns[0].column_name, "value")
        self.assertEqual(normalized.returns[1].kind, "floor")
        self.assertEqual(normalized.returns[1].alias, "score")
        self.assertIsNone(normalized.returns[1].field)
        self.assertEqual(normalized.returns[1].column_name, "rebound")
        self.assertEqual(normalized.returns[2].kind, "floor")
        self.assertEqual(normalized.returns[2].value, -3.2)
        self.assertEqual(normalized.returns[2].column_name, "lit")
        self.assertEqual(normalized.order_by[0].kind, "floor")
        self.assertEqual(normalized.order_by[1].kind, "floor")
        self.assertEqual(normalized.order_by[2].kind, "floor")

    def test_normalize_cypher_text_normalizes_with_sqrt_returns(self) -> None:
        normalized = cypherglot.normalize_cypher_text(
            "MATCH (u:User) WITH u AS person, u.score AS score RETURN sqrt(person.score) AS value, sqrt(score) AS rebound, sqrt(-3.2) AS lit ORDER BY value, rebound, lit"
        )

        self.assertEqual(type(normalized).__name__, "NormalizedMatchWithReturn")
        self.assertEqual(normalized.returns[0].kind, "sqrt")
        self.assertEqual(normalized.returns[0].alias, "person")
        self.assertEqual(normalized.returns[0].field, "score")
        self.assertEqual(normalized.returns[0].column_name, "value")
        self.assertEqual(normalized.returns[1].kind, "sqrt")
        self.assertEqual(normalized.returns[1].alias, "score")
        self.assertIsNone(normalized.returns[1].field)
        self.assertEqual(normalized.returns[1].column_name, "rebound")
        self.assertEqual(normalized.returns[2].kind, "sqrt")
        self.assertEqual(normalized.returns[2].value, -3.2)
        self.assertEqual(normalized.returns[2].column_name, "lit")
        self.assertEqual(normalized.order_by[0].kind, "sqrt")
        self.assertEqual(normalized.order_by[1].kind, "sqrt")
        self.assertEqual(normalized.order_by[2].kind, "sqrt")

    def test_normalize_cypher_text_normalizes_with_exp_returns(self) -> None:
        normalized = cypherglot.normalize_cypher_text(
            "MATCH (u:User) WITH u AS person, u.score AS score RETURN exp(person.score) AS value, exp(score) AS rebound, exp(-3.2) AS lit ORDER BY value, rebound, lit"
        )

        self.assertEqual(type(normalized).__name__, "NormalizedMatchWithReturn")
        self.assertEqual(normalized.returns[0].kind, "exp")
        self.assertEqual(normalized.returns[0].alias, "person")
        self.assertEqual(normalized.returns[0].field, "score")
        self.assertEqual(normalized.returns[0].column_name, "value")
        self.assertEqual(normalized.returns[1].kind, "exp")
        self.assertEqual(normalized.returns[1].alias, "score")
        self.assertIsNone(normalized.returns[1].field)
        self.assertEqual(normalized.returns[1].column_name, "rebound")
        self.assertEqual(normalized.returns[2].kind, "exp")
        self.assertEqual(normalized.returns[2].value, -3.2)
        self.assertEqual(normalized.returns[2].column_name, "lit")
        self.assertEqual(normalized.order_by[0].kind, "exp")
        self.assertEqual(normalized.order_by[1].kind, "exp")
        self.assertEqual(normalized.order_by[2].kind, "exp")

    def test_normalize_cypher_text_normalizes_with_sin_returns(self) -> None:
        normalized = cypherglot.normalize_cypher_text(
            "MATCH (u:User) WITH u AS person, u.score AS score RETURN sin(person.score) AS value, sin(score) AS rebound, sin(-3.2) AS lit ORDER BY value, rebound, lit"
        )

        self.assertEqual(type(normalized).__name__, "NormalizedMatchWithReturn")
        self.assertEqual(normalized.returns[0].kind, "sin")
        self.assertEqual(normalized.returns[0].alias, "person")
        self.assertEqual(normalized.returns[0].field, "score")
        self.assertEqual(normalized.returns[0].column_name, "value")
        self.assertEqual(normalized.returns[1].kind, "sin")
        self.assertEqual(normalized.returns[1].alias, "score")
        self.assertIsNone(normalized.returns[1].field)
        self.assertEqual(normalized.returns[1].column_name, "rebound")
        self.assertEqual(normalized.returns[2].kind, "sin")
        self.assertEqual(normalized.returns[2].value, -3.2)
        self.assertEqual(normalized.returns[2].column_name, "lit")
        self.assertEqual(normalized.order_by[0].kind, "sin")
        self.assertEqual(normalized.order_by[1].kind, "sin")
        self.assertEqual(normalized.order_by[2].kind, "sin")

    def test_normalize_cypher_text_normalizes_with_cos_returns(self) -> None:
        normalized = cypherglot.normalize_cypher_text(
            "MATCH (u:User) WITH u AS person, u.score AS score RETURN cos(person.score) AS value, cos(score) AS rebound, cos(-3.2) AS lit ORDER BY value, rebound, lit"
        )

        self.assertEqual(type(normalized).__name__, "NormalizedMatchWithReturn")
        self.assertEqual(normalized.returns[0].kind, "cos")
        self.assertEqual(normalized.returns[0].alias, "person")
        self.assertEqual(normalized.returns[0].field, "score")
        self.assertEqual(normalized.returns[0].column_name, "value")
        self.assertEqual(normalized.returns[1].kind, "cos")
        self.assertEqual(normalized.returns[1].alias, "score")
        self.assertIsNone(normalized.returns[1].field)
        self.assertEqual(normalized.returns[1].column_name, "rebound")
        self.assertEqual(normalized.returns[2].kind, "cos")
        self.assertEqual(normalized.returns[2].value, -3.2)
        self.assertEqual(normalized.returns[2].column_name, "lit")
        self.assertEqual(normalized.order_by[0].kind, "cos")
        self.assertEqual(normalized.order_by[1].kind, "cos")
        self.assertEqual(normalized.order_by[2].kind, "cos")

    def test_normalize_cypher_text_normalizes_with_tan_returns(self) -> None:
        normalized = cypherglot.normalize_cypher_text(
            "MATCH (u:User) WITH u AS person, u.score AS score RETURN tan(person.score) AS value, tan(score) AS rebound, tan(-3.2) AS lit ORDER BY value, rebound, lit"
        )

        self.assertEqual(type(normalized).__name__, "NormalizedMatchWithReturn")
        self.assertEqual(normalized.returns[0].kind, "tan")
        self.assertEqual(normalized.returns[0].alias, "person")
        self.assertEqual(normalized.returns[0].field, "score")
        self.assertEqual(normalized.returns[0].column_name, "value")
        self.assertEqual(normalized.returns[1].kind, "tan")
        self.assertEqual(normalized.returns[1].alias, "score")
        self.assertIsNone(normalized.returns[1].field)
        self.assertEqual(normalized.returns[1].column_name, "rebound")
        self.assertEqual(normalized.returns[2].kind, "tan")
        self.assertEqual(normalized.returns[2].value, -3.2)
        self.assertEqual(normalized.returns[2].column_name, "lit")
        self.assertEqual(normalized.order_by[0].kind, "tan")
        self.assertEqual(normalized.order_by[1].kind, "tan")
        self.assertEqual(normalized.order_by[2].kind, "tan")

    def test_normalize_cypher_text_normalizes_with_asin_returns(self) -> None:
        normalized = cypherglot.normalize_cypher_text(
            "MATCH (u:User) WITH u AS person, u.score AS score RETURN asin(person.score) AS value, asin(score) AS rebound, asin(-0.5) AS lit ORDER BY value, rebound, lit"
        )

        self.assertEqual(type(normalized).__name__, "NormalizedMatchWithReturn")
        self.assertEqual(normalized.returns[0].kind, "asin")
        self.assertEqual(normalized.returns[0].alias, "person")
        self.assertEqual(normalized.returns[0].field, "score")
        self.assertEqual(normalized.returns[0].column_name, "value")
        self.assertEqual(normalized.returns[1].kind, "asin")
        self.assertEqual(normalized.returns[1].alias, "score")
        self.assertIsNone(normalized.returns[1].field)
        self.assertEqual(normalized.returns[1].column_name, "rebound")
        self.assertEqual(normalized.returns[2].kind, "asin")
        self.assertEqual(normalized.returns[2].value, -0.5)
        self.assertEqual(normalized.returns[2].column_name, "lit")
        self.assertEqual(normalized.order_by[0].kind, "asin")
        self.assertEqual(normalized.order_by[1].kind, "asin")
        self.assertEqual(normalized.order_by[2].kind, "asin")

    def test_normalize_cypher_text_normalizes_with_acos_returns(self) -> None:
        normalized = cypherglot.normalize_cypher_text(
            "MATCH (u:User) WITH u AS person, u.score AS score RETURN acos(person.score) AS value, acos(score) AS rebound, acos(-0.5) AS lit ORDER BY value, rebound, lit"
        )

        self.assertEqual(type(normalized).__name__, "NormalizedMatchWithReturn")
        self.assertEqual(normalized.returns[0].kind, "acos")
        self.assertEqual(normalized.returns[0].alias, "person")
        self.assertEqual(normalized.returns[0].field, "score")
        self.assertEqual(normalized.returns[0].column_name, "value")
        self.assertEqual(normalized.returns[1].kind, "acos")
        self.assertEqual(normalized.returns[1].alias, "score")
        self.assertIsNone(normalized.returns[1].field)
        self.assertEqual(normalized.returns[1].column_name, "rebound")
        self.assertEqual(normalized.returns[2].kind, "acos")
        self.assertEqual(normalized.returns[2].value, -0.5)
        self.assertEqual(normalized.returns[2].column_name, "lit")
        self.assertEqual(normalized.order_by[0].kind, "acos")
        self.assertEqual(normalized.order_by[1].kind, "acos")
        self.assertEqual(normalized.order_by[2].kind, "acos")

    def test_normalize_cypher_text_normalizes_with_atan_returns(self) -> None:
        normalized = cypherglot.normalize_cypher_text(
            "MATCH (u:User) WITH u AS person, u.score AS score RETURN atan(person.score) AS value, atan(score) AS rebound, atan(-0.5) AS lit ORDER BY value, rebound, lit"
        )

        self.assertEqual(type(normalized).__name__, "NormalizedMatchWithReturn")
        self.assertEqual(normalized.returns[0].kind, "atan")
        self.assertEqual(normalized.returns[0].alias, "person")
        self.assertEqual(normalized.returns[0].field, "score")
        self.assertEqual(normalized.returns[0].column_name, "value")
        self.assertEqual(normalized.returns[1].kind, "atan")
        self.assertEqual(normalized.returns[1].alias, "score")
        self.assertIsNone(normalized.returns[1].field)
        self.assertEqual(normalized.returns[1].column_name, "rebound")
        self.assertEqual(normalized.returns[2].kind, "atan")
        self.assertEqual(normalized.returns[2].value, -0.5)
        self.assertEqual(normalized.returns[2].column_name, "lit")
        self.assertEqual(normalized.order_by[0].kind, "atan")
        self.assertEqual(normalized.order_by[1].kind, "atan")
        self.assertEqual(normalized.order_by[2].kind, "atan")

    def test_normalize_cypher_text_normalizes_with_ln_returns(self) -> None:
        normalized = cypherglot.normalize_cypher_text(
            "MATCH (u:User) WITH u AS person, u.score AS score RETURN ln(person.score) AS value, ln(score) AS rebound, ln(0.5) AS lit ORDER BY value, rebound, lit"
        )

        self.assertEqual(type(normalized).__name__, "NormalizedMatchWithReturn")
        self.assertEqual(normalized.returns[0].kind, "ln")
        self.assertEqual(normalized.returns[0].alias, "person")
        self.assertEqual(normalized.returns[0].field, "score")
        self.assertEqual(normalized.returns[0].column_name, "value")
        self.assertEqual(normalized.returns[1].kind, "ln")
        self.assertEqual(normalized.returns[1].alias, "score")
        self.assertIsNone(normalized.returns[1].field)
        self.assertEqual(normalized.returns[1].column_name, "rebound")
        self.assertEqual(normalized.returns[2].kind, "ln")
        self.assertEqual(normalized.returns[2].value, 0.5)
        self.assertEqual(normalized.returns[2].column_name, "lit")
        self.assertEqual(normalized.order_by[0].kind, "ln")
        self.assertEqual(normalized.order_by[1].kind, "ln")
        self.assertEqual(normalized.order_by[2].kind, "ln")

    def test_normalize_cypher_text_normalizes_with_log_returns(self) -> None:
        normalized = cypherglot.normalize_cypher_text(
            "MATCH (u:User) WITH u AS person, u.score AS score RETURN log(person.score) AS value, log(score) AS rebound, log(0.5) AS lit ORDER BY value, rebound, lit"
        )

        self.assertEqual(type(normalized).__name__, "NormalizedMatchWithReturn")
        self.assertEqual(normalized.returns[0].kind, "log")
        self.assertEqual(normalized.returns[0].alias, "person")
        self.assertEqual(normalized.returns[0].field, "score")
        self.assertEqual(normalized.returns[0].column_name, "value")
        self.assertEqual(normalized.returns[1].kind, "log")
        self.assertEqual(normalized.returns[1].alias, "score")
        self.assertIsNone(normalized.returns[1].field)
        self.assertEqual(normalized.returns[1].column_name, "rebound")
        self.assertEqual(normalized.returns[2].kind, "log")
        self.assertEqual(normalized.returns[2].value, 0.5)
        self.assertEqual(normalized.returns[2].column_name, "lit")
        self.assertEqual(normalized.order_by[0].kind, "log")
        self.assertEqual(normalized.order_by[1].kind, "log")
        self.assertEqual(normalized.order_by[2].kind, "log")

    def test_normalize_cypher_text_normalizes_with_radians_returns(self) -> None:
        normalized = cypherglot.normalize_cypher_text(
            "MATCH (u:User) WITH u AS person, u.score AS score RETURN radians(person.score) AS value, radians(score) AS rebound, radians(180) AS lit ORDER BY value, rebound, lit"
        )

        self.assertEqual(type(normalized).__name__, "NormalizedMatchWithReturn")
        self.assertEqual(normalized.returns[0].kind, "radians")
        self.assertEqual(normalized.returns[0].alias, "person")
        self.assertEqual(normalized.returns[0].field, "score")
        self.assertEqual(normalized.returns[0].column_name, "value")
        self.assertEqual(normalized.returns[1].kind, "radians")
        self.assertEqual(normalized.returns[1].alias, "score")
        self.assertIsNone(normalized.returns[1].field)
        self.assertEqual(normalized.returns[1].column_name, "rebound")
        self.assertEqual(normalized.returns[2].kind, "radians")
        self.assertEqual(normalized.returns[2].value, 180)
        self.assertEqual(normalized.returns[2].column_name, "lit")
        self.assertEqual(normalized.order_by[0].kind, "radians")
        self.assertEqual(normalized.order_by[1].kind, "radians")
        self.assertEqual(normalized.order_by[2].kind, "radians")

    def test_normalize_cypher_text_normalizes_with_degrees_returns(self) -> None:
        normalized = cypherglot.normalize_cypher_text(
            "MATCH (u:User) WITH u AS person, u.score AS score RETURN degrees(person.score) AS value, degrees(score) AS rebound, degrees(3.14159) AS lit ORDER BY value, rebound, lit"
        )

        self.assertEqual(type(normalized).__name__, "NormalizedMatchWithReturn")
        self.assertEqual(normalized.returns[0].kind, "degrees")
        self.assertEqual(normalized.returns[0].alias, "person")
        self.assertEqual(normalized.returns[0].field, "score")
        self.assertEqual(normalized.returns[0].column_name, "value")
        self.assertEqual(normalized.returns[1].kind, "degrees")
        self.assertEqual(normalized.returns[1].alias, "score")
        self.assertIsNone(normalized.returns[1].field)
        self.assertEqual(normalized.returns[1].column_name, "rebound")
        self.assertEqual(normalized.returns[2].kind, "degrees")
        self.assertEqual(normalized.returns[2].value, 3.14159)
        self.assertEqual(normalized.returns[2].column_name, "lit")
        self.assertEqual(normalized.order_by[0].kind, "degrees")
        self.assertEqual(normalized.order_by[1].kind, "degrees")
        self.assertEqual(normalized.order_by[2].kind, "degrees")

    def test_normalize_cypher_text_normalizes_with_log10_returns(self) -> None:
        normalized = cypherglot.normalize_cypher_text(
            "MATCH (u:User) WITH u AS person, u.score AS score RETURN log10(person.score) AS value, log10(score) AS rebound, log10(0.5) AS lit ORDER BY value, rebound, lit"
        )

        self.assertEqual(type(normalized).__name__, "NormalizedMatchWithReturn")
        self.assertEqual(normalized.returns[0].kind, "log10")
        self.assertEqual(normalized.returns[0].alias, "person")
        self.assertEqual(normalized.returns[0].field, "score")
        self.assertEqual(normalized.returns[0].column_name, "value")
        self.assertEqual(normalized.returns[1].kind, "log10")
        self.assertEqual(normalized.returns[1].alias, "score")
        self.assertIsNone(normalized.returns[1].field)
        self.assertEqual(normalized.returns[1].column_name, "rebound")
        self.assertEqual(normalized.returns[2].kind, "log10")
        self.assertEqual(normalized.returns[2].value, 0.5)
        self.assertEqual(normalized.returns[2].column_name, "lit")
        self.assertEqual(normalized.order_by[0].kind, "log10")
        self.assertEqual(normalized.order_by[1].kind, "log10")
        self.assertEqual(normalized.order_by[2].kind, "log10")

    def test_normalize_cypher_text_normalizes_with_to_string_returns(self) -> None:
        normalized = cypherglot.normalize_cypher_text(
            "MATCH (u:User) WITH u AS person, u.age AS age RETURN toString(person.age) AS text, toString(age) AS rebound, toString(-3) AS lit ORDER BY text, rebound, lit"
        )

        self.assertEqual(type(normalized).__name__, "NormalizedMatchWithReturn")
        self.assertEqual(normalized.returns[0].kind, "to_string")
        self.assertEqual(normalized.returns[0].alias, "person")
        self.assertEqual(normalized.returns[0].field, "age")
        self.assertEqual(normalized.returns[0].column_name, "text")
        self.assertEqual(normalized.returns[1].kind, "to_string")
        self.assertEqual(normalized.returns[1].alias, "age")
        self.assertIsNone(normalized.returns[1].field)
        self.assertEqual(normalized.returns[1].column_name, "rebound")
        self.assertEqual(normalized.returns[2].kind, "to_string")
        self.assertEqual(normalized.returns[2].value, -3)

    def test_normalize_cypher_text_normalizes_with_to_integer_returns(self) -> None:
        normalized = cypherglot.normalize_cypher_text(
            "MATCH (u:User) WITH u AS person, u.age AS age RETURN toInteger(person.age) AS age_int, toInteger(age) AS rebound, toInteger(-3.2) AS lit ORDER BY age_int, rebound, lit"
        )

        self.assertEqual(type(normalized).__name__, "NormalizedMatchWithReturn")
        self.assertEqual(normalized.returns[0].kind, "to_integer")
        self.assertEqual(normalized.returns[0].alias, "person")
        self.assertEqual(normalized.returns[0].field, "age")
        self.assertEqual(normalized.returns[0].column_name, "age_int")
        self.assertEqual(normalized.returns[1].kind, "to_integer")
        self.assertEqual(normalized.returns[1].alias, "age")
        self.assertIsNone(normalized.returns[1].field)
        self.assertEqual(normalized.returns[1].column_name, "rebound")
        self.assertEqual(normalized.returns[2].kind, "to_integer")
        self.assertEqual(normalized.returns[2].value, -3.2)
        self.assertEqual(normalized.returns[2].column_name, "lit")
        self.assertEqual(normalized.order_by[0].kind, "to_integer")
        self.assertEqual(normalized.order_by[1].kind, "to_integer")
        self.assertEqual(normalized.order_by[2].kind, "to_integer")

    def test_normalize_cypher_text_normalizes_with_to_float_returns(self) -> None:
        normalized = cypherglot.normalize_cypher_text(
            "MATCH (u:User) WITH u AS person, u.age AS age RETURN toFloat(person.age) AS age_float, toFloat(age) AS rebound, toFloat(-3) AS lit ORDER BY age_float, rebound, lit"
        )

        self.assertEqual(type(normalized).__name__, "NormalizedMatchWithReturn")
        self.assertEqual(normalized.returns[0].kind, "to_float")
        self.assertEqual(normalized.returns[0].alias, "person")
        self.assertEqual(normalized.returns[0].field, "age")
        self.assertEqual(normalized.returns[0].column_name, "age_float")
        self.assertEqual(normalized.returns[1].kind, "to_float")
        self.assertEqual(normalized.returns[1].alias, "age")
        self.assertIsNone(normalized.returns[1].field)
        self.assertEqual(normalized.returns[1].column_name, "rebound")
        self.assertEqual(normalized.returns[2].kind, "to_float")
        self.assertEqual(normalized.returns[2].value, -3)
        self.assertEqual(normalized.returns[2].column_name, "lit")
        self.assertEqual(normalized.order_by[0].kind, "to_float")
        self.assertEqual(normalized.order_by[1].kind, "to_float")
        self.assertEqual(normalized.order_by[2].kind, "to_float")

    def test_normalize_cypher_text_normalizes_with_to_boolean_returns(self) -> None:
        normalized = cypherglot.normalize_cypher_text(
            "MATCH (u:User) WITH u AS person, u.active AS active RETURN toBoolean(person.active) AS is_active, toBoolean(active) AS rebound, toBoolean(true) AS lit ORDER BY is_active, rebound, lit"
        )

        self.assertEqual(type(normalized).__name__, "NormalizedMatchWithReturn")
        self.assertEqual(normalized.returns[0].kind, "to_boolean")
        self.assertEqual(normalized.returns[0].alias, "person")
        self.assertEqual(normalized.returns[0].field, "active")
        self.assertEqual(normalized.returns[0].column_name, "is_active")
        self.assertEqual(normalized.returns[1].kind, "to_boolean")
        self.assertEqual(normalized.returns[1].alias, "active")
        self.assertIsNone(normalized.returns[1].field)
        self.assertEqual(normalized.returns[1].column_name, "rebound")
        self.assertEqual(normalized.returns[2].kind, "to_boolean")
        self.assertEqual(normalized.returns[2].value, True)
        self.assertEqual(normalized.returns[2].column_name, "lit")
        self.assertEqual(normalized.order_by[0].kind, "to_boolean")
        self.assertEqual(normalized.order_by[1].kind, "to_boolean")
        self.assertEqual(normalized.order_by[2].kind, "to_boolean")

    def test_normalize_cypher_text_normalizes_with_substring_returns(self) -> None:
        normalized = cypherglot.normalize_cypher_text(
            "MATCH (u:User) WITH u AS person, u.name AS name RETURN substring(person.name, 0, 2) AS prefix, substring(name, 1, 3) AS rebound, substring('Alice', 1, 3) AS lit ORDER BY prefix, rebound, lit"
        )
        no_alias_normalized = cypherglot.normalize_cypher_text(
            "MATCH (u:User) WITH u AS person, u.name AS name RETURN substring(person.name, 0, 2), substring(name, 1, 3)"
        )
        two_arg_normalized = cypherglot.normalize_cypher_text(
            "MATCH (u:User) WITH u AS person, u.name AS name RETURN substring(person.name, 1) AS suffix, substring(name, 2) AS rebound_suffix, substring('Alice', 3) AS lit_suffix ORDER BY suffix, rebound_suffix, lit_suffix"
        )

        self.assertEqual(type(normalized).__name__, "NormalizedMatchWithReturn")
        self.assertEqual(normalized.returns[0].kind, "substring")
        self.assertEqual(normalized.returns[0].alias, "person")
        self.assertEqual(normalized.returns[0].field, "name")
        self.assertEqual(normalized.returns[0].start_value, 0)
        self.assertEqual(normalized.returns[0].length_value, 2)
        self.assertEqual(normalized.returns[1].kind, "substring")
        self.assertEqual(normalized.returns[1].alias, "name")
        self.assertIsNone(normalized.returns[1].field)
        self.assertEqual(normalized.returns[1].start_value, 1)
        self.assertEqual(normalized.returns[1].length_value, 3)
        self.assertEqual(normalized.returns[2].kind, "substring")
        self.assertEqual(normalized.returns[2].value, "Alice")
        self.assertEqual(normalized.returns[2].start_value, 1)
        self.assertEqual(normalized.returns[2].length_value, 3)
        self.assertEqual(normalized.order_by[0].kind, "substring")
        self.assertEqual(normalized.order_by[1].kind, "substring")
        self.assertEqual(normalized.order_by[2].kind, "substring")
        self.assertEqual(
            no_alias_normalized.returns[0].column_name,
            "substring(person.name, 0, 2)",
        )
        self.assertEqual(
            no_alias_normalized.returns[1].column_name,
            "substring(name, 1, 3)",
        )
        self.assertEqual(two_arg_normalized.returns[0].kind, "substring")
        self.assertEqual(two_arg_normalized.returns[0].start_value, 1)
        self.assertIsNone(two_arg_normalized.returns[0].length_value)
        self.assertEqual(two_arg_normalized.returns[1].kind, "substring")
        self.assertEqual(two_arg_normalized.returns[1].start_value, 2)
        self.assertIsNone(two_arg_normalized.returns[1].length_value)
        self.assertEqual(two_arg_normalized.returns[2].kind, "substring")
        self.assertEqual(two_arg_normalized.returns[2].start_value, 3)
        self.assertIsNone(two_arg_normalized.returns[2].length_value)
        self.assertEqual(two_arg_normalized.order_by[0].kind, "substring")
        self.assertEqual(two_arg_normalized.order_by[1].kind, "substring")
        self.assertEqual(two_arg_normalized.order_by[2].kind, "substring")

    def test_normalize_cypher_text_normalizes_with_relationship_property_string_returns(self) -> None:
        normalized = cypherglot.normalize_cypher_text(
            "MATCH (a:User)-[r:KNOWS]->(b:User) WITH r AS rel, r.note AS note RETURN lower(rel.note) AS lower_note, upper(note) AS upper_note, trim(rel.note) AS trimmed_note, ltrim(note) AS left_trimmed_note, rtrim(rel.note) AS right_trimmed_note, reverse(note) AS reversed_note, coalesce(rel.note, 'unknown') AS display_note, replace(note, 'A', 'B') AS replaced_note, left(rel.note, 2) AS prefix, right(note, 2) AS suffix, split(rel.note, ' ') AS parts, substring(note, 1) AS tail ORDER BY lower_note, upper_note, trimmed_note, left_trimmed_note, right_trimmed_note, reversed_note, display_note, replaced_note, prefix, suffix, parts, tail"
        )

        self.assertEqual(type(normalized).__name__, "NormalizedMatchWithReturn")
        self.assertEqual(normalized.returns[0].kind, "lower")
        self.assertEqual(normalized.returns[0].alias, "rel")
        self.assertEqual(normalized.returns[0].field, "note")
        self.assertEqual(normalized.returns[1].kind, "upper")
        self.assertEqual(normalized.returns[1].alias, "note")
        self.assertIsNone(normalized.returns[1].field)
        self.assertEqual(normalized.returns[6].kind, "coalesce")
        self.assertEqual(normalized.returns[7].kind, "replace")
        self.assertEqual(normalized.returns[10].kind, "split")
        self.assertEqual(normalized.returns[11].kind, "substring")
        self.assertEqual(normalized.order_by[0].kind, "lower")
        self.assertEqual(normalized.order_by[-1].kind, "substring")

    def test_normalize_cypher_text_normalizes_with_relationship_property_numeric_and_conversion_returns(
        self,
    ) -> None:
        normalized = cypherglot.normalize_cypher_text(
            "MATCH (a:User)-[r:KNOWS]->(b:User) WITH r AS rel, r.weight AS weight, r.score AS score, r.active AS active RETURN abs(rel.weight) AS magnitude, sign(weight) AS weight_sign, round(rel.score) AS rounded_score, ceil(score) AS ceil_score, floor(rel.score) AS floor_score, sqrt(rel.score) AS sqrt_score, exp(score) AS exp_score, sin(rel.score) AS sin_score, toString(weight) AS weight_text, toInteger(rel.score) AS score_int, toFloat(weight) AS weight_float, toBoolean(active) AS active_bool ORDER BY magnitude, weight_sign, rounded_score, ceil_score, floor_score, sqrt_score, exp_score, sin_score, weight_text, score_int, weight_float, active_bool"
        )

        self.assertEqual(type(normalized).__name__, "NormalizedMatchWithReturn")
        self.assertEqual(normalized.returns[0].kind, "abs")
        self.assertEqual(normalized.returns[0].alias, "rel")
        self.assertEqual(normalized.returns[0].field, "weight")
        self.assertEqual(normalized.returns[1].kind, "sign")
        self.assertEqual(normalized.returns[1].alias, "weight")
        self.assertIsNone(normalized.returns[1].field)
        self.assertEqual(normalized.returns[2].kind, "round")
        self.assertEqual(normalized.returns[3].kind, "ceil")
        self.assertEqual(normalized.returns[4].kind, "floor")
        self.assertEqual(normalized.returns[5].kind, "sqrt")
        self.assertEqual(normalized.returns[6].kind, "exp")
        self.assertEqual(normalized.returns[7].kind, "sin")
        self.assertEqual(normalized.returns[8].kind, "to_string")
        self.assertEqual(normalized.returns[9].kind, "to_integer")
        self.assertEqual(normalized.returns[10].kind, "to_float")
        self.assertEqual(normalized.returns[11].kind, "to_boolean")
        self.assertEqual(normalized.order_by[0].kind, "abs")
        self.assertEqual(normalized.order_by[-1].kind, "to_boolean")

    def test_normalize_cypher_text_normalizes_with_size_literal_and_parameter_returns(self) -> None:
        normalized = cypherglot.normalize_cypher_text(
            "MATCH (u:User) WITH u AS person RETURN size('tag') AS tag_len, size($value) AS value_len ORDER BY tag_len, value_len"
        )

        self.assertEqual(type(normalized).__name__, "NormalizedMatchWithReturn")
        self.assertEqual(normalized.returns[0].kind, "size")
        self.assertEqual(normalized.returns[0].column_name, "tag_len")
        self.assertEqual(normalized.returns[0].value, "tag")
        self.assertEqual(normalized.returns[1].kind, "size")
        self.assertEqual(normalized.returns[1].column_name, "value_len")
        self.assertEqual(normalized.returns[1].value.name, "value")
        self.assertEqual(normalized.order_by[0].kind, "size")
        self.assertEqual(normalized.order_by[1].kind, "size")

    def test_normalize_cypher_text_normalizes_with_scalar_literal_and_parameter_returns(self) -> None:
        normalized = cypherglot.normalize_cypher_text(
            "MATCH (u:User) WITH u AS person RETURN 'tag' AS tag, $value AS value ORDER BY tag, value"
        )

        self.assertEqual(type(normalized).__name__, "NormalizedMatchWithReturn")
        self.assertEqual(normalized.returns[0].kind, "scalar_value")
        self.assertEqual(normalized.returns[0].column_name, "tag")
        self.assertEqual(normalized.returns[0].value, "tag")
        self.assertEqual(normalized.returns[1].kind, "scalar_value")
        self.assertEqual(normalized.returns[1].column_name, "value")
        self.assertEqual(normalized.returns[1].value.name, "value")
        self.assertEqual(normalized.order_by[0].kind, "scalar_value")
        self.assertEqual(normalized.order_by[1].kind, "scalar_value")

    def test_normalize_cypher_text_normalizes_with_predicate_returns(self) -> None:
        normalized = cypherglot.normalize_cypher_text(
            "MATCH (a:User)-[r:KNOWS]->(b:User) WITH a AS person, r AS rel, a.name AS name RETURN person.age >= 18 AS adult, name = 'Alice' AS is_alice, person.name CONTAINS 'a' AS has_a, name CONTAINS 'i' AS rebound_has_i, person.name STARTS WITH 'Al' AS has_prefix, name ENDS WITH 'ce' AS has_suffix, id(person) >= 1 AS has_id, type(rel) = 'KNOWS' AS rel_matches, size(person.name) >= 3 AS long_name, size(name) >= 3 AS rebound_long, size(id(person)) >= 1 AS long_id, size(type(rel)) >= 5 AS long_type ORDER BY adult, is_alice, has_a, rebound_has_i, has_prefix, has_suffix, has_id, rel_matches, long_name, rebound_long, long_id, long_type"
        )

        self.assertEqual(type(normalized).__name__, "NormalizedMatchWithReturn")
        self.assertEqual(normalized.returns[0].kind, "predicate")
        self.assertEqual(normalized.returns[0].alias, "person")
        self.assertEqual(normalized.returns[0].field, "age")
        self.assertEqual(normalized.returns[0].operator, ">=")
        self.assertEqual(normalized.returns[0].column_name, "adult")
        self.assertEqual(normalized.returns[1].kind, "predicate")
        self.assertEqual(normalized.returns[1].alias, "name")
        self.assertIsNone(normalized.returns[1].field)
        self.assertEqual(normalized.returns[1].operator, "=")
        self.assertEqual(normalized.returns[1].column_name, "is_alice")
        self.assertEqual(normalized.returns[2].kind, "predicate")
        self.assertEqual(normalized.returns[2].alias, "person")
        self.assertEqual(normalized.returns[2].field, "name")
        self.assertEqual(normalized.returns[2].operator, "CONTAINS")
        self.assertEqual(normalized.returns[2].column_name, "has_a")
        self.assertEqual(normalized.returns[3].kind, "predicate")
        self.assertEqual(normalized.returns[3].alias, "name")
        self.assertIsNone(normalized.returns[3].field)
        self.assertEqual(normalized.returns[3].operator, "CONTAINS")
        self.assertEqual(normalized.returns[3].column_name, "rebound_has_i")
        self.assertEqual(normalized.returns[4].kind, "predicate")
        self.assertEqual(normalized.returns[4].alias, "person")
        self.assertEqual(normalized.returns[4].field, "name")
        self.assertEqual(normalized.returns[4].operator, "STARTS WITH")
        self.assertEqual(normalized.returns[4].column_name, "has_prefix")
        self.assertEqual(normalized.returns[5].kind, "predicate")
        self.assertEqual(normalized.returns[5].alias, "name")
        self.assertIsNone(normalized.returns[5].field)
        self.assertEqual(normalized.returns[5].operator, "ENDS WITH")
        self.assertEqual(normalized.returns[5].column_name, "has_suffix")
        self.assertEqual(normalized.returns[6].kind, "predicate")
        self.assertEqual(normalized.returns[6].alias, "person")
        self.assertEqual(normalized.returns[6].field, "id")
        self.assertEqual(normalized.returns[6].column_name, "has_id")
        self.assertEqual(normalized.returns[7].kind, "predicate")
        self.assertEqual(normalized.returns[7].alias, "rel")
        self.assertEqual(normalized.returns[7].field, "type")
        self.assertEqual(normalized.returns[7].column_name, "rel_matches")
        self.assertEqual(normalized.returns[8].kind, "predicate")
        self.assertEqual(normalized.returns[8].alias, "person")
        self.assertEqual(normalized.returns[8].field, "__size__:name")
        self.assertEqual(normalized.returns[8].column_name, "long_name")
        self.assertEqual(normalized.returns[9].kind, "predicate")
        self.assertEqual(normalized.returns[9].alias, "name")
        self.assertEqual(normalized.returns[9].field, "__size__:__value__")
        self.assertEqual(normalized.returns[9].column_name, "rebound_long")
        self.assertEqual(normalized.returns[10].kind, "predicate")
        self.assertEqual(normalized.returns[10].alias, "person")
        self.assertEqual(normalized.returns[10].field, "__size__:id")
        self.assertEqual(normalized.returns[10].column_name, "long_id")
        self.assertEqual(normalized.returns[11].kind, "predicate")
        self.assertEqual(normalized.returns[11].alias, "rel")
        self.assertEqual(normalized.returns[11].field, "__size__:type")
        self.assertEqual(normalized.returns[11].column_name, "long_type")
        self.assertEqual(normalized.order_by[0].kind, "predicate")
        self.assertEqual(normalized.order_by[1].kind, "predicate")
        self.assertEqual(normalized.order_by[2].kind, "predicate")
        self.assertEqual(normalized.order_by[3].kind, "predicate")
        self.assertEqual(normalized.order_by[4].kind, "predicate")
        self.assertEqual(normalized.order_by[5].kind, "predicate")
        self.assertEqual(normalized.order_by[6].kind, "predicate")
        self.assertEqual(normalized.order_by[7].kind, "predicate")
        self.assertEqual(normalized.order_by[8].kind, "predicate")
        self.assertEqual(normalized.order_by[9].kind, "predicate")
        self.assertEqual(normalized.order_by[10].kind, "predicate")
        self.assertEqual(normalized.order_by[11].kind, "predicate")

    def test_normalize_cypher_text_normalizes_with_relationship_property_predicate_returns(
        self,
    ) -> None:
        normalized = cypherglot.normalize_cypher_text(
            "MATCH (a:User)-[r:KNOWS]->(b:User) WITH r AS rel, r.note AS note, r.weight AS weight RETURN rel.weight >= 1 AS heavy, note CONTAINS 'a' AS has_a, rel.note STARTS WITH 'Al' AS has_prefix, note ENDS WITH 'ce' AS has_suffix, size(rel.note) >= 3 AS long_note, size(note) >= 3 AS rebound_long ORDER BY heavy, has_a, has_prefix, has_suffix, long_note, rebound_long"
        )

        self.assertEqual(type(normalized).__name__, "NormalizedMatchWithReturn")
        self.assertEqual(normalized.returns[0].kind, "predicate")
        self.assertEqual(normalized.returns[0].alias, "rel")
        self.assertEqual(normalized.returns[0].field, "weight")
        self.assertEqual(normalized.returns[1].kind, "predicate")
        self.assertEqual(normalized.returns[1].alias, "note")
        self.assertIsNone(normalized.returns[1].field)
        self.assertEqual(normalized.returns[2].operator, "STARTS WITH")
        self.assertEqual(normalized.returns[3].operator, "ENDS WITH")
        self.assertEqual(normalized.returns[4].field, "__size__:note")
        self.assertEqual(normalized.returns[5].field, "__size__:__value__")
        self.assertEqual(normalized.order_by[0].kind, "predicate")
        self.assertEqual(normalized.order_by[-1].kind, "predicate")

    def test_normalize_cypher_text_normalizes_with_null_predicate_returns(self) -> None:
        normalized = cypherglot.normalize_cypher_text(
            "MATCH (a:User)-[r:KNOWS]->(b:User) WITH a AS person, r AS rel, a.name AS name, r.note AS note RETURN person.name IS NULL AS missing_name, name IS NOT NULL AS rebound_present, rel.note IS NULL AS missing_note, note IS NOT NULL AS rebound_note, size(name) IS NULL AS missing_len, size(rel.note) IS NOT NULL AS note_len ORDER BY missing_name, rebound_present, missing_note, rebound_note, missing_len, note_len"
        )

        self.assertEqual(type(normalized).__name__, "NormalizedMatchWithReturn")
        self.assertEqual(normalized.returns[0].operator, "IS NULL")
        self.assertEqual(normalized.returns[0].alias, "person")
        self.assertEqual(normalized.returns[0].field, "name")
        self.assertEqual(normalized.returns[1].operator, "IS NOT NULL")
        self.assertEqual(normalized.returns[1].alias, "name")
        self.assertIsNone(normalized.returns[1].field)
        self.assertEqual(normalized.returns[2].operator, "IS NULL")
        self.assertEqual(normalized.returns[2].alias, "rel")
        self.assertEqual(normalized.returns[2].field, "note")
        self.assertEqual(normalized.returns[3].operator, "IS NOT NULL")
        self.assertEqual(normalized.returns[3].alias, "note")
        self.assertIsNone(normalized.returns[3].field)
        self.assertEqual(normalized.returns[4].field, "__size__:__value__")
        self.assertEqual(normalized.returns[4].operator, "IS NULL")
        self.assertEqual(normalized.returns[5].field, "__size__:note")
        self.assertEqual(normalized.returns[5].operator, "IS NOT NULL")

    def test_normalize_cypher_text_normalizes_with_where_null_filters(self) -> None:
        normalized = cypherglot.normalize_cypher_text(
            "MATCH (a:User)-[r:KNOWS]->(b:User) WITH a AS person, r AS rel, a.name AS name WHERE name IS NOT NULL AND person.name IS NULL AND rel.note IS NOT NULL RETURN person, rel"
        )

        self.assertEqual(type(normalized).__name__, "NormalizedMatchWithReturn")
        self.assertEqual(len(normalized.predicates), 3)
        self.assertEqual(normalized.predicates[0].alias, "name")
        self.assertEqual(normalized.predicates[0].operator, "IS NOT NULL")
        self.assertEqual(normalized.predicates[1].alias, "person")
        self.assertEqual(normalized.predicates[1].field, "name")
        self.assertEqual(normalized.predicates[1].operator, "IS NULL")
        self.assertEqual(normalized.predicates[2].alias, "rel")
        self.assertEqual(normalized.predicates[2].field, "note")
        self.assertEqual(normalized.predicates[2].operator, "IS NOT NULL")

    def test_normalize_cypher_text_normalizes_with_where_string_and_null_filters(self) -> None:
        normalized = cypherglot.normalize_cypher_text(
            "MATCH (a:User)-[r:KNOWS]->(b:User) WITH a AS person, r AS rel, a.name AS name, r.note AS note WHERE person.name STARTS WITH 'Al' AND name CONTAINS 'li' AND rel.note ENDS WITH 'ce' AND note IS NOT NULL RETURN person, rel"
        )

        self.assertEqual(type(normalized).__name__, "NormalizedMatchWithReturn")
        self.assertEqual(len(normalized.predicates), 4)
        self.assertEqual(normalized.predicates[0].alias, "person")
        self.assertEqual(normalized.predicates[0].field, "name")
        self.assertEqual(normalized.predicates[0].operator, "STARTS WITH")
        self.assertEqual(normalized.predicates[1].alias, "name")
        self.assertEqual(normalized.predicates[1].operator, "CONTAINS")
        self.assertEqual(normalized.predicates[2].alias, "rel")
        self.assertEqual(normalized.predicates[2].field, "note")
        self.assertEqual(normalized.predicates[2].operator, "ENDS WITH")
        self.assertEqual(normalized.predicates[3].alias, "note")
        self.assertEqual(normalized.predicates[3].operator, "IS NOT NULL")

    def test_normalize_cypher_text_normalizes_with_where_size_filters(self) -> None:
        normalized = cypherglot.normalize_cypher_text(
            "MATCH (a:User)-[r:KNOWS]->(b:User) WITH a AS person, r AS rel, a.name AS name, r.note AS note WHERE size(person.name) >= 3 AND size(name) >= 3 AND size(rel.note) IS NOT NULL AND size(note) IS NOT NULL RETURN person, rel"
        )

        self.assertEqual(type(normalized).__name__, "NormalizedMatchWithReturn")
        self.assertEqual(len(normalized.predicates), 4)
        self.assertEqual(normalized.predicates[0].alias, "person")
        self.assertEqual(normalized.predicates[0].field, "__size__:name")
        self.assertEqual(normalized.predicates[0].operator, ">=")
        self.assertEqual(normalized.predicates[1].alias, "name")
        self.assertEqual(normalized.predicates[1].field, "__size__:__value__")
        self.assertEqual(normalized.predicates[1].operator, ">=")
        self.assertEqual(normalized.predicates[2].alias, "rel")
        self.assertEqual(normalized.predicates[2].field, "__size__:note")
        self.assertEqual(normalized.predicates[2].operator, "IS NOT NULL")
        self.assertEqual(normalized.predicates[3].alias, "note")
        self.assertEqual(normalized.predicates[3].field, "__size__:__value__")
        self.assertEqual(normalized.predicates[3].operator, "IS NOT NULL")

    def test_normalize_cypher_text_normalizes_optional_match_predicate_returns(self) -> None:
        contains_normalized = cypherglot.normalize_cypher_text(
            "OPTIONAL MATCH (u:User) RETURN u.name CONTAINS 'a' AS has_a ORDER BY has_a"
        )
        starts_with_normalized = cypherglot.normalize_cypher_text(
            "OPTIONAL MATCH (u:User) RETURN u.name STARTS WITH 'Al' AS has_prefix ORDER BY has_prefix"
        )
        ends_with_normalized = cypherglot.normalize_cypher_text(
            "OPTIONAL MATCH (u:User) RETURN u.name ENDS WITH 'ce' AS has_suffix ORDER BY has_suffix"
        )

        self.assertEqual(type(contains_normalized).__name__, "NormalizedOptionalMatchNode")
        self.assertEqual(contains_normalized.returns[0].kind, "predicate")
        self.assertEqual(contains_normalized.returns[0].operator, "CONTAINS")
        self.assertEqual(contains_normalized.returns[0].column_name, "has_a")
        self.assertEqual(type(starts_with_normalized).__name__, "NormalizedOptionalMatchNode")
        self.assertEqual(starts_with_normalized.returns[0].kind, "predicate")
        self.assertEqual(starts_with_normalized.returns[0].operator, "STARTS WITH")
        self.assertEqual(starts_with_normalized.returns[0].column_name, "has_prefix")
        self.assertEqual(type(ends_with_normalized).__name__, "NormalizedOptionalMatchNode")
        self.assertEqual(ends_with_normalized.returns[0].kind, "predicate")
        self.assertEqual(ends_with_normalized.returns[0].operator, "ENDS WITH")
        self.assertEqual(ends_with_normalized.returns[0].column_name, "has_suffix")

    def test_normalize_cypher_text_normalizes_grouped_count_with_return(self) -> None:
        normalized = cypherglot.normalize_cypher_text(
            "MATCH (u:User) WITH u AS person, u.name AS name RETURN name, count(person) AS total ORDER BY total DESC"
        )
        star_normalized = cypherglot.normalize_cypher_text(
            "MATCH (u:User) WITH u.name AS name RETURN name, count(*) AS total ORDER BY total DESC"
        )
        relationship_normalized = cypherglot.normalize_cypher_text(
            "MATCH (a:User)-[r:KNOWS]->(b:User) WITH r AS rel RETURN rel AS edge, count(rel) AS total ORDER BY total DESC"
        )
        relationship_star_normalized = cypherglot.normalize_cypher_text(
            "MATCH (a:User)-[r:KNOWS]->(b:User) WITH r AS rel RETURN rel AS edge, count(*) AS total ORDER BY total DESC"
        )

        self.assertEqual(type(normalized).__name__, "NormalizedMatchWithReturn")
        self.assertEqual(normalized.returns[0].kind, "scalar")
        self.assertEqual(normalized.returns[0].column_name, "name")
        self.assertEqual(normalized.returns[1].kind, "count")
        self.assertEqual(normalized.returns[1].alias, "person")
        self.assertEqual(normalized.returns[1].column_name, "total")
        self.assertEqual(normalized.order_by[0].kind, "aggregate")
        self.assertEqual(normalized.order_by[0].alias, "total")

    def test_normalize_cypher_text_normalizes_grouped_numeric_aggregates_with_return(
        self,
    ) -> None:
        normalized = cypherglot.normalize_cypher_text(
            "MATCH (u:User) WITH u.name AS name, u.score AS score RETURN name, sum(score) AS total ORDER BY total DESC"
        )

        self.assertEqual(type(normalized).__name__, "NormalizedMatchWithReturn")
        self.assertEqual(normalized.returns[0].kind, "scalar")
        self.assertEqual(normalized.returns[0].column_name, "name")
        self.assertEqual(normalized.returns[1].kind, "sum")
        self.assertEqual(normalized.returns[1].alias, "score")
        self.assertEqual(normalized.returns[1].column_name, "total")
        self.assertEqual(normalized.order_by[0].kind, "aggregate")
        self.assertEqual(normalized.order_by[0].alias, "total")

    def test_normalize_cypher_text_normalizes_searched_case_with_return(self) -> None:
        normalized = cypherglot.normalize_cypher_text(
            "MATCH (u:User) WITH u.age AS age, u.name AS name RETURN CASE WHEN age >= 18 THEN name ELSE 'minor' END AS label ORDER BY label"
        )

        self.assertEqual(type(normalized).__name__, "NormalizedMatchWithReturn")
        self.assertEqual(normalized.returns[0].kind, "case")
        case_spec = normalized.returns[0].value
        self.assertEqual(type(case_spec).__name__, "WithCaseSpec")
        assert case_spec is not None
        self.assertEqual(case_spec.when_items[0].condition.kind, "predicate")
        self.assertEqual(case_spec.when_items[0].condition.alias, "age")
        self.assertIsNone(case_spec.when_items[0].condition.field)
        self.assertEqual(case_spec.when_items[0].result.kind, "scalar")
        self.assertEqual(case_spec.when_items[0].result.alias, "name")
        self.assertEqual(case_spec.else_item.kind, "scalar_value")
        self.assertEqual(case_spec.else_item.value, "minor")
        self.assertEqual(normalized.order_by[0].kind, "case")
        self.assertEqual(normalized.order_by[0].alias, "label")

    def test_normalize_cypher_text_normalizes_with_where(self) -> None:
        normalized = cypherglot.normalize_cypher_text(
            "MATCH (u:User) WITH u AS person, u.name AS name WHERE name = 'Alice' AND person.id > 1 RETURN person, name ORDER BY name"
        )

        self.assertEqual(type(normalized).__name__, "NormalizedMatchWithReturn")
        self.assertEqual(len(normalized.predicates), 2)
        self.assertEqual(normalized.predicates[0].kind, "scalar")
        self.assertEqual(normalized.predicates[0].alias, "name")
        self.assertEqual(normalized.predicates[1].kind, "field")
        self.assertEqual(normalized.predicates[1].alias, "person")
        self.assertEqual(normalized.predicates[1].field, "id")

    def test_normalize_cypher_text_normalizes_with_where_id_and_type_filters(self) -> None:
        id_normalized = cypherglot.normalize_cypher_text(
            "MATCH (u:User) WITH u AS person, u.name AS name WHERE id(person) >= 1 RETURN person, name ORDER BY name"
        )
        type_normalized = cypherglot.normalize_cypher_text(
            "MATCH (a:User)-[r:KNOWS]->(b:User) WITH a AS person, r AS rel WHERE type(rel) = 'KNOWS' RETURN person, rel"
        )

        self.assertEqual(id_normalized.predicates[0].kind, "field")
        self.assertEqual(id_normalized.predicates[0].alias, "person")
        self.assertEqual(id_normalized.predicates[0].field, "id")
        self.assertEqual(type_normalized.predicates[0].kind, "field")
        self.assertEqual(type_normalized.predicates[0].alias, "rel")
        self.assertEqual(type_normalized.predicates[0].field, "type")

    def test_normalize_cypher_text_normalizes_unwind(self) -> None:
        normalized = cypherglot.normalize_cypher_text(
            "UNWIND [1, 2, 3] AS x RETURN x AS value ORDER BY value DESC LIMIT 2"
        )

        self.assertEqual(type(normalized).__name__, "NormalizedUnwind")
        self.assertEqual(normalized.alias, "x")
        self.assertEqual(normalized.source_kind, "literal")
        self.assertEqual(normalized.source_items, (1, 2, 3))
        self.assertEqual(normalized.returns[0].column_name, "value")
        self.assertEqual(normalized.order_by[0].kind, "scalar")
        self.assertEqual(normalized.limit, 2)

    def test_normalize_cypher_text_normalizes_optional_match(self) -> None:
        normalized = cypherglot.normalize_cypher_text(
            "OPTIONAL MATCH (u:User) WHERE u.name = 'Alice' RETURN u.name ORDER BY u.name LIMIT 1"
        )

        self.assertEqual(type(normalized).__name__, "NormalizedOptionalMatchNode")
        self.assertEqual(normalized.node.alias, "u")
        self.assertEqual(normalized.node.label, "User")
        self.assertEqual(normalized.predicates[0].field, "name")
        self.assertEqual(normalized.returns[0].column_name, "u.name")
        self.assertEqual(normalized.limit, 1)

    def test_normalize_cypher_text_normalizes_optional_match_two_arg_substring(self) -> None:
        normalized = cypherglot.normalize_cypher_text(
            "OPTIONAL MATCH (u:User) RETURN substring(u.name, 1) AS suffix ORDER BY suffix"
        )

        self.assertEqual(type(normalized).__name__, "NormalizedOptionalMatchNode")
        self.assertEqual(normalized.returns[0].kind, "substring")
        self.assertEqual(normalized.returns[0].alias, "u")
        self.assertEqual(normalized.returns[0].field, "name")
        self.assertEqual(normalized.returns[0].start_value, 1)
        self.assertIsNone(normalized.returns[0].length_value)
        self.assertEqual(normalized.order_by[0].alias, "suffix")

    def test_normalize_cypher_text_normalizes_optional_match_scalar_literal_and_parameter_returns(
        self,
    ) -> None:
        literal_normalized = cypherglot.normalize_cypher_text(
            "OPTIONAL MATCH (u:User) RETURN 'tag' AS tag ORDER BY tag"
        )
        parameter_normalized = cypherglot.normalize_cypher_text(
            "OPTIONAL MATCH (u:User) RETURN $value AS value ORDER BY value"
        )

        self.assertEqual(type(literal_normalized).__name__, "NormalizedOptionalMatchNode")
        self.assertEqual(literal_normalized.returns[0].kind, "scalar")
        self.assertEqual(literal_normalized.returns[0].column_name, "tag")
        self.assertEqual(literal_normalized.order_by[0].alias, "tag")
        self.assertEqual(type(parameter_normalized).__name__, "NormalizedOptionalMatchNode")
        self.assertEqual(parameter_normalized.returns[0].kind, "scalar")
        self.assertEqual(parameter_normalized.returns[0].column_name, "value")
        self.assertEqual(parameter_normalized.order_by[0].alias, "value")

    def test_normalize_cypher_text_normalizes_aliased_optional_match(self) -> None:
        normalized = cypherglot.normalize_cypher_text(
            "OPTIONAL MATCH (u:User) RETURN u.name AS name ORDER BY name"
        )

        self.assertEqual(type(normalized).__name__, "NormalizedOptionalMatchNode")
        self.assertEqual(normalized.returns[0].column_name, "name")
        self.assertEqual(normalized.order_by[0].alias, "name")

    def test_normalize_cypher_text_normalizes_entity_optional_match_return(self) -> None:
        normalized = cypherglot.normalize_cypher_text(
            "OPTIONAL MATCH (u:User) RETURN u AS user"
        )
        ordered_normalized = cypherglot.normalize_cypher_text(
            "OPTIONAL MATCH (u:User) RETURN u AS user ORDER BY user"
        )

        self.assertEqual(type(normalized).__name__, "NormalizedOptionalMatchNode")
        self.assertIsNone(normalized.returns[0].field)
        self.assertEqual(normalized.returns[0].column_name, "user")
        self.assertEqual(type(ordered_normalized).__name__, "NormalizedOptionalMatchNode")
        self.assertEqual(ordered_normalized.order_by[0].alias, "user")
        self.assertEqual(ordered_normalized.order_by[0].field, "__value__")

    def test_normalize_cypher_text_normalizes_optional_match_count_return(
        self,
    ) -> None:
        normalized = cypherglot.normalize_cypher_text(
            "OPTIONAL MATCH (u:User) RETURN u.name AS name, count(u) AS total ORDER BY total DESC"
        )
        star_normalized = cypherglot.normalize_cypher_text(
            "OPTIONAL MATCH (u:User) RETURN u.name AS name, count(*) AS total ORDER BY total DESC"
        )

        self.assertEqual(type(normalized).__name__, "NormalizedOptionalMatchNode")
        self.assertEqual(normalized.returns[0].column_name, "name")
        self.assertEqual(normalized.returns[1].kind, "count")
        self.assertEqual(normalized.returns[1].column_name, "total")
        self.assertEqual(normalized.order_by[0].alias, "total")
        self.assertEqual(normalized.order_by[0].field, "__value__")
        self.assertEqual(type(star_normalized).__name__, "NormalizedOptionalMatchNode")
        self.assertEqual(star_normalized.returns[1].kind, "count")
        self.assertEqual(star_normalized.returns[1].alias, "*")
        self.assertEqual(star_normalized.order_by[0].alias, "total")

    def test_normalize_cypher_text_normalizes_optional_match_grouped_entity_count_return(
        self,
    ) -> None:
        normalized = cypherglot.normalize_cypher_text(
            "OPTIONAL MATCH (u:User) RETURN u AS user, count(u) AS total ORDER BY total DESC"
        )

        self.assertEqual(type(normalized).__name__, "NormalizedOptionalMatchNode")
        self.assertEqual(normalized.returns[0].kind, "entity")
        self.assertEqual(normalized.returns[0].column_name, "user")
        self.assertEqual(normalized.returns[1].kind, "count")
        self.assertEqual(normalized.returns[1].column_name, "total")
        self.assertEqual(normalized.order_by[0].alias, "total")

    def test_normalize_cypher_text_normalizes_aliased_with_returns(self) -> None:
        normalized = cypherglot.normalize_cypher_text(
            "MATCH (u:User) WITH u AS person, u.name AS name RETURN person AS user, person.name AS display_name, name AS raw_name ORDER BY display_name, raw_name"
        )

        self.assertEqual(type(normalized).__name__, "NormalizedMatchWithReturn")
        self.assertEqual(normalized.returns[0].kind, "entity")
        self.assertEqual(normalized.returns[0].column_name, "user")
        self.assertEqual(normalized.returns[1].kind, "field")
        self.assertEqual(normalized.returns[1].column_name, "display_name")
        self.assertEqual(normalized.returns[2].kind, "scalar")
        self.assertEqual(normalized.returns[2].column_name, "raw_name")
        self.assertEqual(normalized.order_by[0].kind, "field")
        self.assertEqual(normalized.order_by[0].alias, "person")
        self.assertEqual(normalized.order_by[0].field, "name")
        self.assertEqual(normalized.order_by[1].kind, "scalar")
        self.assertEqual(normalized.order_by[1].alias, "name")

    def test_normalize_cypher_text_rejects_unknown_where_alias(self) -> None:
        with self.assertRaisesRegex(ValueError, "unknown alias"):
            cypherglot.normalize_cypher_text(
                "MATCH (u:User {name: 'Alice'}) WHERE v.age = 31 RETURN u.name"
            )

    def test_normalize_cypher_text_normalizes_vector_query_nodes(self) -> None:
        normalized = cypherglot.normalize_cypher_text(
            "CALL db.index.vector.queryNodes('user_embedding_idx', 1, $query) "
            "YIELD node, score MATCH (node:User) RETURN node.id, score ORDER BY score DESC"
        )

        self.assertEqual(type(normalized).__name__, "NormalizedQueryNodesVectorSearch")
        self.assertEqual(normalized.index_name, "user_embedding_idx")
        self.assertEqual(normalized.query_param_name, "query")
        self.assertEqual(normalized.top_k, 1)
        self.assertEqual(type(normalized.candidate_query).__name__, "NormalizedMatchNode")
        self.assertEqual(normalized.candidate_query.node.alias, "node")
        self.assertEqual(normalized.return_items, ("node.id", "score"))
        self.assertEqual(normalized.order_by, (("score", "desc"),))

    def test_normalize_cypher_text_normalizes_vector_query_nodes_full_namespace(self) -> None:
        normalized = cypherglot.normalize_cypher_text(
            "CALL db.index.vector.queryNodes('user_embedding_idx', 3, $query) "
            "YIELD node, score RETURN node.id, score"
        )

        self.assertEqual(type(normalized).__name__, "NormalizedQueryNodesVectorSearch")
        self.assertEqual(normalized.top_k, 3)
        self.assertEqual(type(normalized.candidate_query).__name__, "NormalizedMatchNode")
        self.assertEqual(normalized.candidate_query.returns[0].column_name, "node.id")

    def test_normalize_cypher_text_normalizes_vector_query_nodes_yield_where(
        self,
    ) -> None:
        yield_where = cypherglot.normalize_cypher_text(
            "CALL db.index.vector.queryNodes('user_embedding_idx', 3, $query) "
            "YIELD node, score WHERE node.region = 'west' RETURN node.id, score ORDER BY score DESC"
        )

        self.assertEqual(type(yield_where).__name__, "NormalizedQueryNodesVectorSearch")
        self.assertEqual(yield_where.top_k, 3)
        self.assertEqual(type(yield_where.candidate_query).__name__, "NormalizedMatchNode")
        self.assertEqual(yield_where.candidate_query.node.alias, "node")
        self.assertEqual(yield_where.candidate_query.predicates[0].field, "region")
        self.assertEqual(yield_where.candidate_query.predicates[0].operator, "=")
        self.assertEqual(yield_where.candidate_query.predicates[0].value, "west")
        self.assertEqual(yield_where.candidate_query.returns[0].column_name, "node.id")
