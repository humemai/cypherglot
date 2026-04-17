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

    def test_normalize_cypher_text_normalizes_match_merge_self_loop_relationship(self) -> None:
        normalized = cypherglot.normalize_cypher_text(
            "MATCH (a:User) MERGE (a)-[:KNOWS]->(a)"
        )

        self.assertEqual(type(normalized).__name__, "NormalizedMatchMergeRelationshipOnNode")
        self.assertEqual(normalized.match_node.alias, "a")
        self.assertEqual(normalized.match_node.label, "User")
        self.assertEqual(normalized.relationship.type_name, "KNOWS")

    def test_normalize_cypher_text_normalizes_match_merge_with_new_endpoint(self) -> None:
        normalized = cypherglot.normalize_cypher_text(
            "MATCH (a:User) MERGE (a)-[:INTRODUCED]->(:Person {name: 'Cara'})"
        )

        self.assertEqual(type(normalized).__name__, "NormalizedMatchMergeRelationshipOnNode")
        self.assertEqual(normalized.match_node.alias, "a")
        self.assertEqual(normalized.right.label, "Person")
        self.assertEqual(normalized.right.properties, (("name", "Cara"),))
        self.assertEqual(normalized.relationship.type_name, "INTRODUCED")

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

