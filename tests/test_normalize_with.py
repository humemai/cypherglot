from __future__ import annotations

import unittest

import cypherglot


class TestNormalize(unittest.TestCase):
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

    def test_normalize_cypher_text_normalizes_derived_scalar_with_binding(self) -> None:
        normalized = cypherglot.normalize_cypher_text(
            "MATCH (u:User) WITH lower(u.name) AS lowered RETURN lowered ORDER BY lowered"
        )

        self.assertEqual(type(normalized).__name__, "NormalizedMatchWithReturn")
        self.assertEqual(normalized.bindings[0].binding_kind, "scalar")
        self.assertIsNotNone(normalized.bindings[0].expression)
        assert normalized.bindings[0].expression is not None
        self.assertEqual(normalized.bindings[0].expression.kind, "lower")
        self.assertEqual(normalized.bindings[0].expression.alias, "u")
        self.assertEqual(normalized.bindings[0].expression.field, "name")
        self.assertEqual(normalized.bindings[0].output_alias, "lowered")
        self.assertEqual(normalized.returns[0].column_name, "lowered")
        self.assertEqual(normalized.order_by[0].kind, "scalar")

    def test_normalize_cypher_text_normalizes_endpoint_derived_with_binding(
        self,
    ) -> None:
        normalized = cypherglot.normalize_cypher_text(
            "MATCH (a:User)-[r:KNOWS]->(b:User) WITH startNode(r).name AS start_name, endNode(r).id AS end_id RETURN start_name, end_id ORDER BY end_id, start_name"
        )

        self.assertEqual(type(normalized).__name__, "NormalizedMatchWithReturn")
        self.assertEqual(normalized.bindings[0].binding_kind, "scalar")
        self.assertIsNotNone(normalized.bindings[0].expression)
        self.assertIsNotNone(normalized.bindings[1].expression)
        assert normalized.bindings[0].expression is not None
        assert normalized.bindings[1].expression is not None
        self.assertEqual(normalized.bindings[0].expression.kind, "start_node")
        self.assertEqual(normalized.bindings[0].expression.alias, "r")
        self.assertEqual(normalized.bindings[0].expression.field, "name")
        self.assertEqual(normalized.bindings[0].output_alias, "start_name")
        self.assertEqual(normalized.bindings[1].expression.kind, "end_node")
        self.assertEqual(normalized.bindings[1].expression.alias, "r")
        self.assertEqual(normalized.bindings[1].expression.field, "id")
        self.assertEqual(normalized.bindings[1].output_alias, "end_id")
        self.assertEqual(normalized.returns[0].column_name, "start_name")
        self.assertEqual(normalized.returns[1].column_name, "end_id")
        self.assertEqual(normalized.order_by[0].alias, "end_id")
        self.assertEqual(normalized.order_by[1].alias, "start_name")

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

