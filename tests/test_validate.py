from __future__ import annotations

import unittest

import cypherglot


class TestValidate(unittest.TestCase):
    def test_validate_cypher_text_accepts_create_subset(self) -> None:
        validated = cypherglot.validate_cypher_text(
            "CREATE (u:User {name: 'Alice'})"
        )

        self.assertEqual(type(validated).__name__, "OC_SinglePartQueryContext")

    def test_validate_cypher_text_accepts_match_return_subset(self) -> None:
        validated = cypherglot.validate_cypher_text(
            "MATCH (u:User) RETURN u.name ORDER BY u.name LIMIT 1"
        )

        self.assertEqual(type(validated).__name__, "OC_SinglePartQueryContext")

    def test_validate_cypher_text_accepts_match_where_id_and_type_filters(self) -> None:
        id_validated = cypherglot.validate_cypher_text(
            "MATCH (u:User) WHERE id(u) >= 1 RETURN u.name ORDER BY u.name"
        )
        type_validated = cypherglot.validate_cypher_text(
            "MATCH (a:User)-[r:KNOWS]->(b:User) WHERE type(r) = 'KNOWS' RETURN b.name ORDER BY b.name"
        )

        self.assertEqual(type(id_validated).__name__, "OC_SinglePartQueryContext")
        self.assertEqual(type(type_validated).__name__, "OC_SinglePartQueryContext")

    def test_validate_cypher_text_accepts_entity_match_return_subset(self) -> None:
        validated = cypherglot.validate_cypher_text(
            "MATCH (u:User) RETURN u AS user"
        )
        ordered_validated = cypherglot.validate_cypher_text(
            "MATCH (u:User) RETURN u AS user ORDER BY user"
        )

        self.assertEqual(type(validated).__name__, "OC_SinglePartQueryContext")
        self.assertEqual(type(ordered_validated).__name__, "OC_SinglePartQueryContext")

    def test_validate_cypher_text_accepts_relationship_entity_match_return_subset(
        self,
    ) -> None:
        validated = cypherglot.validate_cypher_text(
            "MATCH (a:User)-[r:KNOWS]->(b:User) RETURN a AS user, r AS rel, b.name AS name ORDER BY name"
        )
        ordered_validated = cypherglot.validate_cypher_text(
            "MATCH (a:User)-[r:KNOWS]->(b:User) RETURN r AS rel ORDER BY rel"
        )

        self.assertEqual(type(validated).__name__, "OC_SinglePartQueryContext")
        self.assertEqual(type(ordered_validated).__name__, "OC_SinglePartQueryContext")

    def test_validate_cypher_text_accepts_fixed_length_multi_hop_reads(self) -> None:
        validated = cypherglot.validate_cypher_text(
            "MATCH (a:User)-[r:KNOWS]->(b:User)-[s:WORKS_AT]->(c:Company) RETURN c.name ORDER BY c.name"
        )
        with_validated = cypherglot.validate_cypher_text(
            "MATCH (a:User)-[r:KNOWS]->(b:User)-[s:WORKS_AT]->(c:Company) WITH b AS friend, c.name AS company RETURN friend.name, company ORDER BY company"
        )

        self.assertEqual(type(validated).__name__, "OC_SinglePartQueryContext")
        self.assertEqual(type(with_validated).__name__, "OC_MultiPartQueryContext")

    def test_validate_cypher_text_accepts_bounded_variable_length_reads(self) -> None:
        validated = cypherglot.validate_cypher_text(
            "MATCH (a:User)-[:KNOWS*1..2]->(b:User) RETURN b.name ORDER BY b.name"
        )
        alias_validated = cypherglot.validate_cypher_text(
            "MATCH (a:User)-[r:KNOWS*1..2]->(b:User) RETURN b.name ORDER BY b.name"
        )
        with_validated = cypherglot.validate_cypher_text(
            "MATCH (a:User)-[:KNOWS*..2]->(b:User) WITH b AS friend RETURN friend.name ORDER BY friend.name"
        )

        self.assertEqual(type(validated).__name__, "OC_SinglePartQueryContext")
        self.assertEqual(type(alias_validated).__name__, "OC_SinglePartQueryContext")
        self.assertEqual(type(with_validated).__name__, "OC_MultiPartQueryContext")

    def test_validate_cypher_text_accepts_plain_read_count_subset(self) -> None:
        validated = cypherglot.validate_cypher_text(
            "MATCH (u:User) RETURN count(u) AS total"
        )
        star_validated = cypherglot.validate_cypher_text(
            "MATCH (u:User) RETURN count(*) AS total"
        )
        no_alias_validated = cypherglot.validate_cypher_text(
            "MATCH (u:User) RETURN count(*)"
        )

        self.assertEqual(type(validated).__name__, "OC_SinglePartQueryContext")
        self.assertEqual(type(star_validated).__name__, "OC_SinglePartQueryContext")
        self.assertEqual(type(no_alias_validated).__name__, "OC_SinglePartQueryContext")

    def test_validate_cypher_text_accepts_plain_read_numeric_aggregates(self) -> None:
        summed_validated = cypherglot.validate_cypher_text(
            "MATCH (u:User) RETURN sum(u.score) AS total"
        )
        grouped_validated = cypherglot.validate_cypher_text(
            "MATCH (u:User) RETURN u.name AS name, avg(u.score) AS mean ORDER BY mean DESC"
        )
        optional_validated = cypherglot.validate_cypher_text(
            "OPTIONAL MATCH (u:User) RETURN u.name AS name, max(u.score) AS top ORDER BY top DESC"
        )
        relationship_validated = cypherglot.validate_cypher_text(
            "MATCH (a:User)-[r:KNOWS]->(b:User) RETURN min(r.weight) AS lightest"
        )
        no_alias_validated = cypherglot.validate_cypher_text(
            "MATCH (u:User) RETURN sum(u.score)"
        )

        self.assertEqual(type(summed_validated).__name__, "OC_SinglePartQueryContext")
        self.assertEqual(type(grouped_validated).__name__, "OC_SinglePartQueryContext")
        self.assertEqual(type(optional_validated).__name__, "OC_SinglePartQueryContext")
        self.assertEqual(type(relationship_validated).__name__, "OC_SinglePartQueryContext")
        self.assertEqual(type(no_alias_validated).__name__, "OC_SinglePartQueryContext")

    def test_validate_cypher_text_accepts_searched_case_returns(self) -> None:
        validated = cypherglot.validate_cypher_text(
            "MATCH (u:User) RETURN CASE WHEN u.age >= 18 THEN u.name ELSE 'minor' END AS label ORDER BY label"
        )
        optional_validated = cypherglot.validate_cypher_text(
            "OPTIONAL MATCH (u:User) RETURN CASE WHEN u.age >= 18 THEN u.name ELSE 'minor' END AS label ORDER BY label"
        )

        self.assertEqual(type(validated).__name__, "OC_SinglePartQueryContext")
        self.assertEqual(type(optional_validated).__name__, "OC_SinglePartQueryContext")

    def test_validate_cypher_text_accepts_properties_returns(self) -> None:
        validated = cypherglot.validate_cypher_text(
            "MATCH (u:User) RETURN properties(u) AS props ORDER BY props"
        )
        relationship_validated = cypherglot.validate_cypher_text(
            "MATCH (a:User)-[r:KNOWS]->(b:User) RETURN properties(r) AS props ORDER BY props"
        )
        optional_validated = cypherglot.validate_cypher_text(
            "OPTIONAL MATCH (u:User) RETURN properties(u) AS props ORDER BY props"
        )
        no_alias_validated = cypherglot.validate_cypher_text(
            "MATCH (u:User) RETURN properties(u)"
        )

        self.assertEqual(type(validated).__name__, "OC_SinglePartQueryContext")
        self.assertEqual(type(relationship_validated).__name__, "OC_SinglePartQueryContext")
        self.assertEqual(type(optional_validated).__name__, "OC_SinglePartQueryContext")
        self.assertEqual(type(no_alias_validated).__name__, "OC_SinglePartQueryContext")

    def test_validate_cypher_text_accepts_labels_returns(self) -> None:
        validated = cypherglot.validate_cypher_text(
            "MATCH (u:User) RETURN labels(u) AS labels ORDER BY labels"
        )
        optional_validated = cypherglot.validate_cypher_text(
            "OPTIONAL MATCH (u:User) RETURN labels(u) AS labels ORDER BY labels"
        )

        self.assertEqual(type(validated).__name__, "OC_SinglePartQueryContext")
        self.assertEqual(type(optional_validated).__name__, "OC_SinglePartQueryContext")

    def test_validate_cypher_text_accepts_keys_returns(self) -> None:
        validated = cypherglot.validate_cypher_text(
            "MATCH (u:User) RETURN keys(u) AS keys ORDER BY keys"
        )
        relationship_validated = cypherglot.validate_cypher_text(
            "MATCH (a:User)-[r:KNOWS]->(b:User) RETURN keys(r) AS keys ORDER BY keys"
        )
        optional_validated = cypherglot.validate_cypher_text(
            "OPTIONAL MATCH (u:User) RETURN keys(u) AS keys ORDER BY keys"
        )

        self.assertEqual(type(validated).__name__, "OC_SinglePartQueryContext")
        self.assertEqual(type(relationship_validated).__name__, "OC_SinglePartQueryContext")
        self.assertEqual(type(optional_validated).__name__, "OC_SinglePartQueryContext")

    def test_validate_cypher_text_accepts_start_and_end_node_returns(self) -> None:
        validated = cypherglot.validate_cypher_text(
            "MATCH (a:User)-[r:KNOWS]->(b:User) RETURN startNode(r) AS start, endNode(r) AS ending ORDER BY start, ending"
        )
        field_validated = cypherglot.validate_cypher_text(
            "MATCH (a:User)-[r:KNOWS]->(b:User) RETURN startNode(r).id AS start_id, endNode(r).id AS end_id ORDER BY start_id, end_id"
        )

        self.assertEqual(type(validated).__name__, "OC_SinglePartQueryContext")
        self.assertEqual(type(field_validated).__name__, "OC_SinglePartQueryContext")

    def test_validate_cypher_text_accepts_grouped_entity_count_subset(self) -> None:
        validated = cypherglot.validate_cypher_text(
            "MATCH (u:User) RETURN u AS user, count(u) AS total ORDER BY total DESC"
        )
        star_validated = cypherglot.validate_cypher_text(
            "MATCH (u:User) RETURN u AS user, count(*) AS total ORDER BY total DESC"
        )
        relationship_validated = cypherglot.validate_cypher_text(
            "MATCH (a:User)-[r:KNOWS]->(b:User) RETURN r AS rel, count(r) AS total ORDER BY total DESC"
        )
        relationship_star_validated = cypherglot.validate_cypher_text(
            "MATCH (a:User)-[r:KNOWS]->(b:User) RETURN r AS rel, count(*) AS total ORDER BY total DESC"
        )

        self.assertEqual(type(validated).__name__, "OC_SinglePartQueryContext")
        self.assertEqual(type(star_validated).__name__, "OC_SinglePartQueryContext")
        self.assertEqual(type(relationship_validated).__name__, "OC_SinglePartQueryContext")
        self.assertEqual(type(relationship_star_validated).__name__, "OC_SinglePartQueryContext")

    def test_validate_cypher_text_accepts_scalar_literal_and_parameter_returns(
        self,
    ) -> None:
        literal_validated = cypherglot.validate_cypher_text(
            "MATCH (u:User) RETURN 'tag' AS tag ORDER BY tag"
        )
        parameter_validated = cypherglot.validate_cypher_text(
            "MATCH (u:User) RETURN $value AS value ORDER BY value"
        )

        self.assertEqual(type(literal_validated).__name__, "OC_SinglePartQueryContext")
        self.assertEqual(type(parameter_validated).__name__, "OC_SinglePartQueryContext")

    def test_validate_cypher_text_accepts_size_returns(self) -> None:
        field_validated = cypherglot.validate_cypher_text(
            "MATCH (u:User) RETURN size(u.name) AS name_len ORDER BY name_len"
        )
        no_alias_validated = cypherglot.validate_cypher_text(
            "MATCH (u:User) RETURN size(u.name), size(id(u))"
        )
        literal_validated = cypherglot.validate_cypher_text(
            "MATCH (u:User) RETURN size('tag') AS tag_len ORDER BY tag_len"
        )
        id_validated = cypherglot.validate_cypher_text(
            "MATCH (u:User) RETURN size(id(u)) AS id_len ORDER BY id_len"
        )
        type_validated = cypherglot.validate_cypher_text(
            "MATCH (a:User)-[r:KNOWS]->(b:User) RETURN size(type(r)) AS type_len ORDER BY type_len"
        )
        optional_validated = cypherglot.validate_cypher_text(
            "OPTIONAL MATCH (u:User) RETURN size(u.name) AS name_len ORDER BY name_len"
        )
        optional_id_validated = cypherglot.validate_cypher_text(
            "OPTIONAL MATCH (u:User) RETURN size(id(u)) AS id_len ORDER BY id_len"
        )

        self.assertEqual(type(field_validated).__name__, "OC_SinglePartQueryContext")
        self.assertEqual(type(no_alias_validated).__name__, "OC_SinglePartQueryContext")
        self.assertEqual(type(literal_validated).__name__, "OC_SinglePartQueryContext")
        self.assertEqual(type(id_validated).__name__, "OC_SinglePartQueryContext")
        self.assertEqual(type(type_validated).__name__, "OC_SinglePartQueryContext")
        self.assertEqual(type(optional_validated).__name__, "OC_SinglePartQueryContext")
        self.assertEqual(type(optional_id_validated).__name__, "OC_SinglePartQueryContext")

    def test_validate_cypher_text_accepts_lower_and_upper_returns(self) -> None:
        lower_validated = cypherglot.validate_cypher_text(
            "MATCH (u:User) RETURN lower(u.name) AS lower_name ORDER BY lower_name"
        )
        no_alias_validated = cypherglot.validate_cypher_text(
            "MATCH (u:User) RETURN lower(u.name), reverse(u.name)"
        )
        upper_literal_validated = cypherglot.validate_cypher_text(
            "MATCH (u:User) RETURN upper('tag') AS upper_tag ORDER BY upper_tag"
        )
        trim_validated = cypherglot.validate_cypher_text(
            "MATCH (u:User) RETURN trim(u.name) AS trimmed ORDER BY trimmed"
        )
        ltrim_validated = cypherglot.validate_cypher_text(
            "MATCH (u:User) RETURN ltrim(' tag') AS left_trimmed ORDER BY left_trimmed"
        )
        rtrim_validated = cypherglot.validate_cypher_text(
            "MATCH (u:User) RETURN rtrim('tag ') AS right_trimmed ORDER BY right_trimmed"
        )
        reverse_validated = cypherglot.validate_cypher_text(
            "MATCH (u:User) RETURN reverse(u.name) AS reversed_name ORDER BY reversed_name"
        )
        optional_lower_validated = cypherglot.validate_cypher_text(
            "OPTIONAL MATCH (u:User) RETURN lower(u.name) AS lower_name ORDER BY lower_name"
        )
        optional_trim_validated = cypherglot.validate_cypher_text(
            "OPTIONAL MATCH (u:User) RETURN trim(u.name) AS trimmed ORDER BY trimmed"
        )
        optional_ltrim_validated = cypherglot.validate_cypher_text(
            "OPTIONAL MATCH (u:User) RETURN ltrim(u.name) AS left_trimmed ORDER BY left_trimmed"
        )
        optional_rtrim_validated = cypherglot.validate_cypher_text(
            "OPTIONAL MATCH (u:User) RETURN rtrim(u.name) AS right_trimmed ORDER BY right_trimmed"
        )
        optional_reverse_validated = cypherglot.validate_cypher_text(
            "OPTIONAL MATCH (u:User) RETURN reverse(u.name) AS reversed_name ORDER BY reversed_name"
        )

        self.assertEqual(type(lower_validated).__name__, "OC_SinglePartQueryContext")
        self.assertEqual(type(no_alias_validated).__name__, "OC_SinglePartQueryContext")
        self.assertEqual(type(upper_literal_validated).__name__, "OC_SinglePartQueryContext")
        self.assertEqual(type(trim_validated).__name__, "OC_SinglePartQueryContext")
        self.assertEqual(type(ltrim_validated).__name__, "OC_SinglePartQueryContext")
        self.assertEqual(type(rtrim_validated).__name__, "OC_SinglePartQueryContext")
        self.assertEqual(type(reverse_validated).__name__, "OC_SinglePartQueryContext")
        self.assertEqual(type(optional_lower_validated).__name__, "OC_SinglePartQueryContext")
        self.assertEqual(type(optional_trim_validated).__name__, "OC_SinglePartQueryContext")
        self.assertEqual(type(optional_ltrim_validated).__name__, "OC_SinglePartQueryContext")
        self.assertEqual(type(optional_rtrim_validated).__name__, "OC_SinglePartQueryContext")
        self.assertEqual(type(optional_reverse_validated).__name__, "OC_SinglePartQueryContext")

    def test_validate_cypher_text_accepts_relationship_property_string_returns(self) -> None:
        validated = cypherglot.validate_cypher_text(
            "MATCH (a:User)-[r:KNOWS]->(b:User) RETURN lower(r.note) AS lower_note, upper(r.note) AS upper_note, trim(r.note) AS trimmed_note, ltrim(r.note) AS left_trimmed_note, rtrim(r.note) AS right_trimmed_note, reverse(r.note) AS reversed_note, coalesce(r.note, 'unknown') AS display_note, replace(r.note, 'A', 'B') AS replaced_note, left(r.note, 2) AS prefix, right(r.note, 2) AS suffix, split(r.note, ' ') AS parts, substring(r.note, 1) AS tail ORDER BY lower_note, upper_note, trimmed_note, left_trimmed_note, right_trimmed_note, reversed_note, display_note, replaced_note, prefix, suffix, parts, tail"
        )

        self.assertEqual(type(validated).__name__, "OC_SinglePartQueryContext")

    def test_validate_cypher_text_accepts_relationship_property_numeric_and_conversion_returns(
        self,
    ) -> None:
        validated = cypherglot.validate_cypher_text(
            "MATCH (a:User)-[r:KNOWS]->(b:User) RETURN abs(r.weight) AS magnitude, sign(r.weight) AS weight_sign, round(r.score) AS rounded_score, ceil(r.score) AS ceil_score, floor(r.score) AS floor_score, sqrt(r.score) AS sqrt_score, exp(r.score) AS exp_score, sin(r.score) AS sin_score, toString(r.weight) AS weight_text, toInteger(r.score) AS score_int, toFloat(r.weight) AS weight_float, toBoolean(r.active) AS active_bool ORDER BY magnitude, weight_sign, rounded_score, ceil_score, floor_score, sqrt_score, exp_score, sin_score, weight_text, score_int, weight_float, active_bool"
        )

        self.assertEqual(type(validated).__name__, "OC_SinglePartQueryContext")

    def test_validate_cypher_text_accepts_coalesce_returns(self) -> None:
        field_validated = cypherglot.validate_cypher_text(
            "MATCH (u:User) RETURN coalesce(u.name, 'unknown') AS display_name ORDER BY display_name"
        )
        no_alias_validated = cypherglot.validate_cypher_text(
            "MATCH (u:User) RETURN coalesce(u.name, 'unknown')"
        )
        optional_validated = cypherglot.validate_cypher_text(
            "OPTIONAL MATCH (u:User) RETURN coalesce(u.name, $fallback) AS display_name ORDER BY display_name"
        )

        self.assertEqual(type(field_validated).__name__, "OC_SinglePartQueryContext")
        self.assertEqual(type(no_alias_validated).__name__, "OC_SinglePartQueryContext")
        self.assertEqual(type(optional_validated).__name__, "OC_SinglePartQueryContext")

    def test_validate_cypher_text_accepts_replace_returns(self) -> None:
        field_validated = cypherglot.validate_cypher_text(
            "MATCH (u:User) RETURN replace(u.name, 'A', 'B') AS display_name ORDER BY display_name"
        )
        no_alias_validated = cypherglot.validate_cypher_text(
            "MATCH (u:User) RETURN replace(u.name, 'A', 'B')"
        )
        literal_validated = cypherglot.validate_cypher_text(
            "MATCH (u:User) RETURN replace('Alice', 'l', 'x') AS alias_name ORDER BY alias_name"
        )
        optional_validated = cypherglot.validate_cypher_text(
            "OPTIONAL MATCH (u:User) RETURN replace(u.name, $needle, $replacement) AS display_name ORDER BY display_name"
        )

        self.assertEqual(type(field_validated).__name__, "OC_SinglePartQueryContext")
        self.assertEqual(type(no_alias_validated).__name__, "OC_SinglePartQueryContext")
        self.assertEqual(type(literal_validated).__name__, "OC_SinglePartQueryContext")
        self.assertEqual(type(optional_validated).__name__, "OC_SinglePartQueryContext")

    def test_validate_cypher_text_accepts_left_and_right_returns(self) -> None:
        left_validated = cypherglot.validate_cypher_text(
            "MATCH (u:User) RETURN left(u.name, 2) AS prefix ORDER BY prefix"
        )
        no_alias_validated = cypherglot.validate_cypher_text(
            "MATCH (u:User) RETURN left(u.name, 2)"
        )
        right_literal_validated = cypherglot.validate_cypher_text(
            "MATCH (u:User) RETURN right('Alice', 3) AS suffix ORDER BY suffix"
        )
        optional_validated = cypherglot.validate_cypher_text(
            "OPTIONAL MATCH (u:User) RETURN right(u.name, $count) AS suffix ORDER BY suffix"
        )

        self.assertEqual(type(left_validated).__name__, "OC_SinglePartQueryContext")
        self.assertEqual(type(no_alias_validated).__name__, "OC_SinglePartQueryContext")
        self.assertEqual(type(right_literal_validated).__name__, "OC_SinglePartQueryContext")
        self.assertEqual(type(optional_validated).__name__, "OC_SinglePartQueryContext")

    def test_validate_cypher_text_accepts_split_returns(self) -> None:
        field_validated = cypherglot.validate_cypher_text(
            "MATCH (u:User) RETURN split(u.name, ' ') AS parts ORDER BY parts"
        )
        no_alias_validated = cypherglot.validate_cypher_text(
            "MATCH (u:User) RETURN split(u.name, ' ')"
        )
        literal_validated = cypherglot.validate_cypher_text(
            "MATCH (u:User) RETURN split('Alice Bob', $delimiter) AS parts ORDER BY parts"
        )
        optional_validated = cypherglot.validate_cypher_text(
            "OPTIONAL MATCH (u:User) RETURN split(u.name, $delimiter) AS parts ORDER BY parts"
        )

        self.assertEqual(type(field_validated).__name__, "OC_SinglePartQueryContext")
        self.assertEqual(type(no_alias_validated).__name__, "OC_SinglePartQueryContext")
        self.assertEqual(type(literal_validated).__name__, "OC_SinglePartQueryContext")
        self.assertEqual(type(optional_validated).__name__, "OC_SinglePartQueryContext")

    def test_validate_cypher_text_accepts_abs_returns(self) -> None:
        field_validated = cypherglot.validate_cypher_text(
            "MATCH (u:User) RETURN abs(u.age) AS magnitude ORDER BY magnitude"
        )
        no_alias_validated = cypherglot.validate_cypher_text(
            "MATCH (u:User) RETURN abs(u.age), sign(u.age)"
        )
        literal_validated = cypherglot.validate_cypher_text(
            "MATCH (u:User) RETURN abs(-3) AS magnitude ORDER BY magnitude"
        )
        sign_validated = cypherglot.validate_cypher_text(
            "MATCH (u:User) RETURN sign(u.age) AS age_sign ORDER BY age_sign"
        )
        sign_literal_validated = cypherglot.validate_cypher_text(
            "MATCH (u:User) RETURN sign(-3.2) AS age_sign ORDER BY age_sign"
        )
        optional_validated = cypherglot.validate_cypher_text(
            "OPTIONAL MATCH (u:User) RETURN abs(u.age) AS magnitude ORDER BY magnitude"
        )
        optional_sign_validated = cypherglot.validate_cypher_text(
            "OPTIONAL MATCH (u:User) RETURN sign(u.age) AS age_sign ORDER BY age_sign"
        )

        self.assertEqual(type(field_validated).__name__, "OC_SinglePartQueryContext")
        self.assertEqual(type(no_alias_validated).__name__, "OC_SinglePartQueryContext")
        self.assertEqual(type(literal_validated).__name__, "OC_SinglePartQueryContext")
        self.assertEqual(type(sign_validated).__name__, "OC_SinglePartQueryContext")
        self.assertEqual(type(sign_literal_validated).__name__, "OC_SinglePartQueryContext")
        self.assertEqual(type(optional_validated).__name__, "OC_SinglePartQueryContext")
        self.assertEqual(type(optional_sign_validated).__name__, "OC_SinglePartQueryContext")

    def test_validate_cypher_text_accepts_round_returns(self) -> None:
        field_validated = cypherglot.validate_cypher_text(
            "MATCH (u:User) RETURN round(u.score) AS value ORDER BY value"
        )
        literal_validated = cypherglot.validate_cypher_text(
            "MATCH (u:User) RETURN round(-3.2) AS value ORDER BY value"
        )
        optional_validated = cypherglot.validate_cypher_text(
            "OPTIONAL MATCH (u:User) RETURN round(u.score) AS value ORDER BY value"
        )

        self.assertEqual(type(field_validated).__name__, "OC_SinglePartQueryContext")
        self.assertEqual(type(literal_validated).__name__, "OC_SinglePartQueryContext")
        self.assertEqual(type(optional_validated).__name__, "OC_SinglePartQueryContext")

    def test_validate_cypher_text_accepts_ceil_returns(self) -> None:
        field_validated = cypherglot.validate_cypher_text(
            "MATCH (u:User) RETURN ceil(u.score) AS value ORDER BY value"
        )
        literal_validated = cypherglot.validate_cypher_text(
            "MATCH (u:User) RETURN ceil(-3.2) AS value ORDER BY value"
        )
        optional_validated = cypherglot.validate_cypher_text(
            "OPTIONAL MATCH (u:User) RETURN ceil(u.score) AS value ORDER BY value"
        )

        self.assertEqual(type(field_validated).__name__, "OC_SinglePartQueryContext")
        self.assertEqual(type(literal_validated).__name__, "OC_SinglePartQueryContext")
        self.assertEqual(type(optional_validated).__name__, "OC_SinglePartQueryContext")

    def test_validate_cypher_text_accepts_floor_returns(self) -> None:
        field_validated = cypherglot.validate_cypher_text(
            "MATCH (u:User) RETURN floor(u.score) AS value ORDER BY value"
        )
        literal_validated = cypherglot.validate_cypher_text(
            "MATCH (u:User) RETURN floor(-3.2) AS value ORDER BY value"
        )
        optional_validated = cypherglot.validate_cypher_text(
            "OPTIONAL MATCH (u:User) RETURN floor(u.score) AS value ORDER BY value"
        )

        self.assertEqual(type(field_validated).__name__, "OC_SinglePartQueryContext")
        self.assertEqual(type(literal_validated).__name__, "OC_SinglePartQueryContext")
        self.assertEqual(type(optional_validated).__name__, "OC_SinglePartQueryContext")

    def test_validate_cypher_text_accepts_sqrt_returns(self) -> None:
        field_validated = cypherglot.validate_cypher_text(
            "MATCH (u:User) RETURN sqrt(u.score) AS value ORDER BY value"
        )
        literal_validated = cypherglot.validate_cypher_text(
            "MATCH (u:User) RETURN sqrt(-3.2) AS value ORDER BY value"
        )
        optional_validated = cypherglot.validate_cypher_text(
            "OPTIONAL MATCH (u:User) RETURN sqrt(u.score) AS value ORDER BY value"
        )

        self.assertEqual(type(field_validated).__name__, "OC_SinglePartQueryContext")
        self.assertEqual(type(literal_validated).__name__, "OC_SinglePartQueryContext")
        self.assertEqual(type(optional_validated).__name__, "OC_SinglePartQueryContext")

    def test_validate_cypher_text_accepts_exp_returns(self) -> None:
        field_validated = cypherglot.validate_cypher_text(
            "MATCH (u:User) RETURN exp(u.score) AS value ORDER BY value"
        )
        literal_validated = cypherglot.validate_cypher_text(
            "MATCH (u:User) RETURN exp(-3.2) AS value ORDER BY value"
        )
        optional_validated = cypherglot.validate_cypher_text(
            "OPTIONAL MATCH (u:User) RETURN exp(u.score) AS value ORDER BY value"
        )

        self.assertEqual(type(field_validated).__name__, "OC_SinglePartQueryContext")
        self.assertEqual(type(literal_validated).__name__, "OC_SinglePartQueryContext")
        self.assertEqual(type(optional_validated).__name__, "OC_SinglePartQueryContext")

    def test_validate_cypher_text_accepts_sin_returns(self) -> None:
        field_validated = cypherglot.validate_cypher_text(
            "MATCH (u:User) RETURN sin(u.score) AS value ORDER BY value"
        )
        literal_validated = cypherglot.validate_cypher_text(
            "MATCH (u:User) RETURN sin(-3.2) AS value ORDER BY value"
        )
        optional_validated = cypherglot.validate_cypher_text(
            "OPTIONAL MATCH (u:User) RETURN sin(u.score) AS value ORDER BY value"
        )

        self.assertEqual(type(field_validated).__name__, "OC_SinglePartQueryContext")
        self.assertEqual(type(literal_validated).__name__, "OC_SinglePartQueryContext")
        self.assertEqual(type(optional_validated).__name__, "OC_SinglePartQueryContext")

    def test_validate_cypher_text_accepts_cos_returns(self) -> None:
        field_validated = cypherglot.validate_cypher_text(
            "MATCH (u:User) RETURN cos(u.score) AS value ORDER BY value"
        )
        literal_validated = cypherglot.validate_cypher_text(
            "MATCH (u:User) RETURN cos(-3.2) AS value ORDER BY value"
        )
        optional_validated = cypherglot.validate_cypher_text(
            "OPTIONAL MATCH (u:User) RETURN cos(u.score) AS value ORDER BY value"
        )

        self.assertEqual(type(field_validated).__name__, "OC_SinglePartQueryContext")
        self.assertEqual(type(literal_validated).__name__, "OC_SinglePartQueryContext")
        self.assertEqual(type(optional_validated).__name__, "OC_SinglePartQueryContext")

    def test_validate_cypher_text_accepts_tan_returns(self) -> None:
        field_validated = cypherglot.validate_cypher_text(
            "MATCH (u:User) RETURN tan(u.score) AS value ORDER BY value"
        )
        literal_validated = cypherglot.validate_cypher_text(
            "MATCH (u:User) RETURN tan(-3.2) AS value ORDER BY value"
        )
        optional_validated = cypherglot.validate_cypher_text(
            "OPTIONAL MATCH (u:User) RETURN tan(u.score) AS value ORDER BY value"
        )

        self.assertEqual(type(field_validated).__name__, "OC_SinglePartQueryContext")
        self.assertEqual(type(literal_validated).__name__, "OC_SinglePartQueryContext")
        self.assertEqual(type(optional_validated).__name__, "OC_SinglePartQueryContext")

    def test_validate_cypher_text_accepts_asin_returns(self) -> None:
        field_validated = cypherglot.validate_cypher_text(
            "MATCH (u:User) RETURN asin(u.score) AS value ORDER BY value"
        )
        literal_validated = cypherglot.validate_cypher_text(
            "MATCH (u:User) RETURN asin(-0.5) AS value ORDER BY value"
        )
        optional_validated = cypherglot.validate_cypher_text(
            "OPTIONAL MATCH (u:User) RETURN asin(u.score) AS value ORDER BY value"
        )

        self.assertEqual(type(field_validated).__name__, "OC_SinglePartQueryContext")
        self.assertEqual(type(literal_validated).__name__, "OC_SinglePartQueryContext")
        self.assertEqual(type(optional_validated).__name__, "OC_SinglePartQueryContext")

    def test_validate_cypher_text_accepts_acos_returns(self) -> None:
        field_validated = cypherglot.validate_cypher_text(
            "MATCH (u:User) RETURN acos(u.score) AS value ORDER BY value"
        )
        literal_validated = cypherglot.validate_cypher_text(
            "MATCH (u:User) RETURN acos(-0.5) AS value ORDER BY value"
        )
        optional_validated = cypherglot.validate_cypher_text(
            "OPTIONAL MATCH (u:User) RETURN acos(u.score) AS value ORDER BY value"
        )

        self.assertEqual(type(field_validated).__name__, "OC_SinglePartQueryContext")
        self.assertEqual(type(literal_validated).__name__, "OC_SinglePartQueryContext")
        self.assertEqual(type(optional_validated).__name__, "OC_SinglePartQueryContext")

    def test_validate_cypher_text_accepts_atan_returns(self) -> None:
        field_validated = cypherglot.validate_cypher_text(
            "MATCH (u:User) RETURN atan(u.score) AS value ORDER BY value"
        )
        literal_validated = cypherglot.validate_cypher_text(
            "MATCH (u:User) RETURN atan(-0.5) AS value ORDER BY value"
        )
        optional_validated = cypherglot.validate_cypher_text(
            "OPTIONAL MATCH (u:User) RETURN atan(u.score) AS value ORDER BY value"
        )

        self.assertEqual(type(field_validated).__name__, "OC_SinglePartQueryContext")
        self.assertEqual(type(literal_validated).__name__, "OC_SinglePartQueryContext")
        self.assertEqual(type(optional_validated).__name__, "OC_SinglePartQueryContext")

    def test_validate_cypher_text_accepts_ln_returns(self) -> None:
        field_validated = cypherglot.validate_cypher_text(
            "MATCH (u:User) RETURN ln(u.score) AS value ORDER BY value"
        )
        literal_validated = cypherglot.validate_cypher_text(
            "MATCH (u:User) RETURN ln(0.5) AS value ORDER BY value"
        )
        optional_validated = cypherglot.validate_cypher_text(
            "OPTIONAL MATCH (u:User) RETURN ln(u.score) AS value ORDER BY value"
        )

        self.assertEqual(type(field_validated).__name__, "OC_SinglePartQueryContext")
        self.assertEqual(type(literal_validated).__name__, "OC_SinglePartQueryContext")
        self.assertEqual(type(optional_validated).__name__, "OC_SinglePartQueryContext")

    def test_validate_cypher_text_accepts_log_returns(self) -> None:
        field_validated = cypherglot.validate_cypher_text(
            "MATCH (u:User) RETURN log(u.score) AS value ORDER BY value"
        )
        literal_validated = cypherglot.validate_cypher_text(
            "MATCH (u:User) RETURN log(0.5) AS value ORDER BY value"
        )
        optional_validated = cypherglot.validate_cypher_text(
            "OPTIONAL MATCH (u:User) RETURN log(u.score) AS value ORDER BY value"
        )

        self.assertEqual(type(field_validated).__name__, "OC_SinglePartQueryContext")
        self.assertEqual(type(literal_validated).__name__, "OC_SinglePartQueryContext")
        self.assertEqual(type(optional_validated).__name__, "OC_SinglePartQueryContext")

    def test_validate_cypher_text_accepts_radians_returns(self) -> None:
        field_validated = cypherglot.validate_cypher_text(
            "MATCH (u:User) RETURN radians(u.score) AS value ORDER BY value"
        )
        literal_validated = cypherglot.validate_cypher_text(
            "MATCH (u:User) RETURN radians(180) AS value ORDER BY value"
        )
        optional_validated = cypherglot.validate_cypher_text(
            "OPTIONAL MATCH (u:User) RETURN radians(u.score) AS value ORDER BY value"
        )

        self.assertEqual(type(field_validated).__name__, "OC_SinglePartQueryContext")
        self.assertEqual(type(literal_validated).__name__, "OC_SinglePartQueryContext")
        self.assertEqual(type(optional_validated).__name__, "OC_SinglePartQueryContext")

    def test_validate_cypher_text_accepts_degrees_returns(self) -> None:
        field_validated = cypherglot.validate_cypher_text(
            "MATCH (u:User) RETURN degrees(u.score) AS value ORDER BY value"
        )
        literal_validated = cypherglot.validate_cypher_text(
            "MATCH (u:User) RETURN degrees(3.14159) AS value ORDER BY value"
        )
        optional_validated = cypherglot.validate_cypher_text(
            "OPTIONAL MATCH (u:User) RETURN degrees(u.score) AS value ORDER BY value"
        )

        self.assertEqual(type(field_validated).__name__, "OC_SinglePartQueryContext")
        self.assertEqual(type(literal_validated).__name__, "OC_SinglePartQueryContext")
        self.assertEqual(type(optional_validated).__name__, "OC_SinglePartQueryContext")

    def test_validate_cypher_text_accepts_log10_returns(self) -> None:
        field_validated = cypherglot.validate_cypher_text(
            "MATCH (u:User) RETURN log10(u.score) AS value ORDER BY value"
        )
        literal_validated = cypherglot.validate_cypher_text(
            "MATCH (u:User) RETURN log10(0.5) AS value ORDER BY value"
        )
        optional_validated = cypherglot.validate_cypher_text(
            "OPTIONAL MATCH (u:User) RETURN log10(u.score) AS value ORDER BY value"
        )

        self.assertEqual(type(field_validated).__name__, "OC_SinglePartQueryContext")
        self.assertEqual(type(literal_validated).__name__, "OC_SinglePartQueryContext")
        self.assertEqual(type(optional_validated).__name__, "OC_SinglePartQueryContext")

    def test_validate_cypher_text_accepts_to_string_returns(self) -> None:
        field_validated = cypherglot.validate_cypher_text(
            "MATCH (u:User) RETURN toString(u.age) AS text ORDER BY text"
        )
        no_alias_validated = cypherglot.validate_cypher_text(
            "MATCH (u:User) RETURN toString(u.age), toBoolean(u.active)"
        )
        literal_validated = cypherglot.validate_cypher_text(
            "MATCH (u:User) RETURN toString(-3) AS text ORDER BY text"
        )
        optional_validated = cypherglot.validate_cypher_text(
            "OPTIONAL MATCH (u:User) RETURN toString(u.age) AS text ORDER BY text"
        )

        self.assertEqual(type(field_validated).__name__, "OC_SinglePartQueryContext")
        self.assertEqual(type(no_alias_validated).__name__, "OC_SinglePartQueryContext")
        self.assertEqual(type(literal_validated).__name__, "OC_SinglePartQueryContext")
        self.assertEqual(type(optional_validated).__name__, "OC_SinglePartQueryContext")

    def test_validate_cypher_text_accepts_to_integer_returns(self) -> None:
        field_validated = cypherglot.validate_cypher_text(
            "MATCH (u:User) RETURN toInteger(u.age) AS age_int ORDER BY age_int"
        )
        literal_validated = cypherglot.validate_cypher_text(
            "MATCH (u:User) RETURN toInteger(-3.2) AS age_int ORDER BY age_int"
        )
        optional_validated = cypherglot.validate_cypher_text(
            "OPTIONAL MATCH (u:User) RETURN toInteger(u.age) AS age_int ORDER BY age_int"
        )

        self.assertEqual(type(field_validated).__name__, "OC_SinglePartQueryContext")
        self.assertEqual(type(literal_validated).__name__, "OC_SinglePartQueryContext")
        self.assertEqual(type(optional_validated).__name__, "OC_SinglePartQueryContext")

    def test_validate_cypher_text_accepts_to_float_returns(self) -> None:
        field_validated = cypherglot.validate_cypher_text(
            "MATCH (u:User) RETURN toFloat(u.age) AS age_float ORDER BY age_float"
        )
        literal_validated = cypherglot.validate_cypher_text(
            "MATCH (u:User) RETURN toFloat(-3) AS age_float ORDER BY age_float"
        )
        optional_validated = cypherglot.validate_cypher_text(
            "OPTIONAL MATCH (u:User) RETURN toFloat(u.age) AS age_float ORDER BY age_float"
        )

        self.assertEqual(type(field_validated).__name__, "OC_SinglePartQueryContext")
        self.assertEqual(type(literal_validated).__name__, "OC_SinglePartQueryContext")
        self.assertEqual(type(optional_validated).__name__, "OC_SinglePartQueryContext")

    def test_validate_cypher_text_accepts_to_boolean_returns(self) -> None:
        field_validated = cypherglot.validate_cypher_text(
            "MATCH (u:User) RETURN toBoolean(u.active) AS is_active ORDER BY is_active"
        )
        literal_validated = cypherglot.validate_cypher_text(
            "MATCH (u:User) RETURN toBoolean(true) AS is_active ORDER BY is_active"
        )
        optional_validated = cypherglot.validate_cypher_text(
            "OPTIONAL MATCH (u:User) RETURN toBoolean(u.active) AS is_active ORDER BY is_active"
        )

        self.assertEqual(type(field_validated).__name__, "OC_SinglePartQueryContext")
        self.assertEqual(type(literal_validated).__name__, "OC_SinglePartQueryContext")
        self.assertEqual(type(optional_validated).__name__, "OC_SinglePartQueryContext")

    def test_validate_cypher_text_accepts_substring_returns(self) -> None:
        field_validated = cypherglot.validate_cypher_text(
            "MATCH (u:User) RETURN substring(u.name, 0, 2) AS prefix ORDER BY prefix"
        )
        no_alias_validated = cypherglot.validate_cypher_text(
            "MATCH (u:User) RETURN substring(u.name, 0, 2)"
        )
        field_two_arg_validated = cypherglot.validate_cypher_text(
            "MATCH (u:User) RETURN substring(u.name, 1) AS suffix ORDER BY suffix"
        )
        literal_validated = cypherglot.validate_cypher_text(
            "MATCH (u:User) RETURN substring('Alice', 1, 3) AS prefix ORDER BY prefix"
        )
        literal_two_arg_validated = cypherglot.validate_cypher_text(
            "MATCH (u:User) RETURN substring('Alice', 2) AS suffix ORDER BY suffix"
        )
        optional_validated = cypherglot.validate_cypher_text(
            "OPTIONAL MATCH (u:User) RETURN substring(u.name, 0, 2) AS prefix ORDER BY prefix"
        )
        optional_two_arg_validated = cypherglot.validate_cypher_text(
            "OPTIONAL MATCH (u:User) RETURN substring(u.name, 1) AS suffix ORDER BY suffix"
        )

        self.assertEqual(type(field_validated).__name__, "OC_SinglePartQueryContext")
        self.assertEqual(type(no_alias_validated).__name__, "OC_SinglePartQueryContext")
        self.assertEqual(type(field_two_arg_validated).__name__, "OC_SinglePartQueryContext")
        self.assertEqual(type(literal_validated).__name__, "OC_SinglePartQueryContext")
        self.assertEqual(type(literal_two_arg_validated).__name__, "OC_SinglePartQueryContext")
        self.assertEqual(type(optional_validated).__name__, "OC_SinglePartQueryContext")
        self.assertEqual(type(optional_two_arg_validated).__name__, "OC_SinglePartQueryContext")

    def test_validate_cypher_text_accepts_predicate_returns(self) -> None:
        field_validated = cypherglot.validate_cypher_text(
            "MATCH (u:User) RETURN u.age >= 18 AS adult ORDER BY adult"
        )
        string_validated = cypherglot.validate_cypher_text(
            "MATCH (u:User) RETURN u.name CONTAINS 'a' AS has_a ORDER BY has_a"
        )
        starts_with_validated = cypherglot.validate_cypher_text(
            "MATCH (u:User) RETURN u.name STARTS WITH 'Al' AS has_prefix ORDER BY has_prefix"
        )
        ends_with_validated = cypherglot.validate_cypher_text(
            "MATCH (u:User) RETURN u.name ENDS WITH 'ce' AS has_suffix ORDER BY has_suffix"
        )
        id_validated = cypherglot.validate_cypher_text(
            "MATCH (u:User) RETURN id(u) >= 1 AS has_id ORDER BY has_id"
        )
        type_validated = cypherglot.validate_cypher_text(
            "MATCH (a:User)-[r:KNOWS]->(b:User) RETURN type(r) = 'KNOWS' AS is_knows ORDER BY is_knows"
        )
        size_validated = cypherglot.validate_cypher_text(
            "MATCH (u:User) RETURN size(u.name) >= 3 AS long_name ORDER BY long_name"
        )
        size_id_validated = cypherglot.validate_cypher_text(
            "MATCH (u:User) RETURN size(id(u)) >= 1 AS long_id ORDER BY long_id"
        )
        size_type_validated = cypherglot.validate_cypher_text(
            "MATCH (a:User)-[r:KNOWS]->(b:User) RETURN size(type(r)) >= 5 AS long_type ORDER BY long_type"
        )
        optional_validated = cypherglot.validate_cypher_text(
            "OPTIONAL MATCH (u:User) RETURN u.age >= 18 AS adult ORDER BY adult"
        )
        optional_contains_validated = cypherglot.validate_cypher_text(
            "OPTIONAL MATCH (u:User) RETURN u.name CONTAINS 'a' AS has_a ORDER BY has_a"
        )
        optional_starts_with_validated = cypherglot.validate_cypher_text(
            "OPTIONAL MATCH (u:User) RETURN u.name STARTS WITH 'Al' AS has_prefix ORDER BY has_prefix"
        )
        optional_ends_with_validated = cypherglot.validate_cypher_text(
            "OPTIONAL MATCH (u:User) RETURN u.name ENDS WITH 'ce' AS has_suffix ORDER BY has_suffix"
        )
        optional_id_validated = cypherglot.validate_cypher_text(
            "OPTIONAL MATCH (u:User) RETURN id(u) >= 1 AS has_id ORDER BY has_id"
        )
        optional_size_validated = cypherglot.validate_cypher_text(
            "OPTIONAL MATCH (u:User) RETURN size(u.name) >= 3 AS long_name ORDER BY long_name"
        )

        self.assertEqual(type(field_validated).__name__, "OC_SinglePartQueryContext")
        self.assertEqual(type(string_validated).__name__, "OC_SinglePartQueryContext")
        self.assertEqual(type(starts_with_validated).__name__, "OC_SinglePartQueryContext")
        self.assertEqual(type(ends_with_validated).__name__, "OC_SinglePartQueryContext")
        self.assertEqual(type(id_validated).__name__, "OC_SinglePartQueryContext")
        self.assertEqual(type(type_validated).__name__, "OC_SinglePartQueryContext")
        self.assertEqual(type(size_validated).__name__, "OC_SinglePartQueryContext")
        self.assertEqual(type(size_id_validated).__name__, "OC_SinglePartQueryContext")
        self.assertEqual(type(size_type_validated).__name__, "OC_SinglePartQueryContext")
        self.assertEqual(type(optional_validated).__name__, "OC_SinglePartQueryContext")
        self.assertEqual(type(optional_contains_validated).__name__, "OC_SinglePartQueryContext")
        self.assertEqual(type(optional_starts_with_validated).__name__, "OC_SinglePartQueryContext")
        self.assertEqual(type(optional_ends_with_validated).__name__, "OC_SinglePartQueryContext")
        self.assertEqual(type(optional_id_validated).__name__, "OC_SinglePartQueryContext")
        self.assertEqual(type(optional_size_validated).__name__, "OC_SinglePartQueryContext")

    def test_validate_cypher_text_accepts_relationship_property_predicate_returns(
        self,
    ) -> None:
        validated = cypherglot.validate_cypher_text(
            "MATCH (a:User)-[r:KNOWS]->(b:User) RETURN r.weight >= 1 AS heavy, r.note CONTAINS 'a' AS has_a, r.note STARTS WITH 'Al' AS has_prefix, r.note ENDS WITH 'ce' AS has_suffix, size(r.note) >= 3 AS long_note ORDER BY heavy, has_a, has_prefix, has_suffix, long_note"
        )

        self.assertEqual(type(validated).__name__, "OC_SinglePartQueryContext")

    def test_validate_cypher_text_accepts_where_string_and_null_filters(self) -> None:
        validated = cypherglot.validate_cypher_text(
            "MATCH (u:User) WHERE u.name STARTS WITH 'Al' AND u.name CONTAINS 'li' AND u.name ENDS WITH 'ce' RETURN u.name AS name ORDER BY name"
        )
        relationship_validated = cypherglot.validate_cypher_text(
            "MATCH (a:User)-[r:KNOWS]->(b:User) WHERE r.note IS NULL RETURN r.note AS note ORDER BY note"
        )
        optional_validated = cypherglot.validate_cypher_text(
            "OPTIONAL MATCH (u:User) WHERE u.name STARTS WITH 'Al' RETURN u.name AS name ORDER BY name"
        )

        self.assertEqual(type(validated).__name__, "OC_SinglePartQueryContext")
        self.assertEqual(type(relationship_validated).__name__, "OC_SinglePartQueryContext")
        self.assertEqual(type(optional_validated).__name__, "OC_SinglePartQueryContext")

    def test_validate_cypher_text_accepts_where_size_filters(self) -> None:
        validated = cypherglot.validate_cypher_text(
            "MATCH (u:User) WHERE size(u.name) >= 3 RETURN u.name AS name ORDER BY name"
        )
        relationship_validated = cypherglot.validate_cypher_text(
            "MATCH (a:User)-[r:KNOWS]->(b:User) WHERE size(r.note) IS NOT NULL RETURN r.note AS note ORDER BY note"
        )
        optional_validated = cypherglot.validate_cypher_text(
            "OPTIONAL MATCH (u:User) WHERE size(u.name) >= 3 RETURN u.name AS name ORDER BY name"
        )

        self.assertEqual(type(validated).__name__, "OC_SinglePartQueryContext")
        self.assertEqual(type(relationship_validated).__name__, "OC_SinglePartQueryContext")
        self.assertEqual(type(optional_validated).__name__, "OC_SinglePartQueryContext")

    def test_validate_cypher_text_accepts_null_predicate_returns(self) -> None:
        validated = cypherglot.validate_cypher_text(
            "MATCH (u:User) RETURN u.name IS NULL AS missing_name, size(u.name) IS NOT NULL AS has_len ORDER BY missing_name, has_len"
        )
        relationship_validated = cypherglot.validate_cypher_text(
            "MATCH (a:User)-[r:KNOWS]->(b:User) RETURN r.note IS NULL AS missing_note, size(r.note) IS NOT NULL AS has_len ORDER BY missing_note, has_len"
        )
        optional_validated = cypherglot.validate_cypher_text(
            "OPTIONAL MATCH (u:User) RETURN u.name IS NULL AS missing_name ORDER BY missing_name"
        )

        self.assertEqual(type(validated).__name__, "OC_SinglePartQueryContext")
        self.assertEqual(type(relationship_validated).__name__, "OC_SinglePartQueryContext")
        self.assertEqual(type(optional_validated).__name__, "OC_SinglePartQueryContext")

    def test_validate_cypher_text_accepts_id_and_type_returns(self) -> None:
        id_validated = cypherglot.validate_cypher_text(
            "MATCH (u:User) RETURN id(u) AS uid ORDER BY uid"
        )
        type_validated = cypherglot.validate_cypher_text(
            "MATCH (a:User)-[r:KNOWS]->(b:User) RETURN type(r) AS rel_type ORDER BY rel_type"
        )
        optional_validated = cypherglot.validate_cypher_text(
            "OPTIONAL MATCH (u:User) RETURN id(u) AS uid ORDER BY uid"
        )

        self.assertEqual(type(id_validated).__name__, "OC_SinglePartQueryContext")
        self.assertEqual(type(type_validated).__name__, "OC_SinglePartQueryContext")
        self.assertEqual(type(optional_validated).__name__, "OC_SinglePartQueryContext")

    def test_validate_cypher_text_accepts_narrow_with_subset(self) -> None:
        validated = cypherglot.validate_cypher_text(
            "MATCH (u:User) WITH u AS person RETURN person.name ORDER BY person.name"
        )

        self.assertEqual(type(validated).__name__, "OC_MultiPartQueryContext")

    def test_validate_cypher_text_accepts_scalar_with_rebinding_subset(self) -> None:
        validated = cypherglot.validate_cypher_text(
            "MATCH (u:User) WITH u.name AS name RETURN name ORDER BY name"
        )

        self.assertEqual(type(validated).__name__, "OC_MultiPartQueryContext")

    def test_validate_cypher_text_accepts_with_entity_passthrough_return(self) -> None:
        validated = cypherglot.validate_cypher_text(
            "MATCH (u:User) WITH u AS person, u.name AS name RETURN person, name ORDER BY name"
        )
        ordered_validated = cypherglot.validate_cypher_text(
            "MATCH (u:User) WITH u AS person RETURN person AS user ORDER BY user"
        )
        relationship_ordered_validated = cypherglot.validate_cypher_text(
            "MATCH (a:User)-[r:KNOWS]->(b:User) WITH r AS rel RETURN rel AS edge ORDER BY edge"
        )

        self.assertEqual(type(validated).__name__, "OC_MultiPartQueryContext")
        self.assertEqual(type(ordered_validated).__name__, "OC_MultiPartQueryContext")
        self.assertEqual(type(relationship_ordered_validated).__name__, "OC_MultiPartQueryContext")

    def test_validate_cypher_text_accepts_grouped_count_with_subset(self) -> None:
        validated = cypherglot.validate_cypher_text(
            "MATCH (u:User) WITH u AS person, u.name AS name RETURN name, count(person) AS total ORDER BY total DESC"
        )
        star_validated = cypherglot.validate_cypher_text(
            "MATCH (u:User) WITH u.name AS name RETURN name, count(*) AS total ORDER BY total DESC"
        )
        relationship_validated = cypherglot.validate_cypher_text(
            "MATCH (a:User)-[r:KNOWS]->(b:User) WITH r AS rel RETURN rel AS edge, count(rel) AS total ORDER BY total DESC"
        )
        relationship_star_validated = cypherglot.validate_cypher_text(
            "MATCH (a:User)-[r:KNOWS]->(b:User) WITH r AS rel RETURN rel AS edge, count(*) AS total ORDER BY total DESC"
        )

        self.assertEqual(type(validated).__name__, "OC_MultiPartQueryContext")
        self.assertEqual(type(star_validated).__name__, "OC_MultiPartQueryContext")
        self.assertEqual(type(relationship_validated).__name__, "OC_MultiPartQueryContext")
        self.assertEqual(type(relationship_star_validated).__name__, "OC_MultiPartQueryContext")

    def test_validate_cypher_text_accepts_grouped_numeric_aggregates_with_subset(
        self,
    ) -> None:
        validated = cypherglot.validate_cypher_text(
            "MATCH (u:User) WITH u.name AS name, u.score AS score RETURN name, sum(score) AS total ORDER BY total DESC"
        )
        averaged_validated = cypherglot.validate_cypher_text(
            "MATCH (u:User) WITH u.score AS score RETURN avg(score) AS mean"
        )

        self.assertEqual(type(validated).__name__, "OC_MultiPartQueryContext")
        self.assertEqual(type(averaged_validated).__name__, "OC_MultiPartQueryContext")

    def test_validate_cypher_text_accepts_searched_case_with_subset(self) -> None:
        validated = cypherglot.validate_cypher_text(
            "MATCH (u:User) WITH u.age AS age, u.name AS name RETURN CASE WHEN age >= 18 THEN name ELSE 'minor' END AS label ORDER BY label"
        )

        self.assertEqual(type(validated).__name__, "OC_MultiPartQueryContext")

    def test_validate_cypher_text_accepts_properties_with_subset(self) -> None:
        validated = cypherglot.validate_cypher_text(
            "MATCH (a:User)-[r:KNOWS]->(b:User) WITH a AS person, r AS rel RETURN properties(person) AS user_props, properties(rel) AS rel_props ORDER BY user_props, rel_props"
        )

        self.assertEqual(type(validated).__name__, "OC_MultiPartQueryContext")

    def test_validate_cypher_text_accepts_labels_with_subset(self) -> None:
        validated = cypherglot.validate_cypher_text(
            "MATCH (a:User)-[r:KNOWS]->(b:User) WITH a AS person RETURN labels(person) AS user_labels ORDER BY user_labels"
        )

        self.assertEqual(type(validated).__name__, "OC_MultiPartQueryContext")

    def test_validate_cypher_text_accepts_keys_with_subset(self) -> None:
        validated = cypherglot.validate_cypher_text(
            "MATCH (a:User)-[r:KNOWS]->(b:User) WITH a AS person, r AS rel RETURN keys(person) AS user_keys, keys(rel) AS rel_keys ORDER BY user_keys, rel_keys"
        )

        self.assertEqual(type(validated).__name__, "OC_MultiPartQueryContext")

    def test_validate_cypher_text_accepts_start_and_end_node_with_subset(self) -> None:
        validated = cypherglot.validate_cypher_text(
            "MATCH (a:User)-[r:KNOWS]->(b:User) WITH r AS rel RETURN startNode(rel) AS start, endNode(rel) AS ending ORDER BY start, ending"
        )
        field_validated = cypherglot.validate_cypher_text(
            "MATCH (a:User)-[r:KNOWS]->(b:User) WITH r AS rel RETURN startNode(rel).id AS start_id, endNode(rel).id AS end_id ORDER BY start_id, end_id"
        )
        no_alias_validated = cypherglot.validate_cypher_text(
            "MATCH (a:User)-[r:KNOWS]->(b:User) WITH a AS person, r AS rel RETURN id(person), type(rel), startNode(rel).id"
        )

        self.assertEqual(type(validated).__name__, "OC_MultiPartQueryContext")
        self.assertEqual(type(field_validated).__name__, "OC_MultiPartQueryContext")
        self.assertEqual(type(no_alias_validated).__name__, "OC_MultiPartQueryContext")

    def test_validate_cypher_text_accepts_with_where_subset(self) -> None:
        validated = cypherglot.validate_cypher_text(
            "MATCH (u:User) WITH u AS person, u.name AS name WHERE name = 'Alice' AND person.id > 1 RETURN person, name ORDER BY name"
        )

        self.assertEqual(type(validated).__name__, "OC_MultiPartQueryContext")

    def test_validate_cypher_text_accepts_with_where_id_and_type_subset(self) -> None:
        id_validated = cypherglot.validate_cypher_text(
            "MATCH (u:User) WITH u AS person, u.name AS name WHERE id(person) >= 1 RETURN person, name ORDER BY name"
        )
        type_validated = cypherglot.validate_cypher_text(
            "MATCH (a:User)-[r:KNOWS]->(b:User) WITH a AS person, r AS rel WHERE type(rel) = 'KNOWS' RETURN person, rel"
        )

        self.assertEqual(type(id_validated).__name__, "OC_MultiPartQueryContext")
        self.assertEqual(type(type_validated).__name__, "OC_MultiPartQueryContext")

    def test_validate_cypher_text_accepts_aliased_with_return_subset(self) -> None:
        validated = cypherglot.validate_cypher_text(
            "MATCH (u:User) WITH u AS person, u.name AS name RETURN person AS user, person.name AS display_name, name AS raw_name ORDER BY display_name, raw_name"
        )

        self.assertEqual(type(validated).__name__, "OC_MultiPartQueryContext")

    def test_validate_cypher_text_accepts_with_id_and_type_returns(self) -> None:
        validated = cypherglot.validate_cypher_text(
            "MATCH (a:User)-[r:KNOWS]->(b:User) WITH a AS user, r AS rel RETURN id(user) AS uid, type(rel) AS rel_type ORDER BY uid, rel_type"
        )

        self.assertEqual(type(validated).__name__, "OC_MultiPartQueryContext")

    def test_validate_cypher_text_accepts_with_size_returns(self) -> None:
        validated = cypherglot.validate_cypher_text(
            "MATCH (a:User)-[r:KNOWS]->(b:User) WITH a AS person, r AS rel, a.name AS name RETURN size(person.name) AS name_len, size(name) AS rebound_len, size(id(person)) AS person_id_len, size(type(rel)) AS rel_type_len ORDER BY name_len, rebound_len, person_id_len, rel_type_len"
        )
        no_alias_validated = cypherglot.validate_cypher_text(
            "MATCH (a:User)-[r:KNOWS]->(b:User) WITH a AS person, r AS rel, a.name AS name RETURN size(person.name), size(name), size(id(person)), size(type(rel))"
        )

        self.assertEqual(type(validated).__name__, "OC_MultiPartQueryContext")
        self.assertEqual(type(no_alias_validated).__name__, "OC_MultiPartQueryContext")

    def test_validate_cypher_text_accepts_with_lower_and_upper_returns(self) -> None:
        validated = cypherglot.validate_cypher_text(
            "MATCH (u:User) WITH u AS person, u.name AS name RETURN lower(person.name) AS lower_name, upper(name) AS upper_name, lower('tag') AS lower_tag, upper($value) AS upper_value, trim(name) AS trimmed, ltrim(' tag') AS left_trimmed, rtrim('tag ') AS right_trimmed ORDER BY lower_name, upper_name, lower_tag, upper_value, trimmed, left_trimmed, right_trimmed"
        )
        reverse_validated = cypherglot.validate_cypher_text(
            "MATCH (u:User) WITH u AS person, u.name AS name RETURN reverse(person.name) AS reversed_name, reverse(name) AS rebound_reverse, reverse('tag') AS lit_reverse ORDER BY reversed_name, rebound_reverse, lit_reverse"
        )
        no_alias_validated = cypherglot.validate_cypher_text(
            "MATCH (u:User) WITH u AS person, u.name AS name RETURN lower(person.name), upper(name), reverse(person.name)"
        )

        self.assertEqual(type(validated).__name__, "OC_MultiPartQueryContext")
        self.assertEqual(type(reverse_validated).__name__, "OC_MultiPartQueryContext")
        self.assertEqual(type(no_alias_validated).__name__, "OC_MultiPartQueryContext")

    def test_validate_cypher_text_accepts_with_coalesce_returns(self) -> None:
        validated = cypherglot.validate_cypher_text(
            "MATCH (u:User) WITH u AS person, u.name AS name RETURN coalesce(person.name, 'unknown') AS display_name, coalesce(name, $fallback) AS rebound_name ORDER BY display_name, rebound_name"
        )
        no_alias_validated = cypherglot.validate_cypher_text(
            "MATCH (u:User) WITH u AS person, u.name AS name RETURN coalesce(person.name, 'unknown'), coalesce(name, $fallback)"
        )

        self.assertEqual(type(validated).__name__, "OC_MultiPartQueryContext")
        self.assertEqual(type(no_alias_validated).__name__, "OC_MultiPartQueryContext")

    def test_validate_cypher_text_accepts_with_replace_returns(self) -> None:
        validated = cypherglot.validate_cypher_text(
            "MATCH (u:User) WITH u AS person, u.name AS name RETURN replace(person.name, 'A', 'B') AS display_name, replace(name, $needle, $replacement) AS rebound_name, replace('Alice', 'l', 'x') AS lit_name ORDER BY display_name, rebound_name, lit_name"
        )
        no_alias_validated = cypherglot.validate_cypher_text(
            "MATCH (u:User) WITH u AS person, u.name AS name RETURN replace(person.name, 'A', 'B'), replace(name, $needle, $replacement)"
        )

        self.assertEqual(type(validated).__name__, "OC_MultiPartQueryContext")
        self.assertEqual(type(no_alias_validated).__name__, "OC_MultiPartQueryContext")

    def test_validate_cypher_text_accepts_with_left_and_right_returns(self) -> None:
        validated = cypherglot.validate_cypher_text(
            "MATCH (u:User) WITH u AS person, u.name AS name RETURN left(person.name, 2) AS prefix, right(name, $count) AS rebound_suffix, left('Alice', 3) AS lit_prefix ORDER BY prefix, rebound_suffix, lit_prefix"
        )
        no_alias_validated = cypherglot.validate_cypher_text(
            "MATCH (u:User) WITH u AS person, u.name AS name RETURN left(person.name, 2), right(name, $count)"
        )

        self.assertEqual(type(validated).__name__, "OC_MultiPartQueryContext")
        self.assertEqual(type(no_alias_validated).__name__, "OC_MultiPartQueryContext")

    def test_validate_cypher_text_accepts_with_split_returns(self) -> None:
        validated = cypherglot.validate_cypher_text(
            "MATCH (u:User) WITH u AS person, u.name AS name RETURN split(person.name, ' ') AS parts, split(name, $delimiter) AS rebound_parts, split('Alice Bob', ' ') AS lit_parts ORDER BY parts, rebound_parts, lit_parts"
        )
        no_alias_validated = cypherglot.validate_cypher_text(
            "MATCH (u:User) WITH u AS person, u.name AS name RETURN split(person.name, ' '), split(name, $delimiter)"
        )

        self.assertEqual(type(validated).__name__, "OC_MultiPartQueryContext")
        self.assertEqual(type(no_alias_validated).__name__, "OC_MultiPartQueryContext")

    def test_validate_cypher_text_accepts_with_abs_returns(self) -> None:
        validated = cypherglot.validate_cypher_text(
            "MATCH (u:User) WITH u AS person, u.age AS age RETURN abs(person.age) AS magnitude, abs(age) AS rebound, abs(-3) AS lit ORDER BY magnitude, rebound, lit"
        )
        no_alias_validated = cypherglot.validate_cypher_text(
            "MATCH (u:User) WITH u AS person, u.age AS age RETURN abs(person.age), abs(age), sign(age)"
        )

        self.assertEqual(type(validated).__name__, "OC_MultiPartQueryContext")
        self.assertEqual(type(no_alias_validated).__name__, "OC_MultiPartQueryContext")

    def test_validate_cypher_text_accepts_with_sign_returns(self) -> None:
        validated = cypherglot.validate_cypher_text(
            "MATCH (u:User) WITH u AS person, u.age AS age RETURN sign(person.age) AS age_sign, sign(age) AS rebound_sign, sign(-3.2) AS lit_sign ORDER BY age_sign, rebound_sign, lit_sign"
        )

        self.assertEqual(type(validated).__name__, "OC_MultiPartQueryContext")

    def test_validate_cypher_text_accepts_with_round_returns(self) -> None:
        validated = cypherglot.validate_cypher_text(
            "MATCH (u:User) WITH u AS person, u.score AS score RETURN round(person.score) AS value, round(score) AS rebound, round(-3.2) AS lit ORDER BY value, rebound, lit"
        )

        self.assertEqual(type(validated).__name__, "OC_MultiPartQueryContext")

    def test_validate_cypher_text_accepts_with_ceil_returns(self) -> None:
        validated = cypherglot.validate_cypher_text(
            "MATCH (u:User) WITH u AS person, u.score AS score RETURN ceil(person.score) AS value, ceil(score) AS rebound, ceil(-3.2) AS lit ORDER BY value, rebound, lit"
        )

        self.assertEqual(type(validated).__name__, "OC_MultiPartQueryContext")

    def test_validate_cypher_text_accepts_with_floor_returns(self) -> None:
        validated = cypherglot.validate_cypher_text(
            "MATCH (u:User) WITH u AS person, u.score AS score RETURN floor(person.score) AS value, floor(score) AS rebound, floor(-3.2) AS lit ORDER BY value, rebound, lit"
        )

        self.assertEqual(type(validated).__name__, "OC_MultiPartQueryContext")

    def test_validate_cypher_text_accepts_with_sqrt_returns(self) -> None:
        validated = cypherglot.validate_cypher_text(
            "MATCH (u:User) WITH u AS person, u.score AS score RETURN sqrt(person.score) AS value, sqrt(score) AS rebound, sqrt(-3.2) AS lit ORDER BY value, rebound, lit"
        )

        self.assertEqual(type(validated).__name__, "OC_MultiPartQueryContext")

    def test_validate_cypher_text_accepts_with_exp_returns(self) -> None:
        validated = cypherglot.validate_cypher_text(
            "MATCH (u:User) WITH u AS person, u.score AS score RETURN exp(person.score) AS value, exp(score) AS rebound, exp(-3.2) AS lit ORDER BY value, rebound, lit"
        )

        self.assertEqual(type(validated).__name__, "OC_MultiPartQueryContext")

    def test_validate_cypher_text_accepts_with_sin_returns(self) -> None:
        validated = cypherglot.validate_cypher_text(
            "MATCH (u:User) WITH u AS person, u.score AS score RETURN sin(person.score) AS value, sin(score) AS rebound, sin(-3.2) AS lit ORDER BY value, rebound, lit"
        )

        self.assertEqual(type(validated).__name__, "OC_MultiPartQueryContext")

    def test_validate_cypher_text_accepts_with_cos_returns(self) -> None:
        validated = cypherglot.validate_cypher_text(
            "MATCH (u:User) WITH u AS person, u.score AS score RETURN cos(person.score) AS value, cos(score) AS rebound, cos(-3.2) AS lit ORDER BY value, rebound, lit"
        )

        self.assertEqual(type(validated).__name__, "OC_MultiPartQueryContext")

    def test_validate_cypher_text_accepts_with_tan_returns(self) -> None:
        validated = cypherglot.validate_cypher_text(
            "MATCH (u:User) WITH u AS person, u.score AS score RETURN tan(person.score) AS value, tan(score) AS rebound, tan(-3.2) AS lit ORDER BY value, rebound, lit"
        )

        self.assertEqual(type(validated).__name__, "OC_MultiPartQueryContext")

    def test_validate_cypher_text_accepts_with_asin_returns(self) -> None:
        validated = cypherglot.validate_cypher_text(
            "MATCH (u:User) WITH u AS person, u.score AS score RETURN asin(person.score) AS value, asin(score) AS rebound, asin(-0.5) AS lit ORDER BY value, rebound, lit"
        )

        self.assertEqual(type(validated).__name__, "OC_MultiPartQueryContext")

    def test_validate_cypher_text_accepts_with_acos_returns(self) -> None:
        validated = cypherglot.validate_cypher_text(
            "MATCH (u:User) WITH u AS person, u.score AS score RETURN acos(person.score) AS value, acos(score) AS rebound, acos(-0.5) AS lit ORDER BY value, rebound, lit"
        )

        self.assertEqual(type(validated).__name__, "OC_MultiPartQueryContext")

    def test_validate_cypher_text_accepts_with_atan_returns(self) -> None:
        validated = cypherglot.validate_cypher_text(
            "MATCH (u:User) WITH u AS person, u.score AS score RETURN atan(person.score) AS value, atan(score) AS rebound, atan(-0.5) AS lit ORDER BY value, rebound, lit"
        )

        self.assertEqual(type(validated).__name__, "OC_MultiPartQueryContext")

    def test_validate_cypher_text_accepts_with_ln_returns(self) -> None:
        validated = cypherglot.validate_cypher_text(
            "MATCH (u:User) WITH u AS person, u.score AS score RETURN ln(person.score) AS value, ln(score) AS rebound, ln(0.5) AS lit ORDER BY value, rebound, lit"
        )

        self.assertEqual(type(validated).__name__, "OC_MultiPartQueryContext")

    def test_validate_cypher_text_accepts_with_log_returns(self) -> None:
        validated = cypherglot.validate_cypher_text(
            "MATCH (u:User) WITH u AS person, u.score AS score RETURN log(person.score) AS value, log(score) AS rebound, log(0.5) AS lit ORDER BY value, rebound, lit"
        )

        self.assertEqual(type(validated).__name__, "OC_MultiPartQueryContext")

    def test_validate_cypher_text_accepts_with_radians_returns(self) -> None:
        validated = cypherglot.validate_cypher_text(
            "MATCH (u:User) WITH u AS person, u.score AS score RETURN radians(person.score) AS value, radians(score) AS rebound, radians(180) AS lit ORDER BY value, rebound, lit"
        )

        self.assertEqual(type(validated).__name__, "OC_MultiPartQueryContext")

    def test_validate_cypher_text_accepts_with_degrees_returns(self) -> None:
        validated = cypherglot.validate_cypher_text(
            "MATCH (u:User) WITH u AS person, u.score AS score RETURN degrees(person.score) AS value, degrees(score) AS rebound, degrees(3.14159) AS lit ORDER BY value, rebound, lit"
        )

        self.assertEqual(type(validated).__name__, "OC_MultiPartQueryContext")

    def test_validate_cypher_text_accepts_with_log10_returns(self) -> None:
        validated = cypherglot.validate_cypher_text(
            "MATCH (u:User) WITH u AS person, u.score AS score RETURN log10(person.score) AS value, log10(score) AS rebound, log10(0.5) AS lit ORDER BY value, rebound, lit"
        )

        self.assertEqual(type(validated).__name__, "OC_MultiPartQueryContext")

    def test_validate_cypher_text_accepts_with_to_string_returns(self) -> None:
        validated = cypherglot.validate_cypher_text(
            "MATCH (u:User) WITH u AS person, u.age AS age RETURN toString(person.age) AS text, toString(age) AS rebound, toString(-3) AS lit ORDER BY text, rebound, lit"
        )
        no_alias_validated = cypherglot.validate_cypher_text(
            "MATCH (u:User) WITH u AS person, u.age AS age, u.active AS active RETURN toString(person.age), toString(age), toBoolean(active)"
        )

        self.assertEqual(type(validated).__name__, "OC_MultiPartQueryContext")
        self.assertEqual(type(no_alias_validated).__name__, "OC_MultiPartQueryContext")

    def test_validate_cypher_text_accepts_with_to_integer_returns(self) -> None:
        validated = cypherglot.validate_cypher_text(
            "MATCH (u:User) WITH u AS person, u.age AS age RETURN toInteger(person.age) AS age_int, toInteger(age) AS rebound, toInteger(-3.2) AS lit ORDER BY age_int, rebound, lit"
        )

        self.assertEqual(type(validated).__name__, "OC_MultiPartQueryContext")

    def test_validate_cypher_text_accepts_with_to_float_returns(self) -> None:
        validated = cypherglot.validate_cypher_text(
            "MATCH (u:User) WITH u AS person, u.age AS age RETURN toFloat(person.age) AS age_float, toFloat(age) AS rebound, toFloat(-3) AS lit ORDER BY age_float, rebound, lit"
        )

        self.assertEqual(type(validated).__name__, "OC_MultiPartQueryContext")

    def test_validate_cypher_text_accepts_with_to_boolean_returns(self) -> None:
        validated = cypherglot.validate_cypher_text(
            "MATCH (u:User) WITH u AS person, u.active AS active RETURN toBoolean(person.active) AS is_active, toBoolean(active) AS rebound, toBoolean(true) AS lit ORDER BY is_active, rebound, lit"
        )

        self.assertEqual(type(validated).__name__, "OC_MultiPartQueryContext")

    def test_validate_cypher_text_accepts_with_substring_returns(self) -> None:
        validated = cypherglot.validate_cypher_text(
            "MATCH (u:User) WITH u AS person, u.name AS name RETURN substring(person.name, 0, 2) AS prefix, substring(name, 1, 3) AS rebound, substring('Alice', 1, 3) AS lit ORDER BY prefix, rebound, lit"
        )
        no_alias_validated = cypherglot.validate_cypher_text(
            "MATCH (u:User) WITH u AS person, u.name AS name RETURN substring(person.name, 0, 2), substring(name, 1, 3)"
        )
        two_arg_validated = cypherglot.validate_cypher_text(
            "MATCH (u:User) WITH u AS person, u.name AS name RETURN substring(person.name, 1) AS suffix, substring(name, 2) AS rebound_suffix, substring('Alice', 3) AS lit_suffix ORDER BY suffix, rebound_suffix, lit_suffix"
        )

        self.assertEqual(type(validated).__name__, "OC_MultiPartQueryContext")
        self.assertEqual(type(no_alias_validated).__name__, "OC_MultiPartQueryContext")
        self.assertEqual(type(two_arg_validated).__name__, "OC_MultiPartQueryContext")

    def test_validate_cypher_text_accepts_with_relationship_property_string_returns(self) -> None:
        validated = cypherglot.validate_cypher_text(
            "MATCH (a:User)-[r:KNOWS]->(b:User) WITH r AS rel, r.note AS note RETURN lower(rel.note) AS lower_note, upper(note) AS upper_note, trim(rel.note) AS trimmed_note, ltrim(note) AS left_trimmed_note, rtrim(rel.note) AS right_trimmed_note, reverse(note) AS reversed_note, coalesce(rel.note, 'unknown') AS display_note, replace(note, 'A', 'B') AS replaced_note, left(rel.note, 2) AS prefix, right(note, 2) AS suffix, split(rel.note, ' ') AS parts, substring(note, 1) AS tail ORDER BY lower_note, upper_note, trimmed_note, left_trimmed_note, right_trimmed_note, reversed_note, display_note, replaced_note, prefix, suffix, parts, tail"
        )

        self.assertEqual(type(validated).__name__, "OC_MultiPartQueryContext")

    def test_validate_cypher_text_accepts_with_relationship_property_numeric_and_conversion_returns(
        self,
    ) -> None:
        validated = cypherglot.validate_cypher_text(
            "MATCH (a:User)-[r:KNOWS]->(b:User) WITH r AS rel, r.weight AS weight, r.score AS score, r.active AS active RETURN abs(rel.weight) AS magnitude, sign(weight) AS weight_sign, round(rel.score) AS rounded_score, ceil(score) AS ceil_score, floor(rel.score) AS floor_score, sqrt(rel.score) AS sqrt_score, exp(score) AS exp_score, sin(rel.score) AS sin_score, toString(weight) AS weight_text, toInteger(rel.score) AS score_int, toFloat(weight) AS weight_float, toBoolean(active) AS active_bool ORDER BY magnitude, weight_sign, rounded_score, ceil_score, floor_score, sqrt_score, exp_score, sin_score, weight_text, score_int, weight_float, active_bool"
        )

        self.assertEqual(type(validated).__name__, "OC_MultiPartQueryContext")

    def test_validate_cypher_text_accepts_with_size_literal_and_parameter_returns(self) -> None:
        validated = cypherglot.validate_cypher_text(
            "MATCH (u:User) WITH u AS person RETURN size('tag') AS tag_len, size($value) AS value_len ORDER BY tag_len, value_len"
        )

        self.assertEqual(type(validated).__name__, "OC_MultiPartQueryContext")

    def test_validate_cypher_text_accepts_with_scalar_literal_and_parameter_returns(self) -> None:
        validated = cypherglot.validate_cypher_text(
            "MATCH (u:User) WITH u AS person RETURN 'tag' AS tag, $value AS value ORDER BY tag, value"
        )

        self.assertEqual(type(validated).__name__, "OC_MultiPartQueryContext")

    def test_validate_cypher_text_accepts_with_predicate_returns(self) -> None:
        validated = cypherglot.validate_cypher_text(
            "MATCH (a:User)-[r:KNOWS]->(b:User) WITH a AS person, r AS rel, a.name AS name RETURN person.age >= 18 AS adult, name = 'Alice' AS is_alice, person.name CONTAINS 'a' AS has_a, name CONTAINS 'i' AS rebound_has_i, person.name STARTS WITH 'Al' AS has_prefix, name ENDS WITH 'ce' AS has_suffix, id(person) >= 1 AS has_id, type(rel) = 'KNOWS' AS rel_matches, size(person.name) >= 3 AS long_name, size(name) >= 3 AS rebound_long, size(id(person)) >= 1 AS long_id, size(type(rel)) >= 5 AS long_type ORDER BY adult, is_alice, has_a, rebound_has_i, has_prefix, has_suffix, has_id, rel_matches, long_name, rebound_long, long_id, long_type"
        )

        self.assertEqual(type(validated).__name__, "OC_MultiPartQueryContext")

    def test_validate_cypher_text_accepts_with_relationship_property_predicate_returns(
        self,
    ) -> None:
        validated = cypherglot.validate_cypher_text(
            "MATCH (a:User)-[r:KNOWS]->(b:User) WITH r AS rel, r.note AS note, r.weight AS weight RETURN rel.weight >= 1 AS heavy, note CONTAINS 'a' AS has_a, rel.note STARTS WITH 'Al' AS has_prefix, note ENDS WITH 'ce' AS has_suffix, size(rel.note) >= 3 AS long_note, size(note) >= 3 AS rebound_long ORDER BY heavy, has_a, has_prefix, has_suffix, long_note, rebound_long"
        )

        self.assertEqual(type(validated).__name__, "OC_MultiPartQueryContext")

    def test_validate_cypher_text_accepts_with_null_predicate_returns(self) -> None:
        validated = cypherglot.validate_cypher_text(
            "MATCH (a:User)-[r:KNOWS]->(b:User) WITH a AS person, r AS rel, a.name AS name, r.note AS note RETURN person.name IS NULL AS missing_name, name IS NOT NULL AS rebound_present, rel.note IS NULL AS missing_note, note IS NOT NULL AS rebound_note, size(name) IS NULL AS missing_len, size(rel.note) IS NOT NULL AS note_len ORDER BY missing_name, rebound_present, missing_note, rebound_note, missing_len, note_len"
        )

        self.assertEqual(type(validated).__name__, "OC_MultiPartQueryContext")

    def test_validate_cypher_text_accepts_with_where_null_subset(self) -> None:
        validated = cypherglot.validate_cypher_text(
            "MATCH (a:User)-[r:KNOWS]->(b:User) WITH a AS person, r AS rel, a.name AS name WHERE name IS NOT NULL AND person.name IS NULL AND rel.note IS NOT NULL RETURN person, rel"
        )

        self.assertEqual(type(validated).__name__, "OC_MultiPartQueryContext")

    def test_validate_cypher_text_accepts_with_where_string_and_null_filters(self) -> None:
        validated = cypherglot.validate_cypher_text(
            "MATCH (a:User)-[r:KNOWS]->(b:User) WITH a AS person, r AS rel, a.name AS name, r.note AS note WHERE person.name STARTS WITH 'Al' AND name CONTAINS 'li' AND rel.note ENDS WITH 'ce' AND note IS NOT NULL RETURN person, rel"
        )

        self.assertEqual(type(validated).__name__, "OC_MultiPartQueryContext")

    def test_validate_cypher_text_accepts_with_where_size_filters(self) -> None:
        validated = cypherglot.validate_cypher_text(
            "MATCH (a:User)-[r:KNOWS]->(b:User) WITH a AS person, r AS rel, a.name AS name, r.note AS note WHERE size(person.name) >= 3 AND size(name) >= 3 AND size(rel.note) IS NOT NULL AND size(note) IS NOT NULL RETURN person, rel"
        )

        self.assertEqual(type(validated).__name__, "OC_MultiPartQueryContext")

    def test_validate_cypher_text_accepts_narrow_unwind_subset(self) -> None:
        validated = cypherglot.validate_cypher_text(
            "UNWIND [1, 2, 3] AS x RETURN x AS value ORDER BY value DESC LIMIT 2"
        )

        self.assertEqual(type(validated).__name__, "OC_SinglePartQueryContext")

    def test_validate_cypher_text_accepts_narrow_optional_match_subset(self) -> None:
        validated = cypherglot.validate_cypher_text(
            "OPTIONAL MATCH (u:User) WHERE u.name = 'Alice' RETURN u.name ORDER BY u.name LIMIT 1"
        )

        self.assertEqual(type(validated).__name__, "OC_SinglePartQueryContext")

    def test_validate_cypher_text_accepts_optional_match_count_subset(self) -> None:
        validated = cypherglot.validate_cypher_text(
            "OPTIONAL MATCH (u:User) RETURN u.name AS name, count(u) AS total ORDER BY total DESC"
        )
        star_validated = cypherglot.validate_cypher_text(
            "OPTIONAL MATCH (u:User) RETURN u.name AS name, count(*) AS total ORDER BY total DESC"
        )

        self.assertEqual(type(validated).__name__, "OC_SinglePartQueryContext")
        self.assertEqual(type(star_validated).__name__, "OC_SinglePartQueryContext")

    def test_validate_cypher_text_accepts_optional_match_grouped_entity_count_subset(
        self,
    ) -> None:
        validated = cypherglot.validate_cypher_text(
            "OPTIONAL MATCH (u:User) RETURN u AS user, count(u) AS total ORDER BY total DESC"
        )
        ordered_validated = cypherglot.validate_cypher_text(
            "OPTIONAL MATCH (u:User) RETURN u AS user ORDER BY user"
        )

        self.assertEqual(type(validated).__name__, "OC_SinglePartQueryContext")
        self.assertEqual(type(ordered_validated).__name__, "OC_SinglePartQueryContext")

    def test_validate_cypher_text_accepts_optional_match_scalar_literal_and_parameter_returns(
        self,
    ) -> None:
        literal_validated = cypherglot.validate_cypher_text(
            "OPTIONAL MATCH (u:User) RETURN 'tag' AS tag ORDER BY tag"
        )
        parameter_validated = cypherglot.validate_cypher_text(
            "OPTIONAL MATCH (u:User) RETURN $value AS value ORDER BY value"
        )

        self.assertEqual(type(literal_validated).__name__, "OC_SinglePartQueryContext")
        self.assertEqual(type(parameter_validated).__name__, "OC_SinglePartQueryContext")

    def test_validate_cypher_text_rejects_syntax_errors(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            "CypherGlot frontend reported syntax errors",
        ):
            cypherglot.validate_cypher_text("MATCH (u RETURN u")

    def test_validate_cypher_text_accepts_narrow_merge(self) -> None:
        merge_node = cypherglot.validate_cypher_text("MERGE (u:User {name: 'Alice'})")
        match_merge = cypherglot.validate_cypher_text(
            "MATCH (a:User), (b:User) MERGE (a)-[:KNOWS]->(b)"
        )
        traversal_create = cypherglot.validate_cypher_text(
            "MATCH (a:User)-[:KNOWS]->(b:User) CREATE (a)-[:INTRODUCED]->(b)"
        )
        traversal_create_with_new = cypherglot.validate_cypher_text(
            "MATCH (a:User)-[:KNOWS]->(b:User) CREATE (a)-[:INTRODUCED]->(:Person {name: 'Cara'})"
        )
        traversal_create_with_unlabeled_new = cypherglot.validate_cypher_text(
            "MATCH (a:User)-[:KNOWS]->(b:User) CREATE (a)-[:INTRODUCED]->({name: 'Cara'})"
        )
        traversal_merge_with_new = cypherglot.validate_cypher_text(
            "MATCH (a:User)-[:KNOWS]->(b:User) MERGE (a)-[:INTRODUCED]->(:Person {name: 'Cara'})"
        )
        traversal_merge_with_unlabeled_new = cypherglot.validate_cypher_text(
            "MATCH (a:User)-[:KNOWS]->(b:User) MERGE (a)-[:INTRODUCED]->({name: 'Cara'})"
        )
        traversal_merge = cypherglot.validate_cypher_text(
            "MATCH (a:User)-[:KNOWS]->(b:User)-[:WORKS_AT]->(c:Company) MERGE (a)-[:INTRODUCED]->(c)"
        )
        variable_length_zero_hop = cypherglot.validate_cypher_text(
            "MATCH (a:User)-[:KNOWS*0..2]->(b:User) RETURN b.name"
        )
        variable_length_count = cypherglot.validate_cypher_text(
            "MATCH (a:User)-[:KNOWS*0..2]->(b:User) RETURN count(b) AS total"
        )
        variable_length_star_count = cypherglot.validate_cypher_text(
            "MATCH (a:User)-[:KNOWS*1..2]->(b:User) RETURN count(*) AS total"
        )
        variable_length_sum = cypherglot.validate_cypher_text(
            "MATCH (a:User)-[:KNOWS*1..2]->(b:User) RETURN sum(b.score) AS total"
        )
        variable_length_grouped_count = cypherglot.validate_cypher_text(
            "MATCH (a:User)-[:KNOWS*1..2]->(b:User) RETURN b.name AS friend, count(b) AS total ORDER BY total DESC"
        )

        self.assertEqual(type(merge_node).__name__, "OC_SinglePartQueryContext")
        self.assertEqual(type(match_merge).__name__, "OC_SinglePartQueryContext")
        self.assertEqual(type(traversal_create).__name__, "OC_SinglePartQueryContext")
        self.assertEqual(type(traversal_create_with_new).__name__, "OC_SinglePartQueryContext")
        self.assertEqual(type(traversal_create_with_unlabeled_new).__name__, "OC_SinglePartQueryContext")
        self.assertEqual(type(traversal_merge_with_new).__name__, "OC_SinglePartQueryContext")
        self.assertEqual(type(traversal_merge_with_unlabeled_new).__name__, "OC_SinglePartQueryContext")
        self.assertEqual(type(traversal_merge).__name__, "OC_SinglePartQueryContext")
        self.assertEqual(type(variable_length_zero_hop).__name__, "OC_SinglePartQueryContext")
        self.assertEqual(type(variable_length_count).__name__, "OC_SinglePartQueryContext")
        self.assertEqual(type(variable_length_star_count).__name__, "OC_SinglePartQueryContext")
        self.assertEqual(type(variable_length_sum).__name__, "OC_SinglePartQueryContext")
        self.assertEqual(type(variable_length_grouped_count).__name__, "OC_SinglePartQueryContext")

    def test_validate_cypher_text_rejects_merge_actions_for_now(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            "validates MERGE only without ON CREATE or ON MATCH actions",
        ):
            cypherglot.validate_cypher_text(
                "MERGE (u:User {name: 'Alice'}) ON CREATE SET u.created = true"
            )

    def test_validate_cypher_text_rejects_deferred_clause_families(self) -> None:
        cases = [
            (
                "OPTIONAL MATCH (a:User)-[:KNOWS]->(b:User) RETURN b.name",
                "admits only single-node OPTIONAL MATCH patterns",
            ),
            (
                "MATCH p = (a:User)-[:KNOWS]->(b:User) RETURN b.name",
                "does not admit named path patterns",
            ),
            (
                "MATCH (a:User)-[r:KNOWS*1..2]->(b:User) RETURN r",
                "supports RETURN alias.field for admitted entity bindings, RETURN entity_alias for admitted whole-entity bindings",
            ),
            (
                "MATCH (a:User)-[:KNOWS*1..]->(b:User) RETURN b.name",
                "admits only bounded non-negative-length variable-length relationship patterns",
            ),
            (
                "MATCH (a:User)-[:KNOWS*1..2]->(b:User) RETURN b AS friend, count(b) AS total",
                r"admits variable-length relationship MATCH ... RETURN only with non-aggregate RETURN projections, or with endpoint-field grouped aggregate projections built from count\(\*\) / count\(endpoint_alias\) / aggregate\(endpoint.field\)",
            ),
            (
                "MATCH (a:User)-[:KNOWS*1..2]->(b:User) RETURN lower(b.name) AS friend, sum(b.score) AS total",
                r"admits variable-length relationship MATCH ... RETURN only with non-aggregate RETURN projections, or with endpoint-field grouped aggregate projections built from count\(\*\) / count\(endpoint_alias\) / aggregate\(endpoint.field\)",
            ),
            (
                "MATCH (a:User)-[:KNOWS]->(b:User)-[:KNOWS]->(c:User) SET c.name = 'Carol'",
                "does not admit multi-hop pattern chains",
            ),
            (
                "MATCH (a:User)-[:KNOWS]->(b:User) CREATE (:Left)-[:INTRODUCED]->(:Right)",
                "validates traversal-backed MATCH ... CREATE only with exactly one reused matched node alias plus at most one fresh endpoint node",
            ),
            (
                "MATCH (n), (m) RETURN n.id",
                "does not admit disconnected multi-pattern MATCH clauses",
            ),
            (
                "MATCH (u:User) WITH u.name AS name RETURN name.value",
                "supports RETURN alias.field for entity bindings, RETURN entity_alias for pass-through entity bindings, RETURN scalar_alias for scalar bindings, and optional AS aliases for those projection forms",
            ),
            (
                "MATCH (u:User) WITH u AS person RETURN type(person) AS rel_type",
                "supports type\(\.\.\.\) in the WITH subset only over admitted relationship entity bindings",
            ),
            (
                "MATCH (u:User) WITH u AS person RETURN coalesce(person, 'unknown') AS display_name",
                "supports coalesce\(\.\.\.\) in the WITH subset only as coalesce\(entity_alias.field, literal_or_parameter\) or coalesce\(scalar_alias, literal_or_parameter\)",
            ),
            (
                "MATCH (u:User) WITH u AS person RETURN replace(person, 'A', 'B') AS display_name",
                "supports replace\(\.\.\.\) in the WITH subset only as replace\(admitted_input, literal_or_parameter, literal_or_parameter\)",
            ),
            (
                "MATCH (u:User) WITH u AS person RETURN replace(person.name, person.age, 'B') AS display_name",
                "supports replace\(\.\.\.\) in the WITH subset only as replace\(admitted_input, literal_or_parameter, literal_or_parameter\)",
            ),
            (
                "MATCH (u:User) WITH u AS person RETURN right(person, 2) AS value",
                "supports left\(\.\.\.\) and right\(\.\.\.\) in the WITH subset only as function\(admitted_input, literal_or_parameter\)",
            ),
            (
                "MATCH (u:User) WITH u AS person RETURN left(person.name, person.age) AS value",
                "supports left\(\.\.\.\) and right\(\.\.\.\) in the WITH subset only as function\(admitted_input, literal_or_parameter\)",
            ),
            (
                "MATCH (u:User) WITH u AS person RETURN split(person, ' ') AS value",
                "supports split\(\.\.\.\) in the WITH subset only as split\(admitted_input, literal_or_parameter\)",
            ),
            (
                "MATCH (u:User) WITH u AS person RETURN split(person.name, person.age) AS value",
                "supports split\(\.\.\.\) in the WITH subset only as split\(admitted_input, literal_or_parameter\)",
            ),
            (
                "MATCH (u:User) WITH u AS person RETURN abs(person) AS magnitude",
                "supports abs\(\.\.\.\) in the WITH subset only over admitted entity-field projections, scalar bindings, or scalar literal/parameter inputs",
            ),
            (
                "MATCH (u:User) WITH u AS person RETURN sign(person) AS age_sign",
                "supports sign\(\.\.\.\) in the WITH subset only over admitted entity-field projections, scalar bindings, or scalar literal/parameter inputs",
            ),
            (
                "MATCH (u:User) WITH u AS person RETURN round(person) AS value",
                "supports round\(\.\.\.\) in the WITH subset only over admitted entity-field projections, scalar bindings, or scalar literal/parameter inputs",
            ),
            (
                "MATCH (u:User) WITH u AS person RETURN ceil(person) AS value",
                "supports ceil\(\.\.\.\) in the WITH subset only over admitted entity-field projections, scalar bindings, or scalar literal/parameter inputs",
            ),
            (
                "MATCH (u:User) WITH u AS person RETURN floor(person) AS value",
                "supports floor\(\.\.\.\) in the WITH subset only over admitted entity-field projections, scalar bindings, or scalar literal/parameter inputs",
            ),
            (
                "MATCH (u:User) WITH u AS person RETURN sqrt(person) AS value",
                "supports sqrt\(\.\.\.\) in the WITH subset only over admitted entity-field projections, scalar bindings, or scalar literal/parameter inputs",
            ),
            (
                "MATCH (u:User) WITH u AS person RETURN exp(person) AS value",
                "supports exp\(\.\.\.\) in the WITH subset only over admitted entity-field projections, scalar bindings, or scalar literal/parameter inputs",
            ),
            (
                "MATCH (u:User) WITH u AS person RETURN sin(person) AS value",
                "supports sin\(\.\.\.\) in the WITH subset only over admitted entity-field projections, scalar bindings, or scalar literal/parameter inputs",
            ),
            (
                "MATCH (u:User) WITH u AS person RETURN cos(person) AS value",
                "supports cos\(\.\.\.\) in the WITH subset only over admitted entity-field projections, scalar bindings, or scalar literal/parameter inputs",
            ),
            (
                "MATCH (u:User) WITH u AS person RETURN tan(person) AS value",
                "supports tan\(\.\.\.\) in the WITH subset only over admitted entity-field projections, scalar bindings, or scalar literal/parameter inputs",
            ),
            (
                "MATCH (u:User) WITH u AS person RETURN asin(person) AS value",
                "supports asin\(\.\.\.\) in the WITH subset only over admitted entity-field projections, scalar bindings, or scalar literal/parameter inputs",
            ),
            (
                "MATCH (u:User) WITH u AS person RETURN acos(person) AS value",
                "supports acos\(\.\.\.\) in the WITH subset only over admitted entity-field projections, scalar bindings, or scalar literal/parameter inputs",
            ),
            (
                "MATCH (u:User) WITH u AS person RETURN atan(person) AS value",
                "supports atan\(\.\.\.\) in the WITH subset only over admitted entity-field projections, scalar bindings, or scalar literal/parameter inputs",
            ),
            (
                "MATCH (u:User) WITH u AS person RETURN ln(person) AS value",
                "supports ln\(\.\.\.\) in the WITH subset only over admitted entity-field projections, scalar bindings, or scalar literal/parameter inputs",
            ),
            (
                "MATCH (u:User) WITH u AS person RETURN log(person) AS value",
                "supports log\(\.\.\.\) in the WITH subset only over admitted entity-field projections, scalar bindings, or scalar literal/parameter inputs",
            ),
            (
                "MATCH (u:User) WITH u AS person RETURN radians(person) AS value",
                "supports radians\(\.\.\.\) in the WITH subset only over admitted entity-field projections, scalar bindings, or scalar literal/parameter inputs",
            ),
            (
                "MATCH (u:User) WITH u AS person RETURN degrees(person) AS value",
                "supports degrees\(\.\.\.\) in the WITH subset only over admitted entity-field projections, scalar bindings, or scalar literal/parameter inputs",
            ),
            (
                "MATCH (u:User) WITH u AS person RETURN log10(person) AS value",
                "supports log10\(\.\.\.\) in the WITH subset only over admitted entity-field projections, scalar bindings, or scalar literal/parameter inputs",
            ),
            (
                "MATCH (u:User) WITH u AS person RETURN ltrim(person) AS value",
                "supports lower\(\.\.\.\), upper\(\.\.\.\), trim\(\.\.\.\), ltrim\(\.\.\.\), rtrim\(\.\.\.\), and reverse\(\.\.\.\) in the WITH subset only over admitted entity-field projections, scalar bindings, or scalar literal/parameter inputs",
            ),
            (
                "MATCH (u:User) WITH u AS person RETURN rtrim(person) AS value",
                "supports lower\(\.\.\.\), upper\(\.\.\.\), trim\(\.\.\.\), ltrim\(\.\.\.\), rtrim\(\.\.\.\), and reverse\(\.\.\.\) in the WITH subset only over admitted entity-field projections, scalar bindings, or scalar literal/parameter inputs",
            ),
            (
                "MATCH (u:User) WITH u AS person RETURN toString(person) AS text",
                "supports toString\(\.\.\.\) in the WITH subset only over admitted entity-field projections, scalar bindings, or scalar literal/parameter inputs",
            ),
            (
                "MATCH (u:User) WITH u AS person RETURN toInteger(person) AS value",
                "supports toInteger\(\.\.\.\) in the WITH subset only over admitted entity-field projections, scalar bindings, or scalar literal/parameter inputs",
            ),
            (
                "MATCH (u:User) WITH u AS person RETURN toFloat(person) AS value",
                "supports toFloat\(\.\.\.\) in the WITH subset only over admitted entity-field projections, scalar bindings, or scalar literal/parameter inputs",
            ),
            (
                "MATCH (u:User) WITH u AS person RETURN toBoolean(person) AS value",
                "supports toBoolean\(\.\.\.\) in the WITH subset only over admitted entity-field projections, scalar bindings, or scalar literal/parameter inputs",
            ),
            (
                "MATCH (u:User) WITH u AS person RETURN reverse(person) AS value",
                "supports lower\(\.\.\.\), upper\(\.\.\.\), trim\(\.\.\.\), ltrim\(\.\.\.\), rtrim\(\.\.\.\), and reverse\(\.\.\.\) in the WITH subset only over admitted entity-field projections, scalar bindings, or scalar literal/parameter inputs",
            ),
            (
                "MATCH (u:User) WITH u AS person RETURN substring(person, 0, 2) AS value",
                "supports substring\(\.\.\.\) in the WITH subset only as substring\(admitted_input, literal_or_parameter\) or substring\(admitted_input, literal_or_parameter, literal_or_parameter\)",
            ),
            (
                "MATCH (u:User) WITH u AS person RETURN substring(person.name, person.age, 2) AS value",
                "supports substring\(\.\.\.\) in the WITH subset only as substring\(admitted_input, literal_or_parameter\) or substring\(admitted_input, literal_or_parameter, literal_or_parameter\)",
            ),
            (
                "MATCH (u:User) WITH u AS person RETURN size(person) AS n",
                "supports size\(\.\.\.\) in the WITH subset only over admitted entity-field projections, admitted id/type outputs, scalar bindings, or scalar literal/parameter inputs",
            ),
            (
                "MATCH (u:User) WITH u AS person RETURN size(type(person)) AS n",
                "supports size\(\.\.\.\) in the WITH subset only over admitted entity-field projections, admitted id/type outputs, scalar bindings, or scalar literal/parameter inputs",
            ),
            (
                "MATCH (u:User) WITH u AS person RETURN 'tag'",
                "requires scalar literal and parameter RETURN items in the WITH subset to use an explicit AS alias",
            ),
            (
                "MATCH (u:User) WITH u AS person RETURN person.age >= 18",
                "requires predicate RETURN items in the WITH subset to use an explicit AS alias",
            ),
            (
                "MATCH (u:User) WITH u AS person RETURN person >= 18 AS adult",
                "supports predicate RETURN items in the WITH subset only as scalar_alias OP value, entity_alias.field OP value, id\(entity_alias\) OP value, type\(rel_alias\) OP value, or size\(admitted_input\) OP value",
            ),
            (
                "MATCH (u:User) WITH u AS person RETURN type(person) = 'User' AS is_user",
                "supports predicate RETURN items in the WITH subset only as scalar_alias OP literal_or_parameter, entity_alias.field OP literal_or_parameter, id\(entity_alias\) OP literal_or_parameter, type\(rel_alias\) OP literal_or_parameter, or size\(admitted_input\) OP literal_or_parameter",
            ),
            (
                "MATCH (u:User) WITH u.name RETURN u.name",
                "requires WITH scalar rebinding to use an explicit AS alias",
            ),
            (
                "MATCH (u:User) WITH u AS person RETURN count(missing) AS total",
                "requires count\(\.\.\.\) in the WITH subset to target an admitted binding alias",
            ),
            (
                "MATCH (u:User) WITH u.name AS name RETURN properties(name) AS props",
                "supports properties\(\.\.\.\) in the WITH subset only over admitted entity bindings",
            ),
            (
                "MATCH (a:User)-[r:KNOWS]->(b:User) WITH r AS rel RETURN labels(rel) AS labels",
                "supports labels\(\.\.\.\) in the WITH subset only over admitted node entity bindings",
            ),
            (
                "MATCH (u:User) WITH u.name AS name RETURN keys(name) AS keys",
                "supports keys\(\.\.\.\) in the WITH subset only over admitted entity bindings",
            ),
            (
                "MATCH (u:User) WITH u AS person RETURN startNode(person) AS start",
                "supports startNode\(\.\.\.\) in the WITH subset only over admitted relationship entity bindings",
            ),
            (
                "MATCH (u:User) WITH u AS person RETURN endNode(person) AS ending",
                "supports endNode\(\.\.\.\) in the WITH subset only over admitted relationship entity bindings",
            ),
            (
                "MATCH (u:User) WITH u AS person, u.name AS name RETURN person AS item, name AS item",
                "does not allow duplicate RETURN output alias",
            ),
            (
                "MATCH (u:User) RETURN missing",
                "supports RETURN alias.field for admitted entity bindings",
            ),
            (
                "MATCH (u:User) RETURN properties(missing) AS props",
                "supports properties\(\.\.\.\) in the supported read subset only over admitted entity or relationship bindings",
            ),
            (
                "MATCH (a:User)-[r:KNOWS]->(b:User) RETURN labels(r) AS labels",
                "supports labels\(\.\.\.\) in the supported read subset only over admitted node bindings",
            ),
            (
                "MATCH (u:User) RETURN keys(missing) AS keys",
                "supports keys\(\.\.\.\) in the supported read subset only over admitted entity or relationship bindings",
            ),
            (
                "MATCH (u:User) RETURN startNode(u) AS start",
                "supports startNode\(\.\.\.\) in the supported read subset only over admitted relationship bindings",
            ),
            (
                "MATCH (u:User) RETURN endNode(u) AS ending",
                "supports endNode\(\.\.\.\) in the supported read subset only over admitted relationship bindings",
            ),
            (
                "MATCH (a:User)-[r:KNOWS]->(b:User) RETURN a AS item, r AS item",
                "does not allow duplicate RETURN output alias",
            ),
            (
                "MATCH (u:User) RETURN count(missing) AS total",
                "requires count\(\.\.\.\) in the supported read subset to target an admitted binding alias",
            ),
            (
                "MATCH (u:User) WITH u AS person RETURN sum(person) AS total",
                "supports sum\(\.\.\.\), avg\(\.\.\.\), min\(\.\.\.\), and max\(\.\.\.\) in the WITH subset only over admitted scalar bindings",
            ),
            (
                "MATCH (u:User) RETURN 'tag'",
                "requires scalar literal and parameter RETURN items in the supported read subset to use an explicit AS alias",
            ),
            (
                "MATCH (u:User) RETURN coalesce(u, 'unknown') AS display_name",
                "supports coalesce\(\.\.\.\) in the supported read subset only as coalesce\(alias.field, literal_or_parameter\) over admitted bindings",
            ),
            (
                "MATCH (u:User) RETURN replace(u, 'A', 'B') AS display_name",
                "supports replace\(\.\.\.\) in the supported read subset only as replace\(admitted_input, literal_or_parameter, literal_or_parameter\)",
            ),
            (
                "MATCH (u:User) RETURN replace(u.name, u.age, 'B') AS display_name",
                "supports replace\(\.\.\.\) in the supported read subset only as replace\(admitted_input, literal_or_parameter, literal_or_parameter\)",
            ),
            (
                "MATCH (u:User) RETURN right(u, 2) AS value",
                "supports left\(\.\.\.\) and right\(\.\.\.\) in the supported read subset only as function\(admitted_input, literal_or_parameter\)",
            ),
            (
                "MATCH (u:User) RETURN left(u.name, u.age) AS value",
                "supports left\(\.\.\.\) and right\(\.\.\.\) in the supported read subset only as function\(admitted_input, literal_or_parameter\)",
            ),
            (
                "MATCH (u:User) RETURN split(u, ' ') AS value",
                "supports split\(\.\.\.\) in the supported read subset only as split\(admitted_input, literal_or_parameter\)",
            ),
            (
                "MATCH (u:User) RETURN split(u.name, u.age) AS value",
                "supports split\(\.\.\.\) in the supported read subset only as split\(admitted_input, literal_or_parameter\)",
            ),
            (
                "MATCH (u:User) RETURN abs(u) AS magnitude",
                "supports abs\(\.\.\.\) in the supported read subset only over admitted field projections or scalar literal/parameter inputs",
            ),
            (
                "MATCH (u:User) RETURN sign(u) AS age_sign",
                "supports sign\(\.\.\.\) in the supported read subset only over admitted field projections or scalar literal/parameter inputs",
            ),
            (
                "MATCH (u:User) RETURN round(u) AS value",
                "supports round\(\.\.\.\) in the supported read subset only over admitted field projections or scalar literal/parameter inputs",
            ),
            (
                "MATCH (u:User) RETURN ceil(u) AS value",
                "supports ceil\(\.\.\.\) in the supported read subset only over admitted field projections or scalar literal/parameter inputs",
            ),
            (
                "MATCH (u:User) RETURN floor(u) AS value",
                "supports floor\(\.\.\.\) in the supported read subset only over admitted field projections or scalar literal/parameter inputs",
            ),
            (
                "MATCH (u:User) RETURN sqrt(u) AS value",
                "supports sqrt\(\.\.\.\) in the supported read subset only over admitted field projections or scalar literal/parameter inputs",
            ),
            (
                "MATCH (u:User) RETURN exp(u) AS value",
                "supports exp\(\.\.\.\) in the supported read subset only over admitted field projections or scalar literal/parameter inputs",
            ),
            (
                "MATCH (u:User) RETURN sin(u) AS value",
                "supports sin\(\.\.\.\) in the supported read subset only over admitted field projections or scalar literal/parameter inputs",
            ),
            (
                "MATCH (u:User) RETURN cos(u) AS value",
                "supports cos\(\.\.\.\) in the supported read subset only over admitted field projections or scalar literal/parameter inputs",
            ),
            (
                "MATCH (u:User) RETURN tan(u) AS value",
                "supports tan\(\.\.\.\) in the supported read subset only over admitted field projections or scalar literal/parameter inputs",
            ),
            (
                "MATCH (u:User) RETURN asin(u) AS value",
                "supports asin\(\.\.\.\) in the supported read subset only over admitted field projections or scalar literal/parameter inputs",
            ),
            (
                "MATCH (u:User) RETURN acos(u) AS value",
                "supports acos\(\.\.\.\) in the supported read subset only over admitted field projections or scalar literal/parameter inputs",
            ),
            (
                "MATCH (u:User) RETURN atan(u) AS value",
                "supports atan\(\.\.\.\) in the supported read subset only over admitted field projections or scalar literal/parameter inputs",
            ),
            (
                "MATCH (u:User) RETURN ln(u) AS value",
                "supports ln\(\.\.\.\) in the supported read subset only over admitted field projections or scalar literal/parameter inputs",
            ),
            (
                "MATCH (u:User) RETURN log(u) AS value",
                "supports log\(\.\.\.\) in the supported read subset only over admitted field projections or scalar literal/parameter inputs",
            ),
            (
                "MATCH (u:User) RETURN radians(u) AS value",
                "supports radians\(\.\.\.\) in the supported read subset only over admitted field projections or scalar literal/parameter inputs",
            ),
            (
                "MATCH (u:User) RETURN degrees(u) AS value",
                "supports degrees\(\.\.\.\) in the supported read subset only over admitted field projections or scalar literal/parameter inputs",
            ),
            (
                "MATCH (u:User) RETURN log10(u) AS value",
                "supports log10\(\.\.\.\) in the supported read subset only over admitted field projections or scalar literal/parameter inputs",
            ),
            (
                "MATCH (u:User) RETURN ltrim(u) AS value",
                "supports lower\(\.\.\.\), upper\(\.\.\.\), trim\(\.\.\.\), ltrim\(\.\.\.\), rtrim\(\.\.\.\), and reverse\(\.\.\.\) in the supported read subset only over admitted field projections or scalar literal/parameter inputs",
            ),
            (
                "MATCH (u:User) RETURN rtrim(u) AS value",
                "supports lower\(\.\.\.\), upper\(\.\.\.\), trim\(\.\.\.\), ltrim\(\.\.\.\), rtrim\(\.\.\.\), and reverse\(\.\.\.\) in the supported read subset only over admitted field projections or scalar literal/parameter inputs",
            ),
            (
                "MATCH (u:User) RETURN toString(u) AS text",
                "supports toString\(\.\.\.\) in the supported read subset only over admitted field projections or scalar literal/parameter inputs",
            ),
            (
                "MATCH (u:User) RETURN toInteger(u) AS value",
                "supports toInteger\(\.\.\.\) in the supported read subset only over admitted field projections or scalar literal/parameter inputs",
            ),
            (
                "MATCH (u:User) RETURN toFloat(u) AS value",
                "supports toFloat\(\.\.\.\) in the supported read subset only over admitted field projections or scalar literal/parameter inputs",
            ),
            (
                "MATCH (u:User) RETURN toBoolean(u) AS value",
                "supports toBoolean\(\.\.\.\) in the supported read subset only over admitted field projections or scalar literal/parameter inputs",
            ),
            (
                "MATCH (u:User) RETURN reverse(u) AS value",
                "supports lower\(\.\.\.\), upper\(\.\.\.\), trim\(\.\.\.\), ltrim\(\.\.\.\), rtrim\(\.\.\.\), and reverse\(\.\.\.\) in the supported read subset only over admitted field projections or scalar literal/parameter inputs",
            ),
            (
                "MATCH (u:User) RETURN substring(u, 0, 2) AS value",
                "supports substring\(\.\.\.\) in the supported read subset only as substring\(admitted_input, literal_or_parameter\) or substring\(admitted_input, literal_or_parameter, literal_or_parameter\)",
            ),
            (
                "MATCH (u:User) RETURN substring(u.name, u.age, 2) AS value",
                "supports substring\(\.\.\.\) in the supported read subset only as substring\(admitted_input, literal_or_parameter\) or substring\(admitted_input, literal_or_parameter, literal_or_parameter\)",
            ),
            (
                "MATCH (u:User) RETURN size(u) AS n",
                "supports size\(\.\.\.\) in the supported read subset only over admitted field projections, admitted id/type outputs, or scalar literal/parameter inputs",
            ),
            (
                "MATCH (u:User) RETURN size(type(u)) AS n",
                "supports size\(\.\.\.\) in the supported read subset only over admitted field projections, admitted id/type outputs, or scalar literal/parameter inputs",
            ),
            (
                "MATCH (u:User) RETURN type(u) AS t",
                "supports type\(\.\.\.\) in the supported read subset only over admitted relationship bindings",
            ),
            (
                "MATCH (u:User) RETURN u.age >= 18",
                "requires predicate RETURN items in the supported read subset to use an explicit AS alias",
            ),
            (
                "MATCH (u:User) RETURN u >= 18 AS adult",
                "supports predicate RETURN items in the supported read subset only as alias.field OP value, id\(alias\) OP value, type\(rel_alias\) OP value, or size\(admitted_input\) OP value over admitted bindings",
            ),
            (
                "MATCH (u:User) RETURN type(u) = 'User' AS is_user",
                "supports predicate RETURN items in the supported read subset only as alias.field OP value, id\(alias\) OP value, type\(rel_alias\) OP value, or size\(admitted_input\) OP value over admitted bindings",
            ),
            (
                "MATCH (u:User) WITH u AS person WHERE person = 'Alice' RETURN person",
                "supports WITH WHERE items shaped as scalar_alias OP value, entity_alias.field OP value, id\(entity_alias\) OP value, type\(rel_alias\) OP value, or size\(admitted_input\) OP value",
            ),
            (
                "MATCH (u:User) WITH u AS person WHERE type(person) = 'User' RETURN person",
                "supports WITH WHERE type\(rel_alias\) only for relationship entity bindings",
            ),
            (
                "UNWIND [1, 2, 3] AS x RETURN x.value",
                "supports only RETURN unwind_alias or RETURN unwind_alias AS output_alias",
            ),
            (
                "UNWIND 1 AS x RETURN x",
                "requires UNWIND sources to be list literals or named parameters",
            ),
        ]

        for query, message in cases:
            with self.subTest(query=query):
                with self.assertRaisesRegex(ValueError, message):
                    cypherglot.validate_cypher_text(query)

    def test_validate_cypher_text_accepts_vector_query_nodes_subset(self) -> None:
        validated = cypherglot.validate_cypher_text(
            "CALL db.index.vector.queryNodes('user_embedding_idx', 1, $query) "
            "YIELD node, score MATCH (node:User) RETURN node.id, score ORDER BY score DESC"
        )
        yield_where_validated = cypherglot.validate_cypher_text(
            "CALL db.index.vector.queryNodes('user_embedding_idx', 3, $query) "
            "YIELD node, score WHERE node.region = 'west' RETURN node.id, score ORDER BY score DESC"
        )

        self.assertEqual(type(validated).__name__, "OC_SinglePartQueryContext")
        self.assertEqual(type(yield_where_validated).__name__, "OC_SinglePartQueryContext")

    def test_validate_cypher_text_rejects_other_call_shapes_in_vector_subset(self) -> None:
        with self.assertRaisesRegex(ValueError, "admits only the db.index.vector.queryNodes"):
            cypherglot.validate_cypher_text(
                "CALL db.index.vector.queryRelationships('idx', 1, $query) "
                "YIELD node, score RETURN node.id, score"
            )
