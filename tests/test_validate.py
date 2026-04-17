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

