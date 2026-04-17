from __future__ import annotations

import unittest

import cypherglot


class TestValidate(unittest.TestCase):
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

