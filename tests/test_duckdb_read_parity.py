from __future__ import annotations

import json
import sqlite3
import unittest

import cypherglot

try:
    import duckdb
except ImportError:  # pragma: no cover - optional test dependency
    duckdb = None


@unittest.skipIf(duckdb is None, "duckdb is not installed")
class DuckDBReadParityTests(unittest.TestCase):
    def setUp(self) -> None:
        assert duckdb is not None
        self.sqlite = sqlite3.connect(":memory:")
        self.sqlite.execute("PRAGMA foreign_keys = ON")
        self.sqlite.create_function("REVERSE", 1, self._sqlite_reverse)
        self.sqlite.create_function("LEFT", 2, self._sqlite_left)
        self.sqlite.create_function("RIGHT", 2, self._sqlite_right)
        self.sqlite.create_function("SPLIT", 2, self._sqlite_split)
        self.sqlite.create_function("STR_POSITION", 2, self._sqlite_str_position)
        self.sqlite.executescript(
            """
            CREATE TABLE nodes (
              id INTEGER PRIMARY KEY,
              properties TEXT NOT NULL DEFAULT '{}',
              CHECK (json_valid(properties)),
              CHECK (json_type(properties) = 'object')
            ) STRICT;

            CREATE TABLE edges (
              id INTEGER PRIMARY KEY,
              type TEXT NOT NULL,
              from_id INTEGER NOT NULL,
              to_id INTEGER NOT NULL,
              properties TEXT NOT NULL DEFAULT '{}',
              CHECK (json_valid(properties)),
              CHECK (json_type(properties) = 'object'),
              FOREIGN KEY (from_id) REFERENCES nodes(id) ON DELETE CASCADE,
              FOREIGN KEY (to_id) REFERENCES nodes(id) ON DELETE CASCADE
            ) STRICT;

            CREATE TABLE node_labels (
              node_id INTEGER NOT NULL,
              label TEXT NOT NULL,
              PRIMARY KEY (node_id, label),
              FOREIGN KEY (node_id) REFERENCES nodes(id) ON DELETE CASCADE
            ) STRICT;
            """
        )
        self.duckdb = duckdb.connect()
        self.duckdb.execute("CREATE TABLE nodes (id BIGINT, properties VARCHAR)")
        self.duckdb.execute(
            "CREATE TABLE edges (id BIGINT, type VARCHAR, from_id BIGINT, to_id BIGINT, properties VARCHAR)"
        )
        self.duckdb.execute("CREATE TABLE node_labels (node_id BIGINT, label VARCHAR)")
        self._seed_graphs()

    def tearDown(self) -> None:
        self.sqlite.close()
        self.duckdb.close()

    def test_curated_admitted_reads_match_sqlite_results(self) -> None:
        queries = (
            "MATCH (u:User) RETURN u.name ORDER BY u.name",
            "MATCH (u:User) RETURN u ORDER BY u.name",
            "OPTIONAL MATCH (u:User) RETURN u AS user ORDER BY user",
            ("OPTIONAL MATCH (u:User) RETURN $value AS value ORDER BY value", {"value": "tag"}),
            "MATCH (u:User) RETURN rtrim(u.name) AS name ORDER BY name",
            "MATCH (u:User) WITH u.name AS name RETURN name ORDER BY name",
            ("MATCH (u:User) WITH u AS person RETURN 'tag' AS tag, $value AS value ORDER BY tag, value", {"value": "topic"}),
            ("MATCH (u:User) WITH u AS person RETURN size('tag') AS tag_len, size($value) AS value_len ORDER BY tag_len, value_len", {"value": "topic"}),
            "MATCH (u:User) WITH u AS person, u.name AS name RETURN person AS user, person.name AS display_name, name AS raw_name ORDER BY display_name, raw_name",
            ("MATCH (u:User) WITH u AS person, u.name AS name RETURN lower(person.name) AS lower_name, upper(name) AS upper_name, lower('tag') AS lower_tag, upper($value) AS upper_value, trim(name) AS trimmed, ltrim(' tag') AS left_trimmed, rtrim('tag ') AS right_trimmed ORDER BY lower_name, upper_name, lower_tag, upper_value, trimmed, left_trimmed, right_trimmed", {"value": "topic"}),
            "MATCH (u:User) WITH u AS person, u.name AS name RETURN reverse(person.name) AS reversed_name, reverse(name) AS rebound_reverse, reverse('tag') AS lit_reverse ORDER BY reversed_name, rebound_reverse, lit_reverse",
            ("MATCH (u:User) WITH u AS person, u.name AS name RETURN replace(person.name, 'A', 'B') AS display_name, replace(name, $needle, $replacement) AS rebound_name, replace('Alice', 'l', 'x') AS lit_name ORDER BY display_name, rebound_name, lit_name", {"needle": "o", "replacement": "0"}),
            ("MATCH (u:User) WITH u AS person, u.name AS name RETURN left(person.name, 2) AS prefix, right(name, $count) AS rebound_suffix, left('Alice', 3) AS lit_prefix ORDER BY prefix, rebound_suffix, lit_prefix", {"count": 2}),
            ("MATCH (u:User) WITH u AS person, u.name AS name RETURN split(person.name, ' ') AS parts, split(name, $delimiter) AS rebound_parts, split('Alice Bob', ' ') AS lit_parts ORDER BY parts, rebound_parts, lit_parts", {"delimiter": "i"}),
            "MATCH (u:User) WITH u AS person, u.age AS age RETURN abs(person.age) AS magnitude, abs(age) AS rebound, abs(-3) AS lit ORDER BY magnitude, rebound, lit",
            "MATCH (u:User) WITH u AS person, u.age AS age RETURN toInteger(person.age) AS age_int, toInteger(age) AS rebound, toInteger(-3.2) AS lit ORDER BY age_int, rebound, lit",
            "MATCH (u:User) WITH u AS person, u.age AS age RETURN toFloat(person.age) AS age_float, toFloat(age) AS rebound, toFloat(-3) AS lit ORDER BY age_float, rebound, lit",
            "MATCH (u:User) WITH u AS person, u.active AS active RETURN toBoolean(person.active) AS is_active, toBoolean(active) AS rebound, toBoolean(true) AS lit ORDER BY is_active, rebound, lit",
            "MATCH (u:User) WITH u AS person, u.name AS name RETURN substring(person.name, 0, 2) AS prefix, substring(name, 1, 3) AS rebound, substring('Alice', 1, 3) AS lit ORDER BY prefix, rebound, lit",
            "MATCH (u:User) WITH u.name AS name RETURN name, count(*) AS total ORDER BY total DESC, name ASC",
            "MATCH (u:User) WITH u.name AS name, u.score AS score RETURN name, avg(score) AS mean ORDER BY mean DESC, name ASC",
            "MATCH (u:User) WITH u AS person RETURN person AS user, count(person) AS total ORDER BY total DESC",
            "MATCH (a:User)-[r:KNOWS]->(b:User) WITH a.name AS name RETURN name, count(r) AS total ORDER BY total DESC, name ASC",
            "MATCH (a:User)-[r:KNOWS]->(b:User) WITH r AS rel RETURN rel AS edge, count(rel) AS total ORDER BY total DESC",
            "MATCH (u:User) WITH u AS person, u.name AS name RETURN coalesce(person.name, 'unknown'), coalesce(name, 'fallback') ORDER BY coalesce(person.name, 'unknown'), coalesce(name, 'fallback')",
            ("MATCH (u:User) WITH u AS person, u.name AS name RETURN replace(person.name, 'A', 'B'), replace(name, $needle, $replacement) ORDER BY replace(person.name, 'A', 'B'), replace(name, $needle, $replacement)", {"needle": "o", "replacement": "0"}),
            ("MATCH (u:User) WITH u AS person, u.name AS name RETURN left(person.name, 2), right(name, $count) ORDER BY left(person.name, 2), right(name, $count)", {"count": 2}),
            ("MATCH (u:User) WITH u AS person, u.name AS name RETURN split(person.name, ' '), split(name, $delimiter) ORDER BY split(person.name, ' '), split(name, $delimiter)", {"delimiter": "i"}),
            "MATCH (u:User) WITH u AS person, u.age AS age, u.active AS active RETURN toString(person.age), toString(age), toBoolean(active) ORDER BY toString(person.age), toString(age), toBoolean(active)",
            "MATCH (a:User)-[r:KNOWS]->(b:User) WITH a AS person, r AS rel, a.name AS name RETURN id(person), type(rel), size(person.name), size(name) ORDER BY id(person)",
            "MATCH (a:User)-[r:KNOWS]->(b:User) WITH a AS person, r AS rel, a.name AS name RETURN person.age >= 18 AS adult, name = 'Alice' AS is_alice, person.name CONTAINS 'a' AS has_a, name CONTAINS 'i' AS rebound_has_i, person.name STARTS WITH 'Al' AS has_prefix, name ENDS WITH 'ce' AS has_suffix, id(person) >= 1 AS has_id, type(rel) = 'KNOWS' AS rel_matches, size(person.name) >= 3 AS long_name, size(name) >= 3 AS rebound_long, size(id(person)) >= 1 AS long_id, size(type(rel)) >= 5 AS long_type ORDER BY adult, is_alice, has_a, rebound_has_i, has_prefix, has_suffix, has_id, rel_matches, long_name, rebound_long, long_id, long_type",
            "MATCH (a:User)-[r:KNOWS]->(b:User) WITH a AS person, r AS rel, a.name AS name, r.note AS note RETURN person.name IS NULL AS missing_person_name, name IS NOT NULL AS has_name, rel.note IS NULL AS missing_rel_note, note IS NOT NULL AS has_note ORDER BY missing_person_name, has_name, missing_rel_note, has_note",
            "MATCH (a:User)-[r:KNOWS]->(b:User) WITH r AS rel, r.note AS note RETURN lower(rel.note) AS lower_note, upper(note) AS upper_note, reverse(note) AS reversed_note, left(rel.note, 2) AS prefix, right(note, 2) AS suffix, split(rel.note, ' ') AS parts, substring(note, 1) AS tail ORDER BY lower_note, upper_note, reversed_note, prefix, suffix, parts, tail",
            "MATCH (a:User)-[r:KNOWS]->(b:User) WITH r AS rel, r.weight AS weight, r.score AS score, r.active AS active RETURN abs(rel.weight) AS magnitude, sign(weight) AS weight_sign, round(rel.score) AS rounded_score, ceil(score) AS ceil_score, toString(weight) AS weight_text, toInteger(rel.score) AS score_int, toFloat(weight) AS weight_float, toBoolean(active) AS active_bool ORDER BY magnitude, weight_sign, rounded_score, ceil_score, weight_text, score_int, weight_float, active_bool",
            "MATCH (u:User) WITH u AS person, u.name AS name WHERE name = 'Alice' AND person.id > 1 RETURN person, name ORDER BY name",
            "MATCH (u:User) WITH u AS person, u.name AS name WHERE id(person) >= 1 RETURN person, name ORDER BY name",
            "MATCH (a:User)-[r:KNOWS]->(b:User) WITH a AS person, r AS rel WHERE type(rel) = 'KNOWS' RETURN person, rel",
            "MATCH (a:User)-[r:KNOWS]->(b:User) WITH a AS person, r AS rel, a.name AS name WHERE name IS NOT NULL AND person.name IS NULL AND rel.note IS NOT NULL RETURN person, rel",
            "MATCH (a:User)-[r:KNOWS]->(b:User) WITH a AS person, r AS rel, a.name AS name, r.note AS note WHERE person.name STARTS WITH 'Al' AND name CONTAINS 'li' AND rel.note ENDS WITH 'ce' AND note IS NOT NULL RETURN person, rel",
            "MATCH (a:User)-[r:KNOWS]->(b:User) WITH a AS person, r AS rel, a.name AS name, r.note AS note WHERE size(person.name) >= 3 AND size(name) >= 3 AND size(rel.note) IS NOT NULL AND size(note) IS NOT NULL RETURN person, rel",
            "OPTIONAL MATCH (u:User) WHERE u.name = 'Alice' RETURN u.name AS name",
            "OPTIONAL MATCH (u:User) WHERE u.name = 'Zed' RETURN u.name AS name",
            ("OPTIONAL MATCH (u:User) RETURN coalesce(u.name, $fallback) AS display_name ORDER BY display_name", {"fallback": "unknown"}),
            "OPTIONAL MATCH (u:User) RETURN lower(u.name) AS lower_name ORDER BY lower_name",
            "OPTIONAL MATCH (u:User) RETURN trim(u.name) AS trimmed ORDER BY trimmed",
            "OPTIONAL MATCH (u:User) RETURN ltrim(u.name) AS left_trimmed ORDER BY left_trimmed",
            "OPTIONAL MATCH (u:User) RETURN rtrim(u.name) AS right_trimmed ORDER BY right_trimmed",
            "OPTIONAL MATCH (u:User) RETURN reverse(u.name) AS reversed_name ORDER BY reversed_name",
            "OPTIONAL MATCH (u:User) RETURN properties(u) AS props ORDER BY props",
            "OPTIONAL MATCH (u:User) RETURN labels(u) AS labels ORDER BY labels",
            "OPTIONAL MATCH (u:User) RETURN keys(u) AS keys ORDER BY keys",
            "OPTIONAL MATCH (u:User) RETURN abs(u.age) AS magnitude ORDER BY magnitude",
            "OPTIONAL MATCH (u:User) RETURN sign(u.age) AS age_sign ORDER BY age_sign",
            "OPTIONAL MATCH (u:User) RETURN round(u.score) AS value ORDER BY value",
            "OPTIONAL MATCH (u:User) RETURN ceil(u.score) AS value ORDER BY value",
            "OPTIONAL MATCH (u:User) RETURN floor(u.score) AS value ORDER BY value",
            "OPTIONAL MATCH (u:User) RETURN sqrt(u.score) AS value ORDER BY value",
            "OPTIONAL MATCH (u:User) RETURN exp(u.score) AS value ORDER BY value",
            "OPTIONAL MATCH (u:User) RETURN toString(u.age) AS text ORDER BY text",
            "OPTIONAL MATCH (u:User) RETURN toInteger(u.age) AS age_int ORDER BY age_int",
            "OPTIONAL MATCH (u:User) RETURN toFloat(u.age) AS age_float ORDER BY age_float",
            "OPTIONAL MATCH (u:User) RETURN toBoolean(u.active) AS is_active ORDER BY is_active",
            ("OPTIONAL MATCH (u:User) RETURN replace(u.name, $needle, $replacement) AS display_name ORDER BY display_name", {"needle": "A", "replacement": "B"}),
            ("OPTIONAL MATCH (u:User) RETURN right(u.name, $count) AS suffix ORDER BY suffix", {"count": 2}),
            ("OPTIONAL MATCH (u:User) RETURN split(u.name, $delimiter) AS parts ORDER BY parts", {"delimiter": "i"}),
            "OPTIONAL MATCH (u:User) RETURN substring(u.name, 0, 2) AS prefix ORDER BY prefix",
            "OPTIONAL MATCH (u:User) RETURN substring(u.name, 1) AS suffix ORDER BY suffix",
            "OPTIONAL MATCH (u:User) RETURN u.name IS NULL AS missing_name ORDER BY missing_name",
            "OPTIONAL MATCH (u:User) RETURN u.name AS name, count(u) AS total ORDER BY total DESC",
            "OPTIONAL MATCH (u:User) RETURN u.name AS name, count(*) AS total ORDER BY total DESC",
            "OPTIONAL MATCH (u:User) RETURN u AS user, count(u) AS total ORDER BY total DESC",
            "OPTIONAL MATCH (u:User) RETURN 'tag' AS tag ORDER BY tag",
            "OPTIONAL MATCH (u:User) RETURN size(id(u)) AS id_len ORDER BY id_len",
            "OPTIONAL MATCH (u:User) RETURN id(u) AS uid ORDER BY uid",
            "OPTIONAL MATCH (u:User) RETURN u.age >= 18 AS adult, u.name CONTAINS 'a' AS has_a, u.name STARTS WITH 'Al' AS has_prefix, u.name ENDS WITH 'ce' AS has_suffix, id(u) >= 1 AS has_id ORDER BY adult, has_a, has_prefix, has_suffix, has_id",
            "MATCH (u:User) RETURN count(*) AS total",
            "MATCH (u:User) RETURN 'tag' AS tag ORDER BY tag",
            ("MATCH (u:User) RETURN $value AS value ORDER BY value", {"value": "tag"}),
            "MATCH (u:User) RETURN u.name AS name, count(*) AS total ORDER BY total DESC, name ASC",
            "MATCH (u:User) RETURN u.name AS name, avg(u.score) AS mean ORDER BY mean DESC",
            "MATCH (u:User) RETURN u AS user, count(u) AS total ORDER BY total DESC",
            "MATCH (u:User) RETURN properties(u) AS props, labels(u) AS labels ORDER BY u.name",
            "MATCH (a:User)-[r:KNOWS]->(b:User) RETURN keys(r) AS rel_keys ORDER BY a.name ASC",
            "MATCH (a:User)-[r:KNOWS]->(b:User) RETURN a AS user, r AS rel, b.name AS name ORDER BY name",
            "MATCH (a:User)-[r:KNOWS]->(b:User) RETURN r AS rel ORDER BY rel",
            "MATCH (a:User)-[r:KNOWS]->(b:User) RETURN id(a) AS uid, type(r) AS rel_type, startNode(r).id AS start_id, endNode(r).id AS end_id ORDER BY uid",
            "MATCH (a:User)-[r:KNOWS]->(b:User) RETURN startNode(r) AS start, endNode(r) AS ending ORDER BY start, ending",
            "MATCH (a:User)-[r:KNOWS]->(b:User) RETURN a.name AS name, count(r) AS total ORDER BY total DESC",
            "MATCH (a:User)-[r:KNOWS]->(b:User) RETURN r AS rel, count(r) AS total ORDER BY total DESC",
            "MATCH (a:User)-[r:KNOWS]->(b:User) RETURN r AS rel, count(*) AS total ORDER BY total DESC",
            "MATCH (u:User) RETURN CASE WHEN u.age >= 18 THEN u.name ELSE 'minor' END AS label ORDER BY label",
            "MATCH (u:User) RETURN size(u.name), size(id(u))",
            "MATCH (u:User) RETURN lower(u.name) AS lower_name, reverse(u.name) AS reversed_name ORDER BY lower_name, reversed_name",
            "MATCH (u:User) RETURN lower(u.name), reverse(u.name)",
            "MATCH (u:User) RETURN size(u.name) AS name_len, substring(u.name, 1) AS tail ORDER BY name_len, tail",
            "MATCH (u:User) RETURN size('tag') AS tag_len ORDER BY tag_len",
            "MATCH (u:User) RETURN size(id(u)) AS id_len ORDER BY id_len",
            "MATCH (a:User)-[r:KNOWS]->(b:User) RETURN size(type(r)) AS type_len ORDER BY type_len",
            "MATCH (u:User) RETURN coalesce(u.name, 'unknown') AS display_name, replace(u.name, 'A', 'B') AS replaced ORDER BY display_name, replaced",
            "MATCH (u:User) RETURN coalesce(u.name, 'unknown')",
            "MATCH (u:User) RETURN replace(u.name, 'A', 'B')",
            "MATCH (u:User) RETURN left(u.name, 2) AS prefix, right(u.name, 2) AS suffix ORDER BY prefix, suffix",
            "MATCH (u:User) RETURN left(u.name, 2)",
            "MATCH (u:User) RETURN split(u.name, 'i') AS parts ORDER BY u.name",
            "MATCH (u:User) RETURN split(u.name, ' ')",
            "MATCH (u:User) RETURN upper('tag') AS upper_tag ORDER BY upper_tag",
            "MATCH (u:User) RETURN ltrim(' tag') AS left_trimmed ORDER BY left_trimmed",
            "MATCH (u:User) RETURN rtrim('tag ') AS right_trimmed ORDER BY right_trimmed",
            "MATCH (u:User) RETURN sum(u.age) AS total_age",
            "MATCH (u:User) RETURN avg(u.age) AS avg_age",
            "MATCH (u:User) RETURN min(u.age) AS min_age",
            "MATCH (u:User) RETURN max(u.age) AS max_age",
            "MATCH (u:User) RETURN abs(u.age) AS magnitude, sign(u.age) AS age_sign, toInteger(u.age) AS age_int, toFloat(u.age) AS age_float ORDER BY magnitude, age_sign, age_int, age_float",
            "MATCH (u:User) RETURN abs(-3) AS magnitude ORDER BY magnitude",
            "MATCH (u:User) RETURN sign(-3.2) AS age_sign ORDER BY age_sign",
            "MATCH (u:User) RETURN round(u.score) AS rounded, ceil(u.score) AS ceil_score, floor(u.score) AS floor_score ORDER BY rounded, ceil_score, floor_score",
            "MATCH (u:User) RETURN round(-3.2) AS value ORDER BY value",
            "MATCH (u:User) RETURN ceil(-3.2) AS value ORDER BY value",
            "MATCH (u:User) RETURN floor(-3.2) AS value ORDER BY value",
            "MATCH (u:User) RETURN sqrt(u.score) AS sqrt_score, exp(u.score) AS exp_score, sin(u.score) AS sin_score, cos(u.score) AS cos_score, tan(u.score) AS tan_score ORDER BY sqrt_score, exp_score, sin_score, cos_score, tan_score",
            "MATCH (u:User) RETURN sqrt(4) AS value ORDER BY value",
            "MATCH (u:User) RETURN exp(-3.2) AS value ORDER BY value",
            "MATCH (u:User) RETURN sin(-3.2) AS value ORDER BY value",
            "MATCH (u:User) RETURN cos(-3.2) AS value ORDER BY value",
            "MATCH (u:User) RETURN tan(-3.2) AS value ORDER BY value",
            "MATCH (u:User) RETURN ln(u.score) AS ln_score, log(u.score) AS log_score, log10(u.score) AS log10_score, radians(u.score) AS rad_score, degrees(u.score) AS deg_score ORDER BY ln_score, log_score, log10_score, rad_score, deg_score",
            "MATCH (u:User) RETURN asin(0.5) AS value ORDER BY value",
            "MATCH (u:User) RETURN acos(0.5) AS value ORDER BY value",
            "MATCH (u:User) RETURN atan(-0.5) AS value ORDER BY value",
            "MATCH (u:User) RETURN ln(0.5) AS value ORDER BY value",
            "MATCH (u:User) RETURN log(0.5) AS value ORDER BY value",
            "MATCH (u:User) RETURN radians(180) AS value ORDER BY value",
            "MATCH (u:User) RETURN degrees(3.14159) AS value ORDER BY value",
            "MATCH (u:User) RETURN log10(0.5) AS value ORDER BY value",
            "MATCH (u:User) RETURN toBoolean(u.active) AS is_active ORDER BY is_active",
            "MATCH (u:User) RETURN toString(-3) AS text ORDER BY text",
            "MATCH (u:User) RETURN toInteger(-3.2) AS age_int ORDER BY age_int",
            "MATCH (u:User) RETURN toFloat(-3) AS age_float ORDER BY age_float",
            "MATCH (u:User) RETURN toBoolean(true) AS is_active ORDER BY is_active",
            "MATCH (u:User) RETURN u.age >= 18 AS adult, u.name CONTAINS 'i' AS has_i, u.name STARTS WITH 'Al' AS has_prefix, u.name ENDS WITH 'ce' AS has_suffix ORDER BY adult, has_i, has_prefix, has_suffix",
            "MATCH (u:User) RETURN id(u) AS uid ORDER BY uid",
            "MATCH (u:User) WHERE u.age = 30 RETURN u.name ORDER BY u.name",
            "MATCH (u:User) RETURN u.age AS age ORDER BY age ASC",
            "MATCH (a:User)-[r:KNOWS]->(b:User) RETURN type(r) AS rel_type ORDER BY rel_type",
            "MATCH (a:User)-[r:KNOWS]->(b:User) RETURN lower(r.note) AS lower_note, upper(r.note) AS upper_note, trim(r.note) AS trimmed_note, ltrim(r.note) AS left_trimmed_note, rtrim(r.note) AS right_trimmed_note, reverse(r.note) AS reversed_note, coalesce(r.note, 'unknown') AS display_note, replace(r.note, 'A', 'B') AS replaced_note, left(r.note, 2) AS prefix, right(r.note, 2) AS suffix, split(r.note, ' ') AS parts, substring(r.note, 1) AS tail ORDER BY lower_note, upper_note, trimmed_note, left_trimmed_note, right_trimmed_note, reversed_note, display_note, replaced_note, prefix, suffix, parts, tail",
            "MATCH (a:User)-[r:KNOWS]->(b:User) RETURN abs(r.weight) AS magnitude, sign(r.weight) AS weight_sign, round(r.score) AS rounded_score, ceil(r.score) AS ceil_score, floor(r.score) AS floor_score, sqrt(r.score) AS sqrt_score, exp(r.score) AS exp_score, sin(r.score) AS sin_score, toString(r.weight) AS weight_text, toInteger(r.score) AS score_int, toFloat(r.weight) AS weight_float, toBoolean(r.active) AS active_bool ORDER BY magnitude, weight_sign, rounded_score, ceil_score, floor_score, sqrt_score, exp_score, sin_score, weight_text, score_int, weight_float, active_bool",
            "MATCH (a:User)-[r:KNOWS]->(b:User) RETURN r.weight >= 1 AS heavy, r.note CONTAINS 'a' AS has_a, r.note STARTS WITH 'Al' AS has_prefix, r.note ENDS WITH 'ce' AS has_suffix, size(r.note) >= 3 AS long_note ORDER BY heavy, has_a, has_prefix, has_suffix, long_note",
            "MATCH (u:User) RETURN u.name IS NULL AS missing_name, size(u.name) IS NOT NULL AS has_len ORDER BY missing_name, has_len",
            "MATCH (a:User)-[r:KNOWS]->(b:User) RETURN r.note IS NULL AS missing_note, size(r.note) IS NOT NULL AS has_len ORDER BY missing_note, has_len",
            "MATCH (a:User)-[r:KNOWS]->(b:User) RETURN lower(r.note) AS lower_note, size(r.note) AS note_len, abs(r.weight) AS weight_abs, toBoolean(r.active) AS rel_active ORDER BY lower_note, note_len, weight_abs, rel_active",
            "MATCH (a:User)-[r:KNOWS]->(b:User) RETURN round(r.score) AS rounded_score, sqrt(r.score) AS sqrt_score, toFloat(r.weight) AS weight_float, toInteger(r.score) AS score_int ORDER BY rounded_score, sqrt_score, weight_float, score_int",
            "MATCH (a:User)-[r:KNOWS]->(b:User) WHERE r.note IS NOT NULL RETURN r.note AS note, r.weight >= 1 AS heavy ORDER BY note, heavy",
            "MATCH (a:User)-[:KNOWS*1..2]->(b:User) WHERE a.name = 'Alice' RETURN b.name AS friend ORDER BY friend",
        )

        for query_spec in queries:
            query, params = self._unpack_query_spec(query_spec)
            with self.subTest(query=query, params=params):
                self.assertEqual(
                    self._execute_sqlite(query, params),
                    self._execute_duckdb(query, params),
                )

    def _unpack_query_spec(
        self,
        query_spec: str | tuple[str, dict[str, object]],
    ) -> tuple[str, dict[str, object]]:
        if isinstance(query_spec, tuple):
            return query_spec

        return query_spec, {}

    def _seed_graphs(self) -> None:
        node_rows = [
            (1, '{"name":"Alice","age":30,"score":1.2,"active":true}'),
            (2, '{"name":"Bob","age":25,"score":2.8,"active":false}'),
            (3, '{"name":"Alice","age":22,"score":3.0,"active":true}'),
            (4, '{"name":"Cara","age":4,"score":4.4,"active":false}'),
        ]
        label_rows = [(1, "User"), (2, "User"), (3, "User"), (4, "User")]
        edge_rows = [
            (10, "KNOWS", 1, 2, '{"note":"Alice met","weight":1.5,"score":2.2,"active":true}'),
            (11, "KNOWS", 2, 4, '{"note":"coworker","weight":0.5,"score":3.7,"active":false}'),
            (12, "KNOWS", 3, 2, '{"note":"friend","weight":2.0,"score":1.1,"active":true}'),
        ]

        self.sqlite.executemany(
            "INSERT INTO nodes (id, properties) VALUES (?, ?)",
            node_rows,
        )
        self.sqlite.executemany(
            "INSERT INTO node_labels (node_id, label) VALUES (?, ?)",
            label_rows,
        )
        self.sqlite.executemany(
            "INSERT INTO edges (id, type, from_id, to_id, properties) VALUES (?, ?, ?, ?, ?)",
            edge_rows,
        )
        self.sqlite.commit()

        for row in node_rows:
            self.duckdb.execute("INSERT INTO nodes VALUES (?, ?)", row)
        for row in label_rows:
            self.duckdb.execute("INSERT INTO node_labels VALUES (?, ?)", row)
        for row in edge_rows:
            self.duckdb.execute("INSERT INTO edges VALUES (?, ?, ?, ?, ?)", row)

    def _execute_sqlite(
        self,
        query: str,
        params: dict[str, object],
    ) -> list[tuple[object, ...]]:
        sql = cypherglot.to_sql(query)
        return self._stabilize_rows(
            query,
            self._normalize_rows(self.sqlite.execute(sql, params).fetchall()),
        )

    def _execute_duckdb(
        self,
        query: str,
        params: dict[str, object],
    ) -> list[tuple[object, ...]]:
        sql = cypherglot.to_sql(query, dialect="duckdb")
        return self._stabilize_rows(
            query,
            self._normalize_rows(self.duckdb.execute(sql, params).fetchall()),
        )

    def _stabilize_rows(
        self,
        query: str,
        rows: list[tuple[object, ...]],
    ) -> list[tuple[object, ...]]:
        if query in {
            "MATCH (u:User) RETURN u AS user, count(u) AS total ORDER BY total DESC",
            "MATCH (a:User)-[r:KNOWS]->(b:User) RETURN r AS rel, count(r) AS total ORDER BY total DESC",
            "MATCH (a:User)-[r:KNOWS]->(b:User) RETURN r AS rel, count(*) AS total ORDER BY total DESC",
            "OPTIONAL MATCH (u:User) RETURN u.name AS name, count(u) AS total ORDER BY total DESC",
            "OPTIONAL MATCH (u:User) RETURN u.name AS name, count(*) AS total ORDER BY total DESC",
            "OPTIONAL MATCH (u:User) RETURN u AS user, count(u) AS total ORDER BY total DESC",
        }:
            return sorted(rows, key=lambda row: json.dumps(row, sort_keys=True))

        return rows

    def _normalize_rows(
        self,
        rows: list[tuple[object, ...]] | tuple[tuple[object, ...], ...],
    ) -> list[tuple[object, ...]]:
        return [tuple(self._normalize_value(value) for value in row) for row in rows]

    def _normalize_value(self, value: object) -> object:
        if isinstance(value, dict):
            return {key: self._normalize_value(item) for key, item in value.items()}
        if isinstance(value, list):
            return [self._normalize_value(item) for item in value]
        if isinstance(value, str):
            stripped = value.strip()
            if stripped.startswith("{") or stripped.startswith("["):
                return self._normalize_value(json.loads(stripped))
            if stripped.startswith('"') and stripped.endswith('"'):
                return self._normalize_value(json.loads(stripped))
            if stripped in {"true", "false", "null"}:
                return self._normalize_value(json.loads(stripped))
            if stripped and stripped[0] in "-0123456789":
                try:
                    return self._normalize_value(json.loads(stripped))
                except json.JSONDecodeError:
                    pass
            return value
        if isinstance(value, float) and value.is_integer():
            return int(value)
        if isinstance(value, float):
            return round(value, 12)
        return value

    @staticmethod
    def _sqlite_reverse(value: object) -> object:
        if value is None:
            return None
        return str(value)[::-1]

    @staticmethod
    def _sqlite_left(value: object, count: object) -> object:
        if value is None or count is None:
            return None
        return str(value)[: int(count)]

    @staticmethod
    def _sqlite_right(value: object, count: object) -> object:
        if value is None or count is None:
            return None
        count_int = int(count)
        if count_int <= 0:
            return ""
        return str(value)[-count_int:]

    @staticmethod
    def _sqlite_split(value: object, delimiter: object) -> object:
        if value is None or delimiter is None:
            return None
        return json.dumps(str(value).split(str(delimiter)))

    @staticmethod
    def _sqlite_str_position(haystack: object, needle: object) -> object:
        if haystack is None or needle is None:
            return None
        return str(haystack).find(str(needle)) + 1