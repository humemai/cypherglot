from __future__ import annotations

import unittest

import cypherglot


class TestValidate(unittest.TestCase):
    def test_validate_cypher_text_rejects_syntax_errors(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            "CypherGlot frontend reported syntax errors",
        ):
            cypherglot.validate_cypher_text("MATCH (u RETURN u")

    def test_validate_cypher_text_accepts_narrow_merge(self) -> None:
        merge_node = cypherglot.validate_cypher_text("MERGE (u:User {name: 'Alice'})")
        match_merge_self_loop = cypherglot.validate_cypher_text(
            "MATCH (a:User) MERGE (a)-[:KNOWS]->(a)"
        )
        match_merge_with_new = cypherglot.validate_cypher_text(
            "MATCH (a:User) MERGE (a)-[:INTRODUCED]->(:Person {name: 'Cara'})"
        )
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
        variable_length_grouped_entity = cypherglot.validate_cypher_text(
            "MATCH (a:User)-[:KNOWS*1..2]->(b:User) RETURN b AS friend, count(b) AS total"
        )
        variable_length_grouped_scalar = cypherglot.validate_cypher_text(
            "MATCH (a:User)-[:KNOWS*1..2]->(b:User) RETURN lower(b.name) AS friend, sum(b.score) AS total"
        )

        self.assertEqual(type(merge_node).__name__, "OC_SinglePartQueryContext")
        self.assertEqual(type(match_merge_self_loop).__name__, "OC_SinglePartQueryContext")
        self.assertEqual(type(match_merge_with_new).__name__, "OC_SinglePartQueryContext")
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
        self.assertEqual(type(variable_length_grouped_entity).__name__, "OC_SinglePartQueryContext")
        self.assertEqual(type(variable_length_grouped_scalar).__name__, "OC_SinglePartQueryContext")

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
                "supports ltrim\(\.\.\.\) in the WITH subset only over admitted entity-field projections, scalar bindings, or scalar literal/parameter inputs",
            ),
            (
                "MATCH (u:User) WITH u AS person RETURN rtrim(person) AS value",
                "supports rtrim\(\.\.\.\) in the WITH subset only over admitted entity-field projections, scalar bindings, or scalar literal/parameter inputs",
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
                "supports reverse\(\.\.\.\) in the WITH subset only over admitted entity-field projections, scalar bindings, or scalar literal/parameter inputs",
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
