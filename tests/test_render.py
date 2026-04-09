from __future__ import annotations

import unittest

import cypherglot


class RenderTests(unittest.TestCase):
    def test_to_sqlglot_ast_returns_expression(self) -> None:
        expression = cypherglot.to_sqlglot_ast(
            "MATCH (u:User) RETURN u.name ORDER BY u.name LIMIT 1"
        )

        self.assertEqual(
            expression.sql(),
            "SELECT JSON_EXTRACT(u.properties, '$.name') AS \"u.name\" "
            "FROM nodes AS u "
            "JOIN node_labels AS u_label_0 ON u_label_0.node_id = u.id "
            "AND u_label_0.label = 'User' "
            "ORDER BY JSON_EXTRACT(u.properties, '$.name') ASC "
            "LIMIT 1",
        )

    def test_to_sql_renders_single_statement_shape(self) -> None:
        sql = cypherglot.to_sql(
            "MATCH (u:User) WHERE u.name = $name RETURN u.name LIMIT 1"
        )

        self.assertEqual(
            sql,
            "SELECT JSON_EXTRACT(u.properties, '$.name') AS \"u.name\" "
            "FROM nodes AS u "
            "JOIN node_labels AS u_label_0 ON u_label_0.node_id = u.id "
            "AND u_label_0.label = 'User' "
            "WHERE JSON_EXTRACT(u.properties, '$.name') = :name "
            "LIMIT 1",
        )

    def test_to_sql_defaults_to_sqlite_json_object_rendering(self) -> None:
        sql = cypherglot.to_sql("MATCH (u:User) RETURN u AS user")

        self.assertEqual(
            sql,
            "SELECT JSON_OBJECT('id', u.id, 'label', (SELECT u_label_return.label FROM node_labels AS u_label_return WHERE u_label_return.node_id = u.id LIMIT 1), 'properties', JSON(COALESCE(u.properties, '{}'))) AS \"user\" "
            "FROM nodes AS u JOIN node_labels AS u_label_0 ON u_label_0.node_id = u.id AND u_label_0.label = 'User'",
        )

    def test_to_sql_renders_duckdb_string_property_access(self) -> None:
        sql = cypherglot.to_sql(
            "MATCH (u:User) RETURN u.name ORDER BY u.name",
            dialect="duckdb",
        )

        self.assertEqual(
            sql,
            "SELECT JSON_EXTRACT_STRING(u.properties, '$.name') AS \"u.name\" "
            "FROM nodes AS u "
            "JOIN node_labels AS u_label_0 ON u_label_0.node_id = u.id "
            "AND u_label_0.label = 'User' "
            "ORDER BY TRY_CAST(JSON_EXTRACT_STRING(u.properties, '$.name') AS DOUBLE) ASC, JSON_EXTRACT_STRING(u.properties, '$.name') ASC NULLS FIRST",
        )

    def test_to_sql_renders_duckdb_numeric_aggregate_cast(self) -> None:
        sql = cypherglot.to_sql(
            "MATCH (u:User) RETURN sum(u.age) AS total",
            dialect="duckdb",
        )

        self.assertEqual(
            sql,
            'SELECT SUM(TRY_CAST(JSON_EXTRACT_STRING(u.properties, \'$.age\') AS DOUBLE)) AS "total" '
            'FROM nodes AS u JOIN node_labels AS u_label_0 ON u_label_0.node_id = u.id AND u_label_0.label = \'User\'',
        )

    def test_to_sql_renders_duckdb_numeric_ordering_dual_keys(self) -> None:
        sql = cypherglot.to_sql(
            "MATCH (u:User) RETURN u.age AS age ORDER BY age ASC",
            dialect="duckdb",
        )

        self.assertEqual(
            sql,
            'SELECT JSON_EXTRACT_STRING(u.properties, \'$.age\') AS "age" '
            'FROM nodes AS u JOIN node_labels AS u_label_0 ON u_label_0.node_id = u.id AND u_label_0.label = \'User\' '
            'ORDER BY TRY_CAST(JSON_EXTRACT_STRING(u.properties, \'$.age\') AS DOUBLE) ASC, JSON_EXTRACT_STRING(u.properties, \'$.age\') ASC NULLS FIRST',
        )

    def test_to_sql_renders_duckdb_min_over_property_with_ordered_first(self) -> None:
        sql = cypherglot.to_sql(
            "MATCH (u:User) RETURN min(u.age) AS min_age",
            dialect="duckdb",
        )

        self.assertEqual(
            sql,
            'SELECT FIRST(JSON_EXTRACT_STRING(u.properties, \'$.age\') ORDER BY TRY_CAST(JSON_EXTRACT_STRING(u.properties, \'$.age\') AS DOUBLE) ASC, JSON_EXTRACT_STRING(u.properties, \'$.age\') ASC) AS "min_age" '
            'FROM nodes AS u JOIN node_labels AS u_label_0 ON u_label_0.node_id = u.id AND u_label_0.label = \'User\'',
        )

    def test_to_sql_renders_duckdb_without_constant_order_by(self) -> None:
        sql = cypherglot.to_sql(
            "OPTIONAL MATCH (u:User) RETURN $value AS value ORDER BY value",
            dialect="duckdb",
        )

        self.assertEqual(
            sql,
            'SELECT $value AS "value" FROM (SELECT 1 AS __cg_seed) AS seed LEFT JOIN nodes AS u ON 1 = 1 AND EXISTS(SELECT 1 FROM node_labels AS u_label_filter WHERE u_label_filter.node_id = u.id AND u_label_filter.label = \'User\')',
        )

    def test_to_sql_renders_duckdb_without_mixed_constant_order_by(self) -> None:
        sql = cypherglot.to_sql(
            "MATCH (u:User) WITH u AS person RETURN 'tag' AS tag, $value AS value ORDER BY tag, value",
            dialect="duckdb",
        )

        self.assertEqual(
            sql,
            'SELECT \'tag\' AS "tag", $value AS "value" FROM (SELECT u.id AS "__cg_with_person_id", u.properties AS "__cg_with_person_properties" FROM nodes AS u JOIN node_labels AS u_label_0 ON u_label_0.node_id = u.id AND u_label_0.label = \'User\') AS with_q',
        )

    def test_to_sql_renders_duckdb_numeric_cast_on_rebound_alias(self) -> None:
        sql = cypherglot.to_sql(
            "MATCH (u:User) WITH u AS person, u.age AS age RETURN abs(person.age) AS magnitude, abs(age) AS rebound, abs(-3) AS lit ORDER BY magnitude, rebound, lit",
            dialect="duckdb",
        )

        self.assertEqual(
            sql,
            'SELECT ABS(TRY_CAST(JSON_EXTRACT_STRING(with_q."__cg_with_person_properties", \'$.age\') AS DOUBLE)) AS "magnitude", ABS(TRY_CAST(with_q."__cg_with_scalar_age" AS DOUBLE)) AS "rebound", ABS(-3) AS "lit" FROM (SELECT u.id AS "__cg_with_person_id", u.properties AS "__cg_with_person_properties", JSON_EXTRACT_STRING(u.properties, \'$.age\') AS "__cg_with_scalar_age" FROM nodes AS u JOIN node_labels AS u_label_0 ON u_label_0.node_id = u.id AND u_label_0.label = \'User\') AS with_q ORDER BY ABS(TRY_CAST(JSON_EXTRACT_STRING(with_q."__cg_with_person_properties", \'$.age\') AS DOUBLE)) ASC NULLS FIRST, ABS(TRY_CAST(with_q."__cg_with_scalar_age" AS DOUBLE)) ASC NULLS FIRST, ABS(-3) ASC NULLS FIRST',
        )

    def test_to_sql_renders_duckdb_truncating_integer_cast(self) -> None:
        sql = cypherglot.to_sql(
            "MATCH (a:User)-[r:KNOWS]->(b:User) RETURN toInteger(r.score) AS score_int ORDER BY score_int",
            dialect="duckdb",
        )

        self.assertEqual(
            sql,
            'SELECT CAST(TRUNC(TRY_CAST(JSON_EXTRACT_STRING(r.properties, \'$.score\') AS DOUBLE)) AS INT) AS "score_int" FROM edges AS r JOIN nodes AS a ON a.id = r.from_id JOIN nodes AS b ON b.id = r.to_id JOIN node_labels AS a_label_0 ON a_label_0.node_id = a.id AND a_label_0.label = \'User\' JOIN node_labels AS b_label_1 ON b_label_1.node_id = b.id AND b_label_1.label = \'User\' WHERE r.type = \'KNOWS\' ORDER BY CAST(TRUNC(TRY_CAST(JSON_EXTRACT_STRING(r.properties, \'$.score\') AS DOUBLE)) AS INT) ASC NULLS FIRST',
        )

    def test_to_sql_rejects_multi_step_shape(self) -> None:
        with self.assertRaisesRegex(ValueError, "multi-step SQL program"):
            cypherglot.to_sql("CREATE (:User {name: 'Alice'})")

    def test_to_sqlglot_program_returns_program(self) -> None:
        program = cypherglot.to_sqlglot_program("CREATE (:User {name: 'Alice'})")

        self.assertEqual(len(program.steps), 2)
        self.assertIsInstance(program, cypherglot.CompiledCypherProgram)

    def test_render_cypher_program_text_preserves_structure(self) -> None:
        rendered = cypherglot.render_cypher_program_text(
            "MATCH (x:Begin) CREATE (x)-[:TYPE]->(:End {name: 'finish'})"
        )

        self.assertEqual(len(rendered.steps), 1)
        loop = rendered.steps[0]
        self.assertIsInstance(loop, cypherglot.RenderedCypherLoop)
        assert isinstance(loop, cypherglot.RenderedCypherLoop)

        self.assertEqual(loop.row_bindings, ("match_node_id",))
        self.assertEqual(
            loop.source,
            "SELECT x.id AS match_node_id FROM nodes AS x "
            "WHERE EXISTS(SELECT 1 FROM node_labels AS x_label_filter_0 "
            "WHERE x_label_filter_0.node_id = x.id "
            "AND x_label_filter_0.label = 'Begin')",
        )
        self.assertEqual(loop.body[0].bind_columns, ("created_node_id",))
        self.assertEqual(
            loop.body[2].sql,
            "INSERT INTO edges (type, from_id, to_id, properties) VALUES "
            "('TYPE', :match_node_id, :created_node_id, '{}')",
        )
