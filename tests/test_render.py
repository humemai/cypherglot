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
