from __future__ import annotations

import unittest

import cypherglot
from cypherglot.schema import (
    CompilerSchemaContext,
    GraphSchema,
    NodeTypeSpec,
    PropertyField,
)


def _public_api_schema_context() -> CompilerSchemaContext:
    return CompilerSchemaContext.type_aware(
        GraphSchema(
            node_types=(
                NodeTypeSpec(
                    name="User",
                    properties=(
                        PropertyField("name", "string"),
                        PropertyField("age", "integer"),
                        PropertyField("score", "float"),
                        PropertyField("active", "boolean"),
                    ),
                ),
                NodeTypeSpec(
                    name="Company",
                    properties=(PropertyField("name", "string"),),
                ),
            ),
            edge_types=(
                cypherglot.EdgeTypeSpec(
                    name="KNOWS",
                    source_type="User",
                    target_type="User",
                    properties=(
                        PropertyField("note", "string"),
                        PropertyField("weight", "float"),
                        PropertyField("score", "float"),
                        PropertyField("active", "boolean"),
                    ),
                ),
                cypherglot.EdgeTypeSpec(
                    name="WORKS_AT",
                    source_type="User",
                    target_type="Company",
                    properties=(PropertyField("since", "integer"),),
                ),
            ),
        )
    )


class RenderTests(unittest.TestCase):
    def test_to_sql_renders_type_aware_optional_match_node_scalar_return(self) -> None:
        sql = cypherglot.to_sql(
            (
                "OPTIONAL MATCH (u:User) WHERE u.name = 'Alice' "
                "RETURN u.name AS name ORDER BY name LIMIT 1"
            ),
            backend="sqlite",
            schema_context=CompilerSchemaContext.type_aware(
                GraphSchema(
                    node_types=(
                        NodeTypeSpec(
                            name="User",
                            properties=(PropertyField("name", "string"),),
                        ),
                    ),
                    edge_types=(),
                )
            ),
        )

        self.assertEqual(
            sql,
            'SELECT u.name AS "name" '
            'FROM (SELECT 1 AS __cg_seed) AS seed '
            'LEFT JOIN cg_node_user AS u ON 1 = 1 AND u.name = \'Alice\' '
            'ORDER BY u.name ASC LIMIT 1',
        )

    def test_to_sql_renders_type_aware_optional_match_node_relational_entity_return(self) -> None:
        sql = cypherglot.to_sql(
            (
                "OPTIONAL MATCH (u:User) RETURN u AS user, count(u) AS total "
                "ORDER BY total DESC"
            ),
            backend="sqlite",
            schema_context=CompilerSchemaContext.type_aware(
                GraphSchema(
                    node_types=(
                        NodeTypeSpec(
                            name="User",
                            properties=(PropertyField("name", "string"),),
                        ),
                    ),
                    edge_types=(),
                ),
            ),
        )

        self.assertEqual(
            sql,
            'SELECT u.id AS "user.id", \'User\' AS "user.label", '
            'u.name AS "user.name", COUNT(u.id) AS "total" '
            'FROM (SELECT 1 AS __cg_seed) AS seed '
            'LEFT JOIN cg_node_user AS u ON 1 = 1 '
            'GROUP BY u.id, \'User\', u.name ORDER BY "total" DESC',
        )

    def test_to_sqlglot_ast_returns_expression(self) -> None:
        expression = cypherglot.to_sqlglot_ast(
            "MATCH (u:User) RETURN u.name ORDER BY u.name LIMIT 1",
            schema_context=_public_api_schema_context(),
        
            backend="sqlite",)

        self.assertEqual(
            expression.sql(),
            'SELECT u.name AS "u.name" FROM cg_node_user AS u ORDER BY u.name ASC LIMIT 1',
        )
    def test_to_sqlglot_ast_requires_explicit_backend(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            "requires an explicit SQL backend",
        ):
            cypherglot.to_sqlglot_ast(
                "MATCH (u:User) RETURN u.name LIMIT 1",
                schema_context=_public_api_schema_context(),
            )

    def test_to_sql_renders_single_statement_shape(self) -> None:
        sql = cypherglot.to_sql(
            "MATCH (u:User) WHERE u.name = $name RETURN u.name LIMIT 1",
            backend="sqlite",
            schema_context=_public_api_schema_context(),
        )

        self.assertEqual(
            sql,
            'SELECT u.name AS "u.name" FROM cg_node_user AS u WHERE u.name = :name LIMIT 1',
        )

    def test_to_sql_requires_explicit_backend_or_dialect(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            "requires an explicit SQL dialect or backend",
        ):
            cypherglot.to_sql(
                "MATCH (u:User) RETURN u.name LIMIT 1",
                schema_context=_public_api_schema_context(),
            )

    def test_to_sql_defaults_to_type_aware_relational_entity_rendering(self) -> None:
        sql = cypherglot.to_sql(
            "MATCH (u:User) RETURN u AS user",
            backend="sqlite",
            schema_context=_public_api_schema_context(),
        )

        self.assertEqual(
            sql,
            'SELECT u.id AS "user.id", \'User\' AS "user.label", '
            'u.name AS "user.name", u.age AS "user.age", '
            'u.score AS "user.score", u.active AS "user.active" '
            'FROM cg_node_user AS u',
        )

    def test_to_sql_renders_duckdb_string_property_access(self) -> None:
        sql = cypherglot.to_sql(
            "MATCH (u:User) RETURN u.name ORDER BY u.name",
            dialect="duckdb",
            schema_context=_public_api_schema_context(),
        )

        self.assertEqual(
            sql,
            'SELECT u.name AS "u.name" FROM cg_node_user AS u ORDER BY u.name ASC NULLS FIRST',
        )

    def test_to_sql_renders_duckdb_numeric_aggregate_cast(self) -> None:
        sql = cypherglot.to_sql(
            "MATCH (u:User) RETURN sum(u.age) AS total",
            dialect="duckdb",
            schema_context=_public_api_schema_context(),
        )

        self.assertEqual(
            sql,
            'SELECT SUM(u.age) AS "total" FROM cg_node_user AS u',
        )

    def test_to_sql_renders_duckdb_numeric_where_predicate_cast(self) -> None:
        sql = cypherglot.to_sql(
            "MATCH (u:User) WHERE u.age >= 18 RETURN u.name AS name ORDER BY name",
            dialect="duckdb",
            schema_context=_public_api_schema_context(),
        )

        self.assertEqual(
            sql,
            'SELECT u.name AS "name" FROM cg_node_user AS u WHERE u.age >= 18 ORDER BY u.name ASC NULLS FIRST',
        )

    def test_to_sql_renders_duckdb_numeric_ordering_dual_keys(self) -> None:
        sql = cypherglot.to_sql(
            "MATCH (u:User) RETURN u.age AS age ORDER BY age ASC",
            dialect="duckdb",
            schema_context=_public_api_schema_context(),
        )

        self.assertEqual(
            sql,
            'SELECT u.age AS "age" FROM cg_node_user AS u ORDER BY u.age ASC NULLS FIRST',
        )

    def test_to_sql_renders_duckdb_min_over_property_with_ordered_first(self) -> None:
        sql = cypherglot.to_sql(
            "MATCH (u:User) RETURN min(u.age) AS min_age",
            dialect="duckdb",
            schema_context=_public_api_schema_context(),
        )

        self.assertEqual(
            sql,
            'SELECT MIN(u.age) AS "min_age" FROM cg_node_user AS u',
        )

    def test_to_sql_renders_duckdb_without_constant_order_by(self) -> None:
        sql = cypherglot.to_sql(
            "OPTIONAL MATCH (u:User) RETURN $value AS value ORDER BY value",
            dialect="duckdb",
            schema_context=_public_api_schema_context(),
        )

        self.assertEqual(
            sql,
            'SELECT $value AS "value" FROM (SELECT 1 AS __cg_seed) AS seed LEFT JOIN cg_node_user AS u ON 1 = 1',
        )

    def test_to_sql_renders_duckdb_without_mixed_constant_order_by(self) -> None:
        sql = cypherglot.to_sql(
            "MATCH (u:User) WITH u AS person RETURN 'tag' AS tag, $value AS value ORDER BY tag, value",
            dialect="duckdb",
            schema_context=_public_api_schema_context(),
        )

        self.assertEqual(
            sql,
            'SELECT \'tag\' AS "tag", $value AS "value" FROM (SELECT u.id AS "__cg_with_person_id", u.name AS "__cg_with_person_prop_name", u.age AS "__cg_with_person_prop_age", u.score AS "__cg_with_person_prop_score", u.active AS "__cg_with_person_prop_active" FROM cg_node_user AS u) AS with_q',
        )

    def test_to_sql_renders_duckdb_numeric_cast_on_rebound_alias(self) -> None:
        sql = cypherglot.to_sql(
            "MATCH (u:User) WITH u AS person, u.age AS age RETURN abs(person.age) AS magnitude, abs(age) AS rebound, abs(-3) AS lit ORDER BY magnitude, rebound, lit",
            dialect="duckdb",
            schema_context=_public_api_schema_context(),
        )

        self.assertEqual(
            sql,
            'SELECT ABS(with_q."__cg_with_person_prop_age") AS "magnitude", ABS(with_q."__cg_with_scalar_age") AS "rebound", ABS(-3) AS "lit" FROM (SELECT u.id AS "__cg_with_person_id", u.name AS "__cg_with_person_prop_name", u.age AS "__cg_with_person_prop_age", u.score AS "__cg_with_person_prop_score", u.active AS "__cg_with_person_prop_active", u.age AS "__cg_with_scalar_age" FROM cg_node_user AS u) AS with_q ORDER BY ABS(with_q."__cg_with_person_prop_age") ASC NULLS FIRST, ABS(with_q."__cg_with_scalar_age") ASC NULLS FIRST',
        )

    def test_to_sql_renders_duckdb_length_cast_for_id_size(self) -> None:
        sql = cypherglot.to_sql(
            "MATCH (u:User) RETURN size(id(u)) AS id_len ORDER BY id_len",
            dialect="duckdb",
            schema_context=_public_api_schema_context(),
        )

        self.assertEqual(
            sql,
            'SELECT LENGTH(CAST(u.id AS TEXT)) AS "id_len" FROM cg_node_user AS u ORDER BY LENGTH(CAST(u.id AS TEXT)) ASC NULLS FIRST',
        )

    def test_to_sql_renders_duckdb_avg_cast_on_with_scalar_binding(self) -> None:
        sql = cypherglot.to_sql(
            "MATCH (u:User) WITH u.name AS name, u.score AS score RETURN name, avg(score) AS mean ORDER BY mean DESC, name ASC",
            dialect="duckdb",
            schema_context=_public_api_schema_context(),
        )

        self.assertEqual(
            sql,
            'SELECT with_q."__cg_with_scalar_name" AS "name", AVG(with_q."__cg_with_scalar_score") AS "mean" FROM (SELECT u.name AS "__cg_with_scalar_name", u.score AS "__cg_with_scalar_score" FROM cg_node_user AS u) AS with_q GROUP BY with_q."__cg_with_scalar_name" ORDER BY "mean" DESC, with_q."__cg_with_scalar_name" ASC NULLS FIRST',
        )

    def test_to_sql_renders_duckdb_numeric_predicate_return_cast(self) -> None:
        sql = cypherglot.to_sql(
            (
                "MATCH (u:User) WITH u AS person, u.name AS name "
                "RETURN person.age >= 18 AS adult, name = 'Alice' AS is_alice "
                "ORDER BY adult, is_alice"
            ),
            dialect="duckdb",
            schema_context=_public_api_schema_context(),
        )

        self.assertEqual(
            sql,
            'SELECT with_q."__cg_with_person_prop_age" >= 18 AS "adult", with_q."__cg_with_scalar_name" = \'Alice\' AS "is_alice" FROM (SELECT u.id AS "__cg_with_person_id", u.name AS "__cg_with_person_prop_name", u.age AS "__cg_with_person_prop_age", u.score AS "__cg_with_person_prop_score", u.active AS "__cg_with_person_prop_active", u.name AS "__cg_with_scalar_name" FROM cg_node_user AS u) AS with_q ORDER BY with_q."__cg_with_person_prop_age" >= 18 ASC NULLS FIRST, with_q."__cg_with_scalar_name" = \'Alice\' ASC NULLS FIRST',
        )

    def test_to_sql_renders_duckdb_numeric_predicate_on_typed_with_scalar(self) -> None:
        sql = cypherglot.to_sql(
            "MATCH (u:User) WITH u.age AS age RETURN age >= 18 AS adult ORDER BY adult",
            dialect="duckdb",
            schema_context=_public_api_schema_context(),
        )

        self.assertEqual(
            sql,
            'SELECT with_q."__cg_with_scalar_age" >= 18 AS "adult" FROM (SELECT u.age AS "__cg_with_scalar_age" FROM cg_node_user AS u) AS with_q ORDER BY with_q."__cg_with_scalar_age" >= 18 ASC NULLS FIRST',
        )

    def test_to_sql_renders_derived_with_scalar_binding(self) -> None:
        sql = cypherglot.to_sql(
            "MATCH (u:User) WITH lower(u.name) AS lowered RETURN lowered ORDER BY lowered",
            backend="sqlite",
            schema_context=_public_api_schema_context(),
        )

        self.assertEqual(
            sql,
            'SELECT with_q."__cg_with_scalar_lowered" AS "lowered" FROM (SELECT LOWER(u.name) AS "__cg_with_scalar_lowered" FROM cg_node_user AS u) AS with_q ORDER BY with_q."__cg_with_scalar_lowered" ASC',
        )

    def test_to_sql_renders_endpoint_derived_with_scalar_binding(self) -> None:
        sql = cypherglot.to_sql(
            "MATCH (a:User)-[r:KNOWS]->(b:User) WITH startNode(r).name AS start_name, endNode(r).id AS end_id RETURN start_name, end_id ORDER BY end_id, start_name",
            backend="sqlite",
            schema_context=_public_api_schema_context(),
        )

        self.assertEqual(
            sql,
            'SELECT with_q."__cg_with_scalar_start_name" AS "start_name", with_q."__cg_with_scalar_end_id" AS "end_id" FROM (SELECT a.name AS "__cg_with_scalar_start_name", b.id AS "__cg_with_scalar_end_id" FROM cg_edge_knows AS r JOIN cg_node_user AS a ON a.id = r.from_id JOIN cg_node_user AS b ON b.id = r.to_id) AS with_q ORDER BY with_q."__cg_with_scalar_end_id" ASC, with_q."__cg_with_scalar_start_name" ASC',
        )

    def test_to_sql_renders_duckdb_derived_numeric_with_binding_predicate(self) -> None:
        sql = cypherglot.to_sql(
            "MATCH (u:User) WITH toInteger(u.score) AS score_int RETURN score_int >= 2 AS ge_two ORDER BY ge_two",
            dialect="duckdb",
            schema_context=_public_api_schema_context(),
        )

        self.assertEqual(
            sql,
            'SELECT with_q."__cg_with_scalar_score_int" >= 2 AS "ge_two" FROM (SELECT CAST(TRUNC(u.score) AS INT) AS "__cg_with_scalar_score_int" FROM cg_node_user AS u) AS with_q ORDER BY with_q."__cg_with_scalar_score_int" >= 2 ASC NULLS FIRST',
        )

    def test_to_sql_renders_with_order_by_projected_expression_without_alias(self) -> None:
        sql = cypherglot.to_sql(
            "MATCH (u:User) WITH u AS person, u.name AS name RETURN left(person.name, 2), right(name, 2) ORDER BY left(person.name, 2), right(name, 2)",
            backend="sqlite",
            schema_context=_public_api_schema_context(),
        )

        self.assertEqual(
            sql,
            'SELECT LEFT(with_q."__cg_with_person_prop_name", 2) AS "left(person.name, 2)", RIGHT(with_q."__cg_with_scalar_name", 2) AS "right(name, 2)" FROM (SELECT u.id AS "__cg_with_person_id", u.name AS "__cg_with_person_prop_name", u.age AS "__cg_with_person_prop_age", u.score AS "__cg_with_person_prop_score", u.active AS "__cg_with_person_prop_active", u.name AS "__cg_with_scalar_name" FROM cg_node_user AS u) AS with_q ORDER BY LEFT(with_q."__cg_with_person_prop_name", 2) ASC, RIGHT(with_q."__cg_with_scalar_name", 2) ASC',
        )

    def test_to_sql_renders_duckdb_truncating_integer_cast(self) -> None:
        sql = cypherglot.to_sql(
            "MATCH (a:User)-[r:KNOWS]->(b:User) RETURN toInteger(r.score) AS score_int ORDER BY score_int",
            dialect="duckdb",
            schema_context=_public_api_schema_context(),
        )

        self.assertEqual(
            sql,
            'SELECT CAST(TRUNC(r.score) AS INT) AS "score_int" FROM cg_edge_knows AS r JOIN cg_node_user AS a ON a.id = r.from_id JOIN cg_node_user AS b ON b.id = r.to_id ORDER BY CAST(TRUNC(r.score) AS INT) ASC NULLS FIRST',
        )

    def test_to_sql_renders_duckdb_with_truncating_integer_casts(self) -> None:
        sql = cypherglot.to_sql(
            (
                "MATCH (u:User) WITH u AS person, u.age AS age "
                "RETURN toInteger(person.age) AS age_int, "
                "toInteger(age) AS rebound, toInteger(-3.2) AS lit "
                "ORDER BY age_int, rebound, lit"
            ),
            dialect="duckdb",
            schema_context=_public_api_schema_context(),
        )

        self.assertEqual(
            sql,
            'SELECT CAST(TRUNC(with_q."__cg_with_person_prop_age") AS INT) AS "age_int", CAST(TRUNC(with_q."__cg_with_scalar_age") AS INT) AS "rebound", CAST(TRUNC(-3.2) AS INT) AS "lit" FROM (SELECT u.id AS "__cg_with_person_id", u.name AS "__cg_with_person_prop_name", u.age AS "__cg_with_person_prop_age", u.score AS "__cg_with_person_prop_score", u.active AS "__cg_with_person_prop_active", u.age AS "__cg_with_scalar_age" FROM cg_node_user AS u) AS with_q ORDER BY CAST(TRUNC(with_q."__cg_with_person_prop_age") AS INT) ASC NULLS FIRST, CAST(TRUNC(with_q."__cg_with_scalar_age") AS INT) ASC NULLS FIRST',
        )

    def test_to_sql_rejects_multi_step_shape(self) -> None:
        with self.assertRaisesRegex(ValueError, "multi-step SQL program"):
            cypherglot.to_sql(
                "CREATE (:User {name: 'Alice'})",
                backend="sqlite",
                schema_context=CompilerSchemaContext.type_aware(
                    GraphSchema(
                        node_types=(
                            NodeTypeSpec(
                                name="User",
                                properties=(PropertyField("name", "string"),),
                            ),
                        ),
                        edge_types=(),
                    )
                ),
            )

    def test_to_sqlglot_program_returns_program(self) -> None:
        program = cypherglot.to_sqlglot_program(
            "CREATE (:User {name: 'Alice'})",
            schema_context=CompilerSchemaContext.type_aware(
                GraphSchema(
                    node_types=(
                        NodeTypeSpec(
                            name="User",
                            properties=(PropertyField("name", "string"),),
                        ),
                    ),
                    edge_types=(),
                )
            ),
        
            backend="sqlite",)

        self.assertEqual(len(program.steps), 1)
        self.assertIsInstance(program, cypherglot.CompiledCypherProgram)

    def test_render_cypher_program_text_preserves_structure(self) -> None:
        rendered = cypherglot.render_cypher_program_text(
            "MATCH (x:Begin) CREATE (x)-[:TYPE]->(:End {name: 'finish'})",
            backend="sqlite",
            schema_context=CompilerSchemaContext.type_aware(
                GraphSchema(
                    node_types=(
                        NodeTypeSpec(name="Begin"),
                        NodeTypeSpec(
                            name="End",
                            properties=(PropertyField("name", "string"),),
                        ),
                    ),
                    edge_types=(
                        cypherglot.EdgeTypeSpec(
                            name="TYPE",
                            source_type="Begin",
                            target_type="End",
                        ),
                    ),
                )
            ),
        )
        type_aware_rendered = cypherglot.render_cypher_program_text(
            "MATCH (x:Begin) CREATE (x)-[:TYPE]->(:End {name: 'finish'})",
            backend="sqlite",
            schema_context=CompilerSchemaContext.type_aware(
                GraphSchema(
                    node_types=(
                        NodeTypeSpec(name="Begin"),
                        NodeTypeSpec(
                            name="End",
                            properties=(PropertyField("name", "string"),),
                        ),
                    ),
                    edge_types=(
                        cypherglot.EdgeTypeSpec(
                            name="TYPE",
                            source_type="Begin",
                            target_type="End",
                        ),
                    ),
                )
            ),
        )

        self.assertEqual(len(rendered.steps), 1)
        loop = rendered.steps[0]
        self.assertIsInstance(loop, cypherglot.RenderedCypherLoop)
        assert isinstance(loop, cypherglot.RenderedCypherLoop)

        self.assertEqual(loop.row_bindings, ("match_node_id",))
        self.assertEqual(loop.source, "SELECT x.id AS match_node_id FROM cg_node_begin AS x")
        self.assertEqual(loop.body[0].bind_columns, ("created_node_id",))
        self.assertEqual(
            loop.body[1].sql,
            "INSERT INTO cg_edge_type (from_id, to_id) VALUES (:match_node_id, :created_node_id)",
        )

        type_aware_loop = type_aware_rendered.steps[0]
        self.assertIsInstance(type_aware_loop, cypherglot.RenderedCypherLoop)
        assert isinstance(type_aware_loop, cypherglot.RenderedCypherLoop)
        self.assertEqual(
            type_aware_loop.source,
            "SELECT x.id AS match_node_id FROM cg_node_begin AS x",
        )
        self.assertEqual(
            type_aware_loop.body[0].sql,
            "INSERT INTO cg_node_end (name) VALUES ('finish') RETURNING id",
        )
        self.assertEqual(
            type_aware_loop.body[1].sql,
            "INSERT INTO cg_edge_type (from_id, to_id) VALUES (:match_node_id, :created_node_id)",
        )
