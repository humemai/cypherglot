from __future__ import annotations

import unittest

import cypherglot


class TestNormalize(unittest.TestCase):
    def test_normalize_cypher_text_normalizes_optional_match_predicate_returns(self) -> None:
        contains_normalized = cypherglot.normalize_cypher_text(
            "OPTIONAL MATCH (u:User) RETURN u.name CONTAINS 'a' AS has_a ORDER BY has_a"
        )
        starts_with_normalized = cypherglot.normalize_cypher_text(
            "OPTIONAL MATCH (u:User) RETURN u.name STARTS WITH 'Al' AS has_prefix ORDER BY has_prefix"
        )
        ends_with_normalized = cypherglot.normalize_cypher_text(
            "OPTIONAL MATCH (u:User) RETURN u.name ENDS WITH 'ce' AS has_suffix ORDER BY has_suffix"
        )

        self.assertEqual(type(contains_normalized).__name__, "NormalizedOptionalMatchNode")
        self.assertEqual(contains_normalized.returns[0].kind, "predicate")
        self.assertEqual(contains_normalized.returns[0].operator, "CONTAINS")
        self.assertEqual(contains_normalized.returns[0].column_name, "has_a")
        self.assertEqual(type(starts_with_normalized).__name__, "NormalizedOptionalMatchNode")
        self.assertEqual(starts_with_normalized.returns[0].kind, "predicate")
        self.assertEqual(starts_with_normalized.returns[0].operator, "STARTS WITH")
        self.assertEqual(starts_with_normalized.returns[0].column_name, "has_prefix")
        self.assertEqual(type(ends_with_normalized).__name__, "NormalizedOptionalMatchNode")
        self.assertEqual(ends_with_normalized.returns[0].kind, "predicate")
        self.assertEqual(ends_with_normalized.returns[0].operator, "ENDS WITH")
        self.assertEqual(ends_with_normalized.returns[0].column_name, "has_suffix")

    def test_normalize_cypher_text_normalizes_grouped_count_with_return(self) -> None:
        normalized = cypherglot.normalize_cypher_text(
            "MATCH (u:User) WITH u AS person, u.name AS name RETURN name, count(person) AS total ORDER BY total DESC"
        )
        star_normalized = cypherglot.normalize_cypher_text(
            "MATCH (u:User) WITH u.name AS name RETURN name, count(*) AS total ORDER BY total DESC"
        )
        relationship_normalized = cypherglot.normalize_cypher_text(
            "MATCH (a:User)-[r:KNOWS]->(b:User) WITH r AS rel RETURN rel AS edge, count(rel) AS total ORDER BY total DESC"
        )
        relationship_star_normalized = cypherglot.normalize_cypher_text(
            "MATCH (a:User)-[r:KNOWS]->(b:User) WITH r AS rel RETURN rel AS edge, count(*) AS total ORDER BY total DESC"
        )

        self.assertEqual(type(normalized).__name__, "NormalizedMatchWithReturn")
        self.assertEqual(normalized.returns[0].kind, "scalar")
        self.assertEqual(normalized.returns[0].column_name, "name")
        self.assertEqual(normalized.returns[1].kind, "count")
        self.assertEqual(normalized.returns[1].alias, "person")
        self.assertEqual(normalized.returns[1].column_name, "total")
        self.assertEqual(normalized.order_by[0].kind, "aggregate")
        self.assertEqual(normalized.order_by[0].alias, "total")

    def test_normalize_cypher_text_normalizes_grouped_numeric_aggregates_with_return(
        self,
    ) -> None:
        normalized = cypherglot.normalize_cypher_text(
            "MATCH (u:User) WITH u.name AS name, u.score AS score RETURN name, sum(score) AS total ORDER BY total DESC"
        )

        self.assertEqual(type(normalized).__name__, "NormalizedMatchWithReturn")
        self.assertEqual(normalized.returns[0].kind, "scalar")
        self.assertEqual(normalized.returns[0].column_name, "name")
        self.assertEqual(normalized.returns[1].kind, "sum")
        self.assertEqual(normalized.returns[1].alias, "score")
        self.assertEqual(normalized.returns[1].column_name, "total")
        self.assertEqual(normalized.order_by[0].kind, "aggregate")
        self.assertEqual(normalized.order_by[0].alias, "total")

    def test_normalize_cypher_text_normalizes_searched_case_with_return(self) -> None:
        normalized = cypherglot.normalize_cypher_text(
            "MATCH (u:User) WITH u.age AS age, u.name AS name RETURN CASE WHEN age >= 18 THEN name ELSE 'minor' END AS label ORDER BY label"
        )

        self.assertEqual(type(normalized).__name__, "NormalizedMatchWithReturn")
        self.assertEqual(normalized.returns[0].kind, "case")
        case_spec = normalized.returns[0].value
        self.assertEqual(type(case_spec).__name__, "WithCaseSpec")
        assert case_spec is not None
        self.assertEqual(case_spec.when_items[0].condition.kind, "predicate")
        self.assertEqual(case_spec.when_items[0].condition.alias, "age")
        self.assertIsNone(case_spec.when_items[0].condition.field)
        self.assertEqual(case_spec.when_items[0].result.kind, "scalar")
        self.assertEqual(case_spec.when_items[0].result.alias, "name")
        self.assertEqual(case_spec.else_item.kind, "scalar_value")
        self.assertEqual(case_spec.else_item.value, "minor")
        self.assertEqual(normalized.order_by[0].kind, "case")
        self.assertEqual(normalized.order_by[0].alias, "label")

    def test_normalize_cypher_text_normalizes_with_where(self) -> None:
        normalized = cypherglot.normalize_cypher_text(
            "MATCH (u:User) WITH u AS person, u.name AS name WHERE name = 'Alice' AND person.id > 1 RETURN person, name ORDER BY name"
        )

        self.assertEqual(type(normalized).__name__, "NormalizedMatchWithReturn")
        self.assertEqual(len(normalized.predicates), 2)
        self.assertEqual(normalized.predicates[0].kind, "scalar")
        self.assertEqual(normalized.predicates[0].alias, "name")
        self.assertEqual(normalized.predicates[1].kind, "field")
        self.assertEqual(normalized.predicates[1].alias, "person")
        self.assertEqual(normalized.predicates[1].field, "id")

    def test_normalize_cypher_text_normalizes_with_where_id_and_type_filters(self) -> None:
        id_normalized = cypherglot.normalize_cypher_text(
            "MATCH (u:User) WITH u AS person, u.name AS name WHERE id(person) >= 1 RETURN person, name ORDER BY name"
        )
        type_normalized = cypherglot.normalize_cypher_text(
            "MATCH (a:User)-[r:KNOWS]->(b:User) WITH a AS person, r AS rel WHERE type(rel) = 'KNOWS' RETURN person, rel"
        )

        self.assertEqual(id_normalized.predicates[0].kind, "field")
        self.assertEqual(id_normalized.predicates[0].alias, "person")
        self.assertEqual(id_normalized.predicates[0].field, "id")
        self.assertEqual(type_normalized.predicates[0].kind, "field")
        self.assertEqual(type_normalized.predicates[0].alias, "rel")
        self.assertEqual(type_normalized.predicates[0].field, "type")

    def test_normalize_cypher_text_normalizes_unwind(self) -> None:
        normalized = cypherglot.normalize_cypher_text(
            "UNWIND [1, 2, 3] AS x RETURN x AS value ORDER BY value DESC LIMIT 2"
        )

        self.assertEqual(type(normalized).__name__, "NormalizedUnwind")
        self.assertEqual(normalized.alias, "x")
        self.assertEqual(normalized.source_kind, "literal")
        self.assertEqual(normalized.source_items, (1, 2, 3))
        self.assertEqual(normalized.returns[0].column_name, "value")
        self.assertEqual(normalized.order_by[0].kind, "scalar")
        self.assertEqual(normalized.limit, 2)

    def test_normalize_cypher_text_normalizes_optional_match(self) -> None:
        normalized = cypherglot.normalize_cypher_text(
            "OPTIONAL MATCH (u:User) WHERE u.name = 'Alice' RETURN u.name ORDER BY u.name LIMIT 1"
        )

        self.assertEqual(type(normalized).__name__, "NormalizedOptionalMatchNode")
        self.assertEqual(normalized.node.alias, "u")
        self.assertEqual(normalized.node.label, "User")
        self.assertEqual(normalized.predicates[0].field, "name")
        self.assertEqual(normalized.returns[0].column_name, "u.name")
        self.assertEqual(normalized.limit, 1)

    def test_normalize_cypher_text_normalizes_optional_match_two_arg_substring(self) -> None:
        normalized = cypherglot.normalize_cypher_text(
            "OPTIONAL MATCH (u:User) RETURN substring(u.name, 1) AS suffix ORDER BY suffix"
        )

        self.assertEqual(type(normalized).__name__, "NormalizedOptionalMatchNode")
        self.assertEqual(normalized.returns[0].kind, "substring")
        self.assertEqual(normalized.returns[0].alias, "u")
        self.assertEqual(normalized.returns[0].field, "name")
        self.assertEqual(normalized.returns[0].start_value, 1)
        self.assertIsNone(normalized.returns[0].length_value)
        self.assertEqual(normalized.order_by[0].alias, "suffix")

    def test_normalize_cypher_text_normalizes_optional_match_scalar_literal_and_parameter_returns(
        self,
    ) -> None:
        literal_normalized = cypherglot.normalize_cypher_text(
            "OPTIONAL MATCH (u:User) RETURN 'tag' AS tag ORDER BY tag"
        )
        parameter_normalized = cypherglot.normalize_cypher_text(
            "OPTIONAL MATCH (u:User) RETURN $value AS value ORDER BY value"
        )

        self.assertEqual(type(literal_normalized).__name__, "NormalizedOptionalMatchNode")
        self.assertEqual(literal_normalized.returns[0].kind, "scalar")
        self.assertEqual(literal_normalized.returns[0].column_name, "tag")
        self.assertEqual(literal_normalized.order_by[0].alias, "tag")
        self.assertEqual(type(parameter_normalized).__name__, "NormalizedOptionalMatchNode")
        self.assertEqual(parameter_normalized.returns[0].kind, "scalar")
        self.assertEqual(parameter_normalized.returns[0].column_name, "value")
        self.assertEqual(parameter_normalized.order_by[0].alias, "value")

    def test_normalize_cypher_text_normalizes_aliased_optional_match(self) -> None:
        normalized = cypherglot.normalize_cypher_text(
            "OPTIONAL MATCH (u:User) RETURN u.name AS name ORDER BY name"
        )

        self.assertEqual(type(normalized).__name__, "NormalizedOptionalMatchNode")
        self.assertEqual(normalized.returns[0].column_name, "name")
        self.assertEqual(normalized.order_by[0].alias, "name")

    def test_normalize_cypher_text_normalizes_entity_optional_match_return(self) -> None:
        normalized = cypherglot.normalize_cypher_text(
            "OPTIONAL MATCH (u:User) RETURN u AS user"
        )
        ordered_normalized = cypherglot.normalize_cypher_text(
            "OPTIONAL MATCH (u:User) RETURN u AS user ORDER BY user"
        )

        self.assertEqual(type(normalized).__name__, "NormalizedOptionalMatchNode")
        self.assertIsNone(normalized.returns[0].field)
        self.assertEqual(normalized.returns[0].column_name, "user")
        self.assertEqual(type(ordered_normalized).__name__, "NormalizedOptionalMatchNode")
        self.assertEqual(ordered_normalized.order_by[0].alias, "user")
        self.assertEqual(ordered_normalized.order_by[0].field, "__value__")

    def test_normalize_cypher_text_normalizes_optional_match_count_return(
        self,
    ) -> None:
        normalized = cypherglot.normalize_cypher_text(
            "OPTIONAL MATCH (u:User) RETURN u.name AS name, count(u) AS total ORDER BY total DESC"
        )
        star_normalized = cypherglot.normalize_cypher_text(
            "OPTIONAL MATCH (u:User) RETURN u.name AS name, count(*) AS total ORDER BY total DESC"
        )

        self.assertEqual(type(normalized).__name__, "NormalizedOptionalMatchNode")
        self.assertEqual(normalized.returns[0].column_name, "name")
        self.assertEqual(normalized.returns[1].kind, "count")
        self.assertEqual(normalized.returns[1].column_name, "total")
        self.assertEqual(normalized.order_by[0].alias, "total")
        self.assertEqual(normalized.order_by[0].field, "__value__")
        self.assertEqual(type(star_normalized).__name__, "NormalizedOptionalMatchNode")
        self.assertEqual(star_normalized.returns[1].kind, "count")
        self.assertEqual(star_normalized.returns[1].alias, "*")
        self.assertEqual(star_normalized.order_by[0].alias, "total")

    def test_normalize_cypher_text_normalizes_optional_match_grouped_entity_count_return(
        self,
    ) -> None:
        normalized = cypherglot.normalize_cypher_text(
            "OPTIONAL MATCH (u:User) RETURN u AS user, count(u) AS total ORDER BY total DESC"
        )

        self.assertEqual(type(normalized).__name__, "NormalizedOptionalMatchNode")
        self.assertEqual(normalized.returns[0].kind, "entity")
        self.assertEqual(normalized.returns[0].column_name, "user")
        self.assertEqual(normalized.returns[1].kind, "count")
        self.assertEqual(normalized.returns[1].column_name, "total")
        self.assertEqual(normalized.order_by[0].alias, "total")

    def test_normalize_cypher_text_normalizes_aliased_with_returns(self) -> None:
        normalized = cypherglot.normalize_cypher_text(
            "MATCH (u:User) WITH u AS person, u.name AS name RETURN person AS user, person.name AS display_name, name AS raw_name ORDER BY display_name, raw_name"
        )

        self.assertEqual(type(normalized).__name__, "NormalizedMatchWithReturn")
        self.assertEqual(normalized.returns[0].kind, "entity")
        self.assertEqual(normalized.returns[0].column_name, "user")
        self.assertEqual(normalized.returns[1].kind, "field")
        self.assertEqual(normalized.returns[1].column_name, "display_name")
        self.assertEqual(normalized.returns[2].kind, "scalar")
        self.assertEqual(normalized.returns[2].column_name, "raw_name")
        self.assertEqual(normalized.order_by[0].kind, "field")
        self.assertEqual(normalized.order_by[0].alias, "person")
        self.assertEqual(normalized.order_by[0].field, "name")
        self.assertEqual(normalized.order_by[1].kind, "scalar")
        self.assertEqual(normalized.order_by[1].alias, "name")

    def test_normalize_cypher_text_rejects_unknown_where_alias(self) -> None:
        with self.assertRaisesRegex(ValueError, "unknown alias"):
            cypherglot.normalize_cypher_text(
                "MATCH (u:User {name: 'Alice'}) WHERE v.age = 31 RETURN u.name"
            )

    def test_normalize_cypher_text_normalizes_vector_query_nodes(self) -> None:
        normalized = cypherglot.normalize_cypher_text(
            "CALL db.index.vector.queryNodes('user_embedding_idx', 1, $query) "
            "YIELD node, score MATCH (node:User) RETURN node.id, score ORDER BY score DESC"
        )

        self.assertEqual(type(normalized).__name__, "NormalizedQueryNodesVectorSearch")
        self.assertEqual(normalized.index_name, "user_embedding_idx")
        self.assertEqual(normalized.query_param_name, "query")
        self.assertEqual(normalized.top_k, 1)
        self.assertEqual(type(normalized.candidate_query).__name__, "NormalizedMatchNode")
        self.assertEqual(normalized.candidate_query.node.alias, "node")
        self.assertEqual(normalized.return_items, ("node.id", "score"))
        self.assertEqual(normalized.order_by, (("score", "desc"),))

    def test_normalize_cypher_text_normalizes_vector_query_nodes_full_namespace(self) -> None:
        normalized = cypherglot.normalize_cypher_text(
            "CALL db.index.vector.queryNodes('user_embedding_idx', 3, $query) "
            "YIELD node, score RETURN node.id, score"
        )

        self.assertEqual(type(normalized).__name__, "NormalizedQueryNodesVectorSearch")
        self.assertEqual(normalized.top_k, 3)
        self.assertEqual(type(normalized.candidate_query).__name__, "NormalizedMatchNode")
        self.assertEqual(normalized.candidate_query.returns[0].column_name, "node.id")

    def test_normalize_cypher_text_normalizes_vector_query_nodes_yield_where(
        self,
    ) -> None:
        yield_where = cypherglot.normalize_cypher_text(
            "CALL db.index.vector.queryNodes('user_embedding_idx', 3, $query) "
            "YIELD node, score WHERE node.region = 'west' RETURN node.id, score ORDER BY score DESC"
        )

        self.assertEqual(type(yield_where).__name__, "NormalizedQueryNodesVectorSearch")
        self.assertEqual(yield_where.top_k, 3)
        self.assertEqual(type(yield_where.candidate_query).__name__, "NormalizedMatchNode")
        self.assertEqual(yield_where.candidate_query.node.alias, "node")
        self.assertEqual(yield_where.candidate_query.predicates[0].field, "region")
        self.assertEqual(yield_where.candidate_query.predicates[0].operator, "=")
        self.assertEqual(yield_where.candidate_query.predicates[0].value, "west")
        self.assertEqual(yield_where.candidate_query.returns[0].column_name, "node.id")
