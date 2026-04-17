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


class CompileTests(unittest.TestCase):
    def test_compile_type_aware_relational_output_mode_expands_with_chain_endpoints(
        self,
    ) -> None:
        expression = cypherglot.compile_cypher_text(
            (
                "MATCH (a:User)-[r:KNOWS]->(b:User)-[s:WORKS_AT]->(c:Company) "
                "WITH b AS friend, c AS company, s AS rel "
                "RETURN startNode(rel) AS employee, endNode(rel) AS employer, company.name AS company_name "
                "ORDER BY company_name"
            ),
            schema_context=CompilerSchemaContext.type_aware(
                GraphSchema(
                    node_types=(
                        NodeTypeSpec(
                            name="User",
                            properties=(PropertyField("name", "string"),),
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
                        ),
                        cypherglot.EdgeTypeSpec(
                            name="WORKS_AT",
                            source_type="User",
                            target_type="Company",
                        ),
                    ),
                ),
            ),
        )

        self.assertEqual(
            expression.sql(),
            'SELECT with_q."__cg_with_friend_id" AS "employee.id", '
            '\'User\' AS "employee.label", '
            'with_q."__cg_with_friend_prop_name" AS "employee.name", '
            'with_q."__cg_with_company_id" AS "employer.id", '
            '\'Company\' AS "employer.label", '
            'with_q."__cg_with_company_prop_name" AS "employer.name", '
            'with_q."__cg_with_company_prop_name" AS "company_name" '
            'FROM (SELECT b.id AS "__cg_with_friend_id", '
            'b.name AS "__cg_with_friend_prop_name", '
            'c.id AS "__cg_with_company_id", '
            'c.name AS "__cg_with_company_prop_name", '
            's.id AS "__cg_with_rel_id", '
            's.from_id AS "__cg_with_rel_from_id", '
            's.to_id AS "__cg_with_rel_to_id" '
            'FROM cg_edge_knows AS r '
            'JOIN cg_node_user AS a ON a.id = r.from_id '
            'JOIN cg_node_user AS b ON b.id = r.to_id '
            'JOIN cg_edge_works_at AS s ON b.id = s.from_id '
            'JOIN cg_node_company AS c ON c.id = s.to_id) AS with_q '
            'ORDER BY with_q."__cg_with_company_prop_name" ASC',
        )

    def test_compile_type_aware_relational_output_mode_expands_with_chain_entities_and_properties(
        self,
    ) -> None:
        expression = cypherglot.compile_cypher_text(
            (
                "MATCH (a:User)-[r:KNOWS]->(b:User)-[s:WORKS_AT]->(c:Company) "
                "WITH b AS friend, s AS rel, c AS company "
                "RETURN friend AS employee, properties(friend) AS employee_props, "
                "rel AS job, properties(rel) AS job_props, company.name AS company_name "
                "ORDER BY company_name"
            ),
            schema_context=CompilerSchemaContext.type_aware(
                GraphSchema(
                    node_types=(
                        NodeTypeSpec(
                            name="User",
                            properties=(PropertyField("name", "string"),),
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
                        ),
                        cypherglot.EdgeTypeSpec(
                            name="WORKS_AT",
                            source_type="User",
                            target_type="Company",
                            properties=(PropertyField("since", "integer"),),
                        ),
                    ),
                ),
            ),
        )

        self.assertEqual(
            expression.sql(),
            'SELECT with_q."__cg_with_friend_id" AS "employee.id", '
            '\'User\' AS "employee.label", '
            'with_q."__cg_with_friend_prop_name" AS "employee.name", '
            'with_q."__cg_with_friend_prop_name" AS "employee_props.name", '
            'with_q."__cg_with_rel_id" AS "job.id", '
            '\'WORKS_AT\' AS "job.type", '
            'with_q."__cg_with_rel_from_id" AS "job.from_id", '
            'with_q."__cg_with_rel_to_id" AS "job.to_id", '
            'with_q."__cg_with_rel_prop_since" AS "job.since", '
            'with_q."__cg_with_rel_prop_since" AS "job_props.since", '
            'with_q."__cg_with_company_prop_name" AS "company_name" '
            'FROM (SELECT b.id AS "__cg_with_friend_id", '
            'b.name AS "__cg_with_friend_prop_name", '
            's.id AS "__cg_with_rel_id", '
            's.from_id AS "__cg_with_rel_from_id", '
            's.to_id AS "__cg_with_rel_to_id", '
            's.since AS "__cg_with_rel_prop_since", '
            'c.id AS "__cg_with_company_id", '
            'c.name AS "__cg_with_company_prop_name" '
            'FROM cg_edge_knows AS r '
            'JOIN cg_node_user AS a ON a.id = r.from_id '
            'JOIN cg_node_user AS b ON b.id = r.to_id '
            'JOIN cg_edge_works_at AS s ON b.id = s.from_id '
            'JOIN cg_node_company AS c ON c.id = s.to_id) AS with_q '
            'ORDER BY with_q."__cg_with_company_prop_name" ASC',
        )

    def test_compile_type_aware_relational_output_mode_groups_with_chain_entities_and_properties(
        self,
    ) -> None:
        expression = cypherglot.compile_cypher_text(
            (
                "MATCH (a:User)-[r:KNOWS]->(b:User)-[s:WORKS_AT]->(c:Company) "
                "WITH b AS friend, s AS rel "
                "RETURN friend AS employee, properties(friend) AS employee_props, "
                "rel AS job, properties(rel) AS job_props, count(rel) AS total "
                "ORDER BY total DESC"
            ),
            schema_context=CompilerSchemaContext.type_aware(
                GraphSchema(
                    node_types=(
                        NodeTypeSpec(
                            name="User",
                            properties=(PropertyField("name", "string"),),
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
                        ),
                        cypherglot.EdgeTypeSpec(
                            name="WORKS_AT",
                            source_type="User",
                            target_type="Company",
                            properties=(PropertyField("since", "integer"),),
                        ),
                    ),
                ),
            ),
        )

        self.assertEqual(
            expression.sql(),
            'SELECT with_q."__cg_with_friend_id" AS "employee.id", '
            '\'User\' AS "employee.label", '
            'with_q."__cg_with_friend_prop_name" AS "employee.name", '
            'with_q."__cg_with_friend_prop_name" AS "employee_props.name", '
            'with_q."__cg_with_rel_id" AS "job.id", '
            '\'WORKS_AT\' AS "job.type", '
            'with_q."__cg_with_rel_from_id" AS "job.from_id", '
            'with_q."__cg_with_rel_to_id" AS "job.to_id", '
            'with_q."__cg_with_rel_prop_since" AS "job.since", '
            'with_q."__cg_with_rel_prop_since" AS "job_props.since", '
            'COUNT(with_q."__cg_with_rel_id") AS "total" '
            'FROM (SELECT b.id AS "__cg_with_friend_id", '
            'b.name AS "__cg_with_friend_prop_name", '
            's.id AS "__cg_with_rel_id", '
            's.from_id AS "__cg_with_rel_from_id", '
            's.to_id AS "__cg_with_rel_to_id", '
            's.since AS "__cg_with_rel_prop_since" '
            'FROM cg_edge_knows AS r '
            'JOIN cg_node_user AS a ON a.id = r.from_id '
            'JOIN cg_node_user AS b ON b.id = r.to_id '
            'JOIN cg_edge_works_at AS s ON b.id = s.from_id '
            'JOIN cg_node_company AS c ON c.id = s.to_id) AS with_q '
            'GROUP BY with_q."__cg_with_friend_id", \'User\', '
            'with_q."__cg_with_friend_prop_name", '
            'with_q."__cg_with_friend_prop_name", '
            'with_q."__cg_with_rel_id", \'WORKS_AT\', '
            'with_q."__cg_with_rel_from_id", with_q."__cg_with_rel_to_id", '
            'with_q."__cg_with_rel_prop_since", '
            'with_q."__cg_with_rel_prop_since" ORDER BY "total" DESC',
        )

    def test_compile_type_aware_match_with_introspection_returns(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            "relational output mode does not yet support whole-entity or introspection returns",
        ):
            cypherglot.compile_cypher_text(
                (
                    "MATCH (a:User)-[r:WORKS_AT]->(b:Company) "
                    "WITH a AS person, r AS rel, b AS company "
                    "RETURN properties(person) AS person_props, labels(person) AS person_labels, "
                    "keys(rel) AS rel_keys, startNode(rel).name AS start_name, "
                    "endNode(rel) AS employer ORDER BY person_props, person_labels, rel_keys, start_name, employer"
                ),
                schema_context=CompilerSchemaContext.type_aware(
                    GraphSchema(
                        node_types=(
                            NodeTypeSpec(
                                name="User",
                                properties=(PropertyField("name", "string"),),
                            ),
                            NodeTypeSpec(
                                name="Company",
                                properties=(PropertyField("name", "string"),),
                            ),
                        ),
                        edge_types=(
                            cypherglot.EdgeTypeSpec(
                                name="WORKS_AT",
                                source_type="User",
                                target_type="Company",
                                properties=(PropertyField("since", "integer"),),
                            ),
                        ),
                    )
                ),
            )

    def test_compile_type_aware_match_with_start_node_requires_endpoint_binding(self) -> None:
        with self.assertRaisesRegex(ValueError, "explicit rebound endpoint node bindings"):
            cypherglot.compile_cypher_text(
                (
                    "MATCH (a:User)-[r:WORKS_AT]->(b:Company) "
                    "WITH r AS rel RETURN startNode(rel).name AS start_name"
                ),
                schema_context=CompilerSchemaContext.type_aware(
                    GraphSchema(
                        node_types=(
                            NodeTypeSpec(
                                name="User",
                                properties=(PropertyField("name", "string"),),
                            ),
                            NodeTypeSpec(name="Company"),
                        ),
                        edge_types=(
                            cypherglot.EdgeTypeSpec(
                                name="WORKS_AT",
                                source_type="User",
                                target_type="Company",
                            ),
                        ),
                    )
                ),
            )

    def test_compile_type_aware_match_with_grouped_aggregates(self) -> None:
        expression = cypherglot.compile_cypher_text(
            (
                "MATCH (u:User) WITH u.name AS name, u.age AS age "
                "RETURN name, max(age) AS top_age ORDER BY top_age DESC"
            ),
            schema_context=CompilerSchemaContext.type_aware(
                GraphSchema(
                    node_types=(
                        NodeTypeSpec(
                            name="User",
                            properties=(
                                PropertyField("name", "string"),
                                PropertyField("age", "integer"),
                            ),
                        ),
                    ),
                    edge_types=(),
                )
            ),
        )
        relationship_expression = cypherglot.compile_cypher_text(
            (
                "MATCH (a:User)-[r:WORKS_AT]->(b:Company) "
                "WITH r AS rel RETURN rel AS edge, count(rel) AS total "
                "ORDER BY total DESC"
            ),
            schema_context=CompilerSchemaContext.type_aware(
                GraphSchema(
                    node_types=(
                        NodeTypeSpec(name="User"),
                        NodeTypeSpec(name="Company"),
                    ),
                    edge_types=(
                        cypherglot.EdgeTypeSpec(
                            name="WORKS_AT",
                            source_type="User",
                            target_type="Company",
                        ),
                    ),
                )
            ),
        )

        self.assertEqual(
            expression.sql(),
            'SELECT with_q."__cg_with_scalar_name" AS "name", '
            'MAX(with_q."__cg_with_scalar_age") AS "top_age" '
            'FROM (SELECT u.name AS "__cg_with_scalar_name", '
            'u.age AS "__cg_with_scalar_age" FROM cg_node_user AS u) AS with_q '
            'GROUP BY with_q."__cg_with_scalar_name" ORDER BY "top_age" DESC',
        )
        self.assertEqual(
            relationship_expression.sql(),
            'SELECT with_q."__cg_with_rel_id" AS "edge.id", '
            '\'WORKS_AT\' AS "edge.type", '
            'with_q."__cg_with_rel_from_id" AS "edge.from_id", '
            'with_q."__cg_with_rel_to_id" AS "edge.to_id", '
            'COUNT(with_q."__cg_with_rel_id") AS "total" '
            'FROM (SELECT r.id AS "__cg_with_rel_id", '
            'r.from_id AS "__cg_with_rel_from_id", '
            'r.to_id AS "__cg_with_rel_to_id" '
            'FROM cg_edge_works_at AS r '
            'JOIN cg_node_user AS a ON a.id = r.from_id '
            'JOIN cg_node_company AS b ON b.id = r.to_id) AS with_q '
            'GROUP BY with_q."__cg_with_rel_id", \'WORKS_AT\', '
            'with_q."__cg_with_rel_from_id", with_q."__cg_with_rel_to_id" '
            'ORDER BY "total" DESC',
        )

    def test_compile_type_aware_match_with_entity_field_aggregates(self) -> None:
        node_expression = cypherglot.compile_cypher_text(
            (
                "MATCH (u:User) WITH u AS person "
                "RETURN max(person.age) AS top_age ORDER BY top_age DESC"
            ),
            schema_context=CompilerSchemaContext.type_aware(
                GraphSchema(
                    node_types=(
                        NodeTypeSpec(
                            name="User",
                            properties=(PropertyField("age", "integer"),),
                        ),
                    ),
                    edge_types=(),
                )
            ),
        )
        relationship_expression = cypherglot.compile_cypher_text(
            (
                "MATCH (a:User)-[r:WORKS_AT]->(b:Company) "
                "WITH a AS person, r AS rel "
                "RETURN person.name AS name, avg(rel.since) AS mean_since "
                "ORDER BY mean_since DESC"
            ),
            schema_context=CompilerSchemaContext.type_aware(
                GraphSchema(
                    node_types=(
                        NodeTypeSpec(
                            name="User",
                            properties=(PropertyField("name", "string"),),
                        ),
                        NodeTypeSpec(name="Company"),
                    ),
                    edge_types=(
                        cypherglot.EdgeTypeSpec(
                            name="WORKS_AT",
                            source_type="User",
                            target_type="Company",
                            properties=(PropertyField("since", "integer"),),
                        ),
                    ),
                )
            ),
        )

        self.assertEqual(
            node_expression.sql(),
            'SELECT MAX(with_q."__cg_with_person_prop_age") AS "top_age" '
            'FROM (SELECT u.id AS "__cg_with_person_id", '
            'u.age AS "__cg_with_person_prop_age" '
            'FROM cg_node_user AS u) AS with_q ORDER BY "top_age" DESC',
        )
        self.assertEqual(
            relationship_expression.sql(),
            'SELECT with_q."__cg_with_person_prop_name" AS "name", '
            'AVG(with_q."__cg_with_rel_prop_since") AS "mean_since" '
            'FROM (SELECT a.id AS "__cg_with_person_id", '
            'a.name AS "__cg_with_person_prop_name", '
            'r.id AS "__cg_with_rel_id", '
            'r.from_id AS "__cg_with_rel_from_id", '
            'r.to_id AS "__cg_with_rel_to_id", '
            'r.since AS "__cg_with_rel_prop_since" '
            'FROM cg_edge_works_at AS r '
            'JOIN cg_node_user AS a ON a.id = r.from_id '
            'JOIN cg_node_company AS b ON b.id = r.to_id) AS with_q '
            'GROUP BY with_q."__cg_with_person_prop_name" ORDER BY "mean_since" DESC',
        )

    def test_compile_type_aware_match_with_node_source_scalar_functions(self) -> None:
        expression = cypherglot.compile_cypher_text(
            (
                "MATCH (u:User) WITH u AS person, u.name AS name, u.age AS age "
                "RETURN lower(person.name) AS lower_name, size(name) AS name_len, "
                "toString(age) AS age_text, coalesce(person.name, 'unknown') AS display_name "
                "ORDER BY lower_name, name_len, age_text, display_name"
            ),
            schema_context=CompilerSchemaContext.type_aware(
                GraphSchema(
                    node_types=(
                        NodeTypeSpec(
                            name="User",
                            properties=(
                                PropertyField("name", "string"),
                                PropertyField("age", "integer"),
                            ),
                        ),
                    ),
                    edge_types=(),
                )
            ),
        )

        self.assertEqual(
            expression.sql(),
            'SELECT LOWER(with_q."__cg_with_person_prop_name") AS "lower_name", '
            'LENGTH(with_q."__cg_with_scalar_name") AS "name_len", '
            'CAST(with_q."__cg_with_scalar_age" AS TEXT) AS "age_text", '
            'COALESCE(with_q."__cg_with_person_prop_name", \'unknown\') AS "display_name" '
            'FROM (SELECT u.id AS "__cg_with_person_id", '
            'u.name AS "__cg_with_person_prop_name", '
            'u.age AS "__cg_with_person_prop_age", '
            'u.name AS "__cg_with_scalar_name", '
            'u.age AS "__cg_with_scalar_age" '
            'FROM cg_node_user AS u) AS with_q '
            'ORDER BY LOWER(with_q."__cg_with_person_prop_name") ASC, '
            'LENGTH(with_q."__cg_with_scalar_name") ASC, '
            'CAST(with_q."__cg_with_scalar_age" AS TEXT) ASC, '
            'COALESCE(with_q."__cg_with_person_prop_name", \'unknown\') ASC',
        )

    def test_compile_type_aware_match_with_relationship_source_scalar_functions(self) -> None:
        expression = cypherglot.compile_cypher_text(
            (
                "MATCH (a:User)-[r:WORKS_AT]->(b:Company) "
                "WITH r AS rel, r.note AS note, r.since AS since "
                "RETURN lower(rel.note) AS lower_note, size(type(rel)) AS type_len, "
                "toString(since) AS since_text, coalesce(note, 'unknown') AS display_note "
                "ORDER BY lower_note, type_len, since_text, display_note"
            ),
            schema_context=CompilerSchemaContext.type_aware(
                GraphSchema(
                    node_types=(
                        NodeTypeSpec(name="User"),
                        NodeTypeSpec(name="Company"),
                    ),
                    edge_types=(
                        cypherglot.EdgeTypeSpec(
                            name="WORKS_AT",
                            source_type="User",
                            target_type="Company",
                            properties=(
                                PropertyField("note", "string"),
                                PropertyField("since", "integer"),
                            ),
                        ),
                    ),
                )
            ),
        )

        self.assertEqual(
            expression.sql(),
            'SELECT LOWER(with_q."__cg_with_rel_prop_note") AS "lower_note", '
            'LENGTH(\'WORKS_AT\') AS "type_len", '
            'CAST(with_q."__cg_with_scalar_since" AS TEXT) AS "since_text", '
            'COALESCE(with_q."__cg_with_scalar_note", \'unknown\') AS "display_note" '
            'FROM (SELECT r.id AS "__cg_with_rel_id", '
            'r.from_id AS "__cg_with_rel_from_id", '
            'r.to_id AS "__cg_with_rel_to_id", '
            'r.note AS "__cg_with_rel_prop_note", '
            'r.since AS "__cg_with_rel_prop_since", '
            'r.note AS "__cg_with_scalar_note", '
            'r.since AS "__cg_with_scalar_since" '
            'FROM cg_edge_works_at AS r '
            'JOIN cg_node_user AS a ON a.id = r.from_id '
            'JOIN cg_node_company AS b ON b.id = r.to_id) AS with_q '
            'ORDER BY LOWER(with_q."__cg_with_rel_prop_note") ASC, '
            'LENGTH(\'WORKS_AT\') ASC, '
            'CAST(with_q."__cg_with_scalar_since" AS TEXT) ASC, '
            'COALESCE(with_q."__cg_with_scalar_note", \'unknown\') ASC',
        )

    def test_compile_type_aware_match_with_case_returns(self) -> None:
        node_expression = cypherglot.compile_cypher_text(
            (
                "MATCH (u:User) WITH u AS person "
                "RETURN CASE WHEN person.age >= 18 THEN person.name ELSE 'minor' END AS label "
                "ORDER BY label"
            ),
            schema_context=CompilerSchemaContext.type_aware(
                GraphSchema(
                    node_types=(
                        NodeTypeSpec(
                            name="User",
                            properties=(
                                PropertyField("name", "string"),
                                PropertyField("age", "integer"),
                            ),
                        ),
                    ),
                    edge_types=(),
                )
            ),
        )
        relationship_expression = cypherglot.compile_cypher_text(
            (
                "MATCH (a:User)-[r:WORKS_AT]->(b:Company) WITH r AS rel "
                "RETURN CASE WHEN rel.since >= 2020 THEN 'recent' ELSE 'legacy' END AS rel_class "
                "ORDER BY rel_class"
            ),
            schema_context=CompilerSchemaContext.type_aware(
                GraphSchema(
                    node_types=(
                        NodeTypeSpec(name="User"),
                        NodeTypeSpec(name="Company"),
                    ),
                    edge_types=(
                        cypherglot.EdgeTypeSpec(
                            name="WORKS_AT",
                            source_type="User",
                            target_type="Company",
                            properties=(PropertyField("since", "integer"),),
                        ),
                    ),
                )
            ),
        )

        self.assertEqual(
            node_expression.sql(),
            'SELECT CASE WHEN with_q."__cg_with_person_prop_age" >= 18 THEN '
            'with_q."__cg_with_person_prop_name" ELSE \'minor\' END AS "label" '
            'FROM (SELECT u.id AS "__cg_with_person_id", '
            'u.name AS "__cg_with_person_prop_name", '
            'u.age AS "__cg_with_person_prop_age" '
            'FROM cg_node_user AS u) AS with_q '
            'ORDER BY CASE WHEN with_q."__cg_with_person_prop_age" >= 18 THEN '
            'with_q."__cg_with_person_prop_name" ELSE \'minor\' END ASC',
        )
        self.assertEqual(
            relationship_expression.sql(),
            'SELECT CASE WHEN with_q."__cg_with_rel_prop_since" >= 2020 THEN '
            '\'recent\' ELSE \'legacy\' END AS "rel_class" '
            'FROM (SELECT r.id AS "__cg_with_rel_id", '
            'r.from_id AS "__cg_with_rel_from_id", '
            'r.to_id AS "__cg_with_rel_to_id", '
            'r.since AS "__cg_with_rel_prop_since" '
            'FROM cg_edge_works_at AS r '
            'JOIN cg_node_user AS a ON a.id = r.from_id '
            'JOIN cg_node_company AS b ON b.id = r.to_id) AS with_q '
            'ORDER BY CASE WHEN with_q."__cg_with_rel_prop_since" >= 2020 THEN '
            '\'recent\' ELSE \'legacy\' END ASC',
        )

    def test_compile_type_aware_match_with_predicate_returns(self) -> None:
        node_expression = cypherglot.compile_cypher_text(
            (
                "MATCH (u:User) WITH u AS person, u.name AS name "
                "RETURN person.age >= 18 AS adult, name = 'Alice' AS is_alice, "
                "size(name) >= 3 AS long_name, person.name IS NOT NULL AS has_name "
                "ORDER BY adult, is_alice, long_name, has_name"
            ),
            schema_context=CompilerSchemaContext.type_aware(
                GraphSchema(
                    node_types=(
                        NodeTypeSpec(
                            name="User",
                            properties=(
                                PropertyField("name", "string"),
                                PropertyField("age", "integer"),
                            ),
                        ),
                    ),
                    edge_types=(),
                )
            ),
        )
        relationship_expression = cypherglot.compile_cypher_text(
            (
                "MATCH (a:User)-[r:WORKS_AT]->(b:Company) "
                "WITH r AS rel, r.note AS note "
                "RETURN rel.note STARTS WITH 'Al' AS has_prefix, "
                "note ENDS WITH 'ce' AS has_suffix, "
                "type(rel) = 'WORKS_AT' AS rel_matches, "
                "size(rel.note) >= 3 AS long_note "
                "ORDER BY has_prefix, has_suffix, rel_matches, long_note"
            ),
            schema_context=CompilerSchemaContext.type_aware(
                GraphSchema(
                    node_types=(
                        NodeTypeSpec(name="User"),
                        NodeTypeSpec(name="Company"),
                    ),
                    edge_types=(
                        cypherglot.EdgeTypeSpec(
                            name="WORKS_AT",
                            source_type="User",
                            target_type="Company",
                            properties=(PropertyField("note", "string"),),
                        ),
                    ),
                )
            ),
        )

        self.assertEqual(
            node_expression.sql(),
            'SELECT with_q."__cg_with_person_prop_age" >= 18 AS "adult", '
            'with_q."__cg_with_scalar_name" = \'Alice\' AS "is_alice", '
            'LENGTH(with_q."__cg_with_scalar_name") >= 3 AS "long_name", '
            'NOT with_q."__cg_with_person_prop_name" IS NULL AS "has_name" '
            'FROM (SELECT u.id AS "__cg_with_person_id", '
            'u.name AS "__cg_with_person_prop_name", '
            'u.age AS "__cg_with_person_prop_age", '
            'u.name AS "__cg_with_scalar_name" '
            'FROM cg_node_user AS u) AS with_q '
            'ORDER BY with_q."__cg_with_person_prop_age" >= 18 ASC, '
            'with_q."__cg_with_scalar_name" = \'Alice\' ASC, '
            'LENGTH(with_q."__cg_with_scalar_name") >= 3 ASC, '
            'NOT with_q."__cg_with_person_prop_name" IS NULL ASC',
        )
        self.assertEqual(
            relationship_expression.sql(),
            'SELECT SUBSTRING(with_q."__cg_with_rel_prop_note", 1, LENGTH(\'Al\')) = \'Al\' AS "has_prefix", '
            'LENGTH(with_q."__cg_with_scalar_note") >= LENGTH(\'ce\') AND '
            'SUBSTRING(with_q."__cg_with_scalar_note", LENGTH(with_q."__cg_with_scalar_note") - LENGTH(\'ce\') + 1) = \'ce\' AS "has_suffix", '
            '\'WORKS_AT\' = \'WORKS_AT\' AS "rel_matches", '
            'LENGTH(with_q."__cg_with_rel_prop_note") >= 3 AS "long_note" '
            'FROM (SELECT r.id AS "__cg_with_rel_id", '
            'r.from_id AS "__cg_with_rel_from_id", '
            'r.to_id AS "__cg_with_rel_to_id", '
            'r.note AS "__cg_with_rel_prop_note", '
            'r.note AS "__cg_with_scalar_note" '
            'FROM cg_edge_works_at AS r '
            'JOIN cg_node_user AS a ON a.id = r.from_id '
            'JOIN cg_node_company AS b ON b.id = r.to_id) AS with_q '
            'ORDER BY SUBSTRING(with_q."__cg_with_rel_prop_note", 1, LENGTH(\'Al\')) = \'Al\' ASC, '
            'LENGTH(with_q."__cg_with_scalar_note") >= LENGTH(\'ce\') AND '
            'SUBSTRING(with_q."__cg_with_scalar_note", LENGTH(with_q."__cg_with_scalar_note") - LENGTH(\'ce\') + 1) = \'ce\' ASC, '
            '\'WORKS_AT\' = \'WORKS_AT\' ASC, '
            'LENGTH(with_q."__cg_with_rel_prop_note") >= 3 ASC',
        )

    def test_compile_type_aware_relational_output_mode_rejects_match_with_broader_introspection_returns(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            "relational output mode does not yet support whole-entity or introspection returns",
        ):
            cypherglot.compile_cypher_text(
                (
                    "MATCH (a:User)-[r:WORKS_AT]->(b:Company) "
                    "WITH a AS person, r AS rel, b AS company "
                    "RETURN id(person) AS person_id, type(rel) AS rel_type, "
                    "properties(rel) AS rel_props, keys(person) AS person_keys, "
                    "startNode(rel) AS start_person, endNode(rel).id AS company_id "
                    "ORDER BY person_id, rel_type, rel_props, person_keys, start_person, company_id"
                ),
                schema_context=CompilerSchemaContext.type_aware(
                    GraphSchema(
                        node_types=(
                            NodeTypeSpec(
                                name="User",
                                properties=(PropertyField("name", "string"),),
                            ),
                            NodeTypeSpec(
                                name="Company",
                                properties=(PropertyField("name", "string"),),
                            ),
                        ),
                        edge_types=(
                            cypherglot.EdgeTypeSpec(
                                name="WORKS_AT",
                                source_type="User",
                                target_type="Company",
                                properties=(PropertyField("since", "integer"),),
                            ),
                        ),
                    ),
                ),
            )

    def test_compile_type_aware_match_node_introspection_returns(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            "relational output mode does not yet support whole-entity or introspection returns",
        ):
            cypherglot.compile_cypher_text(
                "MATCH (u:User) RETURN u AS user, properties(u) AS props, labels(u) AS labels, keys(u) AS keys ORDER BY user, props, labels, keys",
                schema_context=CompilerSchemaContext.type_aware(
                    GraphSchema(
                        node_types=(
                            NodeTypeSpec(
                                name="User",
                                properties=(
                                    PropertyField("name", "string"),
                                    PropertyField("age", "integer"),
                                ),
                            ),
                        ),
                        edge_types=(),
                    )
                ),
            )

    def test_compile_type_aware_match_relationship_introspection_returns(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            "relational output mode does not yet support whole-entity or introspection returns",
        ):
            cypherglot.compile_cypher_text(
                (
                    "MATCH (a:User)-[r:WORKS_AT]->(b:Company) "
                    "RETURN r AS rel, properties(r) AS props, keys(r) AS keys, "
                    "startNode(r) AS start, endNode(r).name AS company "
                    "ORDER BY rel, props, keys, start, company"
                ),
                schema_context=CompilerSchemaContext.type_aware(
                    GraphSchema(
                        node_types=(
                            NodeTypeSpec(
                                name="User",
                                properties=(PropertyField("name", "string"),),
                            ),
                            NodeTypeSpec(
                                name="Company",
                                properties=(PropertyField("name", "string"),),
                            ),
                        ),
                        edge_types=(
                            cypherglot.EdgeTypeSpec(
                                name="WORKS_AT",
                                source_type="User",
                                target_type="Company",
                                properties=(PropertyField("since", "integer"),),
                            ),
                        ),
                    )
                ),
            )

