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

    def test_compile_program_create_node_requires_type_aware_schema_context(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            "requires an explicit type-aware CompilerSchemaContext",
        ):
            cypherglot.compile_cypher_program_text(
                "CREATE (:User {name: 'Alice'})",
            )

    def test_compile_program_create_node(self) -> None:
        program = cypherglot.compile_cypher_program_text(
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
        )

        self.assertEqual(len(program.steps), 1)
        self.assertIsInstance(program.steps[0], cypherglot.CompiledCypherStatement)
        self.assertEqual(
            program.steps[0].sql.sql(),
            "INSERT INTO cg_node_user (name) VALUES ('Alice') RETURNING id",
        )
        self.assertEqual(program.steps[0].bind_columns, ("created_node_id",))

    def test_compile_program_create_relationship(self) -> None:
        program = cypherglot.compile_cypher_program_text(
            "CREATE (a:User {name: 'Alice'})"
            "-[r:KNOWS {since: 2020}]->"
            "(b:User {name: 'Bob'})",
            schema_context=CompilerSchemaContext.type_aware(
                GraphSchema(
                    node_types=(
                        NodeTypeSpec(
                            name="User",
                            properties=(PropertyField("name", "string"),),
                        ),
                    ),
                    edge_types=(
                        cypherglot.EdgeTypeSpec(
                            name="KNOWS",
                            source_type="User",
                            target_type="User",
                            properties=(PropertyField("since", "integer"),),
                        ),
                    ),
                )
            ),
        )

        self.assertEqual(len(program.steps), 3)
        self.assertEqual(
            program.steps[0].sql.sql(),
            "INSERT INTO cg_node_user (name) VALUES ('Alice') RETURNING id",
        )
        self.assertEqual(program.steps[0].bind_columns, ("left_node_id",))
        self.assertEqual(
            program.steps[1].sql.sql(),
            "INSERT INTO cg_node_user (name) VALUES ('Bob') RETURNING id",
        )
        self.assertEqual(program.steps[1].bind_columns, ("right_node_id",))
        self.assertEqual(
            program.steps[2].sql.sql(),
            "INSERT INTO cg_edge_knows (from_id, to_id, since) VALUES (:left_node_id, :right_node_id, 2020)",
        )

    def test_compile_program_create_relationship_self_loop(self) -> None:
        program = cypherglot.compile_cypher_program_text(
            "CREATE (root:Root)-[:LINK]->(root:Root)",
            schema_context=CompilerSchemaContext.type_aware(
                GraphSchema(
                    node_types=(NodeTypeSpec(name="Root"),),
                    edge_types=(
                        cypherglot.EdgeTypeSpec(
                            name="LINK",
                            source_type="Root",
                            target_type="Root",
                        ),
                    ),
                )
            ),
        )

        self.assertEqual(len(program.steps), 2)
        self.assertEqual(program.steps[0].bind_columns, ("left_node_id",))
        self.assertEqual(
            program.steps[0].sql.sql(),
            "INSERT INTO cg_node_root DEFAULT VALUES RETURNING id",
        )
        self.assertEqual(
            program.steps[1].sql.sql(),
            "INSERT INTO cg_edge_link (from_id, to_id) VALUES (:left_node_id, :left_node_id)",
        )

    def test_compile_program_create_relationship_from_separate_patterns(self) -> None:
        program = cypherglot.compile_cypher_program_text(
            "CREATE (a:A {name: 'Alice'}), (b:B {name: 'Bob'}), (a:A)-[:R]->(b:B)",
            schema_context=CompilerSchemaContext.type_aware(
                GraphSchema(
                    node_types=(
                        NodeTypeSpec(
                            name="A",
                            properties=(PropertyField("name", "string"),),
                        ),
                        NodeTypeSpec(
                            name="B",
                            properties=(PropertyField("name", "string"),),
                        ),
                    ),
                    edge_types=(
                        cypherglot.EdgeTypeSpec(
                            name="R",
                            source_type="A",
                            target_type="B",
                        ),
                    ),
                )
            ),
        )

        self.assertEqual(len(program.steps), 3)
        self.assertEqual(program.steps[0].bind_columns, ("first_node_id",))
        self.assertEqual(program.steps[1].bind_columns, ("second_node_id",))
        self.assertEqual(
            program.steps[0].sql.sql(),
            "INSERT INTO cg_node_a (name) VALUES ('Alice') RETURNING id",
        )
        self.assertEqual(
            program.steps[1].sql.sql(),
            "INSERT INTO cg_node_b (name) VALUES ('Bob') RETURNING id",
        )
        self.assertEqual(
            program.steps[2].sql.sql(),
            "INSERT INTO cg_edge_r (from_id, to_id) VALUES (:first_node_id, :second_node_id)",
        )

    def test_compile_rejects_merge_program_from_single_statement_api(self) -> None:
        with self.assertRaisesRegex(ValueError, "multi-step SQL program"):
            cypherglot.compile_cypher_text(
                "MERGE (:User {name: 'Alice'})-[:WORKS_AT {since: 2020}]->(:Company {name: 'Acme'})",
                schema_context=_public_api_schema_context(),
            )

    def test_compile_program_merge_node(self) -> None:
        program = cypherglot.compile_cypher_program_text(
            "MERGE (u:User {name: 'Alice'})",
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

        self.assertEqual(len(program.steps), 1)
        stmt = program.steps[0]
        self.assertIsInstance(stmt, cypherglot.CompiledCypherStatement)
        assert isinstance(stmt, cypherglot.CompiledCypherStatement)

        self.assertEqual(
            stmt.sql.sql(),
            "INSERT INTO cg_node_user (name) SELECT 'Alice' FROM (SELECT 1) AS merge_guard WHERE NOT EXISTS(SELECT 1 FROM cg_node_user AS u WHERE u.name = 'Alice' LIMIT 1)",
        )
        self.assertEqual(stmt.bind_columns, ())

    def test_compile_program_merge_relationship(self) -> None:
        program = cypherglot.compile_cypher_program_text(
            "MERGE (:User {name: 'Alice'})-[:WORKS_AT {since: 2020}]->(:Company {name: 'Acme'})",
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

        self.assertEqual(len(program.steps), 3)
        self.assertIsInstance(program.steps[0], cypherglot.CompiledCypherStatement)
        self.assertIsInstance(program.steps[1], cypherglot.CompiledCypherStatement)
        self.assertIsInstance(program.steps[2], cypherglot.CompiledCypherStatement)
        self.assertEqual(
            program.steps[0].sql.sql(),
            "INSERT INTO cg_node_user (name) SELECT 'Alice' FROM (SELECT 1) AS merge_guard WHERE NOT EXISTS(SELECT 1 FROM cg_node_user AS __humem_merge_left_node WHERE __humem_merge_left_node.name = 'Alice' LIMIT 1)",
        )
        self.assertEqual(
            program.steps[1].sql.sql(),
            "INSERT INTO cg_node_company (name) SELECT 'Acme' FROM (SELECT 1) AS merge_guard WHERE NOT EXISTS(SELECT 1 FROM cg_node_company AS __humem_merge_right_node WHERE __humem_merge_right_node.name = 'Acme' LIMIT 1)",
        )
        self.assertEqual(
            program.steps[2].sql.sql(),
            "INSERT INTO cg_edge_works_at (from_id, to_id, since) SELECT __humem_merge_left_node.id, __humem_merge_right_node.id, 2020 FROM cg_node_user AS __humem_merge_left_node, cg_node_company AS __humem_merge_right_node WHERE __humem_merge_left_node.name = 'Alice' AND __humem_merge_right_node.name = 'Acme' AND NOT EXISTS(SELECT 1 FROM cg_edge_works_at AS existing_merge_edge WHERE existing_merge_edge.from_id = __humem_merge_left_node.id AND existing_merge_edge.to_id = __humem_merge_right_node.id AND existing_merge_edge.since = 2020)",
        )

    def test_compile_program_merge_relationship_self_loop(self) -> None:
        program = cypherglot.compile_cypher_program_text(
            "MERGE (u:User {name: 'Alice'})-[:KNOWS]->(u:User {name: 'Alice'})",
            schema_context=CompilerSchemaContext.type_aware(
                GraphSchema(
                    node_types=(
                        NodeTypeSpec(
                            name="User",
                            properties=(PropertyField("name", "string"),),
                        ),
                    ),
                    edge_types=(
                        cypherglot.EdgeTypeSpec(
                            name="KNOWS",
                            source_type="User",
                            target_type="User",
                        ),
                    ),
                )
            ),
        )

        self.assertEqual(len(program.steps), 2)

        merge_node = program.steps[0]
        self.assertIsInstance(merge_node, cypherglot.CompiledCypherStatement)
        self.assertEqual(
            merge_node.sql.sql(),
            "INSERT INTO cg_node_user (name) SELECT 'Alice' FROM (SELECT 1) AS merge_guard WHERE NOT EXISTS(SELECT 1 FROM cg_node_user AS u WHERE u.name = 'Alice' LIMIT 1)",
        )

        merge_edge = program.steps[1]
        self.assertIsInstance(merge_edge, cypherglot.CompiledCypherStatement)
        self.assertEqual(
            merge_edge.sql.sql(),
            "INSERT INTO cg_edge_knows (from_id, to_id) SELECT u.id, u.id FROM cg_node_user AS u WHERE u.name = 'Alice' AND NOT EXISTS(SELECT 1 FROM cg_edge_knows AS existing_merge_edge WHERE existing_merge_edge.from_id = u.id AND existing_merge_edge.to_id = u.id)",
        )

    def test_compile_traversal_self_loop_existing_endpoint_write_sql(self) -> None:
        create_sql = cypherglot.to_sql(
            "MATCH (a:User)-[r:KNOWS]->(a:User) CREATE (a)-[:KNOWS]->(a)",
            schema_context=CompilerSchemaContext.type_aware(
                GraphSchema(
                    node_types=(
                        NodeTypeSpec(
                            name="User",
                            properties=(PropertyField("name", "string"),),
                        ),
                    ),
                    edge_types=(
                        cypherglot.EdgeTypeSpec(
                            name="KNOWS",
                            source_type="User",
                            target_type="User",
                        ),
                    ),
                )
            ),
        )
        merge_sql = cypherglot.to_sql(
            "MATCH (a:User)-[r:KNOWS]->(a:User) MERGE (a)-[:KNOWS]->(a)",
            schema_context=CompilerSchemaContext.type_aware(
                GraphSchema(
                    node_types=(
                        NodeTypeSpec(
                            name="User",
                            properties=(PropertyField("name", "string"),),
                        ),
                    ),
                    edge_types=(
                        cypherglot.EdgeTypeSpec(
                            name="KNOWS",
                            source_type="User",
                            target_type="User",
                        ),
                    ),
                )
            ),
        )

        self.assertEqual(
            create_sql,
            "INSERT INTO cg_edge_knows (from_id, to_id) SELECT a.id, a.id FROM cg_edge_knows AS r JOIN cg_node_user AS a ON a.id = r.from_id WHERE r.from_id = r.to_id",
        )
        self.assertEqual(
            merge_sql,
            "INSERT INTO cg_edge_knows (from_id, to_id) SELECT a.id, a.id FROM cg_edge_knows AS r JOIN cg_node_user AS a ON a.id = r.from_id WHERE r.from_id = r.to_id AND NOT EXISTS(SELECT 1 FROM cg_edge_knows AS existing_merge_edge WHERE existing_merge_edge.from_id = a.id AND existing_merge_edge.to_id = a.id)",
        )

    def test_compile_traversal_self_loop_relationship_set_delete_sql(self) -> None:
        set_sql = cypherglot.to_sql(
            "MATCH (a:User)-[r:KNOWS]->(a:User) WHERE a.name = 'Alice' SET r.since = 2021",
            schema_context=CompilerSchemaContext.type_aware(
                GraphSchema(
                    node_types=(
                        NodeTypeSpec(
                            name="User",
                            properties=(PropertyField("name", "string"),),
                        ),
                    ),
                    edge_types=(
                        cypherglot.EdgeTypeSpec(
                            name="KNOWS",
                            source_type="User",
                            target_type="User",
                            properties=(PropertyField("since", "integer"),),
                        ),
                    ),
                )
            ),
        )
        delete_sql = cypherglot.to_sql(
            "MATCH (a:User)-[r:KNOWS]->(a:User) WHERE a.name = 'Alice' DELETE r",
            schema_context=CompilerSchemaContext.type_aware(
                GraphSchema(
                    node_types=(
                        NodeTypeSpec(
                            name="User",
                            properties=(PropertyField("name", "string"),),
                        ),
                    ),
                    edge_types=(
                        cypherglot.EdgeTypeSpec(
                            name="KNOWS",
                            source_type="User",
                            target_type="User",
                            properties=(PropertyField("since", "integer"),),
                        ),
                    ),
                )
            ),
        )

        self.assertEqual(
            set_sql,
            "UPDATE cg_edge_knows AS r SET since = 2021 FROM cg_node_user AS a WHERE a.id = r.from_id AND r.from_id = r.to_id AND a.name = 'Alice'",
        )
        self.assertEqual(
            delete_sql,
            "DELETE FROM cg_edge_knows WHERE id IN (SELECT r.id FROM cg_edge_knows AS r JOIN cg_node_user AS a ON a.id = r.from_id WHERE r.from_id = r.to_id AND a.name = 'Alice')",
        )

    def test_compile_match_relationship_set_delete_with_right_endpoint_filter(self) -> None:
        set_sql = cypherglot.to_sql(
            "MATCH (a:User)-[r:WORKS_AT]->(b:Company {name: 'Acme'}) SET r.since = 2025",
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
        delete_sql = cypherglot.to_sql(
            "MATCH (a:User)-[r:WORKS_AT]->(b:Company {name: 'Acme'}) DELETE r",
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

        self.assertEqual(
            set_sql,
            "UPDATE cg_edge_works_at AS r SET since = 2025 FROM cg_node_user AS a, cg_node_company AS b WHERE a.id = r.from_id AND b.id = r.to_id AND b.name = 'Acme'",
        )
        self.assertEqual(
            delete_sql,
            "DELETE FROM cg_edge_works_at WHERE id IN (SELECT r.id FROM cg_edge_works_at AS r JOIN cg_node_user AS a ON a.id = r.from_id JOIN cg_node_company AS b ON b.id = r.to_id WHERE b.name = 'Acme')",
        )

    def test_compile_match_relationship_set_delete_with_relationship_filter(self) -> None:
        set_sql = cypherglot.to_sql(
            "MATCH (a:User)-[r:WORKS_AT]->(b:Company) WHERE r.since = 2020 SET r.since = 2025",
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
        delete_sql = cypherglot.to_sql(
            "MATCH (a:User)-[r:WORKS_AT]->(b:Company) WHERE r.since = 2020 DELETE r",
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

        self.assertEqual(
            set_sql,
            "UPDATE cg_edge_works_at AS r SET since = 2025 FROM cg_node_user AS a, cg_node_company AS b WHERE a.id = r.from_id AND b.id = r.to_id AND r.since = 2020",
        )
        self.assertEqual(
            delete_sql,
            "DELETE FROM cg_edge_works_at WHERE id IN (SELECT r.id FROM cg_edge_works_at AS r JOIN cg_node_user AS a ON a.id = r.from_id JOIN cg_node_company AS b ON b.id = r.to_id WHERE r.since = 2020)",
        )

    def test_compile_match_relationship_set_delete_with_combined_filters(self) -> None:
        set_sql = cypherglot.to_sql(
            "MATCH (a:User)-[r:WORKS_AT]->(b:Company {name: 'Acme'}) WHERE r.since = 2020 SET r.since = 2025",
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
        delete_sql = cypherglot.to_sql(
            "MATCH (a:User)-[r:WORKS_AT]->(b:Company {name: 'Acme'}) WHERE r.since = 2020 DELETE r",
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

        self.assertEqual(
            set_sql,
            "UPDATE cg_edge_works_at AS r SET since = 2025 FROM cg_node_user AS a, cg_node_company AS b WHERE a.id = r.from_id AND b.id = r.to_id AND b.name = 'Acme' AND r.since = 2020",
        )
        self.assertEqual(
            delete_sql,
            "DELETE FROM cg_edge_works_at WHERE id IN (SELECT r.id FROM cg_edge_works_at AS r JOIN cg_node_user AS a ON a.id = r.from_id JOIN cg_node_company AS b ON b.id = r.to_id WHERE b.name = 'Acme' AND r.since = 2020)",
        )

    def test_compile_match_relationship_set_delete_with_additional_filter_combinations(
        self,
    ) -> None:
        left_and_relationship_set_sql = cypherglot.to_sql(
            "MATCH (a:User)-[r:WORKS_AT]->(b:Company) WHERE a.name = 'Alice' AND r.since = 2020 SET r.since = 2025",
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
        left_and_relationship_delete_sql = cypherglot.to_sql(
            "MATCH (a:User)-[r:WORKS_AT]->(b:Company) WHERE a.name = 'Alice' AND r.since = 2020 DELETE r",
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
        both_endpoints_set_sql = cypherglot.to_sql(
            "MATCH (a:User {name: 'Alice'})-[r:WORKS_AT]->(b:Company {name: 'Acme'}) SET r.since = 2025",
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
        both_endpoints_delete_sql = cypherglot.to_sql(
            "MATCH (a:User {name: 'Alice'})-[r:WORKS_AT]->(b:Company {name: 'Acme'}) DELETE r",
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

        self.assertEqual(
            left_and_relationship_set_sql,
            "UPDATE cg_edge_works_at AS r SET since = 2025 FROM cg_node_user AS a, cg_node_company AS b WHERE a.id = r.from_id AND b.id = r.to_id AND a.name = 'Alice' AND r.since = 2020",
        )
        self.assertEqual(
            left_and_relationship_delete_sql,
            "DELETE FROM cg_edge_works_at WHERE id IN (SELECT r.id FROM cg_edge_works_at AS r JOIN cg_node_user AS a ON a.id = r.from_id JOIN cg_node_company AS b ON b.id = r.to_id WHERE a.name = 'Alice' AND r.since = 2020)",
        )
        self.assertEqual(
            both_endpoints_set_sql,
            "UPDATE cg_edge_works_at AS r SET since = 2025 FROM cg_node_user AS a, cg_node_company AS b WHERE a.id = r.from_id AND b.id = r.to_id AND a.name = 'Alice' AND b.name = 'Acme'",
        )
        self.assertEqual(
            both_endpoints_delete_sql,
            "DELETE FROM cg_edge_works_at WHERE id IN (SELECT r.id FROM cg_edge_works_at AS r JOIN cg_node_user AS a ON a.id = r.from_id JOIN cg_node_company AS b ON b.id = r.to_id WHERE a.name = 'Alice' AND b.name = 'Acme')",
        )

    def test_compile_match_relationship_set_delete_with_all_filters(self) -> None:
        set_sql = cypherglot.to_sql(
            "MATCH (a:User {name: 'Alice'})-[r:WORKS_AT]->(b:Company {name: 'Acme'}) WHERE r.since = 2020 SET r.since = 2025",
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
        delete_sql = cypherglot.to_sql(
            "MATCH (a:User {name: 'Alice'})-[r:WORKS_AT]->(b:Company {name: 'Acme'}) WHERE r.since = 2020 DELETE r",
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

        self.assertEqual(
            set_sql,
            "UPDATE cg_edge_works_at AS r SET since = 2025 FROM cg_node_user AS a, cg_node_company AS b WHERE a.id = r.from_id AND b.id = r.to_id AND a.name = 'Alice' AND b.name = 'Acme' AND r.since = 2020",
        )
        self.assertEqual(
            delete_sql,
            "DELETE FROM cg_edge_works_at WHERE id IN (SELECT r.id FROM cg_edge_works_at AS r JOIN cg_node_user AS a ON a.id = r.from_id JOIN cg_node_company AS b ON b.id = r.to_id WHERE a.name = 'Alice' AND b.name = 'Acme' AND r.since = 2020)",
        )

