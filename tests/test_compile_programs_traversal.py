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
    def test_compile_program_traversal_match_create_with_new_right_endpoint(self) -> None:
        program = cypherglot.compile_cypher_program_text(
            "MATCH (a:User)-[r:KNOWS]->(b:User) CREATE (a)-[:INTRODUCED]->(:Person {name: 'Cara'})",
            schema_context=CompilerSchemaContext.type_aware(
                GraphSchema(
                    node_types=(
                        NodeTypeSpec(
                            name="User",
                            properties=(PropertyField("name", "string"),),
                        ),
                        NodeTypeSpec(
                            name="Person",
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
                            name="INTRODUCED",
                            source_type="User",
                            target_type="Person",
                        ),
                    ),
                )
            ),
        )
        self.assertEqual(len(program.steps), 1)
        loop = program.steps[0]
        self.assertIsInstance(loop, cypherglot.CompiledCypherLoop)
        assert isinstance(loop, cypherglot.CompiledCypherLoop)
        self.assertEqual(loop.row_bindings, ("match_node_id",))
        self.assertEqual(
            loop.source.sql(),
            "SELECT a.id AS match_node_id FROM cg_edge_knows AS r JOIN cg_node_user AS a ON a.id = r.from_id JOIN cg_node_user AS b ON b.id = r.to_id",
        )
        self.assertEqual(len(loop.body), 2)
        self.assertEqual(
            loop.body[0].sql.sql(),
            "INSERT INTO cg_node_person (name) VALUES ('Cara') RETURNING id",
        )
        self.assertEqual(
            loop.body[1].sql.sql(),
            "INSERT INTO cg_edge_introduced (from_id, to_id) VALUES (:match_node_id, :created_node_id)",
        )

        filtered_left_program = cypherglot.compile_cypher_program_text(
            "MATCH (a:User {name: 'Alice'})-[r:KNOWS]->(b:User) CREATE (a)-[:INTRODUCED]->(:Person {name: 'Dana'})",
            schema_context=CompilerSchemaContext.type_aware(
                GraphSchema(
                    node_types=(
                        NodeTypeSpec(
                            name="User",
                            properties=(PropertyField("name", "string"),),
                        ),
                        NodeTypeSpec(
                            name="Person",
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
                            name="INTRODUCED",
                            source_type="User",
                            target_type="Person",
                        ),
                    ),
                )
            ),
        )
        filtered_right_program = cypherglot.compile_cypher_program_text(
            "MATCH (a:User)-[r:KNOWS]->(b:User {name: 'Bob'}) CREATE (a)-[:INTRODUCED]->(:Person {name: 'Erin'})",
            schema_context=CompilerSchemaContext.type_aware(
                GraphSchema(
                    node_types=(
                        NodeTypeSpec(
                            name="User",
                            properties=(PropertyField("name", "string"),),
                        ),
                        NodeTypeSpec(
                            name="Person",
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
                            name="INTRODUCED",
                            source_type="User",
                            target_type="Person",
                        ),
                    ),
                )
            ),
        )

        filtered_left_loop = filtered_left_program.steps[0]
        assert isinstance(filtered_left_loop, cypherglot.CompiledCypherLoop)
        self.assertEqual(
            filtered_left_loop.source.sql(),
            "SELECT a.id AS match_node_id FROM cg_edge_knows AS r JOIN cg_node_user AS a ON a.id = r.from_id JOIN cg_node_user AS b ON b.id = r.to_id WHERE a.name = 'Alice'",
        )
        filtered_right_loop = filtered_right_program.steps[0]
        assert isinstance(filtered_right_loop, cypherglot.CompiledCypherLoop)
        self.assertEqual(
            filtered_right_loop.source.sql(),
            "SELECT a.id AS match_node_id FROM cg_edge_knows AS r JOIN cg_node_user AS a ON a.id = r.from_id JOIN cg_node_user AS b ON b.id = r.to_id WHERE b.name = 'Bob'",
        )

    def test_compile_program_traversal_self_loop_match_create_with_new_right_endpoint(
        self,
    ) -> None:
        type_aware_program = cypherglot.compile_cypher_program_text(
            "MATCH (a:User)-[r:KNOWS]->(a:User) CREATE (a)-[:INTRODUCED]->(:Person {name: 'Cara'})",
            schema_context=CompilerSchemaContext.type_aware(
                GraphSchema(
                    node_types=(
                        NodeTypeSpec(
                            name="User",
                            properties=(PropertyField("name", "string"),),
                        ),
                        NodeTypeSpec(
                            name="Person",
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
                            name="INTRODUCED",
                            source_type="User",
                            target_type="Person",
                        ),
                    ),
                )
            ),
        )

        type_aware_loop = type_aware_program.steps[0]
        self.assertIsInstance(type_aware_loop, cypherglot.CompiledCypherLoop)
        assert isinstance(type_aware_loop, cypherglot.CompiledCypherLoop)
        self.assertEqual(type_aware_loop.row_bindings, ("match_node_id",))
        self.assertEqual(
            type_aware_loop.source.sql(),
            "SELECT a.id AS match_node_id FROM cg_edge_knows AS r JOIN cg_node_user AS a ON a.id = r.from_id WHERE r.from_id = r.to_id",
        )
        self.assertEqual(len(type_aware_loop.body), 2)
        self.assertEqual(
            type_aware_loop.body[0].sql.sql(),
            "INSERT INTO cg_node_person (name) VALUES ('Cara') RETURNING id",
        )
        self.assertEqual(
            type_aware_loop.body[1].sql.sql(),
            "INSERT INTO cg_edge_introduced (from_id, to_id) VALUES (:match_node_id, :created_node_id)",
        )

        filtered_left_program = cypherglot.compile_cypher_program_text(
            "MATCH (a:User {name: 'Alice'})-[r:KNOWS]->(b:User) MERGE (a)-[:INTRODUCED]->(:Person {name: 'Dana'})",
            schema_context=CompilerSchemaContext.type_aware(
                GraphSchema(
                    node_types=(
                        NodeTypeSpec(
                            name="User",
                            properties=(PropertyField("name", "string"),),
                        ),
                        NodeTypeSpec(
                            name="Person",
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
                            name="INTRODUCED",
                            source_type="User",
                            target_type="Person",
                        ),
                    ),
                )
            ),
        )
        filtered_right_program = cypherglot.compile_cypher_program_text(
            "MATCH (a:User)-[r:KNOWS]->(b:User {name: 'Bob'}) MERGE (a)-[:INTRODUCED]->(:Person {name: 'Erin'})",
            schema_context=CompilerSchemaContext.type_aware(
                GraphSchema(
                    node_types=(
                        NodeTypeSpec(
                            name="User",
                            properties=(PropertyField("name", "string"),),
                        ),
                        NodeTypeSpec(
                            name="Person",
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
                            name="INTRODUCED",
                            source_type="User",
                            target_type="Person",
                        ),
                    ),
                )
            ),
        )

        filtered_left_loop = filtered_left_program.steps[0]
        assert isinstance(filtered_left_loop, cypherglot.CompiledCypherLoop)
        self.assertEqual(
            filtered_left_loop.source.sql(),
            "SELECT a.id AS match_node_id FROM cg_edge_knows AS r JOIN cg_node_user AS a ON a.id = r.from_id JOIN cg_node_user AS b ON b.id = r.to_id WHERE a.name = 'Alice' AND NOT EXISTS(SELECT 1 FROM cg_edge_introduced AS existing_merge_edge JOIN cg_node_person AS existing_merge_new_node ON existing_merge_new_node.id = existing_merge_edge.to_id WHERE existing_merge_edge.from_id = a.id AND existing_merge_new_node.name = 'Dana' LIMIT 1)",
        )
        filtered_right_loop = filtered_right_program.steps[0]
        assert isinstance(filtered_right_loop, cypherglot.CompiledCypherLoop)
        self.assertEqual(
            filtered_right_loop.source.sql(),
            "SELECT a.id AS match_node_id FROM cg_edge_knows AS r JOIN cg_node_user AS a ON a.id = r.from_id JOIN cg_node_user AS b ON b.id = r.to_id WHERE b.name = 'Bob' AND NOT EXISTS(SELECT 1 FROM cg_edge_introduced AS existing_merge_edge JOIN cg_node_person AS existing_merge_new_node ON existing_merge_new_node.id = existing_merge_edge.to_id WHERE existing_merge_edge.from_id = a.id AND existing_merge_new_node.name = 'Erin' LIMIT 1)",
        )

    def test_compile_program_match_create_with_new_left_endpoint(self) -> None:
        type_aware_program = cypherglot.compile_cypher_program_text(
            "MATCH (b:Company {name: 'Acme'}) CREATE (:User {name: 'Cara'})-[:WORKS_AT {since: 2024}]->(b)",
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

        self.assertEqual(len(type_aware_program.steps), 1)
        type_aware_loop = type_aware_program.steps[0]
        self.assertIsInstance(type_aware_loop, cypherglot.CompiledCypherLoop)
        assert isinstance(type_aware_loop, cypherglot.CompiledCypherLoop)
        self.assertEqual(type_aware_loop.row_bindings, ("match_node_id",))
        self.assertEqual(
            type_aware_loop.source.sql(),
            "SELECT b.id AS match_node_id FROM cg_node_company AS b WHERE b.name = 'Acme'",
        )
        self.assertEqual(len(type_aware_loop.body), 2)
        self.assertEqual(
            type_aware_loop.body[0].sql.sql(),
            "INSERT INTO cg_node_user (name) VALUES ('Cara') RETURNING id",
        )
        self.assertEqual(
            type_aware_loop.body[1].sql.sql(),
            "INSERT INTO cg_edge_works_at (from_id, to_id, since) VALUES (:created_node_id, :match_node_id, 2024)",
        )

        filtered_left_program = cypherglot.compile_cypher_program_text(
            "MATCH (a:User {name: 'Alice'})-[r:WORKS_AT]->(b:Company) CREATE (:User {name: 'Dana'})-[:WORKS_AT {since: 2025}]->(b)",
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
        filtered_right_program = cypherglot.compile_cypher_program_text(
            "MATCH (a:User)-[r:WORKS_AT]->(b:Company {name: 'Bravo'}) CREATE (:User {name: 'Erin'})-[:WORKS_AT {since: 2026}]->(b)",
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

        filtered_left_loop = filtered_left_program.steps[0]
        assert isinstance(filtered_left_loop, cypherglot.CompiledCypherLoop)
        self.assertEqual(
            filtered_left_loop.source.sql(),
            "SELECT b.id AS match_node_id FROM cg_edge_works_at AS r JOIN cg_node_user AS a ON a.id = r.from_id JOIN cg_node_company AS b ON b.id = r.to_id WHERE a.name = 'Alice'",
        )
        filtered_right_loop = filtered_right_program.steps[0]
        assert isinstance(filtered_right_loop, cypherglot.CompiledCypherLoop)
        self.assertEqual(
            filtered_right_loop.source.sql(),
            "SELECT b.id AS match_node_id FROM cg_edge_works_at AS r JOIN cg_node_user AS a ON a.id = r.from_id JOIN cg_node_company AS b ON b.id = r.to_id WHERE b.name = 'Bravo'",
        )

        relationship_filtered_program = cypherglot.compile_cypher_program_text(
            "MATCH (a:User)-[r:WORKS_AT]->(b:Company) WHERE r.since = 2020 CREATE (:User {name: 'Fiona'})-[:WORKS_AT {since: 2027}]->(b)",
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
        combined_filtered_program = cypherglot.compile_cypher_program_text(
            "MATCH (a:User {name: 'Alice'})-[r:WORKS_AT]->(b:Company) WHERE r.since = 2020 CREATE (:User {name: 'Gina'})-[:WORKS_AT {since: 2028}]->(b)",
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

        relationship_filtered_loop = relationship_filtered_program.steps[0]
        assert isinstance(relationship_filtered_loop, cypherglot.CompiledCypherLoop)
        self.assertEqual(
            relationship_filtered_loop.source.sql(),
            "SELECT b.id AS match_node_id FROM cg_edge_works_at AS r JOIN cg_node_user AS a ON a.id = r.from_id JOIN cg_node_company AS b ON b.id = r.to_id WHERE r.since = 2020",
        )
        combined_filtered_loop = combined_filtered_program.steps[0]
        assert isinstance(combined_filtered_loop, cypherglot.CompiledCypherLoop)
        self.assertEqual(
            combined_filtered_loop.source.sql(),
            "SELECT b.id AS match_node_id FROM cg_edge_works_at AS r JOIN cg_node_user AS a ON a.id = r.from_id JOIN cg_node_company AS b ON b.id = r.to_id WHERE a.name = 'Alice' AND r.since = 2020",
        )

        right_and_relationship_program = cypherglot.compile_cypher_program_text(
            "MATCH (a:User)-[r:WORKS_AT]->(b:Company {name: 'Bravo'}) WHERE r.since = 2021 CREATE (:User {name: 'Hana'})-[:WORKS_AT {since: 2029}]->(b)",
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

        right_and_relationship_loop = right_and_relationship_program.steps[0]
        assert isinstance(right_and_relationship_loop, cypherglot.CompiledCypherLoop)
        self.assertEqual(
            right_and_relationship_loop.source.sql(),
            "SELECT b.id AS match_node_id FROM cg_edge_works_at AS r JOIN cg_node_user AS a ON a.id = r.from_id JOIN cg_node_company AS b ON b.id = r.to_id WHERE b.name = 'Bravo' AND r.since = 2021",
        )

        all_filters_program = cypherglot.compile_cypher_program_text(
            "MATCH (a:User {name: 'Bob'})-[r:WORKS_AT]->(b:Company {name: 'Bravo'}) WHERE r.since = 2021 CREATE (:User {name: 'Iris'})-[:WORKS_AT {since: 2030}]->(b)",
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

        all_filters_loop = all_filters_program.steps[0]
        assert isinstance(all_filters_loop, cypherglot.CompiledCypherLoop)
        self.assertEqual(
            all_filters_loop.source.sql(),
            "SELECT b.id AS match_node_id FROM cg_edge_works_at AS r JOIN cg_node_user AS a ON a.id = r.from_id JOIN cg_node_company AS b ON b.id = r.to_id WHERE a.name = 'Bob' AND b.name = 'Bravo' AND r.since = 2021",
        )

    def test_compile_program_traversal_match_merge_with_new_right_endpoint(self) -> None:
        program = cypherglot.compile_cypher_program_text(
            "MATCH (a:User)-[r:KNOWS]->(b:User) MERGE (a)-[:INTRODUCED]->(:Person {name: 'Cara'})",
            schema_context=CompilerSchemaContext.type_aware(
                GraphSchema(
                    node_types=(
                        NodeTypeSpec(
                            name="User",
                            properties=(PropertyField("name", "string"),),
                        ),
                        NodeTypeSpec(
                            name="Person",
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
                            name="INTRODUCED",
                            source_type="User",
                            target_type="Person",
                        ),
                    ),
                )
            ),
        )
        self.assertEqual(len(program.steps), 1)
        loop = program.steps[0]
        self.assertIsInstance(loop, cypherglot.CompiledCypherLoop)
        assert isinstance(loop, cypherglot.CompiledCypherLoop)
        self.assertEqual(loop.row_bindings, ("match_node_id",))
        self.assertEqual(
            loop.source.sql(),
            "SELECT a.id AS match_node_id FROM cg_edge_knows AS r JOIN cg_node_user AS a ON a.id = r.from_id JOIN cg_node_user AS b ON b.id = r.to_id WHERE NOT EXISTS(SELECT 1 FROM cg_edge_introduced AS existing_merge_edge JOIN cg_node_person AS existing_merge_new_node ON existing_merge_new_node.id = existing_merge_edge.to_id WHERE existing_merge_edge.from_id = a.id AND existing_merge_new_node.name = 'Cara' LIMIT 1)",
        )
        self.assertEqual(len(loop.body), 2)
        self.assertEqual(
            loop.body[0].sql.sql(),
            "INSERT INTO cg_node_person (name) VALUES ('Cara') RETURNING id",
        )
        self.assertEqual(
            loop.body[1].sql.sql(),
            "INSERT INTO cg_edge_introduced (from_id, to_id) VALUES (:match_node_id, :created_node_id)",
        )

    def test_compile_program_traversal_self_loop_match_merge_with_new_right_endpoint(
        self,
    ) -> None:
        type_aware_program = cypherglot.compile_cypher_program_text(
            "MATCH (a:User)-[r:KNOWS]->(a:User) MERGE (a)-[:INTRODUCED]->(:Person {name: 'Cara'})",
            schema_context=CompilerSchemaContext.type_aware(
                GraphSchema(
                    node_types=(
                        NodeTypeSpec(
                            name="User",
                            properties=(PropertyField("name", "string"),),
                        ),
                        NodeTypeSpec(
                            name="Person",
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
                            name="INTRODUCED",
                            source_type="User",
                            target_type="Person",
                        ),
                    ),
                )
            ),
        )

        type_aware_loop = type_aware_program.steps[0]
        self.assertIsInstance(type_aware_loop, cypherglot.CompiledCypherLoop)
        assert isinstance(type_aware_loop, cypherglot.CompiledCypherLoop)
        self.assertEqual(type_aware_loop.row_bindings, ("match_node_id",))
        self.assertEqual(
            type_aware_loop.source.sql(),
            "SELECT a.id AS match_node_id FROM cg_edge_knows AS r JOIN cg_node_user AS a ON a.id = r.from_id WHERE r.from_id = r.to_id AND NOT EXISTS(SELECT 1 FROM cg_edge_introduced AS existing_merge_edge JOIN cg_node_person AS existing_merge_new_node ON existing_merge_new_node.id = existing_merge_edge.to_id WHERE existing_merge_edge.from_id = a.id AND existing_merge_new_node.name = 'Cara' LIMIT 1)",
        )
        self.assertEqual(len(type_aware_loop.body), 2)
        self.assertEqual(
            type_aware_loop.body[0].sql.sql(),
            "INSERT INTO cg_node_person (name) VALUES ('Cara') RETURNING id",
        )
        self.assertEqual(
            type_aware_loop.body[1].sql.sql(),
            "INSERT INTO cg_edge_introduced (from_id, to_id) VALUES (:match_node_id, :created_node_id)",
        )

    def test_compile_program_traversal_match_merge_with_new_left_endpoint(self) -> None:
        type_aware_program = cypherglot.compile_cypher_program_text(
            "MATCH (a:User)-[r:WORKS_AT]->(b:Company {name: 'Acme'}) MERGE (:User {name: 'Cara'})-[:WORKS_AT {since: 2024}]->(b)",
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

        type_aware_loop = type_aware_program.steps[0]
        assert isinstance(type_aware_loop, cypherglot.CompiledCypherLoop)
        self.assertEqual(
            type_aware_loop.source.sql(),
            "SELECT b.id AS match_node_id FROM cg_edge_works_at AS r JOIN cg_node_user AS a ON a.id = r.from_id JOIN cg_node_company AS b ON b.id = r.to_id WHERE b.name = 'Acme' AND NOT EXISTS(SELECT 1 FROM cg_edge_works_at AS existing_merge_edge JOIN cg_node_user AS existing_merge_new_node ON existing_merge_new_node.id = existing_merge_edge.from_id WHERE existing_merge_edge.to_id = b.id AND existing_merge_edge.since = 2024 AND existing_merge_new_node.name = 'Cara' LIMIT 1)",
        )

        filtered_left_program = cypherglot.compile_cypher_program_text(
            "MATCH (a:User {name: 'Alice'})-[r:WORKS_AT]->(b:Company) MERGE (:User {name: 'Dana'})-[:WORKS_AT {since: 2025}]->(b)",
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
        filtered_right_program = cypherglot.compile_cypher_program_text(
            "MATCH (a:User)-[r:WORKS_AT]->(b:Company {name: 'Bravo'}) MERGE (:User {name: 'Erin'})-[:WORKS_AT {since: 2026}]->(b)",
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

        filtered_left_loop = filtered_left_program.steps[0]
        assert isinstance(filtered_left_loop, cypherglot.CompiledCypherLoop)
        self.assertEqual(
            filtered_left_loop.source.sql(),
            "SELECT b.id AS match_node_id FROM cg_edge_works_at AS r JOIN cg_node_user AS a ON a.id = r.from_id JOIN cg_node_company AS b ON b.id = r.to_id WHERE a.name = 'Alice' AND NOT EXISTS(SELECT 1 FROM cg_edge_works_at AS existing_merge_edge JOIN cg_node_user AS existing_merge_new_node ON existing_merge_new_node.id = existing_merge_edge.from_id WHERE existing_merge_edge.to_id = b.id AND existing_merge_edge.since = 2025 AND existing_merge_new_node.name = 'Dana' LIMIT 1)",
        )
        filtered_right_loop = filtered_right_program.steps[0]
        assert isinstance(filtered_right_loop, cypherglot.CompiledCypherLoop)
        self.assertEqual(
            filtered_right_loop.source.sql(),
            "SELECT b.id AS match_node_id FROM cg_edge_works_at AS r JOIN cg_node_user AS a ON a.id = r.from_id JOIN cg_node_company AS b ON b.id = r.to_id WHERE b.name = 'Bravo' AND NOT EXISTS(SELECT 1 FROM cg_edge_works_at AS existing_merge_edge JOIN cg_node_user AS existing_merge_new_node ON existing_merge_new_node.id = existing_merge_edge.from_id WHERE existing_merge_edge.to_id = b.id AND existing_merge_edge.since = 2026 AND existing_merge_new_node.name = 'Erin' LIMIT 1)",
        )

        relationship_filtered_program = cypherglot.compile_cypher_program_text(
            "MATCH (a:User)-[r:WORKS_AT]->(b:Company) WHERE r.since = 2020 MERGE (:User {name: 'Fiona'})-[:WORKS_AT {since: 2027}]->(b)",
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
        combined_filtered_program = cypherglot.compile_cypher_program_text(
            "MATCH (a:User {name: 'Alice'})-[r:WORKS_AT]->(b:Company) WHERE r.since = 2020 MERGE (:User {name: 'Gina'})-[:WORKS_AT {since: 2028}]->(b)",
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

        relationship_filtered_loop = relationship_filtered_program.steps[0]
        assert isinstance(relationship_filtered_loop, cypherglot.CompiledCypherLoop)
        self.assertEqual(
            relationship_filtered_loop.source.sql(),
            "SELECT b.id AS match_node_id FROM cg_edge_works_at AS r JOIN cg_node_user AS a ON a.id = r.from_id JOIN cg_node_company AS b ON b.id = r.to_id WHERE r.since = 2020 AND NOT EXISTS(SELECT 1 FROM cg_edge_works_at AS existing_merge_edge JOIN cg_node_user AS existing_merge_new_node ON existing_merge_new_node.id = existing_merge_edge.from_id WHERE existing_merge_edge.to_id = b.id AND existing_merge_edge.since = 2027 AND existing_merge_new_node.name = 'Fiona' LIMIT 1)",
        )
        combined_filtered_loop = combined_filtered_program.steps[0]
        assert isinstance(combined_filtered_loop, cypherglot.CompiledCypherLoop)
        self.assertEqual(
            combined_filtered_loop.source.sql(),
            "SELECT b.id AS match_node_id FROM cg_edge_works_at AS r JOIN cg_node_user AS a ON a.id = r.from_id JOIN cg_node_company AS b ON b.id = r.to_id WHERE a.name = 'Alice' AND r.since = 2020 AND NOT EXISTS(SELECT 1 FROM cg_edge_works_at AS existing_merge_edge JOIN cg_node_user AS existing_merge_new_node ON existing_merge_new_node.id = existing_merge_edge.from_id WHERE existing_merge_edge.to_id = b.id AND existing_merge_edge.since = 2028 AND existing_merge_new_node.name = 'Gina' LIMIT 1)",
        )

        right_and_relationship_program = cypherglot.compile_cypher_program_text(
            "MATCH (a:User)-[r:WORKS_AT]->(b:Company {name: 'Bravo'}) WHERE r.since = 2021 MERGE (:User {name: 'Hana'})-[:WORKS_AT {since: 2029}]->(b)",
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

        right_and_relationship_loop = right_and_relationship_program.steps[0]
        assert isinstance(right_and_relationship_loop, cypherglot.CompiledCypherLoop)
        self.assertEqual(
            right_and_relationship_loop.source.sql(),
            "SELECT b.id AS match_node_id FROM cg_edge_works_at AS r JOIN cg_node_user AS a ON a.id = r.from_id JOIN cg_node_company AS b ON b.id = r.to_id WHERE b.name = 'Bravo' AND r.since = 2021 AND NOT EXISTS(SELECT 1 FROM cg_edge_works_at AS existing_merge_edge JOIN cg_node_user AS existing_merge_new_node ON existing_merge_new_node.id = existing_merge_edge.from_id WHERE existing_merge_edge.to_id = b.id AND existing_merge_edge.since = 2029 AND existing_merge_new_node.name = 'Hana' LIMIT 1)",
        )

        all_filters_program = cypherglot.compile_cypher_program_text(
            "MATCH (a:User {name: 'Bob'})-[r:WORKS_AT]->(b:Company {name: 'Bravo'}) WHERE r.since = 2021 MERGE (:User {name: 'Iris'})-[:WORKS_AT {since: 2030}]->(b)",
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

        all_filters_loop = all_filters_program.steps[0]
        assert isinstance(all_filters_loop, cypherglot.CompiledCypherLoop)
        self.assertEqual(
            all_filters_loop.source.sql(),
            "SELECT b.id AS match_node_id FROM cg_edge_works_at AS r JOIN cg_node_user AS a ON a.id = r.from_id JOIN cg_node_company AS b ON b.id = r.to_id WHERE a.name = 'Bob' AND b.name = 'Bravo' AND r.since = 2021 AND NOT EXISTS(SELECT 1 FROM cg_edge_works_at AS existing_merge_edge JOIN cg_node_user AS existing_merge_new_node ON existing_merge_new_node.id = existing_merge_edge.from_id WHERE existing_merge_edge.to_id = b.id AND existing_merge_edge.since = 2030 AND existing_merge_new_node.name = 'Iris' LIMIT 1)",
        )

    def test_compile_program_traversal_self_loop_match_merge_with_new_left_endpoint(
        self,
    ) -> None:
        type_aware_program = cypherglot.compile_cypher_program_text(
            "MATCH (a:User)-[r:KNOWS]->(a:User) MERGE (:User {name: 'Cara'})-[:KNOWS]->(a)",
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

        type_aware_loop = type_aware_program.steps[0]
        assert isinstance(type_aware_loop, cypherglot.CompiledCypherLoop)
        self.assertEqual(
            type_aware_loop.source.sql(),
            "SELECT a.id AS match_node_id FROM cg_edge_knows AS r JOIN cg_node_user AS a ON a.id = r.from_id WHERE r.from_id = r.to_id AND NOT EXISTS(SELECT 1 FROM cg_edge_knows AS existing_merge_edge JOIN cg_node_user AS existing_merge_new_node ON existing_merge_new_node.id = existing_merge_edge.from_id WHERE existing_merge_edge.to_id = a.id AND existing_merge_new_node.name = 'Cara' LIMIT 1)",
        )
        self.assertEqual(
            type_aware_loop.body[0].sql.sql(),
            "INSERT INTO cg_node_user (name) VALUES ('Cara') RETURNING id",
        )
        self.assertEqual(
            type_aware_loop.body[1].sql.sql(),
            "INSERT INTO cg_edge_knows (from_id, to_id) VALUES (:created_node_id, :match_node_id)",
        )

    def test_compile_program_traversal_match_create_with_new_left_endpoint(self) -> None:
        type_aware_program = cypherglot.compile_cypher_program_text(
            "MATCH (a:User)-[r:WORKS_AT]->(b:Company {name: 'Acme'}) CREATE (:User {name: 'Cara'})-[:WORKS_AT {since: 2024}]->(b)",
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

        type_aware_loop = type_aware_program.steps[0]
        assert isinstance(type_aware_loop, cypherglot.CompiledCypherLoop)
        self.assertEqual(
            type_aware_loop.source.sql(),
            "SELECT b.id AS match_node_id FROM cg_edge_works_at AS r JOIN cg_node_user AS a ON a.id = r.from_id JOIN cg_node_company AS b ON b.id = r.to_id WHERE b.name = 'Acme'",
        )
        self.assertEqual(len(type_aware_loop.body), 2)
        self.assertEqual(
            type_aware_loop.body[0].sql.sql(),
            "INSERT INTO cg_node_user (name) VALUES ('Cara') RETURNING id",
        )
        self.assertEqual(
            type_aware_loop.body[1].sql.sql(),
            "INSERT INTO cg_edge_works_at (from_id, to_id, since) VALUES (:created_node_id, :match_node_id, 2024)",
        )

    def test_compile_program_traversal_self_loop_match_create_with_new_left_endpoint(
        self,
    ) -> None:
        type_aware_program = cypherglot.compile_cypher_program_text(
            "MATCH (a:User)-[r:KNOWS]->(a:User) CREATE (:User {name: 'Cara'})-[:KNOWS]->(a)",
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

        type_aware_loop = type_aware_program.steps[0]
        assert isinstance(type_aware_loop, cypherglot.CompiledCypherLoop)
        self.assertEqual(
            type_aware_loop.source.sql(),
            "SELECT a.id AS match_node_id FROM cg_edge_knows AS r JOIN cg_node_user AS a ON a.id = r.from_id WHERE r.from_id = r.to_id",
        )
        self.assertEqual(
            type_aware_loop.body[0].sql.sql(),
            "INSERT INTO cg_node_user (name) VALUES ('Cara') RETURNING id",
        )
        self.assertEqual(
            type_aware_loop.body[1].sql.sql(),
            "INSERT INTO cg_edge_knows (from_id, to_id) VALUES (:created_node_id, :match_node_id)",
        )

