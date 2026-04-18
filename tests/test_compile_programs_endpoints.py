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
    def test_compile_program_match_create_with_new_right_endpoint(self) -> None:
        program = cypherglot.compile_cypher_program_text(
            "MATCH (x:Begin) CREATE (x)-[:TYPE]->(:End {name: 'finish'})",
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
        
            backend="sqlite",)

        self.assertEqual(len(program.steps), 1)
        loop = program.steps[0]
        self.assertIsInstance(loop, cypherglot.CompiledCypherLoop)
        assert isinstance(loop, cypherglot.CompiledCypherLoop)

        self.assertEqual(loop.row_bindings, ("match_node_id",))
        self.assertEqual(
            loop.source.sql(),
            "SELECT x.id AS match_node_id FROM cg_node_begin AS x",
        )
        self.assertEqual(len(loop.body), 2)
        self.assertEqual(
            loop.body[0].sql.sql(),
            "INSERT INTO cg_node_end (name) VALUES ('finish') RETURNING id",
        )
        self.assertEqual(loop.body[0].bind_columns, ("created_node_id",))
        self.assertEqual(
            loop.body[1].sql.sql(),
            "INSERT INTO cg_edge_type (from_id, to_id) VALUES (:match_node_id, :created_node_id)",
        )

        filtered_type_aware_program = cypherglot.compile_cypher_program_text(
            "MATCH (x:Begin {name: 'start'}) CREATE (x)-[:TYPE]->(:End {name: 'finish'})",
            schema_context=CompilerSchemaContext.type_aware(
                GraphSchema(
                    node_types=(
                        NodeTypeSpec(
                            name="Begin",
                            properties=(PropertyField("name", "string"),),
                        ),
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
        
            backend="sqlite",)

        filtered_type_aware_loop = filtered_type_aware_program.steps[0]
        assert isinstance(filtered_type_aware_loop, cypherglot.CompiledCypherLoop)
        self.assertEqual(
            filtered_type_aware_loop.source.sql(),
            "SELECT x.id AS match_node_id FROM cg_node_begin AS x WHERE x.name = 'start'",
        )

    def test_compile_program_match_merge_with_new_right_endpoint(self) -> None:
        program = cypherglot.compile_cypher_program_text(
            "MATCH (x:Begin) MERGE (x)-[:TYPE]->(:End {name: 'finish'})",
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
        
            backend="sqlite",)

        self.assertEqual(len(program.steps), 1)
        loop = program.steps[0]
        self.assertIsInstance(loop, cypherglot.CompiledCypherLoop)
        assert isinstance(loop, cypherglot.CompiledCypherLoop)
        self.assertEqual(loop.row_bindings, ("match_node_id",))
        self.assertEqual(
            loop.source.sql(),
            "SELECT x.id AS match_node_id FROM cg_node_begin AS x WHERE NOT EXISTS(SELECT 1 FROM cg_edge_type AS existing_merge_edge JOIN cg_node_end AS existing_merge_new_node ON existing_merge_new_node.id = existing_merge_edge.to_id WHERE existing_merge_edge.from_id = x.id AND existing_merge_new_node.name = 'finish' LIMIT 1)",
        )
        self.assertEqual(len(loop.body), 2)
        self.assertEqual(
            loop.body[0].sql.sql(),
            "INSERT INTO cg_node_end (name) VALUES ('finish') RETURNING id",
        )
        self.assertEqual(
            loop.body[1].sql.sql(),
            "INSERT INTO cg_edge_type (from_id, to_id) VALUES (:match_node_id, :created_node_id)",
        )

        filtered_type_aware_program = cypherglot.compile_cypher_program_text(
            "MATCH (x:Begin {name: 'start'}) MERGE (x)-[:TYPE]->(:End {name: 'finish'})",
            schema_context=CompilerSchemaContext.type_aware(
                GraphSchema(
                    node_types=(
                        NodeTypeSpec(
                            name="Begin",
                            properties=(PropertyField("name", "string"),),
                        ),
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
        
            backend="sqlite",)

        filtered_type_aware_loop = filtered_type_aware_program.steps[0]
        assert isinstance(filtered_type_aware_loop, cypherglot.CompiledCypherLoop)
        self.assertEqual(
            filtered_type_aware_loop.source.sql(),
            "SELECT x.id AS match_node_id FROM cg_node_begin AS x WHERE x.name = 'start' AND NOT EXISTS(SELECT 1 FROM cg_edge_type AS existing_merge_edge JOIN cg_node_end AS existing_merge_new_node ON existing_merge_new_node.id = existing_merge_edge.to_id WHERE existing_merge_edge.from_id = x.id AND existing_merge_new_node.name = 'finish' LIMIT 1)",
        )

    def test_compile_program_match_merge_with_new_left_endpoint(self) -> None:
        program = cypherglot.compile_cypher_program_text(
            "MATCH (x:End) MERGE (:Begin {name: 'start'})-[:TYPE]->(x)",
            schema_context=CompilerSchemaContext.type_aware(
                GraphSchema(
                    node_types=(
                        NodeTypeSpec(
                            name="Begin",
                            properties=(PropertyField("name", "string"),),
                        ),
                        NodeTypeSpec(name="End"),
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
        
            backend="sqlite",)

        loop = program.steps[0]
        assert isinstance(loop, cypherglot.CompiledCypherLoop)
        self.assertEqual(
            loop.source.sql(),
            "SELECT x.id AS match_node_id FROM cg_node_end AS x WHERE NOT EXISTS(SELECT 1 FROM cg_edge_type AS existing_merge_edge JOIN cg_node_begin AS existing_merge_new_node ON existing_merge_new_node.id = existing_merge_edge.from_id WHERE existing_merge_edge.to_id = x.id AND existing_merge_new_node.name = 'start' LIMIT 1)",
        )
        self.assertEqual(
            loop.body[1].sql.sql(),
            "INSERT INTO cg_edge_type (from_id, to_id) VALUES (:created_node_id, :match_node_id)",
        )

        filtered_type_aware_program = cypherglot.compile_cypher_program_text(
            "MATCH (x:End {name: 'finish'}) MERGE (:Begin {name: 'start'})-[:TYPE]->(x)",
            schema_context=CompilerSchemaContext.type_aware(
                GraphSchema(
                    node_types=(
                        NodeTypeSpec(
                            name="Begin",
                            properties=(PropertyField("name", "string"),),
                        ),
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
        
            backend="sqlite",)

        filtered_type_aware_loop = filtered_type_aware_program.steps[0]
        assert isinstance(filtered_type_aware_loop, cypherglot.CompiledCypherLoop)
        self.assertEqual(
            filtered_type_aware_loop.source.sql(),
            "SELECT x.id AS match_node_id FROM cg_node_end AS x WHERE x.name = 'finish' AND NOT EXISTS(SELECT 1 FROM cg_edge_type AS existing_merge_edge JOIN cg_node_begin AS existing_merge_new_node ON existing_merge_new_node.id = existing_merge_edge.from_id WHERE existing_merge_edge.to_id = x.id AND existing_merge_new_node.name = 'start' LIMIT 1)",
        )

    def test_compile_program_match_create_with_new_left_endpoint(self) -> None:
        program = cypherglot.compile_cypher_program_text(
            "MATCH (x:End) CREATE (:Begin {name: 'start'})-[:TYPE]->(x)",
            schema_context=CompilerSchemaContext.type_aware(
                GraphSchema(
                    node_types=(
                        NodeTypeSpec(
                            name="Begin",
                            properties=(PropertyField("name", "string"),),
                        ),
                        NodeTypeSpec(name="End"),
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
        
            backend="sqlite",)

        loop = program.steps[0]
        assert isinstance(loop, cypherglot.CompiledCypherLoop)
        self.assertEqual(
            loop.source.sql(),
            "SELECT x.id AS match_node_id FROM cg_node_end AS x",
        )
        self.assertEqual(
            loop.body[0].sql.sql(),
            "INSERT INTO cg_node_begin (name) VALUES ('start') RETURNING id",
        )
        self.assertEqual(
            loop.body[1].sql.sql(),
            "INSERT INTO cg_edge_type (from_id, to_id) VALUES (:created_node_id, :match_node_id)",
        )

        filtered_type_aware_program = cypherglot.compile_cypher_program_text(
            "MATCH (x:End {name: 'finish'}) CREATE (:Begin {name: 'start'})-[:TYPE]->(x)",
            schema_context=CompilerSchemaContext.type_aware(
                GraphSchema(
                    node_types=(
                        NodeTypeSpec(
                            name="Begin",
                            properties=(PropertyField("name", "string"),),
                        ),
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
        
            backend="sqlite",)

        filtered_type_aware_loop = filtered_type_aware_program.steps[0]
        assert isinstance(filtered_type_aware_loop, cypherglot.CompiledCypherLoop)
        self.assertEqual(
            filtered_type_aware_loop.source.sql(),
            "SELECT x.id AS match_node_id FROM cg_node_end AS x WHERE x.name = 'finish'",
        )

    def test_compile_rejects_non_match_statement(self) -> None:
        with self.assertRaisesRegex(ValueError, "multi-step SQL program"):
            cypherglot.compile_cypher_text(
                "CREATE (:User {name: 'Alice'})",
                schema_context=_public_api_schema_context(),
            
            backend="sqlite",)
