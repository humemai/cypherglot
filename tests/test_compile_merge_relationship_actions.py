"""Relationship MERGE ON CREATE / ON MATCH actions (redefined-v0.1, M1-E)."""

from __future__ import annotations

import sqlite3
import unittest

import cypherglot
from cypherglot import CompilerSchemaContext, GraphSchema
from cypherglot.schema import EdgeTypeSpec, NodeTypeSpec, PropertyField

_SCHEMA = GraphSchema(
    node_types=(
        NodeTypeSpec(
            name="User",
            properties=(PropertyField(name="name", logical_type="string"),),
        ),
    ),
    edge_types=(
        EdgeTypeSpec(
            name="KNOWS",
            source_type="User",
            target_type="User",
            properties=(
                PropertyField(name="since", logical_type="integer"),
                PropertyField(name="weight", logical_type="integer"),
            ),
        ),
    ),
)

_MERGE = (
    "MERGE (a:User {name: 'A'})-[r:KNOWS]->(b:User {name: 'B'}) "
    "ON CREATE SET r.since = 2020 ON MATCH SET r.weight = 5"
)


class MergeRelationshipActionTests(unittest.TestCase):
    maxDiff = None

    def setUp(self) -> None:
        self.ctx = CompilerSchemaContext.type_aware(_SCHEMA)

    def _program(self, query: str):
        return cypherglot.render_cypher_program_text(
            query, backend="sqlite", schema_context=self.ctx
        )

    def test_program_shape_update_before_guarded_insert(self) -> None:
        steps = [step.sql for step in self._program(_MERGE).steps]
        self.assertEqual(len(steps), 4)
        self.assertIn("UPDATE cg_edge_knows SET weight = 5", steps[2])
        self.assertIn("WHERE EXISTS", steps[2])
        self.assertIn("INSERT INTO cg_edge_knows (from_id, to_id, since)", steps[3])
        self.assertIn("NOT EXISTS", steps[3])

    def test_live_semantics_create_then_match(self) -> None:
        db = sqlite3.connect(":memory:")
        db.execute("CREATE TABLE cg_node_user (id INTEGER PRIMARY KEY, name TEXT)")
        db.execute(
            "CREATE TABLE cg_edge_knows (id INTEGER PRIMARY KEY, from_id INT, "
            "to_id INT, since INT, weight INT)"
        )
        program = self._program(_MERGE)

        def run() -> None:
            for step in program.steps:
                db.execute(step.sql)
            db.commit()

        run()
        self.assertEqual(
            db.execute(
                "SELECT from_id, to_id, since, weight FROM cg_edge_knows"
            ).fetchall(),
            [(1, 2, 2020, None)],
        )
        run()
        self.assertEqual(
            db.execute(
                "SELECT from_id, to_id, since, weight FROM cg_edge_knows"
            ).fetchall(),
            [(1, 2, 2020, 5)],
        )
        self.assertEqual(
            db.execute("SELECT count(*) FROM cg_node_user").fetchone()[0], 2
        )

    def test_actions_require_relationship_alias(self) -> None:
        with self.assertRaisesRegex(ValueError, "relationship alias"):
            self._program(
                "MERGE (a:User {name: 'A'})-[:KNOWS]->(b:User {name: 'B'}) "
                "ON CREATE SET r.since = 1"
            )

    def test_actions_reject_node_alias_assignments(self) -> None:
        with self.assertRaisesRegex(ValueError, "merged relationship alias"):
            self._program(
                "MERGE (a:User {name: 'A'})-[r:KNOWS]->(b:User {name: 'B'}) "
                "ON CREATE SET a.name = 'X'"
            )

    def test_self_loop_actions_rejected(self) -> None:
        with self.assertRaisesRegex(ValueError, "self-loop"):
            self._program(
                "MERGE (a:User {name: 'A'})-[r:KNOWS]->(a:User {name: 'A'}) "
                "ON CREATE SET r.since = 1"
            )


if __name__ == "__main__":
    unittest.main()
