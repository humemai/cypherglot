"""Undirected and inbound one-hop MATCH lowering (redefined-v0.1, M1-B)."""

from __future__ import annotations

import unittest

import cypherglot
from cypherglot import CompilerSchemaContext, GraphSchema
from cypherglot.schema import EdgeTypeSpec, NodeTypeSpec, PropertyField

_SCHEMA = GraphSchema(
    node_types=(
        NodeTypeSpec(
            name="User",
            properties=(
                PropertyField(name="name", logical_type="string"),
                PropertyField(name="age", logical_type="integer"),
            ),
        ),
        NodeTypeSpec(
            name="Company",
            properties=(PropertyField(name="name", logical_type="string"),),
        ),
    ),
    edge_types=(
        EdgeTypeSpec(name="KNOWS", source_type="User", target_type="User"),
        EdgeTypeSpec(name="WORKS_AT", source_type="User", target_type="Company"),
    ),
)


class UndirectedOneHopTests(unittest.TestCase):
    maxDiff = None

    def setUp(self) -> None:
        self.ctx = CompilerSchemaContext.type_aware(_SCHEMA)

    def _sql(self, query: str) -> str:
        return cypherglot.to_sql(query, backend="sqlite", schema_context=self.ctx)

    def test_inbound_one_hop_binds_reversed_endpoint_columns(self) -> None:
        sql = self._sql(
            "MATCH (a:User)<-[r:KNOWS]-(b:User) RETURN a.name AS dst, b.name AS src"
        )
        self.assertIn("AS a ON a.id = r.to_id", sql)
        self.assertIn("AS b ON b.id = r.from_id", sql)

    def test_undirected_same_type_lowers_to_union_all_with_self_loop_guard(
        self,
    ) -> None:
        sql = self._sql(
            "MATCH (a:User)-[r:KNOWS]-(b:User) RETURN a.name AS x, b.name AS y"
        )
        self.assertIn("UNION ALL", sql)
        self.assertIn("AS undirected_q", sql)
        # out orientation
        self.assertIn("a ON a.id = r.from_id", sql)
        # reversed orientation
        self.assertIn("a ON a.id = r.to_id", sql)
        # exactly one self-loop guard, on the reversed branch
        self.assertEqual(sql.count("r.from_id <> r.to_id"), 1)

    def test_undirected_heterogeneous_collapses_to_single_orientation(self) -> None:
        sql = self._sql(
            "MATCH (c:Company)-[r:WORKS_AT]-(u:User) "
            "RETURN u.name AS person, c.name AS company"
        )
        self.assertNotIn("UNION ALL", sql)
        # only the in-orientation satisfies the contract for these labels
        self.assertIn("AS c ON c.id = r.to_id", sql)
        self.assertIn("AS u ON u.id = r.from_id", sql)

    def test_undirected_order_limit_apply_over_the_union(self) -> None:
        sql = self._sql(
            "MATCH (a:User)-[r:KNOWS]-(b:User) "
            "RETURN a.name AS x ORDER BY x LIMIT 3"
        )
        self.assertTrue(
            sql.rstrip().endswith('ORDER BY "x" ASC LIMIT 3'),
            sql,
        )
        # paging must not appear inside the branches
        head, _, tail = sql.partition("AS undirected_q")
        self.assertNotIn("LIMIT", head)

    def test_undirected_rejects_aggregates(self) -> None:
        with self.assertRaisesRegex(ValueError, "aggregation over an undirected"):
            self._sql("MATCH (a:User)-[r:KNOWS]-(b:User) RETURN count(*) AS c")

    def test_undirected_rejects_writes(self) -> None:
        with self.assertRaisesRegex(ValueError, "directed"):
            self._sql("MATCH (a:User)-[r:KNOWS]-(b:User) SET a.age = 1")

    def test_undirected_rejects_variable_length(self) -> None:
        with self.assertRaisesRegex(ValueError, "variable-length|directed"):
            self._sql(
                "MATCH (a:User)-[r:KNOWS*1..2]-(b:User) RETURN b.name AS n"
            )

    def test_undirected_impossible_labels_rejected(self) -> None:
        with self.assertRaisesRegex(ValueError, "contract|labels"):
            self._sql(
                "MATCH (a:Company)-[r:KNOWS]-(b:Company) RETURN a.name AS n"
            )


if __name__ == "__main__":
    unittest.main()
