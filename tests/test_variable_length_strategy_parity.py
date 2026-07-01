# pyright: reportPrivateUsage=false
# pylint: disable=protected-access
"""Parity between the two variable-length lowering strategies.

The ``recursive_cte`` lowering of ``(a)-[:E*lo..hi]->(b)`` must return the *same
result multiset* as the default ``unroll`` lowering. Both realise the admitted
*walk* semantics (nodes/edges may repeat; no relationship-uniqueness), so on a
graph WITH CYCLES and MULTIPLE distinct paths a node reachable by two length-2
walks must appear twice under both strategies.

The fixture below is intentionally small and hand-traceable: an independent
Python walk enumerator computes the expected end-node multiset so the assertions
are not merely "two wrongs agree". Executed on in-memory SQLite (always) and
DuckDB (when importable).
"""

from __future__ import annotations

import sqlite3
import unittest

import cypherglot
from cypherglot.render import to_sql
from cypherglot.schema import EdgeTypeSpec, GraphSchema, NodeTypeSpec, PropertyField

try:  # pragma: no cover - availability depends on the environment
    import duckdb
except ImportError:  # pragma: no cover
    duckdb = None


# --- fixture graph -------------------------------------------------------
# Person nodes p1..p5 (id 1..5) with a KNOWS self-relation. The edge set gives
# p1 two distinct 2-hop paths to p4 (p1->p2->p4 and p1->p3->p4) and a cycle
# p2 -> p4 -> p2, so walks of the same length reach the same node more than once.
# p5 is isolated (exercises the all-pairs zero-hop base).
_NODES: dict[int, tuple[str, float, int]] = {
    1: ("p1", 5.0, 1),
    2: ("p2", 4.0, 1),
    3: ("p3", 3.0, 0),
    4: ("p4", 2.0, 1),
    5: ("p5", 1.0, 0),
}
_ADJ: dict[int, list[int]] = {
    1: [2, 3],
    2: [4, 3],
    3: [4],
    4: [2],
    5: [],
}


def _enumerate_end_ids(starts: list[int], lo: int, hi: int) -> list[int]:
    """Independent walk enumerator: every walk of length ``lo..hi`` from any of
    ``starts``, returning its end-node id (with multiplicity)."""
    result: list[int] = []
    current = list(starts)  # depth-0 walks are the start nodes themselves
    for depth in range(0, hi + 1):
        if depth >= lo:
            result.extend(current)
        current = [target for end in current for target in _ADJ[end]]
    return result


def _canon(rows: list[tuple[object, ...]]) -> list[tuple[object, ...]]:
    def normalize(value: object) -> object:
        if isinstance(value, bool):
            return int(value)
        if isinstance(value, float):
            return round(value, 6)
        return value

    return sorted(
        (tuple(normalize(value) for value in row) for row in rows),
        key=repr,
    )


_SCHEMA = GraphSchema(
    node_types=(
        NodeTypeSpec(
            "Person",
            (
                PropertyField("name", "string"),
                PropertyField("score", "float"),
                PropertyField("active", "boolean"),
            ),
        ),
    ),
    edge_types=(EdgeTypeSpec("KNOWS", "Person", "Person"),),
)
_SCHEMA_CONTEXT = cypherglot.CompilerSchemaContext.type_aware(_SCHEMA)

_REACHABILITY = (
    "MATCH (a:Person {name: 'p1'})-[:KNOWS*1..3]->(b:Person) "
    "RETURN b.name AS friend, b.score AS score ORDER BY score DESC, friend LIMIT 50"
)
_GROUPED_MAX = (
    "MATCH (a:Person {name: 'p1'})-[:KNOWS*0..3]->(b:Person) "
    "RETURN b.active AS active, count(*) AS total, max(b.score) AS max_score "
    "ORDER BY total DESC, active"
)
_GROUPED_ROLLUP = (
    "MATCH (a:Person)-[:KNOWS*0..3]->(b:Person) "
    "RETURN b.active AS active, count(b) AS total, avg(b.score) AS avg_score "
    "ORDER BY total DESC, active"
)


def _expected_reachability() -> list[tuple[object, ...]]:
    ends = _enumerate_end_ids([1], 1, 3)
    return _canon([(_NODES[e][0], _NODES[e][1]) for e in ends])


def _grouped_expectations(
    ends: list[int],
) -> list[tuple[object, ...]]:
    groups: dict[int, list[float]] = {}
    for end in ends:
        _, score, active = _NODES[end]
        groups.setdefault(active, []).append(score)
    return groups


def _expected_grouped_max() -> list[tuple[object, ...]]:
    groups = _grouped_expectations(_enumerate_end_ids([1], 0, 3))
    return _canon(
        [(active, len(scores), max(scores)) for active, scores in groups.items()]
    )


def _expected_grouped_rollup() -> list[tuple[object, ...]]:
    groups = _grouped_expectations(_enumerate_end_ids([1, 2, 3, 4, 5], 0, 3))
    return _canon(
        [
            (active, len(scores), sum(scores) / len(scores))
            for active, scores in groups.items()
        ]
    )


class VariableLengthStrategyParityTests(unittest.TestCase):
    def _seed_sqlite(self) -> sqlite3.Connection:
        conn = sqlite3.connect(":memory:")
        conn.executescript("\n".join(_SCHEMA.ddl("sqlite")))
        conn.executemany(
            "INSERT INTO cg_node_person (id, name, score, active) VALUES (?, ?, ?, ?)",
            [(node_id, *fields) for node_id, fields in _NODES.items()],
        )
        edge_id = 1
        edges: list[tuple[int, int, int]] = []
        for source, targets in _ADJ.items():
            for target in targets:
                edges.append((edge_id, source, target))
                edge_id += 1
        conn.executemany(
            "INSERT INTO cg_edge_knows (id, from_id, to_id) VALUES (?, ?, ?)",
            edges,
        )
        conn.commit()
        return conn

    def _seed_duckdb(self):  # pragma: no cover - exercised only when duckdb present
        con = duckdb.connect(":memory:")
        for statement in _SCHEMA.ddl("duckdb"):
            con.execute(statement)
        for node_id, (name, score, active) in _NODES.items():
            con.execute(
                "INSERT INTO cg_node_person (id, name, score, active) "
                "VALUES (?, ?, ?, ?)",
                [node_id, name, score, bool(active)],
            )
        edge_id = 1
        for source, targets in _ADJ.items():
            for target in targets:
                con.execute(
                    "INSERT INTO cg_edge_knows (id, from_id, to_id) VALUES (?, ?, ?)",
                    [edge_id, source, target],
                )
                edge_id += 1
        return con

    def _run_sqlite(self, conn: sqlite3.Connection, query: str, strategy: str):
        sql = to_sql(
            query,
            backend="sqlite",
            schema_context=_SCHEMA_CONTEXT,
            variable_length_strategy=strategy,
        )
        return conn.execute(sql).fetchall()

    def _run_duckdb(self, con, query: str, strategy: str):  # pragma: no cover
        sql = to_sql(
            query,
            backend="duckdb",
            schema_context=_SCHEMA_CONTEXT,
            variable_length_strategy=strategy,
        )
        return con.execute(sql).fetchall()

    def test_reachability_parity_and_expected(self) -> None:
        conn = self._seed_sqlite()
        try:
            unroll = _canon(self._run_sqlite(conn, _REACHABILITY, "unroll"))
            recursive = _canon(
                self._run_sqlite(conn, _REACHABILITY, "recursive_cte")
            )
        finally:
            conn.close()
        expected = _expected_reachability()
        # Hand-checked: p1 reaches p2 x3, p3 x2, p4 x3 within 1..3 hops.
        self.assertEqual(len(expected), 8)
        self.assertEqual(recursive, expected)
        self.assertEqual(unroll, expected)
        self.assertEqual(recursive, unroll)

    def test_grouped_max_parity_and_expected(self) -> None:
        conn = self._seed_sqlite()
        try:
            unroll = _canon(self._run_sqlite(conn, _GROUPED_MAX, "unroll"))
            recursive = _canon(self._run_sqlite(conn, _GROUPED_MAX, "recursive_cte"))
        finally:
            conn.close()
        expected = _expected_grouped_max()
        self.assertEqual(recursive, expected)
        self.assertEqual(unroll, expected)
        self.assertEqual(recursive, unroll)

    def test_grouped_rollup_all_pairs_parity_and_expected(self) -> None:
        conn = self._seed_sqlite()
        try:
            unroll = _canon(self._run_sqlite(conn, _GROUPED_ROLLUP, "unroll"))
            recursive = _canon(
                self._run_sqlite(conn, _GROUPED_ROLLUP, "recursive_cte")
            )
        finally:
            conn.close()
        expected = _expected_grouped_rollup()
        self.assertEqual(recursive, expected)
        self.assertEqual(unroll, expected)
        self.assertEqual(recursive, unroll)

    @unittest.skipIf(duckdb is None, "duckdb is not installed")
    def test_parity_on_duckdb(self) -> None:  # pragma: no cover
        con = self._seed_duckdb()
        try:
            for query in (_REACHABILITY, _GROUPED_MAX, _GROUPED_ROLLUP):
                with self.subTest(query=query):
                    unroll = _canon(self._run_duckdb(con, query, "unroll"))
                    recursive = _canon(
                        self._run_duckdb(con, query, "recursive_cte")
                    )
                    self.assertEqual(recursive, unroll)
            self.assertEqual(
                _canon(self._run_duckdb(con, _REACHABILITY, "recursive_cte")),
                _expected_reachability(),
            )
        finally:
            con.close()


if __name__ == "__main__":
    unittest.main()
