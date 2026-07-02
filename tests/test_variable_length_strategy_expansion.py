# pyright: reportPrivateUsage=false
# pylint: disable=protected-access
"""Tests for the benchmark's variable-length strategy expansion (the ablation)."""

from __future__ import annotations

import unittest

from scripts.benchmarks.common.shared import (
    RECURSIVE_CTE_NAME_SUFFIX,
    CorpusQuery,
    _expand_variable_length_strategies,
    _query_has_variable_length,
)


def _query(name: str, text: str) -> CorpusQuery:
    return CorpusQuery(
        name=name,
        workload="olap",
        category="variable-length",
        query=text,
        backends=("sqlite",),
    )


_PLAIN = _query("plain", "MATCH (a:T)-[:E]->(b:T) RETURN b.name AS name")
_VARIABLE = _query(
    "reach", "MATCH (a:T {name: 'x'})-[:E*1..3]->(b:T) RETURN b.name AS name"
)


class VariableLengthDetectionTests(unittest.TestCase):
    def test_detects_variable_length_pattern(self) -> None:
        self.assertTrue(_query_has_variable_length(_VARIABLE))
        self.assertFalse(_query_has_variable_length(_PLAIN))

    def test_detects_zero_lower_bound(self) -> None:
        query = _query("zero", "MATCH (a:T)-[:E*0..2]->(b:T) RETURN count(*) AS n")
        self.assertTrue(_query_has_variable_length(query))


class StrategyExpansionTests(unittest.TestCase):
    def test_unroll_leaves_corpus_unchanged(self) -> None:
        queries = [_PLAIN, _VARIABLE]
        expanded = _expand_variable_length_strategies(queries, "unroll")
        self.assertEqual(expanded, queries)
        self.assertTrue(
            all(q.variable_length_strategy == "unroll" for q in expanded)
        )

    def test_recursive_cte_switches_only_variable_length(self) -> None:
        expanded = _expand_variable_length_strategies([_PLAIN, _VARIABLE], "recursive_cte")
        self.assertEqual([q.name for q in expanded], ["plain", "reach"])
        self.assertEqual(expanded[0].variable_length_strategy, "unroll")
        self.assertEqual(expanded[1].variable_length_strategy, "recursive_cte")

    def test_both_duplicates_variable_length_with_suffix(self) -> None:
        expanded = _expand_variable_length_strategies([_PLAIN, _VARIABLE], "both")
        self.assertEqual(
            [q.name for q in expanded],
            ["plain", "reach", f"reach{RECURSIVE_CTE_NAME_SUFFIX}"],
        )
        duplicate = expanded[2]
        self.assertEqual(duplicate.variable_length_strategy, "recursive_cte")
        # Everything except name and strategy stays identical.
        self.assertEqual(duplicate.query, _VARIABLE.query)
        self.assertEqual(duplicate.workload, _VARIABLE.workload)
        self.assertEqual(duplicate.category, _VARIABLE.category)
        self.assertEqual(duplicate.backends, _VARIABLE.backends)

    def test_unknown_mode_raises(self) -> None:
        with self.assertRaises(ValueError):
            _expand_variable_length_strategies([_PLAIN], "nope")

    def test_rcte_excludes_backends_without_recursive_cte(self) -> None:
        # Turso rejects WITH RECURSIVE at parse time, so the rcte twin (and the
        # whole-corpus recursive_cte mode) must drop it from the backend list.
        query = CorpusQuery(
            name="reach",
            workload="olap",
            category="variable-length",
            query="MATCH (a:T {name: 'x'})-[:E*1..3]->(b:T) RETURN b.name AS n",
            backends=("sqlite", "turso", "duckdb", "postgresql", "clickhouse"),
        )
        expanded = _expand_variable_length_strategies([query], "both")
        self.assertEqual(expanded[0].backends, query.backends)  # unroll untouched
        self.assertEqual(
            expanded[1].backends,
            ("sqlite", "duckdb", "postgresql", "clickhouse"),
        )
        switched = _expand_variable_length_strategies([query], "recursive_cte")
        self.assertEqual(
            switched[0].backends,
            ("sqlite", "duckdb", "postgresql", "clickhouse"),
        )


if __name__ == "__main__":
    unittest.main()
