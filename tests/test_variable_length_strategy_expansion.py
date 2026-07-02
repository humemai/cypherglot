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


if __name__ == "__main__":
    unittest.main()
