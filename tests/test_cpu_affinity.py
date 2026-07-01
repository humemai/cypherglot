"""Tests for the benchmark CPU-affinity helpers (the locked equal-CPU budget)."""

from __future__ import annotations

import os
import unittest

from scripts.benchmarks.common.shared import apply_cpu_affinity, parse_cpu_affinity


class ParseCpuAffinityTests(unittest.TestCase):
    def test_none_and_empty_return_none(self) -> None:
        self.assertIsNone(parse_cpu_affinity(None))
        self.assertIsNone(parse_cpu_affinity(""))
        self.assertIsNone(parse_cpu_affinity("  "))
        self.assertIsNone(parse_cpu_affinity(","))

    def test_sorted_and_deduplicated(self) -> None:
        self.assertEqual(parse_cpu_affinity("4, 0 ,2, 2"), [0, 2, 4])
        self.assertEqual(parse_cpu_affinity("0,2,4,6,8,10"), [0, 2, 4, 6, 8, 10])

    def test_negative_rejected(self) -> None:
        with self.assertRaises(ValueError):
            parse_cpu_affinity("0,-1")

    def test_non_integer_rejected(self) -> None:
        with self.assertRaises(ValueError):
            parse_cpu_affinity("0,two")


class ApplyCpuAffinityTests(unittest.TestCase):
    def test_no_ids_is_noop(self) -> None:
        # Must not touch scheduling when nothing is requested.
        before = (
            sorted(os.sched_getaffinity(0))
            if hasattr(os, "sched_getaffinity")
            else None
        )
        apply_cpu_affinity(None)
        apply_cpu_affinity([])
        after = (
            sorted(os.sched_getaffinity(0))
            if hasattr(os, "sched_getaffinity")
            else None
        )
        self.assertEqual(before, after)

    def test_pins_process_when_supported(self) -> None:
        if not hasattr(os, "sched_setaffinity") or not hasattr(os, "sched_getaffinity"):
            self.skipTest("sched_setaffinity/getaffinity unavailable on this platform")
        available = sorted(os.sched_getaffinity(0))
        if len(available) < 2:
            self.skipTest("need at least two available CPUs to test pinning")
        original = set(os.sched_getaffinity(0))
        try:
            target = available[:2]
            apply_cpu_affinity(target)
            self.assertEqual(sorted(os.sched_getaffinity(0)), target)
        finally:
            os.sched_setaffinity(0, original)


if __name__ == "__main__":
    unittest.main()
