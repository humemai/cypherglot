"""Tests for the W3 result aggregator."""

from __future__ import annotations

import json
import subprocess
import sys
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

REPO_ROOT = Path(__file__).resolve().parents[1]


def _result_payload(engine: str, p50: float, rcte_p50: float) -> dict:
    def q(name, value):
        return {"name": name, "status": "passed",
                "execute": {"p50_ms": value, "mean_ms": value,
                            "p95_ms": value, "p99_ms": value,
                            "min_ms": value, "max_ms": value}}
    return {
        "results": {"workloads": {"olap": {f"{engine}_indexed": {
            "backend": engine, "index_mode": "indexed",
            "pass_count": 2, "timeout_count": 1, "fail_count": 0,
            "execute": {"p50_ms": p50},
            "compile": {"p50_ms": 1.0},
            "setup": {"ingest_ms": 2000.0},
            "storage": {"db_size_mib": 12.5},
            "queries": [
                q("olap_variable_length_reachability", p50),
                q("olap_variable_length_reachability+rcte", rcte_p50),
            ],
        }}}}
    }


class AnalyzeW3Tests(unittest.TestCase):
    def test_tables_render_from_result_dir(self) -> None:
        with TemporaryDirectory() as tmp:
            out = Path(tmp) / "stage" / "out"
            out.mkdir(parents=True)
            (out / "sqlite-r01.json").write_text(
                json.dumps(_result_payload("sqlite", 2.0, 1.0))
            )
            (out / "sqlite-r02.json").write_text(
                json.dumps(_result_payload("sqlite", 4.0, 2.0))
            )
            proc = subprocess.run(
                [sys.executable, "-m", "scripts.benchmarks.runtime.analyze_w3",
                 str(Path(tmp)), "--label-from-parent"],
                capture_output=True, text=True, cwd=REPO_ROOT,
            )
            self.assertEqual(proc.returncode, 0, proc.stderr)
            self.assertIn("3.00±1.00", proc.stdout)      # mean±std of 2,4
            self.assertIn("0.50x", proc.stdout)           # rcte/unroll = 1.5/3.0
            self.assertIn("| stage | sqlite | indexed | olap | 4 | 2 | 0 |",
                          proc.stdout)                    # outcome accounting
            self.assertIn("| stage | sqlite | 2.00 | 12.50 |", proc.stdout)


if __name__ == "__main__":
    unittest.main()
