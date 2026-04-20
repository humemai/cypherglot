from __future__ import annotations

import sys
from pathlib import Path


# The benchmark scripts and shared benchmark modules are loaded as standalone
# siblings and use bare-name imports such as
# `from _benchmark_common import ...`.  Those imports resolve only when the
# scripts/benchmarks/ directory is on sys.path, so we add it here once for the
# whole test session rather than patching it inside individual test modules.
_BENCHMARKS_DIR = str(Path(__file__).resolve().parents[1] / "scripts" / "benchmarks")
if _BENCHMARKS_DIR not in sys.path:
    sys.path.insert(0, _BENCHMARKS_DIR)
