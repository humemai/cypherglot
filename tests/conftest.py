from __future__ import annotations

import sys
from pathlib import Path


# The benchmark modules now import through the scripts.benchmarks package, so
# tests that load them by file path still need the repository root on sys.path.
_REPO_ROOT = str(Path(__file__).resolve().parents[1])
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)
