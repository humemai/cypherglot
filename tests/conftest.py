from __future__ import annotations

import os
import sys
from pathlib import Path

import pytest


# The benchmark modules now import through the scripts.benchmarks package, so
# tests that load them by file path still need the repository root on sys.path.
_REPO_ROOT = str(Path(__file__).resolve().parents[1])
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)


# Integration test modules that need a live database server (Neo4j / PostgreSQL /
# ClickHouse / Apache AGE). They auto-provision Docker containers on Linux, which
# is slow and leaks containers on an unhealthy Docker host, so they are skipped
# by default to keep `pytest` a fast unit-test run. Set CYPHERGLOT_TEST_INTEGRATION=1
# to run them (CI sets it in the dedicated per-engine jobs).
_INTEGRATION_TEST_FILES = frozenset(
    {
        "test_cypher_conformance.py",
        "test_cypher_conformance_age.py",
        "test_clickhouse_read_parity.py",
        "test_postgresql_read_parity.py",
        "test_postgresql_runtime.py",
        "test_runtime_oltp_parity.py",
        "test_runtime_olap_parity.py",
    }
)

_TRUTHY = frozenset({"1", "true", "yes", "on"})


def _integration_enabled() -> bool:
    return os.environ.get("CYPHERGLOT_TEST_INTEGRATION", "").strip().lower() in _TRUTHY


def pytest_collection_modifyitems(
    config: pytest.Config,
    items: list[pytest.Item],
) -> None:
    if _integration_enabled():
        return
    skip_integration = pytest.mark.skip(
        reason=(
            "integration test (needs a Neo4j/PostgreSQL/ClickHouse/AGE server); "
            "set CYPHERGLOT_TEST_INTEGRATION=1 to run"
        )
    )
    for item in items:
        if Path(str(item.fspath)).name in _INTEGRATION_TEST_FILES:
            item.add_marker(skip_integration)
