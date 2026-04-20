from __future__ import annotations

import importlib.util
import sys
import unittest
from pathlib import Path
from unittest import mock


REPO_ROOT = Path(__file__).resolve().parents[1]


def _load_entrypoint_module(module_name: str, filename: str):
    script_path = REPO_ROOT / "scripts" / "benchmarks" / filename
    module_spec = importlib.util.spec_from_file_location(module_name, script_path)
    if module_spec is None or module_spec.loader is None:
        raise RuntimeError(f"Unable to load benchmark script from {script_path}")
    module = importlib.util.module_from_spec(module_spec)
    sys.modules[module_spec.name] = module
    module_spec.loader.exec_module(module)
    return module


class BenchmarkSQLRuntimeEntrypointTests(unittest.TestCase):
    def test_sqlite_wrapper_delegates_to_core(self) -> None:
        module = _load_entrypoint_module(
            "benchmark_sqlite_runtime",
            "benchmark_sqlite_runtime.py",
        )

        with mock.patch.object(module, "_shared_main", return_value=7) as shared_main:
            result = module.main()

        self.assertEqual(result, 7)
        shared_main.assert_called_once_with(module.SQLITE_ENTRYPOINT)

    def test_duckdb_wrapper_delegates_to_core(self) -> None:
        module = _load_entrypoint_module(
            "benchmark_duckdb_runtime",
            "benchmark_duckdb_runtime.py",
        )

        with mock.patch.object(module, "_shared_main", return_value=11) as shared_main:
            result = module.main()

        self.assertEqual(result, 11)
        shared_main.assert_called_once_with(module.DUCKDB_ENTRYPOINT)

    def test_postgresql_wrapper_delegates_to_core(self) -> None:
        module = _load_entrypoint_module(
            "benchmark_postgresql_runtime",
            "benchmark_postgresql_runtime.py",
        )

        with mock.patch.object(module, "_shared_main", return_value=13) as shared_main:
            result = module.main()

        self.assertEqual(result, 13)
        shared_main.assert_called_once_with(module.POSTGRESQL_ENTRYPOINT)
