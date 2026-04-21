from __future__ import annotations

import importlib.util
import sys
import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
BENCHMARK_SCRIPTS_DIR = REPO_ROOT / "scripts" / "benchmarks"
BENCHMARK_CORE_PATH = (
    REPO_ROOT / "scripts" / "benchmarks" / "_benchmark_sql_runtime_core.py"
)
POSTGRES_BACKEND_PATH = (
    REPO_ROOT
    / "scripts"
    / "benchmarks"
    / "_benchmark_sql_runtime_postgresql_backend.py"
)

if str(BENCHMARK_SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(BENCHMARK_SCRIPTS_DIR))


def _load_module(module_name: str, path: Path):
    module_spec = importlib.util.spec_from_file_location(module_name, path)
    if module_spec is None or module_spec.loader is None:
        raise RuntimeError(f"Unable to load module from {path}")
    module = importlib.util.module_from_spec(module_spec)
    sys.modules[module_spec.name] = module
    module_spec.loader.exec_module(module)
    return module


benchmark_sql_runtime_core = _load_module(
    "benchmark_sql_runtime_core_for_postgresql_backend_tests",
    BENCHMARK_CORE_PATH,
)
benchmark_postgresql_backend = _load_module(
    "benchmark_postgresql_backend",
    POSTGRES_BACKEND_PATH,
)

SMALL_SCALE = benchmark_sql_runtime_core.RuntimeScale(
    node_type_count=3,
    edge_type_count=3,
    nodes_per_type=20,
    edges_per_source=2,
    ingest_batch_size=10,
    variable_hop_max=2,
)


class _FakeCursor:
    def __init__(self) -> None:
        self.copy_payloads: dict[str, str] = {}
        self.setval_calls: list[tuple[str, tuple[object, ...] | None]] = []

    def __enter__(self) -> _FakeCursor:
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        return None

    def copy_expert(self, sql: str, handle) -> None:
        table_name = sql.split()[1]
        self.copy_payloads[table_name] = handle.read()

    def execute(self, sql: str, params: tuple[object, ...] | None = None) -> None:
        if sql.startswith("SELECT setval"):
            self.setval_calls.append((sql, params))


class _FakeConnection:
    def __init__(self) -> None:
        self.cursor_instance = _FakeCursor()
        self.commit_count = 0

    def cursor(self) -> _FakeCursor:
        return self.cursor_instance

    def commit(self) -> None:
        self.commit_count += 1


class BenchmarkPostgreSQLBackendTests(unittest.TestCase):
    def test_seed_postgresql_from_fixture_uses_copy_and_resets_sequences(self) -> None:
        build_graph_schema = getattr(benchmark_sql_runtime_core, "_build_graph_schema")
        prepare_fixture = getattr(
            benchmark_sql_runtime_core,
            "_prepare_generated_graph_fixture",
        )
        postgresql_copy_value = getattr(
            benchmark_postgresql_backend,
            "_postgresql_copy_value",
        )
        seed_postgresql_from_fixture = getattr(
            benchmark_postgresql_backend,
            "_seed_postgresql_from_fixture",
        )

        graph_schema, edge_plans = build_graph_schema(SMALL_SCALE)
        fixture = prepare_fixture(
            scale=SMALL_SCALE,
            graph_schema=graph_schema,
            edge_plans=edge_plans,
            index_mode="indexed",
        )
        fake_conn = _FakeConnection()
        try:
            row_counts = seed_postgresql_from_fixture(
                fake_conn,
                generated_fixture=fixture,
                graph_schema=graph_schema,
            )
        finally:
            fixture.close()

        self.assertEqual(row_counts, fixture.row_counts)
        self.assertEqual(
            set(fake_conn.cursor_instance.copy_payloads),
            {
                *(node_type.table_name for node_type in graph_schema.node_types),
                *(edge_type.table_name for edge_type in graph_schema.edge_types),
            },
        )
        self.assertEqual(
            len(fake_conn.cursor_instance.setval_calls),
            len(fake_conn.cursor_instance.copy_payloads),
        )
        self.assertEqual(fake_conn.commit_count, 1)

        user_payload = fake_conn.cursor_instance.copy_payloads[
            graph_schema.node_types[0].table_name
        ]
        knows_payload = fake_conn.cursor_instance.copy_payloads[
            graph_schema.edge_types[0].table_name
        ]
        self.assertIn("\tt\t", user_payload)
        self.assertIn("\tf\t", knows_payload)
        self.assertEqual(postgresql_copy_value(None), r"\N")


if __name__ == "__main__":
    unittest.main()
