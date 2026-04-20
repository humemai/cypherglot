from __future__ import annotations

import importlib.util
import sys
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest import mock


REPO_ROOT = Path(__file__).resolve().parents[1]
BENCHMARK_SCRIPTS_DIR = REPO_ROOT / "scripts" / "benchmarks"
SQLITE_BACKEND_PATH = (
    REPO_ROOT / "scripts" / "benchmarks" / "_benchmark_sql_runtime_sqlite_backend.py"
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


benchmark_sqlite_backend = _load_module(
    "benchmark_sqlite_backend_for_tests",
    SQLITE_BACKEND_PATH,
)


class BenchmarkSQLiteBackendTests(unittest.TestCase):
    def test_prepare_shared_fixture_ingests_before_indexing(self) -> None:
        call_order: list[str] = []
        scale = SimpleNamespace(total_nodes=1, total_edges=2)
        prepare_shared_sqlite_fixture = getattr(
            benchmark_sqlite_backend,
            "_prepare_shared_sqlite_fixture",
        )

        fake_conn = mock.Mock()
        fake_conn.close = mock.Mock()
        fake_work_dir = mock.Mock()
        fake_work_dir.path = Path("/tmp/fake-runtime-workdir")

        with mock.patch.object(
            benchmark_sqlite_backend,
            "_create_managed_directory",
            return_value=fake_work_dir,
        ), mock.patch.object(
            benchmark_sqlite_backend,
            "_create_sqlite_connection",
            side_effect=lambda db_path: fake_conn,
        ), mock.patch.object(
            benchmark_sqlite_backend,
            "_capture_rss_snapshot",
            return_value={"client_mib": 1.0, "server_mib": None, "combined_mib": 1.0},
        ), mock.patch.object(
            benchmark_sqlite_backend,
            "_sqlite_file_size_mib",
            return_value=(1.0, 0.0),
        ), mock.patch.object(
            benchmark_sqlite_backend,
            "_progress",
        ), mock.patch.object(
            benchmark_sqlite_backend,
            "_measure_ns",
        ) as measure_ns:
            def run_measured(callback):
                result = callback()
                return result, 1

            measure_ns.side_effect = run_measured

            def record_schema(conn, graph_schema):
                _ = (conn, graph_schema)
                call_order.append("schema")

            def record_seed(conn, *, scale, graph_schema, edge_plans, progress_label):
                _ = (conn, scale, graph_schema, edge_plans, progress_label)
                call_order.append("ingest")
                return {"node_count": 1, "edge_count": 2}

            def record_index(conn, graph_schema, *, index_mode):
                _ = (conn, graph_schema, index_mode)
                call_order.append("index")

            def record_analyze(conn):
                _ = conn
                call_order.append("analyze")

            with mock.patch.object(
                benchmark_sqlite_backend,
                "_create_sqlite_schema",
                side_effect=record_schema,
            ), mock.patch.object(
                benchmark_sqlite_backend,
                "_seed_sqlite",
                side_effect=record_seed,
            ), mock.patch.object(
                benchmark_sqlite_backend,
                "_configure_sqlite_indexes",
                side_effect=record_index,
            ), mock.patch.object(
                benchmark_sqlite_backend,
                "_analyze_sqlite",
                side_effect=record_analyze,
            ):
                fixture = prepare_shared_sqlite_fixture(
                    scale=scale,
                    graph_schema=mock.sentinel.graph_schema,
                    edge_plans=mock.sentinel.edge_plans,
                    index_mode="indexed",
                )

        self.assertEqual(call_order, ["schema", "ingest", "index", "analyze"])
        self.assertEqual(fixture.setup_metrics["ingest_ns"], 1)
        self.assertEqual(fixture.setup_metrics["index_ns"], 1)
        fake_conn.close.assert_called_once()


if __name__ == "__main__":
    unittest.main()
