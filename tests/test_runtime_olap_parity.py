# pyright: reportPrivateUsage=false
# pylint: disable=protected-access

from __future__ import annotations

import importlib.util
import json
import shutil
import socket
import sys
import unittest
from decimal import Decimal
from pathlib import Path

import cypherglot

REPO_ROOT = Path(__file__).resolve().parents[1]
BENCHMARKS_DIR = REPO_ROOT / "scripts" / "benchmarks"
from tests._postgres_runtime_support import (  # noqa: E402
    acquire_postgresql_test_dsn,
    release_postgresql_test_dsn,
)


def _load_module(module_name: str, relative_path: str):
    module_path = REPO_ROOT / relative_path
    module_spec = importlib.util.spec_from_file_location(module_name, module_path)
    if module_spec is None or module_spec.loader is None:
        raise RuntimeError(f"Unable to load module from {module_path}")
    module = importlib.util.module_from_spec(module_spec)
    sys.modules[module_name] = module
    module_spec.loader.exec_module(module)
    return module


if str(BENCHMARKS_DIR) not in sys.path:
    sys.path.insert(0, str(BENCHMARKS_DIR))

_benchmark_common = _load_module(
    "_benchmark_common",
    "scripts/benchmarks/_benchmark_common.py",
)
_benchmark_sql_runtime_core = _load_module(
    "_benchmark_sql_runtime_core",
    "scripts/benchmarks/_benchmark_sql_runtime_core.py",
)
_benchmark_sql_runtime_postgresql_backend = _load_module(
    "_benchmark_sql_runtime_postgresql_backend",
    "scripts/benchmarks/_benchmark_sql_runtime_postgresql_backend.py",
)
_benchmark_sql_runtime_shared = _load_module(
    "_benchmark_sql_runtime_shared",
    "scripts/benchmarks/_benchmark_sql_runtime_shared.py",
)
benchmark_arcadedb_embedded_runtime = _load_module(
    "benchmark_arcadedb_embedded_runtime",
    "scripts/benchmarks/benchmark_arcadedb_embedded_runtime.py",
)
benchmark_ladybug_runtime = _load_module(
    "benchmark_ladybug_runtime",
    "scripts/benchmarks/benchmark_ladybug_runtime.py",
)
benchmark_neo4j_runtime = _load_module(
    "benchmark_neo4j_runtime",
    "scripts/benchmarks/benchmark_neo4j_runtime.py",
)

RuntimeScale = _benchmark_common.RuntimeScale
_build_graph_schema = _benchmark_common._build_graph_schema
_render_corpus_queries = _benchmark_common._render_corpus_queries
_token_map = _benchmark_common._token_map
_BackendRunner = _benchmark_sql_runtime_core._BackendRunner
_execute_bound_postgresql_sql = (
    _benchmark_sql_runtime_postgresql_backend._execute_bound_postgresql_sql
)
_create_managed_directory = _benchmark_sql_runtime_shared._create_managed_directory
_prepare_generated_graph_fixture = (
    _benchmark_sql_runtime_shared._prepare_generated_graph_fixture
)
_arcadedb_available = benchmark_arcadedb_embedded_runtime._arcadedb_available
_load_arcadedb_corpus = benchmark_arcadedb_embedded_runtime._load_corpus
_prepare_arcadedb_fixture = (
    benchmark_arcadedb_embedded_runtime._prepare_arcadedb_fixture
)
_rewrite_arcadedb_query = benchmark_arcadedb_embedded_runtime._rewrite_arcadedb_query
_ladybug_available = benchmark_ladybug_runtime._ladybug_available
_prepare_ladybug_fixture = benchmark_ladybug_runtime._prepare_ladybug_fixture
_rewrite_ladybug_query = benchmark_ladybug_runtime._rewrite_ladybug_query
DockerNeo4jConfig = benchmark_neo4j_runtime.DockerNeo4jConfig
_docker_default_container_name = benchmark_neo4j_runtime._docker_default_container_name
_neo4j_graph_database = benchmark_neo4j_runtime.GraphDatabase
_setup_neo4j_mode = benchmark_neo4j_runtime._setup_mode
_start_docker_neo4j = benchmark_neo4j_runtime._start_docker_neo4j
_stop_docker_neo4j = benchmark_neo4j_runtime._stop_docker_neo4j
_wait_for_docker_server_ready = benchmark_neo4j_runtime._wait_for_docker_server_ready
_wait_for_neo4j_driver_ready = benchmark_neo4j_runtime._wait_for_driver_ready


CORPUS_PATH = (
    REPO_ROOT
    / "scripts"
    / "benchmarks"
    / "corpora"
    / "sqlite_runtime_benchmark_corpus.json"
)
BOOLEAN_COLUMNS = {"active", "target_active"}
SMALL_SCALE = RuntimeScale(
    node_type_count=3,
    edge_type_count=3,
    nodes_per_type=20,
    edges_per_source=2,
    ingest_batch_size=10,
    variable_hop_max=2,
)


class ArcadeDBEmbeddedOlapParityTests(unittest.TestCase):
    @staticmethod
    def _allocate_free_port() -> int:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.bind(("127.0.0.1", 0))
            return int(sock.getsockname()[1])

    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        if not _ladybug_available():
            raise unittest.SkipTest("ladybug is not installed")
        if not _arcadedb_available():
            raise unittest.SkipTest("arcadedb-embedded is not installed")
        if _neo4j_graph_database is None:
            raise unittest.SkipTest("neo4j is not installed")
        if shutil.which("docker") is None:
            raise unittest.SkipTest("docker is not installed")

        cls._postgres_dsn = acquire_postgresql_test_dsn()
        cls.graph_schema, edge_plans = _build_graph_schema(SMALL_SCALE)
        cls.edge_plans = edge_plans
        cls.schema_context = cypherglot.CompilerSchemaContext.type_aware(
            cls.graph_schema
        )
        cls.olap_queries = [
            query
            for query in _render_corpus_queries(
                _load_arcadedb_corpus(CORPUS_PATH),
                _token_map(SMALL_SCALE, cls.graph_schema, edge_plans),
            )
            if query.workload == "olap"
        ]

        cls.indexed_source = _prepare_generated_graph_fixture(
            scale=SMALL_SCALE,
            graph_schema=cls.graph_schema,
            edge_plans=edge_plans,
            index_mode="indexed",
        )
        cls.unindexed_source = _prepare_generated_graph_fixture(
            scale=SMALL_SCALE,
            graph_schema=cls.graph_schema,
            edge_plans=edge_plans,
            index_mode="unindexed",
        )

        cls.sqlite_indexed = cls._open_runner("sqlite", cls.indexed_source)
        cls.sqlite_unindexed = cls._open_runner("sqlite", cls.unindexed_source)
        cls.duckdb = cls._open_runner("duckdb", cls.indexed_source)
        cls.postgresql_indexed = cls._open_runner("postgresql", cls.indexed_source)
        cls.postgresql_unindexed = cls._open_runner(
            "postgresql",
            cls.unindexed_source,
        )
        cls.ladybug_unindexed = _prepare_ladybug_fixture(
            workload="olap",
            graph_schema=cls.graph_schema,
            sqlite_source=cls.unindexed_source,
            db_root_dir=None,
        )
        cls.arcadedb_indexed = _prepare_arcadedb_fixture(
            workload="olap",
            index_mode="indexed",
            graph_schema=cls.graph_schema,
            sqlite_source=cls.indexed_source,
            ingest_batch_size=SMALL_SCALE.ingest_batch_size,
            db_root_dir=None,
        )
        cls.arcadedb_unindexed = _prepare_arcadedb_fixture(
            workload="olap",
            index_mode="unindexed",
            graph_schema=cls.graph_schema,
            sqlite_source=cls.unindexed_source,
            ingest_batch_size=SMALL_SCALE.ingest_batch_size,
            db_root_dir=None,
        )

        cls._neo4j_password = "cypherglot-parity"
        cls._neo4j_database = "neo4j"
        cls._neo4j_config = DockerNeo4jConfig(
            image="neo4j:5-community",
            container_name=_docker_default_container_name(),
            bolt_port=cls._allocate_free_port(),
            http_port=cls._allocate_free_port(),
            startup_timeout_s=120,
            keep_container=False,
        )
        _start_docker_neo4j(cls._neo4j_config, cls._neo4j_password)
        _wait_for_docker_server_ready(
            cls._neo4j_config,
            cls._neo4j_config.startup_timeout_s,
        )
        cls._neo4j_driver = _wait_for_neo4j_driver_ready(
            f"bolt://127.0.0.1:{cls._neo4j_config.bolt_port}",
            "neo4j",
            cls._neo4j_password,
            cls._neo4j_config.startup_timeout_s,
        )
        cls.neo4j_indexed_results = cls._capture_neo4j_results(index_mode="indexed")
        cls.neo4j_unindexed_results = cls._capture_neo4j_results(
            index_mode="unindexed"
        )

    @classmethod
    def tearDownClass(cls) -> None:
        for attribute_name in (
            "arcadedb_unindexed",
            "arcadedb_indexed",
            "ladybug_unindexed",
            "postgresql_unindexed",
            "postgresql_indexed",
            "duckdb",
            "sqlite_unindexed",
            "sqlite_indexed",
            "unindexed_source",
            "indexed_source",
        ):
            fixture = getattr(cls, attribute_name, None)
            if fixture is not None:
                fixture.close()
        neo4j_driver = getattr(cls, "_neo4j_driver", None)
        if neo4j_driver is not None:
            neo4j_driver.close()
        neo4j_config = getattr(cls, "_neo4j_config", None)
        if neo4j_config is not None:
            _stop_docker_neo4j(neo4j_config)
        release_postgresql_test_dsn()
        super().tearDownClass()

    @classmethod
    def _open_runner(cls, backend: str, source) -> _BackendRunner:
        work_dir = _create_managed_directory(
            root_dir=None,
            prefix=f"olap-parity-{backend}-",
        )
        return _BackendRunner(
            backend,
            work_dir,
            graph_schema=cls.graph_schema,
            schema_context=cls.schema_context,
            sqlite_source=source,
            postgres_dsn=cls._postgres_dsn,
        )

    @classmethod
    def _capture_neo4j_results(
        cls,
        *,
        index_mode: str,
    ) -> dict[str, list[tuple[object, ...]]]:
        _setup_neo4j_mode(
            cls._neo4j_driver,
            database=cls._neo4j_database,
            index_mode=index_mode,
            scale=SMALL_SCALE,
            graph_schema=cls.graph_schema,
            edge_plans=cls.edge_plans,
            docker_config=cls._neo4j_config,
        )
        results: dict[str, list[tuple[object, ...]]] = {}
        for query in cls.olap_queries:
            column_names, rows = cls._execute_neo4j(query)
            results[query.name] = cls()._canonize_rows(column_names, rows)
        return results

    def test_olap_results_match_across_backends(self) -> None:
        for query in self.olap_queries:
            with self.subTest(query=query.name):
                column_names, sqlite_indexed_rows = self._execute_sql_runner(
                    self.sqlite_indexed,
                    query,
                )
                expected_rows = self._canonize_rows(column_names, sqlite_indexed_rows)

                backend_rows = {
                    "sqlite_unindexed": self._canonize_rows(
                        column_names,
                        self._execute_sql_runner(
                            self.sqlite_unindexed,
                            query,
                        )[1],
                    ),
                    "duckdb": self._canonize_rows(
                        column_names,
                        self._execute_sql_runner(self.duckdb, query)[1],
                    ),
                    "postgresql_indexed": self._canonize_rows(
                        column_names,
                        self._execute_sql_runner(
                            self.postgresql_indexed,
                            query,
                        )[1],
                    ),
                    "postgresql_unindexed": self._canonize_rows(
                        column_names,
                        self._execute_sql_runner(
                            self.postgresql_unindexed,
                            query,
                        )[1],
                    ),
                    "ladybug_unindexed": self._canonize_rows(
                        column_names,
                        self._execute_ladybug(query),
                    ),
                    "arcadedb_indexed": self._canonize_rows(
                        column_names,
                        self._execute_arcadedb(
                            self.arcadedb_indexed,
                            query,
                            column_names,
                        ),
                    ),
                    "arcadedb_unindexed": self._canonize_rows(
                        column_names,
                        self._execute_arcadedb(
                            self.arcadedb_unindexed,
                            query,
                            column_names,
                        ),
                    ),
                    "neo4j_indexed": self.neo4j_indexed_results[query.name],
                    "neo4j_unindexed": self.neo4j_unindexed_results[query.name],
                }

                mismatches = {
                    backend_name: rows
                    for backend_name, rows in backend_rows.items()
                    if rows != expected_rows
                }
                mismatch_summary = json.dumps(
                    self._summarize_mismatches(expected_rows, mismatches),
                    sort_keys=True,
                )
                self.assertEqual(
                    mismatches,
                    {},
                    msg=f"OLAP parity mismatch for {query.name}: {mismatch_summary}",
                )

    def _execute_sql_runner(
        self,
        runner: _BackendRunner,
        query,
    ) -> tuple[list[str], list[tuple[object, ...]]]:
        artifact = runner.compile_query(query)
        if runner.backend == "sqlite":
            cursor = runner.sqlite.execute(artifact.compiled)
            column_names = [description[0] for description in cursor.description or []]
            return column_names, cursor.fetchall()
        if runner.backend == "duckdb":
            cursor = runner.duck.execute(artifact.compiled)
            column_names = [description[0] for description in cursor.description or []]
            return column_names, cursor.fetchall()

        with runner.postgresql.cursor() as cur:
            _execute_bound_postgresql_sql(cur, artifact.compiled, {})
            column_names = [description[0] for description in cur.description or []]
            return column_names, cur.fetchall()

    def _execute_ladybug(self, query) -> list[list[object]]:
        statement = _rewrite_ladybug_query(self.ladybug_unindexed, query)
        return list(self.ladybug_unindexed.connection.execute(statement))

    def _execute_arcadedb(
        self,
        fixture,
        query,
        column_names: list[str],
    ) -> list[tuple[object, ...]]:
        statement = _rewrite_arcadedb_query(fixture, query)
        rows = list(fixture.database.query("opencypher", statement))
        return [
            tuple(row.to_dict()[column_name] for column_name in column_names)
            for row in rows
        ]

    @classmethod
    def _execute_neo4j(
        cls,
        query,
    ) -> tuple[list[str], list[tuple[object, ...]]]:
        with cls._neo4j_driver.session(database=cls._neo4j_database) as session:
            result = session.run(query.query)
            column_names = list(result.keys())
            rows = [
                tuple(record[column_name] for column_name in column_names)
                for record in result
            ]
        return column_names, rows

    def _canonize_rows(
        self,
        column_names: list[str],
        rows: list[tuple[object, ...]] | list[list[object]],
    ) -> list[tuple[object, ...]]:
        normalized_rows = []
        for row in rows:
            normalized_rows.append(
                tuple(
                    self._normalize_scalar(
                        value,
                        boolean_hint=column_name in BOOLEAN_COLUMNS,
                    )
                    for column_name, value in zip(column_names, row, strict=True)
                )
            )
        return sorted(
            normalized_rows,
            key=lambda row: json.dumps(row, sort_keys=True),
        )

    def _normalize_scalar(
        self,
        value: object,
        *,
        boolean_hint: bool = False,
    ) -> object:
        if value is None:
            return None
        if isinstance(value, Decimal):
            value = float(value)
        if boolean_hint and isinstance(value, int) and value in (0, 1):
            return bool(value)
        if isinstance(value, bool):
            return value
        if isinstance(value, float):
            if value.is_integer():
                return int(value)
            return round(value, 12)
        if isinstance(value, dict):
            return {
                key: self._normalize_scalar(
                    item,
                    boolean_hint=key in BOOLEAN_COLUMNS,
                )
                for key, item in value.items()
            }
        if isinstance(value, list):
            return [self._normalize_scalar(item) for item in value]
        if isinstance(value, tuple):
            return tuple(self._normalize_scalar(item) for item in value)
        return value

    def _summarize_mismatches(
        self,
        expected_rows: list[tuple[object, ...]],
        mismatches: dict[str, list[tuple[object, ...]]],
    ) -> dict[str, object]:
        summary: dict[str, object] = {
            "expected_sample": expected_rows[:5],
        }
        for backend_name, rows in mismatches.items():
            summary[backend_name] = rows[:5]
        return summary


if __name__ == "__main__":
    unittest.main()
