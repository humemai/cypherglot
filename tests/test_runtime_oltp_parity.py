# pyright: reportPrivateUsage=false
# pylint: disable=protected-access

from __future__ import annotations

import json
import os
import shutil
import socket
import sys
import unittest
from decimal import Decimal
from typing import cast

import cypherglot

from tests import test_runtime_olap_parity as olap_parity

RuntimeScale = olap_parity.RuntimeScale
_build_graph_schema = olap_parity._build_graph_schema
_render_corpus_queries = olap_parity._render_corpus_queries
_token_map = olap_parity._token_map
_BackendRunner = olap_parity._BackendRunner
_create_managed_directory = olap_parity._create_managed_directory
_prepare_generated_graph_fixture = olap_parity._prepare_generated_graph_fixture
_execute_bound_postgresql_sql = olap_parity._execute_bound_postgresql_sql
_ladybug_available = olap_parity._ladybug_available
_prepare_ladybug_fixture = olap_parity._prepare_ladybug_fixture
_rewrite_ladybug_query = olap_parity._rewrite_ladybug_query
_arcadedb_available = olap_parity._arcadedb_available
_prepare_arcadedb_fixture = olap_parity._prepare_arcadedb_fixture
_rewrite_arcadedb_query = olap_parity._rewrite_arcadedb_query
_setup_neo4j_mode = olap_parity._setup_neo4j_mode
_start_docker_neo4j = olap_parity._start_docker_neo4j
_stop_docker_neo4j = olap_parity._stop_docker_neo4j
_wait_for_docker_server_ready = olap_parity._wait_for_docker_server_ready
_wait_for_neo4j_driver_ready = olap_parity._wait_for_neo4j_driver_ready
_docker_default_container_name = olap_parity._docker_default_container_name
DockerNeo4jConfig = olap_parity.DockerNeo4jConfig
_neo4j_graph_database = olap_parity._neo4j_graph_database
CORPUS_PATH = olap_parity.CORPUS_PATH
CorpusQuery = olap_parity._benchmark_common.CorpusQuery
_expand_query_tokens = olap_parity._benchmark_common._expand_query_tokens
_load_runtime_corpus = olap_parity._load_arcadedb_corpus

BOOLEAN_COLUMNS = {
    "active",
    "target_active",
    "peer_active",
    "edge_active",
}
SMALL_SCALE = RuntimeScale(
    node_type_count=3,
    edge_type_count=3,
    nodes_per_type=8,
    edges_per_source=2,
    ingest_batch_size=8,
    variable_hop_max=2,
)

OLTP_MUTATION_VERIFICATIONS = {
    "oltp_update_type1_score": (
        (
            "verify_update_type1_score",
            (
                "MATCH (n:%node_type_1% {name: '%node_type_1_name_3%'}) "
                "RETURN n.name AS name, n.score AS score, n.active AS active"
            ),
        ),
    ),
    "oltp_create_type1_node": (
        (
            "verify_create_type1_node",
            (
                "MATCH (n:%node_type_1% {name: '%created_type_1_name%'}) "
                "RETURN n.name AS name, n.age AS age, n.score AS score, "
                "n.active AS active"
            ),
        ),
    ),
    "oltp_create_cross_type_edge": (
        (
            "verify_create_cross_type_edge",
            (
                "MATCH (a:%node_type_1% {name: '%node_type_1_name_1%'})"
                "-[r:%edge_type_2%]->"
                "(b:%node_type_2% {name: '%node_type_2_name_1%'}) "
                "WHERE r.note = 'bench-note' "
                "RETURN a.name AS source_name, b.name AS target_name, "
                "r.weight AS weight, r.score AS score, r.active AS active, "
                "r.rank AS rank ORDER BY source_name, target_name"
            ),
        ),
    ),
    "oltp_delete_type1_edge": (
        (
            "verify_delete_type1_edge",
            (
                "MATCH (a:%node_type_1% {name: '%node_type_1_name_1%'})"
                "-[r:%edge_type_1%]->(b:%node_type_1%) "
                "RETURN b.name AS neighbor ORDER BY neighbor"
            ),
        ),
    ),
    "oltp_delete_type1_node": (
        (
            "verify_delete_type1_node",
            (
                "MATCH (n:%node_type_1% {name: '%node_type_1_name_4%'}) "
                "RETURN count(*) AS total"
            ),
        ),
    ),
    "oltp_program_create_and_link": (
        (
            "verify_program_create_and_link",
            (
                "MATCH (a:%node_type_1% {name: '%node_type_1_name_1%'})"
                "-[r:%edge_type_1%]->"
                "(b:%node_type_1% {name: '%created_type_1_peer_name%'}) "
                "RETURN a.name AS source_name, b.name AS peer_name, "
                "b.age AS age, b.score AS score, b.active AS peer_active, "
                "r.note AS note, r.weight AS weight, r.score AS edge_score, "
                "r.active AS edge_active, r.rank AS rank"
            ),
        ),
    ),
    "oltp_update_cross_type_edge_rank": (
        (
            "verify_update_cross_type_edge_rank",
            (
                "MATCH (a:%node_type_1% {name: '%node_type_1_name_2%'})"
                "-[r:%edge_type_2%]->(b:%node_type_2%) "
                "RETURN b.name AS target_name, r.rank AS rank, "
                "r.active AS active ORDER BY target_name"
            ),
        ),
    ),
}

DEFAULT_OLTP_MUTATION_SMOKE_QUERIES = (
    "oltp_update_type1_score",
    "oltp_create_type1_node",
    "oltp_delete_type1_edge",
    "oltp_program_create_and_link",
)


def _full_mutation_suite_enabled() -> bool:
    return os.environ.get("CYPHERGLOT_OLTP_MUTATION_FULL") == "1"


def _enabled_mutation_query_names() -> tuple[str, ...]:
    if _full_mutation_suite_enabled():
        return tuple(OLTP_MUTATION_VERIFICATIONS)
    return DEFAULT_OLTP_MUTATION_SMOKE_QUERIES


def _parity_progress(message: str) -> None:
    print(f"[oltp-parity] {message}", file=sys.stderr, flush=True)


class RuntimeOltpParityTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        cls._postgres_dsn = olap_parity.acquire_postgresql_test_dsn()
        cls.graph_schema, cls.edge_plans = _build_graph_schema(SMALL_SCALE)
        cls.schema_context = cypherglot.CompilerSchemaContext.type_aware(
            cls.graph_schema
        )
        cls.token_map = _token_map(SMALL_SCALE, cls.graph_schema, cls.edge_plans)
        cls.oltp_queries = [
            query
            for query in _render_corpus_queries(
                _load_runtime_corpus(CORPUS_PATH),
                cls.token_map,
            )
            if query.workload == "oltp"
        ]
        cls.oltp_read_queries = [
            query for query in cls.oltp_queries if not query.mutation
        ]
        cls.oltp_mutation_queries = {
            query.name: query for query in cls.oltp_queries if query.mutation
        }

        cls.indexed_source = _prepare_generated_graph_fixture(
            scale=SMALL_SCALE,
            graph_schema=cls.graph_schema,
            edge_plans=cls.edge_plans,
            index_mode="indexed",
        )
        cls.unindexed_source = _prepare_generated_graph_fixture(
            scale=SMALL_SCALE,
            graph_schema=cls.graph_schema,
            edge_plans=cls.edge_plans,
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
        cls.ladybug_unindexed = None
        cls.arcadedb_indexed = None
        cls.arcadedb_unindexed = None
        cls._neo4j_password = None
        cls._neo4j_database = None
        cls._neo4j_config = None
        cls._neo4j_driver = None
        cls.neo4j_indexed_read_results = None
        cls.neo4j_unindexed_read_results = None

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
        olap_parity.release_postgresql_test_dsn()
        super().tearDownClass()

    @classmethod
    def _ensure_read_matrix_ready(cls) -> None:
        if cls.ladybug_unindexed is not None:
            return
        if not _ladybug_available():
            raise unittest.SkipTest("ladybug is not installed")
        if not _arcadedb_available():
            raise unittest.SkipTest("arcadedb-embedded is not installed")

        cls.ladybug_unindexed = _prepare_ladybug_fixture(
            workload="oltp",
            graph_schema=cls.graph_schema,
            sqlite_source=cls.unindexed_source,
            db_root_dir=None,
        )
        cls.arcadedb_indexed = _prepare_arcadedb_fixture(
            workload="oltp",
            index_mode="indexed",
            graph_schema=cls.graph_schema,
            sqlite_source=cls.indexed_source,
            ingest_batch_size=SMALL_SCALE.ingest_batch_size,
            db_root_dir=None,
        )
        cls.arcadedb_unindexed = _prepare_arcadedb_fixture(
            workload="oltp",
            index_mode="unindexed",
            graph_schema=cls.graph_schema,
            sqlite_source=cls.unindexed_source,
            ingest_batch_size=SMALL_SCALE.ingest_batch_size,
            db_root_dir=None,
        )

    @classmethod
    def _ensure_neo4j_ready(cls) -> None:
        if cls._neo4j_driver is not None:
            return
        if _neo4j_graph_database is None:
            raise unittest.SkipTest("neo4j is not installed")
        if shutil.which("docker") is None:
            raise unittest.SkipTest("docker is not installed")

        cls._neo4j_password = "cypherglot-oltp-parity"
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
        cls.neo4j_indexed_read_results = cls._capture_neo4j_read_results(
            index_mode="indexed"
        )
        cls.neo4j_unindexed_read_results = cls._capture_neo4j_read_results(
            index_mode="unindexed"
        )

    @staticmethod
    def _allocate_free_port() -> int:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.bind(("127.0.0.1", 0))
            return int(sock.getsockname()[1])

    @classmethod
    def _open_runner(cls, backend: str, source) -> _BackendRunner:
        work_dir = _create_managed_directory(
            root_dir=None,
            prefix=f"oltp-parity-{backend}-",
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
    def _render_query(cls, name: str, text: str) -> CorpusQuery:
        return CorpusQuery(
            name=name,
            workload="oltp",
            category="verification",
            backends=(
                "sqlite",
                "duckdb",
                "postgresql",
                "ladybug",
                "arcadedb-embedded",
                "neo4j",
            ),
            mode="statement",
            mutation=False,
            query=_expand_query_tokens(text, cls.token_map),
        )

    @classmethod
    def _capture_neo4j_read_results(
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
        for query in cls.oltp_read_queries:
            column_names, rows = cls._execute_neo4j(query)
            results[query.name] = cls()._canonize_rows(column_names, rows)
        return results

    @classmethod
    def _open_mutation_matrix(cls) -> dict[str, object]:
        return {
            "sqlite_indexed": cls._open_runner("sqlite", cls.indexed_source),
            "duckdb": cls._open_runner("duckdb", cls.indexed_source),
            "postgresql_indexed": cls._open_runner("postgresql", cls.indexed_source),
        }

    @staticmethod
    def _close_mutation_matrix(matrix: dict[str, object]) -> None:
        for value in matrix.values():
            value.close()

    def test_oltp_read_results_match_across_backends(self) -> None:
        self._ensure_read_matrix_ready()
        self._ensure_neo4j_ready()
        for query in self.oltp_read_queries:
            with self.subTest(query=query.name):
                column_names, sqlite_indexed_rows = self._execute_sql_runner(
                    self.sqlite_indexed,
                    query,
                )
                expected_rows = self._canonize_rows(column_names, sqlite_indexed_rows)

                backend_rows = {
                    "sqlite_unindexed": self._canonize_rows(
                        column_names,
                        self._execute_sql_runner(self.sqlite_unindexed, query)[1],
                    ),
                    "duckdb": self._canonize_rows(
                        column_names,
                        self._execute_sql_runner(self.duckdb, query)[1],
                    ),
                    "postgresql_indexed": self._canonize_rows(
                        column_names,
                        self._execute_sql_runner(self.postgresql_indexed, query)[1],
                    ),
                    "postgresql_unindexed": self._canonize_rows(
                        column_names,
                        self._execute_sql_runner(self.postgresql_unindexed, query)[1],
                    ),
                    "ladybug_unindexed": self._canonize_rows(
                        column_names,
                        self._execute_ladybug_read(self.ladybug_unindexed, query),
                    ),
                    "arcadedb_indexed": self._canonize_rows(
                        column_names,
                        self._execute_arcadedb_read(
                            self.arcadedb_indexed,
                            query,
                            column_names,
                        ),
                    ),
                    "arcadedb_unindexed": self._canonize_rows(
                        column_names,
                        self._execute_arcadedb_read(
                            self.arcadedb_unindexed,
                            query,
                            column_names,
                        ),
                    ),
                    "neo4j_indexed": self._require_cached_read_result(
                        self.neo4j_indexed_read_results,
                        query.name,
                    ),
                    "neo4j_unindexed": self._require_cached_read_result(
                        self.neo4j_unindexed_read_results,
                        query.name,
                    ),
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
                    msg=(
                        f"OLTP read parity mismatch for {query.name}: "
                        f"{mismatch_summary}"
                    ),
                )

    def _assert_oltp_mutation_postconditions_match(self, query_name: str) -> None:
        verification_specs = OLTP_MUTATION_VERIFICATIONS[query_name]
        mutation_query = self.oltp_mutation_queries[query_name]
        verification_queries = [
            self._render_query(name, text) for name, text in verification_specs
        ]

        _parity_progress(f"{query_name}: opening backend matrix")
        matrix = self._open_mutation_matrix()
        try:
            _parity_progress(f"{query_name}: applying sqlite/indexed mutation")
            self._execute_sql_mutation(matrix["sqlite_indexed"], mutation_query)
            _parity_progress(f"{query_name}: applying duckdb mutation")
            self._execute_sql_mutation(matrix["duckdb"], mutation_query)
            _parity_progress(f"{query_name}: applying postgresql/indexed mutation")
            self._execute_sql_mutation(
                matrix["postgresql_indexed"],
                mutation_query,
            )

            for verification_query in verification_queries:
                _parity_progress(
                    f"{query_name}: comparing postconditions for "
                    f"{verification_query.name}"
                )
                with self.subTest(
                    mutation=query_name,
                    verification=verification_query.name,
                ):
                    column_names, sqlite_indexed_rows = self._execute_sql_runner(
                        matrix["sqlite_indexed"],
                        verification_query,
                    )
                    expected_rows = self._canonize_rows(
                        column_names,
                        sqlite_indexed_rows,
                    )
                    backend_rows = {
                        "duckdb": self._canonize_rows(
                            column_names,
                            self._execute_sql_runner(
                                matrix["duckdb"],
                                verification_query,
                            )[1],
                        ),
                        "postgresql_indexed": self._canonize_rows(
                            column_names,
                            self._execute_sql_runner(
                                matrix["postgresql_indexed"],
                                verification_query,
                            )[1],
                        ),
                    }

                    mismatches = {
                        backend_name: rows
                        for backend_name, rows in backend_rows.items()
                        if rows != expected_rows
                    }
                    mismatch_summary = json.dumps(
                        self._summarize_mismatches(
                            expected_rows,
                            mismatches,
                        ),
                        sort_keys=True,
                    )
                    self.assertEqual(
                        mismatches,
                        {},
                        msg=(
                            f"OLTP mutation parity mismatch for "
                            f"{query_name}/{verification_query.name}: "
                            f"{mismatch_summary}"
                        ),
                    )
        finally:
            _parity_progress(f"{query_name}: closing backend matrix")
            self._close_mutation_matrix(matrix)

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

    def _execute_sql_mutation(self, runner: _BackendRunner, query) -> None:
        artifact = runner.compile_query(query)
        runner.execute_query(artifact)

    def _execute_ladybug_read(self, fixture, query) -> list[list[object]]:
        statement = _rewrite_ladybug_query(fixture, query)
        return list(fixture.connection.execute(statement))

    def _execute_ladybug_mutation(self, fixture, query) -> None:
        fixture.connection.execute("BEGIN TRANSACTION")
        try:
            statement = _rewrite_ladybug_query(fixture, query)
            list(fixture.connection.execute(statement))
            fixture.connection.execute("COMMIT")
        except Exception:
            fixture.connection.execute("ROLLBACK")
            raise

    def _execute_arcadedb_read(
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

    def _execute_arcadedb_mutation(self, fixture, query) -> None:
        statement = _rewrite_arcadedb_query(fixture, query)
        fixture.database.begin()
        try:
            result = fixture.database.command("opencypher", statement)
            if result is not None:
                list(result)
            fixture.database.commit()
        except Exception:
            fixture.database.rollback()
            raise

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

    @classmethod
    def _execute_neo4j_mutation(cls, query) -> None:
        with cls._neo4j_driver.session(database=cls._neo4j_database) as session:
            transaction = session.begin_transaction()
            try:
                result = transaction.run(query.query)
                list(result)
                transaction.commit()
            except Exception:
                transaction.rollback()
                raise

    @classmethod
    def _run_neo4j_mutation_and_verify(
        cls,
        mutation_query,
        verification_queries: list[CorpusQuery],
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
        cls._execute_neo4j_mutation(mutation_query)
        results: dict[str, list[tuple[object, ...]]] = {}
        for verification_query in verification_queries:
            column_names, rows = cls._execute_neo4j(verification_query)
            results[verification_query.name] = cls()._canonize_rows(column_names, rows)
        return results

    @staticmethod
    def _require_cached_read_result(
        cached_results: object,
        query_name: str,
    ) -> list[tuple[object, ...]]:
        typed_results = cast(dict[str, list[tuple[object, ...]]], cached_results)
        rows = typed_results.get(query_name)
        if rows is None:
            raise KeyError(query_name)
        return rows

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


def _make_mutation_test(query_name: str):
    def _test(self: RuntimeOltpParityTests) -> None:
        self._assert_oltp_mutation_postconditions_match(query_name)

    _test.__name__ = f"test_oltp_mutation_{query_name}"
    return _test


for _query_name in _enabled_mutation_query_names():
    setattr(
        RuntimeOltpParityTests,
        f"test_oltp_mutation_{_query_name}",
        _make_mutation_test(_query_name),
    )


if __name__ == "__main__":
    unittest.main()
