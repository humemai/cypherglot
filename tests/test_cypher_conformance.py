# pyright: reportPrivateUsage=false
# pylint: disable=protected-access
"""Cross-engine Cypher conformance: run the SAME admitted-subset queries on
native Neo4j (the reference) and on the SQL backends (SQLite/DuckDB via
CypherGlot), and assert identical results.

This is the divergence guard: the rest of the suite proves CypherGlot's SQL
backends agree with each other; this proves they agree with Neo4j.

Skips cleanly when the neo4j driver or Docker is unavailable.
"""

from __future__ import annotations

import json
import shutil
import socket
import sqlite3
import unittest
from decimal import Decimal

import cypherglot
from cypherglot.schema import (
    CompilerSchemaContext,
    EdgeTypeSpec,
    GraphSchema,
    NodeTypeSpec,
    PropertyField,
)

from tests import test_runtime_olap_parity as olap_parity

_start_docker_neo4j = olap_parity._start_docker_neo4j
_stop_docker_neo4j = olap_parity._stop_docker_neo4j
_wait_for_docker_server_ready = olap_parity._wait_for_docker_server_ready
_wait_for_neo4j_driver_ready = olap_parity._wait_for_neo4j_driver_ready
_docker_default_container_name = olap_parity._docker_default_container_name
DockerNeo4jConfig = olap_parity.DockerNeo4jConfig
_neo4j_graph_database = olap_parity._neo4j_graph_database

try:
    import duckdb
except ImportError:  # pragma: no cover - optional dependency
    duckdb = None


# Output columns that carry boolean values (SQL backends store these as 0/1).
_BOOLEAN_COLUMNS = {"active"}

_GRAPH_SCHEMA = GraphSchema(
    node_types=(
        NodeTypeSpec(
            name="User",
            properties=(
                PropertyField("name", "string"),
                PropertyField("age", "integer"),
                PropertyField("active", "boolean"),
            ),
        ),
        NodeTypeSpec(
            name="Company",
            properties=(PropertyField("name", "string"),),
        ),
    ),
    edge_types=(
        EdgeTypeSpec(name="KNOWS", source_type="User", target_type="User"),
        EdgeTypeSpec(
            name="WORKS_AT",
            source_type="User",
            target_type="Company",
            properties=(PropertyField("since", "integer"),),
        ),
    ),
)

# (id, name, age, active)
_USERS = [
    (1, "Alice", 30, True),
    (2, "Bob", 25, False),
    (3, "Carol", 30, True),
    (4, "Dave", 40, False),
]
# (id, name)
_COMPANIES = [(10, "Acme"), (11, "Globex")]
# (id, from_id, to_id)
_KNOWS = [(100, 1, 2), (101, 1, 3), (102, 2, 3)]  # Dave (4) knows nobody
# (id, from_id, to_id, since)
_WORKS_AT = [(200, 1, 10, 2020), (201, 2, 11, 2019)]

# name -> (query, is_program). Only safely-comparable shapes: scalar/property
# projections, aggregates, booleans, deterministic ORDER BY. No raw entity
# returns and no id() (Neo4j ids != per-table SQL ids).
_CONFORMANCE_QUERIES: dict[str, str] = {
    "match_where_eq": "MATCH (u:User) WHERE u.age = 30 RETURN u.name AS name ORDER BY name",
    "where_in": "MATCH (u:User) WHERE u.name IN ['Alice', 'Dave'] RETURN u.name AS name ORDER BY name",
    "where_or": "MATCH (u:User) WHERE u.age < 26 OR u.age > 35 RETURN u.name AS name ORDER BY name",
    "where_and_or": "MATCH (u:User) WHERE u.active = true AND (u.name = 'Alice' OR u.name = 'Bob') RETURN u.name AS name ORDER BY name",
    "starts_with": "MATCH (u:User) WHERE u.name STARTS WITH 'A' RETURN u.name AS name ORDER BY name",
    "return_distinct": "MATCH (u:User) RETURN DISTINCT u.age AS age ORDER BY age",
    "with_distinct": "MATCH (u:User) WITH DISTINCT u.age AS age RETURN age ORDER BY age",
    "boolean_projection": "MATCH (u:User) RETURN u.name AS name, u.active AS active ORDER BY name",
    "count_star": "MATCH (u:User) RETURN count(*) AS total",
    "grouped_count": "MATCH (u:User) RETURN u.age AS age, count(*) AS c ORDER BY age",
    "aggregates": "MATCH (u:User) RETURN min(u.age) AS mn, max(u.age) AS mx, avg(u.age) AS av",
    "one_hop": "MATCH (a:User)-[:KNOWS]->(b:User) RETURN a.name AS src, b.name AS dst ORDER BY src, dst",
    "one_hop_where": "MATCH (a:User)-[:KNOWS]->(b:User) WHERE a.name = 'Alice' RETURN b.name AS friend ORDER BY friend",
    "cross_type": "MATCH (u:User)-[:WORKS_AT]->(c:Company) RETURN u.name AS person, c.name AS company ORDER BY person",
    "optional_match": "MATCH (a:User) OPTIONAL MATCH (a)-[:KNOWS]->(b:User) RETURN a.name AS name, b.name AS friend ORDER BY name, friend",
    "collect_grouped": "MATCH (a:User)-[:KNOWS]->(b:User) RETURN a.name AS name, collect(b.name) AS friends ORDER BY name",
    "variable_length": "MATCH (a:User)-[:KNOWS*1..2]->(b:User) WHERE a.name = 'Alice' RETURN b.name AS reachable ORDER BY reachable",
}


@unittest.skipIf(_neo4j_graph_database is None, "neo4j is not installed")
@unittest.skipIf(shutil.which("docker") is None, "docker is not installed")
class CypherNeo4jConformanceTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        cls.schema_context = CompilerSchemaContext.type_aware(_GRAPH_SCHEMA)

        # --- SQLite ---
        cls.sqlite = sqlite3.connect(":memory:")
        cls.sqlite.executescript("\n".join(_GRAPH_SCHEMA.ddl("sqlite")))
        cls._seed_sql(cls.sqlite.execute)
        cls.sqlite.commit()

        # --- DuckDB (optional) ---
        cls.duckdb = None
        if duckdb is not None:
            cls.duckdb = duckdb.connect()
            for statement in _GRAPH_SCHEMA.ddl("duckdb"):
                cls.duckdb.execute(statement)
            cls._seed_sql(cls.duckdb.execute)

        # --- Neo4j (Docker) ---
        cls._neo4j_password = "cypherglot-conformance"
        cls._neo4j_database = "neo4j"
        cls._neo4j_config = DockerNeo4jConfig(
            image="neo4j:5-community",
            container_name=_docker_default_container_name(),
            bolt_port=cls._free_port(),
            http_port=cls._free_port(),
            startup_timeout_s=120,
            keep_container=False,
        )
        _start_docker_neo4j(cls._neo4j_config, cls._neo4j_password)
        _wait_for_docker_server_ready(
            cls._neo4j_config, cls._neo4j_config.startup_timeout_s
        )
        cls._neo4j_driver = _wait_for_neo4j_driver_ready(
            f"bolt://127.0.0.1:{cls._neo4j_config.bolt_port}",
            "neo4j",
            cls._neo4j_password,
            cls._neo4j_config.startup_timeout_s,
        )
        cls._seed_neo4j()

    @classmethod
    def tearDownClass(cls) -> None:
        if getattr(cls, "sqlite", None) is not None:
            cls.sqlite.close()
        if getattr(cls, "duckdb", None) is not None:
            cls.duckdb.close()
        driver = getattr(cls, "_neo4j_driver", None)
        if driver is not None:
            driver.close()
        config = getattr(cls, "_neo4j_config", None)
        if config is not None:
            _stop_docker_neo4j(config)
        super().tearDownClass()

    @staticmethod
    def _free_port() -> int:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.bind(("127.0.0.1", 0))
            return int(sock.getsockname()[1])

    @staticmethod
    def _seed_sql(execute) -> None:
        for row in _USERS:
            execute(
                "INSERT INTO cg_node_user (id, name, age, active) VALUES (?, ?, ?, ?)",
                row,
            )
        for row in _COMPANIES:
            execute("INSERT INTO cg_node_company (id, name) VALUES (?, ?)", row)
        for row in _KNOWS:
            execute(
                "INSERT INTO cg_edge_knows (id, from_id, to_id) VALUES (?, ?, ?)",
                row,
            )
        for row in _WORKS_AT:
            execute(
                "INSERT INTO cg_edge_works_at (id, from_id, to_id, since) "
                "VALUES (?, ?, ?, ?)",
                row,
            )

    @classmethod
    def _seed_neo4j(cls) -> None:
        with cls._neo4j_driver.session(database=cls._neo4j_database) as session:
            session.run("MATCH (n) DETACH DELETE n")
            for node_id, name, age, active in _USERS:
                session.run(
                    "CREATE (:User {id: $id, name: $name, age: $age, active: $active})",
                    id=node_id,
                    name=name,
                    age=age,
                    active=active,
                )
            for node_id, name in _COMPANIES:
                session.run(
                    "CREATE (:Company {id: $id, name: $name})", id=node_id, name=name
                )
            for _, from_id, to_id in _KNOWS:
                session.run(
                    "MATCH (a:User {id: $f}), (b:User {id: $t}) "
                    "CREATE (a)-[:KNOWS]->(b)",
                    f=from_id,
                    t=to_id,
                )
            for _, from_id, to_id, since in _WORKS_AT:
                session.run(
                    "MATCH (a:User {id: $f}), (c:Company {id: $t}) "
                    "CREATE (a)-[:WORKS_AT {since: $since}]->(c)",
                    f=from_id,
                    t=to_id,
                    since=since,
                )

    # --- result execution + normalization ---

    def _run_sqlite(self, query: str) -> list[tuple[object, ...]]:
        sql = cypherglot.to_sql(
            query, backend="sqlite", schema_context=self.schema_context
        )
        cursor = self.sqlite.execute(sql)
        names = [d[0] for d in cursor.description or []]
        return self._canon_rows(names, cursor.fetchall())

    def _run_duckdb(self, query: str) -> list[tuple[object, ...]]:
        sql = cypherglot.to_sql(
            query, dialect="duckdb", schema_context=self.schema_context
        )
        cursor = self.duckdb.execute(sql)
        names = [d[0] for d in cursor.description or []]
        return self._canon_rows(names, cursor.fetchall())

    def _run_neo4j(self, query: str) -> list[tuple[object, ...]]:
        with self._neo4j_driver.session(database=self._neo4j_database) as session:
            result = session.run(query)
            names = list(result.keys())
            rows = [tuple(record[name] for name in names) for record in result]
        return self._canon_rows(names, rows)

    def _canon_rows(
        self, names: list[str], rows
    ) -> list[tuple[object, ...]]:
        canon = [
            tuple(
                self._canon_value(value, boolean_hint=name in _BOOLEAN_COLUMNS)
                for name, value in zip(names, row, strict=True)
            )
            for row in rows
        ]
        return sorted(canon, key=lambda row: json.dumps(row, sort_keys=True, default=str))

    def _canon_value(self, value: object, *, boolean_hint: bool = False) -> object:
        if value is None:
            return None
        if isinstance(value, str):
            stripped = value.strip()
            if stripped.startswith("[") and stripped.endswith("]"):
                try:
                    return self._canon_value(json.loads(stripped))
                except json.JSONDecodeError:
                    return value
            return value
        if isinstance(value, Decimal):
            value = float(value)
        if boolean_hint and isinstance(value, int) and value in (0, 1):
            return bool(value)
        if isinstance(value, bool):
            return value
        if isinstance(value, float):
            return int(value) if value.is_integer() else round(value, 9)
        if isinstance(value, (list, tuple)):
            # collect() order is unspecified without ORDER BY -> multiset compare.
            return sorted(
                (self._canon_value(item) for item in value),
                key=lambda item: json.dumps(item, sort_keys=True, default=str),
            )
        return value

    def test_queries_match_neo4j(self) -> None:
        for name, query in _CONFORMANCE_QUERIES.items():
            with self.subTest(query=name):
                neo4j_rows = self._run_neo4j(query)
                sqlite_rows = self._run_sqlite(query)
                self.assertEqual(
                    sqlite_rows,
                    neo4j_rows,
                    msg=f"SQLite diverges from Neo4j for {name!r}: {query}",
                )
                if self.duckdb is not None:
                    duckdb_rows = self._run_duckdb(query)
                    self.assertEqual(
                        duckdb_rows,
                        neo4j_rows,
                        msg=f"DuckDB diverges from Neo4j for {name!r}: {query}",
                    )


if __name__ == "__main__":
    unittest.main()
