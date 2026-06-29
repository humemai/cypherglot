# pyright: reportPrivateUsage=false
# pylint: disable=protected-access
"""Native-Cypher-on-PostgreSQL conformance: run the SAME admitted-subset queries
on Apache AGE (openCypher run natively inside PostgreSQL) and on SQLite via
CypherGlot, and assert identical results.

AGE is a *baseline* — CypherGlot does not lower for it; AGE runs the Cypher
directly. Since CypherGlot's SQL backends are proven equal to the SQLite
reference elsewhere, AGE == SQLite here is the marquee result: native graph
extension and lowered SQL agree on the same engine family (PostgreSQL).

Skips cleanly when psycopg2 or Docker is unavailable.
"""

from __future__ import annotations

import json
import shutil
import sqlite3
import unittest
from decimal import Decimal

import cypherglot
from cypherglot.schema import CompilerSchemaContext

from tests._age_runtime_support import acquire_age_test_dsn, release_age_test_dsn
from tests.test_cypher_conformance import (
    _BOOLEAN_COLUMNS,
    _COMPANIES,
    _CONFORMANCE_QUERIES,
    _GRAPH_SCHEMA,
    _KNOWS,
    _USERS,
    _WORKS_AT,
)
from scripts.benchmarks.runtime.age import (
    AGE_UNSUPPORTED_QUERIES,
    _prepare_age_cursor,
    create_age_labels,
    execute_age_query,
    seed_age_edge,
    seed_age_node,
    setup_age,
)

try:
    import psycopg2
except ImportError:  # pragma: no cover - optional test dependency
    psycopg2 = None  # type: ignore[assignment]


@unittest.skipIf(psycopg2 is None, "psycopg2 is not installed")
@unittest.skipIf(shutil.which("docker") is None, "docker is not installed")
class CypherAgeConformanceTests(unittest.TestCase):
    _graph_name = "cypherglot_conformance"

    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        cls.schema_context = CompilerSchemaContext.type_aware(_GRAPH_SCHEMA)
        cls._age_dsn = acquire_age_test_dsn()

        # --- SQLite reference ---
        cls.sqlite = sqlite3.connect(":memory:")
        cls.sqlite.executescript("\n".join(_GRAPH_SCHEMA.ddl("sqlite")))
        cls._seed_sqlite()
        cls.sqlite.commit()

        # --- Apache AGE (Docker) ---
        cls.age = psycopg2.connect(cls._age_dsn)
        cls.age.autocommit = False
        setup_age(cls.age, cls._graph_name)
        create_age_labels(cls.age, cls._graph_name, _GRAPH_SCHEMA)
        cls._seed_age()

    @classmethod
    def tearDownClass(cls) -> None:
        if getattr(cls, "sqlite", None) is not None:
            cls.sqlite.close()
        age = getattr(cls, "age", None)
        if age is not None and not age.closed:
            age.close()
        release_age_test_dsn()
        super().tearDownClass()

    @classmethod
    def _seed_sqlite(cls) -> None:
        for row in _USERS:
            cls.sqlite.execute(
                "INSERT INTO cg_node_user (id, name, age, active) VALUES (?, ?, ?, ?)",
                row,
            )
        for row in _COMPANIES:
            cls.sqlite.execute(
                "INSERT INTO cg_node_company (id, name) VALUES (?, ?)", row
            )
        for row in _KNOWS:
            cls.sqlite.execute(
                "INSERT INTO cg_edge_knows (id, from_id, to_id) VALUES (?, ?, ?)", row
            )
        for row in _WORKS_AT:
            cls.sqlite.execute(
                "INSERT INTO cg_edge_works_at (id, from_id, to_id, since) "
                "VALUES (?, ?, ?, ?)",
                row,
            )

    @classmethod
    def _seed_age(cls) -> None:
        with _prepare_age_cursor(cls.age) as cur:
            for node_id, name, age, active in _USERS:
                seed_age_node(
                    cur,
                    cls._graph_name,
                    "User",
                    {"id": node_id, "name": name, "age": age, "active": active},
                )
            for node_id, name in _COMPANIES:
                seed_age_node(
                    cur, cls._graph_name, "Company", {"id": node_id, "name": name}
                )
            for _, from_id, to_id in _KNOWS:
                seed_age_edge(
                    cur,
                    cls._graph_name,
                    edge_label="KNOWS",
                    source_label="User",
                    source_id=from_id,
                    target_label="User",
                    target_id=to_id,
                )
            for _, from_id, to_id, since in _WORKS_AT:
                seed_age_edge(
                    cur,
                    cls._graph_name,
                    edge_label="WORKS_AT",
                    source_label="User",
                    source_id=from_id,
                    target_label="Company",
                    target_id=to_id,
                    properties={"since": since},
                )
        cls.age.commit()

    def _run_sqlite(self, query: str) -> list[tuple[object, ...]]:
        sql = cypherglot.to_sql(
            query, backend="sqlite", schema_context=self.schema_context
        )
        cursor = self.sqlite.execute(sql)
        names = [d[0] for d in cursor.description or []]
        return self._canon_rows(names, cursor.fetchall())

    def _run_age(self, query: str) -> list[tuple[object, ...]]:
        with _prepare_age_cursor(self.age) as cur:
            names, rows = execute_age_query(
                cur, self._graph_name, query, self.schema_context
            )
        return self._canon_rows(names, rows)

    def _canon_rows(self, names: list[str], rows) -> list[tuple[object, ...]]:
        canon = [
            tuple(
                self._canon_value(value, boolean_hint=name in _BOOLEAN_COLUMNS)
                for name, value in zip(names, row, strict=True)
            )
            for row in rows
        ]
        return sorted(
            canon, key=lambda row: json.dumps(row, sort_keys=True, default=str)
        )

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
            return sorted(
                (self._canon_value(item) for item in value),
                key=lambda item: json.dumps(item, sort_keys=True, default=str),
            )
        return value

    def test_queries_match_age(self) -> None:
        ran = 0
        for name, query in _CONFORMANCE_QUERIES.items():
            if name in AGE_UNSUPPORTED_QUERIES:
                continue
            with self.subTest(query=name):
                age_rows = self._run_age(query)
                sqlite_rows = self._run_sqlite(query)
                self.assertEqual(
                    sqlite_rows,
                    age_rows,
                    msg=f"SQLite diverges from AGE for {name!r}: {query}",
                )
                ran += 1
        self.assertGreater(ran, 0, "no AGE conformance queries ran")


if __name__ == "__main__":
    unittest.main()
