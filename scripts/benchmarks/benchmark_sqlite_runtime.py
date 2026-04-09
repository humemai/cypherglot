from __future__ import annotations

import argparse
import gc
import json
import platform
import sqlite3
import tempfile
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import duckdb
import cypherglot


DuckDBConnection = Any


REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_OUTPUT_PATH = (
    REPO_ROOT
    / "scripts"
    / "benchmarks"
    / "results"
    / "sqlite_runtime_benchmark_baseline.json"
)

SQLITE_SAVEPOINT = "benchmark_iteration"


@dataclass(frozen=True, slots=True)
class BenchmarkQuery:
    name: str
    workload: str
    category: str
    query: str
    backends: tuple[str, ...]
    mode: str = "statement"
    mutation: bool = False


@dataclass(frozen=True, slots=True)
class PreparedArtifact:
    mode: str
    compiled: str | cypherglot.RenderedCypherProgram


@dataclass(frozen=True, slots=True)
class GraphScale:
    user_count: int = 100_000
    company_count: int = 1_000
    knows_edges_per_user: int = 3

    @property
    def node_count(self) -> int:
        return self.user_count + self.company_count

    @property
    def edge_count(self) -> int:
        return self.user_count * (1 + self.knows_edges_per_user)


@dataclass(slots=True)
class SharedSQLiteFixture:
    temp_dir: tempfile.TemporaryDirectory[str]
    db_path: Path
    setup_metrics: dict[str, int]
    row_counts: dict[str, int]

    def close(self) -> None:
        self.temp_dir.cleanup()


OLTP_QUERIES: tuple[BenchmarkQuery, ...] = (
    BenchmarkQuery(
        name="oltp_user_point_lookup",
        workload="oltp",
        category="point-read",
        query=(
            "MATCH (u:User) WHERE u.name = 'user-000001' "
            "RETURN u.name AS name, u.age AS age ORDER BY name LIMIT 1"
        ),
        backends=("sqlite",),
    ),
    BenchmarkQuery(
        name="oltp_user_age_filter",
        workload="oltp",
        category="point-read",
        query=(
            "MATCH (u:User) WHERE u.age = 30 RETURN u.name AS name "
            "ORDER BY name LIMIT 5"
        ),
        backends=("sqlite",),
    ),
    BenchmarkQuery(
        name="oltp_optional_miss",
        workload="oltp",
        category="optional-read",
        query=(
            "OPTIONAL MATCH (u:User) WHERE u.name = 'missing-user' "
            "RETURN u.name AS name"
        ),
        backends=("sqlite",),
    ),
    BenchmarkQuery(
        name="oltp_friends_of_user",
        workload="oltp",
        category="adjacency-read",
        query=(
            "MATCH (a:User)-[r:KNOWS]->(b:User) WHERE a.name = 'user-000001' "
            "RETURN b.name AS friend ORDER BY friend LIMIT 10"
        ),
        backends=("sqlite",),
    ),
    BenchmarkQuery(
        name="oltp_company_for_user",
        workload="oltp",
        category="lookup-read",
        query=(
            "MATCH (u:User)-[r:WORKS_AT]->(c:Company) WHERE u.name = 'user-000001' "
            "RETURN c.name AS company ORDER BY company LIMIT 1"
        ),
        backends=("sqlite",),
    ),
    BenchmarkQuery(
        name="oltp_user_score_lookup",
        workload="oltp",
        category="ordered-read",
        query=(
            "MATCH (u:User) WHERE u.active = true RETURN u.name AS name, "
            "u.score AS score ORDER BY score DESC LIMIT 10"
        ),
        backends=("sqlite",),
    ),
    BenchmarkQuery(
        name="oltp_relationship_note_lookup",
        workload="oltp",
        category="relationship-read",
        query=(
            "MATCH (a:User)-[r:KNOWS]->(b:User) WHERE a.name = 'user-000005' "
            "RETURN r.note AS note, b.name AS friend ORDER BY friend LIMIT 5"
        ),
        backends=("sqlite",),
    ),
    BenchmarkQuery(
        name="oltp_bounded_two_hop",
        workload="oltp",
        category="bounded-traversal",
        query=(
            "MATCH (a:User)-[:KNOWS*1..2]->(b:User) WHERE a.name = 'user-000003' "
            "RETURN b.name AS friend ORDER BY friend LIMIT 10"
        ),
        backends=("sqlite",),
    ),
    BenchmarkQuery(
        name="oltp_case_projection",
        workload="oltp",
        category="projection-read",
        query=(
            "MATCH (u:User) WHERE u.name = 'user-000010' "
            "RETURN CASE WHEN u.age >= 18 THEN u.name ELSE 'minor' END AS label"
        ),
        backends=("sqlite",),
    ),
    BenchmarkQuery(
        name="oltp_with_rebound_projection",
        workload="oltp",
        category="with-read",
        query=(
            "MATCH (u:User) WITH u AS person, u.name AS name "
            "RETURN person.name AS display_name, name AS rebound_name "
            "ORDER BY display_name LIMIT 10"
        ),
        backends=("sqlite",),
    ),
    BenchmarkQuery(
        name="oltp_create_user_program",
        workload="oltp",
        category="create",
        query=(
            "MATCH (u:User) WHERE u.name = 'user-000001' "
            "CREATE (u)-[:KNOWS {note: 'fresh link', weight: 1.25, "
            "score: 2.5, active: true}]->"
            "(:User {name: 'user-bench-created', age: 29, score: 3.5, active: true})"
        ),
        backends=("sqlite",),
        mode="program",
        mutation=True,
    ),
    BenchmarkQuery(
        name="oltp_update_user_score",
        workload="oltp",
        category="update",
        query=(
            "MATCH (u:User) WHERE u.name = 'user-000001' "
            "SET u.score = 9.75, u.active = false"
        ),
        backends=("sqlite",),
        mutation=True,
    ),
    BenchmarkQuery(
        name="oltp_delete_knows_relationship",
        workload="oltp",
        category="delete",
        query=(
            "MATCH (a:User)-[r:KNOWS]->(b:User) "
            "WHERE a.name = 'user-000001' AND b.name = 'user-000002' "
            "DELETE r"
        ),
        backends=("sqlite",),
        mutation=True,
    ),
)


OLAP_QUERIES: tuple[BenchmarkQuery, ...] = (
    BenchmarkQuery(
        name="olap_user_count",
        workload="olap",
        category="aggregate",
        query="MATCH (u:User) RETURN count(*) AS total",
        backends=("sqlite", "duckdb"),
    ),
    BenchmarkQuery(
        name="olap_users_per_company",
        workload="olap",
        category="grouped-count",
        query=(
            "MATCH (u:User)-[r:WORKS_AT]->(c:Company) RETURN c.name AS company, "
            "count(u) AS total ORDER BY total DESC, company ASC"
        ),
        backends=("sqlite", "duckdb"),
    ),
    BenchmarkQuery(
        name="olap_avg_score_per_name",
        workload="olap",
        category="grouped-average",
        query=(
            "MATCH (u:User) RETURN u.name AS name, avg(u.score) AS mean "
            "ORDER BY mean DESC"
        ),
        backends=("sqlite", "duckdb"),
    ),
    BenchmarkQuery(
        name="olap_min_max_age",
        workload="olap",
        category="extrema",
        query="MATCH (u:User) RETURN min(u.age) AS min_age, max(u.age) AS max_age",
        backends=("sqlite", "duckdb"),
    ),
    BenchmarkQuery(
        name="olap_age_math_projection",
        workload="olap",
        category="math",
        query=(
            "MATCH (u:User) RETURN abs(u.age) AS magnitude, sign(u.age) AS sign_value, "
            "toInteger(u.age) AS age_int, toFloat(u.age) AS age_float "
            "ORDER BY magnitude, sign_value, age_int, age_float"
        ),
        backends=("sqlite", "duckdb"),
    ),
    BenchmarkQuery(
        name="olap_score_math_projection",
        workload="olap",
        category="math",
        query=(
            "MATCH (u:User) RETURN round(u.score) AS rounded, "
            "ceil(u.score) AS ceil_score, "
            "floor(u.score) AS floor_score ORDER BY rounded, ceil_score, floor_score"
        ),
        backends=("sqlite", "duckdb"),
    ),
    BenchmarkQuery(
        name="olap_predicates",
        workload="olap",
        category="predicate",
        query=(
            "MATCH (u:User) RETURN u.age >= 18 AS adult, "
            "u.name CONTAINS 'user-' AS has_token, "
            "u.name STARTS WITH 'user-00' AS has_prefix, "
            "u.name ENDS WITH '5' AS has_suffix "
            "ORDER BY adult, has_token, has_prefix, has_suffix"
        ),
        backends=("sqlite", "duckdb"),
    ),
    BenchmarkQuery(
        name="olap_relationship_stats",
        workload="olap",
        category="relationship-aggregate",
        query=(
            "MATCH (a:User)-[r:KNOWS]->(b:User) RETURN a.name AS name, "
            "count(r) AS total "
            "ORDER BY total DESC"
        ),
        backends=("sqlite", "duckdb"),
    ),
    BenchmarkQuery(
        name="olap_relationship_projection",
        workload="olap",
        category="relationship-projection",
        query=(
            "MATCH (a:User)-[r:KNOWS]->(b:User) RETURN lower(r.note) AS lower_note, "
            "size(r.note) AS note_len, abs(r.weight) AS weight_abs, "
            "toBoolean(r.active) AS rel_active ORDER BY lower_note, "
            "note_len, weight_abs, rel_active"
        ),
        backends=("sqlite", "duckdb"),
    ),
    BenchmarkQuery(
        name="olap_bounded_reachability",
        workload="olap",
        category="bounded-traversal",
        query=(
            "MATCH (a:User)-[:KNOWS*1..2]->(b:User) WHERE a.name = 'user-000001' "
            "RETURN b.name AS friend ORDER BY friend"
        ),
        backends=("sqlite", "duckdb"),
    ),
)


ALL_QUERIES = OLTP_QUERIES + OLAP_QUERIES


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Benchmark CypherGlot compile and execution latency for predefined "
            "OLTP SQLite and OLAP SQLite/DuckDB workloads."
        )
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=DEFAULT_OUTPUT_PATH,
        help="Path to the JSON file where benchmark results will be written.",
    )
    parser.add_argument(
        "--iterations",
        type=int,
        default=1000,
        help="Measured iterations to run per query.",
    )
    parser.add_argument(
        "--warmup",
        type=int,
        default=10,
        help="Warmup iterations to run per query before measuring.",
    )
    parser.add_argument(
        "--query-name",
        action="append",
        dest="query_names",
        help=(
            "Optional benchmark query name to run. Repeat the flag to run multiple "
            "named workload entries."
        ),
    )
    parser.add_argument(
        "--skip-duckdb",
        action="store_true",
        help="Skip the DuckDB OLAP backend even if the package is installed.",
    )
    parser.add_argument(
        "--user-count",
        type=int,
        default=100_000,
        help="Synthetic user-node count to ingest before benchmarking.",
    )
    parser.add_argument(
        "--company-count",
        type=int,
        default=1_000,
        help="Synthetic company-node count to ingest before benchmarking.",
    )
    parser.add_argument(
        "--knows-edges-per-user",
        type=int,
        default=3,
        help="Synthetic KNOWS edges to ingest per user.",
    )
    parser.add_argument(
        "--ingest-batch-size",
        type=int,
        default=5_000,
        help="Rows per executemany batch during synthetic graph ingestion.",
    )
    return parser.parse_args()


def _select_queries(
    queries: tuple[BenchmarkQuery, ...],
    query_names: list[str] | None,
) -> list[BenchmarkQuery]:
    if not query_names:
        return list(queries)

    requested = set(query_names)
    selected = [query for query in queries if query.name in requested]
    found = {query.name for query in selected}
    missing = sorted(requested - found)
    if missing:
        raise ValueError("Unknown benchmark query name(s): " + ", ".join(missing))
    return selected


def _percentile(sorted_values: list[int], percentile: float) -> float:
    if not sorted_values:
        raise ValueError("Cannot compute a percentile from an empty sample.")
    if len(sorted_values) == 1:
        return float(sorted_values[0])

    position = (len(sorted_values) - 1) * (percentile / 100.0)
    lower_index = int(position)
    upper_index = min(lower_index + 1, len(sorted_values) - 1)
    lower_value = sorted_values[lower_index]
    upper_value = sorted_values[upper_index]
    fraction = position - lower_index
    return lower_value + (upper_value - lower_value) * fraction


def _summarize(latencies_ns: list[int]) -> dict[str, float]:
    sorted_ns = sorted(latencies_ns)
    return {
        "min_ms": min(sorted_ns) / 1_000_000.0,
        "mean_ms": sum(sorted_ns) / len(sorted_ns) / 1_000_000.0,
        "p50_ms": _percentile(sorted_ns, 50) / 1_000_000.0,
        "p95_ms": _percentile(sorted_ns, 95) / 1_000_000.0,
        "p99_ms": _percentile(sorted_ns, 99) / 1_000_000.0,
        "max_ms": max(sorted_ns) / 1_000_000.0,
    }


def _measure_ns(callback: Any) -> tuple[Any, int]:
    started_ns = time.perf_counter_ns()
    result = callback()
    return result, time.perf_counter_ns() - started_ns


def _format_user_name(user_index: int) -> str:
    return f"user-{user_index:06d}"


def _format_company_name(company_index: int) -> str:
    return f"company-{company_index:04d}"


def _iter_company_rows(
    scale: GraphScale,
) -> tuple[list[int], list[tuple[int, str]], list[tuple[int, str]]]:
    company_ids: list[int] = []
    node_rows: list[tuple[int, str]] = []
    label_rows: list[tuple[int, str]] = []
    for company_index in range(1, scale.company_count + 1):
        company_id = scale.user_count + company_index
        company_ids.append(company_id)
        node_rows.append(
            (
                company_id,
                json.dumps(
                    {
                        "name": _format_company_name(company_index),
                        "region": f"region-{(company_index % 12) + 1}",
                    },
                    separators=(",", ":"),
                ),
            )
        )
        label_rows.append((company_id, "Company"))
    return company_ids, node_rows, label_rows


def _iter_user_rows(
    scale: GraphScale,
    company_ids: list[int],
) -> tuple[
    list[tuple[int, str]],
    list[tuple[int, str]],
    list[tuple[int, str, int, int, str]],
]:
    node_rows: list[tuple[int, str]] = []
    label_rows: list[tuple[int, str]] = []
    edge_rows: list[tuple[int, str, int, int, str]] = []
    edge_id = 1
    for user_index in range(1, scale.user_count + 1):
        user_id = user_index
        node_rows.append(
            (
                user_id,
                json.dumps(
                    {
                        "name": _format_user_name(user_index),
                        "age": 20 + (user_index % 25),
                        "score": round(1.0 + ((user_index * 7) % 40) / 10.0, 2),
                        "active": user_index % 3 != 0,
                    },
                    separators=(",", ":"),
                ),
            )
        )
        label_rows.append((user_id, "User"))

        company_id = company_ids[(user_index - 1) % len(company_ids)]
        edge_rows.append(
            (
                edge_id,
                "WORKS_AT",
                user_id,
                company_id,
                json.dumps(
                    {
                        "since": 2015 + (user_index % 8),
                        "active": user_index % 5 != 0,
                    },
                    separators=(",", ":"),
                ),
            )
        )
        edge_id += 1

        for offset in range(1, scale.knows_edges_per_user + 1):
            target_id = ((user_index - 1 + offset) % scale.user_count) + 1
            edge_rows.append(
                (
                    edge_id,
                    "KNOWS",
                    user_id,
                    target_id,
                    json.dumps(
                        {
                            "note": f"friend link {offset}",
                            "weight": round(
                                0.5 + ((user_index + offset) % 7) * 0.4,
                                2,
                            ),
                            "score": round(
                                1.0 + ((user_index + offset) % 9) * 0.35,
                                2,
                            ),
                            "active": (user_index + offset) % 2 == 0,
                        },
                        separators=(",", ":"),
                    ),
                )
            )
            edge_id += 1

    return node_rows, label_rows, edge_rows


def _batched(
    items: list[tuple[Any, ...]],
    batch_size: int,
) -> list[list[tuple[Any, ...]]]:
    return [
        items[index : index + batch_size] for index in range(0, len(items), batch_size)
    ]


def _create_sqlite_connection(db_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode = WAL")
    conn.execute("PRAGMA synchronous = NORMAL")
    conn.execute("PRAGMA foreign_keys = ON")
    conn.create_function("REVERSE", 1, _sqlite_reverse)
    conn.create_function("LEFT", 2, _sqlite_left)
    conn.create_function("RIGHT", 2, _sqlite_right)
    conn.create_function("SPLIT", 2, _sqlite_split)
    conn.create_function("STR_POSITION", 2, _sqlite_str_position)
    return conn


def _create_sqlite_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE TABLE nodes (
          id INTEGER PRIMARY KEY,
          properties TEXT NOT NULL DEFAULT '{}',
          CHECK (json_valid(properties)),
          CHECK (json_type(properties) = 'object')
        ) STRICT;

        CREATE TABLE edges (
          id INTEGER PRIMARY KEY,
          type TEXT NOT NULL,
          from_id INTEGER NOT NULL,
          to_id INTEGER NOT NULL,
          properties TEXT NOT NULL DEFAULT '{}',
          CHECK (json_valid(properties)),
          CHECK (json_type(properties) = 'object'),
          FOREIGN KEY (from_id) REFERENCES nodes(id) ON DELETE CASCADE,
          FOREIGN KEY (to_id) REFERENCES nodes(id) ON DELETE CASCADE
        ) STRICT;

        CREATE TABLE node_labels (
          node_id INTEGER NOT NULL,
          label TEXT NOT NULL,
          PRIMARY KEY (node_id, label),
          FOREIGN KEY (node_id) REFERENCES nodes(id) ON DELETE CASCADE
        ) STRICT;
        """
    )


def _create_sqlite_indexes(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE INDEX idx_node_labels_label_node_id ON node_labels(label, node_id);
        CREATE INDEX idx_node_labels_node_id_label ON node_labels(node_id, label);
        CREATE INDEX idx_edges_from_id ON edges(from_id);
        CREATE INDEX idx_edges_to_id ON edges(to_id);
        CREATE INDEX idx_edges_type ON edges(type);
        CREATE INDEX idx_edges_type_from_id ON edges(type, from_id);
        CREATE INDEX idx_edges_type_to_id ON edges(type, to_id);
        CREATE INDEX idx_nodes_name ON nodes(json_extract(properties, '$.name'));
        CREATE INDEX idx_nodes_age ON nodes(json_extract(properties, '$.age'));
        CREATE INDEX idx_nodes_active ON nodes(json_extract(properties, '$.active'));
        """
    )


def _seed_sqlite(
    conn: sqlite3.Connection,
    *,
    scale: GraphScale,
    batch_size: int,
) -> dict[str, int]:
    company_ids, company_nodes, company_labels = _iter_company_rows(scale)
    user_nodes, user_labels, edge_rows = _iter_user_rows(scale, company_ids)

    for batch in _batched(company_nodes + user_nodes, batch_size):
        conn.executemany("INSERT INTO nodes (id, properties) VALUES (?, ?)", batch)
    for batch in _batched(company_labels + user_labels, batch_size):
        conn.executemany(
            "INSERT INTO node_labels (node_id, label) VALUES (?, ?)",
            batch,
        )
    for batch in _batched(edge_rows, batch_size):
        conn.executemany(
            (
                "INSERT INTO edges (id, type, from_id, to_id, properties) "
                "VALUES (?, ?, ?, ?, ?)"
            ),
            batch,
        )
    conn.commit()
    return {
        "node_count": scale.node_count,
        "label_count": scale.node_count,
        "edge_count": len(edge_rows),
    }


def _analyze_sqlite(conn: sqlite3.Connection) -> None:
    conn.execute("ANALYZE")


def _prepare_shared_sqlite_fixture(
    *,
    scale: GraphScale,
    batch_size: int,
) -> SharedSQLiteFixture:
    temp_dir: tempfile.TemporaryDirectory[str] = tempfile.TemporaryDirectory(
        prefix="cypherglot-shared-sqlite-bench-"
    )
    db_path = Path(temp_dir.name) / "runtime.sqlite3"
    conn, connect_ns = _measure_ns(lambda: _create_sqlite_connection(db_path))
    try:
        _, schema_ns = _measure_ns(lambda: _create_sqlite_schema(conn))
        _, index_ns = _measure_ns(lambda: _create_sqlite_indexes(conn))
        row_counts, ingest_ns = _measure_ns(
            lambda: _seed_sqlite(
                conn,
                scale=scale,
                batch_size=batch_size,
            )
        )
        _, analyze_ns = _measure_ns(lambda: _analyze_sqlite(conn))
    finally:
        conn.close()

    return SharedSQLiteFixture(
        temp_dir=temp_dir,
        db_path=db_path,
        setup_metrics={
            "connect_ns": connect_ns,
            "schema_ns": schema_ns,
            "index_ns": index_ns,
            "ingest_ns": ingest_ns,
            "analyze_ns": analyze_ns,
        },
        row_counts=row_counts,
    )


def _create_duckdb_connection(db_path: Path) -> DuckDBConnection:
    if duckdb is None:
        raise ValueError("duckdb is not installed.")
    return duckdb.connect(str(db_path))


def _configure_duckdb_from_sqlite(
    conn: DuckDBConnection,
    sqlite_path: Path,
) -> None:
    sqlite_path_sql = str(sqlite_path).replace("'", "''")
    conn.execute("INSTALL sqlite")
    conn.execute("LOAD sqlite")
    conn.execute(f"ATTACH '{sqlite_path_sql}' AS sqlite_db (TYPE sqlite)")
    conn.execute("CREATE VIEW nodes AS SELECT * FROM sqlite_db.nodes")
    conn.execute("CREATE VIEW edges AS SELECT * FROM sqlite_db.edges")
    conn.execute("CREATE VIEW node_labels AS SELECT * FROM sqlite_db.node_labels")


def _sqlite_reverse(value: object) -> object:
    if value is None:
        return None
    return str(value)[::-1]


def _sqlite_left(value: object, count: object) -> object:
    if value is None or count is None:
        return None
    return str(value)[: int(count)]


def _sqlite_right(value: object, count: object) -> object:
    if value is None or count is None:
        return None
    count_int = int(count)
    if count_int <= 0:
        return ""
    return str(value)[-count_int:]


def _sqlite_split(value: object, delimiter: object) -> object:
    if value is None or delimiter is None:
        return None
    return json.dumps(str(value).split(str(delimiter)))


def _sqlite_str_position(haystack: object, needle: object) -> object:
    if haystack is None or needle is None:
        return None
    return str(haystack).find(str(needle)) + 1


class _BackendRunner:
    def __init__(
        self,
        backend: str,
        temp_dir: tempfile.TemporaryDirectory[str],
        *,
        scale: GraphScale,
        ingest_batch_size: int,
        sqlite_source: SharedSQLiteFixture | None = None,
    ) -> None:
        self.backend = backend
        self.temp_dir = temp_dir
        self.scale = scale
        self.ingest_batch_size = ingest_batch_size
        self.sqlite_source = sqlite_source
        self.setup_metrics: dict[str, int] = {}
        self.row_counts: dict[str, int] = {}
        self.connection: sqlite3.Connection | DuckDBConnection
        self._initialize()

    def _initialize(self) -> None:
        if self.backend == "sqlite":
            if self.sqlite_source is not None:
                self.connection, connect_ns = _measure_ns(
                    lambda: _create_sqlite_connection(self.sqlite_source.db_path)
                )
                self.setup_metrics = {
                    "connect_ns": connect_ns,
                    "schema_ns": self.sqlite_source.setup_metrics["schema_ns"],
                    "index_ns": self.sqlite_source.setup_metrics["index_ns"],
                    "ingest_ns": self.sqlite_source.setup_metrics["ingest_ns"],
                    "analyze_ns": self.sqlite_source.setup_metrics["analyze_ns"],
                }
                self.row_counts = dict(self.sqlite_source.row_counts)
                return

            db_path = Path(self.temp_dir.name) / "runtime.sqlite3"
            self.connection, self.setup_metrics["connect_ns"] = _measure_ns(
                lambda: _create_sqlite_connection(db_path)
            )
            _, self.setup_metrics["schema_ns"] = _measure_ns(
                lambda: _create_sqlite_schema(self.sqlite)
            )
            _, self.setup_metrics["index_ns"] = _measure_ns(
                lambda: _create_sqlite_indexes(self.sqlite)
            )
            self.row_counts, self.setup_metrics["ingest_ns"] = _measure_ns(
                lambda: _seed_sqlite(
                    self.sqlite,
                    scale=self.scale,
                    batch_size=self.ingest_batch_size,
                )
            )
            _, self.setup_metrics["analyze_ns"] = _measure_ns(
                lambda: _analyze_sqlite(self.sqlite)
            )
            return

        if self.backend == "duckdb":
            if self.sqlite_source is None:
                raise ValueError(
                    "duckdb runtime benchmarks require a SQLite-ingested "
                    "source database."
                )

            db_path = Path(self.temp_dir.name) / "runtime.duckdb"
            self.connection, self.setup_metrics["connect_ns"] = _measure_ns(
                lambda: _create_duckdb_connection(db_path)
            )
            _, self.setup_metrics["schema_ns"] = _measure_ns(
                lambda: _configure_duckdb_from_sqlite(
                    self.duck,
                    self.sqlite_source.db_path,
                )
            )
            self.setup_metrics["index_ns"] = 0
            self.setup_metrics["ingest_ns"] = 0
            self.row_counts = dict(self.sqlite_source.row_counts)
            return

        raise ValueError(f"Unsupported backend {self.backend!r}.")

    @property
    def sqlite(self) -> sqlite3.Connection:
        assert isinstance(self.connection, sqlite3.Connection)
        return self.connection

    @property
    def duck(self) -> DuckDBConnection:
        if duckdb is None:
            raise ValueError("duckdb is not installed.")
        assert isinstance(self.connection, duckdb.DuckDBPyConnection)
        return self.connection

    def close(self) -> None:
        self.connection.close()
        self.temp_dir.cleanup()

    def compile_query(self, query: BenchmarkQuery) -> PreparedArtifact:
        if query.mode == "statement":
            if self.backend == "duckdb":
                return PreparedArtifact(
                    mode="statement",
                    compiled=cypherglot.to_sql(query.query, dialect="duckdb"),
                )
            return PreparedArtifact(
                mode="statement",
                compiled=cypherglot.to_sql(query.query),
            )

        if self.backend != "sqlite":
            raise ValueError("Rendered program execution is only supported on SQLite.")
        return PreparedArtifact(
            mode="program",
            compiled=cypherglot.render_cypher_program_text(query.query),
        )

    def execute_query(self, artifact: PreparedArtifact) -> None:
        if artifact.mode == "statement":
            if self.backend == "sqlite":
                cursor = self.sqlite.execute(artifact.compiled)
                if cursor.description is not None:
                    cursor.fetchall()
                return

            cursor = self.duck.execute(artifact.compiled)
            if cursor.description is not None:
                cursor.fetchall()
            return

        if self.backend != "sqlite":
            raise ValueError("Rendered program execution is only supported on SQLite.")
        _execute_sqlite_program(self.sqlite, artifact.compiled, commit=False)


def _execute_sqlite_program(
    conn: sqlite3.Connection,
    program: cypherglot.RenderedCypherProgram,
    *,
    commit: bool,
) -> None:
    bindings: dict[str, object] = {}
    for step in program.steps:
        if isinstance(step, cypherglot.RenderedCypherLoop):
            rows = conn.execute(step.source, bindings).fetchall()
            for row in rows:
                loop_bindings = bindings | dict(
                    zip(step.row_bindings, row, strict=True)
                )
                for statement in step.body:
                    cursor = conn.execute(statement.sql, loop_bindings)
                    if statement.bind_columns:
                        returned = cursor.fetchone()
                        if returned is None:
                            raise ValueError(
                                "Expected bound columns from program step."
                            )
                        loop_bindings |= dict(
                            zip(statement.bind_columns, returned, strict=True)
                        )
            continue

        cursor = conn.execute(step.sql, bindings)
        if step.bind_columns:
            returned = cursor.fetchone()
            if returned is None:
                raise ValueError("Expected bound columns from benchmark statement.")
            bindings |= dict(zip(step.bind_columns, returned, strict=True))
    if commit:
        conn.commit()


def _run_iteration(
    runner: _BackendRunner,
    query: BenchmarkQuery,
) -> dict[str, int]:
    reset_ns = 0
    if query.mutation and runner.backend == "sqlite":
        runner.sqlite.execute(f"SAVEPOINT {SQLITE_SAVEPOINT}")

    try:
        total_started_ns = time.perf_counter_ns()
        artifact, compile_ns = _measure_ns(lambda: runner.compile_query(query))
        _, execute_ns = _measure_ns(lambda: runner.execute_query(artifact))
        end_to_end_ns = time.perf_counter_ns() - total_started_ns
    finally:
        if query.mutation and runner.backend == "sqlite":
            _, reset_ns = _measure_ns(lambda: _rollback_sqlite_iteration(runner.sqlite))

    return {
        "compile_ns": compile_ns,
        "execute_ns": execute_ns,
        "end_to_end_ns": end_to_end_ns,
        "reset_ns": reset_ns,
    }


def _rollback_sqlite_iteration(conn: sqlite3.Connection) -> None:
    conn.execute(f"ROLLBACK TO {SQLITE_SAVEPOINT}")
    conn.execute(f"RELEASE {SQLITE_SAVEPOINT}")


def _measure_query(
    runner: _BackendRunner,
    query: BenchmarkQuery,
    *,
    iterations: int,
    warmup: int,
) -> dict[str, object]:
    for _ in range(warmup):
        _run_iteration(runner, query)

    compile_latencies: list[int] = []
    execute_latencies: list[int] = []
    end_to_end_latencies: list[int] = []
    reset_latencies: list[int] = []

    gc_was_enabled = gc.isenabled()
    gc.disable()
    try:
        for _ in range(iterations):
            metrics = _run_iteration(runner, query)
            compile_latencies.append(metrics["compile_ns"])
            execute_latencies.append(metrics["execute_ns"])
            end_to_end_latencies.append(metrics["end_to_end_ns"])
            reset_latencies.append(metrics["reset_ns"])
    finally:
        if gc_was_enabled:
            gc.enable()

    return {
        "name": query.name,
        "workload": query.workload,
        "category": query.category,
        "backend": runner.backend,
        "mode": query.mode,
        "mutation": query.mutation,
        "compile": _summarize(compile_latencies),
        "execute": _summarize(execute_latencies),
        "end_to_end": _summarize(end_to_end_latencies),
        "reset": _summarize(reset_latencies),
    }


def _pool_summaries(
    query_results: list[dict[str, object]],
    key: str,
) -> dict[str, float]:
    if not query_results:
        return {
            "mean_of_mean_ms": 0.0,
            "mean_of_p50_ms": 0.0,
            "mean_of_p95_ms": 0.0,
            "mean_of_p99_ms": 0.0,
        }

    mean_values = [result[key]["mean_ms"] for result in query_results]
    p50_values = [result[key]["p50_ms"] for result in query_results]
    p95_values = [result[key]["p95_ms"] for result in query_results]
    p99_values = [result[key]["p99_ms"] for result in query_results]
    return {
        "mean_of_mean_ms": sum(mean_values) / len(mean_values),
        "mean_of_p50_ms": sum(p50_values) / len(p50_values),
        "mean_of_p95_ms": sum(p95_values) / len(p95_values),
        "mean_of_p99_ms": sum(p99_values) / len(p99_values),
    }


def _run_backend_suite(
    backend: str,
    queries: list[BenchmarkQuery],
    *,
    iterations: int,
    warmup: int,
    scale: GraphScale,
    ingest_batch_size: int,
    sqlite_source: SharedSQLiteFixture | None = None,
) -> dict[str, object]:
    temp_dir: tempfile.TemporaryDirectory[str] = tempfile.TemporaryDirectory(
        prefix=f"cypherglot-{backend}-bench-"
    )
    runner = _BackendRunner(
        backend,
        temp_dir,
        scale=scale,
        ingest_batch_size=ingest_batch_size,
        sqlite_source=sqlite_source,
    )
    try:
        query_results = [
            _measure_query(runner, query, iterations=iterations, warmup=warmup)
            for query in queries
        ]
        return {
            "backend": backend,
            "iterations": iterations,
            "warmup": warmup,
            "query_count": len(queries),
            "setup": {
                f"{metric[:-3]}_ms": value / 1_000_000.0
                for metric, value in runner.setup_metrics.items()
            },
            "row_counts": runner.row_counts,
            "compile": _pool_summaries(query_results, "compile"),
            "execute": _pool_summaries(query_results, "execute"),
            "end_to_end": _pool_summaries(query_results, "end_to_end"),
            "reset": _pool_summaries(query_results, "reset"),
            "queries": query_results,
        }
    finally:
        runner.close()


def _benchmark_result(
    queries: list[BenchmarkQuery],
    *,
    iterations: int,
    warmup: int,
    include_duckdb: bool,
    scale: GraphScale,
    ingest_batch_size: int,
) -> dict[str, object]:
    oltp_queries = [query for query in queries if query.workload == "oltp"]
    olap_queries = [query for query in queries if query.workload == "olap"]

    workloads: dict[str, object] = {}
    if oltp_queries:
        workloads["oltp"] = {
            "description": (
                "Selective transactional-style SQLite reads measured as Cypher "
                "compile plus SQLite execution."
            ),
            "sqlite": _run_backend_suite(
                "sqlite",
                oltp_queries,
                iterations=iterations,
                warmup=warmup,
                scale=scale,
                ingest_batch_size=ingest_batch_size,
            ),
        }

    if olap_queries:
        sqlite_source = _prepare_shared_sqlite_fixture(
            scale=scale,
            batch_size=ingest_batch_size,
        )
        try:
            workloads["olap"] = {
                "description": (
                    "Analytical read queries measured on SQLite and DuckDB with "
                    "SQLite as the single ingest source of truth."
                ),
                "sqlite": _run_backend_suite(
                    "sqlite",
                    olap_queries,
                    iterations=iterations,
                    warmup=warmup,
                    scale=scale,
                    ingest_batch_size=ingest_batch_size,
                    sqlite_source=sqlite_source,
                ),
            }
            if include_duckdb:
                workloads["olap"]["duckdb"] = _run_backend_suite(
                    "duckdb",
                    olap_queries,
                    iterations=iterations,
                    warmup=warmup,
                    scale=scale,
                    ingest_batch_size=ingest_batch_size,
                    sqlite_source=sqlite_source,
                )
        finally:
            sqlite_source.close()

    return {"workloads": workloads}


def _print_suite(name: str, suite: dict[str, object]) -> None:
    print(name)
    setup_parts = [
        f"connect={suite['setup']['connect_ms']:.2f} ms",
        f"schema={suite['setup']['schema_ms']:.2f} ms",
        f"index={suite['setup']['index_ms']:.2f} ms",
        f"ingest={suite['setup']['ingest_ms']:.2f} ms",
    ]
    if "analyze_ms" in suite["setup"]:
        setup_parts.append(f"analyze={suite['setup']['analyze_ms']:.2f} ms")
    print("  setup: " + ", ".join(setup_parts))
    print(
        "  pooled compile: "
        f"mean={suite['compile']['mean_of_mean_ms']:.2f} ms, "
        f"p50={suite['compile']['mean_of_p50_ms']:.2f} ms, "
        f"p95={suite['compile']['mean_of_p95_ms']:.2f} ms"
    )
    print(
        "  pooled execute: "
        f"mean={suite['execute']['mean_of_mean_ms']:.2f} ms, "
        f"p50={suite['execute']['mean_of_p50_ms']:.2f} ms, "
        f"p95={suite['execute']['mean_of_p95_ms']:.2f} ms"
    )
    print(
        "  pooled end-to-end: "
        f"mean={suite['end_to_end']['mean_of_mean_ms']:.2f} ms, "
        f"p50={suite['end_to_end']['mean_of_p50_ms']:.2f} ms, "
        f"p95={suite['end_to_end']['mean_of_p95_ms']:.2f} ms"
    )
    for query_result in suite["queries"]:
        print(
            "    - "
            f"{query_result['name']} [{query_result['category']}]: "
            f"compile_mean={query_result['compile']['mean_ms']:.2f} ms, "
            f"compile_p50={query_result['compile']['p50_ms']:.2f} ms, "
            f"compile_p95={query_result['compile']['p95_ms']:.2f} ms, "
            f"compile_p99={query_result['compile']['p99_ms']:.2f} ms, "
            f"execute_mean={query_result['execute']['mean_ms']:.2f} ms, "
            f"execute_p50={query_result['execute']['p50_ms']:.2f} ms, "
            f"execute_p95={query_result['execute']['p95_ms']:.2f} ms, "
            f"execute_p99={query_result['execute']['p99_ms']:.2f} ms, "
            f"end_to_end_mean={query_result['end_to_end']['mean_ms']:.2f} ms, "
            f"end_to_end_p50={query_result['end_to_end']['p50_ms']:.2f} ms, "
            f"end_to_end_p95={query_result['end_to_end']['p95_ms']:.2f} ms, "
            f"end_to_end_p99={query_result['end_to_end']['p99_ms']:.2f} ms"
        )


def main() -> int:
    args = _parse_args()
    if args.iterations <= 0:
        raise ValueError("--iterations must be positive.")
    if args.warmup < 0:
        raise ValueError("--warmup must be zero or positive.")
    if args.user_count <= 0:
        raise ValueError("--user-count must be positive.")
    if args.company_count <= 0:
        raise ValueError("--company-count must be positive.")
    if args.knows_edges_per_user < 0:
        raise ValueError("--knows-edges-per-user must be zero or positive.")
    if args.ingest_batch_size <= 0:
        raise ValueError("--ingest-batch-size must be positive.")

    include_duckdb = not args.skip_duckdb
    if include_duckdb and duckdb is None:
        raise ValueError("duckdb is not installed. Install it or pass --skip-duckdb.")

    queries = _select_queries(ALL_QUERIES, args.query_names)
    scale = GraphScale(
        user_count=args.user_count,
        company_count=args.company_count,
        knows_edges_per_user=args.knows_edges_per_user,
    )
    result = _benchmark_result(
        queries,
        iterations=args.iterations,
        warmup=args.warmup,
        include_duckdb=include_duckdb,
        scale=scale,
        ingest_batch_size=args.ingest_batch_size,
    )
    payload = {
        "generated_at": datetime.now(UTC).isoformat(),
        "python_version": platform.python_version(),
        "platform": platform.platform(),
        "cypherglot_version": cypherglot.__version__,
        "workload_counts": {
            "oltp": len([query for query in queries if query.workload == "oltp"]),
            "olap": len([query for query in queries if query.workload == "olap"]),
        },
        "graph_scale": {
            "user_count": scale.user_count,
            "company_count": scale.company_count,
            "knows_edges_per_user": scale.knows_edges_per_user,
            "node_count": scale.node_count,
            "edge_count": scale.edge_count,
            "ingest_batch_size": args.ingest_batch_size,
        },
        "results": result,
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")

    print(f"Wrote runtime benchmark baseline to {args.output}")
    workloads = result["workloads"]
    if "oltp" in workloads:
        _print_suite("oltp/sqlite", workloads["oltp"]["sqlite"])
    if "olap" in workloads:
        _print_suite("olap/sqlite", workloads["olap"]["sqlite"])
        if include_duckdb:
            _print_suite("olap/duckdb", workloads["olap"]["duckdb"])
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
