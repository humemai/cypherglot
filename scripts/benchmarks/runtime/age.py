"""Apache AGE runtime helpers — a native-Cypher baseline on PostgreSQL.

Apache AGE is a PostgreSQL extension that runs openCypher natively via
``cypher('graph', $$ <query> $$) AS (col agtype, ...)``. Unlike the SQL compile
targets, CypherGlot does NOT lower for AGE: AGE runs the Cypher directly, so it
is a *baseline* (like Neo4j/ArcadeDB/Ladybug). Its unique value is that AGE runs
on PostgreSQL and CypherGlot also targets PostgreSQL, giving the cleanest
head-to-head: native graph extension vs. lowered SQL on the same engine.

This module is deliberately self-contained (psycopg2 + sqlglot) so it can drive
both the conformance comparison and a performance benchmark. It reuses
CypherGlot's own frontend to discover RETURN column names, so the mandatory
``AS (...)`` column list is built automatically for any admitted-subset query.
"""

from __future__ import annotations

import argparse
import gc
import json
import math
import os
import platform
import re
import shutil
import socket
import csv
import subprocess
import tempfile
import sys
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Iterator

import sqlglot

import cypherglot

from scripts.benchmarks.common.runtime_shared import (
    GeneratedGraphFixture,
    _capture_rss_snapshot,
)
from scripts.benchmarks.common._fixture_rows import (
    iter_edge_rows,
    iter_node_property_rows,
)
from scripts.benchmarks.common.topology import (
    SyntheticTopology,
    Topology,
    add_topology_cli_args,
    resolve_topology,
)
from scripts.benchmarks.common.shared import (
    add_cpu_affinity_cli_arg,
    apply_cpu_affinity,
    parse_cpu_affinity,
    BenchmarkQueryTimeoutError,
    CorpusQuery,
    EdgeTypePlan,
    RuntimeScale,
    _average_edges_per_source,
    _build_graph_schema,
    _call_with_timeout,
    _edge_out_degree,
    _extra_edge_boolean_property_name,
    _extra_edge_numeric_property_name,
    _extra_edge_text_property_name,
    _extra_node_boolean_property_name,
    _extra_node_numeric_property_name,
    _extra_node_text_property_name,
    _measure_ns,
    _node_id,
    _node_name,
    _node_type_name,
    _progress,
    _progress_iteration,
    _render_corpus_queries,
    _select_queries,
    _summarize,
    _token_map,
    _write_json_atomic,
)

try:
    import psycopg2
    from psycopg2 import errors as psycopg2_errors
except ImportError:  # pragma: no cover - optional dependency
    psycopg2 = None  # type: ignore[assignment]
    psycopg2_errors = None  # type: ignore[assignment]


# AGE lacks a few constructs CypherGlot admits for the SQL backends. Queries that
# use them are recorded as skipped rather than failed (the set itself is a
# "native AGE vs lowered SQL" finding).
#   - UNION / UNION ALL: AGE's cypher() wrapper returns a single set; stacking
#     two cypher() calls needs UNION at the SQL layer, not inside one call.
# The former lower()/upper() gap is now closed: CypherGlot accepts the standard
# Cypher names toLower()/toUpper() and the corpus uses them, so those queries run
# on native AGE as well.
AGE_UNSUPPORTED_QUERIES: frozenset[str] = frozenset(
    {
        # conformance-suite query names
        "union_distinct",
        "union_dedup",
        "union_all_keeps",
    }
)


def _age_available() -> bool:
    return psycopg2 is not None


def _age_uses_union(query: str) -> bool:
    return " UNION " in f" {query.upper()} "


def setup_age(conn, graph_name: str) -> None:
    """Load the AGE extension and (re)create an empty graph on the connection."""
    with conn.cursor() as cur:
        cur.execute("CREATE EXTENSION IF NOT EXISTS age")
        cur.execute("LOAD 'age'")
        cur.execute('SET search_path = ag_catalog, "$user", public')
        cur.execute(
            "SELECT count(*) FROM ag_catalog.ag_graph WHERE name = %s",
            (graph_name,),
        )
        (exists,) = cur.fetchone()
        if exists:
            cur.execute("SELECT drop_graph(%s, true)", (graph_name,))
        cur.execute("SELECT create_graph(%s)", (graph_name,))
    conn.commit()


def _prepare_age_cursor(conn) -> Any:
    cur = conn.cursor()
    cur.execute("LOAD 'age'")
    cur.execute('SET search_path = ag_catalog, "$user", public')
    return cur


def create_age_labels(
    conn,
    graph_name: str,
    graph_schema: cypherglot.GraphSchema,
) -> None:
    """Declare a vertex label per node type and an edge label per edge type."""
    with _prepare_age_cursor(conn) as cur:
        for node_type in graph_schema.node_types:
            cur.execute("SELECT create_vlabel(%s, %s)", (graph_name, node_type.name))
        for edge_type in graph_schema.edge_types:
            cur.execute("SELECT create_elabel(%s, %s)", (graph_name, edge_type.name))
    conn.commit()


def _agtype_property_literal(value: object) -> str:
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (int, float)):
        return repr(value)
    text = str(value).replace("\\", "\\\\").replace("'", "\\'")
    return f"'{text}'"


def _agtype_property_map(properties: dict[str, object]) -> str:
    return "{" + ", ".join(
        f"{key}: {_agtype_property_literal(value)}"
        for key, value in properties.items()
    ) + "}"


def run_age_cypher(
    cur,
    graph_name: str,
    cypher: str,
    *,
    column_clause: str = "result agtype",
) -> list[tuple[object, ...]]:
    """Execute a Cypher statement through AGE and return raw agtype rows."""
    cur.execute(
        f"SELECT * FROM cypher('{graph_name}', $$ {cypher} $$) AS ({column_clause})"
    )
    if cur.description is None:
        return []
    return cur.fetchall()


def seed_age_node(
    cur,
    graph_name: str,
    label: str,
    properties: dict[str, object],
) -> None:
    run_age_cypher(
        cur,
        graph_name,
        f"CREATE (:{label} {_agtype_property_map(properties)})",
    )


def seed_age_edge(
    cur,
    graph_name: str,
    *,
    edge_label: str,
    source_label: str,
    source_id: int,
    target_label: str,
    target_id: int,
    properties: dict[str, object] | None = None,
) -> None:
    prop_map = _agtype_property_map(properties) if properties else ""
    run_age_cypher(
        cur,
        graph_name,
        (
            f"MATCH (a:{source_label} {{id: {source_id}}}), "
            f"(b:{target_label} {{id: {target_id}}}) "
            f"CREATE (a)-[:{edge_label} {prop_map}]->(b)"
        ),
    )


def age_return_columns(
    query: str,
    schema_context: cypherglot.CompilerSchemaContext,
) -> list[str]:
    """Discover RETURN column names for a query by compiling it to SQL with
    CypherGlot's own frontend and reading the projected output names. This
    builds AGE's mandatory ``AS (c1 agtype, ...)`` list automatically."""
    sql = cypherglot.to_sql(query, backend="sqlite", schema_context=schema_context)
    expression = sqlglot.parse_one(sql, read="sqlite")
    selects = getattr(expression, "selects", None)
    if not selects:
        raise ValueError(f"Unable to determine output columns for query: {query}")
    # Variable-length reads compile to `SELECT * FROM (<union>) AS variable_length_q`,
    # so the top-level projection is a bare Star. Resolve the real column names from
    # the wrapped subquery (its aliased projections) instead of returning "*".
    if len(selects) == 1 and isinstance(selects[0], sqlglot.exp.Star):
        subquery = expression.find(sqlglot.exp.Subquery)
        inner = subquery.this if subquery is not None else None
        inner_selects = getattr(inner, "selects", None) if inner is not None else None
        if inner_selects:
            return [select.alias_or_name for select in inner_selects]
        raise ValueError(
            f"Unable to resolve star projection to output columns for query: {query}"
        )
    return [select.alias_or_name for select in selects]


def age_column_clause(columns: list[str]) -> str:
    return ", ".join(f'"{name}" agtype' for name in columns)


def parse_agtype(value: object) -> object:
    """AGE returns agtype as text via psycopg2 (e.g. ``"Alice"``, ``30``,
    ``["Bob", "Carol"]``, ``true``). Decode it to a native Python value."""
    if value is None:
        return None
    if isinstance(value, (int, float, bool, list, dict)):
        return value
    if isinstance(value, str):
        try:
            return json.loads(value)
        except json.JSONDecodeError:
            return value
    return value


def execute_age_query(
    cur,
    graph_name: str,
    query: str,
    schema_context: cypherglot.CompilerSchemaContext,
) -> tuple[list[str], list[tuple[object, ...]]]:
    """Run an admitted-subset Cypher query natively on AGE and return
    ``(column_names, rows)`` with each cell decoded from agtype."""
    columns = age_return_columns(query, schema_context)
    rows = run_age_cypher(
        cur,
        graph_name,
        query,
        column_clause=age_column_clause(columns),
    )
    decoded = [tuple(parse_agtype(value) for value in row) for row in rows]
    return columns, decoded


# ---------------------------------------------------------------------------
# Timed benchmark runtime
#
# Everything below turns the helpers above into a native-engine benchmark
# runtime that mirrors ``scripts/benchmarks/runtime/neo4j.py``: Docker
# lifecycle, seeding the same generated graph the other engines use, the
# warmup + iterations + per-query timeout measurement loop (server-side
# ``statement_timeout`` plus a ``signal.setitimer`` backstop), suite
# orchestration, and the shared result-JSON payload shape. AGE runs the
# rendered Cypher directly (no SQL lowering), so it is a baseline like Neo4j.
# ---------------------------------------------------------------------------

REPO_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_CORPUS_PATH = (
    REPO_ROOT
    / "scripts"
    / "benchmarks"
    / "corpora"
    / "sqlite_runtime_benchmark_corpus.json"
)
DEFAULT_RUNTIME_RESULTS_DIR = (
    REPO_ROOT / "scripts" / "benchmarks" / "results" / "runtime"
)
DEFAULT_OUTPUT_PATH = DEFAULT_RUNTIME_RESULTS_DIR / "age_runtime_benchmark.json"

DEFAULT_GRAPH_NAME = "cypherglot_benchmark"
DEFAULT_AGE_IMAGE = "apache/age"
DEFAULT_AGE_DB = "cypherglot_age"
DEFAULT_AGE_USER = "cypherglot"
DEFAULT_AGE_PASSWORD = "cypherglot"

# Env vars that mirror the auto-docker provisioning used by the ClickHouse /
# PostgreSQL benchmark support modules. When ``CYPHERGLOT_BENCHMARK_AGE_DSN`` is
# set the runtime connects to it directly; otherwise, if auto-docker is enabled
# (the env flag or ``--docker``), a disposable ``apache/age`` container is
# started.
AGE_DSN_ENV = "CYPHERGLOT_BENCHMARK_AGE_DSN"
AGE_IMAGE_ENV = "CYPHERGLOT_BENCHMARK_AGE_IMAGE"
AGE_AUTO_DOCKER_ENV = "CYPHERGLOT_BENCHMARK_AGE_AUTO_DOCKER"

# AGE seeds via literal batched Cypher CREATE statements. The batch caps keep
# each generated statement string bounded; they are independent of the scale's
# ``ingest_batch_size`` (which is still recorded in the payload).
AGE_SEED_NODE_BATCH = 200
AGE_SEED_EDGE_BATCH = 25

RuntimeProgressCallback = Callable[[dict[str, object], int], None]
_DOCKER_MEMORY_PATTERN = re.compile(r"^\s*([0-9]+(?:\.[0-9]+)?)\s*([A-Za-z]+)\s*$")
_BYTE_UNITS = {
    "b": 1.0 / (1024.0 * 1024.0),
    "kb": 1000.0 / (1024.0 * 1024.0),
    "kib": 1.0 / 1024.0,
    "mb": 1_000_000.0 / (1024.0 * 1024.0),
    "mib": 1.0,
    "gb": 1_000_000_000.0 / (1024.0 * 1024.0),
    "gib": 1024.0,
}


@dataclass(frozen=True, slots=True)
class DockerAgeConfig:
    image: str
    container_name: str
    pg_port: int
    db: str
    user: str
    password: str
    startup_timeout_s: int
    keep_container: bool
    cpuset_cpus: str | None = None

    @property
    def dsn(self) -> str:
        return (
            "postgresql://"
            f"{self.user}:{self.password}@127.0.0.1:{self.pg_port}/{self.db}"
        )


def _run_command(command: list[str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        command,
        check=False,
        capture_output=True,
        text=True,
    )


def _find_free_tcp_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        sock.listen(1)
        return int(sock.getsockname()[1])


def _docker_default_container_name() -> str:
    return (
        "benchmark-age-runtime-"
        + datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
        + f"-{uuid.uuid4().hex[:8]}"
    )


def _docker_run_command(config: DockerAgeConfig) -> list[str]:
    command = [
        "docker",
        "run",
        "--detach",
        "--rm",
        "--name",
        config.container_name,
        "--publish",
        f"127.0.0.1:{config.pg_port}:5432",
        "--env",
        f"POSTGRES_DB={config.db}",
        "--env",
        f"POSTGRES_USER={config.user}",
        "--env",
        f"POSTGRES_PASSWORD={config.password}",
        # AGE runs inside PostgreSQL: parallel workers allocate dynamic shared
        # memory in /dev/shm, and Docker's 64 MB default fails analytic joins.
        "--shm-size",
        os.environ.get("CYPHERGLOT_BENCHMARK_POSTGRES_SHM_SIZE", "2g"),
    ]
    if config.cpuset_cpus:
        command.extend(["--cpuset-cpus", config.cpuset_cpus])
    command.append(config.image)
    return command


def _start_docker_age(config: DockerAgeConfig) -> None:
    _progress(
        f"age runtime benchmark: starting Docker container {config.container_name}"
    )
    result = _run_command(_docker_run_command(config))
    if result.returncode == 0:
        return
    stderr = result.stderr.strip()
    raise RuntimeError(
        "Failed to start Apache AGE Docker container. "
        f"docker stderr: {stderr or 'none'}"
    )


def _stop_docker_age(config: DockerAgeConfig) -> None:
    _progress(
        f"age runtime benchmark: stopping Docker container {config.container_name}"
    )
    _run_command(["docker", "rm", "-f", "-v", config.container_name])


def _docker_logs(config: DockerAgeConfig) -> str:
    result = _run_command(["docker", "logs", config.container_name])
    if result.returncode != 0:
        return result.stderr.strip()
    return result.stdout.strip()


def _parse_docker_memory_to_mib(value: str) -> float | None:
    match = _DOCKER_MEMORY_PATTERN.match(value)
    if match is None:
        return None
    amount = float(match.group(1))
    unit = match.group(2).lower()
    multiplier = _BYTE_UNITS.get(unit)
    if multiplier is None:
        return None
    return amount * multiplier


def _docker_server_rss_mib(config: DockerAgeConfig | None) -> float | None:
    if config is None:
        return None
    result = _run_command(
        [
            "docker",
            "stats",
            "--no-stream",
            "--format",
            "{{.MemUsage}}",
            config.container_name,
        ]
    )
    if result.returncode != 0:
        return None
    usage = result.stdout.strip().split("/", 1)[0].strip()
    return _parse_docker_memory_to_mib(usage)


def _capture_age_rss_snapshot(
    docker_config: DockerAgeConfig | None,
) -> dict[str, float | None]:
    return _capture_rss_snapshot(
        backend="age",
        server_mib=_docker_server_rss_mib(docker_config),
    )


def _connect_age(dsn: str) -> Any:
    if psycopg2 is None:
        raise ValueError(
            "psycopg2 is not installed. Install it with "
            "`uv pip install psycopg2-binary`."
        )
    conn = psycopg2.connect(dsn)
    conn.autocommit = False
    return conn


def _wait_for_connection_ready(dsn: str, timeout_s: int) -> Any:
    deadline = time.monotonic() + timeout_s
    last_error: str | None = None
    while time.monotonic() < deadline:
        try:
            conn = _connect_age(dsn)
            with conn.cursor() as cur:
                cur.execute("CREATE EXTENSION IF NOT EXISTS age")
                cur.execute("LOAD 'age'")
            conn.commit()
            return conn
        except Exception as exc:  # pragma: no cover - timing-dependent
            last_error = str(exc)
            time.sleep(1.0)
    raise RuntimeError(
        "Timed out waiting for Apache AGE to accept connections. "
        f"Last error: {last_error or 'unknown'}"
    )


def _age_server_versions(conn: Any) -> dict[str, str]:
    versions: dict[str, str] = {}
    with conn.cursor() as cur:
        cur.execute("SHOW server_version")
        row = cur.fetchone()
        if row and isinstance(row[0], str):
            versions["postgresql"] = row[0]
        cur.execute("SELECT extversion FROM pg_extension WHERE extname = 'age'")
        row = cur.fetchone()
        if row and isinstance(row[0], str):
            versions["age"] = row[0]
    conn.rollback()
    return versions


def _pool_summaries(
    query_results: list[dict[str, object]],
    key: str,
) -> dict[str, float]:
    successful = [result for result in query_results if result["status"] == "passed"]
    if not successful:
        return {
            "mean_of_mean_ms": 0.0,
            "mean_of_p50_ms": 0.0,
            "mean_of_p95_ms": 0.0,
            "mean_of_p99_ms": 0.0,
        }
    return {
        "mean_of_mean_ms": sum(result[key]["mean_ms"] for result in successful)
        / len(successful),
        "mean_of_p50_ms": sum(result[key]["p50_ms"] for result in successful)
        / len(successful),
        "mean_of_p95_ms": sum(result[key]["p95_ms"] for result in successful)
        / len(successful),
        "mean_of_p99_ms": sum(result[key]["p99_ms"] for result in successful)
        / len(successful),
    }


def _load_corpus(path: Path) -> list[CorpusQuery]:
    raw = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(raw, list) or not raw:
        raise ValueError("Runtime benchmark corpus must be a non-empty JSON list.")

    queries: list[CorpusQuery] = []
    for index, item in enumerate(raw, start=1):
        if not isinstance(item, dict):
            raise ValueError(f"Runtime corpus item {index} must be a JSON object.")
        try:
            name = item["name"]
            workload = item["workload"]
            category = item["category"]
            query = item["query"]
            backends = item["backends"]
        except KeyError as exc:
            raise ValueError(
                f"Runtime corpus item {index} is missing required key {exc.args[0]!r}."
            ) from exc
        mode = item.get("mode", "statement")
        mutation = item.get("mutation", False)

        if not isinstance(name, str) or not name:
            raise ValueError(f"Runtime corpus item {index} has invalid 'name'.")
        if workload not in {"oltp", "olap"}:
            raise ValueError(f"Runtime corpus item {index} has invalid 'workload'.")
        if not isinstance(category, str) or not category:
            raise ValueError(f"Runtime corpus item {index} has invalid 'category'.")
        if not isinstance(query, str) or not query:
            raise ValueError(f"Runtime corpus item {index} has invalid 'query'.")
        if mode not in {"statement", "program"}:
            raise ValueError(f"Runtime corpus item {index} has invalid 'mode'.")
        if not isinstance(mutation, bool):
            raise ValueError(f"Runtime corpus item {index} has invalid 'mutation'.")
        if not isinstance(backends, list) or not backends:
            raise ValueError(f"Runtime corpus item {index} has invalid 'backends'.")

        queries.append(
            CorpusQuery(
                name=name,
                workload=workload,
                category=category,
                query=query,
                backends=tuple(backends),
                mode=mode,
                mutation=mutation,
            )
        )
    return queries


def _age_skip_reason(query: CorpusQuery) -> str | None:
    """Return why a corpus query is out of scope for the native AGE baseline,
    or ``None`` if it should run. Mutations, multi-statement programs, and
    UNION queries are recorded as skipped rather than executed."""
    if query.name in AGE_UNSUPPORTED_QUERIES:
        return "age_unsupported"
    if query.mode == "program":
        return "program_mode"
    if query.mutation:
        return "mutation"
    if _age_uses_union(query.query):
        return "union"
    return None


def _node_properties(
    scale: RuntimeScale,
    type_index: int,
    local_index: int,
) -> dict[str, object]:
    properties: dict[str, object] = {
        "id": _node_id(scale, type_index, local_index),
        "name": _node_name(type_index, local_index),
        "age": 18 + ((type_index * 5 + local_index) % 47),
        "score": round(1.0 + ((type_index * 17 + local_index * 7) % 500) / 100.0, 2),
        "active": bool((type_index + local_index) % 3 != 0),
    }
    for property_index in range(1, scale.node_extra_text_property_count + 1):
        properties[_extra_node_text_property_name(property_index)] = (
            f"{_node_type_name(type_index).lower()}-"
            f"text-{property_index:02d}-{local_index:06d}"
        )
    for property_index in range(1, scale.node_extra_numeric_property_count + 1):
        properties[_extra_node_numeric_property_name(property_index)] = round(
            property_index
            + ((type_index * 31 + local_index * (property_index + 9)) % 10_000)
            / 100.0,
            2,
        )
    for property_index in range(1, scale.node_extra_boolean_property_count + 1):
        properties[_extra_node_boolean_property_name(property_index)] = bool(
            (type_index + local_index + property_index) % 2 == 0
        )
    return properties


def _edge_properties(
    scale: RuntimeScale,
    plan: EdgeTypePlan,
    source_local_index: int,
    edge_ordinal: int,
) -> dict[str, object]:
    properties: dict[str, object] = {
        "note": f"{plan.name.lower()}-note-{edge_ordinal:02d}-{source_local_index:06d}",
        "weight": round(
            0.5 + ((plan.type_index + source_local_index + edge_ordinal) % 11) * 0.35,
            2,
        ),
        "score": round(
            1.0
            + ((plan.type_index * 7 + source_local_index + edge_ordinal) % 17) * 0.4,
            2,
        ),
        "active": bool((plan.type_index + source_local_index + edge_ordinal) % 2 == 0),
        "rank": 1 + ((plan.type_index + source_local_index + edge_ordinal) % 100),
    }
    for property_index in range(1, scale.edge_extra_text_property_count + 1):
        properties[_extra_edge_text_property_name(property_index)] = (
            f"{plan.name.lower()}-text-{property_index:02d}-{source_local_index:06d}"
        )
    for property_index in range(1, scale.edge_extra_numeric_property_count + 1):
        properties[_extra_edge_numeric_property_name(property_index)] = round(
            property_index
            + (
                (
                    plan.type_index * 19
                    + source_local_index * (property_index + 5)
                    + edge_ordinal
                )
                % 5_000
            )
            / 100.0,
            2,
        )
    for property_index in range(1, scale.edge_extra_boolean_property_count + 1):
        properties[_extra_edge_boolean_property_name(property_index)] = bool(
            (plan.type_index + source_local_index + edge_ordinal + property_index) % 2
            == 0
        )
    return properties


def _age_label_table(graph_name: str, label: str) -> str:
    return f'"{graph_name}"."{label}"'


def _reset_graph(conn: Any, graph_name: str, graph_schema: cypherglot.GraphSchema) -> None:
    setup_age(conn, graph_name)


def _seed_constraint_indexes(
    conn: Any,
    graph_name: str,
    graph_schema: cypherglot.GraphSchema,
) -> None:
    """Create a GIN index on each vertex label's ``properties`` column. AGE
    lowers ``MATCH (n {id: X})`` / ``{name: '...'}`` to a ``properties @> ...``
    containment filter, so this index makes edge seeding (which matches
    endpoints by ``id``) and point lookups fast. It is created in both index
    modes, analogous to Neo4j's always-present id constraint."""
    with conn.cursor() as cur:
        for node_type in graph_schema.node_types:
            index_name = f"cg_seed_{node_type.name.lower()}_props"
            cur.execute(
                f'CREATE INDEX IF NOT EXISTS "{index_name}" ON '
                f"{_age_label_table(graph_name, node_type.name)} USING gin (properties)"
            )
    conn.commit()


def _seed_nodes(
    conn: Any,
    *,
    graph_name: str,
    scale: RuntimeScale,
    graph_schema: cypherglot.GraphSchema,
    progress_label: str,
    fixture: GeneratedGraphFixture | None = None,
) -> int:
    total = 0
    type_count = len(graph_schema.node_types)
    with _prepare_age_cursor(conn) as cur:
        for type_index, node_type in enumerate(graph_schema.node_types, start=1):
            _progress(
                f"{progress_label}: node type {type_index}/{type_count} "
                f"({node_type.name})"
            )
            if fixture is None:
                rows = (
                    _node_properties(scale, type_index, local_index)
                    for local_index in range(1, scale.nodes_per_type + 1)
                )
            else:
                rows = iter_node_property_rows(node_type, fixture)
            patterns: list[str] = []
            for properties in rows:
                patterns.append(
                    f"(:{node_type.name} {_agtype_property_map(properties)})"
                )
                if len(patterns) >= AGE_SEED_NODE_BATCH:
                    run_age_cypher(cur, graph_name, "CREATE " + ", ".join(patterns))
                    total += len(patterns)
                    patterns.clear()
            if patterns:
                run_age_cypher(cur, graph_name, "CREATE " + ", ".join(patterns))
                total += len(patterns)
    conn.commit()
    return total


def _synthetic_age_edge_rows(
    scale: RuntimeScale,
    plan: EdgeTypePlan,
) -> Iterator[tuple[int, int, dict[str, object]]]:
    for source_local_index in range(1, scale.nodes_per_type + 1):
        edge_count_for_source = _edge_out_degree(scale, source_local_index)
        from_id = _node_id(scale, plan.source_type_index, source_local_index)
        for edge_ordinal in range(1, edge_count_for_source + 1):
            target_local_index = (
                (source_local_index - 1 + plan.type_index + edge_ordinal)
                % scale.nodes_per_type
            ) + 1
            to_id = _node_id(scale, plan.target_type_index, target_local_index)
            properties = _edge_properties(scale, plan, source_local_index, edge_ordinal)
            yield from_id, to_id, properties


def _seed_edges(
    conn: Any,
    *,
    graph_name: str,
    scale: RuntimeScale,
    graph_schema: cypherglot.GraphSchema,
    edge_plans: list[EdgeTypePlan],
    progress_label: str,
    fixture: GeneratedGraphFixture | None = None,
) -> int:
    total = 0
    edge_type_total = len(edge_plans)

    def flush(cur: Any, match_parts: list[str], create_parts: list[str]) -> int:
        if not create_parts:
            return 0
        statement = "MATCH " + ", ".join(match_parts) + " CREATE " + ", ".join(
            create_parts
        )
        run_age_cypher(cur, graph_name, statement)
        return len(create_parts)

    with _prepare_age_cursor(conn) as cur:
        for edge_type_index, plan in enumerate(edge_plans, start=1):
            _progress(
                f"{progress_label}: edge type {edge_type_index}/{edge_type_total} "
                f"({plan.name})"
            )
            source_label = graph_schema.node_types[plan.source_type_index - 1].name
            target_label = graph_schema.node_types[plan.target_type_index - 1].name
            if fixture is None:
                rows: Iterator[tuple[int, int, dict[str, object]]] = (
                    _synthetic_age_edge_rows(scale, plan)
                )
            else:
                edge_type = graph_schema.edge_types[plan.type_index - 1]
                rows = (
                    (row["from_id"], row["to_id"], row["props"])
                    for row in iter_edge_rows(edge_type, fixture)
                )
            match_parts: list[str] = []
            create_parts: list[str] = []
            slot = 0
            for from_id, to_id, properties in rows:
                a = f"a{slot}"
                b = f"b{slot}"
                match_parts.append(f"({a}:{source_label} {{id: {from_id}}})")
                match_parts.append(f"({b}:{target_label} {{id: {to_id}}})")
                create_parts.append(
                    f"({a})-[:{plan.name} {_agtype_property_map(properties)}]->({b})"
                )
                slot += 1
                if slot >= AGE_SEED_EDGE_BATCH:
                    total += flush(cur, match_parts, create_parts)
                    match_parts.clear()
                    create_parts.clear()
                    slot = 0
            total += flush(cur, match_parts, create_parts)
            match_parts.clear()
            create_parts.clear()
            slot = 0
    conn.commit()
    return total


def _agtype_csv_cell(value: str, logical_type: str) -> str:
    """Render one fixture-CSV cell as an agtype literal for the AGE bulk loader.

    ``load_*_from_file(..., load_as_agtype=true)`` parses each cell as an
    agtype value, which gives properly typed properties (the plain-text mode
    loads everything as strings and breaks typed comparisons).
    """
    if logical_type == "boolean":
        return "true" if value in ("1", "true", "True") else "false"
    if logical_type in ("integer", "float"):
        return value
    escaped = value.replace("\\", "\\\\").replace('"', '\\"')
    return f'"{escaped}"'


def _write_age_load_csvs(
    fixture: GeneratedGraphFixture,
    graph_schema: cypherglot.GraphSchema,
    out_dir: Path,
) -> dict[str, Path]:
    """Rewrite fixture CSVs into the AGE bulk-loader format.

    Nodes keep ``id`` plus agtype-typed property cells. Edges use the loader's
    ``start_id,start_vertex_type,end_id,end_vertex_type`` header (the fixture's
    own edge ``id`` is dropped; the corpus never reads ``r.id``).
    """
    out_dir.mkdir(parents=True, exist_ok=True)
    load_paths: dict[str, Path] = {}

    for node_type in graph_schema.node_types:
        logical = {prop.name: prop.logical_type for prop in node_type.properties}
        out_path = out_dir / f"{node_type.table_name}.csv"
        with fixture.table_csv_paths[node_type.table_name].open(
            "r", encoding="utf-8", newline=""
        ) as src_handle, out_path.open("w", encoding="utf-8", newline="") as dst:
            reader = csv.DictReader(src_handle)
            writer = csv.writer(dst)
            columns = list(reader.fieldnames or [])
            writer.writerow(columns)
            for row in reader:
                writer.writerow(
                    row[c] if c == "id" else _agtype_csv_cell(row[c], logical[c])
                    for c in columns
                )
        load_paths[node_type.table_name] = out_path

    for edge_type in graph_schema.edge_types:
        logical = {prop.name: prop.logical_type for prop in edge_type.properties}
        out_path = out_dir / f"{edge_type.table_name}.csv"
        with fixture.table_csv_paths[edge_type.table_name].open(
            "r", encoding="utf-8", newline=""
        ) as src_handle, out_path.open("w", encoding="utf-8", newline="") as dst:
            reader = csv.DictReader(src_handle)
            writer = csv.writer(dst)
            prop_columns = [
                c for c in (reader.fieldnames or [])
                if c not in ("id", "from_id", "to_id")
            ]
            writer.writerow(
                ["start_id", "start_vertex_type", "end_id", "end_vertex_type"]
                + prop_columns
            )
            for row in reader:
                writer.writerow(
                    [
                        row["from_id"],
                        edge_type.source_type,
                        row["to_id"],
                        edge_type.target_type,
                    ]
                    + [_agtype_csv_cell(row[c], logical[c]) for c in prop_columns]
                )
        load_paths[edge_type.table_name] = out_path
    return load_paths


def _bulk_load_age_graph(
    conn: Any,
    *,
    graph_name: str,
    graph_schema: cypherglot.GraphSchema,
    fixture: GeneratedGraphFixture,
    docker_config: DockerAgeConfig,
    progress_label: str,
) -> dict[str, int]:
    """Ingest via AGE's server-side bulk loaders (its documented fast path).

    Rewrites the fixture CSVs into loader format, copies them into the
    container (the loader roots relative paths at /tmp/age/), and calls
    load_labels_from_file / load_edges_from_file per label.
    """
    _progress(f"{progress_label}: bulk load via load_*_from_file")
    # Stage next to the fixture CSVs (disk-backed): the rewritten loader CSVs
    # are roughly fixture-sized, which can exceed /tmp tmpfs quotas.
    with tempfile.TemporaryDirectory(
        prefix="age-load-", dir=fixture.csv_dir.parent
    ) as tmp:
        out_dir = Path(tmp) / "cgload"
        _write_age_load_csvs(fixture, graph_schema, out_dir)
        container = docker_config.container_name
        subprocess.run(
            ["docker", "exec", container, "mkdir", "-p", "/tmp/age"],
            check=True, capture_output=True,
        )
        subprocess.run(
            ["docker", "cp", str(out_dir), f"{container}:/tmp/age/cgload"],
            check=True, capture_output=True,
        )
    with conn.cursor() as cur:
        for node_type in graph_schema.node_types:
            _progress(f"{progress_label}: bulk load nodes {node_type.name}")
            cur.execute(
                "SELECT ag_catalog.load_labels_from_file(%s, %s, %s, true, true)",
                (graph_name, node_type.name, f"cgload/{node_type.table_name}.csv"),
            )
        for edge_type in graph_schema.edge_types:
            _progress(f"{progress_label}: bulk load edges {edge_type.name}")
            cur.execute(
                "SELECT ag_catalog.load_edges_from_file(%s, %s, %s, true)",
                (graph_name, edge_type.name, f"cgload/{edge_type.table_name}.csv"),
            )
    conn.commit()
    subprocess.run(
        ["docker", "exec", docker_config.container_name, "rm", "-rf",
         "/tmp/age/cgload"],
        check=False, capture_output=True,
    )
    _progress(
        f"{progress_label}: bulk load committed "
        f"({fixture.row_counts['node_count']} nodes, "
        f"{fixture.row_counts['edge_count']} edges)"
    )
    return {
        "node_count": fixture.row_counts["node_count"],
        "edge_count": fixture.row_counts["edge_count"],
        "node_type_count": len(graph_schema.node_types),
        "edge_type_count": len(graph_schema.edge_types),
    }


def _seed_graph(
    conn: Any,
    *,
    graph_name: str,
    scale: RuntimeScale,
    graph_schema: cypherglot.GraphSchema,
    edge_plans: list[EdgeTypePlan],
    progress_label: str,
    fixture: GeneratedGraphFixture | None = None,
) -> dict[str, int]:
    node_count = _seed_nodes(
        conn,
        graph_name=graph_name,
        scale=scale,
        graph_schema=graph_schema,
        progress_label=progress_label,
        fixture=fixture,
    )
    edge_count = _seed_edges(
        conn,
        graph_name=graph_name,
        scale=scale,
        graph_schema=graph_schema,
        edge_plans=edge_plans,
        progress_label=progress_label,
        fixture=fixture,
    )
    _progress(
        f"{progress_label}: ingest committed ({node_count} nodes, {edge_count} edges)"
    )
    return {
        "node_count": node_count,
        "edge_count": edge_count,
        "node_type_count": len(graph_schema.node_types),
        "edge_type_count": len(graph_schema.edge_types),
    }


def _query_index_statements(
    graph_name: str,
    graph_schema: cypherglot.GraphSchema,
) -> list[str]:
    """Btree expression indexes on the properties the corpus filters / sorts on.
    AGE exposes property access via ``ag_catalog.agtype_access_operator``; a
    btree index on that expression accelerates range scans and ORDER BY."""

    def access(property_name: str) -> str:
        return (
            "ag_catalog.agtype_access_operator(VARIADIC ARRAY"
            f"[properties, '\"{property_name}\"'::agtype])"
        )

    statements: list[str] = []
    for node_type in graph_schema.node_types:
        table = _age_label_table(graph_name, node_type.name)
        slug = node_type.name.lower()
        for property_name in ("name", "age", "score", "active"):
            statements.append(
                f'CREATE INDEX IF NOT EXISTS "cg_query_{slug}_{property_name}" '
                f"ON {table} USING btree ({access(property_name)})"
            )
    for edge_type in graph_schema.edge_types:
        table = _age_label_table(graph_name, edge_type.name)
        slug = edge_type.name.lower()
        for property_name in ("rank", "score"):
            statements.append(
                f'CREATE INDEX IF NOT EXISTS "cg_query_{slug}_{property_name}" '
                f"ON {table} USING btree ({access(property_name)})"
            )
    return statements


def _create_query_indexes(
    conn: Any,
    graph_name: str,
    graph_schema: cypherglot.GraphSchema,
) -> list[str]:
    """Create the query indexes best-effort. AGE's expression-index support
    varies by version, so a failure on one statement is recorded and skipped
    rather than aborting the whole benchmark."""
    warnings: list[str] = []
    for statement in _query_index_statements(graph_name, graph_schema):
        try:
            with conn.cursor() as cur:
                cur.execute(statement)
            conn.commit()
        except Exception as exc:  # pragma: no cover - version-dependent
            conn.rollback()
            warnings.append(f"{statement}: {exc}")
    if warnings:
        _progress(
            f"age runtime benchmark: {len(warnings)} query index statement(s) "
            "could not be created (recorded, continuing)"
        )
    return warnings


def _setup_mode(
    conn: Any,
    *,
    graph_name: str,
    index_mode: str,
    scale: RuntimeScale,
    graph_schema: cypherglot.GraphSchema,
    edge_plans: list[EdgeTypePlan],
    docker_config: DockerAgeConfig | None,
    fixture: GeneratedGraphFixture | None = None,
) -> dict[str, object]:
    progress_label = f"age/{index_mode}"
    setup_metrics: dict[str, int] = {}
    rss_snapshots_mib: dict[str, dict[str, float | None]] = {}

    expected_nodes = (
        fixture.row_counts["node_count"] if fixture is not None else scale.total_nodes
    )
    expected_edges = (
        fixture.row_counts["edge_count"] if fixture is not None else scale.total_edges
    )
    _progress(
        f"{progress_label}: preparing graph "
        f"({expected_nodes} nodes, {expected_edges} edges)"
    )
    _, setup_metrics["reset_ns"] = _measure_ns(
        lambda: _reset_graph(conn, graph_name, graph_schema)
    )
    rss_snapshots_mib["after_reset"] = _capture_age_rss_snapshot(docker_config)

    def seed_constraints() -> None:
        create_age_labels(conn, graph_name, graph_schema)
        _seed_constraint_indexes(conn, graph_name, graph_schema)

    _, setup_metrics["seed_constraints_ns"] = _measure_ns(seed_constraints)
    rss_snapshots_mib["after_seed_constraints"] = _capture_age_rss_snapshot(
        docker_config
    )

    def ingest() -> dict[str, int]:
        # AGE's documented bulk path needs container file access; DSN-only
        # runs (no docker handle) keep the statement-based fallback.
        if docker_config is not None and fixture is not None:
            return _bulk_load_age_graph(
                conn,
                graph_name=graph_name,
                graph_schema=graph_schema,
                fixture=fixture,
                docker_config=docker_config,
                progress_label=progress_label,
            )
        return _seed_graph(
            conn,
            graph_name=graph_name,
            scale=scale,
            graph_schema=graph_schema,
            edge_plans=edge_plans,
            progress_label=progress_label,
            fixture=fixture,
        )

    row_counts, setup_metrics["ingest_ns"] = _measure_ns(ingest)
    rss_snapshots_mib["after_ingest"] = _capture_age_rss_snapshot(docker_config)

    index_warnings: list[str] = []
    if index_mode == "indexed":
        _progress(f"{progress_label}: creating query indexes")
        index_result, setup_metrics["index_ns"] = _measure_ns(
            lambda: _create_query_indexes(conn, graph_name, graph_schema)
        )
        index_warnings = index_result
    else:
        setup_metrics["index_ns"] = 0
    rss_snapshots_mib["after_index"] = _capture_age_rss_snapshot(docker_config)

    _progress(
        f"{progress_label}: fixture ready "
        f"(ingest={setup_metrics['ingest_ns'] / 1_000_000_000.0:.2f}s)"
    )
    return {
        "setup_metrics": setup_metrics,
        "row_counts": row_counts,
        "rss_snapshots_mib": rss_snapshots_mib,
        "index_mode": index_mode,
        "index_warnings": index_warnings,
    }


def _set_statement_timeout(cur: Any, timeout_ms: float | None) -> None:
    if timeout_ms is None:
        cur.execute("SET statement_timeout = 0")
    else:
        cur.execute(f"SET statement_timeout = {int(math.ceil(timeout_ms))}")


def _age_error_is_query_timeout(exc: Exception) -> bool:
    if psycopg2_errors is not None and isinstance(
        exc, psycopg2_errors.QueryCanceled
    ):
        return True
    message = str(exc).lower()
    return "statement timeout" in message or "canceling statement" in message


def _status_result(
    *,
    query: CorpusQuery,
    index_mode: str,
    status: str,
    extra: dict[str, object] | None = None,
) -> dict[str, object]:
    result: dict[str, object] = {
        "name": query.name,
        "workload": query.workload,
        "category": query.category,
        "backend": "age",
        "index_mode": index_mode,
        "mode": query.mode,
        "mutation": query.mutation,
        "status": status,
    }
    if extra:
        result.update(extra)
    return result


def _run_query_once(
    cur: Any,
    *,
    graph_name: str,
    query: CorpusQuery,
    column_clause: str,
    timeout_ms: float | None,
) -> dict[str, int]:
    guard_ms = None if timeout_ms is None else max(timeout_ms * 1.5, timeout_ms + 1000.0)

    def execute() -> None:
        run_age_cypher(cur, graph_name, query.query, column_clause=column_clause)

    started_ns = time.perf_counter_ns()
    _call_with_timeout(execute, timeout_ms=guard_ms, operation=f"age:{query.name}")
    execute_ns = time.perf_counter_ns() - started_ns
    return {
        "execute_ns": execute_ns,
        "end_to_end_ns": execute_ns,
        "reset_ns": 0,
    }


def _measure_query(
    conn: Any,
    *,
    graph_name: str,
    index_mode: str,
    query: CorpusQuery,
    schema_context: cypherglot.CompilerSchemaContext,
    iterations: int,
    warmup: int,
    progress_label: str,
    iteration_progress: bool,
    timeout_ms: float | None = None,
) -> dict[str, object]:
    skip_reason = _age_skip_reason(query)
    if skip_reason is not None:
        return _status_result(
            query=query,
            index_mode=index_mode,
            status="skipped",
            extra={"skip_reason": skip_reason},
        )

    try:
        column_clause = age_column_clause(
            age_return_columns(query.query, schema_context)
        )
    except Exception as exc:
        return _status_result(
            query=query,
            index_mode=index_mode,
            status="failed",
            extra={
                "error_type": type(exc).__name__,
                "error_message": f"unable to derive AGE output columns: {exc}",
            },
        )

    cur = _prepare_age_cursor(conn)
    try:
        _set_statement_timeout(cur, timeout_ms)
        conn.commit()
    except Exception:
        conn.rollback()

    def fail(exc: Exception, phase: str, iteration: int) -> dict[str, object]:
        conn.rollback()
        if (timeout_ms is not None and _age_error_is_query_timeout(exc)) or isinstance(
            exc, BenchmarkQueryTimeoutError
        ):
            return _status_result(
                query=query,
                index_mode=index_mode,
                status="timed_out",
                extra={
                    "query_timeout": {
                        "phase": phase,
                        "timeout_ms": timeout_ms,
                        "iteration": iteration,
                    }
                },
            )
        return _status_result(
            query=query,
            index_mode=index_mode,
            status="failed",
            extra={
                "error_type": type(exc).__name__,
                "error_message": str(exc),
            },
        )

    try:
        try:
            for warmup_index in range(1, warmup + 1):
                if iteration_progress:
                    _progress_iteration(
                        progress_label,
                        phase="warmup",
                        current=warmup_index,
                        total=warmup,
                    )
                _run_query_once(
                    cur,
                    graph_name=graph_name,
                    query=query,
                    column_clause=column_clause,
                    timeout_ms=timeout_ms,
                )
                conn.commit()
        except Exception as exc:
            return fail(exc, "warmup", warmup_index)

        execute_latencies: list[int] = []
        end_to_end_latencies: list[int] = []
        reset_latencies: list[int] = []

        gc_was_enabled = gc.isenabled()
        gc.disable()
        try:
            for iteration_index in range(1, iterations + 1):
                if iteration_progress:
                    _progress_iteration(
                        progress_label,
                        phase="iteration",
                        current=iteration_index,
                        total=iterations,
                    )
                metrics = _run_query_once(
                    cur,
                    graph_name=graph_name,
                    query=query,
                    column_clause=column_clause,
                    timeout_ms=timeout_ms,
                )
                conn.commit()
                execute_latencies.append(metrics["execute_ns"])
                end_to_end_latencies.append(metrics["end_to_end_ns"])
                reset_latencies.append(metrics["reset_ns"])
        except Exception as exc:
            return fail(exc, "iteration", len(execute_latencies) + 1)
        finally:
            if gc_was_enabled:
                gc.enable()
    finally:
        try:
            _set_statement_timeout(cur, None)
            conn.commit()
        except Exception:
            conn.rollback()
        cur.close()

    return _status_result(
        query=query,
        index_mode=index_mode,
        status="passed",
        extra={
            "execute": _summarize(execute_latencies),
            "end_to_end": _summarize(end_to_end_latencies),
            "reset": _summarize(reset_latencies),
        },
    )


def _run_workload_suite(
    conn: Any,
    *,
    graph_name: str,
    workload: str,
    index_mode: str,
    queries: list[CorpusQuery],
    schema_context: cypherglot.CompilerSchemaContext,
    iterations: int,
    warmup: int,
    setup: dict[str, object],
    docker_config: DockerAgeConfig | None,
    iteration_progress: bool,
    timeout_ms: float | None = None,
) -> dict[str, object]:
    suite_name = f"{workload}/age_{index_mode}"
    rss_snapshots_mib = {
        key: dict(value) for key, value in setup["rss_snapshots_mib"].items()
    }
    rss_snapshots_mib["suite_start"] = _capture_age_rss_snapshot(docker_config)
    _progress(
        f"{suite_name}: starting suite with {len(queries)} queries "
        f"({iterations} iterations, {warmup} warmup)"
    )
    query_results: list[dict[str, object]] = []
    for query_index, query in enumerate(queries, start=1):
        query_progress_label = (
            f"{suite_name}: query {query_index}/{len(queries)} {query.name}"
        )
        _progress(query_progress_label)
        query_results.append(
            _measure_query(
                conn,
                graph_name=graph_name,
                index_mode=index_mode,
                query=query,
                schema_context=schema_context,
                iterations=iterations,
                warmup=warmup,
                progress_label=query_progress_label,
                iteration_progress=iteration_progress,
                timeout_ms=timeout_ms,
            )
        )
    rss_snapshots_mib["suite_complete"] = _capture_age_rss_snapshot(docker_config)
    _progress(f"{suite_name}: suite complete")

    failures = [r for r in query_results if r["status"] == "failed"]
    timed_out = [r for r in query_results if r["status"] == "timed_out"]
    skipped = [r for r in query_results if r["status"] == "skipped"]
    return {
        "backend": "age",
        "index_mode": index_mode,
        "iterations": iterations,
        "warmup": warmup,
        "query_count": len(queries),
        "pass_count": len(query_results)
        - len(failures)
        - len(timed_out)
        - len(skipped),
        "skip_count": len(skipped),
        "timeout_count": len(timed_out),
        "fail_count": len(failures),
        "setup": {
            "reset_ms": setup["setup_metrics"]["reset_ns"] / 1_000_000.0,
            "seed_constraints_ms": (
                setup["setup_metrics"]["seed_constraints_ns"] / 1_000_000.0
            ),
            "ingest_ms": setup["setup_metrics"]["ingest_ns"] / 1_000_000.0,
            "index_ms": setup["setup_metrics"]["index_ns"] / 1_000_000.0,
        },
        "row_counts": setup["row_counts"],
        "index_warnings": setup.get("index_warnings", []),
        "rss_snapshots_mib": rss_snapshots_mib,
        "execute": _pool_summaries(query_results, "execute"),
        "end_to_end": _pool_summaries(query_results, "end_to_end"),
        "reset": _pool_summaries(query_results, "reset"),
        "queries": query_results,
    }


def _benchmark_result(
    conn: Any,
    *,
    graph_name: str,
    queries: list[CorpusQuery],
    schema_context: cypherglot.CompilerSchemaContext,
    iterations: int,
    warmup: int,
    oltp_iterations: int | None,
    oltp_warmup: int | None,
    olap_iterations: int | None,
    olap_warmup: int | None,
    scale: RuntimeScale,
    index_mode: str,
    docker_config: DockerAgeConfig | None,
    iteration_progress: bool,
    oltp_timeout_ms: float | None = None,
    olap_timeout_ms: float | None = None,
    topology: Topology | None = None,
    progress_callback: RuntimeProgressCallback | None = None,
) -> tuple[dict[str, object], int]:
    active_topology = topology if topology is not None else SyntheticTopology()
    graph_schema, edge_plans = active_topology.build_schema(scale)
    token_map = active_topology.token_map(scale, graph_schema, edge_plans)
    rendered_queries = _render_corpus_queries(queries, token_map)

    oltp_queries = [q for q in rendered_queries if q.workload == "oltp"]
    olap_queries = [q for q in rendered_queries if q.workload == "olap"]
    oltp_iterations_value = iterations if oltp_iterations is None else oltp_iterations
    oltp_warmup_value = warmup if oltp_warmup is None else oltp_warmup
    olap_iterations_value = iterations if olap_iterations is None else olap_iterations
    olap_warmup_value = warmup if olap_warmup is None else olap_warmup

    index_modes = [index_mode] if index_mode != "both" else ["indexed", "unindexed"]

    # For a non-synthetic topology, materialise the fixture CSVs once (the data
    # is identical across index modes) and seed the graph from them.
    seed_fixture: GeneratedGraphFixture | None = None
    # Non-synthetic topologies need the fixture as the data source; docker-
    # managed runs also need it for the bulk loader, so materialise it for the
    # synthetic topology too in that case (DSN-only synthetic runs keep the
    # in-process generator).
    if active_topology.name != "synthetic" or docker_config is not None:
        seed_fixture = active_topology.prepare_fixture(
            scale=scale,
            graph_schema=graph_schema,
            edge_plans=edge_plans,
            index_mode=index_modes[0],
        )

    def suite_kwargs(timeout_ms: float | None) -> dict[str, object]:
        kwargs: dict[str, object] = {"iteration_progress": iteration_progress}
        if timeout_ms is not None:
            kwargs["timeout_ms"] = timeout_ms
        return kwargs

    workloads: dict[str, object] = {}
    failure_count = 0

    if progress_callback is not None:
        progress_callback({"workloads": workloads, "token_map": token_map}, failure_count)

    for mode in index_modes:
        setup = _setup_mode(
            conn,
            graph_name=graph_name,
            index_mode=mode,
            scale=scale,
            graph_schema=graph_schema,
            edge_plans=edge_plans,
            docker_config=docker_config,
            fixture=seed_fixture,
        )
        if oltp_queries:
            workloads.setdefault(
                "oltp",
                {
                    "description": (
                        "Transactional-style Apache AGE execution over the "
                        "generated graph using the runtime corpus directly as "
                        "native Cypher."
                    )
                },
            )
            suite = _run_workload_suite(
                conn,
                graph_name=graph_name,
                workload="oltp",
                index_mode=mode,
                queries=oltp_queries,
                schema_context=schema_context,
                iterations=oltp_iterations_value,
                warmup=oltp_warmup_value,
                setup=setup,
                docker_config=docker_config,
                **suite_kwargs(oltp_timeout_ms),
            )
            workloads["oltp"][f"age_{mode}"] = suite
            failure_count += int(suite["fail_count"])
            # Timeouts are data (an exceeded budget is a result, not an
            # operational failure), so they do not affect the exit code.
            if progress_callback is not None:
                progress_callback(
                    {"workloads": workloads, "token_map": token_map}, failure_count
                )

        if olap_queries:
            workloads.setdefault(
                "olap",
                {
                    "description": (
                        "Analytical-style Apache AGE execution over the generated "
                        "graph using the runtime corpus directly as native Cypher."
                    )
                },
            )
            suite = _run_workload_suite(
                conn,
                graph_name=graph_name,
                workload="olap",
                index_mode=mode,
                queries=olap_queries,
                schema_context=schema_context,
                iterations=olap_iterations_value,
                warmup=olap_warmup_value,
                setup=setup,
                docker_config=docker_config,
                **suite_kwargs(olap_timeout_ms),
            )
            workloads["olap"][f"age_{mode}"] = suite
            failure_count += int(suite["fail_count"])
            # Timeouts are data (an exceeded budget is a result, not an
            # operational failure), so they do not affect the exit code.
            if progress_callback is not None:
                progress_callback(
                    {"workloads": workloads, "token_map": token_map}, failure_count
                )

    return {"workloads": workloads, "token_map": token_map}, failure_count


def _build_payload(
    *,
    started_at: datetime,
    database_versions: dict[str, str],
    age_dsn: str,
    graph_name: str,
    docker_config: DockerAgeConfig | None,
    corpus_path: Path,
    queries: list[CorpusQuery],
    scale: RuntimeScale,
    graph_schema: cypherglot.GraphSchema,
    index_mode: str,
    default_iterations: int,
    default_warmup: int,
    oltp_iterations: int,
    oltp_warmup: int,
    olap_iterations: int,
    olap_warmup: int,
    oltp_timeout_ms: float | None = None,
    olap_timeout_ms: float | None = None,
    connect_ms: float | None,
    connect_rss_mib: dict[str, float | None] | None,
    result: dict[str, object],
    failure_count: int,
    status: str,
    completed_at: datetime | None = None,
) -> dict[str, object]:
    payload: dict[str, object] = {
        "generated_at": started_at.isoformat(),
        "updated_at": datetime.now(timezone.utc).isoformat(),
        "run_status": status,
        "python_version": platform.python_version(),
        "platform": platform.platform(),
        "cypherglot_version": cypherglot.__version__,
        "database_versions": database_versions,
        "age": {
            "dsn": age_dsn,
            "graph_name": graph_name,
            "docker": (
                {
                    "image": docker_config.image,
                    "container_name": docker_config.container_name,
                    "pg_port": docker_config.pg_port,
                    "keep_container": docker_config.keep_container,
                    "cpuset_cpus": docker_config.cpuset_cpus,
                }
                if docker_config is not None
                else None
            ),
        },
        "corpus_path": str(corpus_path),
        "workload_counts": {
            "oltp": len([q for q in queries if q.workload == "oltp"]),
            "olap": len([q for q in queries if q.workload == "olap"]),
        },
        "graph_scale": {
            "node_type_count": scale.node_type_count,
            "edge_type_count": scale.edge_type_count,
            "nodes_per_type": scale.nodes_per_type,
            "edges_per_source": scale.edges_per_source,
            "edge_degree_profile": scale.edge_degree_profile,
            "average_edges_per_source": _average_edges_per_source(scale),
            "total_nodes": scale.total_nodes,
            "total_edges": scale.total_edges,
            "node_extra_text_property_count": scale.node_extra_text_property_count,
            "node_extra_numeric_property_count": (
                scale.node_extra_numeric_property_count
            ),
            "node_extra_boolean_property_count": (
                scale.node_extra_boolean_property_count
            ),
            "edge_extra_text_property_count": scale.edge_extra_text_property_count,
            "edge_extra_numeric_property_count": (
                scale.edge_extra_numeric_property_count
            ),
            "edge_extra_boolean_property_count": (
                scale.edge_extra_boolean_property_count
            ),
            "ingest_batch_size": scale.ingest_batch_size,
            "variable_hop_max": scale.variable_hop_max,
        },
        "schema_contract": {
            "layout": "property-graph",
            "node_labels": [node_type.name for node_type in graph_schema.node_types],
            "relationship_types": [
                edge_type.name for edge_type in graph_schema.edge_types
            ],
        },
        "index_mode": index_mode,
        "workload_controls": {
            "default_iterations": default_iterations,
            "default_warmup": default_warmup,
            "oltp_iterations": oltp_iterations,
            "oltp_warmup": oltp_warmup,
            "oltp_timeout_ms": oltp_timeout_ms,
            "olap_iterations": olap_iterations,
            "olap_warmup": olap_warmup,
            "olap_timeout_ms": olap_timeout_ms,
        },
        "setup": {
            "connect_ms": connect_ms,
            "connect_rss_mib": connect_rss_mib,
        },
        "results": result,
        "failure_count": failure_count,
    }
    if completed_at is not None:
        payload["completed_at"] = completed_at.isoformat()
    return payload


def _print_suite(name: str, suite: dict[str, object]) -> None:
    def format_rss_snapshot(snapshot: dict[str, float | None]) -> str:
        parts = [f"client={snapshot['client_mib']:.2f} MiB"]
        if snapshot["server_mib"] is not None:
            parts.append(f"server={snapshot['server_mib']:.2f} MiB")
        if snapshot["combined_mib"] is not None:
            parts.append(f"combined={snapshot['combined_mib']:.2f} MiB")
        return ", ".join(parts)

    print(name)
    print(
        "  setup: "
        f"reset={suite['setup']['reset_ms']:.2f} ms, "
        f"seed_constraints={suite['setup']['seed_constraints_ms']:.2f} ms, "
        f"ingest={suite['setup']['ingest_ms']:.2f} ms, "
        f"index={suite['setup']['index_ms']:.2f} ms"
    )
    print(
        "  rss: "
        + ", ".join(
            f"{key}({format_rss_snapshot(value)})"
            for key, value in suite["rss_snapshots_mib"].items()
        )
    )
    print(
        "  status: "
        f"passed={suite['pass_count']}, skipped={suite.get('skip_count', 0)}, "
        f"timed_out={suite.get('timeout_count', 0)}, failed={suite['fail_count']}"
    )
    if suite["pass_count"]:
        print(
            "  pooled execute: "
            f"mean={suite['execute']['mean_of_mean_ms']:.2f} ms, "
            f"p50={suite['execute']['mean_of_p50_ms']:.2f} ms, "
            f"p95={suite['execute']['mean_of_p95_ms']:.2f} ms"
        )
    for query_result in suite["queries"]:
        if query_result["status"] == "passed":
            print(
                "    - "
                f"{query_result['name']} [{query_result['category']}]: "
                f"execute_p50={query_result['execute']['p50_ms']:.2f} ms, "
                f"execute_p95={query_result['execute']['p95_ms']:.2f} ms, "
                f"end_to_end_p50={query_result['end_to_end']['p50_ms']:.2f} ms"
            )
            continue
        if query_result["status"] == "skipped":
            print(
                "    - "
                f"{query_result['name']} [{query_result['category']}]: "
                f"SKIPPED ({query_result.get('skip_reason')})"
            )
            continue
        if query_result["status"] == "timed_out":
            timeout = query_result["query_timeout"]
            print(
                "    - "
                f"{query_result['name']} [{query_result['category']}]: "
                "TIMED OUT "
                f"(phase={timeout['phase']}, iteration={timeout['iteration']})"
            )
            continue
        print(
            "    - "
            f"{query_result['name']} [{query_result['category']}]: "
            f"FAILED {query_result['error_type']}: {query_result['error_message']}"
        )


def _auto_docker_enabled() -> bool:
    flag = os.environ.get(AGE_AUTO_DOCKER_ENV)
    if flag is None:
        return False
    return flag.strip().lower() in {"1", "true", "yes", "on"}


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Validate and benchmark the runtime Cypher corpus directly on Apache "
            "AGE (openCypher run natively inside PostgreSQL) using the same "
            "synthetic graph shape as the other runtime harnesses."
        )
    )
    parser.add_argument("--corpus", type=Path, default=DEFAULT_CORPUS_PATH)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT_PATH)
    parser.add_argument("--iterations", type=int, default=10)
    parser.add_argument("--warmup", type=int, default=1)
    parser.add_argument("--oltp-iterations", type=int)
    parser.add_argument("--oltp-warmup", type=int)
    parser.add_argument("--olap-iterations", type=int)
    parser.add_argument("--olap-warmup", type=int)
    parser.add_argument("--oltp-timeout-ms", type=float)
    parser.add_argument("--olap-timeout-ms", type=float)
    parser.add_argument(
        "--iteration-progress",
        action="store_true",
        help="Print warmup and measured iteration counters for each query.",
    )
    parser.add_argument("--query-name", action="append", dest="query_names")
    parser.add_argument(
        "--index-mode",
        choices=("indexed", "unindexed", "both"),
        default="both",
    )
    parser.add_argument(
        "--age-dsn",
        default=os.environ.get(AGE_DSN_ENV),
        help=(
            "PostgreSQL DSN for an existing Apache AGE server. Defaults to "
            f"${AGE_DSN_ENV}. When unset, --docker (or "
            f"${AGE_AUTO_DOCKER_ENV}=1) starts a disposable container."
        ),
    )
    parser.add_argument("--graph-name", default=DEFAULT_GRAPH_NAME)
    parser.add_argument(
        "--docker",
        action="store_true",
        help="Start a disposable local apache/age container automatically.",
    )
    parser.add_argument(
        "--docker-image",
        default=os.environ.get(AGE_IMAGE_ENV, DEFAULT_AGE_IMAGE),
        help="Docker image to use when --docker is enabled.",
    )
    parser.add_argument(
        "--docker-container-name",
        help="Optional Docker container name. Defaults to a timestamped name.",
    )
    parser.add_argument(
        "--docker-pg-port",
        type=int,
        help="Local PostgreSQL port to publish. Defaults to a free port.",
    )
    parser.add_argument("--docker-db", default=DEFAULT_AGE_DB)
    parser.add_argument("--docker-user", default=DEFAULT_AGE_USER)
    parser.add_argument("--docker-password", default=DEFAULT_AGE_PASSWORD)
    parser.add_argument(
        "--docker-cpuset-cpus",
        help="Optional --cpuset-cpus passed to the apache/age container.",
    )
    parser.add_argument(
        "--docker-startup-timeout",
        type=int,
        default=120,
        help="Seconds to wait for the Docker AGE instance to become ready.",
    )
    parser.add_argument(
        "--docker-keep-container",
        action="store_true",
        help="Keep the Docker AGE container running after the benchmark finishes.",
    )
    parser.add_argument("--node-type-count", type=int, default=4)
    parser.add_argument("--edge-type-count", type=int, default=4)
    parser.add_argument("--nodes-per-type", type=int, default=25_000)
    parser.add_argument("--edges-per-source", type=int, default=3)
    parser.add_argument(
        "--edge-degree-profile",
        choices=("uniform", "skewed"),
        default="uniform",
    )
    parser.add_argument("--node-extra-text-property-count", type=int, default=2)
    parser.add_argument("--node-extra-numeric-property-count", type=int, default=6)
    parser.add_argument("--node-extra-boolean-property-count", type=int, default=2)
    parser.add_argument("--edge-extra-text-property-count", type=int, default=1)
    parser.add_argument("--edge-extra-numeric-property-count", type=int, default=3)
    parser.add_argument("--edge-extra-boolean-property-count", type=int, default=1)
    parser.add_argument("--variable-hop-max", type=int, default=2)
    parser.add_argument("--ingest-batch-size", type=int, default=5_000)
    add_topology_cli_args(parser)
    add_cpu_affinity_cli_arg(parser)
    return parser.parse_args()


def main() -> int:
    args = _parse_args()
    apply_cpu_affinity(parse_cpu_affinity(args.cpu_affinity))
    started_at = datetime.now(timezone.utc)
    args_dict = vars(args)
    oltp_timeout_ms = args_dict.get("oltp_timeout_ms", None)
    olap_timeout_ms = args_dict.get("olap_timeout_ms", None)
    if args.iterations <= 0:
        raise ValueError("--iterations must be positive.")
    if args.warmup < 0:
        raise ValueError("--warmup must be non-negative.")
    if args.oltp_iterations is not None and args.oltp_iterations <= 0:
        raise ValueError("--oltp-iterations must be positive.")
    if args.oltp_warmup is not None and args.oltp_warmup < 0:
        raise ValueError("--oltp-warmup must be non-negative.")
    if args.olap_iterations is not None and args.olap_iterations <= 0:
        raise ValueError("--olap-iterations must be positive.")
    if args.olap_warmup is not None and args.olap_warmup < 0:
        raise ValueError("--olap-warmup must be non-negative.")
    if oltp_timeout_ms is not None and oltp_timeout_ms <= 0:
        raise ValueError("--oltp-timeout-ms must be positive.")
    if olap_timeout_ms is not None and olap_timeout_ms <= 0:
        raise ValueError("--olap-timeout-ms must be positive.")
    if args.node_type_count <= 0:
        raise ValueError("--node-type-count must be positive.")
    if args.edge_type_count <= 0:
        raise ValueError("--edge-type-count must be positive.")
    if args.nodes_per_type <= 0:
        raise ValueError("--nodes-per-type must be positive.")
    if args.edges_per_source <= 0:
        raise ValueError("--edges-per-source must be positive.")
    if args.variable_hop_max <= 0:
        raise ValueError("--variable-hop-max must be positive.")
    if args.ingest_batch_size <= 0:
        raise ValueError("--ingest-batch-size must be positive.")
    if args.docker_startup_timeout <= 0:
        raise ValueError("--docker-startup-timeout must be positive.")

    if psycopg2 is None:
        raise ValueError(
            "psycopg2 is not installed. Install it with "
            "`uv pip install psycopg2-binary`."
        )

    scale = RuntimeScale(
        node_type_count=args.node_type_count,
        edge_type_count=args.edge_type_count,
        nodes_per_type=args.nodes_per_type,
        edges_per_source=args.edges_per_source,
        edge_degree_profile=args.edge_degree_profile,
        node_extra_text_property_count=args.node_extra_text_property_count,
        node_extra_numeric_property_count=args.node_extra_numeric_property_count,
        node_extra_boolean_property_count=args.node_extra_boolean_property_count,
        edge_extra_text_property_count=args.edge_extra_text_property_count,
        edge_extra_numeric_property_count=args.edge_extra_numeric_property_count,
        edge_extra_boolean_property_count=args.edge_extra_boolean_property_count,
        ingest_batch_size=args.ingest_batch_size,
        variable_hop_max=args.variable_hop_max,
    )

    queries = _select_queries(_load_corpus(args.corpus), args.query_names)

    use_docker = args.docker or (args.age_dsn is None and _auto_docker_enabled())
    docker_config: DockerAgeConfig | None = None
    age_dsn = args.age_dsn
    if use_docker:
        if shutil.which("docker") is None:
            raise ValueError(
                "--docker requested but docker is not available in PATH."
            )
        docker_config = DockerAgeConfig(
            image=args.docker_image,
            container_name=(
                args.docker_container_name or _docker_default_container_name()
            ),
            pg_port=args.docker_pg_port or _find_free_tcp_port(),
            db=args.docker_db,
            user=args.docker_user,
            password=args.docker_password,
            startup_timeout_s=args.docker_startup_timeout,
            keep_container=args.docker_keep_container,
            cpuset_cpus=args.docker_cpuset_cpus,
        )
        age_dsn = docker_config.dsn
    if not age_dsn:
        raise ValueError(
            "No Apache AGE connection configured. Provide --age-dsn / "
            f"${AGE_DSN_ENV}, pass --docker, or set ${AGE_AUTO_DOCKER_ENV}=1."
        )

    topology = resolve_topology(
        args.topology, ldbc_snb_data_dir=args.ldbc_snb_data_dir
    )
    graph_schema, _ = topology.build_schema(scale)
    schema_context = cypherglot.CompilerSchemaContext.type_aware(graph_schema)

    _progress(
        "age runtime benchmark: starting "
        f"({len(queries)} queries, iterations={args.iterations}, "
        f"warmup={args.warmup}, index_mode={args.index_mode})"
    )

    connect_ms: float | None = None
    connect_rss_mib: dict[str, float | None] | None = None
    database_versions: dict[str, str] = {}

    def write_checkpoint(
        result: dict[str, object],
        *,
        failure_count: int,
        status: str,
    ) -> None:
        payload = _build_payload(
            started_at=started_at,
            database_versions=database_versions,
            age_dsn=age_dsn,
            graph_name=args.graph_name,
            docker_config=docker_config,
            corpus_path=args.corpus,
            queries=queries,
            scale=scale,
            graph_schema=graph_schema,
            index_mode=args.index_mode,
            default_iterations=args.iterations,
            default_warmup=args.warmup,
            oltp_iterations=(
                args.oltp_iterations
                if args.oltp_iterations is not None
                else args.iterations
            ),
            oltp_warmup=(
                args.oltp_warmup if args.oltp_warmup is not None else args.warmup
            ),
            oltp_timeout_ms=oltp_timeout_ms,
            olap_iterations=(
                args.olap_iterations
                if args.olap_iterations is not None
                else args.iterations
            ),
            olap_warmup=(
                args.olap_warmup if args.olap_warmup is not None else args.warmup
            ),
            olap_timeout_ms=olap_timeout_ms,
            connect_ms=connect_ms,
            connect_rss_mib=connect_rss_mib,
            result=result,
            failure_count=failure_count,
            status=status,
            completed_at=datetime.now(timezone.utc) if status == "completed" else None,
        )
        _write_json_atomic(args.output, payload)

    write_checkpoint({"workloads": {}, "token_map": {}}, failure_count=0, status="running")

    if docker_config is not None:
        _start_docker_age(docker_config)

    try:
        conn, connect_ns = _measure_ns(
            lambda: _wait_for_connection_ready(
                age_dsn,
                docker_config.startup_timeout_s if docker_config is not None else 15,
            )
        )
        connect_ms = connect_ns / 1_000_000.0
        connect_rss_mib = _capture_age_rss_snapshot(docker_config)
        database_versions = _age_server_versions(conn)
    except Exception:
        if docker_config is not None:
            logs = _docker_logs(docker_config)
            if logs:
                _progress("age runtime benchmark: container logs follow")
                print(logs, file=sys.stderr, flush=True)
            if not docker_config.keep_container:
                _stop_docker_age(docker_config)
        raise
    _progress(f"age runtime benchmark: connected ({age_dsn}, graph={args.graph_name})")

    try:
        result, failure_count = _benchmark_result(
            conn,
            graph_name=args.graph_name,
            queries=queries,
            schema_context=schema_context,
            iterations=args.iterations,
            warmup=args.warmup,
            oltp_iterations=args.oltp_iterations,
            oltp_warmup=args.oltp_warmup,
            olap_iterations=args.olap_iterations,
            olap_warmup=args.olap_warmup,
            docker_config=docker_config,
            scale=scale,
            index_mode=args.index_mode,
            iteration_progress=args.iteration_progress,
            oltp_timeout_ms=oltp_timeout_ms,
            olap_timeout_ms=olap_timeout_ms,
            topology=topology,
            progress_callback=lambda partial_result, partial_failure_count: (
                write_checkpoint(
                    partial_result,
                    failure_count=partial_failure_count,
                    status="running",
                )
            ),
        )
    finally:
        conn.close()
        if docker_config is not None and not docker_config.keep_container:
            _stop_docker_age(docker_config)

    write_checkpoint(result, failure_count=failure_count, status="completed")

    _progress(f"age runtime benchmark: wrote baseline to {args.output}")
    print(f"Wrote Apache AGE runtime benchmark baseline to {args.output}")
    workloads = result["workloads"]
    if "oltp" in workloads:
        for suite_name, suite in workloads["oltp"].items():
            if suite_name == "description":
                continue
            _print_suite(f"oltp/{suite_name}", suite)
    if "olap" in workloads:
        for suite_name, suite in workloads["olap"].items():
            if suite_name == "description":
                continue
            _print_suite(f"olap/{suite_name}", suite)
    return 1 if failure_count else 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
