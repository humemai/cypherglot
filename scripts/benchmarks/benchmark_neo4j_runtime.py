"""Validate and benchmark the runtime corpus directly on Neo4j.

This script seeds a synthetic graph into Neo4j Community Edition using the same
runtime corpus and graph-shape knobs as the SQLite runtime harness. Its primary
goal is compatibility validation: confirm that the same Cypher corpus entries
execute successfully on Neo4j. It also records coarse setup and execution
timings so the run is comparable in shape to the SQLite benchmark outputs.
"""

from __future__ import annotations

import argparse
import gc
import json
import os
import platform
import resource
import subprocess
import sys
import time
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Callable

import cypherglot

try:
    from neo4j import GraphDatabase
    from neo4j.exceptions import Neo4jError, ServiceUnavailable
except ImportError:  # pragma: no cover - optional dependency
    GraphDatabase = None

    class Neo4jError(Exception):
        pass

    class ServiceUnavailable(Exception):
        pass


REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_CORPUS_PATH = (
    REPO_ROOT
    / "scripts"
    / "benchmarks"
    / "corpora"
    / "sqlite_runtime_benchmark_corpus.json"
)
DEFAULT_OUTPUT_PATH = (
    REPO_ROOT
    / "scripts"
    / "benchmarks"
    / "results"
    / "neo4j_runtime_benchmark.json"
)
SKEWED_EDGE_DEGREE_CYCLE = 1_000
RuntimeProgressCallback = Callable[[dict[str, object], int], None]


@dataclass(frozen=True, slots=True)
class DockerNeo4jConfig:
    image: str
    container_name: str
    bolt_port: int
    http_port: int
    startup_timeout_s: int
    keep_container: bool


def _progress(message: str) -> None:
    timestamp = datetime.now().strftime("%H:%M:%S")
    print(f"[progress {timestamp}] {message}", file=sys.stderr, flush=True)


def _run_command(command: list[str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        command,
        check=False,
        capture_output=True,
        text=True,
    )


def _docker_default_container_name() -> str:
    return "benchmark-neo4j-runtime-" + datetime.now(UTC).strftime("%Y%m%d%H%M%S")


def _docker_run_command(config: DockerNeo4jConfig, password: str) -> list[str]:
    return [
        "docker",
        "run",
        "--detach",
        "--rm",
        "--name",
        config.container_name,
        "--publish",
        f"{config.bolt_port}:7687",
        "--publish",
        f"{config.http_port}:7474",
        "--env",
        f"NEO4J_AUTH=neo4j/{password}",
        config.image,
    ]


def _start_docker_neo4j(config: DockerNeo4jConfig, password: str) -> None:
    _progress(
        f"neo4j runtime benchmark: starting Docker container {config.container_name}"
    )
    result = _run_command(_docker_run_command(config, password))
    if result.returncode == 0:
        return
    stderr = result.stderr.strip()
    raise RuntimeError(
        "Failed to start Neo4j Docker container. "
        f"docker stderr: {stderr or 'none'}"
    )


def _stop_docker_neo4j(config: DockerNeo4jConfig) -> None:
    _progress(
        f"neo4j runtime benchmark: stopping Docker container {config.container_name}"
    )
    _run_command(["docker", "rm", "-f", config.container_name])


def _docker_logs(config: DockerNeo4jConfig) -> str:
    result = _run_command(["docker", "logs", config.container_name])
    if result.returncode != 0:
        return result.stderr.strip()
    return result.stdout.strip()


def _wait_for_docker_server_ready(
    config: DockerNeo4jConfig,
    timeout_s: int,
) -> None:
    deadline = time.monotonic() + timeout_s
    last_logs = ""
    ready_markers = (
        "Bolt enabled on",
        "Remote interface available",
        "Started.",
    )
    while time.monotonic() < deadline:
        logs = _docker_logs(config)
        if any(marker in logs for marker in ready_markers):
            return
        last_logs = logs
        time.sleep(1.0)
    raise RuntimeError(
        "Timed out waiting for Neo4j Docker server startup. "
        f"Last logs: {last_logs[-1000:] if last_logs else 'none'}"
    )


def _wait_for_driver_ready(
    uri: str,
    user: str,
    password: str,
    timeout_s: int,
) -> Any:
    deadline = time.monotonic() + timeout_s
    last_error: str | None = None
    while time.monotonic() < deadline:
        try:
            return _create_driver(uri, user, password)
        except (Neo4jError, ServiceUnavailable, OSError, ValueError) as exc:
            last_error = str(exc)
            time.sleep(1.0)
    raise RuntimeError(
        "Timed out waiting for Neo4j to accept Bolt connections. "
        f"Last error: {last_error or 'unknown'}"
    )


@dataclass(frozen=True, slots=True)
class RuntimeScale:
    node_type_count: int = 4
    edge_type_count: int = 4
    nodes_per_type: int = 25_000
    edges_per_source: int = 3
    edge_degree_profile: str = "uniform"
    node_extra_text_property_count: int = 2
    node_extra_numeric_property_count: int = 6
    node_extra_boolean_property_count: int = 2
    edge_extra_text_property_count: int = 1
    edge_extra_numeric_property_count: int = 3
    edge_extra_boolean_property_count: int = 1
    ingest_batch_size: int = 5_000
    variable_hop_max: int = 2

    @property
    def total_nodes(self) -> int:
        return self.node_type_count * self.nodes_per_type

    @property
    def total_edges(self) -> int:
        return _estimated_total_edges(self)


@dataclass(frozen=True, slots=True)
class EdgeTypePlan:
    type_index: int
    name: str
    source_type_index: int
    target_type_index: int


@dataclass(frozen=True, slots=True)
class CorpusQuery:
    name: str
    workload: str
    category: str
    query: str
    backends: tuple[str, ...]
    mode: str = "statement"
    mutation: bool = False


def _node_type_name(type_index: int) -> str:
    return f"NodeType{type_index:02d}"


def _edge_type_name(type_index: int) -> str:
    return f"EdgeType{type_index:02d}"


def _node_id(scale: RuntimeScale, type_index: int, local_index: int) -> int:
    return ((type_index - 1) * scale.nodes_per_type) + local_index


def _node_name(type_index: int, local_index: int) -> str:
    return f"{_node_type_name(type_index).lower()}-{local_index:06d}"


def _edge_plans(scale: RuntimeScale) -> list[EdgeTypePlan]:
    plans: list[EdgeTypePlan] = []
    for edge_type_index in range(1, scale.edge_type_count + 1):
        if edge_type_index == 1:
            source_type_index = 1
            target_type_index = 1
        elif edge_type_index == 2:
            source_type_index = 1
            target_type_index = min(2, scale.node_type_count)
        elif edge_type_index == 3:
            source_type_index = min(2, scale.node_type_count)
            target_type_index = min(3, scale.node_type_count)
        else:
            source_type_index = ((edge_type_index - 1) % scale.node_type_count) + 1
            target_type_index = (source_type_index % scale.node_type_count) + 1
        plans.append(
            EdgeTypePlan(
                type_index=edge_type_index,
                name=_edge_type_name(edge_type_index),
                source_type_index=source_type_index,
                target_type_index=target_type_index,
            )
        )
    return plans


def _extra_node_text_property_name(property_index: int) -> str:
    return f"text_{property_index:02d}"


def _extra_node_numeric_property_name(property_index: int) -> str:
    return f"num_{property_index:02d}"


def _extra_node_boolean_property_name(property_index: int) -> str:
    return f"flag_{property_index:02d}"


def _extra_edge_text_property_name(property_index: int) -> str:
    return f"text_{property_index:02d}"


def _extra_edge_numeric_property_name(property_index: int) -> str:
    return f"num_{property_index:02d}"


def _extra_edge_boolean_property_name(property_index: int) -> str:
    return f"flag_{property_index:02d}"


def _build_graph_schema(
    scale: RuntimeScale,
) -> tuple[cypherglot.GraphSchema, list[EdgeTypePlan]]:
    edge_plans = _edge_plans(scale)

    node_properties = [
        cypherglot.PropertyField("name", "string"),
        cypherglot.PropertyField("age", "integer"),
        cypherglot.PropertyField("score", "float"),
        cypherglot.PropertyField("active", "boolean"),
    ]
    node_properties.extend(
        cypherglot.PropertyField(_extra_node_text_property_name(index), "string")
        for index in range(1, scale.node_extra_text_property_count + 1)
    )
    node_properties.extend(
        cypherglot.PropertyField(_extra_node_numeric_property_name(index), "float")
        for index in range(1, scale.node_extra_numeric_property_count + 1)
    )
    node_properties.extend(
        cypherglot.PropertyField(
            _extra_node_boolean_property_name(index),
            "boolean",
        )
        for index in range(1, scale.node_extra_boolean_property_count + 1)
    )

    edge_properties = [
        cypherglot.PropertyField("note", "string"),
        cypherglot.PropertyField("weight", "float"),
        cypherglot.PropertyField("score", "float"),
        cypherglot.PropertyField("active", "boolean"),
        cypherglot.PropertyField("rank", "integer"),
    ]
    edge_properties.extend(
        cypherglot.PropertyField(_extra_edge_text_property_name(index), "string")
        for index in range(1, scale.edge_extra_text_property_count + 1)
    )
    edge_properties.extend(
        cypherglot.PropertyField(_extra_edge_numeric_property_name(index), "float")
        for index in range(1, scale.edge_extra_numeric_property_count + 1)
    )
    edge_properties.extend(
        cypherglot.PropertyField(
            _extra_edge_boolean_property_name(index),
            "boolean",
        )
        for index in range(1, scale.edge_extra_boolean_property_count + 1)
    )

    graph_schema = cypherglot.GraphSchema(
        node_types=tuple(
            cypherglot.NodeTypeSpec(
                name=_node_type_name(type_index),
                properties=tuple(node_properties),
            )
            for type_index in range(1, scale.node_type_count + 1)
        ),
        edge_types=tuple(
            cypherglot.EdgeTypeSpec(
                name=plan.name,
                source_type=_node_type_name(plan.source_type_index),
                target_type=_node_type_name(plan.target_type_index),
                properties=tuple(edge_properties),
            )
            for plan in edge_plans
        ),
    )
    return graph_schema, edge_plans


def _skewed_edge_degree(cycle_position: int) -> int:
    if not 0 <= cycle_position < SKEWED_EDGE_DEGREE_CYCLE:
        raise ValueError("cycle_position must be within the skewed degree cycle.")

    if cycle_position < 700:
        bucket_position = cycle_position / 699.0 if cycle_position else 0.0
        return min(5, 2 + int((4 * (bucket_position**3.0)) + 1e-9))
    if cycle_position < 950:
        bucket_position = (cycle_position - 700) / 249.0
        return min(15, 6 + int((10 * (bucket_position**1.5)) + 1e-9))

    bucket_position = (cycle_position - 950) / 49.0
    return min(200, 20 + int((181 * (bucket_position**2.6)) + 1e-9))


def _edge_out_degree(scale: RuntimeScale, source_local_index: int) -> int:
    if scale.edge_degree_profile == "uniform":
        return scale.edges_per_source
    if scale.edge_degree_profile == "skewed":
        cycle_position = (source_local_index - 1) % SKEWED_EDGE_DEGREE_CYCLE
        return _skewed_edge_degree(cycle_position)
    raise ValueError(f"Unsupported edge degree profile {scale.edge_degree_profile!r}.")


def _sum_out_degree_per_edge_type(scale: RuntimeScale) -> int:
    if scale.edge_degree_profile == "uniform":
        return scale.nodes_per_type * scale.edges_per_source

    full_cycles, remainder = divmod(scale.nodes_per_type, SKEWED_EDGE_DEGREE_CYCLE)
    cycle_sum = sum(
        _skewed_edge_degree(cycle_position)
        for cycle_position in range(SKEWED_EDGE_DEGREE_CYCLE)
    )
    remainder_sum = sum(
        _skewed_edge_degree(cycle_position)
        for cycle_position in range(remainder)
    )
    return (full_cycles * cycle_sum) + remainder_sum


def _estimated_total_edges(scale: RuntimeScale) -> int:
    return scale.edge_type_count * _sum_out_degree_per_edge_type(scale)


def _average_edges_per_source(scale: RuntimeScale) -> float:
    return _sum_out_degree_per_edge_type(scale) / float(scale.nodes_per_type)


def _measure_ns(callback: Any) -> tuple[Any, int]:
    started_ns = time.perf_counter_ns()
    result = callback()
    return result, time.perf_counter_ns() - started_ns


def _progress_iteration(
    progress_label: str,
    *,
    phase: str,
    current: int,
    total: int,
) -> None:
    _progress(f"{progress_label}: {phase} {current}/{total}")


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


def _rss_mib() -> float:
    status_path = Path("/proc/self/status")
    if status_path.exists():
        for line in status_path.read_text(encoding="utf-8").splitlines():
            if line.startswith("VmRSS:"):
                rss_kib = int(line.split()[1])
                return rss_kib / 1024.0

    usage = resource.getrusage(resource.RUSAGE_SELF)
    rss = float(usage.ru_maxrss)
    if platform.system() == "Darwin":
        return rss / (1024.0 * 1024.0)
    return rss / 1024.0


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


def _write_json_atomic(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = path.with_name(f"{path.name}.{uuid.uuid4().hex}.tmp")
    temp_path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    temp_path.replace(path)


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


def _token_map(
    scale: RuntimeScale,
    graph_schema: cypherglot.GraphSchema,
    edge_plans: list[EdgeTypePlan],
) -> dict[str, str]:
    token_map: dict[str, str] = {
        "variable_hop_max": str(scale.variable_hop_max),
        "grouped_rollup_variable_hop_max": str(min(scale.variable_hop_max, 3)),
        "created_type_1_name": f"{_node_type_name(1).lower()}-created-node",
        "created_type_1_peer_name": f"{_node_type_name(1).lower()}-created-peer",
        "created_type_2_name": (
            f"{_node_type_name(min(2, scale.node_type_count)).lower()}-created-node"
        ),
    }
    for type_index, node_type in enumerate(graph_schema.node_types, start=1):
        token_map[f"node_type_{type_index}"] = node_type.name
        for local_index in range(1, min(scale.nodes_per_type, 4) + 1):
            token_map[f"node_type_{type_index}_name_{local_index}"] = _node_name(
                type_index,
                local_index,
            )
    for edge_type_index, plan in enumerate(edge_plans, start=1):
        token_map[f"edge_type_{edge_type_index}"] = plan.name
    return token_map


def _expand_query_tokens(text: str, token_map: dict[str, str]) -> str:
    expanded = text
    for key, value in token_map.items():
        expanded = expanded.replace(f"%{key}%", value)
    return expanded


def _render_corpus_queries(
    queries: list[CorpusQuery],
    token_map: dict[str, str],
) -> list[CorpusQuery]:
    return [
        CorpusQuery(
            name=query.name,
            workload=query.workload,
            category=query.category,
            query=_expand_query_tokens(query.query, token_map),
            backends=query.backends,
            mode=query.mode,
            mutation=query.mutation,
        )
        for query in queries
    ]


def _select_queries(
    queries: list[CorpusQuery],
    query_names: list[str] | None,
) -> list[CorpusQuery]:
    if not query_names:
        return list(queries)
    requested = set(query_names)
    selected = [query for query in queries if query.name in requested]
    found = {query.name for query in selected}
    missing = sorted(requested - found)
    if missing:
        raise ValueError("Unknown benchmark query name(s): " + ", ".join(missing))
    return selected


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


def _seed_constraint_statements(graph_schema: cypherglot.GraphSchema) -> list[str]:
    statements: list[str] = []
    for node_type in graph_schema.node_types:
        constraint_name = f"cg_seed_{node_type.table_name}_id_unique"
        statements.append(
            f"CREATE CONSTRAINT `{constraint_name}` IF NOT EXISTS "
            f"FOR (n:`{node_type.name}`) REQUIRE n.id IS UNIQUE"
        )
    return statements


def _query_index_statements(graph_schema: cypherglot.GraphSchema) -> list[str]:
    statements: list[str] = []
    for node_type in graph_schema.node_types:
        statements.append(
            f"CREATE INDEX `cg_query_{node_type.table_name}_name` IF NOT EXISTS "
            f"FOR (n:`{node_type.name}`) ON (n.name)"
        )
        statements.append(
            f"CREATE INDEX `cg_query_{node_type.table_name}_active_score` "
            f"IF NOT EXISTS FOR (n:`{node_type.name}`) ON (n.active, n.score)"
        )
        statements.append(
            f"CREATE INDEX `cg_query_{node_type.table_name}_age` IF NOT EXISTS "
            f"FOR (n:`{node_type.name}`) ON (n.age)"
        )
    for edge_type in graph_schema.edge_types:
        statements.append(
            f"CREATE INDEX `cg_query_{edge_type.table_name}_rank` IF NOT EXISTS "
            f"FOR ()-[r:`{edge_type.name}`]-() ON (r.rank)"
        )
        statements.append(
            f"CREATE INDEX `cg_query_{edge_type.table_name}_active_score` "
            f"IF NOT EXISTS FOR ()-[r:`{edge_type.name}`]-() ON (r.active, r.score)"
        )
    return statements


def _drop_query_indexes(session: Any) -> None:
    records = session.run(
        "SHOW INDEXES YIELD name "
        "WHERE name STARTS WITH 'cg_query_' "
        "RETURN name"
    )
    for record in records:
        session.run(f"DROP INDEX `{record['name']}` IF EXISTS").consume()


RESET_DELETE_BATCH_SIZE = 10_000


def _reset_graph(session: Any) -> None:
    _drop_query_indexes(session)
    while True:
        deleted = session.run(
            "MATCH (n) "
            "WITH n LIMIT $batch_size "
            "DETACH DELETE n "
            "RETURN count(n) AS deleted",
            batch_size=RESET_DELETE_BATCH_SIZE,
        ).single()["deleted"]
        if deleted == 0:
            break


def _ensure_seed_constraints(
    session: Any,
    graph_schema: cypherglot.GraphSchema,
) -> None:
    for statement in _seed_constraint_statements(graph_schema):
        session.run(statement).consume()
    session.run("CALL db.awaitIndexes()").consume()


def _seed_nodes(
    session: Any,
    *,
    scale: RuntimeScale,
    graph_schema: cypherglot.GraphSchema,
    progress_label: str,
) -> int:
    total = 0
    for type_index, node_type in enumerate(graph_schema.node_types, start=1):
        _progress(
            f"{progress_label}: node type {type_index}/{scale.node_type_count} "
            f"({node_type.name})"
        )
        batch: list[dict[str, object]] = []
        query = (
            f"UNWIND $rows AS row "
            f"CREATE (n:`{node_type.name}`) "
            f"SET n = row"
        )
        for local_index in range(1, scale.nodes_per_type + 1):
            batch.append(_node_properties(scale, type_index, local_index))
            if len(batch) < scale.ingest_batch_size:
                continue
            session.run(query, rows=batch).consume()
            total += len(batch)
            batch.clear()
        if batch:
            session.run(query, rows=batch).consume()
            total += len(batch)
    return total


def _seed_relationships(
    session: Any,
    *,
    scale: RuntimeScale,
    graph_schema: cypherglot.GraphSchema,
    edge_plans: list[EdgeTypePlan],
    progress_label: str,
) -> int:
    total = 0
    for edge_type_index, plan in enumerate(edge_plans, start=1):
        _progress(
            f"{progress_label}: edge type {edge_type_index}/{scale.edge_type_count} "
            f"({plan.name})"
        )
        source_label = graph_schema.node_types[plan.source_type_index - 1].name
        target_label = graph_schema.node_types[plan.target_type_index - 1].name
        query = (
            f"UNWIND $rows AS row "
            f"MATCH (a:`{source_label}` {{id: row.from_id}}) "
            f"MATCH (b:`{target_label}` {{id: row.to_id}}) "
            f"CREATE (a)-[r:`{plan.name}`]->(b) "
            f"SET r = row.props"
        )
        batch: list[dict[str, object]] = []
        for source_local_index in range(1, scale.nodes_per_type + 1):
            edge_count_for_source = _edge_out_degree(scale, source_local_index)
            from_id = _node_id(scale, plan.source_type_index, source_local_index)
            for edge_ordinal in range(1, edge_count_for_source + 1):
                target_local_index = (
                    (source_local_index - 1 + plan.type_index + edge_ordinal)
                    % scale.nodes_per_type
                ) + 1
                to_id = _node_id(scale, plan.target_type_index, target_local_index)
                batch.append(
                    {
                        "from_id": from_id,
                        "to_id": to_id,
                        "props": _edge_properties(
                            scale,
                            plan,
                            source_local_index,
                            edge_ordinal,
                        ),
                    }
                )
                if len(batch) < scale.ingest_batch_size:
                    continue
                session.run(query, rows=batch).consume()
                total += len(batch)
                batch.clear()
        if batch:
            session.run(query, rows=batch).consume()
            total += len(batch)
    return total


def _seed_graph(
    session: Any,
    *,
    scale: RuntimeScale,
    graph_schema: cypherglot.GraphSchema,
    edge_plans: list[EdgeTypePlan],
    progress_label: str,
) -> dict[str, int]:
    node_count = _seed_nodes(
        session,
        scale=scale,
        graph_schema=graph_schema,
        progress_label=progress_label,
    )
    edge_count = _seed_relationships(
        session,
        scale=scale,
        graph_schema=graph_schema,
        edge_plans=edge_plans,
        progress_label=progress_label,
    )
    _progress(
        f"{progress_label}: ingest committed ({node_count} nodes, {edge_count} edges)"
    )
    return {
        "node_count": node_count,
        "edge_count": edge_count,
        "node_type_count": scale.node_type_count,
        "edge_type_count": scale.edge_type_count,
    }


def _create_query_indexes(session: Any, graph_schema: cypherglot.GraphSchema) -> None:
    for statement in _query_index_statements(graph_schema):
        session.run(statement).consume()
    session.run("CALL db.awaitIndexes()").consume()


def _create_driver(uri: str, user: str, password: str) -> Any:
    if GraphDatabase is None:
        raise ValueError(
            "neo4j is not installed. Install it with `uv pip install neo4j`."
        )
    driver = GraphDatabase.driver(uri, auth=(user, password))
    driver.verify_connectivity()
    return driver


def _setup_mode(
    driver: Any,
    *,
    database: str,
    index_mode: str,
    scale: RuntimeScale,
    graph_schema: cypherglot.GraphSchema,
    edge_plans: list[EdgeTypePlan],
) -> dict[str, object]:
    progress_label = f"neo4j/{index_mode}"
    setup_metrics: dict[str, int] = {}
    rss_snapshots_mib: dict[str, float] = {}

    _progress(
        f"{progress_label}: preparing graph "
        f"({scale.total_nodes} nodes, {scale.total_edges} edges)"
    )
    with driver.session(database=database) as session:
        _, setup_metrics["reset_ns"] = _measure_ns(lambda: _reset_graph(session))
        rss_snapshots_mib["after_reset"] = _rss_mib()
        _, setup_metrics["seed_constraints_ns"] = _measure_ns(
            lambda: _ensure_seed_constraints(session, graph_schema)
        )
        rss_snapshots_mib["after_seed_constraints"] = _rss_mib()
        row_counts, setup_metrics["ingest_ns"] = _measure_ns(
            lambda: _seed_graph(
                session,
                scale=scale,
                graph_schema=graph_schema,
                edge_plans=edge_plans,
                progress_label=progress_label,
            )
        )
        rss_snapshots_mib["after_ingest"] = _rss_mib()

        if index_mode == "indexed":
            _progress(f"{progress_label}: creating query indexes")
            _, setup_metrics["index_ns"] = _measure_ns(
                lambda: _create_query_indexes(session, graph_schema)
            )
        else:
            setup_metrics["index_ns"] = 0
        rss_snapshots_mib["after_index"] = _rss_mib()

    _progress(
        f"{progress_label}: fixture ready "
        f"(ingest={setup_metrics['ingest_ns'] / 1_000_000_000.0:.2f}s)"
    )
    return {
        "setup_metrics": setup_metrics,
        "row_counts": row_counts,
        "rss_snapshots_mib": rss_snapshots_mib,
        "index_mode": index_mode,
    }


def _run_query_once(
    driver: Any,
    *,
    database: str,
    query: CorpusQuery,
) -> dict[str, int]:
    reset_ns = 0
    with driver.session(database=database) as session:
        transaction = session.begin_transaction()
        try:
            total_started_ns = time.perf_counter_ns()
            result = transaction.run(query.query)
            list(result)
            execute_ns = time.perf_counter_ns() - total_started_ns
            end_to_end_ns = execute_ns
        except Exception:
            transaction.rollback()
            raise
        if query.mutation:
            _, reset_ns = _measure_ns(transaction.rollback)
        else:
            transaction.commit()
    return {
        "execute_ns": execute_ns,
        "end_to_end_ns": end_to_end_ns,
        "reset_ns": reset_ns,
    }


def _measure_query(
    driver: Any,
    *,
    database: str,
    index_mode: str,
    query: CorpusQuery,
    iterations: int,
    warmup: int,
    progress_label: str,
    iteration_progress: bool,
) -> dict[str, object]:
    try:
        for warmup_index in range(1, warmup + 1):
            if iteration_progress:
                _progress_iteration(
                    progress_label,
                    phase="warmup",
                    current=warmup_index,
                    total=warmup,
                )
            _run_query_once(driver, database=database, query=query)
    except Neo4jError as exc:
        return {
            "name": query.name,
            "workload": query.workload,
            "category": query.category,
            "backend": "neo4j",
            "index_mode": index_mode,
            "mode": query.mode,
            "mutation": query.mutation,
            "status": "failed",
            "error_type": type(exc).__name__,
            "error_message": str(exc),
        }

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
            metrics = _run_query_once(driver, database=database, query=query)
            execute_latencies.append(metrics["execute_ns"])
            end_to_end_latencies.append(metrics["end_to_end_ns"])
            reset_latencies.append(metrics["reset_ns"])
    except Neo4jError as exc:
        return {
            "name": query.name,
            "workload": query.workload,
            "category": query.category,
            "backend": "neo4j",
            "index_mode": index_mode,
            "mode": query.mode,
            "mutation": query.mutation,
            "status": "failed",
            "error_type": type(exc).__name__,
            "error_message": str(exc),
        }
    finally:
        if gc_was_enabled:
            gc.enable()

    return {
        "name": query.name,
        "workload": query.workload,
        "category": query.category,
        "backend": "neo4j",
        "index_mode": index_mode,
        "mode": query.mode,
        "mutation": query.mutation,
        "status": "passed",
        "execute": _summarize(execute_latencies),
        "end_to_end": _summarize(end_to_end_latencies),
        "reset": _summarize(reset_latencies),
    }


def _run_workload_suite(
    driver: Any,
    *,
    database: str,
    workload: str,
    index_mode: str,
    queries: list[CorpusQuery],
    iterations: int,
    warmup: int,
    setup: dict[str, object],
    iteration_progress: bool,
) -> dict[str, object]:
    suite_name = f"{workload}/neo4j_{index_mode}"
    _progress(
        f"{suite_name}: starting suite with {len(queries)} queries "
        f"({iterations} iterations, {warmup} warmup)"
    )
    query_results = []
    for query_index, query in enumerate(queries, start=1):
        query_progress_label = (
            f"{suite_name}: query {query_index}/{len(queries)} {query.name}"
        )
        _progress(query_progress_label)
        query_results.append(
            _measure_query(
                driver,
                database=database,
                index_mode=index_mode,
                query=query,
                iterations=iterations,
                warmup=warmup,
                progress_label=query_progress_label,
                iteration_progress=iteration_progress,
            )
        )
    _progress(f"{suite_name}: suite complete")

    failures = [result for result in query_results if result["status"] == "failed"]
    return {
        "backend": "neo4j",
        "index_mode": index_mode,
        "iterations": iterations,
        "warmup": warmup,
        "query_count": len(queries),
        "pass_count": len(query_results) - len(failures),
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
        "rss_snapshots_mib": setup["rss_snapshots_mib"],
        "execute": _pool_summaries(query_results, "execute"),
        "end_to_end": _pool_summaries(query_results, "end_to_end"),
        "reset": _pool_summaries(query_results, "reset"),
        "queries": query_results,
    }


def _benchmark_result(
    driver: Any,
    *,
    database: str,
    queries: list[CorpusQuery],
    iterations: int,
    warmup: int,
    oltp_iterations: int | None,
    oltp_warmup: int | None,
    olap_iterations: int | None,
    olap_warmup: int | None,
    scale: RuntimeScale,
    index_mode: str,
    iteration_progress: bool,
    progress_callback: RuntimeProgressCallback | None = None,
) -> tuple[dict[str, object], int]:
    graph_schema, edge_plans = _build_graph_schema(scale)
    token_map = _token_map(scale, graph_schema, edge_plans)
    rendered_queries = _render_corpus_queries(queries, token_map)

    oltp_queries = [query for query in rendered_queries if query.workload == "oltp"]
    olap_queries = [query for query in rendered_queries if query.workload == "olap"]
    oltp_iterations_value = iterations if oltp_iterations is None else oltp_iterations
    oltp_warmup_value = warmup if oltp_warmup is None else oltp_warmup
    olap_iterations_value = iterations if olap_iterations is None else olap_iterations
    olap_warmup_value = warmup if olap_warmup is None else olap_warmup

    index_modes = [index_mode] if index_mode != "both" else ["indexed", "unindexed"]
    workloads: dict[str, object] = {}
    failure_count = 0

    if progress_callback is not None:
        progress_callback(
            {"workloads": workloads, "token_map": token_map},
            failure_count,
        )

    for mode in index_modes:
        setup = _setup_mode(
            driver,
            database=database,
            index_mode=mode,
            scale=scale,
            graph_schema=graph_schema,
            edge_plans=edge_plans,
        )
        if oltp_queries:
            workloads.setdefault(
                "oltp",
                {
                    "description": (
                        "Transactional-style Neo4j execution over the generated "
                        "graph using the runtime corpus directly as Cypher."
                    )
                },
            )
            suite = _run_workload_suite(
                driver,
                database=database,
                workload="oltp",
                index_mode=mode,
                queries=oltp_queries,
                iterations=oltp_iterations_value,
                warmup=oltp_warmup_value,
                setup=setup,
                iteration_progress=iteration_progress,
            )
            workloads["oltp"][f"neo4j_{mode}"] = suite
            failure_count += int(suite["fail_count"])
            if progress_callback is not None:
                progress_callback(
                    {"workloads": workloads, "token_map": token_map},
                    failure_count,
                )

        if olap_queries:
            workloads.setdefault(
                "olap",
                {
                    "description": (
                        "Analytical-style Neo4j execution over the generated graph "
                        "using the runtime corpus directly as Cypher."
                    )
                },
            )
            suite = _run_workload_suite(
                driver,
                database=database,
                workload="olap",
                index_mode=mode,
                queries=olap_queries,
                iterations=olap_iterations_value,
                warmup=olap_warmup_value,
                setup=setup,
                iteration_progress=iteration_progress,
            )
            workloads["olap"][f"neo4j_{mode}"] = suite
            failure_count += int(suite["fail_count"])
            if progress_callback is not None:
                progress_callback(
                    {"workloads": workloads, "token_map": token_map},
                    failure_count,
                )

    return {
        "workloads": workloads,
        "token_map": token_map,
    }, failure_count


def _build_payload(
    *,
    started_at: datetime,
    neo4j_uri: str,
    neo4j_database: str,
    neo4j_user: str,
    docker_config: DockerNeo4jConfig | None,
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
    connect_ms: float | None,
    result: dict[str, object],
    failure_count: int,
    status: str,
    completed_at: datetime | None = None,
) -> dict[str, object]:
    payload: dict[str, object] = {
        "generated_at": started_at.isoformat(),
        "updated_at": datetime.now(UTC).isoformat(),
        "run_status": status,
        "python_version": platform.python_version(),
        "platform": platform.platform(),
        "cypherglot_version": cypherglot.__version__,
        "neo4j": {
            "uri": neo4j_uri,
            "database": neo4j_database,
            "user": neo4j_user,
            "docker": (
                {
                    "image": docker_config.image,
                    "container_name": docker_config.container_name,
                    "bolt_port": docker_config.bolt_port,
                    "http_port": docker_config.http_port,
                    "keep_container": docker_config.keep_container,
                }
                if docker_config is not None
                else None
            ),
        },
        "corpus_path": str(corpus_path),
        "workload_counts": {
            "oltp": len([query for query in queries if query.workload == "oltp"]),
            "olap": len([query for query in queries if query.workload == "olap"]),
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
            "olap_iterations": olap_iterations,
            "olap_warmup": olap_warmup,
        },
        "setup": {
            "connect_ms": connect_ms,
        },
        "results": result,
        "failure_count": failure_count,
    }
    if completed_at is not None:
        payload["completed_at"] = completed_at.isoformat()
    return payload


def _print_suite(name: str, suite: dict[str, object]) -> None:
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
            f"{key}={value:.2f} MiB"
            for key, value in suite["rss_snapshots_mib"].items()
        )
    )
    print(
        "  status: "
        f"passed={suite['pass_count']}, failed={suite['fail_count']}"
    )
    if suite["pass_count"]:
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
        if query_result["status"] == "passed":
            print(
                "    - "
                f"{query_result['name']} [{query_result['category']}]: "
                f"execute_p50={query_result['execute']['p50_ms']:.2f} ms, "
                f"execute_p95={query_result['execute']['p95_ms']:.2f} ms, "
                f"end_to_end_p50={query_result['end_to_end']['p50_ms']:.2f} ms, "
                f"end_to_end_p95={query_result['end_to_end']['p95_ms']:.2f} ms"
            )
            continue
        print(
            "    - "
            f"{query_result['name']} [{query_result['category']}]: "
            f"FAILED {query_result['error_type']}: {query_result['error_message']}"
        )


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Validate and benchmark the runtime Cypher corpus directly on Neo4j "
            "using the same synthetic graph shape as the SQLite runtime harness."
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
        "--neo4j-uri",
        default=os.environ.get("NEO4J_URI", "bolt://localhost:7687"),
    )
    parser.add_argument(
        "--neo4j-user",
        default=os.environ.get("NEO4J_USER", "neo4j"),
    )
    parser.add_argument(
        "--neo4j-password",
        default=os.environ.get("NEO4J_PASSWORD"),
    )
    parser.add_argument(
        "--neo4j-database",
        default=os.environ.get("NEO4J_DATABASE", "neo4j"),
    )
    parser.add_argument(
        "--docker",
        action="store_true",
        help="Start a disposable local Neo4j Community container automatically.",
    )
    parser.add_argument(
        "--docker-image",
        default="neo4j:5-community",
        help="Docker image to use when --docker is enabled.",
    )
    parser.add_argument(
        "--docker-container-name",
        help="Optional Docker container name. Defaults to a timestamped name.",
    )
    parser.add_argument(
        "--docker-bolt-port",
        type=int,
        default=7687,
        help="Local Bolt port to publish when --docker is enabled.",
    )
    parser.add_argument(
        "--docker-http-port",
        type=int,
        default=7474,
        help="Local HTTP port to publish when --docker is enabled.",
    )
    parser.add_argument(
        "--docker-startup-timeout",
        type=int,
        default=120,
        help="Seconds to wait for the Docker Neo4j instance to become ready.",
    )
    parser.add_argument(
        "--docker-keep-container",
        action="store_true",
        help="Keep the Docker Neo4j container running after the benchmark finishes.",
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
    return parser.parse_args()


def main() -> int:
    args = _parse_args()
    started_at = datetime.now(UTC)
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
    if args.docker_bolt_port <= 0:
        raise ValueError("--docker-bolt-port must be positive.")
    if args.docker_http_port <= 0:
        raise ValueError("--docker-http-port must be positive.")
    if args.docker_startup_timeout <= 0:
        raise ValueError("--docker-startup-timeout must be positive.")
    if not args.neo4j_password:
        raise ValueError(
            "Provide --neo4j-password or set NEO4J_PASSWORD in the environment."
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
    docker_config = None
    neo4j_uri = args.neo4j_uri
    if args.docker:
        docker_config = DockerNeo4jConfig(
            image=args.docker_image,
            container_name=(
                args.docker_container_name or _docker_default_container_name()
            ),
            bolt_port=args.docker_bolt_port,
            http_port=args.docker_http_port,
            startup_timeout_s=args.docker_startup_timeout,
            keep_container=args.docker_keep_container,
        )
        neo4j_uri = f"bolt://127.0.0.1:{docker_config.bolt_port}"
    _progress(
        "neo4j runtime benchmark: starting "
        f"({len(queries)} queries, iterations={args.iterations}, "
        f"warmup={args.warmup}, index_mode={args.index_mode})"
    )

    graph_schema, _ = _build_graph_schema(scale)
    connect_ms: float | None = None

    def write_checkpoint(
        result: dict[str, object],
        *,
        failure_count: int,
        status: str,
    ) -> None:
        payload = _build_payload(
            started_at=started_at,
            neo4j_uri=neo4j_uri,
            neo4j_database=args.neo4j_database,
            neo4j_user=args.neo4j_user,
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
            olap_iterations=(
                args.olap_iterations
                if args.olap_iterations is not None
                else args.iterations
            ),
            olap_warmup=(
                args.olap_warmup if args.olap_warmup is not None else args.warmup
            ),
            connect_ms=connect_ms,
            result=result,
            failure_count=failure_count,
            status=status,
            completed_at=datetime.now(UTC) if status == "completed" else None,
        )
        _write_json_atomic(args.output, payload)

    write_checkpoint(
        {"workloads": {}, "token_map": {}},
        failure_count=0,
        status="running",
    )

    if docker_config is not None:
        _start_docker_neo4j(docker_config, args.neo4j_password)
        _wait_for_docker_server_ready(
            docker_config,
            docker_config.startup_timeout_s,
        )
    try:
        driver, connect_ns = _measure_ns(
            lambda: _wait_for_driver_ready(
                neo4j_uri,
                args.neo4j_user,
                args.neo4j_password,
                (
                    docker_config.startup_timeout_s
                    if docker_config is not None
                    else 15
                ),
            )
        )
        connect_ms = connect_ns / 1_000_000.0
    except Exception:
        if docker_config is not None:
            logs = _docker_logs(docker_config)
            if logs:
                _progress("neo4j runtime benchmark: container logs follow")
                print(logs, file=sys.stderr, flush=True)
            if not docker_config.keep_container:
                _stop_docker_neo4j(docker_config)
        raise
    _progress(
        "neo4j runtime benchmark: connected "
        f"({neo4j_uri}, database={args.neo4j_database})"
    )
    try:
        result, failure_count = _benchmark_result(
            driver,
            database=args.neo4j_database,
            queries=queries,
            iterations=args.iterations,
            warmup=args.warmup,
            oltp_iterations=args.oltp_iterations,
            oltp_warmup=args.oltp_warmup,
            olap_iterations=args.olap_iterations,
            olap_warmup=args.olap_warmup,
            scale=scale,
            index_mode=args.index_mode,
            iteration_progress=args.iteration_progress,
            progress_callback=lambda partial_result, partial_failure_count: (
                write_checkpoint(
                    partial_result,
                    failure_count=partial_failure_count,
                    status="running",
                )
            ),
        )
    finally:
        driver.close()
        if docker_config is not None and not docker_config.keep_container:
            _stop_docker_neo4j(docker_config)

    write_checkpoint(
        result,
        failure_count=failure_count,
        status="completed",
    )

    _progress(f"neo4j runtime benchmark: wrote baseline to {args.output}")
    print(f"Wrote Neo4j runtime benchmark baseline to {args.output}")
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
