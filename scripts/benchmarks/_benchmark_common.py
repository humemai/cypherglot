"""Shared helpers for the runtime benchmark scripts.

Functions and dataclasses used by both benchmark_sqlite_runtime.py and
benchmark_neo4j_runtime.py are collected here to avoid duplication.
"""

from __future__ import annotations

import json
import platform
import resource
import sys
import time
import uuid
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Callable

import cypherglot


RuntimeProgressCallback = Callable[[dict[str, object]], None]
SKEWED_EDGE_DEGREE_CYCLE = 1_000


def _progress(message: str) -> None:
    timestamp = datetime.now().strftime("%H:%M:%S")
    print(f"[progress {timestamp}] {message}", file=sys.stderr, flush=True)


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
        cypherglot.PropertyField(_extra_node_boolean_property_name(index), "boolean")
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
        cypherglot.PropertyField(_extra_edge_boolean_property_name(index), "boolean")
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
        return min(5, 2 + int((4 * (bucket_position ** 3.0)) + 1e-9))
    if cycle_position < 950:
        bucket_position = (cycle_position - 700) / 249.0
        return min(15, 6 + int((10 * (bucket_position ** 1.5)) + 1e-9))

    bucket_position = (cycle_position - 950) / 49.0
    return min(200, 20 + int((181 * (bucket_position ** 2.6)) + 1e-9))


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


def _write_json_atomic(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = path.with_name(f"{path.name}.{uuid.uuid4().hex}.tmp")
    temp_path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    temp_path.replace(path)


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
