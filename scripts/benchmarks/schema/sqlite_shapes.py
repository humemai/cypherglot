"""Benchmark SQLite schema-shape scaling costs for graph-oriented storage.

This script generates synthetic multi-type node and edge schemas, measures how
query and setup costs change across the supported schema layouts, and writes a
JSON baseline with scale parameters, derived edge specifications, environment
metadata, and per-schema benchmark results.
"""

from __future__ import annotations

import argparse
import gc
import json
import platform
import sqlite3
import tempfile
import time
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path

from scripts.benchmarks.common.cli import parse_sqlite_schema_shapes_args
from scripts.benchmarks.common.shared import (
    _edge_type_name,
    _node_name,
    _node_type_name,
    _rss_mib,
    _summarize,
)


REPO_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_OUTPUT_PATH = (
    REPO_ROOT
    / "scripts"
    / "benchmarks"
    / "results"
    / "schema"
    / "sqlite_schema_shape_benchmark.json"
)


def _progress(message: str) -> None:
    timestamp = datetime.now().strftime("%H:%M:%S")
    print(f"[progress {timestamp}] {message}", flush=True)


@dataclass(frozen=True, slots=True)
class SchemaShapeScale:
    node_type_count: int = 10
    edge_type_count: int = 10
    nodes_per_type: int = 5_000
    edges_per_source: int = 4
    multi_hop_length: int = 5
    node_numeric_property_count: int = 10
    node_text_property_count: int = 2
    node_boolean_property_count: int = 2
    edge_numeric_property_count: int = 6
    edge_text_property_count: int = 2
    edge_boolean_property_count: int = 1

    @property
    def total_nodes(self) -> int:
        return self.node_type_count * self.nodes_per_type

    @property
    def total_edges(self) -> int:
        return self.edge_type_count * self.nodes_per_type * self.edges_per_source


@dataclass(frozen=True, slots=True)
class EdgeTypeSpec:
    type_index: int
    name: str
    source_type_index: int
    target_type_index: int


@dataclass(frozen=True, slots=True)
class SchemaQuery:
    name: str
    category: str
    sql_by_schema: dict[str, str]


def _node_table_name(type_index: int) -> str:
    return f"node_type_{type_index:02d}_nodes"


def _edge_table_name(type_index: int) -> str:
    return f"edge_type_{type_index:02d}_edges"


def _node_id(scale: SchemaShapeScale, type_index: int, local_index: int) -> int:
    return (type_index - 1) * scale.nodes_per_type + local_index


def _edge_specs(scale: SchemaShapeScale) -> list[EdgeTypeSpec]:
    specs: list[EdgeTypeSpec] = []
    for edge_type_index in range(1, scale.edge_type_count + 1):
        source_type_index = ((edge_type_index - 1) % scale.node_type_count) + 1
        target_type_index = (source_type_index % scale.node_type_count) + 1
        specs.append(
            EdgeTypeSpec(
                type_index=edge_type_index,
                name=_edge_type_name(edge_type_index),
                source_type_index=source_type_index,
                target_type_index=target_type_index,
            )
        )
    return specs


def _batched_insert(
    conn: sqlite3.Connection,
    sql: str,
    rows: list[tuple[object, ...]],
    batch_size: int,
) -> None:
    if not rows:
        return
    for start in range(0, len(rows), batch_size):
        conn.executemany(sql, rows[start:start + batch_size])


def _json_node_properties(
    scale: SchemaShapeScale,
    type_index: int,
    local_index: int,
) -> str:
    properties: dict[str, object] = {"name": _node_name(type_index, local_index)}
    for property_index in range(1, scale.node_text_property_count + 1):
        properties[f"text_{property_index:02d}"] = (
            f"{_node_type_name(type_index).lower()}-text-{property_index:02d}-"
            f"{local_index:06d}"
        )
    for property_index in range(1, scale.node_numeric_property_count + 1):
        properties[f"num_{property_index:02d}"] = round(
            property_index
            + ((type_index * 17 + local_index * (property_index + 5)) % 10_000) / 100.0,
            2,
        )
    for property_index in range(1, scale.node_boolean_property_count + 1):
        properties[f"flag_{property_index:02d}"] = (
            (type_index + local_index + property_index) % (property_index + 2) != 0
        )
    return json.dumps(properties, separators=(",", ":"))


def _json_edge_properties(
    scale: SchemaShapeScale,
    edge_type_index: int,
    source_local_index: int,
    edge_ordinal: int,
) -> str:
    properties: dict[str, object] = {
        "note": (
            f"edge-type-{edge_type_index:02d}-hop-"
            f"{edge_ordinal:02d}-{source_local_index:06d}"
        )
    }
    for property_index in range(1, scale.edge_text_property_count + 1):
        properties[f"text_{property_index:02d}"] = (
            f"edge-{edge_type_index:02d}-text-"
            f"{property_index:02d}-{source_local_index:06d}"
        )
    for property_index in range(1, scale.edge_numeric_property_count + 1):
        properties[f"num_{property_index:02d}"] = round(
            property_index
            + (
                (
                    edge_type_index * 11
                    + source_local_index * (property_index + 3)
                    + edge_ordinal
                )
                % 5_000
            )
            / 100.0,
            2,
        )
    for property_index in range(1, scale.edge_boolean_property_count + 1):
        properties[f"flag_{property_index:02d}"] = (
            (
                edge_type_index
                + source_local_index
                + edge_ordinal
                + property_index
            )
            % 2
            == 0
        )
    return json.dumps(properties, separators=(",", ":"))


def _create_sqlite_connection(db_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode = WAL")
    conn.execute("PRAGMA synchronous = NORMAL")
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


def _measure_ms(fn):
    start = time.perf_counter_ns()
    result = fn()
    end = time.perf_counter_ns()
    return result, (end - start) / 1_000_000.0


def _benchmark_query(
    conn: sqlite3.Connection,
    sql: str,
    *,
    warmup: int,
    iterations: int,
) -> dict[str, float]:
    for _ in range(warmup):
        conn.execute(sql).fetchall()

    samples_ns: list[int] = []
    gc_was_enabled = gc.isenabled()
    gc.disable()
    try:
        for _ in range(iterations):
            start = time.perf_counter_ns()
            conn.execute(sql).fetchall()
            end = time.perf_counter_ns()
            samples_ns.append(end - start)
    finally:
        if gc_was_enabled:
            gc.enable()
    return _summarize(samples_ns)


def _create_json_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE TABLE nodes (
          node_id INTEGER PRIMARY KEY,
          node_type TEXT NOT NULL,
          properties TEXT NOT NULL,
          CHECK (json_valid(properties)),
          CHECK (json_type(properties) = 'object')
        ) STRICT;

        CREATE TABLE edges (
          edge_id INTEGER PRIMARY KEY,
          edge_type TEXT NOT NULL,
          from_node_id INTEGER NOT NULL,
          to_node_id INTEGER NOT NULL,
          properties TEXT NOT NULL,
          CHECK (json_valid(properties)),
          CHECK (json_type(properties) = 'object')
        ) STRICT;
        """
    )


def _create_json_indexes(conn: sqlite3.Connection) -> None:
        conn.executescript(
                """
                CREATE INDEX idx_nodes_type_name
                    ON nodes(node_type, json_extract(properties, '$.name'));
                CREATE INDEX idx_nodes_type_flag_num
                    ON nodes(
                        node_type,
                        json_extract(properties, '$.flag_01'),
                        CAST(json_extract(properties, '$.num_01') AS REAL) DESC
                    );
                CREATE INDEX idx_edges_type_from_to
                    ON edges(edge_type, from_node_id, to_node_id);
                CREATE INDEX idx_edges_type_to_from
                    ON edges(edge_type, to_node_id, from_node_id);
                """
        )


def _seed_json_schema(
    conn: sqlite3.Connection,
    *,
    scale: SchemaShapeScale,
    batch_size: int,
) -> dict[str, int]:
    node_rows: list[tuple[object, ...]] = []
    edge_rows: list[tuple[object, ...]] = []
    node_count = 0
    edge_count = 0
    edge_id = 1

    for type_index in range(1, scale.node_type_count + 1):
        for local_index in range(1, scale.nodes_per_type + 1):
            node_rows.append(
                (
                    _node_id(scale, type_index, local_index),
                    _node_type_name(type_index),
                    _json_node_properties(scale, type_index, local_index),
                )
            )
            node_count += 1
            if len(node_rows) >= batch_size:
                conn.executemany(
                    (
                        "INSERT INTO nodes(node_id, node_type, properties) "
                        "VALUES (?, ?, ?)"
                    ),
                    node_rows,
                )
                node_rows.clear()

    for spec in _edge_specs(scale):
        for source_local_index in range(1, scale.nodes_per_type + 1):
            from_node_id = _node_id(scale, spec.source_type_index, source_local_index)
            for edge_ordinal in range(1, scale.edges_per_source + 1):
                target_local_index = (
                    (
                        source_local_index
                        - 1
                        + spec.type_index
                        + edge_ordinal
                    )
                    % scale.nodes_per_type
                ) + 1
                edge_rows.append(
                    (
                        edge_id,
                        spec.name,
                        from_node_id,
                        _node_id(scale, spec.target_type_index, target_local_index),
                        _json_edge_properties(
                            scale,
                            spec.type_index,
                            source_local_index,
                            edge_ordinal,
                        ),
                    )
                )
                edge_id += 1
                edge_count += 1
                if len(edge_rows) >= batch_size:
                    conn.executemany(
                        (
                            "INSERT INTO edges(edge_id, edge_type, from_node_id, "
                            "to_node_id, properties) VALUES (?, ?, ?, ?, ?)"
                        ),
                        edge_rows,
                    )
                    edge_rows.clear()

    if node_rows:
        conn.executemany(
            "INSERT INTO nodes(node_id, node_type, properties) VALUES (?, ?, ?)",
            node_rows,
        )
    if edge_rows:
        conn.executemany(
            (
                "INSERT INTO edges(edge_id, edge_type, from_node_id, to_node_id, "
                "properties) VALUES (?, ?, ?, ?, ?)"
            ),
            edge_rows,
        )
    conn.commit()
    return {"node_count": node_count, "edge_count": edge_count}


def _create_typed_property_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE TABLE nodes (
          node_id INTEGER PRIMARY KEY,
          node_type TEXT NOT NULL
        ) STRICT;

        CREATE TABLE edges (
          edge_id INTEGER PRIMARY KEY,
          edge_type TEXT NOT NULL,
          from_node_id INTEGER NOT NULL,
          to_node_id INTEGER NOT NULL
        ) STRICT;

        CREATE TABLE node_props_text (
          node_id INTEGER NOT NULL,
          property_key TEXT NOT NULL,
          value TEXT NOT NULL,
          PRIMARY KEY (node_id, property_key)
        ) STRICT;

        CREATE TABLE node_props_num (
          node_id INTEGER NOT NULL,
          property_key TEXT NOT NULL,
          value REAL NOT NULL,
          PRIMARY KEY (node_id, property_key)
        ) STRICT;

        CREATE TABLE node_props_bool (
          node_id INTEGER NOT NULL,
          property_key TEXT NOT NULL,
          value INTEGER NOT NULL,
          PRIMARY KEY (node_id, property_key)
        ) STRICT;

        CREATE TABLE edge_props_text (
          edge_id INTEGER NOT NULL,
          property_key TEXT NOT NULL,
          value TEXT NOT NULL,
          PRIMARY KEY (edge_id, property_key)
        ) STRICT;

        CREATE TABLE edge_props_num (
          edge_id INTEGER NOT NULL,
          property_key TEXT NOT NULL,
          value REAL NOT NULL,
          PRIMARY KEY (edge_id, property_key)
        ) STRICT;

        CREATE TABLE edge_props_bool (
          edge_id INTEGER NOT NULL,
          property_key TEXT NOT NULL,
          value INTEGER NOT NULL,
          PRIMARY KEY (edge_id, property_key)
        ) STRICT;
        """
    )


def _create_typed_property_indexes(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE INDEX idx_nodes_type_node_id ON nodes(node_type, node_id);
        CREATE INDEX idx_edges_type_from_to
            ON edges(edge_type, from_node_id, to_node_id);
        CREATE INDEX idx_edges_type_to_from
            ON edges(edge_type, to_node_id, from_node_id);
        CREATE INDEX idx_node_props_text_key_value
            ON node_props_text(property_key, value, node_id);
        CREATE INDEX idx_node_props_num_key_value
            ON node_props_num(property_key, value, node_id);
        CREATE INDEX idx_node_props_bool_key_value
            ON node_props_bool(property_key, value, node_id);
        CREATE INDEX idx_edge_props_text_key_value
            ON edge_props_text(property_key, value, edge_id);
        CREATE INDEX idx_edge_props_num_key_value
            ON edge_props_num(property_key, value, edge_id);
        CREATE INDEX idx_edge_props_bool_key_value
            ON edge_props_bool(property_key, value, edge_id);
        """
    )


def _seed_typed_property_schema(
    conn: sqlite3.Connection,
    *,
    scale: SchemaShapeScale,
    batch_size: int,
) -> dict[str, int]:
    node_rows: list[tuple[object, ...]] = []
    node_text_rows: list[tuple[object, ...]] = []
    node_num_rows: list[tuple[object, ...]] = []
    node_bool_rows: list[tuple[object, ...]] = []
    edge_rows: list[tuple[object, ...]] = []
    edge_text_rows: list[tuple[object, ...]] = []
    edge_num_rows: list[tuple[object, ...]] = []
    edge_bool_rows: list[tuple[object, ...]] = []
    node_count = 0
    edge_count = 0
    edge_id = 1

    def flush_node_batches() -> None:
        if node_rows:
            conn.executemany(
                "INSERT INTO nodes(node_id, node_type) VALUES (?, ?)",
                node_rows,
            )
            node_rows.clear()
        if node_text_rows:
            conn.executemany(
                (
                    "INSERT INTO node_props_text(node_id, property_key, value) "
                    "VALUES (?, ?, ?)"
                ),
                node_text_rows,
            )
            node_text_rows.clear()
        if node_num_rows:
            conn.executemany(
                (
                    "INSERT INTO node_props_num(node_id, property_key, value) "
                    "VALUES (?, ?, ?)"
                ),
                node_num_rows,
            )
            node_num_rows.clear()
        if node_bool_rows:
            conn.executemany(
                (
                    "INSERT INTO node_props_bool(node_id, property_key, value) "
                    "VALUES (?, ?, ?)"
                ),
                node_bool_rows,
            )
            node_bool_rows.clear()

    def flush_edge_batches() -> None:
        if edge_rows:
            conn.executemany(
                (
                    "INSERT INTO edges(edge_id, edge_type, from_node_id, to_node_id) "
                    "VALUES (?, ?, ?, ?)"
                ),
                edge_rows,
            )
            edge_rows.clear()
        if edge_text_rows:
            conn.executemany(
                (
                    "INSERT INTO edge_props_text(edge_id, property_key, value) "
                    "VALUES (?, ?, ?)"
                ),
                edge_text_rows,
            )
            edge_text_rows.clear()
        if edge_num_rows:
            conn.executemany(
                (
                    "INSERT INTO edge_props_num(edge_id, property_key, value) "
                    "VALUES (?, ?, ?)"
                ),
                edge_num_rows,
            )
            edge_num_rows.clear()
        if edge_bool_rows:
            conn.executemany(
                (
                    "INSERT INTO edge_props_bool(edge_id, property_key, value) "
                    "VALUES (?, ?, ?)"
                ),
                edge_bool_rows,
            )
            edge_bool_rows.clear()

    for type_index in range(1, scale.node_type_count + 1):
        type_name = _node_type_name(type_index)
        for local_index in range(1, scale.nodes_per_type + 1):
            node_id = _node_id(scale, type_index, local_index)
            node_rows.append((node_id, type_name))
            node_count += 1
            node_text_rows.append(
                (node_id, "name", _node_name(type_index, local_index))
            )
            for property_index in range(1, scale.node_text_property_count + 1):
                node_text_rows.append(
                    (
                        node_id,
                        f"text_{property_index:02d}",
                        (
                            f"{type_name.lower()}-text-{property_index:02d}-"
                            f"{local_index:06d}"
                        ),
                    )
                )
            for property_index in range(1, scale.node_numeric_property_count + 1):
                node_num_rows.append(
                    (
                        node_id,
                        f"num_{property_index:02d}",
                        round(
                            property_index
                            + (
                                (
                                    type_index * 17
                                    + local_index * (property_index + 5)
                                )
                                % 10_000
                            )
                            / 100.0,
                            2,
                        ),
                    )
                )
            for property_index in range(1, scale.node_boolean_property_count + 1):
                node_bool_rows.append(
                    (
                        node_id,
                        f"flag_{property_index:02d}",
                        (
                            1
                            if (type_index + local_index + property_index)
                            % (property_index + 2)
                            != 0
                            else 0
                        ),
                    )
                )
            if len(node_rows) >= batch_size:
                flush_node_batches()

    for spec in _edge_specs(scale):
        for source_local_index in range(1, scale.nodes_per_type + 1):
            from_node_id = _node_id(scale, spec.source_type_index, source_local_index)
            for edge_ordinal in range(1, scale.edges_per_source + 1):
                target_local_index = (
                    (
                        source_local_index
                        - 1
                        + spec.type_index
                        + edge_ordinal
                    )
                    % scale.nodes_per_type
                ) + 1
                edge_rows.append(
                    (
                        edge_id,
                        _edge_type_name(spec.type_index),
                        from_node_id,
                        _node_id(scale, spec.target_type_index, target_local_index),
                    )
                )
                edge_text_rows.append(
                    (
                        edge_id,
                        "note",
                        (
                            f"edge-type-{spec.type_index:02d}-hop-"
                            f"{edge_ordinal:02d}-{source_local_index:06d}"
                        ),
                    )
                )
                for property_index in range(1, scale.edge_text_property_count + 1):
                    edge_text_rows.append(
                        (
                            edge_id,
                            f"text_{property_index:02d}",
                            (
                                f"edge-{spec.type_index:02d}-text-"
                                f"{property_index:02d}-{source_local_index:06d}"
                            ),
                        )
                    )
                for property_index in range(1, scale.edge_numeric_property_count + 1):
                    edge_num_rows.append(
                        (
                            edge_id,
                            f"num_{property_index:02d}",
                            round(
                                property_index
                                + (
                                    (
                                        spec.type_index * 11
                                        + source_local_index
                                        * (property_index + 3)
                                        + edge_ordinal
                                    )
                                    % 5_000
                                )
                                / 100.0,
                                2,
                            ),
                        )
                    )
                for property_index in range(1, scale.edge_boolean_property_count + 1):
                    edge_bool_rows.append(
                        (
                            edge_id,
                            f"flag_{property_index:02d}",
                            (
                                1
                                if (
                                    spec.type_index
                                    + source_local_index
                                    + edge_ordinal
                                    + property_index
                                )
                                % 2
                                == 0
                                else 0
                            ),
                        )
                    )
                edge_id += 1
                edge_count += 1
                if len(edge_rows) >= batch_size:
                    flush_edge_batches()

    flush_node_batches()
    flush_edge_batches()
    conn.commit()
    return {
        "node_count": node_count,
        "edge_count": edge_count,
        "node_property_rows": (
            node_count
            * (
                1
                + scale.node_text_property_count
                + scale.node_numeric_property_count
                + scale.node_boolean_property_count
            )
        ),
        "edge_property_rows": (
            edge_count
            * (
                1
                + scale.edge_text_property_count
                + scale.edge_numeric_property_count
                + scale.edge_boolean_property_count
            )
        ),
    }


def _create_type_aware_schema(
    conn: sqlite3.Connection,
    scale: SchemaShapeScale,
) -> None:
    statements: list[str] = []

    node_text_columns = [
        f"text_{property_index:02d} TEXT NOT NULL"
        for property_index in range(1, scale.node_text_property_count + 1)
    ]
    node_num_columns = [
        f"num_{property_index:02d} REAL NOT NULL"
        for property_index in range(1, scale.node_numeric_property_count + 1)
    ]
    node_bool_columns = [
        (
            f"flag_{property_index:02d} INTEGER NOT NULL "
            f"CHECK (flag_{property_index:02d} IN (0, 1))"
        )
        for property_index in range(1, scale.node_boolean_property_count + 1)
    ]

    edge_text_columns = [
        f"text_{property_index:02d} TEXT NOT NULL"
        for property_index in range(1, scale.edge_text_property_count + 1)
    ]
    edge_num_columns = [
        f"num_{property_index:02d} REAL NOT NULL"
        for property_index in range(1, scale.edge_numeric_property_count + 1)
    ]
    edge_bool_columns = [
        (
            f"flag_{property_index:02d} INTEGER NOT NULL "
            f"CHECK (flag_{property_index:02d} IN (0, 1))"
        )
        for property_index in range(1, scale.edge_boolean_property_count + 1)
    ]

    for type_index in range(1, scale.node_type_count + 1):
        table_name = _node_table_name(type_index)
        columns = ["node_id INTEGER PRIMARY KEY", "name TEXT NOT NULL"]
        columns.extend(node_text_columns)
        columns.extend(node_num_columns)
        columns.extend(node_bool_columns)
        statements.append(f"CREATE TABLE {table_name} ({', '.join(columns)}) STRICT;")

    for spec in _edge_specs(scale):
        table_name = _edge_table_name(spec.type_index)
        columns = [
            "edge_id INTEGER PRIMARY KEY",
            "from_node_id INTEGER NOT NULL",
            "to_node_id INTEGER NOT NULL",
            "note TEXT NOT NULL",
        ]
        columns.extend(edge_text_columns)
        columns.extend(edge_num_columns)
        columns.extend(edge_bool_columns)
        statements.append(f"CREATE TABLE {table_name} ({', '.join(columns)}) STRICT;")

    conn.executescript("\n".join(statements))


def _create_type_aware_indexes(
    conn: sqlite3.Connection,
    scale: SchemaShapeScale,
) -> None:
    statements: list[str] = []
    for type_index in range(1, scale.node_type_count + 1):
        table_name = _node_table_name(type_index)
        statements.append(
            f"CREATE INDEX idx_{table_name}_name ON {table_name}(name);"
        )
        statements.append(
            (
                f"CREATE INDEX idx_{table_name}_flag_num "
                f"ON {table_name}(flag_01, num_01 DESC);"
            )
        )

    for spec in _edge_specs(scale):
        table_name = _edge_table_name(spec.type_index)
        statements.append(
            (
                f"CREATE INDEX idx_{table_name}_from_to "
                f"ON {table_name}(from_node_id, to_node_id);"
            )
        )
        statements.append(
            (
                f"CREATE INDEX idx_{table_name}_to_from "
                f"ON {table_name}(to_node_id, from_node_id);"
            )
        )

    conn.executescript("\n".join(statements))


def _seed_type_aware_schema(
    conn: sqlite3.Connection,
    *,
    scale: SchemaShapeScale,
    batch_size: int,
) -> dict[str, int]:
    node_count = 0
    for type_index in range(1, scale.node_type_count + 1):
        table_name = _node_table_name(type_index)
        column_names = ["node_id", "name"]
        column_names.extend(
            f"text_{property_index:02d}"
            for property_index in range(1, scale.node_text_property_count + 1)
        )
        column_names.extend(
            f"num_{property_index:02d}"
            for property_index in range(1, scale.node_numeric_property_count + 1)
        )
        column_names.extend(
            f"flag_{property_index:02d}"
            for property_index in range(1, scale.node_boolean_property_count + 1)
        )
        placeholders = ", ".join("?" for _ in column_names)
        rows: list[tuple[object, ...]] = []
        for local_index in range(1, scale.nodes_per_type + 1):
            row: list[object] = [
                _node_id(scale, type_index, local_index),
                _node_name(type_index, local_index),
            ]
            node_count += 1
            for property_index in range(1, scale.node_text_property_count + 1):
                row.append(
                    (
                        f"{_node_type_name(type_index).lower()}-text-"
                        f"{property_index:02d}-{local_index:06d}"
                    )
                )
            for property_index in range(1, scale.node_numeric_property_count + 1):
                row.append(
                    round(
                        property_index
                        + (
                            (
                                type_index * 17
                                + local_index * (property_index + 5)
                            )
                            % 10_000
                        )
                        / 100.0,
                        2,
                    )
                )
            for property_index in range(1, scale.node_boolean_property_count + 1):
                row.append(
                    (
                        1
                        if (type_index + local_index + property_index)
                        % (property_index + 2)
                        != 0
                        else 0
                    )
                )
            rows.append(tuple(row))
            if len(rows) >= batch_size:
                conn.executemany(
                    (
                        f"INSERT INTO {table_name}({', '.join(column_names)}) "
                        f"VALUES ({placeholders})"
                    ),
                    rows,
                )
                rows.clear()
        if rows:
            conn.executemany(
                (
                    f"INSERT INTO {table_name}({', '.join(column_names)}) "
                    f"VALUES ({placeholders})"
                ),
                rows,
            )

    edge_count = 0
    for spec in _edge_specs(scale):
        table_name = _edge_table_name(spec.type_index)
        column_names = ["edge_id", "from_node_id", "to_node_id", "note"]
        column_names.extend(
            f"text_{property_index:02d}"
            for property_index in range(1, scale.edge_text_property_count + 1)
        )
        column_names.extend(
            f"num_{property_index:02d}"
            for property_index in range(1, scale.edge_numeric_property_count + 1)
        )
        column_names.extend(
            f"flag_{property_index:02d}"
            for property_index in range(1, scale.edge_boolean_property_count + 1)
        )
        placeholders = ", ".join("?" for _ in column_names)
        rows: list[tuple[object, ...]] = []
        for source_local_index in range(1, scale.nodes_per_type + 1):
            from_node_id = _node_id(scale, spec.source_type_index, source_local_index)
            for edge_ordinal in range(1, scale.edges_per_source + 1):
                target_local_index = (
                    (
                        source_local_index
                        - 1
                        + spec.type_index
                        + edge_ordinal
                    )
                    % scale.nodes_per_type
                ) + 1
                edge_count += 1
                row: list[object] = [
                    edge_count,
                    from_node_id,
                    _node_id(scale, spec.target_type_index, target_local_index),
                    (
                        f"edge-type-{spec.type_index:02d}-hop-"
                        f"{edge_ordinal:02d}-{source_local_index:06d}"
                    ),
                ]
                for property_index in range(1, scale.edge_text_property_count + 1):
                    row.append(
                        (
                            f"edge-{spec.type_index:02d}-text-"
                            f"{property_index:02d}-{source_local_index:06d}"
                        )
                    )
                for property_index in range(1, scale.edge_numeric_property_count + 1):
                    row.append(
                        round(
                            property_index
                            + (
                                (
                                    spec.type_index * 11
                                    + source_local_index
                                    * (property_index + 3)
                                    + edge_ordinal
                                )
                                % 5_000
                            )
                            / 100.0,
                            2,
                        )
                    )
                for property_index in range(1, scale.edge_boolean_property_count + 1):
                    row.append(
                        (
                            1
                            if (
                                spec.type_index
                                + source_local_index
                                + edge_ordinal
                                + property_index
                            )
                            % 2
                            == 0
                            else 0
                        )
                    )
                rows.append(tuple(row))
                if len(rows) >= batch_size:
                    conn.executemany(
                        (
                            f"INSERT INTO {table_name}({', '.join(column_names)}) "
                            f"VALUES ({placeholders})"
                        ),
                        rows,
                    )
                    rows.clear()
        if rows:
            conn.executemany(
                (
                    f"INSERT INTO {table_name}({', '.join(column_names)}) "
                    f"VALUES ({placeholders})"
                ),
                rows,
            )

    conn.commit()
    return {"node_count": node_count, "edge_count": edge_count}


def _build_queries(scale: SchemaShapeScale) -> list[SchemaQuery]:
    edge_specs = _edge_specs(scale)
    seed_type_index = 1
    seed_name = _node_name(seed_type_index, 1)
    first_edge_spec = edge_specs[0]
    final_hop_spec = edge_specs[scale.multi_hop_length - 1]

    one_hop_target_type_name = _node_type_name(first_edge_spec.target_type_index)
    multi_hop_final_type_name = _node_type_name(final_hop_spec.target_type_index)

    json_multi_hop_parts = [
        "SELECT DISTINCT json_extract(n_end.properties, '$.name') AS terminal_name",
        "FROM nodes n0",
    ]
    typed_multi_hop_parts = [
        "SELECT DISTINCT terminal_name.value AS terminal_name",
        "FROM nodes n0",
        (
            "JOIN node_props_text start_name ON start_name.node_id = n0.node_id "
            "AND start_name.property_key = 'name'"
        ),
    ]
    typeaware_multi_hop_parts = [
        f"SELECT DISTINCT n{scale.multi_hop_length}.name AS terminal_name",
        f"FROM {_node_table_name(seed_type_index)} n0",
    ]

    previous_json_alias = "n0"
    previous_typed_alias = "n0"
    previous_typeaware_alias = "n0"
    for hop_index, spec in enumerate(edge_specs[: scale.multi_hop_length], start=1):
        current_json_alias = f"n{hop_index}"
        current_edge_alias = f"e{hop_index}"
        json_multi_hop_parts.append(
            (
                f"JOIN edges {current_edge_alias} ON "
                f"{current_edge_alias}.from_node_id = {previous_json_alias}.node_id "
                f"AND {current_edge_alias}.edge_type = '{spec.name}'"
            )
        )
        json_multi_hop_parts.append(
            (
                f"JOIN nodes {current_json_alias} ON "
                f"{current_json_alias}.node_id = {current_edge_alias}.to_node_id "
                f"AND {current_json_alias}.node_type = "
                f"'{_node_type_name(spec.target_type_index)}'"
            )
        )
        previous_json_alias = current_json_alias

        current_typed_edge_alias = f"e{hop_index}"
        current_typed_node_alias = f"n{hop_index}"
        typed_multi_hop_parts.append(
            (
                f"JOIN edges {current_typed_edge_alias} ON "
                f"{current_typed_edge_alias}.from_node_id = "
                f"{previous_typed_alias}.node_id AND "
                f"{current_typed_edge_alias}.edge_type = '{spec.name}'"
            )
        )
        typed_multi_hop_parts.append(
            (
                f"JOIN nodes {current_typed_node_alias} ON "
                f"{current_typed_node_alias}.node_id = "
                f"{current_typed_edge_alias}.to_node_id AND "
                f"{current_typed_node_alias}.node_type = "
                f"'{_node_type_name(spec.target_type_index)}'"
            )
        )
        previous_typed_alias = current_typed_node_alias

        current_typeaware_edge_alias = f"e{hop_index}"
        current_typeaware_node_alias = f"n{hop_index}"
        typeaware_multi_hop_parts.append(
            (
                f"JOIN {_edge_table_name(spec.type_index)} "
                f"{current_typeaware_edge_alias} ON "
                f"{current_typeaware_edge_alias}.from_node_id = "
                f"{previous_typeaware_alias}.node_id"
            )
        )
        typeaware_multi_hop_parts.append(
            (
                f"JOIN {_node_table_name(spec.target_type_index)} "
                f"{current_typeaware_node_alias} ON "
                f"{current_typeaware_node_alias}.node_id = "
                f"{current_typeaware_edge_alias}.to_node_id"
            )
        )
        previous_typeaware_alias = current_typeaware_node_alias

    typed_multi_hop_parts.append(
        (
            "JOIN node_props_text terminal_name ON terminal_name.node_id = "
            f"{previous_typed_alias}.node_id AND terminal_name.property_key = 'name'"
        )
    )
    json_multi_hop_parts.append(
        (
            f"JOIN nodes n_end ON n_end.node_id = {previous_json_alias}.node_id "
            f"AND n_end.node_type = '{multi_hop_final_type_name}'"
        )
    )
    json_multi_hop_parts.append(
        (
            f"WHERE n0.node_type = '{_node_type_name(seed_type_index)}' "
            f"AND json_extract(n0.properties, '$.name') = '{seed_name}'"
        )
    )
    typed_multi_hop_parts.append(
        (
            f"WHERE n0.node_type = '{_node_type_name(seed_type_index)}' "
            f"AND start_name.value = '{seed_name}'"
        )
    )
    typeaware_multi_hop_parts.append(f"WHERE n0.name = '{seed_name}'")
    json_multi_hop_parts.append("ORDER BY terminal_name LIMIT 20")
    typed_multi_hop_parts.append("ORDER BY terminal_name LIMIT 20")
    typeaware_multi_hop_parts.append("ORDER BY terminal_name LIMIT 20")

    queries = [
        SchemaQuery(
            name="point_lookup",
            category="point-read",
            sql_by_schema={
                "json": (
                    "SELECT json_extract(properties, '$.name') AS name "
                    "FROM nodes "
                    f"WHERE node_type = '{_node_type_name(seed_type_index)}' "
                    f"AND json_extract(properties, '$.name') = '{seed_name}'"
                ),
                "typed": (
                    "SELECT name_prop.value AS name "
                    "FROM nodes n "
                    "JOIN node_props_text name_prop ON "
                    "name_prop.node_id = n.node_id AND "
                    "name_prop.property_key = 'name' "
                    f"WHERE n.node_type = '{_node_type_name(seed_type_index)}' "
                    f"AND name_prop.value = '{seed_name}'"
                ),
                "typeaware": (
                    f"SELECT name FROM {_node_table_name(seed_type_index)} "
                    f"WHERE name = '{seed_name}'"
                ),
            },
        ),
        SchemaQuery(
            name="top_active_score",
            category="ordered-top-k",
            sql_by_schema={
                "json": (
                    "SELECT json_extract(properties, '$.name') AS name, "
                    "CAST(json_extract(properties, '$.num_01') AS REAL) AS metric "
                    "FROM nodes "
                    f"WHERE node_type = '{_node_type_name(seed_type_index)}' "
                    "AND json_extract(properties, '$.flag_01') = 1 "
                    "ORDER BY metric DESC LIMIT 10"
                ),
                "typed": (
                    "SELECT name_prop.value AS name, num_prop.value AS metric "
                    "FROM nodes n "
                    "JOIN node_props_bool flag_prop ON "
                    "flag_prop.node_id = n.node_id AND "
                    "flag_prop.property_key = 'flag_01' "
                    "JOIN node_props_num num_prop ON "
                    "num_prop.node_id = n.node_id AND "
                    "num_prop.property_key = 'num_01' "
                    "JOIN node_props_text name_prop ON "
                    "name_prop.node_id = n.node_id AND "
                    "name_prop.property_key = 'name' "
                    f"WHERE n.node_type = '{_node_type_name(seed_type_index)}' "
                    "AND flag_prop.value = 1 "
                    "ORDER BY metric DESC LIMIT 10"
                ),
                "typeaware": (
                    "SELECT name, num_01 AS metric "
                    f"FROM {_node_table_name(seed_type_index)} "
                    "WHERE flag_01 = 1 ORDER BY metric DESC LIMIT 10"
                ),
            },
        ),
        SchemaQuery(
            name="one_hop_neighbors",
            category="adjacency-read",
            sql_by_schema={
                "json": (
                    "SELECT json_extract(target.properties, '$.name') AS neighbor_name "
                    "FROM nodes source "
                    "JOIN edges e ON e.from_node_id = source.node_id "
                    f"AND e.edge_type = '{first_edge_spec.name}' "
                    "JOIN nodes target ON target.node_id = e.to_node_id "
                    f"WHERE source.node_type = '{_node_type_name(seed_type_index)}' "
                    f"AND target.node_type = '{one_hop_target_type_name}' "
                    f"AND json_extract(source.properties, '$.name') = '{seed_name}' "
                    "ORDER BY neighbor_name LIMIT 10"
                ),
                "typed": (
                    "SELECT target_name.value AS neighbor_name "
                    "FROM nodes source "
                    "JOIN node_props_text source_name ON "
                    "source_name.node_id = source.node_id AND "
                    "source_name.property_key = 'name' "
                    "JOIN edges e ON e.from_node_id = source.node_id "
                    f"AND e.edge_type = '{first_edge_spec.name}' "
                    "JOIN nodes target ON target.node_id = e.to_node_id "
                    "JOIN node_props_text target_name ON "
                    "target_name.node_id = target.node_id AND "
                    "target_name.property_key = 'name' "
                    f"WHERE source.node_type = '{_node_type_name(seed_type_index)}' "
                    f"AND target.node_type = '{one_hop_target_type_name}' "
                    f"AND source_name.value = '{seed_name}' "
                    "ORDER BY neighbor_name LIMIT 10"
                ),
                "typeaware": (
                    "SELECT target.name AS neighbor_name "
                    f"FROM {_node_table_name(seed_type_index)} source "
                    f"JOIN {_edge_table_name(first_edge_spec.type_index)} e "
                    "ON e.from_node_id = source.node_id "
                    f"JOIN {_node_table_name(first_edge_spec.target_type_index)} "
                    "target "
                    "ON target.node_id = e.to_node_id "
                    f"WHERE source.name = '{seed_name}' ORDER BY neighbor_name LIMIT 10"
                ),
            },
        ),
        SchemaQuery(
            name="multi_hop_chain",
            category="multi-hop-read",
            sql_by_schema={
                "json": " ".join(json_multi_hop_parts),
                "typed": " ".join(typed_multi_hop_parts),
                "typeaware": " ".join(typeaware_multi_hop_parts),
            },
        ),
        SchemaQuery(
            name="relationship_stats",
            category="relationship-aggregate",
            sql_by_schema={
                "json": (
                    "SELECT json_extract(source.properties, '$.name') AS source_name, "
                    "count(*) AS total "
                    "FROM edges e "
                    "JOIN nodes source ON source.node_id = e.from_node_id "
                    f"WHERE e.edge_type = '{first_edge_spec.name}' "
                    "GROUP BY e.from_node_id ORDER BY total DESC, source_name ASC"
                ),
                "typed": (
                    "SELECT source_name.value AS source_name, count(*) AS total "
                    "FROM edges e "
                    "JOIN node_props_text source_name ON "
                    "source_name.node_id = e.from_node_id AND "
                    "source_name.property_key = 'name' "
                    f"WHERE e.edge_type = '{first_edge_spec.name}' "
                    "GROUP BY e.from_node_id ORDER BY total DESC, source_name ASC"
                ),
                "typeaware": (
                    f"SELECT source.name AS source_name, count(*) AS total "
                    f"FROM {_edge_table_name(first_edge_spec.type_index)} e "
                    f"JOIN {_node_table_name(first_edge_spec.source_type_index)} "
                    "source "
                    "ON source.node_id = e.from_node_id "
                    "GROUP BY e.from_node_id ORDER BY total DESC, source_name ASC"
                ),
            },
        ),
        SchemaQuery(
            name="relationship_projection",
            category="relationship-projection",
            sql_by_schema={
                "json": (
                    "SELECT lower(json_extract(properties, '$.note')) AS lowered_note, "
                    "length(json_extract(properties, '$.note')) AS note_len, "
                    "abs(CAST(json_extract(properties, '$.num_01') AS REAL)) "
                    "AS metric_abs, "
                    "json_extract(properties, '$.flag_01') AS edge_flag "
                    "FROM edges "
                    f"WHERE edge_type = '{first_edge_spec.name}' "
                    "ORDER BY lowered_note, note_len, metric_abs, edge_flag"
                ),
                "typed": (
                    "SELECT lower(note_prop.value) AS lowered_note, "
                    "length(note_prop.value) AS note_len, "
                    "abs(metric_prop.value) AS metric_abs, "
                    "flag_prop.value AS edge_flag "
                    "FROM edges e "
                    "JOIN edge_props_text note_prop ON "
                    "note_prop.edge_id = e.edge_id AND "
                    "note_prop.property_key = 'note' "
                    "JOIN edge_props_num metric_prop ON "
                    "metric_prop.edge_id = e.edge_id AND "
                    "metric_prop.property_key = 'num_01' "
                    "JOIN edge_props_bool flag_prop ON "
                    "flag_prop.edge_id = e.edge_id AND "
                    "flag_prop.property_key = 'flag_01' "
                    f"WHERE e.edge_type = '{first_edge_spec.name}' "
                    "ORDER BY lowered_note, note_len, metric_abs, edge_flag"
                ),
                "typeaware": (
                    "SELECT lower(note) AS lowered_note, "
                    "length(note) AS note_len, "
                    "abs(num_01) AS metric_abs, "
                    "flag_01 AS edge_flag "
                    f"FROM {_edge_table_name(first_edge_spec.type_index)} "
                    "ORDER BY lowered_note, note_len, metric_abs, edge_flag"
                ),
            },
        ),
    ]
    return queries


def _database_size_mb(conn: sqlite3.Connection) -> float:
    page_count = conn.execute("PRAGMA page_count").fetchone()[0]
    page_size = conn.execute("PRAGMA page_size").fetchone()[0]
    return (page_count * page_size) / (1024 * 1024)


def _run_schema_suite(
    schema_name: str,
    *,
    queries: list[SchemaQuery],
    scale: SchemaShapeScale,
    iterations: int,
    warmup: int,
    batch_size: int,
) -> dict[str, object]:
    _progress(
        f"schema benchmark: {schema_name} start "
        f"({len(queries)} queries, iterations={iterations}, warmup={warmup})"
    )
    with tempfile.TemporaryDirectory(
        prefix=f"cypherglot-schema-{schema_name}-"
    ) as tmpdir_str:
        db_path = Path(tmpdir_str) / f"{schema_name}.sqlite3"
        _progress(f"schema benchmark: {schema_name} connect")
        conn, connect_ms = _measure_ms(lambda: _create_sqlite_connection(db_path))
        rss_by_step_mib = {"connect": _rss_mib()}
        if schema_name == "json":
            _progress(f"schema benchmark: {schema_name} schema")
            _, schema_ms = _measure_ms(lambda: _create_json_schema(conn))
            rss_by_step_mib["schema"] = _rss_mib()
            _progress(f"schema benchmark: {schema_name} ingest")
            row_counts, ingest_ms = _measure_ms(
                lambda: _seed_json_schema(conn, scale=scale, batch_size=batch_size)
            )
            rss_by_step_mib["ingest"] = _rss_mib()
            _progress(f"schema benchmark: {schema_name} index")
            _, index_ms = _measure_ms(lambda: _create_json_indexes(conn))
        elif schema_name == "typed":
            _progress(f"schema benchmark: {schema_name} schema")
            _, schema_ms = _measure_ms(lambda: _create_typed_property_schema(conn))
            rss_by_step_mib["schema"] = _rss_mib()
            _progress(f"schema benchmark: {schema_name} ingest")
            row_counts, ingest_ms = _measure_ms(
                lambda: _seed_typed_property_schema(
                    conn,
                    scale=scale,
                    batch_size=batch_size,
                )
            )
            rss_by_step_mib["ingest"] = _rss_mib()
            _progress(f"schema benchmark: {schema_name} index")
            _, index_ms = _measure_ms(lambda: _create_typed_property_indexes(conn))
        elif schema_name == "typeaware":
            _progress(f"schema benchmark: {schema_name} schema")
            _, schema_ms = _measure_ms(lambda: _create_type_aware_schema(conn, scale))
            rss_by_step_mib["schema"] = _rss_mib()
            _progress(f"schema benchmark: {schema_name} ingest")
            row_counts, ingest_ms = _measure_ms(
                lambda: _seed_type_aware_schema(
                    conn,
                    scale=scale,
                    batch_size=batch_size,
                )
            )
            rss_by_step_mib["ingest"] = _rss_mib()
            _progress(f"schema benchmark: {schema_name} index")
            _, index_ms = _measure_ms(
                lambda: _create_type_aware_indexes(conn, scale)
            )
        else:
            raise ValueError(f"Unsupported schema name: {schema_name}")

        rss_by_step_mib["index"] = _rss_mib()
        _progress(f"schema benchmark: {schema_name} analyze")
        _, analyze_ms = _measure_ms(lambda: conn.execute("ANALYZE"))
        rss_by_step_mib["analyze"] = _rss_mib()
        query_results = []
        for index, query in enumerate(queries, start=1):
            _progress(
                f"schema benchmark: {schema_name} query {index}/{len(queries)} "
                f"{query.name}"
            )
            query_results.append(
                {
                    "name": query.name,
                    "category": query.category,
                    "execute": _benchmark_query(
                        conn,
                        query.sql_by_schema[schema_name],
                        warmup=warmup,
                        iterations=iterations,
                    ),
                }
            )
        size_mb = _database_size_mb(conn)
        conn.close()

    _progress(f"schema benchmark: {schema_name} complete")

    pooled = _summarize(
        [
            int(result["execute"]["mean_ms"] * 1_000_000.0)
            for result in query_results
        ]
    )
    return {
        "setup": {
            "connect_ms": connect_ms,
            "schema_ms": schema_ms,
            "ingest_ms": ingest_ms,
            "index_ms": index_ms,
            "analyze_ms": analyze_ms,
            "database_size_mb": size_mb,
            "rss_mib": rss_by_step_mib,
        },
        "row_counts": row_counts,
        "pooled_execute": pooled,
        "queries": query_results,
    }


def _print_schema_summary(schema_name: str, suite: dict[str, object]) -> None:
    print(schema_name)
    print(
        "  setup: "
        f"connect={suite['setup']['connect_ms']:.2f} ms, "
        f"schema={suite['setup']['schema_ms']:.2f} ms, "
        f"ingest={suite['setup']['ingest_ms']:.2f} ms, "
        f"index={suite['setup']['index_ms']:.2f} ms, "
        f"analyze={suite['setup']['analyze_ms']:.2f} ms, "
        f"size={suite['setup']['database_size_mb']:.2f} MiB"
    )
    print(
        "  rss: "
        f"connect={suite['setup']['rss_mib']['connect']:.2f} MiB, "
        f"schema={suite['setup']['rss_mib']['schema']:.2f} MiB, "
        f"ingest={suite['setup']['rss_mib']['ingest']:.2f} MiB, "
        f"index={suite['setup']['rss_mib']['index']:.2f} MiB, "
        f"analyze={suite['setup']['rss_mib']['analyze']:.2f} MiB"
    )
    print(
        "  pooled execute: "
        f"mean={suite['pooled_execute']['mean_ms']:.2f} ms, "
        f"p50={suite['pooled_execute']['p50_ms']:.2f} ms, "
        f"p95={suite['pooled_execute']['p95_ms']:.2f} ms"
    )
    for query_result in suite["queries"]:
        print(
            "    - "
            f"{query_result['name']} [{query_result['category']}]: "
            f"mean={query_result['execute']['mean_ms']:.2f} ms, "
            f"p50={query_result['execute']['p50_ms']:.2f} ms, "
            f"p95={query_result['execute']['p95_ms']:.2f} ms, "
            f"p99={query_result['execute']['p99_ms']:.2f} ms"
        )


def _parse_args() -> argparse.Namespace:
    return parse_sqlite_schema_shapes_args(default_output_path=DEFAULT_OUTPUT_PATH)


def main() -> int:
    args = _parse_args()
    if args.iterations <= 0:
        raise ValueError("--iterations must be positive.")
    if args.warmup < 0:
        raise ValueError("--warmup must be zero or positive.")
    if args.batch_size <= 0:
        raise ValueError("--batch-size must be positive.")
    if args.node_type_count <= 0:
        raise ValueError("--node-type-count must be positive.")
    if args.edge_type_count <= 0:
        raise ValueError("--edge-type-count must be positive.")
    if args.nodes_per_type <= 0:
        raise ValueError("--nodes-per-type must be positive.")
    if args.edges_per_source <= 0:
        raise ValueError("--edges-per-source must be positive.")
    if args.multi_hop_length <= 0:
        raise ValueError("--multi-hop-length must be positive.")
    if args.multi_hop_length > args.edge_type_count:
        raise ValueError("--multi-hop-length cannot exceed --edge-type-count.")
    if args.node_numeric_property_count <= 0:
        raise ValueError("--node-numeric-property-count must be positive.")
    if args.node_boolean_property_count <= 0:
        raise ValueError("--node-boolean-property-count must be positive.")
    if args.edge_numeric_property_count <= 0:
        raise ValueError("--edge-numeric-property-count must be positive.")
    if args.edge_boolean_property_count <= 0:
        raise ValueError("--edge-boolean-property-count must be positive.")
    if args.node_text_property_count < 0:
        raise ValueError("--node-text-property-count must be zero or positive.")
    if args.edge_text_property_count < 0:
        raise ValueError("--edge-text-property-count must be zero or positive.")

    scale = SchemaShapeScale(
        node_type_count=args.node_type_count,
        edge_type_count=args.edge_type_count,
        nodes_per_type=args.nodes_per_type,
        edges_per_source=args.edges_per_source,
        multi_hop_length=args.multi_hop_length,
        node_numeric_property_count=args.node_numeric_property_count,
        node_text_property_count=args.node_text_property_count,
        node_boolean_property_count=args.node_boolean_property_count,
        edge_numeric_property_count=args.edge_numeric_property_count,
        edge_text_property_count=args.edge_text_property_count,
        edge_boolean_property_count=args.edge_boolean_property_count,
    )
    schemas = args.schema or ["json", "typed", "typeaware"]
    queries = _build_queries(scale)

    results = {
        "benchmark_entrypoint": "scripts.benchmarks.schema.sqlite_shapes",
        "generated_at": datetime.now(UTC).isoformat(),
        "run_status": "completed",
        "environment": {
            "python": platform.python_version(),
            "sqlite": sqlite3.sqlite_version,
            "platform": platform.platform(),
        },
        "controls": {
            "iterations": args.iterations,
            "warmup": args.warmup,
            "batch_size": args.batch_size,
            "schemas": schemas,
        },
        "scale": asdict(scale),
        "edge_specs": [asdict(spec) for spec in _edge_specs(scale)],
        "schemas": {
            schema_name: _run_schema_suite(
                schema_name,
                queries=queries,
                scale=scale,
                iterations=args.iterations,
                warmup=args.warmup,
                batch_size=args.batch_size,
            )
            for schema_name in schemas
        },
    }

    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(results, indent=2) + "\n", encoding="utf-8")

    print(f"Wrote schema-shape benchmark baseline to {args.output}")
    for schema_name in schemas:
        _print_schema_summary(schema_name, results["schemas"][schema_name])
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
