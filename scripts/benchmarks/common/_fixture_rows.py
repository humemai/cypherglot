"""Read converted fixture CSVs into typed Cypher-property row batches.

Used by the native-Cypher benchmark engines (Neo4j, Apache AGE) to seed a graph
from a :class:`GeneratedGraphFixture` when the topology is not the in-process
synthetic generator (e.g. LDBC SNB). The synthetic topology keeps generating its
rows in-process, byte-for-byte unchanged; these helpers only feed the
fixture-CSV seeding path.

Fixture CSV layout (see ``runtime_shared._graph_fixture_table_columns``):

* node tables start with ``id`` then one column per property;
* edge tables start with ``id,from_id,to_id`` then one column per property.

Values are coerced to Python types from each :class:`cypherglot.PropertyField`'s
``logical_type`` so they land in the graph with the right type (booleans are
stored as ``0``/``1`` in the CSV and become real ``bool`` values).
"""

from __future__ import annotations

import csv
from typing import Any, Iterator

import cypherglot

from scripts.benchmarks.common.runtime_shared import GeneratedGraphFixture


def _coerce(value: str, logical_type: str) -> Any:
    if logical_type == "integer":
        return int(value)
    if logical_type == "float":
        return float(value)
    if logical_type == "boolean":
        return bool(int(value))
    return value


def iter_node_property_rows(
    node_type: cypherglot.NodeTypeSpec,
    fixture: GeneratedGraphFixture,
) -> Iterator[dict[str, Any]]:
    """Yield each node as a property dict (``id`` plus typed properties)."""
    logical_type = {prop.name: prop.logical_type for prop in node_type.properties}
    csv_path = fixture.table_csv_paths[node_type.table_name]
    with csv_path.open("r", encoding="utf-8", newline="") as handle:
        for row in csv.DictReader(handle):
            properties: dict[str, Any] = {"id": int(row["id"])}
            for column, raw in row.items():
                if column == "id":
                    continue
                properties[column] = _coerce(raw, logical_type[column])
            yield properties


def iter_edge_rows(
    edge_type: cypherglot.EdgeTypeSpec,
    fixture: GeneratedGraphFixture,
) -> Iterator[dict[str, Any]]:
    """Yield each edge as ``{"from_id", "to_id", "props"}``.

    ``props`` excludes ``id``/``from_id``/``to_id`` to match the synthetic
    ``_edge_properties`` shape the engines already consume.
    """
    logical_type = {prop.name: prop.logical_type for prop in edge_type.properties}
    csv_path = fixture.table_csv_paths[edge_type.table_name]
    with csv_path.open("r", encoding="utf-8", newline="") as handle:
        for row in csv.DictReader(handle):
            props: dict[str, Any] = {}
            for column, raw in row.items():
                if column in ("id", "from_id", "to_id"):
                    continue
                props[column] = _coerce(raw, logical_type[column])
            yield {
                "from_id": int(row["from_id"]),
                "to_id": int(row["to_id"]),
                "props": props,
            }
