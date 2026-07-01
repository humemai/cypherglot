"""LDBC SNB topology: load a real LDBC SNB Datagen graph for the benchmark.

This maps the LDBC Social Network Benchmark graph onto the corpus's abstract
slot contract so the *identical* workload runs against a real, skewed social
graph instead of the parametric synthetic generator:

* ``node_type_1 = Person`` -- the skewed ``KNOWS`` social graph drives the
  variable-length traversal queries;
* ``node_type_2 = Message = Post union Comment`` -- LDBC's own message
  abstraction; the message-dominated bulk of the graph;
* ``node_type_3 = Tag`` -- the static topic vocabulary.

Edges:

* ``edge_type_1 = KNOWS`` (``Person -> Person``, materialised in both directions
  so the undirected friendship graph is traversable);
* ``edge_type_2 = CREATED`` (``Person -> Message``, from each message's
  ``CreatorPersonId``);
* ``edge_type_3 = HAS_TAG`` (``Message -> Tag``, the union of ``Post_hasTag_Tag``
  and ``Comment_hasTag_Tag``).

**What is real and what is derived.** The graph *structure* (which nodes exist,
who connects to whom, the real degree distribution and cardinalities) is taken
verbatim from LDBC SNB Datagen output. Only the analytic *payload* columns
(``name``, ``age``, ``score``, ``active``, the ``text_*``/``num_*``/``flag_*``
extras, and the edge attributes) are derived deterministically -- so the schema
width matches the synthetic topology exactly and the workload is byte-identical,
isolating graph structure as the single experimental factor. This is declared in
the paper's threats-to-validity.

SNB's 64-bit entity ids are remapped to compact per-type ordinals (1..N), which
keeps them within 32-bit integer columns (PostgreSQL ``INTEGER``), and node
``name`` values are ``"<type>-<ordinal:06d>"`` with Person ordinals assigned in
descending ``KNOWS``-degree order, so ``person-000001`` is the densest hub and
the point-lookup / traversal queries exercise the skewed core of the real graph.

Input layout (LDBC SNB Datagen ``--mode bi --format csv``, composite-merged-fk):
``.../initial_snapshot/dynamic/<Entity>/part-*.csv`` and
``.../initial_snapshot/static/<Entity>/part-*.csv``, pipe-delimited with headers.
"""

from __future__ import annotations

import csv
import json
import zlib
from pathlib import Path
from typing import Any, Iterator

import cypherglot

from scripts.benchmarks.common.runtime_shared import (
    GeneratedGraphFixture,
    ManagedDirectory,
    _capture_rss_snapshot,
    _graph_fixture_table_columns,
    _write_graph_fixture_csv,
)
from scripts.benchmarks.common.shared import (
    EdgeTypePlan,
    RuntimeScale,
    _build_graph_schema,
    _measure_ns,
    _progress,
)
from scripts.benchmarks.common.topology import Topology


SNB_DELIMITER = "|"
# Version tag for the converted-fixture cache; bump when the derivation changes
# so stale caches are ignored.
_CONVERSION_VERSION = "v1"
_CACHE_DIRNAME = f"_cypherglot_fixture_{_CONVERSION_VERSION}"

_NODE_TYPE_NAMES = ("Person", "Message", "Tag")
_EDGE_SPECS = (
    ("KNOWS", "Person", "Person"),
    ("CREATED", "Person", "Message"),
    ("HAS_TAG", "Message", "Tag"),
)


# ---------------------------------------------------------------------------
# Schema, edge plan, and token map (pure -- no data access)
# ---------------------------------------------------------------------------
def _snb_edge_plans() -> list[EdgeTypePlan]:
    type_index_of = {name: index for index, name in enumerate(_NODE_TYPE_NAMES, 1)}
    return [
        EdgeTypePlan(
            type_index=edge_index,
            name=name,
            source_type_index=type_index_of[source],
            target_type_index=type_index_of[target],
        )
        for edge_index, (name, source, target) in enumerate(_EDGE_SPECS, 1)
    ]


def snb_graph_schema(
    scale: RuntimeScale,
) -> tuple[cypherglot.GraphSchema, list[EdgeTypePlan]]:
    """SNB schema carrying the *same* property set as the synthetic topology."""
    synthetic_schema, _ = _build_graph_schema(scale)
    node_properties = synthetic_schema.node_types[0].properties
    edge_properties = synthetic_schema.edge_types[0].properties

    node_types = tuple(
        cypherglot.NodeTypeSpec(name=name, properties=node_properties)
        for name in _NODE_TYPE_NAMES
    )
    edge_types = tuple(
        cypherglot.EdgeTypeSpec(
            name=name,
            source_type=source,
            target_type=target,
            properties=edge_properties,
        )
        for name, source, target in _EDGE_SPECS
    )
    schema = cypherglot.GraphSchema(node_types=node_types, edge_types=edge_types)
    return schema, _snb_edge_plans()


def _snb_node_name(type_name: str, ordinal: int) -> str:
    return f"{type_name.lower()}-{ordinal:06d}"


def snb_token_map(scale: RuntimeScale) -> dict[str, str]:
    """Bind the corpus placeholders to SNB type and sample names.

    Sample names (``person-000001`` ...) are guaranteed to exist because the
    loader labels the first ``N`` nodes of each type with exactly these names,
    so this needs no data access.
    """
    token_map: dict[str, str] = {
        "variable_hop_max": str(scale.variable_hop_max),
        "grouped_rollup_variable_hop_max": str(min(scale.variable_hop_max, 3)),
        "created_type_1_name": "person-created-node",
        "created_type_1_peer_name": "person-created-peer",
        "created_type_2_name": "message-created-node",
    }
    for type_index, type_name in enumerate(_NODE_TYPE_NAMES, start=1):
        token_map[f"node_type_{type_index}"] = type_name
        for local_index in range(1, 5):
            token_map[f"node_type_{type_index}_name_{local_index}"] = _snb_node_name(
                type_name, local_index
            )
    for edge_index, (name, _source, _target) in enumerate(_EDGE_SPECS, start=1):
        token_map[f"edge_type_{edge_index}"] = name
    return token_map


# ---------------------------------------------------------------------------
# Deterministic payload derivation (column-name driven, width-agnostic)
# ---------------------------------------------------------------------------
def _column_seed(column: str, entity_seed: int) -> int:
    return (zlib.crc32(column.encode("utf-8")) ^ (entity_seed * 2654435761)) & 0xFFFFFFFF


def _derive_node_value(column: str, ordinal: int, entity_seed: int) -> Any:
    if column == "id":
        return ordinal
    if column == "name":
        return None  # supplied by the caller (type-prefixed)
    seed = _column_seed(column, entity_seed)
    if column == "age":
        return 18 + (seed % 47)
    if column == "score" or column.startswith("num_"):
        return round((seed % 10_000) / 100.0, 2)
    if column == "active" or column.startswith("flag_"):
        return int(seed % 2 == 0)
    if column.startswith("text_"):
        return f"{column}-{ordinal:06d}"
    raise ValueError(f"Unhandled SNB node column {column!r}.")


def _derive_edge_value(
    column: str,
    *,
    edge_id: int,
    from_ordinal: int,
    to_ordinal: int,
    edge_name: str,
) -> Any:
    if column == "id":
        return edge_id
    if column == "from_id":
        return from_ordinal
    if column == "to_id":
        return to_ordinal
    entity_seed = (from_ordinal * 2654435761) ^ (to_ordinal * 40503)
    seed = _column_seed(column, entity_seed)
    if column == "note" or column.startswith("text_"):
        return f"{edge_name.lower()}-{column}-{from_ordinal:06d}-{to_ordinal:06d}"
    if column == "rank":
        return 1 + (seed % 100)
    if column in ("weight", "score") or column.startswith("num_"):
        return round((seed % 10_000) / 100.0, 2)
    if column == "active" or column.startswith("flag_"):
        return int(seed % 2 == 0)
    raise ValueError(f"Unhandled SNB edge column {column!r}.")


def _node_row(columns: list[str], type_name: str, ordinal: int) -> tuple[Any, ...]:
    entity_seed = ordinal ^ (zlib.crc32(type_name.encode("utf-8")) << 1)
    values: list[Any] = []
    for column in columns:
        if column == "name":
            values.append(_snb_node_name(type_name, ordinal))
        else:
            values.append(_derive_node_value(column, ordinal, entity_seed))
    return tuple(values)


# ---------------------------------------------------------------------------
# SNB CSV reading
# ---------------------------------------------------------------------------
def _find_initial_snapshot(data_dir: Path) -> Path:
    """Locate the ``initial_snapshot`` directory under an SNB output dir."""
    candidate = data_dir
    if candidate.name == "initial_snapshot":
        return candidate
    matches = sorted(data_dir.rglob("initial_snapshot"))
    for match in matches:
        if (match / "dynamic").is_dir():
            return match
    raise FileNotFoundError(
        f"Could not find an 'initial_snapshot/dynamic' directory under {data_dir}. "
        "Point --ldbc-snb-data-dir at LDBC SNB Datagen '--mode bi --format csv' "
        "output (the directory containing graphs/csv/bi/composite-merged-fk)."
    )


def _entity_dir(snapshot: Path, group: str, entity: str) -> Path:
    return snapshot / group / entity


def _iter_entity_rows(entity_dir: Path) -> Iterator[dict[str, str]]:
    """Yield each row of every ``part-*.csv`` under an entity dir as a dict."""
    part_files = sorted(entity_dir.glob("*.csv"))
    if not part_files:
        raise FileNotFoundError(f"No CSV part files under {entity_dir}.")
    for part_file in part_files:
        with part_file.open("r", encoding="utf-8", newline="") as handle:
            reader = csv.DictReader(handle, delimiter=SNB_DELIMITER)
            for row in reader:
                yield row


def _entity_exists(snapshot: Path, group: str, entity: str) -> bool:
    entity_dir = _entity_dir(snapshot, group, entity)
    return entity_dir.is_dir() and any(entity_dir.glob("*.csv"))


# ---------------------------------------------------------------------------
# Conversion: SNB output -> per-type fixture CSVs (cached on disk)
# ---------------------------------------------------------------------------
def _convert_snb_to_fixture(
    *,
    snapshot: Path,
    graph_schema: cypherglot.GraphSchema,
    out_dir: Path,
    progress_label: str,
) -> dict[str, int]:
    """Read SNB CSVs, remap ids, derive payloads, write per-type fixture CSVs.

    Returns the row counts. Output CSVs are written into ``out_dir`` keyed by the
    cypherglot table name for each node/edge type.
    """
    table_columns = _graph_fixture_table_columns(graph_schema)
    table_name = {
        node_type.name: node_type.table_name for node_type in graph_schema.node_types
    }
    edge_table_name = {
        edge_type.name: edge_type.table_name for edge_type in graph_schema.edge_types
    }
    out_dir.mkdir(parents=True, exist_ok=True)

    # --- Pass 1: Person ids + KNOWS degree for hub-first ordinal assignment ---
    _progress(f"{progress_label}: scanning Person + KNOWS degree")
    person_ids: list[int] = []
    for row in _iter_entity_rows(_entity_dir(snapshot, "dynamic", "Person")):
        person_ids.append(int(row["id"]))
    degree: dict[int, int] = {pid: 0 for pid in person_ids}
    knows_pairs: list[tuple[int, int]] = []
    for row in _iter_entity_rows(_entity_dir(snapshot, "dynamic", "Person_knows_Person")):
        p1 = int(row["Person1Id"])
        p2 = int(row["Person2Id"])
        if p1 in degree and p2 in degree:
            knows_pairs.append((p1, p2))
            degree[p1] += 1
            degree[p2] += 1
    # Ordinal 1 = densest hub; ties broken by id for determinism.
    person_order = sorted(person_ids, key=lambda pid: (-degree[pid], pid))
    person_ordinal = {pid: index for index, pid in enumerate(person_order, start=1)}

    # --- Messages (Post + Comment), with creator, assign ordinals by id ---
    _progress(f"{progress_label}: scanning Messages (Post + Comment)")
    message_creator: dict[int, int] = {}
    for row in _iter_entity_rows(_entity_dir(snapshot, "dynamic", "Post")):
        message_creator[int(row["id"])] = int(row["CreatorPersonId"])
    for row in _iter_entity_rows(_entity_dir(snapshot, "dynamic", "Comment")):
        message_creator[int(row["id"])] = int(row["CreatorPersonId"])
    message_order = sorted(message_creator)
    message_ordinal = {mid: index for index, mid in enumerate(message_order, start=1)}

    # --- Tags (static), assign ordinals by id ---
    _progress(f"{progress_label}: scanning Tags")
    tag_ids = sorted(int(row["id"]) for row in _iter_entity_rows(
        _entity_dir(snapshot, "static", "Tag")
    ))
    tag_ordinal = {tid: index for index, tid in enumerate(tag_ids, start=1)}

    # --- Write node CSVs ---
    def write_node_csv(type_name: str, ordinals: int) -> None:
        name = table_name[type_name]
        columns = table_columns[name]
        rows = [_node_row(columns, type_name, ordinal) for ordinal in range(1, ordinals + 1)]
        _write_graph_fixture_csv(out_dir / f"{name}.csv", column_names=columns, rows=rows)

    write_node_csv("Person", len(person_ordinal))
    write_node_csv("Message", len(message_ordinal))
    write_node_csv("Tag", len(tag_ordinal))

    # --- Write edge CSVs ---
    edge_id = 1

    def open_edge_writer(edge_name: str):
        name = edge_table_name[edge_name]
        handle = (out_dir / f"{name}.csv").open("w", encoding="utf-8", newline="")
        writer = csv.writer(handle)
        writer.writerow(table_columns[name])
        return handle, writer, table_columns[name]

    # KNOWS: both directions (undirected friendship -> bidirected traversal).
    _progress(f"{progress_label}: writing KNOWS ({len(knows_pairs)} pairs, bidirected)")
    handle, writer, columns = open_edge_writer("KNOWS")
    knows_count = 0
    for p1, p2 in knows_pairs:
        o1 = person_ordinal[p1]
        o2 = person_ordinal[p2]
        for from_ordinal, to_ordinal in ((o1, o2), (o2, o1)):
            writer.writerow(
                _derive_edge_value(
                    column,
                    edge_id=edge_id,
                    from_ordinal=from_ordinal,
                    to_ordinal=to_ordinal,
                    edge_name="KNOWS",
                )
                for column in columns
            )
            edge_id += 1
            knows_count += 1
    handle.close()

    # CREATED: Person -> Message (from each message's creator).
    _progress(f"{progress_label}: writing CREATED")
    handle, writer, columns = open_edge_writer("CREATED")
    created_count = 0
    for message_id, creator_id in message_creator.items():
        if creator_id not in person_ordinal:
            continue
        from_ordinal = person_ordinal[creator_id]
        to_ordinal = message_ordinal[message_id]
        writer.writerow(
            _derive_edge_value(
                column,
                edge_id=edge_id,
                from_ordinal=from_ordinal,
                to_ordinal=to_ordinal,
                edge_name="CREATED",
            )
            for column in columns
        )
        edge_id += 1
        created_count += 1
    handle.close()

    # HAS_TAG: Message -> Tag (Post_hasTag_Tag + Comment_hasTag_Tag).
    _progress(f"{progress_label}: writing HAS_TAG")
    handle, writer, columns = open_edge_writer("HAS_TAG")
    has_tag_count = 0
    for group_entity, message_key in (
        ("Post_hasTag_Tag", "PostId"),
        ("Comment_hasTag_Tag", "CommentId"),
    ):
        if not _entity_exists(snapshot, "dynamic", group_entity):
            continue
        for row in _iter_entity_rows(_entity_dir(snapshot, "dynamic", group_entity)):
            message_id = int(row[message_key])
            tag_id = int(row["TagId"])
            if message_id not in message_ordinal or tag_id not in tag_ordinal:
                continue
            writer.writerow(
                _derive_edge_value(
                    column,
                    edge_id=edge_id,
                    from_ordinal=message_ordinal[message_id],
                    to_ordinal=tag_ordinal[tag_id],
                    edge_name="HAS_TAG",
                )
                for column in columns
            )
            edge_id += 1
            has_tag_count += 1
    handle.close()

    node_count = len(person_ordinal) + len(message_ordinal) + len(tag_ordinal)
    edge_count = knows_count + created_count + has_tag_count
    row_counts = {
        "node_count": node_count,
        "edge_count": edge_count,
        "node_type_count": len(graph_schema.node_types),
        "edge_type_count": len(graph_schema.edge_types),
        "person_count": len(person_ordinal),
        "message_count": len(message_ordinal),
        "tag_count": len(tag_ordinal),
        "knows_count": knows_count,
        "created_count": created_count,
        "has_tag_count": has_tag_count,
    }
    (out_dir / "row_counts.json").write_text(
        json.dumps(row_counts, indent=2) + "\n", encoding="utf-8"
    )
    return row_counts


def _prepare_snb_fixture(
    *,
    data_dir: Path,
    scale: RuntimeScale,
    graph_schema: cypherglot.GraphSchema,
    index_mode: str,
    db_root_dir: Path | None = None,
) -> GeneratedGraphFixture:
    """Build a ``GeneratedGraphFixture`` from converted SNB CSVs (disk-cached).

    The conversion is expensive, so its output is cached under the SNB data dir
    and reused across index modes, repeats, and engines (the CSV content does not
    depend on any of those).
    """
    progress_label = f"ldbc-snb-fixture/{index_mode}"
    snapshot = _find_initial_snapshot(data_dir)
    cache_dir = data_dir / _CACHE_DIRNAME
    row_counts_path = cache_dir / "row_counts.json"

    table_columns = _graph_fixture_table_columns(graph_schema)
    rss_snapshots_mib: dict[str, dict[str, float | None]] = {}
    rss_snapshots_mib["before_generate"] = _capture_rss_snapshot(backend="fixture")

    if row_counts_path.exists():
        _progress(f"{progress_label}: reusing converted fixture cache at {cache_dir}")
        row_counts = json.loads(row_counts_path.read_text(encoding="utf-8"))
    else:
        _progress(f"{progress_label}: converting SNB output -> fixture CSVs")
        row_counts, _convert_ns = _measure_ns(
            lambda: _convert_snb_to_fixture(
                snapshot=snapshot,
                graph_schema=graph_schema,
                out_dir=cache_dir,
                progress_label=progress_label,
            )
        )
    rss_snapshots_mib["after_generate"] = _capture_rss_snapshot(backend="fixture")

    table_csv_paths = {
        name: cache_dir / f"{name}.csv" for name in table_columns
    }
    missing = [name for name, path in table_csv_paths.items() if not path.exists()]
    if missing:
        raise FileNotFoundError(
            f"Converted SNB fixture is missing tables {missing}; "
            f"delete the cache dir {cache_dir} and re-run."
        )

    # The cache is persistent, so the fixture's managed work dir must not delete
    # it; point work_dir + csv_dir at the cache with no temp-dir cleanup.
    work_dir = ManagedDirectory(path=cache_dir, temp_dir=None)
    manifest_path = cache_dir / f"manifest-{index_mode}.json"
    manifest_path.write_text(
        json.dumps(
            {
                "index_mode": index_mode,
                "topology": "ldbc_snb",
                "row_counts": row_counts,
                "table_columns": table_columns,
                "table_csv_paths": {
                    name: str(path) for name, path in table_csv_paths.items()
                },
            },
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )
    _progress(
        f"{progress_label}: fixture ready "
        f"({row_counts['node_count']} nodes, {row_counts['edge_count']} edges)"
    )
    return GeneratedGraphFixture(
        work_dir=work_dir,
        csv_dir=cache_dir,
        manifest_path=manifest_path,
        table_csv_paths=table_csv_paths,
        table_columns=table_columns,
        row_counts=row_counts,
        rss_snapshots_mib=rss_snapshots_mib,
        index_mode=index_mode,
    )


class LdbcSnbTopology(Topology):
    """Load a real LDBC SNB Datagen graph as the benchmark topology."""

    name = "ldbc_snb"

    def __init__(self, *, data_dir: Path) -> None:
        self.data_dir = data_dir

    def build_schema(
        self,
        scale: RuntimeScale,
    ) -> tuple[cypherglot.GraphSchema, list[EdgeTypePlan]]:
        return snb_graph_schema(scale)

    def token_map(
        self,
        scale: RuntimeScale,
        graph_schema: cypherglot.GraphSchema,
        edge_plans: list[EdgeTypePlan],
    ) -> dict[str, str]:
        return snb_token_map(scale)

    def prepare_fixture(
        self,
        *,
        scale: RuntimeScale,
        graph_schema: cypherglot.GraphSchema,
        edge_plans: list[EdgeTypePlan],
        index_mode: str,
        db_root_dir: Path | None = None,
    ) -> GeneratedGraphFixture:
        return _prepare_snb_fixture(
            data_dir=self.data_dir,
            scale=scale,
            graph_schema=graph_schema,
            index_mode=index_mode,
            db_root_dir=db_root_dir,
        )
