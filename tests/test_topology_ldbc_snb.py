# pyright: reportPrivateUsage=false
# pylint: disable=protected-access
"""Hermetic tests for the LDBC SNB benchmark topology.

These build a tiny synthetic LDBC-SNB-format directory on the fly (Datagen
``--mode bi --format csv`` composite-merged-fk layout), so they run without
Docker or a real generated dataset. The end-to-end run against a real SNB
dataset is exercised separately by the benchmark entrypoints."""

from __future__ import annotations

import csv
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from scripts.benchmarks.common.shared import RuntimeScale, _build_graph_schema
from scripts.benchmarks.common.topology import (
    SyntheticTopology,
    resolve_topology,
)
from scripts.benchmarks.common.topology_ldbc_snb import (
    LdbcSnbTopology,
    _convert_snb_to_fixture,
    _find_initial_snapshot,
    snb_graph_schema,
    snb_token_map,
)


# Real LDBC SNB Datagen headers (composite-merged-fk, pipe-delimited).
_HEADERS = {
    ("dynamic", "Person"): (
        "creationDate|id|firstName|lastName|gender|birthday|"
        "locationIP|browserUsed|LocationCityId|language|email"
    ),
    ("dynamic", "Person_knows_Person"): "creationDate|Person1Id|Person2Id",
    ("dynamic", "Post"): (
        "creationDate|id|imageFile|locationIP|browserUsed|language|content|"
        "length|CreatorPersonId|ContainerForumId|LocationCountryId"
    ),
    ("dynamic", "Comment"): (
        "creationDate|id|locationIP|browserUsed|content|length|"
        "CreatorPersonId|LocationCountryId|ParentPostId|ParentCommentId"
    ),
    ("dynamic", "Post_hasTag_Tag"): "creationDate|PostId|TagId",
    ("dynamic", "Comment_hasTag_Tag"): "creationDate|CommentId|TagId",
    ("static", "Tag"): "id|name|url|TypeTagClassId",
}


def _write_entity(snapshot: Path, group: str, entity: str, rows: list[str]) -> None:
    entity_dir = snapshot / group / entity
    entity_dir.mkdir(parents=True, exist_ok=True)
    header = _HEADERS[(group, entity)]
    (entity_dir / "part-00000.csv").write_text(
        header + "\n" + "\n".join(rows) + "\n", encoding="utf-8"
    )


def _build_tiny_snb(root: Path) -> Path:
    """Create a tiny SNB initial-snapshot dir; return the snapshot path.

    KNOWS degrees (bidirected): person 100 -> 3, 200 -> 2, 300 -> 2, 400 -> 1,
    so hub-first ordinal assignment must map 100 -> person-000001.
    """
    snapshot = root / "graphs/csv/bi/composite-merged-fk/initial_snapshot"
    d = "2012-01-01T00:00:00.000+00:00"
    _write_entity(
        snapshot, "dynamic", "Person",
        [
            f"{d}|100|A|A|male|1980-01-01|1.1.1.1|Firefox|1|en|a@x.com",
            f"{d}|200|B|B|female|1985-01-01|1.1.1.2|Firefox|1|en|b@x.com",
            f"{d}|300|C|C|male|1990-01-01|1.1.1.3|Firefox|1|en|c@x.com",
            f"{d}|400|D|D|female|1995-01-01|1.1.1.4|Firefox|1|en|d@x.com",
        ],
    )
    _write_entity(
        snapshot, "dynamic", "Person_knows_Person",
        [f"{d}|100|200", f"{d}|100|300", f"{d}|100|400", f"{d}|200|300"],
    )
    _write_entity(
        snapshot, "dynamic", "Post",
        [
            f"{d}|1000||1.1.1.1|Firefox|en|hello|5|100|0|1",
            f"{d}|1001||1.1.1.2|Firefox|en|world|5|200|0|1",
        ],
    )
    _write_entity(
        snapshot, "dynamic", "Comment",
        [f"{d}|2000|1.1.1.3|Firefox|nice|4|300|1|1000|"],
    )
    _write_entity(
        snapshot, "dynamic", "Post_hasTag_Tag",
        [f"{d}|1000|50", f"{d}|1001|60"],
    )
    _write_entity(
        snapshot, "dynamic", "Comment_hasTag_Tag",
        [f"{d}|2000|50"],
    )
    _write_entity(
        snapshot, "static", "Tag",
        ["50|Alpha|http://x/Alpha|1", "60|Beta|http://x/Beta|1"],
    )
    return snapshot


class SnbSchemaTests(unittest.TestCase):
    def test_schema_types_and_connectivity(self) -> None:
        schema, plans = snb_graph_schema(RuntimeScale())
        self.assertEqual(
            [node.name for node in schema.node_types], ["Person", "Message", "Tag"]
        )
        self.assertEqual(
            [(edge.name, edge.source_type, edge.target_type) for edge in schema.edge_types],
            [
                ("KNOWS", "Person", "Person"),
                ("CREATED", "Person", "Message"),
                ("HAS_TAG", "Message", "Tag"),
            ],
        )
        # Corpus contract: edge1 self-loop, edge2 t1->t2, edge3 t2->t3.
        self.assertEqual(
            [(p.source_type_index, p.target_type_index) for p in plans],
            [(1, 1), (1, 2), (2, 3)],
        )

    def test_schema_width_matches_synthetic(self) -> None:
        scale = RuntimeScale()
        snb_schema, _ = snb_graph_schema(scale)
        synthetic_schema, _ = _build_graph_schema(scale)
        self.assertEqual(
            snb_schema.node_types[0].properties,
            synthetic_schema.node_types[0].properties,
        )
        self.assertEqual(
            snb_schema.edge_types[0].properties,
            synthetic_schema.edge_types[0].properties,
        )

    def test_token_map_binds_slots_and_sample_names(self) -> None:
        token_map = snb_token_map(RuntimeScale(variable_hop_max=5))
        self.assertEqual(token_map["node_type_1"], "Person")
        self.assertEqual(token_map["node_type_2"], "Message")
        self.assertEqual(token_map["node_type_3"], "Tag")
        self.assertEqual(token_map["edge_type_1"], "KNOWS")
        self.assertEqual(token_map["node_type_1_name_1"], "person-000001")
        self.assertEqual(token_map["node_type_2_name_1"], "message-000001")
        self.assertEqual(token_map["variable_hop_max"], "5")
        self.assertEqual(token_map["grouped_rollup_variable_hop_max"], "3")


class SnbConversionTests(unittest.TestCase):
    def test_convert_tiny_snb(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            snapshot = _build_tiny_snb(root)
            self.assertEqual(_find_initial_snapshot(root), snapshot)

            scale = RuntimeScale()
            schema, _ = snb_graph_schema(scale)
            out_dir = root / "out"
            row_counts = _convert_snb_to_fixture(
                snapshot=snapshot,
                graph_schema=schema,
                out_dir=out_dir,
                progress_label="test",
            )

            self.assertEqual(row_counts["person_count"], 4)
            self.assertEqual(row_counts["message_count"], 3)  # 2 Post + 1 Comment
            self.assertEqual(row_counts["tag_count"], 2)
            self.assertEqual(row_counts["knows_count"], 8)  # 4 pairs, bidirected
            self.assertEqual(row_counts["created_count"], 3)
            self.assertEqual(row_counts["has_tag_count"], 3)
            self.assertEqual(row_counts["node_count"], 9)
            self.assertEqual(row_counts["edge_count"], 14)

    def test_dense_ids_and_hub_first_ordering(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            snapshot = _build_tiny_snb(root)
            scale = RuntimeScale()
            schema, _ = snb_graph_schema(scale)
            out_dir = root / "out"
            _convert_snb_to_fixture(
                snapshot=snapshot,
                graph_schema=schema,
                out_dir=out_dir,
                progress_label="test",
            )
            person_rows = list(
                csv.DictReader((out_dir / "cg_node_person.csv").open())
            )
            # Dense ids 1..4, contiguous.
            self.assertEqual([int(r["id"]) for r in person_rows], [1, 2, 3, 4])
            # Densest hub (real id 100, degree 3) is labelled person-000001.
            self.assertEqual(person_rows[0]["name"], "person-000001")

            knows_rows = list(
                csv.DictReader((out_dir / "cg_edge_knows.csv").open())
            )
            hub_out = [r for r in knows_rows if r["from_id"] == "1"]
            self.assertEqual(len(hub_out), 3)  # hub knows 200, 300, 400
            # Every endpoint is a valid dense person ordinal (1..4).
            for row in knows_rows:
                self.assertIn(int(row["from_id"]), range(1, 5))
                self.assertIn(int(row["to_id"]), range(1, 5))

    def test_conversion_is_deterministic(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            snapshot = _build_tiny_snb(root)
            schema, _ = snb_graph_schema(RuntimeScale())
            first = _convert_snb_to_fixture(
                snapshot=snapshot, graph_schema=schema,
                out_dir=root / "out1", progress_label="t",
            )
            second = _convert_snb_to_fixture(
                snapshot=snapshot, graph_schema=schema,
                out_dir=root / "out2", progress_label="t",
            )
            self.assertEqual(first, second)
            self.assertEqual(
                (root / "out1/cg_node_person.csv").read_text(),
                (root / "out2/cg_node_person.csv").read_text(),
            )


class SnbFixtureTests(unittest.TestCase):
    def test_prepare_fixture_caches_and_exposes_tables(self) -> None:
        with TemporaryDirectory() as tmp:
            data_dir = Path(tmp)
            _build_tiny_snb(data_dir)
            topology = LdbcSnbTopology(data_dir=data_dir)
            scale = RuntimeScale()
            schema, edge_plans = topology.build_schema(scale)
            fixture = topology.prepare_fixture(
                scale=scale,
                graph_schema=schema,
                edge_plans=edge_plans,
                index_mode="indexed",
            )
            for node_type in schema.node_types:
                self.assertIn(node_type.table_name, fixture.table_csv_paths)
                self.assertTrue(fixture.table_csv_paths[node_type.table_name].exists())
            self.assertEqual(fixture.row_counts["node_count"], 9)

            # A second call reuses the on-disk cache (no re-conversion needed).
            fixture2 = topology.prepare_fixture(
                scale=scale,
                graph_schema=schema,
                edge_plans=edge_plans,
                index_mode="unindexed",
            )
            self.assertEqual(fixture2.row_counts, fixture.row_counts)


class TopologyResolutionTests(unittest.TestCase):
    def test_resolve_synthetic_default(self) -> None:
        self.assertIsInstance(resolve_topology("synthetic"), SyntheticTopology)

    def test_synthetic_topology_matches_shared_builders(self) -> None:
        scale = RuntimeScale()
        schema_a, plans_a = SyntheticTopology().build_schema(scale)
        schema_b, plans_b = _build_graph_schema(scale)
        self.assertEqual(schema_a, schema_b)
        self.assertEqual(plans_a, plans_b)

    def test_resolve_ldbc_snb_requires_data_dir(self) -> None:
        with self.assertRaises(ValueError):
            resolve_topology("ldbc_snb")

    def test_resolve_unknown_topology_raises(self) -> None:
        with self.assertRaises(ValueError):
            resolve_topology("nope")


if __name__ == "__main__":
    unittest.main()
