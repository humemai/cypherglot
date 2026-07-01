"""Graph-topology abstraction for the runtime benchmark.

A *topology* supplies the graph that the fixed workload runs against: its schema
(node/edge types and properties), the concrete dataset (materialised as a
``GeneratedGraphFixture``), and the token map that binds the corpus's abstract
slots (``%node_type_1%``, ``%edge_type_1%``, ``%node_type_1_name_1%``, ...) to
this topology's real type and sample names.

The ``synthetic`` topology is the historical default (a parametric generator);
``ldbc_snb`` loads a real LDBC SNB Datagen dataset. Both satisfy the *same*
corpus contract so the identical corpus renders and runs unchanged on either one,
isolating graph structure as the experimental factor:

* ``node_type_1..3`` carrying node properties ``{name, age, score, active}``
  (plus the ``id`` primary key);
* ``edge_type_1`` (``node_type_1 -> node_type_1``, a self loop -- the
  variable-length traversal edge), ``edge_type_2``
  (``node_type_1 -> node_type_2``), and ``edge_type_3``
  (``node_type_2 -> node_type_3``), each carrying edge properties
  ``{note, weight, score, active, rank}``.

The LDBC SNB topology preserves the *real* graph structure (the skewed
Person-knows-Person social graph, heterogeneous entity types, real
cardinalities) and derives the analytic payload columns (``score``, ``active``,
``weight``, ``rank``, ``note``) deterministically from real SNB fields, so the
workload stays byte-identical while only the topology changes.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from pathlib import Path

import cypherglot

from scripts.benchmarks.common.runtime_shared import (
    GeneratedGraphFixture,
    _prepare_generated_graph_fixture,
)
from scripts.benchmarks.common.shared import (
    EdgeTypePlan,
    RuntimeScale,
    _build_graph_schema,
    _token_map,
)


TOPOLOGY_CHOICES: tuple[str, ...] = ("synthetic", "ldbc_snb")
DEFAULT_TOPOLOGY = "synthetic"


class Topology(ABC):
    """Supplies the schema, dataset, and token map for one graph topology."""

    name: str

    @abstractmethod
    def build_schema(
        self,
        scale: RuntimeScale,
    ) -> tuple[cypherglot.GraphSchema, list[EdgeTypePlan]]:
        """Return the type-aware schema and its edge-connectivity plan."""

    @abstractmethod
    def token_map(
        self,
        scale: RuntimeScale,
        graph_schema: cypherglot.GraphSchema,
        edge_plans: list[EdgeTypePlan],
    ) -> dict[str, str]:
        """Return the corpus placeholder -> concrete value map for this graph."""

    @abstractmethod
    def prepare_fixture(
        self,
        *,
        scale: RuntimeScale,
        graph_schema: cypherglot.GraphSchema,
        edge_plans: list[EdgeTypePlan],
        index_mode: str,
        db_root_dir: Path | None = None,
    ) -> GeneratedGraphFixture:
        """Materialise the per-table CSV dataset the backends ingest from."""


class SyntheticTopology(Topology):
    """The parametric synthetic generator (historical default, unchanged)."""

    name = "synthetic"

    def build_schema(
        self,
        scale: RuntimeScale,
    ) -> tuple[cypherglot.GraphSchema, list[EdgeTypePlan]]:
        return _build_graph_schema(scale)

    def token_map(
        self,
        scale: RuntimeScale,
        graph_schema: cypherglot.GraphSchema,
        edge_plans: list[EdgeTypePlan],
    ) -> dict[str, str]:
        return _token_map(scale, graph_schema, edge_plans)

    def prepare_fixture(
        self,
        *,
        scale: RuntimeScale,
        graph_schema: cypherglot.GraphSchema,
        edge_plans: list[EdgeTypePlan],
        index_mode: str,
        db_root_dir: Path | None = None,
    ) -> GeneratedGraphFixture:
        return _prepare_generated_graph_fixture(
            scale=scale,
            graph_schema=graph_schema,
            edge_plans=edge_plans,
            index_mode=index_mode,
            db_root_dir=db_root_dir,
        )


def resolve_topology(
    name: str,
    *,
    ldbc_snb_data_dir: Path | None = None,
) -> Topology:
    """Build the ``Topology`` selected on the command line."""
    if name == "synthetic":
        return SyntheticTopology()
    if name == "ldbc_snb":
        if ldbc_snb_data_dir is None:
            raise ValueError(
                "The ldbc_snb topology requires --ldbc-snb-data-dir "
                "(a directory of LDBC SNB Datagen composite-merged-fk CSV output)."
            )
        # Imported lazily so the synthetic path never pays for the SNB loader.
        from scripts.benchmarks.common.topology_ldbc_snb import LdbcSnbTopology

        return LdbcSnbTopology(data_dir=ldbc_snb_data_dir)
    raise ValueError(
        f"Unknown topology {name!r}; expected one of {TOPOLOGY_CHOICES}."
    )
