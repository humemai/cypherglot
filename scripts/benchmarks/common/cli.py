"""CLI argument builders shared by benchmark entrypoints."""

from __future__ import annotations

import argparse
import os
from pathlib import Path


def parse_sql_runtime_args(
    *,
    description: str,
    default_corpus_path: Path,
    default_output_path: Path,
    enabled_backends: tuple[str, ...],
    default_index_mode: str = "both",
    index_mode_choices: tuple[str, ...] = ("indexed", "unindexed", "both"),
) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument(
        "--corpus",
        type=Path,
        default=default_corpus_path,
        help="Path to the runtime benchmark corpus JSON file.",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=default_output_path,
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
        "--oltp-iterations",
        type=int,
        help=(
            "Optional measured iterations to run per OLTP query. "
            "Defaults to --iterations."
        ),
    )
    parser.add_argument(
        "--oltp-warmup",
        type=int,
        help="Optional warmup iterations to run per OLTP query. Defaults to --warmup.",
    )
    parser.add_argument(
        "--olap-iterations",
        type=int,
        help=(
            "Optional measured iterations to run per OLAP query. "
            "Defaults to --iterations."
        ),
    )
    parser.add_argument(
        "--olap-warmup",
        type=int,
        help="Optional warmup iterations to run per OLAP query. Defaults to --warmup.",
    )
    parser.add_argument(
        "--oltp-timeout-ms",
        type=float,
        help="Abort an OLTP warmup or measured iteration if it exceeds this timeout.",
    )
    parser.add_argument(
        "--olap-timeout-ms",
        type=float,
        help="Abort an OLAP warmup or measured iteration if it exceeds this timeout.",
    )
    parser.add_argument(
        "--query-name",
        action="append",
        dest="query_names",
        help="Optional benchmark query name to run. Repeat to run multiple entries.",
    )
    parser.add_argument(
        "--iteration-progress",
        action="store_true",
        help="Print warmup and measured iteration counters for each query.",
    )
    if "duckdb" in enabled_backends and len(enabled_backends) > 1:
        parser.add_argument(
            "--skip-duckdb",
            action="store_true",
            help="Skip the DuckDB backend even if the package is installed.",
        )
    if "postgresql" in enabled_backends:
        parser.add_argument(
            "--postgres-dsn",
            default=os.environ.get("CYPHERGLOT_TEST_POSTGRES_DSN", ""),
            help=(
                "Optional PostgreSQL DSN for running the same compiled runtime "
                "benchmark against PostgreSQL. Defaults to the "
                "CYPHERGLOT_TEST_POSTGRES_DSN environment variable when set."
            ),
        )
    parser.add_argument(
        "--index-mode",
        choices=index_mode_choices,
        default=default_index_mode,
        help="Benchmark indexed and/or unindexed SQL-runtime suites.",
    )
    parser.add_argument("--node-type-count", type=int, default=4)
    parser.add_argument("--edge-type-count", type=int, default=4)
    parser.add_argument("--nodes-per-type", type=int, default=25_000)
    parser.add_argument("--edges-per-source", type=int, default=3)
    parser.add_argument(
        "--edge-degree-profile",
        choices=("uniform", "skewed"),
        default="uniform",
        help=(
            "Use a uniform out-degree or a skewed profile with 70%% of sources at "
            "2-5 edges, 25%% at 6-15 edges, and 5%% at 20-200 edges."
        ),
    )
    parser.add_argument("--node-extra-text-property-count", type=int, default=2)
    parser.add_argument("--node-extra-numeric-property-count", type=int, default=6)
    parser.add_argument("--node-extra-boolean-property-count", type=int, default=2)
    parser.add_argument("--edge-extra-text-property-count", type=int, default=1)
    parser.add_argument("--edge-extra-numeric-property-count", type=int, default=3)
    parser.add_argument("--edge-extra-boolean-property-count", type=int, default=1)
    parser.add_argument("--variable-hop-max", type=int, default=2)
    parser.add_argument("--ingest-batch-size", type=int, default=5_000)
    parser.add_argument(
        "--db-root-dir",
        type=Path,
        default=None,
        help=(
            "Optional directory under which the benchmark will create a named run "
            "folder and persist generated SQLite and DuckDB database files."
        ),
    )
    return parser.parse_args()


def parse_sqlite_runtime_args(
    *,
    default_corpus_path: Path,
    default_output_path: Path,
) -> argparse.Namespace:
    return parse_sql_runtime_args(
        description=(
            "Benchmark end-to-end runtime over a generated multi-type type-aware "
            "graph for OLTP and OLAP Cypher workloads."
        ),
        default_corpus_path=default_corpus_path,
        default_output_path=default_output_path,
        enabled_backends=("sqlite", "duckdb", "postgresql"),
    )


def parse_sqlite_schema_shapes_args(*, default_output_path: Path) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Benchmark three SQLite schema shapes for graph workloads: generic "
            "JSON properties, generic typed-property tables, and type-aware "
            "per-node/per-edge tables."
        )
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=default_output_path,
        help="Path to the JSON file where benchmark results will be written.",
    )
    parser.add_argument(
        "--iterations",
        type=int,
        default=20,
        help="Measured iterations to run per query.",
    )
    parser.add_argument(
        "--warmup",
        type=int,
        default=3,
        help="Warmup iterations to run per query before measuring.",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=5_000,
        help="Rows per executemany batch during synthetic ingestion.",
    )
    parser.add_argument(
        "--schema",
        action="append",
        choices=("json", "typed", "typeaware"),
        help="Optional schema shape to run. Repeat the flag to run multiple shapes.",
    )
    parser.add_argument(
        "--node-type-count",
        type=int,
        default=10,
        help="Number of synthetic node types to create.",
    )
    parser.add_argument(
        "--edge-type-count",
        type=int,
        default=10,
        help="Number of synthetic edge types to create.",
    )
    parser.add_argument(
        "--nodes-per-type",
        type=int,
        default=5_000,
        help="Synthetic node count to seed for each node type.",
    )
    parser.add_argument(
        "--edges-per-source",
        type=int,
        default=4,
        help="Outgoing edges to seed per source node for each edge type.",
    )
    parser.add_argument(
        "--multi-hop-length",
        type=int,
        default=5,
        help="Hop count for the multi-hop benchmark query.",
    )
    parser.add_argument(
        "--node-numeric-property-count",
        type=int,
        default=10,
        help="Numeric properties to seed on every node type.",
    )
    parser.add_argument(
        "--node-text-property-count",
        type=int,
        default=2,
        help="Extra text properties to seed on every node type in addition to name.",
    )
    parser.add_argument(
        "--node-boolean-property-count",
        type=int,
        default=2,
        help="Boolean properties to seed on every node type.",
    )
    parser.add_argument(
        "--edge-numeric-property-count",
        type=int,
        default=6,
        help="Numeric properties to seed on every edge type.",
    )
    parser.add_argument(
        "--edge-text-property-count",
        type=int,
        default=2,
        help="Extra text properties to seed on every edge type in addition to note.",
    )
    parser.add_argument(
        "--edge-boolean-property-count",
        type=int,
        default=1,
        help="Boolean properties to seed on every edge type.",
    )
    return parser.parse_args()
