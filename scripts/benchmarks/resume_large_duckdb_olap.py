from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

import cypherglot

from scripts.benchmarks import benchmark_sqlite_runtime as mod


PAYLOAD_PATH = (
    Path(__file__).resolve().parents[2] / "scripts" / "benchmarks" / "results" / "runtime-large.json"
)


def main() -> int:
    payload = json.loads(PAYLOAD_PATH.read_text(encoding="utf-8"))

    scale_data = payload["graph_scale"]
    scale = mod.RuntimeScale(
        node_type_count=scale_data["node_type_count"],
        edge_type_count=scale_data["edge_type_count"],
        nodes_per_type=scale_data["nodes_per_type"],
        edges_per_source=scale_data["edges_per_source"],
        edge_degree_profile=scale_data["edge_degree_profile"],
        node_extra_text_property_count=scale_data["node_extra_text_property_count"],
        node_extra_numeric_property_count=scale_data["node_extra_numeric_property_count"],
        node_extra_boolean_property_count=scale_data["node_extra_boolean_property_count"],
        edge_extra_text_property_count=scale_data["edge_extra_text_property_count"],
        edge_extra_numeric_property_count=scale_data["edge_extra_numeric_property_count"],
        edge_extra_boolean_property_count=scale_data["edge_extra_boolean_property_count"],
        ingest_batch_size=scale_data["ingest_batch_size"],
        variable_hop_max=scale_data["variable_hop_max"],
    )

    graph_schema, edge_plans = mod._build_graph_schema(scale)
    schema_context = cypherglot.CompilerSchemaContext.type_aware(graph_schema)

    corpus_path = Path(payload["corpus_path"])
    queries = mod._select_queries(mod._load_corpus(corpus_path), None)
    token_map = mod._token_map(scale, graph_schema, edge_plans)
    rendered_queries = mod._render_corpus_queries(queries, token_map)
    olap_queries = [query for query in rendered_queries if query.workload == "olap"]
    duckdb_olap_queries = mod._filter_duckdb_olap_queries(olap_queries)
    if not duckdb_olap_queries:
        raise SystemExit("No DuckDB OLAP queries remain after filtering.")

    run_root = Path(payload["db_root_dir"])
    sqlite_suite = payload["results"]["workloads"]["olap"]["sqlite_indexed"]
    sqlite_fixture = mod.SharedSQLiteFixture(
        work_dir=mod.ManagedDirectory(path=run_root / "sqlite-indexed"),
        db_path=Path(sqlite_suite["db_path"]),
        setup_metrics={
            f"{key[:-3]}_ns": int(value * 1_000_000)
            for key, value in sqlite_suite["setup"].items()
        },
        row_counts=dict(sqlite_suite["row_counts"]),
        rss_snapshots_mib=dict(sqlite_suite["rss_snapshots_mib"]),
        db_size_mib=float(sqlite_suite["storage"]["db_size_mib"]),
        wal_size_mib=float(sqlite_suite["storage"]["wal_size_mib"]),
        index_mode="indexed",
    )

    duckdb_suite_dir = run_root / "olap-duckdb-suite"
    duckdb_suite_dir.mkdir(parents=True, exist_ok=True)
    duckdb_db_path = duckdb_suite_dir / "runtime.duckdb"
    if duckdb_db_path.exists():
        duckdb_db_path.unlink()

    runner = mod._BackendRunner(
        "duckdb",
        mod.ManagedDirectory(path=duckdb_suite_dir),
        graph_schema=graph_schema,
        schema_context=schema_context,
        sqlite_source=sqlite_fixture,
    )

    olap_iterations = payload["workload_controls"]["olap_iterations"]
    olap_warmup = payload["workload_controls"]["olap_warmup"]
    query_results: list[dict[str, object]] = []
    try:
        mod._progress(
            f"olap/duckdb: resuming suite with {len(duckdb_olap_queries)} queries "
            f"({olap_iterations} iterations, {olap_warmup} warmup)"
        )
        skipped_names = sorted(
            query.name
            for query in olap_queries
            if "duckdb" in query.backends
            and query.name in mod.DUCKDB_OLAP_QUERY_SKIP_NAMES
        )
        if skipped_names:
            mod._progress(
                "olap/duckdb: skipping query/queries not admitted for the attached-SQLite "
                "DuckDB benchmark path: "
                + ", ".join(skipped_names)
            )
        for index, query in enumerate(duckdb_olap_queries, start=1):
            label = f"olap/duckdb: query {index}/{len(duckdb_olap_queries)} {query.name}"
            mod._progress(label)
            query_results.append(
                mod._measure_query(
                    runner,
                    query,
                    iterations=olap_iterations,
                    warmup=olap_warmup,
                    progress_label=label,
                    iteration_progress=True,
                )
            )

        suite = {
            "backend": "duckdb",
            "index_mode": "n/a",
            "iterations": olap_iterations,
            "warmup": olap_warmup,
            "query_count": len(duckdb_olap_queries),
            "setup": {
                f"{metric[:-3]}_ms": value / 1_000_000.0
                for metric, value in runner.setup_metrics.items()
            },
            "row_counts": runner.row_counts,
            "rss_snapshots_mib": runner.rss_snapshots_mib,
            "storage": {
                "db_size_mib": runner.db_size_mib,
                "wal_size_mib": runner.wal_size_mib,
            },
            "db_path": str(duckdb_db_path),
            "compile": mod._pool_summaries(query_results, "compile"),
            "execute": mod._pool_summaries(query_results, "execute"),
            "end_to_end": mod._pool_summaries(query_results, "end_to_end"),
            "reset": mod._pool_summaries(query_results, "reset"),
            "queries": query_results,
        }
    finally:
        runner.close()

    payload["results"]["token_map"] = token_map
    payload["results"]["workloads"]["olap"]["duckdb_skipped_queries"] = sorted(
        mod.DUCKDB_OLAP_QUERY_SKIP_NAMES
    )
    payload["results"]["workloads"]["olap"]["duckdb"] = suite
    now = datetime.now(UTC).isoformat()
    payload["updated_at"] = now
    payload["run_status"] = "completed"
    payload["completed_at"] = now
    mod._write_json_atomic(PAYLOAD_PATH, payload)
    mod._progress(
        f"runtime benchmark: updated {PAYLOAD_PATH} with resumed DuckDB OLAP results"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
