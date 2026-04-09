from __future__ import annotations

import argparse
import gc
import json
import platform
import sqlite3
import tempfile
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

import cypherglot


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
    / "sqlite_runtime_benchmark_baseline.json"
)


@dataclass(frozen=True, slots=True)
class RuntimeCorpusQuery:
    name: str
    category: str
    fixture: str
    mode: str
    query: str


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Benchmark end-to-end CypherGlot compilation plus SQLite execution "
            "over a representative admitted corpus."
        )
    )
    parser.add_argument(
        "--corpus",
        type=Path,
        default=DEFAULT_CORPUS_PATH,
        help="Path to the SQLite runtime benchmark corpus JSON file.",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=DEFAULT_OUTPUT_PATH,
        help="Path to the JSON file where runtime benchmark results will be written.",
    )
    parser.add_argument(
        "--iterations",
        type=int,
        default=1000,
        help="Measured iterations to run per query and per entrypoint.",
    )
    parser.add_argument(
        "--warmup",
        type=int,
        default=10,
        help="Warmup iterations to run per query and per entrypoint before measuring.",
    )
    parser.add_argument(
        "--query-name",
        action="append",
        dest="query_names",
        help=(
            "Optional benchmark query name to run. Repeat the flag to run "
            "multiple named corpus entries."
        ),
    )
    return parser.parse_args()


def _load_corpus(path: Path) -> list[RuntimeCorpusQuery]:
    raw = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(raw, list) or not raw:
        raise ValueError(
            "SQLite runtime benchmark corpus must be a non-empty JSON list."
        )

    queries: list[RuntimeCorpusQuery] = []
    for index, item in enumerate(raw, start=1):
        if not isinstance(item, dict):
            raise ValueError(f"Runtime corpus item {index} must be a JSON object.")
        try:
            name = item["name"]
            category = item["category"]
            fixture = item["fixture"]
            mode = item["mode"]
            query = item["query"]
        except KeyError as exc:
            raise ValueError(
                f"Runtime corpus item {index} is missing required key {exc.args[0]!r}."
            ) from exc
        if not isinstance(name, str) or not name:
            raise ValueError(f"Runtime corpus item {index} has invalid 'name'.")
        if not isinstance(category, str) or not category:
            raise ValueError(f"Runtime corpus item {index} has invalid 'category'.")
        if fixture not in {
            "basic_graph",
            "chain_graph",
            "user_chain_graph",
            "duplicate_name_graph",
        }:
            raise ValueError(
                f"Runtime corpus item {index} references unknown fixture {fixture!r}."
            )
        if mode not in {"statement", "program"}:
            raise ValueError(
                f"Runtime corpus item {index} has invalid mode {mode!r}; "
                "expected 'statement' or 'program'."
            )
        if not isinstance(query, str) or not query:
            raise ValueError(f"Runtime corpus item {index} has invalid 'query'.")
        queries.append(
            RuntimeCorpusQuery(
                name=name,
                category=category,
                fixture=fixture,
                mode=mode,
                query=query,
            )
        )
    return queries


def _select_queries(
    queries: list[RuntimeCorpusQuery],
    query_names: list[str] | None,
) -> list[RuntimeCorpusQuery]:
    if not query_names:
        return queries

    requested = set(query_names)
    selected = [query for query in queries if query.name in requested]
    found = {query.name for query in selected}
    missing = sorted(requested - found)
    if missing:
        raise ValueError(
            "Unknown runtime benchmark query name(s): " + ", ".join(missing)
        )
    return selected


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
        "min_us": min(sorted_ns) / 1_000.0,
        "mean_us": sum(sorted_ns) / len(sorted_ns) / 1_000.0,
        "p50_us": _percentile(sorted_ns, 50) / 1_000.0,
        "p95_us": _percentile(sorted_ns, 95) / 1_000.0,
        "p99_us": _percentile(sorted_ns, 99) / 1_000.0,
        "max_us": max(sorted_ns) / 1_000.0,
    }


def _create_connection(db_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    journal_mode_row = conn.execute("PRAGMA journal_mode = WAL").fetchone()
    if journal_mode_row is None or str(journal_mode_row[0]).upper() != "WAL":
        raise ValueError("SQLite runtime benchmark requires WAL journal mode.")

    conn.execute("PRAGMA synchronous = NORMAL")
    synchronous_row = conn.execute("PRAGMA synchronous").fetchone()
    if synchronous_row is None or int(synchronous_row[0]) != 1:
        raise ValueError(
            "SQLite runtime benchmark requires synchronous=NORMAL."
        )

    conn.execute("PRAGMA foreign_keys = ON")
    conn.executescript(
        """
        CREATE TABLE nodes (
          id INTEGER PRIMARY KEY,
          properties TEXT NOT NULL DEFAULT '{}',
          CHECK (json_valid(properties)),
          CHECK (json_type(properties) = 'object')
        ) STRICT;

        CREATE TABLE edges (
          id INTEGER PRIMARY KEY,
          type TEXT NOT NULL,
          from_id INTEGER NOT NULL,
          to_id INTEGER NOT NULL,
          properties TEXT NOT NULL DEFAULT '{}',
          CHECK (json_valid(properties)),
          CHECK (json_type(properties) = 'object'),
          FOREIGN KEY (from_id) REFERENCES nodes(id) ON DELETE CASCADE,
          FOREIGN KEY (to_id) REFERENCES nodes(id) ON DELETE CASCADE
        ) STRICT;

        CREATE TABLE node_labels (
          node_id INTEGER NOT NULL,
          label TEXT NOT NULL,
          PRIMARY KEY (node_id, label),
          FOREIGN KEY (node_id) REFERENCES nodes(id) ON DELETE CASCADE
        ) STRICT;

        CREATE INDEX idx_node_labels_label_node_id ON node_labels(label, node_id);
        CREATE INDEX idx_node_labels_node_id_label ON node_labels(node_id, label);
        CREATE INDEX idx_edges_from_id ON edges(from_id);
        CREATE INDEX idx_edges_to_id ON edges(to_id);
        CREATE INDEX idx_edges_type ON edges(type);
        CREATE INDEX idx_edges_type_from_id ON edges(type, from_id);
        CREATE INDEX idx_edges_type_to_id ON edges(type, to_id);
        """
    )
    return conn


def _seed_basic_graph(conn: sqlite3.Connection) -> None:
    conn.execute(
        "INSERT INTO nodes (id, properties) VALUES (?, ?)",
        (1, '{"name":"Alice","age":30,"score":9}'),
    )
    conn.execute(
        "INSERT INTO nodes (id, properties) VALUES (?, ?)",
        (2, '{"name":"Bob","age":25,"score":7}'),
    )
    conn.execute(
        "INSERT INTO node_labels (node_id, label) VALUES (?, ?)",
        (1, "User"),
    )
    conn.execute(
        "INSERT INTO node_labels (node_id, label) VALUES (?, ?)",
        (2, "User"),
    )
    conn.execute(
        (
            "INSERT INTO edges (id, type, from_id, to_id, properties) "
            "VALUES (?, ?, ?, ?, ?)"
        ),
        (10, "KNOWS", 1, 2, '{"note":"met","weight":2}'),
    )
    conn.commit()


def _seed_chain_graph(conn: sqlite3.Connection) -> None:
    _seed_basic_graph(conn)
    conn.execute(
        "INSERT INTO nodes (id, properties) VALUES (?, ?)",
        (3, '{"name":"Acme"}'),
    )
    conn.execute(
        "INSERT INTO node_labels (node_id, label) VALUES (?, ?)",
        (3, "Company"),
    )
    conn.execute(
        (
            "INSERT INTO edges (id, type, from_id, to_id, properties) "
            "VALUES (?, ?, ?, ?, ?)"
        ),
        (11, "WORKS_AT", 2, 3, '{}'),
    )
    conn.commit()


def _seed_user_chain_graph(conn: sqlite3.Connection) -> None:
    _seed_basic_graph(conn)
    conn.execute(
        "INSERT INTO nodes (id, properties) VALUES (?, ?)",
        (3, '{"name":"Cara","age":28,"score":8}'),
    )
    conn.execute(
        "INSERT INTO node_labels (node_id, label) VALUES (?, ?)",
        (3, "User"),
    )
    conn.execute(
        (
            "INSERT INTO edges (id, type, from_id, to_id, properties) "
            "VALUES (?, ?, ?, ?, ?)"
        ),
        (11, "KNOWS", 2, 3, '{"note":"coworker"}'),
    )
    conn.commit()


def _seed_duplicate_name_graph(conn: sqlite3.Connection) -> None:
    _seed_basic_graph(conn)
    conn.execute(
        "INSERT INTO nodes (id, properties) VALUES (?, ?)",
        (3, '{"name":"Alice","age":22,"score":5}'),
    )
    conn.execute(
        "INSERT INTO node_labels (node_id, label) VALUES (?, ?)",
        (3, "User"),
    )
    conn.commit()


def _seed_fixture(conn: sqlite3.Connection, fixture: str) -> None:
    if fixture == "basic_graph":
        _seed_basic_graph(conn)
        return
    if fixture == "chain_graph":
        _seed_chain_graph(conn)
        return
    if fixture == "user_chain_graph":
        _seed_user_chain_graph(conn)
        return
    if fixture == "duplicate_name_graph":
        _seed_duplicate_name_graph(conn)
        return
    raise ValueError(f"Unknown runtime fixture {fixture!r}.")


def _execute_program(
    conn: sqlite3.Connection,
    program: cypherglot.RenderedCypherProgram,
    *,
    commit: bool = True,
) -> None:
    bindings: dict[str, object] = {}
    for step in program.steps:
        if isinstance(step, cypherglot.RenderedCypherLoop):
            rows = conn.execute(step.source, bindings).fetchall()
            for row in rows:
                loop_bindings = bindings | dict(
                    zip(step.row_bindings, row, strict=True)
                )
                for statement in step.body:
                    cursor = conn.execute(statement.sql, loop_bindings)
                    if statement.bind_columns:
                        returned = cursor.fetchone()
                        if returned is None:
                            raise ValueError(
                                "Rendered benchmark program expected bound columns."
                            )
                        loop_bindings |= dict(
                            zip(statement.bind_columns, returned, strict=True)
                        )
            continue

        cursor = conn.execute(step.sql, bindings)
        if step.bind_columns:
            returned = cursor.fetchone()
            if returned is None:
                raise ValueError("Rendered benchmark statement expected bound columns.")
            bindings |= dict(zip(step.bind_columns, returned, strict=True))
    if commit:
        conn.commit()


class _RuntimeQueryRunner:
    def __init__(self, corpus_query: RuntimeCorpusQuery) -> None:
        self._temp_dir = tempfile.TemporaryDirectory(
            prefix="cypherglot-runtime-bench-"
        )
        self._conn = _create_connection(
            Path(self._temp_dir.name) / "runtime.sqlite3"
        )
        self._corpus_query = corpus_query
        _seed_fixture(self._conn, corpus_query.fixture)

    @property
    def conn(self) -> sqlite3.Connection:
        return self._conn

    def close(self) -> None:
        self._conn.close()
        self._temp_dir.cleanup()

    def run_once(self) -> None:
        self._conn.execute("SAVEPOINT benchmark_iteration")
        try:
            if self._corpus_query.mode == "statement":
                sql = cypherglot.to_sql(self._corpus_query.query)
                cursor = self._conn.execute(sql)
                if cursor.description is not None:
                    cursor.fetchall()
            else:
                program = cypherglot.render_cypher_program_text(
                    self._corpus_query.query
                )
                _execute_program(self._conn, program, commit=False)
        finally:
            self._conn.execute("ROLLBACK TO benchmark_iteration")
            self._conn.execute("RELEASE benchmark_iteration")


def _measure_query(
    corpus_query: RuntimeCorpusQuery,
    *,
    iterations: int,
    warmup: int,
) -> list[int]:
    runner = _RuntimeQueryRunner(corpus_query)
    try:
        for _ in range(warmup):
            runner.run_once()

        latencies_ns: list[int] = []
        gc_was_enabled = gc.isenabled()
        gc.disable()
        try:
            for _ in range(iterations):
                started_ns = time.perf_counter_ns()
                runner.run_once()
                latencies_ns.append(time.perf_counter_ns() - started_ns)
        finally:
            if gc_was_enabled:
                gc.enable()
        return latencies_ns
    finally:
        runner.close()


def _measure_batch(
    queries: list[RuntimeCorpusQuery],
    *,
    iterations: int,
    warmup: int,
) -> list[int]:
    runners = [_RuntimeQueryRunner(corpus_query) for corpus_query in queries]
    try:
        for _ in range(warmup):
            for runner in runners:
                runner.run_once()

        latencies_ns: list[int] = []
        gc_was_enabled = gc.isenabled()
        gc.disable()
        try:
            for _ in range(iterations):
                started_ns = time.perf_counter_ns()
                for runner in runners:
                    runner.run_once()
                latencies_ns.append(time.perf_counter_ns() - started_ns)
        finally:
            if gc_was_enabled:
                gc.enable()
        return latencies_ns
    finally:
        for runner in runners:
            runner.close()


def _runtime_suite_result(
    queries: list[RuntimeCorpusQuery],
    *,
    iterations: int,
    warmup: int,
) -> dict[str, object]:
    per_query_results: list[dict[str, object]] = []
    all_latencies_ns: list[int] = []
    for corpus_query in queries:
        latencies_ns = _measure_query(
            corpus_query,
            iterations=iterations,
            warmup=warmup,
        )
        all_latencies_ns.extend(latencies_ns)
        per_query_results.append(
            {
                "name": corpus_query.name,
                "category": corpus_query.category,
                "fixture": corpus_query.fixture,
                "mode": corpus_query.mode,
                "summary": _summarize(latencies_ns),
            }
        )

    batch_latencies_ns = _measure_batch(
        queries,
        iterations=iterations,
        warmup=warmup,
    )
    return {
        "iterations": iterations,
        "warmup": warmup,
        "query_count": len(queries),
        "overall": _summarize(all_latencies_ns),
        "batch": _summarize(batch_latencies_ns),
        "queries": per_query_results,
    }


def _print_summary(result: dict[str, object]) -> None:
    overall = result["overall"]
    assert isinstance(overall, dict)
    batch = result["batch"]
    assert isinstance(batch, dict)
    print("runtime queries")
    for query_result in result["queries"]:
        assert isinstance(query_result, dict)
        summary = query_result["summary"]
        assert isinstance(summary, dict)
        print(
            "  - "
            f"{query_result['name']} "
            f"[{query_result['category']}/{query_result['fixture']}]: "
            f"mean={summary['mean_us']:.2f} us, "
            f"p50={summary['p50_us']:.2f} us, "
            f"p95={summary['p95_us']:.2f} us, "
            f"p99={summary['p99_us']:.2f} us"
        )
    print("runtime aggregate")
    print(
        "  pooled: "
        f"mean={overall['mean_us']:.2f} us, "
        f"p50={overall['p50_us']:.2f} us, "
        f"p95={overall['p95_us']:.2f} us, "
        f"p99={overall['p99_us']:.2f} us"
    )
    print(
        "  batch: "
        f"mean={batch['mean_us']:.2f} us, "
        f"p50={batch['p50_us']:.2f} us, "
        f"p95={batch['p95_us']:.2f} us, "
        f"p99={batch['p99_us']:.2f} us"
    )


def main() -> int:
    args = _parse_args()
    if args.iterations <= 0:
        raise ValueError("--iterations must be positive.")
    if args.warmup < 0:
        raise ValueError("--warmup must be zero or positive.")

    queries = _select_queries(_load_corpus(args.corpus), args.query_names)
    result = _runtime_suite_result(
        queries,
        iterations=args.iterations,
        warmup=args.warmup,
    )
    payload = {
        "generated_at": datetime.now(UTC).isoformat(),
        "python_version": platform.python_version(),
        "platform": platform.platform(),
        "cypherglot_version": cypherglot.__version__,
        "corpus_path": str(args.corpus),
        "results": result,
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")

    print(f"Wrote runtime benchmark baseline to {args.output}")
    _print_summary(result)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
