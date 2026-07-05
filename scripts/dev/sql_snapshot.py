"""Golden-SQL snapshot of the benchmark corpus.

Compiles every benchmark corpus query for every declared SQL compile-target
backend and both variable-length lowering strategies, and freezes the emitted
SQL in ``tests/snapshots/benchmark_corpus_sql.json``.

The snapshot is a divergence *detector*, not a blocker: when a compiler or
dependency change alters emitted SQL, regenerate with ``--write`` and review
the git diff — the committed snapshot diff is the review artifact that tells
us exactly which benchmark measurements a change invalidates.

Usage:
  uv run python scripts/dev/sql_snapshot.py            # print mismatch summary
  uv run python scripts/dev/sql_snapshot.py --write    # regenerate snapshot
  uv run python scripts/dev/sql_snapshot.py --diff     # unified diff vs file
"""

from __future__ import annotations

import argparse
import difflib
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT))

import cypherglot  # noqa: E402

from scripts.benchmarks.common.runtime_core import (  # noqa: E402
    DEFAULT_CORPUS_PATH,
    _load_corpus,
)
from scripts.benchmarks.common.shared import (  # noqa: E402
    RECURSIVE_CTE_BACKENDS,
    _build_graph_schema,
    _render_corpus_queries,
    _query_has_variable_length,
    _token_map,
)
from scripts.benchmarks.runtime.matrix import SCALE_PRESETS  # noqa: E402

SNAPSHOT_PATH = REPO_ROOT / "tests" / "snapshots" / "benchmark_corpus_sql.json"

# The SQL compile targets and how the runtime harness invokes the renderer for
# each (mirrors _BackendRunner.compile_query in runtime_core.py).
_BACKEND_RENDER_KWARGS: dict[str, dict[str, str]] = {
    "sqlite": {"backend": "sqlite"},
    "turso": {"backend": "turso"},
    "duckdb": {"dialect": "duckdb"},
    "postgresql": {"dialect": "postgres", "backend": "postgresql"},
    "clickhouse": {"backend": "clickhouse"},
}


def build_snapshot() -> dict[str, dict[str, str]]:
    scale = SCALE_PRESETS["medium"].scale
    graph_schema, edge_plans = _build_graph_schema(scale)
    schema_context = cypherglot.CompilerSchemaContext.type_aware(graph_schema)
    token_map = _token_map(scale, graph_schema, edge_plans)
    queries = _render_corpus_queries(_load_corpus(DEFAULT_CORPUS_PATH), token_map)

    snapshot: dict[str, dict[str, str]] = {}
    for query in queries:
        strategies = ["unroll"]
        if _query_has_variable_length(query):
            strategies.append("recursive_cte")
        for backend in query.backends:
            render_kwargs = _BACKEND_RENDER_KWARGS.get(backend)
            if render_kwargs is None:
                continue  # native engine: no SQL is emitted
            for strategy in strategies:
                if (
                    strategy == "recursive_cte"
                    and backend not in RECURSIVE_CTE_BACKENDS
                ):
                    continue
                key = f"{query.name}::{backend}::{strategy}"
                try:
                    if query.mode == "statement":
                        cell = {
                            "sql": cypherglot.to_sql(
                                query.query,
                                schema_context=schema_context,
                                variable_length_strategy=strategy,
                                **render_kwargs,
                            )
                        }
                    else:
                        rendered = cypherglot.render_cypher_program_text(
                            query.query,
                            schema_context=schema_context,
                            **render_kwargs,
                        )
                        cell = {
                            "sql": "\n".join(
                                getattr(step, "sql", str(step))
                                for step in rendered.steps
                            )
                        }
                except Exception as error:  # noqa: BLE001 - rejections are data
                    cell = {"error": str(error).splitlines()[0]}
                snapshot[key] = cell
    return dict(sorted(snapshot.items()))


def _dump(snapshot: dict[str, dict[str, str]]) -> str:
    return json.dumps(snapshot, indent=1, sort_keys=True) + "\n"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--write", action="store_true", help="Regenerate the snapshot file.")
    parser.add_argument("--diff", action="store_true", help="Print a unified diff vs the committed snapshot.")
    args = parser.parse_args()

    current = _dump(build_snapshot())
    if args.write:
        SNAPSHOT_PATH.parent.mkdir(parents=True, exist_ok=True)
        SNAPSHOT_PATH.write_text(current, encoding="utf-8")
        print(f"wrote {SNAPSHOT_PATH} ({current.count(chr(10))} lines)")
        return 0

    committed = SNAPSHOT_PATH.read_text(encoding="utf-8") if SNAPSHOT_PATH.exists() else ""
    if current == committed:
        print("snapshot up to date")
        return 0
    if args.diff:
        sys.stdout.writelines(
            difflib.unified_diff(
                committed.splitlines(keepends=True),
                current.splitlines(keepends=True),
                fromfile="committed",
                tofile="current",
            )
        )
    else:
        old = json.loads(committed) if committed else {}
        new = json.loads(current)
        changed = [k for k in new if k in old and old[k] != new[k]]
        added = [k for k in new if k not in old]
        removed = [k for k in old if k not in new]
        print(f"SNAPSHOT DIVERGED: {len(changed)} changed, {len(added)} added, {len(removed)} removed")
        for k in (changed + added + removed)[:20]:
            print("  ", k)
        print("Review with --diff; regenerate deliberately with --write.")
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
