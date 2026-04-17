"""Benchmark CypherGlot compiler throughput across representative corpora.

This script runs the main CypherGlot compiler entrypoints over the benchmark
corpus, optionally compares comparable SQL workloads against sqlglot, and
writes a JSON baseline with timing summaries, environment metadata, and corpus
configuration details.
"""

from __future__ import annotations

import argparse
import contextlib
import gc
import importlib
import importlib.metadata
import json
import platform
import shutil
import statistics
import subprocess
import sys
import tempfile
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Callable

from cypherglot.ir import (
    SQLBackend,
    bind_graph_relational_backend,
    build_graph_relational_ir,
    lower_graph_relational_ir,
)
from _benchmark_common import _progress

REPO_ROOT = Path(__file__).resolve().parents[2]


DEFAULT_CORPUS_PATH = (
    REPO_ROOT
    / "scripts"
    / "benchmarks"
    / "corpora"
    / "compiler_benchmark_corpus.json"
)
DEFAULT_SQLGLOT_CORPUS_PATH = (
    REPO_ROOT
    / "scripts"
    / "benchmarks"
    / "corpora"
    / "compiler_sqlglot_benchmark_corpus.json"
)
DEFAULT_OUTPUT_PATH = (
    REPO_ROOT
    / "scripts"
    / "benchmarks"
    / "results"
    / "compiler_benchmark_baseline.json"
)
SQLGLOT_READ_DIALECT = "postgres"
SQLGLOT_WRITE_DIALECT = "sqlite"
SCHEMA_REQUIRED_ENTRYPOINTS = frozenset(
    {
        "to_sqlglot_ast",
        "to_sql",
        "to_sqlglot_program",
        "render_cypher_program_text",
    }
)


@dataclass(frozen=True, slots=True)
class CorpusQuery:
    name: str
    category: str
    entrypoints: tuple[str, ...]
    query: str


@dataclass(frozen=True, slots=True)
class SqlCorpusQuery:
    name: str
    category: str
    query: str


CYPHERGLOT_ENTRYPOINT_ATTRS: dict[str, str] = {
    "parse_cypher_text": "parse_cypher_text",
    "validate_cypher_text": "validate_cypher_text",
    "normalize_cypher_text": "normalize_cypher_text",
    "to_sqlglot_ast": "to_sqlglot_ast",
    "to_sql": "to_sql",
    "to_sqlglot_program": "to_sqlglot_program",
    "render_cypher_program_text": "render_cypher_program_text",
}


DEFAULT_ENTRYPOINT_ORDER: tuple[str, ...] = (
    "parse_cypher_text",
    "validate_cypher_text",
    "normalize_cypher_text",
    "to_sqlglot_ast",
    "to_sql",
    "to_sqlglot_program",
    "render_cypher_program_text",
)


def _benchmark_schema_context(cypherglot_module):
    return cypherglot_module.CompilerSchemaContext.type_aware(
        cypherglot_module.GraphSchema(
            node_types=(
                cypherglot_module.NodeTypeSpec(
                    name="User",
                    properties=(
                        cypherglot_module.PropertyField("name", "string"),
                        cypherglot_module.PropertyField("age", "integer"),
                        cypherglot_module.PropertyField("score", "float"),
                        cypherglot_module.PropertyField("active", "boolean"),
                    ),
                ),
                cypherglot_module.NodeTypeSpec(
                    name="Company",
                    properties=(
                        cypherglot_module.PropertyField("name", "string"),
                    ),
                ),
                cypherglot_module.NodeTypeSpec(
                    name="Person",
                    properties=(
                        cypherglot_module.PropertyField("name", "string"),
                    ),
                ),
            ),
            edge_types=(
                cypherglot_module.EdgeTypeSpec(
                    name="KNOWS",
                    source_type="User",
                    target_type="User",
                    properties=(
                        cypherglot_module.PropertyField("note", "string"),
                        cypherglot_module.PropertyField("weight", "float"),
                        cypherglot_module.PropertyField("score", "float"),
                        cypherglot_module.PropertyField("active", "boolean"),
                    ),
                ),
                cypherglot_module.EdgeTypeSpec(
                    name="WORKS_AT",
                    source_type="User",
                    target_type="Company",
                    properties=(
                        cypherglot_module.PropertyField("since", "integer"),
                    ),
                ),
                cypherglot_module.EdgeTypeSpec(
                    name="INTRODUCED",
                    source_type="User",
                    target_type="Person",
                ),
            ),
        )
    )


def _benchmark_schema_metadata(schema_context) -> dict[str, object]:
    graph_schema = schema_context.graph_schema
    assert graph_schema is not None
    return {
        "layout": schema_context.layout,
        "node_types": [node_type.name for node_type in graph_schema.node_types],
        "edge_types": [edge_type.name for edge_type in graph_schema.edge_types],
    }


def _benchmark_entrypoint_callable(
    cypherglot_module,
    *,
    label: str,
) -> Callable[[str], object]:
    func = getattr(cypherglot_module, CYPHERGLOT_ENTRYPOINT_ATTRS[label])
    if label not in SCHEMA_REQUIRED_ENTRYPOINTS:
        return func

    schema_context = _benchmark_schema_context(cypherglot_module)
    return lambda query: func(query, schema_context=schema_context)


def _backend_lowerers() -> dict[SQLBackend, Callable[[object], object]]:
    compile_module = importlib.import_module("cypherglot.compile")
    duckdb_module = importlib.import_module("cypherglot._compile_duckdb")
    shared_lowerer = getattr(
        compile_module,
        "_compile_graph_relational_backend_program",
    )
    duckdb_lowerer = getattr(duckdb_module, "_compile_duckdb_backend_program")
    return {
        SQLBackend.SQLITE: shared_lowerer,
        SQLBackend.DUCKDB: duckdb_lowerer,
        SQLBackend.POSTGRESQL: shared_lowerer,
    }


def _backend_dialect(backend: SQLBackend) -> str:
    return {
        SQLBackend.SQLITE: "sqlite",
        SQLBackend.DUCKDB: "duckdb",
        SQLBackend.POSTGRESQL: "postgres",
    }[backend]


def _backend_lowering_result(
    queries: list[CorpusQuery],
    *,
    backend: SQLBackend,
    iterations: int,
    warmup: int,
) -> dict[str, object]:
    cypherglot_module = _import_cypherglot()
    render_module = importlib.import_module("cypherglot.render")
    schema_context = _benchmark_schema_context(cypherglot_module)
    lowerers = _backend_lowerers()
    render_cypher_program_text = getattr(
        cypherglot_module,
        "render_cypher_program_text",
    )

    applicable_queries = [
        query
        for query in queries
        if any(
            entrypoint in query.entrypoints
            for entrypoint in ("to_sqlglot_ast", "to_sqlglot_program")
        )
    ]
    if not applicable_queries:
        raise ValueError(
            f"No corpus queries apply to backend lowering benchmark {backend.value!r}."
        )
    _progress(
        "compiler benchmark: backend "
        f"{backend.value} start ({len(applicable_queries)} queries, "
        f"iterations={iterations}, warmup={warmup})"
    )

    per_query: list[dict[str, object]] = []
    build_ir_all_latencies_ns: list[int] = []
    bind_backend_all_latencies_ns: list[int] = []
    lower_backend_all_latencies_ns: list[int] = []
    render_program_all_latencies_ns: list[int] = []
    end_to_end_all_latencies_ns: list[int] = []

    for query_index, corpus_query in enumerate(applicable_queries, start=1):
        _progress(
            "compiler benchmark: backend "
            f"{backend.value} query {query_index}/{len(applicable_queries)} "
            f"{corpus_query.name}"
        )
        normalized_statement = cypherglot_module.normalize_cypher_text(
            corpus_query.query
        )
        program_ir = build_graph_relational_ir(
            normalized_statement,
            schema_context=schema_context,
        )
        compiled_program = lower_graph_relational_ir(
            program_ir,
            backend=backend,
            lowerers=lowerers,
        )
        backend_dialect = _backend_dialect(backend)

        build_ir_latencies_ns = _measure(
            corpus_query.query,
            lambda _query, normalized_statement=normalized_statement: (
                build_graph_relational_ir(
                    normalized_statement,
                    schema_context=schema_context,
                )
            ),
            iterations=iterations,
            warmup=warmup,
        )
        bind_backend_latencies_ns = _measure(
            corpus_query.query,
            lambda _query, program_ir=program_ir: bind_graph_relational_backend(
                program_ir,
                backend=backend,
            ),
            iterations=iterations,
            warmup=warmup,
        )
        lower_backend_latencies_ns = _measure(
            corpus_query.query,
            lambda _query, program_ir=program_ir: lower_graph_relational_ir(
                program_ir,
                backend=backend,
                lowerers=lowerers,
            ),
            iterations=iterations,
            warmup=warmup,
        )
        render_program_latencies_ns = _measure(
            corpus_query.query,
            lambda _query,
            compiled_program=compiled_program,
            backend=backend,
            backend_dialect=backend_dialect: (
                render_module.render_compiled_cypher_program(
                    compiled_program,
                    dialect=backend_dialect,
                    backend=backend,
                )
            ),
            iterations=iterations,
            warmup=warmup,
        )
        end_to_end_latencies_ns = _measure(
            corpus_query.query,
            lambda query,
            backend=backend,
            backend_dialect=backend_dialect,
            render_cypher_program_text=render_cypher_program_text: (
                render_cypher_program_text(
                    query,
                    dialect=backend_dialect,
                    backend=backend,
                    schema_context=schema_context,
                )
            ),
            iterations=iterations,
            warmup=warmup,
        )

        build_ir_all_latencies_ns.extend(build_ir_latencies_ns)
        bind_backend_all_latencies_ns.extend(bind_backend_latencies_ns)
        lower_backend_all_latencies_ns.extend(lower_backend_latencies_ns)
        render_program_all_latencies_ns.extend(render_program_latencies_ns)
        end_to_end_all_latencies_ns.extend(end_to_end_latencies_ns)

        per_query.append(
            {
                "name": corpus_query.name,
                "category": corpus_query.category,
                "build_ir": _summarize(build_ir_latencies_ns),
                "bind_backend": _summarize(bind_backend_latencies_ns),
                "lower_backend": _summarize(lower_backend_latencies_ns),
                "render_program": _summarize(render_program_latencies_ns),
                "end_to_end": _summarize(end_to_end_latencies_ns),
            }
        )

    _progress(f"compiler benchmark: backend {backend.value} complete")

    return {
        "backend": backend.value,
        "iterations": iterations,
        "warmup": warmup,
        "query_count": len(applicable_queries),
        "overall": {
            "build_ir": _summarize(build_ir_all_latencies_ns),
            "bind_backend": _summarize(bind_backend_all_latencies_ns),
            "lower_backend": _summarize(lower_backend_all_latencies_ns),
            "render_program": _summarize(render_program_all_latencies_ns),
            "end_to_end": _summarize(end_to_end_all_latencies_ns),
        },
        "queries": per_query,
    }


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Benchmark CypherGlot compiler entrypoints and comparable SQLGlot "
            "PostgreSQL-to-SQLite SQL workloads."
        )
    )
    parser.add_argument(
        "--corpus",
        type=Path,
        default=DEFAULT_CORPUS_PATH,
        help="Path to the benchmark corpus JSON file.",
    )
    parser.add_argument(
        "--sql-corpus",
        type=Path,
        default=DEFAULT_SQLGLOT_CORPUS_PATH,
        help="Path to the SQLGlot benchmark corpus JSON file.",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=DEFAULT_OUTPUT_PATH,
        help="Path to the JSON file where baseline results will be written.",
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
        "--sqlglot-mode",
        choices=("installed", "python", "both", "off"),
        default="both",
        help=(
            "Which SQLGlot implementation(s) to benchmark: the currently installed "
            "package layout, a pure-Python fallback copy, both, or none."
        ),
    )
    parser.add_argument(
        "--_sqlglot-subprocess-output",
        dest="sqlglot_subprocess_output_internal",
        type=Path,
        default=None,
        help=argparse.SUPPRESS,
    )
    parser.add_argument(
        "--_sqlglot-subprocess-mode",
        dest="sqlglot_subprocess_mode_internal",
        choices=("python",),
        default=None,
        help=argparse.SUPPRESS,
    )
    return parser.parse_args()


def _import_cypherglot():
    return importlib.import_module("cypherglot")


def _load_corpus(path: Path) -> list[CorpusQuery]:
    raw = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(raw, list) or not raw:
        raise ValueError("Benchmark corpus must be a non-empty JSON list.")

    queries: list[CorpusQuery] = []
    for index, item in enumerate(raw, start=1):
        if not isinstance(item, dict):
            raise ValueError(f"Corpus item {index} must be a JSON object.")
        try:
            name = item["name"]
            category = item["category"]
            entrypoints = item["entrypoints"]
            query = item["query"]
        except KeyError as exc:
            raise ValueError(
                f"Corpus item {index} is missing required key {exc.args[0]!r}."
            ) from exc
        if not isinstance(name, str) or not name:
            raise ValueError(f"Corpus item {index} has invalid 'name'.")
        if not isinstance(category, str) or not category:
            raise ValueError(f"Corpus item {index} has invalid 'category'.")
        if not isinstance(entrypoints, list) or not entrypoints:
            raise ValueError(f"Corpus item {index} has invalid 'entrypoints'.")
        normalized_entrypoints: list[str] = []
        for entrypoint in entrypoints:
            if not isinstance(entrypoint, str) or not entrypoint:
                raise ValueError(
                    f"Corpus item {index} has invalid entrypoint value {entrypoint!r}."
                )
            if entrypoint not in CYPHERGLOT_ENTRYPOINT_ATTRS:
                raise ValueError(
                    f"Corpus item {index} references unknown entrypoint {entrypoint!r}."
                )
            normalized_entrypoints.append(entrypoint)
        if not isinstance(query, str) or not query:
            raise ValueError(f"Corpus item {index} has invalid 'query'.")
        queries.append(
            CorpusQuery(
                name=name,
                category=category,
                entrypoints=tuple(normalized_entrypoints),
                query=query,
            )
        )
    return queries


def _load_sql_corpus(path: Path) -> list[SqlCorpusQuery]:
    raw = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(raw, list) or not raw:
        raise ValueError("SQL benchmark corpus must be a non-empty JSON list.")

    queries: list[SqlCorpusQuery] = []
    for index, item in enumerate(raw, start=1):
        if not isinstance(item, dict):
            raise ValueError(f"SQL corpus item {index} must be a JSON object.")
        try:
            name = item["name"]
            category = item["category"]
            query = item["query"]
        except KeyError as exc:
            raise ValueError(
                f"SQL corpus item {index} is missing required key {exc.args[0]!r}."
            ) from exc
        if not isinstance(name, str) or not name:
            raise ValueError(f"SQL corpus item {index} has invalid 'name'.")
        if not isinstance(category, str) or not category:
            raise ValueError(f"SQL corpus item {index} has invalid 'category'.")
        if not isinstance(query, str) or not query:
            raise ValueError(f"SQL corpus item {index} has invalid 'query'.")
        queries.append(SqlCorpusQuery(name=name, category=category, query=query))
    return queries


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
        "mean_us": statistics.fmean(sorted_ns) / 1_000.0,
        "p50_us": _percentile(sorted_ns, 50) / 1_000.0,
        "p95_us": _percentile(sorted_ns, 95) / 1_000.0,
        "p99_us": _percentile(sorted_ns, 99) / 1_000.0,
        "max_us": max(sorted_ns) / 1_000.0,
    }


def _measure(
    query: str,
    func: Callable[[str], object],
    *,
    iterations: int,
    warmup: int,
) -> list[int]:
    for _ in range(warmup):
        func(query)

    latencies_ns: list[int] = []
    gc_was_enabled = gc.isenabled()
    gc.disable()
    try:
        for _ in range(iterations):
            started_ns = time.perf_counter_ns()
            func(query)
            finished_ns = time.perf_counter_ns()
            latencies_ns.append(finished_ns - started_ns)
    finally:
        if gc_was_enabled:
            gc.enable()
    return latencies_ns


def _purge_sqlglot_modules() -> None:
    names_to_remove = [
        name
        for name in sys.modules
        if name == "sqlglot" or name.startswith("sqlglot.") or name.endswith("__mypyc")
    ]
    for name in names_to_remove:
        sys.modules.pop(name, None)


def _snapshot_sqlglot_modules() -> dict[str, object]:
    return {
        name: module
        for name, module in sys.modules.items()
        if name == "sqlglot" or name.startswith("sqlglot.") or name.endswith("__mypyc")
    }


@contextlib.contextmanager
def _sqlglot_import_context(mode: str):
    if mode != "python":
        yield
        return

    saved_modules = _snapshot_sqlglot_modules()
    package_root = Path(
        importlib.metadata.distribution("sqlglot").locate_file("sqlglot")
    )
    with tempfile.TemporaryDirectory(prefix="sqlglot-pure-") as temp_dir:
        temp_root = Path(temp_dir)
        shutil.copytree(
            package_root,
            temp_root / "sqlglot",
            ignore=shutil.ignore_patterns(
                "*.so",
                "*.so.*",
                "*__mypyc*.so",
                "__pycache__",
            ),
        )
        _purge_sqlglot_modules()
        sys.path.insert(0, str(temp_root))
        try:
            yield
        finally:
            sys.path.pop(0)
            _purge_sqlglot_modules()
            sys.modules.update(saved_modules)


def _sqlglot_module_details(sqlglot_module) -> dict[str, str]:
    parser_module = importlib.import_module("sqlglot.parser")
    generator_module = importlib.import_module("sqlglot.generator")
    tokenizer_core_module = importlib.import_module("sqlglot.tokenizer_core")
    module_files = {
        "sqlglot_module": getattr(sqlglot_module, "__file__", ""),
        "parser_module": getattr(parser_module, "__file__", ""),
        "generator_module": getattr(generator_module, "__file__", ""),
        "tokenizer_core_module": getattr(tokenizer_core_module, "__file__", ""),
    }
    compiled = any(
        Path(path).suffix in {".so", ".pyd"} for path in module_files.values()
    )
    return {
        "implementation": "compiled" if compiled else "python",
        "version": sqlglot_module.__version__,
        **module_files,
    }


def _sqlglot_benchmark_funcs(sqlglot_module) -> dict[str, Callable[[str], object]]:
    def parse_then_render(query: str) -> str:
        return sqlglot_module.parse_one(query, read=SQLGLOT_READ_DIALECT).sql(
            dialect=SQLGLOT_WRITE_DIALECT
        )

    return {
        "tokenize": lambda query: sqlglot_module.tokenize(
            query,
            read=SQLGLOT_READ_DIALECT,
        ),
        "parse_one": lambda query: sqlglot_module.parse_one(
            query,
            read=SQLGLOT_READ_DIALECT,
        ),
        "parse_one_to_sql": parse_then_render,
        "transpile": lambda query: sqlglot_module.transpile(
            query,
            read=SQLGLOT_READ_DIALECT,
            write=SQLGLOT_WRITE_DIALECT,
        ),
    }


def _entrypoint_result(
    queries: list[CorpusQuery],
    *,
    label: str,
    iterations: int,
    warmup: int,
) -> dict[str, object]:
    cypherglot_module = _import_cypherglot()
    func = _benchmark_entrypoint_callable(cypherglot_module, label=label)
    per_query: list[dict[str, object]] = []
    all_latencies_ns: list[int] = []
    applicable_queries = [query for query in queries if label in query.entrypoints]
    if not applicable_queries:
        raise ValueError(f"No corpus queries apply to benchmark entrypoint {label!r}.")
    _progress(
        "compiler benchmark: entrypoint "
        f"{label} start ({len(applicable_queries)} queries, "
        f"iterations={iterations}, warmup={warmup})"
    )

    for query_index, corpus_query in enumerate(applicable_queries, start=1):
        _progress(
            "compiler benchmark: entrypoint "
            f"{label} query {query_index}/{len(applicable_queries)} "
            f"{corpus_query.name}"
        )
        latencies_ns = _measure(
            corpus_query.query,
            func,
            iterations=iterations,
            warmup=warmup,
        )
        all_latencies_ns.extend(latencies_ns)
        per_query.append(
            {
                "name": corpus_query.name,
                "category": corpus_query.category,
                "summary": _summarize(latencies_ns),
            }
        )

    _progress(f"compiler benchmark: entrypoint {label} complete")

    return {
        "entrypoint": label,
        "iterations": iterations,
        "warmup": warmup,
        "query_count": len(applicable_queries),
        "overall": _summarize(all_latencies_ns),
        "queries": per_query,
    }


def _sqlglot_result(
    queries: list[SqlCorpusQuery],
    *,
    label: str,
    func: Callable[[str], object],
    iterations: int,
    warmup: int,
) -> dict[str, object]:
    per_query: list[dict[str, object]] = []
    all_latencies_ns: list[int] = []

    _progress(
        "compiler benchmark: sqlglot "
        f"{label} start ({len(queries)} queries, iterations={iterations}, "
        f"warmup={warmup})"
    )
    for query_index, corpus_query in enumerate(queries, start=1):
        _progress(
            "compiler benchmark: sqlglot "
            f"{label} query {query_index}/{len(queries)} {corpus_query.name}"
        )
        latencies_ns = _measure(
            corpus_query.query,
            func,
            iterations=iterations,
            warmup=warmup,
        )
        all_latencies_ns.extend(latencies_ns)
        per_query.append(
            {
                "name": corpus_query.name,
                "category": corpus_query.category,
                "summary": _summarize(latencies_ns),
            }
        )

    _progress(f"compiler benchmark: sqlglot {label} complete")

    return {
        "method": label,
        "iterations": iterations,
        "warmup": warmup,
        "query_count": len(queries),
        "overall": _summarize(all_latencies_ns),
        "queries": per_query,
    }


def _sqlglot_suite_result(
    queries: list[SqlCorpusQuery],
    *,
    iterations: int,
    warmup: int,
    mode: str,
) -> dict[str, object]:
    _progress(f"compiler benchmark: sqlglot suite {mode} start")
    with _sqlglot_import_context(mode):
        sqlglot_module = importlib.import_module("sqlglot")
        details = _sqlglot_module_details(sqlglot_module)
        results = [
            _sqlglot_result(
                queries,
                label=label,
                func=func,
                iterations=iterations,
                warmup=warmup,
            )
            for label, func in _sqlglot_benchmark_funcs(sqlglot_module).items()
        ]
    _progress(f"compiler benchmark: sqlglot suite {mode} complete")

    return {
        "implementation": details["implementation"],
        "sqlglot_version": details["version"],
        "dialect_pair": {
            "read": SQLGLOT_READ_DIALECT,
            "write": SQLGLOT_WRITE_DIALECT,
        },
        "module_files": {
            "sqlglot": details["sqlglot_module"],
            "parser": details["parser_module"],
            "generator": details["generator_module"],
            "tokenizer_core": details["tokenizer_core_module"],
        },
        "query_count": len(queries),
        "results": results,
    }


def _run_sqlglot_subprocess(
    *,
    sql_corpus: Path,
    iterations: int,
    warmup: int,
    mode: str,
) -> dict[str, object]:
    with tempfile.TemporaryDirectory(prefix="sqlglot-benchmark-") as temp_dir:
        _progress(f"compiler benchmark: launching sqlglot subprocess {mode}")
        output_path = Path(temp_dir) / "sqlglot-result.json"
        subprocess.run(
            [
                sys.executable,
                str(Path(__file__).resolve()),
                "--sql-corpus",
                str(sql_corpus),
                "--iterations",
                str(iterations),
                "--warmup",
                str(warmup),
                "--_sqlglot-subprocess-output",
                str(output_path),
                "--_sqlglot-subprocess-mode",
                mode,
            ],
            check=True,
            cwd=REPO_ROOT,
        )
        _progress(f"compiler benchmark: sqlglot subprocess {mode} complete")
        return json.loads(output_path.read_text(encoding="utf-8"))


def _print_summary(result: dict[str, object]) -> None:
    overall = result["overall"]
    assert isinstance(overall, dict)
    print(
        f"{result['entrypoint']}: "
        f"p50={overall['p50_us']:.2f} us, "
        f"p95={overall['p95_us']:.2f} us, "
        f"p99={overall['p99_us']:.2f} us"
    )
    for query_result in result["queries"]:
        assert isinstance(query_result, dict)
        summary = query_result["summary"]
        assert isinstance(summary, dict)
        print(
            f"  - {query_result['name']} [{query_result['category']}]: "
            f"p50={summary['p50_us']:.2f} us, "
            f"p95={summary['p95_us']:.2f} us, "
            f"p99={summary['p99_us']:.2f} us"
        )


def _print_sqlglot_summary(suite: dict[str, object]) -> None:
    print(
        "sqlglot "
        f"[{suite['implementation']}] "
        f"{SQLGLOT_READ_DIALECT}->{SQLGLOT_WRITE_DIALECT}"
    )
    for result in suite["results"]:
        assert isinstance(result, dict)
        overall = result["overall"]
        assert isinstance(overall, dict)
        print(
            f"  - {result['method']}: "
            f"p50={overall['p50_us']:.2f} us, "
            f"p95={overall['p95_us']:.2f} us, "
            f"p99={overall['p99_us']:.2f} us"
        )


def main() -> int:
    args = _parse_args()
    if args.iterations <= 0:
        raise ValueError("--iterations must be positive.")
    if args.warmup < 0:
        raise ValueError("--warmup must be zero or positive.")

    if args.sqlglot_subprocess_output_internal is not None:
        sql_queries = _load_sql_corpus(args.sql_corpus)
        sqlglot_suite = _sqlglot_suite_result(
            sql_queries,
            iterations=args.iterations,
            warmup=args.warmup,
            mode=args.sqlglot_subprocess_mode_internal,
        )
        args.sqlglot_subprocess_output_internal.write_text(
            json.dumps(sqlglot_suite, indent=2) + "\n",
            encoding="utf-8",
        )
        return 0

    queries = _load_corpus(args.corpus)
    _progress(
        "compiler benchmark: starting main suite "
        f"({len(queries)} queries, iterations={args.iterations}, "
        f"warmup={args.warmup}, sqlglot_mode={args.sqlglot_mode})"
    )
    cypherglot_results = [
        _entrypoint_result(
            queries,
            label=entrypoint,
            iterations=args.iterations,
            warmup=args.warmup,
        )
        for entrypoint in DEFAULT_ENTRYPOINT_ORDER
        if any(entrypoint in query.entrypoints for query in queries)
    ]

    sql_queries = (
        _load_sql_corpus(args.sql_corpus) if args.sqlglot_mode != "off" else []
    )
    sqlglot_suites: list[dict[str, object]] = []
    if args.sqlglot_mode in {"installed", "both"}:
        sqlglot_suites.append(
            _sqlglot_suite_result(
                sql_queries,
                iterations=args.iterations,
                warmup=args.warmup,
                mode="installed",
            )
        )
    if args.sqlglot_mode in {"python", "both"}:
        sqlglot_suites.append(
            _run_sqlglot_subprocess(
                sql_corpus=args.sql_corpus,
                iterations=args.iterations,
                warmup=args.warmup,
                mode="python",
            )
        )

    cypherglot_module = _import_cypherglot()

    payload = {
        "generated_at": datetime.now(UTC).isoformat(),
        "python_version": platform.python_version(),
        "platform": platform.platform(),
        "cypherglot_version": cypherglot_module.__version__,
        "schema_contract": {
            "layout": "type-aware",
            "emitted_sql": "strict-relational",
            "release_subset": "admitted-v0.1.0",
        },
        "benchmark_schema": _benchmark_schema_metadata(
            _benchmark_schema_context(cypherglot_module)
        ),
        "corpus_path": str(args.corpus),
        "query_count": len(queries),
        "results": cypherglot_results,
        "backend_lowering_results": [
            _backend_lowering_result(
                queries,
                backend=backend,
                iterations=args.iterations,
                warmup=args.warmup,
            )
            for backend in SQLBackend
        ],
        "sqlglot_mode": args.sqlglot_mode,
        "sql_corpus_path": str(args.sql_corpus),
        "sql_query_count": len(sql_queries),
        "sqlglot_suites": sqlglot_suites,
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    _progress(f"compiler benchmark: wrote baseline to {args.output}")

    print(f"Wrote benchmark baseline to {args.output}")
    for result in cypherglot_results:
        _print_summary(result)
    for result in payload["backend_lowering_results"]:
        overall = result["overall"]
        assert isinstance(overall, dict)
        lower_backend = overall["lower_backend"]
        assert isinstance(lower_backend, dict)
        end_to_end = overall["end_to_end"]
        assert isinstance(end_to_end, dict)
        print(
            f"backend-lowering [{result['backend']}]: "
            f"p50={lower_backend['p50_us']:.2f} us, "
            f"p95={lower_backend['p95_us']:.2f} us, "
            f"p99={lower_backend['p99_us']:.2f} us"
        )
        print(
            f"backend-end-to-end [{result['backend']}]: "
            f"p50={end_to_end['p50_us']:.2f} us, "
            f"p95={end_to_end['p95_us']:.2f} us, "
            f"p99={end_to_end['p99_us']:.2f} us"
        )
    for suite in sqlglot_suites:
        _print_sqlglot_summary(suite)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
