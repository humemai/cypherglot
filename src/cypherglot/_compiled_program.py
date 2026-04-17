from __future__ import annotations

from dataclasses import dataclass

from sqlglot import exp, parse_one


@dataclass(frozen=True, slots=True)
class CompiledCypherStatement:
    sql: exp.Expression
    bind_columns: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class CompiledCypherLoop:
    source: exp.Expression
    row_bindings: tuple[str, ...]
    body: tuple[CompiledCypherStatement, ...]


CompiledCypherProgramStep = CompiledCypherStatement | CompiledCypherLoop


@dataclass(frozen=True, slots=True)
class CompiledCypherProgram:
    steps: tuple[CompiledCypherProgramStep, ...]


def _single_statement_program(sql: str) -> CompiledCypherProgram:
    return CompiledCypherProgram(steps=(CompiledCypherStatement(parse_one(sql)),))


def _require_single_statement_program(program: CompiledCypherProgram) -> exp.Expression:
    if len(program.steps) != 1:
        raise ValueError(
            "This Cypher shape compiles to a multi-step SQL program; use "
            "compile_cypher_program_text(...) instead."
        )

    step = program.steps[0]
    if not isinstance(step, CompiledCypherStatement) or step.bind_columns:
        raise ValueError(
            "This Cypher shape compiles to a multi-step SQL program; use "
            "compile_cypher_program_text(...) instead."
        )
    return step.sql