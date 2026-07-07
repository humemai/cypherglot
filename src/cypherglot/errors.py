"""Structured compile-time diagnostics for CypherGlot.

``CypherCompileError`` subclasses ``ValueError`` so every existing caller
(and test) that catches ``ValueError`` keeps working; new code can catch the
subclass to read the structured fields.
"""

from __future__ import annotations

from typing import Any


class CypherCompileError(ValueError):
    """A Cypher query was rejected by the CypherGlot compiler.

    Attributes:
        message: The human-readable rejection reason (without location/hint).
        line: 1-based source line of the offending construct, when known.
        column: 0-based source column of the offending construct, when known.
        construct: Short identifier of the rejected construct (e.g.
            ``"pattern_comprehension"``), when the rejection maps to one.
        hint: A porting suggestion, when one exists.
    """

    def __init__(
        self,
        message: str,
        *,
        line: int | None = None,
        column: int | None = None,
        construct: str | None = None,
        hint: str | None = None,
    ) -> None:
        self.message = message
        self.line = line
        self.column = column
        self.construct = construct
        self.hint = hint

        rendered = message
        if line is not None:
            location = f"line {line}"
            if column is not None:
                location += f", column {column}"
            rendered = f"{rendered} ({location})"
        if hint is not None:
            rendered = f"{rendered} Hint: {hint}"
        super().__init__(rendered)


def reject(
    message: str,
    *,
    ctx: Any = None,
    construct: str | None = None,
    hint: str | None = None,
) -> None:
    """Raise ``CypherCompileError``, extracting the location from an ANTLR ctx.

    ``ctx`` may be any object with a ``start`` token carrying ``line`` and
    ``column`` attributes (every ANTLR parser rule context qualifies); pass
    None when no source location is available.
    """

    line = column = None
    start = getattr(ctx, "start", None)
    if start is not None:
        line = getattr(start, "line", None)
        column = getattr(start, "column", None)
    raise CypherCompileError(
        message,
        line=line,
        column=column,
        construct=construct,
        hint=hint,
    )
