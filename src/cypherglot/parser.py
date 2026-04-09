"""Parsing entrypoints for the CypherGlot frontend."""

from __future__ import annotations

from dataclasses import dataclass

from antlr4 import CommonTokenStream, InputStream
from antlr4.error.ErrorListener import ErrorListener

from .generated.CypherLexer import CypherLexer
from .generated.CypherParser import CypherParser


@dataclass(frozen=True, slots=True)
class CypherSyntaxError:
    """One syntax error reported by the generated Cypher frontend."""

    line: int
    column: int
    message: str


@dataclass(frozen=True, slots=True)
class CypherParseResult:
    """Raw parse result from the generated Cypher frontend."""

    source_text: str
    tree: object
    token_stream: object
    syntax_errors: tuple[CypherSyntaxError, ...]

    @property
    def has_errors(self) -> bool:
        """Return whether the parse encountered syntax errors."""

        return bool(self.syntax_errors)


class _CollectingErrorListener(ErrorListener):
    """Collect ANTLR syntax errors into a stable package-facing structure."""

    def __init__(self) -> None:
        self.errors: list[CypherSyntaxError] = []

    def syntaxError(
        self,
        _recognizer,
        _offending_symbol,
        line: int,
        column: int,
        msg: str,
        _exc,
    ) -> None:
        self.errors.append(
            CypherSyntaxError(
                line=line,
                column=column,
                message=msg,
            )
        )


def parse_cypher_text(text: str) -> CypherParseResult:
    """Parse one Cypher statement through the generated ANTLR frontend."""

    input_stream = InputStream(text)
    lexer = CypherLexer(input_stream)
    token_stream = CommonTokenStream(lexer)
    parser = CypherParser(token_stream)

    error_listener = _CollectingErrorListener()
    lexer.removeErrorListeners()
    parser.removeErrorListeners()
    lexer.addErrorListener(error_listener)
    parser.addErrorListener(error_listener)

    tree = parser.oC_Cypher()
    return CypherParseResult(
        source_text=text,
        tree=tree,
        token_stream=token_stream,
        syntax_errors=tuple(error_listener.errors),
    )
