"""Validate the admitted CypherGlot subset against parsed structures."""

from __future__ import annotations

import re

from ._logging import get_logger
from ._normalize_support import (
    _parse_return_items,
    _split_return_clause,
)
from ._validate_projection import (
    _validate_plain_read_projection_shape as _validate_plain_read_projection_shape_impl,
)
from ._validate_shape_helpers import (
    _extract_single_match_source_nodes as _extract_single_match_source_nodes_impl,
    _validate_match_pattern_shape as _validate_match_pattern_shape_impl,
    _validate_optional_match_shape as _validate_optional_match_shape_impl,
    _validate_query_nodes_vector_shape as _validate_query_nodes_vector_shape_impl,
    _validate_traversal_write_endpoints as _validate_traversal_write_endpoints_impl,
    _validate_unwind_shape as _validate_unwind_shape_impl,
)
from ._validate_with_helpers import _validate_with_shape as _validate_with_shape_impl
from ._validate_write_helpers import (
    _validate_mixed_read_write_shape as _validate_mixed_read_write_shape_impl,
    _validate_standalone_write_shape as _validate_standalone_write_shape_impl,
)
from .parser import CypherParseResult, parse_cypher_text


logger = get_logger(__name__)


def _context_text(result: CypherParseResult, ctx: object) -> str:
    start_index = ctx.start.tokenIndex
    stop_index = ctx.stop.tokenIndex
    return result.token_stream.getText(start=start_index, stop=stop_index)


def _validate_match_pattern_shape(
    result: CypherParseResult,
    match_ctx,
    *,
    allow_two_node_disconnected: bool,
    allow_optional: bool = False,
    allow_multi_hop: bool = False,
    allow_variable_length: bool = False,
) -> None:
    _validate_match_pattern_shape_impl(
        result,
        match_ctx,
        allow_two_node_disconnected=allow_two_node_disconnected,
        allow_optional=allow_optional,
        allow_multi_hop=allow_multi_hop,
        allow_variable_length=allow_variable_length,
    )


def _extract_single_match_source_nodes(
    result: CypherParseResult,
    match_ctx,
) -> dict[str, tuple[str | None, tuple[tuple[str, object], ...]]]:
    return _extract_single_match_source_nodes_impl(result, match_ctx)


def _validate_traversal_write_endpoints(
    source_nodes: dict[str, tuple[str | None, tuple[tuple[str, object], ...]]],
    left: object,
    right: object,
    *,
    allow_one_new_endpoint: bool = False,
) -> None:
    _validate_traversal_write_endpoints_impl(
        source_nodes,
        left,
        right,
        allow_one_new_endpoint=allow_one_new_endpoint,
    )


def _validate_query_nodes_vector_shape(
    result: CypherParseResult,
    single_part_query_ctx,
) -> None:
    _validate_query_nodes_vector_shape_impl(result, single_part_query_ctx)


def _validate_standalone_write_shape(
    result: CypherParseResult,
    single_part_query_ctx,
    *,
    updating_clauses,
    reading_clauses,
    return_ctx,
) -> None:
    _validate_standalone_write_shape_impl(
        result,
        single_part_query_ctx,
        updating_clauses=updating_clauses,
        reading_clauses=reading_clauses,
        return_ctx=return_ctx,
    )


def _validate_mixed_read_write_shape(
    result: CypherParseResult,
    single_part_query_ctx,
    *,
    reading_clauses,
    updating_clauses,
    return_ctx,
) -> None:
    _validate_mixed_read_write_shape_impl(
        result,
        single_part_query_ctx,
        reading_clauses=reading_clauses,
        updating_clauses=updating_clauses,
        return_ctx=return_ctx,
    )

def _validate_with_shape(result: CypherParseResult, multi_part_query_ctx) -> None:
    _validate_with_shape_impl(result, multi_part_query_ctx)


def _validate_unwind_shape(result: CypherParseResult, single_part_query_ctx) -> None:
    _validate_unwind_shape_impl(result, single_part_query_ctx)


def _validate_optional_match_shape(result: CypherParseResult, single_part_query_ctx) -> None:
    _validate_optional_match_shape_impl(
        result,
        single_part_query_ctx,
        validate_plain_read_projection_shape=_validate_plain_read_projection_shape,
    )


def _validate_plain_read_projection_shape(
    projection_text: str,
    *,
    allowed_aliases: set[str],
    allowed_relationship_aliases: set[str] | None = None,
) -> None:
    _validate_plain_read_projection_shape_impl(
        projection_text,
        allowed_aliases=allowed_aliases,
        allowed_relationship_aliases=allowed_relationship_aliases,
    )

def validate_cypher_text(text: str):
    """Parse and validate one Cypher statement for the current frontend subset."""

    logger.debug("Validating Cypher text")
    try:
        validated = validate_cypher_parse_result(parse_cypher_text(text))
    except Exception:
        logger.debug("Validation failed", exc_info=True)
        raise
    logger.debug(
        "Validated Cypher text",
        extra={"context_kind": type(validated).__name__},
    )
    return validated


def validate_cypher_parse_result(result: CypherParseResult):
    """Validate that one parse result fits the current admitted subset."""

    logger.debug("Validating parsed Cypher result")

    if result.has_errors:
        first_error = result.syntax_errors[0]
        raise ValueError(
            "CypherGlot frontend reported syntax errors: "
            + (
                f"line {first_error.line}, column {first_error.column}: "
                f"{first_error.message}"
            )
        )

    statement_ctx = result.tree.oC_Statement()
    query_ctx = statement_ctx.oC_Query()
    regular_query_ctx = query_ctx.oC_RegularQuery()
    if regular_query_ctx is None:
        raise ValueError(
            "CypherGlot currently validates only regular CREATE, MATCH, and UNWIND queries."
        )

    single_query_ctx = regular_query_ctx.oC_SingleQuery()
    multi_part_query_ctx = single_query_ctx.oC_MultiPartQuery()
    if multi_part_query_ctx is not None:
        _validate_with_shape(result, multi_part_query_ctx)
        logger.debug("Validated parsed Cypher result", extra={"context_kind": type(multi_part_query_ctx).__name__})
        return multi_part_query_ctx

    single_part_query_ctx = single_query_ctx.oC_SinglePartQuery()
    if single_part_query_ctx is None:
        raise ValueError(
            "CypherGlot currently validates only single-part CREATE and MATCH "
            "queries."
        )

    updating_clauses = single_part_query_ctx.oC_UpdatingClause()
    reading_clauses = single_part_query_ctx.oC_ReadingClause()
    return_ctx = single_part_query_ctx.oC_Return()

    if reading_clauses and reading_clauses[0].oC_InQueryCall() is not None:
        _validate_query_nodes_vector_shape(result, single_part_query_ctx)
        logger.debug("Validated parsed Cypher result", extra={"context_kind": type(single_part_query_ctx).__name__})
        return single_part_query_ctx

    if reading_clauses and reading_clauses[0].oC_Match() is not None:
        match_ctx = reading_clauses[0].oC_Match()
        if match_ctx.OPTIONAL() is not None:
            _validate_optional_match_shape(result, single_part_query_ctx)
            return single_part_query_ctx

    if reading_clauses and reading_clauses[0].oC_Unwind() is not None:
        _validate_unwind_shape(result, single_part_query_ctx)
        return single_part_query_ctx

    if len(updating_clauses) > 1 or len(reading_clauses) > 1:
        raise ValueError(
            "CypherGlot currently validates only one CREATE, MATCH, or SET clause "
            "per statement."
        )

    if updating_clauses and not reading_clauses:
        _validate_standalone_write_shape(
            result,
            single_part_query_ctx,
            updating_clauses=updating_clauses,
            reading_clauses=reading_clauses,
            return_ctx=return_ctx,
        )
        return single_part_query_ctx

    if reading_clauses and not updating_clauses:
        read_ctx = reading_clauses[0]
        if read_ctx.oC_Match() is None or return_ctx is None:
            raise ValueError(
                "CypherGlot currently validates only MATCH ... RETURN statements "
                "in the read subset, plus narrow standalone OPTIONAL MATCH ... RETURN and UNWIND ... RETURN subsets."
            )
        _validate_match_pattern_shape(
            result,
            read_ctx.oC_Match(),
            allow_two_node_disconnected=False,
            allow_multi_hop=True,
            allow_variable_length=True,
        )
        pattern_text = _context_text(result, read_ctx.oC_Match().oC_Pattern())
        allowed_aliases = set(re.findall(r"\(([A-Za-z_][A-Za-z0-9_]*)", pattern_text))
        uses_variable_length = _match_uses_variable_length_relationship(
            result,
            read_ctx.oC_Match(),
        )
        relationship_aliases: set[str] = set()
        if ("-[" in pattern_text or "<-[" in pattern_text) and not uses_variable_length:
            for alias in re.findall(r"\[\s*([A-Za-z_][A-Za-z0-9_]*)", pattern_text):
                allowed_aliases.add(alias)
                relationship_aliases.add(alias)
        _validate_plain_read_projection_shape(
            _context_text(result, return_ctx.oC_ProjectionBody()),
            allowed_aliases=allowed_aliases,
            allowed_relationship_aliases=relationship_aliases,
        )
        if uses_variable_length:
            projection_text = _context_text(result, return_ctx.oC_ProjectionBody())
            return_text, _order, _limit, _distinct, _skip = _split_return_clause(
                projection_text
            )
            return_items = _parse_return_items(return_text)
            if any(
                item.kind in {"count", "sum", "avg", "min", "max"}
                for item in return_items
            ):
                if (
                    _supports_variable_length_grouped_aggregate_returns(
                        return_items,
                        allowed_aliases=allowed_aliases,
                    )
                ):
                    return single_part_query_ctx
                raise ValueError(
                    "CypherGlot currently admits variable-length relationship MATCH ... RETURN only with non-aggregate RETURN projections, or with grouped aggregate projections built from admitted endpoint return expressions plus count(*) / count(endpoint_alias) / aggregate(endpoint.field); relationship-type and endpoint introspection still require MATCH ... WITH ... RETURN or wider support."
                )
        return single_part_query_ctx

    if reading_clauses and updating_clauses:
        _validate_mixed_read_write_shape(
            result,
            single_part_query_ctx,
            reading_clauses=reading_clauses,
            updating_clauses=updating_clauses,
            return_ctx=return_ctx,
        )
        return single_part_query_ctx

    raise ValueError(
        "CypherGlot currently validates only CREATE, MATCH ... RETURN, narrow OPTIONAL MATCH ... RETURN, narrow UNWIND ... RETURN, MATCH ... "
        "SET, narrow MATCH ... CREATE, narrow MATCH ... MERGE, narrow MERGE, and narrow MATCH ... DELETE statements."
    )


def _match_uses_variable_length_relationship(
    result: CypherParseResult,
    match_ctx,
) -> bool:
    pattern_parts = match_ctx.oC_Pattern().oC_PatternPart()
    for pattern_part in pattern_parts:
        pattern_element = (
            pattern_part.oC_AnonymousPatternPart().oC_PatternElement()
        )
        for chain in pattern_element.oC_PatternElementChain():
            relationship_detail = (
                chain.oC_RelationshipPattern().oC_RelationshipDetail()
            )
            if (
                relationship_detail is not None
                and relationship_detail.oC_RangeLiteral() is not None
            ):
                return True
    return False


def _supports_variable_length_grouped_aggregate_returns(
    return_items: tuple[ReturnItem, ...],
    *,
    allowed_aliases: set[str],
) -> bool:
    for item in return_items:
        if item.kind in {"count", "sum", "avg", "min", "max"}:
            if item.kind == "count":
                if item.alias == "*":
                    continue
                if item.alias in allowed_aliases and item.field is None:
                    continue
                return False
            if item.alias in allowed_aliases and item.field is not None:
                continue
            return False

        if item.kind in {"type", "start_node", "end_node"}:
            return False

        if item.alias in allowed_aliases:
            continue

        return False

    return True
