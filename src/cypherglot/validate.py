"""Validate the admitted CypherGlot subset against parsed structures."""

from __future__ import annotations

import re

from ._logging import get_logger
from ._normalize_support import (
    _find_top_level_keyword,
    _looks_like_relationship_pattern,
    _parse_boolean_predicate_groups,
    _parse_case_expression,
    _parse_literal,
    _parse_node_pattern,
    _parse_query_nodes_limit_ref,
    _parse_query_nodes_order_items,
    _parse_query_nodes_return_items,
    _parse_return_items,
    _parse_relationship_chain_segment,
    _parse_relationship_pattern,
    _split_comma_separated,
    _split_predicate_comparison,
    _split_relationship_pattern,
    _split_return_clause,
    _split_query_nodes_return_and_order,
    _unwrap_node_pattern,
    _validate_match_create_relationship_between_nodes_endpoints,
    _validate_match_merge_relationship_endpoints,
)
from .parser import CypherParseResult, parse_cypher_text


logger = get_logger(__name__)


def _context_text(result: CypherParseResult, ctx: object) -> str:
    start_index = ctx.start.tokenIndex
    stop_index = ctx.stop.tokenIndex
    return result.token_stream.getText(start=start_index, stop=stop_index)


def _validate_plain_case_result_item(
    item,
    *,
    allowed_aliases: set[str],
    relationship_aliases: set[str],
) -> None:
    if item.kind == "field":
        if item.alias not in allowed_aliases:
            raise ValueError(
                "CypherGlot currently supports searched CASE results in the supported read subset only over admitted field projections, admitted id/type/size outputs, or scalar literal/parameter inputs."
            )
        return
    if item.kind == "id":
        if item.alias not in allowed_aliases:
            raise ValueError(
                "CypherGlot currently supports searched CASE results in the supported read subset only over admitted field projections, admitted id/type/size outputs, or scalar literal/parameter inputs."
            )
        return
    if item.kind == "type":
        if item.alias not in relationship_aliases:
            raise ValueError(
                "CypherGlot currently supports searched CASE results in the supported read subset only over admitted field projections, admitted id/type/size outputs, or scalar literal/parameter inputs."
            )
        return
    if item.kind == "size":
        if item.value is not None:
            return
        if item.alias not in allowed_aliases:
            raise ValueError(
                "CypherGlot currently supports searched CASE results in the supported read subset only over admitted field projections, admitted id/type/size outputs, or scalar literal/parameter inputs."
            )
        if item.field == "type" and item.alias not in relationship_aliases:
            raise ValueError(
                "CypherGlot currently supports searched CASE results in the supported read subset only over admitted field projections, admitted id/type/size outputs, or scalar literal/parameter inputs."
            )
        return
    if item.kind == "scalar":
        return
    raise ValueError(
        "CypherGlot currently supports searched CASE results in the supported read subset only over admitted field projections, admitted id/type/size outputs, or scalar literal/parameter inputs."
    )


def _validate_plain_case_expression(
    expression_text: str,
    *,
    output_alias: str | None,
    allowed_aliases: set[str],
    relationship_aliases: set[str],
) -> bool:
    case_spec = _parse_case_expression(expression_text)
    if case_spec is None:
        return False
    if output_alias is None:
        raise ValueError(
            "CypherGlot currently requires searched CASE in the supported read subset to use an explicit AS alias."
        )
    for arm in case_spec.when_items:
        if arm.condition.alias not in allowed_aliases:
            raise ValueError(
                "CypherGlot currently supports searched CASE WHEN conditions in the supported read subset only over admitted field, id(...), type(...), or size(...) predicate surfaces."
            )
        if arm.condition.field == "type" and arm.condition.alias not in relationship_aliases:
            raise ValueError(
                "CypherGlot currently supports searched CASE WHEN conditions in the supported read subset only over admitted field, id(...), type(...), or size(...) predicate surfaces."
            )
        if arm.condition.field is not None and arm.condition.field.startswith("__size__:"):
            inner_field = arm.condition.field.removeprefix("__size__:")
            if inner_field == "type" and arm.condition.alias not in relationship_aliases:
                raise ValueError(
                    "CypherGlot currently supports searched CASE WHEN conditions in the supported read subset only over admitted field, id(...), type(...), or size(...) predicate surfaces."
                )
        _validate_plain_case_result_item(
            arm.result,
            allowed_aliases=allowed_aliases,
            relationship_aliases=relationship_aliases,
        )
    _validate_plain_case_result_item(
        case_spec.else_item,
        allowed_aliases=allowed_aliases,
        relationship_aliases=relationship_aliases,
    )
    return True


def _parse_with_case_parts(expression_text: str) -> tuple[list[tuple[str, str]], str] | None:
    if re.match(r"case\b", expression_text, flags=re.IGNORECASE) is None:
        return None
    if re.fullmatch(r"case\s+.+\s+end", expression_text, flags=re.IGNORECASE) is None:
        raise ValueError(
            "CypherGlot currently supports searched CASE expressions only in the form CASE WHEN ... THEN ... [WHEN ... THEN ...]* ELSE ... END in the WITH subset."
        )
    inner = expression_text[4:-3].strip()
    if re.match(r"when\b", inner, flags=re.IGNORECASE) is None:
        raise ValueError(
            "CypherGlot currently supports only searched CASE expressions beginning with CASE WHEN ... in the WITH subset."
        )

    when_items: list[tuple[str, str]] = []
    cursor = 0
    while True:
        when_index = _find_top_level_keyword(inner, "WHEN", start=cursor)
        if when_index == -1:
            break
        then_index = _find_top_level_keyword(inner, "THEN", start=when_index + 4)
        if then_index == -1:
            raise ValueError(
                "CypherGlot searched CASE expressions require THEN after every WHEN condition in the WITH subset."
            )
        next_when = _find_top_level_keyword(inner, "WHEN", start=then_index + 4)
        else_index = _find_top_level_keyword(inner, "ELSE", start=then_index + 4)
        if else_index == -1 and next_when == -1:
            raise ValueError(
                "CypherGlot searched CASE expressions currently require an ELSE branch in the WITH subset."
            )
        condition_text = inner[when_index + 4:then_index].strip()
        if next_when != -1 and (else_index == -1 or next_when < else_index):
            result_text = inner[then_index + 4:next_when].strip()
            cursor = next_when
            else_text = None
        else:
            result_text = inner[then_index + 4:else_index].strip()
            else_text = inner[else_index + 4:].strip()
            cursor = len(inner)
        when_items.append((condition_text, result_text))
        if else_text is not None:
            if not else_text:
                raise ValueError(
                    "CypherGlot searched CASE expressions currently require a non-empty ELSE result in the WITH subset."
                )
            return when_items, else_text
    raise ValueError(
        "CypherGlot searched CASE expressions currently require at least one WHEN ... THEN ... arm and a final ELSE branch in the WITH subset."
    )


def _validate_with_case_result_text(
    text: str,
    *,
    binding_kinds: dict[str, str],
) -> None:
    field_match = re.fullmatch(
        r"(?P<alias>[A-Za-z_][A-Za-z0-9_]*)\.(?P<field>[A-Za-z_][A-Za-z0-9_]*)",
        text,
    )
    if field_match is not None:
        if binding_kinds.get(field_match.group("alias")) != "entity":
            raise ValueError(
                "CypherGlot currently supports searched CASE results in the WITH subset only over admitted entity-field projections, admitted scalar bindings, or scalar literal/parameter inputs."
            )
        return
    if binding_kinds.get(text) == "scalar":
        return
    try:
        _parse_literal(text)
    except ValueError as exc:
        raise ValueError(
            "CypherGlot currently supports searched CASE results in the WITH subset only over admitted entity-field projections, admitted scalar bindings, or scalar literal/parameter inputs."
        ) from exc


def _validate_with_case_condition_text(
    text: str,
    *,
    binding_kinds: dict[str, str],
) -> None:
    left_text, operator, value_text = _split_predicate_comparison(text.strip())
    left_text = left_text.strip()
    value_text = value_text.strip()
    if operator in {"IS NULL", "IS NOT NULL"}:
        if value_text:
            raise ValueError(
                "CypherGlot WITH searched CASE null predicates cannot include a trailing literal value."
            )
    else:
        _parse_literal(value_text)

    size_match = re.fullmatch(
        r"size\s*\(\s*(?P<expr>.+?)\s*\)",
        left_text,
        flags=re.IGNORECASE,
    )
    if size_match is not None:
        size_expr = size_match.group("expr").strip()
        field_match = re.fullmatch(
            r"(?P<alias>[A-Za-z_][A-Za-z0-9_]*)\.(?P<field>[A-Za-z_][A-Za-z0-9_]*)",
            size_expr,
        )
        if field_match is not None and binding_kinds.get(field_match.group("alias")) == "entity":
            return
        if binding_kinds.get(size_expr) == "scalar":
            return
        raise ValueError(
            "CypherGlot currently supports searched CASE WHEN conditions in the WITH subset only over admitted entity-field or scalar-binding predicate surfaces."
        )

    field_match = re.fullmatch(
        r"(?P<alias>[A-Za-z_][A-Za-z0-9_]*)\.(?P<field>[A-Za-z_][A-Za-z0-9_]*)",
        left_text,
    )
    if field_match is not None:
        if binding_kinds.get(field_match.group("alias")) != "entity":
            raise ValueError(
                "CypherGlot currently supports searched CASE WHEN conditions in the WITH subset only over admitted entity-field or scalar-binding predicate surfaces."
            )
        return
    if binding_kinds.get(left_text) == "scalar":
        return
    raise ValueError(
        "CypherGlot currently supports searched CASE WHEN conditions in the WITH subset only over admitted entity-field or scalar-binding predicate surfaces."
    )


def _validate_with_case_expression(
    expression_text: str,
    *,
    output_alias: str | None,
    binding_kinds: dict[str, str],
) -> bool:
    case_parts = _parse_with_case_parts(expression_text)
    if case_parts is None:
        return False
    if output_alias is None:
        raise ValueError(
            "CypherGlot currently requires searched CASE in the WITH subset to use an explicit AS alias."
        )
    when_items, else_text = case_parts
    for condition_text, result_text in when_items:
        _validate_with_case_condition_text(condition_text, binding_kinds=binding_kinds)
        _validate_with_case_result_text(result_text, binding_kinds=binding_kinds)
    _validate_with_case_result_text(else_text, binding_kinds=binding_kinds)
    return True


def _validate_match_pattern_shape(
    result: CypherParseResult,
    match_ctx,
    *,
    allow_two_node_disconnected: bool,
    allow_optional: bool = False,
    allow_multi_hop: bool = False,
    allow_variable_length: bool = False,
) -> None:
    if match_ctx.OPTIONAL() is not None and not allow_optional:
        raise ValueError(
            "CypherGlot currently does not admit OPTIONAL MATCH in the supported subset."
        )

    pattern_ctx = match_ctx.oC_Pattern()
    pattern_parts = pattern_ctx.oC_PatternPart()
    chain_counts: list[int] = []

    for pattern_part in pattern_parts:
        if pattern_part.oC_Variable() is not None:
            raise ValueError(
                "CypherGlot currently does not admit named path patterns in the "
                "supported subset."
            )

        anonymous_part = pattern_part.oC_AnonymousPatternPart()
        pattern_element = anonymous_part.oC_PatternElement()
        chains = pattern_element.oC_PatternElementChain()
        if len(chains) > 1 and not allow_multi_hop:
            raise ValueError(
                "CypherGlot currently does not admit multi-hop pattern chains in "
                "the supported subset."
            )

        for chain in chains:
            relationship_detail = chain.oC_RelationshipPattern().oC_RelationshipDetail()
            if (
                relationship_detail is not None
                and relationship_detail.oC_RangeLiteral() is not None
            ):
                if not allow_variable_length:
                    raise ValueError(
                        "CypherGlot currently does not admit variable-length "
                        "relationship patterns in the supported subset."
                    )

                if len(pattern_parts) != 1 or len(chains) != 1:
                    raise ValueError(
                        "CypherGlot currently admits bounded variable-length relationship patterns only as one directed MATCH relationship read."
                    )

                relationship = _parse_relationship_chain_segment(
                    _context_text(result, chain.oC_RelationshipPattern())
                )
                if relationship.min_hops < 0 or relationship.max_hops is None:
                    raise ValueError(
                        "CypherGlot currently admits only bounded non-negative-length variable-length relationship patterns in the supported subset."
                    )
                if relationship.max_hops < relationship.min_hops:
                    raise ValueError(
                        "CypherGlot currently admits only bounded non-negative-length variable-length relationship patterns in the supported subset."
                    )

        chain_counts.append(len(chains))

    if len(pattern_parts) <= 1:
        return

    if not allow_two_node_disconnected:
        raise ValueError(
            "CypherGlot currently does not admit disconnected multi-pattern MATCH "
            "clauses in the supported subset."
        )

    if len(pattern_parts) != 2 or any(chain_count != 0 for chain_count in chain_counts):
        raise ValueError(
            "CypherGlot currently admits at most two disconnected node patterns "
            "before narrow MATCH ... CREATE statements."
        )


def _extract_single_match_source_nodes(
    result: CypherParseResult,
    match_ctx,
) -> dict[str, tuple[str | None, tuple[tuple[str, object], ...]]]:
    pattern_element = (
        match_ctx.oC_Pattern().oC_PatternPart()[0].oC_AnonymousPatternPart().oC_PatternElement()
    )
    nodes = [
        _parse_node_pattern(
            _unwrap_node_pattern(_context_text(result, pattern_element.oC_NodePattern())),
            default_alias="__humem_validate_match_node_0",
        )
    ]
    for index, chain in enumerate(pattern_element.oC_PatternElementChain(), start=1):
        nodes.append(
            _parse_node_pattern(
                _unwrap_node_pattern(_context_text(result, chain.oC_NodePattern())),
                default_alias=f"__humem_validate_match_node_{index}",
            )
        )
    return {
        node.alias: (node.label, node.properties)
        for node in nodes
    }


def _validate_traversal_write_endpoints(
    source_nodes: dict[str, tuple[str | None, tuple[tuple[str, object], ...]]],
    left: object,
    right: object,
    *,
    allow_one_new_endpoint: bool = False,
) -> None:
    reused_count = 0
    new_count = 0
    for endpoint in (left, right):
        source_node = source_nodes.get(endpoint.alias)
        if source_node is None:
            new_count += 1
            if not allow_one_new_endpoint:
                raise ValueError(
                    "CypherGlot currently validates traversal-backed MATCH write clauses only when the relationship endpoints reuse matched node aliases exactly."
                )
            continue

        reused_count += 1
        source_label, _source_properties = source_node
        if endpoint.properties:
            raise ValueError(
                "CypherGlot currently validates traversal-backed MATCH write clauses only without inline properties on reused endpoint aliases."
            )
        if endpoint.label is not None and endpoint.label != source_label:
            raise ValueError(
                "CypherGlot currently validates traversal-backed MATCH write clauses only when reused endpoint labels match the matched source aliases."
            )

    if allow_one_new_endpoint and (reused_count == 0 or new_count > 1):
        raise ValueError(
            "CypherGlot currently validates traversal-backed MATCH ... CREATE only with exactly one reused matched node alias plus at most one fresh endpoint node."
        )


def _validate_query_nodes_vector_shape(
    result: CypherParseResult,
    single_part_query_ctx,
) -> None:
    updating_clauses = single_part_query_ctx.oC_UpdatingClause()
    reading_clauses = single_part_query_ctx.oC_ReadingClause()
    return_ctx = single_part_query_ctx.oC_Return()

    if updating_clauses or return_ctx is None:
        raise ValueError(
            "CypherGlot currently admits vector-aware CALL queries only in the "
            "narrow read subset."
        )

    if len(reading_clauses) not in {1, 2}:
        raise ValueError(
            "CypherGlot currently admits only CALL db.index.vector.queryNodes(...) "
            "YIELD node, score [MATCH ...] RETURN ... in the vector-aware subset."
        )

    call_ctx = reading_clauses[0].oC_InQueryCall()
    if call_ctx is None:
        raise ValueError(
            "CypherGlot currently admits only the db.index.vector.queryNodes(...) "
            "procedure in the vector-aware subset."
        )

    procedure_ctx = call_ctx.oC_ExplicitProcedureInvocation()
    if procedure_ctx is None:
        raise ValueError(
            "CypherGlot currently admits only explicit db.index.vector.queryNodes(...) "
            "procedure calls in the vector-aware subset."
        )

    procedure_name = _context_text(result, procedure_ctx.oC_ProcedureName()).casefold()
    if procedure_name != "db.index.vector.querynodes":
        raise ValueError(
            "CypherGlot currently admits only the db.index.vector.queryNodes(...) "
            "procedure in the vector-aware subset."
        )

    procedure_args = procedure_ctx.oC_Expression()
    if len(procedure_args) != 3:
        raise ValueError(
            "CypherGlot currently admits only db.index.vector.queryNodes(index, k, "
            "$query) in the vector-aware subset."
        )

    index_text = _context_text(result, procedure_args[0]).strip()
    if re.fullmatch(r"'([^'\\]|\\.)*'", index_text) is None:
        raise ValueError(
            "CypherGlot currently requires vector procedure index names to be "
            "single-quoted string literals."
        )

    _parse_query_nodes_limit_ref(_context_text(result, procedure_args[1]).strip())

    query_text = _context_text(result, procedure_args[2]).strip()
    if re.fullmatch(r"\$[A-Za-z_][A-Za-z0-9_]*", query_text) is None:
        raise ValueError(
            "CypherGlot currently requires vector procedure query embeddings to "
            "come from a named parameter."
        )

    yield_items = call_ctx.oC_YieldItems()
    if yield_items is None:
        raise ValueError(
            "CypherGlot currently requires vector procedure queries to include "
            "YIELD node, score."
        )

    yielded = tuple(
        _context_text(result, item_ctx).strip() for item_ctx in yield_items.oC_YieldItem()
    )
    if yielded != ("node", "score"):
        raise ValueError(
            "CypherGlot currently requires vector procedure queries to yield "
            "node, score in that order."
        )

    yield_where = yield_items.oC_Where()
    if yield_where is not None and len(reading_clauses) == 2:
        raise ValueError(
            "CypherGlot currently admits either YIELD ... WHERE ... or YIELD ... "
            "MATCH ..., but not both in the same vector-aware query."
        )

    if len(reading_clauses) == 2:
        match_ctx = reading_clauses[1].oC_Match()
        if match_ctx is None:
            raise ValueError(
                "CypherGlot currently admits only an optional MATCH clause after "
                "the vector procedure call."
            )
        _validate_match_pattern_shape(
            result,
            match_ctx,
            allow_two_node_disconnected=False,
        )

    projection_text = _context_text(result, return_ctx.oC_ProjectionBody())
    return_text, order_text = _split_query_nodes_return_and_order(projection_text)
    _parse_query_nodes_return_items(return_text)
    _parse_query_nodes_order_items(order_text)


def _validate_with_shape(result: CypherParseResult, multi_part_query_ctx) -> None:
    if len(multi_part_query_ctx.oC_With()) != 1:
        raise ValueError(
            "CypherGlot currently admits only one WITH clause in the supported "
            "multi-part subset."
        )

    reading_clauses = multi_part_query_ctx.oC_ReadingClause()
    updating_clauses = multi_part_query_ctx.oC_UpdatingClause()
    final_query = multi_part_query_ctx.oC_SinglePartQuery()

    if len(reading_clauses) != 1 or updating_clauses:
        raise ValueError(
            "CypherGlot currently admits only MATCH ... WITH ... RETURN in the "
            "supported multi-part subset."
        )

    match_ctx = reading_clauses[0].oC_Match()
    if match_ctx is None:
        raise ValueError(
            "CypherGlot currently admits only MATCH before WITH in the supported "
            "multi-part subset."
        )
    _validate_match_pattern_shape(
        result,
        match_ctx,
        allow_two_node_disconnected=False,
        allow_multi_hop=True,
        allow_variable_length=True,
    )

    if final_query.oC_ReadingClause() or final_query.oC_UpdatingClause() or final_query.oC_Return() is None:
        raise ValueError(
            "CypherGlot currently admits only MATCH ... WITH ... RETURN in the "
            "supported multi-part subset."
        )

    with_ctx = multi_part_query_ctx.oC_With()[0]
    projection_body = with_ctx.oC_ProjectionBody()
    if (
        projection_body.DISTINCT() is not None
        or projection_body.oC_Order() is not None
        or projection_body.oC_Skip() is not None
        or projection_body.oC_Limit() is not None
    ):
        raise ValueError(
            "CypherGlot currently admits only simple WITH passthrough items "
            "without DISTINCT, ORDER BY, SKIP/OFFSET, or LIMIT."
        )
    projection_items = projection_body.oC_ProjectionItems().oC_ProjectionItem()
    if not projection_items:
        raise ValueError(
            "CypherGlot currently requires at least one passthrough variable item "
            "in the supported WITH subset."
        )

    allowed_aliases: set[str]
    pattern_text = _context_text(result, match_ctx.oC_Pattern())
    uses_variable_length = _match_uses_variable_length_relationship(result, match_ctx)
    if "-[" in pattern_text or "<-[" in pattern_text:
        allowed_aliases = set()
        for alias in re.findall(r"\(([A-Za-z_][A-Za-z0-9_]*)", pattern_text):
            allowed_aliases.add(alias)
        if not uses_variable_length:
            for alias in re.findall(r"\[\s*([A-Za-z_][A-Za-z0-9_]*)", pattern_text):
                allowed_aliases.add(alias)
    else:
        allowed_aliases = set(re.findall(r"\(([A-Za-z_][A-Za-z0-9_]*)", pattern_text))

    source_alias_kinds: dict[str, str] = {}
    for alias in re.findall(r"\(([A-Za-z_][A-Za-z0-9_]*)", pattern_text):
        source_alias_kinds[alias] = "node"
    if not uses_variable_length:
        for alias in re.findall(r"\[\s*([A-Za-z_][A-Za-z0-9_]*)", pattern_text):
            source_alias_kinds[alias] = "relationship"

    binding_kinds: dict[str, str] = {}
    binding_alias_kinds: dict[str, str] = {}

    for item_ctx in projection_items:
        expression_text = _context_text(result, item_ctx.oC_Expression()).strip()
        output_alias = (
            _context_text(result, item_ctx.oC_Variable()).strip()
            if item_ctx.oC_Variable() is not None
            else expression_text
        )
        if expression_text in allowed_aliases:
            binding_kinds[output_alias] = "entity"
            binding_alias_kinds[output_alias] = source_alias_kinds[expression_text]
            continue
        field_match = re.fullmatch(
            r"(?P<alias>[A-Za-z_][A-Za-z0-9_]*)\.(?P<field>[A-Za-z_][A-Za-z0-9_]*)",
            expression_text,
        )
        if field_match is not None and field_match.group("alias") in allowed_aliases:
            if item_ctx.oC_Variable() is None:
                raise ValueError(
                    "CypherGlot currently requires WITH scalar rebinding to use an "
                    "explicit AS alias in the supported multi-part subset."
                )
            binding_kinds[output_alias] = "scalar"
            continue
        raise ValueError(
            "CypherGlot currently admits only passthrough variable items such as "
            "WITH u or WITH u AS person, plus simple scalar rebinding such as "
            "WITH u.name AS name, in the supported WITH subset."
        )

    if with_ctx.oC_Where() is not None:
        _validate_with_where_shape(
            _context_text(result, with_ctx.oC_Where().oC_Expression()),
            binding_kinds,
            binding_alias_kinds,
        )

    return_ctx = final_query.oC_Return()
    assert return_ctx is not None
    projection_text = _context_text(result, return_ctx.oC_ProjectionBody())
    return_text, order_items, _limit, _distinct, _skip = _split_return_clause(
        projection_text
    )

    return_items = [item.strip() for item in _split_comma_separated(return_text)]
    if not return_items:
        raise ValueError(
            "CypherGlot currently requires at least one RETURN item in the supported "
            "WITH subset."
        )

    aggregate_aliases: set[str] = set()
    seen_output_names: set[str] = set()
    projected_output_kinds: dict[str, str] = {}

    for item_text in return_items:
        alias_match = re.fullmatch(
            r"(?P<expr>.+?)\s+as\s+(?P<output>[A-Za-z_][A-Za-z0-9_]*)",
            item_text,
            flags=re.IGNORECASE,
        )
        output_alias = alias_match.group("output") if alias_match is not None else None
        expression_text = alias_match.group("expr").strip() if alias_match is not None else item_text

        if _validate_with_case_expression(
            expression_text,
            output_alias=output_alias,
            binding_kinds=binding_kinds,
        ):
            if output_alias in seen_output_names:
                raise ValueError(
                    f"CypherGlot currently does not allow duplicate RETURN output alias {output_alias!r} in the WITH subset."
                )
            seen_output_names.add(output_alias)
            projected_output_kinds[output_alias] = "scalar"
            continue

        count_match = re.fullmatch(
            r"count\s*\(\s*(?P<alias>\*|[A-Za-z_][A-Za-z0-9_]*)\s*\)",
            expression_text,
            flags=re.IGNORECASE,
        )
        if count_match is not None:
            if count_match.group("alias") != "*" and count_match.group("alias") not in binding_kinds:
                raise ValueError(
                    "CypherGlot currently requires count(...) in the WITH subset to target an admitted binding alias or *."
                )
            output_name = output_alias or expression_text
            if output_name in seen_output_names:
                raise ValueError(
                    f"CypherGlot currently does not allow duplicate RETURN output alias {output_name!r} in the WITH subset."
                )
            seen_output_names.add(output_name)
            aggregate_aliases.add(output_name)
            projected_output_kinds[output_name] = "aggregate"
            continue

        aggregate_match = re.fullmatch(
            r"(?P<func>sum|avg|min|max)\s*\(\s*(?P<alias>[A-Za-z_][A-Za-z0-9_]*)\s*\)",
            expression_text,
            flags=re.IGNORECASE,
        )
        if aggregate_match is not None:
            alias = aggregate_match.group("alias")
            if binding_kinds.get(alias) != "scalar":
                raise ValueError(
                    "CypherGlot currently supports sum(...), avg(...), min(...), and max(...) in the WITH subset only over admitted scalar bindings."
                )
            output_name = output_alias or expression_text
            if output_name in seen_output_names:
                raise ValueError(
                    f"CypherGlot currently does not allow duplicate RETURN output alias {output_name!r} in the WITH subset."
                )
            seen_output_names.add(output_name)
            aggregate_aliases.add(output_name)
            projected_output_kinds[output_name] = "aggregate"
            continue

        aggregate_field_match = re.fullmatch(
            (
                r"(?P<func>sum|avg|min|max)\s*\(\s*"
                r"(?P<alias>[A-Za-z_][A-Za-z0-9_]*)\."
                r"(?P<field>[A-Za-z_][A-Za-z0-9_]*)\s*\)"
            ),
            expression_text,
            flags=re.IGNORECASE,
        )
        if aggregate_field_match is not None:
            alias = aggregate_field_match.group("alias")
            if binding_kinds.get(alias) != "entity":
                raise ValueError(
                    "CypherGlot currently supports sum(...), avg(...), "
                    "min(...), and max(...) in the WITH subset over "
                    "entity-field inputs only when the alias is an admitted "
                    "entity binding."
                )
            output_name = output_alias or expression_text
            if output_name in seen_output_names:
                raise ValueError(
                    f"CypherGlot currently does not allow duplicate RETURN output alias {output_name!r} in the WITH subset."
                )
            seen_output_names.add(output_name)
            aggregate_aliases.add(output_name)
            projected_output_kinds[output_name] = "aggregate"
            continue

        id_match = re.fullmatch(
            r"id\s*\(\s*(?P<alias>[A-Za-z_][A-Za-z0-9_]*)\s*\)",
            expression_text,
            flags=re.IGNORECASE,
        )
        if id_match is not None:
            alias = id_match.group("alias")
            if binding_kinds.get(alias) != "entity":
                raise ValueError(
                    "CypherGlot currently supports id(...) in the WITH subset only over admitted entity bindings."
                )
            output_name = output_alias or expression_text
            if output_name in seen_output_names:
                raise ValueError(
                    f"CypherGlot currently does not allow duplicate RETURN output alias {output_name!r} in the WITH subset."
                )
            seen_output_names.add(output_name)
            projected_output_kinds[output_name] = "scalar"
            continue

        type_match = re.fullmatch(
            r"type\s*\(\s*(?P<alias>[A-Za-z_][A-Za-z0-9_]*)\s*\)",
            expression_text,
            flags=re.IGNORECASE,
        )
        if type_match is not None:
            alias = type_match.group("alias")
            if (
                binding_kinds.get(alias) != "entity"
                or binding_alias_kinds.get(alias) != "relationship"
            ):
                raise ValueError(
                    "CypherGlot currently supports type(...) in the WITH subset only over admitted relationship entity bindings."
                )
            output_name = output_alias or expression_text
            if output_name in seen_output_names:
                raise ValueError(
                    f"CypherGlot currently does not allow duplicate RETURN output alias {output_name!r} in the WITH subset."
                )
            seen_output_names.add(output_name)
            projected_output_kinds[output_name] = "scalar"
            continue

        properties_match = re.fullmatch(
            r"properties\s*\(\s*(?P<alias>[A-Za-z_][A-Za-z0-9_]*)\s*\)",
            expression_text,
            flags=re.IGNORECASE,
        )
        if properties_match is not None:
            alias = properties_match.group("alias")
            if binding_kinds.get(alias) != "entity":
                raise ValueError(
                    "CypherGlot currently supports properties(...) in the WITH subset only over admitted entity bindings."
                )
            output_name = output_alias or expression_text
            if output_name in seen_output_names:
                raise ValueError(
                    f"CypherGlot currently does not allow duplicate RETURN output alias {output_name!r} in the WITH subset."
                )
            seen_output_names.add(output_name)
            projected_output_kinds[output_name] = "scalar"
            continue

        labels_match = re.fullmatch(
            r"labels\s*\(\s*(?P<alias>[A-Za-z_][A-Za-z0-9_]*)\s*\)",
            expression_text,
            flags=re.IGNORECASE,
        )
        if labels_match is not None:
            alias = labels_match.group("alias")
            if (
                binding_kinds.get(alias) != "entity"
                or binding_alias_kinds.get(alias) != "node"
            ):
                raise ValueError(
                    "CypherGlot currently supports labels(...) in the WITH subset only over admitted node entity bindings."
                )
            output_name = output_alias or expression_text
            if output_name in seen_output_names:
                raise ValueError(
                    f"CypherGlot currently does not allow duplicate RETURN output alias {output_name!r} in the WITH subset."
                )
            seen_output_names.add(output_name)
            projected_output_kinds[output_name] = "scalar"
            continue

        keys_match = re.fullmatch(
            r"keys\s*\(\s*(?P<alias>[A-Za-z_][A-Za-z0-9_]*)\s*\)",
            expression_text,
            flags=re.IGNORECASE,
        )
        if keys_match is not None:
            alias = keys_match.group("alias")
            if binding_kinds.get(alias) != "entity":
                raise ValueError(
                    "CypherGlot currently supports keys(...) in the WITH subset only over admitted entity bindings."
                )
            output_name = output_alias or expression_text
            if output_name in seen_output_names:
                raise ValueError(
                    f"CypherGlot currently does not allow duplicate RETURN output alias {output_name!r} in the WITH subset."
                )
            seen_output_names.add(output_name)
            projected_output_kinds[output_name] = "scalar"
            continue

        start_node_match = re.fullmatch(
            r"startNode\s*\(\s*(?P<alias>[A-Za-z_][A-Za-z0-9_]*)\s*\)(?:\.(?P<field>[A-Za-z_][A-Za-z0-9_]*))?",
            expression_text,
            flags=re.IGNORECASE,
        )
        if start_node_match is not None:
            alias = start_node_match.group("alias")
            if (
                binding_kinds.get(alias) != "entity"
                or binding_alias_kinds.get(alias) != "relationship"
            ):
                raise ValueError(
                    "CypherGlot currently supports startNode(...) in the WITH subset only over admitted relationship entity bindings."
                )
            output_name = output_alias or expression_text
            if output_name in seen_output_names:
                raise ValueError(
                    f"CypherGlot currently does not allow duplicate RETURN output alias {output_name!r} in the WITH subset."
                )
            seen_output_names.add(output_name)
            projected_output_kinds[output_name] = (
                "scalar" if start_node_match.group("field") is not None else "entity"
            )
            continue

        end_node_match = re.fullmatch(
            r"endNode\s*\(\s*(?P<alias>[A-Za-z_][A-Za-z0-9_]*)\s*\)(?:\.(?P<field>[A-Za-z_][A-Za-z0-9_]*))?",
            expression_text,
            flags=re.IGNORECASE,
        )
        if end_node_match is not None:
            alias = end_node_match.group("alias")
            if (
                binding_kinds.get(alias) != "entity"
                or binding_alias_kinds.get(alias) != "relationship"
            ):
                raise ValueError(
                    "CypherGlot currently supports endNode(...) in the WITH subset only over admitted relationship entity bindings."
                )
            output_name = output_alias or expression_text
            if output_name in seen_output_names:
                raise ValueError(
                    f"CypherGlot currently does not allow duplicate RETURN output alias {output_name!r} in the WITH subset."
                )
            seen_output_names.add(output_name)
            projected_output_kinds[output_name] = (
                "scalar" if end_node_match.group("field") is not None else "entity"
            )
            continue

        if output_alias is None:
            size_match = re.fullmatch(
                r"size\s*\(\s*(?P<expr>.+?)\s*\)",
                expression_text,
                flags=re.IGNORECASE,
            )
            if size_match is not None:
                size_expr = size_match.group("expr").strip()
                if binding_kinds.get(size_expr) == "scalar":
                    if expression_text in seen_output_names:
                        raise ValueError(
                            f"CypherGlot currently does not allow duplicate RETURN output alias {expression_text!r} in the WITH subset."
                        )
                    seen_output_names.add(expression_text)
                    projected_output_kinds[expression_text] = "scalar"
                    continue
                id_match = re.fullmatch(
                    r"id\s*\(\s*(?P<alias>[A-Za-z_][A-Za-z0-9_]*)\s*\)",
                    size_expr,
                    flags=re.IGNORECASE,
                )
                if id_match is not None and binding_kinds.get(id_match.group("alias")) == "entity":
                    if expression_text in seen_output_names:
                        raise ValueError(
                            f"CypherGlot currently does not allow duplicate RETURN output alias {expression_text!r} in the WITH subset."
                        )
                    seen_output_names.add(expression_text)
                    projected_output_kinds[expression_text] = "scalar"
                    continue
                type_match = re.fullmatch(
                    r"type\s*\(\s*(?P<alias>[A-Za-z_][A-Za-z0-9_]*)\s*\)",
                    size_expr,
                    flags=re.IGNORECASE,
                )
                if type_match is not None:
                    alias = type_match.group("alias")
                    if (
                        binding_kinds.get(alias) == "entity"
                        and binding_alias_kinds.get(alias) == "relationship"
                    ):
                        if expression_text in seen_output_names:
                            raise ValueError(
                                f"CypherGlot currently does not allow duplicate RETURN output alias {expression_text!r} in the WITH subset."
                            )
                        seen_output_names.add(expression_text)
                        projected_output_kinds[expression_text] = "scalar"
                        continue
                field_match = re.fullmatch(
                    r"(?P<alias>[A-Za-z_][A-Za-z0-9_]*)\.(?P<field>[A-Za-z_][A-Za-z0-9_]*)",
                    size_expr,
                )
                if field_match is not None and binding_kinds.get(field_match.group("alias")) == "entity":
                    if expression_text in seen_output_names:
                        raise ValueError(
                            f"CypherGlot currently does not allow duplicate RETURN output alias {expression_text!r} in the WITH subset."
                        )
                    seen_output_names.add(expression_text)
                    projected_output_kinds[expression_text] = "scalar"
                    continue

            unary_match = re.fullmatch(
                r"(?P<func>lower|upper|trim|ltrim|rtrim|reverse|abs|sign|round|ceil|floor|sqrt|exp|sin|cos|tan|asin|acos|atan|ln|log|log10|radians|degrees|tostring|tointeger|tofloat|toboolean)\s*\(\s*(?P<expr>.+?)\s*\)",
                expression_text,
                flags=re.IGNORECASE,
            )
            if unary_match is not None:
                function_expr = unary_match.group("expr").strip()
                if binding_kinds.get(function_expr) == "scalar":
                    if expression_text in seen_output_names:
                        raise ValueError(
                            f"CypherGlot currently does not allow duplicate RETURN output alias {expression_text!r} in the WITH subset."
                        )
                    seen_output_names.add(expression_text)
                    projected_output_kinds[expression_text] = "scalar"
                    continue
                field_match = re.fullmatch(
                    r"(?P<alias>[A-Za-z_][A-Za-z0-9_]*)\.(?P<field>[A-Za-z_][A-Za-z0-9_]*)",
                    function_expr,
                )
                if field_match is not None and binding_kinds.get(field_match.group("alias")) == "entity":
                    if expression_text in seen_output_names:
                        raise ValueError(
                            f"CypherGlot currently does not allow duplicate RETURN output alias {expression_text!r} in the WITH subset."
                        )
                    seen_output_names.add(expression_text)
                    projected_output_kinds[expression_text] = "scalar"
                    continue

        size_match = re.fullmatch(
            r"size\s*\(\s*(?P<expr>.+?)\s*\)",
            expression_text,
            flags=re.IGNORECASE,
        )
        if size_match is not None:
            if output_alias is None:
                raise ValueError(
                    "CypherGlot currently requires size(...) in the WITH subset to use an explicit AS alias."
                )
            size_expr = size_match.group("expr").strip()
            if binding_kinds.get(size_expr) == "scalar":
                if output_alias in seen_output_names:
                    raise ValueError(
                        f"CypherGlot currently does not allow duplicate RETURN output alias {output_alias!r} in the WITH subset."
                    )
                seen_output_names.add(output_alias)
                projected_output_kinds[output_alias] = "scalar"
                continue
            id_match = re.fullmatch(
                r"id\s*\(\s*(?P<alias>[A-Za-z_][A-Za-z0-9_]*)\s*\)",
                size_expr,
                flags=re.IGNORECASE,
            )
            if id_match is not None:
                if binding_kinds.get(id_match.group("alias")) != "entity":
                    raise ValueError(
                        "CypherGlot currently supports size(...) in the WITH subset only over admitted entity-field projections, admitted id/type outputs, scalar bindings, or scalar literal/parameter inputs."
                    )
                if output_alias in seen_output_names:
                    raise ValueError(
                        f"CypherGlot currently does not allow duplicate RETURN output alias {output_alias!r} in the WITH subset."
                    )
                seen_output_names.add(output_alias)
                projected_output_kinds[output_alias] = "scalar"
                continue
            type_match = re.fullmatch(
                r"type\s*\(\s*(?P<alias>[A-Za-z_][A-Za-z0-9_]*)\s*\)",
                size_expr,
                flags=re.IGNORECASE,
            )
            if type_match is not None:
                alias = type_match.group("alias")
                if (
                    binding_kinds.get(alias) != "entity"
                    or binding_alias_kinds.get(alias) != "relationship"
                ):
                    raise ValueError(
                        "CypherGlot currently supports size(...) in the WITH subset only over admitted entity-field projections, admitted id/type outputs, scalar bindings, or scalar literal/parameter inputs."
                    )
                if output_alias in seen_output_names:
                    raise ValueError(
                        f"CypherGlot currently does not allow duplicate RETURN output alias {output_alias!r} in the WITH subset."
                    )
                seen_output_names.add(output_alias)
                projected_output_kinds[output_alias] = "scalar"
                continue
            try:
                _parse_literal(size_expr)
            except ValueError:
                pass
            else:
                if output_alias in seen_output_names:
                    raise ValueError(
                        f"CypherGlot currently does not allow duplicate RETURN output alias {output_alias!r} in the WITH subset."
                    )
                seen_output_names.add(output_alias)
                projected_output_kinds[output_alias] = "scalar"
                continue
            field_match = re.fullmatch(
                r"(?P<alias>[A-Za-z_][A-Za-z0-9_]*)\.(?P<field>[A-Za-z_][A-Za-z0-9_]*)",
                size_expr,
            )
            if field_match is not None and binding_kinds.get(field_match.group("alias")) == "entity":
                if output_alias in seen_output_names:
                    raise ValueError(
                        f"CypherGlot currently does not allow duplicate RETURN output alias {output_alias!r} in the WITH subset."
                    )
                seen_output_names.add(output_alias)
                projected_output_kinds[output_alias] = "scalar"
                continue
            raise ValueError(
                "CypherGlot currently supports size(...) in the WITH subset only over admitted entity-field projections, admitted id/type outputs, scalar bindings, or scalar literal/parameter inputs."
            )

        unary_string_match = re.fullmatch(
            r"(?P<func>lower|upper|trim|ltrim|rtrim|reverse)\s*\(\s*(?P<expr>.+?)\s*\)",
            expression_text,
            flags=re.IGNORECASE,
        )
        if unary_string_match is not None:
            if output_alias is None:
                raise ValueError(
                "CypherGlot currently requires lower(...), upper(...), trim(...), ltrim(...), rtrim(...), and reverse(...) in the WITH subset to use an explicit AS alias."
                )
            function_expr = unary_string_match.group("expr").strip()
            if binding_kinds.get(function_expr) == "scalar":
                if output_alias in seen_output_names:
                    raise ValueError(
                        f"CypherGlot currently does not allow duplicate RETURN output alias {output_alias!r} in the WITH subset."
                    )
                seen_output_names.add(output_alias)
                projected_output_kinds[output_alias] = "scalar"
                continue
            try:
                _parse_literal(function_expr)
            except ValueError:
                pass
            else:
                if output_alias in seen_output_names:
                    raise ValueError(
                        f"CypherGlot currently does not allow duplicate RETURN output alias {output_alias!r} in the WITH subset."
                    )
                seen_output_names.add(output_alias)
                projected_output_kinds[output_alias] = "scalar"
                continue
            field_match = re.fullmatch(
                r"(?P<alias>[A-Za-z_][A-Za-z0-9_]*)\.(?P<field>[A-Za-z_][A-Za-z0-9_]*)",
                function_expr,
            )
            if field_match is None or binding_kinds.get(field_match.group("alias")) != "entity":
                raise ValueError(
                    "CypherGlot currently supports lower(...), upper(...), trim(...), ltrim(...), rtrim(...), and reverse(...) in the WITH subset only over admitted entity-field projections, scalar bindings, or scalar literal/parameter inputs."
                )
            if output_alias in seen_output_names:
                raise ValueError(
                    f"CypherGlot currently does not allow duplicate RETURN output alias {output_alias!r} in the WITH subset."
                )
            seen_output_names.add(output_alias)
            projected_output_kinds[output_alias] = "scalar"
            continue

        coalesce_match = re.fullmatch(
            r"coalesce\s*\(\s*(?P<args>.+)\s*\)",
            expression_text,
            flags=re.IGNORECASE,
        )
        if coalesce_match is not None:
            args = [part.strip() for part in _split_comma_separated(coalesce_match.group("args"))]
            if len(args) != 2:
                raise ValueError(
                    "CypherGlot currently requires coalesce(...) in the WITH subset to use exactly two arguments."
                )
            primary_expr, fallback_expr = args
            try:
                _parse_literal(fallback_expr)
            except ValueError as exc:
                raise ValueError(
                    "CypherGlot currently supports coalesce(...) in the WITH subset only as coalesce(entity_alias.field, literal_or_parameter) or coalesce(scalar_alias, literal_or_parameter)."
                ) from exc
            field_match = re.fullmatch(
                r"(?P<alias>[A-Za-z_][A-Za-z0-9_]*)\.(?P<field>[A-Za-z_][A-Za-z0-9_]*)",
                primary_expr,
            )
            if field_match is not None:
                if binding_kinds.get(field_match.group("alias")) != "entity":
                    raise ValueError(
                        "CypherGlot currently supports coalesce(...) in the WITH subset only as coalesce(entity_alias.field, literal_or_parameter) or coalesce(scalar_alias, literal_or_parameter)."
                    )
            elif binding_kinds.get(primary_expr) != "scalar":
                raise ValueError(
                    "CypherGlot currently supports coalesce(...) in the WITH subset only as coalesce(entity_alias.field, literal_or_parameter) or coalesce(scalar_alias, literal_or_parameter)."
                )
            output_name = output_alias or expression_text
            if output_name in seen_output_names:
                raise ValueError(
                    f"CypherGlot currently does not allow duplicate RETURN output alias {output_name!r} in the WITH subset."
                )
            seen_output_names.add(output_name)
            projected_output_kinds[output_name] = "scalar"
            continue

        replace_match = re.fullmatch(
            r"replace\s*\(\s*(?P<args>.+)\s*\)",
            expression_text,
            flags=re.IGNORECASE,
        )
        if replace_match is not None:
            args = [part.strip() for part in _split_comma_separated(replace_match.group("args"))]
            if len(args) != 3:
                raise ValueError(
                    "CypherGlot currently requires replace(...) in the WITH subset to use exactly three arguments."
                )
            primary_expr, search_expr, replace_expr = args
            try:
                _parse_literal(search_expr)
                _parse_literal(replace_expr)
            except ValueError as exc:
                raise ValueError(
                    "CypherGlot currently supports replace(...) in the WITH subset only as replace(admitted_input, literal_or_parameter, literal_or_parameter)."
                ) from exc
            field_match = re.fullmatch(
                r"(?P<alias>[A-Za-z_][A-Za-z0-9_]*)\.(?P<field>[A-Za-z_][A-Za-z0-9_]*)",
                primary_expr,
            )
            if field_match is not None:
                if binding_kinds.get(field_match.group("alias")) != "entity":
                    raise ValueError(
                        "CypherGlot currently supports replace(...) in the WITH subset only as replace(admitted_input, literal_or_parameter, literal_or_parameter)."
                    )
            elif binding_kinds.get(primary_expr) != "scalar":
                try:
                    _parse_literal(primary_expr)
                except ValueError as exc:
                    raise ValueError(
                        "CypherGlot currently supports replace(...) in the WITH subset only as replace(admitted_input, literal_or_parameter, literal_or_parameter)."
                    ) from exc
            output_name = output_alias or expression_text
            if output_name in seen_output_names:
                raise ValueError(
                    f"CypherGlot currently does not allow duplicate RETURN output alias {output_name!r} in the WITH subset."
                )
            seen_output_names.add(output_name)
            projected_output_kinds[output_name] = "scalar"
            continue

        left_right_match = re.fullmatch(
            r"(?P<func>left|right)\s*\(\s*(?P<args>.+)\s*\)",
            expression_text,
            flags=re.IGNORECASE,
        )
        if left_right_match is not None:
            args = [part.strip() for part in _split_comma_separated(left_right_match.group("args"))]
            if len(args) != 2:
                raise ValueError(
                    "CypherGlot currently requires left(...) and right(...) in the WITH subset to use exactly two arguments."
                )
            primary_expr, length_expr = args
            try:
                _parse_literal(length_expr)
            except ValueError as exc:
                raise ValueError(
                    "CypherGlot currently supports left(...) and right(...) in the WITH subset only as function(admitted_input, literal_or_parameter)."
                ) from exc
            field_match = re.fullmatch(
                r"(?P<alias>[A-Za-z_][A-Za-z0-9_]*)\.(?P<field>[A-Za-z_][A-Za-z0-9_]*)",
                primary_expr,
            )
            if field_match is not None:
                if binding_kinds.get(field_match.group("alias")) != "entity":
                    raise ValueError(
                        "CypherGlot currently supports left(...) and right(...) in the WITH subset only as function(admitted_input, literal_or_parameter)."
                    )
            elif binding_kinds.get(primary_expr) != "scalar":
                try:
                    _parse_literal(primary_expr)
                except ValueError as exc:
                    raise ValueError(
                        "CypherGlot currently supports left(...) and right(...) in the WITH subset only as function(admitted_input, literal_or_parameter)."
                    ) from exc
            output_name = output_alias or expression_text
            if output_name in seen_output_names:
                raise ValueError(
                    f"CypherGlot currently does not allow duplicate RETURN output alias {output_name!r} in the WITH subset."
                )
            seen_output_names.add(output_name)
            projected_output_kinds[output_name] = "scalar"
            continue

        split_match = re.fullmatch(
            r"split\s*\(\s*(?P<args>.+)\s*\)",
            expression_text,
            flags=re.IGNORECASE,
        )
        if split_match is not None:
            args = [part.strip() for part in _split_comma_separated(split_match.group("args"))]
            if len(args) != 2:
                raise ValueError(
                    "CypherGlot currently requires split(...) in the WITH subset to use exactly two arguments."
                )
            primary_expr, delimiter_expr = args
            try:
                _parse_literal(delimiter_expr)
            except ValueError as exc:
                raise ValueError(
                    "CypherGlot currently supports split(...) in the WITH subset only as split(admitted_input, literal_or_parameter)."
                ) from exc
            field_match = re.fullmatch(
                r"(?P<alias>[A-Za-z_][A-Za-z0-9_]*)\.(?P<field>[A-Za-z_][A-Za-z0-9_]*)",
                primary_expr,
            )
            if field_match is not None:
                if binding_kinds.get(field_match.group("alias")) != "entity":
                    raise ValueError(
                        "CypherGlot currently supports split(...) in the WITH subset only as split(admitted_input, literal_or_parameter)."
                    )
            elif binding_kinds.get(primary_expr) != "scalar":
                try:
                    _parse_literal(primary_expr)
                except ValueError as exc:
                    raise ValueError(
                        "CypherGlot currently supports split(...) in the WITH subset only as split(admitted_input, literal_or_parameter)."
                    ) from exc
            output_name = output_alias or expression_text
            if output_name in seen_output_names:
                raise ValueError(
                    f"CypherGlot currently does not allow duplicate RETURN output alias {output_name!r} in the WITH subset."
                )
            seen_output_names.add(output_name)
            projected_output_kinds[output_name] = "scalar"
            continue

        substring_match = re.fullmatch(
            r"substring\s*\(\s*(?P<args>.+)\s*\)",
            expression_text,
            flags=re.IGNORECASE,
        )
        if substring_match is not None:
            args = [part.strip() for part in _split_comma_separated(substring_match.group("args"))]
            if len(args) not in {2, 3}:
                raise ValueError(
                    "CypherGlot currently requires substring(...) in the WITH subset to use exactly two or three arguments."
                )
            primary_expr, start_expr = args[:2]
            try:
                _parse_literal(start_expr)
                if len(args) == 3:
                    _parse_literal(args[2])
            except ValueError as exc:
                raise ValueError(
                    "CypherGlot currently supports substring(...) in the WITH subset only as substring(admitted_input, literal_or_parameter) or substring(admitted_input, literal_or_parameter, literal_or_parameter)."
                ) from exc
            field_match = re.fullmatch(
                r"(?P<alias>[A-Za-z_][A-Za-z0-9_]*)\.(?P<field>[A-Za-z_][A-Za-z0-9_]*)",
                primary_expr,
            )
            if field_match is not None:
                if binding_kinds.get(field_match.group("alias")) != "entity":
                    raise ValueError(
                        "CypherGlot currently supports substring(...) in the WITH subset only as substring(admitted_input, literal_or_parameter) or substring(admitted_input, literal_or_parameter, literal_or_parameter)."
                    )
            elif binding_kinds.get(primary_expr) != "scalar":
                try:
                    _parse_literal(primary_expr)
                except ValueError as exc:
                    raise ValueError(
                        "CypherGlot currently supports substring(...) in the WITH subset only as substring(admitted_input, literal_or_parameter) or substring(admitted_input, literal_or_parameter, literal_or_parameter)."
                    ) from exc
            output_name = output_alias or expression_text
            if output_name in seen_output_names:
                raise ValueError(
                    f"CypherGlot currently does not allow duplicate RETURN output alias {output_name!r} in the WITH subset."
                )
            seen_output_names.add(output_name)
            projected_output_kinds[output_name] = "scalar"
            continue

        abs_match = re.fullmatch(
            r"abs\s*\(\s*(?P<expr>.+?)\s*\)",
            expression_text,
            flags=re.IGNORECASE,
        )
        if abs_match is not None:
            if output_alias is None:
                raise ValueError(
                    "CypherGlot currently requires abs(...) in the WITH subset to use an explicit AS alias."
                )
            abs_expr = abs_match.group("expr").strip()
            if binding_kinds.get(abs_expr) == "scalar":
                if output_alias in seen_output_names:
                    raise ValueError(
                        f"CypherGlot currently does not allow duplicate RETURN output alias {output_alias!r} in the WITH subset."
                    )
                seen_output_names.add(output_alias)
                projected_output_kinds[output_alias] = "scalar"
                continue
            try:
                _parse_literal(abs_expr)
            except ValueError:
                pass
            else:
                if output_alias in seen_output_names:
                    raise ValueError(
                        f"CypherGlot currently does not allow duplicate RETURN output alias {output_alias!r} in the WITH subset."
                    )
                seen_output_names.add(output_alias)
                projected_output_kinds[output_alias] = "scalar"
                continue
            field_match = re.fullmatch(
                r"(?P<alias>[A-Za-z_][A-Za-z0-9_]*)\.(?P<field>[A-Za-z_][A-Za-z0-9_]*)",
                abs_expr,
            )
            if field_match is None or binding_kinds.get(field_match.group("alias")) != "entity":
                raise ValueError(
                    "CypherGlot currently supports abs(...) in the WITH subset only over admitted entity-field projections, scalar bindings, or scalar literal/parameter inputs."
                )
            if output_alias in seen_output_names:
                raise ValueError(
                    f"CypherGlot currently does not allow duplicate RETURN output alias {output_alias!r} in the WITH subset."
                )
            seen_output_names.add(output_alias)
            projected_output_kinds[output_alias] = "scalar"
            continue

        sign_match = re.fullmatch(
            r"sign\s*\(\s*(?P<expr>.+?)\s*\)",
            expression_text,
            flags=re.IGNORECASE,
        )
        if sign_match is not None:
            if output_alias is None:
                raise ValueError(
                    "CypherGlot currently requires sign(...) in the WITH subset to use an explicit AS alias."
                )
            sign_expr = sign_match.group("expr").strip()
            if binding_kinds.get(sign_expr) == "scalar":
                if output_alias in seen_output_names:
                    raise ValueError(
                        f"CypherGlot currently does not allow duplicate RETURN output alias {output_alias!r} in the WITH subset."
                    )
                seen_output_names.add(output_alias)
                projected_output_kinds[output_alias] = "scalar"
                continue
            try:
                _parse_literal(sign_expr)
            except ValueError:
                pass
            else:
                if output_alias in seen_output_names:
                    raise ValueError(
                        f"CypherGlot currently does not allow duplicate RETURN output alias {output_alias!r} in the WITH subset."
                    )
                seen_output_names.add(output_alias)
                projected_output_kinds[output_alias] = "scalar"
                continue
            field_match = re.fullmatch(
                r"(?P<alias>[A-Za-z_][A-Za-z0-9_]*)\.(?P<field>[A-Za-z_][A-Za-z0-9_]*)",
                sign_expr,
            )
            if field_match is None or binding_kinds.get(field_match.group("alias")) != "entity":
                raise ValueError(
                    "CypherGlot currently supports sign(...) in the WITH subset only over admitted entity-field projections, scalar bindings, or scalar literal/parameter inputs."
                )
            if output_alias in seen_output_names:
                raise ValueError(
                    f"CypherGlot currently does not allow duplicate RETURN output alias {output_alias!r} in the WITH subset."
                )
            seen_output_names.add(output_alias)
            projected_output_kinds[output_alias] = "scalar"
            continue

        round_match = re.fullmatch(
            r"round\s*\(\s*(?P<expr>.+?)\s*\)",
            expression_text,
            flags=re.IGNORECASE,
        )
        if round_match is not None:
            if output_alias is None:
                raise ValueError(
                    "CypherGlot currently requires round(...) in the WITH subset to use an explicit AS alias."
                )
            round_expr = round_match.group("expr").strip()
            if binding_kinds.get(round_expr) == "scalar":
                if output_alias in seen_output_names:
                    raise ValueError(
                        f"CypherGlot currently does not allow duplicate RETURN output alias {output_alias!r} in the WITH subset."
                    )
                seen_output_names.add(output_alias)
                projected_output_kinds[output_alias] = "scalar"
                continue
            try:
                _parse_literal(round_expr)
            except ValueError:
                pass
            else:
                if output_alias in seen_output_names:
                    raise ValueError(
                        f"CypherGlot currently does not allow duplicate RETURN output alias {output_alias!r} in the WITH subset."
                    )
                seen_output_names.add(output_alias)
                projected_output_kinds[output_alias] = "scalar"
                continue
            field_match = re.fullmatch(
                r"(?P<alias>[A-Za-z_][A-Za-z0-9_]*)\.(?P<field>[A-Za-z_][A-Za-z0-9_]*)",
                round_expr,
            )
            if field_match is None or binding_kinds.get(field_match.group("alias")) != "entity":
                raise ValueError(
                    "CypherGlot currently supports round(...) in the WITH subset only over admitted entity-field projections, scalar bindings, or scalar literal/parameter inputs."
                )
            if output_alias in seen_output_names:
                raise ValueError(
                    f"CypherGlot currently does not allow duplicate RETURN output alias {output_alias!r} in the WITH subset."
                )
            seen_output_names.add(output_alias)
            projected_output_kinds[output_alias] = "scalar"
            continue

        floor_match = re.fullmatch(
            r"floor\s*\(\s*(?P<expr>.+?)\s*\)",
            expression_text,
            flags=re.IGNORECASE,
        )
        if floor_match is not None:
            if output_alias is None:
                raise ValueError(
                    "CypherGlot currently requires floor(...) in the WITH subset to use an explicit AS alias."
                )
            floor_expr = floor_match.group("expr").strip()
            if binding_kinds.get(floor_expr) == "scalar":
                if output_alias in seen_output_names:
                    raise ValueError(
                        f"CypherGlot currently does not allow duplicate RETURN output alias {output_alias!r} in the WITH subset."
                    )
                seen_output_names.add(output_alias)
                projected_output_kinds[output_alias] = "scalar"
                continue
            try:
                _parse_literal(floor_expr)
            except ValueError:
                pass
            else:
                if output_alias in seen_output_names:
                    raise ValueError(
                        f"CypherGlot currently does not allow duplicate RETURN output alias {output_alias!r} in the WITH subset."
                    )
                seen_output_names.add(output_alias)
                projected_output_kinds[output_alias] = "scalar"
                continue
            field_match = re.fullmatch(
                r"(?P<alias>[A-Za-z_][A-Za-z0-9_]*)\.(?P<field>[A-Za-z_][A-Za-z0-9_]*)",
                floor_expr,
            )
            if field_match is None or binding_kinds.get(field_match.group("alias")) != "entity":
                raise ValueError(
                    "CypherGlot currently supports floor(...) in the WITH subset only over admitted entity-field projections, scalar bindings, or scalar literal/parameter inputs."
                )
            if output_alias in seen_output_names:
                raise ValueError(
                    f"CypherGlot currently does not allow duplicate RETURN output alias {output_alias!r} in the WITH subset."
                )
            seen_output_names.add(output_alias)
            projected_output_kinds[output_alias] = "scalar"
            continue

        ceil_match = re.fullmatch(
            r"ceil\s*\(\s*(?P<expr>.+?)\s*\)",
            expression_text,
            flags=re.IGNORECASE,
        )
        if ceil_match is not None:
            if output_alias is None:
                raise ValueError(
                    "CypherGlot currently requires ceil(...) in the WITH subset to use an explicit AS alias."
                )
            ceil_expr = ceil_match.group("expr").strip()
            if binding_kinds.get(ceil_expr) == "scalar":
                if output_alias in seen_output_names:
                    raise ValueError(
                        f"CypherGlot currently does not allow duplicate RETURN output alias {output_alias!r} in the WITH subset."
                    )
                seen_output_names.add(output_alias)
                projected_output_kinds[output_alias] = "scalar"
                continue
            try:
                _parse_literal(ceil_expr)
            except ValueError:
                pass
            else:
                if output_alias in seen_output_names:
                    raise ValueError(
                        f"CypherGlot currently does not allow duplicate RETURN output alias {output_alias!r} in the WITH subset."
                    )
                seen_output_names.add(output_alias)
                projected_output_kinds[output_alias] = "scalar"
                continue
            field_match = re.fullmatch(
                r"(?P<alias>[A-Za-z_][A-Za-z0-9_]*)\.(?P<field>[A-Za-z_][A-Za-z0-9_]*)",
                ceil_expr,
            )
            if field_match is None or binding_kinds.get(field_match.group("alias")) != "entity":
                raise ValueError(
                    "CypherGlot currently supports ceil(...) in the WITH subset only over admitted entity-field projections, scalar bindings, or scalar literal/parameter inputs."
                )
            if output_alias in seen_output_names:
                raise ValueError(
                    f"CypherGlot currently does not allow duplicate RETURN output alias {output_alias!r} in the WITH subset."
                )
            seen_output_names.add(output_alias)
            projected_output_kinds[output_alias] = "scalar"
            continue

        sqrt_match = re.fullmatch(
            r"sqrt\s*\(\s*(?P<expr>.+?)\s*\)",
            expression_text,
            flags=re.IGNORECASE,
        )
        if sqrt_match is not None:
            if output_alias is None:
                raise ValueError(
                    "CypherGlot currently requires sqrt(...) in the WITH subset to use an explicit AS alias."
                )
            sqrt_expr = sqrt_match.group("expr").strip()
            if binding_kinds.get(sqrt_expr) == "scalar":
                if output_alias in seen_output_names:
                    raise ValueError(
                        f"CypherGlot currently does not allow duplicate RETURN output alias {output_alias!r} in the WITH subset."
                    )
                seen_output_names.add(output_alias)
                projected_output_kinds[output_alias] = "scalar"
                continue
            try:
                _parse_literal(sqrt_expr)
            except ValueError:
                pass
            else:
                if output_alias in seen_output_names:
                    raise ValueError(
                        f"CypherGlot currently does not allow duplicate RETURN output alias {output_alias!r} in the WITH subset."
                    )
                seen_output_names.add(output_alias)
                projected_output_kinds[output_alias] = "scalar"
                continue
            field_match = re.fullmatch(
                r"(?P<alias>[A-Za-z_][A-Za-z0-9_]*)\.(?P<field>[A-Za-z_][A-Za-z0-9_]*)",
                sqrt_expr,
            )
            if field_match is None or binding_kinds.get(field_match.group("alias")) != "entity":
                raise ValueError(
                    "CypherGlot currently supports sqrt(...) in the WITH subset only over admitted entity-field projections, scalar bindings, or scalar literal/parameter inputs."
                )
            if output_alias in seen_output_names:
                raise ValueError(
                    f"CypherGlot currently does not allow duplicate RETURN output alias {output_alias!r} in the WITH subset."
                )
            seen_output_names.add(output_alias)
            projected_output_kinds[output_alias] = "scalar"
            continue

        exp_match = re.fullmatch(
            r"exp\s*\(\s*(?P<expr>.+?)\s*\)",
            expression_text,
            flags=re.IGNORECASE,
        )
        if exp_match is not None:
            if output_alias is None:
                raise ValueError(
                    "CypherGlot currently requires exp(...) in the WITH subset to use an explicit AS alias."
                )
            exp_expr = exp_match.group("expr").strip()
            if binding_kinds.get(exp_expr) == "scalar":
                if output_alias in seen_output_names:
                    raise ValueError(
                        f"CypherGlot currently does not allow duplicate RETURN output alias {output_alias!r} in the WITH subset."
                    )
                seen_output_names.add(output_alias)
                projected_output_kinds[output_alias] = "scalar"
                continue
            try:
                _parse_literal(exp_expr)
            except ValueError:
                pass
            else:
                if output_alias in seen_output_names:
                    raise ValueError(
                        f"CypherGlot currently does not allow duplicate RETURN output alias {output_alias!r} in the WITH subset."
                    )
                seen_output_names.add(output_alias)
                projected_output_kinds[output_alias] = "scalar"
                continue
            field_match = re.fullmatch(
                r"(?P<alias>[A-Za-z_][A-Za-z0-9_]*)\.(?P<field>[A-Za-z_][A-Za-z0-9_]*)",
                exp_expr,
            )
            if field_match is None or binding_kinds.get(field_match.group("alias")) != "entity":
                raise ValueError(
                    "CypherGlot currently supports exp(...) in the WITH subset only over admitted entity-field projections, scalar bindings, or scalar literal/parameter inputs."
                )
            if output_alias in seen_output_names:
                raise ValueError(
                    f"CypherGlot currently does not allow duplicate RETURN output alias {output_alias!r} in the WITH subset."
                )
            seen_output_names.add(output_alias)
            projected_output_kinds[output_alias] = "scalar"
            continue

        sin_match = re.fullmatch(
            r"sin\s*\(\s*(?P<expr>.+?)\s*\)",
            expression_text,
            flags=re.IGNORECASE,
        )
        if sin_match is not None:
            if output_alias is None:
                raise ValueError(
                    "CypherGlot currently requires sin(...) in the WITH subset to use an explicit AS alias."
                )
            sin_expr = sin_match.group("expr").strip()
            if binding_kinds.get(sin_expr) == "scalar":
                if output_alias in seen_output_names:
                    raise ValueError(
                        f"CypherGlot currently does not allow duplicate RETURN output alias {output_alias!r} in the WITH subset."
                    )
                seen_output_names.add(output_alias)
                projected_output_kinds[output_alias] = "scalar"
                continue
            try:
                _parse_literal(sin_expr)
            except ValueError:
                pass
            else:
                if output_alias in seen_output_names:
                    raise ValueError(
                        f"CypherGlot currently does not allow duplicate RETURN output alias {output_alias!r} in the WITH subset."
                    )
                seen_output_names.add(output_alias)
                projected_output_kinds[output_alias] = "scalar"
                continue
            field_match = re.fullmatch(
                r"(?P<alias>[A-Za-z_][A-Za-z0-9_]*)\.(?P<field>[A-Za-z_][A-Za-z0-9_]*)",
                sin_expr,
            )
            if field_match is None or binding_kinds.get(field_match.group("alias")) != "entity":
                raise ValueError(
                    "CypherGlot currently supports sin(...) in the WITH subset only over admitted entity-field projections, scalar bindings, or scalar literal/parameter inputs."
                )
            if output_alias in seen_output_names:
                raise ValueError(
                    f"CypherGlot currently does not allow duplicate RETURN output alias {output_alias!r} in the WITH subset."
                )
            seen_output_names.add(output_alias)
            projected_output_kinds[output_alias] = "scalar"
            continue

        cos_match = re.fullmatch(
            r"cos\s*\(\s*(?P<expr>.+?)\s*\)",
            expression_text,
            flags=re.IGNORECASE,
        )
        if cos_match is not None:
            if output_alias is None:
                raise ValueError(
                    "CypherGlot currently requires cos(...) in the WITH subset to use an explicit AS alias."
                )
            cos_expr = cos_match.group("expr").strip()
            if binding_kinds.get(cos_expr) == "scalar":
                if output_alias in seen_output_names:
                    raise ValueError(
                        f"CypherGlot currently does not allow duplicate RETURN output alias {output_alias!r} in the WITH subset."
                    )
                seen_output_names.add(output_alias)
                projected_output_kinds[output_alias] = "scalar"
                continue
            try:
                _parse_literal(cos_expr)
            except ValueError:
                pass
            else:
                if output_alias in seen_output_names:
                    raise ValueError(
                        f"CypherGlot currently does not allow duplicate RETURN output alias {output_alias!r} in the WITH subset."
                    )
                seen_output_names.add(output_alias)
                projected_output_kinds[output_alias] = "scalar"
                continue
            field_match = re.fullmatch(
                r"(?P<alias>[A-Za-z_][A-Za-z0-9_]*)\.(?P<field>[A-Za-z_][A-Za-z0-9_]*)",
                cos_expr,
            )
            if field_match is None or binding_kinds.get(field_match.group("alias")) != "entity":
                raise ValueError(
                    "CypherGlot currently supports cos(...) in the WITH subset only over admitted entity-field projections, scalar bindings, or scalar literal/parameter inputs."
                )
            if output_alias in seen_output_names:
                raise ValueError(
                    f"CypherGlot currently does not allow duplicate RETURN output alias {output_alias!r} in the WITH subset."
                )
            seen_output_names.add(output_alias)
            projected_output_kinds[output_alias] = "scalar"
            continue

        tan_match = re.fullmatch(
            r"tan\s*\(\s*(?P<expr>.+?)\s*\)",
            expression_text,
            flags=re.IGNORECASE,
        )
        if tan_match is not None:
            if output_alias is None:
                raise ValueError(
                    "CypherGlot currently requires tan(...) in the WITH subset to use an explicit AS alias."
                )
            tan_expr = tan_match.group("expr").strip()
            if binding_kinds.get(tan_expr) == "scalar":
                if output_alias in seen_output_names:
                    raise ValueError(
                        f"CypherGlot currently does not allow duplicate RETURN output alias {output_alias!r} in the WITH subset."
                    )
                seen_output_names.add(output_alias)
                projected_output_kinds[output_alias] = "scalar"
                continue
            try:
                _parse_literal(tan_expr)
            except ValueError:
                pass
            else:
                if output_alias in seen_output_names:
                    raise ValueError(
                        f"CypherGlot currently does not allow duplicate RETURN output alias {output_alias!r} in the WITH subset."
                    )
                seen_output_names.add(output_alias)
                projected_output_kinds[output_alias] = "scalar"
                continue
            field_match = re.fullmatch(
                r"(?P<alias>[A-Za-z_][A-Za-z0-9_]*)\.(?P<field>[A-Za-z_][A-Za-z0-9_]*)",
                tan_expr,
            )
            if field_match is None or binding_kinds.get(field_match.group("alias")) != "entity":
                raise ValueError(
                    "CypherGlot currently supports tan(...) in the WITH subset only over admitted entity-field projections, scalar bindings, or scalar literal/parameter inputs."
                )
            if output_alias in seen_output_names:
                raise ValueError(
                    f"CypherGlot currently does not allow duplicate RETURN output alias {output_alias!r} in the WITH subset."
                )
            seen_output_names.add(output_alias)
            projected_output_kinds[output_alias] = "scalar"
            continue

        asin_match = re.fullmatch(
            r"asin\s*\(\s*(?P<expr>.+?)\s*\)",
            expression_text,
            flags=re.IGNORECASE,
        )
        if asin_match is not None:
            if output_alias is None:
                raise ValueError(
                    "CypherGlot currently requires asin(...) in the WITH subset to use an explicit AS alias."
                )
            asin_expr = asin_match.group("expr").strip()
            if binding_kinds.get(asin_expr) == "scalar":
                if output_alias in seen_output_names:
                    raise ValueError(
                        f"CypherGlot currently does not allow duplicate RETURN output alias {output_alias!r} in the WITH subset."
                    )
                seen_output_names.add(output_alias)
                projected_output_kinds[output_alias] = "scalar"
                continue
            try:
                _parse_literal(asin_expr)
            except ValueError:
                pass
            else:
                if output_alias in seen_output_names:
                    raise ValueError(
                        f"CypherGlot currently does not allow duplicate RETURN output alias {output_alias!r} in the WITH subset."
                    )
                seen_output_names.add(output_alias)
                projected_output_kinds[output_alias] = "scalar"
                continue
            field_match = re.fullmatch(
                r"(?P<alias>[A-Za-z_][A-Za-z0-9_]*)\.(?P<field>[A-Za-z_][A-Za-z0-9_]*)",
                asin_expr,
            )
            if field_match is None or binding_kinds.get(field_match.group("alias")) != "entity":
                raise ValueError(
                    "CypherGlot currently supports asin(...) in the WITH subset only over admitted entity-field projections, scalar bindings, or scalar literal/parameter inputs."
                )
            if output_alias in seen_output_names:
                raise ValueError(
                    f"CypherGlot currently does not allow duplicate RETURN output alias {output_alias!r} in the WITH subset."
                )
            seen_output_names.add(output_alias)
            projected_output_kinds[output_alias] = "scalar"
            continue

        acos_match = re.fullmatch(
            r"acos\s*\(\s*(?P<expr>.+?)\s*\)",
            expression_text,
            flags=re.IGNORECASE,
        )
        if acos_match is not None:
            if output_alias is None:
                raise ValueError(
                    "CypherGlot currently requires acos(...) in the WITH subset to use an explicit AS alias."
                )
            acos_expr = acos_match.group("expr").strip()
            if binding_kinds.get(acos_expr) == "scalar":
                if output_alias in seen_output_names:
                    raise ValueError(
                        f"CypherGlot currently does not allow duplicate RETURN output alias {output_alias!r} in the WITH subset."
                    )
                seen_output_names.add(output_alias)
                projected_output_kinds[output_alias] = "scalar"
                continue
            try:
                _parse_literal(acos_expr)
            except ValueError:
                pass
            else:
                if output_alias in seen_output_names:
                    raise ValueError(
                        f"CypherGlot currently does not allow duplicate RETURN output alias {output_alias!r} in the WITH subset."
                    )
                seen_output_names.add(output_alias)
                projected_output_kinds[output_alias] = "scalar"
                continue
            field_match = re.fullmatch(
                r"(?P<alias>[A-Za-z_][A-Za-z0-9_]*)\.(?P<field>[A-Za-z_][A-Za-z0-9_]*)",
                acos_expr,
            )
            if field_match is None or binding_kinds.get(field_match.group("alias")) != "entity":
                raise ValueError(
                    "CypherGlot currently supports acos(...) in the WITH subset only over admitted entity-field projections, scalar bindings, or scalar literal/parameter inputs."
                )
            if output_alias in seen_output_names:
                raise ValueError(
                    f"CypherGlot currently does not allow duplicate RETURN output alias {output_alias!r} in the WITH subset."
                )
            seen_output_names.add(output_alias)
            projected_output_kinds[output_alias] = "scalar"
            continue

        atan_match = re.fullmatch(
            r"atan\s*\(\s*(?P<expr>.+?)\s*\)",
            expression_text,
            flags=re.IGNORECASE,
        )
        if atan_match is not None:
            if output_alias is None:
                raise ValueError(
                    "CypherGlot currently requires atan(...) in the WITH subset to use an explicit AS alias."
                )
            atan_expr = atan_match.group("expr").strip()
            if binding_kinds.get(atan_expr) == "scalar":
                if output_alias in seen_output_names:
                    raise ValueError(
                        f"CypherGlot currently does not allow duplicate RETURN output alias {output_alias!r} in the WITH subset."
                    )
                seen_output_names.add(output_alias)
                projected_output_kinds[output_alias] = "scalar"
                continue
            try:
                _parse_literal(atan_expr)
            except ValueError:
                pass
            else:
                if output_alias in seen_output_names:
                    raise ValueError(
                        f"CypherGlot currently does not allow duplicate RETURN output alias {output_alias!r} in the WITH subset."
                    )
                seen_output_names.add(output_alias)
                projected_output_kinds[output_alias] = "scalar"
                continue
            field_match = re.fullmatch(
                r"(?P<alias>[A-Za-z_][A-Za-z0-9_]*)\.(?P<field>[A-Za-z_][A-Za-z0-9_]*)",
                atan_expr,
            )
            if field_match is None or binding_kinds.get(field_match.group("alias")) != "entity":
                raise ValueError(
                    "CypherGlot currently supports atan(...) in the WITH subset only over admitted entity-field projections, scalar bindings, or scalar literal/parameter inputs."
                )
            if output_alias in seen_output_names:
                raise ValueError(
                    f"CypherGlot currently does not allow duplicate RETURN output alias {output_alias!r} in the WITH subset."
                )
            seen_output_names.add(output_alias)
            projected_output_kinds[output_alias] = "scalar"
            continue

        ln_match = re.fullmatch(
            r"ln\s*\(\s*(?P<expr>.+?)\s*\)",
            expression_text,
            flags=re.IGNORECASE,
        )
        if ln_match is not None:
            if output_alias is None:
                raise ValueError(
                    "CypherGlot currently requires ln(...) in the WITH subset to use an explicit AS alias."
                )
            ln_expr = ln_match.group("expr").strip()
            if binding_kinds.get(ln_expr) == "scalar":
                if output_alias in seen_output_names:
                    raise ValueError(
                        f"CypherGlot currently does not allow duplicate RETURN output alias {output_alias!r} in the WITH subset."
                    )
                seen_output_names.add(output_alias)
                projected_output_kinds[output_alias] = "scalar"
                continue
            try:
                _parse_literal(ln_expr)
            except ValueError:
                pass
            else:
                if output_alias in seen_output_names:
                    raise ValueError(
                        f"CypherGlot currently does not allow duplicate RETURN output alias {output_alias!r} in the WITH subset."
                    )
                seen_output_names.add(output_alias)
                projected_output_kinds[output_alias] = "scalar"
                continue
            field_match = re.fullmatch(
                r"(?P<alias>[A-Za-z_][A-Za-z0-9_]*)\.(?P<field>[A-Za-z_][A-Za-z0-9_]*)",
                ln_expr,
            )
            if field_match is None or binding_kinds.get(field_match.group("alias")) != "entity":
                raise ValueError(
                    "CypherGlot currently supports ln(...) in the WITH subset only over admitted entity-field projections, scalar bindings, or scalar literal/parameter inputs."
                )
            if output_alias in seen_output_names:
                raise ValueError(
                    f"CypherGlot currently does not allow duplicate RETURN output alias {output_alias!r} in the WITH subset."
                )
            seen_output_names.add(output_alias)
            projected_output_kinds[output_alias] = "scalar"
            continue

        log_match = re.fullmatch(
            r"log\s*\(\s*(?P<expr>.+?)\s*\)",
            expression_text,
            flags=re.IGNORECASE,
        )
        if log_match is not None:
            if output_alias is None:
                raise ValueError(
                    "CypherGlot currently requires log(...) in the WITH subset to use an explicit AS alias."
                )
            log_expr = log_match.group("expr").strip()
            if binding_kinds.get(log_expr) == "scalar":
                if output_alias in seen_output_names:
                    raise ValueError(
                        f"CypherGlot currently does not allow duplicate RETURN output alias {output_alias!r} in the WITH subset."
                    )
                seen_output_names.add(output_alias)
                projected_output_kinds[output_alias] = "scalar"
                continue
            try:
                _parse_literal(log_expr)
            except ValueError:
                pass
            else:
                if output_alias in seen_output_names:
                    raise ValueError(
                        f"CypherGlot currently does not allow duplicate RETURN output alias {output_alias!r} in the WITH subset."
                    )
                seen_output_names.add(output_alias)
                projected_output_kinds[output_alias] = "scalar"
                continue
            field_match = re.fullmatch(
                r"(?P<alias>[A-Za-z_][A-Za-z0-9_]*)\.(?P<field>[A-Za-z_][A-Za-z0-9_]*)",
                log_expr,
            )
            if field_match is None or binding_kinds.get(field_match.group("alias")) != "entity":
                raise ValueError(
                    "CypherGlot currently supports log(...) in the WITH subset only over admitted entity-field projections, scalar bindings, or scalar literal/parameter inputs."
                )
            if output_alias in seen_output_names:
                raise ValueError(
                    f"CypherGlot currently does not allow duplicate RETURN output alias {output_alias!r} in the WITH subset."
                )
            seen_output_names.add(output_alias)
            projected_output_kinds[output_alias] = "scalar"
            continue

        radians_match = re.fullmatch(
            r"radians\s*\(\s*(?P<expr>.+?)\s*\)",
            expression_text,
            flags=re.IGNORECASE,
        )
        if radians_match is not None:
            if output_alias is None:
                raise ValueError(
                    "CypherGlot currently requires radians(...) in the WITH subset to use an explicit AS alias."
                )
            radians_expr = radians_match.group("expr").strip()
            if binding_kinds.get(radians_expr) == "scalar":
                if output_alias in seen_output_names:
                    raise ValueError(
                        f"CypherGlot currently does not allow duplicate RETURN output alias {output_alias!r} in the WITH subset."
                    )
                seen_output_names.add(output_alias)
                projected_output_kinds[output_alias] = "scalar"
                continue
            try:
                _parse_literal(radians_expr)
            except ValueError:
                pass
            else:
                if output_alias in seen_output_names:
                    raise ValueError(
                        f"CypherGlot currently does not allow duplicate RETURN output alias {output_alias!r} in the WITH subset."
                    )
                seen_output_names.add(output_alias)
                projected_output_kinds[output_alias] = "scalar"
                continue
            field_match = re.fullmatch(
                r"(?P<alias>[A-Za-z_][A-Za-z0-9_]*)\.(?P<field>[A-Za-z_][A-Za-z0-9_]*)",
                radians_expr,
            )
            if field_match is None or binding_kinds.get(field_match.group("alias")) != "entity":
                raise ValueError(
                    "CypherGlot currently supports radians(...) in the WITH subset only over admitted entity-field projections, scalar bindings, or scalar literal/parameter inputs."
                )
            if output_alias in seen_output_names:
                raise ValueError(
                    f"CypherGlot currently does not allow duplicate RETURN output alias {output_alias!r} in the WITH subset."
                )
            seen_output_names.add(output_alias)
            projected_output_kinds[output_alias] = "scalar"
            continue

        degrees_match = re.fullmatch(
            r"degrees\s*\(\s*(?P<expr>.+?)\s*\)",
            expression_text,
            flags=re.IGNORECASE,
        )
        if degrees_match is not None:
            if output_alias is None:
                raise ValueError(
                    "CypherGlot currently requires degrees(...) in the WITH subset to use an explicit AS alias."
                )
            degrees_expr = degrees_match.group("expr").strip()
            if binding_kinds.get(degrees_expr) == "scalar":
                if output_alias in seen_output_names:
                    raise ValueError(
                        f"CypherGlot currently does not allow duplicate RETURN output alias {output_alias!r} in the WITH subset."
                    )
                seen_output_names.add(output_alias)
                projected_output_kinds[output_alias] = "scalar"
                continue
            try:
                _parse_literal(degrees_expr)
            except ValueError:
                pass
            else:
                if output_alias in seen_output_names:
                    raise ValueError(
                        f"CypherGlot currently does not allow duplicate RETURN output alias {output_alias!r} in the WITH subset."
                    )
                seen_output_names.add(output_alias)
                projected_output_kinds[output_alias] = "scalar"
                continue
            field_match = re.fullmatch(
                r"(?P<alias>[A-Za-z_][A-Za-z0-9_]*)\.(?P<field>[A-Za-z_][A-Za-z0-9_]*)",
                degrees_expr,
            )
            if field_match is None or binding_kinds.get(field_match.group("alias")) != "entity":
                raise ValueError(
                    "CypherGlot currently supports degrees(...) in the WITH subset only over admitted entity-field projections, scalar bindings, or scalar literal/parameter inputs."
                )
            if output_alias in seen_output_names:
                raise ValueError(
                    f"CypherGlot currently does not allow duplicate RETURN output alias {output_alias!r} in the WITH subset."
                )
            seen_output_names.add(output_alias)
            projected_output_kinds[output_alias] = "scalar"
            continue

        log10_match = re.fullmatch(
            r"log10\s*\(\s*(?P<expr>.+?)\s*\)",
            expression_text,
            flags=re.IGNORECASE,
        )
        if log10_match is not None:
            if output_alias is None:
                raise ValueError(
                    "CypherGlot currently requires log10(...) in the WITH subset to use an explicit AS alias."
                )
            log10_expr = log10_match.group("expr").strip()
            if binding_kinds.get(log10_expr) == "scalar":
                if output_alias in seen_output_names:
                    raise ValueError(
                        f"CypherGlot currently does not allow duplicate RETURN output alias {output_alias!r} in the WITH subset."
                    )
                seen_output_names.add(output_alias)
                projected_output_kinds[output_alias] = "scalar"
                continue
            try:
                _parse_literal(log10_expr)
            except ValueError:
                pass
            else:
                if output_alias in seen_output_names:
                    raise ValueError(
                        f"CypherGlot currently does not allow duplicate RETURN output alias {output_alias!r} in the WITH subset."
                    )
                seen_output_names.add(output_alias)
                projected_output_kinds[output_alias] = "scalar"
                continue
            field_match = re.fullmatch(
                r"(?P<alias>[A-Za-z_][A-Za-z0-9_]*)\.(?P<field>[A-Za-z_][A-Za-z0-9_]*)",
                log10_expr,
            )
            if field_match is None or binding_kinds.get(field_match.group("alias")) != "entity":
                raise ValueError(
                    "CypherGlot currently supports log10(...) in the WITH subset only over admitted entity-field projections, scalar bindings, or scalar literal/parameter inputs."
                )
            if output_alias in seen_output_names:
                raise ValueError(
                    f"CypherGlot currently does not allow duplicate RETURN output alias {output_alias!r} in the WITH subset."
                )
            seen_output_names.add(output_alias)
            projected_output_kinds[output_alias] = "scalar"
            continue

        to_string_match = re.fullmatch(
            r"tostring\s*\(\s*(?P<expr>.+?)\s*\)",
            expression_text,
            flags=re.IGNORECASE,
        )
        if to_string_match is not None:
            if output_alias is None:
                raise ValueError(
                    "CypherGlot currently requires toString(...) in the WITH subset to use an explicit AS alias."
                )
            to_string_expr = to_string_match.group("expr").strip()
            if binding_kinds.get(to_string_expr) == "scalar":
                if output_alias in seen_output_names:
                    raise ValueError(
                        f"CypherGlot currently does not allow duplicate RETURN output alias {output_alias!r} in the WITH subset."
                    )
                seen_output_names.add(output_alias)
                projected_output_kinds[output_alias] = "scalar"
                continue
            try:
                _parse_literal(to_string_expr)
            except ValueError:
                pass
            else:
                if output_alias in seen_output_names:
                    raise ValueError(
                        f"CypherGlot currently does not allow duplicate RETURN output alias {output_alias!r} in the WITH subset."
                    )
                seen_output_names.add(output_alias)
                projected_output_kinds[output_alias] = "scalar"
                continue
            field_match = re.fullmatch(
                r"(?P<alias>[A-Za-z_][A-Za-z0-9_]*)\.(?P<field>[A-Za-z_][A-Za-z0-9_]*)",
                to_string_expr,
            )
            if field_match is None or binding_kinds.get(field_match.group("alias")) != "entity":
                raise ValueError(
                    "CypherGlot currently supports toString(...) in the WITH subset only over admitted entity-field projections, scalar bindings, or scalar literal/parameter inputs."
                )
            if output_alias in seen_output_names:
                raise ValueError(
                    f"CypherGlot currently does not allow duplicate RETURN output alias {output_alias!r} in the WITH subset."
                )
            seen_output_names.add(output_alias)
            projected_output_kinds[output_alias] = "scalar"
            continue

        to_integer_match = re.fullmatch(
            r"tointeger\s*\(\s*(?P<expr>.+?)\s*\)",
            expression_text,
            flags=re.IGNORECASE,
        )
        if to_integer_match is not None:
            if output_alias is None:
                raise ValueError(
                    "CypherGlot currently requires toInteger(...) in the WITH subset to use an explicit AS alias."
                )
            to_integer_expr = to_integer_match.group("expr").strip()
            if binding_kinds.get(to_integer_expr) == "scalar":
                if output_alias in seen_output_names:
                    raise ValueError(
                        f"CypherGlot currently does not allow duplicate RETURN output alias {output_alias!r} in the WITH subset."
                    )
                seen_output_names.add(output_alias)
                projected_output_kinds[output_alias] = "scalar"
                continue
            try:
                _parse_literal(to_integer_expr)
            except ValueError:
                pass
            else:
                if output_alias in seen_output_names:
                    raise ValueError(
                        f"CypherGlot currently does not allow duplicate RETURN output alias {output_alias!r} in the WITH subset."
                    )
                seen_output_names.add(output_alias)
                projected_output_kinds[output_alias] = "scalar"
                continue
            field_match = re.fullmatch(
                r"(?P<alias>[A-Za-z_][A-Za-z0-9_]*)\.(?P<field>[A-Za-z_][A-Za-z0-9_]*)",
                to_integer_expr,
            )
            if field_match is None or binding_kinds.get(field_match.group("alias")) != "entity":
                raise ValueError(
                    "CypherGlot currently supports toInteger(...) in the WITH subset only over admitted entity-field projections, scalar bindings, or scalar literal/parameter inputs."
                )
            if output_alias in seen_output_names:
                raise ValueError(
                    f"CypherGlot currently does not allow duplicate RETURN output alias {output_alias!r} in the WITH subset."
                )
            seen_output_names.add(output_alias)
            projected_output_kinds[output_alias] = "scalar"
            continue

        to_float_match = re.fullmatch(
            r"tofloat\s*\(\s*(?P<expr>.+?)\s*\)",
            expression_text,
            flags=re.IGNORECASE,
        )
        if to_float_match is not None:
            if output_alias is None:
                raise ValueError(
                    "CypherGlot currently requires toFloat(...) in the WITH subset to use an explicit AS alias."
                )
            to_float_expr = to_float_match.group("expr").strip()
            if binding_kinds.get(to_float_expr) == "scalar":
                if output_alias in seen_output_names:
                    raise ValueError(
                        f"CypherGlot currently does not allow duplicate RETURN output alias {output_alias!r} in the WITH subset."
                    )
                seen_output_names.add(output_alias)
                projected_output_kinds[output_alias] = "scalar"
                continue
            try:
                _parse_literal(to_float_expr)
            except ValueError:
                pass
            else:
                if output_alias in seen_output_names:
                    raise ValueError(
                        f"CypherGlot currently does not allow duplicate RETURN output alias {output_alias!r} in the WITH subset."
                    )
                seen_output_names.add(output_alias)
                projected_output_kinds[output_alias] = "scalar"
                continue
            field_match = re.fullmatch(
                r"(?P<alias>[A-Za-z_][A-Za-z0-9_]*)\.(?P<field>[A-Za-z_][A-Za-z0-9_]*)",
                to_float_expr,
            )
            if field_match is None or binding_kinds.get(field_match.group("alias")) != "entity":
                raise ValueError(
                    "CypherGlot currently supports toFloat(...) in the WITH subset only over admitted entity-field projections, scalar bindings, or scalar literal/parameter inputs."
                )
            if output_alias in seen_output_names:
                raise ValueError(
                    f"CypherGlot currently does not allow duplicate RETURN output alias {output_alias!r} in the WITH subset."
                )
            seen_output_names.add(output_alias)
            projected_output_kinds[output_alias] = "scalar"
            continue

        to_boolean_match = re.fullmatch(
            r"toboolean\s*\(\s*(?P<expr>.+?)\s*\)",
            expression_text,
            flags=re.IGNORECASE,
        )
        if to_boolean_match is not None:
            if output_alias is None:
                raise ValueError(
                    "CypherGlot currently requires toBoolean(...) in the WITH subset to use an explicit AS alias."
                )
            to_boolean_expr = to_boolean_match.group("expr").strip()
            if binding_kinds.get(to_boolean_expr) == "scalar":
                if output_alias in seen_output_names:
                    raise ValueError(
                        f"CypherGlot currently does not allow duplicate RETURN output alias {output_alias!r} in the WITH subset."
                    )
                seen_output_names.add(output_alias)
                projected_output_kinds[output_alias] = "scalar"
                continue
            try:
                _parse_literal(to_boolean_expr)
            except ValueError:
                pass
            else:
                if output_alias in seen_output_names:
                    raise ValueError(
                        f"CypherGlot currently does not allow duplicate RETURN output alias {output_alias!r} in the WITH subset."
                    )
                seen_output_names.add(output_alias)
                projected_output_kinds[output_alias] = "scalar"
                continue
            field_match = re.fullmatch(
                r"(?P<alias>[A-Za-z_][A-Za-z0-9_]*)\.(?P<field>[A-Za-z_][A-Za-z0-9_]*)",
                to_boolean_expr,
            )
            if field_match is None or binding_kinds.get(field_match.group("alias")) != "entity":
                raise ValueError(
                    "CypherGlot currently supports toBoolean(...) in the WITH subset only over admitted entity-field projections, scalar bindings, or scalar literal/parameter inputs."
                )
            if output_alias in seen_output_names:
                raise ValueError(
                    f"CypherGlot currently does not allow duplicate RETURN output alias {output_alias!r} in the WITH subset."
                )
            seen_output_names.add(output_alias)
            projected_output_kinds[output_alias] = "scalar"
            continue

        try:
            _parse_literal(expression_text)
        except ValueError:
            pass
        else:
            if output_alias is None:
                raise ValueError(
                    "CypherGlot currently requires scalar literal and parameter RETURN items in the WITH subset to use an explicit AS alias."
                )
            if output_alias in seen_output_names:
                raise ValueError(
                    f"CypherGlot currently does not allow duplicate RETURN output alias {output_alias!r} in the WITH subset."
                )
            seen_output_names.add(output_alias)
            projected_output_kinds[output_alias] = "scalar"
            continue

        try:
            left_text, operator, value_text = _split_predicate_comparison(expression_text)
        except ValueError:
            pass
        else:
            if output_alias is None:
                raise ValueError(
                    "CypherGlot currently requires predicate RETURN items in the WITH subset to use an explicit AS alias."
                )
            left_text = left_text.strip()
            if binding_kinds.get(left_text) == "scalar":
                if operator not in {"IS NULL", "IS NOT NULL"}:
                    try:
                        _parse_literal(value_text.strip())
                    except ValueError as exc:
                        raise ValueError(
                            "CypherGlot currently supports predicate RETURN items in the WITH subset only as scalar_alias OP literal_or_parameter or entity_alias.field OP literal_or_parameter."
                        ) from exc
                elif value_text.strip():
                    raise ValueError(
                        "CypherGlot WITH null predicate RETURN items cannot include a trailing literal value."
                    )
                if output_alias in seen_output_names:
                    raise ValueError(
                        f"CypherGlot currently does not allow duplicate RETURN output alias {output_alias!r} in the WITH subset."
                    )
                seen_output_names.add(output_alias)
                projected_output_kinds[output_alias] = "scalar"
                continue
            size_match = re.fullmatch(
                r"size\s*\(\s*(?P<expr>.+?)\s*\)",
                left_text,
                flags=re.IGNORECASE,
            )
            if size_match is not None:
                size_expr = size_match.group("expr").strip()
                if binding_kinds.get(size_expr) == "scalar":
                    if operator not in {"IS NULL", "IS NOT NULL"}:
                        try:
                            _parse_literal(value_text.strip())
                        except ValueError as exc:
                            raise ValueError(
                                "CypherGlot currently supports predicate RETURN items in the WITH subset only as scalar_alias OP literal_or_parameter, entity_alias.field OP literal_or_parameter, id(entity_alias) OP literal_or_parameter, type(rel_alias) OP literal_or_parameter, or size(admitted_input) OP literal_or_parameter."
                            ) from exc
                    elif value_text.strip():
                        raise ValueError(
                            "CypherGlot WITH null predicate RETURN items cannot include a trailing literal value."
                        )
                    if output_alias in seen_output_names:
                        raise ValueError(
                            f"CypherGlot currently does not allow duplicate RETURN output alias {output_alias!r} in the WITH subset."
                        )
                    seen_output_names.add(output_alias)
                    projected_output_kinds[output_alias] = "scalar"
                    continue
                id_match = re.fullmatch(
                    r"id\s*\(\s*(?P<alias>[A-Za-z_][A-Za-z0-9_]*)\s*\)",
                    size_expr,
                    flags=re.IGNORECASE,
                )
                if id_match is not None and binding_kinds.get(id_match.group("alias")) == "entity":
                    if operator not in {"IS NULL", "IS NOT NULL"}:
                        try:
                            _parse_literal(value_text.strip())
                        except ValueError as exc:
                            raise ValueError(
                                "CypherGlot currently supports predicate RETURN items in the WITH subset only as scalar_alias OP literal_or_parameter, entity_alias.field OP literal_or_parameter, id(entity_alias) OP literal_or_parameter, type(rel_alias) OP literal_or_parameter, or size(admitted_input) OP literal_or_parameter."
                            ) from exc
                    elif value_text.strip():
                        raise ValueError(
                            "CypherGlot WITH null predicate RETURN items cannot include a trailing literal value."
                        )
                    if output_alias in seen_output_names:
                        raise ValueError(
                            f"CypherGlot currently does not allow duplicate RETURN output alias {output_alias!r} in the WITH subset."
                        )
                    seen_output_names.add(output_alias)
                    projected_output_kinds[output_alias] = "scalar"
                    continue
                type_match = re.fullmatch(
                    r"type\s*\(\s*(?P<alias>[A-Za-z_][A-Za-z0-9_]*)\s*\)",
                    size_expr,
                    flags=re.IGNORECASE,
                )
                if type_match is not None:
                    alias = type_match.group("alias")
                    if binding_kinds.get(alias) == "entity" and binding_alias_kinds.get(alias) == "relationship":
                        if operator not in {"IS NULL", "IS NOT NULL"}:
                            try:
                                _parse_literal(value_text.strip())
                            except ValueError as exc:
                                raise ValueError(
                                    "CypherGlot currently supports predicate RETURN items in the WITH subset only as scalar_alias OP literal_or_parameter, entity_alias.field OP literal_or_parameter, id(entity_alias) OP literal_or_parameter, type(rel_alias) OP literal_or_parameter, or size(admitted_input) OP literal_or_parameter."
                                ) from exc
                        elif value_text.strip():
                            raise ValueError(
                                "CypherGlot WITH null predicate RETURN items cannot include a trailing literal value."
                            )
                        if output_alias in seen_output_names:
                            raise ValueError(
                                f"CypherGlot currently does not allow duplicate RETURN output alias {output_alias!r} in the WITH subset."
                            )
                        seen_output_names.add(output_alias)
                        projected_output_kinds[output_alias] = "scalar"
                        continue
                field_match = re.fullmatch(
                    r"(?P<alias>[A-Za-z_][A-Za-z0-9_]*)\.(?P<field>[A-Za-z_][A-Za-z0-9_]*)",
                    size_expr,
                )
                if field_match is not None and binding_kinds.get(field_match.group("alias")) == "entity":
                    if operator not in {"IS NULL", "IS NOT NULL"}:
                        try:
                            _parse_literal(value_text.strip())
                        except ValueError as exc:
                            raise ValueError(
                                "CypherGlot currently supports predicate RETURN items in the WITH subset only as scalar_alias OP literal_or_parameter, entity_alias.field OP literal_or_parameter, id(entity_alias) OP literal_or_parameter, type(rel_alias) OP literal_or_parameter, or size(admitted_input) OP literal_or_parameter."
                            ) from exc
                    elif value_text.strip():
                        raise ValueError(
                            "CypherGlot WITH null predicate RETURN items cannot include a trailing literal value."
                        )
                    if output_alias in seen_output_names:
                        raise ValueError(
                            f"CypherGlot currently does not allow duplicate RETURN output alias {output_alias!r} in the WITH subset."
                        )
                    seen_output_names.add(output_alias)
                    projected_output_kinds[output_alias] = "scalar"
                    continue
            id_match = re.fullmatch(
                r"id\s*\(\s*(?P<alias>[A-Za-z_][A-Za-z0-9_]*)\s*\)",
                left_text,
                flags=re.IGNORECASE,
            )
            if id_match is not None:
                if binding_kinds.get(id_match.group("alias")) != "entity":
                    raise ValueError(
                        "CypherGlot currently supports predicate RETURN items in the WITH subset only as scalar_alias OP literal_or_parameter, entity_alias.field OP literal_or_parameter, id(entity_alias) OP literal_or_parameter, type(rel_alias) OP literal_or_parameter, or size(admitted_input) OP literal_or_parameter."
                    )
                if operator not in {"IS NULL", "IS NOT NULL"}:
                    try:
                        _parse_literal(value_text.strip())
                    except ValueError as exc:
                        raise ValueError(
                            "CypherGlot currently supports predicate RETURN items in the WITH subset only as scalar_alias OP literal_or_parameter, entity_alias.field OP literal_or_parameter, id(entity_alias) OP literal_or_parameter, type(rel_alias) OP literal_or_parameter, or size(admitted_input) OP literal_or_parameter."
                        ) from exc
                elif value_text.strip():
                    raise ValueError(
                        "CypherGlot WITH null predicate RETURN items cannot include a trailing literal value."
                    )
                if output_alias in seen_output_names:
                    raise ValueError(
                        f"CypherGlot currently does not allow duplicate RETURN output alias {output_alias!r} in the WITH subset."
                    )
                seen_output_names.add(output_alias)
                projected_output_kinds[output_alias] = "scalar"
                continue
            type_match = re.fullmatch(
                r"type\s*\(\s*(?P<alias>[A-Za-z_][A-Za-z0-9_]*)\s*\)",
                left_text,
                flags=re.IGNORECASE,
            )
            if type_match is not None:
                alias = type_match.group("alias")
                if binding_kinds.get(alias) != "entity" or binding_alias_kinds.get(alias) != "relationship":
                    raise ValueError(
                        "CypherGlot currently supports predicate RETURN items in the WITH subset only as scalar_alias OP literal_or_parameter, entity_alias.field OP literal_or_parameter, id(entity_alias) OP literal_or_parameter, type(rel_alias) OP literal_or_parameter, or size(admitted_input) OP literal_or_parameter."
                    )
                if operator not in {"IS NULL", "IS NOT NULL"}:
                    try:
                        _parse_literal(value_text.strip())
                    except ValueError as exc:
                        raise ValueError(
                            "CypherGlot currently supports predicate RETURN items in the WITH subset only as scalar_alias OP literal_or_parameter, entity_alias.field OP literal_or_parameter, id(entity_alias) OP literal_or_parameter, type(rel_alias) OP literal_or_parameter, or size(admitted_input) OP literal_or_parameter."
                        ) from exc
                elif value_text.strip():
                    raise ValueError(
                        "CypherGlot WITH null predicate RETURN items cannot include a trailing literal value."
                    )
                if output_alias in seen_output_names:
                    raise ValueError(
                        f"CypherGlot currently does not allow duplicate RETURN output alias {output_alias!r} in the WITH subset."
                    )
                seen_output_names.add(output_alias)
                projected_output_kinds[output_alias] = "scalar"
                continue
            field_match = re.fullmatch(
                r"(?P<alias>[A-Za-z_][A-Za-z0-9_]*)\.(?P<field>[A-Za-z_][A-Za-z0-9_]*)",
                left_text,
            )
            if field_match is not None and binding_kinds.get(field_match.group("alias")) == "entity":
                if operator not in {"IS NULL", "IS NOT NULL"}:
                    try:
                        _parse_literal(value_text.strip())
                    except ValueError as exc:
                        raise ValueError(
                            "CypherGlot currently supports predicate RETURN items in the WITH subset only as scalar_alias OP literal_or_parameter or entity_alias.field OP literal_or_parameter."
                        ) from exc
                elif value_text.strip():
                    raise ValueError(
                        "CypherGlot WITH null predicate RETURN items cannot include a trailing literal value."
                    )
                if output_alias in seen_output_names:
                    raise ValueError(
                        f"CypherGlot currently does not allow duplicate RETURN output alias {output_alias!r} in the WITH subset."
                    )
                seen_output_names.add(output_alias)
                projected_output_kinds[output_alias] = "scalar"
                continue
            raise ValueError(
                "CypherGlot currently supports predicate RETURN items in the WITH subset only as scalar_alias OP value, entity_alias.field OP value, id(entity_alias) OP value, type(rel_alias) OP value, or size(admitted_input) OP value."
            )

        field_match = re.fullmatch(
            r"(?P<alias>[A-Za-z_][A-Za-z0-9_]*)\.(?P<field>[A-Za-z_][A-Za-z0-9_]*)",
            expression_text,
        )
        if field_match is not None:
            if binding_kinds.get(field_match.group("alias")) != "entity":
                raise ValueError(
                    "CypherGlot currently supports RETURN alias.field for entity "
                    "bindings, RETURN entity_alias for pass-through entity bindings, "
                    "RETURN scalar_alias for scalar bindings, and optional AS aliases for "
                    "those projection forms."
                )
            output_name = output_alias or expression_text
            if output_name in seen_output_names:
                raise ValueError(
                    f"CypherGlot currently does not allow duplicate RETURN output alias {output_name!r} in the WITH subset."
                )
            seen_output_names.add(output_name)
            projected_output_kinds[output_name] = "field"
            continue
        binding_kind = binding_kinds.get(expression_text)
        if binding_kind not in {"entity", "scalar"}:
            raise ValueError(
                "CypherGlot currently supports RETURN alias.field for entity "
                "bindings, RETURN entity_alias for pass-through entity bindings, "
                "RETURN scalar_alias for scalar bindings, id(binding_alias) AS output_alias, "
                "sum(scalar_alias) AS output_alias, avg(scalar_alias) AS output_alias, "
                "min(scalar_alias) AS output_alias, max(scalar_alias) AS output_alias, "
                "type(rel_binding_alias) AS output_alias, size(admitted_input) AS output_alias, "
                "lower(admitted_input) AS output_alias, upper(admitted_input) AS output_alias, trim(admitted_input) AS output_alias, ltrim(admitted_input) AS output_alias, rtrim(admitted_input) AS output_alias, reverse(admitted_input) AS output_alias, coalesce(admitted_input, literal_or_parameter) AS output_alias, replace(admitted_input, literal_or_parameter, literal_or_parameter) AS output_alias, left(admitted_input, literal_or_parameter) AS output_alias, right(admitted_input, literal_or_parameter) AS output_alias, split(admitted_input, literal_or_parameter) AS output_alias, substring(admitted_input, literal_or_parameter) AS output_alias, substring(admitted_input, literal_or_parameter, literal_or_parameter) AS output_alias, abs(admitted_input) AS output_alias, sign(admitted_input) AS output_alias, round(admitted_input) AS output_alias, ceil(admitted_input) AS output_alias, floor(admitted_input) AS output_alias, sqrt(admitted_input) AS output_alias, exp(admitted_input) AS output_alias, toString(admitted_input) AS output_alias, toInteger(admitted_input) AS output_alias, toFloat(admitted_input) AS output_alias, toBoolean(admitted_input) AS output_alias, "
                "scalar_literal_or_parameter AS output_alias, predicate admitted_input OP value AS output_alias, searched CASE WHEN admitted_predicate THEN admitted_result ELSE admitted_result END AS output_alias, and optional AS aliases for those projection forms."
            )
        output_name = output_alias or expression_text
        if output_name in seen_output_names:
            raise ValueError(
                f"CypherGlot currently does not allow duplicate RETURN output alias {output_name!r} in the WITH subset."
            )
        seen_output_names.add(output_name)
        projected_output_kinds[output_name] = binding_kind

    for item in order_items:
        if item.expression is not None:
            if item.expression in aggregate_aliases:
                continue
            if item.expression in projected_output_kinds:
                continue
            raise ValueError(
                "CypherGlot currently supports ORDER BY exact projected RETURN expressions, projected aliases, or admitted bindings in the supported WITH subset."
            )
        if item.field == "__value__" and item.alias in aggregate_aliases:
            continue
        if item.field == "__value__" and item.alias in projected_output_kinds:
            continue
        binding_kind = binding_kinds.get(item.alias)
        if binding_kind is None:
            raise ValueError(
                f"CypherGlot currently cannot ORDER BY unknown alias {item.alias!r} in the supported WITH subset."
            )
        if binding_kind == "scalar" and item.field != "__value__":
            raise ValueError(
                "CypherGlot currently supports ORDER BY scalar_alias for scalar "
                "bindings in the supported WITH subset."
            )


def _validate_with_where_shape(
    text: str,
    binding_kinds: dict[str, str],
    binding_alias_kinds: dict[str, str],
) -> None:
    groups = _parse_boolean_predicate_groups(text)
    saw_predicate = False
    for group in groups:
        for item in group:
            saw_predicate = True
            try:
                left_text, operator, value_text = _split_predicate_comparison(item)
            except ValueError as exc:
                raise ValueError(
                    "CypherGlot currently supports WITH WHERE items shaped as scalar_alias OP value, entity_alias.field OP value, id(entity_alias) OP value, type(rel_alias) OP value, or size(admitted_input) OP value."
                ) from exc

            target_text = left_text.strip()
            id_match = re.fullmatch(
                r"id\s*\(\s*(?P<alias>[A-Za-z_][A-Za-z0-9_]*)\s*\)",
                target_text,
                flags=re.IGNORECASE,
            )
            if id_match is not None:
                if binding_kinds.get(id_match.group("alias")) != "entity":
                    raise ValueError(
                        "CypherGlot currently supports WITH WHERE id(entity_alias) only for entity bindings."
                    )
            else:
                type_match = re.fullmatch(
                    r"type\s*\(\s*(?P<alias>[A-Za-z_][A-Za-z0-9_]*)\s*\)",
                    target_text,
                    flags=re.IGNORECASE,
                )
                if type_match is not None:
                    alias = type_match.group("alias")
                    if binding_kinds.get(alias) != "entity" or binding_alias_kinds.get(alias) != "relationship":
                        raise ValueError(
                            "CypherGlot currently supports WITH WHERE type(rel_alias) only for relationship entity bindings."
                        )
                else:
                    size_match = re.fullmatch(
                        r"size\s*\(\s*(?P<expr>.+?)\s*\)",
                        target_text,
                        flags=re.IGNORECASE,
                    )
                    if size_match is not None:
                        size_expr = size_match.group("expr").strip()
                        if binding_kinds.get(size_expr) != "scalar":
                            field_match = re.fullmatch(
                                r"(?P<alias>[A-Za-z_][A-Za-z0-9_]*)\.(?P<field>[A-Za-z_][A-Za-z0-9_]*)",
                                size_expr,
                            )
                            if field_match is None or binding_kinds.get(field_match.group("alias")) != "entity":
                                raise ValueError(
                                    "CypherGlot currently supports WITH WHERE size(admitted_input) only for scalar bindings or entity field projections."
                                )
                    else:
                        field_match = re.fullmatch(
                            r"(?P<alias>[A-Za-z_][A-Za-z0-9_]*)\.(?P<field>[A-Za-z_][A-Za-z0-9_]*)",
                            target_text,
                        )
                        if field_match is not None:
                            if binding_kinds.get(field_match.group("alias")) != "entity":
                                raise ValueError(
                                    "CypherGlot currently supports WITH WHERE entity_alias.field only for entity bindings."
                                )
                        elif binding_kinds.get(target_text) != "scalar":
                            raise ValueError(
                                "CypherGlot currently supports WITH WHERE items shaped as scalar_alias OP value, entity_alias.field OP value, id(entity_alias) OP value, type(rel_alias) OP value, or size(admitted_input) OP value."
                            )

            if operator in {"IS NULL", "IS NOT NULL"}:
                if value_text.strip():
                    raise ValueError(
                        "CypherGlot null predicates cannot include a trailing literal value."
                    )

    if not saw_predicate:
        raise ValueError("CypherGlot WITH WHERE clauses cannot be empty.")


def _validate_unwind_shape(result: CypherParseResult, single_part_query_ctx) -> None:
    reading_clauses = single_part_query_ctx.oC_ReadingClause()
    updating_clauses = single_part_query_ctx.oC_UpdatingClause()
    return_ctx = single_part_query_ctx.oC_Return()

    if len(reading_clauses) != 1 or updating_clauses or return_ctx is None:
        raise ValueError(
            "CypherGlot currently admits only standalone UNWIND ... RETURN statements in the narrow UNWIND subset."
        )

    unwind_ctx = reading_clauses[0].oC_Unwind()
    if unwind_ctx is None:
        raise ValueError(
            "CypherGlot currently admits only standalone UNWIND ... RETURN statements in the narrow UNWIND subset."
        )

    source_text = _context_text(result, unwind_ctx.oC_Expression()).strip()
    if not (
        (source_text.startswith("[") and source_text.endswith("]"))
        or re.fullmatch(r"\$[A-Za-z_][A-Za-z0-9_]*", source_text) is not None
    ):
        raise ValueError(
            "CypherGlot currently requires UNWIND sources to be list literals or named parameters."
        )

    alias = _context_text(result, unwind_ctx.oC_Variable()).strip()
    projection_text = _context_text(result, return_ctx.oC_ProjectionBody())
    return_text, order_items, _limit, distinct, _skip = _split_return_clause(projection_text)
    if distinct:
        raise ValueError(
            "CypherGlot currently does not admit RETURN DISTINCT in the narrow UNWIND subset."
        )

    return_items = [item.strip() for item in _split_comma_separated(return_text)]
    if not return_items:
        raise ValueError(
            "CypherGlot currently requires at least one RETURN item in the narrow UNWIND subset."
        )

    projected_aliases: set[str] = set()
    for item_text in return_items:
        alias_match = re.fullmatch(
            r"(?P<expr>.+?)\s+as\s+(?P<output>[A-Za-z_][A-Za-z0-9_]*)",
            item_text,
            flags=re.IGNORECASE,
        )
        output_alias = alias_match.group("output") if alias_match is not None else None
        expression_text = alias_match.group("expr").strip() if alias_match is not None else item_text
        if expression_text != alias:
            raise ValueError(
                "CypherGlot currently supports only RETURN unwind_alias or RETURN unwind_alias AS output_alias in the narrow UNWIND subset."
            )
        output_name = output_alias or alias
        if output_name in projected_aliases:
            raise ValueError(
                f"CypherGlot currently does not allow duplicate RETURN output alias {output_name!r} in the narrow UNWIND subset."
            )
        projected_aliases.add(output_name)

    for item in order_items:
        if item.field == "__value__" and item.alias in projected_aliases:
            continue
        raise ValueError(
            "CypherGlot currently supports ORDER BY unwind_alias or ORDER BY projected_alias in the narrow UNWIND subset."
        )


def _validate_optional_match_shape(result: CypherParseResult, single_part_query_ctx) -> None:
    updating_clauses = single_part_query_ctx.oC_UpdatingClause()
    reading_clauses = single_part_query_ctx.oC_ReadingClause()
    return_ctx = single_part_query_ctx.oC_Return()

    if len(reading_clauses) != 1 or updating_clauses or return_ctx is None:
        raise ValueError(
            "CypherGlot currently admits only standalone OPTIONAL MATCH ... RETURN statements in the narrow OPTIONAL MATCH subset."
        )

    match_ctx = reading_clauses[0].oC_Match()
    if match_ctx is None or match_ctx.OPTIONAL() is None:
        raise ValueError(
            "CypherGlot currently admits only standalone OPTIONAL MATCH ... RETURN statements in the narrow OPTIONAL MATCH subset."
        )

    _validate_match_pattern_shape(
        result,
        match_ctx,
        allow_two_node_disconnected=False,
        allow_optional=True,
    )

    pattern_parts = match_ctx.oC_Pattern().oC_PatternPart()
    if len(pattern_parts) != 1:
        raise ValueError(
            "CypherGlot currently admits only one node pattern in the narrow OPTIONAL MATCH subset."
        )
    pattern_element = (
        pattern_parts[0].oC_AnonymousPatternPart().oC_PatternElement()
    )
    if pattern_element.oC_PatternElementChain():
        raise ValueError(
            "CypherGlot currently admits only single-node OPTIONAL MATCH patterns in the narrow OPTIONAL MATCH subset."
        )

    projection_text = _context_text(result, return_ctx.oC_ProjectionBody())
    _validate_plain_read_projection_shape(
        projection_text,
        allowed_aliases={
            alias
            for alias in re.findall(
                r"\(([A-Za-z_][A-Za-z0-9_]*)",
                _context_text(result, match_ctx.oC_Pattern()),
            )
        },
        allowed_relationship_aliases=set(),
    )


def _validate_plain_read_projection_shape(
    projection_text: str,
    *,
    allowed_aliases: set[str],
    allowed_relationship_aliases: set[str] | None = None,
) -> None:
    relationship_aliases = allowed_relationship_aliases or set()
    return_text, order_items, _limit, _distinct, _skip = _split_return_clause(
        projection_text
    )

    return_items = [item.strip() for item in _split_comma_separated(return_text)]
    if not return_items:
        raise ValueError(
            "CypherGlot currently requires at least one RETURN item in the supported read subset."
        )

    projected_output_kinds: dict[str, str] = {}
    for item_text in return_items:
        alias_match = re.fullmatch(
            r"(?P<expr>.+?)\s+as\s+(?P<output>[A-Za-z_][A-Za-z0-9_]*)",
            item_text,
            flags=re.IGNORECASE,
        )
        output_alias = alias_match.group("output") if alias_match is not None else None
        expression_text = (
            alias_match.group("expr").strip() if alias_match is not None else item_text
        )

        if _validate_plain_case_expression(
            expression_text,
            output_alias=output_alias,
            allowed_aliases=allowed_aliases,
            relationship_aliases=relationship_aliases,
        ):
            if output_alias in projected_output_kinds:
                raise ValueError(
                    f"CypherGlot currently does not allow duplicate RETURN output alias {output_alias!r} in the supported read subset."
                )
            projected_output_kinds[output_alias] = "scalar"
            continue

        count_match = re.fullmatch(
            r"count\s*\(\s*(?P<alias>\*|[A-Za-z_][A-Za-z0-9_]*)\s*\)",
            expression_text,
            flags=re.IGNORECASE,
        )
        if count_match is not None:
            if count_match.group("alias") != "*" and count_match.group("alias") not in allowed_aliases:
                raise ValueError(
                    "CypherGlot currently requires count(...) in the supported read subset to target an admitted binding alias or *."
                )
            output_name = output_alias or expression_text
            if output_name in projected_output_kinds:
                raise ValueError(
                    f"CypherGlot currently does not allow duplicate RETURN output alias {output_name!r} in the supported read subset."
                )
            projected_output_kinds[output_name] = "aggregate"
            continue

        aggregate_match = re.fullmatch(
            r"(?P<func>sum|avg|min|max)\s*\(\s*(?P<alias>[A-Za-z_][A-Za-z0-9_]*)\.(?P<field>[A-Za-z_][A-Za-z0-9_]*)\s*\)",
            expression_text,
            flags=re.IGNORECASE,
        )
        if aggregate_match is not None:
            if aggregate_match.group("alias") not in allowed_aliases:
                raise ValueError(
                    "CypherGlot currently supports sum(...), avg(...), min(...), and max(...) in the supported read subset only over admitted entity or relationship fields."
                )
            output_name = output_alias or expression_text
            if output_name in projected_output_kinds:
                raise ValueError(
                    f"CypherGlot currently does not allow duplicate RETURN output alias {output_name!r} in the supported read subset."
                )
            projected_output_kinds[output_name] = "aggregate"
            continue

        id_match = re.fullmatch(
            r"id\s*\(\s*(?P<alias>[A-Za-z_][A-Za-z0-9_]*)\s*\)",
            expression_text,
            flags=re.IGNORECASE,
        )
        if id_match is not None:
            if id_match.group("alias") not in allowed_aliases:
                raise ValueError(
                    "CypherGlot currently supports id(...) in the supported read subset only over admitted entity or relationship bindings."
                )
            output_name = output_alias or expression_text
            if output_name in projected_output_kinds:
                raise ValueError(
                    f"CypherGlot currently does not allow duplicate RETURN output alias {output_name!r} in the supported read subset."
                )
            projected_output_kinds[output_name] = "scalar"
            continue

        type_match = re.fullmatch(
            r"type\s*\(\s*(?P<alias>[A-Za-z_][A-Za-z0-9_]*)\s*\)",
            expression_text,
            flags=re.IGNORECASE,
        )
        if type_match is not None:
            alias = type_match.group("alias")
            if alias not in relationship_aliases:
                raise ValueError(
                    "CypherGlot currently supports type(...) in the supported read subset only over admitted relationship bindings."
                )
            output_name = output_alias or expression_text
            if output_name in projected_output_kinds:
                raise ValueError(
                    f"CypherGlot currently does not allow duplicate RETURN output alias {output_name!r} in the supported read subset."
                )
            projected_output_kinds[output_name] = "scalar"
            continue

        properties_match = re.fullmatch(
            r"properties\s*\(\s*(?P<alias>[A-Za-z_][A-Za-z0-9_]*)\s*\)",
            expression_text,
            flags=re.IGNORECASE,
        )
        if properties_match is not None:
            alias = properties_match.group("alias")
            if alias not in allowed_aliases:
                raise ValueError(
                    "CypherGlot currently supports properties(...) in the supported read subset only over admitted entity or relationship bindings."
                )
            output_name = output_alias or expression_text
            if output_name in projected_output_kinds:
                raise ValueError(
                    f"CypherGlot currently does not allow duplicate RETURN output alias {output_name!r} in the supported read subset."
                )
            projected_output_kinds[output_name] = "scalar"
            continue

        labels_match = re.fullmatch(
            r"labels\s*\(\s*(?P<alias>[A-Za-z_][A-Za-z0-9_]*)\s*\)",
            expression_text,
            flags=re.IGNORECASE,
        )
        if labels_match is not None:
            alias = labels_match.group("alias")
            if alias not in allowed_aliases or alias in relationship_aliases:
                raise ValueError(
                    "CypherGlot currently supports labels(...) in the supported read subset only over admitted node bindings."
                )
            output_name = output_alias or expression_text
            if output_name in projected_output_kinds:
                raise ValueError(
                    f"CypherGlot currently does not allow duplicate RETURN output alias {output_name!r} in the supported read subset."
                )
            projected_output_kinds[output_name] = "scalar"
            continue

        keys_match = re.fullmatch(
            r"keys\s*\(\s*(?P<alias>[A-Za-z_][A-Za-z0-9_]*)\s*\)",
            expression_text,
            flags=re.IGNORECASE,
        )
        if keys_match is not None:
            alias = keys_match.group("alias")
            if alias not in allowed_aliases:
                raise ValueError(
                    "CypherGlot currently supports keys(...) in the supported read subset only over admitted entity or relationship bindings."
                )
            output_name = output_alias or expression_text
            if output_name in projected_output_kinds:
                raise ValueError(
                    f"CypherGlot currently does not allow duplicate RETURN output alias {output_name!r} in the supported read subset."
                )
            projected_output_kinds[output_name] = "scalar"
            continue

        start_node_match = re.fullmatch(
            r"startNode\s*\(\s*(?P<alias>[A-Za-z_][A-Za-z0-9_]*)\s*\)(?:\.(?P<field>[A-Za-z_][A-Za-z0-9_]*))?",
            expression_text,
            flags=re.IGNORECASE,
        )
        if start_node_match is not None:
            alias = start_node_match.group("alias")
            if alias not in relationship_aliases:
                raise ValueError(
                    "CypherGlot currently supports startNode(...) in the supported read subset only over admitted relationship bindings."
                )
            output_name = output_alias or expression_text
            if output_name in projected_output_kinds:
                raise ValueError(
                    f"CypherGlot currently does not allow duplicate RETURN output alias {output_name!r} in the supported read subset."
                )
            projected_output_kinds[output_name] = (
                "scalar" if start_node_match.group("field") is not None else "entity"
            )
            continue

        end_node_match = re.fullmatch(
            r"endNode\s*\(\s*(?P<alias>[A-Za-z_][A-Za-z0-9_]*)\s*\)(?:\.(?P<field>[A-Za-z_][A-Za-z0-9_]*))?",
            expression_text,
            flags=re.IGNORECASE,
        )
        if end_node_match is not None:
            alias = end_node_match.group("alias")
            if alias not in relationship_aliases:
                raise ValueError(
                    "CypherGlot currently supports endNode(...) in the supported read subset only over admitted relationship bindings."
                )
            output_name = output_alias or expression_text
            if output_name in projected_output_kinds:
                raise ValueError(
                    f"CypherGlot currently does not allow duplicate RETURN output alias {output_name!r} in the supported read subset."
                )
            projected_output_kinds[output_name] = (
                "scalar" if end_node_match.group("field") is not None else "entity"
            )
            continue

        if output_alias is None:
            size_match = re.fullmatch(
                r"size\s*\(\s*(?P<expr>.+?)\s*\)",
                expression_text,
                flags=re.IGNORECASE,
            )
            if size_match is not None:
                size_expr = size_match.group("expr").strip()
                id_match = re.fullmatch(
                    r"id\s*\(\s*(?P<alias>[A-Za-z_][A-Za-z0-9_]*)\s*\)",
                    size_expr,
                    flags=re.IGNORECASE,
                )
                if id_match is not None and id_match.group("alias") in allowed_aliases:
                    if expression_text in projected_output_kinds:
                        raise ValueError(
                            f"CypherGlot currently does not allow duplicate RETURN output alias {expression_text!r} in the supported read subset."
                        )
                    projected_output_kinds[expression_text] = "scalar"
                    continue
                type_match = re.fullmatch(
                    r"type\s*\(\s*(?P<alias>[A-Za-z_][A-Za-z0-9_]*)\s*\)",
                    size_expr,
                    flags=re.IGNORECASE,
                )
                if type_match is not None and type_match.group("alias") in relationship_aliases:
                    if expression_text in projected_output_kinds:
                        raise ValueError(
                            f"CypherGlot currently does not allow duplicate RETURN output alias {expression_text!r} in the supported read subset."
                        )
                    projected_output_kinds[expression_text] = "scalar"
                    continue
                field_match = re.fullmatch(
                    r"(?P<alias>[A-Za-z_][A-Za-z0-9_]*)\.(?P<field>[A-Za-z_][A-Za-z0-9_]*)",
                    size_expr,
                )
                if field_match is not None and field_match.group("alias") in allowed_aliases:
                    if expression_text in projected_output_kinds:
                        raise ValueError(
                            f"CypherGlot currently does not allow duplicate RETURN output alias {expression_text!r} in the supported read subset."
                        )
                    projected_output_kinds[expression_text] = "scalar"
                    continue

            unary_match = re.fullmatch(
                r"(?P<func>lower|upper|trim|ltrim|rtrim|reverse|abs|sign|round|ceil|floor|sqrt|exp|sin|cos|tan|asin|acos|atan|ln|log|log10|radians|degrees|tostring|tointeger|tofloat|toboolean)\s*\(\s*(?P<expr>.+?)\s*\)",
                expression_text,
                flags=re.IGNORECASE,
            )
            if unary_match is not None:
                function_expr = unary_match.group("expr").strip()
                field_match = re.fullmatch(
                    r"(?P<alias>[A-Za-z_][A-Za-z0-9_]*)\.(?P<field>[A-Za-z_][A-Za-z0-9_]*)",
                    function_expr,
                )
                if field_match is not None and field_match.group("alias") in allowed_aliases:
                    if expression_text in projected_output_kinds:
                        raise ValueError(
                            f"CypherGlot currently does not allow duplicate RETURN output alias {expression_text!r} in the supported read subset."
                        )
                    projected_output_kinds[expression_text] = "scalar"
                    continue

        size_match = re.fullmatch(
            r"size\s*\(\s*(?P<expr>.+?)\s*\)",
            expression_text,
            flags=re.IGNORECASE,
        )
        if size_match is not None:
            if output_alias is None:
                raise ValueError(
                    "CypherGlot currently requires size(...) in the supported read subset to use an explicit AS alias."
                )
            size_expr = size_match.group("expr").strip()
            id_match = re.fullmatch(
                r"id\s*\(\s*(?P<alias>[A-Za-z_][A-Za-z0-9_]*)\s*\)",
                size_expr,
                flags=re.IGNORECASE,
            )
            if id_match is not None:
                if id_match.group("alias") not in allowed_aliases:
                    raise ValueError(
                        "CypherGlot currently supports size(...) in the supported read subset only over admitted field projections, admitted id/type outputs, or scalar literal/parameter inputs."
                    )
                if output_alias in projected_output_kinds:
                    raise ValueError(
                        f"CypherGlot currently does not allow duplicate RETURN output alias {output_alias!r} in the supported read subset."
                    )
                projected_output_kinds[output_alias] = "scalar"
                continue
            type_match = re.fullmatch(
                r"type\s*\(\s*(?P<alias>[A-Za-z_][A-Za-z0-9_]*)\s*\)",
                size_expr,
                flags=re.IGNORECASE,
            )
            if type_match is not None:
                if type_match.group("alias") not in relationship_aliases:
                    raise ValueError(
                        "CypherGlot currently supports size(...) in the supported read subset only over admitted field projections, admitted id/type outputs, or scalar literal/parameter inputs."
                    )
                if output_alias in projected_output_kinds:
                    raise ValueError(
                        f"CypherGlot currently does not allow duplicate RETURN output alias {output_alias!r} in the supported read subset."
                    )
                projected_output_kinds[output_alias] = "scalar"
                continue
            field_match = re.fullmatch(
                r"(?P<alias>[A-Za-z_][A-Za-z0-9_]*)\.(?P<field>[A-Za-z_][A-Za-z0-9_]*)",
                size_expr,
            )
            if field_match is not None:
                if field_match.group("alias") not in allowed_aliases:
                    raise ValueError(
                        "CypherGlot currently supports size(...) in the supported read subset only over admitted field projections, admitted id/type outputs, or scalar literal/parameter inputs."
                    )
            else:
                try:
                    _parse_literal(size_expr)
                except ValueError as exc:
                    raise ValueError(
                        "CypherGlot currently supports size(...) in the supported read subset only over admitted field projections, admitted id/type outputs, or scalar literal/parameter inputs."
                    ) from exc
            if output_alias in projected_output_kinds:
                raise ValueError(
                    f"CypherGlot currently does not allow duplicate RETURN output alias {output_alias!r} in the supported read subset."
                )
            projected_output_kinds[output_alias] = "scalar"
            continue

        unary_string_match = re.fullmatch(
            r"(?P<func>lower|upper|trim|ltrim|rtrim|reverse)\s*\(\s*(?P<expr>.+?)\s*\)",
            expression_text,
            flags=re.IGNORECASE,
        )
        if unary_string_match is not None:
            if output_alias is None:
                raise ValueError(
                "CypherGlot currently requires lower(...), upper(...), trim(...), ltrim(...), rtrim(...), and reverse(...) in the supported read subset to use an explicit AS alias."
                )
            function_expr = unary_string_match.group("expr").strip()
            field_match = re.fullmatch(
                r"(?P<alias>[A-Za-z_][A-Za-z0-9_]*)\.(?P<field>[A-Za-z_][A-Za-z0-9_]*)",
                function_expr,
            )
            if field_match is not None:
                if field_match.group("alias") not in allowed_aliases:
                    raise ValueError(
                        "CypherGlot currently supports lower(...), upper(...), trim(...), ltrim(...), and rtrim(...) in the supported read subset only over admitted field projections or scalar literal/parameter inputs."
                    )
            else:
                try:
                    _parse_literal(function_expr)
                except ValueError as exc:
                    raise ValueError(
                        "CypherGlot currently supports lower(...), upper(...), trim(...), ltrim(...), rtrim(...), and reverse(...) in the supported read subset only over admitted field projections or scalar literal/parameter inputs."
                    ) from exc
            if output_alias in projected_output_kinds:
                raise ValueError(
                    f"CypherGlot currently does not allow duplicate RETURN output alias {output_alias!r} in the supported read subset."
                )
            projected_output_kinds[output_alias] = "scalar"
            continue

        coalesce_match = re.fullmatch(
            r"coalesce\s*\(\s*(?P<args>.+)\s*\)",
            expression_text,
            flags=re.IGNORECASE,
        )
        if coalesce_match is not None:
            args = [part.strip() for part in _split_comma_separated(coalesce_match.group("args"))]
            if len(args) != 2:
                raise ValueError(
                    "CypherGlot currently requires coalesce(...) in the supported read subset to use exactly two arguments."
                )
            primary_expr, fallback_expr = args
            field_match = re.fullmatch(
                r"(?P<alias>[A-Za-z_][A-Za-z0-9_]*)\.(?P<field>[A-Za-z_][A-Za-z0-9_]*)",
                primary_expr,
            )
            if field_match is None or field_match.group("alias") not in allowed_aliases:
                raise ValueError(
                    "CypherGlot currently supports coalesce(...) in the supported read subset only as coalesce(alias.field, literal_or_parameter) over admitted bindings."
                )
            try:
                _parse_literal(fallback_expr)
            except ValueError as exc:
                raise ValueError(
                    "CypherGlot currently supports coalesce(...) in the supported read subset only as coalesce(alias.field, literal_or_parameter) over admitted bindings."
                ) from exc
            output_name = output_alias or expression_text
            if output_name in projected_output_kinds:
                raise ValueError(
                    f"CypherGlot currently does not allow duplicate RETURN output alias {output_name!r} in the supported read subset."
                )
            projected_output_kinds[output_name] = "scalar"
            continue

        replace_match = re.fullmatch(
            r"replace\s*\(\s*(?P<args>.+)\s*\)",
            expression_text,
            flags=re.IGNORECASE,
        )
        if replace_match is not None:
            args = [part.strip() for part in _split_comma_separated(replace_match.group("args"))]
            if len(args) != 3:
                raise ValueError(
                    "CypherGlot currently requires replace(...) in the supported read subset to use exactly three arguments."
                )
            primary_expr, search_expr, replace_expr = args
            field_match = re.fullmatch(
                r"(?P<alias>[A-Za-z_][A-Za-z0-9_]*)\.(?P<field>[A-Za-z_][A-Za-z0-9_]*)",
                primary_expr,
            )
            if field_match is not None:
                if field_match.group("alias") not in allowed_aliases:
                    raise ValueError(
                        "CypherGlot currently supports replace(...) in the supported read subset only as replace(admitted_input, literal_or_parameter, literal_or_parameter)."
                    )
            else:
                try:
                    _parse_literal(primary_expr)
                except ValueError as exc:
                    raise ValueError(
                        "CypherGlot currently supports replace(...) in the supported read subset only as replace(admitted_input, literal_or_parameter, literal_or_parameter)."
                    ) from exc
            try:
                _parse_literal(search_expr)
                _parse_literal(replace_expr)
            except ValueError as exc:
                raise ValueError(
                    "CypherGlot currently supports replace(...) in the supported read subset only as replace(admitted_input, literal_or_parameter, literal_or_parameter)."
                ) from exc
            output_name = output_alias or expression_text
            if output_name in projected_output_kinds:
                raise ValueError(
                    f"CypherGlot currently does not allow duplicate RETURN output alias {output_name!r} in the supported read subset."
                )
            projected_output_kinds[output_name] = "scalar"
            continue

        left_right_match = re.fullmatch(
            r"(?P<func>left|right)\s*\(\s*(?P<args>.+)\s*\)",
            expression_text,
            flags=re.IGNORECASE,
        )
        if left_right_match is not None:
            args = [part.strip() for part in _split_comma_separated(left_right_match.group("args"))]
            if len(args) != 2:
                raise ValueError(
                    "CypherGlot currently requires left(...) and right(...) in the supported read subset to use exactly two arguments."
                )
            primary_expr, length_expr = args
            try:
                _parse_literal(length_expr)
            except ValueError as exc:
                raise ValueError(
                    "CypherGlot currently supports left(...) and right(...) in the supported read subset only as function(admitted_input, literal_or_parameter)."
                ) from exc
            field_match = re.fullmatch(
                r"(?P<alias>[A-Za-z_][A-Za-z0-9_]*)\.(?P<field>[A-Za-z_][A-Za-z0-9_]*)",
                primary_expr,
            )
            if field_match is not None:
                if field_match.group("alias") not in allowed_aliases:
                    raise ValueError(
                        "CypherGlot currently supports left(...) and right(...) in the supported read subset only as function(admitted_input, literal_or_parameter)."
                    )
            else:
                try:
                    _parse_literal(primary_expr)
                except ValueError as exc:
                    raise ValueError(
                        "CypherGlot currently supports left(...) and right(...) in the supported read subset only as function(admitted_input, literal_or_parameter)."
                    ) from exc
            output_name = output_alias or expression_text
            if output_name in projected_output_kinds:
                raise ValueError(
                    f"CypherGlot currently does not allow duplicate RETURN output alias {output_name!r} in the supported read subset."
                )
            projected_output_kinds[output_name] = "scalar"
            continue

        split_match = re.fullmatch(
            r"split\s*\(\s*(?P<args>.+)\s*\)",
            expression_text,
            flags=re.IGNORECASE,
        )
        if split_match is not None:
            args = [part.strip() for part in _split_comma_separated(split_match.group("args"))]
            if len(args) != 2:
                raise ValueError(
                    "CypherGlot currently requires split(...) in the supported read subset to use exactly two arguments."
                )
            primary_expr, delimiter_expr = args
            try:
                _parse_literal(delimiter_expr)
            except ValueError as exc:
                raise ValueError(
                    "CypherGlot currently supports split(...) in the supported read subset only as split(admitted_input, literal_or_parameter)."
                ) from exc
            field_match = re.fullmatch(
                r"(?P<alias>[A-Za-z_][A-Za-z0-9_]*)\.(?P<field>[A-Za-z_][A-Za-z0-9_]*)",
                primary_expr,
            )
            if field_match is not None:
                if field_match.group("alias") not in allowed_aliases:
                    raise ValueError(
                        "CypherGlot currently supports split(...) in the supported read subset only as split(admitted_input, literal_or_parameter)."
                    )
            else:
                try:
                    _parse_literal(primary_expr)
                except ValueError as exc:
                    raise ValueError(
                        "CypherGlot currently supports split(...) in the supported read subset only as split(admitted_input, literal_or_parameter)."
                    ) from exc
            output_name = output_alias or expression_text
            if output_name in projected_output_kinds:
                raise ValueError(
                    f"CypherGlot currently does not allow duplicate RETURN output alias {output_name!r} in the supported read subset."
                )
            projected_output_kinds[output_name] = "scalar"
            continue

        substring_match = re.fullmatch(
            r"substring\s*\(\s*(?P<args>.+)\s*\)",
            expression_text,
            flags=re.IGNORECASE,
        )
        if substring_match is not None:
            args = [part.strip() for part in _split_comma_separated(substring_match.group("args"))]
            if len(args) not in {2, 3}:
                raise ValueError(
                    "CypherGlot currently requires substring(...) in the supported read subset to use exactly two or three arguments."
                )
            primary_expr, start_expr = args[:2]
            try:
                _parse_literal(start_expr)
                if len(args) == 3:
                    _parse_literal(args[2])
            except ValueError as exc:
                raise ValueError(
                    "CypherGlot currently supports substring(...) in the supported read subset only as substring(admitted_input, literal_or_parameter) or substring(admitted_input, literal_or_parameter, literal_or_parameter)."
                ) from exc
            field_match = re.fullmatch(
                r"(?P<alias>[A-Za-z_][A-Za-z0-9_]*)\.(?P<field>[A-Za-z_][A-Za-z0-9_]*)",
                primary_expr,
            )
            if field_match is not None:
                if field_match.group("alias") not in allowed_aliases:
                    raise ValueError(
                        "CypherGlot currently supports substring(...) in the supported read subset only as substring(admitted_input, literal_or_parameter) or substring(admitted_input, literal_or_parameter, literal_or_parameter)."
                    )
            else:
                try:
                    _parse_literal(primary_expr)
                except ValueError as exc:
                    raise ValueError(
                        "CypherGlot currently supports substring(...) in the supported read subset only as substring(admitted_input, literal_or_parameter) or substring(admitted_input, literal_or_parameter, literal_or_parameter)."
                    ) from exc
            output_name = output_alias or expression_text
            if output_name in projected_output_kinds:
                raise ValueError(
                    f"CypherGlot currently does not allow duplicate RETURN output alias {output_name!r} in the supported read subset."
                )
            projected_output_kinds[output_name] = "scalar"
            continue

        abs_match = re.fullmatch(
            r"abs\s*\(\s*(?P<expr>.+?)\s*\)",
            expression_text,
            flags=re.IGNORECASE,
        )
        if abs_match is not None:
            if output_alias is None:
                raise ValueError(
                    "CypherGlot currently requires abs(...) in the supported read subset to use an explicit AS alias."
                )
            abs_expr = abs_match.group("expr").strip()
            field_match = re.fullmatch(
                r"(?P<alias>[A-Za-z_][A-Za-z0-9_]*)\.(?P<field>[A-Za-z_][A-Za-z0-9_]*)",
                abs_expr,
            )
            if field_match is not None:
                if field_match.group("alias") not in allowed_aliases:
                    raise ValueError(
                        "CypherGlot currently supports abs(...) in the supported read subset only over admitted field projections or scalar literal/parameter inputs."
                    )
            else:
                try:
                    _parse_literal(abs_expr)
                except ValueError as exc:
                    raise ValueError(
                        "CypherGlot currently supports abs(...) in the supported read subset only over admitted field projections or scalar literal/parameter inputs."
                    ) from exc
            if output_alias in projected_output_kinds:
                raise ValueError(
                    f"CypherGlot currently does not allow duplicate RETURN output alias {output_alias!r} in the supported read subset."
                )
            projected_output_kinds[output_alias] = "scalar"
            continue

        sign_match = re.fullmatch(
            r"sign\s*\(\s*(?P<expr>.+?)\s*\)",
            expression_text,
            flags=re.IGNORECASE,
        )
        if sign_match is not None:
            if output_alias is None:
                raise ValueError(
                    "CypherGlot currently requires sign(...) in the supported read subset to use an explicit AS alias."
                )
            sign_expr = sign_match.group("expr").strip()
            field_match = re.fullmatch(
                r"(?P<alias>[A-Za-z_][A-Za-z0-9_]*)\.(?P<field>[A-Za-z_][A-Za-z0-9_]*)",
                sign_expr,
            )
            if field_match is not None:
                if field_match.group("alias") not in allowed_aliases:
                    raise ValueError(
                        "CypherGlot currently supports sign(...) in the supported read subset only over admitted field projections or scalar literal/parameter inputs."
                    )
            else:
                try:
                    _parse_literal(sign_expr)
                except ValueError as exc:
                    raise ValueError(
                        "CypherGlot currently supports sign(...) in the supported read subset only over admitted field projections or scalar literal/parameter inputs."
                    ) from exc
            if output_alias in projected_output_kinds:
                raise ValueError(
                    f"CypherGlot currently does not allow duplicate RETURN output alias {output_alias!r} in the supported read subset."
                )
            projected_output_kinds[output_alias] = "scalar"
            continue

        round_match = re.fullmatch(
            r"round\s*\(\s*(?P<expr>.+?)\s*\)",
            expression_text,
            flags=re.IGNORECASE,
        )
        if round_match is not None:
            if output_alias is None:
                raise ValueError(
                    "CypherGlot currently requires round(...) in the supported read subset to use an explicit AS alias."
                )
            round_expr = round_match.group("expr").strip()
            field_match = re.fullmatch(
                r"(?P<alias>[A-Za-z_][A-Za-z0-9_]*)\.(?P<field>[A-Za-z_][A-Za-z0-9_]*)",
                round_expr,
            )
            if field_match is not None:
                if field_match.group("alias") not in allowed_aliases:
                    raise ValueError(
                        "CypherGlot currently supports round(...) in the supported read subset only over admitted field projections or scalar literal/parameter inputs."
                    )
            else:
                try:
                    _parse_literal(round_expr)
                except ValueError as exc:
                    raise ValueError(
                        "CypherGlot currently supports round(...) in the supported read subset only over admitted field projections or scalar literal/parameter inputs."
                    ) from exc
            if output_alias in projected_output_kinds:
                raise ValueError(
                    f"CypherGlot currently does not allow duplicate RETURN output alias {output_alias!r} in the supported read subset."
                )
            projected_output_kinds[output_alias] = "scalar"
            continue

        floor_match = re.fullmatch(
            r"floor\s*\(\s*(?P<expr>.+?)\s*\)",
            expression_text,
            flags=re.IGNORECASE,
        )
        if floor_match is not None:
            if output_alias is None:
                raise ValueError(
                    "CypherGlot currently requires floor(...) in the supported read subset to use an explicit AS alias."
                )
            floor_expr = floor_match.group("expr").strip()
            field_match = re.fullmatch(
                r"(?P<alias>[A-Za-z_][A-Za-z0-9_]*)\.(?P<field>[A-Za-z_][A-Za-z0-9_]*)",
                floor_expr,
            )
            if field_match is not None:
                if field_match.group("alias") not in allowed_aliases:
                    raise ValueError(
                        "CypherGlot currently supports floor(...) in the supported read subset only over admitted field projections or scalar literal/parameter inputs."
                    )
            else:
                try:
                    _parse_literal(floor_expr)
                except ValueError as exc:
                    raise ValueError(
                        "CypherGlot currently supports floor(...) in the supported read subset only over admitted field projections or scalar literal/parameter inputs."
                    ) from exc
            if output_alias in projected_output_kinds:
                raise ValueError(
                    f"CypherGlot currently does not allow duplicate RETURN output alias {output_alias!r} in the supported read subset."
                )
            projected_output_kinds[output_alias] = "scalar"
            continue

        ceil_match = re.fullmatch(
            r"ceil\s*\(\s*(?P<expr>.+?)\s*\)",
            expression_text,
            flags=re.IGNORECASE,
        )
        if ceil_match is not None:
            if output_alias is None:
                raise ValueError(
                    "CypherGlot currently requires ceil(...) in the supported read subset to use an explicit AS alias."
                )
            ceil_expr = ceil_match.group("expr").strip()
            field_match = re.fullmatch(
                r"(?P<alias>[A-Za-z_][A-Za-z0-9_]*)\.(?P<field>[A-Za-z_][A-Za-z0-9_]*)",
                ceil_expr,
            )
            if field_match is not None:
                if field_match.group("alias") not in allowed_aliases:
                    raise ValueError(
                        "CypherGlot currently supports ceil(...) in the supported read subset only over admitted field projections or scalar literal/parameter inputs."
                    )
            else:
                try:
                    _parse_literal(ceil_expr)
                except ValueError as exc:
                    raise ValueError(
                        "CypherGlot currently supports ceil(...) in the supported read subset only over admitted field projections or scalar literal/parameter inputs."
                    ) from exc
            if output_alias in projected_output_kinds:
                raise ValueError(
                    f"CypherGlot currently does not allow duplicate RETURN output alias {output_alias!r} in the supported read subset."
                )
            projected_output_kinds[output_alias] = "scalar"
            continue

        sqrt_match = re.fullmatch(
            r"sqrt\s*\(\s*(?P<expr>.+?)\s*\)",
            expression_text,
            flags=re.IGNORECASE,
        )
        if sqrt_match is not None:
            if output_alias is None:
                raise ValueError(
                    "CypherGlot currently requires sqrt(...) in the supported read subset to use an explicit AS alias."
                )
            sqrt_expr = sqrt_match.group("expr").strip()
            field_match = re.fullmatch(
                r"(?P<alias>[A-Za-z_][A-Za-z0-9_]*)\.(?P<field>[A-Za-z_][A-Za-z0-9_]*)",
                sqrt_expr,
            )
            if field_match is not None:
                if field_match.group("alias") not in allowed_aliases:
                    raise ValueError(
                        "CypherGlot currently supports sqrt(...) in the supported read subset only over admitted field projections or scalar literal/parameter inputs."
                    )
            else:
                try:
                    _parse_literal(sqrt_expr)
                except ValueError as exc:
                    raise ValueError(
                        "CypherGlot currently supports sqrt(...) in the supported read subset only over admitted field projections or scalar literal/parameter inputs."
                    ) from exc
            if output_alias in projected_output_kinds:
                raise ValueError(
                    f"CypherGlot currently does not allow duplicate RETURN output alias {output_alias!r} in the supported read subset."
                )
            projected_output_kinds[output_alias] = "scalar"
            continue

        exp_match = re.fullmatch(
            r"exp\s*\(\s*(?P<expr>.+?)\s*\)",
            expression_text,
            flags=re.IGNORECASE,
        )
        if exp_match is not None:
            if output_alias is None:
                raise ValueError(
                    "CypherGlot currently requires exp(...) in the supported read subset to use an explicit AS alias."
                )
            exp_expr = exp_match.group("expr").strip()
            field_match = re.fullmatch(
                r"(?P<alias>[A-Za-z_][A-Za-z0-9_]*)\.(?P<field>[A-Za-z_][A-Za-z0-9_]*)",
                exp_expr,
            )
            if field_match is not None:
                if field_match.group("alias") not in allowed_aliases:
                    raise ValueError(
                        "CypherGlot currently supports exp(...) in the supported read subset only over admitted field projections or scalar literal/parameter inputs."
                    )
            else:
                try:
                    _parse_literal(exp_expr)
                except ValueError as exc:
                    raise ValueError(
                        "CypherGlot currently supports exp(...) in the supported read subset only over admitted field projections or scalar literal/parameter inputs."
                    ) from exc
            if output_alias in projected_output_kinds:
                raise ValueError(
                    f"CypherGlot currently does not allow duplicate RETURN output alias {output_alias!r} in the supported read subset."
                )
            projected_output_kinds[output_alias] = "scalar"
            continue

        sin_match = re.fullmatch(
            r"sin\s*\(\s*(?P<expr>.+?)\s*\)",
            expression_text,
            flags=re.IGNORECASE,
        )
        if sin_match is not None:
            if output_alias is None:
                raise ValueError(
                    "CypherGlot currently requires sin(...) in the supported read subset to use an explicit AS alias."
                )
            sin_expr = sin_match.group("expr").strip()
            field_match = re.fullmatch(
                r"(?P<alias>[A-Za-z_][A-Za-z0-9_]*)\.(?P<field>[A-Za-z_][A-Za-z0-9_]*)",
                sin_expr,
            )
            if field_match is not None:
                if field_match.group("alias") not in allowed_aliases:
                    raise ValueError(
                        "CypherGlot currently supports sin(...) in the supported read subset only over admitted field projections or scalar literal/parameter inputs."
                    )
            else:
                try:
                    _parse_literal(sin_expr)
                except ValueError as exc:
                    raise ValueError(
                        "CypherGlot currently supports sin(...) in the supported read subset only over admitted field projections or scalar literal/parameter inputs."
                    ) from exc
            if output_alias in projected_output_kinds:
                raise ValueError(
                    f"CypherGlot currently does not allow duplicate RETURN output alias {output_alias!r} in the supported read subset."
                )
            projected_output_kinds[output_alias] = "scalar"
            continue

        cos_match = re.fullmatch(
            r"cos\s*\(\s*(?P<expr>.+?)\s*\)",
            expression_text,
            flags=re.IGNORECASE,
        )
        if cos_match is not None:
            if output_alias is None:
                raise ValueError(
                    "CypherGlot currently requires cos(...) in the supported read subset to use an explicit AS alias."
                )
            cos_expr = cos_match.group("expr").strip()
            field_match = re.fullmatch(
                r"(?P<alias>[A-Za-z_][A-Za-z0-9_]*)\.(?P<field>[A-Za-z_][A-Za-z0-9_]*)",
                cos_expr,
            )
            if field_match is not None:
                if field_match.group("alias") not in allowed_aliases:
                    raise ValueError(
                        "CypherGlot currently supports cos(...) in the supported read subset only over admitted field projections or scalar literal/parameter inputs."
                    )
            else:
                try:
                    _parse_literal(cos_expr)
                except ValueError as exc:
                    raise ValueError(
                        "CypherGlot currently supports cos(...) in the supported read subset only over admitted field projections or scalar literal/parameter inputs."
                    ) from exc
            if output_alias in projected_output_kinds:
                raise ValueError(
                    f"CypherGlot currently does not allow duplicate RETURN output alias {output_alias!r} in the supported read subset."
                )
            projected_output_kinds[output_alias] = "scalar"
            continue

        tan_match = re.fullmatch(
            r"tan\s*\(\s*(?P<expr>.+?)\s*\)",
            expression_text,
            flags=re.IGNORECASE,
        )
        if tan_match is not None:
            if output_alias is None:
                raise ValueError(
                    "CypherGlot currently requires tan(...) in the supported read subset to use an explicit AS alias."
                )
            tan_expr = tan_match.group("expr").strip()
            field_match = re.fullmatch(
                r"(?P<alias>[A-Za-z_][A-Za-z0-9_]*)\.(?P<field>[A-Za-z_][A-Za-z0-9_]*)",
                tan_expr,
            )
            if field_match is not None:
                if field_match.group("alias") not in allowed_aliases:
                    raise ValueError(
                        "CypherGlot currently supports tan(...) in the supported read subset only over admitted field projections or scalar literal/parameter inputs."
                    )
            else:
                try:
                    _parse_literal(tan_expr)
                except ValueError as exc:
                    raise ValueError(
                        "CypherGlot currently supports tan(...) in the supported read subset only over admitted field projections or scalar literal/parameter inputs."
                    ) from exc
            if output_alias in projected_output_kinds:
                raise ValueError(
                    f"CypherGlot currently does not allow duplicate RETURN output alias {output_alias!r} in the supported read subset."
                )
            projected_output_kinds[output_alias] = "scalar"
            continue

        asin_match = re.fullmatch(
            r"asin\s*\(\s*(?P<expr>.+?)\s*\)",
            expression_text,
            flags=re.IGNORECASE,
        )
        if asin_match is not None:
            if output_alias is None:
                raise ValueError(
                    "CypherGlot currently requires asin(...) in the supported read subset to use an explicit AS alias."
                )
            asin_expr = asin_match.group("expr").strip()
            field_match = re.fullmatch(
                r"(?P<alias>[A-Za-z_][A-Za-z0-9_]*)\.(?P<field>[A-Za-z_][A-Za-z0-9_]*)",
                asin_expr,
            )
            if field_match is not None:
                if field_match.group("alias") not in allowed_aliases:
                    raise ValueError(
                        "CypherGlot currently supports asin(...) in the supported read subset only over admitted field projections or scalar literal/parameter inputs."
                    )
            else:
                try:
                    _parse_literal(asin_expr)
                except ValueError as exc:
                    raise ValueError(
                        "CypherGlot currently supports asin(...) in the supported read subset only over admitted field projections or scalar literal/parameter inputs."
                    ) from exc
            if output_alias in projected_output_kinds:
                raise ValueError(
                    f"CypherGlot currently does not allow duplicate RETURN output alias {output_alias!r} in the supported read subset."
                )
            projected_output_kinds[output_alias] = "scalar"
            continue

        acos_match = re.fullmatch(
            r"acos\s*\(\s*(?P<expr>.+?)\s*\)",
            expression_text,
            flags=re.IGNORECASE,
        )
        if acos_match is not None:
            if output_alias is None:
                raise ValueError(
                    "CypherGlot currently requires acos(...) in the supported read subset to use an explicit AS alias."
                )
            acos_expr = acos_match.group("expr").strip()
            field_match = re.fullmatch(
                r"(?P<alias>[A-Za-z_][A-Za-z0-9_]*)\.(?P<field>[A-Za-z_][A-Za-z0-9_]*)",
                acos_expr,
            )
            if field_match is not None:
                if field_match.group("alias") not in allowed_aliases:
                    raise ValueError(
                        "CypherGlot currently supports acos(...) in the supported read subset only over admitted field projections or scalar literal/parameter inputs."
                    )
            else:
                try:
                    _parse_literal(acos_expr)
                except ValueError as exc:
                    raise ValueError(
                        "CypherGlot currently supports acos(...) in the supported read subset only over admitted field projections or scalar literal/parameter inputs."
                    ) from exc
            if output_alias in projected_output_kinds:
                raise ValueError(
                    f"CypherGlot currently does not allow duplicate RETURN output alias {output_alias!r} in the supported read subset."
                )
            projected_output_kinds[output_alias] = "scalar"
            continue

        atan_match = re.fullmatch(
            r"atan\s*\(\s*(?P<expr>.+?)\s*\)",
            expression_text,
            flags=re.IGNORECASE,
        )
        if atan_match is not None:
            if output_alias is None:
                raise ValueError(
                    "CypherGlot currently requires atan(...) in the supported read subset to use an explicit AS alias."
                )
            atan_expr = atan_match.group("expr").strip()
            field_match = re.fullmatch(
                r"(?P<alias>[A-Za-z_][A-Za-z0-9_]*)\.(?P<field>[A-Za-z_][A-Za-z0-9_]*)",
                atan_expr,
            )
            if field_match is not None:
                if field_match.group("alias") not in allowed_aliases:
                    raise ValueError(
                        "CypherGlot currently supports atan(...) in the supported read subset only over admitted field projections or scalar literal/parameter inputs."
                    )
            else:
                try:
                    _parse_literal(atan_expr)
                except ValueError as exc:
                    raise ValueError(
                        "CypherGlot currently supports atan(...) in the supported read subset only over admitted field projections or scalar literal/parameter inputs."
                    ) from exc
            if output_alias in projected_output_kinds:
                raise ValueError(
                    f"CypherGlot currently does not allow duplicate RETURN output alias {output_alias!r} in the supported read subset."
                )
            projected_output_kinds[output_alias] = "scalar"
            continue

        ln_match = re.fullmatch(
            r"ln\s*\(\s*(?P<expr>.+?)\s*\)",
            expression_text,
            flags=re.IGNORECASE,
        )
        if ln_match is not None:
            if output_alias is None:
                raise ValueError(
                    "CypherGlot currently requires ln(...) in the supported read subset to use an explicit AS alias."
                )
            ln_expr = ln_match.group("expr").strip()
            field_match = re.fullmatch(
                r"(?P<alias>[A-Za-z_][A-Za-z0-9_]*)\.(?P<field>[A-Za-z_][A-Za-z0-9_]*)",
                ln_expr,
            )
            if field_match is not None:
                if field_match.group("alias") not in allowed_aliases:
                    raise ValueError(
                        "CypherGlot currently supports ln(...) in the supported read subset only over admitted field projections or scalar literal/parameter inputs."
                    )
            else:
                try:
                    _parse_literal(ln_expr)
                except ValueError as exc:
                    raise ValueError(
                        "CypherGlot currently supports ln(...) in the supported read subset only over admitted field projections or scalar literal/parameter inputs."
                    ) from exc
            if output_alias in projected_output_kinds:
                raise ValueError(
                    f"CypherGlot currently does not allow duplicate RETURN output alias {output_alias!r} in the supported read subset."
                )
            projected_output_kinds[output_alias] = "scalar"
            continue

        log_match = re.fullmatch(
            r"log\s*\(\s*(?P<expr>.+?)\s*\)",
            expression_text,
            flags=re.IGNORECASE,
        )
        if log_match is not None:
            if output_alias is None:
                raise ValueError(
                    "CypherGlot currently requires log(...) in the supported read subset to use an explicit AS alias."
                )
            log_expr = log_match.group("expr").strip()
            field_match = re.fullmatch(
                r"(?P<alias>[A-Za-z_][A-Za-z0-9_]*)\.(?P<field>[A-Za-z_][A-Za-z0-9_]*)",
                log_expr,
            )
            if field_match is not None:
                if field_match.group("alias") not in allowed_aliases:
                    raise ValueError(
                        "CypherGlot currently supports log(...) in the supported read subset only over admitted field projections or scalar literal/parameter inputs."
                    )
            else:
                try:
                    _parse_literal(log_expr)
                except ValueError as exc:
                    raise ValueError(
                        "CypherGlot currently supports log(...) in the supported read subset only over admitted field projections or scalar literal/parameter inputs."
                    ) from exc
            if output_alias in projected_output_kinds:
                raise ValueError(
                    f"CypherGlot currently does not allow duplicate RETURN output alias {output_alias!r} in the supported read subset."
                )
            projected_output_kinds[output_alias] = "scalar"
            continue

        radians_match = re.fullmatch(
            r"radians\s*\(\s*(?P<expr>.+?)\s*\)",
            expression_text,
            flags=re.IGNORECASE,
        )
        if radians_match is not None:
            if output_alias is None:
                raise ValueError(
                    "CypherGlot currently requires radians(...) in the supported read subset to use an explicit AS alias."
                )
            radians_expr = radians_match.group("expr").strip()
            field_match = re.fullmatch(
                r"(?P<alias>[A-Za-z_][A-Za-z0-9_]*)\.(?P<field>[A-Za-z_][A-Za-z0-9_]*)",
                radians_expr,
            )
            if field_match is not None:
                if field_match.group("alias") not in allowed_aliases:
                    raise ValueError(
                        "CypherGlot currently supports radians(...) in the supported read subset only over admitted field projections or scalar literal/parameter inputs."
                    )
            else:
                try:
                    _parse_literal(radians_expr)
                except ValueError as exc:
                    raise ValueError(
                        "CypherGlot currently supports radians(...) in the supported read subset only over admitted field projections or scalar literal/parameter inputs."
                    ) from exc
            if output_alias in projected_output_kinds:
                raise ValueError(
                    f"CypherGlot currently does not allow duplicate RETURN output alias {output_alias!r} in the supported read subset."
                )
            projected_output_kinds[output_alias] = "scalar"
            continue

        degrees_match = re.fullmatch(
            r"degrees\s*\(\s*(?P<expr>.+?)\s*\)",
            expression_text,
            flags=re.IGNORECASE,
        )
        if degrees_match is not None:
            if output_alias is None:
                raise ValueError(
                    "CypherGlot currently requires degrees(...) in the supported read subset to use an explicit AS alias."
                )
            degrees_expr = degrees_match.group("expr").strip()
            field_match = re.fullmatch(
                r"(?P<alias>[A-Za-z_][A-Za-z0-9_]*)\.(?P<field>[A-Za-z_][A-Za-z0-9_]*)",
                degrees_expr,
            )
            if field_match is not None:
                if field_match.group("alias") not in allowed_aliases:
                    raise ValueError(
                        "CypherGlot currently supports degrees(...) in the supported read subset only over admitted field projections or scalar literal/parameter inputs."
                    )
            else:
                try:
                    _parse_literal(degrees_expr)
                except ValueError as exc:
                    raise ValueError(
                        "CypherGlot currently supports degrees(...) in the supported read subset only over admitted field projections or scalar literal/parameter inputs."
                    ) from exc
            if output_alias in projected_output_kinds:
                raise ValueError(
                    f"CypherGlot currently does not allow duplicate RETURN output alias {output_alias!r} in the supported read subset."
                )
            projected_output_kinds[output_alias] = "scalar"
            continue

        log10_match = re.fullmatch(
            r"log10\s*\(\s*(?P<expr>.+?)\s*\)",
            expression_text,
            flags=re.IGNORECASE,
        )
        if log10_match is not None:
            if output_alias is None:
                raise ValueError(
                    "CypherGlot currently requires log10(...) in the supported read subset to use an explicit AS alias."
                )
            log10_expr = log10_match.group("expr").strip()
            field_match = re.fullmatch(
                r"(?P<alias>[A-Za-z_][A-Za-z0-9_]*)\.(?P<field>[A-Za-z_][A-Za-z0-9_]*)",
                log10_expr,
            )
            if field_match is not None:
                if field_match.group("alias") not in allowed_aliases:
                    raise ValueError(
                        "CypherGlot currently supports log10(...) in the supported read subset only over admitted field projections or scalar literal/parameter inputs."
                    )
            else:
                try:
                    _parse_literal(log10_expr)
                except ValueError as exc:
                    raise ValueError(
                        "CypherGlot currently supports log10(...) in the supported read subset only over admitted field projections or scalar literal/parameter inputs."
                    ) from exc
            if output_alias in projected_output_kinds:
                raise ValueError(
                    f"CypherGlot currently does not allow duplicate RETURN output alias {output_alias!r} in the supported read subset."
                )
            projected_output_kinds[output_alias] = "scalar"
            continue

        to_string_match = re.fullmatch(
            r"tostring\s*\(\s*(?P<expr>.+?)\s*\)",
            expression_text,
            flags=re.IGNORECASE,
        )
        if to_string_match is not None:
            if output_alias is None:
                raise ValueError(
                    "CypherGlot currently requires toString(...) in the supported read subset to use an explicit AS alias."
                )
            to_string_expr = to_string_match.group("expr").strip()
            field_match = re.fullmatch(
                r"(?P<alias>[A-Za-z_][A-Za-z0-9_]*)\.(?P<field>[A-Za-z_][A-Za-z0-9_]*)",
                to_string_expr,
            )
            if field_match is not None:
                if field_match.group("alias") not in allowed_aliases:
                    raise ValueError(
                        "CypherGlot currently supports toString(...) in the supported read subset only over admitted field projections or scalar literal/parameter inputs."
                    )
            else:
                try:
                    _parse_literal(to_string_expr)
                except ValueError as exc:
                    raise ValueError(
                        "CypherGlot currently supports toString(...) in the supported read subset only over admitted field projections or scalar literal/parameter inputs."
                    ) from exc
            if output_alias in projected_output_kinds:
                raise ValueError(
                    f"CypherGlot currently does not allow duplicate RETURN output alias {output_alias!r} in the supported read subset."
                )
            projected_output_kinds[output_alias] = "scalar"
            continue

        to_integer_match = re.fullmatch(
            r"tointeger\s*\(\s*(?P<expr>.+?)\s*\)",
            expression_text,
            flags=re.IGNORECASE,
        )
        if to_integer_match is not None:
            if output_alias is None:
                raise ValueError(
                    "CypherGlot currently requires toInteger(...) in the supported read subset to use an explicit AS alias."
                )
            to_integer_expr = to_integer_match.group("expr").strip()
            field_match = re.fullmatch(
                r"(?P<alias>[A-Za-z_][A-Za-z0-9_]*)\.(?P<field>[A-Za-z_][A-Za-z0-9_]*)",
                to_integer_expr,
            )
            if field_match is not None:
                if field_match.group("alias") not in allowed_aliases:
                    raise ValueError(
                        "CypherGlot currently supports toInteger(...) in the supported read subset only over admitted field projections or scalar literal/parameter inputs."
                    )
            else:
                try:
                    _parse_literal(to_integer_expr)
                except ValueError as exc:
                    raise ValueError(
                        "CypherGlot currently supports toInteger(...) in the supported read subset only over admitted field projections or scalar literal/parameter inputs."
                    ) from exc
            if output_alias in projected_output_kinds:
                raise ValueError(
                    f"CypherGlot currently does not allow duplicate RETURN output alias {output_alias!r} in the supported read subset."
                )
            projected_output_kinds[output_alias] = "scalar"
            continue

        to_float_match = re.fullmatch(
            r"tofloat\s*\(\s*(?P<expr>.+?)\s*\)",
            expression_text,
            flags=re.IGNORECASE,
        )
        if to_float_match is not None:
            if output_alias is None:
                raise ValueError(
                    "CypherGlot currently requires toFloat(...) in the supported read subset to use an explicit AS alias."
                )
            to_float_expr = to_float_match.group("expr").strip()
            field_match = re.fullmatch(
                r"(?P<alias>[A-Za-z_][A-Za-z0-9_]*)\.(?P<field>[A-Za-z_][A-Za-z0-9_]*)",
                to_float_expr,
            )
            if field_match is not None:
                if field_match.group("alias") not in allowed_aliases:
                    raise ValueError(
                        "CypherGlot currently supports toFloat(...) in the supported read subset only over admitted field projections or scalar literal/parameter inputs."
                    )
            else:
                try:
                    _parse_literal(to_float_expr)
                except ValueError as exc:
                    raise ValueError(
                        "CypherGlot currently supports toFloat(...) in the supported read subset only over admitted field projections or scalar literal/parameter inputs."
                    ) from exc
            if output_alias in projected_output_kinds:
                raise ValueError(
                    f"CypherGlot currently does not allow duplicate RETURN output alias {output_alias!r} in the supported read subset."
                )
            projected_output_kinds[output_alias] = "scalar"
            continue

        to_boolean_match = re.fullmatch(
            r"toboolean\s*\(\s*(?P<expr>.+?)\s*\)",
            expression_text,
            flags=re.IGNORECASE,
        )
        if to_boolean_match is not None:
            if output_alias is None:
                raise ValueError(
                    "CypherGlot currently requires toBoolean(...) in the supported read subset to use an explicit AS alias."
                )
            to_boolean_expr = to_boolean_match.group("expr").strip()
            field_match = re.fullmatch(
                r"(?P<alias>[A-Za-z_][A-Za-z0-9_]*)\.(?P<field>[A-Za-z_][A-Za-z0-9_]*)",
                to_boolean_expr,
            )
            if field_match is not None:
                if field_match.group("alias") not in allowed_aliases:
                    raise ValueError(
                        "CypherGlot currently supports toBoolean(...) in the supported read subset only over admitted field projections or scalar literal/parameter inputs."
                    )
            else:
                try:
                    _parse_literal(to_boolean_expr)
                except ValueError as exc:
                    raise ValueError(
                        "CypherGlot currently supports toBoolean(...) in the supported read subset only over admitted field projections or scalar literal/parameter inputs."
                    ) from exc
            if output_alias in projected_output_kinds:
                raise ValueError(
                    f"CypherGlot currently does not allow duplicate RETURN output alias {output_alias!r} in the supported read subset."
                )
            projected_output_kinds[output_alias] = "scalar"
            continue

        try:
            left_text, operator, value_text = _split_predicate_comparison(expression_text)
        except ValueError:
            pass
        else:
            if output_alias is None:
                raise ValueError(
                    "CypherGlot currently requires predicate RETURN items in the supported read subset to use an explicit AS alias."
                )
            size_match = re.fullmatch(
                r"size\s*\(\s*(?P<expr>.+?)\s*\)",
                left_text.strip(),
                flags=re.IGNORECASE,
            )
            if size_match is not None:
                size_expr = size_match.group("expr").strip()
                id_match = re.fullmatch(
                    r"id\s*\(\s*(?P<alias>[A-Za-z_][A-Za-z0-9_]*)\s*\)",
                    size_expr,
                    flags=re.IGNORECASE,
                )
                if id_match is not None:
                    if id_match.group("alias") not in allowed_aliases:
                        raise ValueError(
                            "CypherGlot currently supports predicate RETURN items in the supported read subset only as alias.field OP value, id(alias) OP value, type(rel_alias) OP value, or size(admitted_input) OP value over admitted bindings."
                        )
                else:
                    type_match = re.fullmatch(
                        r"type\s*\(\s*(?P<alias>[A-Za-z_][A-Za-z0-9_]*)\s*\)",
                        size_expr,
                        flags=re.IGNORECASE,
                    )
                    if type_match is not None:
                        if type_match.group("alias") not in relationship_aliases:
                            raise ValueError(
                                "CypherGlot currently supports predicate RETURN items in the supported read subset only as alias.field OP value, id(alias) OP value, type(rel_alias) OP value, or size(admitted_input) OP value over admitted bindings."
                            )
                    else:
                        field_match = re.fullmatch(
                            r"(?P<alias>[A-Za-z_][A-Za-z0-9_]*)\.(?P<field>[A-Za-z_][A-Za-z0-9_]*)",
                            size_expr,
                        )
                        if field_match is None or field_match.group("alias") not in allowed_aliases:
                            raise ValueError(
                                "CypherGlot currently supports predicate RETURN items in the supported read subset only as alias.field OP value, id(alias) OP value, type(rel_alias) OP value, or size(admitted_input) OP value over admitted bindings."
                            )
                if operator not in {"IS NULL", "IS NOT NULL"}:
                    try:
                        _parse_literal(value_text.strip())
                    except ValueError as exc:
                        raise ValueError(
                            "CypherGlot currently supports predicate RETURN items in the supported read subset only as alias.field OP literal_or_parameter, id(alias) OP literal_or_parameter, type(rel_alias) OP literal_or_parameter, or size(admitted_input) OP literal_or_parameter over admitted bindings."
                        ) from exc
                elif value_text.strip():
                    raise ValueError(
                        "CypherGlot null predicate RETURN items cannot include a trailing literal value."
                    )
                if output_alias in projected_output_kinds:
                    raise ValueError(
                        f"CypherGlot currently does not allow duplicate RETURN output alias {output_alias!r} in the supported read subset."
                    )
                projected_output_kinds[output_alias] = "scalar"
                continue
            id_match = re.fullmatch(
                r"id\s*\(\s*(?P<alias>[A-Za-z_][A-Za-z0-9_]*)\s*\)",
                left_text.strip(),
                flags=re.IGNORECASE,
            )
            if id_match is not None:
                if id_match.group("alias") not in allowed_aliases:
                    raise ValueError(
                        "CypherGlot currently supports predicate RETURN items in the supported read subset only as alias.field OP value, id(alias) OP value, type(rel_alias) OP value, or size(admitted_input) OP value over admitted bindings."
                    )
                if operator not in {"IS NULL", "IS NOT NULL"}:
                    try:
                        _parse_literal(value_text.strip())
                    except ValueError as exc:
                        raise ValueError(
                            "CypherGlot currently supports predicate RETURN items in the supported read subset only as alias.field OP literal_or_parameter, id(alias) OP literal_or_parameter, type(rel_alias) OP literal_or_parameter, or size(admitted_input) OP literal_or_parameter over admitted bindings."
                        ) from exc
                elif value_text.strip():
                    raise ValueError(
                        "CypherGlot null predicate RETURN items cannot include a trailing literal value."
                    )
                if output_alias in projected_output_kinds:
                    raise ValueError(
                        f"CypherGlot currently does not allow duplicate RETURN output alias {output_alias!r} in the supported read subset."
                    )
                projected_output_kinds[output_alias] = "scalar"
                continue
            type_match = re.fullmatch(
                r"type\s*\(\s*(?P<alias>[A-Za-z_][A-Za-z0-9_]*)\s*\)",
                left_text.strip(),
                flags=re.IGNORECASE,
            )
            if type_match is not None:
                if type_match.group("alias") not in relationship_aliases:
                    raise ValueError(
                        "CypherGlot currently supports predicate RETURN items in the supported read subset only as alias.field OP value, id(alias) OP value, type(rel_alias) OP value, or size(admitted_input) OP value over admitted bindings."
                    )
                if operator not in {"IS NULL", "IS NOT NULL"}:
                    try:
                        _parse_literal(value_text.strip())
                    except ValueError as exc:
                        raise ValueError(
                            "CypherGlot currently supports predicate RETURN items in the supported read subset only as alias.field OP literal_or_parameter, id(alias) OP literal_or_parameter, type(rel_alias) OP literal_or_parameter, or size(admitted_input) OP literal_or_parameter over admitted bindings."
                        ) from exc
                elif value_text.strip():
                    raise ValueError(
                        "CypherGlot null predicate RETURN items cannot include a trailing literal value."
                    )
                if output_alias in projected_output_kinds:
                    raise ValueError(
                        f"CypherGlot currently does not allow duplicate RETURN output alias {output_alias!r} in the supported read subset."
                    )
                projected_output_kinds[output_alias] = "scalar"
                continue
            field_match = re.fullmatch(
                r"(?P<alias>[A-Za-z_][A-Za-z0-9_]*)\.(?P<field>[A-Za-z_][A-Za-z0-9_]*)",
                left_text.strip(),
            )
            if field_match is None or field_match.group("alias") not in allowed_aliases:
                raise ValueError(
                    "CypherGlot currently supports predicate RETURN items in the supported read subset only as alias.field OP value, id(alias) OP value, type(rel_alias) OP value, or size(admitted_input) OP value over admitted bindings."
                )
            if operator not in {"IS NULL", "IS NOT NULL"}:
                try:
                    _parse_literal(value_text.strip())
                except ValueError as exc:
                    raise ValueError(
                        "CypherGlot currently supports predicate RETURN items in the supported read subset only as alias.field OP literal_or_parameter, id(alias) OP literal_or_parameter, type(rel_alias) OP literal_or_parameter, or size(admitted_input) OP literal_or_parameter over admitted bindings."
                    ) from exc
            elif value_text.strip():
                raise ValueError(
                    "CypherGlot null predicate RETURN items cannot include a trailing literal value."
                )
            if output_alias in projected_output_kinds:
                raise ValueError(
                    f"CypherGlot currently does not allow duplicate RETURN output alias {output_alias!r} in the supported read subset."
                )
            projected_output_kinds[output_alias] = "scalar"
            continue

        try:
            _parse_literal(expression_text)
        except ValueError:
            pass
        else:
            if output_alias is None:
                raise ValueError(
                    "CypherGlot currently requires scalar literal and parameter RETURN items in the supported read subset to use an explicit AS alias."
                )
            if output_alias in projected_output_kinds:
                raise ValueError(
                    f"CypherGlot currently does not allow duplicate RETURN output alias {output_alias!r} in the supported read subset."
                )
            projected_output_kinds[output_alias] = "scalar"
            continue

        field_match = re.fullmatch(
            r"(?P<alias>[A-Za-z_][A-Za-z0-9_]*)\.(?P<field>[A-Za-z_][A-Za-z0-9_]*)",
            expression_text,
        )
        if field_match is not None:
            if field_match.group("alias") not in allowed_aliases:
                raise ValueError(
                    "CypherGlot currently supports RETURN alias.field for admitted entity bindings and RETURN entity_alias for admitted whole-entity bindings in the supported read subset."
                )
            output_name = output_alias or expression_text
            if output_name in projected_output_kinds:
                raise ValueError(
                    f"CypherGlot currently does not allow duplicate RETURN output alias {output_name!r} in the supported read subset."
                )
            projected_output_kinds[output_name] = "field"
            continue

        if expression_text not in allowed_aliases:
            raise ValueError(
                "CypherGlot currently supports RETURN alias.field for admitted entity bindings, RETURN entity_alias for admitted whole-entity bindings, id(alias) with optional AS output_alias, type(rel_alias) with optional AS output_alias, properties(entity_alias) with optional AS output_alias, labels(node_alias) with optional AS output_alias, keys(entity_alias) with optional AS output_alias, startNode(rel_alias) with optional AS output_alias, startNode(rel_alias).field with optional AS output_alias, endNode(rel_alias) with optional AS output_alias, endNode(rel_alias).field with optional AS output_alias, sum(alias.field) AS output_alias, avg(alias.field) AS output_alias, min(alias.field) AS output_alias, max(alias.field) AS output_alias, size(admitted_input) AS output_alias, lower(admitted_input) AS output_alias, upper(admitted_input) AS output_alias, trim(admitted_input) AS output_alias, ltrim(admitted_input) AS output_alias, rtrim(admitted_input) AS output_alias, reverse(admitted_input) AS output_alias, coalesce(admitted_input, literal_or_parameter) AS output_alias, replace(admitted_input, literal_or_parameter, literal_or_parameter) AS output_alias, left(admitted_input, literal_or_parameter) AS output_alias, right(admitted_input, literal_or_parameter) AS output_alias, split(admitted_input, literal_or_parameter) AS output_alias, substring(admitted_input, literal_or_parameter) AS output_alias, substring(admitted_input, literal_or_parameter, literal_or_parameter) AS output_alias, abs(admitted_input) AS output_alias, sign(admitted_input) AS output_alias, round(admitted_input) AS output_alias, ceil(admitted_input) AS output_alias, floor(admitted_input) AS output_alias, sqrt(admitted_input) AS output_alias, exp(admitted_input) AS output_alias, toString(admitted_input) AS output_alias, toInteger(admitted_input) AS output_alias, toFloat(admitted_input) AS output_alias, toBoolean(admitted_input) AS output_alias, predicate admitted_input OP value AS output_alias, searched CASE WHEN admitted_predicate THEN admitted_result ELSE admitted_result END AS output_alias, and scalar_literal_or_parameter AS output_alias in the supported read subset."
            )
        output_name = output_alias or expression_text
        if output_name in projected_output_kinds:
            raise ValueError(
                f"CypherGlot currently does not allow duplicate RETURN output alias {output_name!r} in the supported read subset."
            )
        projected_output_kinds[output_name] = "entity"

    for item in order_items:
        if item.field == "__value__":
            kind = projected_output_kinds.get(item.alias)
            if kind is None:
                raise ValueError(
                    "CypherGlot currently supports ORDER BY alias.field or ORDER BY projected_alias in the supported read subset."
                )
            continue

        if item.alias not in allowed_aliases:
            raise ValueError(
                "CypherGlot currently supports ORDER BY alias.field only for admitted entity bindings in the supported read subset."
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
        update_ctx = updating_clauses[0]
        if return_ctx is not None:
            raise ValueError(
                "CypherGlot currently validates only CREATE and narrow MERGE statements in the "
                "write subset."
            )
        create_ctx = update_ctx.oC_Create()
        merge_ctx = update_ctx.oC_Merge()
        if create_ctx is None and merge_ctx is None:
            raise ValueError(
                "CypherGlot currently validates only CREATE and narrow MERGE statements in the "
                "write subset."
            )
        if merge_ctx is not None:
            if merge_ctx.oC_MergeAction():
                raise ValueError(
                    "CypherGlot currently validates MERGE only without ON CREATE or ON MATCH actions."
                )
            merge_pattern_text = _context_text(result, merge_ctx.oC_PatternPart())
            if _looks_like_relationship_pattern(merge_pattern_text):
                left_text, relationship_text, right_text, direction = (
                    _split_relationship_pattern(merge_pattern_text)
                )
                _parse_node_pattern(
                    left_text,
                    require_label=True,
                    default_alias="__humem_validate_merge_left_node",
                )
                _parse_relationship_pattern(relationship_text, direction)
                _parse_node_pattern(
                    right_text,
                    require_label=True,
                    default_alias="__humem_validate_merge_right_node",
                )
            else:
                _parse_node_pattern(
                    _unwrap_node_pattern(merge_pattern_text),
                    require_label=True,
                    default_alias="__humem_validate_merge_node",
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
        read_ctx = reading_clauses[0]
        update_ctx = updating_clauses[0]
        if read_ctx.oC_Match() is None or return_ctx is not None:
            raise ValueError(
                "CypherGlot currently validates only MATCH ... SET, MATCH ... "
                "CREATE, and narrow MATCH ... DELETE statements in the mixed "
                "read-write subset."
            )

        if (
            update_ctx.oC_Set() is None
            and update_ctx.oC_Create() is None
            and update_ctx.oC_Merge() is None
            and update_ctx.oC_Delete() is None
        ):
            raise ValueError(
                "CypherGlot currently validates only MATCH ... SET, MATCH ... "
                "CREATE, MATCH ... MERGE, and narrow MATCH ... DELETE statements in the mixed "
                "read-write subset."
            )

        merge_ctx = update_ctx.oC_Merge()
        if merge_ctx is not None:
            if merge_ctx.oC_MergeAction():
                raise ValueError(
                    "CypherGlot currently validates MERGE only without ON CREATE or ON MATCH actions."
                )
            merge_pattern_text = _context_text(result, merge_ctx.oC_PatternPart())
            if not _looks_like_relationship_pattern(merge_pattern_text):
                raise ValueError(
                    "CypherGlot currently validates MATCH ... MERGE only for one directed relationship pattern in the MERGE clause."
                )
            match_pattern_text = _context_text(result, read_ctx.oC_Match().oC_Pattern())
            match_patterns = _split_comma_separated(match_pattern_text)
            left_text, relationship_text, right_text, direction = (
                _split_relationship_pattern(merge_pattern_text)
            )
            left = _parse_node_pattern(
                left_text,
                default_alias="__humem_validate_match_merge_left_node",
            )
            _parse_relationship_pattern(relationship_text, direction)
            right = _parse_node_pattern(
                right_text,
                default_alias="__humem_validate_match_merge_right_node",
            )
            if len(match_patterns) == 1 and _looks_like_relationship_pattern(
                match_patterns[0]
            ):
                _validate_traversal_write_endpoints(
                    _extract_single_match_source_nodes(result, read_ctx.oC_Match()),
                    left,
                    right,
                    allow_one_new_endpoint=True,
                )
            elif len(match_patterns) == 1:
                match_node = _parse_node_pattern(
                    _unwrap_node_pattern(match_patterns[0]),
                    default_alias="__humem_validate_match_merge_node",
                )
                _validate_match_merge_relationship_endpoints(
                    match_node,
                    left,
                    right,
                )
            else:
                if len(match_patterns) != 2 or any(
                    _looks_like_relationship_pattern(pattern)
                    for pattern in match_patterns
                ):
                    raise ValueError(
                        "CypherGlot currently validates MATCH ... MERGE only with two disconnected matched node patterns before MERGE, or one matched relationship or fixed-length chain source whose node aliases are reused by MERGE."
                    )
                left_match = _parse_node_pattern(
                    _unwrap_node_pattern(match_patterns[0]),
                    default_alias="__humem_validate_match_merge_left_match_node",
                )
                right_match = _parse_node_pattern(
                    _unwrap_node_pattern(match_patterns[1]),
                    default_alias="__humem_validate_match_merge_right_match_node",
                )
                _validate_match_create_relationship_between_nodes_endpoints(
                    left_match,
                    right_match,
                    left,
                    right,
                )

        create_ctx = update_ctx.oC_Create()
        if create_ctx is not None:
            create_pattern_text = _context_text(result, create_ctx.oC_Pattern())
            if _looks_like_relationship_pattern(create_pattern_text):
                match_pattern_text = _context_text(result, read_ctx.oC_Match().oC_Pattern())
                match_patterns = _split_comma_separated(match_pattern_text)
                if len(match_patterns) == 1 and _looks_like_relationship_pattern(
                    match_patterns[0]
                ):
                    left_text, _relationship_text, right_text, _direction = (
                        _split_relationship_pattern(create_pattern_text)
                    )
                    left = _parse_node_pattern(
                        left_text,
                        default_alias="__humem_validate_match_create_left_node",
                    )
                    right = _parse_node_pattern(
                        right_text,
                        default_alias="__humem_validate_match_create_right_node",
                    )
                    _validate_traversal_write_endpoints(
                        _extract_single_match_source_nodes(result, read_ctx.oC_Match()),
                        left,
                        right,
                        allow_one_new_endpoint=True,
                    )

        _validate_match_pattern_shape(
            result,
            read_ctx.oC_Match(),
            allow_two_node_disconnected=(
                update_ctx.oC_Create() is not None or update_ctx.oC_Merge() is not None
            ),
            allow_multi_hop=(
                update_ctx.oC_Create() is not None or update_ctx.oC_Merge() is not None
            ),
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
