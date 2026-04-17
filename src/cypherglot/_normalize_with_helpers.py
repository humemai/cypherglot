from __future__ import annotations

import re
from typing import Literal, cast

from ._normalize_support import (
    _SIZE_PREDICATE_FIELD_PREFIX,
    _find_top_level_keyword,
    _looks_like_relationship_pattern,
    _ParameterRef,
    _parse_boolean_predicate_groups,
    _parse_literal,
    _parse_node_pattern,
    _parse_predicates,
    _parse_relationship_chain_segment,
    _parse_relationship_pattern,
    _split_comma_separated,
    _split_predicate_comparison,
    _split_relationship_pattern,
    _split_return_clause,
    _unwrap_node_pattern,
    CypherValue,
    NodePattern,
    OrderItem,
    Predicate,
    RelationshipPattern,
)
from .normalize import (
    NormalizedMatchChain,
    NormalizedMatchNode,
    NormalizedMatchRelationship,
    NormalizedMatchWithReturn,
    NormalizedUnwind,
    WithBinding,
    WithCaseSpec,
    WithCaseWhen,
    WithOrderItem,
    WithPredicate,
    WithReturnItem,
    _context_text,
    _source_alias_kinds,
    _validate_normalized_match_predicates,
)
from .parser import CypherParseResult


_WITH_AGGREGATE_RETURN_KINDS = {"count", "sum", "avg", "min", "max"}
_WITH_UNARY_FUNCTION_KIND_BY_NAME = {
    "lower": "lower",
    "upper": "upper",
    "trim": "trim",
    "ltrim": "ltrim",
    "rtrim": "rtrim",
    "reverse": "reverse",
    "abs": "abs",
    "sign": "sign",
    "round": "round",
    "ceil": "ceil",
    "floor": "floor",
    "sqrt": "sqrt",
    "exp": "exp",
    "sin": "sin",
    "cos": "cos",
    "tan": "tan",
    "asin": "asin",
    "acos": "acos",
    "atan": "atan",
    "ln": "ln",
    "log": "log",
    "log10": "log10",
    "radians": "radians",
    "degrees": "degrees",
    "tostring": "to_string",
    "tointeger": "to_integer",
    "tofloat": "to_float",
    "toboolean": "to_boolean",
}
_WITH_FIELD_RE = re.compile(
    r"(?P<alias>[A-Za-z_][A-Za-z0-9_]*)\.(?P<field>[A-Za-z_][A-Za-z0-9_]*)"
)
_WITH_ALIAS_RE = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")


def _parse_match_chain(
    result: CypherParseResult,
    pattern_element,
) -> tuple[tuple[NodePattern, ...], tuple[RelationshipPattern, ...]]:
    nodes = [
        _parse_node_pattern(
            _unwrap_node_pattern(_context_text(result, pattern_element.oC_NodePattern())),
            default_alias="__humem_match_chain_node_0",
        )
    ]
    relationships: list[RelationshipPattern] = []

    for index, chain_ctx in enumerate(pattern_element.oC_PatternElementChain()):
        relationships.append(
            _parse_relationship_chain_segment(
                _context_text(result, chain_ctx.oC_RelationshipPattern())
            )
        )
        nodes.append(
            _parse_node_pattern(
                _unwrap_node_pattern(_context_text(result, chain_ctx.oC_NodePattern())),
                default_alias=f"__humem_match_chain_node_{index + 1}",
            )
        )

    return tuple(nodes), tuple(relationships)


def _normalize_match_with_return(
    result: CypherParseResult,
    multi_part_query,
) -> NormalizedMatchWithReturn:
    reading_clause = multi_part_query.oC_ReadingClause()[0]
    match_ctx = reading_clause.oC_Match()
    assert match_ctx is not None
    source = _normalize_match_source(result, match_ctx)

    alias_kinds = _source_alias_kinds(source)
    with_ctx = multi_part_query.oC_With()[0]
    bindings = _parse_with_bindings(result, with_ctx, alias_kinds)
    predicates: tuple[WithPredicate, ...] = ()
    if with_ctx.oC_Where() is not None:
        predicates = _parse_with_predicates(
            _context_text(result, with_ctx.oC_Where().oC_Expression()),
            bindings,
        )

    final_query = multi_part_query.oC_SinglePartQuery()
    return_ctx = final_query.oC_Return()
    assert return_ctx is not None
    projection_text = _context_text(result, return_ctx.oC_ProjectionBody())
    return_text, order_by, limit, distinct, skip = _split_return_clause(projection_text)
    returns = _parse_with_return_items(return_text, bindings)
    with_order_by = _parse_with_order_items(order_by, bindings, returns)

    return NormalizedMatchWithReturn(
        kind="with",
        source=source,
        bindings=bindings,
        predicates=predicates,
        returns=returns,
        order_by=with_order_by,
        limit=limit,
        distinct=distinct,
        skip=skip,
    )


def _normalize_match_source(
    result: CypherParseResult,
    match_ctx,
) -> NormalizedMatchNode | NormalizedMatchRelationship | NormalizedMatchChain:
    pattern_text = _context_text(result, match_ctx.oC_Pattern())
    predicates: tuple[Predicate, ...] = ()
    where_ctx = match_ctx.oC_Where()
    if where_ctx is not None:
        predicates = _parse_predicates(_context_text(result, where_ctx.oC_Expression()))

    pattern_element = (
        match_ctx.oC_Pattern().oC_PatternPart()[0].oC_AnonymousPatternPart().oC_PatternElement()
    )
    if len(pattern_element.oC_PatternElementChain()) > 1:
        nodes, relationships = _parse_match_chain(result, pattern_element)
        alias_kinds: dict[str, Literal["node", "relationship"]] = {
            node.alias: "node" for node in nodes
        }
        for relationship in relationships:
            if relationship.alias is not None:
                alias_kinds[relationship.alias] = "relationship"
        _validate_normalized_match_predicates(predicates, alias_kinds=alias_kinds)
        return NormalizedMatchChain(
            kind="match",
            pattern_kind="relationship_chain",
            nodes=nodes,
            relationships=relationships,
            predicates=predicates,
            returns=(),
        )

    if _looks_like_relationship_pattern(pattern_text):
        left_text, relationship_text, right_text, direction = _split_relationship_pattern(
            pattern_text
        )
        left = _parse_node_pattern(left_text, default_alias="__humem_match_left_node")
        relationship = _parse_relationship_pattern(relationship_text, direction)
        right = _parse_node_pattern(
            right_text,
            default_alias="__humem_match_right_node",
        )
        _validate_normalized_match_predicates(
            predicates,
            alias_kinds={
                left.alias: "node",
                right.alias: "node",
                **(
                    {relationship.alias: "relationship"}
                    if relationship.alias is not None
                    else {}
                ),
            },
        )
        return NormalizedMatchRelationship(
            kind="match",
            pattern_kind="relationship",
            left=left,
            relationship=relationship,
            right=right,
            predicates=predicates,
            returns=(),
        )

    node = _parse_node_pattern(
        _unwrap_node_pattern(pattern_text),
        default_alias="__humem_match_node",
    )
    _validate_normalized_match_predicates(
        predicates,
        alias_kinds={node.alias: "node"},
    )
    return NormalizedMatchNode(
        kind="match",
        pattern_kind="node",
        node=node,
        predicates=predicates,
        returns=(),
    )


def _parse_with_bindings(
    result: CypherParseResult,
    with_ctx,
    alias_kinds: dict[str, Literal["node", "relationship"]],
) -> tuple[WithBinding, ...]:
    projection_body = with_ctx.oC_ProjectionBody()
    if (
        projection_body.DISTINCT() is not None
        or projection_body.oC_Order() is not None
        or projection_body.oC_Skip() is not None
        or projection_body.oC_Limit() is not None
    ):
        raise ValueError(
            "HumemCypher v0 WITH support currently admits only simple passthrough "
            "projection items without DISTINCT, ORDER BY, SKIP/OFFSET, or LIMIT."
        )

    bindings: list[WithBinding] = []
    seen_output_aliases: set[str] = set()
    for item_ctx in projection_body.oC_ProjectionItems().oC_ProjectionItem():
        expression_text = _context_text(result, item_ctx.oC_Expression()).strip()
        output_alias = (
            _context_text(result, item_ctx.oC_Variable()).strip()
            if item_ctx.oC_Variable() is not None
            else expression_text
        )
        if output_alias in seen_output_aliases:
            raise ValueError(
                f"HumemCypher v0 WITH support does not allow duplicate output alias {output_alias!r}."
            )
        seen_output_aliases.add(output_alias)

        field_match = _match_with_field(expression_text)
        if field_match is not None:
            source_alias = field_match.group("alias")
            if source_alias not in alias_kinds:
                raise ValueError(
                    "HumemCypher v0 WITH support currently admits scalar rebinding "
                    "only from known MATCH aliases."
                )
            if item_ctx.oC_Variable() is None:
                raise ValueError(
                    "HumemCypher v0 WITH scalar rebinding currently requires an AS alias."
                )
            bindings.append(
                WithBinding(
                    source_alias=source_alias,
                    output_alias=output_alias,
                    binding_kind="scalar",
                    alias_kind=alias_kinds[source_alias],
                    source_field=field_match.group("field"),
                )
            )
            continue

        if expression_text not in alias_kinds:
            raise ValueError(
                "HumemCypher v0 WITH support currently admits only passthrough "
                "variable items such as WITH u or WITH u AS person, plus simple "
                "scalar rebinding such as WITH u.name AS name."
            )
        bindings.append(
            WithBinding(
                source_alias=expression_text,
                output_alias=output_alias,
                binding_kind="entity",
                alias_kind=alias_kinds[expression_text],
            )
        )
    return tuple(bindings)


def _parse_with_predicates(
    text: str,
    bindings: tuple[WithBinding, ...],
) -> tuple[WithPredicate, ...]:
    binding_map = {binding.output_alias: binding for binding in bindings}
    predicates: list[WithPredicate] = []

    for disjunct_index, disjunct in enumerate(_parse_boolean_predicate_groups(text)):
        for item in disjunct:
            try:
                left_text, operator, value_text = _split_predicate_comparison(item)
            except ValueError as exc:
                raise ValueError(
                    "HumemCypher v0 WITH WHERE items must look like scalar_alias OP value "
                    "or entity_alias.field OP value."
                ) from exc

            parsed_value = _parse_with_predicate_value(operator, value_text)
            target_text = left_text.strip()

            id_alias = _match_with_id_alias(target_text)
            if id_alias is not None:
                _require_entity_binding(
                    binding_map,
                    id_alias,
                    "HumemCypher v0 WITH WHERE currently supports id(entity_alias) only for entity bindings.",
                )
                predicates.append(
                    WithPredicate(
                        kind="field",
                        alias=id_alias,
                        field="id",
                        operator=operator,
                        value=parsed_value,
                        disjunct_index=disjunct_index,
                    )
                )
                continue

            type_alias = _match_with_type_alias(target_text)
            if type_alias is not None:
                binding = _require_entity_binding(
                    binding_map,
                    type_alias,
                    "HumemCypher v0 WITH WHERE currently supports type(rel_alias) only for relationship entity bindings.",
                )
                if binding.alias_kind != "relationship":
                    raise ValueError(
                        "HumemCypher v0 WITH WHERE currently supports type(rel_alias) only for relationship entity bindings."
                    )
                predicates.append(
                    WithPredicate(
                        kind="field",
                        alias=type_alias,
                        field="type",
                        operator=operator,
                        value=parsed_value,
                        disjunct_index=disjunct_index,
                    )
                )
                continue

            size_expr = _match_with_size_expression(target_text)
            if size_expr is not None:
                item_value = _parse_with_size_input(size_expr, binding_map)
                if item_value is None:
                    raise ValueError(
                        "HumemCypher v0 WITH WHERE items must look like scalar_alias OP value, entity_alias.field OP value, id(entity_alias) OP value, type(rel_alias) OP value, or size(admitted_input) OP value."
                    )
                predicates.append(
                    WithPredicate(
                        kind="field",
                        alias=item_value.alias,
                        field=_predicate_field_for_size_item(item_value),
                        operator=operator,
                        value=parsed_value,
                        disjunct_index=disjunct_index,
                    )
                )
                continue

            field_match = _match_with_field(target_text)
            if field_match is not None:
                alias = field_match.group("alias")
                _require_entity_binding(
                    binding_map,
                    alias,
                    "HumemCypher v0 WITH WHERE currently supports entity_alias.field only for entity bindings.",
                )
                predicates.append(
                    WithPredicate(
                        kind="field",
                        alias=alias,
                        field=field_match.group("field"),
                        operator=operator,
                        value=parsed_value,
                        disjunct_index=disjunct_index,
                    )
                )
                continue

            binding = binding_map.get(target_text)
            if binding is None or binding.binding_kind != "scalar":
                raise ValueError(
                    "HumemCypher v0 WITH WHERE items must look like scalar_alias OP value, "
                    "entity_alias.field OP value, id(entity_alias) OP value, or "
                    "type(rel_alias) OP value."
                )
            predicates.append(
                WithPredicate(
                    kind="scalar",
                    alias=target_text,
                    operator=operator,
                    value=parsed_value,
                    disjunct_index=disjunct_index,
                )
            )

    if not predicates:
        raise ValueError("HumemCypher v0 WITH WHERE clauses cannot be empty.")
    return tuple(predicates)


def _parse_with_case_result_item(
    text: str,
    binding_map: dict[str, WithBinding],
) -> WithReturnItem:
    expression_text = text.strip()
    field_match = _match_with_field(expression_text)
    if field_match is not None:
        alias = field_match.group("alias")
        _require_entity_binding(
            binding_map,
            alias,
            "HumemCypher v0 searched CASE results currently support entity_alias.field only for admitted entity bindings.",
        )
        return WithReturnItem(kind="field", alias=alias, field=field_match.group("field"))

    binding = binding_map.get(expression_text)
    if binding is not None and binding.binding_kind == "scalar":
        return WithReturnItem(kind="scalar", alias=expression_text)

    try:
        literal_value = _parse_literal(expression_text)
    except ValueError as exc:
        raise ValueError(
            "HumemCypher v0 searched CASE results currently support only admitted entity-field projections, admitted scalar bindings, or scalar literal/parameter inputs in the WITH subset."
        ) from exc
    return WithReturnItem(kind="scalar_value", alias="__case__", value=literal_value)


def _parse_with_case_condition_item(
    text: str,
    binding_map: dict[str, WithBinding],
) -> WithReturnItem:
    left_text, operator, value_text = _split_predicate_comparison(text.strip())
    parsed_value = _parse_with_predicate_value(operator, value_text)
    target_text = left_text.strip()

    size_expr = _match_with_size_expression(target_text)
    if size_expr is not None:
        size_item = _parse_with_size_input(size_expr, binding_map)
        if size_item is None:
            raise ValueError(
                "HumemCypher v0 searched CASE WHEN size(...) conditions currently support only admitted entity-field projections or admitted scalar bindings in the WITH subset."
            )
        return WithReturnItem(
            kind="predicate",
            alias=size_item.alias,
            field=_predicate_field_for_size_item(size_item),
            operator=operator,
            value=parsed_value,
        )

    field_match = _match_with_field(target_text)
    if field_match is not None:
        alias = field_match.group("alias")
        _require_entity_binding(
            binding_map,
            alias,
            "HumemCypher v0 searched CASE WHEN conditions currently support entity_alias.field only for admitted entity bindings in the WITH subset.",
        )
        return WithReturnItem(
            kind="predicate",
            alias=alias,
            field=field_match.group("field"),
            operator=operator,
            value=parsed_value,
        )

    binding = binding_map.get(target_text)
    if binding is not None and binding.binding_kind == "scalar":
        return WithReturnItem(
            kind="predicate",
            alias=target_text,
            operator=operator,
            value=parsed_value,
        )

    raise ValueError(
        "HumemCypher v0 searched CASE WHEN conditions currently support only admitted entity-field or scalar-binding predicate surfaces in the WITH subset."
    )


def _parse_with_case_expression(
    text: str,
    binding_map: dict[str, WithBinding],
) -> WithCaseSpec | None:
    expression_text = text.strip()
    if re.match(r"case\b", expression_text, flags=re.IGNORECASE) is None:
        return None
    if re.fullmatch(r"case\s+.+\s+end", expression_text, flags=re.IGNORECASE) is None:
        raise ValueError(
            "HumemCypher v0 currently supports searched CASE expressions only in the form CASE WHEN ... THEN ... [WHEN ... THEN ...]* ELSE ... END in the WITH subset."
        )

    inner = expression_text[4:-3].strip()
    if re.match(r"when\b", inner, flags=re.IGNORECASE) is None:
        raise ValueError(
            "HumemCypher v0 currently supports only searched CASE expressions beginning with CASE WHEN ... in the WITH subset."
        )

    when_items: list[WithCaseWhen] = []
    cursor = 0
    while True:
        when_index = _find_top_level_keyword(inner, "WHEN", start=cursor)
        if when_index == -1:
            break
        then_index = _find_top_level_keyword(inner, "THEN", start=when_index + 4)
        if then_index == -1:
            raise ValueError(
                "HumemCypher v0 searched CASE expressions require THEN after every WHEN condition in the WITH subset."
            )
        next_when = _find_top_level_keyword(inner, "WHEN", start=then_index + 4)
        else_index = _find_top_level_keyword(inner, "ELSE", start=then_index + 4)
        if else_index == -1 and next_when == -1:
            raise ValueError(
                "HumemCypher v0 searched CASE expressions currently require an ELSE branch in the WITH subset."
            )

        condition_text = inner[when_index + 4:then_index].strip()
        if next_when != -1 and (else_index == -1 or next_when < else_index):
            result_text = inner[then_index + 4:next_when].strip()
            cursor = next_when
            final_else_text = None
        else:
            result_text = inner[then_index + 4:else_index].strip()
            final_else_text = inner[else_index + 4:].strip()
            cursor = len(inner)

        when_items.append(
            WithCaseWhen(
                condition=_parse_with_case_condition_item(condition_text, binding_map),
                result=_parse_with_case_result_item(result_text, binding_map),
            )
        )

        if final_else_text is not None:
            if not final_else_text:
                raise ValueError(
                    "HumemCypher v0 searched CASE expressions currently require a non-empty ELSE result in the WITH subset."
                )
            return WithCaseSpec(
                when_items=tuple(when_items),
                else_item=_parse_with_case_result_item(final_else_text, binding_map),
            )

    raise ValueError(
        "HumemCypher v0 searched CASE expressions currently require at least one WHEN ... THEN ... arm and a final ELSE branch in the WITH subset."
    )


def _parse_with_return_items(
    text: str,
    bindings: tuple[WithBinding, ...],
) -> tuple[WithReturnItem, ...]:
    binding_map = {binding.output_alias: binding for binding in bindings}
    items: list[WithReturnItem] = []
    seen_output_names: set[str] = set()

    for raw_item in _split_comma_separated(text):
        expression_text, output_alias = _split_with_projection_alias(raw_item.strip())

        case_spec = _parse_with_case_expression(expression_text, binding_map)
        if case_spec is not None:
            if output_alias is None:
                raise ValueError(
                    "HumemCypher v0 WITH searched CASE projections currently require an explicit AS alias."
                )
            item = WithReturnItem(
                kind="case",
                alias=output_alias,
                output_alias=output_alias,
                value=case_spec,
            )
            _append_with_item(items, seen_output_names, item)
            continue

        aggregate_item = _parse_with_aggregate_item(expression_text, output_alias, binding_map)
        if aggregate_item is not None:
            _append_with_item(items, seen_output_names, aggregate_item)
            continue

        simple_item = _parse_with_simple_entity_function_item(
            expression_text,
            output_alias,
            binding_map,
        )
        if simple_item is not None:
            _append_with_item(items, seen_output_names, simple_item)
            continue

        size_item = _parse_with_size_item(expression_text, output_alias, binding_map)
        if size_item is not None:
            _append_with_item(items, seen_output_names, size_item)
            continue

        unary_item = _parse_with_unary_function_item(
            expression_text,
            output_alias,
            binding_map,
        )
        if unary_item is not None:
            _append_with_item(items, seen_output_names, unary_item)
            continue

        binary_item = _parse_with_multi_argument_function_item(
            expression_text,
            output_alias,
            binding_map,
        )
        if binary_item is not None:
            _append_with_item(items, seen_output_names, binary_item)
            continue

        predicate_item = _parse_with_predicate_return_item(
            expression_text,
            output_alias,
            binding_map,
        )
        if predicate_item is not None:
            _append_with_item(items, seen_output_names, predicate_item)
            continue

        literal_item = _parse_with_scalar_literal_item(expression_text, output_alias)
        if literal_item is not None:
            _append_with_item(items, seen_output_names, literal_item)
            continue

        field_match = _match_with_field(expression_text)
        if field_match is not None:
            alias = field_match.group("alias")
            _require_entity_binding(
                binding_map,
                alias,
                f"HumemCypher v0 WITH queries cannot return unknown entity alias {alias!r}.",
            )
            _append_with_item(
                items,
                seen_output_names,
                WithReturnItem(
                    kind="field",
                    alias=alias,
                    field=field_match.group("field"),
                    output_alias=output_alias,
                ),
            )
            continue

        binding = binding_map.get(expression_text)
        if binding is None:
            raise ValueError(
                "HumemCypher v0 WITH queries currently support RETURN alias.field for "
                "entity bindings, RETURN entity_alias for pass-through entity bindings, "
                "RETURN scalar_alias for scalar bindings, id(binding_alias) AS output_alias, "
                "type(rel_binding_alias) AS output_alias, size(admitted_input) AS output_alias, "
                "lower(admitted_input) AS output_alias, upper(admitted_input) AS output_alias, trim(admitted_input) AS output_alias, ltrim(admitted_input) AS output_alias, rtrim(admitted_input) AS output_alias, reverse(admitted_input) AS output_alias, coalesce(admitted_input, literal_or_parameter) AS output_alias, replace(admitted_input, literal_or_parameter, literal_or_parameter) AS output_alias, left(admitted_input, literal_or_parameter) AS output_alias, right(admitted_input, literal_or_parameter) AS output_alias, split(admitted_input, literal_or_parameter) AS output_alias, substring(admitted_input, literal_or_parameter) AS output_alias, substring(admitted_input, literal_or_parameter, literal_or_parameter) AS output_alias, abs(admitted_input) AS output_alias, sign(admitted_input) AS output_alias, round(admitted_input) AS output_alias, ceil(admitted_input) AS output_alias, floor(admitted_input) AS output_alias, toString(admitted_input) AS output_alias, toInteger(admitted_input) AS output_alias, toFloat(admitted_input) AS output_alias, toBoolean(admitted_input) AS output_alias, "
                "scalar_literal_or_parameter AS output_alias, predicate admitted_input OP value AS output_alias, and optional AS aliases for those projection forms."
            )
        _append_with_item(
            items,
            seen_output_names,
            WithReturnItem(
                kind="scalar" if binding.binding_kind == "scalar" else "entity",
                alias=expression_text,
                output_alias=output_alias,
            ),
        )

    if not items:
        raise ValueError("HumemCypher v0 WITH RETURN clauses cannot be empty.")
    return tuple(items)


def _parse_with_order_items(
    order_items: tuple[OrderItem, ...],
    bindings: tuple[WithBinding, ...],
    returns: tuple[WithReturnItem, ...],
) -> tuple[WithOrderItem, ...]:
    binding_map = {binding.output_alias: binding for binding in bindings}
    aggregate_aliases = {
        item.column_name for item in returns if item.kind in _WITH_AGGREGATE_RETURN_KINDS
    }
    projected_aliases = {
        item.column_name: item
        for item in returns
        if item.kind not in _WITH_AGGREGATE_RETURN_KINDS
    }

    items: list[WithOrderItem] = []
    for item in order_items:
        if item.expression is not None:
            if item.expression in aggregate_aliases:
                items.append(
                    WithOrderItem(
                        kind="aggregate",
                        alias=item.expression,
                        direction=item.direction,
                    )
                )
                continue
            projected = projected_aliases.get(item.expression)
            if projected is not None:
                items.append(_with_order_item_from_projection(projected, item.direction))
                continue
            raise ValueError(
                "HumemCypher v0 WITH queries can ORDER BY a projected alias, admitted binding, or an exact projected RETURN expression."
            )

        if item.field == "__value__" and item.alias in aggregate_aliases:
            items.append(
                WithOrderItem(kind="aggregate", alias=item.alias, direction=item.direction)
            )
            continue
        if item.field == "__value__" and item.alias in projected_aliases:
            items.append(
                _with_order_item_from_projection(
                    projected_aliases[item.alias],
                    item.direction,
                )
            )
            continue

        binding = binding_map.get(item.alias)
        if binding is None:
            raise ValueError(
                f"HumemCypher v0 WITH queries cannot order by unknown alias {item.alias!r}."
            )
        if binding.binding_kind == "scalar":
            if item.field != "__value__":
                raise ValueError(
                    "HumemCypher v0 WITH queries currently support ORDER BY scalar_alias "
                    "for scalar bindings."
                )
            items.append(
                WithOrderItem(kind="scalar", alias=item.alias, direction=item.direction)
            )
            continue
        items.append(
            WithOrderItem(
                kind="field",
                alias=item.alias,
                field=item.field,
                direction=item.direction,
            )
        )
    return tuple(items)


def _normalize_unwind_query(
    result: CypherParseResult,
    single_part_query,
) -> NormalizedUnwind:
    unwind_ctx = single_part_query.oC_ReadingClause()[0].oC_Unwind()
    assert unwind_ctx is not None
    source_kind, source_items, source_param_name = _parse_unwind_source(
        _context_text(result, unwind_ctx.oC_Expression()).strip()
    )
    alias = _context_text(result, unwind_ctx.oC_Variable()).strip()

    return_ctx = single_part_query.oC_Return()
    assert return_ctx is not None
    projection_text = _context_text(result, return_ctx.oC_ProjectionBody())
    return_text, order_by, limit, _distinct, skip = _split_return_clause(projection_text)
    returns = _parse_unwind_return_items(return_text, alias)
    unwind_order_by = _parse_unwind_order_items(order_by, alias, returns)

    return NormalizedUnwind(
        kind="unwind",
        alias=alias,
        source_kind=source_kind,
        source_items=source_items,
        source_param_name=source_param_name,
        returns=returns,
        order_by=unwind_order_by,
        limit=limit,
        skip=skip,
    )


def _parse_unwind_source(
    text: str,
) -> tuple[Literal["literal", "parameter"], tuple[CypherValue, ...], str | None]:
    if text.startswith("[") and text.endswith("]"):
        inner = text[1:-1].strip()
        if not inner:
            return "literal", (), None
        return (
            "literal",
            tuple(_parse_literal(item.strip()) for item in _split_comma_separated(inner)),
            None,
        )

    value = _parse_literal(text)
    if isinstance(value, _ParameterRef):
        return "parameter", (), value.name

    raise ValueError(
        "HumemCypher v0 UNWIND currently requires a list literal or named parameter source."
    )


def _parse_unwind_return_items(text: str, alias: str) -> tuple[WithReturnItem, ...]:
    items: list[WithReturnItem] = []
    seen_output_names: set[str] = set()
    for raw_item in _split_comma_separated(text):
        expression_text, output_alias = _split_with_projection_alias(raw_item.strip())
        if expression_text != alias:
            raise ValueError(
                "HumemCypher v0 UNWIND currently supports only RETURN unwind_alias or RETURN unwind_alias AS output_alias."
            )
        item = WithReturnItem(kind="scalar", alias=alias, output_alias=output_alias)
        _append_with_item(items, seen_output_names, item, duplicate_scope="UNWIND")
    if not items:
        raise ValueError("HumemCypher v0 UNWIND RETURN clauses cannot be empty.")
    return tuple(items)


def _parse_unwind_order_items(
    order_items: tuple[OrderItem, ...],
    alias: str,
    returns: tuple[WithReturnItem, ...],
) -> tuple[WithOrderItem, ...]:
    projected_aliases = {
        item.column_name: item
        for item in returns
        if item.output_alias is not None
    }
    items: list[WithOrderItem] = []
    for item in order_items:
        if item.field == "__value__" and item.alias in projected_aliases:
            projected = projected_aliases[item.alias]
            items.append(
                WithOrderItem(kind="scalar", alias=projected.alias, direction=item.direction)
            )
            continue
        if item.alias != alias or item.field != "__value__":
            raise ValueError(
                "HumemCypher v0 UNWIND currently supports ORDER BY unwind_alias or ORDER BY projected_alias."
            )
        items.append(WithOrderItem(kind="scalar", alias=alias, direction=item.direction))
    return tuple(items)


def _split_with_projection_alias(item_text: str) -> tuple[str, str | None]:
    alias_match = re.fullmatch(
        r"(?P<expr>.+?)\s+as\s+(?P<output>[A-Za-z_][A-Za-z0-9_]*)",
        item_text,
        flags=re.IGNORECASE,
    )
    if alias_match is None:
        return item_text, None
    return alias_match.group("expr").strip(), alias_match.group("output")


def _append_with_item(
    items: list[WithReturnItem],
    seen_output_names: set[str],
    item: WithReturnItem,
    *,
    duplicate_scope: str = "WITH queries",
) -> None:
    output_name = item.column_name
    if output_name in seen_output_names:
        raise ValueError(
            f"HumemCypher v0 {duplicate_scope} do not allow duplicate RETURN output alias {output_name!r}."
        )
    seen_output_names.add(output_name)
    items.append(item)


def _parse_with_aggregate_item(
    expression_text: str,
    output_alias: str | None,
    binding_map: dict[str, WithBinding],
) -> WithReturnItem | None:
    count_match = re.fullmatch(
        r"count\s*\(\s*(?P<alias>\*|[A-Za-z_][A-Za-z0-9_]*)\s*\)",
        expression_text,
        flags=re.IGNORECASE,
    )
    if count_match is not None:
        alias = count_match.group("alias")
        if alias != "*" and alias not in binding_map:
            raise ValueError(
                f"HumemCypher v0 WITH queries cannot aggregate unknown binding alias {alias!r}."
            )
        return WithReturnItem(kind="count", alias=alias, output_alias=output_alias)

    aggregate_match = re.fullmatch(
        r"(?P<func>sum|avg|min|max)\s*\(\s*(?P<alias>[A-Za-z_][A-Za-z0-9_]*)\s*\)",
        expression_text,
        flags=re.IGNORECASE,
    )
    if aggregate_match is not None:
        alias = aggregate_match.group("alias")
        binding = binding_map.get(alias)
        if binding is None or binding.binding_kind != "scalar":
            raise ValueError(
                f"HumemCypher v0 WITH queries currently support {aggregate_match.group('func').lower()}(...) only over admitted scalar bindings."
            )
        return WithReturnItem(
            kind=cast(Literal["sum", "avg", "min", "max"], aggregate_match.group("func").lower()),
            alias=alias,
            output_alias=output_alias,
        )

    aggregate_field_match = re.fullmatch(
        r"(?P<func>sum|avg|min|max)\s*\(\s*(?P<alias>[A-Za-z_][A-Za-z0-9_]*)\.(?P<field>[A-Za-z_][A-Za-z0-9_]*)\s*\)",
        expression_text,
        flags=re.IGNORECASE,
    )
    if aggregate_field_match is None:
        return None
    alias = aggregate_field_match.group("alias")
    _require_entity_binding(
        binding_map,
        alias,
        "HumemCypher v0 WITH queries currently support aggregate(...) over entity-field inputs only when the alias is an admitted entity binding.",
    )
    return WithReturnItem(
        kind=cast(Literal["sum", "avg", "min", "max"], aggregate_field_match.group("func").lower()),
        alias=alias,
        field=aggregate_field_match.group("field"),
        output_alias=output_alias,
    )


def _parse_with_simple_entity_function_item(
    expression_text: str,
    output_alias: str | None,
    binding_map: dict[str, WithBinding],
) -> WithReturnItem | None:
    for pattern, kind, requirement in (
        (
            r"id\s*\(\s*(?P<alias>[A-Za-z_][A-Za-z0-9_]*)\s*\)",
            "id",
            "HumemCypher v0 WITH queries cannot apply id(...) to unknown entity binding {alias!r}.",
        ),
        (
            r"type\s*\(\s*(?P<alias>[A-Za-z_][A-Za-z0-9_]*)\s*\)",
            "type",
            "HumemCypher v0 WITH queries cannot apply type(...) to unknown relationship binding {alias!r}.",
        ),
        (
            r"properties\s*\(\s*(?P<alias>[A-Za-z_][A-Za-z0-9_]*)\s*\)",
            "properties",
            "HumemCypher v0 WITH queries cannot apply properties(...) to unknown entity binding {alias!r}.",
        ),
        (
            r"labels\s*\(\s*(?P<alias>[A-Za-z_][A-Za-z0-9_]*)\s*\)",
            "labels",
            "HumemCypher v0 WITH queries cannot apply labels(...) to unknown node binding {alias!r}.",
        ),
        (
            r"keys\s*\(\s*(?P<alias>[A-Za-z_][A-Za-z0-9_]*)\s*\)",
            "keys",
            "HumemCypher v0 WITH queries cannot apply keys(...) to unknown entity binding {alias!r}.",
        ),
    ):
        match = re.fullmatch(pattern, expression_text, flags=re.IGNORECASE)
        if match is None:
            continue
        alias = match.group("alias")
        binding = _require_entity_binding(binding_map, alias, requirement.format(alias=alias))
        if kind == "type" and binding.alias_kind != "relationship":
            raise ValueError(requirement.format(alias=alias))
        if kind == "labels" and binding.alias_kind != "node":
            raise ValueError(requirement.format(alias=alias))
        return WithReturnItem(kind=cast(object, kind), alias=alias, output_alias=output_alias)

    node_match = re.fullmatch(
        r"(?P<func>startNode|endNode)\s*\(\s*(?P<alias>[A-Za-z_][A-Za-z0-9_]*)\s*\)(?:\.(?P<field>[A-Za-z_][A-Za-z0-9_]*))?",
        expression_text,
        flags=re.IGNORECASE,
    )
    if node_match is None:
        return None
    alias = node_match.group("alias")
    binding = _require_entity_binding(
        binding_map,
        alias,
        f"HumemCypher v0 WITH queries cannot apply {node_match.group('func')}(...) to unknown relationship binding {alias!r}.",
    )
    if binding.alias_kind != "relationship":
        raise ValueError(
            f"HumemCypher v0 WITH queries cannot apply {node_match.group('func')}(...) to unknown relationship binding {alias!r}."
        )
    return WithReturnItem(
        kind="start_node" if node_match.group("func").lower() == "startnode" else "end_node",
        alias=alias,
        field=node_match.group("field"),
        output_alias=output_alias,
    )


def _parse_with_size_item(
    expression_text: str,
    output_alias: str | None,
    binding_map: dict[str, WithBinding],
) -> WithReturnItem | None:
    size_expr = _match_with_size_expression(expression_text)
    if size_expr is None:
        return None
    size_item = _parse_with_size_input(size_expr, binding_map)
    if size_item is not None:
        return WithReturnItem(
            kind="size",
            alias=size_item.alias,
            field=size_item.field,
            output_alias=output_alias,
        )

    if output_alias is None:
        raise ValueError(
            "HumemCypher v0 WITH size(...) projections currently require an explicit AS alias."
        )
    try:
        scalar_value = _parse_literal(size_expr)
    except ValueError as exc:
        raise ValueError(
            "HumemCypher v0 WITH queries currently support size(...) only over admitted entity-field projections, admitted id/type outputs, scalar bindings, or scalar literal/parameter inputs."
        ) from exc
    return WithReturnItem(
        kind="size",
        alias=output_alias,
        output_alias=output_alias,
        value=scalar_value,
    )


def _parse_with_unary_function_item(
    expression_text: str,
    output_alias: str | None,
    binding_map: dict[str, WithBinding],
) -> WithReturnItem | None:
    match = re.fullmatch(
        r"(?P<func>lower|upper|trim|ltrim|rtrim|reverse|abs|sign|round|ceil|floor|sqrt|exp|sin|cos|tan|asin|acos|atan|ln|log|log10|radians|degrees|tostring|tointeger|tofloat|toboolean)\s*\(\s*(?P<expr>.+?)\s*\)",
        expression_text,
        flags=re.IGNORECASE,
    )
    if match is None:
        return None
    function_kind = _WITH_UNARY_FUNCTION_KIND_BY_NAME[match.group("func").lower()]
    function_expr = match.group("expr").strip()
    parsed_input = _parse_with_scalar_or_field_input(function_expr, binding_map)
    if parsed_input is not None:
        return WithReturnItem(
            kind=cast(object, function_kind),
            alias=parsed_input.alias,
            field=parsed_input.field,
            output_alias=output_alias,
        )

    if output_alias is None:
        return None
    try:
        scalar_value = _parse_literal(function_expr)
    except ValueError as exc:
        raise ValueError(
            f"HumemCypher v0 WITH queries currently support {match.group('func')}(...) only over admitted entity-field projections, scalar bindings, or scalar literal/parameter inputs."
        ) from exc
    return WithReturnItem(
        kind=cast(object, function_kind),
        alias=output_alias,
        output_alias=output_alias,
        value=scalar_value,
    )


def _parse_with_multi_argument_function_item(
    expression_text: str,
    output_alias: str | None,
    binding_map: dict[str, WithBinding],
) -> WithReturnItem | None:
    for function_name, min_args, max_args in (
        ("coalesce", 2, 2),
        ("replace", 3, 3),
        ("left", 2, 2),
        ("right", 2, 2),
        ("split", 2, 2),
        ("substring", 2, 3),
    ):
        match = re.fullmatch(
            rf"{function_name}\s*\(\s*(?P<args>.+)\s*\)",
            expression_text,
            flags=re.IGNORECASE,
        )
        if match is None:
            continue
        args = [part.strip() for part in _split_comma_separated(match.group("args"))]
        if not min_args <= len(args) <= max_args:
            if function_name == "substring":
                raise ValueError(
                    "HumemCypher v0 WITH substring(...) currently requires exactly two or three arguments."
                )
            if function_name in {"left", "right"}:
                raise ValueError(
                    "HumemCypher v0 WITH left(...) and right(...) currently require exactly two arguments."
                )
            raise ValueError(
                f"HumemCypher v0 WITH {function_name}(...) currently requires exactly {min_args} arguments."
            )
        return _build_with_multi_argument_function_item(
            function_name,
            args,
            output_alias,
            binding_map,
        )
    return None


def _build_with_multi_argument_function_item(
    function_name: str,
    args: list[str],
    output_alias: str | None,
    binding_map: dict[str, WithBinding],
) -> WithReturnItem:
    primary_expr = args[0]
    parsed_input = _parse_with_scalar_or_field_input(primary_expr, binding_map)
    if parsed_input is None and function_name == "coalesce":
        raise ValueError(
            "HumemCypher v0 WITH queries currently support coalesce(...) only as coalesce(entity_alias.field, literal_or_parameter) or coalesce(scalar_alias, literal_or_parameter)."
        )

    if parsed_input is None:
        try:
            scalar_value = _parse_literal(primary_expr)
        except ValueError as exc:
            if function_name == "replace":
                raise ValueError(
                    "HumemCypher v0 WITH queries currently support replace(...) only as replace(admitted_input, literal_or_parameter, literal_or_parameter)."
                ) from exc
            if function_name in {"left", "right"}:
                raise ValueError(
                    "HumemCypher v0 WITH queries currently support left(...) and right(...) only as function(admitted_input, literal_or_parameter)."
                ) from exc
            if function_name == "split":
                raise ValueError(
                    "HumemCypher v0 WITH queries currently support split(...) only as split(admitted_input, literal_or_parameter)."
                ) from exc
            raise ValueError(
                "HumemCypher v0 WITH queries currently support substring(...) only as substring(admitted_input, literal_or_parameter) or substring(admitted_input, literal_or_parameter, literal_or_parameter)."
            ) from exc
        if output_alias is None and function_name == "coalesce":
            raise ValueError(
                "HumemCypher v0 WITH queries currently support coalesce(...) only as coalesce(entity_alias.field, literal_or_parameter) or coalesce(scalar_alias, literal_or_parameter)."
            )
        alias = output_alias or expression_text_from_args(function_name, args)
        base_kwargs = {"alias": alias, "output_alias": output_alias, "value": scalar_value}
    else:
        base_kwargs = {
            "alias": parsed_input.alias,
            "field": parsed_input.field,
            "output_alias": output_alias,
        }

    if function_name == "coalesce":
        fallback_value = _parse_literal(args[1])
        return WithReturnItem(kind="coalesce", value=fallback_value, **base_kwargs)
    if function_name == "replace":
        search_value = _parse_literal(args[1])
        replace_value = _parse_literal(args[2])
        return WithReturnItem(
            kind="replace",
            search_value=search_value,
            replace_value=replace_value,
            **base_kwargs,
        )
    if function_name in {"left", "right"}:
        length_value = _parse_literal(args[1])
        return WithReturnItem(
            kind=cast(object, function_name),
            length_value=length_value,
            **base_kwargs,
        )
    if function_name == "split":
        delimiter_value = _parse_literal(args[1])
        return WithReturnItem(
            kind="split",
            delimiter_value=delimiter_value,
            **base_kwargs,
        )
    start_value = _parse_literal(args[1])
    length_value = _parse_literal(args[2]) if len(args) == 3 else None
    return WithReturnItem(
        kind="substring",
        start_value=start_value,
        length_value=length_value,
        **base_kwargs,
    )


def _parse_with_predicate_return_item(
    expression_text: str,
    output_alias: str | None,
    binding_map: dict[str, WithBinding],
) -> WithReturnItem | None:
    try:
        left_text, operator, value_text = _split_predicate_comparison(expression_text)
    except ValueError:
        return None

    if output_alias is None:
        raise ValueError(
            "HumemCypher v0 WITH predicate RETURN items currently require an explicit AS alias."
        )

    parsed_value = _parse_with_predicate_value(operator, value_text)
    target_text = left_text.strip()
    scalar_binding = binding_map.get(target_text)
    if scalar_binding is not None and scalar_binding.binding_kind == "scalar":
        return WithReturnItem(
            kind="predicate",
            alias=target_text,
            output_alias=output_alias,
            operator=operator,
            value=parsed_value,
        )

    size_expr = _match_with_size_expression(target_text)
    if size_expr is not None:
        size_item = _parse_with_size_input(size_expr, binding_map)
        if size_item is not None:
            return WithReturnItem(
                kind="predicate",
                alias=size_item.alias,
                field=_predicate_field_for_size_item(size_item),
                output_alias=output_alias,
                operator=operator,
                value=parsed_value,
            )

    id_alias = _match_with_id_alias(target_text)
    if id_alias is not None:
        _require_entity_binding(
            binding_map,
            id_alias,
            f"HumemCypher v0 WITH queries cannot apply id(...) to unknown entity binding {id_alias!r}.",
        )
        return WithReturnItem(
            kind="predicate",
            alias=id_alias,
            field="id",
            output_alias=output_alias,
            operator=operator,
            value=parsed_value,
        )

    type_alias = _match_with_type_alias(target_text)
    if type_alias is not None:
        binding = _require_entity_binding(
            binding_map,
            type_alias,
            f"HumemCypher v0 WITH queries cannot apply type(...) to unknown relationship binding {type_alias!r}.",
        )
        if binding.alias_kind != "relationship":
            raise ValueError(
                f"HumemCypher v0 WITH queries cannot apply type(...) to unknown relationship binding {type_alias!r}."
            )
        return WithReturnItem(
            kind="predicate",
            alias=type_alias,
            field="type",
            output_alias=output_alias,
            operator=operator,
            value=parsed_value,
        )

    field_match = _match_with_field(target_text)
    if field_match is not None:
        alias = field_match.group("alias")
        _require_entity_binding(
            binding_map,
            alias,
            f"HumemCypher v0 WITH queries cannot return unknown entity alias {alias!r}.",
        )
        return WithReturnItem(
            kind="predicate",
            alias=alias,
            field=field_match.group("field"),
            output_alias=output_alias,
            operator=operator,
            value=parsed_value,
        )

    raise ValueError(
        "HumemCypher v0 WITH queries currently support predicate RETURN items only as scalar_alias OP value, entity_alias.field OP value, id(entity_alias) OP value, type(rel_alias) OP value, or size(admitted_input) OP value."
    )


def _parse_with_scalar_literal_item(
    expression_text: str,
    output_alias: str | None,
) -> WithReturnItem | None:
    try:
        scalar_value = _parse_literal(expression_text)
    except ValueError:
        return None
    if output_alias is None:
        raise ValueError(
            "HumemCypher v0 WITH scalar literal and parameter projections currently require an explicit AS alias."
        )
    return WithReturnItem(
        kind="scalar_value",
        alias=output_alias,
        output_alias=output_alias,
        value=scalar_value,
    )


def _parse_with_size_input(
    expression_text: str,
    binding_map: dict[str, WithBinding],
) -> WithReturnItem | None:
    binding = binding_map.get(expression_text)
    if binding is not None and binding.binding_kind == "scalar":
        return WithReturnItem(kind="size", alias=expression_text)

    id_alias = _match_with_id_alias(expression_text)
    if id_alias is not None:
        _require_entity_binding(
            binding_map,
            id_alias,
            f"HumemCypher v0 WITH queries cannot apply size(id(...)) to unknown entity binding {id_alias!r}.",
        )
        return WithReturnItem(kind="size", alias=id_alias, field="id")

    type_alias = _match_with_type_alias(expression_text)
    if type_alias is not None:
        binding = _require_entity_binding(
            binding_map,
            type_alias,
            f"HumemCypher v0 WITH queries cannot apply size(type(...)) to unknown relationship binding {type_alias!r}.",
        )
        if binding.alias_kind != "relationship":
            raise ValueError(
                f"HumemCypher v0 WITH queries cannot apply size(type(...)) to unknown relationship binding {type_alias!r}."
            )
        return WithReturnItem(kind="size", alias=type_alias, field="type")

    field_match = _match_with_field(expression_text)
    if field_match is None:
        return None
    alias = field_match.group("alias")
    _require_entity_binding(
        binding_map,
        alias,
        "HumemCypher v0 WITH size(...) projections currently support entity_alias.field only for admitted entity bindings.",
    )
    return WithReturnItem(kind="size", alias=alias, field=field_match.group("field"))


def _parse_with_scalar_or_field_input(
    expression_text: str,
    binding_map: dict[str, WithBinding],
) -> WithReturnItem | None:
    binding = binding_map.get(expression_text)
    if binding is not None and binding.binding_kind == "scalar":
        return WithReturnItem(kind="scalar", alias=expression_text)
    field_match = _match_with_field(expression_text)
    if field_match is None:
        return None
    alias = field_match.group("alias")
    _require_entity_binding(
        binding_map,
        alias,
        "HumemCypher v0 WITH function projections currently support entity_alias.field only for admitted entity bindings.",
    )
    return WithReturnItem(kind="field", alias=alias, field=field_match.group("field"))


def _parse_with_predicate_value(
    operator: Literal[
        "=",
        "<",
        "<=",
        ">",
        ">=",
        "STARTS WITH",
        "ENDS WITH",
        "CONTAINS",
        "IS NULL",
        "IS NOT NULL",
    ],
    value_text: str,
) -> object | None:
    if operator in {"IS NULL", "IS NOT NULL"}:
        if value_text.strip():
            raise ValueError(
                "HumemCypher v0 null predicates cannot include a trailing literal value."
            )
        return None
    return _parse_literal(value_text.strip())


def _predicate_field_for_size_item(item: WithReturnItem) -> str:
    return f"{_SIZE_PREDICATE_FIELD_PREFIX}{item.field or '__value__'}"


def _with_order_item_from_projection(
    projected: WithReturnItem,
    direction: Literal["asc", "desc"],
) -> WithOrderItem:
    return WithOrderItem(
        kind=projected.kind,
        alias=projected.alias,
        field=projected.field,
        direction=direction,
        operator=projected.operator,
        value=projected.value,
        start_value=projected.start_value,
        length_value=projected.length_value,
        search_value=projected.search_value,
        replace_value=projected.replace_value,
        delimiter_value=projected.delimiter_value,
    )


def _match_with_field(expression_text: str):
    return _WITH_FIELD_RE.fullmatch(expression_text)


def _match_with_id_alias(expression_text: str) -> str | None:
    match = re.fullmatch(
        r"id\s*\(\s*(?P<alias>[A-Za-z_][A-Za-z0-9_]*)\s*\)",
        expression_text,
        flags=re.IGNORECASE,
    )
    return None if match is None else match.group("alias")


def _match_with_type_alias(expression_text: str) -> str | None:
    match = re.fullmatch(
        r"type\s*\(\s*(?P<alias>[A-Za-z_][A-Za-z0-9_]*)\s*\)",
        expression_text,
        flags=re.IGNORECASE,
    )
    return None if match is None else match.group("alias")


def _match_with_size_expression(expression_text: str) -> str | None:
    match = re.fullmatch(
        r"size\s*\(\s*(?P<expr>.+?)\s*\)",
        expression_text,
        flags=re.IGNORECASE,
    )
    return None if match is None else match.group("expr").strip()


def _require_entity_binding(
    binding_map: dict[str, WithBinding],
    alias: str,
    message: str,
) -> WithBinding:
    binding = binding_map.get(alias)
    if binding is None or binding.binding_kind != "entity":
        raise ValueError(message)
    return binding


def expression_text_from_args(function_name: str, args: list[str]) -> str:
    return f"{function_name}({', '.join(args)})"