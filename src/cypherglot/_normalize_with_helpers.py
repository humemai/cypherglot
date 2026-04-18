from __future__ import annotations

from typing import Literal

from ._normalize_support import (
    _looks_like_relationship_pattern,
    _parse_node_pattern,
    _parse_predicates,
    _parse_relationship_chain_segment,
    _parse_relationship_pattern,
    _split_relationship_pattern,
    _split_return_clause,
    _unwrap_node_pattern,
    NodePattern,
    Predicate,
    RelationshipPattern,
)
from ._normalize_with_expression_helpers import (
    _match_with_field,
    _parse_unwind_order_items,
    _parse_unwind_return_items,
    _parse_unwind_source,
    _parse_with_binding_expression_item,
    _parse_with_order_items,
    _parse_with_predicates,
    _parse_with_return_items,
)
from .normalize import (
    NormalizedMatchChain,
    NormalizedMatchNode,
    NormalizedMatchRelationship,
    NormalizedMatchWithReturn,
    NormalizedUnwind,
    WithBinding,
    WithPredicate,
    _context_text,
    _source_alias_kinds,
    _validate_normalized_match_predicates,
)
from .parser import CypherParseResult


def _parse_match_chain(
    result: CypherParseResult,
    pattern_element,
) -> tuple[tuple[NodePattern, ...], tuple[RelationshipPattern, ...]]:
    nodes = [
        _parse_node_pattern(
            _unwrap_node_pattern(
                _context_text(result, pattern_element.oC_NodePattern())
            ),
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

    pattern_element = match_ctx.oC_Pattern().oC_PatternPart()[0]
    pattern_element = pattern_element.oC_AnonymousPatternPart().oC_PatternElement()
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
        left_text, relationship_text, right_text, direction = (
            _split_relationship_pattern(pattern_text)
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
    source_binding_map = {
        alias: WithBinding(
            source_alias=alias,
            output_alias=alias,
            binding_kind="entity",
            alias_kind=alias_kind,
        )
        for alias, alias_kind in alias_kinds.items()
    }
    for item_ctx in projection_body.oC_ProjectionItems().oC_ProjectionItem():
        expression_text = _context_text(result, item_ctx.oC_Expression()).strip()
        output_alias = (
            _context_text(result, item_ctx.oC_Variable()).strip()
            if item_ctx.oC_Variable() is not None
            else expression_text
        )
        if output_alias in seen_output_aliases:
            raise ValueError(
                "HumemCypher v0 WITH support does not allow duplicate output "
                f"alias {output_alias!r}."
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
                    "HumemCypher v0 WITH scalar rebinding currently requires an "
                    "AS alias."
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

        if expression_text in alias_kinds:
            bindings.append(
                WithBinding(
                    source_alias=expression_text,
                    output_alias=output_alias,
                    binding_kind="entity",
                    alias_kind=alias_kinds[expression_text],
                )
            )
            continue

        if item_ctx.oC_Variable() is None:
            raise ValueError(
                "HumemCypher v0 derived scalar WITH bindings currently require an "
                "explicit AS alias."
            )

        derived_item = _parse_with_binding_expression_item(
            expression_text,
            output_alias,
            source_binding_map,
        )
        bindings.append(
            WithBinding(
                source_alias=derived_item.alias,
                output_alias=output_alias,
                binding_kind="scalar",
                alias_kind=source_binding_map.get(derived_item.alias, None).alias_kind
                if derived_item.alias in source_binding_map
                else None,
                source_field=derived_item.field,
                expression=derived_item,
            )
        )
    return tuple(bindings)


def _normalize_unwind_query(
    result: CypherParseResult,
    single_part_query,
) -> NormalizedUnwind:
    unwind_ctx = single_part_query.oC_ReadingClause()[0].oC_Unwind()
    assert unwind_ctx is not None
    source_kind, source_items = _parse_unwind_source(
        _context_text(result, unwind_ctx.oC_Expression()).strip()
    )
    alias = _context_text(result, unwind_ctx.oC_Variable()).strip()

    return_ctx = single_part_query.oC_Return()
    assert return_ctx is not None
    projection_text = _context_text(result, return_ctx.oC_ProjectionBody())
    return_text, order_by, limit, _distinct, skip = _split_return_clause(
        projection_text
    )
    returns = _parse_unwind_return_items(return_text, alias)
    unwind_order_by = _parse_unwind_order_items(order_by, alias, returns)

    return NormalizedUnwind(
        kind="unwind",
        alias=alias,
        source_kind=source_kind,
        source_items=source_items,
        returns=returns,
        order_by=unwind_order_by,
        limit=limit,
        skip=skip,
    )
