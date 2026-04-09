"""Normalize generated Cypher parse output into CypherGlot-owned structures."""

from __future__ import annotations

from dataclasses import dataclass
import re
from typing import Literal, cast

from ._normalize_support import (
    _BINARY_TERNARY_RETURN_FUNCTION_NAMES,
    _SIZE_PREDICATE_FIELD_PREFIX,
    _UNARY_RETURN_FUNCTION_NAMES,
    _find_top_level_keyword,
    _ParameterRef,
    _format_cypher_value,
    NodePattern,
    OrderItem,
    Predicate,
    RelationshipPattern,
    ReturnItem,
    SetItem,
    _looks_like_relationship_pattern,
    _parse_node_pattern,
    _parse_predicates,
    _parse_query_nodes_limit_ref,
    _parse_query_nodes_order_items,
    _parse_query_nodes_return_items,
    _parse_relationship_chain_segment,
    _parse_relationship_pattern,
    _parse_return_items,
    _parse_set_items,
    _parse_literal,
    _parse_boolean_predicate_groups,
    _split_comma_separated,
    _split_predicate_comparison,
    _split_query_nodes_return_and_order,
    _split_relationship_pattern,
    _split_return_clause,
    _unwrap_node_pattern,
    _validate_create_relationship_separate_patterns,
    _validate_match_create_relationship_between_nodes_endpoints,
    _validate_match_create_relationship_endpoints,
)
from .parser import CypherParseResult, parse_cypher_text
from .validate import validate_cypher_parse_result


_AGGREGATE_RETURN_KINDS = {"count", "sum", "avg", "min", "max"}


@dataclass(frozen=True, slots=True)
class WithBinding:
    source_alias: str
    output_alias: str
    binding_kind: Literal["entity", "scalar"]
    alias_kind: Literal["node", "relationship"] | None = None
    source_field: str | None = None


@dataclass(frozen=True, slots=True)
class WithReturnItem:
    kind: Literal["field", "scalar", "entity", "count", "sum", "avg", "min", "max", "id", "type", "properties", "labels", "keys", "start_node", "end_node", "size", "scalar_value", "predicate", "lower", "upper", "trim", "ltrim", "rtrim", "reverse", "coalesce", "replace", "left", "right", "split", "abs", "sign", "round", "ceil", "floor", "sqrt", "exp", "sin", "cos", "tan", "asin", "acos", "atan", "ln", "log", "log10", "radians", "degrees", "to_string", "to_integer", "to_float", "to_boolean", "substring", "case"]
    alias: str
    field: str | None = None
    output_alias: str | None = None
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
    ] | None = None
    value: object | None = None
    start_value: object | None = None
    length_value: object | None = None
    search_value: object | None = None
    replace_value: object | None = None
    delimiter_value: object | None = None

    @property
    def column_name(self) -> str:
        if self.output_alias is not None:
            return self.output_alias
        if self.kind in {"scalar", "entity"}:
            return self.alias
        if self.kind == "count":
            if self.alias == "*":
                return "count(*)"
            return f"count({self.alias})"
        if self.kind in {"sum", "avg", "min", "max"}:
            return f"{self.kind}({self.alias})"
        if self.kind == "id":
            return f"id({self.alias})"
        if self.kind == "type":
            return f"type({self.alias})"
        if self.kind == "properties":
            return f"properties({self.alias})"
        if self.kind == "labels":
            return f"labels({self.alias})"
        if self.kind == "keys":
            return f"keys({self.alias})"
        if self.kind in {"start_node", "end_node"}:
            function_name = "startNode" if self.kind == "start_node" else "endNode"
            if self.field is None:
                return f"{function_name}({self.alias})"
            return f"{function_name}({self.alias}).{self.field}"
        if self.kind == "size":
            if self.field == "id":
                return f"size(id({self.alias}))"
            if self.field == "type":
                return f"size(type({self.alias}))"
            if self.field is not None:
                return f"size({self.alias}.{self.field})"
            return f"size({self.alias})"
        if self.kind in _UNARY_RETURN_FUNCTION_NAMES:
            if self.field is not None:
                return f"{_UNARY_RETURN_FUNCTION_NAMES[self.kind]}({self.alias}.{self.field})"
            return f"{_UNARY_RETURN_FUNCTION_NAMES[self.kind]}({self.alias})"
        if self.kind == "coalesce":
            primary_expr = f"{self.alias}.{self.field}" if self.field is not None else self.alias
            return (
                f"coalesce({primary_expr}, "
                f"{_format_cypher_value(cast(object, self.value))})"
            )
        if self.kind == "replace":
            primary_expr = f"{self.alias}.{self.field}" if self.field is not None else self.alias
            return (
                f"replace({primary_expr}, "
                f"{_format_cypher_value(cast(object, self.search_value))}, "
                f"{_format_cypher_value(cast(object, self.replace_value))})"
            )
        if self.kind in {"left", "right", "split", "substring"}:
            function_name = _BINARY_TERNARY_RETURN_FUNCTION_NAMES[self.kind]
            primary_expr = f"{self.alias}.{self.field}" if self.field is not None else self.alias
            arg_suffix: list[str] = []
            if self.kind in {"left", "right"}:
                arg_suffix.append(_format_cypher_value(cast(object, self.length_value)))
            elif self.kind == "split":
                arg_suffix.append(_format_cypher_value(cast(object, self.delimiter_value)))
            else:
                arg_suffix.append(_format_cypher_value(cast(object, self.start_value)))
                if self.length_value is not None:
                    arg_suffix.append(_format_cypher_value(cast(object, self.length_value)))
            return f"{function_name}({', '.join((primary_expr, *arg_suffix))})"
        if self.kind in {"count", "sum", "avg", "min", "max", "id", "type", "properties", "labels", "keys", "start_node", "end_node", "size", "scalar_value", "predicate", "lower", "upper", "trim", "ltrim", "rtrim", "reverse", "coalesce", "replace", "left", "right", "split", "abs", "sign", "round", "ceil", "floor", "sqrt", "exp", "sin", "cos", "tan", "asin", "acos", "atan", "ln", "log", "log10", "radians", "degrees", "to_string", "to_integer", "to_float", "to_boolean", "substring", "case"}:
            raise ValueError(
                "HumemCypher v0 WITH expression RETURN items require an explicit AS alias."
            )
        assert self.field is not None
        return f"{self.alias}.{self.field}"


@dataclass(frozen=True, slots=True)
class WithCaseWhen:
    condition: WithReturnItem
    result: WithReturnItem


@dataclass(frozen=True, slots=True)
class WithCaseSpec:
    when_items: tuple[WithCaseWhen, ...]
    else_item: WithReturnItem


@dataclass(frozen=True, slots=True)
class WithOrderItem:
    kind: Literal["field", "scalar", "entity", "aggregate", "id", "type", "properties", "labels", "keys", "start_node", "end_node", "size", "scalar_value", "predicate", "lower", "upper", "trim", "ltrim", "rtrim", "reverse", "coalesce", "replace", "left", "right", "split", "abs", "sign", "round", "ceil", "floor", "sqrt", "exp", "sin", "cos", "tan", "asin", "acos", "atan", "ln", "log", "log10", "radians", "degrees", "to_string", "to_integer", "to_float", "to_boolean", "substring", "case"]
    alias: str
    field: str | None = None
    direction: Literal["asc", "desc"] = "asc"
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
    ] | None = None
    value: object | None = None
    start_value: object | None = None
    length_value: object | None = None
    search_value: object | None = None
    replace_value: object | None = None
    delimiter_value: object | None = None


@dataclass(frozen=True, slots=True)
class WithPredicate:
    kind: Literal["field", "scalar"]
    alias: str
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
    ]
    value: object
    field: str | None = None
    disjunct_index: int = 0


@dataclass(frozen=True, slots=True)
class NormalizedCreateNode:
    kind: Literal["create"]
    pattern_kind: Literal["node"]
    node: NodePattern


@dataclass(frozen=True, slots=True)
class NormalizedCreateRelationship:
    kind: Literal["create"]
    pattern_kind: Literal["relationship"]
    left: NodePattern
    relationship: RelationshipPattern
    right: NodePattern


@dataclass(frozen=True, slots=True)
class NormalizedCreateRelationshipFromSeparatePatterns:
    kind: Literal["create"]
    pattern_kind: Literal["relationship"]
    first_node: NodePattern
    second_node: NodePattern
    left: NodePattern
    relationship: RelationshipPattern
    right: NodePattern


@dataclass(frozen=True, slots=True)
class NormalizedMatchNode:
    kind: Literal["match"]
    pattern_kind: Literal["node"]
    node: NodePattern
    predicates: tuple[Predicate, ...]
    returns: tuple[ReturnItem, ...]
    order_by: tuple[OrderItem, ...] = ()
    limit: int | None = None
    distinct: bool = False
    skip: int | None = None


@dataclass(frozen=True, slots=True)
class NormalizedMatchRelationship:
    kind: Literal["match"]
    pattern_kind: Literal["relationship"]
    left: NodePattern
    relationship: RelationshipPattern
    right: NodePattern
    predicates: tuple[Predicate, ...]
    returns: tuple[ReturnItem, ...]
    order_by: tuple[OrderItem, ...] = ()
    limit: int | None = None
    distinct: bool = False
    skip: int | None = None


@dataclass(frozen=True, slots=True)
class NormalizedMatchChain:
    kind: Literal["match"]
    pattern_kind: Literal["relationship_chain"]
    nodes: tuple[NodePattern, ...]
    relationships: tuple[RelationshipPattern, ...]
    predicates: tuple[Predicate, ...]
    returns: tuple[ReturnItem, ...]
    order_by: tuple[OrderItem, ...] = ()
    limit: int | None = None
    distinct: bool = False
    skip: int | None = None


@dataclass(frozen=True, slots=True)
class NormalizedOptionalMatchNode:
    kind: Literal["optional_match"]
    pattern_kind: Literal["node"]
    node: NodePattern
    predicates: tuple[Predicate, ...]
    returns: tuple[ReturnItem, ...]
    order_by: tuple[OrderItem, ...] = ()
    limit: int | None = None
    distinct: bool = False
    skip: int | None = None


@dataclass(frozen=True, slots=True)
class NormalizedMatchWithReturn:
    kind: Literal["with"]
    source: NormalizedMatchNode | NormalizedMatchRelationship | NormalizedMatchChain
    bindings: tuple[WithBinding, ...]
    returns: tuple[WithReturnItem, ...]
    predicates: tuple[WithPredicate, ...] = ()
    order_by: tuple[WithOrderItem, ...] = ()
    limit: int | None = None
    distinct: bool = False
    skip: int | None = None


@dataclass(frozen=True, slots=True)
class NormalizedUnwind:
    kind: Literal["unwind"]
    alias: str
    source_kind: Literal["literal", "parameter"]
    source_items: tuple[CypherValue, ...] = ()
    source_param_name: str | None = None
    returns: tuple[WithReturnItem, ...] = ()
    order_by: tuple[WithOrderItem, ...] = ()
    limit: int | None = None
    skip: int | None = None


@dataclass(frozen=True, slots=True)
class NormalizedQueryNodesVectorSearch:
    kind: Literal["vector_query"]
    procedure_kind: Literal["queryNodes"]
    index_name: str
    query_param_name: str
    top_k: int | str
    candidate_query: NormalizedMatchNode | NormalizedMatchRelationship
    return_items: tuple[str, ...]
    order_by: tuple[tuple[str, Literal["asc", "desc"]], ...] = ()


@dataclass(frozen=True, slots=True)
class NormalizedSetNode:
    kind: Literal["set"]
    pattern_kind: Literal["node"]
    node: NodePattern
    predicates: tuple[Predicate, ...]
    assignments: tuple[SetItem, ...]


@dataclass(frozen=True, slots=True)
class NormalizedSetRelationship:
    kind: Literal["set"]
    pattern_kind: Literal["relationship"]
    left: NodePattern
    relationship: RelationshipPattern
    right: NodePattern
    predicates: tuple[Predicate, ...]
    assignments: tuple[SetItem, ...]


@dataclass(frozen=True, slots=True)
class NormalizedDeleteNode:
    kind: Literal["delete"]
    pattern_kind: Literal["node"]
    node: NodePattern
    predicates: tuple[Predicate, ...]
    detach: bool = True


@dataclass(frozen=True, slots=True)
class NormalizedDeleteRelationship:
    kind: Literal["delete"]
    pattern_kind: Literal["relationship"]
    left: NodePattern
    relationship: RelationshipPattern
    right: NodePattern
    predicates: tuple[Predicate, ...]


@dataclass(frozen=True, slots=True)
class NormalizedMergeNode:
    kind: Literal["merge"]
    pattern_kind: Literal["node"]
    node: NodePattern


@dataclass(frozen=True, slots=True)
class NormalizedMergeRelationship:
    kind: Literal["merge"]
    pattern_kind: Literal["relationship"]
    left: NodePattern
    relationship: RelationshipPattern
    right: NodePattern


@dataclass(frozen=True, slots=True)
class NormalizedMatchMergeRelationship:
    kind: Literal["match_merge"]
    pattern_kind: Literal["relationship"]
    left_match: NodePattern
    right_match: NodePattern
    predicates: tuple[Predicate, ...]
    left: NodePattern
    relationship: RelationshipPattern
    right: NodePattern


@dataclass(frozen=True, slots=True)
class NormalizedMatchMergeRelationshipFromTraversal:
    kind: Literal["match_merge"]
    pattern_kind: Literal["relationship"]
    source: NormalizedMatchRelationship | NormalizedMatchChain
    left: NodePattern
    relationship: RelationshipPattern
    right: NodePattern


@dataclass(frozen=True, slots=True)
class NormalizedMatchCreateRelationship:
    kind: Literal["match_create"]
    pattern_kind: Literal["relationship"]
    match_node: NodePattern
    predicates: tuple[Predicate, ...]
    left: NodePattern
    relationship: RelationshipPattern
    right: NodePattern


@dataclass(frozen=True, slots=True)
class NormalizedMatchCreateRelationshipFromTraversal:
    kind: Literal["match_create"]
    pattern_kind: Literal["relationship"]
    source: NormalizedMatchRelationship | NormalizedMatchChain
    left: NodePattern
    relationship: RelationshipPattern
    right: NodePattern


@dataclass(frozen=True, slots=True)
class NormalizedMatchCreateRelationshipBetweenNodes:
    kind: Literal["match_create"]
    pattern_kind: Literal["relationship"]
    left_match: NodePattern
    right_match: NodePattern
    predicates: tuple[Predicate, ...]
    left: NodePattern
    relationship: RelationshipPattern
    right: NodePattern


NormalizedCypherStatement = (
    NormalizedCreateNode
    | NormalizedCreateRelationship
    | NormalizedCreateRelationshipFromSeparatePatterns
    | NormalizedMergeNode
    | NormalizedMergeRelationship
    | NormalizedMatchNode
    | NormalizedMatchRelationship
    | NormalizedMatchChain
    | NormalizedOptionalMatchNode
    | NormalizedMatchWithReturn
    | NormalizedUnwind
    | NormalizedQueryNodesVectorSearch
    | NormalizedSetNode
    | NormalizedSetRelationship
    | NormalizedDeleteNode
    | NormalizedDeleteRelationship
    | NormalizedMatchMergeRelationship
    | NormalizedMatchMergeRelationshipFromTraversal
    | NormalizedMatchCreateRelationship
    | NormalizedMatchCreateRelationshipFromTraversal
    | NormalizedMatchCreateRelationshipBetweenNodes
)


def _validate_normalized_match_set_assignments(
    assignments: tuple[SetItem, ...],
    *,
    target_alias: str | None,
    target_kind: Literal["node", "relationship"],
) -> None:
    for assignment in assignments:
        if assignment.alias != target_alias:
            raise ValueError(
                "HumemCypher v0 MATCH ... SET assignments must target the "
                f"matched {target_kind} alias."
            )


def _validate_normalized_match_predicates(
    predicates: tuple[Predicate, ...],
    *,
    alias_kinds: dict[str, Literal["node", "relationship"]],
) -> None:
    for predicate in predicates:
        alias_kind = alias_kinds.get(predicate.alias)
        if alias_kind is None:
            raise ValueError(
                f"HumemCypher v0 cannot filter on unknown alias {predicate.alias!r}."
            )

        if (
            alias_kind == "node"
            and predicate.field == "label"
            and predicate.operator != "="
        ):
            raise ValueError(
                "HumemCypher v0 currently supports only equality predicates for "
                "node field 'label'."
            )

        if (
            alias_kind == "relationship"
            and predicate.field == "type"
            and predicate.operator != "="
        ):
            raise ValueError(
                "HumemCypher v0 currently supports only equality predicates for "
                "relationship field 'type'."
            )


def normalize_cypher_text(text: str) -> NormalizedCypherStatement:
    return normalize_cypher_parse_result(parse_cypher_text(text))


def normalize_cypher_parse_result(
    result: CypherParseResult,
) -> NormalizedCypherStatement:
    validated_query = validate_cypher_parse_result(result)
    if type(validated_query).__name__ == "OC_MultiPartQueryContext":
        return _normalize_match_with_return(result, validated_query)

    single_part_query = validated_query
    updating_clauses = single_part_query.oC_UpdatingClause()
    reading_clauses = single_part_query.oC_ReadingClause()

    if reading_clauses and reading_clauses[0].oC_InQueryCall() is not None:
        return _normalize_query_nodes_vector_search(result, single_part_query)

    if reading_clauses and reading_clauses[0].oC_Match() is not None:
        match_ctx = reading_clauses[0].oC_Match()
        if match_ctx.OPTIONAL() is not None:
            return _normalize_optional_match_node(result, single_part_query)

    if reading_clauses and reading_clauses[0].oC_Unwind() is not None:
        return _normalize_unwind_query(result, single_part_query)

    if updating_clauses:
        if reading_clauses:
            match_ctx = reading_clauses[0].oC_Match()
            assert match_ctx is not None
            pattern_text = _context_text(result, match_ctx.oC_Pattern())

            predicates: tuple[Predicate, ...] = ()
            where_ctx = match_ctx.oC_Where()
            if where_ctx is not None:
                predicates = _parse_predicates(
                    _context_text(result, where_ctx.oC_Expression())
                )

            create_ctx = updating_clauses[0].oC_Create()
            if create_ctx is not None:
                match_patterns = _split_comma_separated(pattern_text)

                create_pattern_text = _context_text(result, create_ctx.oC_Pattern())
                if not _looks_like_relationship_pattern(create_pattern_text):
                    raise ValueError(
                        "HumemCypher v0 MATCH ... CREATE currently supports only one "
                        "directed relationship pattern in the CREATE clause."
                    )

                left_text, relationship_text, right_text, direction = (
                    _split_relationship_pattern(create_pattern_text)
                )
                left = _parse_node_pattern(
                    left_text,
                    default_alias="__humem_match_create_left_node",
                )
                relationship = _parse_relationship_pattern(relationship_text, direction)
                right = _parse_node_pattern(
                    right_text,
                    default_alias="__humem_match_create_right_node",
                )
                if len(match_patterns) == 1 and _looks_like_relationship_pattern(
                    match_patterns[0]
                ):
                    source = _normalize_match_source(result, match_ctx)
                    assert isinstance(
                        source,
                        (NormalizedMatchRelationship, NormalizedMatchChain),
                    )
                    _validate_match_write_reused_source_endpoints(
                        source,
                        left,
                        right,
                        allow_one_new_endpoint=True,
                    )
                    return NormalizedMatchCreateRelationshipFromTraversal(
                        kind="match_create",
                        pattern_kind="relationship",
                        source=source,
                        left=left,
                        relationship=relationship,
                        right=right,
                    )

                if len(match_patterns) == 1:
                    match_node = _parse_node_pattern(
                        _unwrap_node_pattern(match_patterns[0]),
                        default_alias="__humem_match_create_node",
                    )
                    _validate_normalized_match_predicates(
                        predicates,
                        alias_kinds={match_node.alias: "node"},
                    )
                    _validate_match_create_relationship_endpoints(
                        match_node,
                        left,
                        right,
                    )
                    return NormalizedMatchCreateRelationship(
                        kind="match_create",
                        pattern_kind="relationship",
                        match_node=match_node,
                        predicates=predicates,
                        left=left,
                        relationship=relationship,
                        right=right,
                    )

                if len(match_patterns) == 2:
                    left_match = _parse_node_pattern(
                        _unwrap_node_pattern(match_patterns[0]),
                        default_alias="__humem_match_create_left_match_node",
                    )
                    right_match = _parse_node_pattern(
                        _unwrap_node_pattern(match_patterns[1]),
                        default_alias="__humem_match_create_right_match_node",
                    )
                    _validate_normalized_match_predicates(
                        predicates,
                        alias_kinds={
                            left_match.alias: "node",
                            right_match.alias: "node",
                        },
                    )
                    _validate_match_create_relationship_between_nodes_endpoints(
                        left_match,
                        right_match,
                        left,
                        right,
                    )
                    return NormalizedMatchCreateRelationshipBetweenNodes(
                        kind="match_create",
                        pattern_kind="relationship",
                        left_match=left_match,
                        right_match=right_match,
                        predicates=predicates,
                        left=left,
                        relationship=relationship,
                        right=right,
                    )

                raise ValueError(
                    "HumemCypher v0 MATCH ... CREATE currently supports one matched "
                    "node pattern, or two disconnected matched node patterns, before "
                    "CREATE."
                )

            merge_ctx = updating_clauses[0].oC_Merge()
            if merge_ctx is not None:
                match_patterns = _split_comma_separated(pattern_text)
                merge_pattern_text = _context_text(result, merge_ctx.oC_PatternPart())
                if not _looks_like_relationship_pattern(merge_pattern_text):
                    raise ValueError(
                        "HumemCypher v0 MATCH ... MERGE currently supports only one directed relationship pattern in the MERGE clause."
                    )

                left_text, relationship_text, right_text, direction = (
                    _split_relationship_pattern(merge_pattern_text)
                )
                left = _parse_node_pattern(
                    left_text,
                    default_alias="__humem_match_merge_left_node",
                )
                relationship = _parse_relationship_pattern(relationship_text, direction)
                right = _parse_node_pattern(
                    right_text,
                    default_alias="__humem_match_merge_right_node",
                )
                if len(match_patterns) == 1 and _looks_like_relationship_pattern(
                    match_patterns[0]
                ):
                    source = _normalize_match_source(result, match_ctx)
                    assert isinstance(
                        source,
                        (NormalizedMatchRelationship, NormalizedMatchChain),
                    )
                    _validate_match_write_reused_source_endpoints(
                        source,
                        left,
                        right,
                        allow_one_new_endpoint=True,
                    )
                    return NormalizedMatchMergeRelationshipFromTraversal(
                        kind="match_merge",
                        pattern_kind="relationship",
                        source=source,
                        left=left,
                        relationship=relationship,
                        right=right,
                    )

                left_match = _parse_node_pattern(
                    _unwrap_node_pattern(match_patterns[0]),
                    default_alias="__humem_match_merge_left_match_node",
                )
                right_match = _parse_node_pattern(
                    _unwrap_node_pattern(match_patterns[1]),
                    default_alias="__humem_match_merge_right_match_node",
                )
                if len(match_patterns) != 2 or any(
                    _looks_like_relationship_pattern(pattern)
                    for pattern in match_patterns
                ):
                    raise ValueError(
                        "HumemCypher v0 MATCH ... MERGE currently supports only two disconnected matched node patterns before MERGE, or one matched relationship or fixed-length chain source whose node aliases are reused by MERGE."
                    )

                _validate_normalized_match_predicates(
                    predicates,
                    alias_kinds={
                        left_match.alias: "node",
                        right_match.alias: "node",
                    },
                )
                _validate_match_create_relationship_between_nodes_endpoints(
                    left_match,
                    right_match,
                    left,
                    right,
                )
                return NormalizedMatchMergeRelationship(
                    kind="match_merge",
                    pattern_kind="relationship",
                    left_match=left_match,
                    right_match=right_match,
                    predicates=predicates,
                    left=left,
                    relationship=relationship,
                    right=right,
                )

            set_ctx = updating_clauses[0].oC_Set()
            if set_ctx is not None:
                assignments = _parse_set_items(
                    ", ".join(
                        _context_text(result, item_ctx)
                        for item_ctx in set_ctx.oC_SetItem()
                    )
                )

                if _looks_like_relationship_pattern(pattern_text):
                    left_text, relationship_text, right_text, direction = (
                        _split_relationship_pattern(pattern_text)
                    )
                    left = _parse_node_pattern(
                        left_text,
                        default_alias="__humem_set_left_node",
                    )
                    relationship = _parse_relationship_pattern(
                        relationship_text,
                        direction,
                    )
                    right = _parse_node_pattern(
                        right_text,
                        default_alias="__humem_set_right_node",
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
                    _validate_normalized_match_set_assignments(
                        assignments,
                        target_alias=relationship.alias,
                        target_kind="relationship",
                    )
                    return NormalizedSetRelationship(
                        kind="set",
                        pattern_kind="relationship",
                        left=left,
                        relationship=relationship,
                        right=right,
                        predicates=predicates,
                        assignments=assignments,
                    )

                node = _parse_node_pattern(
                    _unwrap_node_pattern(pattern_text),
                    default_alias="__humem_set_node",
                )
                _validate_normalized_match_predicates(
                    predicates,
                    alias_kinds={node.alias: "node"},
                )
                _validate_normalized_match_set_assignments(
                    assignments,
                    target_alias=node.alias,
                    target_kind="node",
                )
                return NormalizedSetNode(
                    kind="set",
                    pattern_kind="node",
                    node=node,
                    predicates=predicates,
                    assignments=assignments,
                )

            delete_ctx = updating_clauses[0].oC_Delete()
            assert delete_ctx is not None
            delete_text = _context_text(result, delete_ctx).strip()
            detach_match = re.fullmatch(
                r"(?is)detach\s+delete\s+(?P<target>[A-Za-z_][A-Za-z0-9_]*)",
                delete_text,
            )
            delete_match = re.fullmatch(
                r"(?is)delete\s+(?P<target>[A-Za-z_][A-Za-z0-9_]*)",
                delete_text,
            )
            detach = detach_match is not None
            target_alias = (
                detach_match.group("target")
                if detach_match is not None
                else delete_match.group("target")
                if delete_match is not None
                else None
            )
            if target_alias is None:
                raise ValueError(
                    "Generated Cypher frontend currently validates only narrow "
                    "MATCH ... DELETE alias and MATCH ... DETACH DELETE alias "
                    "statements."
                )

            if _looks_like_relationship_pattern(pattern_text):
                if detach:
                    raise ValueError(
                        "HumemCypher v0 currently supports DETACH DELETE only "
                        "for matched node aliases."
                    )
                left_text, relationship_text, right_text, direction = (
                    _split_relationship_pattern(pattern_text)
                )
                left = _parse_node_pattern(
                    left_text,
                    default_alias="__humem_delete_left_node",
                )
                relationship = _parse_relationship_pattern(relationship_text, direction)
                right = _parse_node_pattern(
                    right_text,
                    default_alias="__humem_delete_right_node",
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
                if relationship.alias is None or target_alias != relationship.alias:
                    raise ValueError(
                        "HumemCypher v0 MATCH ... DELETE relationship statements "
                        "must delete the matched relationship alias."
                    )
                return NormalizedDeleteRelationship(
                    kind="delete",
                    pattern_kind="relationship",
                    left=left,
                    relationship=relationship,
                    right=right,
                    predicates=predicates,
                )

            node = _parse_node_pattern(
                _unwrap_node_pattern(pattern_text),
                default_alias="__humem_delete_node",
            )
            _validate_normalized_match_predicates(
                predicates,
                alias_kinds={node.alias: "node"},
            )
            if target_alias != node.alias:
                raise ValueError(
                    "HumemCypher v0 MATCH ... DELETE node statements must delete "
                    "the matched node alias."
                )
            if not detach:
                raise ValueError(
                    "HumemCypher v0 currently supports node deletion only through "
                    "DETACH DELETE."
                )
            return NormalizedDeleteNode(
                kind="delete",
                pattern_kind="node",
                node=node,
                predicates=predicates,
                detach=True,
            )

        create_ctx = updating_clauses[0].oC_Create()
        merge_ctx = updating_clauses[0].oC_Merge()
        pattern_text = (
            _context_text(result, create_ctx.oC_Pattern())
            if create_ctx is not None
            else _context_text(result, merge_ctx.oC_PatternPart())
        )
        create_patterns = _split_comma_separated(pattern_text)
        if len(create_patterns) == 3:
            if merge_ctx is not None:
                raise ValueError(
                    "HumemCypher v0 MERGE currently supports only one labeled node pattern or one directed relationship pattern."
                )
            if (
                any(
                    _looks_like_relationship_pattern(pattern)
                    for pattern in create_patterns[:2]
                )
                or not _looks_like_relationship_pattern(create_patterns[2])
            ):
                raise ValueError(
                    "HumemCypher v0 CREATE currently supports either one node "
                    "pattern, one directed relationship pattern, or the narrow "
                    "three-pattern form with two node patterns followed by one "
                    "relationship pattern."
                )

            first_node = _parse_node_pattern(
                _unwrap_node_pattern(create_patterns[0]),
                require_label=True,
                default_alias="__humem_create_first_node",
            )
            second_node = _parse_node_pattern(
                _unwrap_node_pattern(create_patterns[1]),
                require_label=True,
                default_alias="__humem_create_second_node",
            )
            left_text, relationship_text, right_text, direction = (
                _split_relationship_pattern(create_patterns[2])
            )
            left = _parse_node_pattern(
                left_text,
                default_alias="__humem_create_left_node",
            )
            relationship = _parse_relationship_pattern(relationship_text, direction)
            right = _parse_node_pattern(
                right_text,
                default_alias="__humem_create_right_node",
            )
            _validate_create_relationship_separate_patterns(
                first_node,
                second_node,
                left,
                right,
            )
            return NormalizedCreateRelationshipFromSeparatePatterns(
                kind="create",
                pattern_kind="relationship",
                first_node=first_node,
                second_node=second_node,
                left=left,
                relationship=relationship,
                right=right,
            )

        if _looks_like_relationship_pattern(pattern_text):
            left_text, relationship_text, right_text, direction = (
                _split_relationship_pattern(pattern_text)
            )
            if merge_ctx is not None:
                return NormalizedMergeRelationship(
                    kind="merge",
                    pattern_kind="relationship",
                    left=_parse_node_pattern(
                        left_text,
                        require_label=True,
                        default_alias="__humem_merge_left_node",
                    ),
                    relationship=_parse_relationship_pattern(relationship_text, direction),
                    right=_parse_node_pattern(
                        right_text,
                        require_label=True,
                        default_alias="__humem_merge_right_node",
                    ),
                )
            return NormalizedCreateRelationship(
                kind="create",
                pattern_kind="relationship",
                left=_parse_node_pattern(
                    left_text,
                    require_label=True,
                    default_alias="__humem_create_left_node",
                ),
                relationship=_parse_relationship_pattern(relationship_text, direction),
                right=_parse_node_pattern(
                    right_text,
                    require_label=True,
                    default_alias="__humem_create_right_node",
                ),
            )

        if merge_ctx is not None:
            return NormalizedMergeNode(
                kind="merge",
                pattern_kind="node",
                node=_parse_node_pattern(
                    _unwrap_node_pattern(pattern_text),
                    require_label=True,
                    default_alias="__humem_merge_node",
                ),
            )

        return NormalizedCreateNode(
            kind="create",
            pattern_kind="node",
            node=_parse_node_pattern(
                _unwrap_node_pattern(pattern_text),
                require_label=True,
                default_alias="__humem_create_node",
            ),
        )

    match_ctx = reading_clauses[0].oC_Match()
    assert match_ctx is not None
    pattern_text = _context_text(result, match_ctx.oC_Pattern())
    predicates: tuple[Predicate, ...] = ()
    where_ctx = match_ctx.oC_Where()
    if where_ctx is not None:
        predicates = _parse_predicates(
            _context_text(result, where_ctx.oC_Expression())
        )

    return_ctx = single_part_query.oC_Return()
    assert return_ctx is not None
    projection_text = _context_text(result, return_ctx.oC_ProjectionBody())
    return_text, order_by, limit, distinct, skip = _split_return_clause(
        projection_text
    )
    returns = _parse_return_items(return_text)

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
            returns=returns,
            order_by=order_by,
            limit=limit,
            distinct=distinct,
            skip=skip,
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
            returns=returns,
            order_by=order_by,
            limit=limit,
            distinct=distinct,
            skip=skip,
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
        returns=returns,
        order_by=order_by,
        limit=limit,
        distinct=distinct,
        skip=skip,
    )


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


def _normalize_optional_match_node(
    result: CypherParseResult,
    single_part_query,
) -> NormalizedOptionalMatchNode:
    match_ctx = single_part_query.oC_ReadingClause()[0].oC_Match()
    assert match_ctx is not None
    pattern_text = _context_text(result, match_ctx.oC_Pattern())
    node = _parse_node_pattern(
        _unwrap_node_pattern(pattern_text),
        default_alias="__humem_optional_match_node",
    )

    predicates: tuple[Predicate, ...] = ()
    where_ctx = match_ctx.oC_Where()
    if where_ctx is not None:
        predicates = _parse_predicates(_context_text(result, where_ctx.oC_Expression()))
    _validate_normalized_match_predicates(predicates, alias_kinds={node.alias: "node"})

    return_ctx = single_part_query.oC_Return()
    assert return_ctx is not None
    projection_text = _context_text(result, return_ctx.oC_ProjectionBody())
    return_text, order_by, limit, distinct, skip = _split_return_clause(projection_text)
    returns = _parse_return_items(return_text)

    return NormalizedOptionalMatchNode(
        kind="optional_match",
        pattern_kind="node",
        node=node,
        predicates=predicates,
        returns=returns,
        order_by=order_by,
        limit=limit,
        distinct=distinct,
        skip=skip,
    )


def _source_alias_kinds(
    source: NormalizedMatchNode | NormalizedMatchRelationship | NormalizedMatchChain,
) -> dict[str, Literal["node", "relationship"]]:
    if isinstance(source, NormalizedMatchNode):
        return {source.node.alias: "node"}

    if isinstance(source, NormalizedMatchChain):
        alias_kinds: dict[str, Literal["node", "relationship"]] = {
            node.alias: "node" for node in source.nodes
        }
        for relationship in source.relationships:
            if relationship.alias is not None:
                alias_kinds[relationship.alias] = "relationship"
        return alias_kinds

    alias_kinds: dict[str, Literal["node", "relationship"]] = {
        source.left.alias: "node",
        source.right.alias: "node",
    }
    if source.relationship.alias is not None:
        alias_kinds[source.relationship.alias] = "relationship"
    return alias_kinds


def _source_node_patterns(
    source: NormalizedMatchRelationship | NormalizedMatchChain,
) -> dict[str, NodePattern]:
    if isinstance(source, NormalizedMatchChain):
        return {node.alias: node for node in source.nodes}

    return {
        source.left.alias: source.left,
        source.right.alias: source.right,
    }


def _validate_match_write_reused_source_endpoints(
    source: NormalizedMatchRelationship | NormalizedMatchChain,
    left: NodePattern,
    right: NodePattern,
    *,
    allow_one_new_endpoint: bool = False,
) -> None:
    source_nodes = _source_node_patterns(source)
    reused_count = 0
    new_count = 0
    for endpoint in (left, right):
        source_node = source_nodes.get(endpoint.alias)
        if source_node is None:
            new_count += 1
            if not allow_one_new_endpoint:
                raise ValueError(
                    "HumemCypher v0 traversal-backed MATCH write clauses currently require relationship endpoints to reuse matched node aliases exactly."
                )
            continue

        reused_count += 1
        if endpoint.properties:
            raise ValueError(
                "HumemCypher v0 traversal-backed MATCH write clauses currently do not allow inline properties on reused endpoint aliases."
            )
        if endpoint.label is not None and endpoint.label != source_node.label:
            raise ValueError(
                "HumemCypher v0 traversal-backed MATCH write clauses currently require reused endpoint labels to match the matched source aliases."
            )

    if allow_one_new_endpoint and (reused_count == 0 or new_count > 1):
        raise ValueError(
            "HumemCypher v0 traversal-backed MATCH ... CREATE currently supports exactly one reused matched node alias plus at most one fresh endpoint node."
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

        field_match = re.fullmatch(
            r"(?P<alias>[A-Za-z_][A-Za-z0-9_]*)\.(?P<field>[A-Za-z_][A-Za-z0-9_]*)",
            expression_text,
        )
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

            target_text = left_text.strip()
            id_match = re.fullmatch(
                r"id\s*\(\s*(?P<alias>[A-Za-z_][A-Za-z0-9_]*)\s*\)",
                target_text,
                flags=re.IGNORECASE,
            )
            if id_match is not None:
                alias = id_match.group("alias")
                binding = binding_map.get(alias)
                if binding is None or binding.binding_kind != "entity":
                    raise ValueError(
                        "HumemCypher v0 WITH WHERE currently supports id(entity_alias) only for entity bindings."
                    )
                if operator in {"IS NULL", "IS NOT NULL"}:
                    parsed_value = None
                    if value_text.strip():
                        raise ValueError(
                            "HumemCypher v0 null predicates cannot include a trailing "
                            "literal value."
                        )
                else:
                    parsed_value = _parse_literal(value_text.strip())
                predicates.append(
                    WithPredicate(
                        kind="field",
                        alias=alias,
                        field="id",
                        operator=operator,
                        value=parsed_value,
                        disjunct_index=disjunct_index,
                    )
                )
                continue

            type_match = re.fullmatch(
                r"type\s*\(\s*(?P<alias>[A-Za-z_][A-Za-z0-9_]*)\s*\)",
                target_text,
                flags=re.IGNORECASE,
            )
            if type_match is not None:
                alias = type_match.group("alias")
                binding = binding_map.get(alias)
                if (
                    binding is None
                    or binding.binding_kind != "entity"
                    or binding.alias_kind != "relationship"
                ):
                    raise ValueError(
                        "HumemCypher v0 WITH WHERE currently supports type(rel_alias) only for relationship entity bindings."
                    )
                if operator in {"IS NULL", "IS NOT NULL"}:
                    parsed_value = None
                    if value_text.strip():
                        raise ValueError(
                            "HumemCypher v0 null predicates cannot include a trailing "
                            "literal value."
                        )
                else:
                    parsed_value = _parse_literal(value_text.strip())
                predicates.append(
                    WithPredicate(
                        kind="field",
                        alias=alias,
                        field="type",
                        operator=operator,
                        value=parsed_value,
                        disjunct_index=disjunct_index,
                    )
                )
                continue

            size_match = re.fullmatch(
                r"size\s*\(\s*(?P<expr>.+?)\s*\)",
                target_text,
                flags=re.IGNORECASE,
            )
            if size_match is not None:
                size_expr = size_match.group("expr").strip()
                binding = binding_map.get(size_expr)
                if binding is not None and binding.binding_kind == "scalar":
                    if operator in {"IS NULL", "IS NOT NULL"}:
                        parsed_value = None
                        if value_text.strip():
                            raise ValueError(
                                "HumemCypher v0 null predicates cannot include a trailing "
                                "literal value."
                            )
                    else:
                        parsed_value = _parse_literal(value_text.strip())
                    predicates.append(
                        WithPredicate(
                            kind="field",
                            alias=size_expr,
                            field=f"{_SIZE_PREDICATE_FIELD_PREFIX}__value__",
                            operator=operator,
                            value=parsed_value,
                            disjunct_index=disjunct_index,
                        )
                    )
                    continue

                field_match = re.fullmatch(
                    r"(?P<alias>[A-Za-z_][A-Za-z0-9_]*)\.(?P<field>[A-Za-z_][A-Za-z0-9_]*)",
                    size_expr,
                )
                if field_match is None:
                    raise ValueError(
                        "HumemCypher v0 WITH WHERE items must look like scalar_alias OP value, entity_alias.field OP value, id(entity_alias) OP value, type(rel_alias) OP value, or size(admitted_input) OP value."
                    )
                alias = field_match.group("alias")
                binding = binding_map.get(alias)
                if binding is None or binding.binding_kind != "entity":
                    raise ValueError(
                        "HumemCypher v0 WITH WHERE currently supports size(entity_alias.field) only for entity bindings, or size(scalar_alias) for scalar bindings."
                    )
                if operator in {"IS NULL", "IS NOT NULL"}:
                    parsed_value = None
                    if value_text.strip():
                        raise ValueError(
                            "HumemCypher v0 null predicates cannot include a trailing "
                            "literal value."
                        )
                else:
                    parsed_value = _parse_literal(value_text.strip())
                predicates.append(
                    WithPredicate(
                        kind="field",
                        alias=alias,
                        field=f"{_SIZE_PREDICATE_FIELD_PREFIX}{field_match.group('field')}",
                        operator=operator,
                        value=parsed_value,
                        disjunct_index=disjunct_index,
                    )
                )
                continue

            field_match = re.fullmatch(
                r"(?P<alias>[A-Za-z_][A-Za-z0-9_]*)\.(?P<field>[A-Za-z_][A-Za-z0-9_]*)",
                target_text,
            )
            if field_match is not None:
                alias = field_match.group("alias")
                binding = binding_map.get(alias)
                if binding is None or binding.binding_kind != "entity":
                    raise ValueError(
                        "HumemCypher v0 WITH WHERE currently supports entity_alias.field "
                        "only for entity bindings."
                    )
                if operator in {"IS NULL", "IS NOT NULL"}:
                    parsed_value = None
                    if value_text.strip():
                        raise ValueError(
                            "HumemCypher v0 null predicates cannot include a trailing "
                            "literal value."
                        )
                else:
                    parsed_value = _parse_literal(value_text.strip())
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
            if operator in {"IS NULL", "IS NOT NULL"}:
                parsed_value = None
                if value_text.strip():
                    raise ValueError(
                        "HumemCypher v0 null predicates cannot include a trailing "
                        "literal value."
                    )
            else:
                parsed_value = _parse_literal(value_text.strip())
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
    field_match = re.fullmatch(
        r"(?P<alias>[A-Za-z_][A-Za-z0-9_]*)\.(?P<field>[A-Za-z_][A-Za-z0-9_]*)",
        expression_text,
    )
    if field_match is not None:
        alias = field_match.group("alias")
        binding = binding_map.get(alias)
        if binding is None or binding.binding_kind != "entity":
            raise ValueError(
                "HumemCypher v0 searched CASE results currently support entity_alias.field only for admitted entity bindings."
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
    left_text = left_text.strip()
    value_text = value_text.strip()
    if operator in {"IS NULL", "IS NOT NULL"}:
        if value_text:
            raise ValueError(
                "HumemCypher v0 searched CASE null predicates cannot include a trailing literal value in the WITH subset."
            )
        parsed_value: object | None = None
    else:
        parsed_value = _parse_literal(value_text)

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
        if field_match is not None:
            alias = field_match.group("alias")
            binding = binding_map.get(alias)
            if binding is None or binding.binding_kind != "entity":
                raise ValueError(
                    "HumemCypher v0 searched CASE WHEN size(...) conditions currently support entity_alias.field only for admitted entity bindings in the WITH subset."
                )
            return WithReturnItem(
                kind="predicate",
                alias=alias,
                field=f"{_SIZE_PREDICATE_FIELD_PREFIX}{field_match.group('field')}",
                operator=operator,
                value=parsed_value,
            )
        binding = binding_map.get(size_expr)
        if binding is not None and binding.binding_kind == "scalar":
            return WithReturnItem(
                kind="predicate",
                alias=size_expr,
                field=f"{_SIZE_PREDICATE_FIELD_PREFIX}__value__",
                operator=operator,
                value=parsed_value,
            )
        raise ValueError(
            "HumemCypher v0 searched CASE WHEN size(...) conditions currently support only admitted entity-field projections or admitted scalar bindings in the WITH subset."
        )

    field_match = re.fullmatch(
        r"(?P<alias>[A-Za-z_][A-Za-z0-9_]*)\.(?P<field>[A-Za-z_][A-Za-z0-9_]*)",
        left_text,
    )
    if field_match is not None:
        alias = field_match.group("alias")
        binding = binding_map.get(alias)
        if binding is None or binding.binding_kind != "entity":
            raise ValueError(
                "HumemCypher v0 searched CASE WHEN conditions currently support entity_alias.field only for admitted entity bindings in the WITH subset."
            )
        return WithReturnItem(
            kind="predicate",
            alias=alias,
            field=field_match.group("field"),
            operator=operator,
            value=parsed_value,
        )

    binding = binding_map.get(left_text)
    if binding is not None and binding.binding_kind == "scalar":
        return WithReturnItem(
            kind="predicate",
            alias=left_text,
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
        item_text = raw_item.strip()
        alias_match = re.fullmatch(
            r"(?P<expr>.+?)\s+as\s+(?P<output>[A-Za-z_][A-Za-z0-9_]*)",
            item_text,
            flags=re.IGNORECASE,
        )
        output_alias = alias_match.group("output") if alias_match is not None else None
        expression_text = alias_match.group("expr").strip() if alias_match is not None else item_text

        case_spec = _parse_with_case_expression(expression_text, binding_map)
        if case_spec is not None:
            if output_alias is None:
                raise ValueError(
                    "HumemCypher v0 WITH searched CASE projections currently require an explicit AS alias."
                )
            if output_alias in seen_output_names:
                raise ValueError(
                    f"HumemCypher v0 WITH queries do not allow duplicate RETURN output alias {output_alias!r}."
                )
            seen_output_names.add(output_alias)
            items.append(
                WithReturnItem(
                    kind="case",
                    alias=output_alias,
                    output_alias=output_alias,
                    value=case_spec,
                )
            )
            continue

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
            output_name = output_alias or expression_text
            if output_name in seen_output_names:
                raise ValueError(
                    f"HumemCypher v0 WITH queries do not allow duplicate RETURN output alias {output_name!r}."
                )
            seen_output_names.add(output_name)
            items.append(
                WithReturnItem(
                    kind="count",
                    alias=alias,
                    output_alias=output_alias,
                )
            )
            continue

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
            output_name = output_alias or expression_text
            if output_name in seen_output_names:
                raise ValueError(
                    f"HumemCypher v0 WITH queries do not allow duplicate RETURN output alias {output_name!r}."
                )
            seen_output_names.add(output_name)
            items.append(
                WithReturnItem(
                    kind=cast(
                        Literal["sum", "avg", "min", "max"],
                        aggregate_match.group("func").lower(),
                    ),
                    alias=alias,
                    output_alias=output_alias,
                )
            )
            continue

        id_match = re.fullmatch(
            r"id\s*\(\s*(?P<alias>[A-Za-z_][A-Za-z0-9_]*)\s*\)",
            expression_text,
            flags=re.IGNORECASE,
        )
        if id_match is not None:
            alias = id_match.group("alias")
            binding = binding_map.get(alias)
            if binding is None or binding.binding_kind != "entity":
                raise ValueError(
                    f"HumemCypher v0 WITH queries cannot apply id(...) to unknown entity binding {alias!r}."
                )
            output_name = output_alias or expression_text
            if output_name in seen_output_names:
                raise ValueError(
                    f"HumemCypher v0 WITH queries do not allow duplicate RETURN output alias {output_name!r}."
                )
            seen_output_names.add(output_name)
            items.append(
                WithReturnItem(
                    kind="id",
                    alias=alias,
                    output_alias=output_alias,
                )
            )
            continue

        type_match = re.fullmatch(
            r"type\s*\(\s*(?P<alias>[A-Za-z_][A-Za-z0-9_]*)\s*\)",
            expression_text,
            flags=re.IGNORECASE,
        )
        if type_match is not None:
            alias = type_match.group("alias")
            binding = binding_map.get(alias)
            if (
                binding is None
                or binding.binding_kind != "entity"
                or binding.alias_kind != "relationship"
            ):
                raise ValueError(
                    f"HumemCypher v0 WITH queries cannot apply type(...) to unknown relationship binding {alias!r}."
                )
            output_name = output_alias or expression_text
            if output_name in seen_output_names:
                raise ValueError(
                    f"HumemCypher v0 WITH queries do not allow duplicate RETURN output alias {output_name!r}."
                )
            seen_output_names.add(output_name)
            items.append(
                WithReturnItem(
                    kind="type",
                    alias=alias,
                    output_alias=output_alias,
                )
            )
            continue

        properties_match = re.fullmatch(
            r"properties\s*\(\s*(?P<alias>[A-Za-z_][A-Za-z0-9_]*)\s*\)",
            expression_text,
            flags=re.IGNORECASE,
        )
        if properties_match is not None:
            alias = properties_match.group("alias")
            binding = binding_map.get(alias)
            if binding is None or binding.binding_kind != "entity":
                raise ValueError(
                    f"HumemCypher v0 WITH queries cannot apply properties(...) to unknown entity binding {alias!r}."
                )
            output_name = output_alias or expression_text
            if output_name in seen_output_names:
                raise ValueError(
                    f"HumemCypher v0 WITH queries do not allow duplicate RETURN output alias {output_name!r}."
                )
            seen_output_names.add(output_name)
            items.append(
                WithReturnItem(
                    kind="properties",
                    alias=alias,
                    output_alias=output_alias,
                )
            )
            continue

        labels_match = re.fullmatch(
            r"labels\s*\(\s*(?P<alias>[A-Za-z_][A-Za-z0-9_]*)\s*\)",
            expression_text,
            flags=re.IGNORECASE,
        )
        if labels_match is not None:
            alias = labels_match.group("alias")
            binding = binding_map.get(alias)
            if (
                binding is None
                or binding.binding_kind != "entity"
                or binding.alias_kind != "node"
            ):
                raise ValueError(
                    f"HumemCypher v0 WITH queries cannot apply labels(...) to unknown node binding {alias!r}."
                )
            output_name = output_alias or expression_text
            if output_name in seen_output_names:
                raise ValueError(
                    f"HumemCypher v0 WITH queries do not allow duplicate RETURN output alias {output_name!r}."
                )
            seen_output_names.add(output_name)
            items.append(
                WithReturnItem(
                    kind="labels",
                    alias=alias,
                    output_alias=output_alias,
                )
            )
            continue

        keys_match = re.fullmatch(
            r"keys\s*\(\s*(?P<alias>[A-Za-z_][A-Za-z0-9_]*)\s*\)",
            expression_text,
            flags=re.IGNORECASE,
        )
        if keys_match is not None:
            alias = keys_match.group("alias")
            binding = binding_map.get(alias)
            if binding is None or binding.binding_kind != "entity":
                raise ValueError(
                    f"HumemCypher v0 WITH queries cannot apply keys(...) to unknown entity binding {alias!r}."
                )
            output_name = output_alias or expression_text
            if output_name in seen_output_names:
                raise ValueError(
                    f"HumemCypher v0 WITH queries do not allow duplicate RETURN output alias {output_name!r}."
                )
            seen_output_names.add(output_name)
            items.append(
                WithReturnItem(
                    kind="keys",
                    alias=alias,
                    output_alias=output_alias,
                )
            )
            continue

        start_node_match = re.fullmatch(
            r"startNode\s*\(\s*(?P<alias>[A-Za-z_][A-Za-z0-9_]*)\s*\)(?:\.(?P<field>[A-Za-z_][A-Za-z0-9_]*))?",
            expression_text,
            flags=re.IGNORECASE,
        )
        if start_node_match is not None:
            alias = start_node_match.group("alias")
            binding = binding_map.get(alias)
            if (
                binding is None
                or binding.binding_kind != "entity"
                or binding.alias_kind != "relationship"
            ):
                raise ValueError(
                    f"HumemCypher v0 WITH queries cannot apply startNode(...) to unknown relationship binding {alias!r}."
                )
            output_name = output_alias or expression_text
            if output_name in seen_output_names:
                raise ValueError(
                    f"HumemCypher v0 WITH queries do not allow duplicate RETURN output alias {output_name!r}."
                )
            seen_output_names.add(output_name)
            items.append(
                WithReturnItem(
                    kind="start_node",
                    alias=alias,
                    field=start_node_match.group("field"),
                    output_alias=output_alias,
                )
            )
            continue

        end_node_match = re.fullmatch(
            r"endNode\s*\(\s*(?P<alias>[A-Za-z_][A-Za-z0-9_]*)\s*\)(?:\.(?P<field>[A-Za-z_][A-Za-z0-9_]*))?",
            expression_text,
            flags=re.IGNORECASE,
        )
        if end_node_match is not None:
            alias = end_node_match.group("alias")
            binding = binding_map.get(alias)
            if (
                binding is None
                or binding.binding_kind != "entity"
                or binding.alias_kind != "relationship"
            ):
                raise ValueError(
                    f"HumemCypher v0 WITH queries cannot apply endNode(...) to unknown relationship binding {alias!r}."
                )
            output_name = output_alias or expression_text
            if output_name in seen_output_names:
                raise ValueError(
                    f"HumemCypher v0 WITH queries do not allow duplicate RETURN output alias {output_name!r}."
                )
            seen_output_names.add(output_name)
            items.append(
                WithReturnItem(
                    kind="end_node",
                    alias=alias,
                    field=end_node_match.group("field"),
                    output_alias=output_alias,
                )
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
                scalar_binding = binding_map.get(size_expr)
                if scalar_binding is not None and scalar_binding.binding_kind == "scalar":
                    if expression_text in seen_output_names:
                        raise ValueError(
                            f"HumemCypher v0 WITH queries do not allow duplicate RETURN output alias {expression_text!r}."
                        )
                    seen_output_names.add(expression_text)
                    items.append(WithReturnItem(kind="size", alias=size_expr))
                    continue
                id_match = re.fullmatch(
                    r"id\s*\(\s*(?P<alias>[A-Za-z_][A-Za-z0-9_]*)\s*\)",
                    size_expr,
                    flags=re.IGNORECASE,
                )
                if id_match is not None:
                    alias = id_match.group("alias")
                    binding = binding_map.get(alias)
                    if binding is None or binding.binding_kind != "entity":
                        raise ValueError(
                            f"HumemCypher v0 WITH queries cannot apply size(id(...)) to unknown entity binding {alias!r}."
                        )
                    if expression_text in seen_output_names:
                        raise ValueError(
                            f"HumemCypher v0 WITH queries do not allow duplicate RETURN output alias {expression_text!r}."
                        )
                    seen_output_names.add(expression_text)
                    items.append(WithReturnItem(kind="size", alias=alias, field="id"))
                    continue
                type_match = re.fullmatch(
                    r"type\s*\(\s*(?P<alias>[A-Za-z_][A-Za-z0-9_]*)\s*\)",
                    size_expr,
                    flags=re.IGNORECASE,
                )
                if type_match is not None:
                    alias = type_match.group("alias")
                    binding = binding_map.get(alias)
                    if (
                        binding is None
                        or binding.binding_kind != "entity"
                        or binding.alias_kind != "relationship"
                    ):
                        raise ValueError(
                            f"HumemCypher v0 WITH queries cannot apply size(type(...)) to unknown relationship binding {alias!r}."
                        )
                    if expression_text in seen_output_names:
                        raise ValueError(
                            f"HumemCypher v0 WITH queries do not allow duplicate RETURN output alias {expression_text!r}."
                        )
                    seen_output_names.add(expression_text)
                    items.append(WithReturnItem(kind="size", alias=alias, field="type"))
                    continue
                field_match = re.fullmatch(
                    r"(?P<alias>[A-Za-z_][A-Za-z0-9_]*)\.(?P<field>[A-Za-z_][A-Za-z0-9_]*)",
                    size_expr,
                )
                if field_match is not None:
                    alias = field_match.group("alias")
                    binding = binding_map.get(alias)
                    if binding is None or binding.binding_kind != "entity":
                        raise ValueError(
                            "HumemCypher v0 WITH size(...) projections currently support entity_alias.field only for admitted entity bindings when AS is omitted."
                        )
                    if expression_text in seen_output_names:
                        raise ValueError(
                            f"HumemCypher v0 WITH queries do not allow duplicate RETURN output alias {expression_text!r}."
                        )
                    seen_output_names.add(expression_text)
                    items.append(
                        WithReturnItem(
                            kind="size",
                            alias=alias,
                            field=field_match.group("field"),
                        )
                    )
                    continue

            unary_match = re.fullmatch(
                r"(?P<func>lower|upper|trim|ltrim|rtrim|reverse|abs|sign|round|ceil|floor|sqrt|exp|sin|cos|tan|asin|acos|atan|ln|log|log10|radians|degrees|tostring|tointeger|tofloat|toboolean)\s*\(\s*(?P<expr>.+?)\s*\)",
                expression_text,
                flags=re.IGNORECASE,
            )
            if unary_match is not None:
                function_expr = unary_match.group("expr").strip()
                kind = {
                    "tostring": "to_string",
                    "tointeger": "to_integer",
                    "tofloat": "to_float",
                    "toboolean": "to_boolean",
                }.get(unary_match.group("func").lower(), unary_match.group("func").lower())
                scalar_binding = binding_map.get(function_expr)
                if scalar_binding is not None and scalar_binding.binding_kind == "scalar":
                    if expression_text in seen_output_names:
                        raise ValueError(
                            f"HumemCypher v0 WITH queries do not allow duplicate RETURN output alias {expression_text!r}."
                        )
                    seen_output_names.add(expression_text)
                    items.append(
                        WithReturnItem(
                            kind=cast(WithReturnItem.__annotations__["kind"], kind),
                            alias=function_expr,
                        )
                    )
                    continue
                field_match = re.fullmatch(
                    r"(?P<alias>[A-Za-z_][A-Za-z0-9_]*)\.(?P<field>[A-Za-z_][A-Za-z0-9_]*)",
                    function_expr,
                )
                if field_match is not None:
                    alias = field_match.group("alias")
                    binding = binding_map.get(alias)
                    if binding is None or binding.binding_kind != "entity":
                        raise ValueError(
                            "HumemCypher v0 WITH unary function projections currently support entity_alias.field only for admitted entity bindings when AS is omitted."
                        )
                    if expression_text in seen_output_names:
                        raise ValueError(
                            f"HumemCypher v0 WITH queries do not allow duplicate RETURN output alias {expression_text!r}."
                        )
                    seen_output_names.add(expression_text)
                    items.append(
                        WithReturnItem(
                            kind=cast(WithReturnItem.__annotations__["kind"], kind),
                            alias=alias,
                            field=field_match.group("field"),
                        )
                    )
                    continue

        size_match = re.fullmatch(
            r"size\s*\(\s*(?P<expr>.+?)\s*\)",
            expression_text,
            flags=re.IGNORECASE,
        )
        if size_match is not None:
            if output_alias is None:
                raise ValueError(
                    "HumemCypher v0 WITH size(...) projections currently require an explicit AS alias."
                )
            size_expr = size_match.group("expr").strip()
            scalar_binding = binding_map.get(size_expr)
            if scalar_binding is not None and scalar_binding.binding_kind == "scalar":
                if output_alias in seen_output_names:
                    raise ValueError(
                        f"HumemCypher v0 WITH queries do not allow duplicate RETURN output alias {output_alias!r}."
                    )
                seen_output_names.add(output_alias)
                items.append(
                    WithReturnItem(
                        kind="size",
                        alias=size_expr,
                        output_alias=output_alias,
                    )
                )
                continue

            id_match = re.fullmatch(
                r"id\s*\(\s*(?P<alias>[A-Za-z_][A-Za-z0-9_]*)\s*\)",
                size_expr,
                flags=re.IGNORECASE,
            )
            if id_match is not None:
                alias = id_match.group("alias")
                binding = binding_map.get(alias)
                if binding is not None and binding.binding_kind == "entity":
                    if output_alias in seen_output_names:
                        raise ValueError(
                            f"HumemCypher v0 WITH queries do not allow duplicate RETURN output alias {output_alias!r}."
                        )
                    seen_output_names.add(output_alias)
                    items.append(
                        WithReturnItem(
                            kind="size",
                            alias=alias,
                            field="id",
                            output_alias=output_alias,
                        )
                    )
                    continue

            type_match = re.fullmatch(
                r"type\s*\(\s*(?P<alias>[A-Za-z_][A-Za-z0-9_]*)\s*\)",
                size_expr,
                flags=re.IGNORECASE,
            )
            if type_match is not None:
                alias = type_match.group("alias")
                binding = binding_map.get(alias)
                if (
                    binding is not None
                    and binding.binding_kind == "entity"
                    and binding.alias_kind == "relationship"
                ):
                    if output_alias in seen_output_names:
                        raise ValueError(
                            f"HumemCypher v0 WITH queries do not allow duplicate RETURN output alias {output_alias!r}."
                        )
                    seen_output_names.add(output_alias)
                    items.append(
                        WithReturnItem(
                            kind="size",
                            alias=alias,
                            field="type",
                            output_alias=output_alias,
                        )
                    )
                    continue

            try:
                scalar_value = _parse_literal(size_expr)
            except ValueError:
                pass
            else:
                if output_alias in seen_output_names:
                    raise ValueError(
                        f"HumemCypher v0 WITH queries do not allow duplicate RETURN output alias {output_alias!r}."
                    )
                seen_output_names.add(output_alias)
                items.append(
                    WithReturnItem(
                        kind="size",
                        alias=output_alias,
                        output_alias=output_alias,
                        value=scalar_value,
                    )
                )
                continue

            field_match = re.fullmatch(
                r"(?P<alias>[A-Za-z_][A-Za-z0-9_]*)\.(?P<field>[A-Za-z_][A-Za-z0-9_]*)",
                size_expr,
            )
            if field_match is not None:
                alias = field_match.group("alias")
                binding = binding_map.get(alias)
                if binding is not None and binding.binding_kind == "entity":
                    if output_alias in seen_output_names:
                        raise ValueError(
                            f"HumemCypher v0 WITH queries do not allow duplicate RETURN output alias {output_alias!r}."
                        )
                    seen_output_names.add(output_alias)
                    items.append(
                        WithReturnItem(
                            kind="size",
                            alias=alias,
                            field=field_match.group("field"),
                            output_alias=output_alias,
                        )
                    )
                    continue

            raise ValueError(
                "HumemCypher v0 WITH queries currently support size(...) only over admitted entity-field projections, admitted id/type outputs, scalar bindings, or scalar literal/parameter inputs."
            )

        unary_string_match = re.fullmatch(
            r"(?P<func>lower|upper|trim|ltrim|rtrim|reverse)\s*\(\s*(?P<expr>.+?)\s*\)",
            expression_text,
            flags=re.IGNORECASE,
        )
        if unary_string_match is not None:
            if output_alias is None:
                raise ValueError(
                    "HumemCypher v0 WITH lower(...), upper(...), trim(...), ltrim(...), rtrim(...), and reverse(...) projections currently require an explicit AS alias."
                )
            function_kind = unary_string_match.group("func").lower()
            function_expr = unary_string_match.group("expr").strip()
            scalar_binding = binding_map.get(function_expr)
            if scalar_binding is not None and scalar_binding.binding_kind == "scalar":
                if output_alias in seen_output_names:
                    raise ValueError(
                        f"HumemCypher v0 WITH queries do not allow duplicate RETURN output alias {output_alias!r}."
                    )
                seen_output_names.add(output_alias)
                items.append(
                    WithReturnItem(
                        kind=function_kind,
                        alias=function_expr,
                        output_alias=output_alias,
                    )
                )
                continue
            try:
                scalar_value = _parse_literal(function_expr)
            except ValueError:
                pass
            else:
                if output_alias in seen_output_names:
                    raise ValueError(
                        f"HumemCypher v0 WITH queries do not allow duplicate RETURN output alias {output_alias!r}."
                    )
                seen_output_names.add(output_alias)
                items.append(
                    WithReturnItem(
                        kind=function_kind,
                        alias=output_alias,
                        output_alias=output_alias,
                        value=scalar_value,
                    )
                )
                continue
            field_match = re.fullmatch(
                r"(?P<alias>[A-Za-z_][A-Za-z0-9_]*)\.(?P<field>[A-Za-z_][A-Za-z0-9_]*)",
                function_expr,
            )
            if field_match is not None:
                alias = field_match.group("alias")
                binding = binding_map.get(alias)
                if binding is not None and binding.binding_kind == "entity":
                    if output_alias in seen_output_names:
                        raise ValueError(
                            f"HumemCypher v0 WITH queries do not allow duplicate RETURN output alias {output_alias!r}."
                        )
                    seen_output_names.add(output_alias)
                    items.append(
                        WithReturnItem(
                            kind=function_kind,
                            alias=alias,
                            field=field_match.group("field"),
                            output_alias=output_alias,
                        )
                    )
                    continue
            raise ValueError(
                "HumemCypher v0 WITH queries currently support lower(...), upper(...), trim(...), ltrim(...), rtrim(...), and reverse(...) only over admitted entity-field projections, scalar bindings, or scalar literal/parameter inputs."
            )

        coalesce_match = re.fullmatch(
            r"coalesce\s*\(\s*(?P<args>.+)\s*\)",
            expression_text,
            flags=re.IGNORECASE,
        )
        if coalesce_match is not None:
            args = [part.strip() for part in _split_comma_separated(coalesce_match.group("args"))]
            if len(args) != 2:
                raise ValueError(
                    "HumemCypher v0 WITH coalesce(...) currently requires exactly two arguments."
                )
            primary_expr, fallback_expr = args
            output_name = output_alias or expression_text
            try:
                fallback_value = _parse_literal(fallback_expr)
            except ValueError as exc:
                raise ValueError(
                    "HumemCypher v0 WITH queries currently support coalesce(...) only as coalesce(entity_alias.field, literal_or_parameter) or coalesce(scalar_alias, literal_or_parameter)."
                ) from exc
            scalar_binding = binding_map.get(primary_expr)
            if scalar_binding is not None and scalar_binding.binding_kind == "scalar":
                if output_name in seen_output_names:
                    raise ValueError(
                        f"HumemCypher v0 WITH queries do not allow duplicate RETURN output alias {output_name!r}."
                    )
                seen_output_names.add(output_name)
                items.append(
                    WithReturnItem(
                        kind="coalesce",
                        alias=primary_expr,
                        output_alias=output_alias,
                        value=fallback_value,
                    )
                )
                continue
            field_match = re.fullmatch(
                r"(?P<alias>[A-Za-z_][A-Za-z0-9_]*)\.(?P<field>[A-Za-z_][A-Za-z0-9_]*)",
                primary_expr,
            )
            if field_match is not None:
                alias = field_match.group("alias")
                binding = binding_map.get(alias)
                if binding is not None and binding.binding_kind == "entity":
                    if output_name in seen_output_names:
                        raise ValueError(
                            f"HumemCypher v0 WITH queries do not allow duplicate RETURN output alias {output_name!r}."
                        )
                    seen_output_names.add(output_name)
                    items.append(
                        WithReturnItem(
                            kind="coalesce",
                            alias=alias,
                            field=field_match.group("field"),
                            output_alias=output_alias,
                            value=fallback_value,
                        )
                    )
                    continue
            raise ValueError(
                "HumemCypher v0 WITH queries currently support coalesce(...) only as coalesce(entity_alias.field, literal_or_parameter) or coalesce(scalar_alias, literal_or_parameter)."
            )

        replace_match = re.fullmatch(
            r"replace\s*\(\s*(?P<args>.+)\s*\)",
            expression_text,
            flags=re.IGNORECASE,
        )
        if replace_match is not None:
            args = [part.strip() for part in _split_comma_separated(replace_match.group("args"))]
            if len(args) != 3:
                raise ValueError(
                    "HumemCypher v0 WITH replace(...) currently requires exactly three arguments."
                )
            primary_expr, search_expr, replace_expr = args
            output_name = output_alias or expression_text
            try:
                search_value = _parse_literal(search_expr)
                replace_value = _parse_literal(replace_expr)
            except ValueError as exc:
                raise ValueError(
                    "HumemCypher v0 WITH queries currently support replace(...) only as replace(admitted_input, literal_or_parameter, literal_or_parameter)."
                ) from exc
            scalar_binding = binding_map.get(primary_expr)
            if scalar_binding is not None and scalar_binding.binding_kind == "scalar":
                if output_name in seen_output_names:
                    raise ValueError(
                        f"HumemCypher v0 WITH queries do not allow duplicate RETURN output alias {output_name!r}."
                    )
                seen_output_names.add(output_name)
                items.append(
                    WithReturnItem(
                        kind="replace",
                        alias=primary_expr,
                        output_alias=output_alias,
                        search_value=search_value,
                        replace_value=replace_value,
                    )
                )
                continue
            try:
                scalar_value = _parse_literal(primary_expr)
            except ValueError:
                pass
            else:
                if output_name in seen_output_names:
                    raise ValueError(
                        f"HumemCypher v0 WITH queries do not allow duplicate RETURN output alias {output_name!r}."
                    )
                seen_output_names.add(output_name)
                items.append(
                    WithReturnItem(
                        kind="replace",
                        alias=output_alias,
                        output_alias=output_alias,
                        value=scalar_value,
                        search_value=search_value,
                        replace_value=replace_value,
                    )
                )
                continue
            field_match = re.fullmatch(
                r"(?P<alias>[A-Za-z_][A-Za-z0-9_]*)\.(?P<field>[A-Za-z_][A-Za-z0-9_]*)",
                primary_expr,
            )
            if field_match is not None:
                alias = field_match.group("alias")
                binding = binding_map.get(alias)
                if binding is not None and binding.binding_kind == "entity":
                    if output_name in seen_output_names:
                        raise ValueError(
                            f"HumemCypher v0 WITH queries do not allow duplicate RETURN output alias {output_name!r}."
                        )
                    seen_output_names.add(output_name)
                    items.append(
                        WithReturnItem(
                            kind="replace",
                            alias=alias,
                            field=field_match.group("field"),
                            output_alias=output_alias,
                            search_value=search_value,
                            replace_value=replace_value,
                        )
                    )
                    continue
            raise ValueError(
                "HumemCypher v0 WITH queries currently support replace(...) only as replace(admitted_input, literal_or_parameter, literal_or_parameter)."
            )

        left_right_match = re.fullmatch(
            r"(?P<func>left|right)\s*\(\s*(?P<args>.+)\s*\)",
            expression_text,
            flags=re.IGNORECASE,
        )
        if left_right_match is not None:
            function_kind = cast(Literal["left", "right"], left_right_match.group("func").lower())
            args = [part.strip() for part in _split_comma_separated(left_right_match.group("args"))]
            if len(args) != 2:
                raise ValueError(
                    "HumemCypher v0 WITH left(...) and right(...) currently require exactly two arguments."
                )
            primary_expr, length_expr = args
            output_name = output_alias or expression_text
            try:
                length_value = _parse_literal(length_expr)
            except ValueError as exc:
                raise ValueError(
                    "HumemCypher v0 WITH queries currently support left(...) and right(...) only as function(admitted_input, literal_or_parameter)."
                ) from exc
            scalar_binding = binding_map.get(primary_expr)
            if scalar_binding is not None and scalar_binding.binding_kind == "scalar":
                if output_name in seen_output_names:
                    raise ValueError(
                        f"HumemCypher v0 WITH queries do not allow duplicate RETURN output alias {output_name!r}."
                    )
                seen_output_names.add(output_name)
                items.append(
                    WithReturnItem(
                        kind=function_kind,
                        alias=primary_expr,
                        output_alias=output_alias,
                        length_value=length_value,
                    )
                )
                continue
            try:
                scalar_value = _parse_literal(primary_expr)
            except ValueError:
                pass
            else:
                if output_name in seen_output_names:
                    raise ValueError(
                        f"HumemCypher v0 WITH queries do not allow duplicate RETURN output alias {output_name!r}."
                    )
                seen_output_names.add(output_name)
                items.append(
                    WithReturnItem(
                        kind=function_kind,
                        alias=output_alias,
                        output_alias=output_alias,
                        value=scalar_value,
                        length_value=length_value,
                    )
                )
                continue
            field_match = re.fullmatch(
                r"(?P<alias>[A-Za-z_][A-Za-z0-9_]*)\.(?P<field>[A-Za-z_][A-Za-z0-9_]*)",
                primary_expr,
            )
            if field_match is not None:
                alias = field_match.group("alias")
                binding = binding_map.get(alias)
                if binding is not None and binding.binding_kind == "entity":
                    if output_name in seen_output_names:
                        raise ValueError(
                            f"HumemCypher v0 WITH queries do not allow duplicate RETURN output alias {output_name!r}."
                        )
                    seen_output_names.add(output_name)
                    items.append(
                        WithReturnItem(
                            kind=function_kind,
                            alias=alias,
                            field=field_match.group("field"),
                            output_alias=output_alias,
                            length_value=length_value,
                        )
                    )
                    continue
            raise ValueError(
                "HumemCypher v0 WITH queries currently support left(...) and right(...) only as function(admitted_input, literal_or_parameter)."
            )

        split_match = re.fullmatch(
            r"split\s*\(\s*(?P<args>.+)\s*\)",
            expression_text,
            flags=re.IGNORECASE,
        )
        if split_match is not None:
            args = [part.strip() for part in _split_comma_separated(split_match.group("args"))]
            if len(args) != 2:
                raise ValueError(
                    "HumemCypher v0 WITH split(...) currently requires exactly two arguments."
                )
            primary_expr, delimiter_expr = args
            output_name = output_alias or expression_text
            try:
                delimiter_value = _parse_literal(delimiter_expr)
            except ValueError as exc:
                raise ValueError(
                    "HumemCypher v0 WITH queries currently support split(...) only as split(admitted_input, literal_or_parameter)."
                ) from exc
            scalar_binding = binding_map.get(primary_expr)
            if scalar_binding is not None and scalar_binding.binding_kind == "scalar":
                if output_name in seen_output_names:
                    raise ValueError(
                        f"HumemCypher v0 WITH queries do not allow duplicate RETURN output alias {output_name!r}."
                    )
                seen_output_names.add(output_name)
                items.append(
                    WithReturnItem(
                        kind="split",
                        alias=primary_expr,
                        output_alias=output_alias,
                        delimiter_value=delimiter_value,
                    )
                )
                continue
            try:
                scalar_value = _parse_literal(primary_expr)
            except ValueError:
                pass
            else:
                if output_name in seen_output_names:
                    raise ValueError(
                        f"HumemCypher v0 WITH queries do not allow duplicate RETURN output alias {output_name!r}."
                    )
                seen_output_names.add(output_name)
                items.append(
                    WithReturnItem(
                        kind="split",
                        alias=output_alias,
                        output_alias=output_alias,
                        value=scalar_value,
                        delimiter_value=delimiter_value,
                    )
                )
                continue
            field_match = re.fullmatch(
                r"(?P<alias>[A-Za-z_][A-Za-z0-9_]*)\.(?P<field>[A-Za-z_][A-Za-z0-9_]*)",
                primary_expr,
            )
            if field_match is not None:
                alias = field_match.group("alias")
                binding = binding_map.get(alias)
                if binding is not None and binding.binding_kind == "entity":
                    if output_name in seen_output_names:
                        raise ValueError(
                            f"HumemCypher v0 WITH queries do not allow duplicate RETURN output alias {output_name!r}."
                        )
                    seen_output_names.add(output_name)
                    items.append(
                        WithReturnItem(
                            kind="split",
                            alias=alias,
                            field=field_match.group("field"),
                            output_alias=output_alias,
                            delimiter_value=delimiter_value,
                        )
                    )
                    continue
            raise ValueError(
                "HumemCypher v0 WITH queries currently support split(...) only as split(admitted_input, literal_or_parameter)."
            )

        substring_match = re.fullmatch(
            r"substring\s*\(\s*(?P<args>.+)\s*\)",
            expression_text,
            flags=re.IGNORECASE,
        )
        if substring_match is not None:
            args = [part.strip() for part in _split_comma_separated(substring_match.group("args"))]
            if len(args) not in {2, 3}:
                raise ValueError(
                    "HumemCypher v0 WITH substring(...) currently requires exactly two or three arguments."
                )
            primary_expr, start_expr = args[:2]
            output_name = output_alias or expression_text
            try:
                start_value = _parse_literal(start_expr)
                length_value = _parse_literal(args[2]) if len(args) == 3 else None
            except ValueError as exc:
                raise ValueError(
                    "HumemCypher v0 WITH queries currently support substring(...) only as substring(admitted_input, literal_or_parameter) or substring(admitted_input, literal_or_parameter, literal_or_parameter)."
                ) from exc
            scalar_binding = binding_map.get(primary_expr)
            if scalar_binding is not None and scalar_binding.binding_kind == "scalar":
                if output_name in seen_output_names:
                    raise ValueError(
                        f"HumemCypher v0 WITH queries do not allow duplicate RETURN output alias {output_name!r}."
                    )
                seen_output_names.add(output_name)
                items.append(
                    WithReturnItem(
                        kind="substring",
                        alias=primary_expr,
                        output_alias=output_alias,
                        start_value=start_value,
                        length_value=length_value,
                    )
                )
                continue
            try:
                scalar_value = _parse_literal(primary_expr)
            except ValueError:
                pass
            else:
                if output_name in seen_output_names:
                    raise ValueError(
                        f"HumemCypher v0 WITH queries do not allow duplicate RETURN output alias {output_name!r}."
                    )
                seen_output_names.add(output_name)
                items.append(
                    WithReturnItem(
                        kind="substring",
                        alias=output_alias,
                        output_alias=output_alias,
                        value=scalar_value,
                        start_value=start_value,
                        length_value=length_value,
                    )
                )
                continue
            field_match = re.fullmatch(
                r"(?P<alias>[A-Za-z_][A-Za-z0-9_]*)\.(?P<field>[A-Za-z_][A-Za-z0-9_]*)",
                primary_expr,
            )
            if field_match is not None:
                alias = field_match.group("alias")
                binding = binding_map.get(alias)
                if binding is not None and binding.binding_kind == "entity":
                    if output_name in seen_output_names:
                        raise ValueError(
                            f"HumemCypher v0 WITH queries do not allow duplicate RETURN output alias {output_name!r}."
                        )
                    seen_output_names.add(output_name)
                    items.append(
                        WithReturnItem(
                            kind="substring",
                            alias=alias,
                            field=field_match.group("field"),
                            output_alias=output_alias,
                            start_value=start_value,
                            length_value=length_value,
                        )
                    )
                    continue
            raise ValueError(
                "HumemCypher v0 WITH queries currently support substring(...) only as substring(admitted_input, literal_or_parameter) or substring(admitted_input, literal_or_parameter, literal_or_parameter)."
            )

        abs_match = re.fullmatch(
            r"abs\s*\(\s*(?P<expr>.+?)\s*\)",
            expression_text,
            flags=re.IGNORECASE,
        )
        if abs_match is not None:
            if output_alias is None:
                raise ValueError(
                    "HumemCypher v0 WITH abs(...) projections currently require an explicit AS alias."
                )
            abs_expr = abs_match.group("expr").strip()
            scalar_binding = binding_map.get(abs_expr)
            if scalar_binding is not None and scalar_binding.binding_kind == "scalar":
                if output_alias in seen_output_names:
                    raise ValueError(
                        f"HumemCypher v0 WITH queries do not allow duplicate RETURN output alias {output_alias!r}."
                    )
                seen_output_names.add(output_alias)
                items.append(
                    WithReturnItem(
                        kind="abs",
                        alias=abs_expr,
                        output_alias=output_alias,
                    )
                )
                continue
            try:
                scalar_value = _parse_literal(abs_expr)
            except ValueError:
                pass
            else:
                if output_alias in seen_output_names:
                    raise ValueError(
                        f"HumemCypher v0 WITH queries do not allow duplicate RETURN output alias {output_alias!r}."
                    )
                seen_output_names.add(output_alias)
                items.append(
                    WithReturnItem(
                        kind="abs",
                        alias=output_alias,
                        output_alias=output_alias,
                        value=scalar_value,
                    )
                )
                continue
            field_match = re.fullmatch(
                r"(?P<alias>[A-Za-z_][A-Za-z0-9_]*)\.(?P<field>[A-Za-z_][A-Za-z0-9_]*)",
                abs_expr,
            )
            if field_match is not None:
                alias = field_match.group("alias")
                binding = binding_map.get(alias)
                if binding is not None and binding.binding_kind == "entity":
                    if output_alias in seen_output_names:
                        raise ValueError(
                            f"HumemCypher v0 WITH queries do not allow duplicate RETURN output alias {output_alias!r}."
                        )
                    seen_output_names.add(output_alias)
                    items.append(
                        WithReturnItem(
                            kind="abs",
                            alias=alias,
                            field=field_match.group("field"),
                            output_alias=output_alias,
                        )
                    )
                    continue
            raise ValueError(
                "HumemCypher v0 WITH queries currently support abs(...) only over admitted entity-field projections, scalar bindings, or scalar literal/parameter inputs."
            )

        sign_match = re.fullmatch(
            r"sign\s*\(\s*(?P<expr>.+?)\s*\)",
            expression_text,
            flags=re.IGNORECASE,
        )
        if sign_match is not None:
            if output_alias is None:
                raise ValueError(
                    "HumemCypher v0 WITH sign(...) projections currently require an explicit AS alias."
                )
            sign_expr = sign_match.group("expr").strip()
            scalar_binding = binding_map.get(sign_expr)
            if scalar_binding is not None and scalar_binding.binding_kind == "scalar":
                if output_alias in seen_output_names:
                    raise ValueError(
                        f"HumemCypher v0 WITH queries do not allow duplicate RETURN output alias {output_alias!r}."
                    )
                seen_output_names.add(output_alias)
                items.append(
                    WithReturnItem(
                        kind="sign",
                        alias=sign_expr,
                        output_alias=output_alias,
                    )
                )
                continue
            try:
                scalar_value = _parse_literal(sign_expr)
            except ValueError:
                pass
            else:
                if output_alias in seen_output_names:
                    raise ValueError(
                        f"HumemCypher v0 WITH queries do not allow duplicate RETURN output alias {output_alias!r}."
                    )
                seen_output_names.add(output_alias)
                items.append(
                    WithReturnItem(
                        kind="sign",
                        alias=output_alias,
                        output_alias=output_alias,
                        value=scalar_value,
                    )
                )
                continue
            field_match = re.fullmatch(
                r"(?P<alias>[A-Za-z_][A-Za-z0-9_]*)\.(?P<field>[A-Za-z_][A-Za-z0-9_]*)",
                sign_expr,
            )
            if field_match is not None:
                alias = field_match.group("alias")
                binding = binding_map.get(alias)
                if binding is not None and binding.binding_kind == "entity":
                    if output_alias in seen_output_names:
                        raise ValueError(
                            f"HumemCypher v0 WITH queries do not allow duplicate RETURN output alias {output_alias!r}."
                        )
                    seen_output_names.add(output_alias)
                    items.append(
                        WithReturnItem(
                            kind="sign",
                            alias=alias,
                            field=field_match.group("field"),
                            output_alias=output_alias,
                        )
                    )
                    continue
            raise ValueError(
                "HumemCypher v0 WITH queries currently support sign(...) only over admitted entity-field projections, scalar bindings, or scalar literal/parameter inputs."
            )

        round_match = re.fullmatch(
            r"round\s*\(\s*(?P<expr>.+?)\s*\)",
            expression_text,
            flags=re.IGNORECASE,
        )
        if round_match is not None:
            if output_alias is None:
                raise ValueError(
                    "HumemCypher v0 WITH round(...) projections currently require an explicit AS alias."
                )
            round_expr = round_match.group("expr").strip()
            scalar_binding = binding_map.get(round_expr)
            if scalar_binding is not None and scalar_binding.binding_kind == "scalar":
                if output_alias in seen_output_names:
                    raise ValueError(
                        f"HumemCypher v0 WITH queries do not allow duplicate RETURN output alias {output_alias!r}."
                    )
                seen_output_names.add(output_alias)
                items.append(
                    WithReturnItem(
                        kind="round",
                        alias=round_expr,
                        output_alias=output_alias,
                    )
                )
                continue
            try:
                scalar_value = _parse_literal(round_expr)
            except ValueError:
                pass
            else:
                if output_alias in seen_output_names:
                    raise ValueError(
                        f"HumemCypher v0 WITH queries do not allow duplicate RETURN output alias {output_alias!r}."
                    )
                seen_output_names.add(output_alias)
                items.append(
                    WithReturnItem(
                        kind="round",
                        alias=output_alias,
                        output_alias=output_alias,
                        value=scalar_value,
                    )
                )
                continue
            field_match = re.fullmatch(
                r"(?P<alias>[A-Za-z_][A-Za-z0-9_]*)\.(?P<field>[A-Za-z_][A-Za-z0-9_]*)",
                round_expr,
            )
            if field_match is not None:
                alias = field_match.group("alias")
                binding = binding_map.get(alias)
                if binding is not None and binding.binding_kind == "entity":
                    if output_alias in seen_output_names:
                        raise ValueError(
                            f"HumemCypher v0 WITH queries do not allow duplicate RETURN output alias {output_alias!r}."
                        )
                    seen_output_names.add(output_alias)
                    items.append(
                        WithReturnItem(
                            kind="round",
                            alias=alias,
                            field=field_match.group("field"),
                            output_alias=output_alias,
                        )
                    )
                    continue
            raise ValueError(
                "HumemCypher v0 WITH queries currently support round(...) only over admitted entity-field projections, scalar bindings, or scalar literal/parameter inputs."
            )

        floor_match = re.fullmatch(
            r"floor\s*\(\s*(?P<expr>.+?)\s*\)",
            expression_text,
            flags=re.IGNORECASE,
        )
        if floor_match is not None:
            if output_alias is None:
                raise ValueError(
                    "HumemCypher v0 WITH floor(...) projections currently require an explicit AS alias."
                )
            floor_expr = floor_match.group("expr").strip()
            scalar_binding = binding_map.get(floor_expr)
            if scalar_binding is not None and scalar_binding.binding_kind == "scalar":
                if output_alias in seen_output_names:
                    raise ValueError(
                        f"HumemCypher v0 WITH queries do not allow duplicate RETURN output alias {output_alias!r}."
                    )
                seen_output_names.add(output_alias)
                items.append(
                    WithReturnItem(
                        kind="floor",
                        alias=floor_expr,
                        output_alias=output_alias,
                    )
                )
                continue
            try:
                scalar_value = _parse_literal(floor_expr)
            except ValueError:
                pass
            else:
                if output_alias in seen_output_names:
                    raise ValueError(
                        f"HumemCypher v0 WITH queries do not allow duplicate RETURN output alias {output_alias!r}."
                    )
                seen_output_names.add(output_alias)
                items.append(
                    WithReturnItem(
                        kind="floor",
                        alias=output_alias,
                        output_alias=output_alias,
                        value=scalar_value,
                    )
                )
                continue
            field_match = re.fullmatch(
                r"(?P<alias>[A-Za-z_][A-Za-z0-9_]*)\.(?P<field>[A-Za-z_][A-Za-z0-9_]*)",
                floor_expr,
            )
            if field_match is not None:
                alias = field_match.group("alias")
                binding = binding_map.get(alias)
                if binding is not None and binding.binding_kind == "entity":
                    if output_alias in seen_output_names:
                        raise ValueError(
                            f"HumemCypher v0 WITH queries do not allow duplicate RETURN output alias {output_alias!r}."
                        )
                    seen_output_names.add(output_alias)
                    items.append(
                        WithReturnItem(
                            kind="floor",
                            alias=alias,
                            field=field_match.group("field"),
                            output_alias=output_alias,
                        )
                    )
                    continue
            raise ValueError(
                "HumemCypher v0 WITH queries currently support floor(...) only over admitted entity-field projections, scalar bindings, or scalar literal/parameter inputs."
            )

        ceil_match = re.fullmatch(
            r"ceil\s*\(\s*(?P<expr>.+?)\s*\)",
            expression_text,
            flags=re.IGNORECASE,
        )
        if ceil_match is not None:
            if output_alias is None:
                raise ValueError(
                    "HumemCypher v0 WITH ceil(...) projections currently require an explicit AS alias."
                )
            ceil_expr = ceil_match.group("expr").strip()
            scalar_binding = binding_map.get(ceil_expr)
            if scalar_binding is not None and scalar_binding.binding_kind == "scalar":
                if output_alias in seen_output_names:
                    raise ValueError(
                        f"HumemCypher v0 WITH queries do not allow duplicate RETURN output alias {output_alias!r}."
                    )
                seen_output_names.add(output_alias)
                items.append(
                    WithReturnItem(
                        kind="ceil",
                        alias=ceil_expr,
                        output_alias=output_alias,
                    )
                )
                continue
            try:
                scalar_value = _parse_literal(ceil_expr)
            except ValueError:
                pass
            else:
                if output_alias in seen_output_names:
                    raise ValueError(
                        f"HumemCypher v0 WITH queries do not allow duplicate RETURN output alias {output_alias!r}."
                    )
                seen_output_names.add(output_alias)
                items.append(
                    WithReturnItem(
                        kind="ceil",
                        alias=output_alias,
                        output_alias=output_alias,
                        value=scalar_value,
                    )
                )
                continue
            field_match = re.fullmatch(
                r"(?P<alias>[A-Za-z_][A-Za-z0-9_]*)\.(?P<field>[A-Za-z_][A-Za-z0-9_]*)",
                ceil_expr,
            )
            if field_match is not None:
                alias = field_match.group("alias")
                binding = binding_map.get(alias)
                if binding is not None and binding.binding_kind == "entity":
                    if output_alias in seen_output_names:
                        raise ValueError(
                            f"HumemCypher v0 WITH queries do not allow duplicate RETURN output alias {output_alias!r}."
                        )
                    seen_output_names.add(output_alias)
                    items.append(
                        WithReturnItem(
                            kind="ceil",
                            alias=alias,
                            field=field_match.group("field"),
                            output_alias=output_alias,
                        )
                    )
                    continue
            raise ValueError(
                "HumemCypher v0 WITH queries currently support ceil(...) only over admitted entity-field projections, scalar bindings, or scalar literal/parameter inputs."
            )

        sqrt_match = re.fullmatch(
            r"sqrt\s*\(\s*(?P<expr>.+?)\s*\)",
            expression_text,
            flags=re.IGNORECASE,
        )
        if sqrt_match is not None:
            if output_alias is None:
                raise ValueError(
                    "HumemCypher v0 WITH sqrt(...) projections currently require an explicit AS alias."
                )
            sqrt_expr = sqrt_match.group("expr").strip()
            scalar_binding = binding_map.get(sqrt_expr)
            if scalar_binding is not None and scalar_binding.binding_kind == "scalar":
                if output_alias in seen_output_names:
                    raise ValueError(
                        f"HumemCypher v0 WITH queries do not allow duplicate RETURN output alias {output_alias!r}."
                    )
                seen_output_names.add(output_alias)
                items.append(
                    WithReturnItem(
                        kind="sqrt",
                        alias=sqrt_expr,
                        output_alias=output_alias,
                    )
                )
                continue
            try:
                scalar_value = _parse_literal(sqrt_expr)
            except ValueError:
                pass
            else:
                if output_alias in seen_output_names:
                    raise ValueError(
                        f"HumemCypher v0 WITH queries do not allow duplicate RETURN output alias {output_alias!r}."
                    )
                seen_output_names.add(output_alias)
                items.append(
                    WithReturnItem(
                        kind="sqrt",
                        alias=output_alias,
                        output_alias=output_alias,
                        value=scalar_value,
                    )
                )
                continue
            field_match = re.fullmatch(
                r"(?P<alias>[A-Za-z_][A-Za-z0-9_]*)\.(?P<field>[A-Za-z_][A-Za-z0-9_]*)",
                sqrt_expr,
            )
            if field_match is not None:
                alias = field_match.group("alias")
                binding = binding_map.get(alias)
                if binding is not None and binding.binding_kind == "entity":
                    if output_alias in seen_output_names:
                        raise ValueError(
                            f"HumemCypher v0 WITH queries do not allow duplicate RETURN output alias {output_alias!r}."
                        )
                    seen_output_names.add(output_alias)
                    items.append(
                        WithReturnItem(
                            kind="sqrt",
                            alias=alias,
                            field=field_match.group("field"),
                            output_alias=output_alias,
                        )
                    )
                    continue
            raise ValueError(
                "HumemCypher v0 WITH queries currently support sqrt(...) only over admitted entity-field projections, scalar bindings, or scalar literal/parameter inputs."
            )

        exp_match = re.fullmatch(
            r"exp\s*\(\s*(?P<expr>.+?)\s*\)",
            expression_text,
            flags=re.IGNORECASE,
        )
        if exp_match is not None:
            if output_alias is None:
                raise ValueError(
                    "HumemCypher v0 WITH exp(...) projections currently require an explicit AS alias."
                )
            exp_expr = exp_match.group("expr").strip()
            scalar_binding = binding_map.get(exp_expr)
            if scalar_binding is not None and scalar_binding.binding_kind == "scalar":
                if output_alias in seen_output_names:
                    raise ValueError(
                        f"HumemCypher v0 WITH queries do not allow duplicate RETURN output alias {output_alias!r}."
                    )
                seen_output_names.add(output_alias)
                items.append(
                    WithReturnItem(
                        kind="exp",
                        alias=exp_expr,
                        output_alias=output_alias,
                    )
                )
                continue
            try:
                scalar_value = _parse_literal(exp_expr)
            except ValueError:
                pass
            else:
                if output_alias in seen_output_names:
                    raise ValueError(
                        f"HumemCypher v0 WITH queries do not allow duplicate RETURN output alias {output_alias!r}."
                    )
                seen_output_names.add(output_alias)
                items.append(
                    WithReturnItem(
                        kind="exp",
                        alias=output_alias,
                        output_alias=output_alias,
                        value=scalar_value,
                    )
                )
                continue
            field_match = re.fullmatch(
                r"(?P<alias>[A-Za-z_][A-Za-z0-9_]*)\.(?P<field>[A-Za-z_][A-Za-z0-9_]*)",
                exp_expr,
            )
            if field_match is not None:
                alias = field_match.group("alias")
                binding = binding_map.get(alias)
                if binding is not None and binding.binding_kind == "entity":
                    if output_alias in seen_output_names:
                        raise ValueError(
                            f"HumemCypher v0 WITH queries do not allow duplicate RETURN output alias {output_alias!r}."
                        )
                    seen_output_names.add(output_alias)
                    items.append(
                        WithReturnItem(
                            kind="exp",
                            alias=alias,
                            field=field_match.group("field"),
                            output_alias=output_alias,
                        )
                    )
                    continue
            raise ValueError(
                "HumemCypher v0 WITH queries currently support exp(...) only over admitted entity-field projections, scalar bindings, or scalar literal/parameter inputs."
            )

        sin_match = re.fullmatch(
            r"sin\s*\(\s*(?P<expr>.+?)\s*\)",
            expression_text,
            flags=re.IGNORECASE,
        )
        if sin_match is not None:
            if output_alias is None:
                raise ValueError(
                    "HumemCypher v0 WITH sin(...) projections currently require an explicit AS alias."
                )
            sin_expr = sin_match.group("expr").strip()
            scalar_binding = binding_map.get(sin_expr)
            if scalar_binding is not None and scalar_binding.binding_kind == "scalar":
                if output_alias in seen_output_names:
                    raise ValueError(
                        f"HumemCypher v0 WITH queries do not allow duplicate RETURN output alias {output_alias!r}."
                    )
                seen_output_names.add(output_alias)
                items.append(
                    WithReturnItem(
                        kind="sin",
                        alias=sin_expr,
                        output_alias=output_alias,
                    )
                )
                continue
            try:
                scalar_value = _parse_literal(sin_expr)
            except ValueError:
                pass
            else:
                if output_alias in seen_output_names:
                    raise ValueError(
                        f"HumemCypher v0 WITH queries do not allow duplicate RETURN output alias {output_alias!r}."
                    )
                seen_output_names.add(output_alias)
                items.append(
                    WithReturnItem(
                        kind="sin",
                        alias=output_alias,
                        output_alias=output_alias,
                        value=scalar_value,
                    )
                )
                continue
            field_match = re.fullmatch(
                r"(?P<alias>[A-Za-z_][A-Za-z0-9_]*)\.(?P<field>[A-Za-z_][A-Za-z0-9_]*)",
                sin_expr,
            )
            if field_match is not None:
                alias = field_match.group("alias")
                binding = binding_map.get(alias)
                if binding is not None and binding.binding_kind == "entity":
                    if output_alias in seen_output_names:
                        raise ValueError(
                            f"HumemCypher v0 WITH queries do not allow duplicate RETURN output alias {output_alias!r}."
                        )
                    seen_output_names.add(output_alias)
                    items.append(
                        WithReturnItem(
                            kind="sin",
                            alias=alias,
                            field=field_match.group("field"),
                            output_alias=output_alias,
                        )
                    )
                    continue
            raise ValueError(
                "HumemCypher v0 WITH queries currently support sin(...) only over admitted entity-field projections, scalar bindings, or scalar literal/parameter inputs."
            )

        cos_match = re.fullmatch(
            r"cos\s*\(\s*(?P<expr>.+?)\s*\)",
            expression_text,
            flags=re.IGNORECASE,
        )
        if cos_match is not None:
            if output_alias is None:
                raise ValueError(
                    "HumemCypher v0 WITH cos(...) projections currently require an explicit AS alias."
                )
            cos_expr = cos_match.group("expr").strip()
            scalar_binding = binding_map.get(cos_expr)
            if scalar_binding is not None and scalar_binding.binding_kind == "scalar":
                if output_alias in seen_output_names:
                    raise ValueError(
                        f"HumemCypher v0 WITH queries do not allow duplicate RETURN output alias {output_alias!r}."
                    )
                seen_output_names.add(output_alias)
                items.append(
                    WithReturnItem(
                        kind="cos",
                        alias=cos_expr,
                        output_alias=output_alias,
                    )
                )
                continue
            try:
                scalar_value = _parse_literal(cos_expr)
            except ValueError:
                pass
            else:
                if output_alias in seen_output_names:
                    raise ValueError(
                        f"HumemCypher v0 WITH queries do not allow duplicate RETURN output alias {output_alias!r}."
                    )
                seen_output_names.add(output_alias)
                items.append(
                    WithReturnItem(
                        kind="cos",
                        alias=output_alias,
                        output_alias=output_alias,
                        value=scalar_value,
                    )
                )
                continue
            field_match = re.fullmatch(
                r"(?P<alias>[A-Za-z_][A-Za-z0-9_]*)\.(?P<field>[A-Za-z_][A-Za-z0-9_]*)",
                cos_expr,
            )
            if field_match is not None:
                alias = field_match.group("alias")
                binding = binding_map.get(alias)
                if binding is not None and binding.binding_kind == "entity":
                    if output_alias in seen_output_names:
                        raise ValueError(
                            f"HumemCypher v0 WITH queries do not allow duplicate RETURN output alias {output_alias!r}."
                        )
                    seen_output_names.add(output_alias)
                    items.append(
                        WithReturnItem(
                            kind="cos",
                            alias=alias,
                            field=field_match.group("field"),
                            output_alias=output_alias,
                        )
                    )
                    continue
            raise ValueError(
                "HumemCypher v0 WITH queries currently support cos(...) only over admitted entity-field projections, scalar bindings, or scalar literal/parameter inputs."
            )

        tan_match = re.fullmatch(
            r"tan\s*\(\s*(?P<expr>.+?)\s*\)",
            expression_text,
            flags=re.IGNORECASE,
        )
        if tan_match is not None:
            if output_alias is None:
                raise ValueError(
                    "HumemCypher v0 WITH tan(...) projections currently require an explicit AS alias."
                )
            tan_expr = tan_match.group("expr").strip()
            scalar_binding = binding_map.get(tan_expr)
            if scalar_binding is not None and scalar_binding.binding_kind == "scalar":
                if output_alias in seen_output_names:
                    raise ValueError(
                        f"HumemCypher v0 WITH queries do not allow duplicate RETURN output alias {output_alias!r}."
                    )
                seen_output_names.add(output_alias)
                items.append(
                    WithReturnItem(
                        kind="tan",
                        alias=tan_expr,
                        output_alias=output_alias,
                    )
                )
                continue
            try:
                scalar_value = _parse_literal(tan_expr)
            except ValueError:
                pass
            else:
                if output_alias in seen_output_names:
                    raise ValueError(
                        f"HumemCypher v0 WITH queries do not allow duplicate RETURN output alias {output_alias!r}."
                    )
                seen_output_names.add(output_alias)
                items.append(
                    WithReturnItem(
                        kind="tan",
                        alias=output_alias,
                        output_alias=output_alias,
                        value=scalar_value,
                    )
                )
                continue
            field_match = re.fullmatch(
                r"(?P<alias>[A-Za-z_][A-Za-z0-9_]*)\.(?P<field>[A-Za-z_][A-Za-z0-9_]*)",
                tan_expr,
            )
            if field_match is not None:
                alias = field_match.group("alias")
                binding = binding_map.get(alias)
                if binding is not None and binding.binding_kind == "entity":
                    if output_alias in seen_output_names:
                        raise ValueError(
                            f"HumemCypher v0 WITH queries do not allow duplicate RETURN output alias {output_alias!r}."
                        )
                    seen_output_names.add(output_alias)
                    items.append(
                        WithReturnItem(
                            kind="tan",
                            alias=alias,
                            field=field_match.group("field"),
                            output_alias=output_alias,
                        )
                    )
                    continue
            raise ValueError(
                "HumemCypher v0 WITH queries currently support tan(...) only over admitted entity-field projections, scalar bindings, or scalar literal/parameter inputs."
            )

        asin_match = re.fullmatch(
            r"asin\s*\(\s*(?P<expr>.+?)\s*\)",
            expression_text,
            flags=re.IGNORECASE,
        )
        if asin_match is not None:
            if output_alias is None:
                raise ValueError(
                    "HumemCypher v0 WITH asin(...) projections currently require an explicit AS alias."
                )
            asin_expr = asin_match.group("expr").strip()
            scalar_binding = binding_map.get(asin_expr)
            if scalar_binding is not None and scalar_binding.binding_kind == "scalar":
                if output_alias in seen_output_names:
                    raise ValueError(
                        f"HumemCypher v0 WITH queries do not allow duplicate RETURN output alias {output_alias!r}."
                    )
                seen_output_names.add(output_alias)
                items.append(
                    WithReturnItem(
                        kind="asin",
                        alias=asin_expr,
                        output_alias=output_alias,
                    )
                )
                continue
            try:
                scalar_value = _parse_literal(asin_expr)
            except ValueError:
                pass
            else:
                if output_alias in seen_output_names:
                    raise ValueError(
                        f"HumemCypher v0 WITH queries do not allow duplicate RETURN output alias {output_alias!r}."
                    )
                seen_output_names.add(output_alias)
                items.append(
                    WithReturnItem(
                        kind="asin",
                        alias=output_alias,
                        output_alias=output_alias,
                        value=scalar_value,
                    )
                )
                continue
            field_match = re.fullmatch(
                r"(?P<alias>[A-Za-z_][A-Za-z0-9_]*)\.(?P<field>[A-Za-z_][A-Za-z0-9_]*)",
                asin_expr,
            )
            if field_match is not None:
                alias = field_match.group("alias")
                binding = binding_map.get(alias)
                if binding is not None and binding.binding_kind == "entity":
                    if output_alias in seen_output_names:
                        raise ValueError(
                            f"HumemCypher v0 WITH queries do not allow duplicate RETURN output alias {output_alias!r}."
                        )
                    seen_output_names.add(output_alias)
                    items.append(
                        WithReturnItem(
                            kind="asin",
                            alias=alias,
                            field=field_match.group("field"),
                            output_alias=output_alias,
                        )
                    )
                    continue
            raise ValueError(
                "HumemCypher v0 WITH queries currently support asin(...) only over admitted entity-field projections, scalar bindings, or scalar literal/parameter inputs."
            )

        acos_match = re.fullmatch(
            r"acos\s*\(\s*(?P<expr>.+?)\s*\)",
            expression_text,
            flags=re.IGNORECASE,
        )
        if acos_match is not None:
            if output_alias is None:
                raise ValueError(
                    "HumemCypher v0 WITH acos(...) projections currently require an explicit AS alias."
                )
            acos_expr = acos_match.group("expr").strip()
            scalar_binding = binding_map.get(acos_expr)
            if scalar_binding is not None and scalar_binding.binding_kind == "scalar":
                if output_alias in seen_output_names:
                    raise ValueError(
                        f"HumemCypher v0 WITH queries do not allow duplicate RETURN output alias {output_alias!r}."
                    )
                seen_output_names.add(output_alias)
                items.append(
                    WithReturnItem(
                        kind="acos",
                        alias=acos_expr,
                        output_alias=output_alias,
                    )
                )
                continue
            try:
                scalar_value = _parse_literal(acos_expr)
            except ValueError:
                pass
            else:
                if output_alias in seen_output_names:
                    raise ValueError(
                        f"HumemCypher v0 WITH queries do not allow duplicate RETURN output alias {output_alias!r}."
                    )
                seen_output_names.add(output_alias)
                items.append(
                    WithReturnItem(
                        kind="acos",
                        alias=output_alias,
                        output_alias=output_alias,
                        value=scalar_value,
                    )
                )
                continue
            field_match = re.fullmatch(
                r"(?P<alias>[A-Za-z_][A-Za-z0-9_]*)\.(?P<field>[A-Za-z_][A-Za-z0-9_]*)",
                acos_expr,
            )
            if field_match is not None:
                alias = field_match.group("alias")
                binding = binding_map.get(alias)
                if binding is not None and binding.binding_kind == "entity":
                    if output_alias in seen_output_names:
                        raise ValueError(
                            f"HumemCypher v0 WITH queries do not allow duplicate RETURN output alias {output_alias!r}."
                        )
                    seen_output_names.add(output_alias)
                    items.append(
                        WithReturnItem(
                            kind="acos",
                            alias=alias,
                            field=field_match.group("field"),
                            output_alias=output_alias,
                        )
                    )
                    continue
            raise ValueError(
                "HumemCypher v0 WITH queries currently support acos(...) only over admitted entity-field projections, scalar bindings, or scalar literal/parameter inputs."
            )

        atan_match = re.fullmatch(
            r"atan\s*\(\s*(?P<expr>.+?)\s*\)",
            expression_text,
            flags=re.IGNORECASE,
        )
        if atan_match is not None:
            if output_alias is None:
                raise ValueError(
                    "HumemCypher v0 WITH atan(...) projections currently require an explicit AS alias."
                )
            atan_expr = atan_match.group("expr").strip()
            scalar_binding = binding_map.get(atan_expr)
            if scalar_binding is not None and scalar_binding.binding_kind == "scalar":
                if output_alias in seen_output_names:
                    raise ValueError(
                        f"HumemCypher v0 WITH queries do not allow duplicate RETURN output alias {output_alias!r}."
                    )
                seen_output_names.add(output_alias)
                items.append(
                    WithReturnItem(
                        kind="atan",
                        alias=atan_expr,
                        output_alias=output_alias,
                    )
                )
                continue
            try:
                scalar_value = _parse_literal(atan_expr)
            except ValueError:
                pass
            else:
                if output_alias in seen_output_names:
                    raise ValueError(
                        f"HumemCypher v0 WITH queries do not allow duplicate RETURN output alias {output_alias!r}."
                    )
                seen_output_names.add(output_alias)
                items.append(
                    WithReturnItem(
                        kind="atan",
                        alias=output_alias,
                        output_alias=output_alias,
                        value=scalar_value,
                    )
                )
                continue
            field_match = re.fullmatch(
                r"(?P<alias>[A-Za-z_][A-Za-z0-9_]*)\.(?P<field>[A-Za-z_][A-Za-z0-9_]*)",
                atan_expr,
            )
            if field_match is not None:
                alias = field_match.group("alias")
                binding = binding_map.get(alias)
                if binding is not None and binding.binding_kind == "entity":
                    if output_alias in seen_output_names:
                        raise ValueError(
                            f"HumemCypher v0 WITH queries do not allow duplicate RETURN output alias {output_alias!r}."
                        )
                    seen_output_names.add(output_alias)
                    items.append(
                        WithReturnItem(
                            kind="atan",
                            alias=alias,
                            field=field_match.group("field"),
                            output_alias=output_alias,
                        )
                    )
                    continue
            raise ValueError(
                "HumemCypher v0 WITH queries currently support atan(...) only over admitted entity-field projections, scalar bindings, or scalar literal/parameter inputs."
            )

        ln_match = re.fullmatch(
            r"ln\s*\(\s*(?P<expr>.+?)\s*\)",
            expression_text,
            flags=re.IGNORECASE,
        )
        if ln_match is not None:
            if output_alias is None:
                raise ValueError(
                    "HumemCypher v0 WITH ln(...) projections currently require an explicit AS alias."
                )
            ln_expr = ln_match.group("expr").strip()
            scalar_binding = binding_map.get(ln_expr)
            if scalar_binding is not None and scalar_binding.binding_kind == "scalar":
                if output_alias in seen_output_names:
                    raise ValueError(
                        f"HumemCypher v0 WITH queries do not allow duplicate RETURN output alias {output_alias!r}."
                    )
                seen_output_names.add(output_alias)
                items.append(
                    WithReturnItem(
                        kind="ln",
                        alias=ln_expr,
                        output_alias=output_alias,
                    )
                )
                continue
            try:
                scalar_value = _parse_literal(ln_expr)
            except ValueError:
                pass
            else:
                if output_alias in seen_output_names:
                    raise ValueError(
                        f"HumemCypher v0 WITH queries do not allow duplicate RETURN output alias {output_alias!r}."
                    )
                seen_output_names.add(output_alias)
                items.append(
                    WithReturnItem(
                        kind="ln",
                        alias=output_alias,
                        output_alias=output_alias,
                        value=scalar_value,
                    )
                )
                continue
            field_match = re.fullmatch(
                r"(?P<alias>[A-Za-z_][A-Za-z0-9_]*)\.(?P<field>[A-Za-z_][A-Za-z0-9_]*)",
                ln_expr,
            )
            if field_match is not None:
                alias = field_match.group("alias")
                binding = binding_map.get(alias)
                if binding is not None and binding.binding_kind == "entity":
                    if output_alias in seen_output_names:
                        raise ValueError(
                            f"HumemCypher v0 WITH queries do not allow duplicate RETURN output alias {output_alias!r}."
                        )
                    seen_output_names.add(output_alias)
                    items.append(
                        WithReturnItem(
                            kind="ln",
                            alias=alias,
                            field=field_match.group("field"),
                            output_alias=output_alias,
                        )
                    )
                    continue
            raise ValueError(
                "HumemCypher v0 WITH queries currently support ln(...) only over admitted entity-field projections, scalar bindings, or scalar literal/parameter inputs."
            )

        log_match = re.fullmatch(
            r"log\s*\(\s*(?P<expr>.+?)\s*\)",
            expression_text,
            flags=re.IGNORECASE,
        )
        if log_match is not None:
            if output_alias is None:
                raise ValueError(
                    "HumemCypher v0 WITH log(...) projections currently require an explicit AS alias."
                )
            log_expr = log_match.group("expr").strip()
            scalar_binding = binding_map.get(log_expr)
            if scalar_binding is not None and scalar_binding.binding_kind == "scalar":
                if output_alias in seen_output_names:
                    raise ValueError(
                        f"HumemCypher v0 WITH queries do not allow duplicate RETURN output alias {output_alias!r}."
                    )
                seen_output_names.add(output_alias)
                items.append(
                    WithReturnItem(
                        kind="log",
                        alias=log_expr,
                        output_alias=output_alias,
                    )
                )
                continue
            try:
                scalar_value = _parse_literal(log_expr)
            except ValueError:
                pass
            else:
                if output_alias in seen_output_names:
                    raise ValueError(
                        f"HumemCypher v0 WITH queries do not allow duplicate RETURN output alias {output_alias!r}."
                    )
                seen_output_names.add(output_alias)
                items.append(
                    WithReturnItem(
                        kind="log",
                        alias=output_alias,
                        output_alias=output_alias,
                        value=scalar_value,
                    )
                )
                continue
            field_match = re.fullmatch(
                r"(?P<alias>[A-Za-z_][A-Za-z0-9_]*)\.(?P<field>[A-Za-z_][A-Za-z0-9_]*)",
                log_expr,
            )
            if field_match is not None:
                alias = field_match.group("alias")
                binding = binding_map.get(alias)
                if binding is not None and binding.binding_kind == "entity":
                    if output_alias in seen_output_names:
                        raise ValueError(
                            f"HumemCypher v0 WITH queries do not allow duplicate RETURN output alias {output_alias!r}."
                        )
                    seen_output_names.add(output_alias)
                    items.append(
                        WithReturnItem(
                            kind="log",
                            alias=alias,
                            field=field_match.group("field"),
                            output_alias=output_alias,
                        )
                    )
                    continue
            raise ValueError(
                "HumemCypher v0 WITH queries currently support log(...) only over admitted entity-field projections, scalar bindings, or scalar literal/parameter inputs."
            )

        radians_match = re.fullmatch(
            r"radians\s*\(\s*(?P<expr>.+?)\s*\)",
            expression_text,
            flags=re.IGNORECASE,
        )
        if radians_match is not None:
            if output_alias is None:
                raise ValueError(
                    "HumemCypher v0 WITH radians(...) projections currently require an explicit AS alias."
                )
            radians_expr = radians_match.group("expr").strip()
            scalar_binding = binding_map.get(radians_expr)
            if scalar_binding is not None and scalar_binding.binding_kind == "scalar":
                if output_alias in seen_output_names:
                    raise ValueError(
                        f"HumemCypher v0 WITH queries do not allow duplicate RETURN output alias {output_alias!r}."
                    )
                seen_output_names.add(output_alias)
                items.append(
                    WithReturnItem(
                        kind="radians",
                        alias=radians_expr,
                        output_alias=output_alias,
                    )
                )
                continue
            try:
                scalar_value = _parse_literal(radians_expr)
            except ValueError:
                pass
            else:
                if output_alias in seen_output_names:
                    raise ValueError(
                        f"HumemCypher v0 WITH queries do not allow duplicate RETURN output alias {output_alias!r}."
                    )
                seen_output_names.add(output_alias)
                items.append(
                    WithReturnItem(
                        kind="radians",
                        alias=output_alias,
                        output_alias=output_alias,
                        value=scalar_value,
                    )
                )
                continue
            field_match = re.fullmatch(
                r"(?P<alias>[A-Za-z_][A-Za-z0-9_]*)\.(?P<field>[A-Za-z_][A-Za-z0-9_]*)",
                radians_expr,
            )
            if field_match is not None:
                alias = field_match.group("alias")
                binding = binding_map.get(alias)
                if binding is not None and binding.binding_kind == "entity":
                    if output_alias in seen_output_names:
                        raise ValueError(
                            f"HumemCypher v0 WITH queries do not allow duplicate RETURN output alias {output_alias!r}."
                        )
                    seen_output_names.add(output_alias)
                    items.append(
                        WithReturnItem(
                            kind="radians",
                            alias=alias,
                            field=field_match.group("field"),
                            output_alias=output_alias,
                        )
                    )
                    continue
            raise ValueError(
                "HumemCypher v0 WITH queries currently support radians(...) only over admitted entity-field projections, scalar bindings, or scalar literal/parameter inputs."
            )

        degrees_match = re.fullmatch(
            r"degrees\s*\(\s*(?P<expr>.+?)\s*\)",
            expression_text,
            flags=re.IGNORECASE,
        )
        if degrees_match is not None:
            if output_alias is None:
                raise ValueError(
                    "HumemCypher v0 WITH degrees(...) projections currently require an explicit AS alias."
                )
            degrees_expr = degrees_match.group("expr").strip()
            scalar_binding = binding_map.get(degrees_expr)
            if scalar_binding is not None and scalar_binding.binding_kind == "scalar":
                if output_alias in seen_output_names:
                    raise ValueError(
                        f"HumemCypher v0 WITH queries do not allow duplicate RETURN output alias {output_alias!r}."
                    )
                seen_output_names.add(output_alias)
                items.append(
                    WithReturnItem(
                        kind="degrees",
                        alias=degrees_expr,
                        output_alias=output_alias,
                    )
                )
                continue
            try:
                scalar_value = _parse_literal(degrees_expr)
            except ValueError:
                pass
            else:
                if output_alias in seen_output_names:
                    raise ValueError(
                        f"HumemCypher v0 WITH queries do not allow duplicate RETURN output alias {output_alias!r}."
                    )
                seen_output_names.add(output_alias)
                items.append(
                    WithReturnItem(
                        kind="degrees",
                        alias=output_alias,
                        output_alias=output_alias,
                        value=scalar_value,
                    )
                )
                continue
            field_match = re.fullmatch(
                r"(?P<alias>[A-Za-z_][A-Za-z0-9_]*)\.(?P<field>[A-Za-z_][A-Za-z0-9_]*)",
                degrees_expr,
            )
            if field_match is not None:
                alias = field_match.group("alias")
                binding = binding_map.get(alias)
                if binding is not None and binding.binding_kind == "entity":
                    if output_alias in seen_output_names:
                        raise ValueError(
                            f"HumemCypher v0 WITH queries do not allow duplicate RETURN output alias {output_alias!r}."
                        )
                    seen_output_names.add(output_alias)
                    items.append(
                        WithReturnItem(
                            kind="degrees",
                            alias=alias,
                            field=field_match.group("field"),
                            output_alias=output_alias,
                        )
                    )
                    continue
            raise ValueError(
                "HumemCypher v0 WITH queries currently support degrees(...) only over admitted entity-field projections, scalar bindings, or scalar literal/parameter inputs."
            )

        log10_match = re.fullmatch(
            r"log10\s*\(\s*(?P<expr>.+?)\s*\)",
            expression_text,
            flags=re.IGNORECASE,
        )
        if log10_match is not None:
            if output_alias is None:
                raise ValueError(
                    "HumemCypher v0 WITH log10(...) projections currently require an explicit AS alias."
                )
            log10_expr = log10_match.group("expr").strip()
            scalar_binding = binding_map.get(log10_expr)
            if scalar_binding is not None and scalar_binding.binding_kind == "scalar":
                if output_alias in seen_output_names:
                    raise ValueError(
                        f"HumemCypher v0 WITH queries do not allow duplicate RETURN output alias {output_alias!r}."
                    )
                seen_output_names.add(output_alias)
                items.append(
                    WithReturnItem(
                        kind="log10",
                        alias=log10_expr,
                        output_alias=output_alias,
                    )
                )
                continue
            try:
                scalar_value = _parse_literal(log10_expr)
            except ValueError:
                pass
            else:
                if output_alias in seen_output_names:
                    raise ValueError(
                        f"HumemCypher v0 WITH queries do not allow duplicate RETURN output alias {output_alias!r}."
                    )
                seen_output_names.add(output_alias)
                items.append(
                    WithReturnItem(
                        kind="log10",
                        alias=output_alias,
                        output_alias=output_alias,
                        value=scalar_value,
                    )
                )
                continue
            field_match = re.fullmatch(
                r"(?P<alias>[A-Za-z_][A-Za-z0-9_]*)\.(?P<field>[A-Za-z_][A-Za-z0-9_]*)",
                log10_expr,
            )
            if field_match is not None:
                alias = field_match.group("alias")
                binding = binding_map.get(alias)
                if binding is not None and binding.binding_kind == "entity":
                    if output_alias in seen_output_names:
                        raise ValueError(
                            f"HumemCypher v0 WITH queries do not allow duplicate RETURN output alias {output_alias!r}."
                        )
                    seen_output_names.add(output_alias)
                    items.append(
                        WithReturnItem(
                            kind="log10",
                            alias=alias,
                            field=field_match.group("field"),
                            output_alias=output_alias,
                        )
                    )
                    continue
            raise ValueError(
                "HumemCypher v0 WITH queries currently support log10(...) only over admitted entity-field projections, scalar bindings, or scalar literal/parameter inputs."
            )

        to_string_match = re.fullmatch(
            r"tostring\s*\(\s*(?P<expr>.+?)\s*\)",
            expression_text,
            flags=re.IGNORECASE,
        )
        if to_string_match is not None:
            if output_alias is None:
                raise ValueError(
                    "HumemCypher v0 WITH toString(...) projections currently require an explicit AS alias."
                )
            to_string_expr = to_string_match.group("expr").strip()
            scalar_binding = binding_map.get(to_string_expr)
            if scalar_binding is not None and scalar_binding.binding_kind == "scalar":
                if output_alias in seen_output_names:
                    raise ValueError(
                        f"HumemCypher v0 WITH queries do not allow duplicate RETURN output alias {output_alias!r}."
                    )
                seen_output_names.add(output_alias)
                items.append(
                    WithReturnItem(
                        kind="to_string",
                        alias=to_string_expr,
                        output_alias=output_alias,
                    )
                )
                continue
            try:
                scalar_value = _parse_literal(to_string_expr)
            except ValueError:
                pass
            else:
                if output_alias in seen_output_names:
                    raise ValueError(
                        f"HumemCypher v0 WITH queries do not allow duplicate RETURN output alias {output_alias!r}."
                    )
                seen_output_names.add(output_alias)
                items.append(
                    WithReturnItem(
                        kind="to_string",
                        alias=output_alias,
                        output_alias=output_alias,
                        value=scalar_value,
                    )
                )
                continue
            field_match = re.fullmatch(
                r"(?P<alias>[A-Za-z_][A-Za-z0-9_]*)\.(?P<field>[A-Za-z_][A-Za-z0-9_]*)",
                to_string_expr,
            )
            if field_match is not None:
                alias = field_match.group("alias")
                binding = binding_map.get(alias)
                if binding is not None and binding.binding_kind == "entity":
                    if output_alias in seen_output_names:
                        raise ValueError(
                            f"HumemCypher v0 WITH queries do not allow duplicate RETURN output alias {output_alias!r}."
                        )
                    seen_output_names.add(output_alias)
                    items.append(
                        WithReturnItem(
                            kind="to_string",
                            alias=alias,
                            field=field_match.group("field"),
                            output_alias=output_alias,
                        )
                    )
                    continue
            raise ValueError(
                "HumemCypher v0 WITH queries currently support toString(...) only over admitted entity-field projections, scalar bindings, or scalar literal/parameter inputs."
            )

        to_integer_match = re.fullmatch(
            r"tointeger\s*\(\s*(?P<expr>.+?)\s*\)",
            expression_text,
            flags=re.IGNORECASE,
        )
        if to_integer_match is not None:
            if output_alias is None:
                raise ValueError(
                    "HumemCypher v0 WITH toInteger(...) projections currently require an explicit AS alias."
                )
            to_integer_expr = to_integer_match.group("expr").strip()
            scalar_binding = binding_map.get(to_integer_expr)
            if scalar_binding is not None and scalar_binding.binding_kind == "scalar":
                if output_alias in seen_output_names:
                    raise ValueError(
                        f"HumemCypher v0 WITH queries do not allow duplicate RETURN output alias {output_alias!r}."
                    )
                seen_output_names.add(output_alias)
                items.append(
                    WithReturnItem(
                        kind="to_integer",
                        alias=to_integer_expr,
                        output_alias=output_alias,
                    )
                )
                continue
            try:
                scalar_value = _parse_literal(to_integer_expr)
            except ValueError:
                pass
            else:
                if output_alias in seen_output_names:
                    raise ValueError(
                        f"HumemCypher v0 WITH queries do not allow duplicate RETURN output alias {output_alias!r}."
                    )
                seen_output_names.add(output_alias)
                items.append(
                    WithReturnItem(
                        kind="to_integer",
                        alias=output_alias,
                        output_alias=output_alias,
                        value=scalar_value,
                    )
                )
                continue
            field_match = re.fullmatch(
                r"(?P<alias>[A-Za-z_][A-Za-z0-9_]*)\.(?P<field>[A-Za-z_][A-Za-z0-9_]*)",
                to_integer_expr,
            )
            if field_match is not None:
                alias = field_match.group("alias")
                binding = binding_map.get(alias)
                if binding is not None and binding.binding_kind == "entity":
                    if output_alias in seen_output_names:
                        raise ValueError(
                            f"HumemCypher v0 WITH queries do not allow duplicate RETURN output alias {output_alias!r}."
                        )
                    seen_output_names.add(output_alias)
                    items.append(
                        WithReturnItem(
                            kind="to_integer",
                            alias=alias,
                            field=field_match.group("field"),
                            output_alias=output_alias,
                        )
                    )
                    continue
            raise ValueError(
                "HumemCypher v0 WITH queries currently support toInteger(...) only over admitted entity-field projections, scalar bindings, or scalar literal/parameter inputs."
            )

        to_float_match = re.fullmatch(
            r"tofloat\s*\(\s*(?P<expr>.+?)\s*\)",
            expression_text,
            flags=re.IGNORECASE,
        )
        if to_float_match is not None:
            if output_alias is None:
                raise ValueError(
                    "HumemCypher v0 WITH toFloat(...) projections currently require an explicit AS alias."
                )
            to_float_expr = to_float_match.group("expr").strip()
            scalar_binding = binding_map.get(to_float_expr)
            if scalar_binding is not None and scalar_binding.binding_kind == "scalar":
                if output_alias in seen_output_names:
                    raise ValueError(
                        f"HumemCypher v0 WITH queries do not allow duplicate RETURN output alias {output_alias!r}."
                    )
                seen_output_names.add(output_alias)
                items.append(
                    WithReturnItem(
                        kind="to_float",
                        alias=to_float_expr,
                        output_alias=output_alias,
                    )
                )
                continue
            try:
                scalar_value = _parse_literal(to_float_expr)
            except ValueError:
                pass
            else:
                if output_alias in seen_output_names:
                    raise ValueError(
                        f"HumemCypher v0 WITH queries do not allow duplicate RETURN output alias {output_alias!r}."
                    )
                seen_output_names.add(output_alias)
                items.append(
                    WithReturnItem(
                        kind="to_float",
                        alias=output_alias,
                        output_alias=output_alias,
                        value=scalar_value,
                    )
                )
                continue
            field_match = re.fullmatch(
                r"(?P<alias>[A-Za-z_][A-Za-z0-9_]*)\.(?P<field>[A-Za-z_][A-Za-z0-9_]*)",
                to_float_expr,
            )
            if field_match is not None:
                alias = field_match.group("alias")
                binding = binding_map.get(alias)
                if binding is not None and binding.binding_kind == "entity":
                    if output_alias in seen_output_names:
                        raise ValueError(
                            f"HumemCypher v0 WITH queries do not allow duplicate RETURN output alias {output_alias!r}."
                        )
                    seen_output_names.add(output_alias)
                    items.append(
                        WithReturnItem(
                            kind="to_float",
                            alias=alias,
                            field=field_match.group("field"),
                            output_alias=output_alias,
                        )
                    )
                    continue
            raise ValueError(
                "HumemCypher v0 WITH queries currently support toFloat(...) only over admitted entity-field projections, scalar bindings, or scalar literal/parameter inputs."
            )

        to_boolean_match = re.fullmatch(
            r"toboolean\s*\(\s*(?P<expr>.+?)\s*\)",
            expression_text,
            flags=re.IGNORECASE,
        )
        if to_boolean_match is not None:
            if output_alias is None:
                raise ValueError(
                    "HumemCypher v0 WITH toBoolean(...) projections currently require an explicit AS alias."
                )
            to_boolean_expr = to_boolean_match.group("expr").strip()
            scalar_binding = binding_map.get(to_boolean_expr)
            if scalar_binding is not None and scalar_binding.binding_kind == "scalar":
                if output_alias in seen_output_names:
                    raise ValueError(
                        f"HumemCypher v0 WITH queries do not allow duplicate RETURN output alias {output_alias!r}."
                    )
                seen_output_names.add(output_alias)
                items.append(
                    WithReturnItem(
                        kind="to_boolean",
                        alias=to_boolean_expr,
                        output_alias=output_alias,
                    )
                )
                continue
            try:
                scalar_value = _parse_literal(to_boolean_expr)
            except ValueError:
                pass
            else:
                if output_alias in seen_output_names:
                    raise ValueError(
                        f"HumemCypher v0 WITH queries do not allow duplicate RETURN output alias {output_alias!r}."
                    )
                seen_output_names.add(output_alias)
                items.append(
                    WithReturnItem(
                        kind="to_boolean",
                        alias=output_alias,
                        output_alias=output_alias,
                        value=scalar_value,
                    )
                )
                continue
            field_match = re.fullmatch(
                r"(?P<alias>[A-Za-z_][A-Za-z0-9_]*)\.(?P<field>[A-Za-z_][A-Za-z0-9_]*)",
                to_boolean_expr,
            )
            if field_match is not None:
                alias = field_match.group("alias")
                binding = binding_map.get(alias)
                if binding is not None and binding.binding_kind == "entity":
                    if output_alias in seen_output_names:
                        raise ValueError(
                            f"HumemCypher v0 WITH queries do not allow duplicate RETURN output alias {output_alias!r}."
                        )
                    seen_output_names.add(output_alias)
                    items.append(
                        WithReturnItem(
                            kind="to_boolean",
                            alias=alias,
                            field=field_match.group("field"),
                            output_alias=output_alias,
                        )
                    )
                    continue
            raise ValueError(
                "HumemCypher v0 WITH queries currently support toBoolean(...) only over admitted entity-field projections, scalar bindings, or scalar literal/parameter inputs."
            )

        try:
            scalar_value = _parse_literal(expression_text)
        except ValueError:
            pass
        else:
            if output_alias is None:
                raise ValueError(
                    "HumemCypher v0 WITH scalar literal and parameter projections currently require an explicit AS alias."
                )
            if output_alias in seen_output_names:
                raise ValueError(
                    f"HumemCypher v0 WITH queries do not allow duplicate RETURN output alias {output_alias!r}."
                )
            seen_output_names.add(output_alias)
            items.append(
                WithReturnItem(
                    kind="scalar_value",
                    alias=output_alias,
                    output_alias=output_alias,
                    value=scalar_value,
                )
            )
            continue

        try:
            left_text, operator, value_text = _split_predicate_comparison(expression_text)
        except ValueError:
            pass
        else:
            if output_alias is None:
                raise ValueError(
                    "HumemCypher v0 WITH predicate RETURN items currently require an explicit AS alias."
                )

            left_text = left_text.strip()
            parsed_value = None
            if operator in {"IS NULL", "IS NOT NULL"}:
                if value_text.strip():
                    raise ValueError(
                        "HumemCypher v0 WITH null predicate RETURN items cannot include a trailing literal value."
                    )
            else:
                parsed_value = _parse_literal(value_text.strip())

            binding = binding_map.get(left_text)
            if binding is not None and binding.binding_kind == "scalar":
                if output_alias in seen_output_names:
                    raise ValueError(
                        f"HumemCypher v0 WITH queries do not allow duplicate RETURN output alias {output_alias!r}."
                    )
                seen_output_names.add(output_alias)
                items.append(
                    WithReturnItem(
                        kind="predicate",
                        alias=left_text,
                        output_alias=output_alias,
                        operator=operator,
                        value=parsed_value,
                    )
                )
                continue

            size_match = re.fullmatch(
                r"size\s*\(\s*(?P<expr>.+?)\s*\)",
                left_text,
                flags=re.IGNORECASE,
            )
            if size_match is not None:
                size_expr = size_match.group("expr").strip()
                scalar_binding = binding_map.get(size_expr)
                if scalar_binding is not None and scalar_binding.binding_kind == "scalar":
                    if output_alias in seen_output_names:
                        raise ValueError(
                            f"HumemCypher v0 WITH queries do not allow duplicate RETURN output alias {output_alias!r}."
                        )
                    seen_output_names.add(output_alias)
                    items.append(
                        WithReturnItem(
                            kind="predicate",
                            alias=size_expr,
                            field=f"{_SIZE_PREDICATE_FIELD_PREFIX}__value__",
                            output_alias=output_alias,
                            operator=operator,
                            value=parsed_value,
                        )
                    )
                    continue
                id_match = re.fullmatch(
                    r"id\s*\(\s*(?P<alias>[A-Za-z_][A-Za-z0-9_]*)\s*\)",
                    size_expr,
                    flags=re.IGNORECASE,
                )
                if id_match is not None:
                    alias = id_match.group("alias")
                    binding = binding_map.get(alias)
                    if binding is not None and binding.binding_kind == "entity":
                        if output_alias in seen_output_names:
                            raise ValueError(
                                f"HumemCypher v0 WITH queries do not allow duplicate RETURN output alias {output_alias!r}."
                            )
                        seen_output_names.add(output_alias)
                        items.append(
                            WithReturnItem(
                                kind="predicate",
                                alias=alias,
                                field=f"{_SIZE_PREDICATE_FIELD_PREFIX}id",
                                output_alias=output_alias,
                                operator=operator,
                                value=parsed_value,
                            )
                        )
                        continue
                type_match = re.fullmatch(
                    r"type\s*\(\s*(?P<alias>[A-Za-z_][A-Za-z0-9_]*)\s*\)",
                    size_expr,
                    flags=re.IGNORECASE,
                )
                if type_match is not None:
                    alias = type_match.group("alias")
                    binding = binding_map.get(alias)
                    if (
                        binding is not None
                        and binding.binding_kind == "entity"
                        and binding.alias_kind == "relationship"
                    ):
                        if output_alias in seen_output_names:
                            raise ValueError(
                                f"HumemCypher v0 WITH queries do not allow duplicate RETURN output alias {output_alias!r}."
                            )
                        seen_output_names.add(output_alias)
                        items.append(
                            WithReturnItem(
                                kind="predicate",
                                alias=alias,
                                field=f"{_SIZE_PREDICATE_FIELD_PREFIX}type",
                                output_alias=output_alias,
                                operator=operator,
                                value=parsed_value,
                            )
                        )
                        continue
                field_match = re.fullmatch(
                    r"(?P<alias>[A-Za-z_][A-Za-z0-9_]*)\.(?P<field>[A-Za-z_][A-Za-z0-9_]*)",
                    size_expr,
                )
                if field_match is not None:
                    alias = field_match.group("alias")
                    binding = binding_map.get(alias)
                    if binding is not None and binding.binding_kind == "entity":
                        if output_alias in seen_output_names:
                            raise ValueError(
                                f"HumemCypher v0 WITH queries do not allow duplicate RETURN output alias {output_alias!r}."
                            )
                        seen_output_names.add(output_alias)
                        items.append(
                            WithReturnItem(
                                kind="predicate",
                                alias=alias,
                                field=f"{_SIZE_PREDICATE_FIELD_PREFIX}{field_match.group('field')}",
                                output_alias=output_alias,
                                operator=operator,
                                value=parsed_value,
                            )
                        )
                        continue

            id_match = re.fullmatch(
                r"id\s*\(\s*(?P<alias>[A-Za-z_][A-Za-z0-9_]*)\s*\)",
                left_text,
                flags=re.IGNORECASE,
            )
            if id_match is not None:
                alias = id_match.group("alias")
                binding = binding_map.get(alias)
                if binding is not None and binding.binding_kind == "entity":
                    if output_alias in seen_output_names:
                        raise ValueError(
                            f"HumemCypher v0 WITH queries do not allow duplicate RETURN output alias {output_alias!r}."
                        )
                    seen_output_names.add(output_alias)
                    items.append(
                        WithReturnItem(
                            kind="predicate",
                            alias=alias,
                            field="id",
                            output_alias=output_alias,
                            operator=operator,
                            value=parsed_value,
                        )
                    )
                    continue

            type_match = re.fullmatch(
                r"type\s*\(\s*(?P<alias>[A-Za-z_][A-Za-z0-9_]*)\s*\)",
                left_text,
                flags=re.IGNORECASE,
            )
            if type_match is not None:
                alias = type_match.group("alias")
                binding = binding_map.get(alias)
                if (
                    binding is not None
                    and binding.binding_kind == "entity"
                    and binding.alias_kind == "relationship"
                ):
                    if output_alias in seen_output_names:
                        raise ValueError(
                            f"HumemCypher v0 WITH queries do not allow duplicate RETURN output alias {output_alias!r}."
                        )
                    seen_output_names.add(output_alias)
                    items.append(
                        WithReturnItem(
                            kind="predicate",
                            alias=alias,
                            field="type",
                            output_alias=output_alias,
                            operator=operator,
                            value=parsed_value,
                        )
                    )
                    continue

            field_match = re.fullmatch(
                r"(?P<alias>[A-Za-z_][A-Za-z0-9_]*)\.(?P<field>[A-Za-z_][A-Za-z0-9_]*)",
                left_text,
            )
            if field_match is not None:
                alias = field_match.group("alias")
                binding = binding_map.get(alias)
                if binding is not None and binding.binding_kind == "entity":
                    if output_alias in seen_output_names:
                        raise ValueError(
                            f"HumemCypher v0 WITH queries do not allow duplicate RETURN output alias {output_alias!r}."
                        )
                    seen_output_names.add(output_alias)
                    items.append(
                        WithReturnItem(
                            kind="predicate",
                            alias=alias,
                            field=field_match.group("field"),
                            output_alias=output_alias,
                            operator=operator,
                            value=parsed_value,
                        )
                    )
                    continue

            raise ValueError(
                "HumemCypher v0 WITH queries currently support predicate RETURN items only as scalar_alias OP value, entity_alias.field OP value, id(entity_alias) OP value, type(rel_alias) OP value, or size(admitted_input) OP value."
            )

        field_match = re.fullmatch(
            r"(?P<alias>[A-Za-z_][A-Za-z0-9_]*)\.(?P<field>[A-Za-z_][A-Za-z0-9_]*)",
            expression_text,
        )
        if field_match is not None:
            alias = field_match.group("alias")
            binding = binding_map.get(alias)
            if binding is None or binding.binding_kind != "entity":
                raise ValueError(
                    f"HumemCypher v0 WITH queries cannot return unknown entity alias {alias!r}."
                )
            column_name = output_alias or f"{alias}.{field_match.group('field')}"
            if column_name in seen_output_names:
                raise ValueError(
                    f"HumemCypher v0 WITH queries do not allow duplicate RETURN output alias {column_name!r}."
                )
            seen_output_names.add(column_name)
            items.append(
                WithReturnItem(
                    kind="field",
                    alias=alias,
                    field=field_match.group("field"),
                    output_alias=output_alias,
                )
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
        column_name = output_alias or expression_text
        if column_name in seen_output_names:
            raise ValueError(
                f"HumemCypher v0 WITH queries do not allow duplicate RETURN output alias {column_name!r}."
            )
        seen_output_names.add(column_name)
        if binding.binding_kind == "scalar":
            items.append(
                WithReturnItem(kind="scalar", alias=expression_text, output_alias=output_alias)
            )
            continue
        items.append(
            WithReturnItem(kind="entity", alias=expression_text, output_alias=output_alias)
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
        item.column_name for item in returns if item.kind in _AGGREGATE_RETURN_KINDS
    }
    projected_aliases = {
        item.column_name: item
        for item in returns
        if item.output_alias is not None and item.kind not in _AGGREGATE_RETURN_KINDS
    }
    items: list[WithOrderItem] = []
    for item in order_items:
        if item.field == "__value__" and item.alias in aggregate_aliases:
            items.append(
                WithOrderItem(kind="aggregate", alias=item.alias, direction=item.direction)
            )
            continue
        if item.field == "__value__" and item.alias in projected_aliases:
            projected = projected_aliases[item.alias]
            items.append(
                WithOrderItem(
                    kind=projected.kind,
                    alias=projected.alias,
                    field=projected.field,
                    direction=item.direction,
                    operator=projected.operator,
                    value=projected.value,
                    start_value=projected.start_value,
                    length_value=projected.length_value,
                    search_value=projected.search_value,
                    replace_value=projected.replace_value,
                    delimiter_value=projected.delimiter_value,
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
        item_text = raw_item.strip()
        alias_match = re.fullmatch(
            r"(?P<expr>.+?)\s+as\s+(?P<output>[A-Za-z_][A-Za-z0-9_]*)",
            item_text,
            flags=re.IGNORECASE,
        )
        output_alias = alias_match.group("output") if alias_match is not None else None
        expression_text = alias_match.group("expr").strip() if alias_match is not None else item_text
        if expression_text != alias:
            raise ValueError(
                "HumemCypher v0 UNWIND currently supports only RETURN unwind_alias or RETURN unwind_alias AS output_alias."
            )
        output_name = output_alias or alias
        if output_name in seen_output_names:
            raise ValueError(
                f"HumemCypher v0 UNWIND does not allow duplicate RETURN output alias {output_name!r}."
            )
        seen_output_names.add(output_name)
        items.append(WithReturnItem(kind="scalar", alias=alias, output_alias=output_alias))
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


def _normalize_query_nodes_vector_search(
    result: CypherParseResult,
    single_part_query,
) -> NormalizedQueryNodesVectorSearch:
    reading_clauses = single_part_query.oC_ReadingClause()
    in_query_call = reading_clauses[0].oC_InQueryCall()
    assert in_query_call is not None

    procedure_ctx = in_query_call.oC_ExplicitProcedureInvocation()
    assert procedure_ctx is not None
    procedure_args = procedure_ctx.oC_Expression()

    index_value = _parse_literal(_context_text(result, procedure_args[0]).strip())
    assert isinstance(index_value, str)
    top_k = _parse_query_nodes_limit_ref(_context_text(result, procedure_args[1]).strip())
    query_value = _parse_literal(_context_text(result, procedure_args[2]).strip())
    if not isinstance(query_value, _ParameterRef):
        raise ValueError(
            "CypherGlot currently requires vector procedure query embeddings to "
            "come from a named parameter."
        )

    yield_items = in_query_call.oC_YieldItems()
    assert yield_items is not None
    yield_where = yield_items.oC_Where()

    if len(reading_clauses) == 2:
        match_ctx = reading_clauses[1].oC_Match()
        assert match_ctx is not None
        candidate_query_text = f"{_context_text(result, match_ctx)} RETURN node.id"
    elif yield_where is not None:
        candidate_query_text = (
            "MATCH (node) WHERE "
            f"{_context_text(result, yield_where.oC_Expression())} RETURN node.id"
        )
    else:
        candidate_query_text = "MATCH (node) RETURN node.id"

    candidate_query = normalize_cypher_text(candidate_query_text)
    if not isinstance(candidate_query, (NormalizedMatchNode, NormalizedMatchRelationship)):
        raise ValueError(
            "CypherGlot vector-aware normalization requires a node-oriented MATCH "
            "candidate query."
        )

    return_ctx = single_part_query.oC_Return()
    assert return_ctx is not None
    projection_text = _context_text(result, return_ctx.oC_ProjectionBody())
    return_text, order_text = _split_query_nodes_return_and_order(projection_text)
    return_items = _parse_query_nodes_return_items(return_text)
    order_by = _parse_query_nodes_order_items(order_text)

    return NormalizedQueryNodesVectorSearch(
        kind="vector_query",
        procedure_kind="queryNodes",
        index_name=index_value,
        query_param_name=query_value.name,
        top_k=top_k,
        candidate_query=candidate_query,
        return_items=return_items,
        order_by=order_by,
    )


def _context_text(result: CypherParseResult, ctx: object) -> str:
    start_index = ctx.start.tokenIndex
    stop_index = ctx.stop.tokenIndex
    return result.token_stream.getText(start=start_index, stop=stop_index)
