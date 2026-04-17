"""Normalize generated Cypher parse output into CypherGlot-owned structures."""

from __future__ import annotations

from dataclasses import dataclass
import re
from typing import Literal, cast

from ._logging import get_logger
from ._normalize_support import (
    _BINARY_TERNARY_RETURN_FUNCTION_NAMES,
    _UNARY_RETURN_FUNCTION_NAMES,
    _ParameterRef,
    _format_cypher_value,
    CypherValue,
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
    _parse_relationship_pattern,
    _parse_return_items,
    _parse_set_items,
    _parse_literal,
    _split_comma_separated,
    _split_query_nodes_return_and_order,
    _split_relationship_pattern,
    _split_return_clause,
    _unwrap_node_pattern,
    _validate_create_relationship_separate_patterns,
    _validate_match_create_relationship_between_nodes_endpoints,
    _validate_match_create_relationship_endpoints,
    _validate_match_merge_relationship_endpoints,
)
from .parser import CypherParseResult, parse_cypher_text
from .validate import validate_cypher_parse_result


logger = get_logger(__name__)


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
class NormalizedMatchMergeRelationshipOnNode:
    kind: Literal["match_merge"]
    pattern_kind: Literal["relationship"]
    match_node: NodePattern
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
    | NormalizedMatchMergeRelationshipOnNode
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
    logger.debug("Normalizing Cypher text")
    try:
        statement = normalize_cypher_parse_result(parse_cypher_text(text))
    except Exception:
        logger.debug("Normalization failed", exc_info=True)
        raise
    logger.debug(
        "Normalized Cypher text",
        extra={"statement_kind": type(statement).__name__},
    )
    return statement


def normalize_cypher_parse_result(
    result: CypherParseResult,
) -> NormalizedCypherStatement:
    logger.debug("Normalizing parsed Cypher result")
    validated_query = validate_cypher_parse_result(result)
    if type(validated_query).__name__ == "OC_MultiPartQueryContext":
        statement = _normalize_match_with_return(result, validated_query)
        logger.debug(
            "Normalized parsed Cypher result",
            extra={"statement_kind": type(statement).__name__},
        )
        return statement

    single_part_query = validated_query
    updating_clauses = single_part_query.oC_UpdatingClause()
    reading_clauses = single_part_query.oC_ReadingClause()

    if reading_clauses and reading_clauses[0].oC_InQueryCall() is not None:
        statement = _normalize_query_nodes_vector_search(result, single_part_query)
        logger.debug(
            "Normalized parsed Cypher result",
            extra={"statement_kind": type(statement).__name__},
        )
        return statement

    if reading_clauses and reading_clauses[0].oC_Match() is not None:
        match_ctx = reading_clauses[0].oC_Match()
        if match_ctx.OPTIONAL() is not None:
            statement = _normalize_optional_match_node(result, single_part_query)
            logger.debug(
                "Normalized parsed Cypher result",
                extra={"statement_kind": type(statement).__name__},
            )
            return statement

    if reading_clauses and reading_clauses[0].oC_Unwind() is not None:
        statement = _normalize_unwind_query(result, single_part_query)
        logger.debug(
            "Normalized parsed Cypher result",
            extra={"statement_kind": type(statement).__name__},
        )
        return statement

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

                if len(match_patterns) == 1:
                    match_node = _parse_node_pattern(
                        _unwrap_node_pattern(match_patterns[0]),
                        default_alias="__humem_match_merge_node",
                    )
                    _validate_normalized_match_predicates(
                        predicates,
                        alias_kinds={match_node.alias: "node"},
                    )
                    _validate_match_merge_relationship_endpoints(
                        match_node,
                        left,
                        right,
                    )
                    return NormalizedMatchMergeRelationshipOnNode(
                        kind="match_merge",
                        pattern_kind="relationship",
                        match_node=match_node,
                        predicates=predicates,
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


def _parse_match_chain(result: CypherParseResult, pattern_element) -> tuple[
    tuple[NodePattern, ...], tuple[RelationshipPattern, ...]
]:
    from ._normalize_with_helpers import _parse_match_chain as _impl

    return _impl(result, pattern_element)


def _normalize_match_with_return(result: CypherParseResult, multi_part_query) -> NormalizedMatchWithReturn:
    from ._normalize_with_helpers import _normalize_match_with_return as _impl

    return _impl(result, multi_part_query)


def _normalize_match_source(result: CypherParseResult, match_ctx) -> (
    NormalizedMatchNode | NormalizedMatchRelationship | NormalizedMatchChain
):
    from ._normalize_with_helpers import _normalize_match_source as _impl

    return _impl(result, match_ctx)


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
    result: CypherParseResult, with_ctx, alias_kinds: dict[str, Literal["node", "relationship"]]
) -> tuple[WithBinding, ...]:
    from ._normalize_with_helpers import _parse_with_bindings as _impl

    return _impl(result, with_ctx, alias_kinds)


def _parse_with_predicates(text: str, bindings: tuple[WithBinding, ...]) -> tuple[WithPredicate, ...]:
    from ._normalize_with_helpers import _parse_with_predicates as _impl

    return _impl(text, bindings)


def _parse_with_case_result_item(text: str, binding_map: dict[str, WithBinding]) -> WithReturnItem:
    from ._normalize_with_helpers import _parse_with_case_result_item as _impl

    return _impl(text, binding_map)


def _parse_with_case_condition_item(text: str, binding_map: dict[str, WithBinding]) -> WithReturnItem:
    from ._normalize_with_helpers import _parse_with_case_condition_item as _impl

    return _impl(text, binding_map)


def _parse_with_case_expression(text: str, binding_map: dict[str, WithBinding]) -> WithCaseSpec | None:
    from ._normalize_with_helpers import _parse_with_case_expression as _impl

    return _impl(text, binding_map)


def _parse_with_return_items(text: str, bindings: tuple[WithBinding, ...]) -> tuple[WithReturnItem, ...]:
    from ._normalize_with_helpers import _parse_with_return_items as _impl

    return _impl(text, bindings)


def _parse_with_order_items(
    order_items: tuple[OrderItem, ...],
    bindings: tuple[WithBinding, ...],
    returns: tuple[WithReturnItem, ...],
) -> tuple[WithOrderItem, ...]:
    from ._normalize_with_helpers import _parse_with_order_items as _impl

    return _impl(order_items, bindings, returns)


def _normalize_unwind_query(result: CypherParseResult, single_part_query) -> NormalizedUnwind:
    from ._normalize_with_helpers import _normalize_unwind_query as _impl

    return _impl(result, single_part_query)


def _parse_unwind_source(text: str) -> tuple[
    Literal["literal", "parameter"], tuple[CypherValue, ...], str | None
]:
    from ._normalize_with_helpers import _parse_unwind_source as _impl

    return _impl(text)


def _parse_unwind_return_items(text: str, alias: str) -> tuple[WithReturnItem, ...]:
    from ._normalize_with_helpers import _parse_unwind_return_items as _impl

    return _impl(text, alias)


def _parse_unwind_order_items(
    order_items: tuple[OrderItem, ...],
    alias: str,
    returns: tuple[WithReturnItem, ...],
) -> tuple[WithOrderItem, ...]:
    from ._normalize_with_helpers import _parse_unwind_order_items as _impl

    return _impl(order_items, alias, returns)


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
