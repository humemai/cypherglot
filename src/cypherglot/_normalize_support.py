from __future__ import annotations

from dataclasses import dataclass
import re
from typing import Literal, cast


_ScalarPropertyValue = str | int | float | bool | None
_VectorPropertyValue = tuple[float, ...]
PropertyValue = _ScalarPropertyValue | _VectorPropertyValue


@dataclass(frozen=True, slots=True)
class _ParameterRef:
    name: str


CypherValue = PropertyValue | _ParameterRef
PropertyItems = tuple[tuple[str, CypherValue], ...]


@dataclass(frozen=True, slots=True)
class Predicate:
    alias: str
    field: str
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
    value: CypherValue
    disjunct_index: int = 0


@dataclass(frozen=True, slots=True)
class NodePattern:
    alias: str
    label: str | None
    properties: PropertyItems = ()


@dataclass(frozen=True, slots=True)
class RelationshipPattern:
    alias: str | None
    type_name: str | None
    direction: Literal["out", "in"]
    properties: PropertyItems = ()
    min_hops: int = 1
    max_hops: int | None = 1


@dataclass(frozen=True, slots=True)
class ReturnItem:
    alias: str
    field: str | None = None
    kind: Literal["field", "entity", "count", "sum", "avg", "min", "max", "scalar", "size", "predicate", "id", "type", "properties", "labels", "keys", "start_node", "end_node", "lower", "upper", "trim", "ltrim", "rtrim", "reverse", "coalesce", "replace", "left", "right", "split", "abs", "sign", "round", "ceil", "floor", "sqrt", "exp", "sin", "cos", "tan", "asin", "acos", "atan", "ln", "log", "log10", "radians", "degrees", "to_string", "to_integer", "to_float", "to_boolean", "substring", "case"] = "field"
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
    start_value: CypherValue | None = None
    length_value: CypherValue | None = None
    search_value: CypherValue | None = None
    replace_value: CypherValue | None = None
    delimiter_value: CypherValue | None = None
    output_alias: str | None = None

    @property
    def column_name(self) -> str:
        if self.output_alias is not None:
            return self.output_alias
        if self.kind == "count":
            if self.alias == "*":
                return "count(*)"
            return f"count({self.alias})"
        if self.kind in {"sum", "avg", "min", "max"}:
            assert self.field is not None
            return f"{self.kind}({self.alias}.{self.field})"
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
        if self.kind in _UNARY_RETURN_FUNCTION_NAMES and self.field is not None:
            return f"{_UNARY_RETURN_FUNCTION_NAMES[self.kind]}({self.alias}.{self.field})"
        if self.kind == "coalesce" and self.field is not None:
            return (
                f"coalesce({self.alias}.{self.field}, "
                f"{_format_cypher_value(cast(CypherValue, self.value))})"
            )
        if self.kind == "replace":
            if self.field is not None:
                primary_expr = f"{self.alias}.{self.field}"
            else:
                primary_expr = _format_cypher_value(cast(CypherValue, self.value))
            return (
                f"replace({primary_expr}, "
                f"{_format_cypher_value(cast(CypherValue, self.search_value))}, "
                f"{_format_cypher_value(cast(CypherValue, self.replace_value))})"
            )
        if self.kind in {"left", "right", "split", "substring"}:
            function_name = _BINARY_TERNARY_RETURN_FUNCTION_NAMES[self.kind]
            if self.field is not None:
                primary_expr = f"{self.alias}.{self.field}"
            else:
                primary_expr = _format_cypher_value(cast(CypherValue, self.value))
            arg_suffix: list[str] = []
            if self.kind in {"left", "right"}:
                arg_suffix.append(_format_cypher_value(cast(CypherValue, self.length_value)))
            elif self.kind == "split":
                arg_suffix.append(_format_cypher_value(cast(CypherValue, self.delimiter_value)))
            else:
                arg_suffix.append(_format_cypher_value(cast(CypherValue, self.start_value)))
                if self.length_value is not None:
                    arg_suffix.append(_format_cypher_value(cast(CypherValue, self.length_value)))
            return f"{function_name}({', '.join((primary_expr, *arg_suffix))})"
        if self.kind in {"sum", "avg", "min", "max", "scalar", "size", "predicate", "id", "type", "properties", "labels", "keys", "start_node", "end_node", "lower", "upper", "trim", "ltrim", "rtrim", "reverse", "coalesce", "replace", "left", "right", "split", "abs", "sign", "round", "ceil", "floor", "sqrt", "exp", "sin", "cos", "tan", "asin", "acos", "atan", "ln", "log", "log10", "radians", "degrees", "to_string", "to_integer", "to_float", "to_boolean", "substring", "case"}:
            raise ValueError(
                "HumemCypher v0 scalar expression RETURN items require an explicit AS alias."
            )
        if self.field is None:
            return self.alias
        return f"{self.alias}.{self.field}"


@dataclass(frozen=True, slots=True)
class CaseWhen:
    condition: ReturnItem
    result: ReturnItem


@dataclass(frozen=True, slots=True)
class CaseSpec:
    when_items: tuple[CaseWhen, ...]
    else_item: ReturnItem


@dataclass(frozen=True, slots=True)
class OrderItem:
    alias: str
    field: str
    direction: Literal["asc", "desc"] = "asc"
    expression: str | None = None


@dataclass(frozen=True, slots=True)
class SetItem:
    alias: str
    field: str
    value: CypherValue


_IDENTIFIER = r"[A-Za-z_][A-Za-z0-9_]*"
_NODE_PATTERN_RE = re.compile(
    rf"^(?:(?P<alias>{_IDENTIFIER}))?(?::(?P<label>{_IDENTIFIER}))?"
    r"(?:\s*\{\s*(?P<properties>.*)\s*\})?$"
)
_REL_PATTERN_RE = re.compile(
    rf"^(?:(?P<alias>{_IDENTIFIER})\s*)?"
    rf"(?::(?P<type>{_IDENTIFIER}(?:\|{_IDENTIFIER})*))?"
    r"(?P<range>\s*\*\s*(?:\d+\s*)?(?:\.\.\s*(?:\d+\s*)?)?)?"
    r"(?:\s*\{\s*(?P<properties>.*)\s*\})?$"
)
_RETURN_ITEM_RE = re.compile(rf"^(?P<alias>{_IDENTIFIER})\.(?P<field>{_IDENTIFIER})$")
_SCALAR_ITEM_RE = re.compile(rf"^(?P<alias>{_IDENTIFIER})$")
_SIZE_PREDICATE_FIELD_PREFIX = "__size__:"
_NODE_FUNCTION_RETURN_ITEM_RE = re.compile(
    rf"^(?P<function>startNode|endNode)\s*\(\s*(?P<alias>{_IDENTIFIER})\s*\)(?:\.(?P<field>{_IDENTIFIER}))?$",
    flags=re.IGNORECASE,
)

_UNARY_RETURN_FUNCTION_NAMES: dict[str, str] = {
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
    "to_string": "toString",
    "to_integer": "toInteger",
    "to_float": "toFloat",
    "to_boolean": "toBoolean",
}

_BINARY_TERNARY_RETURN_FUNCTION_NAMES: dict[str, str] = {
    "coalesce": "coalesce",
    "replace": "replace",
    "left": "left",
    "right": "right",
    "split": "split",
    "substring": "substring",
}


def _format_cypher_value(value: CypherValue | None) -> str:
    if isinstance(value, _ParameterRef):
        return f"${value.name}"
    if isinstance(value, str):
        return "'" + value.replace("\\", "\\\\").replace("'", "\\'") + "'"
    if value is True:
        return "true"
    if value is False:
        return "false"
    if value is None:
        return "null"
    return str(value)


def _parse_node_pattern(
    text: str,
    *,
    require_label: bool = False,
    default_alias: str | None = None,
) -> NodePattern:
    match = _NODE_PATTERN_RE.fullmatch(text.strip())
    if match is None:
        raise ValueError(f"HumemCypher v0 could not parse node pattern: {text!r}")

    label = match.group("label")
    if require_label and label is None:
        raise ValueError("HumemCypher v0 CREATE patterns require labeled nodes.")

    alias = match.group("alias") or default_alias
    if alias is None:
        raise ValueError(
            "HumemCypher v0 currently requires a node alias unless the pattern "
            "position admits an anonymous node."
        )

    return NodePattern(
        alias=alias,
        label=label,
        properties=_parse_properties(match.group("properties")),
    )


def _parse_relationship_pattern(
    text: str,
    direction: Literal["out", "in"],
) -> RelationshipPattern:
    match = _REL_PATTERN_RE.fullmatch(text.strip())
    if match is None:
        raise ValueError(
            f"HumemCypher v0 could not parse relationship pattern: {text!r}"
        )

    min_hops, max_hops = _parse_relationship_hop_bounds(match.group("range"))

    return RelationshipPattern(
        alias=match.group("alias"),
        type_name=match.group("type"),
        direction=direction,
        properties=_parse_properties(match.group("properties")),
        min_hops=min_hops,
        max_hops=max_hops,
    )


def _parse_relationship_hop_bounds(range_text: str | None) -> tuple[int, int | None]:
    if range_text is None or not range_text.strip():
        return 1, 1

    match = re.fullmatch(
        r"\*\s*(?:(?P<start>\d+)\s*)?(?:(?P<dots>\.\.)\s*(?:(?P<end>\d+)\s*)?)?",
        range_text.strip(),
    )
    if match is None:
        raise ValueError(
            f"HumemCypher v0 could not parse relationship range literal: {range_text!r}"
        )

    start_text = match.group("start")
    end_text = match.group("end")
    if match.group("dots") is None:
        if start_text is None:
            return 1, None
        exact = int(start_text)
        return exact, exact

    start = int(start_text) if start_text is not None else 1
    end = int(end_text) if end_text is not None else None
    return start, end


def _parse_relationship_chain_segment(text: str) -> RelationshipPattern:
    outbound = re.fullmatch(r"-\[\s*(?P<body>[^\]]*)\s*\]->", text.strip())
    if outbound is not None:
        return _parse_relationship_pattern(outbound.group("body"), "out")

    inbound = re.fullmatch(r"<-\[\s*(?P<body>[^\]]*)\s*\]-", text.strip())
    if inbound is not None:
        return _parse_relationship_pattern(inbound.group("body"), "in")

    raise ValueError(
        "CypherGlot currently supports only fixed-length directed relationship chains."
    )


def _parse_order_items(text: str) -> tuple[OrderItem, ...]:
    items: list[OrderItem] = []

    for raw_item in _split_comma_separated(text):
        item_text = raw_item.strip()
        parts = item_text.rsplit(None, 1)
        direction: Literal["asc", "desc"] = "asc"
        target = item_text

        if len(parts) == 2 and parts[1].lower() in {"asc", "desc"}:
            target = parts[0]
            direction = cast(Literal["asc", "desc"], parts[1].lower())

        match = _RETURN_ITEM_RE.fullmatch(target.strip())
        if match is not None:
            items.append(
                OrderItem(
                    alias=match.group("alias"),
                    field=match.group("field"),
                    direction=direction,
                )
            )
            continue

        scalar_match = _SCALAR_ITEM_RE.fullmatch(target.strip())
        if scalar_match is not None:
            items.append(
                OrderItem(
                    alias=scalar_match.group("alias"),
                    field="__value__",
                    direction=direction,
                )
            )
            continue

        items.append(
            OrderItem(
                alias="",
                field="__expression__",
                direction=direction,
                expression=target.strip(),
            )
        )

    if not items:
        raise ValueError("HumemCypher v0 ORDER BY clauses cannot be empty.")

    return tuple(items)


def _split_query_nodes_return_and_order(text: str) -> tuple[str, str | None]:
    match = re.search(r"\bORDER\s+BY\b", text, flags=re.IGNORECASE)
    if match is None:
        return text.strip(), None
    return text[:match.start()].strip(), text[match.end():].strip()


def _parse_query_nodes_order_items(
    text: str | None,
) -> tuple[tuple[str, Literal["asc", "desc"]], ...]:
    if text is None:
        return ()
    if not text:
        raise ValueError("HumemCypher v0 ORDER BY clauses cannot be empty.")

    items: list[tuple[str, Literal["asc", "desc"]]] = []
    for raw_item in _split_comma_separated(text):
        item_text = raw_item.strip()
        parts = item_text.rsplit(None, 1)
        direction: Literal["asc", "desc"] = "asc"
        target = item_text
        if len(parts) == 2 and parts[1].lower() in {"asc", "desc"}:
            target = parts[0]
            direction = cast(Literal["asc", "desc"], parts[1].lower())
        if target not in {"node.id", "score"}:
            raise ValueError(
                "HumemCypher v0 vector procedure ORDER BY currently supports only "
                "node.id and score."
            )
        items.append((target, direction))

    return tuple(items)


def _parse_query_nodes_return_items(text: str) -> tuple[str, ...]:
    items = tuple(item.strip() for item in _split_comma_separated(text) if item.strip())
    if not items:
        raise ValueError(
            "HumemCypher v0 vector procedure queries currently require a RETURN "
            "clause over node.id and/or score."
        )
    for item in items:
        if item not in {"node.id", "score"}:
            raise ValueError(
                "HumemCypher v0 vector procedure queries currently support only "
                "RETURN node.id and score."
            )
    return items


def _parse_query_nodes_limit_ref(text: str) -> int | str:
    value = _parse_literal(text)
    if type(value) is int:
        return value
    if isinstance(value, _ParameterRef):
        return value.name
    raise ValueError(
        "HumemCypher v0 vector procedure queries currently require the top-k "
        "argument to be an integer literal or named parameter."
    )


def _split_return_clause(
    text: str,
) -> tuple[str, tuple[OrderItem, ...], int | None, bool, int | None]:
    order_by_match = re.search(r"\border\s+by\b", text, flags=re.IGNORECASE)
    skip_match = re.search(r"\b(skip|offset)\b", text, flags=re.IGNORECASE)
    limit_match = re.search(r"\blimit\b", text, flags=re.IGNORECASE)

    if order_by_match is None and skip_match is None and limit_match is None:
        return_text, distinct = _parse_return_projection(text)
        return return_text, (), None, distinct, None

    clause_positions = [
        match.start()
        for match in (order_by_match, skip_match, limit_match)
        if match is not None
    ]
    returns_text = text[: min(clause_positions)].strip()

    if order_by_match is not None and (
        (skip_match is not None and skip_match.start() < order_by_match.start())
        or (limit_match is not None and limit_match.start() < order_by_match.start())
    ):
        raise ValueError(
            "HumemCypher v0 requires ORDER BY to appear before SKIP/OFFSET and LIMIT."
        )
    if (
        skip_match is not None
        and limit_match is not None
        and limit_match.start() < skip_match.start()
    ):
        raise ValueError("HumemCypher v0 requires SKIP/OFFSET to appear before LIMIT.")

    order_by: tuple[OrderItem, ...] = ()
    if order_by_match is not None:
        order_end = len(text)
        if skip_match is not None:
            order_end = skip_match.start()
        elif limit_match is not None:
            order_end = limit_match.start()
        order_text = text[order_by_match.end():order_end].strip()
        order_by = _parse_order_items(order_text)

    skip: int | None = None
    if skip_match is not None:
        skip_end = limit_match.start() if limit_match is not None else len(text)
        skip = _parse_skip_clause(
            text[skip_match.end():skip_end].strip(),
            clause_name=skip_match.group(1).upper(),
        )

    limit: int | None = None
    if limit_match is not None:
        limit = _parse_limit_clause(text[limit_match.end():].strip())

    return_text, distinct = _parse_return_projection(returns_text)
    return return_text, order_by, limit, distinct, skip


def _parse_return_projection(text: str) -> tuple[str, bool]:
    projection_text = text.strip()
    distinct_match = re.match(r"distinct\b", projection_text, flags=re.IGNORECASE)
    if distinct_match is None:
        return projection_text, False

    projection_text = projection_text[distinct_match.end():].strip()
    if not projection_text:
        raise ValueError("HumemCypher v0 RETURN DISTINCT clauses cannot be empty.")
    return projection_text, True


def _parse_skip_clause(text: str, *, clause_name: str = "SKIP") -> int:
    if not text:
        raise ValueError(f"HumemCypher v0 {clause_name} clauses cannot be empty.")
    if not re.fullmatch(r"\d+", text):
        raise ValueError(
            f"HumemCypher v0 {clause_name} currently requires an integer literal."
        )

    skip = int(text)
    if skip < 0:
        raise ValueError(f"HumemCypher v0 {clause_name} must be at least 0.")
    return skip


def _parse_limit_clause(text: str) -> int:
    if not text:
        raise ValueError("HumemCypher v0 LIMIT clauses cannot be empty.")
    if not re.fullmatch(r"\d+", text):
        raise ValueError("HumemCypher v0 LIMIT currently requires an integer literal.")

    limit = int(text)
    if limit < 1:
        raise ValueError("HumemCypher v0 LIMIT must be at least 1.")
    return limit


def _parse_predicates(text: str) -> tuple[Predicate, ...]:
    predicates: list[Predicate] = []

    for disjunct_index, disjunct in enumerate(_parse_boolean_predicate_groups(text)):
        for item in disjunct:
            try:
                left_text, operator, value_text = _split_predicate_comparison(item)
            except ValueError as exc:
                raise ValueError(
                    "HumemCypher v0 WHERE items must look like alias.field OP value, id(alias) OP value, or type(rel_alias) OP value."
                ) from exc
            left_text = left_text.strip()
            if operator in {"IS NULL", "IS NOT NULL"}:
                if value_text.strip():
                    raise ValueError(
                        "HumemCypher v0 null predicates cannot include a trailing "
                        "literal value."
                    )
                parsed_value: CypherValue = None
            else:
                parsed_value = _parse_literal(value_text.strip())

            id_match = re.fullmatch(
                rf"id\s*\(\s*(?P<alias>{_IDENTIFIER})\s*\)",
                left_text,
                flags=re.IGNORECASE,
            )
            if id_match is not None:
                predicates.append(
                    Predicate(
                        alias=id_match.group("alias"),
                        field="id",
                        operator=operator,
                        disjunct_index=disjunct_index,
                        value=parsed_value,
                    )
                )
                continue

            type_match = re.fullmatch(
                rf"type\s*\(\s*(?P<alias>{_IDENTIFIER})\s*\)",
                left_text,
                flags=re.IGNORECASE,
            )
            if type_match is not None:
                predicates.append(
                    Predicate(
                        alias=type_match.group("alias"),
                        field="type",
                        operator=operator,
                        disjunct_index=disjunct_index,
                        value=parsed_value,
                    )
                )
                continue

            size_match = re.fullmatch(
                rf"size\s*\(\s*(?P<expr>.+?)\s*\)",
                left_text,
                flags=re.IGNORECASE,
            )
            if size_match is not None:
                size_expr = size_match.group("expr").strip()
                match = _RETURN_ITEM_RE.fullmatch(size_expr)
                if match is None:
                    raise ValueError(
                        "HumemCypher v0 WHERE items must look like alias.field OP value, id(alias) OP value, type(rel_alias) OP value, or size(alias.field) OP value."
                    )
                predicates.append(
                    Predicate(
                        alias=match.group("alias"),
                        field=f"{_SIZE_PREDICATE_FIELD_PREFIX}{match.group('field')}",
                        operator=operator,
                        disjunct_index=disjunct_index,
                        value=parsed_value,
                    )
                )
                continue

            match = _RETURN_ITEM_RE.fullmatch(left_text)
            if match is None:
                raise ValueError(
                    "HumemCypher v0 WHERE items must look like alias.field OP value, id(alias) OP value, type(rel_alias) OP value, or size(alias.field) OP value."
                )
            predicates.append(
                Predicate(
                    alias=match.group("alias"),
                    field=match.group("field"),
                    operator=operator,
                    disjunct_index=disjunct_index,
                    value=parsed_value,
                )
            )

    if not predicates:
        raise ValueError("HumemCypher v0 WHERE clauses cannot be empty.")

    return tuple(predicates)


def _parse_boolean_predicate_groups(text: str) -> list[list[str]]:
    tokens = _tokenize_boolean_expression(text)
    if not tokens:
        raise ValueError("HumemCypher v0 WHERE clauses cannot be empty.")

    parser = _BooleanPredicateParser(tokens)
    groups = parser.parse_expression()
    if parser.has_more_tokens():
        raise ValueError("HumemCypher v0 could not parse the full WHERE clause.")
    return groups


class _BooleanPredicateParser:
    def __init__(self, tokens: list[str]) -> None:
        self._tokens = tokens
        self._index = 0

    def has_more_tokens(self) -> bool:
        return self._index < len(self._tokens)

    def parse_expression(self) -> list[list[str]]:
        groups = self._parse_or_expression()
        if not groups:
            raise ValueError("HumemCypher v0 WHERE clauses cannot be empty.")
        return groups

    def _parse_or_expression(self) -> list[list[str]]:
        groups = self._parse_and_expression()
        while self._matches_keyword("OR"):
            self._index += 1
            groups.extend(self._parse_and_expression())
        return groups

    def _parse_and_expression(self) -> list[list[str]]:
        groups = self._parse_primary_expression()
        while self._matches_keyword("AND"):
            self._index += 1
            right_groups = self._parse_primary_expression()
            groups = [
                left_group + right_group
                for left_group in groups
                for right_group in right_groups
            ]
        return groups

    def _parse_primary_expression(self) -> list[list[str]]:
        token = self._peek()
        if token is None:
            raise ValueError("HumemCypher v0 WHERE clauses cannot end abruptly.")

        if token == "(":
            self._index += 1
            groups = self._parse_or_expression()
            if self._peek() != ")":
                raise ValueError(
                    "HumemCypher v0 found an unmatched '(' in WHERE clause."
                )
            self._index += 1
            return groups

        comparison_tokens: list[str] = []
        while self.has_more_tokens():
            current = self._peek()
            assert current is not None
            if current in ("(", ")") or current.upper() in ("AND", "OR"):
                break
            comparison_tokens.append(current)
            self._index += 1

        if not comparison_tokens:
            raise ValueError(
                "HumemCypher v0 WHERE items must look like alias.field OP value."
            )
        return [[" ".join(comparison_tokens)]]

    def _peek(self) -> str | None:
        if not self.has_more_tokens():
            return None
        return self._tokens[self._index]

    def _matches_keyword(self, keyword: str) -> bool:
        token = self._peek()
        return token is not None and token.upper() == keyword


def _tokenize_boolean_expression(text: str) -> list[str]:
    tokens: list[str] = []
    current: list[str] = []
    in_string = False
    escape = False
    call_depth = 0

    for character in text:
        if escape:
            current.append(character)
            escape = False
            continue

        if character == "\\":
            current.append(character)
            escape = True
            continue

        if character == "'":
            current.append(character)
            in_string = not in_string
            continue

        if not in_string and character == "(":
            if call_depth > 0 or re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*\s*", "".join(current)):
                current.append(character)
                call_depth += 1
                continue
            token = "".join(current).strip()
            if token:
                tokens.append(token)
            tokens.append(character)
            current = []
            continue

        if not in_string and character == ")":
            if call_depth > 0:
                current.append(character)
                call_depth -= 1
                continue
            token = "".join(current).strip()
            if token:
                tokens.append(token)
            tokens.append(character)
            current = []
            continue

        if not in_string and character.isspace() and call_depth == 0:
            token = "".join(current).strip()
            if token:
                tokens.append(token)
                current = []
            continue

        current.append(character)

    final_token = "".join(current).strip()
    if in_string:
        raise ValueError("HumemCypher v0 found an unterminated string literal.")
    if call_depth != 0:
        raise ValueError("HumemCypher v0 found an unmatched function-call parenthesis.")
    if final_token:
        tokens.append(final_token)
    return tokens


def _parse_set_items(text: str) -> tuple[SetItem, ...]:
    assignments: list[SetItem] = []
    for item in _split_comma_separated(text):
        left_text, value_text = _split_outside_string(item, "=")
        match = _RETURN_ITEM_RE.fullmatch(left_text.strip())
        if match is None:
            raise ValueError(
                "HumemCypher v0 SET items must look like alias.field = value."
            )
        assignments.append(
            SetItem(
                alias=match.group("alias"),
                field=match.group("field"),
                value=_parse_literal(value_text.strip()),
            )
        )

    if not assignments:
        raise ValueError("HumemCypher v0 SET clauses cannot be empty.")

    return tuple(assignments)


def _parse_properties(text: str | None) -> PropertyItems:
    if text is None or not text.strip():
        return ()

    properties: list[tuple[str, CypherValue]] = []
    for item in _split_comma_separated(text):
        key_text, value_text = _split_outside_string(item, ":")
        key = key_text.strip()
        if not re.fullmatch(_IDENTIFIER, key):
            raise ValueError(
                f"HumemCypher v0 property keys must be simple identifiers; got {key!r}."
            )
        properties.append((key, _parse_literal(value_text.strip())))

    return tuple(properties)


def _parse_literal(text: str) -> CypherValue:
    if text.startswith("$"):
        parameter_name = text[1:]
        if not re.fullmatch(_IDENTIFIER, parameter_name):
            raise ValueError(
                f"HumemCypher v0 parameter names must be identifiers; got {text!r}."
            )
        return _ParameterRef(parameter_name)

    if len(text) >= 2 and text[0] == "'" and text[-1] == "'":
        return text[1:-1].replace("\\'", "'")

    lowered = text.lower()
    if lowered == "true":
        return True
    if lowered == "false":
        return False
    if lowered == "null":
        return None

    if re.fullmatch(r"-?\d+", text):
        return int(text)
    if re.fullmatch(r"-?\d+\.\d+", text):
        return float(text)

    raise ValueError(
        "HumemCypher v0 only supports inline string, integer, float, boolean, "
        f"and null literals; got {text!r}."
    )


def _looks_like_relationship_pattern(text: str) -> bool:
    return ("-[" in text and "]->" in text) or ("<-[" in text and "]-" in text)


def _split_relationship_pattern(
    text: str,
) -> tuple[str, str, str, Literal["out", "in"]]:
    outbound = re.fullmatch(
        r"\((?P<left>[^)]*)\)\s*-\[\s*(?P<rel>[^\]]+)\s*\]\s*->\s*\((?P<right>[^)]*)\)",
        text.strip(),
    )
    if outbound is not None:
        return (
            outbound.group("left"),
            outbound.group("rel"),
            outbound.group("right"),
            "out",
        )

    inbound = re.fullmatch(
        r"\((?P<left>[^)]*)\)\s*<-\[\s*(?P<rel>[^\]]+)\s*\]\s*-\s*\((?P<right>[^)]*)\)",
        text.strip(),
    )
    if inbound is not None:
        return (
            inbound.group("left"),
            inbound.group("rel"),
            inbound.group("right"),
            "in",
        )

    raise ValueError(
        "HumemCypher v0 only supports a single directed relationship pattern."
    )


def _unwrap_node_pattern(text: str) -> str:
    match = re.fullmatch(r"\((?P<node>[^)]*)\)", text.strip())
    if match is None:
        raise ValueError(
            "HumemCypher v0 only supports a single node pattern or one directed edge."
        )
    return match.group("node")


def _split_comma_separated(text: str) -> list[str]:
    items: list[str] = []
    current: list[str] = []
    in_string = False
    escape = False
    paren_depth = 0
    bracket_depth = 0
    brace_depth = 0

    for character in text:
        if escape:
            current.append(character)
            escape = False
            continue

        if character == "\\":
            current.append(character)
            escape = True
            continue

        if character == "'":
            in_string = not in_string
            current.append(character)
            continue

        if not in_string:
            if character == "(":
                paren_depth += 1
            elif character == ")":
                paren_depth -= 1
            elif character == "[":
                bracket_depth += 1
            elif character == "]":
                bracket_depth -= 1
            elif character == "{":
                brace_depth += 1
            elif character == "}":
                brace_depth -= 1

            if min(paren_depth, bracket_depth, brace_depth) < 0:
                raise ValueError("HumemCypher v0 found an unbalanced pattern clause.")

        if (
            character == ","
            and not in_string
            and paren_depth == 0
            and bracket_depth == 0
            and brace_depth == 0
        ):
            item = "".join(current).strip()
            if not item:
                raise ValueError("HumemCypher v0 does not allow empty list items.")
            items.append(item)
            current = []
            continue

        current.append(character)

    final_item = "".join(current).strip()
    if in_string:
        raise ValueError("HumemCypher v0 found an unterminated string literal.")
    if paren_depth or bracket_depth or brace_depth:
        raise ValueError("HumemCypher v0 found an unbalanced pattern clause.")
    if not final_item:
        raise ValueError("HumemCypher v0 does not allow empty list items.")
    items.append(final_item)
    return items


def _validate_create_relationship_separate_patterns(
    first_node: NodePattern,
    second_node: NodePattern,
    left: NodePattern,
    right: NodePattern,
) -> None:
    if first_node.alias == second_node.alias:
        raise ValueError(
            "HumemCypher v0 CREATE with separate node patterns currently requires "
            "two distinct created node aliases."
        )

    created_nodes = {
        first_node.alias: first_node,
        second_node.alias: second_node,
    }
    if {left.alias, right.alias} != set(created_nodes):
        raise ValueError(
            "HumemCypher v0 CREATE with separate node patterns currently requires "
            "the relationship pattern to reuse exactly those two created aliases."
        )

    for endpoint in (left, right):
        created_node = created_nodes[endpoint.alias]
        if endpoint.label is not None and endpoint.label != created_node.label:
            raise ValueError(
                "HumemCypher v0 CREATE reused-node endpoints must use the same "
                "label as the created node alias."
            )
        if endpoint.properties:
            raise ValueError(
                "HumemCypher v0 CREATE reused-node endpoints in separate-pattern "
                "creates cannot redeclare inline properties."
            )


def _validate_match_create_relationship_endpoints(
    match_node: NodePattern,
    left: NodePattern,
    right: NodePattern,
) -> None:
    if left.alias != match_node.alias and right.alias != match_node.alias:
        raise ValueError(
            "HumemCypher v0 MATCH ... CREATE currently requires the CREATE "
            "relationship pattern to reuse the matched node alias on at least one "
            "endpoint."
        )

    for endpoint in (left, right):
        if endpoint.alias == match_node.alias:
            if endpoint.label is not None and endpoint.label != match_node.label:
                raise ValueError(
                    "HumemCypher v0 MATCH ... CREATE reused-node endpoints must use "
                    "the same label as the matched node alias."
                )
            if endpoint.properties:
                raise ValueError(
                    "HumemCypher v0 MATCH ... CREATE reused-node endpoints cannot "
                    "redeclare inline properties for the matched node alias."
                )
            continue

        if endpoint.label is None:
            raise ValueError(
                "HumemCypher v0 MATCH ... CREATE new endpoint nodes currently "
                "require a label unless they reuse the matched node alias."
            )


def _validate_match_merge_relationship_endpoints(
    match_node: NodePattern,
    left: NodePattern,
    right: NodePattern,
) -> None:
    if left.alias != match_node.alias and right.alias != match_node.alias:
        raise ValueError(
            "HumemCypher v0 MATCH ... MERGE with one matched node pattern currently "
            "requires the MERGE relationship pattern to reuse the matched node alias on at least one endpoint."
        )

    for endpoint in (left, right):
        if endpoint.alias == match_node.alias:
            if endpoint.label is not None and endpoint.label != match_node.label:
                raise ValueError(
                    "HumemCypher v0 MATCH ... MERGE reused-node endpoints must use "
                    "the same label as the matched node alias."
                )
            if endpoint.properties:
                raise ValueError(
                    "HumemCypher v0 MATCH ... MERGE reused-node endpoints cannot "
                    "redeclare inline properties for the matched node alias."
                )
            continue

        if endpoint.label is None:
            raise ValueError(
                "HumemCypher v0 MATCH ... MERGE new endpoint nodes currently "
                "require a label unless they reuse the matched node alias."
            )


def _validate_match_create_relationship_between_nodes_endpoints(
    left_match: NodePattern,
    right_match: NodePattern,
    left: NodePattern,
    right: NodePattern,
) -> None:
    if left_match.alias == right_match.alias:
        raise ValueError(
            "HumemCypher v0 MATCH ... CREATE with two matched node patterns "
            "currently requires two distinct matched aliases."
        )

    matched_aliases = {left_match.alias, right_match.alias}
    endpoint_aliases = {left.alias, right.alias}
    if endpoint_aliases != matched_aliases:
        raise ValueError(
            "HumemCypher v0 MATCH ... CREATE with two matched node patterns "
            "currently requires the CREATE relationship endpoints to reuse those "
            "two matched aliases exactly."
        )

    for matched_node, endpoint in ((left_match, left), (right_match, right)):
        if endpoint.alias != matched_node.alias:
            continue
        if endpoint.label is not None and endpoint.label != matched_node.label:
            raise ValueError(
                "HumemCypher v0 MATCH ... CREATE reused-node endpoints must use "
                "the same label as the matched node alias."
            )
        if endpoint.properties:
            raise ValueError(
                "HumemCypher v0 MATCH ... CREATE reused-node endpoints cannot "
                "redeclare inline properties for matched node aliases."
            )


def _split_outside_string(text: str, delimiter: str) -> tuple[str, str]:
    in_string = False
    escape = False

    for index, character in enumerate(text):
        if escape:
            escape = False
            continue

        if character == "\\":
            escape = True
            continue

        if character == "'":
            in_string = not in_string
            continue

        if character == delimiter and not in_string:
            return text[:index], text[index + 1:]

    raise ValueError(f"HumemCypher v0 expected {delimiter!r} in {text!r}.")


def _split_predicate_comparison(
    text: str,
) -> tuple[
    str,
    Literal[
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
    str,
]:
    in_string = False
    escape = False

    for index, character in enumerate(text):
        if escape:
            escape = False
            continue

        if character == "\\":
            escape = True
            continue

        if character == "'":
            in_string = not in_string
            continue

        if in_string:
            continue

        remaining = text[index:].upper()
        if remaining.startswith(" IS NOT NULL"):
            return text[:index], "IS NOT NULL", text[index + len(" IS NOT NULL"):]
        if remaining.startswith(" IS NULL"):
            return text[:index], "IS NULL", text[index + len(" IS NULL"):]
        if remaining.startswith(" STARTS WITH "):
            return (
                text[:index],
                "STARTS WITH",
                text[index + len(" STARTS WITH "):],
            )
        if remaining.startswith(" ENDS WITH "):
            return text[:index], "ENDS WITH", text[index + len(" ENDS WITH "):]
        if remaining.startswith(" CONTAINS "):
            return text[:index], "CONTAINS", text[index + len(" CONTAINS "):]

        if text.startswith("<=", index) or text.startswith(">=", index):
            operator = cast(Literal["<=", ">="], text[index:index + 2])
            return text[:index], operator, text[index + 2:]
        if character in "=<>":
            operator = cast(Literal["=", "<", ">"], character)
            return text[:index], operator, text[index + 1:]

    raise ValueError(f"HumemCypher v0 expected a comparison operator in {text!r}.")


def _find_top_level_keyword(text: str, keyword: str, *, start: int = 0) -> int:
    in_string = False
    escape = False
    depth = 0
    target = keyword.upper()
    length = len(keyword)

    for index in range(start, len(text)):
        character = text[index]
        if escape:
            escape = False
            continue
        if character == "\\":
            escape = True
            continue
        if character == "'":
            in_string = not in_string
            continue
        if in_string:
            continue
        if character == "(":
            depth += 1
            continue
        if character == ")":
            depth = max(depth - 1, 0)
            continue
        if depth != 0:
            continue
        if text[index:index + length].upper() != target:
            continue
        before = text[index - 1] if index > 0 else " "
        after_index = index + length
        after = text[after_index] if after_index < len(text) else " "
        if (before.isalnum() or before == "_") or (after.isalnum() or after == "_"):
            continue
        return index
    return -1


def _parse_case_result_item(text: str) -> ReturnItem:
    expression_text = text.strip()
    field_match = _RETURN_ITEM_RE.fullmatch(expression_text)
    if field_match is not None:
        return ReturnItem(
            alias=field_match.group("alias"),
            field=field_match.group("field"),
            kind="field",
        )

    id_match = re.fullmatch(
        rf"id\s*\(\s*(?P<alias>{_IDENTIFIER})\s*\)",
        expression_text,
        flags=re.IGNORECASE,
    )
    if id_match is not None:
        return ReturnItem(alias=id_match.group("alias"), kind="id")

    type_match = re.fullmatch(
        rf"type\s*\(\s*(?P<alias>{_IDENTIFIER})\s*\)",
        expression_text,
        flags=re.IGNORECASE,
    )
    if type_match is not None:
        return ReturnItem(alias=type_match.group("alias"), kind="type")

    size_match = re.fullmatch(
        r"size\s*\(\s*(?P<expr>.+?)\s*\)",
        expression_text,
        flags=re.IGNORECASE,
    )
    if size_match is not None:
        size_expr = size_match.group("expr").strip()
        inner_id_match = re.fullmatch(
            rf"id\s*\(\s*(?P<alias>{_IDENTIFIER})\s*\)",
            size_expr,
            flags=re.IGNORECASE,
        )
        if inner_id_match is not None:
            return ReturnItem(alias=inner_id_match.group("alias"), field="id", kind="size")
        inner_type_match = re.fullmatch(
            rf"type\s*\(\s*(?P<alias>{_IDENTIFIER})\s*\)",
            size_expr,
            flags=re.IGNORECASE,
        )
        if inner_type_match is not None:
            return ReturnItem(alias=inner_type_match.group("alias"), field="type", kind="size")
        inner_field_match = _RETURN_ITEM_RE.fullmatch(size_expr)
        if inner_field_match is not None:
            return ReturnItem(
                alias=inner_field_match.group("alias"),
                field=inner_field_match.group("field"),
                kind="size",
            )
        return ReturnItem(alias="__case__", kind="size", value=_parse_literal(size_expr))

    try:
        literal_value = _parse_literal(expression_text)
    except ValueError as exc:
        raise ValueError(
            "HumemCypher v0 searched CASE results currently support only admitted field projections, admitted id/type/size outputs, or scalar literal/parameter inputs."
        ) from exc
    return ReturnItem(alias="__case__", kind="scalar", value=literal_value)


def _parse_case_condition_item(text: str) -> ReturnItem:
    left_text, operator, value_text = _split_predicate_comparison(text.strip())
    left_text = left_text.strip()
    value_text = value_text.strip()
    if operator in {"IS NULL", "IS NOT NULL"}:
        if value_text:
            raise ValueError(
                "HumemCypher v0 searched CASE null predicates cannot include a trailing literal value."
            )
        predicate_value: object | None = None
    else:
        predicate_value = _parse_literal(value_text)

    size_match = re.fullmatch(
        r"size\s*\(\s*(?P<expr>.+?)\s*\)",
        left_text,
        flags=re.IGNORECASE,
    )
    if size_match is not None:
        size_expr = size_match.group("expr").strip()
        inner_id_match = re.fullmatch(
            rf"id\s*\(\s*(?P<alias>{_IDENTIFIER})\s*\)",
            size_expr,
            flags=re.IGNORECASE,
        )
        if inner_id_match is not None:
            return ReturnItem(
                alias=inner_id_match.group("alias"),
                field=f"{_SIZE_PREDICATE_FIELD_PREFIX}id",
                kind="predicate",
                operator=operator,
                value=predicate_value,
            )
        inner_type_match = re.fullmatch(
            rf"type\s*\(\s*(?P<alias>{_IDENTIFIER})\s*\)",
            size_expr,
            flags=re.IGNORECASE,
        )
        if inner_type_match is not None:
            return ReturnItem(
                alias=inner_type_match.group("alias"),
                field=f"{_SIZE_PREDICATE_FIELD_PREFIX}type",
                kind="predicate",
                operator=operator,
                value=predicate_value,
            )
        inner_field_match = _RETURN_ITEM_RE.fullmatch(size_expr)
        if inner_field_match is not None:
            return ReturnItem(
                alias=inner_field_match.group("alias"),
                field=f"{_SIZE_PREDICATE_FIELD_PREFIX}{inner_field_match.group('field')}",
                kind="predicate",
                operator=operator,
                value=predicate_value,
            )
        raise ValueError(
            "HumemCypher v0 searched CASE WHEN conditions currently support size(...) only over admitted field projections or admitted id/type outputs."
        )

    id_match = re.fullmatch(
        rf"id\s*\(\s*(?P<alias>{_IDENTIFIER})\s*\)",
        left_text,
        flags=re.IGNORECASE,
    )
    if id_match is not None:
        return ReturnItem(
            alias=id_match.group("alias"),
            field="id",
            kind="predicate",
            operator=operator,
            value=predicate_value,
        )

    type_match = re.fullmatch(
        rf"type\s*\(\s*(?P<alias>{_IDENTIFIER})\s*\)",
        left_text,
        flags=re.IGNORECASE,
    )
    if type_match is not None:
        return ReturnItem(
            alias=type_match.group("alias"),
            field="type",
            kind="predicate",
            operator=operator,
            value=predicate_value,
        )

    field_match = _RETURN_ITEM_RE.fullmatch(left_text)
    if field_match is None:
        raise ValueError(
            "HumemCypher v0 searched CASE WHEN conditions currently support only admitted field, id(...), type(...), or size(...) predicate surfaces."
        )
    return ReturnItem(
        alias=field_match.group("alias"),
        field=field_match.group("field"),
        kind="predicate",
        operator=operator,
        value=predicate_value,
    )


def _parse_case_expression(text: str) -> CaseSpec | None:
    expression_text = text.strip()
    if re.match(r"case\b", expression_text, flags=re.IGNORECASE) is None:
        return None
    if re.fullmatch(r"case\s+.+\s+end", expression_text, flags=re.IGNORECASE) is None:
        raise ValueError(
            "HumemCypher v0 currently supports searched CASE expressions only in the form CASE WHEN ... THEN ... [WHEN ... THEN ...]* ELSE ... END."
        )

    inner = expression_text[4:-3].strip()
    if re.match(r"when\b", inner, flags=re.IGNORECASE) is None:
        raise ValueError(
            "HumemCypher v0 currently supports only searched CASE expressions beginning with CASE WHEN ... ."
        )

    when_items: list[CaseWhen] = []
    cursor = 0
    while True:
        when_index = _find_top_level_keyword(inner, "WHEN", start=cursor)
        if when_index == -1:
            break
        then_index = _find_top_level_keyword(inner, "THEN", start=when_index + 4)
        if then_index == -1:
            raise ValueError(
                "HumemCypher v0 searched CASE expressions require THEN after every WHEN condition."
            )
        next_when = _find_top_level_keyword(inner, "WHEN", start=then_index + 4)
        else_index = _find_top_level_keyword(inner, "ELSE", start=then_index + 4)
        if else_index == -1 and next_when == -1:
            raise ValueError(
                "HumemCypher v0 searched CASE expressions currently require an ELSE branch."
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
            CaseWhen(
                condition=_parse_case_condition_item(condition_text),
                result=_parse_case_result_item(result_text),
            )
        )

        if final_else_text is not None:
            if not final_else_text:
                raise ValueError(
                    "HumemCypher v0 searched CASE expressions currently require a non-empty ELSE result."
                )
            return CaseSpec(
                when_items=tuple(when_items),
                else_item=_parse_case_result_item(final_else_text),
            )

    raise ValueError(
        "HumemCypher v0 searched CASE expressions currently require at least one WHEN ... THEN ... arm and a final ELSE branch."
    )


def _parse_return_items(text: str) -> tuple[ReturnItem, ...]:
    from ._normalize_return_items import _parse_return_items as _parse_return_items_impl

    return _parse_return_items_impl(text)
