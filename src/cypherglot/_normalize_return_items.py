from __future__ import annotations

import re
from typing import Literal, cast

from ._normalize_support import (
    _IDENTIFIER,
    _NODE_FUNCTION_RETURN_ITEM_RE,
    _RETURN_ITEM_RE,
    _SCALAR_ITEM_RE,
    _SIZE_PREDICATE_FIELD_PREFIX,
    CypherValue,
    ReturnItem,
    _parse_case_expression,
    _parse_literal,
    _split_comma_separated,
    _split_predicate_comparison,
)


def _parse_return_items(text: str) -> tuple[ReturnItem, ...]:
    items: list[ReturnItem] = []

    for raw_item in _split_comma_separated(text):
        item_text = raw_item.strip()
        alias_parts = re.split(r"\s+AS\s+", item_text, flags=re.IGNORECASE)
        if len(alias_parts) > 2:
            raise ValueError(
                "HumemCypher v0 RETURN items may use at most one AS alias."
            )

        expression_text = alias_parts[0].strip()
        output_alias = alias_parts[1].strip() if len(alias_parts) == 2 else None
        if output_alias is not None and _SCALAR_ITEM_RE.fullmatch(output_alias) is None:
            raise ValueError(
                "HumemCypher v0 RETURN item aliases must be bare identifiers."
            )

        case_spec = _parse_case_expression(expression_text)
        if case_spec is not None:
            if output_alias is None:
                raise ValueError(
                    "HumemCypher v0 RETURN searched CASE items currently require an explicit AS alias."
                )
            items.append(
                ReturnItem(
                    alias=output_alias,
                    kind="case",
                    value=case_spec,
                    output_alias=output_alias,
                )
            )
            continue

        count_match = re.fullmatch(
            rf"count\s*\(\s*(?P<alias>\*|{_IDENTIFIER})\s*\)",
            expression_text,
            flags=re.IGNORECASE,
        )
        if count_match is not None:
            items.append(
                ReturnItem(
                    count_match.group("alias"),
                    kind="count",
                    output_alias=output_alias,
                )
            )
            continue

        aggregate_match = re.fullmatch(
            rf"(?P<func>sum|avg|min|max)\s*\(\s*(?P<alias>{_IDENTIFIER})\.(?P<field>{_IDENTIFIER})\s*\)",
            expression_text,
            flags=re.IGNORECASE,
        )
        if aggregate_match is not None:
            items.append(
                ReturnItem(
                    alias=aggregate_match.group("alias"),
                    field=aggregate_match.group("field"),
                    kind=cast(
                        Literal["sum", "avg", "min", "max"],
                        aggregate_match.group("func").lower(),
                    ),
                    output_alias=output_alias,
                )
            )
            continue

        id_match = re.fullmatch(
            rf"id\s*\(\s*(?P<alias>{_IDENTIFIER})\s*\)",
            expression_text,
            flags=re.IGNORECASE,
        )
        if id_match is not None:
            items.append(
                ReturnItem(
                    alias=id_match.group("alias"),
                    kind="id",
                    output_alias=output_alias,
                )
            )
            continue

        type_match = re.fullmatch(
            rf"type\s*\(\s*(?P<alias>{_IDENTIFIER})\s*\)",
            expression_text,
            flags=re.IGNORECASE,
        )
        if type_match is not None:
            items.append(
                ReturnItem(
                    alias=type_match.group("alias"),
                    kind="type",
                    output_alias=output_alias,
                )
            )
            continue

        properties_match = re.fullmatch(
            rf"properties\s*\(\s*(?P<alias>{_IDENTIFIER})\s*\)",
            expression_text,
            flags=re.IGNORECASE,
        )
        if properties_match is not None:
            items.append(
                ReturnItem(
                    alias=properties_match.group("alias"),
                    kind="properties",
                    output_alias=output_alias,
                )
            )
            continue

        labels_match = re.fullmatch(
            rf"labels\s*\(\s*(?P<alias>{_IDENTIFIER})\s*\)",
            expression_text,
            flags=re.IGNORECASE,
        )
        if labels_match is not None:
            items.append(
                ReturnItem(
                    alias=labels_match.group("alias"),
                    kind="labels",
                    output_alias=output_alias,
                )
            )
            continue

        keys_match = re.fullmatch(
            rf"keys\s*\(\s*(?P<alias>{_IDENTIFIER})\s*\)",
            expression_text,
            flags=re.IGNORECASE,
        )
        if keys_match is not None:
            items.append(
                ReturnItem(
                    alias=keys_match.group("alias"),
                    kind="keys",
                    output_alias=output_alias,
                )
            )
            continue

        node_function_match = _NODE_FUNCTION_RETURN_ITEM_RE.fullmatch(expression_text)
        if node_function_match is not None:
            function_name = node_function_match.group("function").lower()
            items.append(
                ReturnItem(
                    alias=node_function_match.group("alias"),
                    field=node_function_match.group("field"),
                    kind="start_node" if function_name == "startnode" else "end_node",
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
                id_match = re.fullmatch(
                    rf"id\s*\(\s*(?P<alias>{_IDENTIFIER})\s*\)",
                    size_expr,
                    flags=re.IGNORECASE,
                )
                if id_match is not None:
                    items.append(
                        ReturnItem(
                            alias=id_match.group("alias"),
                            field="id",
                            kind="size",
                        )
                    )
                    continue
                type_match = re.fullmatch(
                    rf"type\s*\(\s*(?P<alias>{_IDENTIFIER})\s*\)",
                    size_expr,
                    flags=re.IGNORECASE,
                )
                if type_match is not None:
                    items.append(
                        ReturnItem(
                            alias=type_match.group("alias"),
                            field="type",
                            kind="size",
                        )
                    )
                    continue
                field_match = _RETURN_ITEM_RE.fullmatch(size_expr)
                if field_match is not None:
                    items.append(
                        ReturnItem(
                            alias=field_match.group("alias"),
                            field=field_match.group("field"),
                            kind="size",
                        )
                    )
                    continue

            unary_field_match = re.fullmatch(
                rf"(?P<func>lower|upper|trim|ltrim|rtrim|reverse|abs|sign|round|ceil|floor|sqrt|exp|sin|cos|tan|asin|acos|atan|ln|log|log10|radians|degrees|tostring|tointeger|tofloat|toboolean)\s*\(\s*(?P<alias>{_IDENTIFIER})\.(?P<field>{_IDENTIFIER})\s*\)",
                expression_text,
                flags=re.IGNORECASE,
            )
            if unary_field_match is not None:
                function_kind = unary_field_match.group("func").lower()
                items.append(
                    ReturnItem(
                        alias=unary_field_match.group("alias"),
                        field=unary_field_match.group("field"),
                        kind=cast(
                            Literal[
                                "lower",
                                "upper",
                                "trim",
                                "ltrim",
                                "rtrim",
                                "reverse",
                                "abs",
                                "sign",
                                "round",
                                "ceil",
                                "floor",
                                "sqrt",
                                "exp",
                                "sin",
                                "cos",
                                "tan",
                                "asin",
                                "acos",
                                "atan",
                                "ln",
                                "log",
                                "log10",
                                "radians",
                                "degrees",
                                "to_string",
                                "to_integer",
                                "to_float",
                                "to_boolean",
                            ],
                            {
                                "tostring": "to_string",
                                "tointeger": "to_integer",
                                "tofloat": "to_float",
                                "toboolean": "to_boolean",
                            }.get(function_kind, function_kind),
                        ),
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
                    "HumemCypher v0 RETURN size(...) items currently require an explicit AS alias."
                )
            size_expr = size_match.group("expr").strip()
            id_match = re.fullmatch(
                rf"id\s*\(\s*(?P<alias>{_IDENTIFIER})\s*\)",
                size_expr,
                flags=re.IGNORECASE,
            )
            if id_match is not None:
                items.append(
                    ReturnItem(
                        alias=id_match.group("alias"),
                        field="id",
                        kind="size",
                        output_alias=output_alias,
                    )
                )
                continue
            type_match = re.fullmatch(
                rf"type\s*\(\s*(?P<alias>{_IDENTIFIER})\s*\)",
                size_expr,
                flags=re.IGNORECASE,
            )
            if type_match is not None:
                items.append(
                    ReturnItem(
                        alias=type_match.group("alias"),
                        field="type",
                        kind="size",
                        output_alias=output_alias,
                    )
                )
                continue
            field_match = _RETURN_ITEM_RE.fullmatch(size_expr)
            if field_match is not None:
                items.append(
                    ReturnItem(
                        alias=field_match.group("alias"),
                        field=field_match.group("field"),
                        kind="size",
                        output_alias=output_alias,
                    )
                )
                continue
            items.append(
                ReturnItem(
                    alias=output_alias,
                    kind="size",
                    value=_parse_literal(size_expr),
                    output_alias=output_alias,
                )
            )
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
                    "HumemCypher v0 RETURN coalesce(...) currently requires exactly two arguments."
                )
            primary_expr, fallback_expr = args
            primary_match = _RETURN_ITEM_RE.fullmatch(primary_expr)
            if primary_match is None:
                raise ValueError(
                    "HumemCypher v0 RETURN coalesce(...) currently requires alias.field as the primary argument and literal_or_parameter as the fallback argument."
                )
            items.append(
                ReturnItem(
                    alias=primary_match.group("alias"),
                    field=primary_match.group("field"),
                    kind="coalesce",
                    value=_parse_literal(fallback_expr),
                    output_alias=output_alias,
                )
            )
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
                    "HumemCypher v0 RETURN replace(...) currently requires exactly three arguments."
                )
            primary_expr, search_expr, replace_expr = args
            search_value = _parse_literal(search_expr)
            replace_value = _parse_literal(replace_expr)
            field_match = _RETURN_ITEM_RE.fullmatch(primary_expr)
            if field_match is not None:
                items.append(
                    ReturnItem(
                        alias=field_match.group("alias"),
                        field=field_match.group("field"),
                        kind="replace",
                        search_value=search_value,
                        replace_value=replace_value,
                        output_alias=output_alias,
                    )
                )
                continue
            items.append(
                ReturnItem(
                    alias=output_alias,
                    kind="replace",
                    value=_parse_literal(primary_expr),
                    search_value=search_value,
                    replace_value=replace_value,
                    output_alias=output_alias,
                )
            )
            continue

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
                    "HumemCypher v0 RETURN left(...) and right(...) currently require exactly two arguments."
                )
            primary_expr, length_expr = args
            length_value = _parse_literal(length_expr)
            field_match = _RETURN_ITEM_RE.fullmatch(primary_expr)
            if field_match is not None:
                items.append(
                    ReturnItem(
                        alias=field_match.group("alias"),
                        field=field_match.group("field"),
                        kind=function_kind,
                        length_value=length_value,
                        output_alias=output_alias,
                    )
                )
                continue
            items.append(
                ReturnItem(
                    alias=output_alias,
                    kind=function_kind,
                    value=_parse_literal(primary_expr),
                    length_value=length_value,
                    output_alias=output_alias,
                )
            )
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
                    "HumemCypher v0 RETURN split(...) currently requires exactly two arguments."
                )
            primary_expr, delimiter_expr = args
            delimiter_value = _parse_literal(delimiter_expr)
            field_match = _RETURN_ITEM_RE.fullmatch(primary_expr)
            if field_match is not None:
                items.append(
                    ReturnItem(
                        alias=field_match.group("alias"),
                        field=field_match.group("field"),
                        kind="split",
                        delimiter_value=delimiter_value,
                        output_alias=output_alias,
                    )
                )
                continue
            items.append(
                ReturnItem(
                    alias=output_alias,
                    kind="split",
                    value=_parse_literal(primary_expr),
                    delimiter_value=delimiter_value,
                    output_alias=output_alias,
                )
            )
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
                    "HumemCypher v0 RETURN substring(...) currently requires exactly two or three arguments."
                )
            primary_expr, start_expr = args[:2]
            start_value = _parse_literal(start_expr)
            length_value = _parse_literal(args[2]) if len(args) == 3 else None
            field_match = _RETURN_ITEM_RE.fullmatch(primary_expr)
            if field_match is not None:
                items.append(
                    ReturnItem(
                        alias=field_match.group("alias"),
                        field=field_match.group("field"),
                        kind="substring",
                        start_value=start_value,
                        length_value=length_value,
                        output_alias=output_alias,
                    )
                )
                continue
            items.append(
                ReturnItem(
                    alias=output_alias,
                    kind="substring",
                    value=_parse_literal(primary_expr),
                    start_value=start_value,
                    length_value=length_value,
                    output_alias=output_alias,
                )
            )
            continue

        abs_match = re.fullmatch(
            r"abs\s*\(\s*(?P<expr>.+?)\s*\)",
            expression_text,
            flags=re.IGNORECASE,
        )
        if abs_match is not None:
            if output_alias is None:
                raise ValueError(
                    "HumemCypher v0 RETURN abs(...) items currently require an explicit AS alias."
                )
            abs_expr = abs_match.group("expr").strip()
            field_match = _RETURN_ITEM_RE.fullmatch(abs_expr)
            if field_match is not None:
                items.append(
                    ReturnItem(
                        alias=field_match.group("alias"),
                        field=field_match.group("field"),
                        kind="abs",
                        output_alias=output_alias,
                    )
                )
                continue
            items.append(
                ReturnItem(
                    alias=output_alias,
                    kind="abs",
                    value=_parse_literal(abs_expr),
                    output_alias=output_alias,
                )
            )
            continue

        sign_match = re.fullmatch(
            r"sign\s*\(\s*(?P<expr>.+?)\s*\)",
            expression_text,
            flags=re.IGNORECASE,
        )
        if sign_match is not None:
            if output_alias is None:
                raise ValueError(
                    "HumemCypher v0 RETURN sign(...) items currently require an explicit AS alias."
                )
            sign_expr = sign_match.group("expr").strip()
            field_match = _RETURN_ITEM_RE.fullmatch(sign_expr)
            if field_match is not None:
                items.append(
                    ReturnItem(
                        alias=field_match.group("alias"),
                        field=field_match.group("field"),
                        kind="sign",
                        output_alias=output_alias,
                    )
                )
                continue
            items.append(
                ReturnItem(
                    alias=output_alias,
                    kind="sign",
                    value=_parse_literal(sign_expr),
                    output_alias=output_alias,
                )
            )
            continue

        round_match = re.fullmatch(
            r"round\s*\(\s*(?P<expr>.+?)\s*\)",
            expression_text,
            flags=re.IGNORECASE,
        )
        if round_match is not None:
            if output_alias is None:
                raise ValueError(
                    "HumemCypher v0 RETURN round(...) items currently require an explicit AS alias."
                )
            round_expr = round_match.group("expr").strip()
            field_match = _RETURN_ITEM_RE.fullmatch(round_expr)
            if field_match is not None:
                items.append(
                    ReturnItem(
                        alias=field_match.group("alias"),
                        field=field_match.group("field"),
                        kind="round",
                        output_alias=output_alias,
                    )
                )
                continue
            items.append(
                ReturnItem(
                    alias=output_alias,
                    kind="round",
                    value=_parse_literal(round_expr),
                    output_alias=output_alias,
                )
            )
            continue

        floor_match = re.fullmatch(
            r"floor\s*\(\s*(?P<expr>.+?)\s*\)",
            expression_text,
            flags=re.IGNORECASE,
        )
        if floor_match is not None:
            if output_alias is None:
                raise ValueError(
                    "HumemCypher v0 RETURN floor(...) items currently require an explicit AS alias."
                )
            floor_expr = floor_match.group("expr").strip()
            field_match = _RETURN_ITEM_RE.fullmatch(floor_expr)
            if field_match is not None:
                items.append(
                    ReturnItem(
                        alias=field_match.group("alias"),
                        field=field_match.group("field"),
                        kind="floor",
                        output_alias=output_alias,
                    )
                )
                continue
            items.append(
                ReturnItem(
                    alias=output_alias,
                    kind="floor",
                    value=_parse_literal(floor_expr),
                    output_alias=output_alias,
                )
            )
            continue

        ceil_match = re.fullmatch(
            r"ceil\s*\(\s*(?P<expr>.+?)\s*\)",
            expression_text,
            flags=re.IGNORECASE,
        )
        if ceil_match is not None:
            if output_alias is None:
                raise ValueError(
                    "HumemCypher v0 RETURN ceil(...) items currently require an explicit AS alias."
                )
            ceil_expr = ceil_match.group("expr").strip()
            field_match = _RETURN_ITEM_RE.fullmatch(ceil_expr)
            if field_match is not None:
                items.append(
                    ReturnItem(
                        alias=field_match.group("alias"),
                        field=field_match.group("field"),
                        kind="ceil",
                        output_alias=output_alias,
                    )
                )
                continue
            items.append(
                ReturnItem(
                    alias=output_alias,
                    kind="ceil",
                    value=_parse_literal(ceil_expr),
                    output_alias=output_alias,
                )
            )
            continue

        sqrt_match = re.fullmatch(
            r"sqrt\s*\(\s*(?P<expr>.+?)\s*\)",
            expression_text,
            flags=re.IGNORECASE,
        )
        if sqrt_match is not None:
            if output_alias is None:
                raise ValueError(
                    "HumemCypher v0 RETURN sqrt(...) items currently require an explicit AS alias."
                )
            sqrt_expr = sqrt_match.group("expr").strip()
            field_match = _RETURN_ITEM_RE.fullmatch(sqrt_expr)
            if field_match is not None:
                items.append(
                    ReturnItem(
                        alias=field_match.group("alias"),
                        field=field_match.group("field"),
                        kind="sqrt",
                        output_alias=output_alias,
                    )
                )
                continue
            items.append(
                ReturnItem(
                    alias=output_alias,
                    kind="sqrt",
                    value=_parse_literal(sqrt_expr),
                    output_alias=output_alias,
                )
            )
            continue

        exp_match = re.fullmatch(
            r"exp\s*\(\s*(?P<expr>.+?)\s*\)",
            expression_text,
            flags=re.IGNORECASE,
        )
        if exp_match is not None:
            if output_alias is None:
                raise ValueError(
                    "HumemCypher v0 RETURN exp(...) items currently require an explicit AS alias."
                )
            exp_expr = exp_match.group("expr").strip()
            field_match = _RETURN_ITEM_RE.fullmatch(exp_expr)
            if field_match is not None:
                items.append(
                    ReturnItem(
                        alias=field_match.group("alias"),
                        field=field_match.group("field"),
                        kind="exp",
                        output_alias=output_alias,
                    )
                )
                continue
            items.append(
                ReturnItem(
                    alias=output_alias,
                    kind="exp",
                    value=_parse_literal(exp_expr),
                    output_alias=output_alias,
                )
            )
            continue

        sin_match = re.fullmatch(
            r"sin\s*\(\s*(?P<expr>.+?)\s*\)",
            expression_text,
            flags=re.IGNORECASE,
        )
        if sin_match is not None:
            if output_alias is None:
                raise ValueError(
                    "HumemCypher v0 RETURN sin(...) items currently require an explicit AS alias."
                )
            sin_expr = sin_match.group("expr").strip()
            field_match = _RETURN_ITEM_RE.fullmatch(sin_expr)
            if field_match is not None:
                items.append(
                    ReturnItem(
                        alias=field_match.group("alias"),
                        field=field_match.group("field"),
                        kind="sin",
                        output_alias=output_alias,
                    )
                )
                continue
            items.append(
                ReturnItem(
                    alias=output_alias,
                    kind="sin",
                    value=_parse_literal(sin_expr),
                    output_alias=output_alias,
                )
            )
            continue

        cos_match = re.fullmatch(
            r"cos\s*\(\s*(?P<expr>.+?)\s*\)",
            expression_text,
            flags=re.IGNORECASE,
        )
        if cos_match is not None:
            if output_alias is None:
                raise ValueError(
                    "HumemCypher v0 RETURN cos(...) items currently require an explicit AS alias."
                )
            cos_expr = cos_match.group("expr").strip()
            field_match = _RETURN_ITEM_RE.fullmatch(cos_expr)
            if field_match is not None:
                items.append(
                    ReturnItem(
                        alias=field_match.group("alias"),
                        field=field_match.group("field"),
                        kind="cos",
                        output_alias=output_alias,
                    )
                )
                continue
            items.append(
                ReturnItem(
                    alias=output_alias,
                    kind="cos",
                    value=_parse_literal(cos_expr),
                    output_alias=output_alias,
                )
            )
            continue

        tan_match = re.fullmatch(
            r"tan\s*\(\s*(?P<expr>.+?)\s*\)",
            expression_text,
            flags=re.IGNORECASE,
        )
        if tan_match is not None:
            if output_alias is None:
                raise ValueError(
                    "HumemCypher v0 RETURN tan(...) items currently require an explicit AS alias."
                )
            tan_expr = tan_match.group("expr").strip()
            field_match = _RETURN_ITEM_RE.fullmatch(tan_expr)
            if field_match is not None:
                items.append(
                    ReturnItem(
                        alias=field_match.group("alias"),
                        field=field_match.group("field"),
                        kind="tan",
                        output_alias=output_alias,
                    )
                )
                continue
            items.append(
                ReturnItem(
                    alias=output_alias,
                    kind="tan",
                    value=_parse_literal(tan_expr),
                    output_alias=output_alias,
                )
            )
            continue

        asin_match = re.fullmatch(
            r"asin\s*\(\s*(?P<expr>.+?)\s*\)",
            expression_text,
            flags=re.IGNORECASE,
        )
        if asin_match is not None:
            if output_alias is None:
                raise ValueError(
                    "HumemCypher v0 RETURN asin(...) items currently require an explicit AS alias."
                )
            asin_expr = asin_match.group("expr").strip()
            field_match = _RETURN_ITEM_RE.fullmatch(asin_expr)
            if field_match is not None:
                items.append(
                    ReturnItem(
                        alias=field_match.group("alias"),
                        field=field_match.group("field"),
                        kind="asin",
                        output_alias=output_alias,
                    )
                )
                continue
            items.append(
                ReturnItem(
                    alias=output_alias,
                    kind="asin",
                    value=_parse_literal(asin_expr),
                    output_alias=output_alias,
                )
            )
            continue

        acos_match = re.fullmatch(
            r"acos\s*\(\s*(?P<expr>.+?)\s*\)",
            expression_text,
            flags=re.IGNORECASE,
        )
        if acos_match is not None:
            if output_alias is None:
                raise ValueError(
                    "HumemCypher v0 RETURN acos(...) items currently require an explicit AS alias."
                )
            acos_expr = acos_match.group("expr").strip()
            field_match = _RETURN_ITEM_RE.fullmatch(acos_expr)
            if field_match is not None:
                items.append(
                    ReturnItem(
                        alias=field_match.group("alias"),
                        field=field_match.group("field"),
                        kind="acos",
                        output_alias=output_alias,
                    )
                )
                continue
            items.append(
                ReturnItem(
                    alias=output_alias,
                    kind="acos",
                    value=_parse_literal(acos_expr),
                    output_alias=output_alias,
                )
            )
            continue

        atan_match = re.fullmatch(
            r"atan\s*\(\s*(?P<expr>.+?)\s*\)",
            expression_text,
            flags=re.IGNORECASE,
        )
        if atan_match is not None:
            if output_alias is None:
                raise ValueError(
                    "HumemCypher v0 RETURN atan(...) items currently require an explicit AS alias."
                )
            atan_expr = atan_match.group("expr").strip()
            field_match = _RETURN_ITEM_RE.fullmatch(atan_expr)
            if field_match is not None:
                items.append(
                    ReturnItem(
                        alias=field_match.group("alias"),
                        field=field_match.group("field"),
                        kind="atan",
                        output_alias=output_alias,
                    )
                )
                continue
            items.append(
                ReturnItem(
                    alias=output_alias,
                    kind="atan",
                    value=_parse_literal(atan_expr),
                    output_alias=output_alias,
                )
            )
            continue

        ln_match = re.fullmatch(
            r"ln\s*\(\s*(?P<expr>.+?)\s*\)",
            expression_text,
            flags=re.IGNORECASE,
        )
        if ln_match is not None:
            if output_alias is None:
                raise ValueError(
                    "HumemCypher v0 RETURN ln(...) items currently require an explicit AS alias."
                )
            ln_expr = ln_match.group("expr").strip()
            field_match = _RETURN_ITEM_RE.fullmatch(ln_expr)
            if field_match is not None:
                items.append(
                    ReturnItem(
                        alias=field_match.group("alias"),
                        field=field_match.group("field"),
                        kind="ln",
                        output_alias=output_alias,
                    )
                )
                continue
            items.append(
                ReturnItem(
                    alias=output_alias,
                    kind="ln",
                    value=_parse_literal(ln_expr),
                    output_alias=output_alias,
                )
            )
            continue

        log_match = re.fullmatch(
            r"log\s*\(\s*(?P<expr>.+?)\s*\)",
            expression_text,
            flags=re.IGNORECASE,
        )
        if log_match is not None:
            if output_alias is None:
                raise ValueError(
                    "HumemCypher v0 RETURN log(...) items currently require an explicit AS alias."
                )
            log_expr = log_match.group("expr").strip()
            field_match = _RETURN_ITEM_RE.fullmatch(log_expr)
            if field_match is not None:
                items.append(
                    ReturnItem(
                        alias=field_match.group("alias"),
                        field=field_match.group("field"),
                        kind="log",
                        output_alias=output_alias,
                    )
                )
                continue
            items.append(
                ReturnItem(
                    alias=output_alias,
                    kind="log",
                    value=_parse_literal(log_expr),
                    output_alias=output_alias,
                )
            )
            continue

        radians_match = re.fullmatch(
            r"radians\s*\(\s*(?P<expr>.+?)\s*\)",
            expression_text,
            flags=re.IGNORECASE,
        )
        if radians_match is not None:
            if output_alias is None:
                raise ValueError(
                    "HumemCypher v0 RETURN radians(...) items currently require an explicit AS alias."
                )
            radians_expr = radians_match.group("expr").strip()
            field_match = _RETURN_ITEM_RE.fullmatch(radians_expr)
            if field_match is not None:
                items.append(
                    ReturnItem(
                        alias=field_match.group("alias"),
                        field=field_match.group("field"),
                        kind="radians",
                        output_alias=output_alias,
                    )
                )
                continue
            items.append(
                ReturnItem(
                    alias=output_alias,
                    kind="radians",
                    value=_parse_literal(radians_expr),
                    output_alias=output_alias,
                )
            )
            continue

        degrees_match = re.fullmatch(
            r"degrees\s*\(\s*(?P<expr>.+?)\s*\)",
            expression_text,
            flags=re.IGNORECASE,
        )
        if degrees_match is not None:
            if output_alias is None:
                raise ValueError(
                    "HumemCypher v0 RETURN degrees(...) items currently require an explicit AS alias."
                )
            degrees_expr = degrees_match.group("expr").strip()
            field_match = _RETURN_ITEM_RE.fullmatch(degrees_expr)
            if field_match is not None:
                items.append(
                    ReturnItem(
                        alias=field_match.group("alias"),
                        field=field_match.group("field"),
                        kind="degrees",
                        output_alias=output_alias,
                    )
                )
                continue
            items.append(
                ReturnItem(
                    alias=output_alias,
                    kind="degrees",
                    value=_parse_literal(degrees_expr),
                    output_alias=output_alias,
                )
            )
            continue

        log10_match = re.fullmatch(
            r"log10\s*\(\s*(?P<expr>.+?)\s*\)",
            expression_text,
            flags=re.IGNORECASE,
        )
        if log10_match is not None:
            if output_alias is None:
                raise ValueError(
                    "HumemCypher v0 RETURN log10(...) items currently require an explicit AS alias."
                )
            log10_expr = log10_match.group("expr").strip()
            field_match = _RETURN_ITEM_RE.fullmatch(log10_expr)
            if field_match is not None:
                items.append(
                    ReturnItem(
                        alias=field_match.group("alias"),
                        field=field_match.group("field"),
                        kind="log10",
                        output_alias=output_alias,
                    )
                )
                continue
            items.append(
                ReturnItem(
                    alias=output_alias,
                    kind="log10",
                    value=_parse_literal(log10_expr),
                    output_alias=output_alias,
                )
            )
            continue

        to_string_match = re.fullmatch(
            r"tostring\s*\(\s*(?P<expr>.+?)\s*\)",
            expression_text,
            flags=re.IGNORECASE,
        )
        if to_string_match is not None:
            if output_alias is None:
                raise ValueError(
                    "HumemCypher v0 RETURN toString(...) items currently require an explicit AS alias."
                )
            to_string_expr = to_string_match.group("expr").strip()
            field_match = _RETURN_ITEM_RE.fullmatch(to_string_expr)
            if field_match is not None:
                items.append(
                    ReturnItem(
                        alias=field_match.group("alias"),
                        field=field_match.group("field"),
                        kind="to_string",
                        output_alias=output_alias,
                    )
                )
                continue
            items.append(
                ReturnItem(
                    alias=output_alias,
                    kind="to_string",
                    value=_parse_literal(to_string_expr),
                    output_alias=output_alias,
                )
            )
            continue

        to_integer_match = re.fullmatch(
            r"tointeger\s*\(\s*(?P<expr>.+?)\s*\)",
            expression_text,
            flags=re.IGNORECASE,
        )
        if to_integer_match is not None:
            if output_alias is None:
                raise ValueError(
                    "HumemCypher v0 RETURN toInteger(...) items currently require an explicit AS alias."
                )
            to_integer_expr = to_integer_match.group("expr").strip()
            field_match = _RETURN_ITEM_RE.fullmatch(to_integer_expr)
            if field_match is not None:
                items.append(
                    ReturnItem(
                        alias=field_match.group("alias"),
                        field=field_match.group("field"),
                        kind="to_integer",
                        output_alias=output_alias,
                    )
                )
                continue
            items.append(
                ReturnItem(
                    alias=output_alias,
                    kind="to_integer",
                    value=_parse_literal(to_integer_expr),
                    output_alias=output_alias,
                )
            )
            continue

        to_float_match = re.fullmatch(
            r"tofloat\s*\(\s*(?P<expr>.+?)\s*\)",
            expression_text,
            flags=re.IGNORECASE,
        )
        if to_float_match is not None:
            if output_alias is None:
                raise ValueError(
                    "HumemCypher v0 RETURN toFloat(...) items currently require an explicit AS alias."
                )
            to_float_expr = to_float_match.group("expr").strip()
            field_match = _RETURN_ITEM_RE.fullmatch(to_float_expr)
            if field_match is not None:
                items.append(
                    ReturnItem(
                        alias=field_match.group("alias"),
                        field=field_match.group("field"),
                        kind="to_float",
                        output_alias=output_alias,
                    )
                )
                continue
            items.append(
                ReturnItem(
                    alias=output_alias,
                    kind="to_float",
                    value=_parse_literal(to_float_expr),
                    output_alias=output_alias,
                )
            )
            continue

        to_boolean_match = re.fullmatch(
            r"toboolean\s*\(\s*(?P<expr>.+?)\s*\)",
            expression_text,
            flags=re.IGNORECASE,
        )
        if to_boolean_match is not None:
            if output_alias is None:
                raise ValueError(
                    "HumemCypher v0 RETURN toBoolean(...) items currently require an explicit AS alias."
                )
            to_boolean_expr = to_boolean_match.group("expr").strip()
            field_match = _RETURN_ITEM_RE.fullmatch(to_boolean_expr)
            if field_match is not None:
                items.append(
                    ReturnItem(
                        alias=field_match.group("alias"),
                        field=field_match.group("field"),
                        kind="to_boolean",
                        output_alias=output_alias,
                    )
                )
                continue
            items.append(
                ReturnItem(
                    alias=output_alias,
                    kind="to_boolean",
                    value=_parse_literal(to_boolean_expr),
                    output_alias=output_alias,
                )
            )
            continue

        unary_string_match = re.fullmatch(
            r"(?P<func>lower|upper|trim|ltrim|rtrim|reverse)\s*\(\s*(?P<expr>.+?)\s*\)",
            expression_text,
            flags=re.IGNORECASE,
        )
        if unary_string_match is not None:
            if output_alias is None:
                raise ValueError(
                    "HumemCypher v0 RETURN lower(...), upper(...), trim(...), ltrim(...), rtrim(...), and reverse(...) items currently require an explicit AS alias."
                )
            function_kind = cast(
                Literal["lower", "upper", "trim", "ltrim", "rtrim", "reverse"],
                unary_string_match.group("func").lower(),
            )
            function_expr = unary_string_match.group("expr").strip()
            field_match = _RETURN_ITEM_RE.fullmatch(function_expr)
            if field_match is not None:
                items.append(
                    ReturnItem(
                        alias=field_match.group("alias"),
                        field=field_match.group("field"),
                        kind=function_kind,
                        output_alias=output_alias,
                    )
                )
                continue
            items.append(
                ReturnItem(
                    alias=output_alias,
                    kind=function_kind,
                    value=_parse_literal(function_expr),
                    output_alias=output_alias,
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
                    "HumemCypher v0 RETURN predicate items currently require an explicit AS alias."
                )
            left_text = left_text.strip()
            parsed_value: CypherValue = None
            if operator not in {"IS NULL", "IS NOT NULL"}:
                parsed_value = _parse_literal(value_text.strip())
            elif value_text.strip():
                raise ValueError(
                    "HumemCypher v0 null predicate RETURN items cannot include a trailing literal value."
                )
            size_match = re.fullmatch(
                rf"size\s*\(\s*(?P<expr>.+?)\s*\)",
                left_text,
                flags=re.IGNORECASE,
            )
            if size_match is not None:
                size_expr = size_match.group("expr").strip()
                id_match = re.fullmatch(
                    rf"id\s*\(\s*(?P<alias>{_IDENTIFIER})\s*\)",
                    size_expr,
                    flags=re.IGNORECASE,
                )
                if id_match is not None:
                    items.append(
                        ReturnItem(
                            alias=id_match.group("alias"),
                            field=f"{_SIZE_PREDICATE_FIELD_PREFIX}id",
                            kind="predicate",
                            operator=operator,
                            value=parsed_value,
                            output_alias=output_alias,
                        )
                    )
                    continue
                type_match = re.fullmatch(
                    rf"type\s*\(\s*(?P<alias>{_IDENTIFIER})\s*\)",
                    size_expr,
                    flags=re.IGNORECASE,
                )
                if type_match is not None:
                    items.append(
                        ReturnItem(
                            alias=type_match.group("alias"),
                            field=f"{_SIZE_PREDICATE_FIELD_PREFIX}type",
                            kind="predicate",
                            operator=operator,
                            value=parsed_value,
                            output_alias=output_alias,
                        )
                    )
                    continue
                field_match = _RETURN_ITEM_RE.fullmatch(size_expr)
                if field_match is not None:
                    items.append(
                        ReturnItem(
                            alias=field_match.group("alias"),
                            field=f"{_SIZE_PREDICATE_FIELD_PREFIX}{field_match.group('field')}",
                            kind="predicate",
                            operator=operator,
                            value=parsed_value,
                            output_alias=output_alias,
                        )
                    )
                    continue
            id_match = re.fullmatch(
                rf"id\s*\(\s*(?P<alias>{_IDENTIFIER})\s*\)",
                left_text,
                flags=re.IGNORECASE,
            )
            if id_match is not None:
                items.append(
                    ReturnItem(
                        alias=id_match.group("alias"),
                        field="id",
                        kind="predicate",
                        operator=operator,
                        value=parsed_value,
                        output_alias=output_alias,
                    )
                )
                continue

            type_match = re.fullmatch(
                rf"type\s*\(\s*(?P<alias>{_IDENTIFIER})\s*\)",
                left_text,
                flags=re.IGNORECASE,
            )
            if type_match is not None:
                items.append(
                    ReturnItem(
                        alias=type_match.group("alias"),
                        field="type",
                        kind="predicate",
                        operator=operator,
                        value=parsed_value,
                        output_alias=output_alias,
                    )
                )
                continue

            field_match = _RETURN_ITEM_RE.fullmatch(left_text)
            if field_match is None:
                raise ValueError(
                    "HumemCypher v0 RETURN predicate items currently require alias.field OP value, id(alias) OP value, type(rel_alias) OP value, or size(admitted_input) OP value shapes."
                )
            items.append(
                ReturnItem(
                    alias=field_match.group("alias"),
                    field=field_match.group("field"),
                    kind="predicate",
                    operator=operator,
                    value=parsed_value,
                    output_alias=output_alias,
                )
            )
            continue

        try:
            scalar_value = _parse_literal(expression_text)
        except ValueError:
            scalar_value = None
        else:
            if output_alias is None:
                raise ValueError(
                    "HumemCypher v0 RETURN scalar literal and parameter items currently require an explicit AS alias."
                )
            items.append(
                ReturnItem(
                    alias=output_alias,
                    kind="scalar",
                    value=scalar_value,
                    output_alias=output_alias,
                )
            )
            continue

        match = _RETURN_ITEM_RE.fullmatch(expression_text)
        if match is not None:
            items.append(
                ReturnItem(
                    match.group("alias"),
                    match.group("field"),
                    kind="field",
                    output_alias=output_alias,
                )
            )
            continue

        scalar_match = _SCALAR_ITEM_RE.fullmatch(expression_text)
        if scalar_match is None:
            raise ValueError(
                "HumemCypher v0 RETURN items must look like alias.field, alias, count(alias) AS output_alias, count(*) AS output_alias, sum(alias.field) AS output_alias, avg(alias.field) AS output_alias, min(alias.field) AS output_alias, max(alias.field) AS output_alias, id(alias) AS output_alias, type(rel_alias) AS output_alias, size(admitted_input) AS output_alias, lower(admitted_input) AS output_alias, upper(admitted_input) AS output_alias, trim(admitted_input) AS output_alias, ltrim(admitted_input) AS output_alias, rtrim(admitted_input) AS output_alias, reverse(admitted_input) AS output_alias, coalesce(admitted_input, literal_or_parameter) AS output_alias, replace(admitted_input, literal_or_parameter, literal_or_parameter) AS output_alias, left(admitted_input, literal_or_parameter) AS output_alias, right(admitted_input, literal_or_parameter) AS output_alias, split(admitted_input, literal_or_parameter) AS output_alias, substring(admitted_input, literal_or_parameter) AS output_alias, substring(admitted_input, literal_or_parameter, literal_or_parameter) AS output_alias, abs(admitted_input) AS output_alias, sign(admitted_input) AS output_alias, round(admitted_input) AS output_alias, ceil(admitted_input) AS output_alias, floor(admitted_input) AS output_alias, toString(admitted_input) AS output_alias, toInteger(admitted_input) AS output_alias, toFloat(admitted_input) AS output_alias, toBoolean(admitted_input) AS output_alias, startNode(rel_alias) AS output_alias, startNode(rel_alias).field AS output_alias, endNode(rel_alias) AS output_alias, endNode(rel_alias).field AS output_alias, predicate admitted_input OP value AS output_alias, or scalar_literal_or_parameter AS output_alias."
            )
        items.append(
            ReturnItem(
                scalar_match.group("alias"),
                kind="entity",
                output_alias=output_alias,
            )
        )

    if not items:
        raise ValueError("HumemCypher v0 RETURN clauses cannot be empty.")

    return tuple(items)