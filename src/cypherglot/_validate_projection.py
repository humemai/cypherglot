from __future__ import annotations

import re

from ._normalize_support import (
    _find_top_level_keyword,
    _parse_boolean_predicate_groups,
    _parse_case_expression,
    _parse_literal,
    _parse_return_items,
    _split_comma_separated,
    _split_predicate_comparison,
    _split_return_clause,
)


_AGGREGATE_RETURN_KINDS = {"count", "sum", "avg", "min", "max"}
_UNARY_FUNCTION_NAMES = {
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
    "tostring",
    "tointeger",
    "tofloat",
    "toboolean",
}


def _split_projection_alias(item_text: str) -> tuple[str, str | None]:
    alias_match = re.fullmatch(
        r"(?P<expr>.+?)\s+as\s+(?P<output>[A-Za-z_][A-Za-z0-9_]*)",
        item_text,
        flags=re.IGNORECASE,
    )
    if alias_match is None:
        return item_text, None
    return alias_match.group("expr").strip(), alias_match.group("output")


def _require_unique_output(
    projected_output_kinds: dict[str, str],
    output_name: str,
    message: str,
) -> None:
    if output_name in projected_output_kinds:
        raise ValueError(message)


def _match_field(expression_text: str):
    return re.fullmatch(
        r"(?P<alias>[A-Za-z_][A-Za-z0-9_]*)\.(?P<field>[A-Za-z_][A-Za-z0-9_]*)",
        expression_text,
    )


def _match_id_alias(expression_text: str) -> str | None:
    match = re.fullmatch(
        r"id\s*\(\s*(?P<alias>[A-Za-z_][A-Za-z0-9_]*)\s*\)",
        expression_text,
        flags=re.IGNORECASE,
    )
    return None if match is None else match.group("alias")


def _match_type_alias(expression_text: str) -> str | None:
    match = re.fullmatch(
        r"type\s*\(\s*(?P<alias>[A-Za-z_][A-Za-z0-9_]*)\s*\)",
        expression_text,
        flags=re.IGNORECASE,
    )
    return None if match is None else match.group("alias")


def _match_size_expression(expression_text: str) -> str | None:
    match = re.fullmatch(
        r"size\s*\(\s*(?P<expr>.+?)\s*\)",
        expression_text,
        flags=re.IGNORECASE,
    )
    return None if match is None else match.group("expr").strip()


def _validate_plain_case_result_item(
    item,
    *,
    allowed_aliases: set[str],
    relationship_aliases: set[str],
) -> None:
    if item.kind in {"field", "id"}:
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


def _validate_plain_return_item(
    item,
    *,
    allowed_aliases: set[str],
    relationship_aliases: set[str],
) -> str:
    if item.kind in _AGGREGATE_RETURN_KINDS:
        if item.kind == "count":
            if item.alias != "*" and item.alias not in allowed_aliases:
                raise ValueError(
                    "CypherGlot currently requires count(...) in the supported read subset to target an admitted binding alias or *."
                )
            return "aggregate"
        if item.alias not in allowed_aliases or item.field is None:
            raise ValueError(
                "CypherGlot currently supports sum(...), avg(...), min(...), and max(...) in the supported read subset only over admitted entity or relationship fields."
            )
        return "aggregate"

    if item.kind in {"field", "entity"}:
        if item.alias not in allowed_aliases:
                raise ValueError(
                    "CypherGlot currently supports RETURN alias.field for admitted entity bindings, RETURN entity_alias for admitted whole-entity bindings in the supported read subset."
                )
        return "entity" if item.kind == "entity" else "scalar"

    if item.kind == "properties":
        if item.alias not in allowed_aliases:
            raise ValueError(
                "CypherGlot currently supports properties(...) in the supported read subset only over admitted entity or relationship bindings."
            )
        return "scalar"

    if item.kind == "keys":
        if item.alias not in allowed_aliases:
            raise ValueError(
                "CypherGlot currently supports keys(...) in the supported read subset only over admitted entity or relationship bindings."
            )
        return "scalar"

    if item.kind == "id":
        if item.alias not in allowed_aliases:
            raise ValueError(
                "CypherGlot currently supports the admitted RETURN projection in the supported read subset only over admitted aliases."
            )
        return "scalar"

    if item.kind == "labels":
        if item.alias not in allowed_aliases or item.alias in relationship_aliases:
            raise ValueError(
                "CypherGlot currently supports labels(...) in the supported read subset only over admitted node bindings."
            )
        return "scalar"

    if item.kind == "type":
        if item.alias not in relationship_aliases:
            raise ValueError(
                "CypherGlot currently supports type(...) in the supported read subset only over admitted relationship bindings."
            )
        return "scalar"

    if item.kind == "start_node":
        if item.alias not in relationship_aliases:
            raise ValueError(
                "CypherGlot currently supports startNode(...) in the supported read subset only over admitted relationship bindings."
            )
        return "entity" if item.field is None else "scalar"

    if item.kind == "end_node":
        if item.alias not in relationship_aliases:
            raise ValueError(
                "CypherGlot currently supports endNode(...) in the supported read subset only over admitted relationship bindings."
            )
        return "entity" if item.field is None else "scalar"

    if item.kind == "size":
        if item.value is not None:
            return "scalar"
        if item.field is None:
            raise ValueError(
                "CypherGlot currently supports size(...) in the supported read subset only over admitted field projections, admitted id/type outputs, or scalar literal/parameter inputs."
            )
        if item.alias not in allowed_aliases:
            raise ValueError(
                "CypherGlot currently supports size(...) in the supported read subset only over admitted field projections, admitted id/type outputs, or scalar literal/parameter inputs."
            )
        if item.field == "type" and item.alias not in relationship_aliases:
            raise ValueError(
                "CypherGlot currently supports size(...) in the supported read subset only over admitted field projections, admitted id/type outputs, or scalar literal/parameter inputs."
            )
        return "scalar"

    if item.kind == "predicate":
        predicate_shape_message = (
            "CypherGlot currently supports predicate RETURN items in the supported read subset only as alias.field OP value, id(alias) OP value, type(rel_alias) OP value, or size(admitted_input) OP value over admitted bindings."
        )
        if item.field is None:
            raise ValueError(predicate_shape_message)
        if item.alias not in allowed_aliases:
            raise ValueError(predicate_shape_message)
        field = item.field or ""
        if field == "type" and item.alias not in relationship_aliases:
            raise ValueError(predicate_shape_message)
        if field.startswith("__size__:"):
            inner_field = field.removeprefix("__size__:")
            if not inner_field:
                raise ValueError(predicate_shape_message)
            if inner_field == "type" and item.alias not in relationship_aliases:
                raise ValueError(predicate_shape_message)
        return "scalar"

    if item.kind == "scalar":
        return "scalar"

    if item.kind == "coalesce":
        if item.alias not in allowed_aliases or item.field is None:
            raise ValueError(
                "CypherGlot currently supports coalesce(...) in the supported read subset only as coalesce(alias.field, literal_or_parameter) over admitted bindings."
            )
        return "scalar"

    if item.kind in {
        "replace",
        "left",
        "right",
        "split",
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
        "substring",
        "lower",
        "upper",
        "trim",
        "ltrim",
        "rtrim",
        "reverse",
    }:
        if item.kind == "replace":
            if item.value is None and item.field is None:
                raise ValueError(
                    "CypherGlot currently supports replace(...) in the supported read subset only as replace(admitted_input, literal_or_parameter, literal_or_parameter)."
                )
            if item.value is None and item.alias not in allowed_aliases:
                raise ValueError(
                    "CypherGlot currently supports replace(...) in the supported read subset only as replace(admitted_input, literal_or_parameter, literal_or_parameter)."
                )
            return "scalar"

        if item.kind in {"left", "right"}:
            if item.value is None and item.field is None:
                raise ValueError(
                    "CypherGlot currently supports left(...) and right(...) in the supported read subset only as function(admitted_input, literal_or_parameter)."
                )
            if item.value is None and item.alias not in allowed_aliases:
                raise ValueError(
                    "CypherGlot currently supports left(...) and right(...) in the supported read subset only as function(admitted_input, literal_or_parameter)."
                )
            return "scalar"

        if item.kind == "split":
            if item.value is None and item.field is None:
                raise ValueError(
                    "CypherGlot currently supports split(...) in the supported read subset only as split(admitted_input, literal_or_parameter)."
                )
            if item.value is None and item.alias not in allowed_aliases:
                raise ValueError(
                    "CypherGlot currently supports split(...) in the supported read subset only as split(admitted_input, literal_or_parameter)."
                )
            return "scalar"

        if item.kind == "substring":
            if item.value is None and item.field is None:
                raise ValueError(
                    "CypherGlot currently supports substring(...) in the supported read subset only as substring(admitted_input, literal_or_parameter) or substring(admitted_input, literal_or_parameter, literal_or_parameter)."
                )
            if item.value is None and item.alias not in allowed_aliases:
                raise ValueError(
                    "CypherGlot currently supports substring(...) in the supported read subset only as substring(admitted_input, literal_or_parameter) or substring(admitted_input, literal_or_parameter, literal_or_parameter)."
                )
            return "scalar"

        if item.kind in {"lower", "upper", "trim", "ltrim", "rtrim", "reverse"}:
            if item.value is None and item.field is None:
                raise ValueError(
                    "CypherGlot currently supports lower(...), upper(...), trim(...), ltrim(...), rtrim(...), and reverse(...) in the supported read subset only over admitted field projections or scalar literal/parameter inputs."
                )
            if item.value is None and item.alias not in allowed_aliases:
                raise ValueError(
                    "CypherGlot currently supports lower(...), upper(...), trim(...), ltrim(...), rtrim(...), and reverse(...) in the supported read subset only over admitted field projections or scalar literal/parameter inputs."
                )
            return "scalar"

        function_name = {
            "to_string": "toString",
            "to_integer": "toInteger",
            "to_float": "toFloat",
            "to_boolean": "toBoolean",
        }.get(item.kind, item.kind)
        if item.value is None and item.field is None:
            raise ValueError(
                f"CypherGlot currently supports {function_name}(...) in the supported read subset only over admitted field projections or scalar literal/parameter inputs."
            )
        if item.value is None and item.alias not in allowed_aliases:
            raise ValueError(
                f"CypherGlot currently supports {function_name}(...) in the supported read subset only over admitted field projections or scalar literal/parameter inputs."
            )
        return "scalar"

    raise ValueError(
        "CypherGlot currently does not admit this RETURN shape in the supported read subset."
    )


def _plain_projection_output_kind(item) -> str:
    if item.kind in _AGGREGATE_RETURN_KINDS:
        return "aggregate"
    if item.kind == "entity":
        return "entity"
    if item.kind in {"start_node", "end_node"} and item.field is None:
        return "entity"
    return "scalar"


def _translate_plain_read_parse_error(message: str, expression_text: str) -> str:
    direct_mappings = {
        "HumemCypher v0 RETURN searched CASE items currently require an explicit AS alias.": (
            "CypherGlot currently requires searched CASE in the supported read subset to use an explicit AS alias."
        ),
        "HumemCypher v0 RETURN size(...) items currently require an explicit AS alias.": (
            "CypherGlot currently requires size(...) in the supported read subset to use an explicit AS alias."
        ),
        "HumemCypher v0 RETURN predicate items currently require an explicit AS alias.": (
            "CypherGlot currently requires predicate RETURN items in the supported read subset to use an explicit AS alias."
        ),
        "HumemCypher v0 RETURN predicate items currently require alias.field OP value, id(alias) OP value, type(rel_alias) OP value, or size(admitted_input) OP value shapes.": (
            "CypherGlot currently supports predicate RETURN items in the supported read subset only as alias.field OP value, id(alias) OP value, type(rel_alias) OP value, or size(admitted_input) OP value over admitted bindings."
        ),
        "HumemCypher v0 RETURN scalar literal and parameter items currently require an explicit AS alias.": (
            "CypherGlot currently requires scalar literal and parameter RETURN items in the supported read subset to use an explicit AS alias."
        ),
        "HumemCypher v0 RETURN coalesce(...) currently requires exactly two arguments.": (
            "CypherGlot currently supports coalesce(...) in the supported read subset only as coalesce(alias.field, literal_or_parameter) over admitted bindings."
        ),
        "HumemCypher v0 RETURN coalesce(...) currently requires alias.field as the primary argument and literal_or_parameter as the fallback argument.": (
            "CypherGlot currently supports coalesce(...) in the supported read subset only as coalesce(alias.field, literal_or_parameter) over admitted bindings."
        ),
        "HumemCypher v0 RETURN replace(...) currently requires exactly three arguments.": (
            "CypherGlot currently supports replace(...) in the supported read subset only as replace(admitted_input, literal_or_parameter, literal_or_parameter)."
        ),
        "HumemCypher v0 RETURN left(...) and right(...) currently require exactly two arguments.": (
            "CypherGlot currently supports left(...) and right(...) in the supported read subset only as function(admitted_input, literal_or_parameter)."
        ),
        "HumemCypher v0 RETURN split(...) currently requires exactly two arguments.": (
            "CypherGlot currently supports split(...) in the supported read subset only as split(admitted_input, literal_or_parameter)."
        ),
        "HumemCypher v0 RETURN substring(...) currently requires exactly two or three arguments.": (
            "CypherGlot currently supports substring(...) in the supported read subset only as substring(admitted_input, literal_or_parameter) or substring(admitted_input, literal_or_parameter, literal_or_parameter)."
        ),
    }
    if message in direct_mappings:
        return direct_mappings[message]

    if message == (
        "HumemCypher v0 RETURN lower(...), upper(...), trim(...), ltrim(...), rtrim(...), and reverse(...) items currently require an explicit AS alias."
    ):
        return (
            "CypherGlot currently requires lower(...), upper(...), trim(...), ltrim(...), rtrim(...), and reverse(...) in the supported read subset to use an explicit AS alias."
        )

    single_alias_match = re.fullmatch(
        r"HumemCypher v0 RETURN (?P<func>abs|sign|round|floor|ceil|sqrt|exp|sin|cos|tan|asin|acos|atan|ln|log|log10|radians|degrees|toString|toInteger|toFloat|toBoolean)\(\.\.\.\) items currently require an explicit AS alias\.",
        message,
    )
    if single_alias_match is not None:
        func_name = single_alias_match.group("func")
        return f"CypherGlot currently requires {func_name}(...) in the supported read subset to use an explicit AS alias."

    if "HumemCypher v0 only supports inline string, integer, float, boolean, and null literals; got" in message:
        if re.fullmatch(r"size\s*\(.+\)", expression_text, flags=re.IGNORECASE):
            return (
                "CypherGlot currently supports size(...) in the supported read subset only over admitted field projections, admitted id/type outputs, or scalar literal/parameter inputs."
            )
        if re.fullmatch(
            r"(?P<func>lower|upper|trim|ltrim|rtrim|reverse)\s*\(.+\)",
            expression_text,
            flags=re.IGNORECASE,
        ):
            return (
                "CypherGlot currently supports lower(...), upper(...), trim(...), ltrim(...), rtrim(...), and reverse(...) in the supported read subset only over admitted field projections or scalar literal/parameter inputs."
            )
        if re.fullmatch(r"substring\s*\(.+\)", expression_text, flags=re.IGNORECASE):
            return (
                "CypherGlot currently supports substring(...) in the supported read subset only as substring(admitted_input, literal_or_parameter) or substring(admitted_input, literal_or_parameter, literal_or_parameter)."
            )
        if re.fullmatch(r"split\s*\(.+\)", expression_text, flags=re.IGNORECASE):
            return (
                "CypherGlot currently supports split(...) in the supported read subset only as split(admitted_input, literal_or_parameter)."
            )
        if re.fullmatch(r"replace\s*\(.+\)", expression_text, flags=re.IGNORECASE):
            return (
                "CypherGlot currently supports replace(...) in the supported read subset only as replace(admitted_input, literal_or_parameter, literal_or_parameter)."
            )
        if re.fullmatch(r"(?P<func>left|right)\s*\(.+\)", expression_text, flags=re.IGNORECASE):
            return (
                "CypherGlot currently supports left(...) and right(...) in the supported read subset only as function(admitted_input, literal_or_parameter)."
            )
        if re.fullmatch(r"coalesce\s*\(.+\)", expression_text, flags=re.IGNORECASE):
            return (
                "CypherGlot currently supports coalesce(...) in the supported read subset only as coalesce(alias.field, literal_or_parameter) over admitted bindings."
            )
        single_func_match = re.fullmatch(
            r"(?P<func>abs|sign|round|floor|ceil|sqrt|exp|sin|cos|tan|asin|acos|atan|ln|log|log10|radians|degrees|toString|toInteger|toFloat|toBoolean)\s*\(.+\)",
            expression_text,
            flags=re.IGNORECASE,
        )
        if single_func_match is not None:
            func_name = single_func_match.group("func")
            return (
                f"CypherGlot currently supports {func_name}(...) in the supported read subset only over admitted field projections or scalar literal/parameter inputs."
            )

    return message


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

    raw_items = [item.strip() for item in _split_comma_separated(return_text)]
    if not raw_items:
        raise ValueError(
            "CypherGlot currently requires at least one RETURN item in the supported read subset."
        )

    projected_output_kinds: dict[str, str] = {}
    exact_projected_expressions: set[str] = set()
    for item_text in raw_items:
        expression_text, output_alias = _split_projection_alias(item_text)
        if _validate_plain_case_expression(
            expression_text,
            output_alias=output_alias,
            allowed_aliases=allowed_aliases,
            relationship_aliases=relationship_aliases,
        ):
            assert output_alias is not None
            _require_unique_output(
                projected_output_kinds,
                output_alias,
                f"CypherGlot currently does not allow duplicate RETURN output alias {output_alias!r} in the supported read subset.",
            )
            projected_output_kinds[output_alias] = "scalar"
            exact_projected_expressions.add(expression_text)
            continue

        try:
            parsed_items = _parse_return_items(item_text)
        except ValueError as exc:
            raise ValueError(
                _translate_plain_read_parse_error(str(exc), expression_text)
            ) from exc
        if len(parsed_items) != 1:
            raise ValueError(
                "CypherGlot currently validates one RETURN item at a time in the supported read subset."
            )
        parsed_item = parsed_items[0]
        _validate_plain_return_item(
            parsed_item,
            allowed_aliases=allowed_aliases,
            relationship_aliases=relationship_aliases,
        )
        output_name = parsed_item.column_name
        _require_unique_output(
            projected_output_kinds,
            output_name,
            f"CypherGlot currently does not allow duplicate RETURN output alias {output_name!r} in the supported read subset.",
        )
        projected_output_kinds[output_name] = _plain_projection_output_kind(parsed_item)
        exact_projected_expressions.add(expression_text)

    aggregate_aliases = {
        output_name
        for output_name, kind in projected_output_kinds.items()
        if kind == "aggregate"
    }

    for item in order_items:
        if item.expression is not None:
            if item.expression in projected_output_kinds:
                continue
            if item.expression in exact_projected_expressions:
                continue
            raise ValueError(
                "CypherGlot currently supports ORDER BY exact projected RETURN expressions, projected aliases, or admitted bindings in the supported read subset."
            )

        if item.field == "__value__" and item.alias in aggregate_aliases:
            continue
        if item.field == "__value__" and item.alias in projected_output_kinds:
            continue
        if item.alias not in allowed_aliases:
            raise ValueError(
                f"CypherGlot currently cannot ORDER BY unknown alias {item.alias!r} in the supported read subset."
            )


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
    field_match = _match_field(text)
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

    size_expr = _match_size_expression(left_text)
    if size_expr is not None:
        field_match = _match_field(size_expr)
        if field_match is not None and binding_kinds.get(field_match.group("alias")) == "entity":
            return
        if binding_kinds.get(size_expr) == "scalar":
            return
        raise ValueError(
            "CypherGlot currently supports searched CASE WHEN conditions in the WITH subset only over admitted entity-field or scalar-binding predicate surfaces."
        )

    field_match = _match_field(left_text)
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


def _validate_with_entity_alias(
    alias: str,
    binding_kinds: dict[str, str],
    message: str,
) -> None:
    if binding_kinds.get(alias) != "entity":
        raise ValueError(message)


def _validate_with_scalar_or_field_input(
    expression_text: str,
    *,
    binding_kinds: dict[str, str],
    allow_literal: bool,
    message: str,
) -> bool:
    if binding_kinds.get(expression_text) == "scalar":
        return True
    field_match = _match_field(expression_text)
    if field_match is not None and binding_kinds.get(field_match.group("alias")) == "entity":
        return True
    if allow_literal:
        try:
            _parse_literal(expression_text)
            return True
        except ValueError:
            pass
    raise ValueError(message)


def _validate_with_size_input(
    expression_text: str,
    *,
    binding_kinds: dict[str, str],
    binding_alias_kinds: dict[str, str],
) -> bool:
    if binding_kinds.get(expression_text) == "scalar":
        return True
    id_alias = _match_id_alias(expression_text)
    if id_alias is not None:
        _validate_with_entity_alias(
            id_alias,
            binding_kinds,
            f"CypherGlot currently supports size(id(...)) in the WITH subset only for admitted entity bindings.",
        )
        return True
    type_alias = _match_type_alias(expression_text)
    if type_alias is not None:
        _validate_with_entity_alias(
            type_alias,
            binding_kinds,
            "CypherGlot currently supports size(type(...)) in the WITH subset only for admitted relationship bindings.",
        )
        if binding_alias_kinds.get(type_alias) != "relationship":
            raise ValueError(
                "CypherGlot currently supports size(type(...)) in the WITH subset only for admitted relationship bindings."
            )
        return True
    field_match = _match_field(expression_text)
    if field_match is not None and binding_kinds.get(field_match.group("alias")) == "entity":
        return True
    try:
        _parse_literal(expression_text)
        return True
    except ValueError as exc:
        raise ValueError(
            "CypherGlot currently supports size(...) in the WITH subset only over admitted entity-field projections, admitted id/type outputs, admitted scalar bindings, or scalar literal/parameter inputs."
        ) from exc


def _validate_with_predicate_target(
    target_text: str,
    *,
    binding_kinds: dict[str, str],
    binding_alias_kinds: dict[str, str],
    generic_message: str,
    where_context: bool,
) -> None:
    if binding_kinds.get(target_text) == "scalar":
        return
    id_alias = _match_id_alias(target_text)
    if id_alias is not None:
        if where_context:
            _validate_with_entity_alias(
                id_alias,
                binding_kinds,
                "CypherGlot currently supports WITH WHERE id(entity_alias) only for entity bindings.",
            )
        elif binding_kinds.get(id_alias) != "entity":
            raise ValueError(generic_message)
        return
    type_alias = _match_type_alias(target_text)
    if type_alias is not None:
        if where_context:
            _validate_with_entity_alias(
                type_alias,
                binding_kinds,
                "CypherGlot currently supports WITH WHERE type(rel_alias) only for relationship entity bindings.",
            )
            if binding_alias_kinds.get(type_alias) != "relationship":
                raise ValueError(
                    "CypherGlot currently supports WITH WHERE type(rel_alias) only for relationship entity bindings."
                )
        elif (
            binding_kinds.get(type_alias) != "entity"
            or binding_alias_kinds.get(type_alias) != "relationship"
        ):
            raise ValueError(generic_message)
        return
    size_expr = _match_size_expression(target_text)
    if size_expr is not None:
        try:
            _validate_with_size_input(
                size_expr,
                binding_kinds=binding_kinds,
                binding_alias_kinds=binding_alias_kinds,
            )
        except ValueError as exc:
            if where_context:
                raise
            raise ValueError(generic_message) from exc
        return
    field_match = _match_field(target_text)
    if field_match is not None and binding_kinds.get(field_match.group("alias")) == "entity":
        return
    raise ValueError(generic_message)


def _validate_with_function_projection(
    function_name: str,
    args: list[str],
    *,
    binding_kinds: dict[str, str],
    binding_alias_kinds: dict[str, str],
) -> str:
    if function_name == "coalesce":
        if len(args) != 2:
            raise ValueError(
                "CypherGlot currently requires coalesce(...) in the WITH subset to use exactly two arguments."
            )
        primary_expr, fallback_expr = args
        field_match = _match_field(primary_expr)
        if field_match is not None:
            if binding_kinds.get(field_match.group("alias")) != "entity":
                raise ValueError(
                    "CypherGlot currently supports coalesce(...) in the WITH subset only as coalesce(entity_alias.field, literal_or_parameter) or coalesce(scalar_alias, literal_or_parameter)."
                )
        elif binding_kinds.get(primary_expr) != "scalar":
            raise ValueError(
                "CypherGlot currently supports coalesce(...) in the WITH subset only as coalesce(entity_alias.field, literal_or_parameter) or coalesce(scalar_alias, literal_or_parameter)."
            )
        try:
            _parse_literal(fallback_expr)
        except ValueError as exc:
            raise ValueError(
                "CypherGlot currently supports coalesce(...) in the WITH subset only as coalesce(entity_alias.field, literal_or_parameter) or coalesce(scalar_alias, literal_or_parameter)."
            ) from exc
        return "scalar"

    if function_name == "replace":
        if len(args) != 3:
            raise ValueError(
                "CypherGlot currently requires replace(...) in the WITH subset to use exactly three arguments."
            )
        _validate_with_scalar_or_field_input(
            args[0],
            binding_kinds=binding_kinds,
            allow_literal=True,
            message="CypherGlot currently supports replace(...) in the WITH subset only as replace(admitted_input, literal_or_parameter, literal_or_parameter).",
        )
        try:
            _parse_literal(args[1])
            _parse_literal(args[2])
        except ValueError as exc:
            raise ValueError(
                "CypherGlot currently supports replace(...) in the WITH subset only as replace(admitted_input, literal_or_parameter, literal_or_parameter)."
            ) from exc
        return "scalar"

    if function_name in {"left", "right"}:
        if len(args) != 2:
            raise ValueError(
                "CypherGlot currently requires left(...) and right(...) in the WITH subset to use exactly two arguments."
            )
        _validate_with_scalar_or_field_input(
            args[0],
            binding_kinds=binding_kinds,
            allow_literal=True,
            message="CypherGlot currently supports left(...) and right(...) in the WITH subset only as function(admitted_input, literal_or_parameter).",
        )
        try:
            _parse_literal(args[1])
        except ValueError as exc:
            raise ValueError(
                "CypherGlot currently supports left(...) and right(...) in the WITH subset only as function(admitted_input, literal_or_parameter)."
            ) from exc
        return "scalar"

    if function_name == "split":
        if len(args) != 2:
            raise ValueError(
                "CypherGlot currently requires split(...) in the WITH subset to use exactly two arguments."
            )
        _validate_with_scalar_or_field_input(
            args[0],
            binding_kinds=binding_kinds,
            allow_literal=True,
            message="CypherGlot currently supports split(...) in the WITH subset only as split(admitted_input, literal_or_parameter).",
        )
        try:
            _parse_literal(args[1])
        except ValueError as exc:
            raise ValueError(
                "CypherGlot currently supports split(...) in the WITH subset only as split(admitted_input, literal_or_parameter)."
            ) from exc
        return "scalar"

    if len(args) not in {2, 3}:
        raise ValueError(
            "CypherGlot currently requires substring(...) in the WITH subset to use exactly two or three arguments."
        )
    _validate_with_scalar_or_field_input(
        args[0],
        binding_kinds=binding_kinds,
        allow_literal=True,
        message="CypherGlot currently supports substring(...) in the WITH subset only as substring(admitted_input, literal_or_parameter) or substring(admitted_input, literal_or_parameter, literal_or_parameter).",
    )
    try:
        _parse_literal(args[1])
        if len(args) == 3:
            _parse_literal(args[2])
    except ValueError as exc:
        raise ValueError(
            "CypherGlot currently supports substring(...) in the WITH subset only as substring(admitted_input, literal_or_parameter) or substring(admitted_input, literal_or_parameter, literal_or_parameter)."
        ) from exc
    return "scalar"


def _validate_with_projection_expression(
    expression_text: str,
    *,
    output_alias: str | None,
    binding_kinds: dict[str, str],
    binding_alias_kinds: dict[str, str],
) -> str:
    if _validate_with_case_expression(
        expression_text,
        output_alias=output_alias,
        binding_kinds=binding_kinds,
    ):
        return "scalar"

    count_match = re.fullmatch(
        r"count\s*\(\s*(?P<alias>\*|[A-Za-z_][A-Za-z0-9_]*)\s*\)",
        expression_text,
        flags=re.IGNORECASE,
    )
    if count_match is not None:
        alias = count_match.group("alias")
        if alias != "*" and alias not in binding_kinds:
            raise ValueError(
                "CypherGlot currently requires count(...) in the WITH subset to target an admitted binding alias or *."
            )
        return "aggregate"

    aggregate_match = re.fullmatch(
        r"(?P<func>sum|avg|min|max)\s*\(\s*(?P<alias>[A-Za-z_][A-Za-z0-9_]*)(?:\.(?P<field>[A-Za-z_][A-Za-z0-9_]*))?\s*\)",
        expression_text,
        flags=re.IGNORECASE,
    )
    if aggregate_match is not None:
        alias = aggregate_match.group("alias")
        field = aggregate_match.group("field")
        if field is None:
            if binding_kinds.get(alias) != "scalar":
                raise ValueError(
                    "CypherGlot currently supports sum(...), avg(...), min(...), and max(...) in the WITH subset only over admitted scalar bindings."
                )
        else:
            _validate_with_entity_alias(
                alias,
                binding_kinds,
                "CypherGlot currently supports sum(...), avg(...), min(...), and max(...) in the WITH subset over entity-field inputs only when the alias is an admitted entity binding.",
            )
        return "aggregate"

    if expression_text in binding_kinds:
        return "entity" if binding_kinds[expression_text] == "entity" else "scalar"

    field_match = _match_field(expression_text)
    if field_match is not None:
        _validate_with_entity_alias(
            field_match.group("alias"),
            binding_kinds,
            f"CypherGlot currently cannot return unknown entity alias {field_match.group('alias')!r} in the WITH subset.",
        )
        return "scalar"

    id_alias = _match_id_alias(expression_text)
    if id_alias is not None:
        _validate_with_entity_alias(
            id_alias,
            binding_kinds,
            "CypherGlot currently supports id(...) in the WITH subset only over admitted entity bindings.",
        )
        return "scalar"

    type_alias = _match_type_alias(expression_text)
    if type_alias is not None:
        _validate_with_entity_alias(
            type_alias,
            binding_kinds,
            "CypherGlot currently supports type(...) in the WITH subset only over admitted relationship entity bindings.",
        )
        if binding_alias_kinds.get(type_alias) != "relationship":
            raise ValueError(
                "CypherGlot currently supports type(...) in the WITH subset only over admitted relationship entity bindings."
            )
        return "scalar"

    for function_name, kind_message, alias_kind in (
        ("properties", "CypherGlot currently supports properties(...) in the WITH subset only over admitted entity bindings.", None),
        ("labels", "CypherGlot currently supports labels(...) in the WITH subset only over admitted node entity bindings.", "node"),
        ("keys", "CypherGlot currently supports keys(...) in the WITH subset only over admitted entity bindings.", None),
    ):
        match = re.fullmatch(
            rf"{function_name}\s*\(\s*(?P<alias>[A-Za-z_][A-Za-z0-9_]*)\s*\)",
            expression_text,
            flags=re.IGNORECASE,
        )
        if match is not None:
            alias = match.group("alias")
            _validate_with_entity_alias(alias, binding_kinds, kind_message)
            if alias_kind is not None and binding_alias_kinds.get(alias) != alias_kind:
                raise ValueError(kind_message)
            return "scalar"

    node_match = re.fullmatch(
        r"(?P<func>startNode|endNode)\s*\(\s*(?P<alias>[A-Za-z_][A-Za-z0-9_]*)\s*\)(?:\.(?P<field>[A-Za-z_][A-Za-z0-9_]*))?",
        expression_text,
        flags=re.IGNORECASE,
    )
    if node_match is not None:
        function_name = node_match.group("func")
        alias = node_match.group("alias")
        message = (
            "CypherGlot currently supports startNode(...) in the WITH subset only over admitted relationship entity bindings."
            if function_name.lower() == "startnode"
            else "CypherGlot currently supports endNode(...) in the WITH subset only over admitted relationship entity bindings."
        )
        _validate_with_entity_alias(
            alias,
            binding_kinds,
            message,
        )
        if binding_alias_kinds.get(alias) != "relationship":
            raise ValueError(message)
        return "entity" if node_match.group("field") is None else "scalar"

    size_expr = _match_size_expression(expression_text)
    if size_expr is not None:
        _validate_with_size_input(
            size_expr,
            binding_kinds=binding_kinds,
            binding_alias_kinds=binding_alias_kinds,
        )
        return "scalar"

    unary_match = re.fullmatch(
        rf"(?P<func>{'|'.join(sorted(_UNARY_FUNCTION_NAMES))})\s*\(\s*(?P<expr>.+?)\s*\)",
        expression_text,
        flags=re.IGNORECASE,
    )
    if unary_match is not None:
        function_name = unary_match.group("func")
        if function_name.lower() in {"lower", "upper", "trim", "ltrim", "rtrim", "reverse"}:
            message = (
                "CypherGlot currently supports lower(...), upper(...), trim(...), ltrim(...), rtrim(...), and reverse(...) in the WITH subset only over admitted entity-field projections, scalar bindings, or scalar literal/parameter inputs."
            )
        else:
            canonical_name = {
                "tostring": "toString",
                "tointeger": "toInteger",
                "tofloat": "toFloat",
                "toboolean": "toBoolean",
            }.get(function_name.lower(), function_name.lower())
            message = (
                f"CypherGlot currently supports {canonical_name}(...) in the WITH subset only over admitted entity-field projections, scalar bindings, or scalar literal/parameter inputs."
            )
        _validate_with_scalar_or_field_input(
            unary_match.group("expr").strip(),
            binding_kinds=binding_kinds,
            allow_literal=True,
            message=message,
        )
        return "scalar"

    for function_name in ("coalesce", "replace", "left", "right", "split", "substring"):
        match = re.fullmatch(
            rf"{function_name}\s*\(\s*(?P<args>.+)\s*\)",
            expression_text,
            flags=re.IGNORECASE,
        )
        if match is not None:
            args = [part.strip() for part in _split_comma_separated(match.group("args"))]
            return _validate_with_function_projection(
                function_name,
                args,
                binding_kinds=binding_kinds,
                binding_alias_kinds=binding_alias_kinds,
            )

    try:
        _split_predicate_comparison(expression_text)
    except ValueError:
        pass
    else:
        if output_alias is None:
            raise ValueError(
                "CypherGlot currently requires predicate RETURN items in the WITH subset to use an explicit AS alias."
            )
        left_text, operator, value_text = _split_predicate_comparison(expression_text)
        generic_value_message = (
            "CypherGlot currently supports predicate RETURN items in the WITH subset only as scalar_alias OP value, entity_alias.field OP value, id(entity_alias) OP value, type(rel_alias) OP value, or size(admitted_input) OP value."
        )
        generic_literal_message = (
            "CypherGlot currently supports predicate RETURN items in the WITH subset only as scalar_alias OP literal_or_parameter, entity_alias.field OP literal_or_parameter, id(entity_alias) OP literal_or_parameter, type(rel_alias) OP literal_or_parameter, or size(admitted_input) OP literal_or_parameter."
        )
        _validate_with_predicate_target(
            left_text.strip(),
            binding_kinds=binding_kinds,
            binding_alias_kinds=binding_alias_kinds,
            generic_message=(
                generic_value_message
                if operator in {"IS NULL", "IS NOT NULL"}
                else generic_literal_message
            ),
            where_context=False,
        )
        if operator in {"IS NULL", "IS NOT NULL"}:
            if value_text.strip():
                raise ValueError(
                    "CypherGlot null predicates cannot include a trailing literal value."
                )
        else:
            try:
                _parse_literal(value_text.strip())
            except ValueError as exc:
                raise ValueError(generic_literal_message) from exc
        return "scalar"

    if output_alias is not None:
        try:
            _parse_literal(expression_text)
        except ValueError:
            pass
        else:
            return "scalar"

    raise ValueError(
        "CypherGlot currently supports RETURN alias.field for entity bindings, RETURN entity_alias for pass-through entity bindings, RETURN scalar_alias for scalar bindings, id(binding_alias) AS output_alias, sum(scalar_alias) AS output_alias, avg(scalar_alias) AS output_alias, min(scalar_alias) AS output_alias, max(scalar_alias) AS output_alias, type(rel_binding_alias) AS output_alias, size(admitted_input) AS output_alias, lower(admitted_input) AS output_alias, upper(admitted_input) AS output_alias, trim(admitted_input) AS output_alias, ltrim(admitted_input) AS output_alias, rtrim(admitted_input) AS output_alias, reverse(admitted_input) AS output_alias, coalesce(admitted_input, literal_or_parameter) AS output_alias, replace(admitted_input, literal_or_parameter, literal_or_parameter) AS output_alias, left(admitted_input, literal_or_parameter) AS output_alias, right(admitted_input, literal_or_parameter) AS output_alias, split(admitted_input, literal_or_parameter) AS output_alias, substring(admitted_input, literal_or_parameter) AS output_alias, substring(admitted_input, literal_or_parameter, literal_or_parameter) AS output_alias, abs(admitted_input) AS output_alias, sign(admitted_input) AS output_alias, round(admitted_input) AS output_alias, ceil(admitted_input) AS output_alias, floor(admitted_input) AS output_alias, sqrt(admitted_input) AS output_alias, exp(admitted_input) AS output_alias, toString(admitted_input) AS output_alias, toInteger(admitted_input) AS output_alias, toFloat(admitted_input) AS output_alias, toBoolean(admitted_input) AS output_alias, scalar_literal_or_parameter AS output_alias, predicate admitted_input OP value AS output_alias, searched CASE WHEN admitted_predicate THEN admitted_result ELSE admitted_result END AS output_alias, and optional AS aliases for those projection forms."
    )


def _validate_with_return_projection_shape(
    projection_text: str,
    *,
    binding_kinds: dict[str, str],
    binding_alias_kinds: dict[str, str],
) -> None:
    return_text, order_items, _limit, _distinct, _skip = _split_return_clause(
        projection_text
    )
    raw_items = [item.strip() for item in _split_comma_separated(return_text)]
    if not raw_items:
        raise ValueError(
            "CypherGlot currently requires at least one RETURN item in the supported WITH subset."
        )

    projected_output_kinds: dict[str, str] = {}
    aggregate_aliases: set[str] = set()
    exact_projected_expressions: set[str] = set()

    for item_text in raw_items:
        expression_text, output_alias = _split_projection_alias(item_text)
        output_kind = _validate_with_projection_expression(
            expression_text,
            output_alias=output_alias,
            binding_kinds=binding_kinds,
            binding_alias_kinds=binding_alias_kinds,
        )
        output_name = output_alias or expression_text
        _require_unique_output(
            projected_output_kinds,
            output_name,
            f"CypherGlot currently does not allow duplicate RETURN output alias {output_name!r} in the WITH subset.",
        )
        projected_output_kinds[output_name] = output_kind
        exact_projected_expressions.add(expression_text)
        if output_kind == "aggregate":
            aggregate_aliases.add(output_name)

    for item in order_items:
        if item.expression is not None:
            if item.expression in aggregate_aliases:
                continue
            if item.expression in projected_output_kinds:
                continue
            if item.expression in exact_projected_expressions:
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
                "CypherGlot currently supports ORDER BY scalar_alias for scalar bindings in the supported WITH subset."
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

            _validate_with_predicate_target(
                left_text.strip(),
                binding_kinds=binding_kinds,
                binding_alias_kinds=binding_alias_kinds,
                generic_message=(
                    "CypherGlot currently supports WITH WHERE items shaped as scalar_alias OP value, entity_alias.field OP value, id(entity_alias) OP value, type(rel_alias) OP value, or size(admitted_input) OP value."
                ),
                where_context=True,
            )
            if operator in {"IS NULL", "IS NOT NULL"}:
                if value_text.strip():
                    raise ValueError(
                        "CypherGlot null predicates cannot include a trailing literal value."
                    )
            else:
                _parse_literal(value_text.strip())

    if not saw_predicate:
        raise ValueError("CypherGlot WITH WHERE clauses cannot be empty.")