from __future__ import annotations

import re

from ._normalize_support import _parse_literal, _split_comma_separated


_PLAIN_READ_STRING_UNARY_FUNCTIONS = (
    "lower",
    "upper",
    "trim",
    "ltrim",
    "rtrim",
    "reverse",
    "abs",
    "sign",
    "round",
    "floor",
    "ceil",
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
    "radians",
    "degrees",
    "log10",
    "tostring",
    "tointeger",
    "tofloat",
    "toboolean",
)

_PLAIN_READ_UNARY_FUNCTION_DISPLAY_NAMES = ", ".join(
    f"{name}(...)" for name in _PLAIN_READ_STRING_UNARY_FUNCTIONS
)


def _register_plain_projection_output(
    output_name: str,
    *,
    projected_output_kinds: dict[str, str],
) -> None:
    if output_name in projected_output_kinds:
        raise ValueError(
            "CypherGlot currently does not allow duplicate RETURN output "
            f"alias {output_name!r} in the supported read subset."
        )
    projected_output_kinds[output_name] = "scalar"


def _register_with_projection_output(
    output_name: str,
    *,
    seen_output_names: set[str],
    projected_output_kinds: dict[str, str],
) -> None:
    if output_name in seen_output_names:
        raise ValueError(
            "CypherGlot currently does not allow duplicate RETURN output "
            f"alias {output_name!r} in the WITH subset."
        )
    seen_output_names.add(output_name)
    projected_output_kinds[output_name] = "scalar"


def _validate_plain_field_or_literal_input(
    expression_text: str,
    *,
    allowed_aliases: set[str],
    message: str,
) -> None:
    field_match = re.fullmatch(
        r"(?P<alias>[A-Za-z_][A-Za-z0-9_]*)\.(?P<field>[A-Za-z_][A-Za-z0-9_]*)",
        expression_text,
    )
    if field_match is not None:
        if field_match.group("alias") not in allowed_aliases:
            raise ValueError(message)
        return
    try:
        _parse_literal(expression_text)
    except ValueError as exc:
        raise ValueError(message) from exc


def _validate_with_field_scalar_or_literal_input(
    expression_text: str,
    *,
    binding_kinds: dict[str, str],
    message: str,
) -> None:
    field_match = re.fullmatch(
        r"(?P<alias>[A-Za-z_][A-Za-z0-9_]*)\.(?P<field>[A-Za-z_][A-Za-z0-9_]*)",
        expression_text,
    )
    if field_match is not None:
        if binding_kinds.get(field_match.group("alias")) != "entity":
            raise ValueError(message)
        return
    if binding_kinds.get(expression_text) == "scalar":
        return
    try:
        _parse_literal(expression_text)
    except ValueError as exc:
        raise ValueError(message) from exc


def _validate_plain_unary_function_projection(
    expression_text: str,
    *,
    output_alias: str | None,
    allowed_aliases: set[str],
    projected_output_kinds: dict[str, str],
) -> bool:
    unary_string_match = re.fullmatch(
        (
            r"(?P<func>lower|upper|trim|ltrim|rtrim|reverse|abs|sign|round|floor|ceil|sqrt|exp|sin|cos|tan|asin|acos|atan|ln|log|radians|degrees|log10|tostring|tointeger|tofloat|toboolean)"
            r"\s*\(\s*(?P<expr>.+?)\s*\)"
        ),
        expression_text,
        flags=re.IGNORECASE,
    )
    if unary_string_match is None:
        return False
    if output_alias is None:
        raise ValueError(
            "CypherGlot currently requires "
            f"{_PLAIN_READ_UNARY_FUNCTION_DISPLAY_NAMES} in the supported "
            "read subset to use an explicit AS alias."
        )
    function_expr = unary_string_match.group("expr").strip()
    field_match = re.fullmatch(
        r"(?P<alias>[A-Za-z_][A-Za-z0-9_]*)\.(?P<field>[A-Za-z_][A-Za-z0-9_]*)",
        function_expr,
    )
    if field_match is not None:
        if field_match.group("alias") not in allowed_aliases:
            raise ValueError(
                "CypherGlot currently supports "
                f"{_PLAIN_READ_UNARY_FUNCTION_DISPLAY_NAMES} in the supported "
                "read subset only over admitted field projections or scalar "
                "literal/parameter inputs."
            )
    else:
        try:
            _parse_literal(function_expr)
        except ValueError as exc:
            raise ValueError(
                "CypherGlot currently supports "
                f"{_PLAIN_READ_UNARY_FUNCTION_DISPLAY_NAMES} in the supported "
                "read subset only over admitted field projections or scalar "
                "literal/parameter inputs."
            ) from exc
    _register_plain_projection_output(
        output_alias,
        projected_output_kinds=projected_output_kinds,
    )
    return True


def _validate_plain_multi_argument_function_projection(
    expression_text: str,
    *,
    output_alias: str | None,
    allowed_aliases: set[str],
    projected_output_kinds: dict[str, str],
) -> bool:
    coalesce_match = re.fullmatch(
        r"coalesce\s*\(\s*(?P<args>.+)\s*\)",
        expression_text,
        flags=re.IGNORECASE,
    )
    if coalesce_match is not None:
        args = [
            part.strip()
            for part in _split_comma_separated(coalesce_match.group("args"))
        ]
        if len(args) != 2:
            raise ValueError(
                "CypherGlot currently requires coalesce(...) in the supported "
                "read subset to use exactly two arguments."
            )
        primary_expr, fallback_expr = args
        field_match = re.fullmatch(
            r"(?P<alias>[A-Za-z_][A-Za-z0-9_]*)\.(?P<field>[A-Za-z_][A-Za-z0-9_]*)",
            primary_expr,
        )
        if field_match is None or field_match.group("alias") not in allowed_aliases:
            raise ValueError(
                "CypherGlot currently supports coalesce(...) in the supported "
                "read subset only as coalesce(alias.field, "
                "literal_or_parameter) over admitted bindings."
            )
        try:
            _parse_literal(fallback_expr)
        except ValueError as exc:
            raise ValueError(
                "CypherGlot currently supports coalesce(...) in the supported "
                "read subset only as coalesce(alias.field, "
                "literal_or_parameter) over admitted bindings."
            ) from exc
        _register_plain_projection_output(
            output_alias or expression_text,
            projected_output_kinds=projected_output_kinds,
        )
        return True

    replace_match = re.fullmatch(
        r"replace\s*\(\s*(?P<args>.+)\s*\)",
        expression_text,
        flags=re.IGNORECASE,
    )
    if replace_match is not None:
        args = [
            part.strip()
            for part in _split_comma_separated(replace_match.group("args"))
        ]
        if len(args) != 3:
            raise ValueError(
                "CypherGlot currently requires replace(...) in the supported "
                "read subset to use exactly three arguments."
            )
        primary_expr, search_expr, replace_expr = args
        message = (
            "CypherGlot currently supports replace(...) in the supported read "
            "subset only as replace(admitted_input, literal_or_parameter, "
            "literal_or_parameter)."
        )
        _validate_plain_field_or_literal_input(
            primary_expr,
            allowed_aliases=allowed_aliases,
            message=message,
        )
        try:
            _parse_literal(search_expr)
            _parse_literal(replace_expr)
        except ValueError as exc:
            raise ValueError(message) from exc
        _register_plain_projection_output(
            output_alias or expression_text,
            projected_output_kinds=projected_output_kinds,
        )
        return True

    left_right_match = re.fullmatch(
        r"(?P<func>left|right)\s*\(\s*(?P<args>.+)\s*\)",
        expression_text,
        flags=re.IGNORECASE,
    )
    if left_right_match is not None:
        args = [
            part.strip()
            for part in _split_comma_separated(left_right_match.group("args"))
        ]
        if len(args) != 2:
            raise ValueError(
                "CypherGlot currently requires left(...) and right(...) in the "
                "supported read subset to use exactly two arguments."
            )
        primary_expr, length_expr = args
        message = (
            "CypherGlot currently supports left(...) and right(...) in the "
            "supported read subset only as function(admitted_input, "
            "literal_or_parameter)."
        )
        try:
            _parse_literal(length_expr)
        except ValueError as exc:
            raise ValueError(message) from exc
        _validate_plain_field_or_literal_input(
            primary_expr,
            allowed_aliases=allowed_aliases,
            message=message,
        )
        _register_plain_projection_output(
            output_alias or expression_text,
            projected_output_kinds=projected_output_kinds,
        )
        return True

    split_match = re.fullmatch(
        r"split\s*\(\s*(?P<args>.+)\s*\)",
        expression_text,
        flags=re.IGNORECASE,
    )
    if split_match is not None:
        args = [
            part.strip()
            for part in _split_comma_separated(split_match.group("args"))
        ]
        if len(args) != 2:
            raise ValueError(
                "CypherGlot currently requires split(...) in the supported read "
                "subset to use exactly two arguments."
            )
        primary_expr, delimiter_expr = args
        message = (
            "CypherGlot currently supports split(...) in the supported read "
            "subset only as split(admitted_input, literal_or_parameter)."
        )
        try:
            _parse_literal(delimiter_expr)
        except ValueError as exc:
            raise ValueError(message) from exc
        _validate_plain_field_or_literal_input(
            primary_expr,
            allowed_aliases=allowed_aliases,
            message=message,
        )
        _register_plain_projection_output(
            output_alias or expression_text,
            projected_output_kinds=projected_output_kinds,
        )
        return True

    substring_match = re.fullmatch(
        r"substring\s*\(\s*(?P<args>.+)\s*\)",
        expression_text,
        flags=re.IGNORECASE,
    )
    if substring_match is not None:
        args = [
            part.strip()
            for part in _split_comma_separated(substring_match.group("args"))
        ]
        if len(args) not in {2, 3}:
            raise ValueError(
                "CypherGlot currently requires substring(...) in the supported "
                "read subset to use exactly two or three arguments."
            )
        primary_expr, start_expr = args[:2]
        message = (
            "CypherGlot currently supports substring(...) in the supported read "
            "subset only as substring(admitted_input, literal_or_parameter) "
            "or substring(admitted_input, literal_or_parameter, "
            "literal_or_parameter)."
        )
        try:
            _parse_literal(start_expr)
            if len(args) == 3:
                _parse_literal(args[2])
        except ValueError as exc:
            raise ValueError(message) from exc
        _validate_plain_field_or_literal_input(
            primary_expr,
            allowed_aliases=allowed_aliases,
            message=message,
        )
        _register_plain_projection_output(
            output_alias or expression_text,
            projected_output_kinds=projected_output_kinds,
        )
        return True

    return False


def _validate_with_multi_argument_function_projection(
    expression_text: str,
    *,
    output_alias: str | None,
    binding_kinds: dict[str, str],
    seen_output_names: set[str],
    projected_output_kinds: dict[str, str],
) -> bool:
    coalesce_match = re.fullmatch(
        r"coalesce\s*\(\s*(?P<args>.+)\s*\)",
        expression_text,
        flags=re.IGNORECASE,
    )
    if coalesce_match is not None:
        args = [
            part.strip()
            for part in _split_comma_separated(coalesce_match.group("args"))
        ]
        if len(args) != 2:
            raise ValueError(
                "CypherGlot currently requires coalesce(...) in the WITH subset "
                "to use exactly two arguments."
            )
        primary_expr, fallback_expr = args
        try:
            _parse_literal(fallback_expr)
        except ValueError as exc:
            raise ValueError(
                "CypherGlot currently supports coalesce(...) in the WITH subset "
                "only as coalesce(entity_alias.field, literal_or_parameter) or "
                "coalesce(scalar_alias, literal_or_parameter)."
            ) from exc
        field_match = re.fullmatch(
            r"(?P<alias>[A-Za-z_][A-Za-z0-9_]*)\.(?P<field>[A-Za-z_][A-Za-z0-9_]*)",
            primary_expr,
        )
        if field_match is not None:
            if binding_kinds.get(field_match.group("alias")) != "entity":
                raise ValueError(
                    "CypherGlot currently supports coalesce(...) in the WITH "
                    "subset only as coalesce(entity_alias.field, "
                    "literal_or_parameter) or coalesce(scalar_alias, "
                    "literal_or_parameter)."
                )
        elif binding_kinds.get(primary_expr) != "scalar":
            raise ValueError(
                "CypherGlot currently supports coalesce(...) in the WITH subset "
                "only as coalesce(entity_alias.field, literal_or_parameter) or "
                "coalesce(scalar_alias, literal_or_parameter)."
            )
        _register_with_projection_output(
            output_alias or expression_text,
            seen_output_names=seen_output_names,
            projected_output_kinds=projected_output_kinds,
        )
        return True

    replace_match = re.fullmatch(
        r"replace\s*\(\s*(?P<args>.+)\s*\)",
        expression_text,
        flags=re.IGNORECASE,
    )
    if replace_match is not None:
        args = [
            part.strip()
            for part in _split_comma_separated(replace_match.group("args"))
        ]
        if len(args) != 3:
            raise ValueError(
                "CypherGlot currently requires replace(...) in the WITH subset "
                "to use exactly three arguments."
            )
        primary_expr, search_expr, replace_expr = args
        message = (
            "CypherGlot currently supports replace(...) in the WITH subset only "
            "as replace(admitted_input, literal_or_parameter, "
            "literal_or_parameter)."
        )
        try:
            _parse_literal(search_expr)
            _parse_literal(replace_expr)
        except ValueError as exc:
            raise ValueError(message) from exc
        _validate_with_field_scalar_or_literal_input(
            primary_expr,
            binding_kinds=binding_kinds,
            message=message,
        )
        _register_with_projection_output(
            output_alias or expression_text,
            seen_output_names=seen_output_names,
            projected_output_kinds=projected_output_kinds,
        )
        return True

    left_right_match = re.fullmatch(
        r"(?P<func>left|right)\s*\(\s*(?P<args>.+)\s*\)",
        expression_text,
        flags=re.IGNORECASE,
    )
    if left_right_match is not None:
        args = [
            part.strip()
            for part in _split_comma_separated(left_right_match.group("args"))
        ]
        if len(args) != 2:
            raise ValueError(
                "CypherGlot currently requires left(...) and right(...) in the "
                "WITH subset to use exactly two arguments."
            )
        primary_expr, length_expr = args
        message = (
            "CypherGlot currently supports left(...) and right(...) in the WITH "
            "subset only as function(admitted_input, literal_or_parameter)."
        )
        try:
            _parse_literal(length_expr)
        except ValueError as exc:
            raise ValueError(message) from exc
        _validate_with_field_scalar_or_literal_input(
            primary_expr,
            binding_kinds=binding_kinds,
            message=message,
        )
        _register_with_projection_output(
            output_alias or expression_text,
            seen_output_names=seen_output_names,
            projected_output_kinds=projected_output_kinds,
        )
        return True

    split_match = re.fullmatch(
        r"split\s*\(\s*(?P<args>.+)\s*\)",
        expression_text,
        flags=re.IGNORECASE,
    )
    if split_match is not None:
        args = [
            part.strip()
            for part in _split_comma_separated(split_match.group("args"))
        ]
        if len(args) != 2:
            raise ValueError(
                "CypherGlot currently requires split(...) in the WITH subset "
                "to use exactly two arguments."
            )
        primary_expr, delimiter_expr = args
        message = (
            "CypherGlot currently supports split(...) in the WITH subset only "
            "as split(admitted_input, literal_or_parameter)."
        )
        try:
            _parse_literal(delimiter_expr)
        except ValueError as exc:
            raise ValueError(message) from exc
        _validate_with_field_scalar_or_literal_input(
            primary_expr,
            binding_kinds=binding_kinds,
            message=message,
        )
        _register_with_projection_output(
            output_alias or expression_text,
            seen_output_names=seen_output_names,
            projected_output_kinds=projected_output_kinds,
        )
        return True

    substring_match = re.fullmatch(
        r"substring\s*\(\s*(?P<args>.+)\s*\)",
        expression_text,
        flags=re.IGNORECASE,
    )
    if substring_match is not None:
        args = [
            part.strip()
            for part in _split_comma_separated(substring_match.group("args"))
        ]
        if len(args) not in {2, 3}:
            raise ValueError(
                "CypherGlot currently requires substring(...) in the WITH "
                "subset to use exactly two or three arguments."
            )
        primary_expr, start_expr = args[:2]
        message = (
            "CypherGlot currently supports substring(...) in the WITH subset "
            "only as substring(admitted_input, literal_or_parameter) or "
            "substring(admitted_input, literal_or_parameter, "
            "literal_or_parameter)."
        )
        try:
            _parse_literal(start_expr)
            if len(args) == 3:
                _parse_literal(args[2])
        except ValueError as exc:
            raise ValueError(message) from exc
        _validate_with_field_scalar_or_literal_input(
            primary_expr,
            binding_kinds=binding_kinds,
            message=message,
        )
        _register_with_projection_output(
            output_alias or expression_text,
            seen_output_names=seen_output_names,
            projected_output_kinds=projected_output_kinds,
        )
        return True

    return False


def _validate_with_size_projection(
    expression_text: str,
    *,
    output_alias: str | None,
    binding_kinds: dict[str, str],
    binding_alias_kinds: dict[str, str],
    seen_output_names: set[str],
    projected_output_kinds: dict[str, str],
) -> bool:
    size_match = re.fullmatch(
        r"size\s*\(\s*(?P<expr>.+?)\s*\)",
        expression_text,
        flags=re.IGNORECASE,
    )
    if size_match is None:
        return False
    if output_alias is None:
        raise ValueError(
            "CypherGlot currently requires size(...) in the WITH subset to use "
            "an explicit AS alias."
        )
    size_expr = size_match.group("expr").strip()
    if binding_kinds.get(size_expr) == "scalar":
        _register_with_projection_output(
            output_alias,
            seen_output_names=seen_output_names,
            projected_output_kinds=projected_output_kinds,
        )
        return True

    id_match = re.fullmatch(
        r"id\s*\(\s*(?P<alias>[A-Za-z_][A-Za-z0-9_]*)\s*\)",
        size_expr,
        flags=re.IGNORECASE,
    )
    if id_match is not None:
        if binding_kinds.get(id_match.group("alias")) != "entity":
            raise ValueError(
                "CypherGlot currently supports size(...) in the WITH subset only "
                "over admitted entity-field projections, admitted id/type "
                "outputs, scalar bindings, or scalar literal/parameter inputs."
            )
        _register_with_projection_output(
            output_alias,
            seen_output_names=seen_output_names,
            projected_output_kinds=projected_output_kinds,
        )
        return True

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
                "CypherGlot currently supports size(...) in the WITH subset only "
                "over admitted entity-field projections, admitted id/type "
                "outputs, scalar bindings, or scalar literal/parameter inputs."
            )
        _register_with_projection_output(
            output_alias,
            seen_output_names=seen_output_names,
            projected_output_kinds=projected_output_kinds,
        )
        return True

    try:
        _parse_literal(size_expr)
    except ValueError:
        pass
    else:
        _register_with_projection_output(
            output_alias,
            seen_output_names=seen_output_names,
            projected_output_kinds=projected_output_kinds,
        )
        return True

    field_match = re.fullmatch(
        r"(?P<alias>[A-Za-z_][A-Za-z0-9_]*)\.(?P<field>[A-Za-z_][A-Za-z0-9_]*)",
        size_expr,
    )
    if (
        field_match is not None
        and binding_kinds.get(field_match.group("alias")) == "entity"
    ):
        _register_with_projection_output(
            output_alias,
            seen_output_names=seen_output_names,
            projected_output_kinds=projected_output_kinds,
        )
        return True

    raise ValueError(
        "CypherGlot currently supports size(...) in the WITH subset only over "
        "admitted entity-field projections, admitted id/type outputs, scalar "
        "bindings, or scalar literal/parameter inputs."
    )


def _validate_with_unary_function_projection(
    expression_text: str,
    *,
    output_alias: str | None,
    binding_kinds: dict[str, str],
    seen_output_names: set[str],
    projected_output_kinds: dict[str, str],
) -> bool:
    unary_string_match = re.fullmatch(
        r"(?P<func>lower|upper|trim|ltrim|rtrim|reverse)\s*\(\s*(?P<expr>.+?)\s*\)",
        expression_text,
        flags=re.IGNORECASE,
    )
    if unary_string_match is None:
        return False
    if output_alias is None:
        raise ValueError(
            "CypherGlot currently requires lower(...), upper(...), trim(...), "
            "ltrim(...), rtrim(...), and reverse(...) in the WITH subset to "
            "use an explicit AS alias."
        )
    _validate_with_field_scalar_or_literal_input(
        unary_string_match.group("expr").strip(),
        binding_kinds=binding_kinds,
        message=(
            "CypherGlot currently supports lower(...), upper(...), trim(...), "
            "ltrim(...), rtrim(...), and reverse(...) in the WITH subset only "
            "over admitted entity-field projections, scalar bindings, or "
            "scalar literal/parameter inputs."
        ),
    )
    _register_with_projection_output(
        output_alias,
        seen_output_names=seen_output_names,
        projected_output_kinds=projected_output_kinds,
    )
    return True