"""WITH-shape validation helpers extracted from validate.py."""

from __future__ import annotations

import re

from ._normalize_support import (
    _parse_literal,
    _parse_return_items,
    _split_comma_separated,
    _split_predicate_comparison,
    _split_return_clause,
)
from ._validate_projection import (
    _validate_plain_case_expression,
    _validate_plain_read_projection_shape,
    _validate_with_case_expression,
    _validate_with_projection_expression,
    _validate_with_where_shape,
)
from ._validate_shape_helpers import _context_text, _validate_match_pattern_shape
from .parser import CypherParseResult

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
        if output_alias is None:
            raise ValueError(
                "CypherGlot currently requires derived scalar WITH bindings to use "
                "an explicit AS alias in the supported multi-part subset."
            )
        projected_kind = _validate_with_projection_expression(
            expression_text,
            output_alias=output_alias,
            binding_kinds={alias: "entity" for alias in allowed_aliases},
            binding_alias_kinds=source_alias_kinds,
        )
        if projected_kind != "scalar":
            raise ValueError(
                "CypherGlot currently supports derived WITH bindings only for "
                "scalar-producing expressions in the supported multi-part subset."
            )
        binding_kinds[output_alias] = "scalar"

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

        _unary_func_m = re.fullmatch(
            r"(?P<func>lower|upper|trim|ltrim|rtrim|reverse|abs|sign|round"
            r"|floor|ceil|sqrt|exp|sin|cos|tan|asin|acos|atan|ln|log"
            r"|radians|degrees|log10|tostring|tointeger|tofloat|toboolean"
            r")\s*\(\s*(?P<expr>.+?)\s*\)",
            expression_text,
            flags=re.IGNORECASE,
        )
        if _unary_func_m is not None:
            _uf_lower = _unary_func_m.group("func").lower()
            _uf_display = {
                "tostring": "toString", "tointeger": "toInteger",
                "tofloat": "toFloat", "toboolean": "toBoolean",
            }.get(_uf_lower, _uf_lower)
            if output_alias is None:
                raise ValueError(
                    f"CypherGlot currently requires {_uf_display}(...) in the WITH subset to use an explicit AS alias."
                )
            _uf_expr = _unary_func_m.group("expr").strip()
            if binding_kinds.get(_uf_expr) == "scalar":
                if output_alias in seen_output_names:
                    raise ValueError(
                        f"CypherGlot currently does not allow duplicate RETURN output alias {output_alias!r} in the WITH subset."
                    )
                seen_output_names.add(output_alias)
                projected_output_kinds[output_alias] = "scalar"
                continue
            try:
                _parse_literal(_uf_expr)
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
                _uf_expr,
            )
            if field_match is None or binding_kinds.get(field_match.group("alias")) != "entity":
                raise ValueError(
                    f"CypherGlot currently supports {_uf_display}(...) in the WITH subset only over admitted entity-field projections, scalar bindings, or scalar literal/parameter inputs."
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
