"""One-shot script to consolidate 28 repetitive unary-function blocks in
_validate_with_helpers.py into a single unified handler.

The blocks to remove:
  - unary_string_match (lower/upper/trim/ltrim/rtrim/reverse) at ~line 576
  - abs_match through to_boolean_match at ~lines 838-1871

Replaced with a single _unary_func_m handler that covers all 28 functions.
"""
import re
import sys
from pathlib import Path

TARGET = Path("src/cypherglot/_validate_with_helpers.py")

text = TARGET.read_text()

# ---------------------------------------------------------------------------
# Step 1: Replace unary_string_match block with unified handler
# ---------------------------------------------------------------------------

OLD_UNARY_STRING = """\
        unary_string_match = re.fullmatch(
            r"(?P<func>lower|upper|trim|ltrim|rtrim|reverse)\\s*\\(\\s*(?P<expr>.+?)\\s*\\)",
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
                r"(?P<alias>[A-Za-z_][A-Za-z0-9_]*)\\.(?P<field>[A-Za-z_][A-Za-z0-9_]*)",
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
            continue"""

NEW_UNIFIED = """\
        _unary_func_m = re.fullmatch(
            r"(?P<func>lower|upper|trim|ltrim|rtrim|reverse|abs|sign|round"
            r"|floor|ceil|sqrt|exp|sin|cos|tan|asin|acos|atan|ln|log"
            r"|radians|degrees|log10|tostring|tointeger|tofloat|toboolean"
            r")\\s*\\(\\s*(?P<expr>.+?)\\s*\\)",
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
                r"(?P<alias>[A-Za-z_][A-Za-z0-9_]*)\\.(?P<field>[A-Za-z_][A-Za-z0-9_]*)",
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
            continue"""

if OLD_UNARY_STRING not in text:
    print("ERROR: Could not find unary_string_match block in source file.", file=sys.stderr)
    sys.exit(1)

text = text.replace(OLD_UNARY_STRING, NEW_UNIFIED, 1)
print("Step 1: Replaced unary_string_match block with unified handler.")

# ---------------------------------------------------------------------------
# Step 2: Remove all individual abs/sign/round/.../to_boolean blocks.
# These now span from just after the substring_match block to just before
# the `try: _parse_literal(expression_text)` literal fallback.
#
# We locate the blocks by finding the first occurrence of `        abs_match`
# and the last `continue` before `        try:\n            _parse_literal(expression_text)`.
# ---------------------------------------------------------------------------

# Marker: beginning of abs_match block
ABS_MARKER = "\n        abs_match = re.fullmatch(\n"
# Marker: end sentinel — the literal check that follows all individual blocks
LITERAL_FALLBACK = "\n        try:\n            _parse_literal(expression_text)\n"

abs_start = text.find(ABS_MARKER)
if abs_start == -1:
    print("ERROR: Could not find abs_match block start.", file=sys.stderr)
    sys.exit(1)

literal_start = text.find(LITERAL_FALLBACK, abs_start)
if literal_start == -1:
    print("ERROR: Could not find literal fallback after abs block.", file=sys.stderr)
    sys.exit(1)

# We want to keep the newline before `try:` (the blank line separator)
# The segment to delete is from ABS_MARKER start up to (but NOT including) LITERAL_FALLBACK
# abs_start points to the \n before `        abs_match`; include it in removal
# literal_start points to \n before `        try:`; we keep from literal_start onward

removed_block = text[abs_start:literal_start]
removed_lines = removed_block.count("\n")
print(f"Step 2: Removing {removed_lines} lines (abs through to_boolean blocks).")

text = text[:abs_start] + text[literal_start:]

# ---------------------------------------------------------------------------
# Verify the result compiles
# ---------------------------------------------------------------------------
try:
    compile(text, str(TARGET), "exec")
    print("Step 3: Syntax check passed.")
except SyntaxError as e:
    print(f"ERROR: Syntax error after consolidation: {e}", file=sys.stderr)
    sys.exit(1)

# ---------------------------------------------------------------------------
# Write back
# ---------------------------------------------------------------------------
TARGET.write_text(text)
lines_final = text.count("\n") + 1
print(f"Done. {TARGET} now has {lines_final} lines.")
