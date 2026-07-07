"""Early scan for Cypher constructs outside the admitted subset.

Runs once over the freshly-parsed tree so users of unsupported constructs
get a named, located, hint-carrying rejection instead of whichever shape
gate happens to trip first deeper in validation.
"""

from __future__ import annotations

from typing import Any

from .errors import reject
from .generated.CypherParser import CypherParser

_P = CypherParser

# Context class -> (construct id, message, porting hint). Only constructs
# that are rejected EVERYWHERE in the admitted subset may appear here.
_REJECTED_CONTEXTS: dict[type, tuple[str, str, str]] = {
    _P.OC_ListComprehensionContext: (
        "list_comprehension",
        "CypherGlot does not support list comprehensions yet.",
        "Compute the projection with WITH/RETURN items, or filter rows with WHERE.",
    ),
    _P.OC_PatternComprehensionContext: (
        "pattern_comprehension",
        "CypherGlot does not support pattern comprehensions yet.",
        "Use OPTIONAL MATCH with an aggregate (e.g. collect/count) instead.",
    ),
    _P.OC_ExistentialSubqueryContext: (
        "exists_subquery",
        "CypherGlot does not support EXISTS { ... } subqueries yet.",
        "Express the check as a MATCH/OPTIONAL MATCH plus a WHERE predicate.",
    ),
    # NOTE: CALL contexts are deliberately absent — the vector-aware subset
    # admits CALL db.index.vector.queryNodes(...), and the dedicated vector
    # gates produce more specific rejections for every other CALL shape.
    _P.OC_QuantifierContext: (
        "quantified_predicate",
        "CypherGlot does not support all()/any()/none()/single() predicates yet.",
        "Rewrite as explicit MATCH/WHERE conditions over the pattern.",
    ),
}

_REJECTED_FUNCTIONS: dict[str, tuple[str, str, str]] = {
    "shortestpath": (
        "shortest_path",
        "CypherGlot does not support shortestPath().",
        "Bounded variable-length patterns ([*1..k]) cover fixed-depth reachability.",
    ),
    "allshortestpaths": (
        "shortest_path",
        "CypherGlot does not support allShortestPaths().",
        "Bounded variable-length patterns ([*1..k]) cover fixed-depth reachability.",
    ),
}
# NOTE: reduce() and FOREACH never reach this scanner — their special syntax
# (initializer / clause form) is not in the grammar, so they surface as
# located syntax errors instead.


def scan_for_unsupported_constructs(tree: Any) -> None:
    """Reject the query if the parse tree contains an unsupported construct."""

    stack = [tree]
    while stack:
        node = stack.pop()
        entry = _REJECTED_CONTEXTS.get(type(node))
        if entry is not None:
            construct, message, hint = entry
            reject(message, ctx=node, construct=construct, hint=hint)

        if isinstance(node, _P.OC_FunctionInvocationContext):
            name_ctx = node.oC_FunctionName()
            if name_ctx is not None:
                entry = _REJECTED_FUNCTIONS.get(name_ctx.getText().lower())
                if entry is not None:
                    construct, message, hint = entry
                    reject(message, ctx=node, construct=construct, hint=hint)

        if isinstance(node, _P.OC_NodeLabelsContext):
            if len(node.oC_NodeLabel()) > 1:
                reject(
                    "CypherGlot does not support multiple labels on one node.",
                    ctx=node,
                    construct="multi_label",
                    hint="Model the second label as a boolean property, or split the type.",
                )

        if isinstance(node, _P.OC_PatternPartContext):
            if node.oC_Variable() is not None:
                reject(
                    "CypherGlot currently does not admit named path patterns in "
                    "the supported subset.",
                    ctx=node,
                    construct="named_path",
                    hint="Bind the nodes and relationships you need with aliases instead.",
                )

        child_count = getattr(node, "getChildCount", None)
        if child_count is not None and child_count():
            stack.extend(node.children)
