"""Write-family validation helpers extracted from validate.py."""

from __future__ import annotations

from ._normalize_support import (
    _looks_like_relationship_pattern,
    _parse_node_pattern,
    _parse_relationship_pattern,
    _split_comma_separated,
    _split_relationship_pattern,
    _unwrap_node_pattern,
    _validate_match_create_relationship_between_nodes_endpoints,
    _validate_match_merge_relationship_endpoints,
)
from ._validate_shape_helpers import (
    _context_text,
    _extract_single_match_source_nodes,
    _validate_match_pattern_shape,
    _validate_traversal_write_endpoints,
)
from .parser import CypherParseResult


def _validate_standalone_write_shape(
    result: CypherParseResult,
    single_part_query_ctx,
    *,
    updating_clauses,
    reading_clauses,
    return_ctx,
) -> None:
    update_ctx = updating_clauses[0]
    if return_ctx is not None:
        raise ValueError(
            "CypherGlot currently validates only CREATE and narrow MERGE statements in the "
            "write subset."
        )

    create_ctx = update_ctx.oC_Create()
    merge_ctx = update_ctx.oC_Merge()
    if create_ctx is None and merge_ctx is None:
        raise ValueError(
            "CypherGlot currently validates only CREATE and narrow MERGE statements in the "
            "write subset."
        )

    if merge_ctx is None:
        return

    if merge_ctx.oC_MergeAction():
        raise ValueError(
            "CypherGlot currently validates MERGE only without ON CREATE or ON MATCH actions."
        )

    merge_pattern_text = _context_text(result, merge_ctx.oC_PatternPart())
    if _looks_like_relationship_pattern(merge_pattern_text):
        left_text, relationship_text, right_text, direction = _split_relationship_pattern(
            merge_pattern_text
        )
        _parse_node_pattern(
            left_text,
            require_label=True,
            default_alias="__humem_validate_merge_left_node",
        )
        _parse_relationship_pattern(relationship_text, direction)
        _parse_node_pattern(
            right_text,
            require_label=True,
            default_alias="__humem_validate_merge_right_node",
        )
        return

    _parse_node_pattern(
        _unwrap_node_pattern(merge_pattern_text),
        require_label=True,
        default_alias="__humem_validate_merge_node",
    )


def _validate_mixed_read_write_shape(
    result: CypherParseResult,
    single_part_query_ctx,
    *,
    reading_clauses,
    updating_clauses,
    return_ctx,
) -> None:
    read_ctx = reading_clauses[0]
    update_ctx = updating_clauses[0]
    if read_ctx.oC_Match() is None or return_ctx is not None:
        raise ValueError(
            "CypherGlot currently validates only MATCH ... SET, MATCH ... "
            "CREATE, and narrow MATCH ... DELETE statements in the mixed "
            "read-write subset."
        )

    if (
        update_ctx.oC_Set() is None
        and update_ctx.oC_Create() is None
        and update_ctx.oC_Merge() is None
        and update_ctx.oC_Delete() is None
    ):
        raise ValueError(
            "CypherGlot currently validates only MATCH ... SET, MATCH ... "
            "CREATE, MATCH ... MERGE, and narrow MATCH ... DELETE statements in the mixed "
            "read-write subset."
        )

    merge_ctx = update_ctx.oC_Merge()
    if merge_ctx is not None:
        if merge_ctx.oC_MergeAction():
            raise ValueError(
                "CypherGlot currently validates MERGE only without ON CREATE or ON MATCH actions."
            )

        merge_pattern_text = _context_text(result, merge_ctx.oC_PatternPart())
        if not _looks_like_relationship_pattern(merge_pattern_text):
            raise ValueError(
                "CypherGlot currently validates MATCH ... MERGE only for one directed relationship pattern in the MERGE clause."
            )

        match_pattern_text = _context_text(result, read_ctx.oC_Match().oC_Pattern())
        match_patterns = _split_comma_separated(match_pattern_text)
        left_text, relationship_text, right_text, direction = _split_relationship_pattern(
            merge_pattern_text
        )
        left = _parse_node_pattern(
            left_text,
            default_alias="__humem_validate_match_merge_left_node",
        )
        _parse_relationship_pattern(relationship_text, direction)
        right = _parse_node_pattern(
            right_text,
            default_alias="__humem_validate_match_merge_right_node",
        )
        if len(match_patterns) == 1 and _looks_like_relationship_pattern(match_patterns[0]):
            _validate_traversal_write_endpoints(
                _extract_single_match_source_nodes(result, read_ctx.oC_Match()),
                left,
                right,
                allow_one_new_endpoint=True,
            )
        elif len(match_patterns) == 1:
            match_node = _parse_node_pattern(
                _unwrap_node_pattern(match_patterns[0]),
                default_alias="__humem_validate_match_merge_node",
            )
            _validate_match_merge_relationship_endpoints(
                match_node,
                left,
                right,
            )
        else:
            if len(match_patterns) != 2 or any(
                _looks_like_relationship_pattern(pattern) for pattern in match_patterns
            ):
                raise ValueError(
                    "CypherGlot currently validates MATCH ... MERGE only with two disconnected matched node patterns before MERGE, or one matched relationship or fixed-length chain source whose node aliases are reused by MERGE."
                )
            left_match = _parse_node_pattern(
                _unwrap_node_pattern(match_patterns[0]),
                default_alias="__humem_validate_match_merge_left_match_node",
            )
            right_match = _parse_node_pattern(
                _unwrap_node_pattern(match_patterns[1]),
                default_alias="__humem_validate_match_merge_right_match_node",
            )
            _validate_match_create_relationship_between_nodes_endpoints(
                left_match,
                right_match,
                left,
                right,
            )

    create_ctx = update_ctx.oC_Create()
    if create_ctx is not None:
        create_pattern_text = _context_text(result, create_ctx.oC_Pattern())
        if _looks_like_relationship_pattern(create_pattern_text):
            match_pattern_text = _context_text(result, read_ctx.oC_Match().oC_Pattern())
            match_patterns = _split_comma_separated(match_pattern_text)
            if len(match_patterns) == 1 and _looks_like_relationship_pattern(match_patterns[0]):
                left_text, _relationship_text, right_text, _direction = _split_relationship_pattern(
                    create_pattern_text
                )
                left = _parse_node_pattern(
                    left_text,
                    default_alias="__humem_validate_match_create_left_node",
                )
                right = _parse_node_pattern(
                    right_text,
                    default_alias="__humem_validate_match_create_right_node",
                )
                _validate_traversal_write_endpoints(
                    _extract_single_match_source_nodes(result, read_ctx.oC_Match()),
                    left,
                    right,
                    allow_one_new_endpoint=True,
                )

    _validate_match_pattern_shape(
        result,
        read_ctx.oC_Match(),
        allow_two_node_disconnected=(
            update_ctx.oC_Create() is not None or update_ctx.oC_Merge() is not None
        ),
        allow_multi_hop=(
            update_ctx.oC_Create() is not None or update_ctx.oC_Merge() is not None
        ),
    )
