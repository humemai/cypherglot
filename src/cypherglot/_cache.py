"""Bounded LRU cache for rendered SQL output.

CypherGlot caches only at the string-rendering entrypoints (``to_sql`` and
``render_cypher_program_text``): the cached values are immutable, so sharing
them between callers is always safe. The AST-producing entrypoints are never
cached — sqlglot expressions are mutable and a shared instance could be
corrupted by one caller and silently served to the next.

Set ``CYPHERGLOT_COMPILE_CACHE=0`` in the environment to bypass the cache
without code changes.
"""

from __future__ import annotations

import os
from collections import OrderedDict
from threading import Lock
from typing import Any, Hashable, NamedTuple

_ENV_FLAG = "CYPHERGLOT_COMPILE_CACHE"
_DEFAULT_MAXSIZE = 512


class CompileCacheInfo(NamedTuple):
    """Snapshot of compile-cache statistics (see ``compile_cache_info``)."""

    hits: int
    misses: int
    maxsize: int
    currsize: int
    enabled: bool


class _CompileCache:
    def __init__(self, *, maxsize: int = _DEFAULT_MAXSIZE) -> None:
        self._lock = Lock()
        self._entries: OrderedDict[Hashable, Any] = OrderedDict()
        self._maxsize = maxsize
        self._enabled = True
        self._hits = 0
        self._misses = 0

    def _active(self) -> bool:
        return self._enabled and os.environ.get(_ENV_FLAG, "1") != "0"

    def get(self, key: Hashable) -> Any | None:
        """Return the cached value for ``key``, or None on a miss.

        Cached values are never None, so None unambiguously means "miss".
        """

        if not self._active():
            return None
        with self._lock:
            try:
                value = self._entries[key]
            except (KeyError, TypeError):
                # TypeError: unhashable key component — treat as uncacheable.
                self._misses += 1
                return None
            self._entries.move_to_end(key)
            self._hits += 1
            return value

    def put(self, key: Hashable, value: Any) -> None:
        if not self._active():
            return
        with self._lock:
            try:
                self._entries[key] = value
            except TypeError:
                return
            self._entries.move_to_end(key)
            while len(self._entries) > self._maxsize:
                self._entries.popitem(last=False)

    def configure(
        self,
        *,
        enabled: bool | None = None,
        maxsize: int | None = None,
    ) -> None:
        with self._lock:
            if enabled is not None:
                self._enabled = enabled
            if maxsize is not None:
                if maxsize < 1:
                    raise ValueError("compile cache maxsize must be >= 1")
                self._maxsize = maxsize
                while len(self._entries) > self._maxsize:
                    self._entries.popitem(last=False)

    def clear(self) -> None:
        with self._lock:
            self._entries.clear()
            self._hits = 0
            self._misses = 0

    def info(self) -> CompileCacheInfo:
        with self._lock:
            return CompileCacheInfo(
                hits=self._hits,
                misses=self._misses,
                maxsize=self._maxsize,
                currsize=len(self._entries),
                enabled=self._active(),
            )


_CACHE = _CompileCache()


def set_compile_cache(
    *,
    enabled: bool | None = None,
    maxsize: int | None = None,
) -> None:
    """Configure the transparent compile cache.

    ``enabled=False`` turns caching off (every render recompiles);
    ``maxsize`` bounds the number of retained entries (LRU eviction).
    """

    _CACHE.configure(enabled=enabled, maxsize=maxsize)


def clear_compile_cache() -> None:
    """Drop all cached entries and reset hit/miss statistics."""

    _CACHE.clear()


def compile_cache_info() -> CompileCacheInfo:
    """Return current cache statistics (hits, misses, maxsize, currsize)."""

    return _CACHE.info()
