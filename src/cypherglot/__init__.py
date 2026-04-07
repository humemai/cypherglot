from __future__ import annotations

from importlib.metadata import PackageNotFoundError, version


def _detect_version() -> str:
    try:
        return version("cypherglot")
    except PackageNotFoundError:
        return "0.0.0"


__version__ = _detect_version()

__all__ = ["__version__"]
