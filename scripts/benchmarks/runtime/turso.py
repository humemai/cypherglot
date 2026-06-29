"""Turso runtime benchmark entrypoint over the shared SQL-runtime core."""

from __future__ import annotations

from scripts.benchmarks.common.runtime_core import TURSO_ENTRYPOINT, main as _shared_main


def main() -> int:
    return _shared_main(TURSO_ENTRYPOINT)


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
