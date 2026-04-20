"""SQLite runtime benchmark entrypoint over the shared SQL-runtime core."""

from __future__ import annotations

from _benchmark_sql_runtime_core import SQLITE_ENTRYPOINT, main as _shared_main


def main() -> int:
    return _shared_main(SQLITE_ENTRYPOINT)


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
