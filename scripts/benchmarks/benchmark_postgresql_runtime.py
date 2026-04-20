"""PostgreSQL runtime benchmark entrypoint over the shared SQL-runtime core."""

from __future__ import annotations

from _benchmark_sql_runtime_core import POSTGRESQL_ENTRYPOINT, main as _shared_main


def main() -> int:
    return _shared_main(POSTGRESQL_ENTRYPOINT)


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
