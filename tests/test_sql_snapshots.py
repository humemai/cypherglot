"""Golden-SQL divergence detector over the benchmark corpus.

Fails when the compiler (or a dependency such as sqlglot) changes the SQL
emitted for any benchmark corpus query. A failure is not necessarily a bug:
it means a deliberate review is required, because changed SQL invalidates any
benchmark measurements taken with the previous output.

To accept a reviewed change:
    uv run python scripts/dev/sql_snapshot.py --write
then review `git diff tests/snapshots/benchmark_corpus_sql.json` and commit
the snapshot together with the change that caused it.
"""

from __future__ import annotations

import json
import sys
import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.dev.sql_snapshot import SNAPSHOT_PATH, build_snapshot  # noqa: E402


class SQLSnapshotTests(unittest.TestCase):
    maxDiff = None

    def test_corpus_sql_matches_committed_snapshot(self) -> None:
        self.assertTrue(
            SNAPSHOT_PATH.exists(),
            f"missing snapshot {SNAPSHOT_PATH}; generate it with "
            "`uv run python scripts/dev/sql_snapshot.py --write`",
        )
        committed = json.loads(SNAPSHOT_PATH.read_text(encoding="utf-8"))
        current = build_snapshot()

        changed = sorted(
            key
            for key in current
            if key in committed and committed[key] != current[key]
        )
        added = sorted(key for key in current if key not in committed)
        removed = sorted(key for key in committed if key not in current)
        if changed or added or removed:
            detail = "\n".join(
                [
                    f"  changed: {key}" for key in changed[:10]
                ]
                + [f"  added:   {key}" for key in added[:10]]
                + [f"  removed: {key}" for key in removed[:10]]
            )
            self.fail(
                "Emitted SQL diverged from the committed snapshot "
                f"({len(changed)} changed, {len(added)} added, "
                f"{len(removed)} removed):\n{detail}\n\n"
                "If this change is deliberate, regenerate with\n"
                "  uv run python scripts/dev/sql_snapshot.py --write\n"
                "and REVIEW `git diff tests/snapshots/` before committing — "
                "changed cells invalidate benchmark measurements taken with "
                "the previous SQL."
            )


if __name__ == "__main__":
    unittest.main()
