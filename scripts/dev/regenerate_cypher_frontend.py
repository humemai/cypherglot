"""Internal helper for Docker-backed Cypher frontend regeneration."""

from __future__ import annotations

import argparse
import os
from pathlib import Path
import shutil
import subprocess
import tempfile


EXPECTED_FILES = (
    "Cypher.interp",
    "Cypher.tokens",
    "CypherLexer.interp",
    "CypherLexer.py",
    "CypherLexer.tokens",
    "CypherParser.py",
    "CypherVisitor.py",
)


def main() -> int:
    """Regenerate or verify the checked-in Cypher ANTLR artifacts."""

    parser = argparse.ArgumentParser(
        description=(
            "Regenerate the checked-in ANTLR Python artifacts for the "
            "CypherGlot frontend. This helper is intended to be invoked by the "
            "Docker wrapper."
        )
    )
    parser.add_argument(
        "--check",
        action="store_true",
        help="Only verify whether checked-in artifacts are up to date.",
    )
    args = parser.parse_args()

    if not Path("/.dockerenv").exists() or (
        os.environ.get("CYPHERGLOT_CYPHER_FRONTEND_REGEN_IN_DOCKER") != "1"
    ):
        raise SystemExit(
            "This helper is intended to run only inside the Docker regeneration "
            "wrapper. Use scripts/dev/regenerate_cypher_frontend_docker.sh."
        )

    repo_root = Path(__file__).resolve().parents[2]
    grammar_path = repo_root / "src/cypherglot/grammar/Cypher.g4"
    generated_dir = repo_root / "src/cypherglot/generated"
    antlr_jar_path = Path("/opt/antlr/antlr-4.13.2-complete.jar")
    java_executable = "java"

    if not grammar_path.is_file():
        raise SystemExit(f"Missing grammar file: {grammar_path}")

    if not antlr_jar_path.is_file():
        raise SystemExit(f"ANTLR jar not found: {antlr_jar_path}")

    with tempfile.TemporaryDirectory(prefix="cypherglot-cypher-antlr-") as tmpdir_str:
        tmpdir = Path(tmpdir_str)
        command = [
            java_executable,
            "-jar",
            str(antlr_jar_path),
            "-Dlanguage=Python3",
            "-visitor",
            "-o",
            str(tmpdir),
            str(grammar_path),
        ]
        subprocess.run(command, check=True)

        generated_root = _find_generated_root(tmpdir)
        if args.check:
            return _check_generated_files(generated_root, generated_dir)

        for filename in EXPECTED_FILES:
            shutil.copy2(generated_root / filename, generated_dir / filename)

    print(
        "Regenerated "
        f"{len(EXPECTED_FILES)} Cypher frontend artifacts in {generated_dir}"
    )
    return 0


def _find_generated_root(tmpdir: Path) -> Path:
    """Locate the generated artifact directory inside the ANTLR output tree."""

    parser_file = next(tmpdir.rglob("CypherParser.py"), None)
    if parser_file is None:
        raise SystemExit("ANTLR generation did not produce CypherParser.py")
    generated_root = parser_file.parent
    missing = [name for name in EXPECTED_FILES if not (generated_root / name).is_file()]
    if missing:
        raise SystemExit(
            "ANTLR generation produced an incomplete artifact set; missing: "
            + ", ".join(sorted(missing))
        )
    return generated_root


def _check_generated_files(generated_root: Path, checked_in_dir: Path) -> int:
    """Compare freshly generated artifacts against the checked-in copies."""

    stale: list[str] = []
    missing: list[str] = []

    for filename in EXPECTED_FILES:
        checked_in_file = checked_in_dir / filename
        fresh_file = generated_root / filename
        if not checked_in_file.is_file():
            missing.append(filename)
            continue
        if checked_in_file.read_bytes() != fresh_file.read_bytes():
            stale.append(filename)

    if not stale and not missing:
        print("Checked-in Cypher frontend artifacts are up to date.")
        return 0

    if missing:
        print("Missing checked-in artifacts:")
        for filename in missing:
            print(f"- {filename}")
    if stale:
        print("Stale checked-in artifacts:")
        for filename in stale:
            print(f"- {filename}")
    return 1


if __name__ == "__main__":
    raise SystemExit(main())