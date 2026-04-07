#!/usr/bin/env bash

set -euo pipefail

if [[ "${1:-}" == "--check" ]]; then
  echo "No generated Cypher frontend artifacts are checked in yet; nothing to verify."
  exit 0
fi

echo "No generated Cypher frontend artifacts are checked in yet; nothing to regenerate."
