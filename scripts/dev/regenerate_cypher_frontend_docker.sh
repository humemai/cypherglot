#!/usr/bin/env sh
set -eu

script_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
repo_root=$(CDPATH= cd -- "$script_dir/../.." && pwd)
docker_bin=${DOCKER_BIN:-docker}
image_name=${CYPHERGLOT_CYPHER_FRONTEND_REGEN_IMAGE:-cypherglot-cypher-frontend-regen:java25}

"$docker_bin" build \
    -f "$script_dir/cypher_frontend_regen.Dockerfile" \
    -t "$image_name" \
    "$script_dir"

exec "$docker_bin" run --rm \
    --user "$(id -u):$(id -g)" \
    -e HOME=/tmp \
    -e CYPHERGLOT_CYPHER_FRONTEND_REGEN_IN_DOCKER=1 \
    -v "$repo_root:/workspace" \
    -w /workspace \
    "$image_name" \
    python3 scripts/dev/regenerate_cypher_frontend.py \
    "$@"
