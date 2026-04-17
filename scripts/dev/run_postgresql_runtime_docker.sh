#!/usr/bin/env sh
set -eu

script_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
repo_root=$(CDPATH= cd -- "$script_dir/../.." && pwd)

docker_bin=${DOCKER_BIN:-docker}
image_name=${CYPHERGLOT_POSTGRES_TEST_IMAGE:-postgres:16-alpine}
container_name=${CYPHERGLOT_POSTGRES_TEST_CONTAINER:-cypherglot-postgres-test}
host_port=${CYPHERGLOT_POSTGRES_TEST_PORT:-55432}
db_name=${CYPHERGLOT_POSTGRES_TEST_DB:-cypherglot_test}
db_user=${CYPHERGLOT_POSTGRES_TEST_USER:-cypherglot}
db_password=${CYPHERGLOT_POSTGRES_TEST_PASSWORD:-cypherglot}
startup_timeout=${CYPHERGLOT_POSTGRES_TEST_STARTUP_TIMEOUT:-30}

cleanup() {
    "$docker_bin" rm -f "$container_name" >/dev/null 2>&1 || true
}

trap cleanup EXIT INT TERM

cleanup

"$docker_bin" run -d \
    --name "$container_name" \
    -e POSTGRES_DB="$db_name" \
    -e POSTGRES_USER="$db_user" \
    -e POSTGRES_PASSWORD="$db_password" \
    -p "$host_port:5432" \
    "$image_name" >/dev/null

i=0
while :
do
    if "$docker_bin" logs "$container_name" 2>&1 | grep -q "PostgreSQL init process complete; ready for start up." \
        && "$docker_bin" exec "$container_name" pg_isready -U "$db_user" -d "$db_name" >/dev/null 2>&1
    then
        break
    fi
    i=$((i + 1))
    if [ "$i" -ge "$startup_timeout" ]
    then
        echo "PostgreSQL test container did not become ready in time." >&2
        "$docker_bin" logs "$container_name" >&2 || true
        exit 1
    fi
    sleep 1
done

export CYPHERGLOT_TEST_POSTGRES_DSN="dbname=$db_name user=$db_user password=$db_password host=127.0.0.1 port=$host_port"

cd "$repo_root"
exec uv run --group test pytest tests/test_postgresql_runtime.py "$@"