from __future__ import annotations

import atexit
import os
import re
import shutil
import socket
import subprocess
import sys
import time
import uuid
from dataclasses import dataclass

try:
    import psycopg2
except ImportError:  # pragma: no cover - optional dependency
    psycopg2 = None  # type: ignore[assignment]


_POSTGRES_DSN_ENV = "CYPHERGLOT_TEST_POSTGRES_DSN"
_POSTGRES_IMAGE_ENV = "CYPHERGLOT_BENCHMARK_POSTGRES_IMAGE"
_AUTO_DOCKER_ENV = "CYPHERGLOT_BENCHMARK_POSTGRES_AUTO_DOCKER"
_POSTGRES_IMAGE = os.environ.get(_POSTGRES_IMAGE_ENV, "postgres:16")
_POSTGRES_DB = "cypherglot_benchmark"
_POSTGRES_USER = "cypherglot"
_POSTGRES_PASSWORD = "cypherglot"
_STARTUP_TIMEOUT_SECONDS = 30.0
_DOCKER_MEMORY_PATTERN = re.compile(r"^\s*([0-9]+(?:\.[0-9]+)?)\s*([A-Za-z]+)\s*$")
_BYTE_UNITS = {
    "b": 1.0 / (1024.0 * 1024.0),
    "kb": 1000.0 / (1024.0 * 1024.0),
    "kib": 1.0 / 1024.0,
    "mb": 1_000_000.0 / (1024.0 * 1024.0),
    "mib": 1.0,
    "gb": 1_000_000_000.0 / (1024.0 * 1024.0),
    "gib": 1024.0,
}


@dataclass(slots=True)
class _ManagedPostgresRuntime:
    dsn: str
    container_name: str | None = None


@dataclass(slots=True)
class _RuntimeState:
    runtime: _ManagedPostgresRuntime | None = None
    refcount: int = 0


_STATE = _RuntimeState()


def acquire_postgresql_benchmark_dsn() -> str:
    if psycopg2 is None:
        raise ValueError("psycopg2 is not installed")

    if _STATE.runtime is None:
        _STATE.runtime = _create_postgresql_runtime()

    _STATE.refcount += 1
    return _STATE.runtime.dsn


def release_postgresql_benchmark_dsn() -> None:
    if _STATE.runtime is None:
        return

    _STATE.refcount -= 1
    if _STATE.refcount > 0:
        return

    _stop_runtime(_STATE.runtime)
    _STATE.runtime = None
    _STATE.refcount = 0


def postgresql_benchmark_server_rss_mib(dsn: str | None = None) -> float | None:
    runtime = _STATE.runtime
    if runtime is None or runtime.container_name is None:
        return None
    if dsn is not None and runtime.dsn != dsn:
        return None

    docker = shutil.which("docker")
    if docker is None:
        return None

    result = subprocess.run(
        [
            docker,
            "stats",
            "--no-stream",
            "--format",
            "{{.MemUsage}}",
            runtime.container_name,
        ],
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode != 0:
        return None

    usage = result.stdout.strip().split("/", 1)[0].strip()
    return _parse_docker_memory_to_mib(usage)


def _create_postgresql_runtime() -> _ManagedPostgresRuntime:
    configured_dsn = os.environ.get(_POSTGRES_DSN_ENV, "").strip()
    if configured_dsn:
        _wait_for_postgresql(configured_dsn)
        return _ManagedPostgresRuntime(dsn=configured_dsn)

    skip_reason = _auto_docker_skip_reason()
    if skip_reason is not None:
        raise ValueError(skip_reason)

    docker = shutil.which("docker")
    if docker is None:
        raise ValueError(
            f"{_POSTGRES_DSN_ENV} is not set and docker is not available"
        )

    _require_docker_daemon(docker)

    port = _find_free_tcp_port()
    container_name = (
        f"cypherglot-postgres-benchmark-{os.getpid()}-{uuid.uuid4().hex[:8]}"
    )
    dsn = (
        "postgresql://"
        f"{_POSTGRES_USER}:{_POSTGRES_PASSWORD}@127.0.0.1:{port}/{_POSTGRES_DB}"
    )

    result = subprocess.run(
        [
            docker,
            "run",
            "--detach",
            "--rm",
            "--name",
            container_name,
            "--publish",
            f"127.0.0.1:{port}:5432",
            "--env",
            f"POSTGRES_DB={_POSTGRES_DB}",
            "--env",
            f"POSTGRES_USER={_POSTGRES_USER}",
            "--env",
            f"POSTGRES_PASSWORD={_POSTGRES_PASSWORD}",
            _POSTGRES_IMAGE,
        ],
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode != 0:
        raise ValueError(
            "Unable to start disposable PostgreSQL docker container: "
            f"{result.stderr.strip() or result.stdout.strip() or 'unknown error'}"
        )

    runtime = _ManagedPostgresRuntime(dsn=dsn, container_name=container_name)
    try:
        _wait_for_postgresql(dsn)
    except Exception:
        _stop_runtime(runtime)
        raise

    atexit.register(_stop_runtime, runtime)
    return runtime


def _auto_docker_skip_reason() -> str | None:
    auto_docker = os.environ.get(_AUTO_DOCKER_ENV)
    if auto_docker is not None:
        if auto_docker.strip().lower() in {"1", "true", "yes", "on"}:
            return None
        return (
            f"{_POSTGRES_DSN_ENV} is not set and {_AUTO_DOCKER_ENV} "
            "disabled disposable docker PostgreSQL"
        )

    if sys.platform.startswith("linux"):
        return None

    return (
        f"{_POSTGRES_DSN_ENV} is not set and disposable docker PostgreSQL "
        "auto-start is only enabled by default on Linux; set "
        f"{_AUTO_DOCKER_ENV}=1 to force it on this platform"
    )


def _require_docker_daemon(docker: str) -> None:
    result = subprocess.run(
        [docker, "info"],
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode == 0:
        return

    raise ValueError(
        f"{_POSTGRES_DSN_ENV} is not set and docker is not ready: "
        f"{result.stderr.strip() or result.stdout.strip() or 'unknown error'}"
    )


def _wait_for_postgresql(dsn: str) -> None:
    assert psycopg2 is not None

    deadline = time.monotonic() + _STARTUP_TIMEOUT_SECONDS
    last_error: Exception | None = None
    while time.monotonic() < deadline:
        try:
            conn = psycopg2.connect(dsn)
            try:
                with conn.cursor() as cur:
                    cur.execute("SELECT 1")
                    cur.fetchone()
            finally:
                conn.close()
            return
        except psycopg2.Error as error:  # pragma: no cover - timing-dependent
            last_error = error
            time.sleep(0.5)

    message = "PostgreSQL server is not reachable"
    if last_error is not None:
        message = f"{message}: {last_error}"
    raise ValueError(message)


def _stop_runtime(runtime: _ManagedPostgresRuntime) -> None:
    if runtime.container_name is None:
        return

    docker = shutil.which("docker")
    if docker is None:
        return

    subprocess.run(
        [docker, "rm", "-f", runtime.container_name],
        capture_output=True,
        text=True,
        check=False,
    )


def _find_free_tcp_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        sock.listen(1)
        return int(sock.getsockname()[1])


def _parse_docker_memory_to_mib(value: str) -> float | None:
    match = _DOCKER_MEMORY_PATTERN.match(value)
    if match is None:
        return None

    amount = float(match.group(1))
    unit = match.group(2).lower()
    multiplier = _BYTE_UNITS.get(unit)
    if multiplier is None:
        return None
    return amount * multiplier
