from __future__ import annotations

import atexit
import os
import shutil
import socket
import subprocess
import sys
import time
import unittest
import uuid
from dataclasses import dataclass

try:
    import clickhouse_connect
except ImportError:  # pragma: no cover - optional test dependency
    clickhouse_connect = None  # type: ignore[assignment]


_CLICKHOUSE_HOST = os.environ.get("CYPHERGLOT_TEST_CLICKHOUSE_HOST", "")
_CLICKHOUSE_PORT = os.environ.get("CYPHERGLOT_TEST_CLICKHOUSE_PORT", "")
_CLICKHOUSE_IMAGE = os.environ.get(
    "CYPHERGLOT_TEST_CLICKHOUSE_IMAGE", "clickhouse/clickhouse-server:24.8"
)
_CLICKHOUSE_DATABASE = "cypherglot_test"
_CLICKHOUSE_USER = "default"
_CLICKHOUSE_PASSWORD = os.environ.get(
    "CYPHERGLOT_TEST_CLICKHOUSE_PASSWORD", "cypherglot"
)
_STARTUP_TIMEOUT_SECONDS = 60.0
_AUTO_DOCKER_ENV = "CYPHERGLOT_TEST_CLICKHOUSE_AUTO_DOCKER"


@dataclass(slots=True)
class ClickHouseConnectionParams:
    host: str
    port: int
    username: str
    password: str
    database: str


@dataclass(slots=True)
class _ManagedClickHouseRuntime:
    params: ClickHouseConnectionParams
    container_name: str | None = None


@dataclass(slots=True)
class _RuntimeState:
    runtime: _ManagedClickHouseRuntime | None = None
    refcount: int = 0


_STATE = _RuntimeState()


def acquire_clickhouse_test_params() -> ClickHouseConnectionParams:
    if clickhouse_connect is None:
        raise unittest.SkipTest("clickhouse-connect is not installed")

    if _STATE.runtime is None:
        _STATE.runtime = _create_clickhouse_test_runtime()

    _STATE.refcount += 1
    return _STATE.runtime.params


def release_clickhouse_test_params() -> None:
    if _STATE.runtime is None:
        return

    _STATE.refcount -= 1
    if _STATE.refcount > 0:
        return

    _stop_runtime(_STATE.runtime)
    _STATE.runtime = None
    _STATE.refcount = 0


def _create_clickhouse_test_runtime() -> _ManagedClickHouseRuntime:
    if _CLICKHOUSE_HOST and _CLICKHOUSE_PORT:
        params = ClickHouseConnectionParams(
            host=_CLICKHOUSE_HOST,
            port=int(_CLICKHOUSE_PORT),
            username=_CLICKHOUSE_USER,
            password=_CLICKHOUSE_PASSWORD,
            database="default",
        )
        _wait_for_clickhouse(params)
        _ensure_database(params)
        return _ManagedClickHouseRuntime(params=_with_database(params))

    skip_reason = _auto_docker_skip_reason()
    if skip_reason is not None:
        raise unittest.SkipTest(skip_reason)

    docker = shutil.which("docker")
    if docker is None:
        raise unittest.SkipTest(
            "CYPHERGLOT_TEST_CLICKHOUSE_HOST is not set and docker is not available"
        )

    _require_docker_daemon(docker)

    port = _find_free_tcp_port()
    container_name = f"cypherglot-clickhouse-test-{os.getpid()}-{uuid.uuid4().hex[:8]}"

    result = subprocess.run(
        [
            docker,
            "run",
            "--detach",
            "--rm",
            "--name",
            container_name,
            "--publish",
            f"127.0.0.1:{port}:8123",
            # ClickHouse wants a higher nofile limit; the default image warns
            # without it but still runs, so we keep the invocation minimal.
            "--ulimit",
            "nofile=262144:262144",
            # 24.x images require the default user to have an explicit password.
            "--env",
            f"CLICKHOUSE_PASSWORD={_CLICKHOUSE_PASSWORD}",
            "--env",
            "CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT=1",
            _CLICKHOUSE_IMAGE,
        ],
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode != 0:
        raise unittest.SkipTest(
            "Unable to start disposable ClickHouse docker container: "
            f"{result.stderr.strip() or result.stdout.strip() or 'unknown error'}"
        )

    base_params = ClickHouseConnectionParams(
        host="127.0.0.1",
        port=port,
        username=_CLICKHOUSE_USER,
        password=_CLICKHOUSE_PASSWORD,
        database="default",
    )
    runtime = _ManagedClickHouseRuntime(
        params=_with_database(base_params),
        container_name=container_name,
    )
    try:
        _wait_for_clickhouse(base_params)
        _ensure_database(base_params)
    except Exception:
        _stop_runtime(runtime)
        raise

    atexit.register(_stop_runtime, runtime)
    return runtime


def _with_database(
    params: ClickHouseConnectionParams,
) -> ClickHouseConnectionParams:
    return ClickHouseConnectionParams(
        host=params.host,
        port=params.port,
        username=params.username,
        password=params.password,
        database=_CLICKHOUSE_DATABASE,
    )


def _ensure_database(params: ClickHouseConnectionParams) -> None:
    assert clickhouse_connect is not None
    client = clickhouse_connect.get_client(
        host=params.host,
        port=params.port,
        username=params.username,
        password=params.password,
    )
    try:
        client.command(f"CREATE DATABASE IF NOT EXISTS {_CLICKHOUSE_DATABASE}")
    finally:
        client.close()


def _auto_docker_skip_reason() -> str | None:
    auto_docker = os.environ.get(_AUTO_DOCKER_ENV)
    if auto_docker is not None:
        if auto_docker.strip().lower() in {"1", "true", "yes", "on"}:
            return None
        return (
            f"CYPHERGLOT_TEST_CLICKHOUSE_HOST is not set and {_AUTO_DOCKER_ENV} "
            "disabled disposable docker ClickHouse"
        )

    if sys.platform.startswith("linux"):
        return None

    return (
        "CYPHERGLOT_TEST_CLICKHOUSE_HOST is not set and disposable docker "
        "ClickHouse auto-start is only enabled by default on Linux; set "
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

    raise unittest.SkipTest(
        "CYPHERGLOT_TEST_CLICKHOUSE_HOST is not set and docker is not ready: "
        f"{result.stderr.strip() or result.stdout.strip() or 'unknown error'}"
    )


def _wait_for_clickhouse(params: ClickHouseConnectionParams) -> None:
    assert clickhouse_connect is not None

    deadline = time.monotonic() + _STARTUP_TIMEOUT_SECONDS
    last_error: Exception | None = None
    while time.monotonic() < deadline:
        try:
            client = clickhouse_connect.get_client(
                host=params.host,
                port=params.port,
                username=params.username,
                password=params.password,
            )
            try:
                client.query("SELECT 1")
            finally:
                client.close()
            return
        except Exception as error:  # pragma: no cover - timing-dependent
            last_error = error
            time.sleep(0.5)

    message = "ClickHouse server is not reachable"
    if last_error is not None:
        message = f"{message}: {last_error}"
    raise unittest.SkipTest(message)


def _stop_runtime(runtime: _ManagedClickHouseRuntime) -> None:
    if runtime.container_name is None:
        return

    docker = shutil.which("docker")
    if docker is None:
        return

    subprocess.run(
        [docker, "rm", "-f", "-v", runtime.container_name],
        capture_output=True,
        text=True,
        check=False,
    )


def _find_free_tcp_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        sock.listen(1)
        return int(sock.getsockname()[1])
