from __future__ import annotations

import unittest
from unittest import mock

from scripts.benchmarks.common import postgres_runtime_support


class PostgresRuntimeSupportTests(unittest.TestCase):
    def test_stop_runtime_removes_attached_volumes(self) -> None:
        runtime = postgres_runtime_support._ManagedPostgresRuntime(
            dsn="postgresql://cypherglot:cypherglot@127.0.0.1:5432/cypherglot_benchmark",
            container_name="cypherglot-postgres-test",
        )

        with mock.patch.object(
            postgres_runtime_support.shutil,
            "which",
            return_value="/usr/bin/docker",
        ), mock.patch.object(postgres_runtime_support.subprocess, "run") as run:
            postgres_runtime_support._stop_runtime(runtime)

        run.assert_called_once_with(
            ["/usr/bin/docker", "rm", "-f", "-v", "cypherglot-postgres-test"],
            capture_output=True,
            text=True,
            check=False,
        )