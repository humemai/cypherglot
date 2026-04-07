from __future__ import annotations

import unittest

import cypherglot


class ImportTest(unittest.TestCase):
    def test_version_is_exposed(self) -> None:
        self.assertIsInstance(cypherglot.__version__, str)
        self.assertTrue(cypherglot.__version__)


if __name__ == "__main__":
    unittest.main()
