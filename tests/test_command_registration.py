from __future__ import annotations

import sys
from pathlib import Path
from unittest import TestCase

from typer.testing import CliRunner

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = PROJECT_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from cluster_dispatch.cli import app


class TestCommandRegistration(TestCase):
    def test_root_help_lists_command_families(self) -> None:
        result = CliRunner().invoke(app, ["--help"])
        self.assertEqual(result.exit_code, 0, result.output)
        for command in ("target", "analysis", "sync", "profile", "status", "run", "sweep"):
            with self.subTest(command=command):
                self.assertIn(command, result.output)

    def test_each_command_family_help_resolves(self) -> None:
        runner = CliRunner()
        for command in ("target", "analysis", "sync", "profile", "status", "run", "sweep"):
            with self.subTest(command=command):
                result = runner.invoke(app, [command, "--help"])
                self.assertEqual(result.exit_code, 0, result.output)
                self.assertIn("Usage:", result.output)
