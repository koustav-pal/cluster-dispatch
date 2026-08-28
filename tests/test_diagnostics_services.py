from __future__ import annotations

import sys
import tempfile
from pathlib import Path
from unittest import TestCase, mock

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = PROJECT_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from cluster_dispatch.config import ProjectConfig, TargetConfig, save_config
from cluster_dispatch.diagnostics import DiagnosticCheck, DiagnosticResult, diagnose_project, diagnose_target


class TestDiagnosticServices(TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory(prefix="cdp-diagnostics-")
        self.root = Path(self.tmp.name).resolve()
        self.target = TargetConfig("localhost", "none", str(self.root), transport="local")
        self.cfg = ProjectConfig("none", "local", {"local": self.target})
        save_config(self.root, self.cfg)

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def test_project_diagnostics_reuses_target_primitive(self) -> None:
        sentinel = DiagnosticResult([DiagnosticCheck("PASS", "scheduler", "none")])
        with mock.patch("cluster_dispatch.diagnostics.diagnose_target", return_value=sentinel) as primitive:
            result = diagnose_project(self.root, remote=False)
        primitive.assert_called_once()
        self.assertTrue(any(check.name == "target:local:scheduler" for check in result.checks))

    def test_local_target_diagnostics(self) -> None:
        result = diagnose_target(self.root, self.cfg, "local", self.target)
        self.assertTrue(any(check.name == "connectivity" and check.status == "PASS" for check in result.checks))

    def test_missing_scheduler_executable_warns(self) -> None:
        slurm = TargetConfig("localhost", "slurm", str(self.root), transport="local", template_header="\n".join(f"# {{{name}}}" for name in ("cpus", "memory", "time", "job_name", "stdout", "stderr", "working_dir")))
        with mock.patch("cluster_dispatch.diagnostics.shutil.which", return_value=None):
            result = diagnose_target(self.root, self.cfg, "slurm", slurm)
        self.assertTrue(any(check.name == "scheduler_cmds" and check.status == "WARN" for check in result.checks))
