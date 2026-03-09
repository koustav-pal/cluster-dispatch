from __future__ import annotations

import json
import os
import sys
import tempfile
import time
from contextlib import contextmanager
from pathlib import Path
from unittest import TestCase, mock

from typer.testing import CliRunner

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = PROJECT_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from cluster_dispatch.cli import app  # noqa: E402


@contextmanager
def _chdir(path: Path):
    previous = Path.cwd()
    os.chdir(path)
    try:
        yield
    finally:
        os.chdir(previous)


def _invoke(runner: CliRunner, cwd: Path, args: list[str]):
    with _chdir(cwd):
        return runner.invoke(app, args, catch_exceptions=False)


def _latest_job_record(project_dir: Path) -> dict:
    files = sorted((project_dir / ".cluster_dispatch" / "jobs").glob("*.json"))
    if not files:
        raise AssertionError("No job records found")
    return json.loads(files[-1].read_text())


class TestClusterDispatchFlows(TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory(prefix="cdp-tests-")
        self.base = Path(self.tmp.name)
        self.project = self.base / "proj"
        self.analysis_rel = Path("analysis/a")
        (self.project / self.analysis_rel).mkdir(parents=True, exist_ok=True)
        self.runner = CliRunner()

        init_result = _invoke(self.runner, self.base, ["init", str(self.project)])
        self.assertEqual(init_result.exit_code, 0, init_result.output)
        use_result = _invoke(self.runner, self.project, ["analysis", "use", str(self.analysis_rel)])
        self.assertEqual(use_result.exit_code, 0, use_result.output)

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def test_analysis_run_dry_run_no_side_effects(self) -> None:
        jobs_dir = self.project / ".cluster_dispatch" / "jobs"
        state_file = self.project / ".cluster_dispatch" / "state.json"
        before_jobs = list(jobs_dir.glob("*.json"))
        self.assertEqual(before_jobs, [])
        self.assertFalse(state_file.exists())

        result = _invoke(
            self.runner,
            self.project,
            ["analysis", "run", "--dry-run", "python", "-c", "print(1)"],
        )
        self.assertEqual(result.exit_code, 0, result.output)
        self.assertIn("Dry run: no changes will be made", result.output)

        after_jobs = list(jobs_dir.glob("*.json"))
        self.assertEqual(after_jobs, [])
        self.assertFalse(state_file.exists())

    def test_analysis_run_local_transport_uses_no_ssh(self) -> None:
        ssh_calls: list[list[str]] = []
        original_run = __import__("subprocess").run

        def _wrapped_run(*args, **kwargs):
            cmd = args[0] if args else kwargs.get("args")
            if isinstance(cmd, list) and cmd and cmd[0] == "ssh":
                ssh_calls.append(cmd)
            return original_run(*args, **kwargs)

        with mock.patch("subprocess.run", side_effect=_wrapped_run):
            result = _invoke(
                self.runner,
                self.project,
                ["analysis", "run", "python", "-c", "print(123)"],
            )
        self.assertEqual(result.exit_code, 0, result.output)
        self.assertEqual(ssh_calls, [], f"Unexpected SSH calls for local target: {ssh_calls}")

        record = _latest_job_record(self.project)
        self.assertEqual(record.get("target"), "local")
        self.assertEqual(record.get("scheduler"), "none")
        self.assertIn("resources", record)
        self.assertIn("paths", record)
        self.assertIn("sync", record)
        self.assertIn("environment", record)

        remote_log_file = Path(str(record["remote_log_file"]))
        for _ in range(40):
            if remote_log_file.exists():
                break
            time.sleep(0.05)
        self.assertTrue(remote_log_file.exists(), "Expected local run log file to exist")

    def test_history_reads_old_and_new_record_shapes(self) -> None:
        old_payload = {
            "submitted_at": "2026-01-01T00:00:00",
            "analysis": str(self.analysis_rel).replace("\\", "/"),
            "target": "local",
            "scheduler": "none",
            "job_id": "legacy-1",
            "job_name": "legacy-job",
            "state": "COMPLETED",
            "command": "echo legacy",
            "remote_run_dir": str(self.project / self.analysis_rel),
            "remote_log_file": str(self.project / self.analysis_rel / "legacy.log"),
        }
        jobs_dir = self.project / ".cluster_dispatch" / "jobs"
        jobs_dir.mkdir(parents=True, exist_ok=True)
        (jobs_dir / "legacy.json").write_text(json.dumps(old_payload, indent=2))

        run_result = _invoke(
            self.runner,
            self.project,
            ["analysis", "run", "python", "-c", "print(5)"],
        )
        self.assertEqual(run_result.exit_code, 0, run_result.output)

        history_result = _invoke(self.runner, self.project, ["history", "--limit", "20"])
        self.assertEqual(history_result.exit_code, 0, history_result.output)
        self.assertIn("legacy-1", history_result.output)
        self.assertIn("Job history:", history_result.output)

    def test_sweep_local_records_include_core_and_sweep_fields(self) -> None:
        sweep_yaml = self.project / "sweep.yml"
        sweep_yaml.write_text(
            "params:\n"
            "  job1:\n"
            "    lr: [0.1]\n"
            "    batch_size: [32]\n"
        )

        result = _invoke(
            self.runner,
            self.project,
            [
                "analysis",
                "sweep",
                "run",
                "--mode",
                "local",
                "--config",
                str(sweep_yaml),
                "python",
                "-c",
                "print({lr},{batch_size})",
            ],
        )
        self.assertEqual(result.exit_code, 0, result.output)

        record = _latest_job_record(self.project)
        for key in [
            "run_id",
            "job_id",
            "job_name",
            "target",
            "scheduler",
            "command",
            "submitted_at",
            "resources",
            "paths",
            "sync",
            "environment",
            "submission_mode",
            "sweep_id",
            "sweep_job",
            "sweep_index",
            "sweep_params",
        ]:
            self.assertIn(key, record)

    def test_sync_push_dry_run_no_side_effects(self) -> None:
        sync_dir = self.project / ".cluster_dispatch" / "sync"
        self.assertFalse(sync_dir.exists())

        result = _invoke(self.runner, self.project, ["sync", "push", "--dry-run"])
        self.assertEqual(result.exit_code, 0, result.output)
        self.assertIn("Dry run: no changes will be made", result.output)
        self.assertIn("Action: sync push", result.output)
        self.assertFalse(sync_dir.exists(), "Dry-run must not write sync records")

    def test_sync_push_writes_sync_event_record(self) -> None:
        result = _invoke(self.runner, self.project, ["sync", "push"])
        self.assertEqual(result.exit_code, 0, result.output)
        self.assertIn("Recorded sync event:", result.output)

        sync_dir = self.project / ".cluster_dispatch" / "sync"
        records = sorted(sync_dir.glob("*.json"))
        self.assertTrue(records, "Expected at least one sync event record")
        payload = json.loads(records[-1].read_text())
        self.assertEqual(payload.get("action"), "push")
        self.assertEqual(payload.get("target"), "local")
        self.assertIn("source", payload)
        self.assertIn("destination", payload)

    def test_target_remove_default_requires_force(self) -> None:
        template = self.project / "none.tmpl"
        template.write_text(
            "# job={job_name}\n"
            "# out={stdout}\n"
            "# err={stderr}\n"
            "# cpus={cpus}\n"
            "# mem={memory}\n"
            "# time={time}\n"
            "# wd={working_dir}\n"
            "# node={node}\n"
        )

        add_result = _invoke(
            self.runner,
            self.project,
            [
                "target",
                "add",
                "remote-a",
                "--transport",
                "ssh",
                "--host",
                "example.org",
                "--scheduler",
                "none",
                "--remote-root",
                "/tmp/remote-a",
                "--template-file",
                str(template),
            ],
        )
        self.assertEqual(add_result.exit_code, 0, add_result.output)
        set_result = _invoke(self.runner, self.project, ["target", "set", "remote-a"])
        self.assertEqual(set_result.exit_code, 0, set_result.output)

        blocked = _invoke(self.runner, self.project, ["target", "remove", "remote-a"])
        self.assertNotEqual(blocked.exit_code, 0)
        self.assertIn("currently the default target", blocked.output)

        forced = _invoke(self.runner, self.project, ["target", "remove", "remote-a", "--force"])
        self.assertEqual(forced.exit_code, 0, forced.output)
        self.assertIn("Removed target 'remote-a'", forced.output)

    def test_target_remove_prune_records(self) -> None:
        template = self.project / "none2.tmpl"
        template.write_text(
            "# job={job_name}\n"
            "# out={stdout}\n"
            "# err={stderr}\n"
            "# cpus={cpus}\n"
            "# mem={memory}\n"
            "# time={time}\n"
            "# wd={working_dir}\n"
            "# node={node}\n"
        )
        add_result = _invoke(
            self.runner,
            self.project,
            [
                "target",
                "add",
                "remote-b",
                "--transport",
                "ssh",
                "--host",
                "example.org",
                "--scheduler",
                "none",
                "--remote-root",
                "/tmp/remote-b",
                "--template-file",
                str(template),
            ],
        )
        self.assertEqual(add_result.exit_code, 0, add_result.output)

        jobs_dir = self.project / ".cluster_dispatch" / "jobs"
        jobs_dir.mkdir(parents=True, exist_ok=True)
        (jobs_dir / "fake_remote.json").write_text(
            json.dumps({"target": "remote-b", "job_id": "x1", "job_name": "n1"}, indent=2)
        )
        sync_dir = self.project / ".cluster_dispatch" / "sync"
        sync_dir.mkdir(parents=True, exist_ok=True)
        (sync_dir / "fake_remote.json").write_text(
            json.dumps({"target": "remote-b", "action": "push"}, indent=2)
        )

        blocked = _invoke(self.runner, self.project, ["target", "remove", "remote-b"])
        self.assertNotEqual(blocked.exit_code, 0)
        self.assertIn("Use --force to remove", blocked.output)

        removed = _invoke(
            self.runner,
            self.project,
            ["target", "remove", "remote-b", "--force", "--prune-records"],
        )
        self.assertEqual(removed.exit_code, 0, removed.output)
        self.assertFalse((jobs_dir / "fake_remote.json").exists())
        self.assertFalse((sync_dir / "fake_remote.json").exists())

    def test_retry_replays_most_recent_normal_run(self) -> None:
        first = _invoke(self.runner, self.project, ["analysis", "run", "python", "-c", "print(10)"])
        self.assertEqual(first.exit_code, 0, first.output)
        jobs_before = sorted((self.project / ".cluster_dispatch" / "jobs").glob("*.json"))
        self.assertEqual(len(jobs_before), 1)

        retry_result = _invoke(self.runner, self.project, ["retry"])
        self.assertEqual(retry_result.exit_code, 0, retry_result.output)
        self.assertIn("Submitted job_id=", retry_result.output)

        jobs_after = sorted((self.project / ".cluster_dispatch" / "jobs").glob("*.json"))
        self.assertEqual(len(jobs_after), 2)

    def test_retry_dry_run_has_no_side_effects(self) -> None:
        run_result = _invoke(self.runner, self.project, ["analysis", "run", "python", "-c", "print(11)"])
        self.assertEqual(run_result.exit_code, 0, run_result.output)
        jobs_before = sorted((self.project / ".cluster_dispatch" / "jobs").glob("*.json"))

        dry_result = _invoke(self.runner, self.project, ["retry", "--dry-run"])
        self.assertEqual(dry_result.exit_code, 0, dry_result.output)
        self.assertIn("Dry run: no changes will be made", dry_result.output)

        jobs_after = sorted((self.project / ".cluster_dispatch" / "jobs").glob("*.json"))
        self.assertEqual(len(jobs_before), len(jobs_after))
