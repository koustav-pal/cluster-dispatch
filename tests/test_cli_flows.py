from __future__ import annotations

import json
import os
import sys
import tempfile
import time
import tarfile
from datetime import datetime, timedelta
from contextlib import contextmanager
from pathlib import Path
from unittest import TestCase, mock

from typer.testing import CliRunner

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = PROJECT_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from cluster_dispatch.cli import app, _build_submit_script  # noqa: E402
from cluster_dispatch.config import load_config  # noqa: E402
from cluster_dispatch.schedulers import get_adapter  # noqa: E402


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
        expected_remote_analysis = str((self.project / self.analysis_rel).resolve())
        self.assertIn(f"cd {expected_remote_analysis}", result.output)

        after_jobs = list(jobs_dir.glob("*.json"))
        self.assertEqual(after_jobs, [])
        self.assertFalse(state_file.exists())

    def test_analysis_run_dry_run_from_subdir_uses_matching_remote_working_dir(self) -> None:
        scripts_dir = self.project / self.analysis_rel / "scripts"
        scripts_dir.mkdir(parents=True, exist_ok=True)
        result = _invoke(
            self.runner,
            scripts_dir,
            ["analysis", "run", "--dry-run", "python", "-c", "print(1)"],
        )
        self.assertEqual(result.exit_code, 0, result.output)
        expected_remote_workdir = f"{(self.project / self.analysis_rel).resolve().as_posix()}/scripts"
        self.assertIn(f"cd {expected_remote_workdir}", result.output)

    def test_build_submit_script_places_scheduler_header_before_commands(self) -> None:
        script = _build_submit_script(
            header="#PBS -l walltime=01:00:00\n#PBS -N demo",
            command="echo hello",
            working_dir="/tmp/work",
            remote_log_file="/tmp/work/run.log",
        )
        lines = script.splitlines()
        self.assertEqual(lines[0], "#!/usr/bin/env bash")
        self.assertIn("#PBS -l walltime=01:00:00", lines[1:4])
        self.assertIn("set -euo pipefail", lines)
        # Ensure directives appear before first executable shell command.
        first_exec_idx = lines.index("set -euo pipefail")
        walltime_idx = lines.index("#PBS -l walltime=01:00:00")
        self.assertLess(walltime_idx, first_exec_idx)

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

    def test_sync_push_remote_transport_uses_multiplexed_ssh_and_rsync(self) -> None:
        template = self.project / "remote-none.tmpl"
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
                "remote-sync",
                "--transport",
                "ssh",
                "--host",
                "example.org",
                "--scheduler",
                "none",
                "--remote-root",
                "/tmp/remote-sync",
                "--template-file",
                str(template),
            ],
        )
        self.assertEqual(add_result.exit_code, 0, add_result.output)
        set_result = _invoke(self.runner, self.project, ["target", "set", "remote-sync"])
        self.assertEqual(set_result.exit_code, 0, set_result.output)

        calls: list[list[str]] = []

        def _fake_run(*args, **kwargs):
            cmd = list(args[0] if args else kwargs.get("args"))
            calls.append(cmd)
            return __import__("subprocess").CompletedProcess(cmd, 0, stdout="", stderr="")

        with mock.patch("cluster_dispatch.cli._SUBPROCESS_RUN", side_effect=_fake_run):
            result = _invoke(self.runner, self.project, ["sync", "push"])

        self.assertEqual(result.exit_code, 0, result.output)
        ssh_calls = [cmd for cmd in calls if cmd and cmd[0] == "ssh"]
        rsync_calls = [cmd for cmd in calls if cmd and cmd[0] == "rsync"]
        self.assertTrue(ssh_calls, f"Expected at least one ssh call: {calls}")
        self.assertTrue(rsync_calls, f"Expected at least one rsync call: {calls}")
        self.assertIn("-F", ssh_calls[0])
        ssh_config = Path(ssh_calls[0][ssh_calls[0].index("-F") + 1])
        self.assertEqual(ssh_config.resolve(), (self.project / ".cluster_dispatch" / "ssh" / "config").resolve())
        self.assertTrue(ssh_config.exists())
        config_text = ssh_config.read_text()
        self.assertIn("Host remote-sync", config_text)
        self.assertIn("HostName example.org", config_text)
        self.assertIn("ControlMaster auto", config_text)
        self.assertIn("ControlPersist 10m", config_text)
        self.assertRegex(config_text, r"ControlPath /tmp/cluster-dispatch-ssh/[0-9a-f]{16}\.sock")
        self.assertIn("-e", rsync_calls[0])
        rsync_shell = rsync_calls[0][rsync_calls[0].index("-e") + 1]
        self.assertIn("-F", rsync_shell)
        self.assertIn(str(ssh_config), rsync_shell)
        self.assertIn("remote-sync:/tmp/remote-sync/analysis/a/", rsync_calls[0])

    def test_scheduler_submit_uses_multiplexed_ssh(self) -> None:
        cfg = load_config(self.project)
        local_target = cfg.targets["local"]
        cfg.targets["local"] = type(local_target)(
            host="example.org",
            scheduler="pbs",
            remote_root="/tmp/remote",
            transport="ssh",
            template_header=local_target.template_header,
            default_cpus=local_target.default_cpus,
            default_memory=local_target.default_memory,
            default_time=local_target.default_time,
            default_node=local_target.default_node,
            default_queue=local_target.default_queue,
            default_parallel_environment=local_target.default_parallel_environment,
        )
        adapter = get_adapter("pbs")
        calls: list[list[str]] = []

        def _fake_run(*args, **kwargs):
            cmd = list(args[0] if args else kwargs.get("args"))
            calls.append(cmd)
            return __import__("subprocess").CompletedProcess(cmd, 0, stdout="123.server\n", stderr="")

        with mock.patch("cluster_dispatch.schedulers.subprocess.run", side_effect=_fake_run):
            result = adapter.submit(
                "example.org",
                "/tmp/job.sh",
                transport="ssh",
                project_root=self.project,
                cfg=cfg,
                target_name="local",
                target_cfg=cfg.targets["local"],
            )

        self.assertEqual(result.job_id, "123.server")
        self.assertTrue(calls, "Expected scheduler submit to invoke subprocess.run")
        ssh_cmd = calls[0]
        self.assertEqual(ssh_cmd[0], "ssh")
        self.assertIn("-F", ssh_cmd)
        ssh_config = Path(ssh_cmd[ssh_cmd.index("-F") + 1])
        self.assertEqual(ssh_config.resolve(), (self.project / ".cluster_dispatch" / "ssh" / "config").resolve())
        self.assertTrue(ssh_config.exists())
        config_text = ssh_config.read_text()
        self.assertIn("Host local", config_text)
        self.assertIn("HostName example.org", config_text)
        self.assertIn("ControlMaster auto", config_text)
        self.assertRegex(config_text, r"ControlPath /tmp/cluster-dispatch-ssh/[0-9a-f]{16}\.sock")

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

    def test_analysis_untag_removes_tag(self) -> None:
        tagged_dir = self.project / self.analysis_rel / "outputs"
        tagged_dir.mkdir(parents=True, exist_ok=True)

        tag_result = _invoke(self.runner, self.project, ["analysis", "tag", "outputs"])
        self.assertEqual(tag_result.exit_code, 0, tag_result.output)

        untag_result = _invoke(self.runner, self.project, ["analysis", "untag", "outputs"])
        self.assertEqual(untag_result.exit_code, 0, untag_result.output)
        self.assertIn("Removed analysis tag: outputs", untag_result.output)

        cfg = load_config(self.project)
        self.assertNotIn("outputs", cfg.analysis_tags.get(str(self.analysis_rel), []))

    def test_analysis_untag_fails_for_missing_tag(self) -> None:
        result = _invoke(self.runner, self.project, ["analysis", "untag", "missing"])
        self.assertNotEqual(result.exit_code, 0)
        self.assertIn("Path is not tagged: missing", result.output)

    def test_analysis_untag_removes_empty_analysis_key(self) -> None:
        tagged_dir = self.project / self.analysis_rel / "results"
        tagged_dir.mkdir(parents=True, exist_ok=True)

        tag_result = _invoke(self.runner, self.project, ["analysis", "tag", "results"])
        self.assertEqual(tag_result.exit_code, 0, tag_result.output)

        untag_result = _invoke(self.runner, self.project, ["analysis", "untag", "results"])
        self.assertEqual(untag_result.exit_code, 0, untag_result.output)

        cfg = load_config(self.project)
        self.assertNotIn(str(self.analysis_rel), cfg.analysis_tags)

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

    def test_sync_status_lists_events(self) -> None:
        push_result = _invoke(self.runner, self.project, ["sync", "push"])
        self.assertEqual(push_result.exit_code, 0, push_result.output)

        status_result = _invoke(self.runner, self.project, ["sync", "status"])
        self.assertEqual(status_result.exit_code, 0, status_result.output)
        self.assertIn("Sync events:", status_result.output)
        self.assertIn("action=push", status_result.output)

    def test_sync_status_json_and_filter(self) -> None:
        push_result = _invoke(self.runner, self.project, ["sync", "push"])
        self.assertEqual(push_result.exit_code, 0, push_result.output)

        status_json = _invoke(
            self.runner,
            self.project,
            ["sync", "status", "--action", "push", "--limit", "5", "--json"],
        )
        self.assertEqual(status_json.exit_code, 0, status_json.output)
        payload = json.loads(status_json.output)
        self.assertTrue(payload)
        self.assertTrue(all(item.get("action") == "push" for item in payload))

    def test_index_writes_manifest_for_active_analysis_scope(self) -> None:
        nested = self.project / self.analysis_rel / "results"
        nested.mkdir(parents=True, exist_ok=True)
        (nested / "summary.txt").write_text("ok\n")

        result = _invoke(self.runner, self.project, ["index", "results"])
        self.assertEqual(result.exit_code, 0, result.output)
        self.assertIn("Index complete", result.output)

        manifest = self.project / ".cluster_dispatch" / "index" / "local" / "analysis" / "a" / "results.json"
        self.assertTrue(manifest.exists(), "Expected index manifest to be written")
        payload = json.loads(manifest.read_text())
        self.assertEqual(payload.get("target"), "local")
        self.assertEqual(payload.get("analysis"), str(self.analysis_rel).replace("\\", "/"))
        self.assertEqual(payload.get("scope"), "results")
        self.assertTrue(payload.get("entry_count", 0) >= 1)
        entries = payload.get("entries", [])
        self.assertTrue(any(item.get("path") == "summary.txt" for item in entries))

    def test_index_all_tags_writes_one_manifest_per_tag(self) -> None:
        (self.project / self.analysis_rel / "nfcore" / "multiqc").mkdir(parents=True, exist_ok=True)
        (self.project / self.analysis_rel / "nfcore" / "report.txt").write_text("r\n")
        (self.project / self.analysis_rel / "logs").mkdir(parents=True, exist_ok=True)
        (self.project / self.analysis_rel / "logs" / "run.log").write_text("x\n")

        tag_a = _invoke(self.runner, self.project, ["analysis", "tag", "nfcore"])
        self.assertEqual(tag_a.exit_code, 0, tag_a.output)
        tag_b = _invoke(self.runner, self.project, ["analysis", "tag", "logs"])
        self.assertEqual(tag_b.exit_code, 0, tag_b.output)

        result = _invoke(self.runner, self.project, ["index", "--all-tags", "--json"])
        self.assertEqual(result.exit_code, 0, result.output)
        payload = json.loads(result.output)
        self.assertEqual(len(payload), 2)
        scopes = sorted(item.get("scope") for item in payload)
        self.assertEqual(scopes, ["logs", "nfcore"])
        for item in payload:
            self.assertTrue(Path(str(item.get("manifest_path", ""))).exists())

    def test_analysis_list_remote_uses_index_when_available(self) -> None:
        (self.project / self.analysis_rel / "alpha").mkdir(parents=True, exist_ok=True)
        (self.project / self.analysis_rel / "beta").mkdir(parents=True, exist_ok=True)

        index_result = _invoke(self.runner, self.project, ["index"])
        self.assertEqual(index_result.exit_code, 0, index_result.output)

        # Add new directory after indexing; list --remote should still return indexed view.
        (self.project / self.analysis_rel / "gamma").mkdir(parents=True, exist_ok=True)
        listed = _invoke(self.runner, self.project, ["analysis", "list", "--remote"])
        self.assertEqual(listed.exit_code, 0, listed.output)
        self.assertIn("(from index)", listed.output)
        self.assertIn("- alpha", listed.output)
        self.assertIn("- beta", listed.output)
        self.assertNotIn("- gamma", listed.output)

    def test_analysis_list_remote_falls_back_to_live_listing_without_index(self) -> None:
        (self.project / self.analysis_rel / "delta").mkdir(parents=True, exist_ok=True)
        listed = _invoke(self.runner, self.project, ["analysis", "list", "--remote"])
        self.assertEqual(listed.exit_code, 0, listed.output)
        self.assertNotIn("(from index)", listed.output)
        self.assertIn("- delta", listed.output)

    def test_sync_pull_all_dry_run_shows_index_diff_summary(self) -> None:
        target_file = self.project / self.analysis_rel / "artifact.txt"
        target_file.write_text("v1\n")
        index_result = _invoke(self.runner, self.project, ["index"])
        self.assertEqual(index_result.exit_code, 0, index_result.output)
        target_file.write_text("v2-changed\n")

        pull_result = _invoke(self.runner, self.project, ["sync", "pull", "--all", "--dry-run"])
        self.assertEqual(pull_result.exit_code, 0, pull_result.output)
        self.assertIn("Index diff summary (preview):", pull_result.output)

    def test_collect_reports_index_transfer_estimate(self) -> None:
        outputs_dir = self.project / self.analysis_rel / "outputs"
        outputs_dir.mkdir(parents=True, exist_ok=True)
        (outputs_dir / "file.txt").write_text("hello\n")
        _invoke(self.runner, self.project, ["index", "outputs"])

        jobs_dir = self.project / ".cluster_dispatch" / "jobs"
        jobs_dir.mkdir(parents=True, exist_ok=True)
        payload = {
            "submitted_at": "2026-01-01T00:00:00",
            "analysis": str(self.analysis_rel).replace("\\", "/"),
            "target": "local",
            "scheduler": "none",
            "job_id": "collect-1",
            "job_name": "collect-job",
            "state": "COMPLETED",
            "command": "echo done",
            "remote_run_dir": str((self.project / self.analysis_rel).resolve()),
            "remote_log_file": str((self.project / self.analysis_rel / "run.log").resolve()),
            "analysis_tags": ["outputs"],
        }
        (jobs_dir / "collect.json").write_text(json.dumps(payload, indent=2))

        collect_result = _invoke(self.runner, self.project, ["collect", "--job-id", "collect-1"])
        self.assertEqual(collect_result.exit_code, 0, collect_result.output)
        self.assertIn("Index transfer estimate:", collect_result.output)

    def test_report_json_includes_index_summary(self) -> None:
        (self.project / self.analysis_rel / "reporting").mkdir(parents=True, exist_ok=True)
        (self.project / self.analysis_rel / "reporting" / "a.txt").write_text("x\n")
        _invoke(self.runner, self.project, ["index", "reporting"])

        report_result = _invoke(self.runner, self.project, ["report", "--json"])
        self.assertEqual(report_result.exit_code, 0, report_result.output)
        payload = json.loads(report_result.output)
        self.assertIn("index", payload)
        self.assertGreaterEqual(int(payload["index"].get("manifests", 0)), 1)

    def test_status_terminal_output_check_uses_index(self) -> None:
        (self.project / self.analysis_rel / "out").mkdir(parents=True, exist_ok=True)
        (self.project / self.analysis_rel / "out" / "done.txt").write_text("ok\n")
        _invoke(self.runner, self.project, ["index"])

        jobs_dir = self.project / ".cluster_dispatch" / "jobs"
        jobs_dir.mkdir(parents=True, exist_ok=True)
        payload = {
            "submitted_at": "2026-01-01T00:00:00",
            "analysis": str(self.analysis_rel).replace("\\", "/"),
            "target": "local",
            "scheduler": "none",
            "job_id": "status-1",
            "job_name": "status-job",
            "state": "COMPLETED",
            "command": "echo done",
            "remote_run_dir": str((self.project / self.analysis_rel).resolve()),
            "remote_log_file": str((self.project / self.analysis_rel / "run.log").resolve()),
            "analysis_tags": ["out"],
        }
        (jobs_dir / "status.json").write_text(json.dumps(payload, indent=2))

        status_result = _invoke(self.runner, self.project, ["status", "--job-id", "status-1"])
        self.assertEqual(status_result.exit_code, 0, status_result.output)
        self.assertIn("output=OK", status_result.output)

    def test_doctor_reports_index_checks(self) -> None:
        (self.project / self.analysis_rel / "doctor").mkdir(parents=True, exist_ok=True)
        _invoke(self.runner, self.project, ["index", "doctor"])
        doctor_result = _invoke(self.runner, self.project, ["doctor", "--no-remote"])
        self.assertIn("index", doctor_result.output)

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

        forced = _invoke(self.runner, self.project, ["target", "remove", "remote-a", "--force", "--yes"])
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
        self.assertIn("referenced by", blocked.output)
        self.assertIn("force", blocked.output)

        removed = _invoke(
            self.runner,
            self.project,
            ["target", "remove", "remote-b", "--force", "--prune-records", "--yes"],
        )
        self.assertEqual(removed.exit_code, 0, removed.output)
        self.assertFalse((jobs_dir / "fake_remote.json").exists())
        self.assertFalse((sync_dir / "fake_remote.json").exists())

    def test_target_test_local(self) -> None:
        result = _invoke(self.runner, self.project, ["target", "test", "local"])
        self.assertEqual(result.exit_code, 0, result.output)
        self.assertIn("Summary:", result.output)
        self.assertIn("[PASS] scheduler:", result.output)

    def test_target_test_json_output(self) -> None:
        result = _invoke(self.runner, self.project, ["target", "test", "local", "--json"])
        self.assertEqual(result.exit_code, 0, result.output)
        payload = json.loads(result.output)
        self.assertEqual(payload["summary"]["target"], "local")
        self.assertIn("checks", payload)
        self.assertTrue(payload["checks"])

    def test_config_show(self) -> None:
        result = _invoke(self.runner, self.project, ["config", "show"])
        self.assertEqual(result.exit_code, 0, result.output)
        self.assertIn("Project root:", result.output)
        self.assertIn("Default target:", result.output)
        self.assertIn("Targets:", result.output)

    def test_config_export_json_and_yaml(self) -> None:
        json_result = _invoke(self.runner, self.project, ["config", "export", "--format", "json"])
        self.assertEqual(json_result.exit_code, 0, json_result.output)
        json_payload = json.loads(json_result.output)
        self.assertIn("project_root", json_payload)
        self.assertIn("targets", json_payload)
        self.assertIn("active_target", json_payload)

        yaml_result = _invoke(self.runner, self.project, ["config", "export", "--format", "yaml"])
        self.assertEqual(yaml_result.exit_code, 0, yaml_result.output)
        self.assertIn("project_root:", yaml_result.output)
        self.assertIn("targets:", yaml_result.output)

    def test_cleanup_records_dry_run_and_apply(self) -> None:
        jobs_dir = self.project / ".cluster_dispatch" / "jobs"
        sync_dir = self.project / ".cluster_dispatch" / "sync"
        jobs_dir.mkdir(parents=True, exist_ok=True)
        sync_dir.mkdir(parents=True, exist_ok=True)

        old_ts = (datetime.now() - timedelta(days=40)).isoformat(timespec="seconds")
        new_ts = datetime.now().isoformat(timespec="seconds")
        old_job = jobs_dir / "old.json"
        new_job = jobs_dir / "new.json"
        old_sync = sync_dir / "old.json"
        old_job.write_text(json.dumps({"submitted_at": old_ts, "target": "local", "analysis": "analysis/a"}, indent=2))
        new_job.write_text(json.dumps({"submitted_at": new_ts, "target": "local", "analysis": "analysis/a"}, indent=2))
        old_sync.write_text(json.dumps({"recorded_at": old_ts, "target": "local", "analysis": "analysis/a"}, indent=2))

        dry_result = _invoke(
            self.runner,
            self.project,
            ["cleanup", "records", "--older-than-days", "30", "--json"],
        )
        self.assertEqual(dry_result.exit_code, 0, dry_result.output)
        dry_payload = json.loads(dry_result.output)
        self.assertGreaterEqual(dry_payload["categories"]["jobs"]["deleted_count"], 1)
        self.assertGreaterEqual(dry_payload["categories"]["sync"]["deleted_count"], 1)
        self.assertTrue(old_job.exists())
        self.assertTrue(old_sync.exists())

        apply_result = _invoke(
            self.runner,
            self.project,
            ["cleanup", "records", "--older-than-days", "30", "--apply", "--yes"],
        )
        self.assertEqual(apply_result.exit_code, 0, apply_result.output)
        self.assertFalse(old_job.exists())
        self.assertFalse(old_sync.exists())
        self.assertTrue(new_job.exists())

    def test_report_text_output(self) -> None:
        run_result = _invoke(self.runner, self.project, ["analysis", "run", "python", "-c", "print(2)"])
        self.assertEqual(run_result.exit_code, 0, run_result.output)
        sync_result = _invoke(self.runner, self.project, ["sync", "push"])
        self.assertEqual(sync_result.exit_code, 0, sync_result.output)

        report_result = _invoke(self.runner, self.project, ["report"])
        self.assertEqual(report_result.exit_code, 0, report_result.output)
        self.assertIn("Run report:", report_result.output)
        self.assertIn("Jobs:", report_result.output)
        self.assertIn("Sync events:", report_result.output)

    def test_report_json_output(self) -> None:
        run_result = _invoke(self.runner, self.project, ["analysis", "run", "python", "-c", "print(3)"])
        self.assertEqual(run_result.exit_code, 0, run_result.output)
        report_result = _invoke(self.runner, self.project, ["report", "--json"])
        self.assertEqual(report_result.exit_code, 0, report_result.output)
        payload = json.loads(report_result.output)
        self.assertIn("jobs_total", payload)
        self.assertIn("sync_total", payload)
        self.assertIn("jobs_by_state", payload)
        self.assertIn("resources", payload)

    def test_export_bundle_includes_optional_sections(self) -> None:
        run_result = _invoke(self.runner, self.project, ["analysis", "run", "python", "-c", "print(7)"])
        self.assertEqual(run_result.exit_code, 0, run_result.output)
        sync_result = _invoke(self.runner, self.project, ["sync", "push"])
        self.assertEqual(sync_result.exit_code, 0, sync_result.output)

        bundle = self.project / "backup.tar.gz"
        export_result = _invoke(
            self.runner,
            self.project,
            ["export", "--output", str(bundle), "--jobs", "--sync"],
        )
        self.assertEqual(export_result.exit_code, 0, export_result.output)
        self.assertTrue(bundle.exists())

        with tarfile.open(bundle, "r:gz") as tar:
            names = set(tar.getnames())
        self.assertIn("export_manifest.json", names)
        self.assertIn(".cluster_dispatch/config.yml", names)
        self.assertTrue(any(n.startswith(".cluster_dispatch/jobs/") for n in names))
        self.assertTrue(any(n.startswith(".cluster_dispatch/sync/") for n in names))

    def test_import_bundle_round_trip(self) -> None:
        run_result = _invoke(self.runner, self.project, ["analysis", "run", "python", "-c", "print(8)"])
        self.assertEqual(run_result.exit_code, 0, run_result.output)

        bundle = self.project / "roundtrip.tar.gz"
        export_result = _invoke(
            self.runner,
            self.project,
            ["export", "--output", str(bundle), "--jobs"],
        )
        self.assertEqual(export_result.exit_code, 0, export_result.output)

        imported_dir = self.base / "imported"
        imported_dir.mkdir(parents=True, exist_ok=True)
        import_result = _invoke(self.runner, imported_dir, ["import", str(bundle)])
        self.assertEqual(import_result.exit_code, 0, import_result.output)
        self.assertTrue((imported_dir / ".cluster_dispatch" / "config.yml").exists())
        self.assertTrue((imported_dir / ".cluster_dispatch" / "jobs").exists())

    def test_watch_exits_on_terminal_state(self) -> None:
        run_result = _invoke(self.runner, self.project, ["analysis", "run", "python", "-c", "print(9)"])
        self.assertEqual(run_result.exit_code, 0, run_result.output)

        watch_result = _invoke(
            self.runner,
            self.project,
            ["watch", "--interval", "1", "--max-polls", "20", "--no-log-tail"],
        )
        self.assertEqual(watch_result.exit_code, 0, watch_result.output)
        self.assertIn("state=", watch_result.output)

    def test_watch_timeout_returns_nonzero(self) -> None:
        run_result = _invoke(
            self.runner,
            self.project,
            ["analysis", "run", "python", "-c", "import time; time.sleep(3)"],
        )
        self.assertEqual(run_result.exit_code, 0, run_result.output)

        watch_result = _invoke(
            self.runner,
            self.project,
            ["watch", "--interval", "1", "--max-polls", "1", "--no-log-tail"],
        )
        self.assertNotEqual(watch_result.exit_code, 0)
        self.assertIn("Watch timed out", watch_result.output)

    def test_run_validate_success(self) -> None:
        result = _invoke(self.runner, self.project, ["run", "validate", "python", "-c", "print(1)"])
        self.assertEqual(result.exit_code, 0, result.output)
        self.assertIn("Summary:", result.output)
        self.assertIn("[PASS] command:", result.output)

    def test_sweep_validate_success(self) -> None:
        sweep_yaml = self.project / "sweep_validate.yml"
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
                "sweep",
                "validate",
                "--config",
                str(sweep_yaml),
                "python",
                "-c",
                "print({lr},{batch_size})",
            ],
        )
        self.assertEqual(result.exit_code, 0, result.output)
        self.assertIn("Summary:", result.output)
        self.assertIn("runs=", result.output)

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
