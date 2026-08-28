from __future__ import annotations

import json
import subprocess
import sys
import tempfile
from pathlib import Path
from unittest import TestCase, mock

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = PROJECT_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from cluster_dispatch.config import ProjectConfig, TargetConfig, save_config
from cluster_dispatch.jobs import (
    JobError,
    cancel_jobs,
    filter_jobs,
    get_log,
    prepare_retry,
    query_jobs,
    resolve_job_state,
)
from cluster_dispatch.records import save_job
from cluster_dispatch.schedulers import StatusResult


class _Adapter:
    def __init__(self, status: str, accounting: str = "NOT_FOUND") -> None:
        self.status_value = status
        self.accounting_value = accounting
        self.status_calls = 0
        self.accounting_calls = 0

    def status(self, *args, **kwargs) -> StatusResult:
        self.status_calls += 1
        return StatusResult(self.status_value, "live")

    def accounting_status(self, *args, **kwargs) -> StatusResult:
        self.accounting_calls += 1
        return StatusResult(self.accounting_value, "accounting")


class TestJobServices(TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory(prefix="cdp-job-services-")
        self.root = Path(self.tmp.name)
        (self.root / "analysis/a").mkdir(parents=True)
        self.cfg = ProjectConfig(
            scheduler="none",
            default_target="local",
            active_analysis="analysis/a",
            targets={
                "local": TargetConfig("localhost", "none", str(self.root), transport="local"),
            },
        )
        save_config(self.root, self.cfg)

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def _record(self, **changes):
        payload = {
            "submitted_at": "2026-01-01T00:00:00",
            "analysis": "analysis/a",
            "analysis_tags": ["results"],
            "target": "local",
            "scheduler": "none",
            "job_id": "1",
            "job_name": "demo",
            "state": "RUNNING",
            "command": "echo ok",
            "command_resolved": "echo ok",
            "remote_run_dir": str(self.root / "analysis/a"),
            "remote_log_file": str(self.root / "run.log"),
            "resources": {"profile": "small", "cpus": 2, "memory": "4G", "time": "00:30:00", "node": "1", "queue": "q", "parallel_environment": "pe"},
        }
        payload.update(changes)
        path = save_job(self.root, payload)
        payload["_record_file"] = str(path)
        return payload

    def test_running_job_queries_scheduler(self) -> None:
        record = self._record()
        adapter = _Adapter("RUNNING")
        with mock.patch("cluster_dispatch.jobs.get_adapter", return_value=adapter):
            self.assertEqual(resolve_job_state(self.root, self.cfg, record), "RUNNING")
        self.assertEqual(adapter.status_calls, 1)

    def test_terminal_record_avoids_polling(self) -> None:
        record = self._record(state="COMPLETED")
        adapter = _Adapter("RUNNING")
        with mock.patch("cluster_dispatch.jobs.get_adapter", return_value=adapter):
            self.assertEqual(resolve_job_state(self.root, self.cfg, record), "COMPLETED")
        self.assertEqual(adapter.status_calls, 0)

    def test_not_found_uses_accounting_and_persists_final_state(self) -> None:
        record = self._record()
        adapter = _Adapter("NOT_FOUND", "COMPLETED")
        with mock.patch("cluster_dispatch.jobs.get_adapter", return_value=adapter):
            rows = query_jobs(self.root, refresh_state=True)
        self.assertEqual(rows[0]["state"], "COMPLETED")
        stored = json.loads(Path(record["_record_file"]).read_text())
        self.assertEqual(stored["final_state"], "COMPLETED")
        self.assertEqual(stored["final_state_source"], "accounting")

    def test_missing_target_persists_target_missing(self) -> None:
        record = self._record(target="gone")
        self.assertEqual(resolve_job_state(self.root, self.cfg, record), "TARGET_MISSING")
        self.assertEqual(json.loads(Path(record["_record_file"]).read_text())["state"], "TARGET_MISSING")

    def test_filter_jobs_supports_all_selectors_and_limit(self) -> None:
        self._record(job_id="2", job_name="other", target="local")
        self._record(job_id="3", job_name="demo", target="local")
        rows = filter_jobs(self.root, job_name="demo", target="local", analysis="analysis/a", limit=1)
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["job_id"], "3")

    def test_cancel_local_uses_scheduler_command(self) -> None:
        record = self._record()
        seen = []

        def run(argv, **kwargs):
            seen.append(argv)
            return subprocess.CompletedProcess(argv, 0, "", "")

        result = cancel_jobs(self.root, [record], run=run)
        self.assertEqual(result.matched, 1)
        self.assertEqual(len(result.cancelled), 1)
        self.assertEqual(seen[0][:2], ["bash", "-lc"])
        self.assertIn("kill", seen[0][2])

    def test_cancel_missing_target_is_structured_failure(self) -> None:
        result = cancel_jobs(self.root, [self._record(target="gone")])
        self.assertEqual(result.cancelled, [])
        self.assertIn("target_not_configured", result.failed[0])

    def test_cancel_ssh_uses_scheduler_specific_command_and_reports_failure(self) -> None:
        self.cfg.targets["remote"] = TargetConfig("example.org", "slurm", "/remote", transport="ssh")
        save_config(self.root, self.cfg)
        record = self._record(target="remote", scheduler="slurm")
        seen = []

        def run(argv, **kwargs):
            seen.append(argv)
            return subprocess.CompletedProcess(argv, 1, "", "denied")

        result = cancel_jobs(self.root, [record], run=run)
        self.assertEqual(result.cancelled, [])
        self.assertIn("denied", result.failed[0])
        self.assertEqual(seen[0][0], "ssh")
        self.assertIn("scancel 1", seen[0][-1])

    def test_local_log_head_tail_and_missing(self) -> None:
        log = self.root / "run.log"
        log.write_text("one\ntwo\nthree\n")
        self._record()
        self.assertEqual(get_log(self.root, job_id="1", head=1).content, "one")
        self.assertEqual(get_log(self.root, job_id="1", tail=1).content, "three")
        log.unlink()
        with self.assertRaisesRegex(JobError, "Local log file not found"):
            get_log(self.root, job_id="1")

    def test_remote_log_builds_ssh_head_request(self) -> None:
        self.cfg.targets["remote"] = TargetConfig("example.org", "slurm", "/remote", transport="ssh")
        save_config(self.root, self.cfg)
        self._record(target="remote", scheduler="slurm", remote_log_file="/remote/run.log")
        seen = []

        def run(argv, **kwargs):
            seen.append(argv)
            return subprocess.CompletedProcess(argv, 0, "first\n", "")

        result = get_log(self.root, job_id="1", head=1, run=run)
        self.assertEqual(result.content, "first")
        self.assertEqual(seen[0][0], "ssh")
        self.assertIn("head -n 1", seen[0][-1])

    def test_prepare_retry_reconstructs_context_and_resources(self) -> None:
        self._record()
        spec = prepare_retry(self.root, job_id="1")
        self.assertEqual(spec.context.analysis.as_posix(), "analysis/a")
        self.assertEqual(spec.request.command, "echo ok")
        self.assertEqual(spec.request.resources.cpus, 2)
        self.assertEqual(spec.request.resources.parallel_environment, "pe")

    def test_prepare_retry_requires_matching_active_analysis(self) -> None:
        self._record(analysis="analysis/b")
        with self.assertRaisesRegex(JobError, "Analysis directory not found"):
            prepare_retry(self.root, job_id="1")

    def test_prepare_retry_rejects_existing_non_active_analysis(self) -> None:
        (self.root / "analysis/b").mkdir(parents=True)
        self._record(analysis="analysis/b")
        with self.assertRaisesRegex(JobError, "active analysis"):
            prepare_retry(self.root, job_id="1")

    def test_prepare_retry_rejects_missing_target(self) -> None:
        self._record(target="gone")
        with self.assertRaisesRegex(JobError, "not configured"):
            prepare_retry(self.root, job_id="1")
