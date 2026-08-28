from __future__ import annotations

import json
import sys
import tempfile
from pathlib import Path
from types import SimpleNamespace
from unittest import TestCase, mock

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = PROJECT_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from cluster_dispatch.config import ProjectConfig, TargetConfig  # noqa: E402
from cluster_dispatch.context import Context  # noqa: E402
from cluster_dispatch.models import ResourceSpec  # noqa: E402
from cluster_dispatch.operations import OperationRuntime  # noqa: E402
from cluster_dispatch.schedulers import ArraySpec, SubmitResult, get_adapter  # noqa: E402
from cluster_dispatch.sweeps import SweepRequest, SweepRuntime, cancel_sweep, execute_sweep, resume_sweep  # noqa: E402


class TestArraySpecifications(TestCase):
    def test_each_scheduler_exposes_array_syntax(self) -> None:
        expected = {
            "sge": ("#$ -t 1-7", "${SGE_TASK_ID:?SGE_TASK_ID not set}"),
            "univa": ("#$ -t 1-7", "${SGE_TASK_ID:?SGE_TASK_ID not set}"),
            "pbs": ("#PBS -J 1-7", "${PBS_ARRAY_INDEX:-${PBS_ARRAYID:?PBS_ARRAY_INDEX/PBS_ARRAYID not set}}"),
            "slurm": ("#SBATCH --array=1-7", "${SLURM_ARRAY_TASK_ID:?SLURM_ARRAY_TASK_ID not set}"),
            "lsf": ('#BSUB -J "job[1-7]"', "${LSB_JOBINDEX:?LSB_JOBINDEX not set}"),
        }
        for scheduler, values in expected.items():
            with self.subTest(scheduler=scheduler):
                spec = get_adapter(scheduler).array_spec(job_name="job", task_count=7)
                self.assertEqual((spec.directive, spec.task_id_expression), values)

    def test_none_scheduler_rejects_arrays(self) -> None:
        with self.assertRaisesRegex(RuntimeError, "not supported"):
            get_adapter("none").array_spec(job_name="job", task_count=2)


class TestSweepResumeCancel(TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory(prefix="sweep-service-")
        self.root = Path(self.temp.name)
        analysis = Path("analysis/a")
        (self.root / analysis).mkdir(parents=True)
        target = TargetConfig(host="localhost", scheduler="none", remote_root=str(self.root), transport="local")
        cfg = ProjectConfig(scheduler="none", default_target="local", targets={"local": target}, active_analysis=str(analysis))
        self.context = Context(
            project_root=self.root, analysis=analysis, local_analysis=self.root / analysis,
            target=target, remote_analysis=self.root / analysis, workdir=self.root / analysis,
            remote_workdir=self.root / analysis, target_name="local", config=cfg,
        )
        self.runtime = SweepRuntime(operation=SimpleNamespace(), run_direct=mock.Mock())
        (self.root / ".cluster_dispatch" / "sweeps").mkdir(parents=True)

    def tearDown(self) -> None:
        self.temp.cleanup()

    def _write(self, payload: dict) -> None:
        (self.root / ".cluster_dispatch" / "sweeps" / f"{payload['sweep_id']}.json").write_text(json.dumps(payload))

    def test_resume_submits_only_pending_runs(self) -> None:
        manifest = {
            "sweep_id": "resume-me", "analysis": "analysis/a", "target": "local", "mode": "local",
            "resources": {"cpus": 1, "memory": "8G", "time": "01:00:00", "node": "1", "queue": "", "parallel_environment": ""},
            "runs": [
                {"run_id": "done", "sweep_job": "x", "sweep_index": 1, "sweep_params": {}, "command": "true", "job_id": "old"},
                {"run_id": "pending", "sweep_job": "x", "sweep_index": 2, "sweep_params": {}, "command": "true", "job_id": None},
            ],
        }
        self._write(manifest)
        with mock.patch("cluster_dispatch.sweeps.execute_run", return_value=SimpleNamespace(job_id="local-pending", state="COMPLETED", exit_code=0)) as execute:
            result = resume_sweep(self.context, "resume-me", runtime=self.runtime)
        self.assertEqual(result.submitted, 1)
        self.assertEqual(execute.call_count, 1)
        self.assertEqual(result.manifest["runs"][0]["job_id"], "old")
        self.assertEqual(result.manifest["runs"][1]["job_id"], "local-pending")

    def test_cancel_local_marks_only_pending_runs(self) -> None:
        manifest = {
            "sweep_id": "cancel-me", "analysis": "analysis/a", "target": "local", "mode": "local",
            "runs": [{"status": "PENDING"}, {"status": "COMPLETED"}],
        }
        self._write(manifest)
        result = cancel_sweep(self.context, "cancel-me", runtime=self.runtime)
        self.assertEqual(result.cancelled, 1)
        self.assertEqual(result.manifest["runs"][0]["status"], "CANCELLED")
        self.assertEqual(result.manifest["runs"][1]["status"], "COMPLETED")

    def test_array_submission_preserves_task_ids_and_manifest_shape(self) -> None:
        target = TargetConfig(host="localhost", scheduler="slurm", remote_root=str(self.root), transport="local")
        self.context.config.targets["local"] = target
        self.context = Context(
            project_root=self.context.project_root, analysis=self.context.analysis,
            local_analysis=self.context.local_analysis, target=target,
            remote_analysis=self.context.remote_analysis, workdir=self.context.workdir,
            remote_workdir=self.context.remote_workdir, target_name="local", config=self.context.config,
        )
        config_file = self.root / "sweep.yml"
        config_file.write_text("params:\n  sample:\n    value: [a, b]\n")

        class FakeAdapter:
            def array_spec(self, *, job_name: str, task_count: int) -> ArraySpec:
                return ArraySpec(f"#SBATCH --array=1-{task_count}", "#SBATCH", "${SLURM_ARRAY_TASK_ID:?missing}")

            def submit(self, *args, **kwargs) -> SubmitResult:
                return SubmitResult("321")

        completed = lambda *args, **kwargs: SimpleNamespace(returncode=0, stdout="", stderr="")
        operation = OperationRuntime(
            run_command=mock.Mock(side_effect=completed),
            render_scheduler_header=lambda *args, **kwargs: "#SBATCH --job-name=test\n",
            write_remote_script=mock.Mock(),
            git_info=lambda root: None,
        )
        request = SweepRequest(
            config_file=config_file, command_template="echo {value}", mode="array",
            resources=ResourceSpec(profile=None, cpus=1, memory="1G", time="00:10:00", node="1"),
            sweep_id="array-test", submit_delay_ms=5000,
        )
        with mock.patch("cluster_dispatch.sweeps.get_adapter", return_value=FakeAdapter()):
            result = execute_sweep(self.context, request, runtime=SweepRuntime(operation, mock.Mock()))

        self.assertEqual(result.submitted, 2)
        self.assertEqual([item["array_task_id"] for item in result.manifest["runs"]], [1, 2])
        self.assertEqual([item["job_id"] for item in result.manifest["runs"]], ["321_1", "321_2"])
        script = Path(result.manifest["remote_sweep_dir"]) / "pc_submit_array_array-test.sh"
        self.assertIn("#SBATCH --array=1-2", script.read_text())
