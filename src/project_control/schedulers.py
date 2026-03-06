from __future__ import annotations

import subprocess
from dataclasses import dataclass
from typing import Protocol


@dataclass
class SubmitResult:
    job_id: str


@dataclass
class StatusResult:
    state: str
    raw: str


class SchedulerAdapter(Protocol):
    def submit(self, host: str, submit_script: str) -> SubmitResult:
        ...

    def status(self, host: str, job_id: str) -> StatusResult:
        ...

    def accounting_status(self, host: str, job_id: str) -> StatusResult:
        ...


class SGEAdapter:
    def submit(self, host: str, submit_script: str) -> SubmitResult:
        proc = subprocess.run(
            ["ssh", host, "bash", "-lc", f"qsub {submit_script}"],
            capture_output=True,
            text=True,
            check=True,
        )
        output = proc.stdout.strip()
        # Typical format: Your job 1234 ("name") has been submitted
        job_id = output.split()[2] if len(output.split()) >= 3 else output
        return SubmitResult(job_id=job_id)

    def status(self, host: str, job_id: str) -> StatusResult:
        proc = subprocess.run(
            ["ssh", host, "bash", "-lc", f"qstat -j {job_id}"],
            capture_output=True,
            text=True,
        )
        if proc.returncode == 0:
            return StatusResult(state="RUNNING_OR_QUEUED", raw=proc.stdout)
        return StatusResult(state="NOT_FOUND", raw=proc.stderr or proc.stdout)

    def accounting_status(self, host: str, job_id: str) -> StatusResult:
        proc = subprocess.run(
            ["ssh", host, "bash", "-lc", f"qacct -j {job_id}"],
            capture_output=True,
            text=True,
        )
        if proc.returncode != 0:
            return StatusResult(state="NOT_FOUND", raw=proc.stderr or proc.stdout)
        exit_status = None
        failed = None
        for line in proc.stdout.splitlines():
            clean = line.strip()
            if clean.startswith("exit_status"):
                parts = clean.split()
                if len(parts) >= 2:
                    exit_status = parts[-1]
            if clean.startswith("failed"):
                parts = clean.split()
                if len(parts) >= 2:
                    failed = parts[-1]
        if failed and failed != "0":
            return StatusResult(state="FAILED", raw=proc.stdout)
        if exit_status is not None:
            return StatusResult(state="COMPLETED" if exit_status == "0" else "FAILED", raw=proc.stdout)
        return StatusResult(state="ACCOUNTED", raw=proc.stdout)


class UnivaAdapter(SGEAdapter):
    """Univa Grid Engine is qsub/qstat compatible with SGE."""


class PBSAdapter:
    def submit(self, host: str, submit_script: str) -> SubmitResult:
        proc = subprocess.run(
            ["ssh", host, "bash", "-lc", f"qsub {submit_script}"],
            capture_output=True,
            text=True,
            check=True,
        )
        output = proc.stdout.strip()
        job_id = output.split()[0] if output else ""
        return SubmitResult(job_id=job_id)

    def status(self, host: str, job_id: str) -> StatusResult:
        proc = subprocess.run(
            ["ssh", host, "bash", "-lc", f"qstat -f {job_id}"],
            capture_output=True,
            text=True,
        )
        if proc.returncode != 0:
            return StatusResult(state="NOT_FOUND", raw=proc.stderr or proc.stdout)

        state = "UNKNOWN"
        for line in proc.stdout.splitlines():
            if "job_state =" in line:
                state = line.split("=", 1)[1].strip()
                break
        return StatusResult(state=state, raw=proc.stdout)

    def accounting_status(self, host: str, job_id: str) -> StatusResult:
        proc = subprocess.run(
            ["ssh", host, "bash", "-lc", f"qstat -xf {job_id}"],
            capture_output=True,
            text=True,
        )
        if proc.returncode != 0:
            return StatusResult(state="NOT_FOUND", raw=proc.stderr or proc.stdout)

        state = None
        exit_status = None
        for line in proc.stdout.splitlines():
            clean = line.strip()
            if "job_state =" in clean:
                state = clean.split("=", 1)[1].strip()
            if "Exit_status =" in clean:
                exit_status = clean.split("=", 1)[1].strip()
        if state:
            if state == "F":
                return StatusResult(state="FAILED", raw=proc.stdout)
            if state == "C":
                if exit_status is not None:
                    return StatusResult(state="COMPLETED" if exit_status == "0" else "FAILED", raw=proc.stdout)
                return StatusResult(state="COMPLETED", raw=proc.stdout)
            return StatusResult(state=state, raw=proc.stdout)
        return StatusResult(state="ACCOUNTED", raw=proc.stdout)


class SlurmAdapter:
    def submit(self, host: str, submit_script: str) -> SubmitResult:
        proc = subprocess.run(
            ["ssh", host, "bash", "-lc", f"sbatch {submit_script}"],
            capture_output=True,
            text=True,
            check=True,
        )
        output = proc.stdout.strip()
        # Typical format: Submitted batch job 12345
        job_id = output.split()[-1] if output else ""
        return SubmitResult(job_id=job_id)

    def status(self, host: str, job_id: str) -> StatusResult:
        proc = subprocess.run(
            ["ssh", host, "bash", "-lc", f"squeue -h -j {job_id} -o %T"],
            capture_output=True,
            text=True,
        )
        if proc.returncode != 0:
            return StatusResult(state="NOT_FOUND", raw=proc.stderr or proc.stdout)

        state = proc.stdout.strip()
        if not state:
            return StatusResult(state="NOT_FOUND", raw=proc.stdout)
        return StatusResult(state=state, raw=proc.stdout)

    def accounting_status(self, host: str, job_id: str) -> StatusResult:
        proc = subprocess.run(
            ["ssh", host, "bash", "-lc", f"sacct -j {job_id} -X -n -o State | head -n 1"],
            capture_output=True,
            text=True,
        )
        if proc.returncode != 0:
            return StatusResult(state="NOT_FOUND", raw=proc.stderr or proc.stdout)
        state = proc.stdout.strip().split()[0] if proc.stdout.strip() else ""
        if not state:
            return StatusResult(state="NOT_FOUND", raw=proc.stdout)
        state = state.split("+", 1)[0]
        return StatusResult(state=state, raw=proc.stdout)


class LSFAdapter:
    def submit(self, host: str, submit_script: str) -> SubmitResult:
        proc = subprocess.run(
            ["ssh", host, "bash", "-lc", f"bsub < {submit_script}"],
            capture_output=True,
            text=True,
            check=True,
        )
        output = proc.stdout.strip()
        # Typical format: Job <12345> is submitted to default queue <normal>.
        job_id = output
        if "<" in output and ">" in output:
            job_id = output.split("<", 1)[1].split(">", 1)[0]
        return SubmitResult(job_id=job_id)

    def status(self, host: str, job_id: str) -> StatusResult:
        proc = subprocess.run(
            ["ssh", host, "bash", "-lc", f"bjobs -noheader -o STAT {job_id}"],
            capture_output=True,
            text=True,
        )
        if proc.returncode != 0:
            return StatusResult(state="NOT_FOUND", raw=proc.stderr or proc.stdout)

        state = proc.stdout.strip().splitlines()[0].strip() if proc.stdout.strip() else "UNKNOWN"
        return StatusResult(state=state, raw=proc.stdout)

    def accounting_status(self, host: str, job_id: str) -> StatusResult:
        proc = subprocess.run(
            ["ssh", host, "bash", "-lc", f"bjobs -a -noheader -o STAT {job_id}"],
            capture_output=True,
            text=True,
        )
        if proc.returncode != 0:
            return StatusResult(state="NOT_FOUND", raw=proc.stderr or proc.stdout)
        state = proc.stdout.strip().splitlines()[0].strip() if proc.stdout.strip() else ""
        if not state:
            return StatusResult(state="NOT_FOUND", raw=proc.stdout)
        if state == "DONE":
            return StatusResult(state="COMPLETED", raw=proc.stdout)
        if state == "EXIT":
            return StatusResult(state="FAILED", raw=proc.stdout)
        return StatusResult(state=state, raw=proc.stdout)


class LocalAdapter:
    def submit(self, host: str, submit_script: str) -> SubmitResult:
        proc = subprocess.run(
            ["ssh", host, "bash", "-lc", f"nohup bash {submit_script} >/dev/null 2>&1 & echo $!"],
            capture_output=True,
            text=True,
            check=True,
        )
        return SubmitResult(job_id=proc.stdout.strip())

    def status(self, host: str, job_id: str) -> StatusResult:
        proc = subprocess.run(
            ["ssh", host, "bash", "-lc", f"if kill -0 {job_id} 2>/dev/null; then echo RUNNING; else echo EXITED; fi"],
            capture_output=True,
            text=True,
            check=True,
        )
        return StatusResult(state=proc.stdout.strip(), raw=proc.stdout)

    def accounting_status(self, host: str, job_id: str) -> StatusResult:
        return StatusResult(state="NOT_FOUND", raw="")


def get_adapter(scheduler: str) -> SchedulerAdapter:
    scheduler = scheduler.lower()
    if scheduler == "sge":
        return SGEAdapter()
    if scheduler == "univa":
        return UnivaAdapter()
    if scheduler == "pbs":
        return PBSAdapter()
    if scheduler == "slurm":
        return SlurmAdapter()
    if scheduler == "lsf":
        return LSFAdapter()
    if scheduler == "none":
        return LocalAdapter()
    raise ValueError(f"Unsupported scheduler: {scheduler}")
