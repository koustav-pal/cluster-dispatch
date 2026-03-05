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


def get_adapter(scheduler: str) -> SchedulerAdapter:
    scheduler = scheduler.lower()
    if scheduler == "sge":
        return SGEAdapter()
    if scheduler == "pbs":
        return PBSAdapter()
    if scheduler == "none":
        return LocalAdapter()
    raise ValueError(f"Unsupported scheduler: {scheduler}")
