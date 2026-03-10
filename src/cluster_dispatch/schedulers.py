from __future__ import annotations

import subprocess
import os
import shlex
import sys
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
    def submit(self, host: str, submit_script: str, transport: str = "ssh") -> SubmitResult:
        ...

    def status(self, host: str, job_id: str, transport: str = "ssh") -> StatusResult:
        ...

    def accounting_status(self, host: str, job_id: str, transport: str = "ssh") -> StatusResult:
        ...


def _is_local_host(host: str) -> bool:
    normalized = host.strip().lower()
    return normalized in {"", "localhost", "127.0.0.1", "::1"}


def _uses_local_transport(transport: str, host: str) -> bool:
    return transport.strip().lower() == "local" or _is_local_host(host)


def _run_shell(host: str, command: str, transport: str = "ssh", check: bool = False) -> subprocess.CompletedProcess[str]:
    verbose_level = int(os.environ.get("CDP_VERBOSE_LEVEL", "0") or "0")
    remote_verbose = os.environ.get("CDP_REMOTE_VERBOSE", "0") == "1"
    quiet_mode = os.environ.get("CDP_QUIET", "0") == "1"
    is_local = _uses_local_transport(transport, host)
    if not is_local and not quiet_mode and (remote_verbose or verbose_level >= 1):
        print(f"[remote] $ ssh {shlex.quote(host)} bash -lc {shlex.quote(command)}", file=sys.stderr)

    if _uses_local_transport(transport, host):
        proc = subprocess.run(["bash", "-lc", command], capture_output=True, text=True, check=check)
    else:
        proc = subprocess.run(
            ["ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=20", host, "bash", "-lc", command],
            capture_output=True,
            text=True,
            check=check,
            stdin=subprocess.DEVNULL,
        )

    if not is_local and not quiet_mode and (remote_verbose or verbose_level >= 2):
        if proc.stdout.strip():
            print(f"[remote][stdout]\n{proc.stdout.rstrip()}", file=sys.stderr)
        if proc.stderr.strip():
            print(f"[remote][stderr]\n{proc.stderr.rstrip()}", file=sys.stderr)
    return proc


class SGEAdapter:
    def submit(self, host: str, submit_script: str, transport: str = "ssh") -> SubmitResult:
        proc = _run_shell(host, f"qsub {submit_script}", transport=transport, check=False)
        if proc.returncode != 0:
            detail = (proc.stderr or proc.stdout or f"exit_code={proc.returncode}").strip()
            raise RuntimeError(f"qsub failed: {detail}")
        output = proc.stdout.strip()
        # Typical format: Your job 1234 ("name") has been submitted
        job_id = output.split()[2] if len(output.split()) >= 3 else output
        return SubmitResult(job_id=job_id)

    def status(self, host: str, job_id: str, transport: str = "ssh") -> StatusResult:
        proc = _run_shell(host, f"qstat -j {job_id}", transport=transport)
        if proc.returncode == 0:
            return StatusResult(state="RUNNING_OR_QUEUED", raw=proc.stdout)
        return StatusResult(state="NOT_FOUND", raw=proc.stderr or proc.stdout)

    def accounting_status(self, host: str, job_id: str, transport: str = "ssh") -> StatusResult:
        proc = _run_shell(host, f"qacct -j {job_id}", transport=transport)
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
    def submit(self, host: str, submit_script: str, transport: str = "ssh") -> SubmitResult:
        proc = _run_shell(host, f"qsub {submit_script}", transport=transport, check=False)
        if proc.returncode != 0:
            detail = (proc.stderr or proc.stdout or f"exit_code={proc.returncode}").strip()
            raise RuntimeError(f"qsub failed: {detail}")
        output = proc.stdout.strip()
        job_id = output.split()[0] if output else ""
        return SubmitResult(job_id=job_id)

    def status(self, host: str, job_id: str, transport: str = "ssh") -> StatusResult:
        proc = _run_shell(host, f"qstat -f {job_id}", transport=transport)
        if proc.returncode != 0:
            return StatusResult(state="NOT_FOUND", raw=proc.stderr or proc.stdout)

        state = "UNKNOWN"
        for line in proc.stdout.splitlines():
            if "job_state =" in line:
                state = line.split("=", 1)[1].strip()
                break
        return StatusResult(state=state, raw=proc.stdout)

    def accounting_status(self, host: str, job_id: str, transport: str = "ssh") -> StatusResult:
        proc = _run_shell(host, f"qstat -xf {job_id}", transport=transport)
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
    def submit(self, host: str, submit_script: str, transport: str = "ssh") -> SubmitResult:
        proc = _run_shell(host, f"sbatch {submit_script}", transport=transport, check=False)
        if proc.returncode != 0:
            detail = (proc.stderr or proc.stdout or f"exit_code={proc.returncode}").strip()
            raise RuntimeError(f"sbatch failed: {detail}")
        output = proc.stdout.strip()
        # Typical format: Submitted batch job 12345
        job_id = output.split()[-1] if output else ""
        return SubmitResult(job_id=job_id)

    def status(self, host: str, job_id: str, transport: str = "ssh") -> StatusResult:
        proc = _run_shell(host, f"squeue -h -j {job_id} -o %T", transport=transport)
        if proc.returncode != 0:
            return StatusResult(state="NOT_FOUND", raw=proc.stderr or proc.stdout)

        state = proc.stdout.strip()
        if not state:
            return StatusResult(state="NOT_FOUND", raw=proc.stdout)
        return StatusResult(state=state, raw=proc.stdout)

    def accounting_status(self, host: str, job_id: str, transport: str = "ssh") -> StatusResult:
        proc = _run_shell(host, f"sacct -j {job_id} -X -n -o State | head -n 1", transport=transport)
        if proc.returncode != 0:
            return StatusResult(state="NOT_FOUND", raw=proc.stderr or proc.stdout)
        state = proc.stdout.strip().split()[0] if proc.stdout.strip() else ""
        if not state:
            return StatusResult(state="NOT_FOUND", raw=proc.stdout)
        state = state.split("+", 1)[0]
        return StatusResult(state=state, raw=proc.stdout)


class LSFAdapter:
    def submit(self, host: str, submit_script: str, transport: str = "ssh") -> SubmitResult:
        proc = _run_shell(host, f"bsub < {submit_script}", transport=transport, check=False)
        if proc.returncode != 0:
            detail = (proc.stderr or proc.stdout or f"exit_code={proc.returncode}").strip()
            raise RuntimeError(f"bsub failed: {detail}")
        output = proc.stdout.strip()
        # Typical format: Job <12345> is submitted to default queue <normal>.
        job_id = output
        if "<" in output and ">" in output:
            job_id = output.split("<", 1)[1].split(">", 1)[0]
        return SubmitResult(job_id=job_id)

    def status(self, host: str, job_id: str, transport: str = "ssh") -> StatusResult:
        proc = _run_shell(host, f"bjobs -noheader -o STAT {job_id}", transport=transport)
        if proc.returncode != 0:
            return StatusResult(state="NOT_FOUND", raw=proc.stderr or proc.stdout)

        state = proc.stdout.strip().splitlines()[0].strip() if proc.stdout.strip() else "UNKNOWN"
        return StatusResult(state=state, raw=proc.stdout)

    def accounting_status(self, host: str, job_id: str, transport: str = "ssh") -> StatusResult:
        proc = _run_shell(host, f"bjobs -a -noheader -o STAT {job_id}", transport=transport)
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
    def submit(self, host: str, submit_script: str, transport: str = "ssh") -> SubmitResult:
        proc = _run_shell(
            host,
            f"nohup bash {submit_script} >/dev/null 2>&1 & echo $!",
            transport=transport,
            check=False,
        )
        if proc.returncode != 0:
            detail = (proc.stderr or proc.stdout or f"exit_code={proc.returncode}").strip()
            raise RuntimeError(f"local submit failed: {detail}")
        return SubmitResult(job_id=proc.stdout.strip())

    def status(self, host: str, job_id: str, transport: str = "ssh") -> StatusResult:
        proc = _run_shell(host, f"if kill -0 {job_id} 2>/dev/null; then echo RUNNING; else echo EXITED; fi", transport=transport, check=True)
        return StatusResult(state=proc.stdout.strip(), raw=proc.stdout)

    def accounting_status(self, host: str, job_id: str, transport: str = "ssh") -> StatusResult:
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
