from __future__ import annotations

import os
import shlex
import subprocess
from pathlib import Path
from typing import Any

import typer

from cluster_dispatch.config import ProjectConfig, TargetConfig, find_project_root
from cluster_dispatch.index import auto_index_active_root, write_index_manifest_for_scope
from cluster_dispatch.operations import OperationRuntime
from cluster_dispatch.sweeps import SweepRuntime
from cluster_dispatch.transport import ssh_command

VERBOSITY_LEVEL = 0
REMOTE_VERBOSE = False
QUIET_MODE = False
_SUBPROCESS_RUN = subprocess.run


def configure_output(verbose: int, remote_verbose: bool, quiet: bool) -> None:
    global VERBOSITY_LEVEL, REMOTE_VERBOSE, QUIET_MODE
    VERBOSITY_LEVEL = max(0, int(verbose))
    REMOTE_VERBOSE = bool(remote_verbose)
    QUIET_MODE = bool(quiet)
    os.environ["CDP_VERBOSE_LEVEL"] = str(VERBOSITY_LEVEL)
    os.environ["CDP_REMOTE_VERBOSE"] = "1" if REMOTE_VERBOSE else "0"
    os.environ["CDP_QUIET"] = "1" if QUIET_MODE else "0"


def project_root() -> Path:
    return find_project_root()


def _is_remote_command(command: Any) -> bool:
    return isinstance(command, list) and bool(command) and str(command[0]).strip().lower() in {
        "ssh", "rsync", "scp", "sftp"
    }


def _render_command(command: Any) -> str:
    return " ".join(shlex.quote(str(part)) for part in command) if isinstance(command, list) else str(command)


def run_direct(*args: Any, **kwargs: Any) -> subprocess.CompletedProcess[str]:
    command = args[0] if args else kwargs.get("args")
    remote = _is_remote_command(command)
    if remote and (REMOTE_VERBOSE or VERBOSITY_LEVEL >= 1) and not QUIET_MODE:
        typer.echo(f"[remote] $ {_render_command(command)}", err=True)
    if remote and "input" not in kwargs and kwargs.get("stdin") is None:
        kwargs["stdin"] = subprocess.DEVNULL
    result = _SUBPROCESS_RUN(*args, **kwargs)
    if remote and not QUIET_MODE and (REMOTE_VERBOSE or VERBOSITY_LEVEL >= 2):
        if isinstance(getattr(result, "stdout", None), str) and result.stdout.strip():
            typer.echo(f"[remote][stdout]\n{result.stdout.rstrip()}", err=True)
        if isinstance(getattr(result, "stderr", None), str) and result.stderr.strip():
            typer.echo(f"[remote][stderr]\n{result.stderr.rstrip()}", err=True)
    return result


def run_command(command: list[str], check: bool = True) -> subprocess.CompletedProcess[str]:
    return run_direct(command, text=True, capture_output=True, check=check)


def get_git_info(root: Path) -> dict[str, Any] | None:
    repo = run_direct(["git", "-C", str(root), "rev-parse", "--show-toplevel"], text=True, capture_output=True, check=False)
    if repo.returncode != 0:
        return None
    branch = run_direct(["git", "-C", str(root), "rev-parse", "--abbrev-ref", "HEAD"], text=True, capture_output=True, check=False)
    head = run_direct(["git", "-C", str(root), "rev-parse", "HEAD"], text=True, capture_output=True, check=False)
    dirty = run_direct(["git", "-C", str(root), "status", "--porcelain"], text=True, capture_output=True, check=False)
    return {
        "repo_root": repo.stdout.strip(),
        "branch": branch.stdout.strip() if branch.returncode == 0 else "",
        "head": head.stdout.strip() if head.returncode == 0 else "",
        "dirty": bool(dirty.stdout.strip()) if dirty.returncode == 0 else False,
    }


def write_remote_script(
    root: Path,
    cfg: ProjectConfig,
    target_name: str,
    target: TargetConfig,
    remote_path: str,
    content: str,
    executable: bool = True,
) -> None:
    command = f"cat > {shlex.quote(remote_path)}"
    if executable:
        command += f" && chmod +x {shlex.quote(remote_path)}"
    result = run_direct(
        ssh_command(root, cfg, target_name, target, command),
        input=content,
        text=True,
        capture_output=True,
        check=False,
    )
    if result.returncode != 0:
        detail = (result.stderr or result.stdout or "unknown remote write error").strip()
        raise typer.BadParameter(f"Failed to write remote script {remote_path}: {detail}")


def render_scheduler_header(
    header: str,
    cpus: int,
    memory: str,
    walltime: str,
    job_name: str,
    stdout: str,
    stderr: str,
    working_dir: str,
    queue: str,
    node: str,
    parallel_environment: str,
) -> str:
    try:
        return header.format(
            cpus=cpus, memory=memory, time=walltime, job_name=job_name, stdout=stdout,
            stderr=stderr, working_dir=working_dir, queue=queue, node=node,
            parallel_environment=parallel_environment,
        )
    except KeyError as exc:
        raise typer.BadParameter(
            f"Scheduler template contains unknown placeholder '{exc.args[0]}'. "
            "Allowed: cpus, memory, time, job_name, stdout, stderr, working_dir, queue, node, parallel_environment."
        ) from exc


def operation_runtime() -> OperationRuntime:
    return OperationRuntime(
        run_command=run_command,
        render_scheduler_header=render_scheduler_header,
        write_remote_script=write_remote_script,
        git_info=get_git_info,
    )


def sweep_runtime() -> SweepRuntime:
    return SweepRuntime(operation=operation_runtime(), run_direct=run_direct)


def write_index_manifest(**kwargs: Any) -> dict[str, Any]:
    return write_index_manifest_for_scope(**kwargs, run_direct=run_direct)


def auto_index(root: Path, cfg: ProjectConfig, target_name: str, target: TargetConfig) -> str | None:
    return auto_index_active_root(root, cfg, target_name, target, run_direct=run_direct)
