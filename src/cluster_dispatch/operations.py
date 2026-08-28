from __future__ import annotations

import hashlib
import json
import shlex
import socket
import sys
from dataclasses import dataclass, field
from datetime import datetime
from importlib.metadata import PackageNotFoundError, version
from pathlib import Path
from typing import Any, Callable

from cluster_dispatch.config import CONFIG_DIR, LastJob, load_state, save_state
from cluster_dispatch.context import Context
from cluster_dispatch.models import (
    IndexRequest,
    JobRecord,
    PullRequest,
    PushRequest,
    ResourceSpec,
    RunPaths,
    RunRequest,
    SyncRecord,
)
from cluster_dispatch.records import save_job, save_sync
from cluster_dispatch.schedulers import get_adapter
from cluster_dispatch.transport import ensure_ssh_config, is_local_transport, remote_spec, ssh_config_path

RUNS_DIR = "runs"


class OperationError(ValueError):
    pass


@dataclass(frozen=True)
class OperationRuntime:
    run_command: Callable[..., Any]
    render_scheduler_header: Callable[..., str]
    write_remote_script: Callable[..., None]
    git_info: Callable[[Path], dict[str, Any] | None]


@dataclass(frozen=True)
class SyncPlan:
    context: Context
    action: str
    mode: str
    source: str
    destination: str
    commands: list[list[str]]
    mkdir_command: list[str] | None = None
    local_target: bool = False
    ignore_file: Path | None = None
    include_remote_only: bool = False
    skipped: list[str] = field(default_factory=list)


@dataclass
class SyncResult:
    plan: SyncPlan
    dry_run: bool
    pulled: list[str] = field(default_factory=list)
    skipped: list[str] = field(default_factory=list)
    record_path: Path | None = None

    @property
    def commands(self) -> list[list[str]]:
        return self.plan.commands


@dataclass(frozen=True)
class RunPlan:
    context: Context
    request: RunRequest
    run_id: str
    paths: RunPaths
    submit_script: str
    stdout: str
    stderr: str
    push_plan: SyncPlan


@dataclass(frozen=True)
class RunResult:
    plan: RunPlan
    dry_run: bool
    job_id: str | None = None
    record_path: Path | None = None


@dataclass(frozen=True)
class IndexPlan:
    context: Context
    request: IndexRequest
    scopes: list[dict[str, str]]


@dataclass(frozen=True)
class IndexResult:
    plan: IndexPlan
    dry_run: bool
    manifests: list[dict[str, Any]] = field(default_factory=list)


def _ssh_preview(context: Context, *args: str) -> list[str]:
    return ["ssh", "-F", str(ssh_config_path(context.project_root)), context.target_name, *args]


def _rsync_preview(context: Context, base: list[str]) -> list[str]:
    if is_local_transport(context.target.transport, context.target.host):
        return base
    remote_shell = shlex.join(["ssh", "-F", str(ssh_config_path(context.project_root))])
    return [base[0], "-e", remote_shell, *base[1:]]


def prepare_push(context: Context) -> SyncPlan:
    local_target = is_local_transport(context.target.transport, context.target.host)
    ignore_file = context.project_root / ".cdpignore"
    base = ["rsync", "-az"]
    if ignore_file.exists():
        base.extend(["--exclude-from", str(ignore_file)])
    source = f"{context.local_analysis}/"
    destination = f"{context.remote_analysis}/"
    if local_target:
        base.extend([source, destination])
    else:
        base.extend([source, remote_spec(context.target_name, destination)])
    return SyncPlan(
        context=context,
        action="push",
        mode="push",
        source=str(context.local_analysis),
        destination=str(context.remote_analysis),
        commands=[_rsync_preview(context, base)],
        mkdir_command=["mkdir", "-p", str(context.remote_analysis)],
        local_target=local_target,
        ignore_file=ignore_file if ignore_file.exists() else None,
    )


def execute_push(
    plan: SyncPlan,
    *,
    run_command: Callable[..., Any],
    record: bool = True,
) -> SyncResult:
    context = plan.context
    if plan.local_target:
        Path(plan.destination).mkdir(parents=True, exist_ok=True)
    else:
        ensure_ssh_config(context.project_root, context.config)
        run_command(_ssh_preview(context, *(plan.mkdir_command or [])))
    run_command(plan.commands[0])
    result = SyncResult(plan=plan, dry_run=False)
    if record:
        result.record_path = save_sync(
            context.project_root,
            SyncRecord(
                action="push",
                target=context.target_name,
                analysis=context.analysis.as_posix(),
                scheduler=context.target.scheduler,
                transport=context.target.transport,
                source=plan.source,
                destination=plan.destination,
                ignore_file=str(plan.ignore_file) if plan.ignore_file else None,
                ignore_used=plan.ignore_file is not None,
            ),
        )
    return result


def push(context: Context, request: PushRequest, *, run_command: Callable[..., Any]) -> SyncResult:
    plan = prepare_push(context)
    if request.dry_run:
        return SyncResult(plan=plan, dry_run=True)
    return execute_push(plan, run_command=run_command, record=request.record)


def prepare_pull(context: Context, request: PullRequest) -> SyncPlan:
    local_target = is_local_transport(context.target.transport, context.target.host)
    source = str(context.remote_analysis)
    destination = str(context.local_analysis)
    if request.all_paths:
        if not request.dry_run and not request.confirmed:
            raise OperationError("Full analysis pull may overwrite local files. Re-run with --yes to confirm.")
        base = ["rsync", "-az"]
        remote_source = f"{context.remote_analysis}/"
        if local_target:
            base.extend([remote_source, f"{context.local_analysis}/"])
        else:
            base.extend([remote_spec(context.target_name, remote_source), f"{context.local_analysis}/"])
        return SyncPlan(
            context=context,
            action="pull",
            mode="all",
            source=source,
            destination=destination,
            commands=[_rsync_preview(context, base)],
            local_target=local_target,
        )

    tags = context.config.analysis_tags.get(context.analysis.as_posix(), [])
    if not tags:
        raise OperationError("No tags found for active analysis. Tag paths with `cdp analysis tag <path>` before pulling.")
    commands: list[list[str]] = []
    skipped: list[str] = []
    for raw_tag in tags:
        tag = raw_tag.strip("/")
        if not tag:
            continue
        if not request.include_remote_only and not (context.local_analysis / tag).exists():
            skipped.append(tag)
            continue
        tagged_remote = f"{context.remote_analysis}/./{tag}"
        base = ["rsync", "-az", "--relative"]
        if local_target:
            base.extend([tagged_remote, f"{context.local_analysis}/"])
        else:
            base.extend([remote_spec(context.target_name, tagged_remote), f"{context.local_analysis}/"])
        commands.append(_rsync_preview(context, base))
    return SyncPlan(
        context=context,
        action="pull",
        mode="tagged",
        source=source,
        destination=destination,
        commands=commands,
        local_target=local_target,
        include_remote_only=request.include_remote_only,
        skipped=skipped,
    )


def execute_pull(plan: SyncPlan, *, run_command: Callable[..., Any]) -> SyncResult:
    context = plan.context
    if not plan.local_target:
        ensure_ssh_config(context.project_root, context.config)
    result = SyncResult(plan=plan, dry_run=False, skipped=list(plan.skipped))
    for command in plan.commands:
        proc = run_command(command, check=plan.mode == "all")
        if plan.mode == "all":
            continue
        source_arg = command[-2]
        tag = source_arg.split("/./", 1)[-1] if "/./" in source_arg else ""
        if getattr(proc, "returncode", 0) == 0:
            result.pulled.append(tag)
        elif tag:
            result.skipped.append(tag)
    result.record_path = save_sync(
        context.project_root,
        SyncRecord(
            action="pull",
            mode=plan.mode,
            target=context.target_name,
            analysis=context.analysis.as_posix(),
            scheduler=context.target.scheduler,
            transport=context.target.transport,
            source=plan.source,
            destination=plan.destination,
            include_remote_only_tags=plan.include_remote_only if plan.mode == "tagged" else None,
            pulled=result.pulled if plan.mode == "tagged" else None,
            skipped=result.skipped if plan.mode == "tagged" else None,
        ),
    )
    return result


def pull(context: Context, request: PullRequest, *, run_command: Callable[..., Any]) -> SyncResult:
    plan = prepare_pull(context, request)
    if request.dry_run:
        return SyncResult(plan=plan, dry_run=True, skipped=list(plan.skipped))
    return execute_pull(plan, run_command=run_command)


def deterministic_run_id(context: Context, request: RunRequest) -> str:
    if request.run_id:
        return request.run_id
    resources = request.resources
    payload = {
        "target": context.target_name,
        "analysis": context.analysis.as_posix(),
        "command": request.command,
        "scheduler": context.target.scheduler,
        "resources": {
            "profile": resources.profile,
            "cpus": resources.cpus,
            "memory": resources.memory,
            "time": resources.time,
            "node": resources.node,
            "queue": resources.queue,
            "parallel_environment": resources.parallel_environment,
            "job_name": request.job_name,
        },
    }
    material = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
    return f"run-{hashlib.sha256(material.encode()).hexdigest()[:16]}"


def build_submit_script(header: str, command: str, working_dir: str, remote_log_file: str) -> str:
    lines = ["#!/usr/bin/env bash"]
    if header.strip():
        lines.extend(header.strip().splitlines())
    lines.extend(
        [
            "set -euo pipefail",
            f"cd {shlex.quote(working_dir)}",
            'echo "[cluster-dispatch] started $(date -Iseconds)"',
            f"{command} >> {shlex.quote(remote_log_file)} 2>&1",
            'echo "[cluster-dispatch] finished $(date -Iseconds)"',
        ]
    )
    return "\n".join(lines) + "\n"


def prepare_run(context: Context, request: RunRequest, *, render_scheduler_header: Callable[..., str]) -> RunPlan:
    resources = request.resources
    if "{queue}" in context.target.template_header and not resources.queue:
        raise OperationError("--queue is required (or set target default_queue) because template includes {queue}.")
    if "{parallel_environment}" in context.target.template_header and not resources.parallel_environment:
        raise OperationError(
            "--parallel-environment is required (or set target default_parallel_environment) "
            "because template includes {parallel_environment}."
        )
    run_id = deterministic_run_id(context, request)
    remote_runs = Path(context.target.remote_root.rstrip("/")) / CONFIG_DIR / RUNS_DIR / context.analysis
    remote_run_dir = remote_runs / run_id
    submit_script_path = remote_run_dir / "pc_submit.sh"
    remote_log_file = remote_run_dir / "run.log"
    stdout = remote_run_dir / "stdout"
    stderr = remote_run_dir / "stderr"
    paths = RunPaths(
        project_root=str(context.project_root),
        analysis_local_path=str(context.local_analysis),
        analysis_remote_path=str(context.remote_analysis),
        remote_run_dir=str(remote_run_dir),
        remote_log_file=str(remote_log_file),
        submit_script=str(submit_script_path),
        working_dir=str(context.remote_workdir),
    )
    header = render_scheduler_header(
        context.target.template_header,
        cpus=resources.cpus,
        memory=resources.memory,
        walltime=resources.time,
        job_name=request.job_name,
        stdout=str(stdout),
        stderr=str(stderr),
        working_dir=str(context.remote_workdir),
        queue=resources.queue,
        node=resources.node,
        parallel_environment=resources.parallel_environment,
    )
    return RunPlan(
        context=context,
        request=request,
        run_id=run_id,
        paths=paths,
        submit_script=build_submit_script(header, request.command, paths.working_dir, paths.remote_log_file),
        stdout=str(stdout),
        stderr=str(stderr),
        push_plan=prepare_push(context),
    )


def _package_version() -> str | None:
    try:
        return version("cluster-dispatch")
    except PackageNotFoundError:
        return None


def _job_record(plan: RunPlan, job_id: str, submitted_at: str, git_info: dict[str, Any] | None) -> dict[str, Any]:
    context = plan.context
    request = plan.request
    resources: ResourceSpec = request.resources
    record = JobRecord(
        submitted_at=submitted_at,
        analysis=context.analysis.as_posix(),
        analysis_tags=context.config.analysis_tags.get(context.analysis.as_posix(), []),
        target=context.target_name,
        scheduler=context.target.scheduler,
        job_id=job_id,
        run_id=plan.run_id,
        job_name=request.job_name,
        state="RUNNING" if context.target.scheduler == "none" else "RUNNING_OR_QUEUED",
        stdout=plan.stdout,
        stderr=plan.stderr,
        working_dir=plan.paths.working_dir,
        cpus=resources.cpus,
        memory=resources.memory,
        time=resources.time,
        queue=resources.queue,
        parallel_environment=resources.parallel_environment,
        node=resources.node,
        command=request.command,
        command_resolved=request.command,
        remote_run_dir=plan.paths.remote_run_dir,
        remote_log_file=plan.paths.remote_log_file,
        submission_mode=request.submission_mode,
        record_version=2,
        resources=resources,
        paths=plan.paths,
        sync={
            "ignore_file": str(plan.push_plan.ignore_file) if plan.push_plan.ignore_file else None,
            "ignore_detected": plan.push_plan.ignore_file is not None,
            "ignore_used": plan.push_plan.ignore_file is not None,
            "source": plan.push_plan.source,
            "destination": plan.push_plan.destination,
        },
        environment={
            "cluster_dispatch_version": _package_version(),
            "python_version": sys.version.split()[0],
            "hostname": socket.gethostname(),
        },
    ).to_dict()
    record.update(request.record_fields)
    if git_info is not None:
        record["git"] = {
            "repo_root": git_info.get("repo_root"),
            "branch": git_info.get("branch"),
            "commit": git_info.get("head"),
            "dirty": git_info.get("dirty"),
        }
    return record


def execute_run(plan: RunPlan, *, runtime: OperationRuntime) -> RunResult:
    context = plan.context
    if plan.request.sync_before:
        execute_push(plan.push_plan, run_command=runtime.run_command, record=False)
    if is_local_transport(context.target.transport, context.target.host):
        script_path = Path(plan.paths.submit_script)
        script_path.parent.mkdir(parents=True, exist_ok=True)
        script_path.write_text(plan.submit_script)
        script_path.chmod(0o755)
    else:
        runtime.run_command(_ssh_preview(context, "mkdir", "-p", plan.paths.remote_run_dir))
        runtime.write_remote_script(
            context.project_root,
            context.config,
            context.target_name,
            context.target,
            plan.paths.submit_script,
            plan.submit_script,
            executable=True,
        )
    try:
        job_id = get_adapter(context.target.scheduler).submit(
            context.target.host,
            plan.paths.submit_script,
            transport=context.target.transport,
            project_root=context.project_root,
            cfg=context.config,
            target_name=context.target_name,
            target_cfg=context.target,
        ).job_id
    except Exception as exc:
        raise OperationError(f"Failed to submit job: {exc}") from exc

    submitted_at = datetime.now().isoformat(timespec="seconds")
    state = load_state(context.project_root)
    state["last_job"] = LastJob(
        job_id=job_id,
        target=context.target_name,
        scheduler=context.target.scheduler,
        remote_run_dir=plan.paths.remote_run_dir,
        remote_log_file=plan.paths.remote_log_file,
        run_id=plan.run_id,
    ).__dict__
    state["last_updated"] = submitted_at
    save_state(context.project_root, state)
    record_path = save_job(
        context.project_root,
        _job_record(plan, job_id, submitted_at, runtime.git_info(context.project_root)),
    )
    return RunResult(plan=plan, dry_run=False, job_id=job_id, record_path=record_path)


def run(context: Context, request: RunRequest, *, dry_run: bool, runtime: OperationRuntime) -> RunResult:
    plan = prepare_run(context, request, render_scheduler_header=runtime.render_scheduler_header)
    if dry_run:
        return RunResult(plan=plan, dry_run=True)
    return execute_run(plan, runtime=runtime)


def prepare_index(
    context: Context,
    request: IndexRequest,
    *,
    resolve_scopes: Callable[..., list[dict[str, str]]],
) -> IndexPlan:
    scopes = resolve_scopes(
        analysis_dir=context.local_analysis,
        analysis_rel=context.analysis.as_posix(),
        target_cfg=context.target,
        remote_analysis_root=str(context.remote_analysis),
        local_path=request.local_path,
        remote_path=request.remote_path,
        all_tags=request.all_tags,
        analysis_tags=context.config.analysis_tags.get(context.analysis.as_posix(), []),
    )
    return IndexPlan(context=context, request=request, scopes=scopes)


def execute_index(
    plan: IndexPlan,
    *,
    write_manifest: Callable[..., dict[str, Any]],
) -> IndexResult:
    context = plan.context
    manifests: list[dict[str, Any]] = []
    for scope in plan.scopes:
        manifest = write_manifest(
            project_root=context.project_root,
            analysis_rel=context.analysis.as_posix(),
            target_name=context.target_name,
            target_cfg=context.target,
            scope_rel=scope["scope"],
            scope_remote=scope["remote_scope"],
        )
        manifests.append({"scope": scope["scope"], "manifest_path": str(manifest.get("_manifest_path", "")), **manifest})
    return IndexResult(plan=plan, dry_run=False, manifests=manifests)


def index(
    context: Context,
    request: IndexRequest,
    *,
    resolve_scopes: Callable[..., list[dict[str, str]]],
    write_manifest: Callable[..., dict[str, Any]],
) -> IndexResult:
    plan = prepare_index(context, request, resolve_scopes=resolve_scopes)
    if request.dry_run:
        return IndexResult(plan=plan, dry_run=True)
    return execute_index(plan, write_manifest=write_manifest)
