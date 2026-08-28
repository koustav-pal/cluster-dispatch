from __future__ import annotations

import shlex
import subprocess
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Iterator

from cluster_dispatch.config import ProjectConfig, load_config
from cluster_dispatch.context import Context, ContextError, resolve_context
from cluster_dispatch.index import (
    index_entry_path_exists,
    index_file_count_and_bytes,
    load_index_manifest,
    output_check_from_index,
)
from cluster_dispatch.models import ResourceSpec, RunRequest
from cluster_dispatch.records import list_jobs, update_job
from cluster_dispatch.schedulers import cancel_command, get_adapter
from cluster_dispatch.transport import is_local_transport, remote_spec, rsync_command, ssh_command


class JobError(RuntimeError):
    pass


@dataclass(frozen=True)
class CancelResult:
    matched: int
    cancelled: list[str]
    failed: list[str]


@dataclass(frozen=True)
class LogResult:
    record: dict[str, Any]
    path: str
    content: str | None
    follow_command: list[str] | None
    local: bool
    match_count: int


@dataclass(frozen=True)
class WatchUpdate:
    record: dict[str, Any]
    state: str
    log_tail: str
    timestamp: str


@dataclass(frozen=True)
class RetrySpec:
    context: Context
    request: RunRequest
    match_count: int


@dataclass(frozen=True)
class CollectResult:
    record: dict[str, Any]
    analysis_dir: Path
    estimated_files: int
    estimated_bytes: int
    index_missing: list[str]
    pulled: list[str]
    skipped: list[str]
    match_count: int


def is_running_state(state: str) -> bool:
    return state.strip().upper() in {"RUNNING", "RUNNING_OR_QUEUED", "QUEUED", "PENDING", "R", "Q"}


def is_terminal_state(state: str) -> bool:
    normalized = state.strip().upper()
    return bool(normalized) and not is_running_state(normalized) and normalized not in {"UNKNOWN", "NOT_FOUND"}


def is_failed_state(state: str) -> bool:
    normalized = state.strip().upper()
    return normalized in {"EXIT", "FAILED"} or any(
        token in normalized for token in ("FAIL", "ERROR", "CANCEL", "TIMEOUT", "OOM", "NODE_FAIL", "PREEMPT")
    )


def filter_jobs(
    project_root: Path,
    *,
    target: str | None = None,
    analysis: str | None = None,
    job_id: str | None = None,
    job_name: str | None = None,
    limit: int | None = None,
) -> list[dict[str, Any]]:
    records = [
        record
        for record in list_jobs(project_root)
        if (not target or str(record.get("target", "")) == target)
        and (not analysis or str(record.get("analysis", "")) == analysis)
        and (not job_id or str(record.get("job_id", "")) == job_id)
        and (not job_name or str(record.get("job_name", "")) == job_name)
    ]
    return records if limit is None else records[:limit]


def select_job(
    project_root: Path,
    *,
    target: str | None = None,
    analysis: str | None = None,
    job_id: str | None = None,
    job_name: str | None = None,
) -> tuple[dict[str, Any], int]:
    matches = filter_jobs(
        project_root, target=target, analysis=analysis, job_id=job_id, job_name=job_name
    )
    if not matches:
        raise JobError("No matching job records found for the provided filters")
    return matches[0], len(matches)


def resolve_job_state(project_root: Path, cfg: ProjectConfig, payload: dict[str, Any]) -> str:
    record_state = str(payload.get("state", "")).strip()
    target_name = str(payload.get("target", ""))
    job_id = str(payload.get("job_id", ""))
    scheduler = str(payload.get("scheduler", ""))
    record_path = Path(str(payload.get("_record_file", ""))) if payload.get("_record_file") else None

    if target_name not in cfg.targets:
        state = "TARGET_MISSING"
    elif record_state and not is_running_state(record_state):
        return record_state
    else:
        target_cfg = cfg.targets[target_name]
        adapter = get_adapter(scheduler)
        state = adapter.status(
            target_cfg.host,
            job_id,
            transport=target_cfg.transport,
            project_root=project_root,
            cfg=cfg,
            target_name=target_name,
            target_cfg=target_cfg,
        ).state
        if state == "NOT_FOUND":
            stored = str(payload.get("final_state", "")).strip()
            if stored:
                state = stored
            else:
                accounting = adapter.accounting_status(
                    target_cfg.host,
                    job_id,
                    transport=target_cfg.transport,
                    project_root=project_root,
                    cfg=cfg,
                    target_name=target_name,
                    target_cfg=target_cfg,
                )
                if accounting.state != "NOT_FOUND":
                    state = accounting.state
                    payload.update(
                        final_state=accounting.state,
                        final_state_source="accounting",
                        final_state_at=datetime.now().isoformat(timespec="seconds"),
                        final_state_raw=accounting.raw,
                    )

    if state and state != record_state:
        payload["state"] = state
        if record_path is not None:
            stored_payload = {key: value for key, value in payload.items() if key != "_record_file"}
            update_job(record_path, stored_payload)
    return state


def query_jobs(
    project_root: Path,
    *,
    target: str | None = None,
    analysis: str | None = None,
    job_id: str | None = None,
    job_name: str | None = None,
    limit: int | None = None,
    refresh_state: bool = False,
) -> list[dict[str, Any]]:
    records = filter_jobs(
        project_root, target=target, analysis=analysis, job_id=job_id, job_name=job_name, limit=limit
    )
    if not refresh_state:
        return records
    cfg = load_config(project_root)
    rows: list[dict[str, Any]] = []
    for record in records:
        state = resolve_job_state(project_root, cfg, record)
        rows.append(
            {
                **record,
                "state": state,
                "output_check": output_check_from_index(project_root, record) if is_terminal_state(state) else "N/A",
            }
        )
    return rows


def unique_jobs(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    seen: set[tuple[str, str, str]] = set()
    unique: list[dict[str, Any]] = []
    for record in records:
        key = tuple(str(record.get(name, "")) for name in ("target", "scheduler", "job_id"))
        if key not in seen:
            seen.add(key)
            unique.append(record)
    return unique


def cancel_jobs(
    project_root: Path,
    records: list[dict[str, Any]],
    *,
    run: Callable[..., subprocess.CompletedProcess[str]] = subprocess.run,
) -> CancelResult:
    cfg = load_config(project_root)
    cancelled: list[str] = []
    failed: list[str] = []
    jobs = unique_jobs(records)
    for record in jobs:
        target_name = str(record.get("target", ""))
        scheduler = str(record.get("scheduler", ""))
        job_id = str(record.get("job_id", ""))
        job_name = str(record.get("job_name", ""))
        if not target_name or not job_id:
            failed.append(f"job_id={job_id or '<missing>'} target={target_name or '<missing>'} reason=missing_metadata")
            continue
        if target_name not in cfg.targets:
            failed.append(f"job_id={job_id} target={target_name} reason=target_not_configured")
            continue
        try:
            command = cancel_command(scheduler, job_id)
        except ValueError as exc:
            raise JobError(str(exc)) from exc
        target_cfg = cfg.targets[target_name]
        argv = (
            ["bash", "-lc", command]
            if is_local_transport(target_cfg.transport, target_cfg.host)
            else ssh_command(project_root, cfg, target_name, target_cfg, command)
        )
        proc = run(argv, text=True, capture_output=True, check=False)
        label = f"job_id={job_id} job_name={job_name} target={target_name}"
        if proc.returncode == 0:
            cancelled.append(label)
        else:
            detail = (proc.stderr or proc.stdout or "unknown_error").strip().replace("\n", " ")
            failed.append(f"{label} reason={detail}")
    return CancelResult(len(jobs), cancelled, failed)


def get_log(
    project_root: Path,
    *,
    target: str | None = None,
    analysis: str | None = None,
    job_id: str | None = None,
    job_name: str | None = None,
    head: int | None = None,
    tail: int | None = None,
    follow: bool = False,
    run: Callable[..., subprocess.CompletedProcess[str]] = subprocess.run,
) -> LogResult:
    record, match_count = select_job(
        project_root, target=target, analysis=analysis, job_id=job_id, job_name=job_name
    )
    cfg = load_config(project_root)
    target_name = str(record.get("target", ""))
    log_path = str(record.get("remote_log_file", "")).strip()
    if not log_path:
        raise JobError("Selected job does not have a remote log file")
    local = str(record.get("submission_mode", "")).lower() == "local"
    if target_name in cfg.targets:
        target_cfg = cfg.targets[target_name]
        local = local or is_local_transport(target_cfg.transport, target_cfg.host)
    elif not local:
        raise JobError(f"Target '{target_name}' from selected job is not configured")

    count = tail if tail is not None else 50
    if local:
        path = Path(log_path).expanduser()
        if not path.is_absolute():
            path = (project_root / path).resolve()
        if not path.is_file():
            raise JobError(f"Local log file not found: {path}")
        if follow:
            return LogResult(record, str(path), None, ["tail", "-n", str(count), "-f", str(path)], True, match_count)
        lines = path.read_text(errors="replace").splitlines()
        content = "\n".join(lines[:head] if head is not None else lines[-count:])
        return LogResult(record, str(path), content, None, True, match_count)

    target_cfg = cfg.targets[target_name]
    quoted = shlex.quote(log_path)
    shell = (
        f"if [ -f {quoted} ]; then head -n {head} {quoted}; fi"
        if head is not None
        else f"tail -n {count} -f {quoted}"
        if follow
        else f"if [ -f {quoted} ]; then tail -n {count} {quoted}; fi"
    )
    argv = ssh_command(project_root, cfg, target_name, target_cfg, shell, tty=follow)
    if follow:
        return LogResult(record, log_path, None, argv, False, match_count)
    proc = run(argv, text=True, capture_output=True, check=False)
    if proc.returncode != 0:
        raise JobError(proc.stderr.strip() or "Failed to fetch remote log")
    return LogResult(record, log_path, proc.stdout.rstrip(), None, False, match_count)


def poll_job(
    project_root: Path,
    record: dict[str, Any],
    *,
    tail_lines: int = 0,
    run: Callable[..., subprocess.CompletedProcess[str]] = subprocess.run,
) -> WatchUpdate:
    cfg = load_config(project_root)
    state = resolve_job_state(project_root, cfg, record)
    log_tail = ""
    if tail_lines:
        try:
            log_tail = get_log(
                project_root, job_id=str(record.get("job_id", "")), target=str(record.get("target", "")),
                tail=tail_lines, run=run
            ).content or ""
        except JobError:
            pass
    return WatchUpdate(record, state, log_tail, datetime.now().isoformat(timespec="seconds"))


def watch_job(
    project_root: Path,
    record: dict[str, Any],
    *,
    max_polls: int = 0,
    tail_lines: int = 0,
    run: Callable[..., subprocess.CompletedProcess[str]] = subprocess.run,
) -> Iterator[WatchUpdate]:
    polls = 0
    while True:
        polls += 1
        update = poll_job(project_root, record, tail_lines=tail_lines, run=run)
        yield update
        if not is_running_state(update.state) or (max_polls and polls >= max_polls):
            return


def prepare_retry(
    project_root: Path,
    *,
    target: str | None = None,
    analysis: str | None = None,
    job_id: str | None = None,
    job_name: str | None = None,
) -> RetrySpec:
    project_root = project_root.resolve()
    records = [
        record for record in filter_jobs(project_root, target=target, analysis=analysis, job_id=job_id, job_name=job_name)
        if "sweep_id" not in record
    ]
    if not records:
        raise JobError("No matching normal run records found for retry")
    selected = records[0]
    analysis_rel = str(selected.get("analysis", "")).strip("/")
    analysis_dir = (project_root / analysis_rel).resolve()
    if not analysis_rel or not analysis_dir.is_dir():
        raise JobError(f"Analysis directory not found for selected record: {analysis_dir}")
    try:
        analysis_dir.relative_to(project_root)
    except ValueError as exc:
        raise JobError("Selected analysis path is outside project root") from exc
    cfg = load_config(project_root)
    target_name = str(selected.get("target", "")).strip()
    if target_name not in cfg.targets:
        raise JobError(f"Target '{target_name}' from selected record is not configured")
    command = str(selected.get("command_resolved", "")).strip() or str(selected.get("command", "")).strip()
    if not command:
        raise JobError("Selected record is missing command")
    try:
        context = resolve_context(start=project_root, target_name=target_name)
    except ContextError as exc:
        raise JobError(str(exc)) from exc
    if context.analysis.as_posix() != analysis_rel:
        raise JobError(
            f"Selected job belongs to analysis '{analysis_rel}', but active analysis is "
            f"'{context.analysis.as_posix()}'. Switch with `cdp analysis use {analysis_rel}`."
        )
    resources = selected.get("resources") if isinstance(selected.get("resources"), dict) else {}
    target_cfg = cfg.targets[target_name]
    value = lambda key, default: selected.get(key, resources.get(key, default))
    request = RunRequest(
        command=command,
        job_name=str(selected.get("job_name", "")).strip() or "cdp-retry",
        resources=ResourceSpec(
            profile=str(resources.get("profile", "")).strip() or None,
            cpus=int(value("cpus", target_cfg.default_cpus)),
            memory=str(value("memory", target_cfg.default_memory)),
            time=str(value("time", target_cfg.default_time)),
            node=str(value("node", target_cfg.default_node or "1")),
            queue=str(value("queue", target_cfg.default_queue)),
            parallel_environment=str(value("parallel_environment", target_cfg.default_parallel_environment)),
        ),
    )
    return RetrySpec(context, request, len(records))


def collect_outputs(
    project_root: Path,
    *,
    target: str | None = None,
    analysis: str | None = None,
    job_id: str | None = None,
    job_name: str | None = None,
    run: Callable[..., subprocess.CompletedProcess[str]] = subprocess.run,
) -> CollectResult:
    record, match_count = select_job(
        project_root, target=target, analysis=analysis, job_id=job_id, job_name=job_name
    )
    cfg = load_config(project_root)
    target_name = str(record.get("target", ""))
    if target_name not in cfg.targets:
        raise JobError(f"Target '{target_name}' from selected record is not configured")
    analysis_rel = str(record.get("analysis", "")).strip("/")
    remote_run_dir = str(record.get("remote_run_dir", "")).strip()
    tags = record.get("analysis_tags")
    if not analysis_rel or not remote_run_dir or not isinstance(tags, list) or not tags:
        raise JobError("Selected record has no analysis path, remote_run_dir, or tagged paths to collect")
    analysis_dir = (project_root / analysis_rel).resolve()
    analysis_dir.mkdir(parents=True, exist_ok=True)
    files = total_bytes = 0
    missing: list[str] = []
    clean_tags = [str(tag).strip("/") for tag in tags if str(tag).strip("/")]
    for tag in clean_tags:
        manifest = load_index_manifest(project_root, target_name, analysis_rel, tag)
        if manifest is None:
            root = load_index_manifest(project_root, target_name, analysis_rel, ".")
            if root is None or not index_entry_path_exists(root, tag):
                missing.append(tag)
                continue
            prefix = f"{tag}/"
            entries = [
                {**entry, "path": str(entry.get("path", "")).strip("/").removeprefix(prefix) or "."}
                for entry in root.get("entries", []) if isinstance(entry, dict)
                and (str(entry.get("path", "")).strip("/") == tag or str(entry.get("path", "")).strip("/").startswith(prefix))
            ]
            manifest = {"entries": entries}
        count, size = index_file_count_and_bytes(manifest)
        files += count
        total_bytes += size
    target_cfg = cfg.targets[target_name]
    pulled: list[str] = []
    skipped: list[str] = []
    for tag in clean_tags:
        source = f"{remote_run_dir}/./{tag}"
        base = ["rsync", "-az", "--relative", source, f"{analysis_dir}/"]
        argv = base if is_local_transport(target_cfg.transport, target_cfg.host) else rsync_command(
            [*base[:3], remote_spec(target_name, source), base[-1]], project_root, cfg, target_name, target_cfg
        )
        proc = run(argv, text=True, capture_output=True, check=False)
        (pulled if proc.returncode == 0 else skipped).append(tag)
    return CollectResult(record, analysis_dir, files, total_bytes, sorted(set(missing)), pulled, skipped, match_count)
