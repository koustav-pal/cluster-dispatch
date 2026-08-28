from __future__ import annotations

import json
import re
import socket
import sys
from datetime import datetime
from importlib.metadata import PackageNotFoundError, version
from pathlib import Path
from typing import Any

from cluster_dispatch.config import CONFIG_DIR, JOBS_DIR, ensure_state_dirs, load_state
from cluster_dispatch.models import JobRecord, SyncRecord

SYNC_DIR = "sync"
SWEEPS_DIR = "sweeps"


def _package_version() -> str | None:
    try:
        return version("cluster-dispatch")
    except PackageNotFoundError:
        return None


def build_job_record(
    *,
    project_root: Path,
    submitted_at: str,
    analysis: str | None,
    analysis_tags: list[str],
    target_name: str,
    scheduler: str,
    job_id: str,
    run_id: str,
    job_name: str,
    state: str,
    command: str,
    remote_run_dir: str,
    remote_log_file: str,
    working_dir: str,
    cpus: int | None,
    memory: str | None,
    walltime: str | None,
    node: str | None,
    queue: str | None,
    parallel_environment: str | None,
    profile_name: str | None,
    submit_script_path: str | None,
    submission_mode: str,
    sync_source: str | None,
    sync_destination: str | None,
    ignore_file_path: str | None,
    ignore_used: bool,
    git_info: dict[str, Any] | None = None,
    record_fields: dict[str, Any] | None = None,
    extra: dict[str, Any] | None = None,
) -> dict[str, Any]:
    record: dict[str, Any] = {
        "submitted_at": submitted_at,
        "analysis": analysis,
        "analysis_tags": analysis_tags,
        "target": target_name,
        "scheduler": scheduler,
        "job_id": job_id,
        "run_id": run_id,
        "job_name": job_name,
        "state": state,
        "stdout": job_name,
        "stderr": job_name,
        "working_dir": working_dir,
        "cpus": cpus,
        "memory": memory,
        "time": walltime,
        "queue": queue,
        "parallel_environment": parallel_environment,
        "node": node,
        "command": command,
        "command_resolved": command,
        "remote_run_dir": remote_run_dir,
        "remote_log_file": remote_log_file,
        "submission_mode": submission_mode,
        "record_version": 2,
        "resources": {
            "profile": profile_name,
            "cpus": cpus,
            "memory": memory,
            "time": walltime,
            "node": node,
            "queue": queue,
            "parallel_environment": parallel_environment,
        },
        "paths": {
            "project_root": str(project_root),
            "analysis_local_path": sync_source,
            "analysis_remote_path": sync_destination,
            "remote_run_dir": remote_run_dir,
            "remote_log_file": remote_log_file,
            "submit_script": submit_script_path,
            "working_dir": working_dir,
        },
        "sync": {
            "ignore_file": ignore_file_path,
            "ignore_detected": bool(ignore_file_path),
            "ignore_used": ignore_used,
            "source": sync_source,
            "destination": sync_destination,
        },
        "environment": {
            "cluster_dispatch_version": _package_version(),
            "python_version": sys.version.split()[0],
            "hostname": socket.gethostname(),
        },
    }
    if git_info is not None:
        record["git"] = {
            "repo_root": git_info.get("repo_root"),
            "branch": git_info.get("branch"),
            "commit": git_info.get("head"),
            "dirty": git_info.get("dirty"),
        }
    if record_fields:
        record.update(record_fields)
    if extra:
        record.update(extra)
    return record


def _safe_stem(parts: list[str], fallback: str) -> str:
    clean = [re.sub(r"[^A-Za-z0-9_.-]+", "_", part).strip("._-") for part in parts if part]
    return "__".join(part for part in clean if part) or fallback


def _unique_json_path(directory: Path, stem: str) -> Path:
    path = directory / f"{stem}.json"
    counter = 1
    while path.exists():
        path = directory / f"{stem}__{counter}.json"
        counter += 1
    return path


def save_job(project_root: Path, record: JobRecord | dict[str, Any]) -> Path:
    payload = record.to_dict() if isinstance(record, JobRecord) else record
    jobs_dir = ensure_state_dirs(project_root) / JOBS_DIR
    stem = _safe_stem(
        [
            str(payload.get("submitted_at", "unknown")).replace(":", "-"),
            str(payload.get("job_id", "")).strip(),
            str(payload.get("job_name", "")).strip(),
            str(payload.get("target", "")).strip(),
        ],
        "job",
    )
    path = _unique_json_path(jobs_dir, stem)
    path.write_text(json.dumps(payload, indent=2))
    return path


def list_jobs(project_root: Path) -> list[dict[str, Any]]:
    jobs_dir = project_root / CONFIG_DIR / JOBS_DIR
    if not jobs_dir.exists():
        return []
    records: list[dict[str, Any]] = []
    for path in sorted(jobs_dir.glob("*.json"), reverse=True):
        try:
            payload = json.loads(path.read_text())
        except json.JSONDecodeError:
            continue
        payload["_record_file"] = str(path)
        records.append(payload)
    return records


def load_job(project_root: Path, job_id: str) -> dict[str, Any]:
    for record in list_jobs(project_root):
        if str(record.get("job_id", "")) == job_id:
            return record
    raise LookupError(f"Job '{job_id}' not found")


def latest_job(project_root: Path) -> dict[str, Any]:
    state = load_state(project_root)
    pointer = state.get("last_job")
    if isinstance(pointer, dict):
        pointer_id = str(pointer.get("job_id", ""))
        pointer_target = str(pointer.get("target", ""))
        for record in list_jobs(project_root):
            if str(record.get("job_id", "")) == pointer_id and str(record.get("target", "")) == pointer_target:
                return record
        return dict(pointer)
    records = list_jobs(project_root)
    if records:
        return records[0]
    raise LookupError("No job records found")


def resolve_job(project_root: Path, job_id: str | None = None) -> dict[str, Any]:
    return load_job(project_root, job_id) if job_id else latest_job(project_root)


def update_job(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(json.dumps(payload, indent=2))


def save_sync(project_root: Path, record: SyncRecord | dict[str, Any]) -> Path:
    payload = record.to_dict() if isinstance(record, SyncRecord) else dict(record)
    now = datetime.now().isoformat(timespec="seconds")
    payload["recorded_at"] = now
    sync_dir = project_root / CONFIG_DIR / SYNC_DIR
    sync_dir.mkdir(parents=True, exist_ok=True)
    stem = _safe_stem(
        [now.replace(":", "-"), str(payload.get("action", "sync")), str(payload.get("target", "unknown"))],
        "sync",
    )
    path = _unique_json_path(sync_dir, stem)
    path.write_text(json.dumps(payload, indent=2))
    return path


def list_syncs(project_root: Path) -> list[dict[str, Any]]:
    sync_dir = project_root / CONFIG_DIR / SYNC_DIR
    if not sync_dir.exists():
        return []
    records: list[dict[str, Any]] = []
    for path in sorted(sync_dir.glob("*.json"), reverse=True):
        try:
            payload = json.loads(path.read_text())
        except json.JSONDecodeError:
            continue
        payload["_record_file"] = str(path)
        records.append(payload)
    return records


def save_sweep(project_root: Path, sweep_id: str, record: dict[str, Any]) -> Path:
    sweep_dir = project_root / CONFIG_DIR / SWEEPS_DIR
    sweep_dir.mkdir(parents=True, exist_ok=True)
    path = sweep_dir / f"{sweep_id}.json"
    path.write_text(json.dumps(record, indent=2))
    return path


def load_sweep(project_root: Path, sweep_id: str) -> dict[str, Any]:
    path = project_root / CONFIG_DIR / SWEEPS_DIR / f"{sweep_id}.json"
    if not path.exists():
        raise LookupError(f"Sweep '{sweep_id}' not found")
    return json.loads(path.read_text())


def list_sweeps(project_root: Path) -> list[dict[str, Any]]:
    sweep_dir = project_root / CONFIG_DIR / SWEEPS_DIR
    if not sweep_dir.exists():
        return []
    manifests: list[dict[str, Any]] = []
    for path in sorted(sweep_dir.glob("*.json"), reverse=True):
        try:
            payload = json.loads(path.read_text())
        except json.JSONDecodeError:
            continue
        payload["_manifest_file"] = str(path)
        manifests.append(payload)
    return manifests


# Compatibility names for callers migrating from pre-v1 internals.
write_job_record = save_job
load_job_records = list_jobs
update_job_record = update_job
write_sync_record = save_sync
load_sync_records = list_syncs
