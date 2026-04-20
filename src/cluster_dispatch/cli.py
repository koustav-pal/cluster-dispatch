from __future__ import annotations

import shlex
import secrets
import subprocess
import os
import json
import re
import tarfile
import io
import sys
import socket
import itertools
import hashlib
import base64
import shutil
import time
from importlib.metadata import PackageNotFoundError, version
from string import Formatter
from datetime import datetime, timedelta
from pathlib import Path
from typing import Annotated, Optional, Any

import typer
import yaml

from cluster_dispatch.config import (
    CONFIG_DIR,
    CONFIG_NAME,
    JOBS_DIR,
    LastJob,
    ProjectConfig,
    TargetConfig,
    append_job_record,
    config_path,
    ensure_state_dirs,
    find_project_root,
    load_config,
    load_state,
    save_config,
    save_state,
)
from cluster_dispatch.schedulers import get_adapter

app = typer.Typer(help="Cluster Dispatch CLI for remote SSH compute targets")
target_app = typer.Typer(help="Manage compute targets")
analysis_app = typer.Typer(help="Manage active analysis directory")
sweep_app = typer.Typer(help="Manage sweep orchestration")
run_ops_app = typer.Typer(help="Run-related utilities")
sweep_ops_app = typer.Typer(help="Sweep-related utilities")
profile_app = typer.Typer(help="Manage resource profiles")
status_app = typer.Typer(help="Show job status", invoke_without_command=True)
ignore_app = typer.Typer(help="Manage .cdpignore patterns")
sync_app = typer.Typer(help="Explicit synchronization commands")
config_app = typer.Typer(help="Inspect project configuration")
cleanup_app = typer.Typer(help="Clean up local metadata and manifests")
app.add_typer(target_app, name="target")
app.add_typer(analysis_app, name="analysis")
analysis_app.add_typer(sweep_app, name="sweep")
app.add_typer(run_ops_app, name="run")
app.add_typer(sweep_ops_app, name="sweep")
app.add_typer(profile_app, name="profile")
app.add_typer(status_app, name="status")
app.add_typer(ignore_app, name="ignore")
app.add_typer(sync_app, name="sync")
app.add_typer(config_app, name="config")
app.add_typer(cleanup_app, name="cleanup")

VERBOSITY_LEVEL = 0
REMOTE_VERBOSE = False
QUIET_MODE = False


TEMPLATES_DIR_NAME = "templates"
TEMPLATE_FILE_NAME = "scheduler_header.tmpl"
SWEEPS_DIR_NAME = "sweeps"
SYNC_EVENTS_DIR_NAME = "sync"
INDEX_DIR_NAME = "index"
RUNS_DIR_NAME = "runs"
BASE_REQUIRED_TEMPLATE_VARS = ("cpus", "memory", "time", "job_name", "stdout", "stderr", "working_dir")
DEFAULT_RESOURCE_PROFILES: dict[str, dict[str, str | int]] = {
    "small": {
        "cpus": 1,
        "memory": "8G",
        "time": "01:00:00",
        "node": "1",
        "queue": "",
        "parallel_environment": "",
    },
    "long": {
        "cpus": 2,
        "memory": "16G",
        "time": "24:00:00",
        "node": "1",
        "queue": "",
        "parallel_environment": "",
    },
    "highmem": {
        "cpus": 4,
        "memory": "128G",
        "time": "08:00:00",
        "node": "1",
        "queue": "",
        "parallel_environment": "",
    },
}

IGNORE_FILE_NAME = ".cdpignore"
GITIGNORE_FILE_NAME = ".gitignore"
DEFAULT_GITIGNORE_CONTENT = (
    "# Git version control exclusions\n"
    "# .gitignore controls what Git tracks.\n"
    "# .cdpignore controls what cdp excludes from remote sync.\n"
    "__pycache__/\n"
    "*.py[cod]\n"
    "*.egg-info/\n"
    "build/\n"
    "dist/\n"
    ".pytest_cache/\n"
    ".mypy_cache/\n"
    ".DS_Store\n"
    ".venv/\n"
    "\n"
    "# data/\n"
    "# results/\n"
    "# cache/\n"
    "# scratch/\n"
)


@app.callback()
def _global_options(
    verbose: int = typer.Option(0, "-v", "--verbose", count=True, help="Increase verbosity (repeat for more detail)"),
    remote_verbose: bool = typer.Option(False, "--remote-verbose", help="Always print remote SSH/rsync commands and output"),
    quiet: bool = typer.Option(False, "--quiet", help="Suppress non-essential output"),
) -> None:
    """Global output controls."""
    global VERBOSITY_LEVEL, REMOTE_VERBOSE, QUIET_MODE
    VERBOSITY_LEVEL = int(max(0, verbose))
    REMOTE_VERBOSE = bool(remote_verbose)
    QUIET_MODE = bool(quiet)
    os.environ["CDP_VERBOSE_LEVEL"] = str(VERBOSITY_LEVEL)
    os.environ["CDP_REMOTE_VERBOSE"] = "1" if REMOTE_VERBOSE else "0"
    os.environ["CDP_QUIET"] = "1" if QUIET_MODE else "0"


def _project_root() -> Path:
    return find_project_root()


def _ignore_file(project_root: Path) -> Path:
    return project_root / IGNORE_FILE_NAME


def get_git_info(project_root: Path) -> Optional[dict[str, Any]]:
    repo_root_proc = _run_direct(
        ["git", "-C", str(project_root), "rev-parse", "--show-toplevel"],
        text=True,
        capture_output=True,
        check=False,
    )
    if repo_root_proc.returncode != 0:
        return None

    repo_root = repo_root_proc.stdout.strip()
    branch_proc = _run_direct(
        ["git", "-C", str(project_root), "rev-parse", "--abbrev-ref", "HEAD"],
        text=True,
        capture_output=True,
        check=False,
    )
    head_proc = _run_direct(
        ["git", "-C", str(project_root), "rev-parse", "HEAD"],
        text=True,
        capture_output=True,
        check=False,
    )
    dirty_proc = _run_direct(
        ["git", "-C", str(project_root), "status", "--porcelain"],
        text=True,
        capture_output=True,
        check=False,
    )

    return {
        "repo_root": repo_root,
        "branch": branch_proc.stdout.strip() if branch_proc.returncode == 0 else "",
        "head": head_proc.stdout.strip() if head_proc.returncode == 0 else "",
        "dirty": bool(dirty_proc.stdout.strip()) if dirty_proc.returncode == 0 else False,
    }


def _cluster_dispatch_version() -> Optional[str]:
    try:
        return version("cluster-dispatch")
    except PackageNotFoundError:
        return None


def _build_job_record(
    *,
    project_root: Path,
    submitted_at: str,
    analysis: Optional[str],
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
    cpus: Optional[int],
    memory: Optional[str],
    walltime: Optional[str],
    node: Optional[str],
    queue: Optional[str],
    parallel_environment: Optional[str],
    profile_name: Optional[str],
    submit_script_path: Optional[str],
    submission_mode: str,
    sync_source: Optional[str],
    sync_destination: Optional[str],
    ignore_file_path: Optional[str],
    ignore_used: bool,
    sweep_fields: Optional[dict[str, Any]] = None,
    extra: Optional[dict[str, Any]] = None,
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
            "cluster_dispatch_version": _cluster_dispatch_version(),
            "python_version": sys.version.split()[0],
            "hostname": socket.gethostname(),
        },
    }
    git_info = get_git_info(project_root)
    if git_info is not None:
        record["git"] = {
            "repo_root": git_info.get("repo_root"),
            "branch": git_info.get("branch"),
            "commit": git_info.get("head"),
            "dirty": git_info.get("dirty"),
        }
    if sweep_fields:
        record.update(sweep_fields)
    if extra:
        record.update(extra)
    return record


def _require_active_analysis(cfg: ProjectConfig, project_root: Path) -> Path:
    if not cfg.active_analysis:
        raise typer.BadParameter("No active analysis set. Run: cdp analysis use <path>")
    path = (project_root / cfg.active_analysis).resolve()
    if not path.exists() or not path.is_dir():
        raise typer.BadParameter(f"Active analysis path missing: {path}")
    return path


def _active_target(cfg: ProjectConfig) -> tuple[str, TargetConfig]:
    name = cfg.default_target
    if name not in cfg.targets:
        raise typer.BadParameter(
            f"Default target '{name}' not configured. Add with: cdp target add {name} ..."
        )
    return name, cfg.targets[name]


def _is_remote_command(cmd: Any) -> bool:
    if isinstance(cmd, list) and cmd:
        first = str(cmd[0]).strip().lower()
        return first in {"ssh", "rsync", "scp", "sftp"}
    return False


def _render_command(cmd: Any) -> str:
    if isinstance(cmd, list):
        return " ".join(shlex.quote(str(part)) for part in cmd)
    return str(cmd)


_SUBPROCESS_RUN = subprocess.run


def _run_direct(*args: Any, **kwargs: Any) -> subprocess.CompletedProcess[str]:
    cmd = args[0] if args else kwargs.get("args")
    is_remote = _is_remote_command(cmd)
    log_remote = is_remote and (REMOTE_VERBOSE or VERBOSITY_LEVEL >= 1) and not QUIET_MODE
    if log_remote:
        typer.echo(f"[remote] $ {_render_command(cmd)}", err=True)
    # Prevent non-interactive remote commands from hanging while waiting on stdin.
    if is_remote and "input" not in kwargs and kwargs.get("stdin") is None:
        kwargs["stdin"] = subprocess.DEVNULL
    proc = _SUBPROCESS_RUN(*args, **kwargs)
    if is_remote and not QUIET_MODE and (REMOTE_VERBOSE or VERBOSITY_LEVEL >= 2):
        stdout = getattr(proc, "stdout", None)
        stderr = getattr(proc, "stderr", None)
        if isinstance(stdout, str) and stdout.strip():
            typer.echo(f"[remote][stdout]\n{stdout.rstrip()}", err=True)
        if isinstance(stderr, str) and stderr.strip():
            typer.echo(f"[remote][stderr]\n{stderr.rstrip()}", err=True)
    return proc


def _run_cmd(cmd: list[str], check: bool = True) -> subprocess.CompletedProcess[str]:
    return _run_direct(cmd, text=True, capture_output=True, check=check)


def _require_yes(yes: bool, message: str) -> None:
    if not yes:
        raise typer.BadParameter(message)


def _write_remote_script(host: str, remote_path: str, content: str, executable: bool = True) -> None:
    command = f"cat > {shlex.quote(remote_path)}"
    if executable:
        command += f" && chmod +x {shlex.quote(remote_path)}"
    proc = _run_direct(
        ["ssh", host, command],
        input=content,
        text=True,
        capture_output=True,
        check=False,
    )
    if proc.returncode != 0:
        detail = (proc.stderr or proc.stdout or "unknown remote write error").strip()
        raise typer.BadParameter(f"Failed to write remote script {remote_path}: {detail}")


def _is_local_host(host: str) -> bool:
    normalized = host.strip().lower()
    return normalized in {"", "localhost", "127.0.0.1", "::1"}


def _uses_local_transport(target: TargetConfig) -> bool:
    return target.transport.strip().lower() == "local"


def _validate_transport(value: str) -> str:
    transport = value.strip().lower()
    if transport not in {"ssh", "local"}:
        raise typer.BadParameter("transport must be one of: ssh, local")
    return transport


def _slug_component(value: str) -> str:
    cleaned = re.sub(r"[^A-Za-z0-9_.-]+", "-", value.strip())
    cleaned = cleaned.strip("-._")
    return cleaned or "job"


def _extract_placeholders(template: str) -> set[str]:
    names: set[str] = set()
    for _, field_name, _, _ in Formatter().parse(template):
        if field_name:
            names.add(field_name)
    return names


def _all_resource_profiles(cfg: ProjectConfig) -> dict[str, dict[str, str | int]]:
    merged: dict[str, dict[str, str | int]] = {
        name: dict(values) for name, values in DEFAULT_RESOURCE_PROFILES.items()
    }
    for name, values in cfg.resource_profiles.items():
        merged[name.strip().lower()] = dict(values)
    return merged


def _resolve_resource_profile(profile: Optional[str], cfg: ProjectConfig) -> dict[str, str | int]:
    if not profile:
        return {}
    key = profile.strip().lower()
    profiles = _all_resource_profiles(cfg)
    if key not in profiles:
        raise typer.BadParameter(
            f"Unknown profile '{profile}'. Available profiles: {', '.join(sorted(profiles.keys()))}"
        )
    return dict(profiles[key])


def _validate_profile_name(name: str) -> str:
    normalized = name.strip().lower()
    if not normalized:
        raise typer.BadParameter("Profile name cannot be empty")
    if not re.fullmatch(r"[a-z0-9_.-]+", normalized):
        raise typer.BadParameter("Profile name may contain only: a-z, 0-9, _, ., -")
    return normalized


def _safe_project_root() -> Optional[Path]:
    try:
        return _project_root()
    except Exception:
        return None


def _prefix_filter(values: list[str], incomplete: str) -> list[str]:
    needle = incomplete or ""
    return [value for value in values if value.startswith(needle)]


def _complete_target_names(incomplete: str) -> list[str]:
    project_root = _safe_project_root()
    if not project_root:
        return []
    try:
        cfg = load_config(project_root)
    except Exception:
        return []
    return _prefix_filter(sorted(cfg.targets.keys()), incomplete)


def _complete_profile_names(incomplete: str) -> list[str]:
    project_root = _safe_project_root()
    if not project_root:
        return _prefix_filter(sorted(DEFAULT_RESOURCE_PROFILES.keys()), incomplete)
    try:
        cfg = load_config(project_root)
        names = sorted(_all_resource_profiles(cfg).keys())
        return _prefix_filter(names, incomplete)
    except Exception:
        return _prefix_filter(sorted(DEFAULT_RESOURCE_PROFILES.keys()), incomplete)


def _complete_user_profile_names(incomplete: str) -> list[str]:
    project_root = _safe_project_root()
    if not project_root:
        return []
    try:
        cfg = load_config(project_root)
    except Exception:
        return []
    return _prefix_filter(sorted(cfg.resource_profiles.keys()), incomplete)


def _complete_analysis_dirs(incomplete: str) -> list[str]:
    project_root = _safe_project_root()
    if not project_root:
        return []

    values: list[str] = []
    for path in project_root.rglob("*"):
        if not path.is_dir():
            continue
        if path.name in {".git", "__pycache__"}:
            continue
        rel = path.relative_to(project_root).as_posix()
        if rel.startswith(f"{CONFIG_DIR}/"):
            continue
        values.append(rel)
    return _prefix_filter(sorted(set(values)), incomplete)


def _complete_active_analysis_paths(incomplete: str) -> list[str]:
    project_root = _safe_project_root()
    if not project_root:
        return []
    try:
        cfg = load_config(project_root)
        analysis_dir = _require_active_analysis(cfg, project_root)
    except Exception:
        return []

    values: list[str] = []
    for path in analysis_dir.rglob("*"):
        rel = path.relative_to(analysis_dir).as_posix()
        if rel:
            values.append(rel)
    return _prefix_filter(sorted(set(values)), incomplete)


def _complete_sweep_ids(incomplete: str) -> list[str]:
    project_root = _safe_project_root()
    if not project_root:
        return []
    sweeps_dir = project_root / CONFIG_DIR / SWEEPS_DIR_NAME
    if not sweeps_dir.exists():
        return []

    active_analysis = ""
    try:
        cfg = load_config(project_root)
        active_analysis = (cfg.active_analysis or "").strip("/")
    except Exception:
        active_analysis = ""

    values: list[str] = []
    for path in sorted(sweeps_dir.glob("*.json")):
        sweep_id = path.stem
        if active_analysis:
            try:
                payload = json.loads(path.read_text())
            except json.JSONDecodeError:
                continue
            if str(payload.get("analysis", "")).strip("/") != active_analysis:
                continue
        values.append(sweep_id)
    return _prefix_filter(values, incomplete)


def _complete_job_ids(incomplete: str) -> list[str]:
    project_root = _safe_project_root()
    if not project_root:
        return []
    try:
        records = _load_job_records(project_root)
    except Exception:
        return []
    seen: set[str] = set()
    values: list[str] = []
    for rec in records:
        job_id = str(rec.get("job_id", "")).strip()
        if job_id and job_id not in seen:
            seen.add(job_id)
            values.append(job_id)
    return _prefix_filter(values, incomplete)


def _complete_job_names(incomplete: str) -> list[str]:
    project_root = _safe_project_root()
    if not project_root:
        return []
    try:
        records = _load_job_records(project_root)
    except Exception:
        return []
    seen: set[str] = set()
    values: list[str] = []
    for rec in records:
        job_name = str(rec.get("job_name", "")).strip()
        if job_name and job_name not in seen:
            seen.add(job_name)
            values.append(job_name)
    return _prefix_filter(values, incomplete)


def _complete_record_analyses(incomplete: str) -> list[str]:
    project_root = _safe_project_root()
    if not project_root:
        return []
    try:
        records = _load_job_records(project_root)
    except Exception:
        return []
    seen: set[str] = set()
    values: list[str] = []
    for rec in records:
        analysis = str(rec.get("analysis", "")).strip()
        if analysis and analysis not in seen:
            seen.add(analysis)
            values.append(analysis)
    return _prefix_filter(values, incomplete)


def _sweeps_dir(project_root: Path) -> Path:
    path = project_root / CONFIG_DIR / SWEEPS_DIR_NAME
    path.mkdir(parents=True, exist_ok=True)
    return path


def _sync_events_dir(project_root: Path) -> Path:
    path = project_root / CONFIG_DIR / SYNC_EVENTS_DIR_NAME
    path.mkdir(parents=True, exist_ok=True)
    return path


def _index_dir(project_root: Path) -> Path:
    path = project_root / CONFIG_DIR / INDEX_DIR_NAME
    path.mkdir(parents=True, exist_ok=True)
    return path


def _index_manifest_path(project_root: Path, target_name: str, analysis_rel: str, scope_rel: str) -> Path:
    safe_target = re.sub(r"[^A-Za-z0-9_.-]+", "_", target_name).strip("._-") or "target"
    safe_analysis = [re.sub(r"[^A-Za-z0-9_.-]+", "_", part).strip("._-") for part in analysis_rel.split("/") if part]
    analysis_dir = _index_dir(project_root) / safe_target
    for part in safe_analysis:
        analysis_dir = analysis_dir / part
    analysis_dir.mkdir(parents=True, exist_ok=True)
    clean_scope = scope_rel.strip("/")
    if clean_scope in {"", "."}:
        scope_stem = "root"
    else:
        scope_stem = re.sub(r"[^A-Za-z0-9_.-]+", "_", clean_scope.replace("/", "__")).strip("._-") or "scope"
    return analysis_dir / f"{scope_stem}.json"


def _index_manifest_candidate(project_root: Path, target_name: str, analysis_rel: str, scope_rel: str) -> Path:
    safe_target = re.sub(r"[^A-Za-z0-9_.-]+", "_", target_name).strip("._-") or "target"
    safe_analysis = [re.sub(r"[^A-Za-z0-9_.-]+", "_", part).strip("._-") for part in analysis_rel.split("/") if part]
    analysis_dir = project_root / CONFIG_DIR / INDEX_DIR_NAME / safe_target
    for part in safe_analysis:
        analysis_dir = analysis_dir / part
    clean_scope = scope_rel.strip("/")
    if clean_scope in {"", "."}:
        scope_stem = "root"
    else:
        scope_stem = re.sub(r"[^A-Za-z0-9_.-]+", "_", clean_scope.replace("/", "__")).strip("._-") or "scope"
    return analysis_dir / f"{scope_stem}.json"


def _load_index_manifest(
    project_root: Path,
    target_name: str,
    analysis_rel: str,
    scope_rel: str,
) -> Optional[dict[str, Any]]:
    manifest_path = _index_manifest_candidate(project_root, target_name, analysis_rel, scope_rel)
    if not manifest_path.exists():
        return None
    try:
        payload = json.loads(manifest_path.read_text())
    except json.JSONDecodeError:
        return None
    if str(payload.get("target", "")) != target_name:
        return None
    if str(payload.get("analysis", "")).strip("/") != analysis_rel.strip("/"):
        return None
    payload["_manifest_path"] = str(manifest_path)
    return payload


def _list_names_from_index_manifest(manifest: dict[str, Any], include_files: bool) -> list[str]:
    entries = manifest.get("entries", [])
    if not isinstance(entries, list):
        return []
    names: set[str] = set()
    for entry in entries:
        if not isinstance(entry, dict):
            continue
        rel_path = str(entry.get("path", "")).strip("/")
        if rel_path in {"", "."}:
            continue
        top_name = rel_path.split("/", 1)[0]
        if not include_files:
            if "/" in rel_path:
                names.add(top_name)
                continue
            if str(entry.get("type", "")).lower() == "dir":
                names.add(top_name)
            continue
        names.add(top_name)
    return sorted(names)


def _index_entry_path_exists(manifest: dict[str, Any], rel_path: str) -> bool:
    clean = rel_path.strip("/")
    if clean in {"", "."}:
        return True
    entries = manifest.get("entries", [])
    if not isinstance(entries, list):
        return False
    for entry in entries:
        if not isinstance(entry, dict):
            continue
        epath = str(entry.get("path", "")).strip("/")
        if not epath or epath == ".":
            continue
        if epath == clean or epath.startswith(f"{clean}/"):
            return True
    return False


def _index_file_count_and_bytes(manifest: dict[str, Any]) -> tuple[int, int]:
    entries = manifest.get("entries", [])
    if not isinstance(entries, list):
        return 0, 0
    file_count = 0
    total_bytes = 0
    for entry in entries:
        if not isinstance(entry, dict):
            continue
        if str(entry.get("type", "")).lower() != "file":
            continue
        file_count += 1
        try:
            total_bytes += int(entry.get("size", 0))
        except Exception:
            pass
    return file_count, total_bytes


def _humanize_bytes(num_bytes: int) -> str:
    value = float(max(0, num_bytes))
    units = ["B", "KB", "MB", "GB", "TB"]
    for unit in units:
        if value < 1024.0 or unit == units[-1]:
            if unit == "B":
                return f"{int(value)} {unit}"
            return f"{value:.1f} {unit}"
        value /= 1024.0
    return f"{int(num_bytes)} B"


def _index_manifest_paths(project_root: Path) -> list[Path]:
    root = project_root / CONFIG_DIR / INDEX_DIR_NAME
    if not root.exists():
        return []
    return sorted(root.rglob("*.json"))


def _load_all_index_manifests(project_root: Path) -> list[dict[str, Any]]:
    manifests: list[dict[str, Any]] = []
    for path in _index_manifest_paths(project_root):
        try:
            payload = json.loads(path.read_text())
        except json.JSONDecodeError:
            continue
        payload["_manifest_path"] = str(path)
        manifests.append(payload)
    return manifests


def _write_index_manifest_for_scope(
    project_root: Path,
    analysis_rel: str,
    target_name: str,
    target_cfg: TargetConfig,
    scope_rel: str,
    scope_remote: str,
) -> dict[str, Any]:
    if _uses_local_transport(target_cfg):
        entries = _index_local_scope(Path(scope_remote))
    else:
        entries = _index_remote_scope(target_cfg.host, scope_remote)
    manifest = {
        "indexed_at": datetime.now().isoformat(timespec="seconds"),
        "project_root": str(project_root),
        "analysis": analysis_rel,
        "target": target_name,
        "transport": target_cfg.transport,
        "scheduler": target_cfg.scheduler,
        "scope": scope_rel,
        "remote_scope": scope_remote,
        "entry_count": len(entries),
        "entries": entries,
    }
    manifest_path = _index_manifest_path(project_root, target_name, analysis_rel, scope_rel)
    manifest_path.write_text(json.dumps(manifest, indent=2))
    manifest["_manifest_path"] = str(manifest_path)
    return manifest


def _summarize_index_vs_local(local_scope_root: Path, index_manifest: dict[str, Any]) -> dict[str, int]:
    local_entries = _index_local_scope(local_scope_root) if local_scope_root.exists() else []
    local_map = {str(e.get("path", "")).strip("/"): e for e in local_entries if isinstance(e, dict)}
    remote_entries = index_manifest.get("entries", [])
    remote_map = {
        str(e.get("path", "")).strip("/"): e for e in remote_entries if isinstance(e, dict) and str(e.get("path", "")).strip("/")
    } if isinstance(remote_entries, list) else {}

    remote_only = len([k for k in remote_map.keys() if k not in local_map])
    local_only = len([k for k in local_map.keys() if k not in remote_map])
    changed = 0
    for key in set(local_map.keys()).intersection(remote_map.keys()):
        lval = local_map[key]
        rval = remote_map[key]
        if str(lval.get("type", "")) != str(rval.get("type", "")):
            changed += 1
            continue
        if str(lval.get("type", "")) == "file":
            if int(lval.get("size", 0) or 0) != int(rval.get("size", 0) or 0):
                changed += 1
                continue
    return {"remote_only": remote_only, "local_only": local_only, "changed": changed}


def _complete_index_remote_paths(incomplete: str) -> list[str]:
    project_root = _safe_project_root()
    if not project_root:
        return []
    try:
        cfg = load_config(project_root)
        target_name, _ = _active_target(cfg)
        analysis_rel = (cfg.active_analysis or "").strip("/")
        if not analysis_rel:
            return []
        manifest = _load_index_manifest(project_root, target_name, analysis_rel, ".")
        if manifest is None:
            return []
    except Exception:
        return []

    entries = manifest.get("entries", [])
    if not isinstance(entries, list):
        return []
    values: set[str] = set()
    for entry in entries:
        if not isinstance(entry, dict):
            continue
        rel = str(entry.get("path", "")).strip("/")
        if not rel or rel == ".":
            continue
        values.add(rel)
        if "/" in rel:
            values.add(rel.rsplit("/", 1)[0])
    return _prefix_filter(sorted(values), incomplete)


def _auto_index_active_root(project_root: Path, cfg: ProjectConfig, target_name: str, target_cfg: TargetConfig) -> Optional[str]:
    analysis_rel = (cfg.active_analysis or "").strip("/")
    if not analysis_rel:
        return None
    remote_analysis_root = f"{target_cfg.remote_root.rstrip('/')}/{analysis_rel}"
    manifest = _write_index_manifest_for_scope(
        project_root=project_root,
        analysis_rel=analysis_rel,
        target_name=target_name,
        target_cfg=target_cfg,
        scope_rel=".",
        scope_remote=remote_analysis_root,
    )
    return str(manifest.get("_manifest_path", ""))


def _iso_from_epoch(value: float) -> str:
    return datetime.fromtimestamp(value).isoformat(timespec="seconds")


def _index_local_scope(scope_root: Path) -> list[dict[str, Any]]:
    if not scope_root.exists():
        raise typer.BadParameter(f"Remote scope not found: {scope_root}")

    entries: list[dict[str, Any]] = []
    if scope_root.is_file():
        stats = scope_root.stat()
        entries.append(
            {
                "path": ".",
                "type": "file",
                "size": int(stats.st_size),
                "mtime_epoch": int(stats.st_mtime),
                "mtime": _iso_from_epoch(stats.st_mtime),
            }
        )
        return entries

    for path in sorted(scope_root.rglob("*")):
        try:
            rel = path.relative_to(scope_root).as_posix()
        except ValueError:
            continue
        stats = path.stat()
        entries.append(
            {
                "path": rel,
                "type": ("dir" if path.is_dir() else "file"),
                "size": int(stats.st_size),
                "mtime_epoch": int(stats.st_mtime),
                "mtime": _iso_from_epoch(stats.st_mtime),
            }
        )
    return entries


def _index_remote_scope(host: str, scope_root: str) -> list[dict[str, Any]]:
    remote_cmd = (
        f'P={shlex.quote(scope_root)}; '
        'if [ ! -e "$P" ]; then exit 3; fi; '
        'if [ -f "$P" ]; then '
        'stat -c ".\\tf\\t%s\\t%Y" "$P"; '
        "else "
        'find "$P" -mindepth 1 -printf "%P\\t%y\\t%s\\t%T@\\n" | sort; '
        "fi"
    )
    proc = _run_direct(
        ["ssh", host, remote_cmd],
        text=True,
        capture_output=True,
        check=False,
    )
    if proc.returncode == 3:
        raise typer.BadParameter(f"Remote scope not found: {scope_root}")
    if proc.returncode != 0:
        err = (proc.stderr or proc.stdout or "unknown remote indexing error").strip()
        raise typer.BadParameter(f"Failed to index remote scope '{scope_root}': {err}")

    entries: list[dict[str, Any]] = []
    for line in proc.stdout.splitlines():
        parts = line.strip().split("\t")
        if len(parts) != 4:
            continue
        rel_path, kind, size_raw, mtime_raw = parts
        entry_type = "dir" if kind == "d" else "file"
        try:
            size_value = int(float(size_raw))
        except ValueError:
            size_value = 0
        try:
            epoch_value = int(float(mtime_raw))
        except ValueError:
            epoch_value = 0
        entries.append(
            {
                "path": rel_path,
                "type": entry_type,
                "size": size_value,
                "mtime_epoch": epoch_value,
                "mtime": _iso_from_epoch(float(epoch_value)) if epoch_value > 0 else "",
            }
        )
    return entries


def _resolve_index_scopes(
    *,
    analysis_dir: Path,
    analysis_rel: str,
    target_cfg: TargetConfig,
    remote_analysis_root: str,
    local_path: str,
    remote_path: Optional[str],
    all_tags: bool,
    analysis_tags: list[str],
) -> list[dict[str, str]]:
    scopes: list[str] = []
    if all_tags:
        scopes = [tag.strip("/") for tag in analysis_tags if tag.strip("/")]
        if not scopes:
            raise typer.BadParameter("No tags found for active analysis. Tag paths with `cdp analysis tag <path>` first.")
    elif remote_path:
        normalized = remote_path.strip()
        if not normalized:
            raise typer.BadParameter("--remote-path cannot be empty")
        if normalized.startswith("/"):
            if normalized != remote_analysis_root and not normalized.startswith(f"{remote_analysis_root.rstrip('/')}/"):
                raise typer.BadParameter("--remote-path must stay inside the active analysis remote root")
            normalized = normalized.removeprefix(remote_analysis_root).strip("/")
        scopes = [normalized]
    else:
        candidate = Path(local_path).expanduser()
        if candidate.is_absolute():
            raise typer.BadParameter("Path must be relative to the active analysis directory")
        local_target = (analysis_dir / candidate).resolve()
        try:
            rel = local_target.relative_to(analysis_dir).as_posix()
        except ValueError as exc:
            raise typer.BadParameter("Path must stay inside the active analysis directory") from exc
        scopes = ["" if rel == "." else rel]

    resolved: list[dict[str, str]] = []
    for scope_rel in scopes:
        clean_scope = scope_rel.strip("/")
        local_scope = analysis_dir if not clean_scope else (analysis_dir / clean_scope)
        remote_scope = remote_analysis_root if not clean_scope else f"{remote_analysis_root.rstrip('/')}/{clean_scope}"
        if _uses_local_transport(target_cfg):
            local_remote_scope = Path(remote_scope).expanduser()
            resolved.append(
                {
                    "scope": clean_scope or ".",
                    "local_scope": str(local_scope),
                    "remote_scope": str(local_remote_scope),
                }
            )
        else:
            resolved.append(
                {
                    "scope": clean_scope or ".",
                    "local_scope": str(local_scope),
                    "remote_scope": remote_scope,
                }
            )
    return resolved


def _write_sync_event(project_root: Path, payload: dict[str, Any]) -> Path:
    now = datetime.now().isoformat(timespec="seconds")
    stamp = now.replace(":", "-")
    action = str(payload.get("action", "sync")).strip() or "sync"
    target = str(payload.get("target", "")).strip() or "unknown"
    stem = f"{stamp}__{action}__{target}"
    clean_stem = re.sub(r"[^A-Za-z0-9_.-]+", "_", stem).strip("._-")
    path = _sync_events_dir(project_root) / f"{clean_stem}.json"
    counter = 1
    while path.exists():
        path = _sync_events_dir(project_root) / f"{clean_stem}__{counter}.json"
        counter += 1
    payload["recorded_at"] = now
    path.write_text(json.dumps(payload, indent=2))
    return path


def _project_config_snapshot(project_root: Path, cfg: ProjectConfig) -> dict[str, Any]:
    targets_payload: dict[str, Any] = {}
    for name, tcfg in cfg.targets.items():
        targets_payload[name] = {
            "host": tcfg.host,
            "transport": tcfg.transport,
            "scheduler": tcfg.scheduler,
            "remote_root": tcfg.remote_root,
            "has_template": bool(tcfg.template_header.strip()),
            "defaults": {
                "cpus": tcfg.default_cpus,
                "memory": tcfg.default_memory,
                "time": tcfg.default_time,
                "node": tcfg.default_node,
                "queue": tcfg.default_queue,
                "parallel_environment": tcfg.default_parallel_environment,
            },
        }

    active_target_name = cfg.default_target
    active_target = cfg.targets.get(active_target_name)
    active_payload = None
    if active_target is not None:
        active_payload = {
            "name": active_target_name,
            "host": active_target.host,
            "transport": active_target.transport,
            "scheduler": active_target.scheduler,
            "remote_root": active_target.remote_root,
        }

    return {
        "project_root": str(project_root),
        "config_path": str(config_path(project_root)),
        "default_target": cfg.default_target,
        "active_target": active_payload,
        "active_analysis": cfg.active_analysis,
        "ignore_file": str(_ignore_file(project_root)),
        "template_path": str(_template_path(project_root)),
        "targets": targets_payload,
        "resource_profiles": cfg.resource_profiles,
    }


def _load_sync_events(project_root: Path) -> list[dict[str, Any]]:
    sync_dir = project_root / CONFIG_DIR / SYNC_EVENTS_DIR_NAME
    if not sync_dir.exists():
        return []
    events: list[dict[str, Any]] = []
    for path in sorted(sync_dir.glob("*.json"), reverse=True):
        try:
            payload = json.loads(path.read_text())
        except json.JSONDecodeError:
            continue
        payload["_record_file"] = str(path)
        events.append(payload)
    return events


def _parse_iso_ts(value: Any) -> Optional[datetime]:
    if not isinstance(value, str):
        return None
    raw = value.strip()
    if not raw:
        return None
    try:
        return datetime.fromisoformat(raw)
    except ValueError:
        return None


def _export_manifest_payload(
    *,
    include_jobs: bool,
    include_sync: bool,
    include_sweeps: bool,
    redact_hosts: bool,
) -> dict[str, Any]:
    return {
        "format": "cluster-dispatch-export-v1",
        "created_at": datetime.now().isoformat(timespec="seconds"),
        "include_jobs": include_jobs,
        "include_sync": include_sync,
        "include_sweeps": include_sweeps,
        "redact_hosts": redact_hosts,
    }


def _validation_summary(checks: list[dict[str, str]]) -> tuple[int, int, int]:
    pass_count = len([c for c in checks if c["status"] == "PASS"])
    warn_count = len([c for c in checks if c["status"] == "WARN"])
    fail_count = len([c for c in checks if c["status"] == "FAIL"])
    return pass_count, warn_count, fail_count


def _sweep_manifest_path(project_root: Path, sweep_id: str) -> Path:
    return _sweeps_dir(project_root) / f"{sweep_id}.json"


def _load_sweep_manifest(project_root: Path, sweep_id: str) -> dict[str, Any]:
    path = _sweep_manifest_path(project_root, sweep_id)
    if not path.exists():
        raise typer.BadParameter(f"Sweep '{sweep_id}' not found")
    return json.loads(path.read_text())


def _save_sweep_manifest(project_root: Path, manifest: dict[str, Any]) -> None:
    path = _sweep_manifest_path(project_root, str(manifest["sweep_id"]))
    manifest["updated_at"] = datetime.now().isoformat(timespec="seconds")
    path.write_text(json.dumps(manifest, indent=2))


def _deterministic_run_id(block_name: str, params: dict[str, Any], command_template: str) -> str:
    payload = {
        "block_name": block_name,
        "params": params,
        "command_template": command_template,
    }
    material = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
    return hashlib.sha256(material.encode("utf-8")).hexdigest()[:16]


def _deterministic_analysis_run_id(
    target_name: str,
    analysis_rel: str,
    command: str,
    scheduler: str,
    resources: dict[str, Any],
) -> str:
    payload = {
        "target": target_name,
        "analysis": analysis_rel,
        "command": command,
        "scheduler": scheduler,
        "resources": resources,
    }
    material = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
    return hashlib.sha256(material.encode("utf-8")).hexdigest()[:16]


def _expand_sweep_runs(config_payload: dict[str, Any], command_template: str, block_filter: Optional[str]) -> list[dict[str, Any]]:
    params_section = config_payload.get("params")
    if not isinstance(params_section, dict) or not params_section:
        raise typer.BadParameter("Sweep config must define non-empty params mapping")

    selected_blocks = {block_filter: params_section.get(block_filter)} if block_filter else params_section
    if block_filter and selected_blocks[block_filter] is None:
        raise typer.BadParameter(f"Job '{block_filter}' not found under params in sweep config")

    command_placeholders = _extract_placeholders(command_template)
    runs: list[dict[str, Any]] = []
    for block_name, block_params in selected_blocks.items():
        if not isinstance(block_params, dict) or not block_params:
            raise typer.BadParameter(f"params.{block_name} must be a non-empty mapping")

        keys = list(block_params.keys())
        missing = [k for k in keys if k not in command_placeholders]
        if missing:
            raise typer.BadParameter(
                f"Command template missing placeholders for params.{block_name}: {', '.join(missing)}"
            )

        value_lists: list[list[Any]] = []
        for key in keys:
            values = block_params[key]
            if isinstance(values, (list, tuple)):
                if not values:
                    raise typer.BadParameter(f"params.{block_name}.{key} must not be empty")
                value_lists.append(list(values))
            else:
                value_lists.append([values])

        for idx, combo in enumerate(itertools.product(*value_lists), start=1):
            param_map = {keys[pos]: combo[pos] for pos in range(len(keys))}
            render_context = dict(param_map)
            render_context["sweep_job"] = block_name
            render_context["sweep_index"] = idx
            try:
                rendered_command = command_template.format(**render_context)
            except KeyError as exc:
                raise typer.BadParameter(f"Missing placeholder value for '{exc.args[0]}' in sweep command template") from exc

            runs.append(
                {
                    "run_id": _deterministic_run_id(block_name, param_map, command_template),
                    "sweep_job": block_name,
                    "sweep_index": idx,
                    "sweep_params": param_map,
                    "command": rendered_command,
                    "status": "PENDING",
                    "job_id": None,
                    "submitted_at": None,
                }
            )
    return runs


def _build_submit_script(
    header: str,
    command: str,
    working_dir: str,
    remote_log_file: str,
) -> str:
    lines: list[str] = ["#!/usr/bin/env bash"]
    if header.strip():
        lines.extend(header.strip().splitlines())
    lines.extend(
        [
            "set -euo pipefail",
            f"cd {shlex.quote(working_dir)}",
            "echo \"[cluster-dispatch] started $(date -Iseconds)\"",
            f"{command} >> {shlex.quote(remote_log_file)} 2>&1",
            "echo \"[cluster-dispatch] finished $(date -Iseconds)\"",
        ]
    )
    return "\n".join(lines) + "\n"


def _template_path(project_root: Path) -> Path:
    return project_root / CONFIG_DIR / TEMPLATES_DIR_NAME / TEMPLATE_FILE_NAME


def _validate_template_variables(template: str) -> None:
    missing = [name for name in BASE_REQUIRED_TEMPLATE_VARS if f"{{{name}}}" not in template]
    if missing:
        missing_fmt = ", ".join(missing)
        required_fmt = ", ".join(BASE_REQUIRED_TEMPLATE_VARS)
        raise typer.BadParameter(
            f"Template missing required placeholders: {missing_fmt}. "
            f"Required placeholders are: {required_fmt}. "
            "Optional placeholders: queue, node, parallel_environment."
        )


def _render_scheduler_header(
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
            cpus=cpus,
            memory=memory,
            time=walltime,
            job_name=job_name,
            stdout=stdout,
            stderr=stderr,
            working_dir=working_dir,
            queue=queue,
            node=node,
            parallel_environment=parallel_environment,
        )
    except KeyError as exc:
        missing = exc.args[0]
        raise typer.BadParameter(
            f"Scheduler template contains unknown placeholder '{missing}'. "
            "Allowed: cpus, memory, time, job_name, stdout, stderr, working_dir, queue, node, parallel_environment."
        ) from exc


@app.command()
def init(
    project_path: Optional[Path] = typer.Argument(
        None, help="Project directory to initialize (defaults to current directory)"
    ),
    with_git: bool = typer.Option(
        False, "--with-git", help="Also initialize Git repository if one is not already detected"
    ),
) -> None:
    """Initialize .cluster_dispatch/config.yml with default local target."""

    project_root = (project_path or Path.cwd()).resolve()
    project_root.mkdir(parents=True, exist_ok=True)
    cfg_path = config_path(project_root)
    parent_project_root: Optional[Path] = None
    for candidate in project_root.parents:
        if config_path(candidate).exists():
            parent_project_root = candidate
            break

    if cfg_path.exists():
        raise typer.BadParameter(f"{CONFIG_DIR}/{CONFIG_NAME} already exists in this directory")
    if parent_project_root is not None:
        typer.secho(
            (
                f"Warning: cdp init is running inside an existing cdp project.\n"
                f"Existing project root: {parent_project_root}\n"
                f"Initializing nested project at: {project_root}"
            ),
            fg=typer.colors.YELLOW,
        )

    local_target = "local"
    local_root = str(project_root.resolve())
    cfg = ProjectConfig(scheduler="none", default_target=local_target)
    cfg.targets[local_target] = TargetConfig(
        host="localhost",
        scheduler="none",
        remote_root=local_root,
        transport="local",
        template_header="",
        default_cpus=1,
        default_memory="8G",
        default_time="01:00:00",
        default_node="1",
        default_queue="",
        default_parallel_environment="",
    )
    save_config(project_root, cfg)
    ensure_state_dirs(project_root)
    ignore_path = _ignore_file(project_root)
    created_ignore = False
    if not ignore_path.exists():
        ignore_path.write_text(
            "# cdp ignore patterns (rsync --exclude-from)\n"
            "# Example:\n"
            "# *.tmp\n"
            "# data/raw/\n"
        )
        created_ignore = True
    typer.echo(f"Initialized {cfg_path}")
    typer.echo("Default target set to 'local' (scheduler=none, host=localhost)")
    if created_ignore:
        typer.echo(f"Created {ignore_path}")

    if with_git:
        if shutil.which("git") is None:
            raise typer.BadParameter("Git is not installed or not on PATH, but --with-git was requested")

        git_info = get_git_info(project_root)
        if git_info is None:
            init_proc = _run_direct(
                ["git", "init"],
                cwd=project_root,
                text=True,
                capture_output=True,
                check=False,
            )
            if init_proc.returncode != 0:
                error_message = (init_proc.stderr or init_proc.stdout or "unknown git error").strip()
                raise typer.BadParameter(f"Failed to initialize Git repository: {error_message}")
            typer.echo(f"Initialized Git repository in {project_root}")
        else:
            typer.echo("Git repository already detected.")

        gitignore_path = project_root / GITIGNORE_FILE_NAME
        if not gitignore_path.exists():
            gitignore_path.write_text(DEFAULT_GITIGNORE_CONTENT)
            typer.echo(f"Created {gitignore_path}")


@target_app.command("add")
def target_add(
    name: str = typer.Argument(..., help="Target name", autocompletion=_complete_target_names),
    host: Optional[str] = typer.Option(None, help="SSH host alias"),
    remote_root: Optional[str] = typer.Option(
        None, help="Remote absolute root directory for this project on the target"
    ),
    default_cpus: Optional[int] = typer.Option(None, "--cpus", help="Default CPUs for this target"),
    default_memory: Optional[str] = typer.Option(None, "--memory", help="Default memory for this target"),
    default_time: Optional[str] = typer.Option(None, "--time", help="Default walltime for this target"),
    default_node: Optional[str] = typer.Option(None, "--node", help="Default node value for this target"),
    default_queue: Optional[str] = typer.Option(None, "--queue", help="Default queue for this target"),
    default_parallel_environment: Optional[str] = typer.Option(
        None, "--parallel-environment", help="Default parallel environment for this target"
    ),
    scheduler: Optional[str] = typer.Option(
        None, help="sge|univa|pbs|slurm|lsf|none; defaults to project scheduler"
    ),
    transport: Optional[str] = typer.Option(None, help="ssh|local; defaults to existing target transport or ssh"),
    template_file: Optional[Path] = typer.Option(None, help="Optional file containing scheduler header template"),
) -> None:
    """Add or update a compute target."""
    if name.strip().lower() == "local":
        raise typer.BadParameter(
            "Target 'local' is managed by cdp init and cannot be modified. Add a new target name for custom configs."
        )

    project_root = _project_root()
    cfg = load_config(project_root)
    scheduler = (scheduler or cfg.scheduler).lower()
    if scheduler not in {"sge", "univa", "pbs", "slurm", "lsf", "none"}:
        raise typer.BadParameter("scheduler must be one of: sge, univa, pbs, slurm, lsf, none")

    if template_file:
        template_header = template_file.read_text()
        _validate_template_variables(template_header)
    else:
        default_template = _template_path(project_root)
        if not default_template.exists():
            raise typer.BadParameter(
                f"Template not found at {default_template}. "
                "Provide --template-file or re-run init with --template-file."
            )
        template_header = default_template.read_text()
        _validate_template_variables(template_header)

    existing = cfg.targets.get(name)
    resolved_transport = _validate_transport(
        transport if transport is not None else (existing.transport if existing else "ssh")
    )
    resolved_host = host or (existing.host if existing else ("localhost" if resolved_transport == "local" else None))
    resolved_remote_root = remote_root or (existing.remote_root if existing else None)
    if not resolved_host:
        raise typer.BadParameter(
            "--host is required when adding a new target without an existing host configuration"
        )
    if not resolved_remote_root:
        raise typer.BadParameter(
            "--remote-root is required when adding a new target without an existing remote root"
        )
    if not Path(resolved_remote_root).is_absolute():
        raise typer.BadParameter("remote_root must be an absolute remote path")

    resolved_default_cpus = default_cpus if default_cpus is not None else (existing.default_cpus if existing else 1)
    resolved_default_memory = (
        default_memory if default_memory is not None else (existing.default_memory if existing else "8G")
    )
    resolved_default_time = default_time if default_time is not None else (existing.default_time if existing else "01:00:00")
    resolved_default_node = default_node if default_node is not None else (existing.default_node if existing else "1")
    resolved_default_queue = (
        default_queue if default_queue is not None else (existing.default_queue if existing else "")
    )
    resolved_default_parallel_environment = (
        default_parallel_environment
        if default_parallel_environment is not None
        else (existing.default_parallel_environment if existing else "")
    )

    cfg.targets[name] = TargetConfig(
        host=resolved_host,
        scheduler=scheduler,
        remote_root=resolved_remote_root,
        transport=resolved_transport,
        template_header=template_header,
        default_cpus=resolved_default_cpus,
        default_memory=resolved_default_memory,
        default_time=resolved_default_time,
        default_node=resolved_default_node,
        default_queue=resolved_default_queue,
        default_parallel_environment=resolved_default_parallel_environment,
    )
    save_config(project_root, cfg)
    typer.echo(f"Saved target '{name}'")


@target_app.command("set")
def target_set(name: Annotated[str, typer.Argument(help="Target name", autocompletion=_complete_target_names)]) -> None:
    """Set default active target."""
    project_root = _project_root()
    cfg = load_config(project_root)
    if name not in cfg.targets:
        raise typer.BadParameter(f"Target '{name}' not found")
    cfg.default_target = name
    save_config(project_root, cfg)
    typer.echo(f"Default target set to '{name}'")


@target_app.command("list")
def target_list() -> None:
    """List configured targets."""
    project_root = _project_root()
    cfg = load_config(project_root)
    if not cfg.targets:
        typer.echo("No targets configured")
        return
    for name, t in cfg.targets.items():
        marker = "*" if name == cfg.default_target else " "
        typer.echo(f"{marker} {name}: host={t.host} transport={t.transport} scheduler={t.scheduler}")


@target_app.command("remove")
def target_remove(
    name: str = typer.Argument(..., help="Target name", autocompletion=_complete_target_names),
    force: bool = typer.Option(False, "--force", help="Force removal even if records reference this target"),
    fallback_target: Optional[str] = typer.Option(
        None, "--fallback-target", help="Target to become default if removing the current default target"
    ),
    prune_records: bool = typer.Option(
        False, "--prune-records", help="Delete local job/sync records that reference the removed target"
    ),
    yes: bool = typer.Option(False, "--yes", help="Confirm destructive removal/pruning actions"),
) -> None:
    """Remove a target configuration."""
    if name.strip().lower() == "local":
        raise typer.BadParameter("Target 'local' is managed by cdp init and cannot be removed")

    project_root = _project_root()
    cfg = load_config(project_root)
    if name not in cfg.targets:
        raise typer.BadParameter(f"Target '{name}' not found")

    jobs_dir = project_root / CONFIG_DIR / JOBS_DIR
    job_files_to_prune: list[Path] = []
    if jobs_dir.exists():
        for job_file in jobs_dir.glob("*.json"):
            try:
                payload = json.loads(job_file.read_text())
            except json.JSONDecodeError:
                continue
            if str(payload.get("target", "")) == name:
                job_files_to_prune.append(job_file)

    sync_dir = project_root / CONFIG_DIR / SYNC_EVENTS_DIR_NAME
    sync_files_to_prune: list[Path] = []
    if sync_dir.exists():
        for sync_file in sync_dir.glob("*.json"):
            try:
                payload = json.loads(sync_file.read_text())
            except json.JSONDecodeError:
                continue
            if str(payload.get("target", "")) == name:
                sync_files_to_prune.append(sync_file)

    if (job_files_to_prune or sync_files_to_prune) and not force:
        raise typer.BadParameter(
            f"Target '{name}' is referenced by {len(job_files_to_prune)} job record(s) and "
            f"{len(sync_files_to_prune)} sync record(s). Use --force to remove."
        )
    if force and (job_files_to_prune or sync_files_to_prune):
        _require_yes(
            yes,
            f"Target '{name}' has {len(job_files_to_prune)} job record(s) and {len(sync_files_to_prune)} sync record(s). "
            "Re-run with --yes to confirm removal.",
        )

    if cfg.default_target == name:
        chosen_fallback = fallback_target or "local"
        if chosen_fallback == name:
            raise typer.BadParameter("--fallback-target cannot be the same as the removed target")
        if chosen_fallback not in cfg.targets:
            raise typer.BadParameter(f"Fallback target '{chosen_fallback}' not found")
        if not force:
            raise typer.BadParameter(
                f"Target '{name}' is currently the default target. Use --force to remove it. "
                f"Fallback target would be '{chosen_fallback}'."
            )
        _require_yes(
            yes,
            f"Target '{name}' is the default target and will be removed (fallback: '{chosen_fallback}'). "
            "Re-run with --yes to confirm.",
        )
        cfg.default_target = chosen_fallback

    del cfg.targets[name]
    save_config(project_root, cfg)

    pruned_jobs = 0
    pruned_sync = 0
    if prune_records:
        _require_yes(yes, "Pruning local records is destructive. Re-run with --yes to confirm.")
        for path in job_files_to_prune:
            path.unlink(missing_ok=True)
            pruned_jobs += 1
        for path in sync_files_to_prune:
            path.unlink(missing_ok=True)
            pruned_sync += 1

    typer.echo(f"Removed target '{name}'")
    if cfg.default_target:
        typer.echo(f"Default target: {cfg.default_target}")
    if prune_records:
        typer.echo(f"Pruned records: jobs={pruned_jobs} sync={pruned_sync}")


@target_app.command("test")
def target_test(
    name: str = typer.Argument(..., help="Target name", autocompletion=_complete_target_names),
    remote: bool = typer.Option(True, "--remote/--no-remote", help="Include connectivity/scheduler probes"),
    create_root: bool = typer.Option(
        False, "--create-root", help="Create target remote_root if missing/inaccessible"
    ),
    as_json: bool = typer.Option(False, "--json", help="Render check results as JSON"),
) -> None:
    """Run focused connectivity and scheduler checks for one target."""
    project_root = _project_root()
    cfg = load_config(project_root)
    if name not in cfg.targets:
        raise typer.BadParameter(f"Target '{name}' not found")
    tcfg = cfg.targets[name]

    checks: list[dict[str, str]] = []

    def _add(status: str, check: str, detail: str) -> None:
        checks.append({"status": status, "check": check, "detail": detail})
        if not as_json:
            typer.echo(f"[{status}] {check}: {detail}")

    scheduler_cmds: dict[str, list[str]] = {
        "sge": ["qsub", "qstat", "qacct"],
        "univa": ["qsub", "qstat", "qacct"],
        "pbs": ["qsub", "qstat"],
        "slurm": ["sbatch", "squeue", "sacct"],
        "lsf": ["bsub", "bjobs", "bkill"],
        "none": ["bash"],
    }

    if tcfg.scheduler not in {"sge", "univa", "pbs", "slurm", "lsf", "none"}:
        _add("FAIL", "scheduler", f"Unsupported scheduler '{tcfg.scheduler}'")
    else:
        _add("PASS", "scheduler", tcfg.scheduler)

    if not tcfg.remote_root:
        _add("FAIL", "remote_root", "Missing remote_root")
    elif not Path(tcfg.remote_root).is_absolute():
        _add("FAIL", "remote_root", f"Not absolute: {tcfg.remote_root}")
    else:
        _add("PASS", "remote_root", tcfg.remote_root)

    if tcfg.scheduler == "none" and not tcfg.template_header.strip():
        _add("PASS", "template", "Skipped for scheduler=none with empty template")
    else:
        try:
            _validate_template_variables(tcfg.template_header)
            _add("PASS", "template", "Template placeholders valid")
        except Exception as exc:
            _add("FAIL", "template", str(exc))

    if remote:
        if _uses_local_transport(tcfg):
            _add("PASS", "connectivity", "Skipped (local target mode)")
            root_path = Path(tcfg.remote_root)
            if root_path.is_dir():
                _add("PASS", "remote_root_exists", tcfg.remote_root)
            elif create_root:
                try:
                    root_path.mkdir(parents=True, exist_ok=True)
                    _add("PASS", "remote_root_create", f"Created: {tcfg.remote_root}")
                except Exception as exc:
                    _add("FAIL", "remote_root_create", str(exc))
            else:
                _add("WARN", "remote_root_exists", f"Local root missing/inaccessible: {tcfg.remote_root}")

            required_cmds = scheduler_cmds.get(tcfg.scheduler, [])
            if required_cmds:
                missing = [c for c in required_cmds if shutil.which(c) is None]
                if not missing:
                    _add("PASS", "scheduler_cmds", ", ".join(required_cmds))
                else:
                    _add("WARN", "scheduler_cmds", f"Missing one or more: {', '.join(required_cmds)}")
        else:
            ssh_probe = _run_direct(
                ["ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=10", tcfg.host, "echo", "cdp-ok"],
                text=True,
                capture_output=True,
                check=False,
            )
            if ssh_probe.returncode == 0 and "cdp-ok" in ssh_probe.stdout:
                _add("PASS", "connectivity", f"Connected to {tcfg.host}")
            else:
                _add("FAIL", "connectivity", ssh_probe.stderr.strip() or ssh_probe.stdout.strip() or "SSH failed")

            if ssh_probe.returncode == 0 and "cdp-ok" in ssh_probe.stdout:
                root_probe = _run_direct(
                    [
                        "ssh",
                        tcfg.host,
                        "bash",
                        "-lc",
                        f"if [ -d {shlex.quote(tcfg.remote_root)} ]; then echo OK; else echo MISSING; fi",
                    ],
                    text=True,
                    capture_output=True,
                    check=False,
                )
                if root_probe.returncode == 0 and root_probe.stdout.strip() == "OK":
                    _add("PASS", "remote_root_exists", tcfg.remote_root)
                elif create_root:
                    create_probe = _run_direct(
                        ["ssh", tcfg.host, "mkdir", "-p", tcfg.remote_root],
                        text=True,
                        capture_output=True,
                        check=False,
                    )
                    if create_probe.returncode == 0:
                        _add("PASS", "remote_root_create", f"Created: {tcfg.remote_root}")
                    else:
                        _add(
                            "FAIL",
                            "remote_root_create",
                            create_probe.stderr.strip() or create_probe.stdout.strip() or "Failed to create remote root",
                        )
                else:
                    _add("WARN", "remote_root_exists", f"Remote root missing/inaccessible: {tcfg.remote_root}")

                required_cmds = scheduler_cmds.get(tcfg.scheduler, [])
                if required_cmds:
                    cmd_probe = _run_direct(
                        ["ssh", tcfg.host, "bash", "-lc", " && ".join([f"command -v {c} >/dev/null 2>&1" for c in required_cmds])],
                        text=True,
                        capture_output=True,
                        check=False,
                    )
                    if cmd_probe.returncode == 0:
                        _add("PASS", "scheduler_cmds", ", ".join(required_cmds))
                    else:
                        _add("WARN", "scheduler_cmds", f"Missing one or more: {', '.join(required_cmds)}")

    pass_count = len([c for c in checks if c["status"] == "PASS"])
    warn_count = len([c for c in checks if c["status"] == "WARN"])
    fail_count = len([c for c in checks if c["status"] == "FAIL"])
    summary = {"target": name, "pass": pass_count, "warn": warn_count, "fail": fail_count}
    if as_json:
        typer.echo(json.dumps({"summary": summary, "checks": checks}, indent=2))
    else:
        typer.echo(f"Summary: PASS={pass_count} WARN={warn_count} FAIL={fail_count}")
    if fail_count:
        raise typer.Exit(code=1)


@ignore_app.command("list")
def ignore_list() -> None:
    """List .cdpignore patterns."""
    project_root = _project_root()
    ignore_path = _ignore_file(project_root)
    if not ignore_path.exists():
        typer.echo(f"{IGNORE_FILE_NAME} not found in project root")
        return
    lines = ignore_path.read_text().splitlines()
    patterns = [line for line in lines if line.strip() and not line.lstrip().startswith("#")]
    if not patterns:
        typer.echo(f"No ignore patterns in {IGNORE_FILE_NAME}")
        return
    typer.echo(f"Ignore patterns in {IGNORE_FILE_NAME}:")
    for pattern in patterns:
        typer.echo(f"- {pattern}")


@ignore_app.command("add")
def ignore_add(
    pattern: str = typer.Argument(..., help="Pattern to add to .cdpignore"),
) -> None:
    """Add a pattern to .cdpignore."""
    clean = pattern.strip()
    if not clean:
        raise typer.BadParameter("Pattern cannot be empty")
    project_root = _project_root()
    ignore_path = _ignore_file(project_root)
    existing = ignore_path.read_text().splitlines() if ignore_path.exists() else []
    if clean in existing:
        typer.echo(f"Already present: {clean}")
        return
    with ignore_path.open("a", encoding="utf-8") as handle:
        if existing and existing[-1].strip():
            handle.write("\n")
        handle.write(f"{clean}\n")
    typer.echo(f"Added to {IGNORE_FILE_NAME}: {clean}")


@ignore_app.command("remove")
def ignore_remove(
    pattern: str = typer.Argument(..., help="Exact pattern to remove from .cdpignore"),
) -> None:
    """Remove an exact pattern from .cdpignore."""
    clean = pattern.strip()
    if not clean:
        raise typer.BadParameter("Pattern cannot be empty")
    project_root = _project_root()
    ignore_path = _ignore_file(project_root)
    if not ignore_path.exists():
        raise typer.BadParameter(f"{IGNORE_FILE_NAME} not found in project root")
    lines = ignore_path.read_text().splitlines()
    filtered = [line for line in lines if line != clean]
    if len(filtered) == len(lines):
        typer.echo(f"Pattern not found: {clean}")
        return
    ignore_path.write_text("\n".join(filtered).rstrip() + "\n")
    typer.echo(f"Removed from {IGNORE_FILE_NAME}: {clean}")


@app.command("doctor")
def doctor(
    target: Optional[str] = typer.Option(
        None, "--target", help="Run checks only for one target", autocompletion=_complete_target_names
    ),
    remote: bool = typer.Option(True, "--remote/--no-remote", help="Include remote SSH/scheduler checks"),
) -> None:
    """Run project preflight checks."""
    project_root = _project_root()
    results: list[tuple[str, str, str]] = []

    def _add(status: str, name: str, detail: str) -> None:
        results.append((status, name, detail))
        typer.echo(f"[{status}] {name}: {detail}")

    # Local checks.
    try:
        cfg = load_config(project_root)
        _add("PASS", "config", f"Loaded {config_path(project_root)}")
    except Exception as exc:
        _add("FAIL", "config", str(exc))
        raise typer.Exit(code=1) from exc

    ensure_state_dirs(project_root)
    _add("PASS", "state_dirs", f"Found/created {project_root / CONFIG_DIR}")

    if cfg.default_target in cfg.targets:
        _add("PASS", "default_target", f"{cfg.default_target}")
    else:
        _add("FAIL", "default_target", f"Default target '{cfg.default_target}' not configured")

    template_path = _template_path(project_root)
    if template_path.exists():
        try:
            _validate_template_variables(template_path.read_text())
            _add("PASS", "template", f"Template placeholders valid: {template_path}")
        except Exception as exc:
            _add("FAIL", "template", f"{template_path}: {exc}")
    else:
        _add("WARN", "template", f"Template file not found at {template_path}")

    needs_ssh_binary = False
    if remote:
        if target and target in cfg.targets:
            needs_ssh_binary = not _uses_local_transport(cfg.targets[target])
        elif not target:
            needs_ssh_binary = any(not _uses_local_transport(tcfg) for tcfg in cfg.targets.values())
    required_binaries = ["rsync"] + (["ssh"] if needs_ssh_binary else [])
    for binary in required_binaries:
        if shutil.which(binary):
            _add("PASS", "local_binary", f"{binary} found")
        else:
            _add("FAIL", "local_binary", f"{binary} not found in PATH")

    if cfg.active_analysis:
        active_path = (project_root / cfg.active_analysis).resolve()
        if active_path.exists() and active_path.is_dir():
            _add("PASS", "active_analysis", str(active_path))
        else:
            _add("WARN", "active_analysis", f"Configured active analysis missing: {active_path}")
    else:
        _add("WARN", "active_analysis", "No active analysis set")

    index_paths = _index_manifest_paths(project_root)
    corrupt_indexes = 0
    newest_index_ts: Optional[datetime] = None
    for path in index_paths:
        try:
            payload = json.loads(path.read_text())
        except json.JSONDecodeError:
            corrupt_indexes += 1
            continue
        ts = _parse_iso_ts(payload.get("indexed_at"))
        if ts is not None and (newest_index_ts is None or ts > newest_index_ts):
            newest_index_ts = ts
    if not index_paths:
        _add("WARN", "index", f"No index manifests found under {project_root / CONFIG_DIR / INDEX_DIR_NAME}")
    else:
        _add("PASS", "index", f"Found {len(index_paths)} manifest(s)")
    if corrupt_indexes:
        _add("FAIL", "index_corrupt", f"{corrupt_indexes} manifest(s) could not be parsed")
    if newest_index_ts is not None:
        age_hours = (datetime.now() - newest_index_ts).total_seconds() / 3600.0
        if age_hours > 24.0:
            _add("WARN", "index_freshness", f"Latest index is stale ({age_hours:.1f}h old)")
        else:
            _add("PASS", "index_freshness", f"Latest index age {age_hours:.1f}h")

    # Target checks.
    selected_targets: dict[str, TargetConfig]
    if target:
        if target not in cfg.targets:
            _add("FAIL", "target", f"Target '{target}' not found")
            raise typer.Exit(code=1)
        selected_targets = {target: cfg.targets[target]}
    else:
        selected_targets = dict(cfg.targets)

    scheduler_cmds: dict[str, list[str]] = {
        "sge": ["qsub", "qstat", "qacct"],
        "univa": ["qsub", "qstat", "qacct"],
        "pbs": ["qsub", "qstat"],
        "slurm": ["sbatch", "squeue", "sacct"],
        "lsf": ["bsub", "bjobs", "bkill"],
        "none": ["bash"],
    }

    for name, tcfg in selected_targets.items():
        if tcfg.scheduler not in {"sge", "univa", "pbs", "slurm", "lsf", "none"}:
            _add("FAIL", f"target:{name}:scheduler", f"Unsupported scheduler '{tcfg.scheduler}'")
        else:
            _add("PASS", f"target:{name}:scheduler", tcfg.scheduler)

        if not tcfg.remote_root:
            _add("FAIL", f"target:{name}:remote_root", "Missing remote_root")
        elif not Path(tcfg.remote_root).is_absolute():
            _add("FAIL", f"target:{name}:remote_root", f"Not absolute: {tcfg.remote_root}")
        else:
            _add("PASS", f"target:{name}:remote_root", tcfg.remote_root)

        try:
            _validate_template_variables(tcfg.template_header)
            _add("PASS", f"target:{name}:template", "Template placeholders valid")
        except Exception as exc:
            _add("FAIL", f"target:{name}:template", str(exc))

        if not remote:
            continue

        if _uses_local_transport(tcfg):
            _add("PASS", f"target:{name}:ssh", "Skipped (local target mode)")
            if Path(tcfg.remote_root).is_dir():
                _add("PASS", f"target:{name}:remote_root_exists", tcfg.remote_root)
            else:
                _add("WARN", f"target:{name}:remote_root_exists", f"Local root missing/inaccessible: {tcfg.remote_root}")
            required_cmds = scheduler_cmds.get(tcfg.scheduler, [])
            if required_cmds:
                missing = [c for c in required_cmds if shutil.which(c) is None]
                if not missing:
                    _add("PASS", f"target:{name}:scheduler_cmds", ", ".join(required_cmds))
                else:
                    _add("WARN", f"target:{name}:scheduler_cmds", f"Missing one or more: {', '.join(required_cmds)}")
            continue

        ssh_probe = _run_direct(
            ["ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=10", tcfg.host, "echo", "cdp-ok"],
            text=True,
            capture_output=True,
            check=False,
        )
        if ssh_probe.returncode == 0 and "cdp-ok" in ssh_probe.stdout:
            _add("PASS", f"target:{name}:ssh", f"Connected to {tcfg.host}")
        else:
            _add("FAIL", f"target:{name}:ssh", ssh_probe.stderr.strip() or ssh_probe.stdout.strip() or "SSH failed")
            continue

        root_probe = _run_direct(
            ["ssh", tcfg.host, "bash", "-lc", f"if [ -d {shlex.quote(tcfg.remote_root)} ]; then echo OK; else echo MISSING; fi"],
            text=True,
            capture_output=True,
            check=False,
        )
        if root_probe.returncode == 0 and root_probe.stdout.strip() == "OK":
            _add("PASS", f"target:{name}:remote_root_exists", tcfg.remote_root)
        else:
            _add("WARN", f"target:{name}:remote_root_exists", f"Remote root missing/inaccessible: {tcfg.remote_root}")

        required_cmds = scheduler_cmds.get(tcfg.scheduler, [])
        if required_cmds:
            cmd_probe = _run_direct(
                ["ssh", tcfg.host, "bash", "-lc", " && ".join([f"command -v {c} >/dev/null 2>&1" for c in required_cmds])],
                text=True,
                capture_output=True,
                check=False,
            )
            if cmd_probe.returncode == 0:
                _add("PASS", f"target:{name}:scheduler_cmds", ", ".join(required_cmds))
            else:
                _add("WARN", f"target:{name}:scheduler_cmds", f"Missing one or more: {', '.join(required_cmds)}")

    fail_count = len([r for r in results if r[0] == "FAIL"])
    warn_count = len([r for r in results if r[0] == "WARN"])
    pass_count = len([r for r in results if r[0] == "PASS"])
    typer.echo(f"Summary: PASS={pass_count} WARN={warn_count} FAIL={fail_count}")
    if fail_count:
        raise typer.Exit(code=1)


@profile_app.command("list")
def profile_list() -> None:
    """List resource profiles (built-in and user-defined)."""
    project_root = _project_root()
    cfg = load_config(project_root)
    merged = _all_resource_profiles(cfg)
    for name in sorted(merged.keys()):
        values = merged[name]
        source = "user" if name in cfg.resource_profiles else "built-in"
        typer.echo(
            f"{name} ({source}): cpus={values['cpus']} memory={values['memory']} time={values['time']} "
            f"node={values['node']} queue={values['queue']} parallel_environment={values['parallel_environment']}"
        )


@profile_app.command("show")
def profile_show(
    name: str = typer.Argument(..., help="Profile name", autocompletion=_complete_profile_names),
) -> None:
    """Show one resource profile."""
    project_root = _project_root()
    cfg = load_config(project_root)
    key = _validate_profile_name(name)
    merged = _all_resource_profiles(cfg)
    if key not in merged:
        raise typer.BadParameter(f"Profile '{name}' not found")
    values = merged[key]
    source = "user" if key in cfg.resource_profiles else "built-in"
    typer.echo(f"{key} ({source})")
    typer.echo(f"cpus={values.get('cpus')}")
    typer.echo(f"memory={values.get('memory')}")
    typer.echo(f"time={values.get('time')}")
    typer.echo(f"node={values.get('node')}")
    typer.echo(f"queue={values.get('queue')}")
    typer.echo(f"parallel_environment={values.get('parallel_environment')}")


@profile_app.command("set")
def profile_set(
    name: str = typer.Argument(..., help="Profile name", autocompletion=_complete_profile_names),
    cpus: Optional[int] = typer.Option(None, "--cpus", min=1, help="CPU cores"),
    memory: Optional[str] = typer.Option(None, "--memory", help="Memory (e.g. 8G)"),
    job_time: Optional[str] = typer.Option(None, "--time", help="Walltime (e.g. 02:00:00)"),
    node: Optional[str] = typer.Option(None, "--node", help="Node value"),
    queue: Optional[str] = typer.Option(None, "--queue", help="Queue value"),
    parallel_environment: Optional[str] = typer.Option(
        None, "--parallel-environment", help="Parallel environment value"
    ),
) -> None:
    """Create or update a user-defined resource profile."""
    key = _validate_profile_name(name)
    if all(v is None for v in [cpus, memory, job_time, node, queue, parallel_environment]):
        raise typer.BadParameter("Provide at least one resource option to set")

    project_root = _project_root()
    cfg = load_config(project_root)
    current = dict(_all_resource_profiles(cfg).get(key, {}))
    if cpus is not None:
        current["cpus"] = cpus
    if memory is not None:
        current["memory"] = memory
    if job_time is not None:
        current["time"] = job_time
    if node is not None:
        current["node"] = node
    if queue is not None:
        current["queue"] = queue
    if parallel_environment is not None:
        current["parallel_environment"] = parallel_environment

    required_keys = {"cpus", "memory", "time", "node", "queue", "parallel_environment"}
    missing = [k for k in sorted(required_keys) if k not in current]
    if missing:
        raise typer.BadParameter(
            f"Profile '{key}' is missing values for: {', '.join(missing)}. "
            "Set all required keys when creating a brand-new profile."
        )

    cfg.resource_profiles[key] = {
        "cpus": int(current["cpus"]),
        "memory": str(current["memory"]),
        "time": str(current["time"]),
        "node": str(current["node"]),
        "queue": str(current["queue"]),
        "parallel_environment": str(current["parallel_environment"]),
    }
    save_config(project_root, cfg)
    typer.echo(f"Saved profile '{key}'")


@profile_app.command("delete")
def profile_delete(
    name: str = typer.Argument(..., help="User profile name", autocompletion=_complete_user_profile_names),
) -> None:
    """Delete a user-defined profile."""
    key = _validate_profile_name(name)
    if key in DEFAULT_RESOURCE_PROFILES:
        raise typer.BadParameter(f"Cannot delete built-in profile '{key}'")
    project_root = _project_root()
    cfg = load_config(project_root)
    if key not in cfg.resource_profiles:
        raise typer.BadParameter(f"User profile '{key}' not found")
    del cfg.resource_profiles[key]
    save_config(project_root, cfg)
    typer.echo(f"Deleted profile '{key}'")


def _build_sweep_manifest(
    sweep_id: str,
    mode: str,
    target_name: str,
    target: TargetConfig,
    analysis_rel: str,
    config_file: Path,
    command_template: str,
    runs: list[dict[str, Any]],
) -> dict[str, Any]:
    now = datetime.now().isoformat(timespec="seconds")
    remote_sweep_dir = f"{target.remote_root.rstrip('/')}/{analysis_rel.strip('/')}/sweeps/{sweep_id}"
    return {
        "sweep_id": sweep_id,
        "created_at": now,
        "updated_at": now,
        "mode": mode,
        "analysis": analysis_rel,
        "target": target_name,
        "host": target.host,
        "scheduler": target.scheduler,
        "config_file": str(config_file),
        "command_template": command_template,
        "remote_sweep_dir": remote_sweep_dir,
        "array_job_id": None,
        "resources": {},
        "runs": runs,
    }


def _append_sweep_job_record(
    project_root: Path,
    cfg: ProjectConfig,
    target_name: str,
    target: TargetConfig,
    sweep_id: str,
    submission_mode: str,
    run: dict[str, Any],
    remote_sweep_dir: str,
    remote_log_file: str,
    submit_script_path: Optional[str],
    sync_source: Optional[str],
    sync_destination: Optional[str],
    ignore_file_path: Optional[str],
    ignore_used: bool,
    profile_name: Optional[str],
    submitted_at: str,
    working_dir: Optional[str] = None,
    scheduler_override: Optional[str] = None,
    state_override: Optional[str] = None,
) -> None:
    scheduler_value = scheduler_override or target.scheduler
    state_value = state_override or "RUNNING_OR_QUEUED"
    append_job_record(
        project_root,
        _build_job_record(
            project_root=project_root,
            submitted_at=submitted_at,
            analysis=cfg.active_analysis,
            analysis_tags=cfg.analysis_tags.get(cfg.active_analysis or "", []),
            target_name=target_name,
            scheduler=scheduler_value,
            job_id=str(run.get("job_id", "")),
            run_id=str(run.get("run_id", "")),
            job_name=str(run.get("job_name", "")),
            state=state_value,
            command=str(run.get("command", "")),
            remote_run_dir=remote_sweep_dir,
            remote_log_file=remote_log_file,
            working_dir=(working_dir if working_dir is not None else target.remote_root),
            cpus=run.get("resources", {}).get("cpus"),
            memory=run.get("resources", {}).get("memory"),
            walltime=run.get("resources", {}).get("time"),
            node=run.get("resources", {}).get("node"),
            queue=run.get("resources", {}).get("queue"),
            parallel_environment=run.get("resources", {}).get("parallel_environment"),
            profile_name=profile_name,
            submit_script_path=submit_script_path,
            submission_mode=submission_mode,
            sync_source=sync_source,
            sync_destination=sync_destination,
            ignore_file_path=ignore_file_path,
            ignore_used=ignore_used,
            sweep_fields={
                "sweep_id": sweep_id,
                "sweep_job": run.get("sweep_job", ""),
                "sweep_index": run.get("sweep_index"),
                "sweep_params": run.get("sweep_params", {}),
            },
        ),
    )


def _require_sweep_in_active_analysis(manifest: dict[str, Any], active_analysis: str) -> None:
    manifest_analysis = str(manifest.get("analysis", "")).strip("/")
    if not manifest_analysis:
        raise typer.BadParameter(
            "Sweep manifest is missing analysis context. Re-run this sweep from an active analysis with `cdp analysis use <path>`."
        )
    if manifest_analysis != active_analysis.strip("/"):
        raise typer.BadParameter(
            f"Sweep belongs to analysis '{manifest_analysis}', but active analysis is '{active_analysis}'. "
            "Switch context with `cdp analysis use <path>`."
        )


def _submit_sweep_single(
    project_root: Path,
    cfg: ProjectConfig,
    target_name: str,
    target: TargetConfig,
    manifest: dict[str, Any],
    run_indexes: list[int],
    resources: dict[str, Any],
    base_job_name: str,
    submit_delay_ms: int,
) -> int:
    remote_sweep_dir = str(manifest["remote_sweep_dir"])
    analysis_rel = str(manifest.get("analysis", "")).strip("/")
    remote_analysis_root = f"{target.remote_root.rstrip('/')}/{analysis_rel}" if analysis_rel else target.remote_root
    sync_source = str((project_root / analysis_rel).resolve()) if analysis_rel else None
    sync_destination = remote_analysis_root
    ignore_path = _ignore_file(project_root)
    ignore_file_path = str(ignore_path) if ignore_path.exists() else None
    ignore_used = bool(ignore_file_path)
    profile_name = str(manifest.get("resources", {}).get("profile", "")).strip() or None
    local_target_mode = _uses_local_transport(target)
    if local_target_mode:
        Path(remote_sweep_dir).mkdir(parents=True, exist_ok=True)
    else:
        _run_cmd(["ssh", target.host, "mkdir", "-p", remote_sweep_dir])
    adapter = get_adapter(target.scheduler)
    submitted_count = 0

    for offset, idx in enumerate(run_indexes):
        run = manifest["runs"][idx]
        run_slug = _slug_component(str(run["sweep_job"]))
        run_name = f"{base_job_name}-{run_slug}-{int(run['sweep_index']):03d}"
        remote_submit_script = f"{remote_sweep_dir}/pc_submit_{run['run_id']}.sh"
        remote_log_file = f"{remote_sweep_dir}/run_{run['run_id']}.log"
        submit_script = _build_submit_script(
            header=_render_scheduler_header(
                target.template_header,
                cpus=int(resources["cpus"]),
                memory=str(resources["memory"]),
                walltime=str(resources["time"]),
                job_name=run_name,
                stdout=run_name,
                stderr=run_name,
                working_dir=remote_analysis_root,
                queue=str(resources["queue"]),
                node=str(resources["node"]),
                parallel_environment=str(resources["parallel_environment"]),
            ),
            command=str(run["command"]),
            working_dir=remote_analysis_root,
            remote_log_file=remote_log_file,
        )
        if local_target_mode:
            submit_path = Path(remote_submit_script)
            submit_path.parent.mkdir(parents=True, exist_ok=True)
            submit_path.write_text(submit_script)
            submit_path.chmod(0o755)
        else:
            _write_remote_script(target.host, remote_submit_script, submit_script, executable=True)
        try:
            submit_result = adapter.submit(target.host, remote_submit_script, transport=target.transport)
        except Exception as exc:
            raise typer.BadParameter(f"Failed to submit sweep run {run.get('run_id', '')}: {exc}") from exc
        submitted_at = datetime.now().isoformat(timespec="seconds")
        run["job_id"] = submit_result.job_id
        run["job_name"] = run_name
        run["status"] = "SUBMITTED"
        run["submitted_at"] = submitted_at
        run["submission_mode"] = "single"
        run["resources"] = resources

        _append_sweep_job_record(
            project_root=project_root,
            cfg=cfg,
            target_name=target_name,
            target=target,
            sweep_id=str(manifest["sweep_id"]),
            submission_mode="single",
            run=run,
            remote_sweep_dir=remote_sweep_dir,
            remote_log_file=remote_log_file,
            submit_script_path=remote_submit_script,
            sync_source=sync_source,
            sync_destination=sync_destination,
            ignore_file_path=ignore_file_path,
            ignore_used=ignore_used,
            profile_name=profile_name,
            submitted_at=submitted_at,
            working_dir=remote_analysis_root,
        )
        submitted_count += 1
        if offset < len(run_indexes) - 1:
            time.sleep(submit_delay_ms / 1000.0)
    return submitted_count


def _submit_sweep_array(
    project_root: Path,
    cfg: ProjectConfig,
    target_name: str,
    target: TargetConfig,
    manifest: dict[str, Any],
    run_indexes: list[int],
    resources: dict[str, Any],
    base_job_name: str,
) -> int:
    scheduler_lc = target.scheduler.lower()
    if scheduler_lc == "none":
        raise typer.BadParameter("Array submission mode is not supported for scheduler 'none'")

    if scheduler_lc == "slurm":
        array_directive_prefix = "#SBATCH --array="
        task_var_expr = "${SLURM_ARRAY_TASK_ID:?SLURM_ARRAY_TASK_ID not set}"
    elif scheduler_lc in {"sge", "univa"}:
        array_directive_prefix = "#$ -t "
        task_var_expr = "${SGE_TASK_ID:?SGE_TASK_ID not set}"
    elif scheduler_lc == "pbs":
        array_directive_prefix = "#PBS -J "
        task_var_expr = "${PBS_ARRAY_INDEX:-${PBS_ARRAYID:?PBS_ARRAY_INDEX/PBS_ARRAYID not set}}"
    elif scheduler_lc == "lsf":
        array_directive_prefix = "#BSUB -J "
        task_var_expr = "${LSB_JOBINDEX:?LSB_JOBINDEX not set}"
    else:
        raise typer.BadParameter(f"Array submission mode not supported for scheduler '{target.scheduler}'")

    if not run_indexes:
        return 0

    remote_sweep_dir = str(manifest["remote_sweep_dir"])
    analysis_rel = str(manifest.get("analysis", "")).strip("/")
    remote_analysis_root = f"{target.remote_root.rstrip('/')}/{analysis_rel}" if analysis_rel else target.remote_root
    sync_source = str((project_root / analysis_rel).resolve()) if analysis_rel else None
    sync_destination = remote_analysis_root
    ignore_path = _ignore_file(project_root)
    ignore_file_path = str(ignore_path) if ignore_path.exists() else None
    ignore_used = bool(ignore_file_path)
    profile_name = str(manifest.get("resources", {}).get("profile", "")).strip() or None
    local_target_mode = _uses_local_transport(target)
    if local_target_mode:
        Path(remote_sweep_dir).mkdir(parents=True, exist_ok=True)
    else:
        _run_cmd(["ssh", target.host, "mkdir", "-p", remote_sweep_dir])

    local_sweeps = _sweeps_dir(project_root)
    sweep_id = str(manifest["sweep_id"])
    tsv_local = local_sweeps / f"{sweep_id}.array.tsv"
    wrapper_local = local_sweeps / f"{sweep_id}.array_wrapper.sh"

    tsv_lines: list[str] = []
    for task_id, run_idx in enumerate(run_indexes, start=1):
        run = manifest["runs"][run_idx]
        run_slug = _slug_component(str(run["sweep_job"]))
        run_name = f"{base_job_name}-{run_slug}-{int(run['sweep_index']):03d}"
        command_b64 = base64.b64encode(str(run["command"]).encode("utf-8")).decode("ascii")
        tsv_lines.append(f"{task_id}\t{run['run_id']}\t{run_name}\t{command_b64}")
    tsv_local.write_text("\n".join(tsv_lines) + "\n")

    wrapper_local.write_text(
        "#!/usr/bin/env bash\n"
        "set -euo pipefail\n"
        "MAP_FILE=\"$1\"\n"
        f"TASK_ID=\"{task_var_expr}\"\n"
        "LINE=\"$(awk -F $'\\t' -v id=\"$TASK_ID\" '$1==id {print; exit}' \"$MAP_FILE\")\"\n"
        "if [ -z \"$LINE\" ]; then\n"
        "  echo \"No mapping found for task ${TASK_ID}\" >&2\n"
        "  exit 1\n"
        "fi\n"
        "RUN_ID=\"$(printf '%s' \"$LINE\" | cut -f2)\"\n"
        "JOB_NAME=\"$(printf '%s' \"$LINE\" | cut -f3)\"\n"
        "CMD_B64=\"$(printf '%s' \"$LINE\" | cut -f4)\"\n"
        "CMD=\"$(printf '%s' \"$CMD_B64\" | base64 --decode)\"\n"
        "echo \"[cluster-dispatch] run_id=${RUN_ID} job_name=${JOB_NAME} task_id=${TASK_ID}\" \n"
        "eval \"$CMD\"\n"
    )
    wrapper_local.chmod(0o755)

    tsv_remote = f"{remote_sweep_dir}/{tsv_local.name}"
    wrapper_remote = f"{remote_sweep_dir}/{wrapper_local.name}"
    if local_target_mode:
        _run_cmd(["rsync", "-az", str(tsv_local), tsv_remote])
        _run_cmd(["rsync", "-az", str(wrapper_local), wrapper_remote])
    else:
        _run_cmd(["rsync", "-az", str(tsv_local), f"{target.host}:{tsv_remote}"])
        _run_cmd(["rsync", "-az", str(wrapper_local), f"{target.host}:{wrapper_remote}"])

    array_size = len(run_indexes)
    array_header = _render_scheduler_header(
        target.template_header,
        cpus=int(resources["cpus"]),
        memory=str(resources["memory"]),
        walltime=str(resources["time"]),
        job_name=f"{base_job_name}-array",
        stdout=f"{base_job_name}-array",
        stderr=f"{base_job_name}-array",
        working_dir=remote_analysis_root,
        queue=str(resources["queue"]),
        node=str(resources["node"]),
        parallel_environment=str(resources["parallel_environment"]),
    )
    if scheduler_lc == "lsf":
        array_header = array_header.rstrip() + f"\n{array_directive_prefix}\"{base_job_name}-array[1-{array_size}]\"\n"
    else:
        array_header = array_header.rstrip() + f"\n{array_directive_prefix}1-{array_size}\n"

    remote_array_submit = f"{remote_sweep_dir}/pc_submit_array_{sweep_id}.sh"
    remote_log_file = f"{remote_sweep_dir}/run_array_{sweep_id}.log"
    array_script = _build_submit_script(
        header=array_header,
        command=f"bash {shlex.quote(wrapper_remote)} {shlex.quote(tsv_remote)}",
        working_dir=remote_analysis_root,
        remote_log_file=remote_log_file,
    )
    if local_target_mode:
        array_submit_path = Path(remote_array_submit)
        array_submit_path.parent.mkdir(parents=True, exist_ok=True)
        array_submit_path.write_text(array_script)
        array_submit_path.chmod(0o755)
    else:
        _write_remote_script(target.host, remote_array_submit, array_script, executable=True)

    adapter = get_adapter(target.scheduler)
    try:
        submit_result = adapter.submit(target.host, remote_array_submit, transport=target.transport)
    except Exception as exc:
        raise typer.BadParameter(f"Failed to submit sweep array job: {exc}") from exc
    base_job_id = submit_result.job_id
    manifest["array_job_id"] = base_job_id

    for task_id, run_idx in enumerate(run_indexes, start=1):
        run = manifest["runs"][run_idx]
        run_slug = _slug_component(str(run["sweep_job"]))
        run_name = f"{base_job_name}-{run_slug}-{int(run['sweep_index']):03d}"
        submitted_at = datetime.now().isoformat(timespec="seconds")
        run["job_id"] = f"{base_job_id}_{task_id}"
        run["array_job_id"] = base_job_id
        run["array_task_id"] = task_id
        run["job_name"] = run_name
        run["status"] = "SUBMITTED"
        run["submitted_at"] = submitted_at
        run["submission_mode"] = "array"
        run["resources"] = resources

        _append_sweep_job_record(
            project_root=project_root,
            cfg=cfg,
            target_name=target_name,
            target=target,
            sweep_id=sweep_id,
            submission_mode="array",
            run=run,
            remote_sweep_dir=remote_sweep_dir,
            remote_log_file=remote_log_file,
            submit_script_path=remote_array_submit,
            sync_source=sync_source,
            sync_destination=sync_destination,
            ignore_file_path=ignore_file_path,
            ignore_used=ignore_used,
            profile_name=profile_name,
            submitted_at=submitted_at,
            working_dir=remote_analysis_root,
        )
    return len(run_indexes)


def _submit_sweep_local(
    project_root: Path,
    cfg: ProjectConfig,
    target_name: str,
    target: TargetConfig,
    manifest: dict[str, Any],
    run_indexes: list[int],
    resources: dict[str, Any],
    base_job_name: str,
) -> int:
    local_sweep_dir = _sweeps_dir(project_root) / str(manifest["sweep_id"])
    analysis_rel = str(manifest.get("analysis", "")).strip("/")
    sync_source = str((project_root / analysis_rel).resolve()) if analysis_rel else None
    sync_destination = str((project_root / analysis_rel).resolve()) if analysis_rel else str(project_root)
    ignore_path = _ignore_file(project_root)
    ignore_file_path = str(ignore_path) if ignore_path.exists() else None
    ignore_used = False
    profile_name = str(manifest.get("resources", {}).get("profile", "")).strip() or None
    local_sweep_dir.mkdir(parents=True, exist_ok=True)
    submitted_count = 0

    for idx in run_indexes:
        run = manifest["runs"][idx]
        run_slug = _slug_component(str(run["sweep_job"]))
        run_name = f"{base_job_name}-{run_slug}-{int(run['sweep_index']):03d}"
        local_log_file = local_sweep_dir / f"run_{run['run_id']}.log"
        submitted_at = datetime.now().isoformat(timespec="seconds")

        proc = _run_direct(
            str(run["command"]),
            shell=True,
            cwd=project_root,
            text=True,
            capture_output=True,
            check=False,
        )
        log_blob = (proc.stdout or "") + (proc.stderr or "")
        local_log_file.write_text(log_blob)

        run["job_id"] = f"local-{run['run_id']}"
        run["job_name"] = run_name
        run["status"] = "COMPLETED" if proc.returncode == 0 else "FAILED"
        run["submitted_at"] = submitted_at
        run["completed_at"] = datetime.now().isoformat(timespec="seconds")
        run["submission_mode"] = "local"
        run["resources"] = resources
        run["exit_code"] = proc.returncode

        _append_sweep_job_record(
            project_root=project_root,
            cfg=cfg,
            target_name=target_name,
            target=target,
            sweep_id=str(manifest["sweep_id"]),
            submission_mode="local",
            run=run,
            remote_sweep_dir=str(local_sweep_dir),
            remote_log_file=str(local_log_file),
            submit_script_path=None,
            sync_source=sync_source,
            sync_destination=sync_destination,
            ignore_file_path=ignore_file_path,
            ignore_used=ignore_used,
            profile_name=profile_name,
            submitted_at=submitted_at,
            scheduler_override="none",
            state_override=run["status"],
        )
        submitted_count += 1
    return submitted_count


@sweep_app.command("run", context_settings={"allow_extra_args": True, "ignore_unknown_options": True})
def sweep_run(
    ctx: typer.Context,
    config_file: Path = typer.Option(..., "--config", help="Sweep YAML config with top-level params mapping"),
    mode: str = typer.Option("single", "--mode", help="Execution mode: single|array|local"),
    sweep_job: Optional[str] = typer.Option(None, "--job", help="Run only one params.<job> block from config"),
    sweep_id: Optional[str] = typer.Option(None, "--sweep-id", help="Optional sweep id (default: auto-generated)"),
    target: Optional[str] = typer.Option(
        None, "--target", help="Target name (defaults to active target)", autocompletion=_complete_target_names
    ),
    profile: Optional[str] = typer.Option(
        None,
        "--profile",
        help="Resource profile name (built-in or user-defined)",
        autocompletion=_complete_profile_names,
    ),
    cpus: Optional[int] = typer.Option(None, help="CPU cores (defaults to target setting)"),
    memory: Optional[str] = typer.Option(None, help="Memory (defaults to target setting)"),
    job_time: Optional[str] = typer.Option(None, "--time", help="Walltime (defaults to target setting)"),
    job_name: Optional[str] = typer.Option(None, help="Base scheduler job name prefix"),
    queue: Optional[str] = typer.Option(None, help="Queue (defaults to target setting if template needs it)"),
    node: Optional[str] = typer.Option(None, help="Node (defaults to target setting)"),
    parallel_environment: Optional[str] = typer.Option(
        None, "--parallel-environment", help="Parallel environment (defaults to target setting if template needs it)"
    ),
    submit_delay_ms: int = typer.Option(
        5000,
        "--submit-delay-ms",
        min=5000,
        help="Delay between submissions in single mode (minimum 5000 ms).",
    ),
    index_after: bool = typer.Option(False, "--index-after", help="Refresh root index manifest after successful submission"),
) -> None:
    """Run a sweep and persist manifest in .cluster_dispatch/sweeps."""
    mode_lc = mode.lower()
    if mode_lc not in {"single", "array", "local"}:
        raise typer.BadParameter("--mode must be one of: single, array, local")
    if submit_delay_ms < 5000:
        raise typer.BadParameter("--submit-delay-ms must be at least 5000")
    if not ctx.args:
        raise typer.BadParameter(
            "Provide a command template, e.g. cdp analysis sweep run --config sweep.yml python train.py --lr {lr}"
        )
    if not config_file.exists() or not config_file.is_file():
        raise typer.BadParameter(f"Sweep config file not found: {config_file}")

    command_template = " ".join(shlex.quote(arg) for arg in ctx.args)
    payload = yaml.safe_load(config_file.read_text()) or {}
    if not isinstance(payload, dict):
        raise typer.BadParameter("Sweep config must be a YAML mapping with top-level key: params")

    runs = _expand_sweep_runs(payload, command_template, sweep_job)
    if not runs:
        raise typer.BadParameter("No sweep runs generated from config")

    project_root = _project_root()
    cfg = load_config(project_root)
    _require_active_analysis(cfg, project_root)
    analysis_rel = (cfg.active_analysis or "").strip("/")
    if not analysis_rel:
        raise typer.BadParameter("Active analysis path is empty. Re-run: cdp analysis use <path>")
    if target:
        if target not in cfg.targets:
            raise typer.BadParameter(f"Target '{target}' not found")
        target_name = target
        target_cfg = cfg.targets[target]
    else:
        target_name, target_cfg = _active_target(cfg)

    if mode_lc == "array" and target_cfg.scheduler.lower() == "none":
        raise typer.BadParameter("Array mode is not supported for scheduler 'none'")

    profile_values = _resolve_resource_profile(profile, cfg)
    resolved_cpus = cpus if cpus is not None else int(profile_values.get("cpus", target_cfg.default_cpus))
    resolved_memory = memory if memory is not None else str(profile_values.get("memory", target_cfg.default_memory))
    resolved_time = job_time if job_time is not None else str(profile_values.get("time", target_cfg.default_time))
    resolved_node = node or str(profile_values.get("node", target_cfg.default_node or "1"))
    resolved_queue = queue if queue is not None else str(profile_values.get("queue", target_cfg.default_queue))
    resolved_pe = (
        parallel_environment
        if parallel_environment is not None
        else str(profile_values.get("parallel_environment", target_cfg.default_parallel_environment))
    )

    if "{queue}" in target_cfg.template_header and not resolved_queue:
        raise typer.BadParameter("--queue is required (or target default_queue) because template includes {queue}.")
    if "{parallel_environment}" in target_cfg.template_header and not resolved_pe:
        raise typer.BadParameter(
            "--parallel-environment is required (or target default_parallel_environment) because template includes {parallel_environment}."
        )

    if sweep_id is None:
        sweep_id = f"sweep-{datetime.now().strftime('%Y%m%d%H%M%S')}-{secrets.token_hex(3)}"
    if _sweep_manifest_path(project_root, sweep_id).exists():
        raise typer.BadParameter(f"Sweep id already exists: {sweep_id}")

    manifest = _build_sweep_manifest(
        sweep_id=sweep_id,
        mode=mode_lc,
        target_name=target_name,
        target=target_cfg,
        analysis_rel=analysis_rel,
        config_file=config_file.resolve(),
        command_template=command_template,
        runs=runs,
    )
    _save_sweep_manifest(project_root, manifest)

    resources = {
        "profile": profile,
        "cpus": resolved_cpus,
        "memory": resolved_memory,
        "time": resolved_time,
        "node": resolved_node,
        "queue": resolved_queue or "",
        "parallel_environment": resolved_pe or "",
    }
    manifest["resources"] = resources
    manifest["submit_delay_ms"] = submit_delay_ms
    base_job_name = _slug_component(job_name or sweep_id)
    run_indexes = list(range(len(manifest["runs"])))
    if mode_lc == "single":
        submitted = _submit_sweep_single(
            project_root=project_root,
            cfg=cfg,
            target_name=target_name,
            target=target_cfg,
            manifest=manifest,
            run_indexes=run_indexes,
            resources=resources,
            base_job_name=base_job_name,
            submit_delay_ms=submit_delay_ms,
        )
    elif mode_lc == "local":
        submitted = _submit_sweep_local(
            project_root=project_root,
            cfg=cfg,
            target_name=target_name,
            target=target_cfg,
            manifest=manifest,
            run_indexes=run_indexes,
            resources=resources,
            base_job_name=base_job_name,
        )
    else:
        submitted = _submit_sweep_array(
            project_root=project_root,
            cfg=cfg,
            target_name=target_name,
            target=target_cfg,
            manifest=manifest,
            run_indexes=run_indexes,
            resources=resources,
            base_job_name=base_job_name,
        )
    _save_sweep_manifest(project_root, manifest)
    typer.echo(f"Submitted sweep_id={sweep_id} mode={mode_lc} target={target_name} runs={submitted}")
    if index_after:
        manifest_path = _auto_index_active_root(project_root, cfg, target_name, target_cfg)
        if manifest_path:
            typer.echo(f"Auto-indexed active analysis root: {manifest_path}")


@sweep_app.command("list")
def sweep_list() -> None:
    """List sweep manifests."""
    project_root = _project_root()
    cfg = load_config(project_root)
    _require_active_analysis(cfg, project_root)
    active_analysis = (cfg.active_analysis or "").strip("/")
    if not active_analysis:
        raise typer.BadParameter("Active analysis path is empty. Re-run: cdp analysis use <path>")
    sweeps_dir = _sweeps_dir(project_root)
    manifests = sorted(sweeps_dir.glob("*.json"), reverse=True)
    if not manifests:
        typer.echo("No sweeps found")
        return
    shown = 0
    for path in manifests:
        try:
            manifest = json.loads(path.read_text())
        except json.JSONDecodeError:
            continue
        if str(manifest.get("analysis", "")).strip("/") != active_analysis:
            continue
        runs = manifest.get("runs", [])
        total = len(runs) if isinstance(runs, list) else 0
        submitted = len([r for r in runs if isinstance(r, dict) and r.get("job_id")])
        typer.echo(
            f"{manifest.get('sweep_id', path.stem)}  analysis={manifest.get('analysis', '')} mode={manifest.get('mode', '')} "
            f"target={manifest.get('target', '')} runs={submitted}/{total} updated_at={manifest.get('updated_at', '')}"
        )
        shown += 1
    if shown == 0:
        typer.echo(f"No sweeps found for active analysis '{active_analysis}'")


@sweep_app.command("show")
def sweep_show(sweep_id: str = typer.Argument(..., help="Sweep id", autocompletion=_complete_sweep_ids)) -> None:
    """Show one sweep manifest."""
    project_root = _project_root()
    cfg = load_config(project_root)
    _require_active_analysis(cfg, project_root)
    active_analysis = (cfg.active_analysis or "").strip("/")
    if not active_analysis:
        raise typer.BadParameter("Active analysis path is empty. Re-run: cdp analysis use <path>")
    manifest = _load_sweep_manifest(project_root, sweep_id)
    _require_sweep_in_active_analysis(manifest, active_analysis)
    runs = manifest.get("runs", [])
    typer.echo(
        f"sweep_id={manifest.get('sweep_id')} analysis={manifest.get('analysis')} "
        f"mode={manifest.get('mode')} target={manifest.get('target')} "
        f"scheduler={manifest.get('scheduler')} runs={len(runs) if isinstance(runs, list) else 0}"
    )
    if not isinstance(runs, list):
        return
    for run in runs:
        if not isinstance(run, dict):
            continue
        typer.echo(
            f"- run_id={run.get('run_id')} job={run.get('sweep_job')} idx={run.get('sweep_index')} "
            f"job_id={run.get('job_id')} status={run.get('status')}"
        )


@sweep_app.command("resume")
def sweep_resume(sweep_id: str = typer.Argument(..., help="Sweep id", autocompletion=_complete_sweep_ids)) -> None:
    """Resume pending runs from a sweep manifest."""
    project_root = _project_root()
    cfg = load_config(project_root)
    _require_active_analysis(cfg, project_root)
    active_analysis = (cfg.active_analysis or "").strip("/")
    if not active_analysis:
        raise typer.BadParameter("Active analysis path is empty. Re-run: cdp analysis use <path>")
    manifest = _load_sweep_manifest(project_root, sweep_id)
    _require_sweep_in_active_analysis(manifest, active_analysis)
    target_name = str(manifest.get("target", ""))
    if target_name not in cfg.targets:
        raise typer.BadParameter(f"Target '{target_name}' from sweep manifest is not configured")
    target_cfg = cfg.targets[target_name]

    runs = manifest.get("runs", [])
    if not isinstance(runs, list):
        raise typer.BadParameter("Invalid sweep manifest runs format")
    pending = [idx for idx, run in enumerate(runs) if isinstance(run, dict) and not run.get("job_id")]
    if not pending:
        typer.echo(f"No pending runs for sweep_id={sweep_id}")
        return

    mode = str(manifest.get("mode", "single")).lower()
    base_job_name = _slug_component(sweep_id)
    stored_resources = manifest.get("resources", {})
    resources = {
        "cpus": stored_resources.get("cpus", target_cfg.default_cpus),
        "memory": stored_resources.get("memory", target_cfg.default_memory),
        "time": stored_resources.get("time", target_cfg.default_time),
        "node": stored_resources.get("node", target_cfg.default_node or "1"),
        "queue": stored_resources.get("queue", target_cfg.default_queue or ""),
        "parallel_environment": stored_resources.get(
            "parallel_environment", target_cfg.default_parallel_environment or ""
        ),
    }
    if mode == "single":
        submitted = _submit_sweep_single(
            project_root=project_root,
            cfg=cfg,
            target_name=target_name,
            target=target_cfg,
            manifest=manifest,
            run_indexes=pending,
            resources=resources,
            base_job_name=base_job_name,
            submit_delay_ms=max(5000, int(manifest.get("submit_delay_ms", 5000))),
        )
    elif mode == "local":
        submitted = _submit_sweep_local(
            project_root=project_root,
            cfg=cfg,
            target_name=target_name,
            target=target_cfg,
            manifest=manifest,
            run_indexes=pending,
            resources=resources,
            base_job_name=base_job_name,
        )
    elif mode == "array":
        submitted = _submit_sweep_array(
            project_root=project_root,
            cfg=cfg,
            target_name=target_name,
            target=target_cfg,
            manifest=manifest,
            run_indexes=pending,
            resources=resources,
            base_job_name=base_job_name,
        )
    else:
        raise typer.BadParameter(f"Unsupported sweep mode in manifest: {mode}")
    _save_sweep_manifest(project_root, manifest)
    typer.echo(f"Resumed sweep_id={sweep_id}; submitted={submitted}")


@sweep_app.command("cancel")
def sweep_cancel(sweep_id: str = typer.Argument(..., help="Sweep id", autocompletion=_complete_sweep_ids)) -> None:
    """Cancel submitted jobs in a sweep manifest."""
    project_root = _project_root()
    cfg = load_config(project_root)
    _require_active_analysis(cfg, project_root)
    active_analysis = (cfg.active_analysis or "").strip("/")
    if not active_analysis:
        raise typer.BadParameter("Active analysis path is empty. Re-run: cdp analysis use <path>")
    manifest = _load_sweep_manifest(project_root, sweep_id)
    _require_sweep_in_active_analysis(manifest, active_analysis)
    target_name = str(manifest.get("target", ""))
    if target_name not in cfg.targets:
        raise typer.BadParameter(f"Target '{target_name}' from sweep manifest is not configured")
    target_cfg = cfg.targets[target_name]
    scheduler = str(manifest.get("scheduler", target_cfg.scheduler))

    runs = manifest.get("runs", [])
    if not isinstance(runs, list):
        raise typer.BadParameter("Invalid sweep manifest runs format")

    cancelled = 0
    mode = str(manifest.get("mode", "single")).lower()
    if mode == "local":
        cancelled_local = 0
        for run in runs:
            if isinstance(run, dict) and run.get("status") == "PENDING":
                run["status"] = "CANCELLED"
                cancelled_local += 1
        _save_sweep_manifest(project_root, manifest)
        typer.echo(f"Cancel requested for sweep_id={sweep_id}; jobs={cancelled_local}")
        return

    if mode == "array" and manifest.get("array_job_id"):
        cmd = _cancel_command_for_scheduler(scheduler, str(manifest["array_job_id"]))
        if _uses_local_transport(target_cfg):
            proc = _run_direct(["bash", "-lc", cmd], text=True, capture_output=True, check=False)
        else:
            proc = _run_direct(["ssh", target_cfg.host, cmd], text=True, capture_output=True, check=False)
        if proc.returncode == 0:
            cancelled += 1
        else:
            raise typer.BadParameter(proc.stderr.strip() or proc.stdout.strip() or "Failed to cancel array job")
        for run in runs:
            if isinstance(run, dict) and run.get("job_id"):
                run["status"] = "CANCEL_REQUESTED"
    else:
        seen: set[str] = set()
        for run in runs:
            if not isinstance(run, dict):
                continue
            job_id = str(run.get("job_id", "")).strip()
            if not job_id or job_id in seen:
                continue
            seen.add(job_id)
            cmd = _cancel_command_for_scheduler(scheduler, job_id)
            if _uses_local_transport(target_cfg):
                proc = _run_direct(["bash", "-lc", cmd], text=True, capture_output=True, check=False)
            else:
                proc = _run_direct(["ssh", target_cfg.host, cmd], text=True, capture_output=True, check=False)
            if proc.returncode == 0:
                cancelled += 1
                run["status"] = "CANCEL_REQUESTED"
    _save_sweep_manifest(project_root, manifest)
    typer.echo(f"Cancel requested for sweep_id={sweep_id}; jobs={cancelled}")


@analysis_app.command("use")
def analysis_use(
    path: Annotated[Path, typer.Argument(help="Analysis directory under project root", autocompletion=_complete_analysis_dirs)],
) -> None:
    """Set active analysis directory."""
    project_root = _project_root()
    cfg = load_config(project_root)

    resolved = path.resolve()
    try:
        relative = resolved.relative_to(project_root)
    except ValueError as exc:
        raise typer.BadParameter("Analysis path must be within project root") from exc

    if not resolved.exists() or not resolved.is_dir():
        raise typer.BadParameter(f"Analysis directory not found: {resolved}")

    cfg.active_analysis = str(relative)
    save_config(project_root, cfg)
    typer.echo(f"Active analysis set to {cfg.active_analysis}")


@analysis_app.command("tag")
def analysis_tag(
    path: Annotated[
        Path, typer.Argument(help="Path inside active analysis to tag for pull", autocompletion=_complete_active_analysis_paths)
    ],
    remote: bool = typer.Option(
        False, "--remote", help="Tag path by validating against remote analysis directory"
    ),
) -> None:
    """Tag a path inside active analysis for future pulls."""
    project_root = _project_root()
    cfg = load_config(project_root)
    analysis_dir = _require_active_analysis(cfg, project_root)

    if path.is_absolute():
        raise typer.BadParameter("Tag path must be relative to the active analysis directory")

    candidate = (analysis_dir / path).resolve()
    try:
        relative_to_analysis = candidate.relative_to(analysis_dir)
    except ValueError as exc:
        raise typer.BadParameter("Tag path must be inside the active analysis directory") from exc

    tag_value = relative_to_analysis.as_posix()
    if not remote:
        if not candidate.exists():
            raise typer.BadParameter(f"Tag path not found locally: {candidate}")
    else:
        target_name, target = _active_target(cfg)
        if not target.remote_root:
            raise typer.BadParameter(
                f"Target '{target_name}' has no remote_root configured. Update it with `cdp target add {target_name} --remote-root ...`."
            )
        analysis_rel = (cfg.active_analysis or "").strip("/")
        if not analysis_rel:
            raise typer.BadParameter("Active analysis path is empty. Re-run: cdp analysis use <path>")
        remote_analysis_root = f"{target.remote_root.rstrip('/')}/{analysis_rel}"
        remote_tag_path = f"{remote_analysis_root}/{tag_value}"
        root_index = _load_index_manifest(project_root, target_name, analysis_rel, ".")
        if root_index is not None and _index_entry_path_exists(root_index, tag_value):
            typer.echo(f"Validated via index: {tag_value}")
        else:
            if _uses_local_transport(target):
                if not Path(remote_tag_path).exists():
                    raise typer.BadParameter(f"Tag path not found remotely: {remote_tag_path}")
            else:
                remote_cmd = f"if [ -e {shlex.quote(remote_tag_path)} ]; then echo OK; else echo MISSING; fi"
                proc = _run_direct(
                    ["ssh", target.host, remote_cmd],
                    text=True,
                    capture_output=True,
                    check=False,
                )
                if proc.returncode != 0 or proc.stdout.strip() != "OK":
                    raise typer.BadParameter(f"Tag path not found remotely: {remote_tag_path}")

    analysis_key = cfg.active_analysis or ""
    tags = cfg.analysis_tags.get(analysis_key, [])
    if tag_value not in tags:
        tags.append(tag_value)
        cfg.analysis_tags[analysis_key] = sorted(tags)
        save_config(project_root, cfg)
        mode = "remote" if remote else "local"
        typer.echo(f"Tagged analysis path ({mode}): {tag_value}")
    else:
        typer.echo(f"Already tagged: {tag_value}")


@analysis_app.command("untag")
def analysis_untag(
    path: Annotated[
        Path, typer.Argument(help="Tagged path inside active analysis to remove", autocompletion=_complete_active_analysis_paths)
    ],
) -> None:
    """Remove a tagged path from the active analysis."""
    project_root = _project_root()
    cfg = load_config(project_root)
    analysis_dir = _require_active_analysis(cfg, project_root)

    if path.is_absolute():
        raise typer.BadParameter("Tag path must be relative to the active analysis directory")

    candidate = (analysis_dir / path).resolve()
    try:
        relative_to_analysis = candidate.relative_to(analysis_dir)
    except ValueError as exc:
        raise typer.BadParameter("Tag path must be inside the active analysis directory") from exc

    tag_value = relative_to_analysis.as_posix()
    analysis_key = cfg.active_analysis or ""
    tags = cfg.analysis_tags.get(analysis_key, [])
    if tag_value not in tags:
        raise typer.BadParameter(f"Path is not tagged: {tag_value}")

    updated_tags = [tag for tag in tags if tag != tag_value]
    if updated_tags:
        cfg.analysis_tags[analysis_key] = sorted(updated_tags)
    else:
        cfg.analysis_tags.pop(analysis_key, None)
    save_config(project_root, cfg)
    typer.echo(f"Removed analysis tag: {tag_value}")


@analysis_app.command("list")
def analysis_list(
    path: str = typer.Argument(
        ".",
        help="Nested path inside active analysis directory.",
        autocompletion=_complete_active_analysis_paths,
    ),
    remote: bool = typer.Option(False, "--remote", help="List directories from remote analysis directory"),
    all_entries: bool = typer.Option(False, "--all", help="Include files (not just directories)"),
) -> None:
    """List entries from active analysis directory or one of its nested paths."""
    project_root = _project_root()
    cfg = load_config(project_root)
    analysis_dir = _require_active_analysis(cfg, project_root)
    active_analysis_rel = (cfg.active_analysis or "").strip("/")

    candidate = Path(path).expanduser()
    if candidate.is_absolute():
        raise typer.BadParameter("Path must be relative to the active analysis directory")
    local_target = (analysis_dir / candidate).resolve()
    try:
        relative_to_analysis = local_target.relative_to(analysis_dir)
    except ValueError as exc:
        raise typer.BadParameter("Path must stay inside the active analysis directory") from exc
    relative_subpath = relative_to_analysis.as_posix()

    if not remote:
        if not local_target.exists():
            raise typer.BadParameter(f"Local path not found: {local_target}")
        if not local_target.is_dir():
            raise typer.BadParameter(f"Local path is not a directory: {local_target}")

        entries = sorted([p.name for p in local_target.iterdir() if (p.is_dir() if not all_entries else True)])
        if not entries:
            if all_entries:
                typer.echo(f"No local entries found in {local_target}")
            else:
                typer.echo(f"No local subdirectories found in {local_target}")
            return
        label = "entries" if all_entries else "directories"
        typer.echo(f"Local {label} in {local_target}:")
        for name in entries:
            typer.echo(f"- {name}")
        return

    target_name, target = _active_target(cfg)
    if not target.remote_root:
        raise typer.BadParameter(
            f"Target '{target_name}' has no remote_root configured. Update it with `cdp target add {target_name} --remote-root ...`."
        )
    if not active_analysis_rel:
        raise typer.BadParameter("Active analysis path is empty. Re-run: cdp analysis use <path>")
    remote_analysis_root = f"{target.remote_root.rstrip('/')}/{active_analysis_rel}"
    remote_list_root = (
        remote_analysis_root
        if relative_subpath in ("", ".")
        else f"{remote_analysis_root.rstrip('/')}/{relative_subpath}"
    )
    manifest_scope = "." if relative_subpath in ("", ".") else relative_subpath.strip("/")
    indexed_manifest = _load_index_manifest(
        project_root=project_root,
        target_name=target_name,
        analysis_rel=active_analysis_rel,
        scope_rel=manifest_scope,
    )
    if indexed_manifest is not None:
        indexed_names = _list_names_from_index_manifest(indexed_manifest, include_files=all_entries)
        if not indexed_names:
            if all_entries:
                typer.echo(f"No indexed entries found in {remote_list_root}")
            else:
                typer.echo(f"No indexed subdirectories found in {remote_list_root}")
            typer.echo(f"Using index manifest: {indexed_manifest.get('_manifest_path', '<unknown>')}")
            return
        label = "entries" if all_entries else "directories"
        typer.echo(f"Remote {label} in {remote_list_root} (from index):")
        for name in indexed_names:
            typer.echo(f"- {name}")
        typer.echo(f"Index manifest: {indexed_manifest.get('_manifest_path', '<unknown>')}")
        return

    remote_cmd = (
        f"P={shlex.quote(remote_list_root)}; "
        'if [ -d "$P" ]; then '
        + ('for d in "$P"/*; do basename "$d"; done | sort; ' if all_entries else 'for d in "$P"/*; do [ -d "$d" ] && basename "$d"; done | sort; ')
        + "fi"
    )
    if _uses_local_transport(target):
        proc = _run_direct(
            ["bash", "-lc", remote_cmd],
            text=True,
            capture_output=True,
            check=False,
        )
    else:
        proc = _run_direct(
            ["ssh", target.host, remote_cmd],
            text=True,
            capture_output=True,
            check=False,
        )
    if proc.returncode != 0:
        raise typer.BadParameter(proc.stderr.strip() or "Failed to list remote directories")

    lines = [line.strip() for line in proc.stdout.splitlines() if line.strip()]
    if not lines:
        if all_entries:
            typer.echo(f"No remote entries found in {remote_list_root}")
        else:
            typer.echo(f"No remote subdirectories found in {remote_list_root}")
        return
    label = "entries" if all_entries else "directories"
    typer.echo(f"Remote {label} in {remote_list_root}:")
    for name in lines:
        typer.echo(f"- {name}")


@analysis_app.command("run", context_settings={"allow_extra_args": True, "ignore_unknown_options": True})
def run(
    ctx: typer.Context,
    dry_run: bool = typer.Option(
        False,
        "--dry-run",
        help="Validate configuration and print planned execution without syncing/submitting/writing state",
    ),
    profile: Optional[str] = typer.Option(
        None,
        "--profile",
        help="Resource profile name (built-in or user-defined)",
        autocompletion=_complete_profile_names,
    ),
    cpus: Optional[int] = typer.Option(None, help="CPU cores for scheduler template (defaults to target setting)"),
    memory: Optional[str] = typer.Option(
        None, help="Memory for scheduler template, e.g. 8G (defaults to target setting)"
    ),
    job_time: Optional[str] = typer.Option(
        None, "--time", help="Walltime for scheduler template, e.g. 02:00:00 (defaults to target setting)"
    ),
    job_name: Optional[str] = typer.Option(
        None, help="Scheduler job name. If omitted, a random name is generated."
    ),
    queue: Optional[str] = typer.Option(
        None, help="Queue for scheduler template (defaults to target setting; required if template uses {queue})"
    ),
    node: Optional[str] = typer.Option(None, help="Node for scheduler template (defaults to target setting)"),
    parallel_environment: Optional[str] = typer.Option(
        None,
        help=(
            "Parallel environment for scheduler template "
            "(defaults to target setting; required if template uses {parallel_environment})"
        ),
    ),
    index_after: bool = typer.Option(False, "--index-after", help="Refresh root index manifest after successful submission"),
) -> None:
    """Sync active analysis, submit command, and store job metadata."""
    if not ctx.args:
        raise typer.BadParameter("Provide a command to execute, e.g. cdp analysis run python main.py")

    command = " ".join(shlex.quote(arg) for arg in ctx.args)

    project_root = _project_root()
    cfg = load_config(project_root)
    analysis_dir = _require_active_analysis(cfg, project_root)
    target_name, target = _active_target(cfg)
    if not target.remote_root:
        raise typer.BadParameter(
            f"Target '{target_name}' has no remote_root configured. Update it with `cdp target add {target_name} --remote-root ...`."
        )
    profile_values = _resolve_resource_profile(profile, cfg)
    resolved_cpus = cpus if cpus is not None else int(profile_values.get("cpus", target.default_cpus))
    resolved_memory = memory if memory is not None else str(profile_values.get("memory", target.default_memory))
    resolved_time = job_time if job_time is not None else str(profile_values.get("time", target.default_time))
    resolved_job_name = job_name or f"cdp-{secrets.token_hex(4)}"
    resolved_node = node or str(profile_values.get("node", target.default_node or "1"))
    resolved_queue = queue if queue is not None else str(profile_values.get("queue", target.default_queue))
    resolved_parallel_environment = (
        parallel_environment
        if parallel_environment is not None
        else str(profile_values.get("parallel_environment", target.default_parallel_environment))
    )
    analysis_rel = (cfg.active_analysis or "").strip("/")
    _submit_or_preview_analysis_run(
        project_root=project_root,
        cfg=cfg,
        analysis_dir=analysis_dir,
        analysis_rel=analysis_rel,
        target_name=target_name,
        target=target,
        command=command,
        profile_name=profile,
        cpus=resolved_cpus,
        memory=resolved_memory,
        walltime=resolved_time,
        job_name=resolved_job_name,
        queue=resolved_queue,
        node=resolved_node,
        parallel_environment=resolved_parallel_environment,
        dry_run=dry_run,
        index_after=index_after,
    )


def _show_last_status(follow: bool = False) -> None:
    """Show status of last submitted job."""
    project_root = _project_root()
    state = load_state(project_root)
    last = state.get("last_job")
    if not last:
        raise typer.BadParameter("No active job in state. Run `cdp analysis run ...` first.")

    job_id = last["job_id"]
    target = last["target"]

    cfg = load_config(project_root)
    if target not in cfg.targets:
        raise typer.BadParameter(f"Target '{target}' from state is no longer configured")

    target_cfg = cfg.targets[target]

    state = "UNKNOWN"
    jobs_dir = project_root / CONFIG_DIR / JOBS_DIR
    if jobs_dir.exists():
        for job_file in sorted(jobs_dir.glob("*.json"), reverse=True):
            try:
                payload = json.loads(job_file.read_text())
            except json.JSONDecodeError:
                continue
            if str(payload.get("job_id", "")) == str(job_id) and str(payload.get("target", "")) == str(target):
                state = _resolve_and_persist_job_state(
                    cfg=cfg,
                    job_file=job_file,
                    payload=payload,
                    job_id=job_id,
                    target_name=target,
                    scheduler=str(last["scheduler"]),
                )
                break
    if state == "UNKNOWN":
        adapter = get_adapter(last["scheduler"])
        result = adapter.status(target_cfg.host, job_id, transport=target_cfg.transport)
        state = result.state
    typer.echo(f"job_id={job_id} target={target} state={state}")

    if follow:
        typer.echo(f"Following log: {last['remote_log_file']}")
        if _uses_local_transport(target_cfg):
            _show_local_log(Path(str(last["remote_log_file"])), follow=True, head=None, tail=50)
        else:
            _run_direct(
                ["ssh", "-t", target_cfg.host, "bash", "-lc", f"tail -n 50 -f {shlex.quote(last['remote_log_file'])}"],
                check=True,
            )
    else:
        if _uses_local_transport(target_cfg):
            log_path = Path(str(last["remote_log_file"]))
            if log_path.exists():
                typer.echo("--- log tail ---")
                _show_local_log(log_path, follow=False, head=None, tail=20)
        else:
            proc = _run_direct(
                [
                    "ssh",
                    target_cfg.host,
                    "bash",
                    "-lc",
                    f"if [ -f {shlex.quote(last['remote_log_file'])} ]; then tail -n 20 {shlex.quote(last['remote_log_file'])}; fi",
                ],
                text=True,
                capture_output=True,
                check=False,
            )
            if proc.stdout.strip():
                typer.echo("--- log tail ---")
                typer.echo(proc.stdout.rstrip())


def _is_running_state(state: str) -> bool:
    normalized = state.strip().upper()
    return normalized in {
        "RUNNING",
        "RUNNING_OR_QUEUED",
        "QUEUED",
        "PENDING",
        "R",
        "Q",
    }


def _is_terminal_state(state: str) -> bool:
    normalized = state.strip().upper()
    if not normalized:
        return False
    if _is_running_state(normalized):
        return False
    return normalized not in {"UNKNOWN", "NOT_FOUND"}


def _output_check_from_index(project_root: Path, payload: dict[str, Any]) -> str:
    analysis_name = str(payload.get("analysis", "")).strip("/")
    target_name = str(payload.get("target", "")).strip()
    if not analysis_name or not target_name:
        return "N/A"
    tags = payload.get("analysis_tags")
    if not isinstance(tags, list) or not tags:
        return "NO_TAGS"
    root_manifest = _load_index_manifest(project_root, target_name, analysis_name, ".")
    if root_manifest is None:
        return "INDEX_MISSING"
    clean_tags = [str(tag).strip("/") for tag in tags if str(tag).strip("/")]
    if not clean_tags:
        return "NO_TAGS"
    present = len([tag for tag in clean_tags if _index_entry_path_exists(root_manifest, tag)])
    if present == len(clean_tags):
        return "OK"
    if present == 0:
        return "MISSING"
    return f"PARTIAL {present}/{len(clean_tags)}"


def _resolve_and_persist_job_state(
    cfg: ProjectConfig,
    job_file: Path,
    payload: dict[str, str],
    job_id: str,
    target_name: str,
    scheduler: str,
) -> str:
    record_state = str(payload.get("state", "")).strip()

    if target_name not in cfg.targets:
        if record_state != "TARGET_MISSING":
            payload["state"] = "TARGET_MISSING"
            job_file.write_text(json.dumps(payload, indent=2))
        return "TARGET_MISSING"

    # Record-first behavior: only poll live queue when state is marked as running.
    if record_state and not _is_running_state(record_state):
        return record_state

    adapter = get_adapter(scheduler)
    target_cfg = cfg.targets[target_name]
    status_result = adapter.status(target_cfg.host, job_id, transport=target_cfg.transport)
    state = status_result.state
    if state == "NOT_FOUND":
        stored_final_state = str(payload.get("final_state", "")).strip()
        if stored_final_state:
            state = stored_final_state
        else:
            accounting_result = adapter.accounting_status(target_cfg.host, job_id, transport=target_cfg.transport)
            if accounting_result.state != "NOT_FOUND":
                state = accounting_result.state
                payload["final_state"] = accounting_result.state
                payload["final_state_source"] = "accounting"
                payload["final_state_at"] = datetime.now().isoformat(timespec="seconds")
                payload["final_state_raw"] = accounting_result.raw

    if state and state != record_state:
        payload["state"] = state
        job_file.write_text(json.dumps(payload, indent=2))
    return state


def _load_job_records(project_root: Path) -> list[dict[str, str]]:
    jobs_dir = project_root / CONFIG_DIR / JOBS_DIR
    if not jobs_dir.exists():
        return []

    records: list[dict[str, str]] = []
    for job_file in sorted(jobs_dir.glob("*.json"), reverse=True):
        try:
            payload = json.loads(job_file.read_text())
        except json.JSONDecodeError:
            continue
        payload["_record_file"] = str(job_file)
        records.append(payload)
    return records


def _resolve_log_job(
    project_root: Path,
    cfg: ProjectConfig,
    target: Optional[str],
    job_id: Optional[str],
    job_name: Optional[str],
    analysis: Optional[str],
) -> dict[str, str]:
    records = _load_job_records(project_root)

    def _matches(rec: dict[str, str]) -> bool:
        if target and str(rec.get("target", "")) != target:
            return False
        if job_id and str(rec.get("job_id", "")) != job_id:
            return False
        if job_name and str(rec.get("job_name", "")) != job_name:
            return False
        if analysis and str(rec.get("analysis", "")) != analysis:
            return False
        return True

    has_filters = any([target, job_id, job_name, analysis])
    if has_filters:
        matched = [rec for rec in records if _matches(rec)]
        if not matched:
            raise typer.BadParameter("No matching job records found for the provided filters")
        if len(matched) > 1:
            typer.echo(f"Multiple matches found ({len(matched)}). Showing most recent match.")
        return matched[0]

    state = load_state(project_root)
    last = state.get("last_job")
    if last:
        for rec in records:
            if str(rec.get("job_id", "")) == str(last.get("job_id", "")) and str(rec.get("target", "")) == str(
                last.get("target", "")
            ):
                return rec
        return {
            "job_id": str(last.get("job_id", "")),
            "job_name": "",
            "target": str(last.get("target", "")),
            "scheduler": str(last.get("scheduler", "")),
            "remote_log_file": str(last.get("remote_log_file", "")),
            "analysis": "",
        }

    if not records:
        raise typer.BadParameter("No job records found. Run `cdp analysis run ...` first.")
    return records[0]


def _show_local_log(log_file: Path, follow: bool, head: Optional[int], tail: Optional[int]) -> None:
    if not log_file.exists() or not log_file.is_file():
        raise typer.BadParameter(f"Local log file not found: {log_file}")

    line_count = tail if tail is not None else 50
    if follow:
        _run_direct(["tail", "-n", str(line_count), "-f", str(log_file)], check=True)
        return

    lines = log_file.read_text(errors="replace").splitlines()
    if head is not None:
        selected = lines[:head]
    else:
        selected = lines[-line_count:]
    if not selected:
        typer.echo(f"No log output found at {log_file}")
        return
    typer.echo("\n".join(selected))


def _fetch_log_tail(selected: dict[str, Any], cfg: ProjectConfig, project_root: Path, tail_lines: int) -> str:
    selected_target = str(selected.get("target", ""))
    remote_log_file = str(selected.get("remote_log_file", "")).strip()
    if not remote_log_file:
        return ""

    submission_mode = str(selected.get("submission_mode", "")).strip().lower()
    if submission_mode == "local" or (selected_target in cfg.targets and _uses_local_transport(cfg.targets[selected_target])):
        local_log_path = Path(remote_log_file).expanduser()
        if not local_log_path.is_absolute():
            local_log_path = (project_root / local_log_path).resolve()
        if not local_log_path.exists():
            return ""
        lines = local_log_path.read_text(errors="replace").splitlines()
        if not lines:
            return ""
        return "\n".join(lines[-tail_lines:])

    if selected_target not in cfg.targets:
        return ""
    host = cfg.targets[selected_target].host
    proc = _run_direct(
        ["ssh", host, f"if [ -f {shlex.quote(remote_log_file)} ]; then tail -n {tail_lines} {shlex.quote(remote_log_file)}; fi"],
        text=True,
        capture_output=True,
        check=False,
    )
    if proc.returncode != 0:
        return ""
    return proc.stdout.rstrip()


def _is_failed_state(state: str) -> bool:
    normalized = state.strip().upper()
    if not normalized:
        return False
    failure_tokens = ("FAIL", "ERROR", "CANCEL", "TIMEOUT", "OOM", "NODE_FAIL", "PREEMPT")
    if any(token in normalized for token in failure_tokens):
        return True
    return normalized in {"EXIT", "FAILED"}


def _cancel_command_for_scheduler(scheduler: str, job_id: str) -> str:
    scheduler_lc = scheduler.lower()
    quoted_job_id = shlex.quote(job_id)
    if scheduler_lc in {"sge", "univa", "pbs"}:
        return f"qdel {quoted_job_id}"
    if scheduler_lc == "slurm":
        return f"scancel {quoted_job_id}"
    if scheduler_lc == "lsf":
        return f"bkill {quoted_job_id}"
    if scheduler_lc == "none":
        return f"kill {quoted_job_id}"
    raise typer.BadParameter(f"Unsupported scheduler for cancel: {scheduler}")


@app.command("cancel")
def cancel(
    job_id: Optional[str] = typer.Option(None, "--job-id", help="Cancel by exact job id", autocompletion=_complete_job_ids),
    job_name: Optional[str] = typer.Option(
        None, "--job-name", help="Cancel by exact job name", autocompletion=_complete_job_names
    ),
    target: Optional[str] = typer.Option(
        None, "--target", help="Restrict cancel to a specific target", autocompletion=_complete_target_names
    ),
    analysis: Optional[str] = typer.Option(
        None, "--analysis", help="Restrict cancel to a specific analysis path", autocompletion=_complete_record_analyses
    ),
    yes: bool = typer.Option(False, "--yes", help="Confirm cancelling multiple matching jobs"),
) -> None:
    """Cancel jobs by job-id or job-name using recorded launch metadata."""
    if not job_id and not job_name:
        raise typer.BadParameter("Provide at least one selector: --job-id or --job-name")

    project_root = _project_root()
    cfg = load_config(project_root)
    if target and target not in cfg.targets:
        raise typer.BadParameter(f"Target '{target}' not found")

    records = _load_job_records(project_root)
    if not records:
        raise typer.BadParameter("No job records found. Nothing to cancel.")

    def _matches(rec: dict[str, str]) -> bool:
        if job_id and str(rec.get("job_id", "")) != job_id:
            return False
        if job_name and str(rec.get("job_name", "")) != job_name:
            return False
        if target and str(rec.get("target", "")) != target:
            return False
        if analysis and str(rec.get("analysis", "")) != analysis:
            return False
        return True

    matches = [rec for rec in records if _matches(rec)]
    if not matches:
        raise typer.BadParameter("No matching job records found for the provided filters")

    # Keep most-recent-first order but avoid repeated cancellation for duplicated records.
    seen: set[tuple[str, str, str]] = set()
    unique_matches: list[dict[str, str]] = []
    for rec in matches:
        key = (
            str(rec.get("target", "")),
            str(rec.get("scheduler", "")),
            str(rec.get("job_id", "")),
        )
        if key in seen:
            continue
        seen.add(key)
        unique_matches.append(rec)
    if len(unique_matches) > 1:
        _require_yes(
            yes,
            f"Filters match {len(unique_matches)} jobs. Re-run with --yes to cancel all matches.",
        )

    cancelled: list[str] = []
    failed: list[str] = []
    for rec in unique_matches:
        rec_target = str(rec.get("target", ""))
        rec_scheduler = str(rec.get("scheduler", ""))
        rec_job_id = str(rec.get("job_id", ""))
        rec_job_name = str(rec.get("job_name", ""))
        if not rec_target or not rec_job_id:
            failed.append(f"job_id={rec_job_id or '<missing>'} target={rec_target or '<missing>'} reason=missing_metadata")
            continue
        if rec_target not in cfg.targets:
            failed.append(f"job_id={rec_job_id} target={rec_target} reason=target_not_configured")
            continue

        cancel_cmd = _cancel_command_for_scheduler(rec_scheduler, rec_job_id)
        target_cfg = cfg.targets[rec_target]
        if _uses_local_transport(target_cfg):
            proc = _run_direct(
                ["bash", "-lc", cancel_cmd],
                text=True,
                capture_output=True,
                check=False,
            )
        else:
            proc = _run_direct(
                ["ssh", target_cfg.host, cancel_cmd],
                text=True,
                capture_output=True,
                check=False,
            )
        label = f"job_id={rec_job_id} job_name={rec_job_name} target={rec_target}"
        if proc.returncode == 0:
            cancelled.append(label)
        else:
            err = (proc.stderr or proc.stdout or "unknown_error").strip().replace("\n", " ")
            failed.append(f"{label} reason={err}")

    if cancelled:
        typer.echo("Cancelled jobs:")
        for line in cancelled:
            typer.echo(f"- {line}")
    if failed:
        typer.echo("Failed to cancel:")
        for line in failed:
            typer.echo(f"- {line}")

@app.command("logs")
def logs(
    follow: Annotated[bool, typer.Option(help="Follow log output (tail -f)")] = False,
    head: Optional[int] = typer.Option(None, "--head", min=1, help="Show first N lines"),
    tail: Optional[int] = typer.Option(None, "--tail", min=1, help="Show last N lines (default: 50)"),
    target: Optional[str] = typer.Option(
        None, "--target", help="Filter by target name", autocompletion=_complete_target_names
    ),
    job_id: Optional[str] = typer.Option(None, "--job-id", help="Filter by exact job id", autocompletion=_complete_job_ids),
    job_name: Optional[str] = typer.Option(
        None, "--job-name", help="Filter by exact job name", autocompletion=_complete_job_names
    ),
    analysis: Optional[str] = typer.Option(
        None, "--analysis", help="Filter by exact analysis path", autocompletion=_complete_record_analyses
    ),
) -> None:
    """Show logs for the selected job (remote by default; local for local sweep mode)."""
    if head is not None and tail is not None:
        raise typer.BadParameter("Use only one of --head or --tail")
    if follow and head is not None:
        raise typer.BadParameter("--follow cannot be combined with --head")

    project_root = _project_root()
    cfg = load_config(project_root)
    if target and target not in cfg.targets:
        raise typer.BadParameter(f"Target '{target}' not found")

    selected = _resolve_log_job(
        project_root=project_root,
        cfg=cfg,
        target=target,
        job_id=job_id,
        job_name=job_name,
        analysis=analysis,
    )
    submission_mode = str(selected.get("submission_mode", "")).strip().lower()
    selected_target = str(selected.get("target", ""))
    remote_log_file = str(selected.get("remote_log_file", "")).strip()
    if not remote_log_file:
        raise typer.BadParameter("Selected job does not have a remote log file")

    if submission_mode == "local":
        local_log_path = Path(remote_log_file).expanduser()
        if not local_log_path.is_absolute():
            local_log_path = (project_root / local_log_path).resolve()
        if follow:
            typer.echo(
                f"Following local log for job_id={selected.get('job_id', '')} "
                f"job_name={selected.get('job_name', '')}: {local_log_path}"
            )
        _show_local_log(local_log_path, follow=follow, head=head, tail=tail)
        return

    if selected_target not in cfg.targets:
        raise typer.BadParameter(f"Target '{selected_target}' from selected job is not configured")
    if _uses_local_transport(cfg.targets[selected_target]):
        local_log_path = Path(remote_log_file).expanduser()
        if not local_log_path.is_absolute():
            local_log_path = (project_root / local_log_path).resolve()
        if follow:
            typer.echo(
                f"Following local log for job_id={selected.get('job_id', '')} "
                f"job_name={selected.get('job_name', '')}: {local_log_path}"
            )
        _show_local_log(local_log_path, follow=follow, head=head, tail=tail)
        return

    line_count = tail if tail is not None else 50
    if head is not None:
        remote_cmd = f"if [ -f {shlex.quote(remote_log_file)} ]; then head -n {head} {shlex.quote(remote_log_file)}; fi"
    elif follow:
        remote_cmd = f"tail -n {line_count} -f {shlex.quote(remote_log_file)}"
    else:
        remote_cmd = (
            f"if [ -f {shlex.quote(remote_log_file)} ]; then tail -n {line_count} {shlex.quote(remote_log_file)}; fi"
        )

    host = cfg.targets[selected_target].host
    if follow:
        typer.echo(
            f"Following log for job_id={selected.get('job_id', '')} "
            f"job_name={selected.get('job_name', '')} target={selected_target}: {remote_log_file}"
        )
        _run_direct(["ssh", "-t", host, remote_cmd], check=True)
        return

    proc = _run_direct(["ssh", host, remote_cmd], text=True, capture_output=True, check=False)
    if proc.returncode != 0:
        raise typer.BadParameter(proc.stderr.strip() or "Failed to fetch remote log")

    if not proc.stdout.strip():
        typer.echo(f"No log output found at {remote_log_file}")
        return
    typer.echo(proc.stdout.rstrip())


@app.command("watch")
def watch(
    interval: int = typer.Option(15, "--interval", min=1, help="Polling interval in seconds"),
    max_polls: int = typer.Option(0, "--max-polls", min=0, help="Maximum polls before timeout (0 = unlimited)"),
    show_log_tail: bool = typer.Option(True, "--log-tail/--no-log-tail", help="Show log tail on each poll"),
    tail_lines: int = typer.Option(20, "--tail-lines", min=1, help="Number of log lines to display"),
    target: Optional[str] = typer.Option(
        None, "--target", help="Filter by target name", autocompletion=_complete_target_names
    ),
    job_id: Optional[str] = typer.Option(None, "--job-id", help="Filter by exact job id", autocompletion=_complete_job_ids),
    job_name: Optional[str] = typer.Option(
        None, "--job-name", help="Filter by exact job name", autocompletion=_complete_job_names
    ),
    analysis: Optional[str] = typer.Option(
        None, "--analysis", help="Filter by exact analysis path", autocompletion=_complete_record_analyses
    ),
) -> None:
    """Poll status (and optional log tail) until selected job reaches a terminal state."""
    project_root = _project_root()
    cfg = load_config(project_root)
    if target and target not in cfg.targets:
        raise typer.BadParameter(f"Target '{target}' not found")

    selected = _resolve_log_job(
        project_root=project_root,
        cfg=cfg,
        target=target,
        job_id=job_id,
        job_name=job_name,
        analysis=analysis,
    )

    selected_target = str(selected.get("target", "")).strip()
    selected_scheduler = str(selected.get("scheduler", "")).strip()
    selected_job_id = str(selected.get("job_id", "")).strip()
    if not selected_target or not selected_scheduler or not selected_job_id:
        raise typer.BadParameter("Selected job record is missing target/scheduler/job_id metadata")
    if selected_target not in cfg.targets:
        raise typer.BadParameter(f"Target '{selected_target}' from selected job is not configured")
    target_cfg = cfg.targets[selected_target]

    record_file = Path(str(selected.get("_record_file", ""))) if selected.get("_record_file") else None
    polls = 0
    while True:
        polls += 1
        current_state = ""
        if record_file and record_file.exists():
            try:
                payload = json.loads(record_file.read_text())
            except json.JSONDecodeError:
                payload = dict(selected)
            current_state = _resolve_and_persist_job_state(
                cfg=cfg,
                job_file=record_file,
                payload=payload,
                job_id=selected_job_id,
                target_name=selected_target,
                scheduler=selected_scheduler,
            )
            selected = payload
        else:
            adapter = get_adapter(selected_scheduler)
            current_state = adapter.status(target_cfg.host, selected_job_id, transport=target_cfg.transport).state

        now = datetime.now().isoformat(timespec="seconds")
        typer.echo(
            f"[{now}] job_id={selected_job_id} job_name={selected.get('job_name', '')} "
            f"target={selected_target} state={current_state}"
        )

        if show_log_tail:
            tail_blob = _fetch_log_tail(selected, cfg, project_root, tail_lines=tail_lines)
            if tail_blob:
                typer.echo("--- log tail ---")
                typer.echo(tail_blob)

        if not _is_running_state(current_state):
            if _is_failed_state(current_state):
                raise typer.Exit(code=1)
            return

        if max_polls and polls >= max_polls:
            typer.echo("Watch timed out before reaching a terminal state")
            raise typer.Exit(code=1)
        time.sleep(interval)


@app.command("history")
def history(
    limit: int = typer.Option(50, "--limit", min=1, help="Maximum number of records to display"),
    target: Optional[str] = typer.Option(
        None, "--target", help="Filter by exact target name", autocompletion=_complete_target_names
    ),
    analysis: Optional[str] = typer.Option(
        None, "--analysis", help="Filter by exact analysis path", autocompletion=_complete_record_analyses
    ),
    job_id: Optional[str] = typer.Option(None, "--job-id", help="Filter by exact job id", autocompletion=_complete_job_ids),
    job_name: Optional[str] = typer.Option(
        None, "--job-name", help="Filter by exact job name", autocompletion=_complete_job_names
    ),
) -> None:
    """Show remembered launch records without querying scheduler status."""
    project_root = _project_root()
    records = _load_job_records(project_root)
    if not records:
        typer.echo("No job history found")
        return

    def _matches(rec: dict[str, str]) -> bool:
        if target and str(rec.get("target", "")) != target:
            return False
        if analysis and str(rec.get("analysis", "")) != analysis:
            return False
        if job_id and str(rec.get("job_id", "")) != job_id:
            return False
        if job_name and str(rec.get("job_name", "")) != job_name:
            return False
        return True

    matched = [rec for rec in records if _matches(rec)]
    if not matched:
        typer.echo("No history records found for the provided filters")
        return

    typer.echo("Job history:")
    for rec in matched[:limit]:
        typer.echo(
            f"{str(rec.get('submitted_at', ''))}  job_id={str(rec.get('job_id', ''))} "
            f"job_name={str(rec.get('job_name', ''))} target={str(rec.get('target', ''))} "
            f"scheduler={str(rec.get('scheduler', ''))} analysis={str(rec.get('analysis', ''))}"
        )


@app.command("report")
def report(
    target: Optional[str] = typer.Option(
        None, "--target", help="Filter by target name", autocompletion=_complete_target_names
    ),
    analysis: Optional[str] = typer.Option(
        None, "--analysis", help="Filter by analysis path", autocompletion=_complete_record_analyses
    ),
    days: Optional[int] = typer.Option(None, "--days", min=0, help="Include only records from last N days"),
    as_json: bool = typer.Option(False, "--json", help="Render report as JSON"),
) -> None:
    """Summarize recent run and sync activity from local records."""
    project_root = _project_root()
    job_records = _load_job_records(project_root)
    sync_events = _load_sync_events(project_root)
    index_manifests = _load_all_index_manifests(project_root)
    cutoff = datetime.now() - timedelta(days=days) if days is not None else None

    def _job_match(rec: dict[str, Any]) -> bool:
        if target and str(rec.get("target", "")) != target:
            return False
        if analysis and str(rec.get("analysis", "")) != analysis:
            return False
        if cutoff is not None:
            ts = _parse_iso_ts(rec.get("submitted_at"))
            if ts is None or ts < cutoff:
                return False
        return True

    def _sync_match(rec: dict[str, Any]) -> bool:
        if target and str(rec.get("target", "")) != target:
            return False
        if analysis and str(rec.get("analysis", "")) != analysis:
            return False
        if cutoff is not None:
            ts = _parse_iso_ts(rec.get("recorded_at"))
            if ts is None or ts < cutoff:
                return False
        return True

    jobs = [rec for rec in job_records if _job_match(rec)]
    syncs = [rec for rec in sync_events if _sync_match(rec)]
    index_filtered: list[dict[str, Any]] = []
    for rec in index_manifests:
        if target and str(rec.get("target", "")) != target:
            continue
        if analysis and str(rec.get("analysis", "")).strip("/") != analysis.strip("/"):
            continue
        if cutoff is not None:
            ts = _parse_iso_ts(rec.get("indexed_at"))
            if ts is None or ts < cutoff:
                continue
        index_filtered.append(rec)

    by_state: dict[str, int] = {}
    by_scheduler: dict[str, int] = {}
    by_target: dict[str, int] = {}
    for rec in jobs:
        state = str(rec.get("state", "UNKNOWN")) or "UNKNOWN"
        scheduler = str(rec.get("scheduler", "UNKNOWN")) or "UNKNOWN"
        tname = str(rec.get("target", "UNKNOWN")) or "UNKNOWN"
        by_state[state] = by_state.get(state, 0) + 1
        by_scheduler[scheduler] = by_scheduler.get(scheduler, 0) + 1
        by_target[tname] = by_target.get(tname, 0) + 1

    failures = []
    for rec in jobs:
        state = str(rec.get("state", "")).upper()
        if "FAIL" in state or state in {"EXIT", "FAILED"}:
            failures.append(
                {
                    "submitted_at": rec.get("submitted_at"),
                    "job_id": rec.get("job_id"),
                    "job_name": rec.get("job_name"),
                    "target": rec.get("target"),
                    "analysis": rec.get("analysis"),
                    "state": rec.get("state"),
                }
            )
    failures = failures[:10]

    sync_by_action: dict[str, int] = {}
    for event in syncs:
        action = str(event.get("action", "unknown")) or "unknown"
        sync_by_action[action] = sync_by_action.get(action, 0) + 1

    memory_top: dict[str, int] = {}
    walltime_top: dict[str, int] = {}
    cpus_values: list[int] = []
    for rec in jobs:
        resources = rec.get("resources", {})
        cpus_raw = rec.get("cpus")
        memory_raw = rec.get("memory")
        time_raw = rec.get("time")
        if isinstance(resources, dict):
            cpus_raw = resources.get("cpus", cpus_raw)
            memory_raw = resources.get("memory", memory_raw)
            time_raw = resources.get("time", time_raw)
        try:
            if cpus_raw is not None:
                cpus_values.append(int(cpus_raw))
        except Exception:
            pass
        if memory_raw:
            key = str(memory_raw)
            memory_top[key] = memory_top.get(key, 0) + 1
        if time_raw:
            key = str(time_raw)
            walltime_top[key] = walltime_top.get(key, 0) + 1

    avg_cpus = round(sum(cpus_values) / len(cpus_values), 2) if cpus_values else None
    top_memory = sorted(memory_top.items(), key=lambda x: (-x[1], x[0]))[:5]
    top_walltime = sorted(walltime_top.items(), key=lambda x: (-x[1], x[0]))[:5]
    index_entries = 0
    index_files = 0
    index_bytes = 0
    index_by_scope: dict[str, int] = {}
    index_last_at: Optional[str] = None
    index_stale_count = 0
    stale_cutoff = datetime.now() - timedelta(hours=24)
    for manifest in index_filtered:
        scope = str(manifest.get("scope", "."))
        index_by_scope[scope] = index_by_scope.get(scope, 0) + 1
        entries = manifest.get("entries", [])
        if isinstance(entries, list):
            index_entries += len(entries)
            for entry in entries:
                if not isinstance(entry, dict):
                    continue
                if str(entry.get("type", "")).lower() == "file":
                    index_files += 1
                    try:
                        index_bytes += int(entry.get("size", 0))
                    except Exception:
                        pass
        ts = _parse_iso_ts(manifest.get("indexed_at"))
        if ts is not None:
            if index_last_at is None or ts > datetime.fromisoformat(index_last_at):
                index_last_at = ts.isoformat(timespec="seconds")
            if ts < stale_cutoff:
                index_stale_count += 1

    payload = {
        "filters": {"target": target, "analysis": analysis, "days": days},
        "jobs_total": len(jobs),
        "sync_total": len(syncs),
        "index_total": len(index_filtered),
        "jobs_by_state": by_state,
        "jobs_by_scheduler": by_scheduler,
        "jobs_by_target": by_target,
        "recent_failures": failures,
        "sync_by_action": sync_by_action,
        "index": {
            "manifests": len(index_filtered),
            "latest_indexed_at": index_last_at,
            "stale_manifests_24h": index_stale_count,
            "entries_total": index_entries,
            "files_total": index_files,
            "bytes_total": index_bytes,
            "bytes_human": _humanize_bytes(index_bytes),
            "by_scope": index_by_scope,
        },
        "resources": {
            "avg_cpus": avg_cpus,
            "top_memory": [{"value": k, "count": v} for k, v in top_memory],
            "top_walltime": [{"value": k, "count": v} for k, v in top_walltime],
        },
    }

    if as_json:
        typer.echo(json.dumps(payload, indent=2))
        return

    typer.echo("Run report:")
    typer.echo(f"Filters: target={target or '*'} analysis={analysis or '*'} days={days if days is not None else '*'}")
    typer.echo(f"Jobs: {len(jobs)}")
    typer.echo(f"Sync events: {len(syncs)}")
    typer.echo(
        "Index manifests: "
        f"{len(index_filtered)} latest={index_last_at or 'n/a'} "
        f"entries={index_entries} files={index_files} size={_humanize_bytes(index_bytes)} stale_24h={index_stale_count}"
    )
    if by_state:
        typer.echo("Jobs by state:")
        for key in sorted(by_state.keys()):
            typer.echo(f"- {key}: {by_state[key]}")
    if by_scheduler:
        typer.echo("Jobs by scheduler:")
        for key in sorted(by_scheduler.keys()):
            typer.echo(f"- {key}: {by_scheduler[key]}")
    if sync_by_action:
        typer.echo("Sync by action:")
        for key in sorted(sync_by_action.keys()):
            typer.echo(f"- {key}: {sync_by_action[key]}")
    if failures:
        typer.echo("Recent failures:")
        for item in failures:
            typer.echo(
                f"- {item.get('submitted_at')} job_id={item.get('job_id')} job_name={item.get('job_name')} "
                f"state={item.get('state')} target={item.get('target')} analysis={item.get('analysis')}"
            )
    if avg_cpus is not None:
        typer.echo(f"Average CPUs requested: {avg_cpus}")


@run_ops_app.command("validate", context_settings={"allow_extra_args": True, "ignore_unknown_options": True})
def run_validate(
    ctx: typer.Context,
    profile: Optional[str] = typer.Option(
        None,
        "--profile",
        help="Resource profile name (built-in or user-defined)",
        autocompletion=_complete_profile_names,
    ),
    cpus: Optional[int] = typer.Option(None, help="CPU cores for scheduler template"),
    memory: Optional[str] = typer.Option(None, help="Memory for scheduler template"),
    job_time: Optional[str] = typer.Option(None, "--time", help="Walltime for scheduler template"),
    job_name: Optional[str] = typer.Option(None, help="Scheduler job name"),
    queue: Optional[str] = typer.Option(None, help="Queue for scheduler template"),
    node: Optional[str] = typer.Option(None, help="Node for scheduler template"),
    parallel_environment: Optional[str] = typer.Option(None, help="Parallel environment for scheduler template"),
    as_json: bool = typer.Option(False, "--json", help="Render validation summary as JSON"),
) -> None:
    """Validate run configuration and command without syncing or submitting."""
    checks: list[dict[str, str]] = []

    def _add(status: str, check: str, detail: str) -> None:
        checks.append({"status": status, "check": check, "detail": detail})
        if not as_json:
            typer.echo(f"[{status}] {check}: {detail}")

    command = " ".join(shlex.quote(arg) for arg in ctx.args) if ctx.args else ""
    if command:
        _add("PASS", "command", command)
    else:
        _add("FAIL", "command", "Missing command. Example: cdp run validate python main.py")

    cfg: Optional[ProjectConfig] = None
    project_root: Optional[Path] = None
    analysis_dir: Optional[Path] = None
    target_name = ""
    target: Optional[TargetConfig] = None
    analysis_rel = ""

    try:
        project_root = _project_root()
        _add("PASS", "project_root", str(project_root))
        cfg = load_config(project_root)
        _add("PASS", "config", str(config_path(project_root)))
    except Exception as exc:
        _add("FAIL", "config", str(exc))

    if cfg is not None and project_root is not None:
        try:
            analysis_dir = _require_active_analysis(cfg, project_root)
            analysis_rel = (cfg.active_analysis or "").strip("/")
            _add("PASS", "active_analysis", str(analysis_dir))
        except Exception as exc:
            _add("FAIL", "active_analysis", str(exc))

        try:
            target_name, target = _active_target(cfg)
            _add("PASS", "target", target_name)
        except Exception as exc:
            _add("FAIL", "target", str(exc))

    resolved_cpus = None
    resolved_memory = None
    resolved_time = None
    resolved_node = None
    resolved_queue = None
    resolved_pe = None
    resolved_job_name = None

    if cfg is not None and target is not None:
        if not target.remote_root:
            _add("FAIL", "remote_root", "Target remote_root is missing")
        else:
            _add("PASS", "remote_root", target.remote_root)
        profile_values: dict[str, str | int] = {}
        try:
            profile_values = _resolve_resource_profile(profile, cfg)
            _add("PASS", "profile", profile or "<none>")
        except Exception as exc:
            _add("FAIL", "profile", str(exc))

        resolved_cpus = cpus if cpus is not None else int(profile_values.get("cpus", target.default_cpus))
        resolved_memory = memory if memory is not None else str(profile_values.get("memory", target.default_memory))
        resolved_time = job_time if job_time is not None else str(profile_values.get("time", target.default_time))
        resolved_node = node or str(profile_values.get("node", target.default_node or "1"))
        resolved_queue = queue if queue is not None else str(profile_values.get("queue", target.default_queue))
        resolved_pe = (
            parallel_environment
            if parallel_environment is not None
            else str(profile_values.get("parallel_environment", target.default_parallel_environment))
        )
        resolved_job_name = job_name or f"cdp-{secrets.token_hex(4)}"
        _add(
            "PASS",
            "resources",
            f"cpus={resolved_cpus} memory={resolved_memory} time={resolved_time} "
            f"node={resolved_node} queue={resolved_queue} parallel_environment={resolved_pe} job_name={resolved_job_name}",
        )

        requires_queue = "{queue}" in target.template_header
        requires_pe = "{parallel_environment}" in target.template_header
        if requires_queue and not resolved_queue:
            _add("FAIL", "template_queue", "Template requires queue but no value resolved")
        else:
            _add("PASS", "template_queue", "Queue requirement satisfied")
        if requires_pe and not resolved_pe:
            _add("FAIL", "template_parallel_environment", "Template requires parallel_environment but no value resolved")
        else:
            _add("PASS", "template_parallel_environment", "Parallel environment requirement satisfied")

        if target.scheduler == "none" and not target.template_header.strip():
            _add("PASS", "template", "Skipped for scheduler=none with empty template")
        else:
            try:
                _validate_template_variables(target.template_header)
                _render_scheduler_header(
                    target.template_header,
                    cpus=int(resolved_cpus),
                    memory=str(resolved_memory),
                    walltime=str(resolved_time),
                    job_name=str(resolved_job_name),
                    stdout=str(resolved_job_name),
                    stderr=str(resolved_job_name),
                    working_dir=(f"{target.remote_root.rstrip('/')}/{analysis_rel}" if analysis_rel else target.remote_root),
                    queue=str(resolved_queue or ""),
                    node=str(resolved_node),
                    parallel_environment=str(resolved_pe or ""),
                )
                _add("PASS", "template", "Template render check passed")
            except Exception as exc:
                _add("FAIL", "template", str(exc))

    if cfg is not None and analysis_rel:
        tags = cfg.analysis_tags.get(cfg.active_analysis or "", [])
        if tags:
            _add("PASS", "analysis_tags", f"{len(tags)} tag(s)")
        else:
            _add("WARN", "analysis_tags", "No analysis tags configured")

    pass_count, warn_count, fail_count = _validation_summary(checks)
    payload = {
        "checks": checks,
        "summary": {"pass": pass_count, "warn": warn_count, "fail": fail_count},
    }
    if as_json:
        typer.echo(json.dumps(payload, indent=2))
    else:
        typer.echo(f"Summary: PASS={pass_count} WARN={warn_count} FAIL={fail_count}")
    if fail_count:
        raise typer.Exit(code=1)


@sweep_ops_app.command("validate", context_settings={"allow_extra_args": True, "ignore_unknown_options": True})
def sweep_validate(
    ctx: typer.Context,
    config_file: Path = typer.Option(..., "--config", help="Sweep YAML config with top-level params mapping"),
    mode: str = typer.Option("single", "--mode", help="Execution mode: single|array|local"),
    sweep_job: Optional[str] = typer.Option(None, "--job", help="Validate only one params.<job> block from config"),
    target: Optional[str] = typer.Option(
        None, "--target", help="Target name (defaults to active target)", autocompletion=_complete_target_names
    ),
    profile: Optional[str] = typer.Option(
        None,
        "--profile",
        help="Resource profile name (built-in or user-defined)",
        autocompletion=_complete_profile_names,
    ),
    cpus: Optional[int] = typer.Option(None, help="CPU cores"),
    memory: Optional[str] = typer.Option(None, help="Memory"),
    job_time: Optional[str] = typer.Option(None, "--time", help="Walltime"),
    queue: Optional[str] = typer.Option(None, help="Queue"),
    node: Optional[str] = typer.Option(None, help="Node"),
    parallel_environment: Optional[str] = typer.Option(None, "--parallel-environment", help="Parallel environment"),
    as_json: bool = typer.Option(False, "--json", help="Render validation summary as JSON"),
) -> None:
    """Validate sweep config/template/resources without submitting jobs."""
    checks: list[dict[str, str]] = []

    def _add(status: str, check: str, detail: str) -> None:
        checks.append({"status": status, "check": check, "detail": detail})
        if not as_json:
            typer.echo(f"[{status}] {check}: {detail}")

    mode_lc = mode.lower()
    if mode_lc not in {"single", "array", "local"}:
        _add("FAIL", "mode", "--mode must be one of: single, array, local")
    else:
        _add("PASS", "mode", mode_lc)

    command_template = " ".join(shlex.quote(arg) for arg in ctx.args) if ctx.args else ""
    if not command_template:
        _add("FAIL", "command_template", "Missing command template")
    else:
        _add("PASS", "command_template", command_template)

    payload: dict[str, Any] = {}
    if not config_file.exists() or not config_file.is_file():
        _add("FAIL", "config_file", f"Sweep config file not found: {config_file}")
    else:
        _add("PASS", "config_file", str(config_file))
        try:
            parsed = yaml.safe_load(config_file.read_text()) or {}
            if not isinstance(parsed, dict):
                raise ValueError("Sweep config must be a YAML mapping")
            payload = parsed
            _add("PASS", "config_parse", "YAML parse ok")
        except Exception as exc:
            _add("FAIL", "config_parse", str(exc))

    runs: list[dict[str, Any]] = []
    if command_template and payload:
        try:
            runs = _expand_sweep_runs(payload, command_template, sweep_job)
            _add("PASS", "sweep_expand", f"Generated {len(runs)} run(s)")
        except Exception as exc:
            _add("FAIL", "sweep_expand", str(exc))

    cfg: Optional[ProjectConfig] = None
    project_root: Optional[Path] = None
    target_cfg: Optional[TargetConfig] = None
    if True:
        try:
            project_root = _project_root()
            cfg = load_config(project_root)
            _add("PASS", "project_config", str(config_path(project_root)))
            _require_active_analysis(cfg, project_root)
            _add("PASS", "active_analysis", str((cfg.active_analysis or "").strip("/")))
        except Exception as exc:
            _add("FAIL", "project_config", str(exc))

    if cfg is not None:
        try:
            if target:
                if target not in cfg.targets:
                    raise typer.BadParameter(f"Target '{target}' not found")
                target_cfg = cfg.targets[target]
                _add("PASS", "target", target)
            else:
                tname, target_cfg = _active_target(cfg)
                _add("PASS", "target", tname)
        except Exception as exc:
            _add("FAIL", "target", str(exc))

    if target_cfg is not None and mode_lc == "array" and target_cfg.scheduler.lower() == "none":
        _add("FAIL", "mode_scheduler", "Array mode is not supported for scheduler 'none'")

    if cfg is not None and target_cfg is not None:
        profile_values: dict[str, str | int] = {}
        try:
            profile_values = _resolve_resource_profile(profile, cfg)
            _add("PASS", "profile", profile or "<none>")
        except Exception as exc:
            _add("FAIL", "profile", str(exc))

        resolved_queue = queue if queue is not None else str(profile_values.get("queue", target_cfg.default_queue))
        resolved_pe = (
            parallel_environment
            if parallel_environment is not None
            else str(profile_values.get("parallel_environment", target_cfg.default_parallel_environment))
        )
        if "{queue}" in target_cfg.template_header and not resolved_queue:
            _add("FAIL", "template_queue", "Template requires queue but no value resolved")
        else:
            _add("PASS", "template_queue", "Queue requirement satisfied")
        if "{parallel_environment}" in target_cfg.template_header and not resolved_pe:
            _add("FAIL", "template_parallel_environment", "Template requires parallel_environment but no value resolved")
        else:
            _add("PASS", "template_parallel_environment", "Parallel environment requirement satisfied")

        if target_cfg.scheduler == "none" and not target_cfg.template_header.strip():
            _add("PASS", "template", "Skipped for scheduler=none with empty template")
        else:
            try:
                _validate_template_variables(target_cfg.template_header)
                _add("PASS", "template", "Template placeholders valid")
            except Exception as exc:
                _add("FAIL", "template", str(exc))

        resolved_cpus = cpus if cpus is not None else int(profile_values.get("cpus", target_cfg.default_cpus))
        resolved_memory = memory if memory is not None else str(profile_values.get("memory", target_cfg.default_memory))
        resolved_time = job_time if job_time is not None else str(profile_values.get("time", target_cfg.default_time))
        resolved_node = node or str(profile_values.get("node", target_cfg.default_node or "1"))
        _add(
            "PASS",
            "resources",
            f"cpus={resolved_cpus} memory={resolved_memory} time={resolved_time} "
            f"node={resolved_node} queue={resolved_queue} parallel_environment={resolved_pe}",
        )

    pass_count, warn_count, fail_count = _validation_summary(checks)
    payload_out = {
        "checks": checks,
        "summary": {"pass": pass_count, "warn": warn_count, "fail": fail_count},
        "runs_generated": len(runs),
    }
    if as_json:
        typer.echo(json.dumps(payload_out, indent=2))
    else:
        typer.echo(f"Summary: PASS={pass_count} WARN={warn_count} FAIL={fail_count} runs={len(runs)}")
    if fail_count:
        raise typer.Exit(code=1)


@app.command("retry")
def retry(
    job_id: Optional[str] = typer.Option(None, "--job-id", help="Retry from exact job id", autocompletion=_complete_job_ids),
    job_name: Optional[str] = typer.Option(
        None, "--job-name", help="Retry from exact job name", autocompletion=_complete_job_names
    ),
    target: Optional[str] = typer.Option(
        None, "--target", help="Restrict retry lookup to target", autocompletion=_complete_target_names
    ),
    analysis: Optional[str] = typer.Option(
        None, "--analysis", help="Restrict retry lookup to analysis", autocompletion=_complete_record_analyses
    ),
    dry_run: bool = typer.Option(False, "--dry-run", help="Preview retry actions without any modifications"),
) -> None:
    """Retry a previous normal run using recorded provenance."""
    project_root = _project_root()
    cfg = load_config(project_root)
    records = _load_job_records(project_root)
    if not records:
        raise typer.BadParameter("No job records found. Run `cdp analysis run ...` first.")

    normal_records = [rec for rec in records if "sweep_id" not in rec]
    if not normal_records:
        raise typer.BadParameter("No normal run records found to retry.")

    def _matches(rec: dict[str, Any]) -> bool:
        if job_id and str(rec.get("job_id", "")) != job_id:
            return False
        if job_name and str(rec.get("job_name", "")) != job_name:
            return False
        if target and str(rec.get("target", "")) != target:
            return False
        if analysis and str(rec.get("analysis", "")) != analysis:
            return False
        return True

    has_filters = any([job_id, job_name, target, analysis])
    candidates = [rec for rec in normal_records if _matches(rec)] if has_filters else normal_records
    if not candidates:
        raise typer.BadParameter("No matching normal run records found for retry")
    if len(candidates) > 1:
        typer.echo(f"Multiple matches found ({len(candidates)}). Retrying the most recent record.")
    selected = candidates[0]

    analysis_rel = str(selected.get("analysis", "")).strip("/")
    if not analysis_rel:
        raise typer.BadParameter("Selected record is missing analysis path")
    analysis_dir = (project_root / analysis_rel).resolve()
    if not analysis_dir.exists() or not analysis_dir.is_dir():
        raise typer.BadParameter(f"Analysis directory not found for selected record: {analysis_dir}")
    try:
        analysis_dir.relative_to(project_root)
    except ValueError as exc:
        raise typer.BadParameter("Selected analysis path is outside project root") from exc

    target_name = str(selected.get("target", "")).strip()
    if not target_name:
        raise typer.BadParameter("Selected record is missing target name")
    if target_name not in cfg.targets:
        raise typer.BadParameter(f"Target '{target_name}' from selected record is not configured")
    target_cfg = cfg.targets[target_name]

    command = str(selected.get("command_resolved", "")).strip() or str(selected.get("command", "")).strip()
    if not command:
        raise typer.BadParameter("Selected record is missing command")

    resource_block = selected.get("resources", {}) if isinstance(selected.get("resources", {}), dict) else {}
    profile_name = str(resource_block.get("profile", "")).strip() or None

    cpus_value = selected.get("cpus", resource_block.get("cpus", target_cfg.default_cpus))
    memory_value = selected.get("memory", resource_block.get("memory", target_cfg.default_memory))
    time_value = selected.get("time", resource_block.get("time", target_cfg.default_time))
    node_value = selected.get("node", resource_block.get("node", target_cfg.default_node or "1"))
    queue_value = selected.get("queue", resource_block.get("queue", target_cfg.default_queue))
    pe_value = selected.get(
        "parallel_environment",
        resource_block.get("parallel_environment", target_cfg.default_parallel_environment),
    )
    retry_job_name = str(selected.get("job_name", "")).strip() or f"cdp-{secrets.token_hex(4)}"

    _submit_or_preview_analysis_run(
        project_root=project_root,
        cfg=cfg,
        analysis_dir=analysis_dir,
        analysis_rel=analysis_rel,
        target_name=target_name,
        target=target_cfg,
        command=command,
        profile_name=profile_name,
        cpus=int(cpus_value),
        memory=str(memory_value),
        walltime=str(time_value),
        job_name=retry_job_name,
        queue=str(queue_value),
        node=str(node_value),
        parallel_environment=str(pe_value),
        dry_run=dry_run,
        index_after=False,
    )


@app.command("collect")
def collect(
    job_id: Optional[str] = typer.Option(None, "--job-id", help="Collect for exact job id", autocompletion=_complete_job_ids),
    job_name: Optional[str] = typer.Option(
        None, "--job-name", help="Collect for exact job name", autocompletion=_complete_job_names
    ),
    target: Optional[str] = typer.Option(
        None, "--target", help="Restrict match to target", autocompletion=_complete_target_names
    ),
    analysis: Optional[str] = typer.Option(
        None, "--analysis", help="Restrict match to analysis path", autocompletion=_complete_record_analyses
    ),
) -> None:
    """Collect tagged outputs for a recorded job."""
    if not job_id and not job_name:
        raise typer.BadParameter("Provide at least one selector: --job-id or --job-name")

    project_root = _project_root()
    cfg = load_config(project_root)
    records = _load_job_records(project_root)
    if not records:
        raise typer.BadParameter("No job records found. Nothing to collect.")

    def _matches(rec: dict[str, str]) -> bool:
        if job_id and str(rec.get("job_id", "")) != job_id:
            return False
        if job_name and str(rec.get("job_name", "")) != job_name:
            return False
        if target and str(rec.get("target", "")) != target:
            return False
        if analysis and str(rec.get("analysis", "")) != analysis:
            return False
        return True

    matched = [rec for rec in records if _matches(rec)]
    if not matched:
        raise typer.BadParameter("No matching job records found for the provided filters")
    if len(matched) > 1:
        typer.echo(f"Multiple matches found ({len(matched)}). Collecting from most recent match.")
    selected = matched[0]

    selected_target = str(selected.get("target", ""))
    if selected_target not in cfg.targets:
        raise typer.BadParameter(f"Target '{selected_target}' from selected record is not configured")
    selected_analysis = str(selected.get("analysis", "")).strip("/")
    if not selected_analysis:
        raise typer.BadParameter("Selected record has no analysis path")
    remote_run_dir = str(selected.get("remote_run_dir", "")).strip()
    if not remote_run_dir:
        raise typer.BadParameter("Selected record has no remote_run_dir")

    analysis_dir = (project_root / selected_analysis).resolve()
    analysis_dir.mkdir(parents=True, exist_ok=True)
    tags = selected.get("analysis_tags")
    if not isinstance(tags, list) or not tags:
        raise typer.BadParameter("Selected record has no tagged paths to collect")

    target_cfg = cfg.targets[selected_target]
    index_plan_files = 0
    index_plan_bytes = 0
    index_missing: list[str] = []
    for tag_dir in tags:
        clean_tag = str(tag_dir).strip("/")
        if not clean_tag:
            continue
        tag_manifest = _load_index_manifest(project_root, selected_target, selected_analysis, clean_tag)
        if tag_manifest is None:
            root_manifest = _load_index_manifest(project_root, selected_target, selected_analysis, ".")
            if root_manifest is None or not _index_entry_path_exists(root_manifest, clean_tag):
                index_missing.append(clean_tag)
                continue
            # Root index has the path; estimate by filtering root entries under tag.
            sub_entries = []
            entries = root_manifest.get("entries", [])
            if isinstance(entries, list):
                prefix = f"{clean_tag}/"
                for entry in entries:
                    if not isinstance(entry, dict):
                        continue
                    rel = str(entry.get("path", "")).strip("/")
                    if rel == clean_tag or rel.startswith(prefix):
                        trimmed = dict(entry)
                        trimmed["path"] = rel.removeprefix(prefix) if rel.startswith(prefix) else "."
                        sub_entries.append(trimmed)
            pseudo_manifest = {"entries": sub_entries}
            file_count, total_bytes = _index_file_count_and_bytes(pseudo_manifest)
        else:
            file_count, total_bytes = _index_file_count_and_bytes(tag_manifest)
        index_plan_files += file_count
        index_plan_bytes += total_bytes

    pulled: list[str] = []
    skipped: list[str] = []
    for tag_dir in tags:
        clean_tag = str(tag_dir).strip("/")
        if not clean_tag:
            continue
        tagged_remote = f"{remote_run_dir}/./{clean_tag}"
        if _uses_local_transport(target_cfg):
            proc = _run_cmd(["rsync", "-az", "--relative", tagged_remote, f"{analysis_dir}/"], check=False)
        else:
            proc = _run_cmd(
                ["rsync", "-az", "--relative", f"{target_cfg.host}:{tagged_remote}", f"{analysis_dir}/"],
                check=False,
            )
        if proc.returncode == 0:
            pulled.append(clean_tag)
        else:
            skipped.append(clean_tag)

    typer.echo(
        f"Collect source: job_id={str(selected.get('job_id', ''))} job_name={str(selected.get('job_name', ''))} "
        f"target={selected_target} analysis={selected_analysis}"
    )
    if index_plan_files or index_plan_bytes:
        typer.echo(
            f"Index transfer estimate: files={index_plan_files} total_size={_humanize_bytes(index_plan_bytes)}"
        )
    if index_missing:
        typer.echo("Index warnings (no index metadata for tagged paths):")
        for item in sorted(set(index_missing)):
            typer.echo(f"- {item}")
    if pulled:
        typer.echo("Collected tagged paths:")
        for item in pulled:
            typer.echo(f"- {analysis_dir / item}")
    if skipped:
        typer.echo("Skipped tagged paths:")
        for item in skipped:
            typer.echo(f"- {item}")


def _fetch_job_stats(host: str, scheduler: str, job_id: str, transport: str = "ssh") -> tuple[dict[str, str], str]:
    scheduler_lc = scheduler.lower()
    quoted_job_id = shlex.quote(job_id)
    local_transport = transport.strip().lower() == "local" or _is_local_host(host)

    def _run_stats_cmd(command: str) -> subprocess.CompletedProcess[str]:
        if local_transport:
            return _run_direct(
                ["bash", "-lc", command],
                text=True,
                capture_output=True,
                check=False,
            )
        return _run_direct(
            ["ssh", host, "bash", "-lc", command],
            text=True,
            capture_output=True,
            check=False,
        )

    if scheduler_lc in {"sge", "univa"}:
        proc = _run_stats_cmd(f"qacct -j {quoted_job_id}")
        if proc.returncode != 0:
            raise typer.BadParameter(proc.stderr.strip() or "Failed to query SGE/Univa accounting")
        stats: dict[str, str] = {}
        for line in proc.stdout.splitlines():
            clean = line.strip()
            if not clean or clean.startswith("="):
                continue
            parts = clean.split()
            if len(parts) < 2:
                continue
            key = parts[0]
            value = parts[-1]
            if key == "exit_status":
                stats["exit_status"] = value
            elif key == "failed":
                stats["failed"] = value
            elif key == "cpu":
                stats["cpu_time"] = value
            elif key == "ru_wallclock":
                stats["wallclock"] = value
            elif key == "maxvmem":
                stats["max_vmem"] = value
            elif key == "mem":
                stats["mem"] = value
            elif key == "io":
                stats["io"] = value
        if stats.get("failed", "0") != "0" or stats.get("exit_status", "0") != "0":
            stats["state"] = "FAILED"
        else:
            stats["state"] = "COMPLETED"
        return stats, proc.stdout

    if scheduler_lc == "pbs":
        proc = _run_stats_cmd(f"qstat -xf {quoted_job_id}")
        if proc.returncode != 0:
            raise typer.BadParameter(proc.stderr.strip() or "Failed to query PBS accounting")
        stats = {}
        for line in proc.stdout.splitlines():
            clean = line.strip()
            if "=" not in clean:
                continue
            key, value = [p.strip() for p in clean.split("=", 1)]
            if key == "job_state":
                stats["state"] = value
            elif key == "Exit_status":
                stats["exit_status"] = value
            elif key.startswith("resources_used."):
                stats[key.replace("resources_used.", "")] = value
        return stats, proc.stdout

    if scheduler_lc == "slurm":
        proc = _run_stats_cmd(
            f"sacct -j {quoted_job_id} -X -P -n -o JobIDRaw,State,ExitCode,Elapsed,TotalCPU,AllocCPUS,MaxRSS,MaxVMSize,AveRSS,ReqMem"
        )
        if proc.returncode != 0:
            raise typer.BadParameter(proc.stderr.strip() or "Failed to query Slurm accounting")
        lines = [line.strip() for line in proc.stdout.splitlines() if line.strip()]
        if not lines:
            raise typer.BadParameter("No Slurm accounting record returned for this job")
        selected_line = lines[0]
        cols = selected_line.split("|")
        stats = {}
        headers = [
            "job_id_raw",
            "state",
            "exit_code",
            "elapsed",
            "total_cpu",
            "alloc_cpus",
            "max_rss",
            "max_vmem",
            "avg_rss",
            "req_mem",
        ]
        for idx, key in enumerate(headers):
            if idx < len(cols):
                stats[key] = cols[idx]
        return stats, proc.stdout

    if scheduler_lc == "lsf":
        proc = _run_stats_cmd(f"bjobs -a -noheader -o 'STAT EXIT_CODE CPU_USED RUN_TIME MAX_MEM MEM SWAP' {quoted_job_id}")
        if proc.returncode != 0:
            raise typer.BadParameter(proc.stderr.strip() or "Failed to query LSF accounting")
        line = proc.stdout.strip().splitlines()[0].strip() if proc.stdout.strip() else ""
        if not line:
            raise typer.BadParameter("No LSF accounting record returned for this job")
        parts = line.split()
        stats = {}
        keys = ["state", "exit_code", "cpu_used", "run_time", "max_mem", "mem", "swap"]
        for idx, key in enumerate(keys):
            if idx < len(parts):
                stats[key] = parts[idx]
        return stats, proc.stdout

    if scheduler_lc == "none":
        proc = _run_stats_cmd(f"ps -p {quoted_job_id} -o pid=,etime=,%cpu=,%mem=,rss=,vsz=")
        line = proc.stdout.strip().splitlines()[0].strip() if proc.stdout.strip() else ""
        if not line:
            return {"state": "EXITED"}, proc.stdout
        parts = line.split()
        stats = {"state": "RUNNING"}
        keys = ["pid", "elapsed", "cpu_percent", "mem_percent", "rss_kb", "vsz_kb"]
        for idx, key in enumerate(keys):
            if idx < len(parts):
                stats[key] = parts[idx]
        return stats, proc.stdout

    raise typer.BadParameter(f"Unsupported scheduler for stats: {scheduler}")


def _render_kv_table(title: str, rows: list[tuple[str, str]]) -> None:
    if not rows:
        typer.echo(f"{title}\n(no data)")
        return
    key_width = max(len("Metric"), max(len(k) for k, _ in rows))
    value_width = max(len("Value"), max(len(v) for _, v in rows))
    border = f"+-{'-' * key_width}-+-{'-' * value_width}-+"

    typer.echo(title)
    typer.echo(border)
    typer.echo(f"| {'Metric'.ljust(key_width)} | {'Value'.ljust(value_width)} |")
    typer.echo(border)
    for key, value in rows:
        typer.echo(f"| {key.ljust(key_width)} | {value.ljust(value_width)} |")
    typer.echo(border)


def _humanize_seconds(total_seconds: int) -> str:
    days, rem = divmod(max(0, total_seconds), 86400)
    hours, rem = divmod(rem, 3600)
    minutes, seconds = divmod(rem, 60)
    if days:
        return f"{days}d {hours:02}:{minutes:02}:{seconds:02}"
    return f"{hours:02}:{minutes:02}:{seconds:02}"


def _parse_duration_to_seconds(raw: str) -> Optional[int]:
    value = raw.strip()
    if not value:
        return None
    if value.isdigit():
        return int(value)

    # Slurm elapsed format: D-HH:MM:SS
    day_part = 0
    core = value
    if "-" in value:
        parts = value.split("-", 1)
        if parts[0].isdigit():
            day_part = int(parts[0])
            core = parts[1]

    hh = mm = ss = None
    chunks = core.split(":")
    if len(chunks) == 3 and all(c.isdigit() for c in chunks):
        hh, mm, ss = [int(c) for c in chunks]
    elif len(chunks) == 2 and all(c.isdigit() for c in chunks):
        hh = 0
        mm, ss = [int(c) for c in chunks]
    if hh is None:
        return None
    return day_part * 86400 + hh * 3600 + mm * 60 + ss


def _humanize_bytes(num_bytes: float) -> str:
    units = ["B", "KiB", "MiB", "GiB", "TiB", "PiB"]
    value = float(num_bytes)
    unit_idx = 0
    while value >= 1024 and unit_idx < len(units) - 1:
        value /= 1024.0
        unit_idx += 1
    return f"{value:.2f} {units[unit_idx]}"


def _parse_memory_to_bytes(raw: str) -> Optional[float]:
    value = raw.strip()
    if not value:
        return None
    value_l = value.lower()

    # Slurm ReqMem can be like 4000Mn/4000Mc; strip trailing scope marker.
    if len(value_l) >= 2 and value_l[-1] in {"n", "c"} and value_l[-2].isalpha():
        value_l = value_l[:-1]

    match = re.match(r"^([0-9]*\.?[0-9]+)\s*([a-zA-Z]+)?$", value_l)
    if not match:
        return None
    amount = float(match.group(1))
    unit = (match.group(2) or "b").lower()

    unit_map = {
        "b": 1,
        "byte": 1,
        "bytes": 1,
        "k": 1024,
        "kb": 1024,
        "kib": 1024,
        "m": 1024**2,
        "mb": 1024**2,
        "mib": 1024**2,
        "g": 1024**3,
        "gb": 1024**3,
        "gib": 1024**3,
        "t": 1024**4,
        "tb": 1024**4,
        "tib": 1024**4,
        "p": 1024**5,
        "pb": 1024**5,
        "pib": 1024**5,
    }
    if unit not in unit_map:
        return None
    return amount * unit_map[unit]


def _normalize_stats_for_display(stats: dict[str, str]) -> dict[str, str]:
    normalized = {k: str(v) for k, v in stats.items()}
    time_keys = {"elapsed", "total_cpu", "cpu_time", "wallclock", "run_time", "cput", "walltime"}
    mem_keys = {"max_vmem", "mem", "max_rss", "avg_rss", "req_mem", "vmem", "max_mem", "swap"}

    for key in list(normalized.keys()):
        value = normalized[key]

        if key in time_keys:
            seconds = _parse_duration_to_seconds(value)
            if seconds is not None:
                normalized[key] = _humanize_seconds(seconds)

        if key in {"rss_kb", "vsz_kb"}:
            if value.isdigit():
                normalized[key] = _humanize_bytes(float(value) * 1024.0)

        if key in mem_keys:
            as_bytes = _parse_memory_to_bytes(value)
            if as_bytes is not None:
                normalized[key] = _humanize_bytes(as_bytes)

    return normalized


@app.command("stats")
def stats(
    job_id: Optional[str] = typer.Option(None, "--job-id", help="Get stats for exact job id", autocompletion=_complete_job_ids),
    job_name: Optional[str] = typer.Option(
        None, "--job-name", help="Get stats for exact job name", autocompletion=_complete_job_names
    ),
    target: Optional[str] = typer.Option(
        None, "--target", help="Restrict match to target", autocompletion=_complete_target_names
    ),
    analysis: Optional[str] = typer.Option(
        None, "--analysis", help="Restrict match to analysis path", autocompletion=_complete_record_analyses
    ),
) -> None:
    """Collect resource usage stats for a recorded job."""
    if not job_id and not job_name:
        raise typer.BadParameter("Provide at least one selector: --job-id or --job-name")

    project_root = _project_root()
    cfg = load_config(project_root)
    records = _load_job_records(project_root)
    if not records:
        raise typer.BadParameter("No job records found.")

    def _matches(rec: dict[str, str]) -> bool:
        if job_id and str(rec.get("job_id", "")) != job_id:
            return False
        if job_name and str(rec.get("job_name", "")) != job_name:
            return False
        if target and str(rec.get("target", "")) != target:
            return False
        if analysis and str(rec.get("analysis", "")) != analysis:
            return False
        return True

    matched = [rec for rec in records if _matches(rec)]
    if not matched:
        raise typer.BadParameter("No matching job records found for the provided filters")
    if len(matched) > 1:
        typer.echo(f"Multiple matches found ({len(matched)}). Using most recent match.")
    selected = matched[0]

    selected_target = str(selected.get("target", ""))
    if selected_target not in cfg.targets:
        raise typer.BadParameter(f"Target '{selected_target}' from selected record is not configured")
    selected_job_id = str(selected.get("job_id", "")).strip()
    if not selected_job_id:
        raise typer.BadParameter("Selected record has no job_id")
    selected_scheduler = str(selected.get("scheduler", "")).strip()
    if not selected_scheduler:
        selected_scheduler = cfg.targets[selected_target].scheduler

    target_cfg = cfg.targets[selected_target]
    usage_stats, _ = _fetch_job_stats(
        host=target_cfg.host,
        scheduler=selected_scheduler,
        job_id=selected_job_id,
        transport=target_cfg.transport,
    )
    display_stats = _normalize_stats_for_display(usage_stats)
    rows = [
        ("job_id", selected_job_id),
        ("job_name", str(selected.get("job_name", ""))),
        ("target", selected_target),
        ("scheduler", selected_scheduler),
    ]
    for key in sorted(display_stats):
        rows.append((key, str(display_stats[key])))
    _render_kv_table("Job Stats", rows)


def _collect_status_rows(
    project_root: Path,
    cfg: ProjectConfig,
    analysis_filter: Optional[str],
    target_filter: Optional[str],
    job_id_filter: Optional[str],
    job_name_filter: Optional[str],
    limit: int,
) -> list[dict[str, str]]:
    jobs_dir = project_root / CONFIG_DIR / JOBS_DIR
    if not jobs_dir.exists():
        return []

    job_files = sorted(jobs_dir.glob("*.json"), reverse=True)
    rows: list[dict[str, str]] = []

    for job_file in job_files:
        try:
            payload = json.loads(job_file.read_text())
        except json.JSONDecodeError:
            continue

        analysis_name = str(payload.get("analysis", ""))
        if analysis_filter and analysis_name != analysis_filter:
            continue
        target_name = str(payload.get("target", ""))
        if target_filter and target_name != target_filter:
            continue

        job_id = str(payload.get("job_id", ""))
        if job_id_filter and job_id != job_id_filter:
            continue
        job_name = str(payload.get("job_name", ""))
        if job_name_filter and job_name != job_name_filter:
            continue
        scheduler = str(payload.get("scheduler", ""))
        submitted_at = str(payload.get("submitted_at", ""))
        state = _resolve_and_persist_job_state(
            cfg=cfg,
            job_file=job_file,
            payload=payload,
            job_id=job_id,
            target_name=target_name,
            scheduler=scheduler,
        )

        rows.append(
            {
                "job_id": job_id,
                "job_name": job_name,
                "analysis": analysis_name,
                "target": target_name,
                "scheduler": scheduler,
                "state": state,
                "submitted_at": submitted_at,
                "output_check": (_output_check_from_index(project_root, payload) if _is_terminal_state(state) else "N/A"),
            }
        )
        if len(rows) >= limit:
            break

    return rows


def _show_global_status(limit: int = 50) -> None:
    project_root = _project_root()
    cfg = load_config(project_root)
    rows = _collect_status_rows(
        project_root,
        cfg,
        analysis_filter=None,
        target_filter=None,
        job_id_filter=None,
        job_name_filter=None,
        limit=limit,
    )

    if not rows:
        typer.echo("No jobs recorded yet")
        return

    typer.echo("Global status across targets:")
    for row in rows:
        typer.echo(
            f"{row['job_id']}  analysis={row['analysis']} target={row['target']} "
            f"scheduler={row['scheduler']} state={row['state']} output={row['output_check']} submitted_at={row['submitted_at']}"
        )


def _show_target_status(target: str, limit: int = 50) -> None:
    project_root = _project_root()
    cfg = load_config(project_root)
    if target not in cfg.targets:
        raise typer.BadParameter(f"Target '{target}' not found")

    rows = _collect_status_rows(
        project_root,
        cfg,
        analysis_filter=None,
        target_filter=target,
        job_id_filter=None,
        job_name_filter=None,
        limit=limit,
    )
    if not rows:
        typer.echo(f"No jobs found for target '{target}'")
        return

    typer.echo(f"Status for target: {target}")
    for row in rows:
        typer.echo(
            f"{row['job_id']}  job_name={row['job_name']} analysis={row['analysis']} scheduler={row['scheduler']} "
            f"state={row['state']} output={row['output_check']} submitted_at={row['submitted_at']}"
        )


def _show_filtered_status(
    target: Optional[str],
    job_id: Optional[str],
    job_name: Optional[str],
    limit: int,
) -> None:
    project_root = _project_root()
    cfg = load_config(project_root)
    if target and target not in cfg.targets:
        raise typer.BadParameter(f"Target '{target}' not found")

    rows = _collect_status_rows(
        project_root,
        cfg,
        analysis_filter=None,
        target_filter=target,
        job_id_filter=job_id,
        job_name_filter=job_name,
        limit=limit,
    )
    if not rows:
        filters = []
        if target:
            filters.append(f"target={target}")
        if job_id:
            filters.append(f"job_id={job_id}")
        if job_name:
            filters.append(f"job_name={job_name}")
        filter_text = " ".join(filters) if filters else "requested filters"
        typer.echo(f"No jobs found for {filter_text}")
        return

    filters = []
    if target:
        filters.append(f"target={target}")
    if job_id:
        filters.append(f"job_id={job_id}")
    if job_name:
        filters.append(f"job_name={job_name}")
    filter_text = " ".join(filters) if filters else "all jobs"
    typer.echo(f"Status for {filter_text}:")
    for row in rows:
        typer.echo(
            f"{row['job_id']}  job_name={row['job_name']} analysis={row['analysis']} "
            f"target={row['target']} scheduler={row['scheduler']} state={row['state']} "
            f"output={row['output_check']} submitted_at={row['submitted_at']}"
        )


@status_app.callback(invoke_without_command=True)
def status_callback(
    ctx: typer.Context,
    follow: Annotated[bool, typer.Option(help="Follow active job log output")] = False,
    target: Optional[str] = typer.Option(
        None, "--target", help="Show scheduler status filtered to a target name", autocompletion=_complete_target_names
    ),
    job_id: Optional[str] = typer.Option(
        None, "--job-id", help="Show status for a specific scheduler job id", autocompletion=_complete_job_ids
    ),
    job_name: Optional[str] = typer.Option(
        None, "--job-name", help="Show status for jobs with an exact job name", autocompletion=_complete_job_names
    ),
    limit: int = typer.Option(50, help="Maximum number of jobs to display for target/global views"),
) -> None:
    """Show context-aware status, or use subcommands like `cdp status list`."""
    if ctx.invoked_subcommand is None:
        if target or job_id or job_name:
            if follow:
                raise typer.BadParameter("--follow cannot be used with --target, --job-id, or --job-name")
            _show_filtered_status(target=target, job_id=job_id, job_name=job_name, limit=limit)
            return

        project_root = _project_root()
        cfg = load_config(project_root)
        cwd = Path.cwd().resolve()

        in_active_analysis = False
        if cfg.active_analysis:
            active_analysis_path = (project_root / cfg.active_analysis).resolve()
            try:
                cwd.relative_to(active_analysis_path)
                in_active_analysis = True
            except ValueError:
                in_active_analysis = False

        if in_active_analysis:
            _show_last_status(follow=follow)
        else:
            if follow:
                raise typer.BadParameter("--follow is only supported when inside the active analysis context")
            _show_global_status(limit=limit)


@status_app.command("list")
def status_list(
    analysis: Optional[str] = typer.Option(
        None,
        help="Analysis path to filter by (defaults to active analysis, e.g. analyses/analysis_a)",
        autocompletion=_complete_record_analyses,
    ),
    limit: int = typer.Option(20, help="Maximum number of jobs to display"),
) -> None:
    """List recent job IDs and current states for an analysis."""
    project_root = _project_root()
    cfg = load_config(project_root)
    target_analysis = analysis or cfg.active_analysis
    if not target_analysis:
        raise typer.BadParameter("No analysis provided and no active analysis set")

    rows = _collect_status_rows(
        project_root,
        cfg,
        analysis_filter=target_analysis,
        target_filter=None,
        job_id_filter=None,
        job_name_filter=None,
        limit=limit,
    )

    if not rows:
        typer.echo(f"No jobs found for analysis '{target_analysis}'")
        return

    typer.echo(f"Jobs for analysis: {target_analysis}")
    for row in rows:
        typer.echo(
            f"{row['job_id']}  job_name={row['job_name']} target={row['target']} scheduler={row['scheduler']} "
            f"state={row['state']} output={row['output_check']} submitted_at={row['submitted_at']}"
        )


@status_app.command("global")
def status_global(
    limit: int = typer.Option(50, help="Maximum number of jobs to display"),
) -> None:
    """Show global status across analyses and targets."""
    _show_global_status(limit=limit)


def _resolve_sync_context() -> tuple[Path, ProjectConfig, Path, str, TargetConfig, str, str]:
    project_root = _project_root()
    cfg = load_config(project_root)
    analysis_dir = _require_active_analysis(cfg, project_root)
    target_name, target_cfg = _active_target(cfg)
    if not target_cfg.remote_root:
        raise typer.BadParameter(
            f"Target '{target_name}' has no remote_root configured. Update it with `cdp target add {target_name} --remote-root ...`."
        )
    analysis_rel = (cfg.active_analysis or "").strip("/")
    if not analysis_rel:
        raise typer.BadParameter("Active analysis path is empty. Re-run: cdp analysis use <path>")
    remote_analysis_root = f"{target_cfg.remote_root.rstrip('/')}/{analysis_rel}"
    return project_root, cfg, analysis_dir, target_name, target_cfg, analysis_rel, remote_analysis_root


def _sync_push(dry_run: bool) -> None:
    project_root, cfg, analysis_dir, target_name, target_cfg, analysis_rel, remote_analysis_root = _resolve_sync_context()
    project_ignore = _ignore_file(project_root)
    local_target_mode = _uses_local_transport(target_cfg)

    mkdir_cmd = ["mkdir", "-p", remote_analysis_root]
    rsync_cmd = ["rsync", "-az"]
    if project_ignore.exists():
        rsync_cmd.extend(["--exclude-from", str(project_ignore)])
    if local_target_mode:
        rsync_cmd.extend([f"{analysis_dir}/", f"{remote_analysis_root}/"])
    else:
        rsync_cmd.extend([f"{analysis_dir}/", f"{target_cfg.host}:{remote_analysis_root}/"])

    if dry_run:
        typer.echo("Dry run: no changes will be made")
        typer.echo(f"Action: sync push")
        typer.echo(f"Project root: {project_root}")
        typer.echo(f"Analysis: {analysis_rel}")
        typer.echo(f"Target: {target_name}")
        typer.echo(f"Transport: {target_cfg.transport}")
        typer.echo(f"Source: {analysis_dir}/")
        typer.echo(f"Destination: {remote_analysis_root}/")
        typer.echo(
            f"Sync exclusions: {IGNORE_FILE_NAME} detected" if project_ignore.exists() else f"Sync exclusions: {IGNORE_FILE_NAME} not found"
        )
        if local_target_mode:
            typer.echo(f"Would run: {' '.join(shlex.quote(arg) for arg in mkdir_cmd)}")
        else:
            typer.echo(f"Would run: ssh {shlex.quote(target_cfg.host)} {' '.join(shlex.quote(arg) for arg in mkdir_cmd)}")
        typer.echo(f"Would run: {' '.join(shlex.quote(arg) for arg in rsync_cmd)}")
        return

    if local_target_mode:
        Path(remote_analysis_root).mkdir(parents=True, exist_ok=True)
    else:
        _run_cmd(["ssh", target_cfg.host, *mkdir_cmd])
    _run_cmd(rsync_cmd)

    record_path = _write_sync_event(
        project_root,
        {
            "action": "push",
            "target": target_name,
            "analysis": cfg.active_analysis,
            "scheduler": target_cfg.scheduler,
            "transport": target_cfg.transport,
            "source": str(analysis_dir.resolve()),
            "destination": remote_analysis_root,
            "ignore_file": str(project_ignore) if project_ignore.exists() else None,
            "ignore_used": project_ignore.exists(),
        },
    )
    typer.echo(f"Synced analysis to target '{target_name}': {analysis_dir} -> {remote_analysis_root}")
    typer.echo(f"Recorded sync event: {record_path}")


def _sync_pull(remote: bool, all_paths: bool, dry_run: bool, index_after: bool = False, yes: bool = False) -> None:
    project_root, cfg, analysis_dir, target_name, target_cfg, analysis_rel, remote_analysis_root = _resolve_sync_context()
    local_target_mode = _uses_local_transport(target_cfg)

    if all_paths:
        if not dry_run:
            _require_yes(
                yes,
                "Full analysis pull may overwrite local files. Re-run with --yes to confirm.",
            )
        rsync_cmd = ["rsync", "-az"]
        if local_target_mode:
            rsync_cmd.extend([f"{remote_analysis_root}/", f"{analysis_dir}/"])
        else:
            rsync_cmd.extend([f"{target_cfg.host}:{remote_analysis_root}/", f"{analysis_dir}/"])

        if dry_run:
            root_index = _load_index_manifest(project_root, target_name, analysis_rel, ".")
            if root_index is not None:
                delta = _summarize_index_vs_local(analysis_dir, root_index)
                typer.echo(
                    "Index diff summary (preview): "
                    f"remote_only={delta['remote_only']} local_only={delta['local_only']} changed={delta['changed']}"
                )
            typer.echo("Dry run: no changes will be made")
            typer.echo("Action: sync pull")
            typer.echo(f"Project root: {project_root}")
            typer.echo(f"Analysis: {analysis_rel}")
            typer.echo(f"Target: {target_name}")
            typer.echo(f"Transport: {target_cfg.transport}")
            typer.echo(f"Source: {remote_analysis_root}/")
            typer.echo(f"Destination: {analysis_dir}/")
            typer.echo(f"Would run: {' '.join(shlex.quote(arg) for arg in rsync_cmd)}")
            return

        root_index = _load_index_manifest(project_root, target_name, analysis_rel, ".")
        if root_index is not None:
            delta = _summarize_index_vs_local(analysis_dir, root_index)
            typer.echo(
                "Index diff summary (before pull): "
                f"remote_only={delta['remote_only']} local_only={delta['local_only']} changed={delta['changed']}"
            )
        _run_cmd(rsync_cmd)
        record_path = _write_sync_event(
            project_root,
            {
                "action": "pull",
                "mode": "all",
                "target": target_name,
                "analysis": cfg.active_analysis,
                "scheduler": target_cfg.scheduler,
                "transport": target_cfg.transport,
                "source": remote_analysis_root,
                "destination": str(analysis_dir.resolve()),
                "ignore_file": None,
                "ignore_used": False,
            },
        )
        typer.echo(f"Pulled full analysis from target '{target_name}': {remote_analysis_root} -> {analysis_dir}")
        typer.echo(f"Recorded sync event: {record_path}")
        if index_after:
            manifest_path = _auto_index_active_root(project_root, cfg, target_name, target_cfg)
            if manifest_path:
                typer.echo(f"Auto-indexed active analysis root: {manifest_path}")
        return

    analysis_tags = cfg.analysis_tags.get(cfg.active_analysis or "", [])
    if not analysis_tags:
        raise typer.BadParameter(
            "No tags found for active analysis. Tag paths with `cdp analysis tag <path>` before pulling."
        )

    pulled: list[str] = []
    skipped: list[str] = []
    missing_in_index: list[str] = []
    planned_cmds: list[list[str]] = []
    root_index = _load_index_manifest(project_root, target_name, analysis_rel, ".")
    for tag_dir in analysis_tags:
        clean_tag = tag_dir.strip("/")
        if not clean_tag:
            continue
        if root_index is not None and not _index_entry_path_exists(root_index, clean_tag):
            missing_in_index.append(clean_tag)
        if not remote and not (analysis_dir / clean_tag).exists():
            skipped.append(clean_tag)
            continue
        tagged_remote = f"{remote_analysis_root}/./{clean_tag}"
        if local_target_mode:
            cmd = ["rsync", "-az", "--relative", tagged_remote, f"{analysis_dir}/"]
        else:
            cmd = ["rsync", "-az", "--relative", f"{target_cfg.host}:{tagged_remote}", f"{analysis_dir}/"]
        planned_cmds.append(cmd)

    if dry_run:
        typer.echo("Dry run: no changes will be made")
        typer.echo("Action: sync pull")
        typer.echo(f"Project root: {project_root}")
        typer.echo(f"Analysis: {analysis_rel}")
        typer.echo(f"Target: {target_name}")
        typer.echo(f"Transport: {target_cfg.transport}")
        typer.echo(f"Mode: tagged ({'include remote-only tags' if remote else 'local-existing tags only'})")
        if planned_cmds:
            for cmd in planned_cmds:
                typer.echo(f"Would run: {' '.join(shlex.quote(arg) for arg in cmd)}")
        else:
            typer.echo("No tagged paths selected for pull")
        if missing_in_index:
            typer.echo("Index warnings (not found in latest root index):")
            for item in sorted(set(missing_in_index)):
                typer.echo(f"- {item}")
        if skipped:
            typer.echo("Would skip tagged paths:")
            for item in skipped:
                typer.echo(f"- {item}")
        return

    for cmd in planned_cmds:
        proc = _run_cmd(cmd, check=False)
        if proc.returncode == 0:
            clean_tag = cmd[-2].split("/./", 1)[-1] if "/./" in cmd[-2] else ""
            if clean_tag:
                pulled.append(clean_tag)
        else:
            clean_tag = cmd[-2].split("/./", 1)[-1] if "/./" in cmd[-2] else ""
            if clean_tag:
                skipped.append(clean_tag)

    record_path = _write_sync_event(
        project_root,
        {
            "action": "pull",
            "mode": "tagged",
            "target": target_name,
            "analysis": cfg.active_analysis,
            "scheduler": target_cfg.scheduler,
            "transport": target_cfg.transport,
            "source": remote_analysis_root,
            "destination": str(analysis_dir.resolve()),
            "include_remote_only_tags": remote,
            "pulled": pulled,
            "skipped": skipped,
            "ignore_file": None,
            "ignore_used": False,
        },
    )

    if pulled:
        typer.echo("Pulled tagged paths:")
        for item in pulled:
            typer.echo(f"- {analysis_dir / item}")
    if skipped:
        typer.echo("Skipped tagged paths:")
        for item in skipped:
            typer.echo(f"- {item}")
    if missing_in_index:
        typer.echo("Index warnings (not found in latest root index):")
        for item in sorted(set(missing_in_index)):
            typer.echo(f"- {item}")
    typer.echo(f"Recorded sync event: {record_path}")
    if index_after:
        manifest_path = _auto_index_active_root(project_root, cfg, target_name, target_cfg)
        if manifest_path:
            typer.echo(f"Auto-indexed active analysis root: {manifest_path}")


def _submit_or_preview_analysis_run(
    *,
    project_root: Path,
    cfg: ProjectConfig,
    analysis_dir: Path,
    analysis_rel: str,
    target_name: str,
    target: TargetConfig,
    command: str,
    profile_name: Optional[str],
    cpus: int,
    memory: str,
    walltime: str,
    job_name: str,
    queue: str,
    node: str,
    parallel_environment: str,
    dry_run: bool,
    index_after: bool,
) -> None:
    if not analysis_rel:
        raise typer.BadParameter("Active analysis path is empty. Re-run: cdp analysis use <path>")
    if not target.remote_root:
        raise typer.BadParameter(
            f"Target '{target_name}' has no remote_root configured. Update it with `cdp target add {target_name} --remote-root ...`."
        )

    requires_queue = "{queue}" in target.template_header
    requires_pe = "{parallel_environment}" in target.template_header
    if requires_queue and not queue:
        raise typer.BadParameter(
            "--queue is required (or set target default_queue) because template includes {queue}."
        )
    if requires_pe and not parallel_environment:
        raise typer.BadParameter(
            "--parallel-environment is required (or set target default_parallel_environment) because template includes {parallel_environment}."
        )

    remote_analysis_root = f"{target.remote_root.rstrip('/')}/{analysis_rel}"
    remote_runs_root = f"{target.remote_root.rstrip('/')}/{CONFIG_DIR}/{RUNS_DIR_NAME}/{analysis_rel}"
    resolved_working_dir = remote_analysis_root
    try:
        local_cwd_rel = Path.cwd().resolve().relative_to(analysis_dir.resolve()).as_posix()
    except ValueError:
        local_cwd_rel = "."
    if local_cwd_rel not in {"", "."}:
        resolved_working_dir = f"{remote_analysis_root.rstrip('/')}/{local_cwd_rel}"
    run_id = _deterministic_analysis_run_id(
        target_name=target_name,
        analysis_rel=analysis_rel,
        command=command,
        scheduler=target.scheduler,
        resources={
            "cpus": cpus,
            "memory": memory,
            "time": walltime,
            "node": node,
            "queue": queue,
            "parallel_environment": parallel_environment,
            "job_name": job_name,
        },
    )
    run_id = f"run-{run_id}"
    remote_run_dir = f"{remote_runs_root}/{run_id}"
    remote_submit_script = f"{remote_run_dir}/pc_submit.sh"
    remote_log_file = f"{remote_run_dir}/run.log"
    resolved_stdout = f"{remote_run_dir}/stdout"
    resolved_stderr = f"{remote_run_dir}/stderr"

    submit_script = _build_submit_script(
        header=_render_scheduler_header(
            target.template_header,
            cpus=cpus,
            memory=memory,
            walltime=walltime,
            job_name=job_name,
            stdout=resolved_stdout,
            stderr=resolved_stderr,
            working_dir=resolved_working_dir,
            queue=queue or "",
            node=node,
            parallel_environment=parallel_environment or "",
        ),
        command=command,
        working_dir=resolved_working_dir,
        remote_log_file=remote_log_file,
    )

    project_ignore = _ignore_file(project_root)
    local_target_mode = _uses_local_transport(target)
    if dry_run:
        typer.echo("Dry run: no changes will be made")
        typer.echo("")
        typer.echo(f"Project root: {project_root}")
        typer.echo(f"Analysis: {analysis_rel}")
        typer.echo(f"Target: {target_name}")
        typer.echo(f"Scheduler: {target.scheduler}")
        typer.echo(f"Remote analysis path: {remote_analysis_root}")
        typer.echo(f"Remote run path: {remote_run_dir}")
        if project_ignore.exists():
            typer.echo(f"Sync exclusions: {IGNORE_FILE_NAME} detected")
        else:
            typer.echo(f"Sync exclusions: {IGNORE_FILE_NAME} not found")
        typer.echo(f"Command: {command}")
        typer.echo("")
        typer.echo("Planned actions:")
        typer.echo("1. Sync analysis directory to remote target")
        typer.echo("2. Prepare scheduler submission in isolated run directory")
        typer.echo("3. Submit job to scheduler")
        typer.echo("4. Record job metadata locally")
        typer.echo("Record preview: includes identity, resources, paths, sync, environment, and optional git metadata")
        typer.echo("")
        typer.echo("Submission script preview:")
        for line in submit_script.splitlines()[:16]:
            typer.echo(line)
        if len(submit_script.splitlines()) > 16:
            typer.echo("...")
        return

    if local_target_mode:
        Path(remote_analysis_root).mkdir(parents=True, exist_ok=True)
    else:
        _run_cmd(["ssh", target.host, "mkdir", "-p", remote_analysis_root])

    rsync_cmd = ["rsync", "-az"]
    if project_ignore.exists():
        rsync_cmd.extend(["--exclude-from", str(project_ignore)])
    if local_target_mode:
        rsync_cmd.extend([f"{analysis_dir}/", f"{remote_analysis_root}/"])
    else:
        rsync_cmd.extend([f"{analysis_dir}/", f"{target.host}:{remote_analysis_root}/"])
    _run_cmd(rsync_cmd)

    if local_target_mode:
        Path(remote_run_dir).mkdir(parents=True, exist_ok=True)
        submit_path = Path(remote_submit_script)
        submit_path.parent.mkdir(parents=True, exist_ok=True)
        submit_path.write_text(submit_script)
        submit_path.chmod(0o755)
    else:
        _run_cmd(["ssh", target.host, "mkdir", "-p", remote_run_dir])
        _write_remote_script(target.host, remote_submit_script, submit_script, executable=True)

    adapter = get_adapter(target.scheduler)
    try:
        job_id = adapter.submit(target.host, remote_submit_script, transport=target.transport).job_id
    except Exception as exc:
        raise typer.BadParameter(f"Failed to submit job: {exc}") from exc

    now = datetime.now().isoformat(timespec="seconds")
    last_job = LastJob(
        job_id=job_id,
        target=target_name,
        scheduler=target.scheduler,
        remote_run_dir=remote_run_dir,
        remote_log_file=remote_log_file,
        run_id=run_id,
    )

    state = load_state(project_root)
    state["last_job"] = last_job.__dict__
    state["last_updated"] = now
    save_state(project_root, state)

    append_job_record(
        project_root,
        _build_job_record(
            project_root=project_root,
            submitted_at=now,
            analysis=analysis_rel,
            analysis_tags=cfg.analysis_tags.get(analysis_rel, []),
            target_name=target_name,
            scheduler=target.scheduler,
            job_id=job_id,
            run_id=run_id,
            job_name=job_name,
            state=("RUNNING" if target.scheduler == "none" else "RUNNING_OR_QUEUED"),
            command=command,
            remote_run_dir=remote_run_dir,
            remote_log_file=remote_log_file,
            working_dir=resolved_working_dir,
            cpus=cpus,
            memory=memory,
            walltime=walltime,
            node=node,
            queue=queue,
            parallel_environment=parallel_environment,
            profile_name=profile_name,
            submit_script_path=remote_submit_script,
            submission_mode="single",
            sync_source=str(analysis_dir.resolve()),
            sync_destination=remote_analysis_root,
            ignore_file_path=(str(project_ignore) if project_ignore.exists() else None),
            ignore_used=project_ignore.exists(),
            extra={
                "stdout": resolved_stdout,
                "stderr": resolved_stderr,
            },
        ),
    )

    typer.echo(f"Submitted job_id={job_id} target={target_name}")
    typer.echo(f"Run id: {run_id}")
    typer.echo(f"Job name: {job_name}")
    typer.echo(f"Node: {node}")
    typer.echo(f"Remote run dir: {remote_run_dir}")
    typer.echo(f"Remote log: {remote_log_file}")
    if index_after:
        manifest_path = _auto_index_active_root(project_root, cfg, target_name, target)
        if manifest_path:
            typer.echo(f"Auto-indexed active analysis root: {manifest_path}")


@sync_app.command("push")
def sync_push(
    dry_run: bool = typer.Option(False, "--dry-run", help="Preview sync actions without any modifications"),
) -> None:
    """Push active analysis directory to the active target."""
    _sync_push(dry_run=dry_run)


@sync_app.command("pull")
def sync_pull(
    remote: bool = typer.Option(
        False, "--remote", help="Pull tagged paths even when they are not present locally (remote-only tags)"
    ),
    all_paths: bool = typer.Option(False, "--all", help="Pull the full active analysis directory"),
    dry_run: bool = typer.Option(False, "--dry-run", help="Preview sync actions without any modifications"),
    index_after: bool = typer.Option(False, "--index-after", help="Refresh root index manifest after successful pull"),
    yes: bool = typer.Option(False, "--yes", help="Confirm destructive pull operations"),
) -> None:
    """Pull active analysis data from the active target."""
    _sync_pull(remote=remote, all_paths=all_paths, dry_run=dry_run, index_after=index_after, yes=yes)


@sync_app.command("status")
def sync_status(
    limit: int = typer.Option(20, "--limit", min=1, help="Maximum sync events to display"),
    target: Optional[str] = typer.Option(
        None, "--target", help="Filter by target name", autocompletion=_complete_target_names
    ),
    analysis: Optional[str] = typer.Option(
        None, "--analysis", help="Filter by analysis path", autocompletion=_complete_record_analyses
    ),
    action: Optional[str] = typer.Option(None, "--action", help="Filter by action: push|pull"),
    as_json: bool = typer.Option(False, "--json", help="Render matched events as JSON"),
) -> None:
    """Show recorded sync events."""
    project_root = _project_root()
    events = _load_sync_events(project_root)
    if not events:
        typer.echo("No sync events recorded")
        return

    action_filter: Optional[str] = None
    if action:
        action_lc = action.strip().lower()
        if action_lc not in {"push", "pull"}:
            raise typer.BadParameter("--action must be one of: push, pull")
        action_filter = action_lc

    def _matches(event: dict[str, Any]) -> bool:
        if target and str(event.get("target", "")) != target:
            return False
        if analysis and str(event.get("analysis", "")) != analysis:
            return False
        if action_filter and str(event.get("action", "")).lower() != action_filter:
            return False
        return True

    matched = [event for event in events if _matches(event)][:limit]
    if not matched:
        typer.echo("No sync events found for the provided filters")
        return

    if as_json:
        for event in matched:
            event.pop("_record_file", None)
        typer.echo(json.dumps(matched, indent=2))
        return

    typer.echo("Sync events:")
    for event in matched:
        typer.echo(
            f"{str(event.get('recorded_at', ''))}  action={str(event.get('action', ''))} "
            f"target={str(event.get('target', ''))} analysis={str(event.get('analysis', ''))} "
            f"mode={str(event.get('mode', ''))} source={str(event.get('source', ''))} "
            f"destination={str(event.get('destination', ''))}"
        )


@app.command("index")
def index_remote(
    path: str = typer.Argument(
        ".",
        help="Nested path inside active analysis directory to index.",
        autocompletion=_complete_active_analysis_paths,
    ),
    remote_path: Optional[str] = typer.Option(
        None,
        "--remote-path",
        help="Remote relative path under the active analysis root to index (can include remote-only directories).",
        autocompletion=_complete_index_remote_paths,
    ),
    all_tags: bool = typer.Option(False, "--all-tags", help="Index all tagged paths for the active analysis"),
    as_json: bool = typer.Option(False, "--json", help="Print resulting index payload as JSON"),
) -> None:
    """Create metadata-only index manifests for remote analysis paths without pulling files."""
    project_root, cfg, analysis_dir, target_name, target_cfg, analysis_rel, remote_analysis_root = _resolve_sync_context()
    scopes = _resolve_index_scopes(
        analysis_dir=analysis_dir,
        analysis_rel=analysis_rel,
        target_cfg=target_cfg,
        remote_analysis_root=remote_analysis_root,
        local_path=path,
        remote_path=remote_path,
        all_tags=all_tags,
        analysis_tags=cfg.analysis_tags.get(cfg.active_analysis or "", []),
    )

    indexed_manifests: list[dict[str, Any]] = []
    for scope in scopes:
        scope_rel = scope["scope"]
        scope_remote = scope["remote_scope"]
        manifest = _write_index_manifest_for_scope(
            project_root=project_root,
            analysis_rel=analysis_rel,
            target_name=target_name,
            target_cfg=target_cfg,
            scope_rel=scope_rel,
            scope_remote=scope_remote,
        )
        indexed_manifests.append({"scope": scope_rel, "manifest_path": str(manifest.get("_manifest_path", "")), **manifest})

    if as_json:
        typer.echo(json.dumps(indexed_manifests, indent=2))
        return

    typer.echo("Index complete (metadata only, no file transfer).")
    for item in indexed_manifests:
        typer.echo(
            f"- scope={item['scope']} entries={item['entry_count']} remote={item['remote_scope']} manifest={item['manifest_path']}"
        )


@config_app.command("show")
def config_show() -> None:
    """Show resolved project configuration context."""
    project_root = _project_root()
    cfg = load_config(project_root)
    snapshot = _project_config_snapshot(project_root, cfg)

    typer.echo(f"Project root: {snapshot['project_root']}")
    typer.echo(f"Config path: {snapshot['config_path']}")
    typer.echo(f"Default target: {snapshot['default_target']}")
    active_target = snapshot.get("active_target")
    if active_target:
        typer.echo(
            f"Active target: {active_target['name']} "
            f"(scheduler={active_target['scheduler']} transport={active_target['transport']} "
            f"host={active_target['host']} remote_root={active_target['remote_root']})"
        )
    else:
        typer.echo("Active target: <missing>")
    typer.echo(f"Active analysis: {snapshot.get('active_analysis') or '<unset>'}")
    typer.echo(f"Ignore file: {snapshot['ignore_file']}")
    typer.echo(f"Template path: {snapshot['template_path']}")
    typer.echo("Targets:")
    for name, payload in snapshot["targets"].items():
        marker = "*" if name == snapshot["default_target"] else " "
        typer.echo(
            f"{marker} {name}: scheduler={payload['scheduler']} transport={payload['transport']} "
            f"host={payload['host']} remote_root={payload['remote_root']}"
        )


@config_app.command("export")
def config_export(
    fmt: str = typer.Option("json", "--format", help="Output format: json|yaml"),
) -> None:
    """Export full configuration snapshot in JSON or YAML."""
    fmt_lc = fmt.strip().lower()
    if fmt_lc not in {"json", "yaml"}:
        raise typer.BadParameter("--format must be one of: json, yaml")

    project_root = _project_root()
    cfg = load_config(project_root)
    snapshot = _project_config_snapshot(project_root, cfg)
    if fmt_lc == "json":
        typer.echo(json.dumps(snapshot, indent=2))
    else:
        typer.echo(yaml.safe_dump(snapshot, sort_keys=False))


@app.command("export")
def export_bundle(
    output: Optional[Path] = typer.Option(None, "--output", help="Output tar.gz bundle path"),
    include_jobs: bool = typer.Option(False, "--jobs/--no-jobs", help="Include .cluster_dispatch/jobs records"),
    include_sync: bool = typer.Option(False, "--sync/--no-sync", help="Include .cluster_dispatch/sync records"),
    include_sweeps: bool = typer.Option(False, "--sweeps/--no-sweeps", help="Include .cluster_dispatch/sweeps manifests"),
    redact_hosts: bool = typer.Option(False, "--redact-hosts", help="Redact target host values in exported config"),
) -> None:
    """Export project metadata bundle as tar.gz."""
    project_root = _project_root()
    bundle_path = (output or Path(f"cdp-export-{datetime.now().strftime('%Y%m%d%H%M%S')}.tar.gz")).resolve()
    cfg = load_config(project_root)

    manifest = _export_manifest_payload(
        include_jobs=include_jobs,
        include_sync=include_sync,
        include_sweeps=include_sweeps,
        redact_hosts=redact_hosts,
    )

    files_to_add: list[Path] = []
    base = project_root / CONFIG_DIR
    config_file = base / CONFIG_NAME
    files_to_add.append(config_file)
    state_file = base / "state.json"
    if state_file.exists():
        files_to_add.append(state_file)
    ignore_file = project_root / IGNORE_FILE_NAME
    if ignore_file.exists():
        files_to_add.append(ignore_file)

    if include_jobs:
        files_to_add.extend(sorted((base / JOBS_DIR).glob("*.json")))
    if include_sync:
        files_to_add.extend(sorted((base / SYNC_EVENTS_DIR_NAME).glob("*.json")))
    if include_sweeps:
        files_to_add.extend(sorted((base / SWEEPS_DIR_NAME).glob("*.json")))

    with tarfile.open(bundle_path, "w:gz") as tar:
        manifest_bytes = json.dumps(manifest, indent=2).encode("utf-8")
        manifest_info = tarfile.TarInfo(name="export_manifest.json")
        manifest_info.size = len(manifest_bytes)
        tar.addfile(manifest_info, io.BytesIO(manifest_bytes))

        if redact_hosts:
            raw_cfg = yaml.safe_load(config_file.read_text()) or {}
            targets = raw_cfg.get("targets", {})
            if isinstance(targets, dict):
                for target_payload in targets.values():
                    if isinstance(target_payload, dict) and "host" in target_payload:
                        target_payload["host"] = "<redacted>"
            redacted = yaml.safe_dump(raw_cfg, sort_keys=False).encode("utf-8")
            cfg_info = tarfile.TarInfo(name=f"{CONFIG_DIR}/{CONFIG_NAME}")
            cfg_info.size = len(redacted)
            tar.addfile(cfg_info, io.BytesIO(redacted))
            skip_config = True
        else:
            skip_config = False

        for path in files_to_add:
            if skip_config and path == config_file:
                continue
            if not path.exists() or not path.is_file():
                continue
            arcname = path.relative_to(project_root).as_posix()
            tar.add(path, arcname=arcname)

    typer.echo(f"Exported bundle: {bundle_path}")
    typer.echo(
        f"Included: config,state,ignore"
        f"{',jobs' if include_jobs else ''}"
        f"{',sync' if include_sync else ''}"
        f"{',sweeps' if include_sweeps else ''}"
    )


@app.command("import")
def import_bundle(
    bundle: Path = typer.Argument(..., exists=True, dir_okay=False, readable=True, help="Path to export tar.gz bundle"),
    overwrite: bool = typer.Option(False, "--overwrite", help="Overwrite existing files"),
    include_jobs: bool = typer.Option(True, "--jobs/--no-jobs", help="Import jobs records from bundle"),
    include_sync: bool = typer.Option(True, "--sync/--no-sync", help="Import sync records from bundle"),
    include_sweeps: bool = typer.Option(True, "--sweeps/--no-sweeps", help="Import sweeps manifests from bundle"),
    yes: bool = typer.Option(False, "--yes", help="Confirm overwrite behavior"),
) -> None:
    """Import project metadata bundle into current directory."""
    dest_root = Path.cwd().resolve()
    allowed_prefixes = {f"{CONFIG_DIR}/", IGNORE_FILE_NAME, "export_manifest.json"}
    included_sections = {
        JOBS_DIR: include_jobs,
        SYNC_EVENTS_DIR_NAME: include_sync,
        SWEEPS_DIR_NAME: include_sweeps,
    }
    if overwrite:
        _require_yes(yes, "Import with --overwrite is destructive. Re-run with --yes to confirm.")

    extracted = 0
    skipped = 0
    with tarfile.open(bundle, "r:gz") as tar:
        members = [m for m in tar.getmembers() if m.isfile()]
        for member in members:
            name = member.name.strip("/")
            if not name:
                skipped += 1
                continue
            if ".." in Path(name).parts:
                skipped += 1
                continue
            if not any(name == p or name.startswith(p) for p in allowed_prefixes):
                skipped += 1
                continue
            if name.startswith(f"{CONFIG_DIR}/{JOBS_DIR}/") and not included_sections[JOBS_DIR]:
                skipped += 1
                continue
            if name.startswith(f"{CONFIG_DIR}/{SYNC_EVENTS_DIR_NAME}/") and not included_sections[SYNC_EVENTS_DIR_NAME]:
                skipped += 1
                continue
            if name.startswith(f"{CONFIG_DIR}/{SWEEPS_DIR_NAME}/") and not included_sections[SWEEPS_DIR_NAME]:
                skipped += 1
                continue

            out_path = dest_root / name
            if out_path.exists() and not overwrite:
                raise typer.BadParameter(
                    f"Refusing to overwrite existing file: {out_path}. Re-run with --overwrite."
                )
            out_path.parent.mkdir(parents=True, exist_ok=True)
            source = tar.extractfile(member)
            if source is None:
                skipped += 1
                continue
            out_path.write_bytes(source.read())
            extracted += 1

    typer.echo(f"Imported bundle: {bundle}")
    typer.echo(f"Files extracted: {extracted}")
    if skipped:
        typer.echo(f"Files skipped: {skipped}")


@cleanup_app.command("records")
def cleanup_records(
    jobs: bool = typer.Option(True, "--jobs/--no-jobs", help="Clean up job records"),
    sync: bool = typer.Option(True, "--sync/--no-sync", help="Clean up sync event records"),
    sweeps: bool = typer.Option(False, "--sweeps/--no-sweeps", help="Clean up sweep manifests"),
    older_than_days: Optional[int] = typer.Option(
        None, "--older-than-days", min=0, help="Delete records older than this many days"
    ),
    keep_last: int = typer.Option(0, "--keep-last", min=0, help="Always keep most recent N records per category"),
    target: Optional[str] = typer.Option(
        None, "--target", help="Filter by target name", autocompletion=_complete_target_names
    ),
    analysis: Optional[str] = typer.Option(
        None, "--analysis", help="Filter by analysis path", autocompletion=_complete_record_analyses
    ),
    dry_run: bool = typer.Option(True, "--dry-run/--apply", help="Preview only by default; use --apply to delete"),
    as_json: bool = typer.Option(False, "--json", help="Print cleanup summary as JSON"),
    yes: bool = typer.Option(False, "--yes", help="Confirm record deletion when using --apply"),
) -> None:
    """Clean local metadata records with retention filters."""
    project_root = _project_root()
    cutoff: Optional[datetime] = None
    if older_than_days is not None:
        cutoff = datetime.now() - timedelta(days=older_than_days)
    if not dry_run:
        _require_yes(yes, "Cleanup --apply deletes records. Re-run with --yes to confirm.")

    summary: dict[str, Any] = {
        "dry_run": dry_run,
        "filters": {
            "target": target,
            "analysis": analysis,
            "older_than_days": older_than_days,
            "keep_last": keep_last,
            "jobs": jobs,
            "sync": sync,
            "sweeps": sweeps,
        },
        "categories": {},
    }

    def _candidate_paths(
        items: list[tuple[Path, Optional[datetime]]],
    ) -> list[Path]:
        ordered = sorted(
            items,
            key=lambda entry: (entry[1] is not None, entry[1] or datetime.min, entry[0].name),
            reverse=True,
        )
        keep_set: set[Path] = {path for path, _ in ordered[:keep_last]} if keep_last else set()
        candidates: list[Path] = []
        for path, ts in ordered:
            if path in keep_set:
                continue
            if cutoff is not None and (ts is None or ts >= cutoff):
                continue
            candidates.append(path)
        return candidates

    def _record_category_result(name: str, scanned: int, matched: int, deleted: list[str]) -> None:
        summary["categories"][name] = {
            "scanned": scanned,
            "matched": matched,
            "deleted": deleted,
            "deleted_count": len(deleted),
        }

    if jobs:
        job_entries: list[tuple[Path, Optional[datetime]]] = []
        jobs_dir = project_root / CONFIG_DIR / JOBS_DIR
        scanned = 0
        if jobs_dir.exists():
            for path in jobs_dir.glob("*.json"):
                scanned += 1
                try:
                    payload = json.loads(path.read_text())
                except json.JSONDecodeError:
                    continue
                if target and str(payload.get("target", "")) != target:
                    continue
                if analysis and str(payload.get("analysis", "")) != analysis:
                    continue
                job_entries.append((path, _parse_iso_ts(payload.get("submitted_at"))))
        candidates = _candidate_paths(job_entries)
        deleted_paths: list[str] = []
        for path in candidates:
            deleted_paths.append(str(path))
            if not dry_run:
                path.unlink(missing_ok=True)
        _record_category_result("jobs", scanned, len(job_entries), deleted_paths)

    if sync:
        sync_entries: list[tuple[Path, Optional[datetime]]] = []
        sync_dir = project_root / CONFIG_DIR / SYNC_EVENTS_DIR_NAME
        scanned = 0
        if sync_dir.exists():
            for path in sync_dir.glob("*.json"):
                scanned += 1
                try:
                    payload = json.loads(path.read_text())
                except json.JSONDecodeError:
                    continue
                if target and str(payload.get("target", "")) != target:
                    continue
                if analysis and str(payload.get("analysis", "")) != analysis:
                    continue
                sync_entries.append((path, _parse_iso_ts(payload.get("recorded_at"))))
        candidates = _candidate_paths(sync_entries)
        deleted_paths = []
        for path in candidates:
            deleted_paths.append(str(path))
            if not dry_run:
                path.unlink(missing_ok=True)
        _record_category_result("sync", scanned, len(sync_entries), deleted_paths)

    if sweeps:
        sweep_entries: list[tuple[Path, Optional[datetime]]] = []
        sweeps_dir = project_root / CONFIG_DIR / SWEEPS_DIR_NAME
        scanned = 0
        if sweeps_dir.exists():
            for path in sweeps_dir.glob("*.json"):
                scanned += 1
                try:
                    payload = json.loads(path.read_text())
                except json.JSONDecodeError:
                    continue
                if target and str(payload.get("target", "")) != target:
                    continue
                if analysis and str(payload.get("analysis", "")) != analysis:
                    continue
                sweep_entries.append((path, _parse_iso_ts(payload.get("created_at"))))
        candidates = _candidate_paths(sweep_entries)
        deleted_paths = []
        for path in candidates:
            deleted_paths.append(str(path))
            if not dry_run:
                path.unlink(missing_ok=True)
        _record_category_result("sweeps", scanned, len(sweep_entries), deleted_paths)

    total_deleted = sum(cat["deleted_count"] for cat in summary["categories"].values())
    summary["deleted_total"] = total_deleted
    if as_json:
        typer.echo(json.dumps(summary, indent=2))
        return

    typer.echo("Cleanup summary:")
    typer.echo(f"Mode: {'dry-run' if dry_run else 'apply'}")
    for name, cat in summary["categories"].items():
        typer.echo(
            f"- {name}: scanned={cat['scanned']} matched={cat['matched']} "
            f"to_delete={cat['deleted_count']}"
        )
    if total_deleted and dry_run:
        typer.echo("Use --apply to perform deletion.")


@analysis_app.command("pull")
def pull(
    remote: bool = typer.Option(
        False, "--remote", help="Pull tagged paths even when they are not present locally (remote-only tags)"
    ),
    index_after: bool = typer.Option(False, "--index-after", help="Refresh root index manifest after successful pull"),
    yes: bool = typer.Option(False, "--yes", help="Confirm destructive pull operations"),
) -> None:
    """Pull all tagged paths into the active analysis directory."""
    _sync_pull(remote=remote, all_paths=False, dry_run=False, index_after=index_after, yes=yes)


if __name__ == "__main__":
    app()
