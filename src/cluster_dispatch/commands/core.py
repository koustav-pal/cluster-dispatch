from __future__ import annotations

import shlex
import secrets
import subprocess
import os
import json
import shutil
import time
from datetime import datetime
from pathlib import Path
from typing import Annotated, Optional, Any

import typer
import yaml

from cluster_dispatch.commands.admin import register as register_admin_commands
from cluster_dispatch.commands.sweep import register as register_sweep_commands
from cluster_dispatch.analysis import (
    AnalysisError,
    add_analysis_tag,
    list_analysis_entries,
    remove_analysis_tag,
    set_active_analysis,
)
from cluster_dispatch.config import (
    CONFIG_DIR,
    CONFIG_NAME,
    ProjectConfig,
    TargetConfig,
    config_path,
    ensure_state_dirs,
    find_project_root,
    load_config,
    load_state,
    save_config,
)
from cluster_dispatch.context import Context, ContextError, resolve_context
from cluster_dispatch.diagnostics import diagnose_target
from cluster_dispatch.index import (
    auto_index_active_root as _auto_index_active_root_impl,
    load_index_manifest as _load_index_manifest,
    resolve_index_scopes as _resolve_index_scopes,
    summarize_index_vs_local as _summarize_index_vs_local,
    write_index_manifest_for_scope as _write_index_manifest_for_scope_impl,
)
from cluster_dispatch.models import IndexRequest, PullRequest, PushRequest, ResourceSpec, RunRequest
from cluster_dispatch.maintenance import prune_target_records, target_record_refs
from cluster_dispatch.jobs import (
    JobError,
    cancel_jobs,
    collect_outputs,
    filter_jobs,
    get_log,
    is_failed_state,
    is_running_state,
    prepare_retry,
    query_jobs,
    select_job,
    unique_jobs,
    poll_job,
)
from cluster_dispatch.operations import (
    OperationError,
    OperationRuntime,
    index as execute_index_operation,
    prepare_run as prepare_run_operation,
    pull as execute_pull_operation,
    push as execute_push_operation,
    run as execute_run_operation,
)
from cluster_dispatch.records import list_jobs, list_syncs
from cluster_dispatch.profiles import (
    DEFAULT_RESOURCE_PROFILES,
    ProfileError,
    all_resource_profiles,
    resource_spec_from_profile,
    validate_profile_name,
)
from cluster_dispatch.sweeps import (
    SweepRequest,
    SweepRuntime,
    validate_sweep as validate_sweep_operation,
)
from cluster_dispatch.transport import (
    is_local_transport,
    ssh_command,
)

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
BASE_REQUIRED_TEMPLATE_VARS = ("cpus", "memory", "time", "job_name", "stdout", "stderr", "working_dir")
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


def _write_remote_script(
    project_root: Path,
    cfg: ProjectConfig,
    target_name: str,
    target_cfg: TargetConfig,
    remote_path: str,
    content: str,
    executable: bool = True,
) -> None:
    command = f"cat > {shlex.quote(remote_path)}"
    if executable:
        command += f" && chmod +x {shlex.quote(remote_path)}"
    proc = _run_direct(
        ssh_command(project_root, cfg, target_name, target_cfg, command),
        input=content,
        text=True,
        capture_output=True,
        check=False,
    )
    if proc.returncode != 0:
        detail = (proc.stderr or proc.stdout or "unknown remote write error").strip()
        raise typer.BadParameter(f"Failed to write remote script {remote_path}: {detail}")


def _uses_local_transport(target: TargetConfig) -> bool:
    return is_local_transport(target.transport, target.host)


def _validate_transport(value: str) -> str:
    transport = value.strip().lower()
    if transport not in {"ssh", "local"}:
        raise typer.BadParameter("transport must be one of: ssh, local")
    return transport


def _all_resource_profiles(cfg: ProjectConfig) -> dict[str, dict[str, str | int]]:
    return all_resource_profiles(cfg)


def _validate_profile_name(name: str) -> str:
    try:
        return validate_profile_name(name)
    except ProfileError as exc:
        raise typer.BadParameter(str(exc)) from exc


def _resource_spec_from_profile(
    profile: Optional[str],
    cfg: ProjectConfig,
    target: TargetConfig,
    **overrides: Any,
) -> ResourceSpec:
    try:
        return resource_spec_from_profile(profile, cfg, target, **overrides)
    except ProfileError as exc:
        raise typer.BadParameter(str(exc)) from exc


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
        records = list_jobs(project_root)
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
        records = list_jobs(project_root)
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
        records = list_jobs(project_root)
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


def _write_index_manifest_for_scope(**kwargs: Any) -> dict[str, Any]:
    return _write_index_manifest_for_scope_impl(**kwargs, run_direct=_run_direct)


def _auto_index_active_root(
    project_root: Path,
    cfg: ProjectConfig,
    target_name: str,
    target_cfg: TargetConfig,
) -> Optional[str]:
    return _auto_index_active_root_impl(
        project_root,
        cfg,
        target_name,
        target_cfg,
        run_direct=_run_direct,
    )


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
    return list_syncs(project_root)


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


def _validation_summary(checks: list[dict[str, str]]) -> tuple[int, int, int]:
    pass_count = len([c for c in checks if c["status"] == "PASS"])
    warn_count = len([c for c in checks if c["status"] == "WARN"])
    fail_count = len([c for c in checks if c["status"] == "FAIL"])
    return pass_count, warn_count, fail_count


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

    refs = target_record_refs(project_root, name)

    if (refs.jobs or refs.sync) and not force:
        raise typer.BadParameter(
            f"Target '{name}' is referenced by {len(refs.jobs)} job record(s) and "
            f"{len(refs.sync)} sync record(s). Use --force to remove."
        )
    if force and (refs.jobs or refs.sync):
        _require_yes(
            yes,
            f"Target '{name}' has {len(refs.jobs)} job record(s) and {len(refs.sync)} sync record(s). "
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

    pruned_jobs = pruned_sync = 0
    if prune_records:
        _require_yes(yes, "Pruning local records is destructive. Re-run with --yes to confirm.")
        pruned_jobs, pruned_sync = prune_target_records(refs)

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
    result = diagnose_target(project_root, cfg, name, cfg.targets[name], remote=remote, create_root=create_root, run=_run_direct)
    checks = [{"status": item.status, "check": item.name, "detail": item.detail} for item in result.checks]
    summary = {"target": name, "pass": result.pass_count, "warn": result.warn_count, "fail": result.fail_count}
    if as_json:
        typer.echo(json.dumps({"summary": summary, "checks": checks}, indent=2))
    else:
        for check in checks:
            typer.echo(f"[{check['status']}] {check['check']}: {check['detail']}")
        typer.echo(f"Summary: PASS={result.pass_count} WARN={result.warn_count} FAIL={result.fail_count}")
    if result.failed:
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


def _sweep_runtime() -> SweepRuntime:
    return SweepRuntime(
        operation=OperationRuntime(
            run_command=_run_cmd,
            render_scheduler_header=_render_scheduler_header,
            write_remote_script=_write_remote_script,
            git_info=get_git_info,
        ),
        run_direct=_run_direct,
    )


@analysis_app.command("use")
def analysis_use(
    path: Annotated[Path, typer.Argument(help="Analysis directory under project root", autocompletion=_complete_analysis_dirs)],
) -> None:
    """Set active analysis directory."""
    try:
        active = set_active_analysis(_project_root(), path)
    except AnalysisError as exc:
        raise typer.BadParameter(str(exc)) from exc
    typer.echo(f"Active analysis set to {active}")


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
    try:
        tag_value, validation, added = add_analysis_tag(_project_root(), path, remote=remote, run=_run_direct)
    except AnalysisError as exc:
        raise typer.BadParameter(str(exc)) from exc
    if validation == "index":
        typer.echo(f"Validated via index: {tag_value}")
    typer.echo(f"Tagged analysis path ({'remote' if remote else 'local'}): {tag_value}" if added else f"Already tagged: {tag_value}")


@analysis_app.command("untag")
def analysis_untag(
    path: Annotated[
        Path, typer.Argument(help="Tagged path inside active analysis to remove", autocompletion=_complete_active_analysis_paths)
    ],
) -> None:
    """Remove a tagged path from the active analysis."""
    try:
        tag_value = remove_analysis_tag(_project_root(), path)
    except AnalysisError as exc:
        raise typer.BadParameter(str(exc)) from exc
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
    try:
        listing = list_analysis_entries(_project_root(), path, remote=remote, include_files=all_entries, run=_run_direct)
    except AnalysisError as exc:
        raise typer.BadParameter(str(exc)) from exc
    label = "entries" if all_entries else "directories"
    location = "Remote" if listing.remote else "Local"
    indexed = " (from index)" if listing.from_index else ""
    if not listing.entries:
        qualifier = "indexed " if listing.from_index else ""
        typer.echo(f"No {qualifier}{location.lower()} {'entries' if all_entries else 'subdirectories'} found in {listing.path}")
        if listing.manifest_path:
            typer.echo(f"Using index manifest: {listing.manifest_path}")
        return
    typer.echo(f"{location} {label} in {listing.path}{indexed}:")
    for name in listing.entries:
        typer.echo(f"- {name}")
    if listing.manifest_path:
        typer.echo(f"Index manifest: {listing.manifest_path}")


def _perform_run(operation_context: Context, request: RunRequest, *, dry_run: bool, index_after: bool) -> None:
    try:
        result = execute_run_operation(
            operation_context,
            request,
            dry_run=dry_run,
            runtime=OperationRuntime(
                run_command=_run_cmd,
                render_scheduler_header=_render_scheduler_header,
                write_remote_script=_write_remote_script,
                git_info=get_git_info,
            ),
        )
    except OperationError as exc:
        raise typer.BadParameter(str(exc)) from exc

    plan = result.plan
    if result.dry_run:
        typer.echo("Dry run: no changes will be made")
        typer.echo("")
        typer.echo(f"Project root: {operation_context.project_root}")
        typer.echo(f"Analysis: {operation_context.analysis.as_posix()}")
        typer.echo(f"Target: {operation_context.target_name}")
        typer.echo(f"Scheduler: {operation_context.target.scheduler}")
        typer.echo(f"Remote analysis path: {operation_context.remote_analysis}")
        typer.echo(f"Remote run path: {plan.paths.remote_run_dir}")
        typer.echo(
            f"Sync exclusions: {IGNORE_FILE_NAME} detected"
            if plan.push_plan.ignore_file
            else f"Sync exclusions: {IGNORE_FILE_NAME} not found"
        )
        typer.echo(f"Command: {request.command}")
        typer.echo("\nPlanned actions:")
        typer.echo("1. Sync analysis directory to remote target")
        typer.echo("2. Prepare scheduler submission in isolated run directory")
        typer.echo("3. Submit job to scheduler")
        typer.echo("4. Record job metadata locally")
        typer.echo("\nSubmission script preview:")
        for line in plan.submit_script.splitlines()[:16]:
            typer.echo(line)
        return

    typer.echo(f"Submitted job_id={result.job_id} target={operation_context.target_name}")
    typer.echo(f"Run id: {plan.run_id}")
    typer.echo(f"Job name: {request.job_name}")
    typer.echo(f"Node: {request.resources.node}")
    typer.echo(f"Remote run dir: {plan.paths.remote_run_dir}")
    typer.echo(f"Remote log: {plan.paths.remote_log_file}")
    if index_after:
        manifest_path = _auto_index_active_root(
            operation_context.project_root,
            operation_context.config,
            operation_context.target_name,
            operation_context.target,
        )
        if manifest_path:
            typer.echo(f"Auto-indexed active analysis root: {manifest_path}")


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
    try:
        operation_context = resolve_context()
        target = operation_context.target
        request = RunRequest(
            command=command,
            job_name=job_name or f"cdp-{secrets.token_hex(4)}",
            resources=_resource_spec_from_profile(
                profile,
                operation_context.config,
                target,
                cpus=cpus,
                memory=memory,
                walltime=job_time,
                node=node,
                queue=queue,
                parallel_environment=parallel_environment,
            ),
        )
    except ContextError as exc:
        raise typer.BadParameter(str(exc)) from exc
    _perform_run(operation_context, request, dry_run=dry_run, index_after=index_after)


def _show_last_status(follow: bool = False) -> None:
    """Show status of last submitted job."""
    project_root = _project_root()
    state = load_state(project_root)
    last = state.get("last_job")
    if not last:
        raise typer.BadParameter("No active job in state. Run `cdp analysis run ...` first.")

    job_id = last["job_id"]
    target = last["target"]

    rows = query_jobs(project_root, target=str(target), job_id=str(job_id), limit=1, refresh_state=True)
    state = str(rows[0].get("state", "UNKNOWN")) if rows else "UNKNOWN"
    typer.echo(f"job_id={job_id} target={target} state={state}")
    try:
        log = get_log(project_root, target=str(target), job_id=str(job_id), tail=50 if follow else 20, follow=follow, run=_run_direct)
    except JobError:
        return
    if follow:
        typer.echo(f"Following log: {log.path}")
        _run_direct(log.follow_command, check=True)
    elif log.content:
        typer.echo("--- log tail ---")
        typer.echo(log.content)


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
    matches = filter_jobs(project_root, job_id=job_id, job_name=job_name, target=target, analysis=analysis)
    if not matches:
        raise typer.BadParameter("No matching job records found for the provided filters")
    unique_matches = unique_jobs(matches)
    if len(unique_matches) > 1:
        _require_yes(
            yes,
            f"Filters match {len(unique_matches)} jobs. Re-run with --yes to cancel all matches.",
        )
    try:
        result = cancel_jobs(project_root, unique_matches, run=_run_direct)
    except JobError as exc:
        raise typer.BadParameter(str(exc)) from exc
    if result.cancelled:
        typer.echo("Cancelled jobs:")
        for line in result.cancelled:
            typer.echo(f"- {line}")
    if result.failed:
        typer.echo("Failed to cancel:")
        for line in result.failed:
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

    try:
        result = get_log(
            _project_root(), target=target, job_id=job_id, job_name=job_name, analysis=analysis,
            head=head, tail=tail, follow=follow, run=_run_direct,
        )
    except JobError as exc:
        raise typer.BadParameter(str(exc)) from exc
    if result.match_count > 1:
        typer.echo(f"Multiple matches found ({result.match_count}). Showing most recent match.")
    if follow:
        prefix = "local " if result.local else ""
        typer.echo(
            f"Following {prefix}log for job_id={result.record.get('job_id', '')} "
            f"job_name={result.record.get('job_name', '')}: {result.path}"
        )
        _run_direct(result.follow_command, check=True)
    elif result.content:
        typer.echo(result.content)
    else:
        typer.echo(f"No log output found at {result.path}")


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
    try:
        selected, match_count = select_job(
            project_root, target=target, job_id=job_id, job_name=job_name, analysis=analysis
        )
    except JobError as exc:
        raise typer.BadParameter(str(exc)) from exc
    if match_count > 1:
        typer.echo(f"Multiple matches found ({match_count}). Showing most recent match.")
    polls = 0
    while True:
        polls += 1
        update = poll_job(project_root, selected, tail_lines=tail_lines if show_log_tail else 0, run=_run_direct)
        typer.echo(
            f"[{update.timestamp}] job_id={selected.get('job_id', '')} job_name={selected.get('job_name', '')} "
            f"target={selected.get('target', '')} state={update.state}"
        )
        if update.log_tail:
            typer.echo("--- log tail ---")
            typer.echo(update.log_tail)
        if not is_running_state(update.state):
            if is_failed_state(update.state):
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
    records = query_jobs(_project_root(), target=target, analysis=analysis, job_id=job_id, job_name=job_name, limit=limit)
    if not records:
        typer.echo("No history records found for the provided filters" if any([target, analysis, job_id, job_name]) else "No job history found")
        return
    typer.echo("Job history:")
    for rec in records:
        typer.echo(
            f"{str(rec.get('submitted_at', ''))}  job_id={str(rec.get('job_id', ''))} "
            f"job_name={str(rec.get('job_name', ''))} target={str(rec.get('target', ''))} "
            f"scheduler={str(rec.get('scheduler', ''))} analysis={str(rec.get('analysis', ''))}"
        )


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
    """Validate through real run preparation path."""
    checks: list[dict[str, str]] = []
    command = " ".join(shlex.quote(arg) for arg in ctx.args) if ctx.args else ""
    if not command:
        checks.append({"status": "FAIL", "check": "command", "detail": "Missing command. Example: cdp run validate python main.py"})
    else:
        checks.append({"status": "PASS", "check": "command", "detail": command})
        try:
            operation_context = resolve_context()
            resources = _resource_spec_from_profile(
                profile,
                operation_context.config,
                operation_context.target,
                cpus=cpus,
                memory=memory,
                walltime=job_time,
                node=node,
                queue=queue,
                parallel_environment=parallel_environment,
            )
            request = RunRequest(
                command=command,
                job_name=job_name or f"cdp-{secrets.token_hex(4)}",
                resources=resources,
            )
            plan = prepare_run_operation(
                operation_context,
                request,
                render_scheduler_header=_render_scheduler_header,
            )
            checks.extend(
                [
                    {"status": "PASS", "check": "project_root", "detail": str(operation_context.project_root)},
                    {"status": "PASS", "check": "active_analysis", "detail": str(operation_context.local_analysis)},
                    {"status": "PASS", "check": "target", "detail": operation_context.target_name},
                    {
                        "status": "PASS",
                        "check": "resources",
                        "detail": (
                            f"cpus={resources.cpus} memory={resources.memory} time={resources.time} "
                            f"node={resources.node} queue={resources.queue} "
                            f"parallel_environment={resources.parallel_environment}"
                        ),
                    },
                    {"status": "PASS", "check": "run_prepare", "detail": plan.run_id},
                ]
            )
            tags = operation_context.config.analysis_tags.get(operation_context.analysis.as_posix(), [])
            checks.append(
                {
                    "status": "PASS" if tags else "WARN",
                    "check": "analysis_tags",
                    "detail": f"{len(tags)} tag(s)" if tags else "No analysis tags configured",
                }
            )
        except Exception as exc:
            checks.append({"status": "FAIL", "check": "run_prepare", "detail": str(exc)})

    pass_count, warn_count, fail_count = _validation_summary(checks)
    payload = {"checks": checks, "summary": {"pass": pass_count, "warn": warn_count, "fail": fail_count}}
    if as_json:
        typer.echo(json.dumps(payload, indent=2))
    else:
        for check in checks:
            typer.echo(f"[{check['status']}] {check['check']}: {check['detail']}")
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
    """Validate through real sweep/run preparation paths."""
    checks: list[dict[str, str]] = []
    command_template = " ".join(shlex.quote(arg) for arg in ctx.args) if ctx.args else ""
    runs_generated = 0
    if not command_template:
        checks.append({"status": "FAIL", "check": "command_template", "detail": "Missing command template"})
    else:
        checks.append({"status": "PASS", "check": "command_template", "detail": command_template})
        try:
            operation_context = resolve_context(target_name=target)
            resources = _resource_spec_from_profile(
                profile,
                operation_context.config,
                operation_context.target,
                cpus=cpus,
                memory=memory,
                walltime=job_time,
                node=node,
                queue=queue,
                parallel_environment=parallel_environment,
            )
            request = SweepRequest(
                config_file=config_file,
                command_template=command_template,
                mode=mode,
                resources=resources,
                block_filter=sweep_job,
            )
            runs_generated = validate_sweep_operation(
                operation_context,
                request,
                runtime=_sweep_runtime(),
            )
            checks.extend(
                [
                    {"status": "PASS", "check": "project", "detail": str(operation_context.project_root)},
                    {"status": "PASS", "check": "target", "detail": operation_context.target_name},
                    {"status": "PASS", "check": "sweep_prepare", "detail": f"Generated {runs_generated} run(s)"},
                ]
            )
        except Exception as exc:
            checks.append({"status": "FAIL", "check": "sweep_prepare", "detail": str(exc)})

    pass_count, warn_count, fail_count = _validation_summary(checks)
    payload = {
        "checks": checks,
        "summary": {"pass": pass_count, "warn": warn_count, "fail": fail_count},
        "runs_generated": runs_generated,
    }
    if as_json:
        typer.echo(json.dumps(payload, indent=2))
    else:
        for check in checks:
            typer.echo(f"[{check['status']}] {check['check']}: {check['detail']}")
        typer.echo(f"Summary: PASS={pass_count} WARN={warn_count} FAIL={fail_count} runs={runs_generated}")
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
    try:
        spec = prepare_retry(_project_root(), target=target, analysis=analysis, job_id=job_id, job_name=job_name)
    except JobError as exc:
        raise typer.BadParameter(str(exc)) from exc
    if spec.match_count > 1:
        typer.echo(f"Multiple matches found ({spec.match_count}). Retrying the most recent record.")
    _perform_run(spec.context, spec.request, dry_run=dry_run, index_after=False)


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
    try:
        result = collect_outputs(
            _project_root(), job_id=job_id, job_name=job_name, target=target, analysis=analysis, run=_run_direct
        )
    except JobError as exc:
        raise typer.BadParameter(str(exc)) from exc
    selected = result.record
    if result.match_count > 1:
        typer.echo(f"Multiple matches found ({result.match_count}). Collecting from most recent match.")
    typer.echo(
        f"Collect source: job_id={str(selected.get('job_id', ''))} job_name={str(selected.get('job_name', ''))} "
        f"target={str(selected.get('target', ''))} analysis={str(selected.get('analysis', '')).strip('/')}"
    )
    if result.estimated_files or result.estimated_bytes:
        typer.echo(
            f"Index transfer estimate: files={result.estimated_files} total_size={_humanize_bytes(result.estimated_bytes)}"
        )
    if result.index_missing:
        typer.echo("Index warnings (no index metadata for tagged paths):")
        for item in result.index_missing:
            typer.echo(f"- {item}")
    if result.pulled:
        typer.echo("Collected tagged paths:")
        for item in result.pulled:
            typer.echo(f"- {result.analysis_dir / item}")
    if result.skipped:
        typer.echo("Skipped tagged paths:")
        for item in result.skipped:
            typer.echo(f"- {item}")


def _collect_status_rows(
    project_root: Path,
    cfg: ProjectConfig,
    analysis_filter: Optional[str],
    target_filter: Optional[str],
    job_id_filter: Optional[str],
    job_name_filter: Optional[str],
    limit: int,
) -> list[dict[str, str]]:
    records = query_jobs(
        project_root,
        analysis=analysis_filter,
        target=target_filter,
        job_id=job_id_filter,
        job_name=job_name_filter,
        limit=limit,
        refresh_state=True,
    )
    return [
        {
            key: str(record.get(key, ""))
            for key in ("job_id", "job_name", "analysis", "target", "scheduler", "state", "submitted_at", "output_check")
        }
        for record in records
    ]


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


def _run_sync_push(dry_run: bool) -> None:
    try:
        operation_context = resolve_context()
        result = execute_push_operation(
            operation_context,
            PushRequest(dry_run=dry_run),
            run_command=_run_cmd,
        )
    except (ContextError, OperationError) as exc:
        raise typer.BadParameter(str(exc)) from exc

    plan = result.plan
    if result.dry_run:
        typer.echo("Dry run: no changes will be made")
        typer.echo("Action: sync push")
        typer.echo(f"Project root: {operation_context.project_root}")
        typer.echo(f"Analysis: {operation_context.analysis.as_posix()}")
        typer.echo(f"Target: {operation_context.target_name}")
        typer.echo(f"Transport: {operation_context.target.transport}")
        typer.echo(f"Source: {plan.source}/")
        typer.echo(f"Destination: {plan.destination}/")
        typer.echo(
            f"Sync exclusions: {IGNORE_FILE_NAME} detected"
            if plan.ignore_file
            else f"Sync exclusions: {IGNORE_FILE_NAME} not found"
        )
        typer.echo(f"Would run: {' '.join(shlex.quote(arg) for arg in (plan.mkdir_command or []))}")
        typer.echo(f"Would run: {' '.join(shlex.quote(arg) for arg in plan.commands[0])}")
        return
    typer.echo(
        f"Synced analysis to target '{operation_context.target_name}': "
        f"{operation_context.local_analysis} -> {operation_context.remote_analysis}"
    )
    typer.echo(f"Recorded sync event: {result.record_path}")


def _run_sync_pull(remote: bool, all_paths: bool, dry_run: bool, index_after: bool, yes: bool) -> None:
    try:
        operation_context = resolve_context()
        request = PullRequest(
            include_remote_only=remote,
            all_paths=all_paths,
            dry_run=dry_run,
            confirmed=yes,
        )
        result = execute_pull_operation(operation_context, request, run_command=_run_cmd)
    except (ContextError, OperationError) as exc:
        raise typer.BadParameter(str(exc)) from exc

    plan = result.plan
    root_index = _load_index_manifest(
        operation_context.project_root,
        operation_context.target_name,
        operation_context.analysis.as_posix(),
        ".",
    )
    if plan.mode == "all" and root_index is not None:
        delta = _summarize_index_vs_local(operation_context.local_analysis, root_index)
        label = "preview" if result.dry_run else "before pull"
        typer.echo(
            f"Index diff summary ({label}): remote_only={delta['remote_only']} "
            f"local_only={delta['local_only']} changed={delta['changed']}"
        )
    if result.dry_run:
        typer.echo("Dry run: no changes will be made")
        typer.echo("Action: sync pull")
        typer.echo(f"Project root: {operation_context.project_root}")
        typer.echo(f"Analysis: {operation_context.analysis.as_posix()}")
        typer.echo(f"Target: {operation_context.target_name}")
        typer.echo(f"Transport: {operation_context.target.transport}")
        typer.echo(f"Mode: {plan.mode}")
        for command in plan.commands:
            typer.echo(f"Would run: {' '.join(shlex.quote(arg) for arg in command)}")
        if not plan.commands:
            typer.echo("No tagged paths selected for pull")
        if result.skipped:
            typer.echo("Would skip tagged paths:")
            for item in result.skipped:
                typer.echo(f"- {item}")
        return
    if plan.mode == "all":
        typer.echo(
            f"Pulled full analysis from target '{operation_context.target_name}': "
            f"{operation_context.remote_analysis} -> {operation_context.local_analysis}"
        )
    else:
        if result.pulled:
            typer.echo("Pulled tagged paths:")
            for item in result.pulled:
                typer.echo(f"- {operation_context.local_analysis / item}")
        if result.skipped:
            typer.echo("Skipped tagged paths:")
            for item in result.skipped:
                typer.echo(f"- {item}")
    typer.echo(f"Recorded sync event: {result.record_path}")
    if index_after:
        manifest_path = _auto_index_active_root(
            operation_context.project_root,
            operation_context.config,
            operation_context.target_name,
            operation_context.target,
        )
        if manifest_path:
            typer.echo(f"Auto-indexed active analysis root: {manifest_path}")


@sync_app.command("push")
def sync_push(
    dry_run: bool = typer.Option(False, "--dry-run", help="Preview sync actions without any modifications"),
) -> None:
    """Push active analysis directory to the active target."""
    _run_sync_push(dry_run=dry_run)


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
    _run_sync_pull(remote=remote, all_paths=all_paths, dry_run=dry_run, index_after=index_after, yes=yes)


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
    dry_run: bool = typer.Option(False, "--dry-run", help="Preview index scopes without writing manifests"),
    as_json: bool = typer.Option(False, "--json", help="Print resulting index payload as JSON"),
) -> None:
    """Create metadata-only index manifests for remote analysis paths without pulling files."""
    try:
        operation_context = resolve_context()
        result = execute_index_operation(
            operation_context,
            IndexRequest(local_path=path, remote_path=remote_path, all_tags=all_tags, dry_run=dry_run),
            resolve_scopes=_resolve_index_scopes,
            write_manifest=_write_index_manifest_for_scope,
        )
    except (ContextError, OperationError) as exc:
        raise typer.BadParameter(str(exc)) from exc

    if result.dry_run:
        payload = {"dry_run": True, "scopes": result.plan.scopes}
        if as_json:
            typer.echo(json.dumps(payload, indent=2))
        else:
            typer.echo("Dry run: no changes will be made")
            for scope in result.plan.scopes:
                typer.echo(f"- scope={scope['scope']} remote={scope['remote_scope']}")
        return

    if as_json:
        typer.echo(json.dumps(result.manifests, indent=2))
        return

    typer.echo("Index complete (metadata only, no file transfer).")
    for item in result.manifests:
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


@analysis_app.command("pull")
def pull(
    remote: bool = typer.Option(
        False, "--remote", help="Pull tagged paths even when they are not present locally (remote-only tags)"
    ),
    index_after: bool = typer.Option(False, "--index-after", help="Refresh root index manifest after successful pull"),
    yes: bool = typer.Option(False, "--yes", help="Confirm destructive pull operations"),
) -> None:
    """Pull all tagged paths into the active analysis directory."""
    _run_sync_pull(remote=remote, all_paths=False, dry_run=False, index_after=index_after, yes=yes)


register_admin_commands(
    app,
    cleanup_app,
    project_root=_project_root,
    run_direct=_run_direct,
    complete_targets=_complete_target_names,
    complete_analyses=_complete_record_analyses,
    complete_job_ids=_complete_job_ids,
    complete_job_names=_complete_job_names,
    require_yes=_require_yes,
)
register_sweep_commands(
    sweep_app,
    project_root=_project_root,
    runtime=_sweep_runtime,
    resources=_resource_spec_from_profile,
    auto_index=_auto_index_active_root,
    complete_targets=_complete_target_names,
    complete_profiles=_complete_profile_names,
    complete_sweeps=_complete_sweep_ids,
)


if __name__ == "__main__":
    app()
