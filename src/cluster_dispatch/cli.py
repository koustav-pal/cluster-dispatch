from __future__ import annotations

import shlex
import secrets
import subprocess
import json
import re
import itertools
import hashlib
import base64
import shutil
from string import Formatter
from datetime import datetime
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
profile_app = typer.Typer(help="Manage resource profiles")
status_app = typer.Typer(help="Show job status", invoke_without_command=True)
app.add_typer(target_app, name="target")
app.add_typer(analysis_app, name="analysis")
analysis_app.add_typer(sweep_app, name="sweep")
app.add_typer(profile_app, name="profile")
app.add_typer(status_app, name="status")


TEMPLATES_DIR_NAME = "templates"
TEMPLATE_FILE_NAME = "scheduler_header.tmpl"
SWEEPS_DIR_NAME = "sweeps"
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


def _project_root() -> Path:
    return find_project_root()


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


def _run_cmd(cmd: list[str], check: bool = True) -> subprocess.CompletedProcess[str]:
    return subprocess.run(cmd, text=True, capture_output=True, check=check)


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
    lines: list[str] = ["#!/usr/bin/env bash", "set -euo pipefail"]
    if header.strip():
        lines.extend(header.strip().splitlines())
    lines.extend(
        [
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
    host: str = typer.Option(..., help="SSH host alias for the default target"),
    scheduler: str = typer.Option(..., help="Default scheduler for this project: sge|univa|pbs|slurm|lsf|none"),
    default_target: Optional[str] = typer.Option(
        None,
        "--target",
        help="Default target name (defaults to current project root directory name)",
    ),
    remote_root: str = typer.Option(..., help="Remote absolute root directory for this project"),
    default_cpus: int = typer.Option(1, "--cpus", help="Default CPUs for this target"),
    default_memory: str = typer.Option("8G", "--memory", help="Default memory for this target"),
    default_time: str = typer.Option("01:00:00", "--time", help="Default walltime for this target"),
    default_node: str = typer.Option("1", "--node", help="Default node value for this target"),
    default_queue: str = typer.Option("", "--queue", help="Default queue for this target"),
    default_parallel_environment: str = typer.Option(
        "", "--parallel-environment", help="Default parallel environment for this target"
    ),
    template_file: Path = typer.Option(
        ...,
        "--template-file",
        help=(
            "User-provided scheduler template file. Must include: "
            "{cpus}, {memory}, {time}, {job_name}, {stdout}, {stderr}, {working_dir}. "
            "Optional: {queue}, {node}, {parallel_environment}."
        ),
    ),
) -> None:
    """Initialize .cluster_dispatch/config.yml and local state directories."""
    scheduler = scheduler.lower()
    if scheduler not in {"sge", "univa", "pbs", "slurm", "lsf", "none"}:
        raise typer.BadParameter("scheduler must be one of: sge, univa, pbs, slurm, lsf, none")
    if not Path(remote_root).is_absolute():
        raise typer.BadParameter("remote_root must be an absolute remote path")

    project_root = Path.cwd()
    resolved_default_target = default_target or project_root.resolve().name
    if not resolved_default_target:
        resolved_default_target = "default"
    cfg_path = config_path(project_root)

    if cfg_path.exists():
        raise typer.BadParameter(f"{CONFIG_DIR}/{CONFIG_NAME} already exists in this directory")
    if not template_file.exists() or not template_file.is_file():
        raise typer.BadParameter(f"Template file not found: {template_file}")

    user_template = template_file.read_text()
    _validate_template_variables(user_template)
    _run_cmd(["ssh", host, "mkdir", "-p", remote_root])

    cfg = ProjectConfig(scheduler=scheduler, default_target=resolved_default_target)
    cfg.targets[resolved_default_target] = TargetConfig(
        host=host,
        scheduler=scheduler,
        remote_root=remote_root,
        template_header=user_template,
        default_cpus=default_cpus,
        default_memory=default_memory,
        default_time=default_time,
        default_node=default_node,
        default_queue=default_queue,
        default_parallel_environment=default_parallel_environment,
    )
    save_config(project_root, cfg)
    ensure_state_dirs(project_root)
    template_path = _template_path(project_root)
    template_path.parent.mkdir(parents=True, exist_ok=True)
    template_path.write_text(user_template)
    typer.echo(f"Initialized {cfg_path}")
    typer.echo(f"Wrote scheduler template: {template_path}")


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
    template_file: Optional[Path] = typer.Option(None, help="Optional file containing scheduler header template"),
) -> None:
    """Add or update a compute target."""
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
    resolved_host = host or (existing.host if existing else None)
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
        typer.echo(f"{marker} {name}: host={t.host} scheduler={t.scheduler}")


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

    for binary in ("ssh", "rsync"):
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

        ssh_probe = subprocess.run(
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

        root_probe = subprocess.run(
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
            cmd_probe = subprocess.run(
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
    submitted_at: str,
    scheduler_override: Optional[str] = None,
    state_override: Optional[str] = None,
) -> None:
    scheduler_value = scheduler_override or target.scheduler
    state_value = state_override or "RUNNING_OR_QUEUED"
    append_job_record(
        project_root,
        {
            "submitted_at": submitted_at,
            "analysis": cfg.active_analysis,
            "analysis_tags": cfg.analysis_tags.get(cfg.active_analysis or "", []),
            "target": target_name,
            "scheduler": scheduler_value,
            "job_id": run.get("job_id", ""),
            "job_name": run.get("job_name", ""),
            "state": state_value,
            "stdout": run.get("job_name", ""),
            "stderr": run.get("job_name", ""),
            "working_dir": target.remote_root,
            "cpus": run.get("resources", {}).get("cpus"),
            "memory": run.get("resources", {}).get("memory"),
            "time": run.get("resources", {}).get("time"),
            "queue": run.get("resources", {}).get("queue"),
            "parallel_environment": run.get("resources", {}).get("parallel_environment"),
            "node": run.get("resources", {}).get("node"),
            "command": run.get("command", ""),
            "remote_run_dir": remote_sweep_dir,
            "remote_log_file": remote_log_file,
            "sweep_id": sweep_id,
            "run_id": run.get("run_id", ""),
            "sweep_job": run.get("sweep_job", ""),
            "sweep_index": run.get("sweep_index"),
            "sweep_params": run.get("sweep_params", {}),
            "submission_mode": submission_mode,
        },
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
) -> int:
    remote_sweep_dir = str(manifest["remote_sweep_dir"])
    _run_cmd(["ssh", target.host, "mkdir", "-p", remote_sweep_dir])
    adapter = get_adapter(target.scheduler)
    submitted_count = 0

    for idx in run_indexes:
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
                working_dir=target.remote_root,
                queue=str(resources["queue"]),
                node=str(resources["node"]),
                parallel_environment=str(resources["parallel_environment"]),
            ),
            command=str(run["command"]),
            working_dir=target.remote_root,
            remote_log_file=remote_log_file,
        )
        _run_cmd(
            [
                "ssh",
                target.host,
                "bash",
                "-lc",
                f"cat > {shlex.quote(remote_submit_script)} <<'PC_EOF'\n{submit_script}PC_EOF\nchmod +x {shlex.quote(remote_submit_script)}",
            ]
        )
        submit_result = adapter.submit(target.host, remote_submit_script)
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
            submitted_at=submitted_at,
        )
        submitted_count += 1
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
        working_dir=target.remote_root,
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
        working_dir=target.remote_root,
        remote_log_file=remote_log_file,
    )
    _run_cmd(
        [
            "ssh",
            target.host,
            "bash",
            "-lc",
            f"cat > {shlex.quote(remote_array_submit)} <<'PC_EOF'\n{array_script}PC_EOF\nchmod +x {shlex.quote(remote_array_submit)}",
        ]
    )

    adapter = get_adapter(target.scheduler)
    submit_result = adapter.submit(target.host, remote_array_submit)
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
            submitted_at=submitted_at,
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
    local_sweep_dir.mkdir(parents=True, exist_ok=True)
    submitted_count = 0

    for idx in run_indexes:
        run = manifest["runs"][idx]
        run_slug = _slug_component(str(run["sweep_job"]))
        run_name = f"{base_job_name}-{run_slug}-{int(run['sweep_index']):03d}"
        local_log_file = local_sweep_dir / f"run_{run['run_id']}.log"
        submitted_at = datetime.now().isoformat(timespec="seconds")

        proc = subprocess.run(
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
) -> None:
    """Run a sweep and persist manifest in .cluster_dispatch/sweeps."""
    mode_lc = mode.lower()
    if mode_lc not in {"single", "array", "local"}:
        raise typer.BadParameter("--mode must be one of: single, array, local")
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
        "cpus": resolved_cpus,
        "memory": resolved_memory,
        "time": resolved_time,
        "node": resolved_node,
        "queue": resolved_queue or "",
        "parallel_environment": resolved_pe or "",
    }
    manifest["resources"] = resources
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
        proc = subprocess.run(["ssh", target_cfg.host, cmd], text=True, capture_output=True, check=False)
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
            proc = subprocess.run(["ssh", target_cfg.host, cmd], text=True, capture_output=True, check=False)
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
        remote_cmd = f"if [ -e {shlex.quote(remote_tag_path)} ]; then echo OK; else echo MISSING; fi"
        proc = subprocess.run(
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

    remote_cmd = (
        f"P={shlex.quote(remote_list_root)}; "
        'if [ -d "$P" ]; then '
        + ('for d in "$P"/*; do basename "$d"; done | sort; ' if all_entries else 'for d in "$P"/*; do [ -d "$d" ] && basename "$d"; done | sort; ')
        + "fi"
    )
    proc = subprocess.run(
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
    resolved_working_dir = target.remote_root
    resolved_node = node or str(profile_values.get("node", target.default_node or "1"))
    resolved_queue = queue if queue is not None else str(profile_values.get("queue", target.default_queue))
    resolved_parallel_environment = (
        parallel_environment
        if parallel_environment is not None
        else str(profile_values.get("parallel_environment", target.default_parallel_environment))
    )

    requires_queue = "{queue}" in target.template_header
    requires_pe = "{parallel_environment}" in target.template_header
    if requires_queue and not resolved_queue:
        raise typer.BadParameter(
            "--queue is required (or set target default_queue) because template includes {queue}."
        )
    if requires_pe and not resolved_parallel_environment:
        raise typer.BadParameter(
            "--parallel-environment is required (or set target default_parallel_environment) because template includes {parallel_environment}."
        )

    analysis_rel = (cfg.active_analysis or "").strip("/")
    if not analysis_rel:
        raise typer.BadParameter("Active analysis path is empty. Re-run: cdp analysis use <path>")
    remote_analysis_root = f"{target.remote_root.rstrip('/')}/{analysis_rel}"
    run_id = _deterministic_analysis_run_id(
        target_name=target_name,
        analysis_rel=analysis_rel,
        command=command,
        scheduler=target.scheduler,
        resources={
            "cpus": resolved_cpus,
            "memory": resolved_memory,
            "time": resolved_time,
            "node": resolved_node,
            "queue": resolved_queue,
            "parallel_environment": resolved_parallel_environment,
            "job_name": resolved_job_name,
        },
    )
    run_id = f"run-{run_id}"
    remote_run_dir = f"{remote_analysis_root}/{run_id}"
    remote_submit_script = f"{remote_run_dir}/pc_submit.sh"
    remote_log_file = f"{remote_run_dir}/run.log"
    resolved_stdout = f"{remote_run_dir}/stdout"
    resolved_stderr = f"{remote_run_dir}/stderr"

    project_ignore = project_root / ".pcignore"

    _run_cmd(["ssh", target.host, "mkdir", "-p", remote_run_dir])

    rsync_cmd = ["rsync", "-az", "--delete"]
    if project_ignore.exists():
        rsync_cmd.extend(["--exclude-from", str(project_ignore)])
    rsync_cmd.extend([f"{analysis_dir}/", f"{target.host}:{remote_analysis_root}/"])
    _run_cmd(rsync_cmd)

    submit_script = _build_submit_script(
        header=_render_scheduler_header(
            target.template_header,
            cpus=resolved_cpus,
            memory=resolved_memory,
            walltime=resolved_time,
            job_name=resolved_job_name,
            stdout=resolved_stdout,
            stderr=resolved_stderr,
            working_dir=resolved_working_dir,
            queue=resolved_queue or "",
            node=resolved_node,
            parallel_environment=resolved_parallel_environment or "",
        ),
        command=command,
        working_dir=resolved_working_dir,
        remote_log_file=remote_log_file,
    )

    _run_cmd(
        [
            "ssh",
            target.host,
            "bash",
            "-lc",
            f"cat > {shlex.quote(remote_submit_script)} <<'PC_EOF'\n{submit_script}PC_EOF\nchmod +x {shlex.quote(remote_submit_script)}",
        ]
    )

    adapter = get_adapter(target.scheduler)
    job_id = adapter.submit(target.host, remote_submit_script).job_id

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
        {
            "submitted_at": now,
            "analysis": cfg.active_analysis,
            "analysis_tags": cfg.analysis_tags.get(cfg.active_analysis or "", []),
            "target": target_name,
            "scheduler": target.scheduler,
            "job_id": job_id,
            "run_id": run_id,
            "job_name": resolved_job_name,
            "state": ("RUNNING" if target.scheduler == "none" else "RUNNING_OR_QUEUED"),
            "stdout": resolved_stdout,
            "stderr": resolved_stderr,
            "working_dir": resolved_working_dir,
            "cpus": resolved_cpus,
            "memory": resolved_memory,
            "time": resolved_time,
            "queue": resolved_queue,
            "parallel_environment": resolved_parallel_environment,
            "node": resolved_node,
            "command": command,
            "remote_run_dir": remote_run_dir,
            "remote_log_file": remote_log_file,
        },
    )

    typer.echo(f"Submitted job_id={job_id} target={target_name}")
    typer.echo(f"Run id: {run_id}")
    typer.echo(f"Job name: {resolved_job_name}")
    typer.echo(f"Node: {resolved_node}")
    typer.echo(f"Remote run dir: {remote_run_dir}")
    typer.echo(f"Remote log: {remote_log_file}")


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
        result = adapter.status(target_cfg.host, job_id)
        state = result.state
    typer.echo(f"job_id={job_id} target={target} state={state}")

    if follow:
        typer.echo(f"Following log: {last['remote_log_file']}")
        subprocess.run(
            ["ssh", "-t", target_cfg.host, "bash", "-lc", f"tail -n 50 -f {shlex.quote(last['remote_log_file'])}"],
            check=True,
        )
    else:
        proc = subprocess.run(
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
    status_result = adapter.status(cfg.targets[target_name].host, job_id)
    state = status_result.state
    if state == "NOT_FOUND":
        stored_final_state = str(payload.get("final_state", "")).strip()
        if stored_final_state:
            state = stored_final_state
        else:
            accounting_result = adapter.accounting_status(cfg.targets[target_name].host, job_id)
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
        subprocess.run(["tail", "-n", str(line_count), "-f", str(log_file)], check=True)
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
        proc = subprocess.run(
            ["ssh", cfg.targets[rec_target].host, cancel_cmd],
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
        subprocess.run(["ssh", "-t", host, remote_cmd], check=True)
        return

    proc = subprocess.run(["ssh", host, remote_cmd], text=True, capture_output=True, check=False)
    if proc.returncode != 0:
        raise typer.BadParameter(proc.stderr.strip() or "Failed to fetch remote log")

    if not proc.stdout.strip():
        typer.echo(f"No log output found at {remote_log_file}")
        return
    typer.echo(proc.stdout.rstrip())


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
    pulled: list[str] = []
    skipped: list[str] = []
    for tag_dir in tags:
        clean_tag = str(tag_dir).strip("/")
        if not clean_tag:
            continue
        tagged_remote = f"{remote_run_dir}/./{clean_tag}"
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
    if pulled:
        typer.echo("Collected tagged paths:")
        for item in pulled:
            typer.echo(f"- {analysis_dir / item}")
    if skipped:
        typer.echo("Skipped tagged paths:")
        for item in skipped:
            typer.echo(f"- {item}")


def _fetch_job_stats(host: str, scheduler: str, job_id: str) -> tuple[dict[str, str], str]:
    scheduler_lc = scheduler.lower()
    quoted_job_id = shlex.quote(job_id)

    if scheduler_lc in {"sge", "univa"}:
        proc = subprocess.run(
            ["ssh", host, "bash", "-lc", f"qacct -j {quoted_job_id}"],
            text=True,
            capture_output=True,
            check=False,
        )
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
        proc = subprocess.run(
            ["ssh", host, "bash", "-lc", f"qstat -xf {quoted_job_id}"],
            text=True,
            capture_output=True,
            check=False,
        )
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
        proc = subprocess.run(
            [
                "ssh",
                host,
                "bash",
                "-lc",
                f"sacct -j {quoted_job_id} -X -P -n -o JobIDRaw,State,ExitCode,Elapsed,TotalCPU,AllocCPUS,MaxRSS,MaxVMSize,AveRSS,ReqMem",
            ],
            text=True,
            capture_output=True,
            check=False,
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
        proc = subprocess.run(
            [
                "ssh",
                host,
                "bash",
                "-lc",
                f"bjobs -a -noheader -o 'STAT EXIT_CODE CPU_USED RUN_TIME MAX_MEM MEM SWAP' {quoted_job_id}",
            ],
            text=True,
            capture_output=True,
            check=False,
        )
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
        proc = subprocess.run(
            [
                "ssh",
                host,
                "bash",
                "-lc",
                f"ps -p {quoted_job_id} -o pid=,etime=,%cpu=,%mem=,rss=,vsz=",
            ],
            text=True,
            capture_output=True,
            check=False,
        )
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

    host = cfg.targets[selected_target].host
    usage_stats, _ = _fetch_job_stats(host=host, scheduler=selected_scheduler, job_id=selected_job_id)
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
            f"scheduler={row['scheduler']} state={row['state']} submitted_at={row['submitted_at']}"
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
            f"state={row['state']} submitted_at={row['submitted_at']}"
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
            f"target={row['target']} scheduler={row['scheduler']} state={row['state']} submitted_at={row['submitted_at']}"
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
            f"state={row['state']} submitted_at={row['submitted_at']}"
        )


@status_app.command("global")
def status_global(
    limit: int = typer.Option(50, help="Maximum number of jobs to display"),
) -> None:
    """Show global status across analyses and targets."""
    _show_global_status(limit=limit)


@analysis_app.command("pull")
def pull(
    remote: bool = typer.Option(
        False, "--remote", help="Pull tagged paths even when they are not present locally (remote-only tags)"
    ),
) -> None:
    """Pull all tagged paths into the active analysis directory."""
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

    analysis_tags = cfg.analysis_tags.get(cfg.active_analysis or "", [])
    if not analysis_tags:
        raise typer.BadParameter(
            "No tags found for active analysis. Tag paths with `cdp analysis tag <path>` before pulling."
        )
    pulled: list[str] = []
    skipped: list[str] = []
    for tag_dir in analysis_tags:
        clean_tag = tag_dir.strip("/")
        if not clean_tag:
            continue
        if not remote and not (analysis_dir / clean_tag).exists():
            skipped.append(clean_tag)
            continue
        tagged_remote = f"{remote_analysis_root}/./{clean_tag}"
        proc = _run_cmd(
            ["rsync", "-az", "--relative", f"{target_cfg.host}:{tagged_remote}", f"{analysis_dir}/"], check=False
        )
        if proc.returncode == 0:
            pulled.append(clean_tag)
        else:
            skipped.append(clean_tag)

    if pulled:
        typer.echo("Pulled tagged paths:")
        for item in pulled:
            typer.echo(f"- {analysis_dir / item}")
    if skipped:
        typer.echo("Skipped tagged paths:")
        for item in skipped:
            typer.echo(f"- {item}")


if __name__ == "__main__":
    app()
