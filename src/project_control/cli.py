from __future__ import annotations

import shlex
import secrets
import subprocess
import json
from datetime import datetime
from pathlib import Path
from typing import Annotated, Optional

import typer

from project_control.config import (
    CONFIG_DIR,
    CONFIG_NAME,
    LEGACY_CONFIG_NAME,
    LastJob,
    ProjectConfig,
    TargetConfig,
    append_job_record,
    config_path,
    ensure_state_dirs,
    find_project_root,
    legacy_config_path,
    load_config,
    load_state,
    save_config,
    save_state,
)
from project_control.schedulers import get_adapter

app = typer.Typer(help="Project control CLI for remote SSH compute targets")
target_app = typer.Typer(help="Manage compute targets")
analysis_app = typer.Typer(help="Manage active analysis directory")
status_app = typer.Typer(help="Show job status", invoke_without_command=True)
app.add_typer(target_app, name="target")
app.add_typer(analysis_app, name="analysis")
app.add_typer(status_app, name="status")


TEMPLATES_DIR_NAME = "templates"
TEMPLATE_FILE_NAME = "scheduler_header.tmpl"
BASE_REQUIRED_TEMPLATE_VARS = ("cpus", "memory", "time", "job_name", "stdout", "stderr")


def _project_root() -> Path:
    return find_project_root()


def _require_active_analysis(cfg: ProjectConfig, project_root: Path) -> Path:
    if not cfg.active_analysis:
        raise typer.BadParameter("No active analysis set. Run: pc analysis use <path>")
    path = (project_root / cfg.active_analysis).resolve()
    if not path.exists() or not path.is_dir():
        raise typer.BadParameter(f"Active analysis path missing: {path}")
    return path


def _active_target(cfg: ProjectConfig) -> tuple[str, TargetConfig]:
    name = cfg.default_target
    if name not in cfg.targets:
        raise typer.BadParameter(
            f"Default target '{name}' not configured. Add with: pc target add {name} ..."
        )
    return name, cfg.targets[name]


def _run_cmd(cmd: list[str], check: bool = True) -> subprocess.CompletedProcess[str]:
    return subprocess.run(cmd, text=True, capture_output=True, check=check)


def _build_submit_script(
    header: str,
    command: str,
    remote_run_dir: str,
    remote_log_file: str,
) -> str:
    lines: list[str] = ["#!/usr/bin/env bash", "set -euo pipefail"]
    if header.strip():
        lines.extend(header.strip().splitlines())
    lines.extend(
        [
            f"mkdir -p {shlex.quote(remote_run_dir)}",
            f"cd {shlex.quote(remote_run_dir)}",
            "echo \"[project-control] started $(date -Iseconds)\"",
            f"{command} >> {shlex.quote(remote_log_file)} 2>&1",
            "echo \"[project-control] finished $(date -Iseconds)\"",
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
            queue=queue,
            node=node,
            parallel_environment=parallel_environment,
        )
    except KeyError as exc:
        missing = exc.args[0]
        raise typer.BadParameter(
            f"Scheduler template contains unknown placeholder '{missing}'. "
            "Allowed: cpus, memory, time, job_name, stdout, stderr, queue, node, parallel_environment."
        ) from exc


@app.command()
def init(
    host: str = typer.Option(..., help="SSH host alias for the default target"),
    scheduler: str = typer.Option(..., help="Default scheduler for this project: sge|univa|pbs|slurm|lsf|none"),
    default_target: Optional[str] = typer.Option(
        None, help="Default target name (defaults to current project root directory name)"
    ),
    remote_root: str = typer.Option(..., help="Remote absolute root directory for this project"),
    default_cpus: int = typer.Option(1, help="Default CPUs for this target"),
    default_memory: str = typer.Option("8G", help="Default memory for this target"),
    default_time: str = typer.Option("01:00:00", help="Default walltime for this target"),
    default_node: str = typer.Option("1", help="Default node value for this target"),
    default_queue: str = typer.Option("", help="Default queue for this target"),
    default_parallel_environment: str = typer.Option("", help="Default parallel environment for this target"),
    template_file: Path = typer.Option(
        ...,
        "--template-file",
        help=(
            "User-provided scheduler template file. Must include: "
            "{cpus}, {memory}, {time}, {job_name}, {stdout}, {stderr}. "
            "Optional: {queue}, {node}, {parallel_environment}."
        ),
    ),
) -> None:
    """Initialize .project_control/config.yml and local state directories."""
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
    legacy_path = legacy_config_path(project_root)

    if cfg_path.exists():
        raise typer.BadParameter(f"{CONFIG_DIR}/{CONFIG_NAME} already exists in this directory")
    if legacy_path.exists():
        raise typer.BadParameter(
            f"Legacy config {LEGACY_CONFIG_NAME} exists. Move it to {CONFIG_DIR}/{CONFIG_NAME} first."
        )
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
    name: str = typer.Argument(..., help="Target name"),
    host: Optional[str] = typer.Option(None, help="SSH host alias"),
    remote_root: Optional[str] = typer.Option(
        None, help="Remote absolute root directory for this project on the target"
    ),
    default_cpus: Optional[int] = typer.Option(None, help="Default CPUs for this target"),
    default_memory: Optional[str] = typer.Option(None, help="Default memory for this target"),
    default_time: Optional[str] = typer.Option(None, help="Default walltime for this target"),
    default_node: Optional[str] = typer.Option(None, help="Default node value for this target"),
    default_queue: Optional[str] = typer.Option(None, help="Default queue for this target"),
    default_parallel_environment: Optional[str] = typer.Option(
        None, help="Default parallel environment for this target"
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
def target_set(name: Annotated[str, typer.Argument(help="Target name")]) -> None:
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


@analysis_app.command("use")
def analysis_use(
    path: Annotated[Path, typer.Argument(help="Analysis directory under project root")],
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
    path: Annotated[Path, typer.Argument(help="Path inside active analysis to tag for pull")],
) -> None:
    """Tag a path inside active analysis for future pulls."""
    project_root = _project_root()
    cfg = load_config(project_root)
    analysis_dir = _require_active_analysis(cfg, project_root)

    candidate = (analysis_dir / path).resolve()
    try:
        relative_to_analysis = candidate.relative_to(analysis_dir)
    except ValueError as exc:
        raise typer.BadParameter("Tag path must be inside the active analysis directory") from exc

    if not candidate.exists():
        raise typer.BadParameter(f"Tag path not found: {candidate}")

    tag_value = relative_to_analysis.as_posix()
    analysis_key = cfg.active_analysis or ""
    tags = cfg.analysis_tags.get(analysis_key, [])
    if tag_value not in tags:
        tags.append(tag_value)
        cfg.analysis_tags[analysis_key] = sorted(tags)
        save_config(project_root, cfg)
        typer.echo(f"Tagged analysis path: {tag_value}")
    else:
        typer.echo(f"Already tagged: {tag_value}")


@app.command(context_settings={"allow_extra_args": True, "ignore_unknown_options": True})
def run(
    ctx: typer.Context,
    interactive: Annotated[bool, typer.Option(help="Run in interactive SSH session (skip scheduler)")] = False,
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
        raise typer.BadParameter("Provide a command to execute, e.g. pc run python main.py")

    command = " ".join(shlex.quote(arg) for arg in ctx.args)

    project_root = _project_root()
    cfg = load_config(project_root)
    analysis_dir = _require_active_analysis(cfg, project_root)
    target_name, target = _active_target(cfg)
    if not target.remote_root:
        raise typer.BadParameter(
            f"Target '{target_name}' has no remote_root configured. Update it with `pc target add {target_name} --remote-root ...`."
        )
    resolved_cpus = cpus if cpus is not None else target.default_cpus
    resolved_memory = memory if memory is not None else target.default_memory
    resolved_time = job_time if job_time is not None else target.default_time
    resolved_job_name = job_name or f"pc-{secrets.token_hex(4)}"
    resolved_stdout = resolved_job_name
    resolved_stderr = resolved_job_name
    resolved_node = node or target.default_node or "1"
    resolved_queue = queue if queue is not None else target.default_queue
    resolved_parallel_environment = (
        parallel_environment if parallel_environment is not None else target.default_parallel_environment
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
        raise typer.BadParameter("Active analysis path is empty. Re-run: pc analysis use <path>")
    remote_analysis_root = f"{target.remote_root.rstrip('/')}/{analysis_rel}"
    remote_run_dir = remote_analysis_root
    remote_submit_script = f"{remote_analysis_root}/pc_submit.sh"
    remote_log_file = f"{remote_analysis_root}/run.log"

    project_ignore = project_root / ".pcignore"

    _run_cmd(["ssh", target.host, "mkdir", "-p", remote_analysis_root])

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
            queue=resolved_queue or "",
            node=resolved_node,
            parallel_environment=resolved_parallel_environment or "",
        ),
        command=command,
        remote_run_dir=remote_run_dir,
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

    if interactive:
        subprocess.run(
            ["ssh", "-t", target.host, "bash", "-lc", f"cd {shlex.quote(remote_run_dir)} && {command}"],
            check=True,
        )
        job_id = "interactive"
    else:
        adapter = get_adapter(target.scheduler)
        job_id = adapter.submit(target.host, remote_submit_script).job_id

    now = datetime.now().isoformat(timespec="seconds")
    last_job = LastJob(
        job_id=job_id,
        target=target_name,
        scheduler=target.scheduler,
        remote_run_dir=remote_analysis_root,
        remote_log_file=remote_log_file,
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
            "job_name": resolved_job_name,
            "stdout": resolved_stdout,
            "stderr": resolved_stderr,
            "cpus": resolved_cpus,
            "memory": resolved_memory,
            "time": resolved_time,
            "queue": resolved_queue,
            "parallel_environment": resolved_parallel_environment,
            "node": resolved_node,
            "command": command,
            "remote_run_dir": remote_analysis_root,
            "remote_log_file": remote_log_file,
        },
    )

    typer.echo(f"Submitted job_id={job_id} target={target_name}")
    typer.echo(f"Job name: {resolved_job_name}")
    typer.echo(f"Node: {resolved_node}")
    typer.echo(f"Remote run dir: {remote_analysis_root}")
    typer.echo(f"Remote log: {remote_log_file}")


def _show_last_status(follow: bool = False) -> None:
    """Show status of last submitted job."""
    project_root = _project_root()
    state = load_state(project_root)
    last = state.get("last_job")
    if not last:
        raise typer.BadParameter("No active job in state. Run `pc run ...` first.")

    job_id = last["job_id"]
    target = last["target"]

    cfg = load_config(project_root)
    if target not in cfg.targets:
        raise typer.BadParameter(f"Target '{target}' from state is no longer configured")

    target_cfg = cfg.targets[target]

    if job_id == "interactive":
        typer.echo("Last run was interactive; no scheduler job id to query")
    else:
        adapter = get_adapter(last["scheduler"])
        result = adapter.status(target_cfg.host, job_id)
        typer.echo(f"job_id={job_id} target={target} state={result.state}")

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


def _collect_status_rows(
    project_root: Path,
    cfg: ProjectConfig,
    analysis_filter: Optional[str],
    target_filter: Optional[str],
    limit: int,
) -> list[dict[str, str]]:
    jobs_dir = project_root / ".project_control" / "jobs"
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
        scheduler = str(payload.get("scheduler", ""))
        submitted_at = str(payload.get("submitted_at", ""))
        state = "UNKNOWN"

        if job_id == "interactive":
            state = "INTERACTIVE"
        elif target_name in cfg.targets:
            adapter = get_adapter(scheduler)
            status_result = adapter.status(cfg.targets[target_name].host, job_id)
            state = status_result.state
        else:
            state = "TARGET_MISSING"

        rows.append(
            {
                "job_id": job_id,
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
    rows = _collect_status_rows(project_root, cfg, analysis_filter=None, target_filter=None, limit=limit)

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

    rows = _collect_status_rows(project_root, cfg, analysis_filter=None, target_filter=target, limit=limit)
    if not rows:
        typer.echo(f"No jobs found for target '{target}'")
        return

    typer.echo(f"Status for target: {target}")
    for row in rows:
        typer.echo(
            f"{row['job_id']}  analysis={row['analysis']} scheduler={row['scheduler']} "
            f"state={row['state']} submitted_at={row['submitted_at']}"
        )


@status_app.callback(invoke_without_command=True)
def status_callback(
    ctx: typer.Context,
    follow: Annotated[bool, typer.Option(help="Follow active job log output")] = False,
    target: Optional[str] = typer.Option(
        None, "--target", help="Show scheduler status filtered to a target name"
    ),
    limit: int = typer.Option(50, help="Maximum number of jobs to display for target/global views"),
) -> None:
    """Show context-aware status, or use subcommands like `pc status list`."""
    if ctx.invoked_subcommand is None:
        if target:
            if follow:
                raise typer.BadParameter("--follow cannot be used with --target")
            _show_target_status(target=target, limit=limit)
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
        project_root, cfg, analysis_filter=target_analysis, target_filter=None, limit=limit
    )

    if not rows:
        typer.echo(f"No jobs found for analysis '{target_analysis}'")
        return

    typer.echo(f"Jobs for analysis: {target_analysis}")
    for row in rows:
        typer.echo(
            f"{row['job_id']}  target={row['target']} scheduler={row['scheduler']} "
            f"state={row['state']} submitted_at={row['submitted_at']}"
        )


@status_app.command("global")
def status_global(
    limit: int = typer.Option(50, help="Maximum number of jobs to display"),
) -> None:
    """Show global status across analyses and targets."""
    _show_global_status(limit=limit)


@app.command()
def pull() -> None:
    """Pull all tagged paths from the last run into the active analysis directory."""
    project_root = _project_root()
    cfg = load_config(project_root)
    analysis_dir = _require_active_analysis(cfg, project_root)

    state = load_state(project_root)
    last = state.get("last_job")
    if not last:
        raise typer.BadParameter("No active job in state. Run `pc run ...` first.")

    target = last["target"]
    if target not in cfg.targets:
        raise typer.BadParameter(f"Target '{target}' from state is no longer configured")

    target_cfg = cfg.targets[target]
    analysis_tags = cfg.analysis_tags.get(cfg.active_analysis or "", [])
    if not analysis_tags:
        raise typer.BadParameter(
            "No tags found for active analysis. Tag paths with `pc analysis tag <path>` before pulling."
        )
    remote_analysis_root = last["remote_run_dir"]
    pulled: list[str] = []
    for tag_dir in analysis_tags:
        clean_tag = tag_dir.strip("/")
        if not clean_tag:
            continue
        tagged_remote = f"{remote_analysis_root}/./{clean_tag}"
        _run_cmd(["rsync", "-az", "--relative", f"{target_cfg.host}:{tagged_remote}", f"{analysis_dir}/"], check=False)
        pulled.append(clean_tag)

    if pulled:
        typer.echo("Pulled tagged paths:")
        for item in pulled:
            typer.echo(f"- {analysis_dir / item}")


if __name__ == "__main__":
    app()
