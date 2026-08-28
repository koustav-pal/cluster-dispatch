from __future__ import annotations

import json
from pathlib import Path
from typing import Annotated, Optional

import typer

from cluster_dispatch.config import TargetConfig, load_config, save_config
from cluster_dispatch.diagnostics import diagnose_target
from cluster_dispatch.maintenance import prune_target_records, target_record_refs
from cluster_dispatch.commands.common import complete_targets as _complete_target_names, require_yes as _require_yes, template_path as _template_path
from cluster_dispatch.commands.runtime import project_root as _project_root, run_direct as _run_direct

BASE_REQUIRED_TEMPLATE_VARS = ("cpus", "memory", "time", "job_name", "stdout", "stderr", "working_dir")


def _validate_template_variables(template: str) -> None:
    missing = [name for name in BASE_REQUIRED_TEMPLATE_VARS if f"{{{name}}}" not in template]
    if missing:
        raise typer.BadParameter(
            f"Template missing required placeholders: {', '.join(missing)}. "
            f"Required placeholders are: {', '.join(BASE_REQUIRED_TEMPLATE_VARS)}. "
            "Optional placeholders: queue, node, parallel_environment."
        )


def register(target_app: typer.Typer) -> None:
    def _validate_transport(value: str) -> str:
        transport = value.strip().lower()
        if transport not in {"ssh", "local"}:
            raise typer.BadParameter("transport must be one of: ssh, local")
        return transport

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
