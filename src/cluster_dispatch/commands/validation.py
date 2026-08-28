from __future__ import annotations

import json
import secrets
import shlex
from pathlib import Path
from typing import Optional

import typer

from cluster_dispatch.context import resolve_context
from cluster_dispatch.models import RunRequest
from cluster_dispatch.operations import prepare_run as prepare_run_operation
from cluster_dispatch.sweeps import SweepRequest, validate_sweep as validate_sweep_operation
from cluster_dispatch.commands.common import complete_profiles as _complete_profile_names, complete_targets as _complete_target_names, resources as _resource_spec_from_profile
from cluster_dispatch.commands.runtime import render_scheduler_header as _render_scheduler_header, sweep_runtime as _sweep_runtime


def register(run_ops_app: typer.Typer, sweep_ops_app: typer.Typer) -> None:
    def _validation_summary(checks: list[dict[str, str]]) -> tuple[int, int, int]:
        pass_count = len([c for c in checks if c["status"] == "PASS"])
        warn_count = len([c for c in checks if c["status"] == "WARN"])
        fail_count = len([c for c in checks if c["status"] == "FAIL"])
        return pass_count, warn_count, fail_count

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
