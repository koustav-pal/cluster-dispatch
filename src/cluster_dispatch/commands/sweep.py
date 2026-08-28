from __future__ import annotations

import shlex
from pathlib import Path
from typing import Any, Callable

import typer

from cluster_dispatch.context import ContextError, resolve_context
from cluster_dispatch.operations import OperationError
from cluster_dispatch.records import load_sweep
from cluster_dispatch.sweeps import (
    SweepError,
    SweepRequest,
    SweepRuntime,
    cancel_sweep,
    execute_sweep,
    get_sweep,
    resume_sweep,
    sweeps_for_analysis,
)


def register(
    app: typer.Typer,
    *,
    project_root: Callable[[], Path],
    runtime: Callable[[], SweepRuntime],
    resources: Callable[..., Any],
    auto_index: Callable[..., Path | None],
    complete_targets: Callable[..., list[str]],
    complete_profiles: Callable[..., list[str]],
    complete_sweeps: Callable[..., list[str]],
) -> None:
    @app.command("run", context_settings={"allow_extra_args": True, "ignore_unknown_options": True})
    def run(
        ctx: typer.Context,
        config_file: Path = typer.Option(..., "--config"),
        mode: str = typer.Option("single", "--mode"),
        sweep_job: str | None = typer.Option(None, "--job"),
        sweep_id: str | None = typer.Option(None, "--sweep-id"),
        target: str | None = typer.Option(None, "--target", autocompletion=complete_targets),
        profile: str | None = typer.Option(None, "--profile", autocompletion=complete_profiles),
        cpus: int | None = typer.Option(None),
        memory: str | None = typer.Option(None),
        job_time: str | None = typer.Option(None, "--time"),
        job_name: str | None = typer.Option(None),
        queue: str | None = typer.Option(None),
        node: str | None = typer.Option(None),
        parallel_environment: str | None = typer.Option(None, "--parallel-environment"),
        submit_delay_ms: int = typer.Option(5000, "--submit-delay-ms", min=5000),
        index_after: bool = typer.Option(False, "--index-after"),
    ) -> None:
        """Run sweep; persist manifest."""
        if not ctx.args:
            raise typer.BadParameter("Provide command template, e.g. cdp analysis sweep run --config sweep.yml python train.py --lr {lr}")
        try:
            context = resolve_context(target_name=target)
            resource_spec = resources(profile, context.config, context.target, cpus=cpus, memory=memory, walltime=job_time, node=node, queue=queue, parallel_environment=parallel_environment)
            result = execute_sweep(
                context,
                SweepRequest(config_file, " ".join(shlex.quote(arg) for arg in ctx.args), mode, resource_spec, sweep_job, sweep_id, job_name, submit_delay_ms),
                runtime=runtime(),
            )
        except (ContextError, SweepError, OperationError) as exc:
            raise typer.BadParameter(str(exc)) from exc
        manifest = result.manifest
        typer.echo(f"Submitted sweep_id={manifest['sweep_id']} mode={manifest['mode']} target={manifest['target']} runs={result.submitted}")
        if index_after and (path := auto_index(context.project_root, context.config, context.target_name, context.target)):
            typer.echo(f"Auto-indexed active analysis root: {path}")

    @app.command("list")
    def list_command() -> None:
        """List sweep manifests for active analysis."""
        try:
            context = resolve_context()
            manifests = sweeps_for_analysis(context)
        except (ContextError, SweepError) as exc:
            raise typer.BadParameter(str(exc)) from exc
        if not manifests:
            typer.echo(f"No sweeps found for active analysis '{context.analysis.as_posix()}'")
            return
        for manifest in manifests:
            runs = manifest.get("runs", [])
            total = len(runs) if isinstance(runs, list) else 0
            submitted = sum(isinstance(item, dict) and bool(item.get("job_id")) for item in runs)
            typer.echo(f"{manifest.get('sweep_id', '')}  analysis={manifest.get('analysis', '')} mode={manifest.get('mode', '')} target={manifest.get('target', '')} runs={submitted}/{total} updated_at={manifest.get('updated_at', '')}")

    @app.command("show")
    def show(sweep_id: str = typer.Argument(..., autocompletion=complete_sweeps)) -> None:
        """Show one sweep manifest."""
        try:
            manifest = get_sweep(resolve_context(), sweep_id)
        except (ContextError, SweepError) as exc:
            raise typer.BadParameter(str(exc)) from exc
        runs = manifest.get("runs", [])
        typer.echo(f"sweep_id={manifest.get('sweep_id')} analysis={manifest.get('analysis')} mode={manifest.get('mode')} target={manifest.get('target')} scheduler={manifest.get('scheduler')} runs={len(runs) if isinstance(runs, list) else 0}")
        if isinstance(runs, list):
            for item in runs:
                if isinstance(item, dict):
                    typer.echo(f"- run_id={item.get('run_id')} job={item.get('sweep_job')} idx={item.get('sweep_index')} job_id={item.get('job_id')} status={item.get('status')}")

    @app.command("resume")
    def resume(sweep_id: str = typer.Argument(..., autocompletion=complete_sweeps)) -> None:
        """Resume pending sweep runs."""
        try:
            root = project_root()
            manifest = load_sweep(root, sweep_id)
            context = resolve_context(start=root, target_name=str(manifest.get("target", "")) or None)
            result = resume_sweep(context, sweep_id, runtime=runtime())
        except (ContextError, LookupError, SweepError, OperationError) as exc:
            raise typer.BadParameter(str(exc)) from exc
        typer.echo(f"No pending runs for sweep_id={sweep_id}" if not result.submitted else f"Resumed sweep_id={sweep_id}; submitted={result.submitted}")

    @app.command("cancel")
    def cancel(sweep_id: str = typer.Argument(..., autocompletion=complete_sweeps)) -> None:
        """Cancel submitted sweep jobs."""
        try:
            root = project_root()
            manifest = load_sweep(root, sweep_id)
            context = resolve_context(start=root, target_name=str(manifest.get("target", "")) or None)
            result = cancel_sweep(context, sweep_id, runtime=runtime())
        except (ContextError, LookupError, SweepError) as exc:
            raise typer.BadParameter(str(exc)) from exc
        typer.echo(f"Cancel requested for sweep_id={sweep_id}; jobs={result.cancelled}")
