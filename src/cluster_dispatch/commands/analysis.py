from __future__ import annotations

import secrets
import shlex
from pathlib import Path
from typing import Annotated, Callable, Optional

import typer

from cluster_dispatch.analysis import AnalysisError, add_analysis_tag, list_analysis_entries, remove_analysis_tag, set_active_analysis
from cluster_dispatch.context import Context, ContextError, resolve_context
from cluster_dispatch.models import RunRequest
from cluster_dispatch.operations import OperationError, run as execute_run_operation
from cluster_dispatch.commands.common import (
    complete_active_paths as _complete_active_analysis_paths,
    complete_analysis_dirs as _complete_analysis_dirs,
    complete_profiles as _complete_profile_names,
    resources as _resource_spec_from_profile,
)
from cluster_dispatch.commands.runtime import (
    auto_index as _auto_index_active_root,
    operation_runtime,
    project_root as _project_root,
    run_direct as _run_direct,
)

IGNORE_FILE_NAME = ".cdpignore"


def register(analysis_app: typer.Typer, _run_sync_pull: Callable[..., None]) -> Callable[..., None]:
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
                runtime=operation_runtime(),
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
    return _perform_run
