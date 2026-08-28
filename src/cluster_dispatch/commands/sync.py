from __future__ import annotations

import json
import shlex
from typing import Any, Optional, Callable

import typer

from cluster_dispatch.context import ContextError, resolve_context
from cluster_dispatch.index import load_index_manifest as _load_index_manifest, summarize_index_vs_local as _summarize_index_vs_local
from cluster_dispatch.models import PullRequest, PushRequest
from cluster_dispatch.operations import OperationError, pull as execute_pull_operation, push as execute_push_operation
from cluster_dispatch.records import list_syncs
from cluster_dispatch.commands.common import complete_record_analyses as _complete_record_analyses, complete_targets as _complete_target_names
from cluster_dispatch.commands.runtime import auto_index as _auto_index_active_root, project_root as _project_root, run_command as _run_cmd

IGNORE_FILE_NAME = ".cdpignore"
def register(sync_app: typer.Typer) -> Callable[..., None]:
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
        events = list_syncs(project_root)
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
    return _run_sync_pull
