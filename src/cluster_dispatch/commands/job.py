from __future__ import annotations

import time
from pathlib import Path
from typing import Annotated, Callable, Optional

import typer

from cluster_dispatch.config import ProjectConfig, load_config, load_state
from cluster_dispatch.jobs import JobError, cancel_jobs, collect_outputs, filter_jobs, get_log, is_failed_state, is_running_state, poll_job, prepare_retry, query_jobs, select_job, unique_jobs
from cluster_dispatch.commands.common import (
    complete_job_ids as _complete_job_ids,
    complete_job_names as _complete_job_names,
    complete_record_analyses as _complete_record_analyses,
    complete_targets as _complete_target_names,
    require_yes as _require_yes,
)
from cluster_dispatch.commands.runtime import project_root as _project_root, run_direct as _run_direct


def register(app: typer.Typer, status_app: typer.Typer, _perform_run: Callable[..., None]) -> None:
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
