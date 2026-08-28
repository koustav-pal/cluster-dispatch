from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any, Callable

import typer

from cluster_dispatch.archive import ArchiveError, export_bundle, import_bundle
from cluster_dispatch.diagnostics import diagnose_project
from cluster_dispatch.maintenance import cleanup_records
from cluster_dispatch.reporting import ReportingError, build_report, collect_stats


def _render_table(title: str, rows: list[tuple[str, str]]) -> None:
    width = max(len("Metric"), *(len(key) for key, _ in rows))
    value_width = max(len("Value"), *(len(value) for _, value in rows))
    border = f"+-{'-' * width}-+-{'-' * value_width}-+"
    typer.echo(title)
    typer.echo(border)
    typer.echo(f"| {'Metric'.ljust(width)} | {'Value'.ljust(value_width)} |")
    typer.echo(border)
    for key, value in rows:
        typer.echo(f"| {key.ljust(width)} | {value.ljust(value_width)} |")
    typer.echo(border)


def register(
    app: typer.Typer,
    cleanup_app: typer.Typer,
    *,
    project_root: Callable[[], Path],
    run_direct: Callable[..., Any],
    complete_targets: Callable[..., list[str]],
    complete_analyses: Callable[..., list[str]],
    complete_job_ids: Callable[..., list[str]],
    complete_job_names: Callable[..., list[str]],
    require_yes: Callable[[bool, str], None],
) -> None:
    @app.command("doctor")
    def doctor(
        target: str | None = typer.Option(None, "--target", autocompletion=complete_targets),
        remote: bool = typer.Option(True, "--remote/--no-remote"),
    ) -> None:
        """Run project preflight checks."""
        result = diagnose_project(project_root(), target=target, remote=remote, run=run_direct)
        for check in result.checks:
            typer.echo(f"[{check.status}] {check.name}: {check.detail}")
        typer.echo(f"Summary: PASS={result.pass_count} WARN={result.warn_count} FAIL={result.fail_count}")
        if result.failed:
            raise typer.Exit(1)

    @app.command("report")
    def report(
        target: str | None = typer.Option(None, "--target", autocompletion=complete_targets),
        analysis: str | None = typer.Option(None, "--analysis", autocompletion=complete_analyses),
        days: int | None = typer.Option(None, "--days", min=0),
        as_json: bool = typer.Option(False, "--json"),
    ) -> None:
        """Summarize recent run and sync activity from local records."""
        payload = build_report(project_root(), target=target, analysis=analysis, days=days)
        if as_json:
            typer.echo(json.dumps(payload, indent=2))
            return
        index = payload["index"]
        typer.echo("Run report:")
        typer.echo(f"Filters: target={target or '*'} analysis={analysis or '*'} days={days if days is not None else '*'}")
        typer.echo(f"Jobs: {payload['jobs_total']}")
        typer.echo(f"Sync events: {payload['sync_total']}")
        typer.echo(
            f"Index manifests: {index['manifests']} latest={index['latest_indexed_at'] or 'n/a'} "
            f"entries={index['entries_total']} files={index['files_total']} size={index['bytes_human']} "
            f"stale_24h={index['stale_manifests_24h']}"
        )
        for label, values in (("Jobs by state", payload["jobs_by_state"]), ("Jobs by scheduler", payload["jobs_by_scheduler"]), ("Sync by action", payload["sync_by_action"])):
            if values:
                typer.echo(f"{label}:")
                for key in sorted(values):
                    typer.echo(f"- {key}: {values[key]}")
        if payload["recent_failures"]:
            typer.echo("Recent failures:")
            for item in payload["recent_failures"]:
                typer.echo(f"- {item.get('submitted_at')} job_id={item.get('job_id')} job_name={item.get('job_name')} state={item.get('state')} target={item.get('target')} analysis={item.get('analysis')}")
        if payload["resources"]["avg_cpus"] is not None:
            typer.echo(f"Average CPUs requested: {payload['resources']['avg_cpus']}")

    @app.command("stats")
    def stats(
        job_id: str | None = typer.Option(None, "--job-id", autocompletion=complete_job_ids),
        job_name: str | None = typer.Option(None, "--job-name", autocompletion=complete_job_names),
        target: str | None = typer.Option(None, "--target", autocompletion=complete_targets),
        analysis: str | None = typer.Option(None, "--analysis", autocompletion=complete_analyses),
    ) -> None:
        """Collect resource usage stats for a recorded job."""
        try:
            result = collect_stats(project_root(), job_id=job_id, job_name=job_name, target=target, analysis=analysis, run=run_direct)
        except ReportingError as exc:
            raise typer.BadParameter(str(exc)) from exc
        if result.match_count > 1:
            typer.echo(f"Multiple matches found ({result.match_count}). Using most recent match.")
        record = result.record
        rows = [("job_id", str(record.get("job_id", ""))), ("job_name", str(record.get("job_name", ""))), ("target", str(record.get("target", ""))), ("scheduler", result.scheduler)]
        rows.extend((key, result.values[key]) for key in sorted(result.values))
        _render_table("Job Stats", rows)

    @app.command("export")
    def export_command(
        output: Path | None = typer.Option(None, "--output"),
        include_jobs: bool = typer.Option(False, "--jobs/--no-jobs"),
        include_sync: bool = typer.Option(False, "--sync/--no-sync"),
        include_sweeps: bool = typer.Option(False, "--sweeps/--no-sweeps"),
        redact_hosts: bool = typer.Option(False, "--redact-hosts"),
    ) -> None:
        """Export project metadata bundle as tar.gz."""
        root = project_root()
        path = (output or Path(f"cdp-export-{datetime.now().strftime('%Y%m%d%H%M%S')}.tar.gz")).resolve()
        result = export_bundle(root, path, include_jobs=include_jobs, include_sync=include_sync, include_sweeps=include_sweeps, redact_hosts=redact_hosts)
        typer.echo(f"Exported bundle: {result.path}")
        typer.echo(f"Included: {','.join(result.sections)}")

    @app.command("import")
    def import_command(
        bundle: Path = typer.Argument(..., exists=True, dir_okay=False, readable=True),
        overwrite: bool = typer.Option(False, "--overwrite"),
        include_jobs: bool = typer.Option(True, "--jobs/--no-jobs"),
        include_sync: bool = typer.Option(True, "--sync/--no-sync"),
        include_sweeps: bool = typer.Option(True, "--sweeps/--no-sweeps"),
        yes: bool = typer.Option(False, "--yes"),
    ) -> None:
        """Import project metadata bundle into current directory."""
        if overwrite:
            require_yes(yes, "Import with --overwrite is destructive. Re-run with --yes to confirm.")
        try:
            result = import_bundle(Path.cwd().resolve(), bundle, overwrite=overwrite, include_jobs=include_jobs, include_sync=include_sync, include_sweeps=include_sweeps)
        except ArchiveError as exc:
            raise typer.BadParameter(str(exc)) from exc
        typer.echo(f"Imported bundle: {result.path}")
        typer.echo(f"Files extracted: {result.extracted}")
        if result.skipped:
            typer.echo(f"Files skipped: {result.skipped}")

    @cleanup_app.command("records")
    def cleanup_command(
        jobs: bool = typer.Option(True, "--jobs/--no-jobs"),
        sync: bool = typer.Option(True, "--sync/--no-sync"),
        sweeps: bool = typer.Option(False, "--sweeps/--no-sweeps"),
        older_than_days: int | None = typer.Option(None, "--older-than-days", min=0),
        keep_last: int = typer.Option(0, "--keep-last", min=0),
        target: str | None = typer.Option(None, "--target", autocompletion=complete_targets),
        analysis: str | None = typer.Option(None, "--analysis", autocompletion=complete_analyses),
        dry_run: bool = typer.Option(True, "--dry-run/--apply"),
        as_json: bool = typer.Option(False, "--json"),
        yes: bool = typer.Option(False, "--yes"),
    ) -> None:
        """Clean local metadata records with retention filters."""
        if not dry_run:
            require_yes(yes, "Cleanup --apply deletes records. Re-run with --yes to confirm.")
        result = cleanup_records(project_root(), jobs=jobs, sync=sync, sweeps=sweeps, older_than_days=older_than_days, keep_last=keep_last, target=target, analysis=analysis, dry_run=dry_run)
        if as_json:
            typer.echo(json.dumps(result, indent=2))
            return
        typer.echo("Cleanup summary:")
        typer.echo(f"Mode: {'dry-run' if dry_run else 'apply'}")
        for name, category in result["categories"].items():
            typer.echo(f"- {name}: scanned={category['scanned']} matched={category['matched']} to_delete={category['deleted_count']}")
        if result["deleted_total"] and dry_run:
            typer.echo("Use --apply to perform deletion.")
