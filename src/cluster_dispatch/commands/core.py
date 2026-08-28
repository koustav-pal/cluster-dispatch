from __future__ import annotations

import typer

from cluster_dispatch.commands import analysis, config, ignore, job, profile, project, sync, target, validation
from cluster_dispatch.commands.admin import register as register_admin
from cluster_dispatch.commands.common import (
    complete_job_ids,
    complete_job_names,
    complete_profiles,
    complete_record_analyses,
    complete_sweeps,
    complete_targets,
    require_yes,
    resources,
)
from cluster_dispatch.commands.runtime import (
    auto_index,
    configure_output,
    project_root,
    run_direct,
    sweep_runtime,
)
from cluster_dispatch.commands.sweep import register as register_sweep

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


@app.callback()
def _global_options(
    verbose: int = typer.Option(0, "-v", "--verbose", count=True, help="Increase verbosity (repeat for more detail)"),
    remote_verbose: bool = typer.Option(False, "--remote-verbose", help="Always print remote SSH/rsync commands and output"),
    quiet: bool = typer.Option(False, "--quiet", help="Suppress non-essential output"),
) -> None:
    """Global output controls."""
    configure_output(verbose, remote_verbose, quiet)


project.register(app)
target.register(target_app)
ignore.register(ignore_app)
profile.register(profile_app)
run_pull = sync.register(sync_app)
perform_run = analysis.register(analysis_app, run_pull)
job.register(app, status_app, perform_run)
validation.register(run_ops_app, sweep_ops_app)

register_sweep(
    sweep_app,
    project_root=project_root,
    runtime=sweep_runtime,
    resources=resources,
    auto_index=auto_index,
    complete_targets=complete_targets,
    complete_profiles=complete_profiles,
    complete_sweeps=complete_sweeps,
)
config.register(config_app)
register_admin(
    app,
    cleanup_app,
    project_root=project_root,
    run_direct=run_direct,
    complete_targets=complete_targets,
    complete_analyses=complete_record_analyses,
    complete_job_ids=complete_job_ids,
    complete_job_names=complete_job_names,
    require_yes=require_yes,
)
