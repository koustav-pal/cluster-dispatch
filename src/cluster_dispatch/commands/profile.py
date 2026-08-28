from __future__ import annotations

from typing import Optional

import typer

from cluster_dispatch.config import load_config, save_config
from cluster_dispatch.profiles import DEFAULT_RESOURCE_PROFILES
from cluster_dispatch.commands.common import (
    all_profiles as _all_resource_profiles,
    complete_profiles as _complete_profile_names,
    complete_user_profiles as _complete_user_profile_names,
    profile_name as _validate_profile_name,
)
from cluster_dispatch.commands.runtime import project_root as _project_root


def register(profile_app: typer.Typer) -> None:
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
