from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import typer
import yaml

from cluster_dispatch.config import ProjectConfig, config_path, load_config
from cluster_dispatch.commands.common import ignore_file as _ignore_file, template_path as _template_path
from cluster_dispatch.commands.runtime import project_root as _project_root
def register(config_app: typer.Typer) -> None:
    def _project_config_snapshot(project_root: Path, cfg: ProjectConfig) -> dict[str, Any]:
        targets_payload: dict[str, Any] = {}
        for name, tcfg in cfg.targets.items():
            targets_payload[name] = {
                "host": tcfg.host,
                "transport": tcfg.transport,
                "scheduler": tcfg.scheduler,
                "remote_root": tcfg.remote_root,
                "has_template": bool(tcfg.template_header.strip()),
                "defaults": {
                    "cpus": tcfg.default_cpus,
                    "memory": tcfg.default_memory,
                    "time": tcfg.default_time,
                    "node": tcfg.default_node,
                    "queue": tcfg.default_queue,
                    "parallel_environment": tcfg.default_parallel_environment,
                },
            }

        active_target_name = cfg.default_target
        active_target = cfg.targets.get(active_target_name)
        active_payload = None
        if active_target is not None:
            active_payload = {
                "name": active_target_name,
                "host": active_target.host,
                "transport": active_target.transport,
                "scheduler": active_target.scheduler,
                "remote_root": active_target.remote_root,
            }

        return {
            "project_root": str(project_root),
            "config_path": str(config_path(project_root)),
            "default_target": cfg.default_target,
            "active_target": active_payload,
            "active_analysis": cfg.active_analysis,
            "ignore_file": str(_ignore_file(project_root)),
            "template_path": str(_template_path(project_root)),
            "targets": targets_payload,
            "resource_profiles": cfg.resource_profiles,
        }

    @config_app.command("show")
    def config_show() -> None:
        """Show resolved project configuration context."""
        project_root = _project_root()
        cfg = load_config(project_root)
        snapshot = _project_config_snapshot(project_root, cfg)

        typer.echo(f"Project root: {snapshot['project_root']}")
        typer.echo(f"Config path: {snapshot['config_path']}")
        typer.echo(f"Default target: {snapshot['default_target']}")
        active_target = snapshot.get("active_target")
        if active_target:
            typer.echo(
                f"Active target: {active_target['name']} "
                f"(scheduler={active_target['scheduler']} transport={active_target['transport']} "
                f"host={active_target['host']} remote_root={active_target['remote_root']})"
            )
        else:
            typer.echo("Active target: <missing>")
        typer.echo(f"Active analysis: {snapshot.get('active_analysis') or '<unset>'}")
        typer.echo(f"Ignore file: {snapshot['ignore_file']}")
        typer.echo(f"Template path: {snapshot['template_path']}")
        typer.echo("Targets:")
        for name, payload in snapshot["targets"].items():
            marker = "*" if name == snapshot["default_target"] else " "
            typer.echo(
                f"{marker} {name}: scheduler={payload['scheduler']} transport={payload['transport']} "
                f"host={payload['host']} remote_root={payload['remote_root']}"
            )

    @config_app.command("export")
    def config_export(
        fmt: str = typer.Option("json", "--format", help="Output format: json|yaml"),
    ) -> None:
        """Export full configuration snapshot in JSON or YAML."""
        fmt_lc = fmt.strip().lower()
        if fmt_lc not in {"json", "yaml"}:
            raise typer.BadParameter("--format must be one of: json, yaml")

        project_root = _project_root()
        cfg = load_config(project_root)
        snapshot = _project_config_snapshot(project_root, cfg)
        if fmt_lc == "json":
            typer.echo(json.dumps(snapshot, indent=2))
        else:
            typer.echo(yaml.safe_dump(snapshot, sort_keys=False))
