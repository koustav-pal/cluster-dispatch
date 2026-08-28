from __future__ import annotations

import json
import shutil
from pathlib import Path
from typing import Optional

import typer

from cluster_dispatch.config import CONFIG_DIR, CONFIG_NAME, ProjectConfig, TargetConfig, config_path, ensure_state_dirs, save_config
from cluster_dispatch.context import ContextError, resolve_context
from cluster_dispatch.index import resolve_index_scopes as _resolve_index_scopes
from cluster_dispatch.models import IndexRequest
from cluster_dispatch.operations import OperationError, index as execute_index_operation
from cluster_dispatch.commands.common import complete_active_paths as _complete_active_analysis_paths, complete_index_paths as _complete_index_remote_paths, ignore_file as _ignore_file
from cluster_dispatch.commands.runtime import get_git_info, run_direct as _run_direct, write_index_manifest as _write_index_manifest_for_scope

GITIGNORE_FILE_NAME = ".gitignore"
DEFAULT_GITIGNORE_CONTENT = """# Git version control exclusions
# .gitignore controls what Git tracks.
# .cdpignore controls what cdp excludes from remote sync.
__pycache__/
*.py[cod]
*.egg-info/
build/
dist/
.pytest_cache/
.mypy_cache/
.DS_Store
.venv/

# data/
# results/
# cache/
# scratch/
"""


def register(app: typer.Typer) -> None:
    @app.command()
    def init(
        project_path: Optional[Path] = typer.Argument(
            None, help="Project directory to initialize (defaults to current directory)"
        ),
        with_git: bool = typer.Option(
            False, "--with-git", help="Also initialize Git repository if one is not already detected"
        ),
    ) -> None:
        """Initialize .cluster_dispatch/config.yml with default local target."""

        project_root = (project_path or Path.cwd()).resolve()
        project_root.mkdir(parents=True, exist_ok=True)
        cfg_path = config_path(project_root)
        parent_project_root: Optional[Path] = None
        for candidate in project_root.parents:
            if config_path(candidate).exists():
                parent_project_root = candidate
                break

        if cfg_path.exists():
            raise typer.BadParameter(f"{CONFIG_DIR}/{CONFIG_NAME} already exists in this directory")
        if parent_project_root is not None:
            typer.secho(
                (
                    f"Warning: cdp init is running inside an existing cdp project.\n"
                    f"Existing project root: {parent_project_root}\n"
                    f"Initializing nested project at: {project_root}"
                ),
                fg=typer.colors.YELLOW,
            )

        local_target = "local"
        local_root = str(project_root.resolve())
        cfg = ProjectConfig(scheduler="none", default_target=local_target)
        cfg.targets[local_target] = TargetConfig(
            host="localhost",
            scheduler="none",
            remote_root=local_root,
            transport="local",
            template_header="",
            default_cpus=1,
            default_memory="8G",
            default_time="01:00:00",
            default_node="1",
            default_queue="",
            default_parallel_environment="",
        )
        save_config(project_root, cfg)
        ensure_state_dirs(project_root)
        ignore_path = _ignore_file(project_root)
        created_ignore = False
        if not ignore_path.exists():
            ignore_path.write_text(
                "# cdp ignore patterns (rsync --exclude-from)\n"
                "# Example:\n"
                "# *.tmp\n"
                "# data/raw/\n"
            )
            created_ignore = True
        typer.echo(f"Initialized {cfg_path}")
        typer.echo("Default target set to 'local' (scheduler=none, host=localhost)")
        if created_ignore:
            typer.echo(f"Created {ignore_path}")

        if with_git:
            if shutil.which("git") is None:
                raise typer.BadParameter("Git is not installed or not on PATH, but --with-git was requested")

            git_info = get_git_info(project_root)
            if git_info is None:
                init_proc = _run_direct(
                    ["git", "init"],
                    cwd=project_root,
                    text=True,
                    capture_output=True,
                    check=False,
                )
                if init_proc.returncode != 0:
                    error_message = (init_proc.stderr or init_proc.stdout or "unknown git error").strip()
                    raise typer.BadParameter(f"Failed to initialize Git repository: {error_message}")
                typer.echo(f"Initialized Git repository in {project_root}")
            else:
                typer.echo("Git repository already detected.")

            gitignore_path = project_root / GITIGNORE_FILE_NAME
            if not gitignore_path.exists():
                gitignore_path.write_text(DEFAULT_GITIGNORE_CONTENT)
                typer.echo(f"Created {gitignore_path}")

    @app.command("index")
    def index_remote(
        path: str = typer.Argument(
            ".",
            help="Nested path inside active analysis directory to index.",
            autocompletion=_complete_active_analysis_paths,
        ),
        remote_path: Optional[str] = typer.Option(
            None,
            "--remote-path",
            help="Remote relative path under the active analysis root to index (can include remote-only directories).",
            autocompletion=_complete_index_remote_paths,
        ),
        all_tags: bool = typer.Option(False, "--all-tags", help="Index all tagged paths for the active analysis"),
        dry_run: bool = typer.Option(False, "--dry-run", help="Preview index scopes without writing manifests"),
        as_json: bool = typer.Option(False, "--json", help="Print resulting index payload as JSON"),
    ) -> None:
        """Create metadata-only index manifests for remote analysis paths without pulling files."""
        try:
            operation_context = resolve_context()
            result = execute_index_operation(
                operation_context,
                IndexRequest(local_path=path, remote_path=remote_path, all_tags=all_tags, dry_run=dry_run),
                resolve_scopes=_resolve_index_scopes,
                write_manifest=_write_index_manifest_for_scope,
            )
        except (ContextError, OperationError) as exc:
            raise typer.BadParameter(str(exc)) from exc

        if result.dry_run:
            payload = {"dry_run": True, "scopes": result.plan.scopes}
            if as_json:
                typer.echo(json.dumps(payload, indent=2))
            else:
                typer.echo("Dry run: no changes will be made")
                for scope in result.plan.scopes:
                    typer.echo(f"- scope={scope['scope']} remote={scope['remote_scope']}")
            return

        if as_json:
            typer.echo(json.dumps(result.manifests, indent=2))
            return

        typer.echo("Index complete (metadata only, no file transfer).")
        for item in result.manifests:
            typer.echo(
                f"- scope={item['scope']} entries={item['entry_count']} remote={item['remote_scope']} manifest={item['manifest_path']}"
            )
