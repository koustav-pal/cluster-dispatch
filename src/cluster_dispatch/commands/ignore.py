from __future__ import annotations

import typer

from cluster_dispatch.commands.common import ignore_file as _ignore_file
from cluster_dispatch.commands.runtime import project_root as _project_root

IGNORE_FILE_NAME = ".cdpignore"


def register(ignore_app: typer.Typer) -> None:
    @ignore_app.command("list")
    def ignore_list() -> None:
        """List .cdpignore patterns."""
        project_root = _project_root()
        ignore_path = _ignore_file(project_root)
        if not ignore_path.exists():
            typer.echo(f"{IGNORE_FILE_NAME} not found in project root")
            return
        lines = ignore_path.read_text().splitlines()
        patterns = [line for line in lines if line.strip() and not line.lstrip().startswith("#")]
        if not patterns:
            typer.echo(f"No ignore patterns in {IGNORE_FILE_NAME}")
            return
        typer.echo(f"Ignore patterns in {IGNORE_FILE_NAME}:")
        for pattern in patterns:
            typer.echo(f"- {pattern}")

    @ignore_app.command("add")
    def ignore_add(
        pattern: str = typer.Argument(..., help="Pattern to add to .cdpignore"),
    ) -> None:
        """Add a pattern to .cdpignore."""
        clean = pattern.strip()
        if not clean:
            raise typer.BadParameter("Pattern cannot be empty")
        project_root = _project_root()
        ignore_path = _ignore_file(project_root)
        existing = ignore_path.read_text().splitlines() if ignore_path.exists() else []
        if clean in existing:
            typer.echo(f"Already present: {clean}")
            return
        with ignore_path.open("a", encoding="utf-8") as handle:
            if existing and existing[-1].strip():
                handle.write("\n")
            handle.write(f"{clean}\n")
        typer.echo(f"Added to {IGNORE_FILE_NAME}: {clean}")

    @ignore_app.command("remove")
    def ignore_remove(
        pattern: str = typer.Argument(..., help="Exact pattern to remove from .cdpignore"),
    ) -> None:
        """Remove an exact pattern from .cdpignore."""
        clean = pattern.strip()
        if not clean:
            raise typer.BadParameter("Pattern cannot be empty")
        project_root = _project_root()
        ignore_path = _ignore_file(project_root)
        if not ignore_path.exists():
            raise typer.BadParameter(f"{IGNORE_FILE_NAME} not found in project root")
        lines = ignore_path.read_text().splitlines()
        filtered = [line for line in lines if line != clean]
        if len(filtered) == len(lines):
            typer.echo(f"Pattern not found: {clean}")
            return
        ignore_path.write_text("\n".join(filtered).rstrip() + "\n")
        typer.echo(f"Removed from {IGNORE_FILE_NAME}: {clean}")
