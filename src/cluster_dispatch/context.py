from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from cluster_dispatch.config import ProjectConfig, TargetConfig, find_project_root, load_config


class ContextError(ValueError):
    pass


@dataclass(frozen=True)
class Context:
    project_root: Path
    analysis: Path
    local_analysis: Path
    target: TargetConfig
    remote_analysis: Path
    workdir: Path
    remote_workdir: Path
    target_name: str
    config: ProjectConfig


def resolve_context(
    start: Path | None = None,
    *,
    cwd: Path | None = None,
    target_name: str | None = None,
) -> Context:
    """Resolve project, active analysis, target, and matching work directories."""
    project_root = find_project_root(start).resolve()
    config = load_config(project_root)

    analysis_value = (config.active_analysis or "").strip("/")
    if not analysis_value:
        raise ContextError("No active analysis set. Run: cdp analysis use <path>")
    analysis = Path(analysis_value)
    local_analysis = (project_root / analysis).resolve()
    if not local_analysis.exists() or not local_analysis.is_dir():
        raise ContextError(f"Active analysis path missing: {local_analysis}")

    resolved_target_name = target_name or config.default_target
    if resolved_target_name not in config.targets:
        raise ContextError(
            f"Target '{resolved_target_name}' not configured. Add with: cdp target add {resolved_target_name} ..."
        )
    target = config.targets[resolved_target_name]
    if not target.remote_root:
        raise ContextError(
            f"Target '{resolved_target_name}' has no remote_root configured. "
            f"Update it with `cdp target add {resolved_target_name} --remote-root ...`."
        )

    remote_analysis = Path(target.remote_root.rstrip("/")) / analysis
    requested_cwd = (cwd or Path.cwd()).resolve()
    try:
        relative_workdir = requested_cwd.relative_to(local_analysis)
        workdir = requested_cwd
    except ValueError:
        relative_workdir = Path(".")
        workdir = local_analysis
    remote_workdir = remote_analysis / relative_workdir

    return Context(
        project_root=project_root,
        analysis=analysis,
        local_analysis=local_analysis,
        target=target,
        remote_analysis=remote_analysis,
        workdir=workdir,
        remote_workdir=remote_workdir,
        target_name=resolved_target_name,
        config=config,
    )
