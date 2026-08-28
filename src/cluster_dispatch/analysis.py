from __future__ import annotations

import shlex
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Callable

from cluster_dispatch.config import ProjectConfig, load_config, save_config
from cluster_dispatch.index import (
    index_entry_path_exists,
    list_names_from_index_manifest,
    load_index_manifest,
)
from cluster_dispatch.transport import is_local_transport, ssh_command


class AnalysisError(RuntimeError):
    pass


@dataclass(frozen=True)
class AnalysisListing:
    entries: list[str]
    path: str
    remote: bool
    from_index: bool = False
    manifest_path: str | None = None


def _analysis_root(project_root: Path, cfg: ProjectConfig) -> tuple[Path, str]:
    analysis_rel = (cfg.active_analysis or "").strip("/")
    if not analysis_rel:
        raise AnalysisError("No active analysis set. Run: cdp analysis use <path>")
    root = (project_root / analysis_rel).resolve()
    if not root.is_dir():
        raise AnalysisError(f"Active analysis path missing: {root}")
    return root, analysis_rel


def _safe_relative(root: Path, path: str | Path) -> tuple[Path, str]:
    value = Path(path).expanduser()
    if value.is_absolute():
        raise AnalysisError("Path must be relative to the active analysis directory")
    candidate = (root / value).resolve()
    try:
        relative = candidate.relative_to(root)
    except ValueError as exc:
        raise AnalysisError("Path must stay inside the active analysis directory") from exc
    return candidate, relative.as_posix()


def _target(cfg: ProjectConfig):
    name = cfg.default_target
    if name not in cfg.targets:
        raise AnalysisError(f"Default target '{name}' not configured")
    return name, cfg.targets[name]


def set_active_analysis(project_root: Path, path: Path) -> str:
    cfg = load_config(project_root)
    resolved = path.resolve()
    try:
        relative = resolved.relative_to(project_root)
    except ValueError as exc:
        raise AnalysisError("Analysis path must be within project root") from exc
    if not resolved.is_dir():
        raise AnalysisError(f"Analysis directory not found: {resolved}")
    cfg.active_analysis = relative.as_posix()
    save_config(project_root, cfg)
    return cfg.active_analysis


def add_analysis_tag(
    project_root: Path,
    path: Path,
    *,
    remote: bool = False,
    run: Callable[..., subprocess.CompletedProcess[str]] = subprocess.run,
) -> tuple[str, str, bool]:
    cfg = load_config(project_root)
    root, analysis_rel = _analysis_root(project_root, cfg)
    candidate, tag = _safe_relative(root, path)
    validation = "local"
    if not remote:
        if not candidate.exists():
            raise AnalysisError(f"Tag path not found locally: {candidate}")
    else:
        target_name, target = _target(cfg)
        remote_root = f"{target.remote_root.rstrip('/')}/{analysis_rel}"
        remote_path = f"{remote_root}/{tag}"
        manifest = load_index_manifest(project_root, target_name, analysis_rel, ".")
        if manifest is not None and index_entry_path_exists(manifest, tag):
            validation = "index"
        elif is_local_transport(target.transport, target.host):
            if not Path(remote_path).exists():
                raise AnalysisError(f"Tag path not found remotely: {remote_path}")
            validation = "live"
        else:
            shell = f"if [ -e {shlex.quote(remote_path)} ]; then echo OK; else echo MISSING; fi"
            proc = run(
                ssh_command(project_root, cfg, target_name, target, shell),
                text=True,
                capture_output=True,
                check=False,
            )
            if proc.returncode != 0 or proc.stdout.strip() != "OK":
                raise AnalysisError(f"Tag path not found remotely: {remote_path}")
            validation = "live"
    tags = cfg.analysis_tags.get(analysis_rel, [])
    added = tag not in tags
    if added:
        cfg.analysis_tags[analysis_rel] = sorted([*tags, tag])
        save_config(project_root, cfg)
    return tag, validation, added


def remove_analysis_tag(project_root: Path, path: Path) -> str:
    cfg = load_config(project_root)
    root, analysis_rel = _analysis_root(project_root, cfg)
    _, tag = _safe_relative(root, path)
    tags = cfg.analysis_tags.get(analysis_rel, [])
    if tag not in tags:
        raise AnalysisError(f"Path is not tagged: {tag}")
    remaining = [item for item in tags if item != tag]
    if remaining:
        cfg.analysis_tags[analysis_rel] = sorted(remaining)
    else:
        cfg.analysis_tags.pop(analysis_rel, None)
    save_config(project_root, cfg)
    return tag


def list_analysis_entries(
    project_root: Path,
    path: str = ".",
    *,
    remote: bool = False,
    include_files: bool = False,
    run: Callable[..., subprocess.CompletedProcess[str]] = subprocess.run,
) -> AnalysisListing:
    cfg = load_config(project_root)
    root, analysis_rel = _analysis_root(project_root, cfg)
    local_path, relative = _safe_relative(root, path)
    if not remote:
        if not local_path.exists():
            raise AnalysisError(f"Local path not found: {local_path}")
        if not local_path.is_dir():
            raise AnalysisError(f"Local path is not a directory: {local_path}")
        entries = sorted(item.name for item in local_path.iterdir() if include_files or item.is_dir())
        return AnalysisListing(entries, str(local_path), False)

    target_name, target = _target(cfg)
    remote_analysis = f"{target.remote_root.rstrip('/')}/{analysis_rel}"
    remote_path = remote_analysis if relative in {"", "."} else f"{remote_analysis}/{relative}"
    scope = "." if relative in {"", "."} else relative.strip("/")
    manifest = load_index_manifest(project_root, target_name, analysis_rel, scope)
    if manifest is not None:
        return AnalysisListing(
            list_names_from_index_manifest(manifest, include_files=include_files),
            remote_path,
            True,
            True,
            str(manifest.get("_manifest_path", "")) or None,
        )
    shell = (
        f"P={shlex.quote(remote_path)}; if [ -d \"$P\" ]; then "
        + ('for d in "$P"/*; do basename "$d"; done | sort; ' if include_files else 'for d in "$P"/*; do [ -d "$d" ] && basename "$d"; done | sort; ')
        + "fi"
    )
    argv = ["bash", "-lc", shell] if is_local_transport(target.transport, target.host) else ssh_command(
        project_root, cfg, target_name, target, shell
    )
    proc = run(argv, text=True, capture_output=True, check=False)
    if proc.returncode != 0:
        raise AnalysisError(proc.stderr.strip() or "Failed to list remote directories")
    return AnalysisListing([line.strip() for line in proc.stdout.splitlines() if line.strip()], remote_path, True)
