from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import typer

from cluster_dispatch.config import CONFIG_DIR, ProjectConfig, TargetConfig, load_config
from cluster_dispatch.index import load_index_manifest
from cluster_dispatch.profiles import (
    DEFAULT_RESOURCE_PROFILES,
    ProfileError,
    all_resource_profiles,
    resource_spec_from_profile,
    validate_profile_name,
)
from cluster_dispatch.records import list_jobs
from cluster_dispatch.models import ResourceSpec
from cluster_dispatch.commands.runtime import project_root

SWEEPS_DIR_NAME = "sweeps"


def require_yes(yes: bool, message: str) -> None:
    if not yes:
        raise typer.BadParameter(message)


def ignore_file(root: Path) -> Path:
    return root / ".cdpignore"


def template_path(root: Path) -> Path:
    return root / CONFIG_DIR / "templates" / "scheduler_header.tmpl"


def all_profiles(cfg: ProjectConfig) -> dict[str, dict[str, str | int]]:
    return all_resource_profiles(cfg)


def profile_name(value: str) -> str:
    try:
        return validate_profile_name(value)
    except ProfileError as exc:
        raise typer.BadParameter(str(exc)) from exc


def resources(profile: str | None, cfg: ProjectConfig, target: TargetConfig, **overrides: Any) -> ResourceSpec:
    try:
        return resource_spec_from_profile(profile, cfg, target, **overrides)
    except ProfileError as exc:
        raise typer.BadParameter(str(exc)) from exc


def _safe_root() -> Path | None:
    try:
        return project_root()
    except Exception:
        return None


def _prefix(values: list[str], incomplete: str) -> list[str]:
    return [value for value in values if value.startswith(incomplete or "")]


def complete_targets(incomplete: str) -> list[str]:
    root = _safe_root()
    if root is None:
        return []
    try:
        return _prefix(sorted(load_config(root).targets), incomplete)
    except Exception:
        return []


def complete_profiles(incomplete: str) -> list[str]:
    root = _safe_root()
    try:
        names = all_profiles(load_config(root)).keys() if root else DEFAULT_RESOURCE_PROFILES.keys()
        return _prefix(sorted(names), incomplete)
    except Exception:
        return _prefix(sorted(DEFAULT_RESOURCE_PROFILES), incomplete)


def complete_user_profiles(incomplete: str) -> list[str]:
    root = _safe_root()
    if root is None:
        return []
    try:
        return _prefix(sorted(load_config(root).resource_profiles), incomplete)
    except Exception:
        return []


def complete_analysis_dirs(incomplete: str) -> list[str]:
    root = _safe_root()
    if root is None:
        return []
    values = []
    for path in root.rglob("*"):
        if not path.is_dir() or path.name in {".git", "__pycache__"}:
            continue
        relative = path.relative_to(root).as_posix()
        if not relative.startswith(f"{CONFIG_DIR}/"):
            values.append(relative)
    return _prefix(sorted(set(values)), incomplete)


def complete_active_paths(incomplete: str) -> list[str]:
    root = _safe_root()
    if root is None:
        return []
    try:
        cfg = load_config(root)
        analysis = (root / str(cfg.active_analysis)).resolve()
        if not cfg.active_analysis or not analysis.is_dir():
            return []
    except Exception:
        return []
    return _prefix(sorted(path.relative_to(analysis).as_posix() for path in analysis.rglob("*")), incomplete)


def complete_sweeps(incomplete: str) -> list[str]:
    root = _safe_root()
    if root is None:
        return []
    directory = root / CONFIG_DIR / SWEEPS_DIR_NAME
    if not directory.exists():
        return []
    try:
        active = (load_config(root).active_analysis or "").strip("/")
    except Exception:
        active = ""
    values = []
    for path in sorted(directory.glob("*.json")):
        if active:
            try:
                if str(json.loads(path.read_text()).get("analysis", "")).strip("/") != active:
                    continue
            except json.JSONDecodeError:
                continue
        values.append(path.stem)
    return _prefix(values, incomplete)


def _record_values(field: str, incomplete: str) -> list[str]:
    root = _safe_root()
    if root is None:
        return []
    seen: set[str] = set()
    values = []
    for record in list_jobs(root):
        value = str(record.get(field, "")).strip()
        if value and value not in seen:
            seen.add(value)
            values.append(value)
    return _prefix(values, incomplete)


def complete_job_ids(incomplete: str) -> list[str]:
    return _record_values("job_id", incomplete)


def complete_job_names(incomplete: str) -> list[str]:
    return _record_values("job_name", incomplete)


def complete_record_analyses(incomplete: str) -> list[str]:
    return _record_values("analysis", incomplete)


def complete_index_paths(incomplete: str) -> list[str]:
    root = _safe_root()
    if root is None:
        return []
    try:
        cfg = load_config(root)
        analysis = (cfg.active_analysis or "").strip("/")
        manifest = load_index_manifest(root, cfg.default_target, analysis, ".")
    except Exception:
        return []
    if not analysis or manifest is None or not isinstance(manifest.get("entries"), list):
        return []
    values: set[str] = set()
    for entry in manifest["entries"]:
        if not isinstance(entry, dict):
            continue
        relative = str(entry.get("path", "")).strip("/")
        if relative and relative != ".":
            values.add(relative)
            if "/" in relative:
                values.add(relative.rsplit("/", 1)[0])
    return _prefix(sorted(values), incomplete)
