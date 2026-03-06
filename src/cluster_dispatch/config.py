from __future__ import annotations

import json
import re
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any

import yaml

CONFIG_DIR = ".project_control"
CONFIG_NAME = "config.yml"
LEGACY_CONFIG_NAME = "project_control.yml"
STATE_DIR = CONFIG_DIR
JOBS_DIR = "jobs"
STATE_FILE = "state.json"


@dataclass
class TargetConfig:
    host: str
    scheduler: str  # sge | univa | pbs | slurm | lsf | none
    remote_root: str
    template_header: str = ""
    default_cpus: int = 1
    default_memory: str = "8G"
    default_time: str = "01:00:00"
    default_node: str = "1"
    default_queue: str = ""
    default_parallel_environment: str = ""


@dataclass
class LastJob:
    job_id: str
    target: str
    scheduler: str
    remote_run_dir: str
    remote_log_file: str
    run_id: str = ""


@dataclass
class ProjectConfig:
    scheduler: str
    default_target: str
    targets: dict[str, TargetConfig] = field(default_factory=dict)
    active_analysis: str | None = None
    analysis_tags: dict[str, list[str]] = field(default_factory=dict)
    resource_profiles: dict[str, dict[str, Any]] = field(default_factory=dict)


def _parse_targets(raw: dict[str, Any]) -> dict[str, TargetConfig]:
    parsed: dict[str, TargetConfig] = {}
    for name, data in raw.items():
        parsed[name] = TargetConfig(
            host=data["host"],
            scheduler=data["scheduler"],
            remote_root=data.get("remote_root", ""),
            template_header=data.get("template_header", ""),
            default_cpus=int(data.get("default_cpus", 1)),
            default_memory=data.get("default_memory", "8G"),
            default_time=data.get("default_time", "01:00:00"),
            default_node=data.get("default_node", "1"),
            default_queue=data.get("default_queue", ""),
            default_parallel_environment=data.get("default_parallel_environment", ""),
        )
    return parsed


def control_dir(project_root: Path) -> Path:
    return project_root / CONFIG_DIR


def config_path(project_root: Path) -> Path:
    return control_dir(project_root) / CONFIG_NAME


def legacy_config_path(project_root: Path) -> Path:
    return project_root / LEGACY_CONFIG_NAME


def find_project_root(start: Path | None = None) -> Path:
    here = (start or Path.cwd()).resolve()
    for candidate in [here, *here.parents]:
        if config_path(candidate).exists() or legacy_config_path(candidate).exists():
            return candidate
    raise FileNotFoundError(
        f"Could not find {CONFIG_DIR}/{CONFIG_NAME} (or legacy {LEGACY_CONFIG_NAME}) from {here}"
    )


def load_config(project_root: Path) -> ProjectConfig:
    cfg_path = config_path(project_root)
    if not cfg_path.exists():
        legacy_path = legacy_config_path(project_root)
        if legacy_path.exists():
            cfg_path = legacy_path
        else:
            raise FileNotFoundError(f"Could not find config at {cfg_path}")

    raw = yaml.safe_load(cfg_path.read_text())
    return ProjectConfig(
        scheduler=raw.get("scheduler", "none"),
        default_target=raw["default_target"],
        targets=_parse_targets(raw.get("targets", {})),
        active_analysis=raw.get("active_analysis"),
        analysis_tags=raw.get("analysis_tags", {}),
        resource_profiles=raw.get("resource_profiles", {}) or {},
    )


def save_config(project_root: Path, cfg: ProjectConfig) -> None:
    cfg_path = config_path(project_root)
    cfg_path.parent.mkdir(parents=True, exist_ok=True)
    data = asdict(cfg)
    cfg_path.write_text(yaml.safe_dump(data, sort_keys=False))


def ensure_state_dirs(project_root: Path) -> Path:
    state_root = control_dir(project_root)
    (state_root / JOBS_DIR).mkdir(parents=True, exist_ok=True)
    return state_root


def load_state(project_root: Path) -> dict[str, Any]:
    state_root = ensure_state_dirs(project_root)
    state_file = state_root / STATE_FILE
    if not state_file.exists():
        return {}
    return json.loads(state_file.read_text())


def save_state(project_root: Path, state: dict[str, Any]) -> None:
    state_root = ensure_state_dirs(project_root)
    state_file = state_root / STATE_FILE
    state_file.write_text(json.dumps(state, indent=2))


def append_job_record(project_root: Path, payload: dict[str, Any]) -> Path:
    state_root = ensure_state_dirs(project_root)
    stamp = payload.get("submitted_at", "unknown").replace(":", "-")
    raw_parts = [
        stamp,
        str(payload.get("job_id", "")).strip(),
        str(payload.get("job_name", "")).strip(),
        str(payload.get("target", "")).strip(),
    ]
    clean_parts = [re.sub(r"[^A-Za-z0-9_.-]+", "_", part).strip("._-") for part in raw_parts if part]
    stem = "__".join(clean_parts) if clean_parts else "job"
    path = state_root / JOBS_DIR / f"{stem}.json"
    counter = 1
    while path.exists():
        path = state_root / JOBS_DIR / f"{stem}__{counter}.json"
        counter += 1
    path.write_text(json.dumps(payload, indent=2))
    return path
