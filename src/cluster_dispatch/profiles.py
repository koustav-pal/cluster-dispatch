from __future__ import annotations

import re

from cluster_dispatch.config import ProjectConfig, TargetConfig
from cluster_dispatch.models import ResourceSpec


DEFAULT_RESOURCE_PROFILES: dict[str, dict[str, str | int]] = {
    "small": {
        "cpus": 1,
        "memory": "8G",
        "time": "01:00:00",
        "node": "1",
        "queue": "",
        "parallel_environment": "",
    },
    "long": {
        "cpus": 2,
        "memory": "16G",
        "time": "24:00:00",
        "node": "1",
        "queue": "",
        "parallel_environment": "",
    },
    "highmem": {
        "cpus": 4,
        "memory": "128G",
        "time": "08:00:00",
        "node": "1",
        "queue": "",
        "parallel_environment": "",
    },
}


class ProfileError(ValueError):
    pass


def all_resource_profiles(cfg: ProjectConfig) -> dict[str, dict[str, str | int]]:
    merged = {name: dict(values) for name, values in DEFAULT_RESOURCE_PROFILES.items()}
    for name, values in cfg.resource_profiles.items():
        merged[name.strip().lower()] = dict(values)
    return merged


def resolve_resource_profile(
    profile: str | None,
    cfg: ProjectConfig,
) -> dict[str, str | int]:
    if not profile:
        return {}
    key = profile.strip().lower()
    profiles = all_resource_profiles(cfg)
    if key not in profiles:
        raise ProfileError(
            f"Unknown profile '{profile}'. Available profiles: {', '.join(sorted(profiles))}"
        )
    return dict(profiles[key])


def validate_profile_name(name: str) -> str:
    normalized = name.strip().lower()
    if not normalized:
        raise ProfileError("Profile name cannot be empty")
    if not re.fullmatch(r"[a-z0-9_.-]+", normalized):
        raise ProfileError("Profile name may contain only: a-z, 0-9, _, ., -")
    return normalized


def resource_spec_from_profile(
    profile: str | None,
    cfg: ProjectConfig,
    target: TargetConfig,
    *,
    cpus: int | None = None,
    memory: str | None = None,
    walltime: str | None = None,
    node: str | None = None,
    queue: str | None = None,
    parallel_environment: str | None = None,
) -> ResourceSpec:
    values = resolve_resource_profile(profile, cfg)
    return ResourceSpec(
        profile=profile,
        cpus=cpus if cpus is not None else int(values.get("cpus", target.default_cpus)),
        memory=memory if memory is not None else str(values.get("memory", target.default_memory)),
        time=walltime if walltime is not None else str(values.get("time", target.default_time)),
        node=node or str(values.get("node", target.default_node or "1")),
        queue=queue if queue is not None else str(values.get("queue", target.default_queue)),
        parallel_environment=(
            parallel_environment
            if parallel_environment is not None
            else str(values.get("parallel_environment", target.default_parallel_environment))
        ),
    )
