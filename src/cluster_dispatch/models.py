from __future__ import annotations

from dataclasses import asdict, dataclass, field


@dataclass(frozen=True)
class ResourceSpec:
    profile: str | None
    cpus: int
    memory: str
    time: str
    node: str
    queue: str = ""
    parallel_environment: str = ""


@dataclass(frozen=True)
class RunRequest:
    command: str
    job_name: str
    resources: ResourceSpec
    run_id: str | None = None
    submission_mode: str = "single"
    sync_before: bool = True
    wait_for_completion: bool = False
    record_fields: dict[str, object] = field(default_factory=dict)


@dataclass(frozen=True)
class PushRequest:
    dry_run: bool = False
    record: bool = True


@dataclass(frozen=True)
class PullRequest:
    include_remote_only: bool = False
    all_paths: bool = False
    dry_run: bool = False
    confirmed: bool = False


@dataclass(frozen=True)
class IndexRequest:
    local_path: str = "."
    remote_path: str | None = None
    all_tags: bool = False
    dry_run: bool = False


@dataclass(frozen=True)
class RunPaths:
    project_root: str
    analysis_local_path: str
    analysis_remote_path: str
    remote_run_dir: str
    remote_log_file: str
    submit_script: str
    working_dir: str


@dataclass
class JobRecord:
    submitted_at: str
    analysis: str
    analysis_tags: list[str]
    target: str
    scheduler: str
    job_id: str
    run_id: str
    job_name: str
    state: str
    stdout: str
    stderr: str
    working_dir: str
    cpus: int
    memory: str
    time: str
    queue: str
    parallel_environment: str
    node: str
    command: str
    command_resolved: str
    remote_run_dir: str
    remote_log_file: str
    submission_mode: str
    record_version: int
    resources: ResourceSpec
    paths: RunPaths
    sync: dict[str, object]
    environment: dict[str, object]

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


@dataclass
class SyncRecord:
    action: str
    target: str
    source: str
    destination: str
    analysis: str
    scheduler: str
    transport: str
    ignore_file: str | None = None
    ignore_used: bool = False
    mode: str | None = None
    include_remote_only_tags: bool | None = None
    pulled: list[str] | None = None
    skipped: list[str] | None = None

    def to_dict(self) -> dict[str, object]:
        payload = asdict(self)
        return {key: value for key, value in payload.items() if value is not None}
