from __future__ import annotations

import base64
import hashlib
import json
import re
import shlex
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Callable

import yaml

from cluster_dispatch.config import CONFIG_DIR, TargetConfig
from cluster_dispatch.context import Context
from cluster_dispatch.models import PushRequest, ResourceSpec, RunRequest
from cluster_dispatch.operations import (
    OperationError,
    OperationRuntime,
    build_submit_script,
    prepare_run,
    push,
    run as execute_run,
)
from cluster_dispatch.records import build_job_record, list_sweeps, load_sweep, save_job, save_sweep
from cluster_dispatch.schedulers import cancel_command, get_adapter
from cluster_dispatch.transport import is_local_transport, remote_spec, rsync_command, ssh_command


class SweepError(ValueError):
    pass


@dataclass(frozen=True)
class SweepRequest:
    config_file: Path
    command_template: str
    mode: str
    resources: ResourceSpec
    block_filter: str | None = None
    sweep_id: str | None = None
    base_job_name: str | None = None
    submit_delay_ms: int = 5000


@dataclass(frozen=True)
class SweepRuntime:
    operation: OperationRuntime
    run_direct: Callable[..., Any]


@dataclass(frozen=True)
class SweepResult:
    manifest: dict[str, Any]
    submitted: int


@dataclass(frozen=True)
class SweepCancelResult:
    manifest: dict[str, Any]
    cancelled: int


def _slug(value: str) -> str:
    return re.sub(r"[^A-Za-z0-9_.-]+", "-", value.strip()).strip("-.") or "run"


def _run_id(block_name: str, params: dict[str, Any], command_template: str) -> str:
    payload = {"block_name": block_name, "params": params, "command_template": command_template}
    material = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
    return hashlib.sha256(material.encode()).hexdigest()[:16]


def load_sweep_config(path: Path) -> dict[str, Any]:
    if not path.exists() or not path.is_file():
        raise SweepError(f"Sweep config file not found: {path}")
    payload = yaml.safe_load(path.read_text()) or {}
    if not isinstance(payload, dict):
        raise SweepError("Sweep config must be a YAML mapping with top-level key: params")
    return payload


def expand_sweep(
    config: dict[str, Any],
    command_template: str,
    block_filter: str | None = None,
) -> list[dict[str, Any]]:
    params = config.get("params")
    if not isinstance(params, dict) or not params:
        raise SweepError("Sweep config must define non-empty params mapping")
    selected = {block_filter: params.get(block_filter)} if block_filter else params
    if block_filter and selected[block_filter] is None:
        raise SweepError(f"Job '{block_filter}' not found under params in sweep config")

    fields = set(re.findall(r"(?<!{){([A-Za-z_][A-Za-z0-9_]*)}", command_template))
    runs: list[dict[str, Any]] = []
    import itertools

    for block_name, block_params in selected.items():
        if not isinstance(block_params, dict) or not block_params:
            raise SweepError(f"params.{block_name} must be a non-empty mapping")
        keys = list(block_params)
        missing = [key for key in keys if key not in fields]
        if missing:
            raise SweepError(f"Command template missing placeholders for params.{block_name}: {', '.join(missing)}")
        values: list[list[Any]] = []
        for key in keys:
            raw = block_params[key]
            choices = list(raw) if isinstance(raw, (list, tuple)) else [raw]
            if not choices:
                raise SweepError(f"params.{block_name}.{key} must not be empty")
            values.append(choices)
        for index, combination in enumerate(itertools.product(*values), start=1):
            sweep_params = dict(zip(keys, combination))
            render_values = {**sweep_params, "sweep_job": block_name, "sweep_index": index}
            try:
                command = command_template.format(**render_values)
            except KeyError as exc:
                raise SweepError(f"Missing placeholder value for '{exc.args[0]}' in sweep command template") from exc
            runs.append(
                {
                    "run_id": _run_id(str(block_name), sweep_params, command_template),
                    "sweep_job": block_name,
                    "sweep_index": index,
                    "sweep_params": sweep_params,
                    "command": command,
                    "status": "PENDING",
                    "job_id": None,
                    "submitted_at": None,
                }
            )
    return runs


def _manifest(context: Context, request: SweepRequest, sweep_id: str, runs: list[dict[str, Any]]) -> dict[str, Any]:
    now = datetime.now().isoformat(timespec="seconds")
    return {
        "sweep_id": sweep_id,
        "created_at": now,
        "updated_at": now,
        "mode": request.mode,
        "analysis": context.analysis.as_posix(),
        "target": context.target_name,
        "host": context.target.host,
        "scheduler": context.target.scheduler,
        "config_file": str(request.config_file.resolve()),
        "command_template": request.command_template,
        "remote_sweep_dir": f"{context.remote_analysis}/sweeps/{sweep_id}",
        "array_job_id": None,
        "resources": {
            "profile": request.resources.profile,
            "cpus": request.resources.cpus,
            "memory": request.resources.memory,
            "time": request.resources.time,
            "node": request.resources.node,
            "queue": request.resources.queue,
            "parallel_environment": request.resources.parallel_environment,
        },
        "submit_delay_ms": request.submit_delay_ms,
        "runs": runs,
    }


def _save(context: Context, manifest: dict[str, Any]) -> None:
    manifest["updated_at"] = datetime.now().isoformat(timespec="seconds")
    save_sweep(context.project_root, str(manifest["sweep_id"]), manifest)


def require_active_analysis(context: Context, manifest: dict[str, Any]) -> None:
    analysis = str(manifest.get("analysis", "")).strip("/")
    if not analysis:
        raise SweepError("Sweep manifest is missing analysis context")
    if analysis != context.analysis.as_posix():
        raise SweepError(
            f"Sweep belongs to analysis '{analysis}', but active analysis is '{context.analysis.as_posix()}'. "
            f"Switch context with `cdp analysis use {analysis}`."
        )


def _record_fields(manifest: dict[str, Any], item: dict[str, Any]) -> dict[str, object]:
    return {
        "sweep_id": str(manifest["sweep_id"]),
        "sweep_job": item.get("sweep_job", ""),
        "sweep_index": item.get("sweep_index"),
        "sweep_params": item.get("sweep_params", {}),
    }


def _submit_normal(
    context: Context,
    manifest: dict[str, Any],
    indexes: list[int],
    resources: ResourceSpec,
    base_name: str,
    runtime: SweepRuntime,
    *,
    local: bool,
) -> int:
    run_context = context
    if local:
        local_target = TargetConfig(
            host="localhost",
            scheduler="none",
            remote_root=str(context.project_root),
            transport="local",
        )
        run_context = Context(
            project_root=context.project_root,
            analysis=context.analysis,
            local_analysis=context.local_analysis,
            target=local_target,
            remote_analysis=context.local_analysis,
            workdir=context.workdir,
            remote_workdir=context.workdir,
            target_name=context.target_name,
            config=context.config,
        )

    submitted = 0
    delay_ms = int(manifest.get("submit_delay_ms", 5000))
    for offset, index in enumerate(indexes):
        item = manifest["runs"][index]
        name = f"{base_name}-{_slug(str(item['sweep_job']))}-{int(item['sweep_index']):03d}"
        request = RunRequest(
            command=str(item["command"]),
            job_name=name,
            resources=resources,
            run_id=str(item["run_id"]),
            submission_mode="local" if local else "single",
            sync_before=(offset == 0 and not local),
            wait_for_completion=local,
            record_fields=_record_fields(manifest, item),
        )
        try:
            result = execute_run(run_context, request, dry_run=False, runtime=runtime.operation)
        except OperationError as exc:
            raise SweepError(f"Failed to submit sweep run {item.get('run_id', '')}: {exc}") from exc
        item.update(
            {
                "job_id": result.job_id,
                "job_name": name,
                "status": result.state if local else "SUBMITTED",
                "submitted_at": datetime.now().isoformat(timespec="seconds"),
                "submission_mode": "local" if local else "single",
                "resources": manifest["resources"],
            }
        )
        if local:
            item["completed_at"] = datetime.now().isoformat(timespec="seconds")
            item["exit_code"] = result.exit_code
        submitted += 1
        if not local and offset < len(indexes) - 1:
            time.sleep(delay_ms / 1000.0)
    return submitted


def _insert_directive(header: str, directive: str, prefix: str) -> str:
    lines = header.rstrip().splitlines()
    positions = [i for i, line in enumerate(lines) if line.lstrip().startswith(prefix)]
    at = positions[-1] + 1 if positions else 0
    lines.insert(at, directive.rstrip())
    return "\n".join(lines) + "\n"


def _submit_array(
    context: Context,
    manifest: dict[str, Any],
    indexes: list[int],
    resources: ResourceSpec,
    base_name: str,
    runtime: SweepRuntime,
) -> int:
    if not indexes:
        return 0
    adapter = get_adapter(context.target.scheduler)
    try:
        array = adapter.array_spec(job_name=f"{base_name}-array", task_count=len(indexes))
    except (RuntimeError, ValueError) as exc:
        raise SweepError(str(exc)) from exc

    push(context, PushRequest(record=False), run_command=runtime.operation.run_command)
    remote_dir = str(manifest["remote_sweep_dir"])
    local_transport = is_local_transport(context.target.transport, context.target.host)
    if local_transport:
        Path(remote_dir).mkdir(parents=True, exist_ok=True)
    else:
        runtime.operation.run_command(
            ssh_command(context.project_root, context.config, context.target_name, context.target, "mkdir", "-p", remote_dir)
        )

    sweep_id = str(manifest["sweep_id"])
    local_dir = context.project_root / CONFIG_DIR / "sweeps"
    local_dir.mkdir(parents=True, exist_ok=True)
    mapping = local_dir / f"{sweep_id}.array.tsv"
    wrapper = local_dir / f"{sweep_id}.array_wrapper.sh"
    rows: list[str] = []
    for task_id, index in enumerate(indexes, start=1):
        item = manifest["runs"][index]
        name = f"{base_name}-{_slug(str(item['sweep_job']))}-{int(item['sweep_index']):03d}"
        encoded = base64.b64encode(str(item["command"]).encode()).decode("ascii")
        rows.append(f"{task_id}\t{item['run_id']}\t{name}\t{encoded}")
    mapping.write_text("\n".join(rows) + "\n")
    wrapper.write_text(
        "#!/usr/bin/env bash\nset -euo pipefail\nMAP_FILE=\"$1\"\n"
        f"TASK_ID=\"{array.task_id_expression}\"\n"
        "LINE=\"$(awk -F $'\\t' -v id=\"$TASK_ID\" '$1==id {print; exit}' \"$MAP_FILE\")\"\n"
        "if [ -z \"$LINE\" ]; then echo \"No mapping found for task ${TASK_ID}\" >&2; exit 1; fi\n"
        "RUN_ID=\"$(printf '%s' \"$LINE\" | cut -f2)\"\n"
        "JOB_NAME=\"$(printf '%s' \"$LINE\" | cut -f3)\"\n"
        "CMD_B64=\"$(printf '%s' \"$LINE\" | cut -f4)\"\n"
        "CMD=\"$(printf '%s' \"$CMD_B64\" | base64 --decode)\"\n"
        "echo \"[cluster-dispatch] run_id=${RUN_ID} job_name=${JOB_NAME} task_id=${TASK_ID}\"\n"
        "eval \"$CMD\"\n"
    )
    wrapper.chmod(0o755)
    mapping_remote = f"{remote_dir}/{mapping.name}"
    wrapper_remote = f"{remote_dir}/{wrapper.name}"
    for source, destination in ((mapping, mapping_remote), (wrapper, wrapper_remote)):
        command = ["rsync", "-az", str(source), destination if local_transport else remote_spec(context.target_name, destination)]
        if not local_transport:
            command = rsync_command(command, context.project_root, context.config, context.target_name, context.target)
        runtime.operation.run_command(command)

    header = runtime.operation.render_scheduler_header(
        context.target.template_header,
        cpus=resources.cpus,
        memory=resources.memory,
        walltime=resources.time,
        job_name=f"{base_name}-array",
        stdout=f"{base_name}-array",
        stderr=f"{base_name}-array",
        working_dir=str(context.remote_analysis),
        queue=resources.queue,
        node=resources.node,
        parallel_environment=resources.parallel_environment,
    )
    header = _insert_directive(header, array.directive, array.directive_prefix)
    submit_path = f"{remote_dir}/pc_submit_array_{sweep_id}.sh"
    log_path = f"{remote_dir}/run_array_{sweep_id}.log"
    script = build_submit_script(
        header,
        f"bash {shlex.quote(wrapper_remote)} {shlex.quote(mapping_remote)}",
        str(context.remote_analysis),
        log_path,
    )
    if local_transport:
        path = Path(submit_path)
        path.write_text(script)
        path.chmod(0o755)
    else:
        runtime.operation.write_remote_script(
            context.project_root,
            context.config,
            context.target_name,
            context.target,
            submit_path,
            script,
            executable=True,
        )
    try:
        base_job_id = adapter.submit(
            context.target.host,
            submit_path,
            transport=context.target.transport,
            project_root=context.project_root,
            cfg=context.config,
            target_name=context.target_name,
            target_cfg=context.target,
        ).job_id
    except Exception as exc:
        raise SweepError(f"Failed to submit sweep array job: {exc}") from exc
    manifest["array_job_id"] = base_job_id

    for task_id, index in enumerate(indexes, start=1):
        item = manifest["runs"][index]
        name = f"{base_name}-{_slug(str(item['sweep_job']))}-{int(item['sweep_index']):03d}"
        submitted_at = datetime.now().isoformat(timespec="seconds")
        item.update(
            {
                "job_id": f"{base_job_id}_{task_id}",
                "array_job_id": base_job_id,
                "array_task_id": task_id,
                "job_name": name,
                "status": "SUBMITTED",
                "submitted_at": submitted_at,
                "submission_mode": "array",
                "resources": manifest["resources"],
            }
        )
        save_job(
            context.project_root,
            build_job_record(
                project_root=context.project_root,
                submitted_at=submitted_at,
                analysis=context.analysis.as_posix(),
                analysis_tags=context.config.analysis_tags.get(context.analysis.as_posix(), []),
                target_name=context.target_name,
                scheduler=context.target.scheduler,
                job_id=str(item["job_id"]),
                run_id=str(item["run_id"]),
                job_name=name,
                state="RUNNING_OR_QUEUED",
                command=str(item["command"]),
                remote_run_dir=remote_dir,
                remote_log_file=log_path,
                working_dir=str(context.remote_analysis),
                cpus=resources.cpus,
                memory=resources.memory,
                walltime=resources.time,
                node=resources.node,
                queue=resources.queue,
                parallel_environment=resources.parallel_environment,
                profile_name=resources.profile,
                submit_script_path=submit_path,
                submission_mode="array",
                sync_source=str(context.local_analysis),
                sync_destination=str(context.remote_analysis),
                ignore_file_path=str(context.project_root / ".cdpignore") if (context.project_root / ".cdpignore").exists() else None,
                ignore_used=(context.project_root / ".cdpignore").exists(),
                git_info=runtime.operation.git_info(context.project_root),
                record_fields=_record_fields(manifest, item),
            ),
        )
    return len(indexes)


def _resources_from_manifest(manifest: dict[str, Any], target: TargetConfig) -> ResourceSpec:
    values = manifest.get("resources", {})
    return ResourceSpec(
        profile=str(values.get("profile", "")).strip() or None,
        cpus=int(values.get("cpus", target.default_cpus)),
        memory=str(values.get("memory", target.default_memory)),
        time=str(values.get("time", target.default_time)),
        node=str(values.get("node", target.default_node or "1")),
        queue=str(values.get("queue", target.default_queue or "")),
        parallel_environment=str(values.get("parallel_environment", target.default_parallel_environment or "")),
    )


def execute_sweep(context: Context, request: SweepRequest, *, runtime: SweepRuntime) -> SweepResult:
    mode = request.mode.lower()
    if mode not in {"single", "array", "local"}:
        raise SweepError("--mode must be one of: single, array, local")
    if request.submit_delay_ms < 5000:
        raise SweepError("--submit-delay-ms must be at least 5000")
    config = load_sweep_config(request.config_file)
    runs = expand_sweep(config, request.command_template, request.block_filter)
    if not runs:
        raise SweepError("No sweep runs generated from config")
    sweep_id = request.sweep_id or f"sweep-{datetime.now().strftime('%Y%m%d%H%M%S')}-{hashlib.sha256(str(time.time_ns()).encode()).hexdigest()[:6]}"
    try:
        load_sweep(context.project_root, sweep_id)
    except LookupError:
        pass
    else:
        raise SweepError(f"Sweep id already exists: {sweep_id}")
    if mode == "array":
        try:
            get_adapter(context.target.scheduler).array_spec(job_name=sweep_id, task_count=len(runs))
        except (RuntimeError, ValueError) as exc:
            raise SweepError(str(exc)) from exc

    manifest = _manifest(context, request, sweep_id, runs)
    _save(context, manifest)
    base_name = _slug(request.base_job_name or sweep_id)
    indexes = list(range(len(runs)))
    if mode == "array":
        submitted = _submit_array(context, manifest, indexes, request.resources, base_name, runtime)
    else:
        submitted = _submit_normal(
            context,
            manifest,
            indexes,
            request.resources,
            base_name,
            runtime,
            local=mode == "local",
        )
    _save(context, manifest)
    return SweepResult(manifest, submitted)


def validate_sweep(context: Context, request: SweepRequest, *, runtime: SweepRuntime) -> int:
    mode = request.mode.lower()
    if mode not in {"single", "array", "local"}:
        raise SweepError("--mode must be one of: single, array, local")
    config = load_sweep_config(request.config_file)
    runs = expand_sweep(config, request.command_template, request.block_filter)
    if not runs:
        raise SweepError("No sweep runs generated from config")
    if mode == "array":
        try:
            get_adapter(context.target.scheduler).array_spec(job_name="validation", task_count=len(runs))
        except (RuntimeError, ValueError) as exc:
            raise SweepError(str(exc)) from exc
    sample = runs[0]
    try:
        prepare_run(
            context,
            RunRequest(
                command=str(sample["command"]),
                job_name="sweep-validation",
                resources=request.resources,
                run_id=str(sample["run_id"]),
                sync_before=False,
            ),
            render_scheduler_header=runtime.operation.render_scheduler_header,
        )
    except OperationError as exc:
        raise SweepError(str(exc)) from exc
    return len(runs)


def resume_sweep(context: Context, sweep_id: str, *, runtime: SweepRuntime) -> SweepResult:
    try:
        manifest = load_sweep(context.project_root, sweep_id)
    except LookupError as exc:
        raise SweepError(str(exc)) from exc
    require_active_analysis(context, manifest)
    if str(manifest.get("target", "")) != context.target_name:
        raise SweepError(f"Sweep target is '{manifest.get('target')}', not '{context.target_name}'")
    runs = manifest.get("runs")
    if not isinstance(runs, list):
        raise SweepError("Invalid sweep manifest runs format")
    pending = [index for index, item in enumerate(runs) if isinstance(item, dict) and not item.get("job_id")]
    if not pending:
        return SweepResult(manifest, 0)
    resources = _resources_from_manifest(manifest, context.target)
    base_name = _slug(sweep_id)
    mode = str(manifest.get("mode", "single")).lower()
    if mode == "array":
        submitted = _submit_array(context, manifest, pending, resources, base_name, runtime)
    elif mode in {"single", "local"}:
        submitted = _submit_normal(context, manifest, pending, resources, base_name, runtime, local=mode == "local")
    else:
        raise SweepError(f"Unsupported sweep mode in manifest: {mode}")
    _save(context, manifest)
    return SweepResult(manifest, submitted)


def cancel_sweep(context: Context, sweep_id: str, *, runtime: SweepRuntime) -> SweepCancelResult:
    try:
        manifest = load_sweep(context.project_root, sweep_id)
    except LookupError as exc:
        raise SweepError(str(exc)) from exc
    require_active_analysis(context, manifest)
    runs = manifest.get("runs")
    if not isinstance(runs, list):
        raise SweepError("Invalid sweep manifest runs format")
    mode = str(manifest.get("mode", "single")).lower()
    if mode == "local":
        cancelled = 0
        for item in runs:
            if isinstance(item, dict) and item.get("status") == "PENDING":
                item["status"] = "CANCELLED"
                cancelled += 1
        _save(context, manifest)
        return SweepCancelResult(manifest, cancelled)

    ids = [str(manifest["array_job_id"])] if mode == "array" and manifest.get("array_job_id") else []
    if not ids:
        ids = list(dict.fromkeys(str(item.get("job_id", "")) for item in runs if isinstance(item, dict) and item.get("job_id")))
    cancelled = 0
    for job_id in ids:
        command = cancel_command(str(manifest.get("scheduler", context.target.scheduler)), job_id)
        if is_local_transport(context.target.transport, context.target.host):
            proc = runtime.run_direct(["bash", "-lc", command], text=True, capture_output=True, check=False)
        else:
            proc = runtime.run_direct(
                ssh_command(context.project_root, context.config, context.target_name, context.target, command),
                text=True,
                capture_output=True,
                check=False,
            )
        if proc.returncode != 0:
            raise SweepError((proc.stderr or proc.stdout or "Failed to cancel sweep job").strip())
        cancelled += 1
    for item in runs:
        if isinstance(item, dict) and item.get("job_id"):
            item["status"] = "CANCEL_REQUESTED"
    _save(context, manifest)
    return SweepCancelResult(manifest, cancelled)


def sweeps_for_analysis(context: Context) -> list[dict[str, Any]]:
    return [item for item in list_sweeps(context.project_root) if str(item.get("analysis", "")).strip("/") == context.analysis.as_posix()]


def get_sweep(context: Context, sweep_id: str) -> dict[str, Any]:
    try:
        manifest = load_sweep(context.project_root, sweep_id)
    except LookupError as exc:
        raise SweepError(str(exc)) from exc
    require_active_analysis(context, manifest)
    return manifest
