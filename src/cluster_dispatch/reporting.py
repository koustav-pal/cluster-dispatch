from __future__ import annotations

import re
import shlex
import subprocess
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Callable

from cluster_dispatch.config import ProjectConfig, TargetConfig, load_config
from cluster_dispatch.index import load_all_index_manifests
from cluster_dispatch.records import list_jobs, list_syncs
from cluster_dispatch.transport import is_local_transport, ssh_command


class ReportingError(RuntimeError):
    pass


@dataclass(frozen=True)
class StatsResult:
    record: dict[str, Any]
    scheduler: str
    values: dict[str, str]
    match_count: int


def _parse_iso(value: object) -> datetime | None:
    if not isinstance(value, str):
        return None
    try:
        return datetime.fromisoformat(value.strip())
    except ValueError:
        return None


def humanize_bytes(num_bytes: float) -> str:
    units = ["B", "KiB", "MiB", "GiB", "TiB", "PiB"]
    value = float(num_bytes)
    unit = 0
    while value >= 1024 and unit < len(units) - 1:
        value /= 1024.0
        unit += 1
    return f"{value:.2f} {units[unit]}"


def build_report(
    project_root: Path,
    *,
    target: str | None = None,
    analysis: str | None = None,
    days: int | None = None,
    now: datetime | None = None,
) -> dict[str, Any]:
    current = now or datetime.now()
    cutoff = current - timedelta(days=days) if days is not None else None

    def matches(record: dict[str, Any], timestamp_key: str) -> bool:
        if target and str(record.get("target", "")) != target:
            return False
        if analysis and str(record.get("analysis", "")).strip("/") != analysis.strip("/"):
            return False
        timestamp = _parse_iso(record.get(timestamp_key))
        return cutoff is None or (timestamp is not None and timestamp >= cutoff)

    jobs = [record for record in list_jobs(project_root) if matches(record, "submitted_at")]
    syncs = [record for record in list_syncs(project_root) if matches(record, "recorded_at")]
    indexes = [record for record in load_all_index_manifests(project_root) if matches(record, "indexed_at")]

    def counts(key: str, default: str) -> dict[str, int]:
        result: dict[str, int] = {}
        for record in jobs:
            value = str(record.get(key, default)) or default
            result[value] = result.get(value, 0) + 1
        return result

    failures = [
        {key: record.get(key) for key in ("submitted_at", "job_id", "job_name", "target", "analysis", "state")}
        for record in jobs
        if "FAIL" in str(record.get("state", "")).upper() or str(record.get("state", "")).upper() == "EXIT"
    ][:10]
    sync_by_action: dict[str, int] = {}
    for event in syncs:
        action = str(event.get("action", "unknown")) or "unknown"
        sync_by_action[action] = sync_by_action.get(action, 0) + 1

    cpu_values: list[int] = []
    memories: dict[str, int] = {}
    walltimes: dict[str, int] = {}
    for record in jobs:
        resources = record.get("resources") if isinstance(record.get("resources"), dict) else {}
        cpu = resources.get("cpus", record.get("cpus"))
        memory = resources.get("memory", record.get("memory"))
        walltime = resources.get("time", record.get("time"))
        try:
            if cpu is not None:
                cpu_values.append(int(cpu))
        except (TypeError, ValueError):
            pass
        if memory:
            memories[str(memory)] = memories.get(str(memory), 0) + 1
        if walltime:
            walltimes[str(walltime)] = walltimes.get(str(walltime), 0) + 1

    index_entries = index_files = index_bytes = stale = 0
    index_by_scope: dict[str, int] = {}
    latest: datetime | None = None
    stale_cutoff = current - timedelta(hours=24)
    for manifest in indexes:
        scope = str(manifest.get("scope", "."))
        index_by_scope[scope] = index_by_scope.get(scope, 0) + 1
        entries = manifest.get("entries", [])
        if isinstance(entries, list):
            index_entries += len(entries)
            for entry in entries:
                if isinstance(entry, dict) and str(entry.get("type", "")).lower() == "file":
                    index_files += 1
                    try:
                        index_bytes += int(entry.get("size", 0))
                    except (TypeError, ValueError):
                        pass
        timestamp = _parse_iso(manifest.get("indexed_at"))
        if timestamp is not None:
            latest = timestamp if latest is None or timestamp > latest else latest
            stale += timestamp < stale_cutoff

    top_memory = sorted(memories.items(), key=lambda item: (-item[1], item[0]))[:5]
    top_walltime = sorted(walltimes.items(), key=lambda item: (-item[1], item[0]))[:5]
    return {
        "filters": {"target": target, "analysis": analysis, "days": days},
        "jobs_total": len(jobs),
        "sync_total": len(syncs),
        "index_total": len(indexes),
        "jobs_by_state": counts("state", "UNKNOWN"),
        "jobs_by_scheduler": counts("scheduler", "UNKNOWN"),
        "jobs_by_target": counts("target", "UNKNOWN"),
        "recent_failures": failures,
        "sync_by_action": sync_by_action,
        "index": {
            "manifests": len(indexes),
            "latest_indexed_at": latest.isoformat(timespec="seconds") if latest else None,
            "stale_manifests_24h": stale,
            "entries_total": index_entries,
            "files_total": index_files,
            "bytes_total": index_bytes,
            "bytes_human": humanize_bytes(index_bytes),
            "by_scope": index_by_scope,
        },
        "resources": {
            "avg_cpus": round(sum(cpu_values) / len(cpu_values), 2) if cpu_values else None,
            "top_memory": [{"value": key, "count": value} for key, value in top_memory],
            "top_walltime": [{"value": key, "count": value} for key, value in top_walltime],
        },
    }


def _run_accounting(
    project_root: Path,
    cfg: ProjectConfig,
    target_name: str,
    target_cfg: TargetConfig,
    command: str,
    run: Callable[..., subprocess.CompletedProcess[str]],
) -> subprocess.CompletedProcess[str]:
    args = ["bash", "-lc", command] if is_local_transport(target_cfg.transport, target_cfg.host) else ssh_command(project_root, cfg, target_name, target_cfg, "bash", "-lc", command)
    return run(args, text=True, capture_output=True, check=False)


def _fetch_stats(project_root: Path, cfg: ProjectConfig, target_name: str, scheduler: str, job_id: str, run: Callable[..., subprocess.CompletedProcess[str]]) -> dict[str, str]:
    target_cfg = cfg.targets[target_name]
    quoted = shlex.quote(job_id)
    scheduler = scheduler.lower()
    commands = {
        "sge": f"qacct -j {quoted}", "univa": f"qacct -j {quoted}",
        "pbs": f"qstat -xf {quoted}",
        "slurm": f"sacct -j {quoted} -X -P -n -o JobIDRaw,State,ExitCode,Elapsed,TotalCPU,AllocCPUS,MaxRSS,MaxVMSize,AveRSS,ReqMem",
        "lsf": f"bjobs -a -noheader -o 'STAT EXIT_CODE CPU_USED RUN_TIME MAX_MEM MEM SWAP' {quoted}",
        "none": f"ps -p {quoted} -o pid=,etime=,%cpu=,%mem=,rss=,vsz=",
    }
    if scheduler not in commands:
        raise ReportingError(f"Unsupported scheduler for stats: {scheduler}")
    proc = _run_accounting(project_root, cfg, target_name, target_cfg, commands[scheduler], run)
    if scheduler == "none" and proc.returncode != 0:
        return {"state": "EXITED"}
    if proc.returncode != 0:
        raise ReportingError(proc.stderr.strip() or f"Failed to query {scheduler} accounting")
    if scheduler in {"sge", "univa"}:
        raw = {parts[0]: parts[-1] for line in proc.stdout.splitlines() if len(parts := line.strip().split()) >= 2 and not line.strip().startswith("=")}
        values = {dst: raw[src] for src, dst in {"exit_status": "exit_status", "failed": "failed", "cpu": "cpu_time", "ru_wallclock": "wallclock", "maxvmem": "max_vmem", "mem": "mem", "io": "io"}.items() if src in raw}
        values["state"] = "FAILED" if values.get("failed", "0") != "0" or values.get("exit_status", "0") != "0" else "COMPLETED"
        return values
    if scheduler == "pbs":
        values = {}
        for line in proc.stdout.splitlines():
            if "=" not in line:
                continue
            key, value = (part.strip() for part in line.split("=", 1))
            if key == "job_state": values["state"] = value
            elif key == "Exit_status": values["exit_status"] = value
            elif key.startswith("resources_used."): values[key.removeprefix("resources_used.")] = value
        return values
    line = next((line.strip() for line in proc.stdout.splitlines() if line.strip()), "")
    if scheduler == "none" and not line:
        return {"state": "EXITED"}
    if not line:
        raise ReportingError(f"No {scheduler.upper()} accounting record returned for this job")
    if scheduler == "slurm":
        keys = ["job_id_raw", "state", "exit_code", "elapsed", "total_cpu", "alloc_cpus", "max_rss", "max_vmem", "avg_rss", "req_mem"]
        parts = line.split("|")
    elif scheduler == "lsf":
        keys = ["state", "exit_code", "cpu_used", "run_time", "max_mem", "mem", "swap"]
        parts = line.split()
    else:
        keys = ["pid", "elapsed", "cpu_percent", "mem_percent", "rss_kb", "vsz_kb"]
        parts = line.split()
        keys.insert(0, "state")
        parts.insert(0, "RUNNING")
    return dict(zip(keys, parts))


def _duration_seconds(raw: str) -> int | None:
    if raw.isdigit(): return int(raw)
    day, core = (raw.split("-", 1) if "-" in raw and raw.split("-", 1)[0].isdigit() else ("0", raw))
    chunks = core.split(":")
    if len(chunks) == 2: chunks.insert(0, "0")
    return int(day) * 86400 + int(chunks[0]) * 3600 + int(chunks[1]) * 60 + int(chunks[2]) if len(chunks) == 3 and all(x.isdigit() for x in chunks) else None


def _memory_bytes(raw: str) -> float | None:
    value = raw.lower().strip()
    if len(value) >= 2 and value[-1] in {"n", "c"} and value[-2].isalpha(): value = value[:-1]
    match = re.match(r"^([0-9]*\.?[0-9]+)\s*([a-z]+)?$", value)
    units = {"b": 1, "k": 1024, "kb": 1024, "kib": 1024, "m": 1024**2, "mb": 1024**2, "mib": 1024**2, "g": 1024**3, "gb": 1024**3, "gib": 1024**3, "t": 1024**4, "tb": 1024**4, "tib": 1024**4, "p": 1024**5, "pb": 1024**5, "pib": 1024**5}
    if not match or (unit := match.group(2) or "b") not in units: return None
    return float(match.group(1)) * units[unit]


def normalize_stats(values: dict[str, str]) -> dict[str, str]:
    result = {key: str(value) for key, value in values.items()}
    for key, value in list(result.items()):
        if key in {"elapsed", "total_cpu", "cpu_time", "wallclock", "run_time", "cput", "walltime"} and (seconds := _duration_seconds(value)) is not None:
            days, rem = divmod(max(0, seconds), 86400); hours, rem = divmod(rem, 3600); minutes, seconds = divmod(rem, 60)
            result[key] = f"{days}d {hours:02}:{minutes:02}:{seconds:02}" if days else f"{hours:02}:{minutes:02}:{seconds:02}"
        if key in {"rss_kb", "vsz_kb"} and value.isdigit(): result[key] = humanize_bytes(float(value) * 1024)
        if key in {"max_vmem", "mem", "max_rss", "avg_rss", "req_mem", "vmem", "max_mem", "swap"} and (size := _memory_bytes(value)) is not None: result[key] = humanize_bytes(size)
    return result


def collect_stats(project_root: Path, *, job_id: str | None, job_name: str | None, target: str | None, analysis: str | None, run: Callable[..., subprocess.CompletedProcess[str]] = subprocess.run) -> StatsResult:
    if not job_id and not job_name: raise ReportingError("Provide at least one selector: --job-id or --job-name")
    records = [record for record in list_jobs(project_root) if (not job_id or str(record.get("job_id", "")) == job_id) and (not job_name or str(record.get("job_name", "")) == job_name) and (not target or str(record.get("target", "")) == target) and (not analysis or str(record.get("analysis", "")) == analysis)]
    if not records: raise ReportingError("No matching job records found for the provided filters")
    record = records[0]; cfg = load_config(project_root); target_name = str(record.get("target", ""))
    if target_name not in cfg.targets: raise ReportingError(f"Target '{target_name}' from selected record is not configured")
    selected_id = str(record.get("job_id", "")).strip()
    if not selected_id: raise ReportingError("Selected record has no job_id")
    scheduler = str(record.get("scheduler", "")).strip() or cfg.targets[target_name].scheduler
    return StatsResult(record, scheduler, normalize_stats(_fetch_stats(project_root, cfg, target_name, scheduler, selected_id, run)), len(records))
