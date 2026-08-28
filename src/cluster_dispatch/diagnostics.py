from __future__ import annotations

import json
import shlex
import shutil
import subprocess
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Callable

from cluster_dispatch.config import CONFIG_DIR, config_path, ensure_state_dirs, load_config
from cluster_dispatch.index import INDEX_DIR_NAME, index_manifest_paths
from cluster_dispatch.transport import is_local_transport, ssh_command

REQUIRED_TEMPLATE_VARS = ("cpus", "memory", "time", "job_name", "stdout", "stderr", "working_dir")
SCHEDULER_COMMANDS = {
    "sge": ["qsub", "qstat", "qacct"],
    "univa": ["qsub", "qstat", "qacct"],
    "pbs": ["qsub", "qstat"],
    "slurm": ["sbatch", "squeue", "sacct"],
    "lsf": ["bsub", "bjobs", "bkill"],
    "none": ["bash"],
}


class DiagnosticError(RuntimeError):
    pass


@dataclass(frozen=True)
class DiagnosticCheck:
    status: str
    name: str
    detail: str


@dataclass(frozen=True)
class DiagnosticResult:
    checks: list[DiagnosticCheck]

    @property
    def pass_count(self) -> int:
        return sum(check.status == "PASS" for check in self.checks)

    @property
    def warn_count(self) -> int:
        return sum(check.status == "WARN" for check in self.checks)

    @property
    def fail_count(self) -> int:
        return sum(check.status == "FAIL" for check in self.checks)

    @property
    def failed(self) -> bool:
        return self.fail_count > 0


def _validate_template(template: str) -> None:
    missing = [name for name in REQUIRED_TEMPLATE_VARS if f"{{{name}}}" not in template]
    if missing:
        raise DiagnosticError(f"Template missing required placeholders: {', '.join(missing)}")


def _parse_timestamp(value: object) -> datetime | None:
    if not isinstance(value, str):
        return None
    try:
        return datetime.fromisoformat(value.strip())
    except ValueError:
        return None


def diagnose_project(
    project_root: Path,
    *,
    target: str | None = None,
    remote: bool = True,
    run: Callable[..., subprocess.CompletedProcess[str]] = subprocess.run,
) -> DiagnosticResult:
    checks: list[DiagnosticCheck] = []

    def add(status: str, name: str, detail: str) -> None:
        checks.append(DiagnosticCheck(status, name, detail))

    try:
        cfg = load_config(project_root)
        add("PASS", "config", f"Loaded {config_path(project_root)}")
    except Exception as exc:
        add("FAIL", "config", str(exc))
        return DiagnosticResult(checks)

    ensure_state_dirs(project_root)
    add("PASS", "state_dirs", f"Found/created {project_root / CONFIG_DIR}")
    if cfg.default_target in cfg.targets:
        add("PASS", "default_target", cfg.default_target)
    else:
        add("FAIL", "default_target", f"Default target '{cfg.default_target}' not configured")

    template_path = project_root / CONFIG_DIR / "templates" / "scheduler_header.tmpl"
    if template_path.exists():
        try:
            _validate_template(template_path.read_text())
            add("PASS", "template", f"Template placeholders valid: {template_path}")
        except Exception as exc:
            add("FAIL", "template", f"{template_path}: {exc}")
    else:
        add("WARN", "template", f"Template file not found at {template_path}")

    if target and target not in cfg.targets:
        add("FAIL", "target", f"Target '{target}' not found")
        return DiagnosticResult(checks)
    selected = {target: cfg.targets[target]} if target else dict(cfg.targets)
    needs_ssh = remote and any(not is_local_transport(item.transport, item.host) for item in selected.values())
    for binary in ["rsync", *( ["ssh"] if needs_ssh else [])]:
        add("PASS" if shutil.which(binary) else "FAIL", "local_binary", f"{binary} {'found' if shutil.which(binary) else 'not found in PATH'}")

    if cfg.active_analysis:
        active_path = (project_root / cfg.active_analysis).resolve()
        add("PASS" if active_path.is_dir() else "WARN", "active_analysis", str(active_path) if active_path.is_dir() else f"Configured active analysis missing: {active_path}")
    else:
        add("WARN", "active_analysis", "No active analysis set")

    manifests = index_manifest_paths(project_root)
    corrupt = 0
    newest: datetime | None = None
    for path in manifests:
        try:
            timestamp = _parse_timestamp(json.loads(path.read_text()).get("indexed_at"))
        except json.JSONDecodeError:
            corrupt += 1
            continue
        if timestamp is not None and (newest is None or timestamp > newest):
            newest = timestamp
    add("PASS" if manifests else "WARN", "index", f"Found {len(manifests)} manifest(s)" if manifests else f"No index manifests found under {project_root / CONFIG_DIR / INDEX_DIR_NAME}")
    if corrupt:
        add("FAIL", "index_corrupt", f"{corrupt} manifest(s) could not be parsed")
    if newest is not None:
        age = (datetime.now() - newest).total_seconds() / 3600.0
        add("WARN" if age > 24 else "PASS", "index_freshness", f"Latest index is stale ({age:.1f}h old)" if age > 24 else f"Latest index age {age:.1f}h")

    for name, target_cfg in selected.items():
        scheduler_ok = target_cfg.scheduler in SCHEDULER_COMMANDS
        add("PASS" if scheduler_ok else "FAIL", f"target:{name}:scheduler", target_cfg.scheduler if scheduler_ok else f"Unsupported scheduler '{target_cfg.scheduler}'")
        root_ok = bool(target_cfg.remote_root) and Path(target_cfg.remote_root).is_absolute()
        root_detail = target_cfg.remote_root if root_ok else ("Missing remote_root" if not target_cfg.remote_root else f"Not absolute: {target_cfg.remote_root}")
        add("PASS" if root_ok else "FAIL", f"target:{name}:remote_root", root_detail)
        try:
            _validate_template(target_cfg.template_header)
            add("PASS", f"target:{name}:template", "Template placeholders valid")
        except Exception as exc:
            add("FAIL", f"target:{name}:template", str(exc))
        if not remote:
            continue

        required = SCHEDULER_COMMANDS.get(target_cfg.scheduler, [])
        if is_local_transport(target_cfg.transport, target_cfg.host):
            add("PASS", f"target:{name}:ssh", "Skipped (local target mode)")
            exists = Path(target_cfg.remote_root).is_dir()
            add("PASS" if exists else "WARN", f"target:{name}:remote_root_exists", target_cfg.remote_root if exists else f"Local root missing/inaccessible: {target_cfg.remote_root}")
            missing = [command for command in required if shutil.which(command) is None]
            if required:
                add("WARN" if missing else "PASS", f"target:{name}:scheduler_cmds", f"Missing one or more: {', '.join(required)}" if missing else ", ".join(required))
            continue

        probe = run(ssh_command(project_root, cfg, name, target_cfg, "echo", "cdp-ok", connect_timeout=10), text=True, capture_output=True, check=False)
        if probe.returncode != 0 or "cdp-ok" not in probe.stdout:
            add("FAIL", f"target:{name}:ssh", probe.stderr.strip() or probe.stdout.strip() or "SSH failed")
            continue
        add("PASS", f"target:{name}:ssh", f"Connected to {target_cfg.host}")
        root_probe = run(ssh_command(project_root, cfg, name, target_cfg, "bash", "-lc", f"if [ -d {shlex.quote(target_cfg.remote_root)} ]; then echo OK; else echo MISSING; fi"), text=True, capture_output=True, check=False)
        root_exists = root_probe.returncode == 0 and root_probe.stdout.strip() == "OK"
        add("PASS" if root_exists else "WARN", f"target:{name}:remote_root_exists", target_cfg.remote_root if root_exists else f"Remote root missing/inaccessible: {target_cfg.remote_root}")
        if required:
            command = " && ".join(f"command -v {item} >/dev/null 2>&1" for item in required)
            cmd_probe = run(ssh_command(project_root, cfg, name, target_cfg, "bash", "-lc", command), text=True, capture_output=True, check=False)
            add("PASS" if cmd_probe.returncode == 0 else "WARN", f"target:{name}:scheduler_cmds", ", ".join(required) if cmd_probe.returncode == 0 else f"Missing one or more: {', '.join(required)}")

    return DiagnosticResult(checks)
