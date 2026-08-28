from __future__ import annotations

import json
import re
import shlex
from datetime import datetime
from pathlib import Path
from typing import Any, Callable

from cluster_dispatch.config import CONFIG_DIR, ProjectConfig, TargetConfig, load_config
from cluster_dispatch.transport import is_local_transport, ssh_command

INDEX_DIR_NAME = "index"


class IndexError(ValueError):
    pass


def _index_dir(project_root: Path) -> Path:
    path = project_root / CONFIG_DIR / INDEX_DIR_NAME
    path.mkdir(parents=True, exist_ok=True)
    return path


def index_manifest_path(project_root: Path, target_name: str, analysis_rel: str, scope_rel: str) -> Path:
    safe_target = re.sub(r"[^A-Za-z0-9_.-]+", "_", target_name).strip("._-") or "target"
    safe_analysis = [re.sub(r"[^A-Za-z0-9_.-]+", "_", part).strip("._-") for part in analysis_rel.split("/") if part]
    analysis_dir = _index_dir(project_root) / safe_target
    for part in safe_analysis:
        analysis_dir = analysis_dir / part
    analysis_dir.mkdir(parents=True, exist_ok=True)
    clean_scope = scope_rel.strip("/")
    if clean_scope in {"", "."}:
        scope_stem = "root"
    else:
        scope_stem = re.sub(r"[^A-Za-z0-9_.-]+", "_", clean_scope.replace("/", "__")).strip("._-") or "scope"
    return analysis_dir / f"{scope_stem}.json"


def _index_manifest_candidate(project_root: Path, target_name: str, analysis_rel: str, scope_rel: str) -> Path:
    safe_target = re.sub(r"[^A-Za-z0-9_.-]+", "_", target_name).strip("._-") or "target"
    safe_analysis = [re.sub(r"[^A-Za-z0-9_.-]+", "_", part).strip("._-") for part in analysis_rel.split("/") if part]
    analysis_dir = project_root / CONFIG_DIR / INDEX_DIR_NAME / safe_target
    for part in safe_analysis:
        analysis_dir = analysis_dir / part
    clean_scope = scope_rel.strip("/")
    if clean_scope in {"", "."}:
        scope_stem = "root"
    else:
        scope_stem = re.sub(r"[^A-Za-z0-9_.-]+", "_", clean_scope.replace("/", "__")).strip("._-") or "scope"
    return analysis_dir / f"{scope_stem}.json"


def load_index_manifest(
    project_root: Path,
    target_name: str,
    analysis_rel: str,
    scope_rel: str,
) -> dict[str, Any] | None:
    manifest_path = _index_manifest_candidate(project_root, target_name, analysis_rel, scope_rel)
    if not manifest_path.exists():
        return None
    try:
        payload = json.loads(manifest_path.read_text())
    except json.JSONDecodeError:
        return None
    if str(payload.get("target", "")) != target_name:
        return None
    if str(payload.get("analysis", "")).strip("/") != analysis_rel.strip("/"):
        return None
    payload["_manifest_path"] = str(manifest_path)
    return payload


def list_names_from_index_manifest(manifest: dict[str, Any], include_files: bool) -> list[str]:
    entries = manifest.get("entries", [])
    if not isinstance(entries, list):
        return []
    names: set[str] = set()
    for entry in entries:
        if not isinstance(entry, dict):
            continue
        rel_path = str(entry.get("path", "")).strip("/")
        if rel_path in {"", "."}:
            continue
        top_name = rel_path.split("/", 1)[0]
        if not include_files:
            if "/" in rel_path:
                names.add(top_name)
                continue
            if str(entry.get("type", "")).lower() == "dir":
                names.add(top_name)
            continue
        names.add(top_name)
    return sorted(names)


def index_entry_path_exists(manifest: dict[str, Any], rel_path: str) -> bool:
    clean = rel_path.strip("/")
    if clean in {"", "."}:
        return True
    entries = manifest.get("entries", [])
    if not isinstance(entries, list):
        return False
    for entry in entries:
        if not isinstance(entry, dict):
            continue
        entry_path = str(entry.get("path", "")).strip("/")
        if not entry_path or entry_path == ".":
            continue
        if entry_path == clean or entry_path.startswith(f"{clean}/"):
            return True
    return False


def index_file_count_and_bytes(manifest: dict[str, Any]) -> tuple[int, int]:
    entries = manifest.get("entries", [])
    if not isinstance(entries, list):
        return 0, 0
    file_count = 0
    total_bytes = 0
    for entry in entries:
        if not isinstance(entry, dict) or str(entry.get("type", "")).lower() != "file":
            continue
        file_count += 1
        try:
            total_bytes += int(entry.get("size", 0))
        except (TypeError, ValueError):
            pass
    return file_count, total_bytes


def index_manifest_paths(project_root: Path) -> list[Path]:
    root = project_root / CONFIG_DIR / INDEX_DIR_NAME
    if not root.exists():
        return []
    return sorted(root.rglob("*.json"))


def load_all_index_manifests(project_root: Path) -> list[dict[str, Any]]:
    manifests: list[dict[str, Any]] = []
    for path in index_manifest_paths(project_root):
        try:
            payload = json.loads(path.read_text())
        except json.JSONDecodeError:
            continue
        payload["_manifest_path"] = str(path)
        manifests.append(payload)
    return manifests


def _iso_from_epoch(value: float) -> str:
    return datetime.fromtimestamp(value).isoformat(timespec="seconds")


def index_local_scope(scope_root: Path) -> list[dict[str, Any]]:
    if not scope_root.exists():
        raise IndexError(f"Remote scope not found: {scope_root}")

    entries: list[dict[str, Any]] = []
    if scope_root.is_file():
        stats = scope_root.stat()
        return [{
            "path": ".",
            "type": "file",
            "size": int(stats.st_size),
            "mtime_epoch": int(stats.st_mtime),
            "mtime": _iso_from_epoch(stats.st_mtime),
        }]

    for path in sorted(scope_root.rglob("*")):
        try:
            rel = path.relative_to(scope_root).as_posix()
        except ValueError:
            continue
        stats = path.stat()
        entries.append({
            "path": rel,
            "type": ("dir" if path.is_dir() else "file"),
            "size": int(stats.st_size),
            "mtime_epoch": int(stats.st_mtime),
            "mtime": _iso_from_epoch(stats.st_mtime),
        })
    return entries


def index_remote_scope(
    project_root: Path,
    cfg: ProjectConfig,
    target_name: str,
    target_cfg: TargetConfig,
    scope_root: str,
    run_direct: Callable[..., Any],
) -> list[dict[str, Any]]:
    remote_cmd = (
        f"P={shlex.quote(scope_root)}; "
        'if [ ! -e "$P" ]; then exit 3; fi; '
        'if [ -f "$P" ]; then '
        'stat -c ".\\tf\\t%s\\t%Y" "$P"; '
        "else "
        'find "$P" -mindepth 1 -printf "%P\\t%y\\t%s\\t%T@\\n" | sort; '
        "fi"
    )
    proc = run_direct(
        ssh_command(project_root, cfg, target_name, target_cfg, remote_cmd),
        text=True,
        capture_output=True,
        check=False,
    )
    if proc.returncode == 3:
        raise IndexError(f"Remote scope not found: {scope_root}")
    if proc.returncode != 0:
        err = (proc.stderr or proc.stdout or "unknown remote indexing error").strip()
        raise IndexError(f"Failed to index remote scope '{scope_root}': {err}")

    entries: list[dict[str, Any]] = []
    for line in proc.stdout.splitlines():
        parts = line.strip().split("\t")
        if len(parts) != 4:
            continue
        rel_path, kind, size_raw, mtime_raw = parts
        try:
            size_value = int(float(size_raw))
        except ValueError:
            size_value = 0
        try:
            epoch_value = int(float(mtime_raw))
        except ValueError:
            epoch_value = 0
        entries.append({
            "path": rel_path,
            "type": ("dir" if kind == "d" else "file"),
            "size": size_value,
            "mtime_epoch": epoch_value,
            "mtime": _iso_from_epoch(float(epoch_value)) if epoch_value > 0 else "",
        })
    return entries


def write_index_manifest_for_scope(
    project_root: Path,
    analysis_rel: str,
    target_name: str,
    target_cfg: TargetConfig,
    scope_rel: str,
    scope_remote: str,
    run_direct: Callable[..., Any],
) -> dict[str, Any]:
    if is_local_transport(target_cfg.transport, target_cfg.host):
        entries = index_local_scope(Path(scope_remote))
    else:
        entries = index_remote_scope(
            project_root,
            load_config(project_root),
            target_name,
            target_cfg,
            scope_remote,
            run_direct,
        )
    manifest = {
        "indexed_at": datetime.now().isoformat(timespec="seconds"),
        "project_root": str(project_root),
        "analysis": analysis_rel,
        "target": target_name,
        "transport": target_cfg.transport,
        "scheduler": target_cfg.scheduler,
        "scope": scope_rel,
        "remote_scope": scope_remote,
        "entry_count": len(entries),
        "entries": entries,
    }
    manifest_path = index_manifest_path(project_root, target_name, analysis_rel, scope_rel)
    manifest_path.write_text(json.dumps(manifest, indent=2))
    manifest["_manifest_path"] = str(manifest_path)
    return manifest


def summarize_index_vs_local(local_scope_root: Path, index_manifest: dict[str, Any]) -> dict[str, int]:
    local_entries = index_local_scope(local_scope_root) if local_scope_root.exists() else []
    local_map = {str(entry.get("path", "")).strip("/"): entry for entry in local_entries if isinstance(entry, dict)}
    remote_entries = index_manifest.get("entries", [])
    remote_map = {
        str(entry.get("path", "")).strip("/"): entry
        for entry in remote_entries
        if isinstance(entry, dict) and str(entry.get("path", "")).strip("/")
    } if isinstance(remote_entries, list) else {}
    remote_only = len([key for key in remote_map if key not in local_map])
    local_only = len([key for key in local_map if key not in remote_map])
    changed = 0
    for key in set(local_map).intersection(remote_map):
        local_value = local_map[key]
        remote_value = remote_map[key]
        if str(local_value.get("type", "")) != str(remote_value.get("type", "")):
            changed += 1
        elif str(local_value.get("type", "")) == "file" and int(local_value.get("size", 0) or 0) != int(
            remote_value.get("size", 0) or 0
        ):
            changed += 1
    return {"remote_only": remote_only, "local_only": local_only, "changed": changed}


def auto_index_active_root(
    project_root: Path,
    cfg: ProjectConfig,
    target_name: str,
    target_cfg: TargetConfig,
    run_direct: Callable[..., Any],
) -> str | None:
    analysis_rel = (cfg.active_analysis or "").strip("/")
    if not analysis_rel:
        return None
    remote_analysis_root = f"{target_cfg.remote_root.rstrip('/')}/{analysis_rel}"
    manifest = write_index_manifest_for_scope(
        project_root=project_root,
        analysis_rel=analysis_rel,
        target_name=target_name,
        target_cfg=target_cfg,
        scope_rel=".",
        scope_remote=remote_analysis_root,
        run_direct=run_direct,
    )
    return str(manifest.get("_manifest_path", ""))


def resolve_index_scopes(
    *,
    analysis_dir: Path,
    analysis_rel: str,
    target_cfg: TargetConfig,
    remote_analysis_root: str,
    local_path: str,
    remote_path: str | None,
    all_tags: bool,
    analysis_tags: list[str],
) -> list[dict[str, str]]:
    if all_tags:
        scopes = [tag.strip("/") for tag in analysis_tags if tag.strip("/")]
        if not scopes:
            raise IndexError("No tags found for active analysis. Tag paths with `cdp analysis tag <path>` first.")
    elif remote_path:
        normalized = remote_path.strip()
        if not normalized:
            raise IndexError("--remote-path cannot be empty")
        if normalized.startswith("/"):
            if normalized != remote_analysis_root and not normalized.startswith(f"{remote_analysis_root.rstrip('/')}/"):
                raise IndexError("--remote-path must stay inside the active analysis remote root")
            normalized = normalized.removeprefix(remote_analysis_root).strip("/")
        scopes = [normalized]
    else:
        candidate = Path(local_path).expanduser()
        if candidate.is_absolute():
            raise IndexError("Path must be relative to the active analysis directory")
        local_target = (analysis_dir / candidate).resolve()
        try:
            rel = local_target.relative_to(analysis_dir).as_posix()
        except ValueError as exc:
            raise IndexError("Path must stay inside the active analysis directory") from exc
        scopes = ["" if rel == "." else rel]

    resolved: list[dict[str, str]] = []
    for scope_rel in scopes:
        clean_scope = scope_rel.strip("/")
        local_scope = analysis_dir if not clean_scope else analysis_dir / clean_scope
        remote_scope = remote_analysis_root if not clean_scope else f"{remote_analysis_root.rstrip('/')}/{clean_scope}"
        if is_local_transport(target_cfg.transport, target_cfg.host):
            remote_scope = str(Path(remote_scope).expanduser())
        resolved.append({
            "scope": clean_scope or ".",
            "local_scope": str(local_scope),
            "remote_scope": remote_scope,
        })
    return resolved


def output_check_from_index(project_root: Path, payload: dict[str, Any]) -> str:
    analysis_name = str(payload.get("analysis", "")).strip("/")
    target_name = str(payload.get("target", "")).strip()
    if not analysis_name or not target_name:
        return "N/A"
    tags = payload.get("analysis_tags")
    if not isinstance(tags, list) or not tags:
        return "NO_TAGS"
    root_manifest = load_index_manifest(project_root, target_name, analysis_name, ".")
    if root_manifest is None:
        return "INDEX_MISSING"
    clean_tags = [str(tag).strip("/") for tag in tags if str(tag).strip("/")]
    if not clean_tags:
        return "NO_TAGS"
    present = len([tag for tag in clean_tags if index_entry_path_exists(root_manifest, tag)])
    if present == len(clean_tags):
        return "OK"
    if present == 0:
        return "MISSING"
    return f"PARTIAL {present}/{len(clean_tags)}"
