from __future__ import annotations

import io
import json
import tarfile
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

import yaml

from cluster_dispatch.config import CONFIG_DIR, CONFIG_NAME, JOBS_DIR

SYNC_DIR = "sync"
SWEEPS_DIR = "sweeps"
IGNORE_FILE = ".cdpignore"


class ArchiveError(RuntimeError):
    pass


@dataclass(frozen=True)
class ExportResult:
    path: Path
    sections: list[str]


@dataclass(frozen=True)
class ImportResult:
    path: Path
    extracted: int
    skipped: int


def export_bundle(project_root: Path, output: Path, *, include_jobs: bool, include_sync: bool, include_sweeps: bool, redact_hosts: bool) -> ExportResult:
    bundle_path = output.resolve()
    base = project_root / CONFIG_DIR
    config_file = base / CONFIG_NAME
    manifest = {
        "format": "cluster-dispatch-export-v1",
        "created_at": datetime.now().isoformat(timespec="seconds"),
        "include_jobs": include_jobs,
        "include_sync": include_sync,
        "include_sweeps": include_sweeps,
        "redact_hosts": redact_hosts,
    }
    files = [config_file, base / "state.json", project_root / IGNORE_FILE]
    sections = ["config", "state", "ignore"]
    for enabled, directory, label in ((include_jobs, JOBS_DIR, "jobs"), (include_sync, SYNC_DIR, "sync"), (include_sweeps, SWEEPS_DIR, "sweeps")):
        if enabled:
            files.extend(sorted((base / directory).glob("*.json")))
            sections.append(label)

    with tarfile.open(bundle_path, "w:gz") as archive:
        encoded = json.dumps(manifest, indent=2).encode()
        info = tarfile.TarInfo("export_manifest.json")
        info.size = len(encoded)
        archive.addfile(info, io.BytesIO(encoded))
        if redact_hosts:
            raw = yaml.safe_load(config_file.read_text()) or {}
            for payload in raw.get("targets", {}).values():
                if isinstance(payload, dict) and "host" in payload:
                    payload["host"] = "<redacted>"
            encoded = yaml.safe_dump(raw, sort_keys=False).encode()
            info = tarfile.TarInfo(f"{CONFIG_DIR}/{CONFIG_NAME}")
            info.size = len(encoded)
            archive.addfile(info, io.BytesIO(encoded))
        for path in files:
            if redact_hosts and path == config_file:
                continue
            if path.is_file():
                archive.add(path, arcname=path.relative_to(project_root).as_posix())
    return ExportResult(bundle_path, sections)


def import_bundle(dest_root: Path, bundle: Path, *, overwrite: bool, include_jobs: bool, include_sync: bool, include_sweeps: bool) -> ImportResult:
    allowed = {f"{CONFIG_DIR}/", IGNORE_FILE, "export_manifest.json"}
    sections = {JOBS_DIR: include_jobs, SYNC_DIR: include_sync, SWEEPS_DIR: include_sweeps}
    extracted = skipped = 0
    with tarfile.open(bundle, "r:gz") as archive:
        for member in (item for item in archive.getmembers() if item.isfile()):
            name = member.name.strip("/")
            if not name or ".." in Path(name).parts or not any(name == prefix or name.startswith(prefix) for prefix in allowed):
                skipped += 1
                continue
            excluded = any(name.startswith(f"{CONFIG_DIR}/{directory}/") and not enabled for directory, enabled in sections.items())
            if excluded:
                skipped += 1
                continue
            output = (dest_root / name).resolve()
            try:
                output.relative_to(dest_root.resolve())
            except ValueError:
                skipped += 1
                continue
            if output.exists() and not overwrite:
                raise ArchiveError(f"Refusing to overwrite existing file: {output}. Re-run with --overwrite.")
            source = archive.extractfile(member)
            if source is None:
                skipped += 1
                continue
            output.parent.mkdir(parents=True, exist_ok=True)
            output.write_bytes(source.read())
            extracted += 1
    return ImportResult(bundle, extracted, skipped)
