from __future__ import annotations

import json
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

from cluster_dispatch.config import CONFIG_DIR, JOBS_DIR

SYNC_DIR = "sync"
SWEEPS_DIR = "sweeps"


def _timestamp(value: object) -> datetime | None:
    if not isinstance(value, str):
        return None
    try:
        return datetime.fromisoformat(value)
    except ValueError:
        return None


def cleanup_records(project_root: Path, *, jobs: bool, sync: bool, sweeps: bool, older_than_days: int | None, keep_last: int, target: str | None, analysis: str | None, dry_run: bool) -> dict[str, Any]:
    cutoff = datetime.now() - timedelta(days=older_than_days) if older_than_days is not None else None
    summary: dict[str, Any] = {
        "dry_run": dry_run,
        "filters": {"target": target, "analysis": analysis, "older_than_days": older_than_days, "keep_last": keep_last, "jobs": jobs, "sync": sync, "sweeps": sweeps},
        "categories": {},
    }
    categories = (("jobs", JOBS_DIR, "submitted_at", jobs), ("sync", SYNC_DIR, "recorded_at", sync), ("sweeps", SWEEPS_DIR, "created_at", sweeps))
    for name, directory, timestamp_key, enabled in categories:
        if not enabled:
            continue
        scanned = 0
        entries: list[tuple[Path, datetime | None]] = []
        root = project_root / CONFIG_DIR / directory
        if root.exists():
            for path in root.glob("*.json"):
                scanned += 1
                try:
                    payload = json.loads(path.read_text())
                except json.JSONDecodeError:
                    continue
                if target and str(payload.get("target", "")) != target:
                    continue
                if analysis and str(payload.get("analysis", "")) != analysis:
                    continue
                entries.append((path, _timestamp(payload.get(timestamp_key))))
        ordered = sorted(entries, key=lambda item: (item[1] is not None, item[1] or datetime.min, item[0].name), reverse=True)
        retained = {path for path, _ in ordered[:keep_last]} if keep_last else set()
        candidates = [path for path, timestamp in ordered if path not in retained and (cutoff is None or (timestamp is not None and timestamp < cutoff))]
        if not dry_run:
            for path in candidates:
                path.unlink(missing_ok=True)
        summary["categories"][name] = {"scanned": scanned, "matched": len(entries), "deleted": [str(path) for path in candidates], "deleted_count": len(candidates)}
    summary["deleted_total"] = sum(item["deleted_count"] for item in summary["categories"].values())
    return summary
