from __future__ import annotations

import json
import sys
import tempfile
from pathlib import Path
from unittest import TestCase

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = PROJECT_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from cluster_dispatch.analysis import AnalysisError, add_analysis_tag, list_analysis_entries
from cluster_dispatch.config import ProjectConfig, TargetConfig, save_config
from cluster_dispatch.index import index_manifest_path


class TestAnalysisServices(TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory(prefix="cdp-analysis-services-")
        self.root = Path(self.tmp.name)
        self.analysis = self.root / "analysis/a"
        (self.analysis / "results").mkdir(parents=True)
        self.cfg = ProjectConfig(
            scheduler="none",
            default_target="local",
            active_analysis="analysis/a",
            targets={"local": TargetConfig("localhost", "none", str(self.root), transport="local")},
        )
        save_config(self.root, self.cfg)

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def test_local_listing(self) -> None:
        result = list_analysis_entries(self.root)
        self.assertEqual(result.entries, ["results"])
        self.assertFalse(result.remote)

    def test_path_traversal_rejected(self) -> None:
        with self.assertRaisesRegex(AnalysisError, "inside"):
            list_analysis_entries(self.root, "../../outside")

    def test_index_backed_remote_listing(self) -> None:
        path = index_manifest_path(self.root, "local", "analysis/a", ".")
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps({"target": "local", "analysis": "analysis/a", "entries": [{"path": "indexed", "type": "dir"}]}))
        result = list_analysis_entries(self.root, remote=True)
        self.assertTrue(result.from_index)
        self.assertEqual(result.entries, ["indexed"])

    def test_live_remote_fallback_for_local_transport(self) -> None:
        result = list_analysis_entries(self.root, remote=True)
        self.assertFalse(result.from_index)
        self.assertIn("analysis", result.path)

    def test_remote_tag_validates_via_index(self) -> None:
        path = index_manifest_path(self.root, "local", "analysis/a", ".")
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps({"target": "local", "analysis": "analysis/a", "entries": [{"path": "results", "type": "dir"}]}))
        tag, source, added = add_analysis_tag(self.root, Path("results"), remote=True)
        self.assertEqual((tag, source, added), ("results", "index", True))

    def test_remote_tag_live_failure(self) -> None:
        with self.assertRaisesRegex(AnalysisError, "not found remotely"):
            add_analysis_tag(self.root, Path("missing"), remote=True)
