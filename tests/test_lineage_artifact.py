"""Tests for the lineage JSON artifact (save/load + finder)."""

import os
from pathlib import Path

import orjson
import pytest

from dbt_meta.lineage import LineageGraph, find_lineage_artifact, load_artifact, save_artifact
from dbt_meta.lineage.artifact import SCHEMA_VERSION, get_artifact_age_hours


@pytest.fixture
def sample_graph():
    g = LineageGraph()
    g.add_edge("raw.id", "stg.client_id", {"transform": "renamed"})
    g.add_edge("stg.client_id", "core.client_id", {"transform": "passthrough"})
    return g


class TestSaveLoadArtifact:
    def test_save_creates_file(self, tmp_path, sample_graph):
        out = tmp_path / "lineage.json"
        path = save_artifact(sample_graph, str(out), manifest_sha="abc123")
        assert os.path.exists(path)

        payload = orjson.loads(out.read_bytes())
        assert payload["schema_version"] == SCHEMA_VERSION
        assert payload["manifest_sha"] == "abc123"
        assert payload["stats"]["nodes"] == 3
        assert payload["stats"]["edges"] == 2
        assert "graph" in payload

    def test_save_creates_parent_dirs(self, tmp_path, sample_graph):
        out = tmp_path / "deeply" / "nested" / "lineage.json"
        save_artifact(sample_graph, str(out))
        assert out.exists()

    def test_load_returns_graph_and_metadata(self, tmp_path, sample_graph):
        out = tmp_path / "lineage.json"
        save_artifact(sample_graph, str(out), manifest_sha="x")

        graph, metadata = load_artifact(str(out))
        assert graph.node_count == 3
        assert graph.edge_count == 2
        assert metadata["schema_version"] == SCHEMA_VERSION
        assert metadata["manifest_sha"] == "x"
        assert "generated_at" in metadata

    def test_load_missing_raises(self, tmp_path):
        with pytest.raises(FileNotFoundError):
            load_artifact(str(tmp_path / "nope.json"))

    def test_load_unsupported_schema_raises(self, tmp_path):
        bad = tmp_path / "lineage.json"
        bad.write_bytes(orjson.dumps({
            "schema_version": "999.0",
            "graph": {"nodes": [], "edges": []},
        }))
        with pytest.raises(ValueError, match="schema version"):
            load_artifact(str(bad))

    def test_warnings_persist_through_roundtrip(self, tmp_path, sample_graph):
        out = tmp_path / "lineage.json"
        save_artifact(sample_graph, str(out), warnings=["model x: parse error"])
        _, metadata = load_artifact(str(out))
        assert metadata["warnings"] == ["model x: parse error"]


class TestArtifactAge:
    def test_returns_none_for_missing(self, tmp_path):
        assert get_artifact_age_hours(str(tmp_path / "missing.json")) is None

    def test_returns_age_for_existing(self, tmp_path, sample_graph):
        out = tmp_path / "lineage.json"
        save_artifact(sample_graph, str(out))
        age = get_artifact_age_hours(str(out))
        assert age is not None
        assert age >= 0


class TestFinder:
    def test_explicit_path_takes_priority(self, tmp_path, sample_graph):
        out = tmp_path / "custom.json"
        save_artifact(sample_graph, str(out))
        found = find_lineage_artifact(explicit_path=str(out))
        assert found == str(out.absolute())

    def test_explicit_path_missing_raises(self, tmp_path):
        with pytest.raises(FileNotFoundError):
            find_lineage_artifact(explicit_path=str(tmp_path / "nope.json"))

    def test_dev_uses_env_var(self, tmp_path, sample_graph, monkeypatch):
        out = tmp_path / "target" / "lineage.json"
        save_artifact(sample_graph, str(out))
        monkeypatch.setenv("DBT_DEV_LINEAGE_PATH", str(out))
        found = find_lineage_artifact(use_dev=True)
        assert found == str(out.absolute())

    def test_prod_uses_env_var(self, tmp_path, sample_graph, monkeypatch):
        out = tmp_path / "lineage.json"
        save_artifact(sample_graph, str(out))
        monkeypatch.setenv("DBT_PROD_LINEAGE_PATH", str(out))
        # Clear any conflicting state
        monkeypatch.chdir(tmp_path)
        found = find_lineage_artifact(use_dev=False)
        assert found == str(out.absolute())

    def test_no_artifact_raises(self, tmp_path, monkeypatch):
        monkeypatch.delenv("DBT_PROD_LINEAGE_PATH", raising=False)
        monkeypatch.delenv("DBT_DEV_LINEAGE_PATH", raising=False)
        monkeypatch.chdir(tmp_path)
        # Force the home-dir fallback to point to an empty tmp dir
        monkeypatch.setattr(Path, "home", lambda: tmp_path)
        with pytest.raises(FileNotFoundError):
            find_lineage_artifact()
