"""Tests for lineage command_impl layer (artifact-driven queries)."""

import pytest

from dbt_meta.command_impl.lineage import (
    column_lineage,
    find_artifact,
    lineage_stats,
)
from dbt_meta.lineage import LineageGraph, save_artifact


@pytest.fixture
def chain_artifact(tmp_path):
    """Three-level chain: raw → stg → core, written as a real artifact."""
    g = LineageGraph()
    g.add_node("raw_clients.id", {"data_type": "INT64", "model": "raw_clients", "column": "id"})
    g.add_node("stg_clients.client_id", {"data_type": "INT64", "model": "stg_clients", "column": "client_id"})
    g.add_node("core_clients.client_id", {"data_type": "INT64", "model": "core_clients", "column": "client_id"})
    g.add_edge("raw_clients.id", "stg_clients.client_id", {"transform": "renamed"})
    g.add_edge("stg_clients.client_id", "core_clients.client_id", {"transform": "passthrough"})
    out = tmp_path / "lineage.json"
    save_artifact(g, str(out), manifest_sha="testsha", warnings=[])
    return str(out)


class TestColumnLineageUpstream:
    def test_returns_direct_and_transitive(self, chain_artifact):
        # Bust the LRU cache between tests by passing unique paths
        result = column_lineage(chain_artifact, "core_clients.client_id", direction="upstream")
        assert result is not None
        assert result["target"]["model"] == "core_clients"
        assert result["target"]["column"] == "client_id"
        assert result["stats"]["direct_count"] == 1
        assert result["stats"]["total_count"] == 2
        direct_ids = {n["id"] for n in result["direct"]}
        all_ids = {n["id"] for n in result["all"]}
        assert direct_ids == {"stg_clients.client_id"}
        assert all_ids == {"stg_clients.client_id", "raw_clients.id"}

    def test_resolves_colon_notation(self, chain_artifact):
        result = column_lineage(chain_artifact, "core_clients:client_id", direction="upstream")
        assert result is not None
        assert result["target"]["column"] == "client_id"

    def test_missing_column_returns_none(self, chain_artifact):
        result = column_lineage(chain_artifact, "nonexistent.col", direction="upstream")
        assert result is None


class TestColumnLineageDownstream:
    def test_returns_descendants(self, chain_artifact):
        result = column_lineage(chain_artifact, "raw_clients.id", direction="downstream")
        assert result is not None
        assert result["stats"]["direct_count"] == 1
        assert result["stats"]["total_count"] == 2
        all_ids = {n["id"] for n in result["all"]}
        assert all_ids == {"stg_clients.client_id", "core_clients.client_id"}


class TestColumnLineageBadDirection:
    def test_invalid_direction_raises(self, chain_artifact):
        with pytest.raises(ValueError):
            column_lineage(chain_artifact, "core_clients.client_id", direction="sideways")


class TestLineageStats:
    def test_returns_metadata(self, chain_artifact):
        info = lineage_stats(chain_artifact)
        assert info["nodes"] == 3
        assert info["edges"] == 2
        assert info["manifest_sha"] == "testsha"
        assert "schema_version" in info
        assert "generated_at" in info


class TestFindArtifact:
    def test_explicit_path(self, chain_artifact):
        assert find_artifact(use_dev=False, explicit=chain_artifact) == chain_artifact

    def test_missing_explicit_raises(self, tmp_path):
        with pytest.raises(FileNotFoundError):
            find_artifact(use_dev=False, explicit=str(tmp_path / "no.json"))


class TestInvalidColumnRefValidation:
    """CLI-level format validation rejects refs that can't address a column.

    Regression: ``meta lineage column foo`` previously fell through to the
    graph lookup and surfaced the misleading "not found in lineage graph"
    message instead of explaining that the user needed ``model.column``.
    """

    def _runner(self):
        from typer.testing import CliRunner

        from dbt_meta.cli import app

        return CliRunner(), app

    def test_missing_separator_fails_with_format_hint(self, chain_artifact):
        runner, app = self._runner()
        result = runner.invoke(
            app, ["lineage", "column", "no_separator_here", "--artifact", chain_artifact]
        )
        assert result.exit_code == 1
        assert "Invalid column reference" in result.output

    def test_trailing_dot_fails_with_format_hint(self, chain_artifact):
        runner, app = self._runner()
        result = runner.invoke(
            app, ["lineage", "column", "core_clients.", "--artifact", chain_artifact]
        )
        assert result.exit_code == 1
        assert "Invalid column reference" in result.output

    def test_invalid_format_emits_json_error(self, chain_artifact):
        import json

        runner, app = self._runner()
        result = runner.invoke(
            app,
            ["lineage", "column", "bad_ref", "-j", "--artifact", chain_artifact],
        )
        assert result.exit_code == 1
        payload = json.loads(result.stdout)
        assert "Invalid column reference" in payload["error"]

    def test_valid_format_still_works(self, chain_artifact):
        runner, app = self._runner()
        result = runner.invoke(
            app,
            ["lineage", "column", "core_clients.client_id", "--artifact", chain_artifact],
        )
        assert result.exit_code == 0
        assert "core_clients" in result.output

    def test_downstream_also_validates_format(self, chain_artifact):
        runner, app = self._runner()
        result = runner.invoke(
            app, ["lineage", "downstream", "no_dot", "--artifact", chain_artifact]
        )
        assert result.exit_code == 1
        assert "Invalid column reference" in result.output
