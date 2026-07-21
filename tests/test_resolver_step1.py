"""Adversarial tests for the step-1 resolver refactor.

Covers the changed branches:
- utils.dev.apply_dev_schema          — copy semantics + schema override
- BaseCommand._fallback_level         — set in dev / prod paths
- BaseCommand._get_model_dev_mode     — DEV vs BIGQUERY level, denied paths
- BaseCommand._get_model_prod_mode    — manifest-source selection (invariant #2)
"""

import json
import warnings
from unittest.mock import MagicMock, patch

import pytest

from dbt_meta.command_impl.base import BaseCommand
from dbt_meta.config import Config
from dbt_meta.fallback import FallbackLevel
from dbt_meta.utils.dev import apply_dev_schema


def _cfg() -> Config:
    with warnings.catch_warnings():
        warnings.filterwarnings("ignore", category=DeprecationWarning)
        return Config.from_env()


class _StubCommand(BaseCommand):
    """Minimal concrete command for exercising BaseCommand branches directly."""

    SUPPORTS_BIGQUERY = True
    SUPPORTS_DEV = True
    _bq_result = None

    def execute(self):  # pragma: no cover - not used
        return self.get_model_with_fallback()

    def process_model(self, model, level=None):  # pragma: no cover - not used
        return model

    def _get_model_bigquery_dev(self):
        return self._bq_result


# --------------------------------------------------------------------------
# apply_dev_schema
# --------------------------------------------------------------------------


class TestApplyDevSchema:
    def test_overrides_schema_to_dev_dataset(self, monkeypatch):
        monkeypatch.setenv("DBT_DEV_SCHEMA", "personal_alice")
        result = apply_dev_schema({"name": "m", "schema": "core"})
        assert result["schema"] == "personal_alice"

    def test_does_not_mutate_original(self, monkeypatch):
        monkeypatch.setenv("DBT_DEV_SCHEMA", "personal_alice")
        original = {"name": "m", "schema": "core", "config": {"alias": "a"}}
        result = apply_dev_schema(original)
        assert result is not original
        assert original["schema"] == "core"  # untouched
        assert result["name"] == "m"  # other fields preserved

    def test_adds_schema_when_absent(self, monkeypatch):
        monkeypatch.setenv("DBT_DEV_SCHEMA", "personal_bob")
        result = apply_dev_schema({"name": "m"})  # no 'schema' key
        assert result["schema"] == "personal_bob"


# --------------------------------------------------------------------------
# _get_model_dev_mode — level tracking + denied paths
# --------------------------------------------------------------------------


class TestDevModeLevel:
    def _cmd(self, monkeypatch):
        monkeypatch.setenv("DBT_DEV_SCHEMA", "personal_dev")
        monkeypatch.setenv("DBT_FALLBACK_BIGQUERY", "true")
        cmd = _StubCommand(_cfg(), "/nonexistent/manifest.json", "m", use_dev=True)
        return cmd

    def test_found_in_dev_sets_dev_level_and_overrides_schema(self, monkeypatch):
        cmd = self._cmd(monkeypatch)
        parser = MagicMock()
        parser.get_model.return_value = {"name": "m", "schema": "core_prod"}
        with (
            patch(
                "dbt_meta.command_impl.base._find_dev_manifest", return_value="/dev/manifest.json"
            ),
            patch("dbt_meta.command_impl.base._get_cached_parser", return_value=parser),
        ):
            result = cmd._get_model_dev_mode()
        assert cmd._fallback_level == FallbackLevel.DEV_MANIFEST
        assert result["schema"] == "personal_dev"  # dev override applied

    def test_bigquery_fallback_sets_bigquery_level(self, monkeypatch):
        cmd = self._cmd(monkeypatch)
        cmd._bq_result = {"name": "m", "schema": "personal_dev"}
        with patch("dbt_meta.command_impl.base._find_dev_manifest", return_value=None):
            result = cmd._get_model_dev_mode()
        assert cmd._fallback_level == FallbackLevel.BIGQUERY
        assert result == {"name": "m", "schema": "personal_dev"}

    def test_bigquery_returns_none_keeps_level_none(self, monkeypatch):
        cmd = self._cmd(monkeypatch)
        cmd._bq_result = None
        with patch("dbt_meta.command_impl.base._find_dev_manifest", return_value=None):
            result = cmd._get_model_dev_mode()
        assert result is None
        assert cmd._fallback_level is None

    def test_bigquery_unsupported_returns_none(self, monkeypatch):
        monkeypatch.setenv("DBT_DEV_SCHEMA", "personal_dev")
        cmd = _StubCommand(_cfg(), "/nonexistent/manifest.json", "m", use_dev=True)
        cmd.SUPPORTS_BIGQUERY = False
        with patch("dbt_meta.command_impl.base._find_dev_manifest", return_value=None):
            result = cmd._get_model_dev_mode()
        assert result is None
        assert cmd._fallback_level is None


# --------------------------------------------------------------------------
# _get_model_prod_mode — manifest-source selection (invariant #2)
# --------------------------------------------------------------------------


def _write_manifest(path, alias):
    path.write_text(
        json.dumps(
            {
                "nodes": {
                    "model.p.test__events": {
                        "name": "test__events",
                        "schema": "core",
                        "database": "proj",
                        "config": {"alias": alias},
                    }
                }
            }
        )
    )


class TestProdModeManifestSource:
    def test_use_dev_false_uses_explicit_manifest_path(self, tmp_path, monkeypatch):
        # conftest disables fallbacks → only PROD_MANIFEST level active.
        explicit = tmp_path / "explicit.json"
        _write_manifest(explicit, "events_explicit")
        # config.prod_manifest_path points elsewhere (must be ignored).
        monkeypatch.setenv("DBT_PROD_MANIFEST_PATH", str(tmp_path / "other.json"))
        cmd = _StubCommand(_cfg(), str(explicit), "test__events", use_dev=False)

        result = cmd._get_model_prod_mode()

        assert result is not None
        assert result["config"]["alias"] == "events_explicit"
        assert cmd._fallback_level == FallbackLevel.PROD_MANIFEST

    def test_use_dev_true_uses_config_prod_manifest_path(self, tmp_path, monkeypatch):
        # SUPPORTS_DEV=False + --dev edge: self.manifest_path may be the dev
        # manifest, so prod lookup must fall back to config.prod_manifest_path.
        config_manifest = tmp_path / "config_prod.json"
        _write_manifest(config_manifest, "events_config")
        monkeypatch.setenv("DBT_PROD_MANIFEST_PATH", str(config_manifest))
        cmd = _StubCommand(_cfg(), str(tmp_path / "dev.json"), "test__events", use_dev=True)

        result = cmd._get_model_prod_mode()

        assert result is not None
        assert result["config"]["alias"] == "events_config"
        assert cmd._fallback_level == FallbackLevel.PROD_MANIFEST

    def test_missing_manifest_returns_none(self, tmp_path, monkeypatch):
        monkeypatch.setenv("DBT_PROD_MANIFEST_PATH", str(tmp_path / "missing.json"))
        cmd = _StubCommand(_cfg(), str(tmp_path / "missing.json"), "test__events", use_dev=False)
        assert cmd._get_model_prod_mode() is None


# --------------------------------------------------------------------------
# schema command — relies on base-populated _fallback_level (no local override)
# --------------------------------------------------------------------------


class TestSchemaUsesBaseLevel:
    def test_prod_hit_returns_prod_location(self, tmp_path, monkeypatch):
        from dbt_meta.command_impl.schema import SchemaCommand

        manifest = tmp_path / "manifest.json"
        _write_manifest(manifest, "events_prod")
        monkeypatch.setenv("DBT_PROD_MANIFEST_PATH", str(manifest))
        result = SchemaCommand(_cfg(), str(manifest), "test__events", False, False).execute()

        assert result["schema"] == "core"
        assert result["table"] == "events_prod"
        assert result["full_name"] == "proj.core.events_prod"

    @pytest.mark.usefixtures("enable_fallbacks")
    def test_dev_manifest_fallback_returns_dev_schema(self, tmp_path, monkeypatch):
        """Found via prod→dev fallback (use_dev=False) → dev schema location.

        Exercises schema.py's DEV_MANIFEST branch reading the base-populated
        _fallback_level.
        """
        from dbt_meta.command_impl.schema import SchemaCommand

        # prod manifest without the model; dev manifest (target/) with it.
        project = tmp_path / "proj"
        (project / "target").mkdir(parents=True)
        prod = project / ".dbt-state" / "manifest.json"
        prod.parent.mkdir(parents=True)
        prod.write_text(json.dumps({"nodes": {}}))
        dev = project / "target" / "manifest.json"
        _write_manifest(dev, "events_dev")

        monkeypatch.setenv("DBT_PROD_MANIFEST_PATH", str(prod))
        monkeypatch.setenv("DBT_DEV_SCHEMA", "personal_tester")
        monkeypatch.chdir(project)

        result = SchemaCommand(_cfg(), str(prod), "test__events", False, False).execute()

        assert result is not None
        assert result["schema"] == "personal_tester"  # dev schema, not prod 'core'
