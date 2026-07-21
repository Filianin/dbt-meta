"""Tests for the resolve_location seam (step 2 of the resolver unification).

Two concerns:
- resolve_location() matrix — dev vs prod semantics, invariant #4, strategies.
- Regression: prod-mode dev-manifest fallback must yield the dev location for
  every command that reports a table location (schema, context), not just schema.
"""

import json
import warnings

import pytest

from dbt_meta.command_impl.resolver import ResolvedLocation, resolve_location
from dbt_meta.config import Config
from dbt_meta.fallback import FallbackLevel


def _cfg() -> Config:
    with warnings.catch_warnings():
        warnings.filterwarnings("ignore", category=DeprecationWarning)
        return Config.from_env()


PROD_MODEL = {
    "name": "test__events",
    "schema": "core",
    "database": "proj",
    "config": {"alias": "events", "schema": "core_cfg", "database": "proj_cfg"},
}


# --------------------------------------------------------------------------
# resolve_location — dev semantics (invariant #4)
# --------------------------------------------------------------------------


class TestResolveLocationDev:
    def test_use_dev_flag_yields_dev_location(self, monkeypatch):
        monkeypatch.setenv("DBT_DEV_SCHEMA", "personal_x")
        loc = resolve_location(
            PROD_MODEL, "test__events", FallbackLevel.PROD_MANIFEST, True, _cfg()
        )
        assert loc.is_dev
        assert loc.schema == "personal_x"
        assert loc.table == "test__events"  # SQL filename, not alias
        assert loc.database == ""
        assert loc.full_name == "personal_x.test__events"  # 2-part

    def test_dev_manifest_fallback_yields_dev_location(self, monkeypatch):
        """THE FIX: prod mode (use_dev=False) but found via prod→dev fallback."""
        monkeypatch.setenv("DBT_DEV_SCHEMA", "personal_x")
        loc = resolve_location(
            PROD_MODEL, "test__events", FallbackLevel.DEV_MANIFEST, False, _cfg()
        )
        assert loc.is_dev
        assert loc.schema == "personal_x"
        assert loc.full_name == "personal_x.test__events"

    def test_dev_table_name_honours_pattern(self, monkeypatch):
        monkeypatch.setenv("DBT_DEV_SCHEMA", "personal_x")
        monkeypatch.setenv("DBT_DEV_TABLE_PATTERN", "alias")
        loc = resolve_location(
            PROD_MODEL, "test__events", FallbackLevel.DEV_MANIFEST, False, _cfg()
        )
        assert loc.table == "events"  # alias pattern


# --------------------------------------------------------------------------
# resolve_location — prod semantics + strategies
# --------------------------------------------------------------------------


class TestResolveLocationProd:
    def test_prod_default_config_or_model(self, monkeypatch):
        monkeypatch.delenv("DBT_PROD_SCHEMA_SOURCE", raising=False)
        monkeypatch.delenv("DBT_PROD_TABLE_NAME", raising=False)
        loc = resolve_location(
            PROD_MODEL, "test__events", FallbackLevel.PROD_MANIFEST, False, _cfg()
        )
        assert not loc.is_dev
        assert loc.schema == "core_cfg"  # config wins
        assert loc.database == "proj_cfg"
        assert loc.table == "events"  # alias wins
        assert loc.full_name == "proj_cfg.core_cfg.events"  # 3-part

    def test_prod_schema_source_model(self, monkeypatch):
        monkeypatch.setenv("DBT_PROD_SCHEMA_SOURCE", "model")
        loc = resolve_location(
            PROD_MODEL, "test__events", FallbackLevel.PROD_MANIFEST, False, _cfg()
        )
        assert loc.schema == "core"  # model, not config
        assert loc.database == "proj"

    def test_prod_table_name_strategy_name(self, monkeypatch):
        monkeypatch.setenv("DBT_PROD_TABLE_NAME", "name")
        loc = resolve_location(
            PROD_MODEL, "test__events", FallbackLevel.PROD_MANIFEST, False, _cfg()
        )
        assert loc.table == "test__events"  # name, not alias

    def test_bigquery_level_uses_prod_formatting(self, monkeypatch):
        monkeypatch.delenv("DBT_PROD_SCHEMA_SOURCE", raising=False)
        loc = resolve_location(PROD_MODEL, "test__events", FallbackLevel.BIGQUERY, False, _cfg())
        assert not loc.is_dev
        assert loc.full_name.count(".") == 2  # 3-part

    def test_prod_full_name_stays_three_part_when_database_empty(self, monkeypatch):
        monkeypatch.setenv("DBT_PROD_SCHEMA_SOURCE", "model")
        model = {"name": "m", "schema": "core", "database": "", "config": {}}
        loc = resolve_location(model, "m", FallbackLevel.PROD_MANIFEST, False, _cfg())
        assert loc.full_name == ".core.m"  # leading dot preserved (historical shape)

    def test_missing_alias_falls_back_to_name(self, monkeypatch):
        monkeypatch.delenv("DBT_PROD_TABLE_NAME", raising=False)
        model = {"name": "m", "schema": "core", "database": "proj", "config": {}}
        loc = resolve_location(model, "m", FallbackLevel.PROD_MANIFEST, False, _cfg())
        assert loc.table == "m"

    def test_none_level_is_prod(self, monkeypatch):
        monkeypatch.delenv("DBT_PROD_SCHEMA_SOURCE", raising=False)
        loc = resolve_location(PROD_MODEL, "test__events", None, False, _cfg())
        assert not loc.is_dev


class TestResolvedLocationDataclass:
    def test_as_dict_shape(self):
        loc = ResolvedLocation(database="p", schema="s", table="t", is_dev=False)
        assert loc.as_dict() == {
            "database": "p",
            "schema": "s",
            "table": "t",
            "full_name": "p.s.t",
        }


# --------------------------------------------------------------------------
# Regression — dev-only model in prod mode → dev location across commands
# --------------------------------------------------------------------------


def _dev_fallback_project(tmp_path):
    """Prod manifest without the model, dev manifest (target/) with it."""
    project = tmp_path / "proj"
    (project / "target").mkdir(parents=True)
    prod = project / ".dbt-state" / "manifest.json"
    prod.parent.mkdir(parents=True)
    prod.write_text(json.dumps({"nodes": {}}))
    dev = project / "target" / "manifest.json"
    dev.write_text(
        json.dumps(
            {
                "nodes": {
                    "model.p.test__events": {
                        "name": "test__events",
                        "schema": "core",  # prod schema baked in by dbt compile
                        "database": "proj",
                        "config": {"alias": "events"},
                    }
                }
            }
        )
    )
    return project, prod


@pytest.mark.usefixtures("enable_fallbacks")
class TestDevFallbackLocationRegression:
    def test_schema_command_returns_dev_location(self, tmp_path, monkeypatch):
        from dbt_meta.command_impl.schema import SchemaCommand

        project, prod = _dev_fallback_project(tmp_path)
        monkeypatch.setenv("DBT_PROD_MANIFEST_PATH", str(prod))
        monkeypatch.setenv("DBT_DEV_SCHEMA", "personal_tester")
        monkeypatch.chdir(project)

        result = SchemaCommand(_cfg(), str(prod), "test__events", False, False).execute()

        assert result["schema"] == "personal_tester"
        assert result["full_name"] == "personal_tester.test__events"

    def test_context_command_returns_dev_full_name(self, tmp_path, monkeypatch):
        """Previously context reported prod schema for dev-fallback models (bug)."""
        from dbt_meta.command_impl.context import ContextCommand

        project, prod = _dev_fallback_project(tmp_path)
        monkeypatch.setenv("DBT_PROD_MANIFEST_PATH", str(prod))
        monkeypatch.setenv("DBT_DEV_SCHEMA", "personal_tester")
        monkeypatch.chdir(project)

        cmd = ContextCommand(_cfg(), str(prod), "test__events", False, True)
        # process_model drives the FQN; columns/stats need BigQuery/catalog which
        # are absent here, so assert on the resolved location directly.
        model = cmd.get_model_with_fallback()
        assert model is not None
        bundle = cmd.process_model(model, cmd._fallback_level)
        assert bundle["full_name"] == "personal_tester.test__events"
