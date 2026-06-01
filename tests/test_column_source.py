"""Tests for CatalogColumnSource and BigQueryColumnSource.

These tests verify column fetching WITHOUT mocking git or state detection.
Only the data store (catalog parser / BigQuery client) is mocked.
"""

import os
from unittest.mock import MagicMock, patch

import pytest

from dbt_meta.command_impl.column_source import (
    BigQueryColumnSource,
    CatalogColumnSource,
    ColumnSourceFactory,
)
from dbt_meta.config import Config
from dbt_meta.utils.model_state import ModelState

_SAMPLE_COLUMNS = [
    {"name": "id", "data_type": "INT64"},
    {"name": "name", "data_type": "STRING"},
]

_STABLE_MODEL = {
    "name": "test_model",
    "alias": "test_model",
    "schema": "core_amas",
    "database": "my-project",
    "original_file_path": "models/core/test_model.sql",
}


def _cfg():
    cfg = Config.from_env()
    cfg.fallback_catalog_enabled = True
    cfg.prod_catalog_path = "/prod/catalog.json"
    cfg.dev_catalog_path = "/dev/catalog.json"
    return cfg


# ---------------------------------------------------------------------------
# BigQueryColumnSource
# ---------------------------------------------------------------------------

class TestBigQueryColumnSourceWithModel:
    def test_returns_columns_for_prod_model(self, capsys):
        source = BigQueryColumnSource(use_dev=False)

        with patch(
            "dbt_meta.command_impl.column_source._fetch_columns_from_bigquery_direct",
            return_value=_SAMPLE_COLUMNS,
        ):
            result = source.fetch(_STABLE_MODEL, "test_model", ModelState.PROD_STABLE)

        assert result == _SAMPLE_COLUMNS
        err = capsys.readouterr().err
        assert "prod table" in err

    def test_uses_prod_model_schema_in_prod_mode(self):
        prod_model = {**_STABLE_MODEL, "schema": "prod_schema"}
        dev_model = {**_STABLE_MODEL, "schema": "personal_user"}
        source = BigQueryColumnSource(use_dev=False)

        captured_calls = []

        def _mock_bq(schema, table, database=None, **kwargs):
            captured_calls.append({"schema": schema, "table": table})
            return _SAMPLE_COLUMNS

        with patch("dbt_meta.command_impl.column_source._fetch_columns_from_bigquery_direct", side_effect=_mock_bq):
            source.fetch(dev_model, "test_model", ModelState.MODIFIED_UNCOMMITTED, prod_model=prod_model)

        assert captured_calls[0]["schema"] == "prod_schema"

    def test_uses_dev_schema_when_use_dev(self):
        source = BigQueryColumnSource(use_dev=True)
        model = {**_STABLE_MODEL, "schema": "personal_user"}

        captured_calls = []

        def _mock_bq(schema, table, database=None, **kwargs):
            captured_calls.append({"schema": schema, "table": table})
            return _SAMPLE_COLUMNS

        with patch("dbt_meta.command_impl.column_source._fetch_columns_from_bigquery_direct", side_effect=_mock_bq):
            source.fetch(model, "test_model", ModelState.PROD_STABLE)

        assert captured_calls[0]["schema"] == "personal_user"

    def test_returns_none_when_bq_fails(self):
        source = BigQueryColumnSource()

        with patch(
            "dbt_meta.command_impl.column_source._fetch_columns_from_bigquery_direct",
            return_value=None,
        ):
            result = source.fetch(_STABLE_MODEL, "test_model", ModelState.PROD_STABLE)

        assert result is None

    def test_suppresses_not_found_in_json_mode(self, capsys):
        source = BigQueryColumnSource(json_output=True)

        with patch(
            "dbt_meta.command_impl.column_source._fetch_columns_from_bigquery_direct",
            return_value=None,
        ):
            source.fetch(_STABLE_MODEL, "test_model", ModelState.PROD_STABLE)

        err = capsys.readouterr().err
        assert "❌ Model not found" not in err


class TestBigQueryColumnSourceWithoutModel:
    def test_new_model_uses_dev_schema(self, capsys):
        source = BigQueryColumnSource()

        with (
            patch("dbt_meta.command_impl.column_source._calculate_dev_schema", return_value="personal_pfilianin"),
            patch(
                "dbt_meta.command_impl.column_source._fetch_columns_from_bigquery_direct",
                return_value=_SAMPLE_COLUMNS,
            ),
        ):
            result = source.fetch(None, "new_model", ModelState.NEW_UNCOMMITTED)

        assert result == _SAMPLE_COLUMNS
        err = capsys.readouterr().err
        assert "dev table" in err

    def test_modified_without_dev_uses_prod_model(self):
        prod_model = {**_STABLE_MODEL, "schema": "prod_schema"}
        source = BigQueryColumnSource(use_dev=False)

        captured = []

        def _mock_bq(schema, table, database=None, **kwargs):
            captured.append(schema)
            return _SAMPLE_COLUMNS

        with patch("dbt_meta.command_impl.column_source._fetch_columns_from_bigquery_direct", side_effect=_mock_bq):
            result = source.fetch(None, "test_model", ModelState.MODIFIED_UNCOMMITTED, prod_model=prod_model)

        assert result == _SAMPLE_COLUMNS
        assert captured[0] == "prod_schema"

    def test_modified_without_dev_infers_schema_when_no_prod_model(self):
        source = BigQueryColumnSource(use_dev=False)

        captured = []

        def _mock_bq(schema, table, database=None, **kwargs):
            captured.append({"schema": schema, "table": table})
            return _SAMPLE_COLUMNS

        with (
            patch("dbt_meta.command_impl.column_source.infer_table_parts", return_value=("core", "model")),
            patch("dbt_meta.command_impl.column_source._fetch_columns_from_bigquery_direct", side_effect=_mock_bq),
        ):
            result = source.fetch(None, "core__model", ModelState.MODIFIED_UNCOMMITTED)

        assert result == _SAMPLE_COLUMNS


# ---------------------------------------------------------------------------
# CatalogColumnSource
# ---------------------------------------------------------------------------

class TestCatalogColumnSource:
    def test_returns_catalog_columns_when_fresh(self, capsys):
        cfg = _cfg()
        source = CatalogColumnSource(cfg)

        mock_parser = MagicMock()
        mock_parser.get_file_age_hours.return_value = 1.0
        mock_parser.get_age_hours.return_value = 2.0
        mock_parser.get_columns.return_value = _SAMPLE_COLUMNS

        with (
            patch("os.path.exists", return_value=True),
            patch("dbt_meta.command_impl.column_source.CatalogParser", return_value=mock_parser),
        ):
            result = source.fetch(_STABLE_MODEL, "test_model", ModelState.PROD_STABLE)

        assert result == _SAMPLE_COLUMNS

    def test_falls_back_to_bigquery_when_catalog_stale(self):
        cfg = _cfg()
        source = CatalogColumnSource(cfg)

        mock_parser = MagicMock()
        mock_parser.get_file_age_hours.return_value = 25.0  # > 24h

        with (
            patch("os.path.exists", return_value=True),
            patch("dbt_meta.command_impl.column_source.CatalogParser", return_value=mock_parser),
            patch(
                "dbt_meta.command_impl.column_source._fetch_columns_from_bigquery_direct",
                return_value=_SAMPLE_COLUMNS,
            ),
        ):
            result = source.fetch(_STABLE_MODEL, "test_model", ModelState.PROD_STABLE)

        assert result == _SAMPLE_COLUMNS
        mock_parser.get_columns.assert_not_called()

    def test_falls_back_to_bigquery_when_model_not_in_catalog(self):
        cfg = _cfg()
        source = CatalogColumnSource(cfg)

        mock_parser = MagicMock()
        mock_parser.get_file_age_hours.return_value = 1.0
        mock_parser.get_age_hours.return_value = 2.0
        mock_parser.get_columns.return_value = None  # not in catalog

        with (
            patch("os.path.exists", return_value=True),
            patch("dbt_meta.command_impl.column_source.CatalogParser", return_value=mock_parser),
            patch(
                "dbt_meta.command_impl.column_source._fetch_columns_from_bigquery_direct",
                return_value=_SAMPLE_COLUMNS,
            ),
        ):
            result = source.fetch(_STABLE_MODEL, "test_model", ModelState.PROD_STABLE)

        assert result == _SAMPLE_COLUMNS

    def test_falls_back_when_catalog_disabled(self):
        cfg = _cfg()
        cfg.fallback_catalog_enabled = False
        source = CatalogColumnSource(cfg)

        with patch(
            "dbt_meta.command_impl.column_source._fetch_columns_from_bigquery_direct",
            return_value=_SAMPLE_COLUMNS,
        ):
            result = source.fetch(_STABLE_MODEL, "test_model", ModelState.PROD_STABLE)

        assert result == _SAMPLE_COLUMNS

    def test_falls_back_when_catalog_path_not_configured(self):
        cfg = _cfg()
        cfg.prod_catalog_path = None
        source = CatalogColumnSource(cfg)

        with patch(
            "dbt_meta.command_impl.column_source._fetch_columns_from_bigquery_direct",
            return_value=_SAMPLE_COLUMNS,
        ):
            result = source.fetch(_STABLE_MODEL, "test_model", ModelState.PROD_STABLE)

        assert result == _SAMPLE_COLUMNS

    def test_falls_back_when_catalog_file_missing(self):
        cfg = _cfg()
        source = CatalogColumnSource(cfg)

        with (
            patch("os.path.exists", return_value=False),
            patch(
                "dbt_meta.command_impl.column_source._fetch_columns_from_bigquery_direct",
                return_value=_SAMPLE_COLUMNS,
            ),
        ):
            result = source.fetch(_STABLE_MODEL, "test_model", ModelState.PROD_STABLE)

        assert result == _SAMPLE_COLUMNS

    def test_falls_back_on_catalog_parse_error(self):
        cfg = _cfg()
        source = CatalogColumnSource(cfg)

        with (
            patch("os.path.exists", return_value=True),
            patch("dbt_meta.command_impl.column_source.CatalogParser", side_effect=Exception("corrupt")),
            patch(
                "dbt_meta.command_impl.column_source._fetch_columns_from_bigquery_direct",
                return_value=_SAMPLE_COLUMNS,
            ),
        ):
            result = source.fetch(_STABLE_MODEL, "test_model", ModelState.PROD_STABLE)

        assert result == _SAMPLE_COLUMNS

    def test_goes_directly_to_bigquery_when_model_is_none(self):
        cfg = _cfg()
        source = CatalogColumnSource(cfg)

        with (
            patch("dbt_meta.command_impl.column_source._calculate_dev_schema", return_value="personal_user"),
            patch(
                "dbt_meta.command_impl.column_source._fetch_columns_from_bigquery_direct",
                return_value=_SAMPLE_COLUMNS,
            ),
        ):
            result = source.fetch(None, "new_model", ModelState.NEW_UNCOMMITTED)

        assert result == _SAMPLE_COLUMNS

    def test_prints_old_catalog_info_when_internal_age_exceeds_7_days(self, capsys):
        cfg = _cfg()
        source = CatalogColumnSource(cfg)

        mock_parser = MagicMock()
        mock_parser.get_file_age_hours.return_value = 1.0
        mock_parser.get_age_hours.return_value = 200.0  # >168h
        mock_parser.get_columns.return_value = _SAMPLE_COLUMNS

        with (
            patch("os.path.exists", return_value=True),
            patch("dbt_meta.command_impl.column_source.CatalogParser", return_value=mock_parser),
        ):
            source.fetch(_STABLE_MODEL, "test_model", ModelState.PROD_STABLE)

        err = capsys.readouterr().err
        assert "generated" in err


# ---------------------------------------------------------------------------
# ColumnSourceFactory
# ---------------------------------------------------------------------------

class TestColumnSourceFactory:
    @pytest.mark.parametrize("state", [
        ModelState.MODIFIED_UNCOMMITTED,
        ModelState.MODIFIED_COMMITTED,
        ModelState.MODIFIED_IN_DEV,
        ModelState.NEW_UNCOMMITTED,
        ModelState.NEW_COMMITTED,
        ModelState.NEW_IN_DEV,
    ])
    def test_returns_bigquery_source_for_changed_states(self, state):
        cfg = _cfg()
        source = ColumnSourceFactory.for_state(state, cfg)
        assert isinstance(source, BigQueryColumnSource)

    @pytest.mark.parametrize("state", [
        ModelState.PROD_STABLE,
        ModelState.DELETED_LOCALLY,
        ModelState.DEPRECATED_DISABLED,
        ModelState.NOT_FOUND,
    ])
    def test_returns_catalog_source_for_stable_states(self, state):
        cfg = _cfg()
        source = ColumnSourceFactory.for_state(state, cfg)
        assert isinstance(source, CatalogColumnSource)
