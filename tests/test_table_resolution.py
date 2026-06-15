"""Test table name resolution in dev vs prod mode.

CRITICAL: These tests verify correct table name resolution which was BROKEN before fixes.
In dev mode, we must use FULL model_name as table name (not extract parts).
"""

from unittest.mock import patch

import pytest

from dbt_meta.command_impl.column_source import BigQueryColumnSource
from dbt_meta.utils.model_state import ModelState


@pytest.mark.critical
class TestTableResolution:
    """Test dev table name resolution - CRITICAL BUG FIX."""

    def test_dev_table_name_uses_full_model_name(self):
        """CRITICAL FIX: Dev mode must use FULL model_name, not extract parts."""
        source = BigQueryColumnSource(use_dev=True)
        model = {
            'database': 'admirals-bi-dwh',
            'schema': 'core_client',
            'name': 'events',
            'alias': 'client_events',
        }

        with patch('dbt_meta.command_impl.column_source._fetch_columns_from_bigquery_direct') as mock_fetch:
            mock_fetch.return_value = [{'name': 'id', 'data_type': 'INT64'}]
            source._fetch_with_model(model, 'core_client__events', ModelState.MODIFIED_UNCOMMITTED, prod_model=None)

            # CRITICAL: Verify it uses FULL model_name in dev mode
            mock_fetch.assert_called_with('core_client', 'core_client__events', 'admirals-bi-dwh')

    def test_prod_table_name_uses_alias_or_name(self):
        """Production mode should use alias if available, otherwise name."""
        source = BigQueryColumnSource(use_dev=False)
        model = {
            'database': 'admirals-bi-dwh',
            'schema': 'core_client',
            'name': 'events',
            'alias': 'client_events',
        }

        with patch('dbt_meta.command_impl.column_source._fetch_columns_from_bigquery_direct') as mock_fetch:
            mock_fetch.return_value = [{'name': 'id', 'data_type': 'INT64'}]
            source._fetch_with_model(model, 'core_client__events', ModelState.PROD_STABLE, prod_model=None)

            mock_fetch.assert_called_with('core_client', 'client_events', 'admirals-bi-dwh')

    def test_dev_table_with_double_underscore(self):
        """Test dev table name for models with double underscores."""
        source = BigQueryColumnSource(use_dev=True)
        model = {
            'database': 'admirals-bi-dwh',
            'schema': 'staging_appsflyer',
            'name': 'in_app_events',
            'alias': 'appsflyer_events',
        }

        with patch('dbt_meta.command_impl.column_source._fetch_columns_from_bigquery_direct') as mock_fetch:
            mock_fetch.return_value = [{'name': 'id', 'data_type': 'INT64'}]
            source._fetch_with_model(model, 'staging_appsflyer__in_app_events', ModelState.NEW_IN_DEV, prod_model=None)

            mock_fetch.assert_called_with('staging_appsflyer', 'staging_appsflyer__in_app_events', 'admirals-bi-dwh')

    def test_dev_table_without_model_uses_full_name(self):
        """Test dev table resolution when model not in manifest."""
        source = BigQueryColumnSource(use_dev=True)

        with patch('dbt_meta.command_impl.column_source._calculate_dev_schema', return_value='personal_pavel_filianin'):
            with patch('dbt_meta.command_impl.column_source._fetch_columns_from_bigquery_direct') as mock_fetch:
                mock_fetch.return_value = [{'name': 'id', 'data_type': 'INT64'}]
                source._fetch_without_model('core_new__feature', ModelState.NEW_UNCOMMITTED, prod_model=None)

                mock_fetch.assert_called_with('personal_pavel_filianin', 'core_new__feature')

    def test_multiple_models_dev_resolution(self):
        """Test multiple models to ensure consistent dev table resolution."""
        test_cases = [
            ("core__clients", "core__clients"),
            ("staging__users", "staging__users"),
            ("mart_finance__revenue", "mart_finance__revenue"),
            ("intermediate__calculations", "intermediate__calculations"),
            ("raw_source__data", "raw_source__data"),
        ]

        for model_name, expected_table in test_cases:
            source = BigQueryColumnSource(use_dev=True)
            model = {
                'database': 'admirals-bi-dwh',
                'schema': 'some_schema',
                'name': model_name.split('__')[-1],
                'alias': 'some_alias',
            }

            with patch('dbt_meta.command_impl.column_source._fetch_columns_from_bigquery_direct') as mock_fetch:
                mock_fetch.return_value = [{'name': 'id', 'data_type': 'INT64'}]
                source._fetch_with_model(model, model_name, ModelState.MODIFIED_UNCOMMITTED, prod_model=None)

                _, table_arg, _ = mock_fetch.call_args[0]
                assert table_arg == expected_table, f"Expected {expected_table}, got {table_arg}"

    def test_modified_uncommitted_uses_prod_schema_not_dev(self):
        """CRITICAL FIX: MODIFIED_UNCOMMITTED without --dev must use production schema."""
        source = BigQueryColumnSource(use_dev=False)

        dev_model = {
            'database': 'admirals-bi-dwh',
            'schema': 'personal_pavel_filianin',
            'name': 'stg_google_play__installs_app_version',
            'alias': '',
        }
        prod_model = {
            'database': 'admirals-bi-dwh',
            'schema': 'staging_google_play',
            'name': 'installs_app_version',
            'alias': 'installs_app_version',
        }

        with patch('dbt_meta.command_impl.column_source._fetch_columns_from_bigquery_direct') as mock_fetch:
            mock_fetch.return_value = [{'name': 'id', 'data_type': 'INT64'}]
            source._fetch_with_model(
                dev_model,
                'stg_google_play__installs_app_version',
                ModelState.MODIFIED_UNCOMMITTED,
                prod_model=prod_model,
            )

            mock_fetch.assert_called_with('staging_google_play', 'installs_app_version', 'admirals-bi-dwh')

    def test_modified_uncommitted_without_model_uses_prod_schema(self):
        """CRITICAL FIX: without model, MODIFIED_UNCOMMITTED must use prod schema."""
        source = BigQueryColumnSource(use_dev=False)

        prod_model = {
            'database': 'admirals-bi-dwh',
            'schema': 'staging_google_play',
            'name': 'installs_app_version',
            'alias': 'installs_app_version',
        }

        with patch('dbt_meta.command_impl.column_source._fetch_columns_from_bigquery_direct') as mock_fetch:
            mock_fetch.return_value = [{'name': 'id', 'data_type': 'INT64'}]
            source._fetch_without_model(
                'stg_google_play__installs_app_version',
                ModelState.MODIFIED_UNCOMMITTED,
                prod_model=prod_model,
            )

            mock_fetch.assert_called_with('staging_google_play', 'installs_app_version', 'admirals-bi-dwh')

    def test_new_uncommitted_still_uses_dev_schema(self):
        """NEW models should still use dev schema (they only exist in dev)."""
        source = BigQueryColumnSource(use_dev=False)

        with patch('dbt_meta.command_impl.column_source._calculate_dev_schema', return_value='personal_pavel_filianin'):
            with patch('dbt_meta.command_impl.column_source._fetch_columns_from_bigquery_direct') as mock_fetch:
                mock_fetch.return_value = [{'name': 'id', 'data_type': 'INT64'}]
                source._fetch_without_model('core_new__feature', ModelState.NEW_UNCOMMITTED, prod_model=None)

                mock_fetch.assert_called_with('personal_pavel_filianin', 'core_new__feature')
