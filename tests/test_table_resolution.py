"""Test table name resolution in dev vs prod mode.

CRITICAL: These tests verify correct table name resolution which was BROKEN before fixes.
In dev mode, we must use FULL model_name as table name (not extract parts).
"""

import pytest
from unittest.mock import MagicMock, patch
from dbt_meta.command_impl.columns import ColumnsCommand
from dbt_meta.config import Config


@pytest.mark.critical
class TestTableResolution:
    """Test dev table name resolution - CRITICAL BUG FIX."""

    def test_dev_table_name_uses_full_model_name(self):
        """CRITICAL FIX: Dev mode must use FULL model_name, not extract parts."""
        # Setup command with dev mode
        config = Config.from_env()
        cmd = ColumnsCommand(
            manifest_path="/path/to/manifest.json",
            model_name="core_client__events",
            use_dev=True,
            json_output=False,
            config=config
        )

        # Mock model data
        model = {
            'database': 'admirals-bi-dwh',
            'schema': 'core_client',
            'name': 'events',
            'alias': 'client_events'
        }

        # Mock state
        from dbt_meta.utils.model_state import ModelState
        state = ModelState.MODIFIED_UNCOMMITTED

        # Test table name resolution
        with patch('dbt_meta.command_impl.columns._fetch_columns_from_bigquery_direct') as mock_fetch:
            mock_fetch.return_value = [{'name': 'id', 'data_type': 'INT64'}]

            # Call the method that resolves table name
            cmd._fetch_from_bigquery_with_model(model, state)

            # CRITICAL: Verify it uses FULL model_name in dev mode
            # BEFORE FIX: Would call with "events" (WRONG!)
            # AFTER FIX: Calls with "core_client__events" (CORRECT!)
            mock_fetch.assert_called_with(
                'core_client',
                'core_client__events',  # Full model name
                'admirals-bi-dwh'
            )

    def test_prod_table_name_uses_alias_or_name(self):
        """Production mode should use alias if available, otherwise name."""
        config = Config.from_env()
        cmd = ColumnsCommand(
            manifest_path="/path/to/manifest.json",
            model_name="core_client__events",
            use_dev=False,  # Production mode
            json_output=False,
            config=config
        )

        # Mock model data with alias
        model = {
            'database': 'admirals-bi-dwh',
            'schema': 'core_client',
            'name': 'events',
            'alias': 'client_events'
        }

        from dbt_meta.utils.model_state import ModelState
        state = ModelState.PROD_STABLE

        with patch('dbt_meta.command_impl.columns._fetch_columns_from_bigquery_direct') as mock_fetch:
            mock_fetch.return_value = [{'name': 'id', 'data_type': 'INT64'}]

            cmd._fetch_from_bigquery_with_model(model, state)

            # Production uses alias
            mock_fetch.assert_called_with(
                'core_client',
                'client_events',  # Uses alias in prod
                'admirals-bi-dwh'
            )

    def test_dev_table_with_double_underscore(self):
        """Test dev table name for models with double underscores."""
        config = Config.from_env()
        cmd = ColumnsCommand(
            manifest_path="/path/to/manifest.json",
            model_name="staging_appsflyer__in_app_events",
            use_dev=True,
            json_output=False,
            config=config
        )

        model = {
            'database': 'admirals-bi-dwh',
            'schema': 'staging_appsflyer',
            'name': 'in_app_events',
            'alias': 'appsflyer_events'
        }

        from dbt_meta.utils.model_state import ModelState
        state = ModelState.NEW_IN_DEV

        with patch('dbt_meta.command_impl.columns._fetch_columns_from_bigquery_direct') as mock_fetch:
            mock_fetch.return_value = [{'name': 'id', 'data_type': 'INT64'}]

            cmd._fetch_from_bigquery_with_model(model, state)

            # Must use FULL name in dev
            mock_fetch.assert_called_with(
                'staging_appsflyer',
                'staging_appsflyer__in_app_events',  # Full name
                'admirals-bi-dwh'
            )

    def test_dev_table_without_model_uses_full_name(self):
        """Test dev table resolution when model not in manifest."""
        config = Config.from_env()
        cmd = ColumnsCommand(
            manifest_path="/path/to/manifest.json",
            model_name="core_new__feature",
            use_dev=True,
            json_output=False,
            config=config
        )

        from dbt_meta.utils.model_state import ModelState
        state = ModelState.NEW_UNCOMMITTED

        with patch('dbt_meta.command_impl.columns._calculate_dev_schema') as mock_schema:
            mock_schema.return_value = 'personal_pavel_filianin'

            with patch('dbt_meta.command_impl.columns._fetch_columns_from_bigquery_direct') as mock_fetch:
                mock_fetch.return_value = [{'name': 'id', 'data_type': 'INT64'}]

                cmd._fetch_from_bigquery_without_model(state)

                # Must use FULL model_name even without model metadata
                mock_fetch.assert_called_with(
                    'personal_pavel_filianin',
                    'core_new__feature'  # Full name
                )

    def test_multiple_models_dev_resolution(self):
        """Test multiple models to ensure consistent dev table resolution."""
        test_cases = [
            ("core__clients", "core__clients"),
            ("staging__users", "staging__users"),
            ("mart_finance__revenue", "mart_finance__revenue"),
            ("intermediate__calculations", "intermediate__calculations"),
            ("raw_source__data", "raw_source__data")
        ]

        config = Config.from_env()
        for model_name, expected_table in test_cases:
            cmd = ColumnsCommand(
                manifest_path="/path/to/manifest.json",
                model_name=model_name,
                use_dev=True,
                json_output=False,
                config=config
            )

            model = {
                'database': 'admirals-bi-dwh',
                'schema': 'some_schema',
                'name': model_name.split('__')[-1],
                'alias': 'some_alias'
            }

            from dbt_meta.utils.model_state import ModelState
            state = ModelState.MODIFIED_UNCOMMITTED

            with patch('dbt_meta.command_impl.columns._fetch_columns_from_bigquery_direct') as mock_fetch:
                mock_fetch.return_value = [{'name': 'id', 'data_type': 'INT64'}]

                cmd._fetch_from_bigquery_with_model(model, state)

                # Always use full model_name in dev
                _, table_arg, _ = mock_fetch.call_args[0]
                assert table_arg == expected_table, f"Expected {expected_table}, got {table_arg}"