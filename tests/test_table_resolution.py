"""Test table name resolution in dev vs prod mode.

CRITICAL: These tests verify correct table name resolution which was BROKEN before fixes.
In dev mode, we must use FULL model_name as table name (not extract parts).
"""

from unittest.mock import patch

import pytest

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

    def test_modified_uncommitted_uses_prod_schema_not_dev(self):
        """CRITICAL FIX: MODIFIED_UNCOMMITTED without --dev must use production schema.

        Bug scenario:
        1. Model is MODIFIED_UNCOMMITTED (in prod manifest with local changes)
        2. User runs `meta columns model_name` (without --dev)
        3. get_model_with_fallback() might return model from dev manifest (fallback)
        4. BEFORE FIX: Would use dev schema (personal_xxx) - WRONG!
        5. AFTER FIX: Uses production schema from prod_model - CORRECT!
        """
        config = Config.from_env()
        cmd = ColumnsCommand(
            manifest_path="/path/to/manifest.json",
            model_name="stg_google_play__installs_app_version",
            use_dev=False,  # Production mode - CRITICAL
            json_output=False,
            config=config
        )

        # Model from dev manifest fallback (has dev schema - WRONG!)
        dev_model = {
            'database': 'admirals-bi-dwh',
            'schema': 'personal_pavel_filianin',  # Dev schema - would be used before fix
            'name': 'stg_google_play__installs_app_version',  # Full name in dev
            'alias': ''
        }

        # Production model (has correct production schema)
        prod_model = {
            'database': 'admirals-bi-dwh',
            'schema': 'staging_google_play',  # Production schema - correct!
            'name': 'installs_app_version',
            'alias': 'installs_app_version'
        }

        from dbt_meta.utils.model_state import ModelState
        state = ModelState.MODIFIED_UNCOMMITTED

        with patch('dbt_meta.command_impl.columns._fetch_columns_from_bigquery_direct') as mock_fetch:
            mock_fetch.return_value = [{'name': 'id', 'data_type': 'INT64'}]

            # Pass dev_model as model (what fallback returns)
            # But also pass prod_model (what we now pass for correct schema)
            cmd._fetch_from_bigquery_with_model(dev_model, state, prod_model)

            # CRITICAL: Must use PRODUCTION schema, not dev schema
            mock_fetch.assert_called_with(
                'staging_google_play',  # Production schema - CORRECT!
                'installs_app_version',  # Production table name - CORRECT!
                'admirals-bi-dwh'
            )

    def test_modified_uncommitted_without_model_uses_prod_schema(self):
        """CRITICAL FIX: _fetch_from_bigquery_without_model must use prod schema for MODIFIED.

        Bug scenario:
        1. Model is MODIFIED_UNCOMMITTED but model=None (mismatch in manifest paths)
        2. BEFORE FIX: Would use dev schema - WRONG!
        3. AFTER FIX: Uses production schema from prod_model - CORRECT!
        """
        config = Config.from_env()
        cmd = ColumnsCommand(
            manifest_path="/path/to/manifest.json",
            model_name="stg_google_play__installs_app_version",
            use_dev=False,  # Production mode
            json_output=False,
            config=config
        )

        # Production model for schema resolution
        prod_model = {
            'database': 'admirals-bi-dwh',
            'schema': 'staging_google_play',
            'name': 'installs_app_version',
            'alias': 'installs_app_version'
        }

        from dbt_meta.utils.model_state import ModelState
        state = ModelState.MODIFIED_UNCOMMITTED

        with patch('dbt_meta.command_impl.columns._fetch_columns_from_bigquery_direct') as mock_fetch:
            mock_fetch.return_value = [{'name': 'id', 'data_type': 'INT64'}]

            # model=None but prod_model provided
            cmd._fetch_from_bigquery_without_model(state, prod_model)

            # CRITICAL: Must use PRODUCTION schema from prod_model
            mock_fetch.assert_called_with(
                'staging_google_play',  # Production schema
                'installs_app_version',  # Production table name
                'admirals-bi-dwh'
            )

    def test_new_uncommitted_still_uses_dev_schema(self):
        """NEW models should still use dev schema (they only exist in dev)."""
        config = Config.from_env()
        cmd = ColumnsCommand(
            manifest_path="/path/to/manifest.json",
            model_name="core_new__feature",
            use_dev=False,  # Even in prod mode
            json_output=False,
            config=config
        )

        from dbt_meta.utils.model_state import ModelState
        state = ModelState.NEW_UNCOMMITTED

        with patch('dbt_meta.command_impl.columns._calculate_dev_schema') as mock_schema:
            mock_schema.return_value = 'personal_pavel_filianin'

            with patch('dbt_meta.command_impl.columns._fetch_columns_from_bigquery_direct') as mock_fetch:
                mock_fetch.return_value = [{'name': 'id', 'data_type': 'INT64'}]

                # NEW models: use dev schema (correct behavior)
                cmd._fetch_from_bigquery_without_model(state, None)

                # NEW models use dev schema - this is correct!
                mock_fetch.assert_called_with(
                    'personal_pavel_filianin',  # Dev schema for NEW
                    'core_new__feature'
                )