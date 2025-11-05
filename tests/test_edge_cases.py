"""
Edge case tests for dbt-meta

Tests covering:
- Empty string environment variables
- Null/None values in manifest data
- Special characters in usernames/templates
- Priority logic validation
- Fallback chain completeness
"""

import os
import pytest
from dbt_meta.commands import schema


class TestEmptyStringHandling:
    """Test handling of empty strings in environment variables"""

    def test_empty_prod_table_name_uses_default(self, prod_manifest, monkeypatch):
        """Empty DBT_PROD_TABLE_NAME should use default strategy"""
        monkeypatch.setenv("DBT_PROD_TABLE_NAME", "")
        result = schema(prod_manifest, "core_client__client_profiles_events")

        assert result is not None
        assert result["table"] == "client_profiles_events"  # Uses alias (default behavior)

    def test_empty_prod_schema_source_uses_default(self, prod_manifest, monkeypatch):
        """Empty DBT_PROD_SCHEMA_SOURCE should use default strategy"""
        monkeypatch.setenv("DBT_PROD_SCHEMA_SOURCE", "")
        result = schema(prod_manifest, "core_client__client_profiles_events")

        assert result is not None
        assert result["schema"] == "core_client"

    def test_empty_dev_schema_template_falls_back_to_prefix(self, dev_manifest_setup, monkeypatch):
        """Empty template should fallback to prefix logic"""
        monkeypatch.setenv("DBT_DEV_SCHEMA_TEMPLATE", "")
        monkeypatch.setenv("DBT_USER", "testuser")
        result = schema(str(dev_manifest_setup), "core_client__client_profiles_events", use_dev=True)

        assert result is not None
        assert result["schema"] == "personal_testuser"

    def test_empty_dev_schema_prefix_no_prefix(self, dev_manifest_setup, monkeypatch):
        """Empty prefix should result in username-only schema"""
        monkeypatch.setenv("DBT_DEV_SCHEMA_PREFIX", "")
        monkeypatch.setenv("DBT_USER", "testuser")
        result = schema(str(dev_manifest_setup), "core_client__client_profiles_events", use_dev=True)

        assert result is not None
        assert result["schema"] == "testuser"  # No prefix, just username

    def test_empty_user_falls_back_to_system(self, dev_manifest_setup, monkeypatch):
        """Empty DBT_USER should fallback to system USER"""
        monkeypatch.setenv("DBT_USER", "")
        # Don't unset USER - should fallback to it
        result = schema(str(dev_manifest_setup), "core_client__client_profiles_events", use_dev=True)

        assert result is not None
        assert result["schema"]  # Should have some value from fallback
        assert "_" in result["schema"]  # Should have prefix_username format


class TestSpecialCharacters:
    """Test handling of special characters in usernames and templates"""

    def test_multiple_dots_in_username_replaced(self, dev_manifest_setup, monkeypatch):
        """Multiple dots in username should all be replaced with underscores"""
        monkeypatch.setenv("DBT_USER", "john.doe.smith.jr")
        result = schema(str(dev_manifest_setup), "core_client__client_profiles_events", use_dev=True)

        assert result is not None
        assert "." not in result["schema"]
        assert result["schema"] == "personal_john_doe_smith_jr"

    def test_template_with_special_chars(self, dev_manifest_setup, monkeypatch):
        """Template can contain hyphens and other special chars"""
        monkeypatch.setenv("DBT_USER", "testuser")
        monkeypatch.setenv("DBT_DEV_SCHEMA_TEMPLATE", "dev-{username}-v2")
        result = schema(str(dev_manifest_setup), "core_client__client_profiles_events", use_dev=True)

        assert result is not None
        assert result["schema"] == "dev-testuser-v2"

    def test_very_long_username_handled(self, dev_manifest_setup, monkeypatch):
        """Very long usernames should be handled without errors"""
        long_username = "a" * 100
        monkeypatch.setenv("DBT_USER", long_username)
        result = schema(str(dev_manifest_setup), "core_client__client_profiles_events", use_dev=True)

        assert result is not None
        assert len(result["schema"]) > 100  # personal_ + username


class TestPriorityLogic:
    """Test priority ordering of configuration options"""

    def test_dev_schema_overrides_all(self, dev_manifest_setup, monkeypatch):
        """DBT_DEV_SCHEMA should override template and prefix"""
        monkeypatch.setenv("DBT_DEV_SCHEMA", "override_schema")
        monkeypatch.setenv("DBT_DEV_SCHEMA_TEMPLATE", "template_{username}")
        monkeypatch.setenv("DBT_DEV_SCHEMA_PREFIX", "prefix")
        result = schema(str(dev_manifest_setup), "core_client__client_profiles_events", use_dev=True)

        assert result is not None
        assert result["schema"] == "override_schema"

    def test_template_overrides_prefix(self, dev_manifest_setup, monkeypatch):
        """Template should override prefix when both are set"""
        monkeypatch.setenv("DBT_USER", "testuser")
        monkeypatch.setenv("DBT_DEV_SCHEMA_TEMPLATE", "tmpl_{username}")
        monkeypatch.setenv("DBT_DEV_SCHEMA_PREFIX", "wrongprefix")
        result = schema(str(dev_manifest_setup), "core_client__client_profiles_events", use_dev=True)

        assert result is not None
        assert result["schema"] == "tmpl_testuser"

    def test_prod_schema_source_model_ignores_config(self, prod_manifest, monkeypatch):
        """Strategy 'model' should use only model values, ignoring config"""
        monkeypatch.setenv("DBT_PROD_SCHEMA_SOURCE", "model")
        result = schema(prod_manifest, "DW_report")

        assert result is not None
        # Should use model.database and model.schema, not config
        assert result["database"] == "analytics-223714"
        assert result["schema"] == "tableau"

    def test_invalid_strategy_defaults_to_config_or_model(self, prod_manifest, monkeypatch):
        """Invalid strategy should default to config_or_model"""
        monkeypatch.setenv("DBT_PROD_SCHEMA_SOURCE", "invalid_strategy")
        result1 = schema(prod_manifest, "core_client__client_profiles_events")

        monkeypatch.delenv("DBT_PROD_SCHEMA_SOURCE", raising=False)
        result2 = schema(prod_manifest, "core_client__client_profiles_events")

        assert result1 == result2  # Should behave identically


class TestFallbackChains:
    """Test completeness of fallback chains"""

    def test_null_config_database_falls_back_to_model(self, prod_manifest):
        """When config.database is null, should use model.database"""
        # core_client__client_profiles_events has null config.database
        result = schema(prod_manifest, "core_client__client_profiles_events")

        assert result is not None
        assert result["database"] == "admirals-bi-dwh"  # From model

    def test_null_alias_falls_back_to_name(self, prod_manifest):
        """When alias is null, should use model name"""
        # DW_report has null alias
        result = schema(prod_manifest, "DW_report")

        assert result is not None
        assert result["table"] == "DW_report"  # From name

    def test_prod_table_name_strategies_all_have_fallback(self, prod_manifest, monkeypatch):
        """All table name strategies should handle missing values gracefully"""
        strategies = ["alias_or_name", "name", "alias"]

        for strategy in strategies:
            monkeypatch.setenv("DBT_PROD_TABLE_NAME", strategy)
            result = schema(prod_manifest, "DW_report")

            assert result is not None
            assert result["table"]  # Should always have a value
            assert len(result["table"]) > 0


class TestNullValues:
    """Test handling of null/None values in manifest data"""

    def test_model_with_null_config_values(self, prod_manifest):
        """Model with null config values should fall back to model values"""
        result = schema(prod_manifest, "core_client__client_profiles_events")

        assert result is not None
        assert result["database"]  # Should have fallback value
        assert result["schema"]    # Should have fallback value
        assert result["table"]     # Should have fallback value

    def test_all_required_fields_present_in_result(self, prod_manifest):
        """Result should always have all required fields"""
        result = schema(prod_manifest, "core_client__client_profiles_events")

        assert result is not None
        assert "database" in result
        assert "schema" in result
        assert "table" in result
        assert "full_name" in result

        # All fields should be non-empty strings
        assert isinstance(result["database"], str) and result["database"]
        assert isinstance(result["schema"], str) and result["schema"]
        assert isinstance(result["table"], str) and result["table"]
        assert isinstance(result["full_name"], str) and result["full_name"]


class TestEnvironmentVariableInteractions:
    """Test interactions between multiple environment variables"""

    def test_multiple_prod_configs_together(self, prod_manifest, monkeypatch):
        """Multiple production config variables should work together"""
        monkeypatch.setenv("DBT_PROD_TABLE_NAME", "name")
        monkeypatch.setenv("DBT_PROD_SCHEMA_SOURCE", "model")
        result = schema(prod_manifest, "core_client__client_profiles_events")

        assert result is not None
        assert result["table"] == "core_client__client_profiles_events"  # name strategy
        assert result["schema"] == "core_client"  # model strategy

    def test_multiple_dev_configs_priority(self, dev_manifest_setup, monkeypatch):
        """Multiple dev config variables should respect priority"""
        monkeypatch.setenv("DBT_USER", "user1")
        monkeypatch.setenv("DBT_DEV_SCHEMA", "schema1")
        monkeypatch.setenv("DBT_DEV_SCHEMA_TEMPLATE", "template_{username}")
        monkeypatch.setenv("DBT_DEV_SCHEMA_PREFIX", "prefix")

        result = schema(str(dev_manifest_setup), "core_client__client_profiles_events", use_dev=True)

        # DBT_DEV_SCHEMA has highest priority
        assert result is not None
        assert result["schema"] == "schema1"


class TestEdgeCasesCombinations:
    """Test complex edge case combinations"""

    def test_empty_user_with_template(self, dev_manifest_setup, monkeypatch):
        """Empty user with template should fall back to system user"""
        monkeypatch.setenv("DBT_USER", "")
        monkeypatch.setenv("DBT_DEV_SCHEMA_TEMPLATE", "dev_{username}")
        result = schema(str(dev_manifest_setup), "core_client__client_profiles_events", use_dev=True)

        assert result is not None
        assert result["schema"].startswith("dev_")
        assert len(result["schema"]) > 4  # More than just "dev_"

    def test_dots_in_username_with_template(self, dev_manifest_setup, monkeypatch):
        """Dots in username should be replaced even with template"""
        monkeypatch.setenv("DBT_USER", "john.doe")
        monkeypatch.setenv("DBT_DEV_SCHEMA_TEMPLATE", "sandbox_{username}")
        result = schema(str(dev_manifest_setup), "core_client__client_profiles_events", use_dev=True)

        assert result is not None
        assert "." not in result["schema"]
        assert result["schema"] == "sandbox_john_doe"

    def test_all_config_empty_uses_sensible_defaults(self, prod_manifest, tmp_path, monkeypatch):
        """When all configs are empty/default, should still produce valid output"""
        # Clear all env vars
        for var in ["DBT_PROD_TABLE_NAME", "DBT_PROD_SCHEMA_SOURCE",
                     "DBT_DEV_SCHEMA", "DBT_DEV_SCHEMA_TEMPLATE",
                     "DBT_DEV_SCHEMA_PREFIX"]:
            monkeypatch.delenv(var, raising=False)

        result_prod = schema(prod_manifest, "core_client__client_profiles_events")

        # Create dev manifest for dev test
        project_root = tmp_path / "project"
        project_root.mkdir()
        dbt_state = project_root / ".dbt-state"
        dbt_state.mkdir()
        target = project_root / "target"
        target.mkdir()

        prod_path = dbt_state / "manifest.json"
        prod_path.write_text('{"nodes": {}}')

        dev_path = target / "manifest.json"
        import json
        dev_data = {
            "nodes": {
                "model.project.core_client__client_profiles_events": {
                    "name": "client_profiles_events",
                    "schema": "core_client",
                    "database": "",
                    "config": {}
                }
            }
        }
        dev_path.write_text(json.dumps(dev_data))

        result_dev = schema(str(prod_path), "core_client__client_profiles_events", use_dev=True)

        assert result_prod is not None
        assert result_dev is not None
        assert all(result_prod.values())  # All values should be truthy
        assert all(result_dev.values())   # All values should be truthy


class TestBigQueryValidation:
    """Test BigQuery schema name validation (opt-in feature)"""

    def test_validation_disabled_by_default(self, dev_manifest_setup, monkeypatch):
        """BigQuery validation should be disabled by default"""
        monkeypatch.setenv("DBT_USER", "user@company")
        monkeypatch.delenv("DBT_VALIDATE_BIGQUERY", raising=False)
        result = schema(str(dev_manifest_setup), "core_client__client_profiles_events", use_dev=True)

        assert result is not None
        # @ should be preserved when validation is disabled
        assert "@" in result["schema"]

    def test_validation_enabled_sanitizes_invalid_chars(self, dev_manifest_setup, monkeypatch, capsys):
        """When enabled, validation should sanitize invalid characters"""
        monkeypatch.setenv("DBT_USER", "user@company")
        monkeypatch.setenv("DBT_VALIDATE_BIGQUERY", "true")
        result = schema(str(dev_manifest_setup), "core_client__client_profiles_events", use_dev=True)

        assert result is not None
        # @ should be replaced with _ when validation is enabled
        assert "@" not in result["schema"]
        assert "_" in result["schema"]

        # Should print warning to stderr
        captured = capsys.readouterr()
        assert "BigQuery validation" in captured.err
        assert "Invalid BigQuery characters replaced" in captured.err

    def test_validation_dots_in_template(self, dev_manifest_setup, monkeypatch, capsys):
        """Validation should sanitize dots in template results"""
        monkeypatch.setenv("DBT_USER", "testuser")
        monkeypatch.setenv("DBT_DEV_SCHEMA_TEMPLATE", "dev-{username}-v2.0")
        monkeypatch.setenv("DBT_VALIDATE_BIGQUERY", "1")  # Also test "1" as true
        result = schema(str(dev_manifest_setup), "core_client__client_profiles_events", use_dev=True)

        assert result is not None
        # Dot should be replaced
        assert "." not in result["schema"]
        assert result["schema"] == "dev-testuser-v2_0"

    def test_validation_various_true_values(self, dev_manifest_setup, monkeypatch):
        """Validation should recognize various true values"""
        true_values = ["true", "True", "TRUE", "1", "yes", "Yes", "YES"]

        for true_val in true_values:
            monkeypatch.setenv("DBT_USER", "user@test")
            monkeypatch.setenv("DBT_VALIDATE_BIGQUERY", true_val)
            result = schema(str(dev_manifest_setup), "core_client__client_profiles_events", use_dev=True)

            assert result is not None
            assert "@" not in result["schema"], f"Failed for DBT_VALIDATE_BIGQUERY={true_val}"

    def test_validation_false_values_disabled(self, dev_manifest_setup, monkeypatch):
        """Validation should be disabled for false/empty values"""
        false_values = ["false", "False", "0", "no", ""]

        for false_val in false_values:
            monkeypatch.setenv("DBT_USER", "user@test")
            if false_val:
                monkeypatch.setenv("DBT_VALIDATE_BIGQUERY", false_val)
            else:
                monkeypatch.delenv("DBT_VALIDATE_BIGQUERY", raising=False)
            result = schema(str(dev_manifest_setup), "core_client__client_profiles_events", use_dev=True)

            assert result is not None
            assert "@" in result["schema"], f"Failed for DBT_VALIDATE_BIGQUERY={false_val}"

    def test_validation_starts_with_number(self, dev_manifest_setup, monkeypatch, capsys):
        """Validation should prepend underscore if name starts with number"""
        monkeypatch.setenv("DBT_USER", "123user")
        monkeypatch.setenv("DBT_DEV_SCHEMA_PREFIX", "")  # No prefix to expose the issue
        monkeypatch.setenv("DBT_VALIDATE_BIGQUERY", "true")
        result = schema(str(dev_manifest_setup), "core_client__client_profiles_events", use_dev=True)

        assert result is not None
        # Should start with underscore after validation
        assert result["schema"].startswith("_")
        assert result["schema"] == "_123user"

        captured = capsys.readouterr()
        assert "must start with letter or underscore" in captured.err

    def test_validation_very_long_name_truncated(self, dev_manifest_setup, monkeypatch, capsys):
        """Validation should truncate names longer than 1024 chars"""
        monkeypatch.setenv("DBT_USER", "a" * 1100)
        monkeypatch.setenv("DBT_DEV_SCHEMA_PREFIX", "")
        monkeypatch.setenv("DBT_VALIDATE_BIGQUERY", "true")
        result = schema(str(dev_manifest_setup), "core_client__client_profiles_events", use_dev=True)

        assert result is not None
        # Should be truncated to 1024
        assert len(result["schema"]) <= 1024

        captured = capsys.readouterr()
        assert "too long" in captured.err
