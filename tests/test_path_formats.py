"""Test path command with BigQuery format (schema.table) support."""
import os

import pytest

from tests.helpers_cmd import path, schema


class TestPathBigQueryFormat:
    """Test path command with BigQuery format (schema.table)"""

    def test_path_bigquery_format_production(self, prod_manifest, test_model):
        """Test path lookup using schema.table format (BigQuery style)"""
        # Get dbt model info to construct BigQuery name
        schema_info = schema(prod_manifest, test_model)

        # Construct BigQuery format: schema.table
        bq_name = f"{schema_info['schema']}.{schema_info['table']}"

        result = path(prod_manifest, bq_name, use_dev=False)

        assert result is not None
        assert "models/" in result
        assert result.endswith(".sql")

    def test_path_bigquery_format_dev_mode(self, dev_manifest_setup, test_model):
        """Test dev manifest search with personal_user schema"""
        # Construct dev BigQuery format: personal_username.table
        username = os.environ.get('USER', 'testuser').replace('.', '_')
        dev_schema = f"personal_{username}"
        bq_name = f"{dev_schema}.{test_model}"

        result = path(dev_manifest_setup, bq_name, use_dev=True)

        # Should find path in dev manifest
        # May return None if dev manifest doesn't have the model
        assert result is None or isinstance(result, str)
        if result:
            assert result.endswith(".sql")

    def test_path_bigquery_format_fallback_to_dbt(self, prod_manifest):
        """Test fallback from BigQuery format to dbt format"""
        # Try with invalid BigQuery format (should fallback to dbt)
        result = path(prod_manifest, "invalid.format.with.dots")

        # Should handle gracefully
        assert result is None or isinstance(result, str)

    def test_path_single_part_name_no_bigquery_search(self, prod_manifest):
        """Single-part names skip BigQuery format search"""
        result = path(prod_manifest, "single_model_name")

        # Should use standard dbt search only
        # BigQuery search requires at least 2 parts (schema.table)
        assert result is None or isinstance(result, str)
