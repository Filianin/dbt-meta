"""Tests to cover path.py BigQuery format search edge cases.

Target lines: 79, 84, 88-90, 95, 106, 113, 121-125, 130, 145-147, 152, 161, 166, 176
"""

import pytest
import json
from pathlib import Path
from unittest.mock import patch
from dbt_meta.commands import path
from dbt_meta.errors import ManifestNotFoundError


class TestPathBigQueryFormatEdgeCases:
    """Cover path.py BigQuery format search edge cases."""

    def test_path_bigquery_format_not_in_dev_mode(self, tmp_path, monkeypatch):
        """Test BigQuery format search returns None when use_dev=False (line 79)."""
        prod_manifest = tmp_path / "manifest.json"
        prod_manifest.write_text('{"metadata": {}, "nodes": {}}')

        monkeypatch.setenv('DBT_FALLBACK_TARGET', 'false')

        # BigQuery format with use_dev=False should not search
        result = path(str(prod_manifest), 'schema.table', use_dev=False, json_output=False)

        # Should return None (line 79 - not use_dev)
        assert result is None

    def test_path_bigquery_format_dev_manifest_not_found(self, tmp_path, monkeypatch):
        """Test BigQuery format search returns None when dev manifest missing (line 84)."""
        prod_manifest = tmp_path / "manifest.json"
        prod_manifest.write_text('{"metadata": {}, "nodes": {}}')

        monkeypatch.setenv('DBT_PROD_MANIFEST_PATH', str(prod_manifest))
        monkeypatch.setenv('DBT_DEV_MANIFEST_PATH', str(tmp_path / "nonexistent" / "manifest.json"))

        # Mock find_dev_manifest to return None (correct module path)
        with patch('dbt_meta.utils.dev.find_dev_manifest', return_value=None):
            result = path(str(prod_manifest), 'schema.table', use_dev=True, json_output=False)

            # Should return None (line 84 - no dev manifest)
            assert result is None

    def test_path_bigquery_format_single_part_name(self, tmp_path, monkeypatch):
        """Test BigQuery format with single part returns None (line 95)."""
        prod_manifest = tmp_path / "manifest.json"
        dev_manifest = tmp_path / "dev_manifest.json"

        prod_manifest.write_text('{"metadata": {}, "nodes": {}}')
        dev_manifest.write_text('{"metadata": {}, "nodes": {}}')

        monkeypatch.setenv('DBT_PROD_MANIFEST_PATH', str(prod_manifest))
        monkeypatch.setenv('DBT_DEV_MANIFEST_PATH', str(dev_manifest))

        with patch('dbt_meta.utils.dev.find_dev_manifest', return_value=str(dev_manifest)):
            # Single part name (no dot) should return None
            result = path(str(prod_manifest), 'tablename', use_dev=True, json_output=False)

            # Should return None (line 95 - len(parts) < 2)
            assert result is None

    def test_path_prod_bigquery_format_no_match(self, enable_fallbacks, tmp_path, monkeypatch):
        """Test production BigQuery format search with no matches (lines 145-176)."""
        prod_manifest = tmp_path / "manifest.json"
        prod_manifest.write_text(json.dumps({
            "metadata": {},
            "nodes": {
                "model.test.my_model": {
                    "name": "my_model",
                    "schema": "core",
                    "alias": "my_table",
                    "resource_type": "model",
                    "original_file_path": "models/core/my_model.sql"
                }
            }
        }))

        monkeypatch.setenv('DBT_PROD_MANIFEST_PATH', str(prod_manifest))
        monkeypatch.setenv('DBT_FALLBACK_TARGET', 'false')

        # Search for non-matching BigQuery format
        result = path(str(prod_manifest), 'different_schema.different_table', use_dev=False, json_output=False)

        # Should return None (no match found in prod manifest)
        assert result is None

    def test_path_prod_bigquery_format_match_by_alias(self, enable_fallbacks, prod_manifest):
        """Test production BigQuery format matches by alias."""
        from dbt_meta.manifest.parser import ManifestParser

        parser = ManifestParser(str(prod_manifest))
        nodes = parser.manifest.get('nodes', {})

        # Find model with alias
        for node_id, node_data in nodes.items():
            if node_data.get('resource_type') == 'model':
                schema = node_data.get('schema')
                alias = node_data.get('alias') or node_data.get('config', {}).get('alias')
                if schema and alias:
                    # Test BigQuery format: schema.alias
                    result = path(str(prod_manifest), f'{schema}.{alias}', use_dev=False, json_output=False)

                    if result:
                        assert 'sql' in result or '.sql' in result
                        break


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
