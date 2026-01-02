"""Test columns command BigQuery-first architecture and catalog fallback."""
import json
import os
import time
from unittest.mock import MagicMock

import pytest

from dbt_meta.commands import columns


class TestColumnsBigQueryFirst:
    """Test columns command's BigQuery-first architecture"""

    def test_columns_skips_catalog_for_modified_models(self, prod_manifest, mocker, test_model):
        """Modified models skip catalog, go directly to BigQuery"""
        # Mock git status - model is modified
        mocker.patch('dbt_meta.utils.git.is_modified', return_value=True)

        # Mock BigQuery call
        mock_run = mocker.patch('subprocess.run')
        mock_run.return_value = MagicMock(
            returncode=0,
            stdout=json.dumps([{"name": "id", "type": "INT64"}])
        )

        result = columns(prod_manifest, test_model)

        # Verify BigQuery was called directly (catalog skipped)
        assert mock_run.called

    def test_columns_catalog_staleness_message(self, prod_manifest, tmp_path, mocker, test_model, capsys):
        """Test catalog file age detection (>24h triggers BigQuery)"""
        # Create old catalog file (>24h)
        catalog_path = tmp_path / "catalog.json"
        catalog_data = {
            "metadata": {"generated_at": "2025-12-10T10:00:00Z"},  # 8 days ago
            "nodes": {}
        }
        catalog_path.write_text(json.dumps(catalog_data))

        # Mock file mtime to be old (>24h)
        mocker.patch('os.path.getmtime', return_value=time.time() - 86400 * 2)  # 2 days ago
        mocker.patch.dict(os.environ, {"DBT_PROD_CATALOG_PATH": str(catalog_path)})

        # Mock BigQuery fallback
        mock_run = mocker.patch('subprocess.run')
        mock_run.return_value = MagicMock(
            returncode=0,
            stdout=json.dumps([{"name": "id", "type": "INT64"}])
        )

        result = columns(prod_manifest, test_model)

        # Verify BigQuery was called (catalog too old)
        assert mock_run.called

        # Check warning message
        captured = capsys.readouterr()
        assert "catalog" in captured.err.lower() or "old" in captured.err.lower()
