"""Tests for catalog.json parser."""

import json
from datetime import datetime, timedelta, timezone

import pytest

from dbt_meta.catalog.parser import CatalogParser


@pytest.fixture
def sample_catalog(tmp_path):
    """Create a sample catalog.json file for testing."""
    catalog_data = {
        "metadata": {
            "generated_at": datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z')
        },
        "nodes": {
            "model.admirals_bi_dwh.test_model": {
                "columns": {
                    "id": {
                        "name": "id",
                        "type": "INT64",
                        "index": 1
                    },
                    "name": {
                        "name": "name",
                        "type": "STRING",
                        "index": 2
                    },
                    "amount": {
                        "name": "amount",
                        "type": "FLOAT64",
                        "index": 3
                    }
                }
            },
            "model.admirals_bi_dwh.empty_columns": {
                "columns": {}
            }
        }
    }

    catalog_path = tmp_path / "catalog.json"
    with open(catalog_path, 'w') as f:
        json.dump(catalog_data, f)

    return catalog_path


@pytest.fixture
def old_catalog(tmp_path):
    """Create an old catalog.json file (48 hours ago)."""
    old_time = datetime.now(timezone.utc) - timedelta(hours=48)
    catalog_data = {
        "metadata": {
            "generated_at": old_time.isoformat().replace('+00:00', 'Z')
        },
        "nodes": {}
    }

    catalog_path = tmp_path / "old_catalog.json"
    with open(catalog_path, 'w') as f:
        json.dump(catalog_data, f)

    return catalog_path


class TestCatalogParser:
    """Test CatalogParser class."""

    def test_init_expands_path(self, tmp_path):
        """Test that __init__ expands user path."""
        parser = CatalogParser("~/test_catalog.json")
        assert parser.catalog_path.startswith("/")
        assert "~" not in parser.catalog_path

    def test_catalog_lazy_loading(self, sample_catalog):
        """Test that catalog is loaded lazily."""
        parser = CatalogParser(str(sample_catalog))

        # Access catalog property
        catalog = parser.catalog

        assert catalog is not None
        assert "metadata" in catalog
        assert "nodes" in catalog
        assert len(catalog["nodes"]) == 2

    def test_get_columns_success(self, sample_catalog):
        """Test get_columns returns columns sorted by index."""
        parser = CatalogParser(str(sample_catalog))

        columns = parser.get_columns("test_model")

        assert columns is not None
        assert len(columns) == 3

        # Check sorted by index
        assert columns[0]["name"] == "id"
        assert columns[0]["data_type"] == "integer"
        assert columns[1]["name"] == "name"
        assert columns[1]["data_type"] == "string"
        assert columns[2]["name"] == "amount"
        assert columns[2]["data_type"] == "float"

        # Check no _index in result
        for col in columns:
            assert "_index" not in col

    def test_get_columns_custom_project(self, tmp_path):
        """Test get_columns with custom project name."""
        catalog_data = {
            "metadata": {"generated_at": datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z')},
            "nodes": {
                "model.custom_project.my_model": {
                    "columns": {
                        "col1": {"name": "col1", "type": "STRING", "index": 1}
                    }
                }
            }
        }

        catalog_path = tmp_path / "catalog.json"
        with open(catalog_path, 'w') as f:
            json.dump(catalog_data, f)

        parser = CatalogParser(str(catalog_path))
        columns = parser.get_columns("my_model", project_name="custom_project")

        assert columns is not None
        assert len(columns) == 1
        assert columns[0]["name"] == "col1"

    def test_get_columns_model_not_found(self, sample_catalog):
        """Test get_columns returns None if model not in catalog."""
        parser = CatalogParser(str(sample_catalog))

        columns = parser.get_columns("nonexistent_model")

        assert columns is None

    def test_get_columns_empty_columns(self, sample_catalog):
        """Test get_columns returns None if columns dict is empty."""
        parser = CatalogParser(str(sample_catalog))

        columns = parser.get_columns("empty_columns")

        assert columns is None

    def test_get_age_hours_recent_catalog(self, sample_catalog):
        """Test get_age_hours for recent catalog."""
        parser = CatalogParser(str(sample_catalog))

        age_hours = parser.get_age_hours()

        assert age_hours is not None
        assert age_hours < 1.0  # Less than 1 hour old

    def test_get_age_hours_old_catalog(self, old_catalog):
        """Test get_age_hours for old catalog."""
        parser = CatalogParser(str(old_catalog))

        age_hours = parser.get_age_hours()

        assert age_hours is not None
        assert age_hours > 47  # About 48 hours old

    def test_get_age_hours_missing_metadata(self, tmp_path):
        """Test get_age_hours returns None if metadata missing."""
        catalog_data = {"nodes": {}}

        catalog_path = tmp_path / "catalog.json"
        with open(catalog_path, 'w') as f:
            json.dump(catalog_data, f)

        parser = CatalogParser(str(catalog_path))
        age_hours = parser.get_age_hours()

        assert age_hours is None

    def test_get_age_hours_invalid_timestamp(self, tmp_path):
        """Test get_age_hours returns None for invalid timestamp."""
        catalog_data = {
            "metadata": {"generated_at": "invalid-timestamp"}
        }

        catalog_path = tmp_path / "catalog.json"
        with open(catalog_path, 'w') as f:
            json.dump(catalog_data, f)

        parser = CatalogParser(str(catalog_path))
        age_hours = parser.get_age_hours()

        assert age_hours is None

    def test_get_file_age_hours_recent_file(self, sample_catalog):
        """Test get_file_age_hours for recently modified file."""
        parser = CatalogParser(str(sample_catalog))

        file_age_hours = parser.get_file_age_hours()

        assert file_age_hours is not None
        assert file_age_hours < 1.0  # File just created

    def test_get_file_age_hours_old_file(self, tmp_path):
        """Test get_file_age_hours for old file (mocked via os.path.getmtime)."""
        import os
        import time

        catalog_data = {"nodes": {}, "metadata": {}}
        catalog_path = tmp_path / "catalog.json"
        with open(catalog_path, 'w') as f:
            json.dump(catalog_data, f)

        # Set file mtime to 48 hours ago
        old_time = time.time() - (48 * 3600)
        os.utime(catalog_path, (old_time, old_time))

        parser = CatalogParser(str(catalog_path))
        file_age_hours = parser.get_file_age_hours()

        assert file_age_hours is not None
        assert file_age_hours > 47  # About 48 hours old

    def test_get_file_age_hours_nonexistent_file(self):
        """Test get_file_age_hours returns None for nonexistent file."""
        parser = CatalogParser("/nonexistent/catalog.json")

        file_age_hours = parser.get_file_age_hours()

        assert file_age_hours is None

    def test_file_age_vs_internal_age(self, tmp_path):
        """Test that file age and internal age can differ.

        This is the key scenario: file was recently synced (fresh mtime)
        but internal generated_at is old.
        """
        from datetime import timedelta

        # Create catalog with old generated_at timestamp
        old_time = datetime.now(timezone.utc) - timedelta(hours=72)
        catalog_data = {
            "metadata": {"generated_at": old_time.isoformat().replace('+00:00', 'Z')},
            "nodes": {}
        }

        catalog_path = tmp_path / "catalog.json"
        with open(catalog_path, 'w') as f:
            json.dump(catalog_data, f)

        parser = CatalogParser(str(catalog_path))

        # File age should be fresh (just created)
        file_age = parser.get_file_age_hours()
        assert file_age is not None
        assert file_age < 1.0

        # Internal age should be old
        internal_age = parser.get_age_hours()
        assert internal_age is not None
        assert internal_age > 71  # About 72 hours

    def test_is_stale_fresh_catalog(self, sample_catalog):
        """Test is_stale returns False for fresh catalog."""
        parser = CatalogParser(str(sample_catalog))

        is_stale = parser.is_stale(max_age_hours=2)

        assert is_stale is False

    def test_is_stale_old_catalog(self, old_catalog):
        """Test is_stale returns True for old catalog."""
        parser = CatalogParser(str(old_catalog))

        is_stale = parser.is_stale(max_age_hours=24)

        assert is_stale is True

    def test_is_stale_no_age_info(self, tmp_path):
        """Test is_stale returns True if age cannot be determined."""
        catalog_data = {"nodes": {}}

        catalog_path = tmp_path / "catalog.json"
        with open(catalog_path, 'w') as f:
            json.dump(catalog_data, f)

        parser = CatalogParser(str(catalog_path))
        is_stale = parser.is_stale()

        assert is_stale is True

    def test_normalize_type_bigquery_types(self):
        """Test _normalize_type converts BigQuery types to lowercase."""
        assert CatalogParser._normalize_type("INT64") == "integer"
        assert CatalogParser._normalize_type("STRING") == "string"
        assert CatalogParser._normalize_type("FLOAT64") == "float"
        assert CatalogParser._normalize_type("BOOL") == "boolean"
        assert CatalogParser._normalize_type("TIMESTAMP") == "timestamp"
        assert CatalogParser._normalize_type("DATE") == "date"
        assert CatalogParser._normalize_type("DATETIME") == "datetime"
        assert CatalogParser._normalize_type("NUMERIC") == "numeric"
        assert CatalogParser._normalize_type("BIGNUMERIC") == "bignumeric"
        assert CatalogParser._normalize_type("BYTES") == "bytes"

    def test_normalize_type_already_lowercase(self):
        """Test _normalize_type handles lowercase input."""
        assert CatalogParser._normalize_type("integer") == "integer"
        assert CatalogParser._normalize_type("string") == "string"

    def test_normalize_type_unknown_type(self):
        """Test _normalize_type returns lowercase for unknown types."""
        assert CatalogParser._normalize_type("CUSTOM_TYPE") == "custom_type"
        assert CatalogParser._normalize_type("WeirdType") == "weirdtype"

    def test_catalog_file_not_found(self):
        """Test that accessing catalog raises error if file doesn't exist."""
        parser = CatalogParser("/nonexistent/catalog.json")

        with pytest.raises(FileNotFoundError):
            _ = parser.catalog

    def test_catalog_invalid_json(self, tmp_path):
        """Test that accessing catalog raises error for invalid JSON."""
        catalog_path = tmp_path / "invalid.json"
        with open(catalog_path, 'w') as f:
            f.write("{ invalid json }")

        parser = CatalogParser(str(catalog_path))

        with pytest.raises(json.JSONDecodeError):
            _ = parser.catalog


class TestCatalogIntegration:
    """Integration tests for catalog parser edge cases."""

    def test_columns_sorted_by_index(self, tmp_path):
        """Test that columns are returned sorted by index, not alphabetically."""
        # Create catalog with columns in non-alphabetical order
        catalog_data = {
            "metadata": {"generated_at": datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z')},
            "nodes": {
                "model.admirals_bi_dwh.unsorted_model": {
                    "columns": {
                        "zebra": {"name": "zebra", "type": "STRING", "index": 3},
                        "apple": {"name": "apple", "type": "STRING", "index": 1},
                        "middle": {"name": "middle", "type": "STRING", "index": 2}
                    }
                }
            }
        }

        catalog_path = tmp_path / "catalog.json"
        with open(catalog_path, 'w') as f:
            json.dump(catalog_data, f)

        parser = CatalogParser(str(catalog_path))
        columns = parser.get_columns("unsorted_model")

        # Should be sorted by index, not alphabetically
        assert columns[0]["name"] == "apple"  # index 1
        assert columns[1]["name"] == "middle"  # index 2
        assert columns[2]["name"] == "zebra"  # index 3

    def test_columns_with_missing_index(self, tmp_path):
        """Test that columns without index field get default index 999."""
        catalog_data = {
            "metadata": {"generated_at": datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z')},
            "nodes": {
                "model.admirals_bi_dwh.missing_index": {
                    "columns": {
                        "col_with_index": {"name": "col_with_index", "type": "STRING", "index": 1},
                        "col_no_index": {"name": "col_no_index", "type": "STRING"}  # Missing index
                    }
                }
            }
        }

        catalog_path = tmp_path / "catalog.json"
        with open(catalog_path, 'w') as f:
            json.dump(catalog_data, f)

        parser = CatalogParser(str(catalog_path))
        columns = parser.get_columns("missing_index")

        # Column with index should come first
        assert columns[0]["name"] == "col_with_index"
        # Column without index should come last (default index 999)
        assert columns[1]["name"] == "col_no_index"
