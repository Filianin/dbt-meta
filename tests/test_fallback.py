"""Tests for fallback strategy module."""

import contextlib
from unittest.mock import Mock, patch

import pytest

from dbt_meta.config import Config
from dbt_meta.errors import ManifestNotFoundError, ModelNotFoundError
from dbt_meta.fallback import FallbackLevel, FallbackResult, FallbackStrategy


class TestFallbackResult:
    """Test FallbackResult dataclass."""

    def test_found_property_with_data(self):
        """Test that found returns True when data exists."""
        result = FallbackResult(
            data={'schema': 'test'},
            level=FallbackLevel.PROD_MANIFEST,
            warnings=[]
        )
        assert result.found is True

    def test_found_property_without_data(self):
        """Test that found returns False when data is None."""
        result = FallbackResult(
            data=None,
            level=None,
            warnings=['Model not found']
        )
        assert result.found is False


class TestFallbackStrategy:
    """Test FallbackStrategy class."""

    @pytest.fixture
    def mock_config(self, tmp_path, monkeypatch):
        """Create mock config with test paths."""
        prod_manifest = tmp_path / ".dbt-state" / "manifest.json"
        dev_manifest = tmp_path / "target" / "manifest.json"

        prod_manifest.parent.mkdir(parents=True)
        dev_manifest.parent.mkdir(parents=True)

        prod_manifest.write_text('{"metadata": {}}')
        dev_manifest.write_text('{"metadata": {}}')

        monkeypatch.setenv('DBT_PROD_MANIFEST_PATH', str(prod_manifest))
        monkeypatch.setenv('DBT_DEV_MANIFEST_PATH', str(dev_manifest))
        monkeypatch.setenv('DBT_FALLBACK_TARGET', 'true')
        monkeypatch.setenv('DBT_FALLBACK_BIGQUERY', 'true')

        return Config.from_env()

    @pytest.fixture
    def mock_prod_parser(self):
        """Create mock production parser."""
        parser = Mock()
        parser.get_model = Mock(return_value=None)
        return parser

    def test_get_model_from_prod_manifest(self, mock_config, mock_prod_parser):
        """Test successful fetch from production manifest."""
        model_data = {'schema': 'core_client', 'name': 'clients'}
        mock_prod_parser.get_model.return_value = model_data

        strategy = FallbackStrategy(mock_config)
        result = strategy.get_model('core_client__clients', mock_prod_parser)

        assert result.found is True
        assert result.data == model_data
        assert result.level == FallbackLevel.PROD_MANIFEST
        assert result.warnings == []
        mock_prod_parser.get_model.assert_called_once_with('core_client__clients')

    def test_fallback_to_dev_manifest(self, mock_config, mock_prod_parser, tmp_path):
        """Test fallback to dev manifest when model not in prod."""
        # Production parser returns None
        mock_prod_parser.get_model.return_value = None

        # Create dev manifest with model
        dev_manifest_path = tmp_path / "target" / "manifest.json"
        dev_manifest_content = {
            "metadata": {},
            "nodes": {
                "model.my_project.clients": {
                    "schema": "personal_user",
                    "name": "clients",
                    "alias": "clients"
                }
            }
        }

        import json
        dev_manifest_path.write_text(json.dumps(dev_manifest_content))

        strategy = FallbackStrategy(mock_config)

        with patch('dbt_meta.fallback.ManifestParser') as mock_parser_class:
            dev_parser = Mock()
            dev_parser.get_model.return_value = {
                'schema': 'personal_user',
                'name': 'clients'
            }
            mock_parser_class.return_value = dev_parser

            result = strategy.get_model('clients', mock_prod_parser)

        assert result.found is True
        assert result.level == FallbackLevel.DEV_MANIFEST
        assert len(result.warnings) == 1
        assert 'Using dev manifest' in result.warnings[0]

    def test_fallback_dev_manifest_disabled(self, mock_config, mock_prod_parser, monkeypatch):
        """Test that dev manifest is skipped when disabled."""
        # Disable dev fallback
        monkeypatch.setenv('DBT_FALLBACK_TARGET', 'false')
        config = Config.from_env()

        mock_prod_parser.get_model.return_value = None

        strategy = FallbackStrategy(config)

        with pytest.raises(ModelNotFoundError) as exc_info:
            strategy.get_model('test_model', mock_prod_parser)

        # Should search prod manifest and BigQuery (dev manifest skipped)
        assert exc_info.value.searched_locations == ['production manifest', 'BigQuery']
        assert 'dev manifest' not in exc_info.value.searched_locations

    def test_fallback_dev_manifest_not_found(self, enable_fallbacks, mock_config, mock_prod_parser, tmp_path):
        """Test fallback continues when dev manifest doesn't exist."""
        mock_prod_parser.get_model.return_value = None

        # Remove dev manifest
        dev_manifest = tmp_path / "target" / "manifest.json"
        if dev_manifest.exists():
            dev_manifest.unlink()

        strategy = FallbackStrategy(mock_config)

        with pytest.raises(ModelNotFoundError) as exc_info:
            # BigQuery fallback not implemented, so should fail
            strategy.get_model('test_model', mock_prod_parser)

        # Should have warning about dev manifest
        assert exc_info.value.searched_locations == [
            'production manifest',
            'dev manifest',
            'BigQuery'
        ]

    def test_fallback_to_bigquery_not_implemented(self, mock_config, mock_prod_parser):
        """Test fallback to BigQuery (currently returns None)."""
        mock_prod_parser.get_model.return_value = None

        strategy = FallbackStrategy(mock_config)

        # BigQuery fallback not implemented, should raise ModelNotFoundError
        with pytest.raises(ModelNotFoundError) as exc_info:
            strategy.get_model('test_model', mock_prod_parser)

        assert 'test_model' in exc_info.value.model_name
        assert 'BigQuery' in exc_info.value.searched_locations

    def test_allowed_levels_prod_only(self, mock_config, mock_prod_parser):
        """Test that only production manifest is searched when specified."""
        mock_prod_parser.get_model.return_value = None

        strategy = FallbackStrategy(mock_config)

        with pytest.raises(ModelNotFoundError) as exc_info:
            strategy.get_model(
                'test_model',
                mock_prod_parser,
                allowed_levels=[FallbackLevel.PROD_MANIFEST]
            )

        # Should only search production
        assert exc_info.value.searched_locations == ['production manifest']

    def test_allowed_levels_exclude_bigquery(self, enable_fallbacks, mock_config, mock_prod_parser, tmp_path):
        """Test excluding BigQuery level (e.g., for deps command)."""
        mock_prod_parser.get_model.return_value = None

        # Remove dev manifest so it fails
        dev_manifest = tmp_path / "target" / "manifest.json"
        if dev_manifest.exists():
            dev_manifest.unlink()

        strategy = FallbackStrategy(mock_config)

        with pytest.raises(ModelNotFoundError) as exc_info:
            strategy.get_model(
                'test_model',
                mock_prod_parser,
                allowed_levels=[
                    FallbackLevel.PROD_MANIFEST,
                    FallbackLevel.DEV_MANIFEST
                ]
            )

        # Should not include BigQuery
        assert 'BigQuery' not in exc_info.value.searched_locations

    def test_warning_collection(self, mock_config, mock_prod_parser, tmp_path):
        """Test that warnings are collected during fallback."""
        mock_prod_parser.get_model.return_value = None

        # Create dev manifest with model
        dev_manifest_path = tmp_path / "target" / "manifest.json"
        dev_manifest_content = {
            "metadata": {},
            "nodes": {
                "model.my_project.test_model": {
                    "schema": "personal_user",
                    "name": "test_model"
                }
            }
        }

        import json
        dev_manifest_path.write_text(json.dumps(dev_manifest_content))

        strategy = FallbackStrategy(mock_config)

        with patch('dbt_meta.fallback.ManifestParser') as mock_parser_class:
            dev_parser = Mock()
            dev_parser.get_model.return_value = {'schema': 'personal_user'}
            mock_parser_class.return_value = dev_parser

            result = strategy.get_model('test_model', mock_prod_parser)

        # Should have warning about using dev manifest
        assert len(result.warnings) == 1
        assert 'dev manifest' in result.warnings[0].lower()

    def test_get_dev_parser_raises_error_when_not_found(self, mock_config, tmp_path):
        """Test that _get_dev_parser raises ManifestNotFoundError."""
        # Remove dev manifest
        dev_manifest = tmp_path / "target" / "manifest.json"
        if dev_manifest.exists():
            dev_manifest.unlink()

        strategy = FallbackStrategy(mock_config)

        with pytest.raises(ManifestNotFoundError):
            strategy._get_dev_parser()

    def test_fetch_from_bigquery_returns_none(self, mock_config):
        """Test that _fetch_from_bigquery returns None (not implemented)."""
        strategy = FallbackStrategy(mock_config)
        result = strategy._fetch_from_bigquery('test_model')

        assert result is None

    def test_model_not_found_error_message(self, mock_config, mock_prod_parser):
        """Test ModelNotFoundError has correct model name and locations."""
        mock_prod_parser.get_model.return_value = None

        strategy = FallbackStrategy(mock_config)

        with pytest.raises(ModelNotFoundError) as exc_info:
            strategy.get_model(
                'core_client__events',
                mock_prod_parser,
                allowed_levels=[FallbackLevel.PROD_MANIFEST]
            )

        error = exc_info.value
        assert error.model_name == 'core_client__events'
        assert error.searched_locations == ['production manifest']
        assert 'core_client__events' in error.message

    def test_searched_levels_tracking(self, mock_config, mock_prod_parser):
        """Test that _searched_levels is updated correctly."""
        mock_prod_parser.get_model.return_value = None

        strategy = FallbackStrategy(mock_config)

        with contextlib.suppress(ModelNotFoundError):
            strategy.get_model('test_model', mock_prod_parser)

        # Should have searched all three levels
        assert len(strategy._searched_levels) == 3
        assert FallbackLevel.PROD_MANIFEST in strategy._searched_levels
        assert FallbackLevel.DEV_MANIFEST in strategy._searched_levels
        assert FallbackLevel.BIGQUERY in strategy._searched_levels

    def test_default_allowed_levels_includes_all(self, mock_config, mock_prod_parser):
        """Test that default allowed_levels includes all three levels."""
        mock_prod_parser.get_model.return_value = None

        strategy = FallbackStrategy(mock_config)

        with pytest.raises(ModelNotFoundError) as exc_info:
            # Don't pass allowed_levels, should use all
            strategy.get_model('test_model', mock_prod_parser)

        # Should have searched all three
        assert len(exc_info.value.searched_locations) == 3
