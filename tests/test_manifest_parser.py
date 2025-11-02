"""
Tests for ManifestParser - Fast manifest.json parsing with orjson

Following TDD: These tests are written FIRST, then implementation.
"""

import pytest
from pathlib import Path
from dbt_meta.manifest.parser import ManifestParser


class TestManifestParser:
    """Test manifest parsing with orjson and lazy loading"""

    def test_load_manifest_from_path(self, prod_manifest):
        """
        Should load manifest from provided path

        Uses orjson for fast parsing.
        """
        parser = ManifestParser(str(prod_manifest))

        assert parser.manifest_path == str(prod_manifest)
        # Manifest should not be loaded yet (lazy loading)
        assert not hasattr(parser, '_manifest')

    def test_lazy_loading_with_cached_property(self, prod_manifest):
        """
        Should use @cached_property for lazy loading

        Manifest is only loaded when accessed,
        and cached for subsequent access.
        """
        parser = ManifestParser(str(prod_manifest))

        # First access triggers loading
        manifest1 = parser.manifest
        assert manifest1 is not None
        assert 'nodes' in manifest1

        # Second access returns cached value (same object)
        manifest2 = parser.manifest
        assert manifest2 is manifest1

    def test_get_model_by_unique_id(self, prod_manifest):
        """
        Should retrieve model by unique_id

        Format: model.project.schema__model_name
        Example: model.reports.core_client__client_profiles_events
        """
        parser = ManifestParser(str(prod_manifest))

        # Get specific model
        model_name = "core_client__client_profiles_events"
        model = parser.get_model(model_name)

        assert model is not None
        assert 'unique_id' in model
        assert model_name in model['unique_id']
        assert 'columns' in model
        assert 'config' in model

    def test_get_model_not_found(self, prod_manifest):
        """
        Should return None for non-existent model

        Graceful error handling without exceptions.
        """
        parser = ManifestParser(str(prod_manifest))

        model = parser.get_model("nonexistent__model")

        assert model is None

    def test_get_all_models(self, prod_manifest):
        """
        Should return all models from manifest

        Filters nodes to include only models (exclude tests, seeds, etc.)
        """
        parser = ManifestParser(str(prod_manifest))

        models = parser.get_all_models()

        assert isinstance(models, dict)
        assert len(models) > 0

        # All entries should be models
        for unique_id, model in models.items():
            assert unique_id.startswith('model.')
            assert 'unique_id' in model
            assert 'columns' in model

    def test_parsing_performance(self, prod_manifest, benchmark):
        """
        Should parse 19MB manifest fast using orjson

        Target: <200ms for full load (orjson is 6-20x faster than stdlib)
        """
        def parse():
            parser = ManifestParser(str(prod_manifest))
            _ = parser.manifest
            return parser

        result = benchmark(parse)

        # Verify successful parse
        assert result.manifest is not None
        assert 'nodes' in result.manifest

    def test_manifest_not_found_raises_error(self, tmp_path):
        """
        Should raise FileNotFoundError for non-existent manifest

        Clear error message with path.
        """
        non_existent = tmp_path / "not_found.json"

        with pytest.raises(FileNotFoundError, match="Manifest not found"):
            parser = ManifestParser(str(non_existent))
            _ = parser.manifest  # Access triggers loading

    def test_invalid_json_raises_error(self, tmp_path):
        """
        Should raise clear error for invalid JSON

        orjson raises JSONDecodeError, wrap with helpful message.
        """
        invalid_manifest = tmp_path / "invalid.json"
        invalid_manifest.write_text("{ invalid json }")

        parser = ManifestParser(str(invalid_manifest))

        with pytest.raises(ValueError, match="Invalid JSON"):
            _ = parser.manifest

    def test_search_models_by_pattern(self, prod_manifest):
        """
        Should search models by name pattern

        Case-insensitive substring search.
        """
        parser = ManifestParser(str(prod_manifest))

        # Search for models containing "client"
        results = parser.search_models("client")

        assert len(results) > 0
        assert isinstance(results, list)

        # All results should contain "client" in name
        for model in results:
            assert 'unique_id' in model
            assert 'client' in model['unique_id'].lower()

    def test_get_model_dependencies(self, prod_manifest):
        """
        Should extract model dependencies (refs and sources)

        Returns: {"refs": [...], "sources": [...]}
        """
        parser = ManifestParser(str(prod_manifest))

        model_name = "core_client__client_profiles_events"
        deps = parser.get_dependencies(model_name)

        assert isinstance(deps, dict)
        assert 'refs' in deps
        assert 'sources' in deps
        assert isinstance(deps['refs'], list)
        assert isinstance(deps['sources'], list)
