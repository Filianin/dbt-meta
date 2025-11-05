"""
Tests for Manifest Discovery & Parsing

This module consolidates tests for manifest infrastructure:
- ManifestFinder: 8-level priority search algorithm
- ManifestParser: Fast orjson parsing with lazy loading

Following TDD: These tests are written FIRST, then implementation.
"""

import pytest
from pathlib import Path
from dbt_meta.manifest.finder import ManifestFinder
from dbt_meta.manifest.parser import ManifestParser


# ============================================================================
# SECTION 1: Manifest Finder - 8-Level Priority Search
# ============================================================================


class TestManifestFinder:
    """Test 8-level priority manifest search logic"""

    def test_priority_1_explicit_env_var(self, tmp_path, monkeypatch):
        """
        Priority 1: DBT_MANIFEST_PATH environment variable

        Should find manifest when DBT_MANIFEST_PATH is set,
        regardless of other locations.
        """
        manifest_path = tmp_path / "custom" / "manifest.json"
        manifest_path.parent.mkdir(parents=True)
        manifest_path.write_text('{"metadata": {}}')

        monkeypatch.setenv("DBT_MANIFEST_PATH", str(manifest_path))

        finder = ManifestFinder()
        assert finder.find() == str(manifest_path.absolute())

    def test_priority_2_dbt_state_current_dir(self, tmp_path, monkeypatch):
        """
        Priority 2: ./.dbt-state/manifest.json

        Should find production manifest in current directory.
        """
        monkeypatch.chdir(tmp_path)

        manifest_path = tmp_path / ".dbt-state" / "manifest.json"
        manifest_path.parent.mkdir(parents=True)
        manifest_path.write_text('{"metadata": {}}')

        finder = ManifestFinder()
        assert finder.find() == str(manifest_path.absolute())

    def test_priority_3_target_current_dir(self, tmp_path, monkeypatch):
        """
        Priority 3: ./target/manifest.json

        Should find dev manifest in current directory
        when .dbt-state doesn't exist.
        """
        monkeypatch.chdir(tmp_path)

        manifest_path = tmp_path / "target" / "manifest.json"
        manifest_path.parent.mkdir(parents=True)
        manifest_path.write_text('{"metadata": {}}')

        finder = ManifestFinder()
        assert finder.find() == str(manifest_path.absolute())

    def test_priority_4_dbt_project_path_dbt_state(self, tmp_path, monkeypatch):
        """
        Priority 4: $DBT_PROJECT_PATH/.dbt-state/manifest.json

        Should find production manifest in DBT_PROJECT_PATH
        when not in current directory.
        """
        project_path = tmp_path / "dbt_project"
        manifest_path = project_path / ".dbt-state" / "manifest.json"
        manifest_path.parent.mkdir(parents=True)
        manifest_path.write_text('{"metadata": {}}')

        # Create other_dir before chdir
        other_dir = tmp_path / "other_dir"
        other_dir.mkdir()

        monkeypatch.setenv("DBT_PROJECT_PATH", str(project_path))
        monkeypatch.chdir(other_dir)

        finder = ManifestFinder()
        assert finder.find() == str(manifest_path.absolute())

    def test_production_prioritized_over_dev(self, tmp_path, monkeypatch):
        """
        CRITICAL: .dbt-state has priority over target

        When both production (.dbt-state) and dev (target) manifests exist,
        must always prefer production manifest.
        """
        monkeypatch.chdir(tmp_path)

        # Create both manifests
        prod_manifest = tmp_path / ".dbt-state" / "manifest.json"
        dev_manifest = tmp_path / "target" / "manifest.json"

        prod_manifest.parent.mkdir(parents=True)
        dev_manifest.parent.mkdir(parents=True)

        prod_manifest.write_text('{"metadata": {"env": "prod"}}')
        dev_manifest.write_text('{"metadata": {"env": "dev"}}')

        finder = ManifestFinder()
        found_path = finder.find()

        # MUST find production manifest
        assert found_path == str(prod_manifest.absolute())

    def test_raises_when_no_manifest_found(self, tmp_path, monkeypatch):
        """
        Should raise clear error when no manifest found

        Error message must explain all searched locations.
        """
        # Clear env vars that could point to manifests
        monkeypatch.delenv("DBT_MANIFEST_PATH", raising=False)
        monkeypatch.delenv("DBT_PROJECT_PATH", raising=False)

        monkeypatch.chdir(tmp_path)

        finder = ManifestFinder()

        with pytest.raises(FileNotFoundError, match="No manifest.json found"):
            finder.find()

    def test_finds_absolute_path(self, tmp_path, monkeypatch):
        """
        Should always return absolute path

        Even when manifest is found via relative path,
        return value must be absolute.
        """
        monkeypatch.chdir(tmp_path)

        manifest_path = tmp_path / "target" / "manifest.json"
        manifest_path.parent.mkdir(parents=True)
        manifest_path.write_text('{"metadata": {}}')

        finder = ManifestFinder()
        found = Path(finder.find())

        assert found.is_absolute()
        assert found.exists()

    def test_priority_5_dbt_project_path_target(self, tmp_path, monkeypatch):
        """
        Priority 5: $DBT_PROJECT_PATH/target/manifest.json

        Should find dev manifest in DBT_PROJECT_PATH
        when .dbt-state doesn't exist.
        """
        project_path = tmp_path / "dbt_project"
        manifest_path = project_path / "target" / "manifest.json"
        manifest_path.parent.mkdir(parents=True)
        manifest_path.write_text('{"metadata": {}}')

        other_dir = tmp_path / "other_dir"
        other_dir.mkdir()

        monkeypatch.setenv("DBT_PROJECT_PATH", str(project_path))
        monkeypatch.chdir(other_dir)

        finder = ManifestFinder()
        assert finder.find() == str(manifest_path.absolute())

    def test_priority_6_search_upward_production(self, tmp_path, monkeypatch):
        """
        Priority 6: Search upward for .dbt-state/manifest.json

        Should find production manifest in parent directory.
        """
        # Clear env vars to skip priorities 1-5
        monkeypatch.delenv("DBT_MANIFEST_PATH", raising=False)
        monkeypatch.delenv("DBT_PROJECT_PATH", raising=False)

        # Create manifest in parent
        parent_manifest = tmp_path / ".dbt-state" / "manifest.json"
        parent_manifest.parent.mkdir(parents=True)
        parent_manifest.write_text('{"metadata": {}}')

        # Change to subdirectory
        subdir = tmp_path / "nested" / "deep"
        subdir.mkdir(parents=True)
        monkeypatch.chdir(subdir)

        finder = ManifestFinder()
        assert finder.find() == str(parent_manifest.absolute())

    def test_priority_7_search_upward_dev(self, tmp_path, monkeypatch):
        """
        Priority 7: Search upward for target/manifest.json

        Should find dev manifest in parent directory
        when .dbt-state doesn't exist.
        """
        # Clear env vars to skip priorities 1-5
        monkeypatch.delenv("DBT_MANIFEST_PATH", raising=False)
        monkeypatch.delenv("DBT_PROJECT_PATH", raising=False)

        # Create manifest in parent
        parent_manifest = tmp_path / "target" / "manifest.json"
        parent_manifest.parent.mkdir(parents=True)
        parent_manifest.write_text('{"metadata": {}}')

        # Change to subdirectory
        subdir = tmp_path / "nested" / "deep"
        subdir.mkdir(parents=True)
        monkeypatch.chdir(subdir)

        finder = ManifestFinder()
        assert finder.find() == str(parent_manifest.absolute())


# ============================================================================
# SECTION 2: Manifest Parser - Fast orjson Parsing
# ============================================================================


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
