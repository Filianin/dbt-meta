"""
Tests for Infrastructure - Manifest discovery, parsing, and warning systems

This module consolidates infrastructure tests:
- ManifestFinder: 4-level global-only priority search
- ManifestParser: Fast orjson parsing with lazy loading and caching
- Warning system: Machine-readable JSON warnings for AI agents
- Git change detection: Intelligent warnings for manifest mismatches

Replaces old files:
- test_manifest_and_discovery.py
- test_warning_system.py
"""

import json
import os
from pathlib import Path

import pytest

from dbt_meta.commands import (
    _check_manifest_git_mismatch,
    _print_warnings,
    children,
    columns,
    config,
    deps,
    docs,
    info,
    parents,
    path,
    schema,
    sql,
)
from dbt_meta.errors import ManifestNotFoundError, ManifestParseError
from dbt_meta.manifest.finder import ManifestFinder
from dbt_meta.manifest.parser import ManifestParser

# ============================================================================
# SECTION 1: Manifest Finder - 4-Level Priority Search
# ============================================================================


class TestManifestFinder:
    """Test 3-level priority manifest search logic (simplified strategy)"""

    def test_priority_1_explicit_path_parameter(self, tmp_path):
        """
        Priority 1: explicit_path parameter (from --manifest flag)

        Should find manifest when explicit_path is provided,
        regardless of environment variables or other locations.
        """
        manifest_path = tmp_path / "custom" / "manifest.json"
        manifest_path.parent.mkdir(parents=True)
        manifest_path.write_text('{"metadata": {}}')

        finder = ManifestFinder()
        assert finder.find(explicit_path=str(manifest_path)) == str(manifest_path.absolute())

    def test_priority_2_dev_manifest_with_use_dev(self, tmp_path, monkeypatch):
        """
        Priority 2: DBT_DEV_MANIFEST_PATH (when use_dev=True)

        Should find dev manifest when use_dev=True is provided.
        Default location: ./target/manifest.json
        """
        monkeypatch.chdir(tmp_path)

        # Create dev manifest
        dev_manifest = tmp_path / "target" / "manifest.json"
        dev_manifest.parent.mkdir(parents=True)
        dev_manifest.write_text('{"metadata": {"env": "dev"}}')

        finder = ManifestFinder()
        found = finder.find(use_dev=True)

        assert found == str(dev_manifest.absolute())

    def test_priority_3_production_manifest(self, tmp_path, monkeypatch):
        """
        Priority 3: DBT_PROD_MANIFEST_PATH (production manifest)

        Should find production manifest via DBT_PROD_MANIFEST_PATH.
        Default location: ~/dbt-state/manifest.json
        """
        # Create production manifest in custom location
        prod_manifest = tmp_path / "dbt-state" / "manifest.json"
        prod_manifest.parent.mkdir(parents=True)
        prod_manifest.write_text('{"metadata": {"env": "prod"}}')

        # Set environment variable
        monkeypatch.setenv("DBT_PROD_MANIFEST_PATH", str(prod_manifest))

        finder = ManifestFinder()
        found = finder.find()

        assert found == str(prod_manifest.absolute())

    def test_explicit_path_overrides_use_dev(self, tmp_path, monkeypatch):
        """
        CRITICAL: explicit_path has highest priority

        When both explicit_path and use_dev=True are provided,
        explicit_path takes precedence and use_dev is ignored.
        """
        # Create custom manifest
        custom_manifest = tmp_path / "custom" / "manifest.json"
        custom_manifest.parent.mkdir(parents=True)
        custom_manifest.write_text('{"metadata": {"source": "custom"}}')

        # Create dev manifest
        dev_manifest = tmp_path / "target" / "manifest.json"
        dev_manifest.parent.mkdir(parents=True)
        dev_manifest.write_text('{"metadata": {"source": "dev"}}')

        monkeypatch.chdir(tmp_path)

        finder = ManifestFinder()
        # Even with use_dev=True, explicit_path takes priority
        found_path = finder.find(explicit_path=str(custom_manifest), use_dev=True)

        # MUST find custom manifest, not dev
        assert found_path == str(custom_manifest.absolute())

    def test_simple_mode_fallback_to_target(self, tmp_path, monkeypatch):
        """
        Simple mode: Fallback to ./target/manifest.json when DBT_PROD_MANIFEST_PATH not set

        This allows dbt-meta to work out-of-box after 'dbt compile'.
        Priority 3: ./target/manifest.json (when DBT_PROD_MANIFEST_PATH not set)
        """
        monkeypatch.chdir(tmp_path)
        # Ensure DBT_PROD_MANIFEST_PATH is not set
        monkeypatch.delenv("DBT_PROD_MANIFEST_PATH", raising=False)

        # Create ./target/manifest.json (simple mode)
        target_manifest = tmp_path / "target" / "manifest.json"
        target_manifest.parent.mkdir(parents=True)
        target_manifest.write_text('{"metadata": {"mode": "simple"}}')

        finder = ManifestFinder()
        found_path = finder.find()

        # MUST find ./target/manifest.json
        assert found_path == str(target_manifest.absolute())

    def test_production_prioritized_over_simple_mode(self, tmp_path, monkeypatch):
        """
        Production manifest (DBT_PROD_MANIFEST_PATH) has priority over ./target/

        When both are present, production manifest takes precedence.
        """
        monkeypatch.chdir(tmp_path)

        # Create production manifest
        prod_manifest = tmp_path / "prod" / "manifest.json"
        prod_manifest.parent.mkdir(parents=True)
        prod_manifest.write_text('{"metadata": {"mode": "production"}}')

        # Create ./target/manifest.json (should be ignored)
        target_manifest = tmp_path / "target" / "manifest.json"
        target_manifest.parent.mkdir(parents=True)
        target_manifest.write_text('{"metadata": {"mode": "simple"}}')

        # Set DBT_PROD_MANIFEST_PATH
        monkeypatch.setenv("DBT_PROD_MANIFEST_PATH", str(prod_manifest))

        finder = ManifestFinder()
        found_path = finder.find()

        # MUST find production manifest, not ./target/
        assert found_path == str(prod_manifest.absolute())

    def test_raises_when_no_manifest_found(self, tmp_path, monkeypatch):
        """
        Should raise clear error when no manifest found

        Error message must explain where it searched.
        """
        # Mock home directory to prevent finding ~/dbt-state/manifest.json
        fake_home = tmp_path / "fake_home"
        fake_home.mkdir()
        monkeypatch.setattr(Path, "home", lambda: fake_home)

        # Clear env vars and ensure no manifest exists anywhere
        monkeypatch.delenv("DBT_PROD_MANIFEST_PATH", raising=False)
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

    def test_get_model_by_unique_id(self, prod_manifest, test_model):
        """
        Should retrieve model by unique_id

        Format: model.project.schema__model_name
        Example: model.project.test_schema__test_model
        """
        parser = ManifestParser(str(prod_manifest))

        # Get specific model
        model_name = test_model
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
        Should raise ManifestNotFoundError for non-existent manifest

        Clear error message with path.
        """
        non_existent = tmp_path / "not_found.json"

        with pytest.raises(ManifestNotFoundError) as exc_info:
            parser = ManifestParser(str(non_existent))
            _ = parser.manifest  # Access triggers loading

        assert str(non_existent) in exc_info.value.searched_paths

    def test_invalid_json_raises_error(self, tmp_path):
        """
        Should raise ManifestParseError for invalid JSON

        orjson raises JSONDecodeError, wrap with helpful message.
        """
        invalid_manifest = tmp_path / "invalid.json"
        invalid_manifest.write_text("{ invalid json }")

        parser = ManifestParser(str(invalid_manifest))

        with pytest.raises(ManifestParseError) as exc_info:
            _ = parser.manifest

        assert str(invalid_manifest) in exc_info.value.path

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

    def test_get_model_dependencies(self, prod_manifest, test_model):
        """
        Should extract model dependencies (refs and sources)

        Returns: {"refs": [...], "sources": [...]}
        """
        parser = ManifestParser(str(prod_manifest))

        model_name = test_model
        deps = parser.get_dependencies(model_name)

        assert isinstance(deps, dict)
        assert 'refs' in deps
        assert 'sources' in deps
        assert isinstance(deps['refs'], list)
        assert isinstance(deps['sources'], list)

# ============================================================================
# SECTION 3: Warning System Tests
# ============================================================================


class TestCheckManifestGitMismatch:
    """Test _check_manifest_git_mismatch() warning generation"""

    def test_git_mismatch_warning_when_modified_without_dev_flag(self, mocker):
        """Should warn when model is modified but querying production"""
        mocker.patch('dbt_meta.utils.git.is_modified', return_value=True)

        warnings = _check_manifest_git_mismatch("test_model", use_dev=False)

        assert len(warnings) == 1
        assert warnings[0]['type'] == 'git_mismatch'
        assert warnings[0]['severity'] == 'warning'
        assert 'is modified in git' in warnings[0]['message']
        assert 'suggestion' in warnings[0]
        assert '--dev' in warnings[0]['suggestion']

    def test_dev_without_changes_warning_when_using_dev_for_unchanged_model(self, mocker):
        """Should warn when using --dev flag but model not modified"""
        mocker.patch('dbt_meta.utils.git.is_modified', return_value=False)
        mocker.patch('dbt_meta.utils.git.is_committed_but_not_in_main', return_value=False)

        # Pass dev_manifest_found to avoid dev_manifest_missing warning
        warnings = _check_manifest_git_mismatch(
            "test_model",
            use_dev=True,
            dev_manifest_found="/path/to/manifest.json"
        )

        assert len(warnings) == 1
        assert warnings[0]['type'] == 'dev_without_changes'
        assert warnings[0]['severity'] == 'warning'
        assert 'has no changes in current branch' in warnings[0]['message']
        assert 'Remove --dev flag' in warnings[0]['suggestion']

    def test_dev_manifest_missing_warning(self, mocker):
        """Should warn when using --dev but dev manifest not found"""
        mocker.patch('dbt_meta.utils.git.is_modified', return_value=False)
        mocker.patch('dbt_meta.utils.git.is_committed_but_not_in_main', return_value=False)

        warnings = _check_manifest_git_mismatch(
            "test_model",
            use_dev=True,
            dev_manifest_found=None
        )

        assert len(warnings) == 2  # dev_without_changes + dev_manifest_missing
        error_warnings = [w for w in warnings if w['severity'] == 'error']
        assert len(error_warnings) == 1
        assert error_warnings[0]['type'] == 'dev_manifest_missing'
        assert 'defer run' in error_warnings[0]['suggestion']

    def test_no_warnings_when_git_matches_command(self, mocker):
        """Should return empty list when git status matches command"""
        mocker.patch('dbt_meta.utils.git.is_modified', return_value=False)

        warnings = _check_manifest_git_mismatch("test_model", use_dev=False)

        assert warnings == []

    def test_no_warnings_when_modified_and_using_dev(self, mocker):
        """Should return empty list when model modified and using --dev"""
        mocker.patch('dbt_meta.utils.git.is_modified', return_value=True)

        warnings = _check_manifest_git_mismatch(
            "test_model",
            use_dev=True,
            dev_manifest_found="/path/to/manifest.json"
        )

        assert warnings == []

    def test_new_model_committed_in_feature_branch(self, tmp_path, mocker):
        """Should detect NEW model when committed to feature branch (not in prod manifest).

        Scenario: Model committed to feature branch but not merged to master.
        - Git status: clean (no uncommitted changes)
        - Prod manifest: model NOT present
        - Dev manifest: model present

        Expected: Should trigger 'new_model' error (regardless of git status)
        """
        import json

        from dbt_meta.manifest.parser import ManifestParser

        # Setup manifests
        prod_manifest = tmp_path / ".dbt-state" / "manifest.json"
        dev_manifest = tmp_path / "target" / "manifest.json"

        prod_manifest.parent.mkdir(parents=True)
        dev_manifest.parent.mkdir(parents=True)

        # Production manifest: model NOT present
        prod_manifest.write_text(json.dumps({
            "metadata": {},
            "nodes": {}
        }))

        # Dev manifest: model IS present
        dev_manifest.write_text(json.dumps({
            "metadata": {},
            "nodes": {
                "model.test_project.core_appsflyer__upload_log": {
                    "name": "core_appsflyer__upload_log",
                    "schema": "personal_testuser",
                    "alias": "upload_log",
                    "database": "test-project",
                    "columns": {"col1": {"name": "col1", "data_type": "string"}},
                    "config": {"materialized": "table"}
                }
            }
        }))

        # Mock git: clean (no uncommitted changes, model committed in feature branch)
        # Since modified=False, no warning will be generated (defer fallback scenario)
        mocker.patch('dbt_meta.utils.git.is_modified', return_value=False)

        prod_parser = ManifestParser(str(prod_manifest))
        dev_parser = ManifestParser(str(dev_manifest))

        warnings = _check_manifest_git_mismatch(
            model_name='core_appsflyer__upload_log',
            use_dev=False,
            dev_manifest_found=str(dev_manifest),
            prod_parser=prod_parser,
            dev_parser=dev_parser
        )

        # When file is committed (modified=False), no warning
        # This allows defer workflow fallback to proceed
        assert len(warnings) == 0

    def test_new_model_uncommitted_in_feature_branch(self, tmp_path, mocker):
        """Should warn about NEW model candidate when modified and only in dev.

        Scenario: Model being developed, uncommitted changes.
        - Git status: modified (uncommitted changes)
        - Prod manifest: model NOT present
        - Dev manifest: model present

        Expected: Should warn 'new_model_candidate' but allow fallback to proceed
        """
        import json

        from dbt_meta.manifest.parser import ManifestParser

        # Setup manifests
        prod_manifest = tmp_path / ".dbt-state" / "manifest.json"
        dev_manifest = tmp_path / "target" / "manifest.json"

        prod_manifest.parent.mkdir(parents=True)
        dev_manifest.parent.mkdir(parents=True)

        prod_manifest.write_text(json.dumps({"metadata": {}, "nodes": {}}))
        dev_manifest.write_text(json.dumps({
            "metadata": {},
            "nodes": {
                "model.test_project.core_appsflyer__upload_log": {
                    "name": "core_appsflyer__upload_log",
                    "schema": "personal_testuser",
                    "alias": "upload_log",
                    "database": "test-project",
                    "columns": {"col1": {"name": "col1", "data_type": "string"}},
                    "config": {"materialized": "table"}
                }
            }
        }))

        # Mock git: modified (uncommitted changes)
        mocker.patch('dbt_meta.utils.git.is_modified', return_value=True)

        prod_parser = ManifestParser(str(prod_manifest))
        dev_parser = ManifestParser(str(dev_manifest))

        warnings = _check_manifest_git_mismatch(
            model_name='core_appsflyer__upload_log',
            use_dev=False,
            dev_manifest_found=str(dev_manifest),
            prod_parser=prod_parser,
            dev_parser=dev_parser
        )

        # Should warn but not block fallback
        # Two warnings: new_model_candidate + git_mismatch (both suggest --dev)
        assert len(warnings) == 2

        # First warning: new_model_candidate
        assert warnings[0]['type'] == 'new_model_candidate'
        assert warnings[0]['severity'] == 'warning'
        assert 'exists in dev manifest but NOT in production' in warnings[0]['message']

        # Second warning: git_mismatch (model is modified)
        assert warnings[1]['type'] == 'git_mismatch'
        assert warnings[1]['severity'] == 'warning'
        assert 'is modified in git' in warnings[1]['message']

    def test_file_exists_but_not_compiled_into_manifest(self, tmp_path, mocker):
        """Should detect file that exists but NOT compiled into manifest.

        Scenario: User created model file, but dbt compile failed (SQL error, missing deps, etc.)
        - Git status: modified (file exists in git)
        - Prod manifest: model NOT present
        - Dev manifest: model NOT present (compilation failed!)

        Expected: Should trigger 'file_not_compiled' error with helpful suggestion
        """
        import json

        from dbt_meta.manifest.parser import ManifestParser

        # Setup manifests (model in neither)
        prod_manifest = tmp_path / ".dbt-state" / "manifest.json"
        dev_manifest = tmp_path / "target" / "manifest.json"

        prod_manifest.parent.mkdir(parents=True)
        dev_manifest.parent.mkdir(parents=True)

        prod_manifest.write_text(json.dumps({"metadata": {}, "nodes": {}}))
        dev_manifest.write_text(json.dumps({"metadata": {}, "nodes": {}}))

        # Mock git: file modified/new (detected in git)
        mocker.patch('dbt_meta.utils.git.is_modified', return_value=True)

        prod_parser = ManifestParser(str(prod_manifest))
        dev_parser = ManifestParser(str(dev_manifest))

        warnings = _check_manifest_git_mismatch(
            model_name='stg_appsflyer__in_app_events_postbacks',
            use_dev=False,
            dev_manifest_found=str(dev_manifest),
            prod_parser=prod_parser,
            dev_parser=dev_parser
        )

        # Should detect file_not_compiled error
        assert len(warnings) == 1
        assert warnings[0]['type'] == 'file_not_compiled'
        assert warnings[0]['severity'] == 'error'
        assert 'file detected' in warnings[0]['message'].lower()
        assert 'NOT in manifest' in warnings[0]['message']
        assert 'dbt compile' in warnings[0]['suggestion']
        assert 'SQL syntax error' in warnings[0]['suggestion']  # Mentions possible causes


# ============================================================================
# SECTION 2: Warning Output Format Tests (JSON vs Text)
# ============================================================================


class TestPrintWarnings:
    """Test _print_warnings() output formatting"""

    def test_json_output_format(self, capsys):
        """Should output valid JSON to stderr when json_output=True"""
        warnings = [
            {
                "type": "git_mismatch",
                "severity": "warning",
                "message": "Test message",
                "detail": "Test detail",
                "suggestion": "Test suggestion"
            }
        ]

        _print_warnings(warnings, json_output=True)
        captured = capsys.readouterr()

        # Verify output goes to stderr
        assert captured.out == ""
        assert captured.err != ""

        # Verify valid JSON
        output_json = json.loads(captured.err.strip())
        assert 'warnings' in output_json
        assert len(output_json['warnings']) == 1
        assert output_json['warnings'][0]['type'] == 'git_mismatch'

    def test_text_output_format(self, capsys):
        """Should output colored text to stderr when json_output=False"""
        warnings = [
            {
                "type": "git_mismatch",
                "severity": "warning",
                "message": "Test message",
                "detail": "Test detail",
                "suggestion": "Test suggestion"
            }
        ]

        _print_warnings(warnings, json_output=False)
        captured = capsys.readouterr()

        # Verify output goes to stderr
        assert captured.out == ""
        assert captured.err != ""

        # Verify contains warning emoji and color codes
        assert "WARNING" in captured.err
        assert "\033[" in captured.err  # ANSI color codes
        assert "Test message" in captured.err

    def test_error_severity_uses_red_color(self, capsys):
        """Should use red color (X) for error severity"""
        warnings = [
            {
                "type": "dev_manifest_missing",
                "severity": "error",
                "message": "Dev manifest not found",
                "detail": "Cannot query dev table",
                "suggestion": "Run defer run"
            }
        ]

        _print_warnings(warnings, json_output=False)
        captured = capsys.readouterr()

        # Verify red color code (\033[31m)
        assert "\033[31m" in captured.err

    def test_empty_warnings_produces_no_output(self, capsys):
        """Should produce no output when warnings list is empty"""
        _print_warnings([], json_output=True)
        captured = capsys.readouterr()

        assert captured.out == ""
        assert captured.err == ""

    def test_multiple_warnings_in_json_output(self, capsys):
        """Should output all warnings in single JSON object"""
        warnings = [
            {
                "type": "git_mismatch",
                "severity": "warning",
                "message": "Modified in git",
                "detail": "Detail 1",
                "suggestion": "Suggestion 1"
            },
            {
                "type": "dev_manifest_fallback",
                "severity": "warning",
                "message": "Using dev manifest",
                "detail": "Detail 2",
                "source": "LEVEL 2"
            }
        ]

        _print_warnings(warnings, json_output=True)
        captured = capsys.readouterr()

        output_json = json.loads(captured.err.strip())
        assert len(output_json['warnings']) == 2
        assert output_json['warnings'][0]['type'] == 'git_mismatch'
        assert output_json['warnings'][1]['type'] == 'dev_manifest_fallback'


# ============================================================================
# SECTION 3: Command Integration Tests - json_output Parameter
# ============================================================================


class TestCommandsWithJsonOutput:
    """Test all 10 model commands accept json_output parameter"""

    def test_schema_accepts_json_output_parameter(self, prod_manifest, test_model, mocker):
        """schema() should accept json_output parameter"""
        mocker.patch('dbt_meta.utils.git.is_modified', return_value=False)
        mocker.patch('dbt_meta.commands._print_warnings')

        # Should not raise TypeError
        result = schema(str(prod_manifest), test_model,
                       json_output=True)
        assert result is not None

    def test_columns_accepts_json_output_parameter(self, prod_manifest, test_model, mocker):
        """columns() should accept json_output parameter"""
        mocker.patch('dbt_meta.utils.git.is_modified', return_value=False)
        mocker.patch('dbt_meta.commands._print_warnings')

        result = columns(str(prod_manifest), test_model,
                        json_output=True)
        assert result is not None

    def test_info_accepts_json_output_parameter(self, prod_manifest, test_model, mocker):
        """info() should accept json_output parameter"""
        mocker.patch('dbt_meta.utils.git.is_modified', return_value=False)
        mocker.patch('dbt_meta.commands._print_warnings')

        result = info(str(prod_manifest), test_model,
                     json_output=True)
        assert result is not None

    def test_config_accepts_json_output_parameter(self, prod_manifest, test_model, mocker):
        """config() should accept json_output parameter"""
        mocker.patch('dbt_meta.utils.git.is_modified', return_value=False)
        mocker.patch('dbt_meta.commands._print_warnings')

        result = config(str(prod_manifest), test_model,
                       json_output=True)
        assert result is not None

    def test_deps_accepts_json_output_parameter(self, prod_manifest, test_model, mocker):
        """deps() should accept json_output parameter"""
        mocker.patch('dbt_meta.utils.git.is_modified', return_value=False)
        mocker.patch('dbt_meta.commands._print_warnings')

        result = deps(str(prod_manifest), test_model,
                     json_output=True)
        assert result is not None

    def test_sql_accepts_json_output_parameter(self, prod_manifest, test_model, mocker):
        """sql() should accept json_output parameter"""
        mocker.patch('dbt_meta.utils.git.is_modified', return_value=False)
        mocker.patch('dbt_meta.commands._print_warnings')

        result = sql(str(prod_manifest), test_model,
                    json_output=True)
        assert result is not None

    def test_path_accepts_json_output_parameter(self, prod_manifest, test_model, mocker):
        """path() should accept json_output parameter"""
        mocker.patch('dbt_meta.utils.git.is_modified', return_value=False)
        mocker.patch('dbt_meta.commands._print_warnings')

        result = path(str(prod_manifest), test_model,
                     json_output=True)
        assert result is not None

    def test_docs_accepts_json_output_parameter(self, prod_manifest, test_model, mocker):
        """docs() should accept json_output parameter"""
        mocker.patch('dbt_meta.utils.git.is_modified', return_value=False)
        mocker.patch('dbt_meta.commands._print_warnings')

        result = docs(str(prod_manifest), test_model,
                     json_output=True)
        assert result is not None

    def test_parents_accepts_json_output_parameter(self, prod_manifest, test_model, mocker):
        """parents() should accept json_output parameter"""
        mocker.patch('dbt_meta.utils.git.is_modified', return_value=False)
        mocker.patch('dbt_meta.commands._print_warnings')

        result = parents(str(prod_manifest), test_model,
                        json_output=True)
        assert result is not None

    def test_children_accepts_json_output_parameter(self, prod_manifest, test_model, mocker):
        """children() should accept json_output parameter"""
        mocker.patch('dbt_meta.utils.git.is_modified', return_value=False)
        mocker.patch('dbt_meta.commands._print_warnings')

        result = children(str(prod_manifest), test_model,
                         json_output=True)
        assert result is not None


# ============================================================================
# SECTION 4: Warning System Integration with Commands
# ============================================================================


class TestWarningsWithCommands:
    """Test warnings are properly triggered across all commands"""

    def test_schema_calls_git_check_and_prints_warnings(self, prod_manifest, test_model, mocker):
        """schema() should check git and print warnings"""
        mocker.patch('dbt_meta.utils.git.is_modified', return_value=True)
        mock_print_warnings = mocker.patch('dbt_meta.command_impl.base._print_warnings')

        schema(str(prod_manifest), test_model,
              use_dev=False, json_output=True)

        # Verify _print_warnings was called
        assert mock_print_warnings.called
        # Verify json_output was passed (check both args and kwargs)
        calls = mock_print_warnings.call_args_list
        json_output_passed = any(
            (len(call.args) >= 2 and call.args[1]) or
            call.kwargs.get('json_output')
            for call in calls
        )
        assert json_output_passed

    def test_columns_calls_git_check_and_prints_warnings(self, prod_manifest, test_model, mocker):
        """columns() should check git and print warnings"""
        mocker.patch('dbt_meta.utils.git.is_modified', return_value=True)
        mock_print_warnings = mocker.patch('dbt_meta.command_impl.base._print_warnings')

        columns(str(prod_manifest), test_model,
               use_dev=False, json_output=True)

        assert mock_print_warnings.called

    def test_info_calls_git_check_and_prints_warnings(self, prod_manifest, test_model, mocker):
        """info() should check git and print warnings"""
        mocker.patch('dbt_meta.utils.git.is_modified', return_value=True)
        mock_print_warnings = mocker.patch('dbt_meta.command_impl.base._print_warnings')

        info(str(prod_manifest), test_model,
            use_dev=False, json_output=True)

        assert mock_print_warnings.called

    def test_config_calls_git_check_and_prints_warnings(self, prod_manifest, test_model, mocker):
        """config() should check git and print warnings"""
        mocker.patch('dbt_meta.utils.git.is_modified', return_value=True)
        mock_print_warnings = mocker.patch('dbt_meta.command_impl.base._print_warnings')

        config(str(prod_manifest), test_model,
              use_dev=False, json_output=True)

        assert mock_print_warnings.called


# ============================================================================
# SECTION 5: Fallback Warning Tests
# ============================================================================


class TestFallbackWarnings:
    """Test fallback warnings (dev_manifest_fallback, bigquery_fallback)"""

    def test_dev_manifest_fallback_warning_structure(self, capsys, mocker):
        """Should generate proper fallback warning when using dev manifest"""
        warnings = [
            {
                "type": "dev_manifest_fallback",
                "severity": "warning",
                "message": "Model 'test_model' not found in production manifest",
                "detail": "Using dev manifest (target/manifest.json) as fallback",
                "source": "LEVEL 2"
            }
        ]

        _print_warnings(warnings, json_output=True)
        captured = capsys.readouterr()

        output_json = json.loads(captured.err.strip())
        assert output_json['warnings'][0]['source'] == 'LEVEL 2'
        assert 'dev manifest' in output_json['warnings'][0]['detail']

    def test_bigquery_fallback_warning_structure(self, capsys):
        """Should generate proper fallback warning when using BigQuery"""
        warnings = [
            {
                "type": "bigquery_fallback",
                "severity": "warning",
                "message": "Model 'test_model' not in manifest",
                "detail": "Using BigQuery table: dataset.table",
                "source": "LEVEL 3"
            }
        ]

        _print_warnings(warnings, json_output=True)
        captured = capsys.readouterr()

        output_json = json.loads(captured.err.strip())
        assert output_json['warnings'][0]['source'] == 'LEVEL 3'
        assert 'BigQuery' in output_json['warnings'][0]['detail']


# ============================================================================
# SECTION 6: Warning Structure Validation
# ============================================================================


class TestWarningStructure:
    """Test warning message structure consistency"""

    def test_git_warning_has_required_fields(self, mocker):
        """Git warnings should have type, severity, message, detail, suggestion"""
        mocker.patch('dbt_meta.utils.git.is_modified', return_value=True)

        warnings = _check_manifest_git_mismatch("test_model", use_dev=False)

        assert len(warnings) > 0
        warning = warnings[0]

        # Required fields
        assert 'type' in warning
        assert 'severity' in warning
        assert 'message' in warning
        assert 'detail' in warning
        assert 'suggestion' in warning

        # Type constraints
        assert warning['severity'] in ['warning', 'error', 'info']
        assert isinstance(warning['message'], str)
        assert len(warning['message']) > 0

    def test_fallback_warning_has_source_field(self):
        """Fallback warnings should have source field (LEVEL 2 or LEVEL 3)"""
        warning = {
            "type": "dev_manifest_fallback",
            "severity": "warning",
            "message": "Test",
            "detail": "Test",
            "source": "LEVEL 2"
        }

        assert 'source' in warning
        assert warning['source'] in ['LEVEL 2', 'LEVEL 3']

    def test_warning_type_values_are_valid(self, mocker):
        """Warning type should be one of predefined values"""
        valid_types = [
            'git_mismatch',
            'dev_without_changes',
            'dev_manifest_missing',
            'dev_manifest_fallback',
            'bigquery_fallback'
        ]

        # Test git_mismatch
        mocker.patch('dbt_meta.utils.git.is_modified', return_value=True)
        warnings = _check_manifest_git_mismatch("test", use_dev=False)
        assert warnings[0]['type'] in valid_types

        # Test dev_without_changes
        mocker.patch('dbt_meta.utils.git.is_modified', return_value=False)
        warnings = _check_manifest_git_mismatch("test", use_dev=True)
        assert warnings[0]['type'] in valid_types


# ============================================================================
# SECTION 7: Edge Cases for Warning System
# ============================================================================


class TestWarningEdgeCases:
    """Test edge cases in warning system"""

    def test_very_long_model_name_in_warning(self, mocker):
        """Should handle very long model names gracefully"""
        long_name = "a" * 200
        mocker.patch('dbt_meta.utils.git.is_modified', return_value=True)

        warnings = _check_manifest_git_mismatch(long_name, use_dev=False)

        assert len(warnings) > 0
        assert long_name in warnings[0]['message']

    def test_special_characters_in_model_name_warning(self, mocker):
        """Should handle special characters in model names"""
        special_name = "model__with-dash_and.dot"
        mocker.patch('dbt_meta.utils.git.is_modified', return_value=True)

        warnings = _check_manifest_git_mismatch(special_name, use_dev=False)

        assert len(warnings) > 0
        assert special_name in warnings[0]['message']

    def test_multiple_warnings_different_types(self, mocker):
        """Should handle multiple warnings of different types"""
        mocker.patch('dbt_meta.utils.git.is_modified', return_value=False)

        warnings = _check_manifest_git_mismatch(
            "test",
            use_dev=True,
            dev_manifest_found=None
        )

        # Should have both dev_without_changes and dev_manifest_missing
        assert len(warnings) == 2
        types = [w['type'] for w in warnings]
        assert 'dev_without_changes' in types
        assert 'dev_manifest_missing' in types

    def test_json_output_with_unicode_characters(self, capsys):
        """Should handle unicode characters in warnings"""
        warnings = [
            {
                "type": "git_mismatch",
                "severity": "warning",
                "message": "Model '测试模型' modified",
                "detail": "Unicode detail: 日本語",
                "suggestion": "Use --dev flag"
            }
        ]

        _print_warnings(warnings, json_output=True)
        captured = capsys.readouterr()

        # Should not raise encoding errors
        output_json = json.loads(captured.err.strip())
        assert '测试模型' in output_json['warnings'][0]['message']

    def test_warning_with_none_dev_manifest(self, mocker):
        """Should handle None dev_manifest_found parameter"""
        mocker.patch('dbt_meta.utils.git.is_modified', return_value=True)

        # Should not raise AttributeError
        warnings = _check_manifest_git_mismatch(
            "test",
            use_dev=True,
            dev_manifest_found=None
        )

        assert isinstance(warnings, list)

    def test_print_warnings_with_missing_optional_fields(self, capsys):
        """Should handle warnings with missing optional fields"""
        warnings = [
            {
                "type": "git_mismatch",
                "severity": "warning",
                "message": "Test message"
                # Missing detail and suggestion
            }
        ]

        # Should not raise KeyError
        _print_warnings(warnings, json_output=False)
        captured = capsys.readouterr()

        assert "Test message" in captured.err


class TestCombinedFlags:
    """Test combined short flags support (-dj, -ajd, etc)"""

    def test_combined_dev_and_json_flags(self, tmp_path, test_model, mocker):
        """Verify -dj equals -d -j"""
        # Create dev manifest in temp directory
        dev_manifest = tmp_path / "target" / "manifest.json"
        dev_manifest.parent.mkdir(parents=True)
        dev_manifest.write_text('{"metadata": {"dbt_version": "1.5.0"}, "nodes": {}}')

        mocker.patch.dict(os.environ, {"DBT_DEV_MANIFEST_PATH": str(dev_manifest)})

        # Mock schema command to verify both flags are processed
        mock_schema = mocker.patch("dbt_meta.commands.schema")
        mock_schema.return_value = {"database": "test", "schema": "personal_user", "table": "model", "full_name": "test.personal_user.model"}

        from typer.testing import CliRunner

        from dbt_meta.cli import app

        runner = CliRunner()

        # Test -dj (combined)
        result = runner.invoke(app, ["schema", "-dj", test_model])

        assert result.exit_code == 0, f"Command failed: {result.stdout}"

        # Verify output is JSON
        assert "{" in result.stdout

    def test_combined_all_json_dev_flags(self, tmp_path, test_model, mocker):
        """Verify -ajd equals -a -j -d"""
        # Create dev manifest in temp directory
        dev_manifest = tmp_path / "target" / "manifest.json"
        dev_manifest.parent.mkdir(parents=True)
        dev_manifest.write_text('{"metadata": {"dbt_version": "1.5.0"}, "nodes": {}}')

        mocker.patch.dict(os.environ, {"DBT_DEV_MANIFEST_PATH": str(dev_manifest)})

        mock_parents = mocker.patch("dbt_meta.commands.parents")
        mock_parents.return_value = ["parent1", "parent2"]

        from typer.testing import CliRunner

        from dbt_meta.cli import app

        runner = CliRunner()

        # Test -ajd (combined)
        result = runner.invoke(app, ["parents", "-ajd", test_model])
        assert result.exit_code == 0

        # Verify output is JSON
        assert "[" in result.stdout or "{" in result.stdout

    def test_combined_json_and_manifest_flags(self, tmp_path, test_model, mocker):
        """Verify -j --manifest PATH works (note: -m is now --modified in list command)"""
        # Create temporary manifest
        manifest_path = tmp_path / "custom.json"
        manifest_path.write_text('{"metadata": {"dbt_version": "1.5.0"}, "nodes": {}}')

        mock_schema = mocker.patch("dbt_meta.commands.schema")
        mock_schema.return_value = {"database": "test", "schema": "analytics", "table": "model", "full_name": "test.analytics.model"}

        from typer.testing import CliRunner

        from dbt_meta.cli import app

        runner = CliRunner()

        # Test -j --manifest PATH (note: -m no longer short for --manifest)
        result = runner.invoke(app, ["schema", "-j", "--manifest", str(manifest_path), test_model])
        assert result.exit_code == 0

        # Verify output is JSON
        assert "{" in result.stdout

    def test_combined_flags_order_independent(self, tmp_path, test_model, mocker):
        """Verify flag order doesn't matter: -dj = -jd"""
        # Create dev manifest in temp directory
        dev_manifest = tmp_path / "target" / "manifest.json"
        dev_manifest.parent.mkdir(parents=True)
        dev_manifest.write_text('{"metadata": {"dbt_version": "1.5.0"}, "nodes": {}}')

        mocker.patch.dict(os.environ, {"DBT_DEV_MANIFEST_PATH": str(dev_manifest)})

        mock_columns = mocker.patch("dbt_meta.commands.columns")
        mock_columns.return_value = [{"name": "id", "data_type": "INTEGER"}]

        from typer.testing import CliRunner

        from dbt_meta.cli import app

        runner = CliRunner()

        # Test -dj
        result1 = runner.invoke(app, ["columns", "-dj", test_model])
        output1 = result1.stdout

        # Test -jd (reversed order)
        result2 = runner.invoke(app, ["columns", "-jd", test_model])
        output2 = result2.stdout

        # Both should succeed and produce JSON output
        assert result1.exit_code == 0
        assert result2.exit_code == 0
        assert "{" in output1 or "[" in output1
        assert "{" in output2 or "[" in output2


# ============================================================================
# SECTION 9: Message Formatting Tests (v0.1.4)
# ============================================================================


class TestGitWarningFormatting:
    """Test lowercase 'is modified' in git warnings (v0.1.4)."""

    def test_git_mismatch_warning_uses_lowercase_is(self, mocker):
        """Should use lowercase 'is modified' not 'IS modified'."""
        mocker.patch('dbt_meta.utils.git.is_modified', return_value=True)

        warnings = _check_manifest_git_mismatch("test_model", use_dev=False)

        assert len(warnings) == 1
        assert warnings[0]['type'] == 'git_mismatch'
        # Check for lowercase 'is', not uppercase 'IS'
        assert 'is modified in git' in warnings[0]['message']
        assert 'IS modified' not in warnings[0]['message']

    def test_git_warning_suggestion_includes_dev_flag(self, mocker):
        """Git mismatch warning should suggest --dev flag."""
        mocker.patch('dbt_meta.utils.git.is_modified', return_value=True)

        warnings = _check_manifest_git_mismatch("test_model", use_dev=False)

        assert warnings[0]['suggestion']
        assert '--dev' in warnings[0]['suggestion']


class TestBigQueryMessageFormatting:
    """Test prod/dev table distinction in BigQuery fallback messages (v0.1.4)."""

    @pytest.fixture
    def mock_config(self):
        """Mock Config with BigQuery enabled."""
        from unittest.mock import Mock
        config = Mock()
        config.fallback_bigquery_enabled = True
        config.prod_table_name_strategy = "alias_or_name"
        config.prod_schema_source = "config_or_model"
        config.dev_schema = "personal_test_user"
        return config

    def test_prod_table_message_shows_prod_table(self, mock_config, capsys):
        """BigQuery fallback for prod should show 'prod table'."""
        from io import StringIO
        from unittest.mock import patch
        from dbt_meta.command_impl.columns import ColumnsCommand
        from dbt_meta.utils.model_state import ModelState

        cmd = ColumnsCommand(
            config=mock_config,
            manifest_path='/fake/manifest.json',
            model_name='test_model',
            use_dev=False
        )

        # Capture stderr
        with patch('sys.stderr', new_callable=StringIO) as mock_stderr:
            cmd._print_result_message(
                state=ModelState.MODIFIED_UNCOMMITTED,
                column_count=5,
                table='core_client.test_table',
                is_dev_table=False
            )

            output = mock_stderr.getvalue()

        # Should show "prod table"
        assert 'prod table' in output
        assert 'BigQuery (prod table: core_client.test_table)' in output

        # Should NOT show "dev table"
        assert 'dev table' not in output

    def test_dev_table_message_shows_dev_table(self, mock_config, capsys):
        """BigQuery fallback for dev should show 'dev table'."""
        from io import StringIO
        from unittest.mock import patch
        from dbt_meta.command_impl.columns import ColumnsCommand
        from dbt_meta.utils.model_state import ModelState

        cmd = ColumnsCommand(
            config=mock_config,
            manifest_path='/fake/manifest.json',
            model_name='test_model',
            use_dev=True
        )

        with patch('sys.stderr', new_callable=StringIO) as mock_stderr:
            cmd._print_result_message(
                state=ModelState.NEW_UNCOMMITTED,
                column_count=3,
                table='personal_test_user.test_model',
                is_dev_table=True
            )

            output = mock_stderr.getvalue()

        # Should show "dev table"
        assert 'dev table' in output
        assert 'BigQuery (dev table: personal_test_user.test_model)' in output

        # Should NOT show "prod table"
        assert 'prod table' not in output

    def test_prod_table_no_using_dev_version_warning(self, mock_config, capsys):
        """Production table should NOT show 'Using dev version' warning."""
        from io import StringIO
        from unittest.mock import patch
        from dbt_meta.command_impl.columns import ColumnsCommand
        from dbt_meta.utils.model_state import ModelState

        cmd = ColumnsCommand(
            config=mock_config,
            manifest_path='/fake/manifest.json',
            model_name='test_model',
            use_dev=False
        )

        with patch('sys.stderr', new_callable=StringIO) as mock_stderr:
            cmd._print_result_message(
                state=ModelState.MODIFIED_UNCOMMITTED,
                column_count=5,
                table='core_client.test_table',
                is_dev_table=False  # Production table
            )

            output = mock_stderr.getvalue()

        # Should NOT show "Using dev version" for production
        assert 'Using dev version' not in output

    def test_dev_table_shows_using_dev_version_for_modified(self, mock_config, capsys):
        """Dev table should show 'Using dev version' for MODIFIED_UNCOMMITTED."""
        from io import StringIO
        from unittest.mock import patch
        from dbt_meta.command_impl.columns import ColumnsCommand
        from dbt_meta.utils.model_state import ModelState

        cmd = ColumnsCommand(
            config=mock_config,
            manifest_path='/fake/manifest.json',
            model_name='test_model',
            use_dev=True
        )

        with patch('sys.stderr', new_callable=StringIO) as mock_stderr:
            cmd._print_result_message(
                state=ModelState.MODIFIED_UNCOMMITTED,
                column_count=5,
                table='personal_test_user.test_model',
                is_dev_table=True  # Dev table
            )

            output = mock_stderr.getvalue()

        # Should show "Using dev version" for dev table
        assert 'Using dev version' in output

    def test_dev_table_no_warning_for_new_models(self, mock_config, capsys):
        """Dev table should NOT show 'Using dev version' for NEW models."""
        from io import StringIO
        from unittest.mock import patch
        from dbt_meta.command_impl.columns import ColumnsCommand
        from dbt_meta.utils.model_state import ModelState

        cmd = ColumnsCommand(
            config=mock_config,
            manifest_path='/fake/manifest.json',
            model_name='test_model',
            use_dev=True
        )

        with patch('sys.stderr', new_callable=StringIO) as mock_stderr:
            cmd._print_result_message(
                state=ModelState.NEW_UNCOMMITTED,
                column_count=3,
                table='personal_test_user.new_model',
                is_dev_table=True
            )

            output = mock_stderr.getvalue()

        # NEW models don't need "Using dev version" warning
        # (no committed version exists)
        assert 'Using dev version' not in output
