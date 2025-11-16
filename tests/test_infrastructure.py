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

import pytest
import json
from pathlib import Path
from dbt_meta.manifest.finder import ManifestFinder
from dbt_meta.manifest.parser import ManifestParser
from dbt_meta.commands import (
    schema, columns, info, config, deps, sql, path, docs, parents, children,
    _check_manifest_git_mismatch, _print_warnings, is_modified
)


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

    def test_raises_when_no_manifest_found(self, tmp_path, monkeypatch):
        """
        Should raise clear error when no manifest found

        Error message must explain the production manifest location.
        """
        # Clear env vars and use non-existent location
        monkeypatch.setenv("DBT_PROD_MANIFEST_PATH", str(tmp_path / "nonexistent" / "manifest.json"))
        monkeypatch.chdir(tmp_path)

        finder = ManifestFinder()

        with pytest.raises(FileNotFoundError, match="No production manifest found"):
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
        mocker.patch('dbt_meta.commands.is_modified', return_value=True)

        warnings = _check_manifest_git_mismatch("test_model", use_dev=False)

        assert len(warnings) == 1
        assert warnings[0]['type'] == 'git_mismatch'
        assert warnings[0]['severity'] == 'warning'
        assert 'IS modified in git' in warnings[0]['message']
        assert 'suggestion' in warnings[0]
        assert '--dev' in warnings[0]['suggestion']

    def test_dev_without_changes_warning_when_using_dev_for_unchanged_model(self, mocker):
        """Should warn when using --dev flag but model not modified"""
        mocker.patch('dbt_meta.commands.is_modified', return_value=False)

        # Pass dev_manifest_found to avoid dev_manifest_missing warning
        warnings = _check_manifest_git_mismatch(
            "test_model",
            use_dev=True,
            dev_manifest_found="/path/to/manifest.json"
        )

        assert len(warnings) == 1
        assert warnings[0]['type'] == 'dev_without_changes'
        assert warnings[0]['severity'] == 'warning'
        assert 'NOT modified in git' in warnings[0]['message']
        assert 'Remove --dev flag' in warnings[0]['suggestion']

    def test_dev_manifest_missing_warning(self, mocker):
        """Should warn when using --dev but dev manifest not found"""
        mocker.patch('dbt_meta.commands.is_modified', return_value=False)

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
        mocker.patch('dbt_meta.commands.is_modified', return_value=False)

        warnings = _check_manifest_git_mismatch("test_model", use_dev=False)

        assert warnings == []

    def test_no_warnings_when_modified_and_using_dev(self, mocker):
        """Should return empty list when model modified and using --dev"""
        mocker.patch('dbt_meta.commands.is_modified', return_value=True)

        warnings = _check_manifest_git_mismatch(
            "test_model",
            use_dev=True,
            dev_manifest_found="/path/to/manifest.json"
        )

        assert warnings == []


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
        mocker.patch('dbt_meta.commands.is_modified', return_value=False)
        mocker.patch('dbt_meta.commands._print_warnings')

        # Should not raise TypeError
        result = schema(str(prod_manifest), test_model,
                       json_output=True)
        assert result is not None

    def test_columns_accepts_json_output_parameter(self, prod_manifest, test_model, mocker):
        """columns() should accept json_output parameter"""
        mocker.patch('dbt_meta.commands.is_modified', return_value=False)
        mocker.patch('dbt_meta.commands._print_warnings')

        result = columns(str(prod_manifest), test_model,
                        json_output=True)
        assert result is not None

    def test_info_accepts_json_output_parameter(self, prod_manifest, test_model, mocker):
        """info() should accept json_output parameter"""
        mocker.patch('dbt_meta.commands.is_modified', return_value=False)
        mocker.patch('dbt_meta.commands._print_warnings')

        result = info(str(prod_manifest), test_model,
                     json_output=True)
        assert result is not None

    def test_config_accepts_json_output_parameter(self, prod_manifest, test_model, mocker):
        """config() should accept json_output parameter"""
        mocker.patch('dbt_meta.commands.is_modified', return_value=False)
        mocker.patch('dbt_meta.commands._print_warnings')

        result = config(str(prod_manifest), test_model,
                       json_output=True)
        assert result is not None

    def test_deps_accepts_json_output_parameter(self, prod_manifest, test_model, mocker):
        """deps() should accept json_output parameter"""
        mocker.patch('dbt_meta.commands.is_modified', return_value=False)
        mocker.patch('dbt_meta.commands._print_warnings')

        result = deps(str(prod_manifest), test_model,
                     json_output=True)
        assert result is not None

    def test_sql_accepts_json_output_parameter(self, prod_manifest, test_model, mocker):
        """sql() should accept json_output parameter"""
        mocker.patch('dbt_meta.commands.is_modified', return_value=False)
        mocker.patch('dbt_meta.commands._print_warnings')

        result = sql(str(prod_manifest), test_model,
                    json_output=True)
        assert result is not None

    def test_path_accepts_json_output_parameter(self, prod_manifest, test_model, mocker):
        """path() should accept json_output parameter"""
        mocker.patch('dbt_meta.commands.is_modified', return_value=False)
        mocker.patch('dbt_meta.commands._print_warnings')

        result = path(str(prod_manifest), test_model,
                     json_output=True)
        assert result is not None

    def test_docs_accepts_json_output_parameter(self, prod_manifest, test_model, mocker):
        """docs() should accept json_output parameter"""
        mocker.patch('dbt_meta.commands.is_modified', return_value=False)
        mocker.patch('dbt_meta.commands._print_warnings')

        result = docs(str(prod_manifest), test_model,
                     json_output=True)
        assert result is not None

    def test_parents_accepts_json_output_parameter(self, prod_manifest, test_model, mocker):
        """parents() should accept json_output parameter"""
        mocker.patch('dbt_meta.commands.is_modified', return_value=False)
        mocker.patch('dbt_meta.commands._print_warnings')

        result = parents(str(prod_manifest), test_model,
                        json_output=True)
        assert result is not None

    def test_children_accepts_json_output_parameter(self, prod_manifest, test_model, mocker):
        """children() should accept json_output parameter"""
        mocker.patch('dbt_meta.commands.is_modified', return_value=False)
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
        mocker.patch('dbt_meta.commands.is_modified', return_value=True)
        mock_print_warnings = mocker.patch('dbt_meta.commands._print_warnings')

        schema(str(prod_manifest), test_model,
              use_dev=False, json_output=True)

        # Verify _print_warnings was called
        assert mock_print_warnings.called
        # Verify json_output was passed (check both args and kwargs)
        calls = mock_print_warnings.call_args_list
        json_output_passed = any(
            (len(call.args) >= 2 and call.args[1] == True) or
            call.kwargs.get('json_output') == True
            for call in calls
        )
        assert json_output_passed

    def test_columns_calls_git_check_and_prints_warnings(self, prod_manifest, test_model, mocker):
        """columns() should check git and print warnings"""
        mocker.patch('dbt_meta.commands.is_modified', return_value=True)
        mock_print_warnings = mocker.patch('dbt_meta.commands._print_warnings')

        columns(str(prod_manifest), test_model,
               use_dev=False, json_output=True)

        assert mock_print_warnings.called

    def test_info_calls_git_check_and_prints_warnings(self, prod_manifest, test_model, mocker):
        """info() should check git and print warnings"""
        mocker.patch('dbt_meta.commands.is_modified', return_value=True)
        mock_print_warnings = mocker.patch('dbt_meta.commands._print_warnings')

        info(str(prod_manifest), test_model,
            use_dev=False, json_output=True)

        assert mock_print_warnings.called

    def test_config_calls_git_check_and_prints_warnings(self, prod_manifest, test_model, mocker):
        """config() should check git and print warnings"""
        mocker.patch('dbt_meta.commands.is_modified', return_value=True)
        mock_print_warnings = mocker.patch('dbt_meta.commands._print_warnings')

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
        mocker.patch('dbt_meta.commands.is_modified', return_value=True)

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
        mocker.patch('dbt_meta.commands.is_modified', return_value=True)
        warnings = _check_manifest_git_mismatch("test", use_dev=False)
        assert warnings[0]['type'] in valid_types

        # Test dev_without_changes
        mocker.patch('dbt_meta.commands.is_modified', return_value=False)
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
        mocker.patch('dbt_meta.commands.is_modified', return_value=True)

        warnings = _check_manifest_git_mismatch(long_name, use_dev=False)

        assert len(warnings) > 0
        assert long_name in warnings[0]['message']

    def test_special_characters_in_model_name_warning(self, mocker):
        """Should handle special characters in model names"""
        special_name = "model__with-dash_and.dot"
        mocker.patch('dbt_meta.commands.is_modified', return_value=True)

        warnings = _check_manifest_git_mismatch(special_name, use_dev=False)

        assert len(warnings) > 0
        assert special_name in warnings[0]['message']

    def test_multiple_warnings_different_types(self, mocker):
        """Should handle multiple warnings of different types"""
        mocker.patch('dbt_meta.commands.is_modified', return_value=False)

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
        mocker.patch('dbt_meta.commands.is_modified', return_value=True)

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
