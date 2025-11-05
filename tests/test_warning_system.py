"""
Tests for Warning System - Machine-readable JSON warnings

This module tests the intelligent warning system introduced in v0.2.1:
- Git change detection warnings (git_mismatch, dev_without_changes, dev_manifest_missing)
- Fallback warnings (dev_manifest_fallback, bigquery_fallback)
- JSON vs text output formatting
- Warning structure consistency
- json_output parameter in all commands

Coverage:
- All 10 model commands with warnings
- All warning types
- JSON and text output formats
- Warning message structure validation
"""

import os
import pytest
import json
import sys
from io import StringIO
from pathlib import Path
from unittest.mock import patch, MagicMock, call
from dbt_meta.commands import (
    schema, columns, info, config, deps, sql, path, docs, parents, children,
    _check_manifest_git_mismatch, _print_warnings, is_modified
)


# ============================================================================
# SECTION 1: Git Warning Generation Tests
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
        assert "⚠️" in captured.err or "WARNING" in captured.err
        assert "\033[" in captured.err  # ANSI color codes
        assert "Test message" in captured.err

    def test_error_severity_uses_red_color(self, capsys):
        """Should use red color (❌) for error severity"""
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

        # Verify red color code (\033[31m) or error emoji (❌)
        assert "\033[31m" in captured.err or "❌" in captured.err

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

    def test_schema_accepts_json_output_parameter(self, prod_manifest, mocker):
        """schema() should accept json_output parameter"""
        mocker.patch('dbt_meta.commands.is_modified', return_value=False)
        mocker.patch('dbt_meta.commands._print_warnings')

        # Should not raise TypeError
        result = schema(str(prod_manifest), "core_client__client_profiles_events",
                       json_output=True)
        assert result is not None

    def test_columns_accepts_json_output_parameter(self, prod_manifest, mocker):
        """columns() should accept json_output parameter"""
        mocker.patch('dbt_meta.commands.is_modified', return_value=False)
        mocker.patch('dbt_meta.commands._print_warnings')

        result = columns(str(prod_manifest), "core_client__client_profiles_events",
                        json_output=True)
        assert result is not None

    def test_info_accepts_json_output_parameter(self, prod_manifest, mocker):
        """info() should accept json_output parameter"""
        mocker.patch('dbt_meta.commands.is_modified', return_value=False)
        mocker.patch('dbt_meta.commands._print_warnings')

        result = info(str(prod_manifest), "core_client__client_profiles_events",
                     json_output=True)
        assert result is not None

    def test_config_accepts_json_output_parameter(self, prod_manifest, mocker):
        """config() should accept json_output parameter"""
        mocker.patch('dbt_meta.commands.is_modified', return_value=False)
        mocker.patch('dbt_meta.commands._print_warnings')

        result = config(str(prod_manifest), "core_client__client_profiles_events",
                       json_output=True)
        assert result is not None

    def test_deps_accepts_json_output_parameter(self, prod_manifest, mocker):
        """deps() should accept json_output parameter"""
        mocker.patch('dbt_meta.commands.is_modified', return_value=False)
        mocker.patch('dbt_meta.commands._print_warnings')

        result = deps(str(prod_manifest), "core_client__client_profiles_events",
                     json_output=True)
        assert result is not None

    def test_sql_accepts_json_output_parameter(self, prod_manifest, mocker):
        """sql() should accept json_output parameter"""
        mocker.patch('dbt_meta.commands.is_modified', return_value=False)
        mocker.patch('dbt_meta.commands._print_warnings')

        result = sql(str(prod_manifest), "core_client__client_profiles_events",
                    json_output=True)
        assert result is not None

    def test_path_accepts_json_output_parameter(self, prod_manifest, mocker):
        """path() should accept json_output parameter"""
        mocker.patch('dbt_meta.commands.is_modified', return_value=False)
        mocker.patch('dbt_meta.commands._print_warnings')

        result = path(str(prod_manifest), "core_client__client_profiles_events",
                     json_output=True)
        assert result is not None

    def test_docs_accepts_json_output_parameter(self, prod_manifest, mocker):
        """docs() should accept json_output parameter"""
        mocker.patch('dbt_meta.commands.is_modified', return_value=False)
        mocker.patch('dbt_meta.commands._print_warnings')

        result = docs(str(prod_manifest), "core_client__client_profiles_events",
                     json_output=True)
        assert result is not None

    def test_parents_accepts_json_output_parameter(self, prod_manifest, mocker):
        """parents() should accept json_output parameter"""
        mocker.patch('dbt_meta.commands.is_modified', return_value=False)
        mocker.patch('dbt_meta.commands._print_warnings')

        result = parents(str(prod_manifest), "core_client__client_profiles_events",
                        json_output=True)
        assert result is not None

    def test_children_accepts_json_output_parameter(self, prod_manifest, mocker):
        """children() should accept json_output parameter"""
        mocker.patch('dbt_meta.commands.is_modified', return_value=False)
        mocker.patch('dbt_meta.commands._print_warnings')

        result = children(str(prod_manifest), "core_client__client_profiles_events",
                         json_output=True)
        assert result is not None


# ============================================================================
# SECTION 4: Warning System Integration with Commands
# ============================================================================


class TestWarningsWithCommands:
    """Test warnings are properly triggered across all commands"""

    def test_schema_calls_git_check_and_prints_warnings(self, prod_manifest, mocker):
        """schema() should check git and print warnings"""
        mocker.patch('dbt_meta.commands.is_modified', return_value=True)
        mock_print_warnings = mocker.patch('dbt_meta.commands._print_warnings')

        schema(str(prod_manifest), "core_client__client_profiles_events",
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

    def test_columns_calls_git_check_and_prints_warnings(self, prod_manifest, mocker):
        """columns() should check git and print warnings"""
        mocker.patch('dbt_meta.commands.is_modified', return_value=True)
        mock_print_warnings = mocker.patch('dbt_meta.commands._print_warnings')

        columns(str(prod_manifest), "core_client__client_profiles_events",
               use_dev=False, json_output=True)

        assert mock_print_warnings.called

    def test_info_calls_git_check_and_prints_warnings(self, prod_manifest, mocker):
        """info() should check git and print warnings"""
        mocker.patch('dbt_meta.commands.is_modified', return_value=True)
        mock_print_warnings = mocker.patch('dbt_meta.commands._print_warnings')

        info(str(prod_manifest), "core_client__client_profiles_events",
            use_dev=False, json_output=True)

        assert mock_print_warnings.called

    def test_config_calls_git_check_and_prints_warnings(self, prod_manifest, mocker):
        """config() should check git and print warnings"""
        mocker.patch('dbt_meta.commands.is_modified', return_value=True)
        mock_print_warnings = mocker.patch('dbt_meta.commands._print_warnings')

        config(str(prod_manifest), "core_client__client_profiles_events",
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
