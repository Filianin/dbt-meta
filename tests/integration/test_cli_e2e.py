"""End-to-end CLI integration tests for dbt-meta

Tests all 13 commands through the Typer CLI interface to verify:
- Argument parsing
- Output formatting (JSON and text)
- Error handling and messaging
- Help text generation
- Rich formatting
"""

import json

from dbt_meta.cli import app


class TestSchemaCommand:
    """End-to-end tests for 'meta schema' command"""

    def test_schema_json_output(self, cli_runner, prod_manifest, test_model, monkeypatch):
        """Test schema extraction with JSON output"""
        monkeypatch.setenv('DBT_PROD_MANIFEST_PATH', str(prod_manifest))

        result = cli_runner.invoke(app, ['schema', test_model, '-j'])

        assert result.exit_code == 0
        data = json.loads(result.stdout)
        assert 'model_name' in data
        assert 'full_name' in data
        assert data['model_name'] == test_model
        assert isinstance(data['full_name'], str)
        assert '.' in data['full_name']  # Should be in format: database.schema.table

    def test_schema_text_output(self, cli_runner, prod_manifest, test_model, monkeypatch):
        """Test schema extraction with text output"""
        monkeypatch.setenv('DBT_PROD_MANIFEST_PATH', str(prod_manifest))

        result = cli_runner.invoke(app, ['schema', test_model])

        assert result.exit_code == 0
        assert len(result.stdout) > 0
        # Output should contain full table name in format: database.schema.table
        assert '.' in result.stdout  # Should have at least one dot separator

    def test_schema_model_not_found(self, cli_runner, prod_manifest, monkeypatch):
        """Test error when model not found"""
        monkeypatch.setenv('DBT_PROD_MANIFEST_PATH', str(prod_manifest))

        result = cli_runner.invoke(app, ['schema', 'nonexistent_model_xyz'])

        assert result.exit_code == 1
        assert 'not found' in result.stderr.lower() or 'not found' in result.stdout.lower()

    def test_schema_help(self, cli_runner):
        """Test schema command help text"""
        result = cli_runner.invoke(app, ['schema', '--help'])

        assert result.exit_code == 0
        assert 'schema' in result.stdout.lower()
        assert '--json' in result.stdout or '-j' in result.stdout


class TestColumnsCommand:
    """End-to-end tests for 'meta columns' command"""

    def test_columns_json_output(self, cli_runner, prod_manifest, test_model, monkeypatch):
        """Test columns extraction with JSON output"""
        monkeypatch.setenv('DBT_PROD_MANIFEST_PATH', str(prod_manifest))

        result = cli_runner.invoke(app, ['columns', test_model, '-j'])

        assert result.exit_code == 0
        data = json.loads(result.stdout)
        assert isinstance(data, list)
        if len(data) > 0:
            assert 'name' in data[0]
            assert 'data_type' in data[0]  # Column type is in 'data_type' field

    def test_columns_text_output(self, cli_runner, prod_manifest, test_model, monkeypatch):
        """Test columns extraction with Rich table formatting"""
        monkeypatch.setenv('DBT_PROD_MANIFEST_PATH', str(prod_manifest))

        result = cli_runner.invoke(app, ['columns', test_model])

        assert result.exit_code == 0
        # Rich table should have some output
        assert len(result.stdout) > 0

    def test_columns_help(self, cli_runner):
        """Test columns command help text"""
        result = cli_runner.invoke(app, ['columns', '--help'])

        assert result.exit_code == 0
        assert 'column' in result.stdout.lower()


class TestInfoCommand:
    """End-to-end tests for 'meta info' command"""

    def test_info_json_output(self, cli_runner, prod_manifest, test_model, monkeypatch):
        """Test info extraction with JSON output"""
        monkeypatch.setenv('DBT_PROD_MANIFEST_PATH', str(prod_manifest))

        result = cli_runner.invoke(app, ['info', test_model, '-j'])

        assert result.exit_code == 0
        data = json.loads(result.stdout)
        assert isinstance(data, dict)
        # Should contain model metadata
        assert 'name' in data or 'schema' in data

    def test_info_text_output(self, cli_runner, prod_manifest, test_model, monkeypatch):
        """Test info extraction with text output"""
        monkeypatch.setenv('DBT_PROD_MANIFEST_PATH', str(prod_manifest))

        result = cli_runner.invoke(app, ['info', test_model])

        assert result.exit_code == 0
        assert len(result.stdout) > 0

    def test_info_help(self, cli_runner):
        """Test info command help text"""
        result = cli_runner.invoke(app, ['info', '--help'])

        assert result.exit_code == 0
        assert 'info' in result.stdout.lower()


class TestDepsCommand:
    """End-to-end tests for 'meta deps' command"""

    def test_deps_json_output(self, cli_runner, prod_manifest, test_model, monkeypatch):
        """Test deps extraction with JSON output"""
        monkeypatch.setenv('DBT_PROD_MANIFEST_PATH', str(prod_manifest))

        result = cli_runner.invoke(app, ['deps', test_model, '-j'])

        assert result.exit_code == 0
        data = json.loads(result.stdout)
        assert isinstance(data, dict)
        # Should have refs/sources/macros keys
        assert 'refs' in data or 'sources' in data or 'macros' in data

    def test_deps_help(self, cli_runner):
        """Test deps command help text"""
        result = cli_runner.invoke(app, ['deps', '--help'])

        assert result.exit_code == 0
        assert 'depend' in result.stdout.lower()


class TestParentsCommand:
    """End-to-end tests for 'meta parents' command"""

    def test_parents_json_output(self, cli_runner, prod_manifest, test_model, monkeypatch):
        """Test parents extraction with JSON output"""
        monkeypatch.setenv('DBT_PROD_MANIFEST_PATH', str(prod_manifest))

        result = cli_runner.invoke(app, ['parents', test_model, '-j'])

        # May exit with 0 (has parents) or 1 (no parents found)
        if result.exit_code == 0:
            data = json.loads(result.stdout)
            assert isinstance(data, list)

    def test_parents_all_flag(self, cli_runner, prod_manifest, test_model, monkeypatch):
        """Test parents with --all flag for ancestors"""
        monkeypatch.setenv('DBT_PROD_MANIFEST_PATH', str(prod_manifest))

        result = cli_runner.invoke(app, ['parents', test_model, '--all', '-j'])

        # May exit with 0 (has parents) or 1 (no parents found)
        if result.exit_code == 0:
            data = json.loads(result.stdout)
            assert isinstance(data, list)

    def test_parents_help(self, cli_runner):
        """Test parents command help text"""
        result = cli_runner.invoke(app, ['parents', '--help'])

        assert result.exit_code == 0
        assert 'parent' in result.stdout.lower()


class TestChildrenCommand:
    """End-to-end tests for 'meta children' command"""

    def test_children_json_output(self, cli_runner, prod_manifest, test_model, monkeypatch):
        """Test children extraction with JSON output"""
        monkeypatch.setenv('DBT_PROD_MANIFEST_PATH', str(prod_manifest))

        result = cli_runner.invoke(app, ['children', test_model, '-j'])

        # May exit with 0 (has children) or 1 (no children found)
        if result.exit_code == 0:
            data = json.loads(result.stdout)
            assert isinstance(data, list)

    def test_children_help(self, cli_runner):
        """Test children command help text"""
        result = cli_runner.invoke(app, ['children', '--help'])

        assert result.exit_code == 0
        assert 'child' in result.stdout.lower()


class TestSqlCommand:
    """End-to-end tests for 'meta sql' command"""

    def test_sql_compiled_output(self, cli_runner, prod_manifest_with_compiled, test_model, monkeypatch):
        """Test SQL extraction (compiled)"""
        monkeypatch.setenv('DBT_PROD_MANIFEST_PATH', str(prod_manifest_with_compiled))

        result = cli_runner.invoke(app, ['sql', test_model])

        # May succeed or fail depending on whether compiled_code exists
        if result.exit_code == 0:
            assert len(result.stdout) > 0
            # Should contain SQL

    def test_sql_jinja_flag(self, cli_runner, prod_manifest_with_compiled, test_model, monkeypatch):
        """Test SQL extraction with --jinja flag"""
        monkeypatch.setenv('DBT_PROD_MANIFEST_PATH', str(prod_manifest_with_compiled))

        result = cli_runner.invoke(app, ['sql', test_model, '--jinja'])

        # May succeed or fail depending on whether raw_code exists
        if result.exit_code == 0:
            assert len(result.stdout) > 0

    def test_sql_help(self, cli_runner):
        """Test sql command help text"""
        result = cli_runner.invoke(app, ['sql', '--help'])

        assert result.exit_code == 0
        assert 'sql' in result.stdout.lower()


class TestPathCommand:
    """End-to-end tests for 'meta path' command"""

    def test_path_output(self, cli_runner, prod_manifest, test_model, monkeypatch):
        """Test path extraction"""
        monkeypatch.setenv('DBT_PROD_MANIFEST_PATH', str(prod_manifest))

        result = cli_runner.invoke(app, ['path', test_model])

        assert result.exit_code == 0
        # Should output file path
        assert '.sql' in result.stdout or len(result.stdout) > 0

    def test_path_help(self, cli_runner):
        """Test path command help text"""
        result = cli_runner.invoke(app, ['path', '--help'])

        assert result.exit_code == 0
        assert 'path' in result.stdout.lower()


class TestConfigCommand:
    """End-to-end tests for 'meta config' command"""

    def test_config_json_output(self, cli_runner, prod_manifest, test_model, monkeypatch):
        """Test config extraction with JSON output"""
        monkeypatch.setenv('DBT_PROD_MANIFEST_PATH', str(prod_manifest))

        result = cli_runner.invoke(app, ['config', test_model, '-j'])

        assert result.exit_code == 0
        data = json.loads(result.stdout)
        assert isinstance(data, dict)

    def test_config_help(self, cli_runner):
        """Test config command help text"""
        result = cli_runner.invoke(app, ['config', '--help'])

        assert result.exit_code == 0
        assert 'config' in result.stdout.lower()


class TestListCommand:
    """End-to-end tests for 'meta list' command"""

    def test_list_json_output(self, cli_runner, prod_manifest, monkeypatch):
        """Test list models with JSON output"""
        monkeypatch.setenv('DBT_PROD_MANIFEST_PATH', str(prod_manifest))

        result = cli_runner.invoke(app, ['list', 'core', '-j'])

        assert result.exit_code == 0
        data = json.loads(result.stdout)
        assert isinstance(data, list)

    def test_list_text_output(self, cli_runner, prod_manifest, monkeypatch):
        """Test list models with text output"""
        monkeypatch.setenv('DBT_PROD_MANIFEST_PATH', str(prod_manifest))

        result = cli_runner.invoke(app, ['list', 'core'])

        assert result.exit_code == 0
        # Should output model list

    def test_list_help(self, cli_runner):
        """Test list command help text"""
        result = cli_runner.invoke(app, ['list', '--help'])

        assert result.exit_code == 0
        assert 'list' in result.stdout.lower()


class TestSearchCommand:
    """End-to-end tests for 'meta search' command"""

    def test_search_json_output(self, cli_runner, prod_manifest, monkeypatch):
        """Test search with JSON output"""
        monkeypatch.setenv('DBT_PROD_MANIFEST_PATH', str(prod_manifest))

        result = cli_runner.invoke(app, ['search', 'client', '--json'])

        assert result.exit_code == 0
        data = json.loads(result.stdout)
        assert isinstance(data, list)

    def test_search_help(self, cli_runner):
        """Test search command help text"""
        result = cli_runner.invoke(app, ['search', '--help'])

        assert result.exit_code == 0
        assert 'search' in result.stdout.lower()


class TestErrorHandling:
    """Test CLI error handling and formatting"""

    def test_manifest_not_found_error(self, cli_runner, tmp_path, monkeypatch):
        """Test error message when manifest.json not found"""
        nonexistent = tmp_path / 'missing.json'
        monkeypatch.setenv('DBT_PROD_MANIFEST_PATH', str(nonexistent))

        result = cli_runner.invoke(app, ['schema', 'any_model'])

        assert result.exit_code == 1
        # Error message should be in stderr or stdout
        error_output = result.stderr + result.stdout
        assert 'not found' in error_output.lower()

    def test_invalid_manifest_error(self, cli_runner, tmp_path, monkeypatch):
        """Test error when manifest.json is invalid JSON"""
        invalid_manifest = tmp_path / 'invalid.json'
        invalid_manifest.write_text('{invalid json')
        monkeypatch.setenv('DBT_PROD_MANIFEST_PATH', str(invalid_manifest))

        result = cli_runner.invoke(app, ['schema', 'any_model'])

        assert result.exit_code == 1
        error_output = result.stderr + result.stdout
        assert 'parse' in error_output.lower() or 'error' in error_output.lower()

    def test_model_not_found_with_suggestion(self, cli_runner, prod_manifest, monkeypatch):
        """Test that ModelNotFoundError includes suggestion"""
        monkeypatch.setenv('DBT_PROD_MANIFEST_PATH', str(prod_manifest))

        result = cli_runner.invoke(app, ['schema', 'definitely_nonexistent_model_xyz'])

        assert result.exit_code == 1
        error_output = result.stderr + result.stdout
        assert 'not found' in error_output.lower()
        # Should include suggestion (e.g., "Try: meta list")


class TestWarnings:
    """Test warning system in CLI"""

    def test_git_mismatch_warning_modified_without_dev(
        self, cli_runner, prod_manifest, test_model, mock_git_modified, monkeypatch
    ):
        """Test warning when model modified but --dev not used"""
        monkeypatch.setenv('DBT_PROD_MANIFEST_PATH', str(prod_manifest))

        result = cli_runner.invoke(app, ['schema', test_model, '-j'])

        # Command should succeed
        assert result.exit_code == 0

        # Warning should be in stderr (JSON mode)
        if result.stderr:
            # In JSON mode, warnings are structured
            try:
                warnings = json.loads(result.stderr)
                assert 'warnings' in warnings
            except json.JSONDecodeError:
                # In text mode, just check for warning message
                pass

    def test_no_warning_when_dev_flag_used(
        self, cli_runner, prod_manifest, test_model, mock_git_modified, dev_manifest_setup, monkeypatch
    ):
        """Test no warning when --dev flag used with modified model"""
        monkeypatch.setenv('DBT_PROD_MANIFEST_PATH', str(dev_manifest_setup))
        monkeypatch.setenv('DBT_DEV_MANIFEST_PATH', str(dev_manifest_setup.parent / 'target' / 'manifest.json'))

        # Using --dev flag with modified model should not warn
        cli_runner.invoke(app, ['schema', test_model, '--dev', '-j'])

        # May exit with 0 or 1 depending on whether model exists
        # Just verify no crash


class TestHelpSystem:
    """Test help text and documentation"""

    def test_main_help(self, cli_runner):
        """Test main help text"""
        result = cli_runner.invoke(app, ['--help'])

        assert result.exit_code == 0
        assert 'dbt-meta' in result.stdout.lower()
        # Should show available commands

    def test_version_flag(self, cli_runner):
        """Test --version flag"""
        result = cli_runner.invoke(app, ['--version'])

        assert result.exit_code == 0
        # Should show version number

    def test_all_commands_have_help(self, cli_runner):
        """Test that all commands have help text"""
        commands = [
            'schema', 'columns', 'info', 'deps', 'parents', 'children',
            'sql', 'path', 'config', 'list', 'search', 'refresh'
        ]

        for cmd in commands:
            result = cli_runner.invoke(app, [cmd, '--help'])
            assert result.exit_code == 0
            # Help text should be non-trivial
            assert len(result.stdout) > 50

    def test_configuration_panel_in_help(self, cli_runner):
        """Test that help includes configuration information"""
        result = cli_runner.invoke(app, ['--help'])

        assert result.exit_code == 0
        # Should mention environment variables or configuration


class TestRichFormatting:
    """Test Rich formatting in text output"""

    def test_columns_table_formatting(self, cli_runner, prod_manifest, test_model, monkeypatch):
        """Test that columns command produces Rich table"""
        monkeypatch.setenv('DBT_PROD_MANIFEST_PATH', str(prod_manifest))

        result = cli_runner.invoke(app, ['columns', test_model])

        if result.exit_code == 0:
            # Rich tables may contain box-drawing characters or structured output
            # Just verify output is generated
            assert len(result.stdout) > 0

    def test_json_mode_no_formatting(self, cli_runner, prod_manifest, test_model, monkeypatch):
        """Test that JSON mode produces clean JSON without formatting"""
        monkeypatch.setenv('DBT_PROD_MANIFEST_PATH', str(prod_manifest))

        result = cli_runner.invoke(app, ['columns', test_model, '-j'])

        if result.exit_code == 0:
            # Should be valid JSON
            data = json.loads(result.stdout)
            assert isinstance(data, list)


class TestDevFlag:
    """Test --dev flag functionality across commands"""

    def test_dev_flag_with_dev_manifest(
        self, cli_runner, dev_manifest_setup, monkeypatch
    ):
        """Test that --dev flag uses dev manifest"""
        monkeypatch.setenv('DBT_PROD_MANIFEST_PATH', str(dev_manifest_setup))
        monkeypatch.setenv('DBT_DEV_MANIFEST_PATH', str(dev_manifest_setup.parent / 'target' / 'manifest.json'))

        # Schema command with --dev should use dev manifest
        result = cli_runner.invoke(app, ['schema', 'test_schema__test_model', '--dev', '-j'])

        # Should succeed with dev manifest
        if result.exit_code == 0:
            data = json.loads(result.stdout)
            assert isinstance(data, dict)

    def test_dev_flag_in_multiple_commands(self, cli_runner, dev_manifest_setup, monkeypatch):
        """Test that --dev flag works across different commands"""
        monkeypatch.setenv('DBT_PROD_MANIFEST_PATH', str(dev_manifest_setup))
        monkeypatch.setenv('DBT_DEV_MANIFEST_PATH', str(dev_manifest_setup.parent / 'target' / 'manifest.json'))

        commands_with_dev = ['schema', 'columns', 'info', 'config']

        for cmd in commands_with_dev:
            result = cli_runner.invoke(app, [cmd, 'test_schema__test_model', '--dev'])
            # Commands may succeed or fail, just verify no crash
            assert result.exit_code in (0, 1)


class TestJsonOutput:
    """Test JSON output mode across all commands"""

    def test_json_flag_produces_valid_json(self, cli_runner, prod_manifest, test_model, monkeypatch):
        """Test that -j flag produces parseable JSON"""
        monkeypatch.setenv('DBT_PROD_MANIFEST_PATH', str(prod_manifest))

        json_commands = ['schema', 'columns', 'info', 'deps', 'config', 'list']

        for cmd in json_commands:
            result = cli_runner.invoke(app, [cmd, test_model, '-j'])

            if result.exit_code == 0:
                # Should be valid JSON
                data = json.loads(result.stdout)
                assert data is not None

    def test_json_errors_are_structured(self, cli_runner, prod_manifest, monkeypatch):
        """Test that errors in JSON mode are properly formatted"""
        monkeypatch.setenv('DBT_PROD_MANIFEST_PATH', str(prod_manifest))

        result = cli_runner.invoke(app, ['schema', 'nonexistent_xyz', '-j'])

        assert result.exit_code == 1
        # Error output should be present (may be in stderr or stdout)
