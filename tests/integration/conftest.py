"""Pytest configuration for CLI integration tests"""

import pytest
from typer.testing import CliRunner


@pytest.fixture
def cli_runner():
    """Create CLI runner for integration tests"""
    return CliRunner()


@pytest.fixture
def mock_git_modified(mocker):
    """Mock git status to show model as modified"""
    return mocker.patch(
        'dbt_meta.utils.git.is_modified',
        return_value=True
    )


@pytest.fixture
def mock_git_unmodified(mocker):
    """Mock git status to show model as unmodified"""
    return mocker.patch(
        'dbt_meta.utils.git.is_modified',
        return_value=False
    )


@pytest.fixture
def mock_bq_fallback_success(mocker):
    """Mock successful BigQuery fallback"""
    mock_run = mocker.patch('subprocess.run')
    mock_run.return_value.returncode = 0
    mock_run.return_value.stdout = '''{
        "tableReference": {
            "projectId": "test-project",
            "datasetId": "test_dataset",
            "tableId": "test_table"
        },
        "schema": {
            "fields": [
                {"name": "id", "type": "INTEGER"},
                {"name": "name", "type": "STRING"}
            ]
        },
        "type": "TABLE"
    }'''
    return mock_run


@pytest.fixture
def mock_bq_fallback_failure(mocker):
    """Mock failed BigQuery fallback"""
    mock_run = mocker.patch('subprocess.run')
    mock_run.return_value.returncode = 1
    mock_run.return_value.stderr = 'Not found: Table test_dataset.test_table'
    return mock_run
