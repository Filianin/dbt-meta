"""Pytest configuration and fixtures for dbt-meta tests"""

import json
import os
from pathlib import Path

import pytest


# Disable fallbacks by default in tests
@pytest.fixture(autouse=True)
def _setup_test_env(request, monkeypatch):
    """
    Setup test environment - disable fallbacks by default unless enable_fallbacks fixture is used.

    This prevents tests from trying to access ./target/manifest.json
    (which doesn't exist in test environment) when testing nonexistent models.
    """
    # Check if test requests enable_fallbacks fixture
    if 'enable_fallbacks' not in request.fixturenames:
        monkeypatch.setenv('DBT_FALLBACK_TARGET', 'false')
        monkeypatch.setenv('DBT_FALLBACK_BIGQUERY', 'false')

@pytest.fixture
def enable_fallbacks(monkeypatch):
    """
    Enable fallbacks for tests that specifically need them.

    Usage:
        def test_with_fallbacks(enable_fallbacks):
            # Fallbacks are enabled in this test
            ...
    """
    monkeypatch.setenv('DBT_FALLBACK_TARGET', 'true')
    monkeypatch.setenv('DBT_FALLBACK_BIGQUERY', 'true')

# Manifest fixtures
@pytest.fixture
def prod_manifest():
    """
    Production manifest - uses production manifest path

    Priority:
    1. DBT_MANIFEST_PATH environment variable (explicit override)
    2. DBT_PROD_MANIFEST_PATH environment variable (default: ~/dbt-state/manifest.json)
    """
    # Priority 1: Explicit override
    env_manifest = os.environ.get('DBT_MANIFEST_PATH')
    if env_manifest and Path(env_manifest).exists():
        return Path(env_manifest)

    # Priority 2: Production manifest path (default ~/dbt-state/manifest.json)
    prod_manifest_path = os.environ.get('DBT_PROD_MANIFEST_PATH', str(Path.home() / "dbt-state" / "manifest.json"))
    prod_path = Path(prod_manifest_path).expanduser()
    if prod_path.exists():
        return prod_path

    pytest.fail(
        "No production manifest found. Options:\n"
        "1. Set DBT_MANIFEST_PATH environment variable\n"
        "2. Place manifest at ~/dbt-state/manifest.json\n"
        "3. Set DBT_PROD_MANIFEST_PATH to custom location"
    )

@pytest.fixture
def prod_manifest_with_compiled():
    """
    Production manifest with compiled_code field

    Uses same priority as prod_manifest fixture
    """
    # Priority 1: Explicit override
    env_manifest = os.environ.get('DBT_MANIFEST_PATH')
    if env_manifest and Path(env_manifest).exists():
        return Path(env_manifest)

    # Priority 2: Production manifest path
    prod_manifest_path = os.environ.get('DBT_PROD_MANIFEST_PATH', str(Path.home() / "dbt-state" / "manifest.json"))
    prod_path = Path(prod_manifest_path).expanduser()
    if prod_path.exists():
        return prod_path

    pytest.fail("No production manifest found.")

@pytest.fixture
def dev_manifest_setup(tmp_path, prod_manifest):
    """
    Create dev manifest structure for tests requiring use_dev=True
    Returns path to production manifest with dev manifest (target/) created alongside
    """
    # Create manifest structure
    project_root = tmp_path / "project"
    project_root.mkdir()
    dbt_state = project_root / ".dbt-state"
    dbt_state.mkdir()
    target = project_root / "target"
    target.mkdir()

    # Production manifest (empty for simplicity)
    prod_path = dbt_state / "manifest.json"
    prod_path.write_text('{"nodes": {}}')

    # Dev manifest with test model
    dev_path = target / "manifest.json"
    dev_data = {
        "nodes": {
            "model.project.test_schema__test_model": {
                "name": "test_model",
                "schema": "test_schema",
                "database": "",
                "config": {},
                "raw_code": "SELECT * FROM {{ ref('upstream_model') }}",
                "compiled_code": "SELECT * FROM upstream_table",
                "original_file_path": "models/test/test_model.sql"
            }
        }
    }
    dev_path.write_text(json.dumps(dev_data))

    return prod_path

# Test model - dynamically selected from manifest
@pytest.fixture
def test_model(prod_manifest):
    """
    Select any model from manifest for testing

    Returns first model found in manifest (anonymous testing)
    """
    from dbt_meta.manifest.parser import ManifestParser

    parser = ManifestParser(str(prod_manifest))
    nodes = parser.manifest.get('nodes', {})

    # Find first model
    for node_id in nodes:
        if node_id.startswith('model.') and nodes[node_id].get('resource_type') == 'model':
            # Format: "model.project.schema__table" → "schema__table"
            model_name = node_id.split(".")[-1]
            return model_name

    pytest.skip("No models found in manifest")

# Mock fixtures
@pytest.fixture
def mock_bq_client(mocker):
    """Mock BigQuery client for columns fallback"""
    mock = mocker.patch("subprocess.run")
    mock.return_value.stdout = json.dumps([
        {"name": "customer_id", "type": "INTEGER"},
        {"name": "customer_name", "type": "STRING"}
    ])
    return mock

# Performance tracking
@pytest.fixture(scope="session")
def performance_tracker():
    """Track performance metrics across test session"""
    metrics = {}
    yield metrics
    # Print summary at end
    if metrics:
        print("\n📊 Performance Summary:")
        for test_name, duration in sorted(metrics.items()):
            print(f"  {test_name}: {duration:.2f}ms")
