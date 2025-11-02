"""Pytest configuration and fixtures for dbt-meta tests"""

import pytest
import json
from pathlib import Path

# Manifest fixtures
@pytest.fixture
def prod_manifest():
    """Production manifest (15MB, 865 models) - for all tests"""
    return Path(__file__).parent / "fixtures" / "manifests" / "prod_manifest.json"

# Test models (real production models)
TEST_MODELS = [
    "core_client__client_profiles_events",
]

@pytest.fixture(params=TEST_MODELS)
def test_model(request):
    """Parametrized fixture for testing all commands on real models"""
    return request.param

# Expected outputs fixtures (from bash version)
@pytest.fixture
def expected_outputs():
    """Helper for loading expected outputs"""
    def _load(model_name, command):
        """Load expected output for model and command"""
        path = Path(__file__).parent / "fixtures" / "expected_outputs" / f"{model_name}_{command}.json"
        if not path.exists():
            pytest.skip(f"Expected output not found: {path}")
        return json.loads(path.read_text())
    return _load

# Specific expected outputs
@pytest.fixture
def expected_info(test_model):
    """Expected info output for test_model"""
    path = Path(__file__).parent / "fixtures" / "expected_outputs" / f"{test_model}_info.json"
    if not path.exists():
        pytest.skip(f"Expected output not found: {path}")
    return json.loads(path.read_text())

@pytest.fixture
def expected_schema(test_model):
    """Expected schema output for test_model"""
    path = Path(__file__).parent / "fixtures" / "expected_outputs" / f"{test_model}_schema.json"
    if not path.exists():
        pytest.skip(f"Expected output not found: {path}")
    return json.loads(path.read_text())

@pytest.fixture
def expected_columns(test_model):
    """Expected columns output for test_model"""
    path = Path(__file__).parent / "fixtures" / "expected_outputs" / f"{test_model}_columns.json"
    if not path.exists():
        pytest.skip(f"Expected output not found: {path}")
    return json.loads(path.read_text())

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
