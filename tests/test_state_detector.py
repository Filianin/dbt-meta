"""Tests for ModelStateDetector.

These tests verify state detection WITHOUT mocking BigQuery or catalog.
Only git status and manifest parser are mocked.
"""

from unittest.mock import MagicMock, patch

import pytest

from dbt_meta.config import Config
from dbt_meta.utils.git import GitStatus
from dbt_meta.utils.model_state import ModelState
from dbt_meta.utils.state_detector import DetectedState, ModelStateDetector


def _git(
    exists=True,
    is_tracked=True,
    is_modified=False,
    is_committed=True,
    is_deleted=False,
    is_new=False,
) -> GitStatus:
    return GitStatus(
        exists=exists,
        is_tracked=is_tracked,
        is_modified=is_modified,
        is_committed=is_committed,
        is_deleted=is_deleted,
        is_new=is_new,
    )


def _config(prod_path="/prod/manifest.json", dev_path="/dev/manifest.json", dev_enabled=True) -> Config:
    cfg = Config.from_env()
    cfg.prod_manifest_path = prod_path
    cfg.dev_manifest_path = dev_path
    cfg.fallback_dev_enabled = dev_enabled
    return cfg


def _make_model(name="test_model", schema="core", alias=None):
    return {
        "name": name,
        "schema": schema,
        "alias": alias,
        "original_file_path": f"models/core/{name}.sql",
        "config": {"enabled": True},
    }


@pytest.fixture
def prod_model():
    return _make_model()


class TestModelStateDetectorProdStable:
    def test_prod_stable_model(self, prod_model):
        cfg = _config()
        detector = ModelStateDetector(cfg, "test_model", use_dev=False)

        prod_parser = MagicMock()
        prod_parser.get_model.return_value = prod_model

        with (
            patch("dbt_meta.utils.state_detector._get_cached_parser", return_value=prod_parser),
            patch("dbt_meta.utils.state_detector.get_model_git_status", return_value=_git()),
            patch("os.path.exists", return_value=True),
        ):
            result = detector.detect()

        assert result.state == ModelState.PROD_STABLE
        assert result.model == prod_model
        assert result.prod_model == prod_model

    def test_returns_detected_state_dataclass(self, prod_model):
        cfg = _config()
        detector = ModelStateDetector(cfg, "test_model")

        prod_parser = MagicMock()
        prod_parser.get_model.return_value = prod_model

        with (
            patch("dbt_meta.utils.state_detector._get_cached_parser", return_value=prod_parser),
            patch("dbt_meta.utils.state_detector.get_model_git_status", return_value=_git()),
            patch("os.path.exists", return_value=True),
        ):
            result = detector.detect()

        assert isinstance(result, DetectedState)
        assert result.file_path == "models/core/test_model.sql"


class TestModelStateDetectorNewModel:
    def test_new_uncommitted(self):
        cfg = _config()
        detector = ModelStateDetector(cfg, "new_model")

        no_model_parser = MagicMock()
        no_model_parser.get_model.return_value = None

        with (
            patch("dbt_meta.utils.state_detector._get_cached_parser", return_value=no_model_parser),
            patch("dbt_meta.utils.state_detector.get_model_git_status", return_value=_git(is_new=True, is_committed=False)),
            patch("os.path.exists", return_value=True),
        ):
            result = detector.detect()

        assert result.state == ModelState.NEW_UNCOMMITTED
        assert result.model is None
        assert result.prod_model is None

    def test_new_committed_not_deployed(self):
        cfg = _config(dev_enabled=False)
        detector = ModelStateDetector(cfg, "new_model")

        no_model_parser = MagicMock()
        no_model_parser.get_model.return_value = None

        with (
            patch("dbt_meta.utils.state_detector._get_cached_parser", return_value=no_model_parser),
            patch("dbt_meta.utils.state_detector.get_model_git_status", return_value=_git(is_committed=True, is_new=False)),
            patch("os.path.exists", return_value=True),
        ):
            result = detector.detect()

        assert result.state == ModelState.NEW_COMMITTED


class TestModelStateDetectorModified:
    def test_modified_uncommitted(self, prod_model):
        cfg = _config()
        detector = ModelStateDetector(cfg, "test_model")

        prod_parser = MagicMock()
        prod_parser.get_model.return_value = prod_model
        dev_parser = MagicMock()
        dev_parser.get_model.return_value = None

        parsers = {"prod": prod_parser, "dev": dev_parser}

        def _get_parser(path):
            if "prod" in path:
                return parsers["prod"]
            return parsers["dev"]

        with (
            patch("dbt_meta.utils.state_detector._get_cached_parser", side_effect=_get_parser),
            patch("dbt_meta.utils.state_detector.get_model_git_status", return_value=_git(is_modified=True)),
            patch("os.path.exists", return_value=True),
        ):
            result = detector.detect()

        assert result.state == ModelState.MODIFIED_UNCOMMITTED
        assert result.prod_model == prod_model


class TestModelStateDetectorDeleted:
    def test_deleted_locally(self, prod_model):
        cfg = _config()
        detector = ModelStateDetector(cfg, "test_model")

        prod_parser = MagicMock()
        prod_parser.get_model.return_value = prod_model

        with (
            patch("dbt_meta.utils.state_detector._get_cached_parser", return_value=prod_parser),
            patch("dbt_meta.utils.state_detector.get_model_git_status", return_value=_git(exists=False, is_deleted=True)),
            patch("os.path.exists", return_value=True),
        ):
            result = detector.detect()

        assert result.state == ModelState.DELETED_LOCALLY

    def test_not_found(self):
        cfg = _config()
        detector = ModelStateDetector(cfg, "ghost_model")

        no_parser = MagicMock()
        no_parser.get_model.return_value = None

        with (
            patch("dbt_meta.utils.state_detector._get_cached_parser", return_value=no_parser),
            patch("dbt_meta.utils.state_detector.get_model_git_status", return_value=_git(exists=False, is_deleted=False)),
            patch("os.path.exists", return_value=True),
        ):
            result = detector.detect()

        assert result.state == ModelState.NOT_FOUND


class TestModelStateDetectorMissingManifest:
    def test_no_prod_manifest_returns_not_found(self):
        cfg = _config(prod_path="/nonexistent/manifest.json")
        detector = ModelStateDetector(cfg, "test_model")

        with (
            patch("os.path.exists", return_value=False),
            patch("dbt_meta.utils.state_detector.get_model_git_status", return_value=_git(exists=False)),
        ):
            result = detector.detect()

        assert result.state == ModelState.NOT_FOUND

    def test_dev_parser_skipped_when_disabled(self, prod_model):
        cfg = _config(dev_enabled=False)
        detector = ModelStateDetector(cfg, "test_model")

        prod_parser = MagicMock()
        prod_parser.get_model.return_value = prod_model

        with (
            patch("dbt_meta.utils.state_detector._get_cached_parser", return_value=prod_parser),
            patch("dbt_meta.utils.state_detector.get_model_git_status", return_value=_git()),
            patch("os.path.exists", return_value=True),
        ):
            result = detector.detect()

        # Dev parser should not have been called
        assert result.state == ModelState.PROD_STABLE


class TestModelStateDetectorDevMode:
    def test_prefers_dev_model_when_use_dev(self):
        cfg = _config()
        detector = ModelStateDetector(cfg, "test_model", use_dev=True)

        dev_model = _make_model(schema="personal_user")
        prod_model = _make_model(schema="core")

        def _get_parser(path):
            p = MagicMock()
            if "prod" in path:
                p.get_model.return_value = prod_model
            else:
                p.get_model.return_value = dev_model
            return p

        with (
            patch("dbt_meta.utils.state_detector._get_cached_parser", side_effect=_get_parser),
            patch("dbt_meta.utils.state_detector.get_model_git_status", return_value=_git()),
            patch("dbt_meta.utils.state_detector._calculate_dev_schema", return_value="personal_user"),
            patch("os.path.exists", return_value=True),
        ):
            result = detector.detect()

        assert result.model is not dev_model  # copy, not same object
        assert result.model['schema'] == "personal_user"
        assert result.model['name'] == dev_model['name']
        assert result.prod_model == prod_model
