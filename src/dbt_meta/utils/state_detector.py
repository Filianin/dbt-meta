"""ModelStateDetector — injectable service for detecting model lifecycle state.

Bundles manifest loading, git status check, and state classification into one
testable unit. Other commands (scan, analyze) can reuse this instead of
duplicating the detection logic inline.
"""

import contextlib
import os
from dataclasses import dataclass, field
from typing import Optional

from dbt_meta.config import Config
from dbt_meta.utils import get_cached_parser as _get_cached_parser
from dbt_meta.utils.dev import calculate_dev_schema as _calculate_dev_schema
from dbt_meta.utils.git import check_manifest_git_mismatch, get_model_git_status
from dbt_meta.utils.model_state import ModelState, detect_model_state


@dataclass
class DetectedState:
    """Result of ModelStateDetector.detect()."""

    state: ModelState
    model: Optional[dict] = None
    prod_model: Optional[dict] = None
    file_path: Optional[str] = None
    warnings: list[dict] = field(default_factory=list)


class ModelStateDetector:
    """Detects dbt model lifecycle state from manifests and git.

    Encapsulates:
    - Loading prod / dev parsers
    - Querying manifest presence
    - Detecting git file status
    - Calling detect_model_state()

    Inject into commands instead of duplicating this logic inline.

    Usage::

        detector = ModelStateDetector(config, model_name="my_model", use_dev=False)
        detected = detector.detect()
        # detected.state, detected.model, detected.prod_model, detected.file_path
    """

    def __init__(self, config: Config, model_name: str, use_dev: bool = False) -> None:
        self._config = config
        self._model_name = model_name
        self._use_dev = use_dev

    def detect(self) -> DetectedState:
        """Run full detection pipeline.

        Returns:
            DetectedState with state enum and associated model dicts.
        """
        prod_parser = self._load_parser(self._config.prod_manifest_path)
        dev_parser = self._load_parser(self._config.dev_manifest_path) if self._config.fallback_dev_enabled else None

        in_prod = prod_parser.get_model(self._model_name) is not None if prod_parser else False
        in_dev = dev_parser.get_model(self._model_name) is not None if dev_parser else False

        prod_model = prod_parser.get_model(self._model_name) if prod_parser else None
        dev_model = dev_parser.get_model(self._model_name) if dev_parser else None

        # Choose primary model: prefer dev when --dev flag is set, else prod.
        # When use_dev=True, override schema to dev schema so BigQuery queries
        # the dev dataset, not prod (dev manifests often contain prod schema from dbt compile).
        if self._use_dev and dev_model:
            model = dev_model.copy()
            model['schema'] = _calculate_dev_schema()
        elif prod_model:
            model = prod_model
        elif dev_model:
            model = dev_model
        else:
            model = None

        file_path = None
        if model:
            file_path = model.get('original_file_path') or model.get('path')

        git_status = get_model_git_status(self._model_name, file_path=file_path)

        state = detect_model_state(
            self._model_name,
            in_prod_manifest=in_prod,
            in_dev_manifest=in_dev,
            git_status=git_status,
            model=model,
            file_path=file_path,
        )

        dev_manifest_path = self._config.dev_manifest_path if self._config.fallback_dev_enabled else None
        warnings = check_manifest_git_mismatch(
            self._model_name,
            self._use_dev,
            dev_manifest_path,
            prod_parser=prod_parser,
            dev_parser=dev_parser,
        )

        return DetectedState(
            state=state,
            model=model,
            prod_model=prod_model,
            file_path=file_path,
            warnings=warnings,
        )

    @staticmethod
    def _load_parser(manifest_path: Optional[str]):
        """Load manifest parser, returning None on any error."""
        if not manifest_path or not os.path.exists(manifest_path):
            return None
        with contextlib.suppress(FileNotFoundError, OSError):
            return _get_cached_parser(manifest_path)
        return None
