"""Base command class for dbt-meta commands.

Provides common functionality for all model metadata extraction commands:
- 3-level fallback strategy (prod → dev → BigQuery)
- Dev mode handling (prioritizes dev manifest)
- Warning collection and output
- Git status checking
"""

import contextlib
from abc import ABC, abstractmethod
from typing import Any, Optional

from dbt_meta.config import Config
from dbt_meta.errors import ManifestNotFoundError, ManifestParseError, ModelNotFoundError
from dbt_meta.fallback import FallbackLevel, FallbackStrategy
from dbt_meta.utils import get_cached_parser as _get_cached_parser
from dbt_meta.utils import print_warnings as _print_warnings
from dbt_meta.utils.dev import (
    find_dev_manifest as _find_dev_manifest,
)
from dbt_meta.utils.git import check_manifest_git_mismatch as _check_manifest_git_mismatch


class BaseCommand(ABC):
    """Abstract base class for dbt-meta commands.

    Provides common functionality for model metadata extraction commands,
    including fallback logic, dev mode handling, and warning management.

    Subclasses must implement:
    - execute(): Main command execution logic
    - process_model(): Process model data and return formatted result
    - SUPPORTS_BIGQUERY: Class attribute indicating BigQuery fallback support
    - SUPPORTS_DEV: Class attribute indicating dev mode support

    Example:
        class InfoCommand(BaseCommand):
            SUPPORTS_BIGQUERY = True
            SUPPORTS_DEV = True

            def execute(self) -> Optional[Dict]:
                model = self.get_model_with_fallback()
                if not model:
                    return None
                return self.process_model(model)

            def process_model(self, model: dict) -> Dict:
                return {'name': model.get('name', '')}
    """

    # Subclasses override these
    SUPPORTS_BIGQUERY: bool = False  # Whether command supports BigQuery fallback
    SUPPORTS_DEV: bool = True        # Whether command supports dev mode

    def __init__(
        self,
        config: Config,
        manifest_path: str,
        model_name: str,
        use_dev: bool = False,
        json_output: bool = False
    ):
        """Initialize base command.

        Args:
            config: Configuration object
            manifest_path: Path to production manifest.json
            model_name: Model name to operate on
            use_dev: If True, prioritize dev manifest over production
            json_output: If True, output warnings in JSON format
        """
        self.config = config
        self.manifest_path = manifest_path
        self.model_name = model_name
        self.use_dev = use_dev
        self.json_output = json_output
        self.warnings: list[dict[str, str]] = []
        self._fallback_strategy = FallbackStrategy(config, manifest_path)

    @abstractmethod
    def execute(self) -> Any:
        """Execute command and return result.

        This is the main entry point for command execution.
        Subclasses implement the command-specific logic here.

        Returns:
            Command-specific result (None if model not found)
        """
        pass

    @abstractmethod
    def process_model(self, model: dict, level: Optional[FallbackLevel] = None) -> Any:
        """Process model data and return formatted result.

        Args:
            model: Model data from manifest or BigQuery
            level: Fallback level where model was found (if applicable)

        Returns:
            Formatted result for this command
        """
        pass

    def get_model_with_fallback(self) -> Optional[dict[str, Any]]:
        """Get model data with 3-level fallback strategy.

        Flow:
        - If use_dev=True: Try dev manifest → BigQuery (if SUPPORTS_BIGQUERY)
        - If use_dev=False: Try prod manifest → dev manifest → BigQuery

        Returns:
            Model data dict if found, None otherwise

        Side effects:
            - Collects warnings in self.warnings
            - Emits warnings via emit_warnings()
        """
        # Get parsers for both prod and dev (for new model detection)
        prod_parser = _get_cached_parser(self.manifest_path)
        dev_parser = None
        dev_manifest = _find_dev_manifest(self.manifest_path)
        if dev_manifest:
            with contextlib.suppress(ManifestNotFoundError, ManifestParseError):
                dev_parser = _get_cached_parser(dev_manifest)

        # Check git status and collect warnings (with parsers for new model detection)
        git_warnings = _check_manifest_git_mismatch(
            self.model_name,
            self.use_dev,
            dev_manifest,
            prod_parser=prod_parser,
            dev_parser=dev_parser
        )
        _print_warnings(git_warnings, self.json_output)

        # CRITICAL: If critical errors detected, fail early
        # - file_not_compiled: File exists but compilation failed
        # - model_not_in_dev: Using --dev but model not built in dev
        # Note: We don't block on "new_model_candidate" to allow defer workflow fallback
        if any(w.get('type') in ('file_not_compiled', 'model_not_in_dev') for w in git_warnings):
            return None

        # Dev mode: prioritize dev manifest first
        if self.use_dev and self.SUPPORTS_DEV:
            return self._get_model_dev_mode()

        # Production mode: use 3-level fallback
        return self._get_model_prod_mode()

    def _get_model_dev_mode(self) -> Optional[dict[str, Any]]:
        """Get model in dev mode (dev manifest → BigQuery).

        Returns:
            Model data if found, None otherwise
        """
        # Try dev manifest first
        dev_manifest = _find_dev_manifest(self.manifest_path)
        if dev_manifest:
            try:
                parser = _get_cached_parser(dev_manifest)
                model = parser.get_model(self.model_name)
                if model:
                    # CRITICAL: Override schema to dev schema when using --dev
                    # Dev manifest may contain production schema (from dbt compile)
                    # but at runtime we want to use dev schema for BigQuery queries
                    from dbt_meta.utils.dev import calculate_dev_schema
                    dev_schema = calculate_dev_schema()
                    model = model.copy()  # Don't modify cached model
                    model['schema'] = dev_schema  # Override with dev schema
                    return model
            except (ManifestNotFoundError, ManifestParseError):  # pragma: no cover
                # Dev manifest not available or invalid - continue to BigQuery fallback
                pass

        # Fallback to BigQuery for dev (if supported and enabled)
        if self.SUPPORTS_BIGQUERY and self.config.fallback_bigquery_enabled:
            return self._get_model_bigquery_dev()

        return None

    def _get_model_prod_mode(self) -> Optional[dict[str, Any]]:
        """Get model in production mode (prod → dev → BigQuery).

        Uses FallbackStrategy for 3-level fallback.

        Returns:
            Model data if found, None otherwise
        """
        parser = _get_cached_parser(self.manifest_path)

        # Build allowed fallback levels
        allowed_levels = [FallbackLevel.PROD_MANIFEST, FallbackLevel.DEV_MANIFEST]
        if self.SUPPORTS_BIGQUERY:
            allowed_levels.append(FallbackLevel.BIGQUERY)

        try:
            result = self._fallback_strategy.get_model(
                model_name=self.model_name,
                prod_parser=parser,
                allowed_levels=allowed_levels
            )

            # Collect warnings from fallback
            if result.warnings:
                fallback_warnings = [
                    {
                        "type": f"{result.level.value}_fallback" if result.level else "fallback",
                        "severity": "warning",
                        "message": warning,
                        "source": result.level.value if result.level else "unknown"
                    }
                    for warning in result.warnings
                ]
                _print_warnings(fallback_warnings, self.json_output)

            return result.data

        except ModelNotFoundError:
            return None

    def _get_model_bigquery_dev(self) -> Optional[dict[str, Any]]:
        """Get model from BigQuery in dev mode.

        Subclasses can override this method to provide BigQuery fallback
        for dev mode. Default implementation returns None.

        Returns:
            Model-like data from BigQuery, or None
        """
        return None

    def emit_warnings(self, warnings: list[dict[str, str]]):
        """Emit warnings to user.

        Args:
            warnings: List of warning dictionaries
        """
        if warnings:
            _print_warnings(warnings, self.json_output)

    def run(self) -> Any:
        """Execute command and return result.

        This is a convenience method that calls execute() and can be
        used by CLI layer. Subclasses should implement execute() instead.

        Returns:
            Command result
        """
        return self.execute()
