"""Fallback strategy for model metadata retrieval.

Implements 3-level fallback system:
1. Production manifest
2. Dev manifest (if enabled)
3. BigQuery (if enabled)
"""

from __future__ import annotations

import subprocess
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import TYPE_CHECKING, Any

from dbt_meta.errors import ManifestNotFoundError, ManifestParseError, ModelNotFoundError
from dbt_meta.manifest.parser import ManifestParser

if TYPE_CHECKING:
    from dbt_meta.config import Config


class FallbackLevel(Enum):
    """Fallback levels in priority order."""

    PROD_MANIFEST = "production manifest"
    DEV_MANIFEST = "dev manifest"
    BIGQUERY = "BigQuery"


@dataclass
class FallbackResult:
    """Result from fallback attempt.

    Attributes:
        data: Model data if found, None otherwise
        level: Level where data was found
        warnings: List of warnings collected during fallback
    """

    data: dict[str, Any] | None
    level: FallbackLevel | None
    warnings: list[str]

    @property
    def found(self) -> bool:
        """Check if model data was found."""
        return self.data is not None


class FallbackStrategy:
    """Handles 3-level fallback: prod manifest → dev manifest → BigQuery.

    This class consolidates the fallback logic that was previously duplicated
    across 10+ command functions. It provides a clean interface for trying
    multiple data sources in priority order.

    Example:
        >>> from dbt_meta.config import Config
        >>> config = Config.from_config_or_env()
        >>> parser = ManifestParser(config.prod_manifest_path)
        >>> fallback = FallbackStrategy(config)
        >>> result = fallback.get_model(
        ...     model_name="core__clients",
        ...     prod_parser=parser,
        ...     allowed_levels=[FallbackLevel.PROD_MANIFEST, FallbackLevel.DEV_MANIFEST]
        ... )
        >>> if result.found:
        ...     print(f"Found in: {result.level.value}")
    """

    def __init__(self, config: Config, prod_manifest_path: str | None = None):
        """Initialize fallback strategy with configuration.

        Args:
            config: Configuration object with fallback settings
            prod_manifest_path: Path to production manifest (for finding dev manifest)
        """
        self.config = config
        self.prod_manifest_path = prod_manifest_path
        self._searched_levels: list[FallbackLevel] = []

    def get_model(
        self,
        model_name: str,
        prod_parser: ManifestParser,
        allowed_levels: list[FallbackLevel] | None = None,
    ) -> FallbackResult:
        """Try to fetch model with multi-level fallback.

        Args:
            model_name: Model name to fetch (e.g., "core__clients")
            prod_parser: Production manifest parser (already initialized)
            allowed_levels: Which fallback levels to try (default: all)

        Returns:
            FallbackResult with data, level found, and warnings

        Raises:
            ModelNotFoundError: If model not found in any allowed level

        Example:
            >>> result = fallback.get_model(
            ...     "staging__orders",
            ...     prod_parser,
            ...     allowed_levels=[FallbackLevel.PROD_MANIFEST]
            ... )
        """
        if allowed_levels is None:
            allowed_levels = list(FallbackLevel)

        result = FallbackResult(data=None, level=None, warnings=[])
        self._searched_levels = []

        # Level 1: Production manifest
        if FallbackLevel.PROD_MANIFEST in allowed_levels:
            self._searched_levels.append(FallbackLevel.PROD_MANIFEST)
            data = prod_parser.get_model(model_name)
            if data:
                return FallbackResult(
                    data=data,
                    level=FallbackLevel.PROD_MANIFEST,
                    warnings=[]
                )

        # Level 2: Dev manifest
        if FallbackLevel.DEV_MANIFEST in allowed_levels and self.config.fallback_dev_enabled:
            self._searched_levels.append(FallbackLevel.DEV_MANIFEST)
            try:
                dev_parser = self._get_dev_parser()
                data = dev_parser.get_model(model_name)
                if data:
                    result.warnings.append(
                        f"Using dev manifest: {self.config.dev_manifest_path}"
                    )
                    return FallbackResult(
                        data=data,
                        level=FallbackLevel.DEV_MANIFEST,
                        warnings=result.warnings
                    )
            except (FileNotFoundError, OSError, KeyError, ValueError, ManifestNotFoundError, ManifestParseError) as e:
                # Dev manifest not available, continue to next level
                # FileNotFoundError: manifest file doesn't exist
                # OSError/IOError: file system issues
                # KeyError: manifest structure issues
                # ValueError: JSON parsing issues
                # ManifestNotFoundError: manifest not found (from _get_dev_parser)
                # ManifestParseError: invalid JSON in dev manifest
                result.warnings.append(f"Dev manifest not available: {e.__class__.__name__}")

        # Level 3: BigQuery
        if FallbackLevel.BIGQUERY in allowed_levels and self.config.fallback_bigquery_enabled:
            self._searched_levels.append(FallbackLevel.BIGQUERY)
            try:
                data = self._fetch_from_bigquery(model_name)
                if data:
                    result.warnings.append(
                        "Using BigQuery fallback (partial metadata - lineage unavailable)"
                    )
                    return FallbackResult(
                        data=data,
                        level=FallbackLevel.BIGQUERY,
                        warnings=result.warnings
                    )
            except (ImportError, subprocess.CalledProcessError, subprocess.TimeoutExpired, ValueError) as e:
                # BigQuery fallback failed
                # ImportError: BigQuery utils not available
                # CalledProcessError: bq command failed
                # TimeoutExpired: bq command timed out
                # ValueError: parsing issues
                result.warnings.append(f"BigQuery fallback failed: {e.__class__.__name__}")

        # Not found in any level
        raise ModelNotFoundError(
            model_name=model_name,
            searched_locations=[level.value for level in self._searched_levels]
        )

    def _get_dev_parser(self) -> ManifestParser:
        """Get dev manifest parser.

        Uses _find_dev_manifest() if prod_manifest_path provided,
        otherwise uses config.dev_manifest_path.

        Returns:
            ManifestParser for dev manifest

        Raises:
            ManifestNotFoundError: If dev manifest doesn't exist
        """
        # Find dev manifest relative to production manifest if provided
        if self.prod_manifest_path:
            from dbt_meta.utils.dev import find_dev_manifest
            dev_path = find_dev_manifest(self.prod_manifest_path)
            if not dev_path:
                from dbt_meta.errors import ManifestNotFoundError
                raise ManifestNotFoundError(searched_paths=["target/manifest.json (relative to prod)"])
        else:
            dev_path = self.config.dev_manifest_path
            if not Path(dev_path).exists():
                from dbt_meta.errors import ManifestNotFoundError
                raise ManifestNotFoundError(searched_paths=[dev_path])

        return ManifestParser(dev_path)

    def _fetch_from_bigquery(self, model_name: str) -> dict[str, Any] | None:
        """Fetch metadata from BigQuery.

        Args:
            model_name: Model name (e.g., "core__clients")

        Returns:
            Partial metadata from BigQuery or None if not available

        Note:
            BigQuery fallback provides limited metadata:
            - Schema, table, columns (available)
            - Lineage, SQL, dependencies (NOT available)
        """
        # Import BigQuery functions from utils module
        try:
            from dbt_meta.utils.bigquery import (
                fetch_table_metadata_from_bigquery,
                infer_table_parts,
            )
        except ImportError:
            return None

        # Infer dataset and table from model name
        dataset, table = infer_table_parts(model_name)

        # Fetch metadata from BigQuery
        metadata = fetch_table_metadata_from_bigquery(dataset, table)

        if not metadata:
            return None

        # Return model-like structure with BigQuery metadata
        return {
            'schema': dataset,
            'name': table,
            'alias': table,
            'database': metadata.get('tableReference', {}).get('projectId', ''),
            'config': {
                'materialized': 'table' if metadata.get('type') == 'TABLE' else 'view'
            },
            '_bigquery_metadata': metadata  # Store original metadata
        }
