"""Configuration management for dbt-meta CLI.

Centralizes all environment variable handling with validation and defaults.
"""

import os
from dataclasses import dataclass
from typing import List, Optional
from pathlib import Path


def _parse_bool(value: str) -> bool:
    """Parse string to boolean.

    Args:
        value: String value to parse

    Returns:
        True for 'true', '1', 'yes' (case-insensitive), False otherwise
    """
    return value.lower() in ('true', '1', 'yes')


def _calculate_dev_schema() -> str:
    """Calculate dev schema name using environment variables.

    Priority (simplified from 4-level to 2-level):
    1. DBT_DEV_SCHEMA - Direct schema name (highest priority)
    2. Default: personal_{username}

    Returns:
        Dev schema name (e.g., 'personal_alice')
    """
    # Priority 1: Direct schema name
    if dev_dataset := os.getenv('DBT_DEV_SCHEMA'):
        return dev_dataset

    # Priority 2: Default with username
    username = os.getenv('USER', 'user')
    return f'personal_{username}'


@dataclass
class Config:
    """Centralized configuration from environment variables.

    All configuration is loaded from environment variables with sensible defaults.
    Provides validation and helper methods for common operations.

    Attributes:
        prod_manifest_path: Path to production manifest
        dev_manifest_path: Path to dev manifest
        prod_catalog_path: Path to production catalog (optional)
        dev_catalog_path: Path to dev catalog (optional)
        fallback_dev_enabled: Whether to fall back to dev manifest
        fallback_bigquery_enabled: Whether to fall back to BigQuery
        fallback_catalog_enabled: Whether to fall back to catalog.json
        dev_dataset: Dev schema/dataset name
        prod_table_name_strategy: Strategy for prod table naming
        prod_schema_source: Source for prod schema name
    """

    # Manifest paths
    prod_manifest_path: str
    dev_manifest_path: str

    # Fallback control
    fallback_dev_enabled: bool
    fallback_bigquery_enabled: bool

    # Dev schema naming
    dev_dataset: str

    # Production table naming
    prod_table_name_strategy: str  # alias_or_name | name | alias
    prod_schema_source: str  # config_or_model | model | config

    # Catalog paths (optional - must be after required fields)
    prod_catalog_path: Optional[str] = None
    dev_catalog_path: Optional[str] = None
    fallback_catalog_enabled: bool = True

    @classmethod
    def from_env(cls) -> 'Config':
        """Load configuration from environment variables.

        Returns:
            Config instance with values from environment

        Environment variables:
            DBT_PROD_MANIFEST_PATH: Production manifest path
            DBT_DEV_MANIFEST_PATH: Dev manifest path
            DBT_PROD_CATALOG_PATH: Production catalog path (optional)
            DBT_DEV_CATALOG_PATH: Dev catalog path (optional)
            DBT_FALLBACK_TARGET: Enable dev manifest fallback (default: true)
            DBT_FALLBACK_BIGQUERY: Enable BigQuery fallback (default: true)
            DBT_FALLBACK_CATALOG: Enable catalog.json fallback (default: true)
            DBT_DEV_SCHEMA: Dev schema name
            DBT_PROD_TABLE_NAME: Table naming strategy
            DBT_PROD_SCHEMA_SOURCE: Schema source
        """
        # Expand home directory in paths
        prod_path = os.getenv('DBT_PROD_MANIFEST_PATH', '~/dbt-state/manifest.json')
        dev_path = os.getenv('DBT_DEV_MANIFEST_PATH', './target/manifest.json')
        prod_catalog = os.getenv('DBT_PROD_CATALOG_PATH', '~/dbt-state/catalog.json')
        dev_catalog = os.getenv('DBT_DEV_CATALOG_PATH', './target/catalog.json')

        return cls(
            prod_manifest_path=str(Path(prod_path).expanduser()),
            dev_manifest_path=str(Path(dev_path).expanduser()),
            prod_catalog_path=str(Path(prod_catalog).expanduser()) if prod_catalog else None,
            dev_catalog_path=str(Path(dev_catalog).expanduser()) if dev_catalog else None,
            fallback_dev_enabled=_parse_bool(os.getenv('DBT_FALLBACK_TARGET', 'true')),
            fallback_bigquery_enabled=_parse_bool(os.getenv('DBT_FALLBACK_BIGQUERY', 'true')),
            fallback_catalog_enabled=_parse_bool(os.getenv('DBT_FALLBACK_CATALOG', 'true')),
            dev_dataset=_calculate_dev_schema(),
            prod_table_name_strategy=os.getenv('DBT_PROD_TABLE_NAME', 'alias_or_name'),
            prod_schema_source=os.getenv('DBT_PROD_SCHEMA_SOURCE', 'config_or_model'),
        )

    def validate(self) -> List[str]:
        """Validate configuration and return warnings.

        Returns:
            List of warning messages (empty if all valid)
        """
        warnings = []

        # Validate prod table name strategy
        valid_table_strategies = ('alias_or_name', 'name', 'alias')
        if self.prod_table_name_strategy not in valid_table_strategies:
            warnings.append(
                f"Invalid DBT_PROD_TABLE_NAME: '{self.prod_table_name_strategy}'. "
                f"Valid values: {', '.join(valid_table_strategies)}. "
                f"Using default: 'alias_or_name'"
            )
            self.prod_table_name_strategy = 'alias_or_name'

        # Validate prod schema source
        valid_schema_sources = ('config_or_model', 'model', 'config')
        if self.prod_schema_source not in valid_schema_sources:
            warnings.append(
                f"Invalid DBT_PROD_SCHEMA_SOURCE: '{self.prod_schema_source}'. "
                f"Valid values: {', '.join(valid_schema_sources)}. "
                f"Using default: 'config_or_model'"
            )
            self.prod_schema_source = 'config_or_model'

        # Check if production manifest exists and is a file
        prod_path = Path(self.prod_manifest_path)
        if not prod_path.exists():
            warnings.append(
                f"Production manifest not found: {self.prod_manifest_path}"
            )
        elif prod_path.is_dir():
            warnings.append(
                f"Production manifest path is a directory, not a file: {self.prod_manifest_path}"
            )

        return warnings
