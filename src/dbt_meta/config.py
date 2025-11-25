"""Configuration management for dbt-meta CLI.

Centralizes all configuration handling with TOML file support.
Priority: CLI flags > TOML config > defaults
"""

import os
import re
import sys
import warnings
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Optional

# TOML support: Python 3.11+ has tomllib, earlier versions need tomli
try:
    import tomllib
except ImportError:
    try:
        import tomli as tomllib  # type: ignore
    except ImportError:
        tomllib = None  # type: ignore


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

    Note:
        Sanitizes username for BigQuery compatibility.
        BigQuery dataset names can only contain letters, numbers, and underscores.
        All other characters are replaced with underscores.
    """
    # Priority 1: Direct schema name
    if dev_dataset := os.getenv('DBT_DEV_SCHEMA'):
        return dev_dataset

    # Priority 2: Default with username
    username = os.getenv('USER', 'user')
    # Replace all non-alphanumeric characters (except underscore) with underscores
    # BigQuery dataset names: only letters (a-z, A-Z), numbers (0-9), underscores (_)
    username_sanitized = re.sub(r'[^a-zA-Z0-9_]', '_', username)
    return f'personal_{username_sanitized}'


@dataclass
class Config:
    """Centralized configuration from TOML file or environment variables.

    All configuration can be loaded from:
    1. TOML config file (~/.config/dbt-meta/config.toml)
    2. Environment variables (deprecated, for backward compatibility)
    3. Built-in defaults

    Priority: CLI flags > TOML > Env vars > Defaults

    Attributes:
        Manifest paths:
            prod_manifest_path: Path to production manifest
            dev_manifest_path: Path to dev manifest
            prod_catalog_path: Path to production catalog (optional)
            dev_catalog_path: Path to dev catalog (optional)

        Fallback control:
            fallback_dev_enabled: Whether to fall back to dev manifest
            fallback_bigquery_enabled: Whether to fall back to BigQuery
            fallback_catalog_enabled: Whether to fall back to catalog.json

        Dev environment:
            dev_dataset: Dev schema/dataset name
            dev_user: Username for dev schema generation

        Production naming:
            prod_table_name_strategy: Strategy for prod table naming
            prod_schema_source: Source for prod schema name

        BigQuery settings:
            bigquery_project_id: GCP project ID (optional, auto-detect)
            bigquery_timeout: Query timeout in seconds
            bigquery_retries: Number of retry attempts
            bigquery_location: BigQuery location/region

        Database settings (future):
            database_type: Database type (postgresql, redshift, snowflake)
            database_host: Database host
            database_port: Database port
            database_name: Database name
            database_username: Database username
            database_password: Database password

        Output settings:
            output_default_format: Default output format (text, json, table)
            output_json_pretty: Pretty-print JSON
            output_color: Color output (auto, always, never)
            output_show_source: Show data source in output

        Defer settings (future):
            defer_auto_sync: Auto-sync manifest before defer
            defer_sync_threshold: Max age before re-sync (seconds)
            defer_sync_command: Custom sync command
            defer_target: Target name for defer builds
    """

    # ===== Manifest paths =====
    prod_manifest_path: str = "~/dbt-state/manifest.json"
    dev_manifest_path: str = "./target/manifest.json"
    prod_catalog_path: Optional[str] = "~/dbt-state/catalog.json"
    dev_catalog_path: Optional[str] = "./target/catalog.json"

    # ===== Fallback control =====
    fallback_dev_enabled: bool = True
    fallback_bigquery_enabled: bool = True
    fallback_catalog_enabled: bool = True

    # ===== Dev environment =====
    dev_dataset: str = field(default_factory=_calculate_dev_schema)
    dev_user: Optional[str] = None

    # ===== Production naming =====
    prod_table_name_strategy: str = "alias_or_name"  # alias_or_name | name | alias
    prod_schema_source: str = "config_or_model"  # config_or_model | model | config

    # ===== BigQuery settings =====
    bigquery_project_id: Optional[str] = None
    bigquery_timeout: int = 10
    bigquery_retries: int = 3
    bigquery_location: str = "US"

    # ===== Database settings (future) =====
    database_type: Optional[str] = None
    database_host: Optional[str] = None
    database_port: Optional[int] = None
    database_name: Optional[str] = None
    database_username: Optional[str] = None
    database_password: Optional[str] = None

    # ===== Output settings =====
    output_default_format: str = "text"  # text | json | table
    output_json_pretty: bool = True
    output_color: str = "auto"  # auto | always | never
    output_show_source: bool = True

    # ===== Defer settings (future) =====
    defer_auto_sync: bool = True
    defer_sync_threshold: int = 3600  # 1 hour
    defer_sync_command: Optional[str] = None
    defer_target: str = "dev"

    @staticmethod
    def find_config_file() -> Optional[Path]:
        """Find config file in standard locations.

        Search order:
        1. ./.dbt-meta.toml (project-local override)
        2. ~/.config/dbt-meta/config.toml (XDG standard, recommended)
        3. ~/.dbt-meta.toml (user home fallback)

        Returns:
            Path to config file if found, None otherwise
        """
        search_paths = [
            Path.cwd() / ".dbt-meta.toml",  # Project-local
            Path.home() / ".config" / "dbt-meta" / "config.toml",  # XDG standard
            Path.home() / ".dbt-meta.toml",  # Home fallback
        ]

        for path in search_paths:
            if path.exists() and path.is_file():
                return path

        return None

    @classmethod
    def from_toml(cls, config_path: Optional[Path] = None) -> 'Config':
        """Load configuration from TOML file.

        Args:
            config_path: Path to config file (if None, auto-detect)

        Returns:
            Config instance with values from TOML file

        Raises:
            FileNotFoundError: If config_path specified but not found
            ValueError: If TOML syntax invalid or tomli not installed
        """
        if tomllib is None:
            raise ValueError(
                "TOML support requires 'tomli' package for Python <3.11. "
                "Install with: pip install tomli"
            )

        # Auto-detect config file if not specified
        if config_path is None:
            config_path = cls.find_config_file()
            if config_path is None:
                # No config file found, return defaults
                return cls()

        # Explicit path must exist
        if not config_path.exists():
            raise FileNotFoundError(f"Config file not found: {config_path}")

        # Parse TOML
        with open(config_path, 'rb') as f:
            try:
                data = tomllib.load(f)
            except Exception as e:
                raise ValueError(f"Invalid TOML syntax in {config_path}: {e}") from e

        # Extract values from TOML sections
        config = cls()

        # [manifest] section
        if 'manifest' in data:
            m = data['manifest']
            if 'prod_path' in m:
                config.prod_manifest_path = str(Path(m['prod_path']).expanduser())
            if 'dev_path' in m:
                config.dev_manifest_path = str(Path(m['dev_path']).expanduser())

        # [catalog] section
        if 'catalog' in data:
            c = data['catalog']
            if 'prod_path' in c:
                config.prod_catalog_path = str(Path(c['prod_path']).expanduser())
            if 'dev_path' in c:
                config.dev_catalog_path = str(Path(c['dev_path']).expanduser())

        # [fallback] section
        if 'fallback' in data:
            f = data['fallback']
            if 'dev_enabled' in f:
                config.fallback_dev_enabled = f['dev_enabled']
            if 'catalog_enabled' in f:
                config.fallback_catalog_enabled = f['catalog_enabled']
            if 'bigquery_enabled' in f:
                config.fallback_bigquery_enabled = f['bigquery_enabled']

        # [dev] section
        if 'dev' in data:
            d = data['dev']
            if 'schema' in d:
                config.dev_dataset = d['schema']
            if 'user' in d:
                config.dev_user = d['user']

        # [production] section
        if 'production' in data:
            p = data['production']
            if 'table_name_strategy' in p:
                config.prod_table_name_strategy = p['table_name_strategy']
            if 'schema_source' in p:
                config.prod_schema_source = p['schema_source']

        # [bigquery] section
        if 'bigquery' in data:
            bq = data['bigquery']
            if 'project_id' in bq:
                config.bigquery_project_id = bq['project_id']
            if 'timeout' in bq:
                config.bigquery_timeout = int(bq['timeout'])
            if 'retries' in bq:
                config.bigquery_retries = int(bq['retries'])
            if 'location' in bq:
                config.bigquery_location = bq['location']

        # [database] section (future)
        if 'database' in data:
            db = data['database']
            if 'type' in db:
                config.database_type = db['type']
            if 'host' in db:
                config.database_host = db['host']
            if 'port' in db:
                config.database_port = int(db['port'])
            if 'name' in db:  # Use 'name' not 'database' to avoid redundancy
                config.database_name = db['name']
            if 'username' in db:
                config.database_username = db['username']
            if 'password' in db:
                config.database_password = db['password']

        # [output] section
        if 'output' in data:
            o = data['output']
            if 'default_format' in o:
                config.output_default_format = o['default_format']
            if 'json_pretty' in o:
                config.output_json_pretty = o['json_pretty']
            if 'color' in o:
                config.output_color = o['color']
            if 'show_source' in o:
                config.output_show_source = o['show_source']

        # [defer] section (future)
        if 'defer' in data:
            df = data['defer']
            if 'auto_sync' in df:
                config.defer_auto_sync = df['auto_sync']
            if 'sync_threshold' in df:
                config.defer_sync_threshold = int(df['sync_threshold'])
            if 'sync_command' in df:
                config.defer_sync_command = df['sync_command']
            if 'target' in df:
                config.defer_target = df['target']

        return config

    @classmethod
    def from_env(cls) -> 'Config':
        """Load configuration from environment variables (DEPRECATED).

        This method is deprecated in favor of TOML config files.
        Use Config.from_toml() or Config.from_config_or_env() instead.

        Returns:
            Config instance with values from environment

        Environment variables:
            DBT_PROD_MANIFEST_PATH: Production manifest path
            DBT_DEV_MANIFEST_PATH: Dev manifest path
            DBT_PROD_CATALOG_PATH: Production catalog path
            DBT_DEV_CATALOG_PATH: Dev catalog path
            DBT_FALLBACK_TARGET: Enable dev manifest fallback
            DBT_FALLBACK_BIGQUERY: Enable BigQuery fallback
            DBT_FALLBACK_CATALOG: Enable catalog.json fallback
            DBT_DEV_SCHEMA: Dev schema name
            DBT_PROD_TABLE_NAME: Table naming strategy
            DBT_PROD_SCHEMA_SOURCE: Schema source
        """
        # Show deprecation warning
        warnings.warn(
            "Config.from_env() is deprecated and will be removed in v1.0. "
            "Please migrate to TOML config file. "
            "Run 'meta config init' to generate config template.",
            DeprecationWarning,
            stacklevel=2
        )

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

    @classmethod
    def from_config_or_env(cls, config_path: Optional[Path] = None) -> 'Config':
        """Load configuration from TOML file with env var fallback.

        Priority:
        1. TOML config file (recommended)
        2. Environment variables (deprecated)
        3. Built-in defaults

        Args:
            config_path: Path to config file (if None, auto-detect)

        Returns:
            Config instance with merged values
        """
        # Try TOML first
        try:
            config = cls.from_toml(config_path)
            # If config file was found, don't check env vars
            if config_path or cls.find_config_file():
                return config
        except ValueError as e:
            # TOML parsing error - show warning but continue with env vars
            print(f"Warning: {e}", file=sys.stderr)
            print("Falling back to environment variables...", file=sys.stderr)

        # Fallback to env vars (with deprecation warning suppressed if TOML failed)
        with warnings.catch_warnings():
            if config_path or cls.find_config_file():
                # Config file exists but failed to parse - don't suppress warning
                pass
            else:
                # No config file - suppress deprecation warning for now
                warnings.filterwarnings('ignore', category=DeprecationWarning)
            return cls.from_env()

    def validate(self) -> list[str]:
        """Validate configuration and return warnings.

        Returns:
            List of warning messages (empty if all valid)
        """
        warnings_list = []

        # Validate prod table name strategy
        valid_table_strategies = ('alias_or_name', 'name', 'alias')
        if self.prod_table_name_strategy not in valid_table_strategies:
            warnings_list.append(
                f"Invalid prod_table_name_strategy: '{self.prod_table_name_strategy}'. "
                f"Valid values: {', '.join(valid_table_strategies)}. "
                f"Using default: 'alias_or_name'"
            )
            self.prod_table_name_strategy = 'alias_or_name'

        # Validate prod schema source
        valid_schema_sources = ('config_or_model', 'model', 'config')
        if self.prod_schema_source not in valid_schema_sources:
            warnings_list.append(
                f"Invalid prod_schema_source: '{self.prod_schema_source}'. "
                f"Valid values: {', '.join(valid_schema_sources)}. "
                f"Using default: 'config_or_model'"
            )
            self.prod_schema_source = 'config_or_model'

        # Validate output format
        valid_formats = ('text', 'json', 'table')
        if self.output_default_format not in valid_formats:
            warnings_list.append(
                f"Invalid output_default_format: '{self.output_default_format}'. "
                f"Valid values: {', '.join(valid_formats)}. "
                f"Using default: 'text'"
            )
            self.output_default_format = 'text'

        # Validate color setting
        valid_colors = ('auto', 'always', 'never')
        if self.output_color not in valid_colors:
            warnings_list.append(
                f"Invalid output_color: '{self.output_color}'. "
                f"Valid values: {', '.join(valid_colors)}. "
                f"Using default: 'auto'"
            )
            self.output_color = 'auto'

        # Check if production manifest exists and is a file
        prod_path = Path(self.prod_manifest_path)
        if not prod_path.exists():
            warnings_list.append(
                f"Production manifest not found: {self.prod_manifest_path}"
            )
        elif prod_path.is_dir():
            warnings_list.append(
                f"Production manifest path is a directory, not a file: {self.prod_manifest_path}"
            )

        return warnings_list

    def to_dict(self) -> dict[str, Any]:
        """Convert config to dictionary for display.

        Returns:
            Dictionary representation of config
        """
        from dataclasses import asdict
        return asdict(self)
