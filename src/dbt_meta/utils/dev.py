"""Dev environment utilities for dbt-meta

Handles dev manifest discovery, dev schema/dataset naming, and dev table naming patterns.
"""

import os
import sys
import getpass
from pathlib import Path
from typing import Optional, Dict
from datetime import datetime


def find_dev_manifest(prod_manifest_path: str) -> Optional[str]:
    """
    Find dev manifest (target/manifest.json) in current directory or upward.

    Searches for target/manifest.json in:
    1. Current directory (./target/manifest.json)
    2. Parent directories up to 5 levels
    3. Production manifest project root (fallback)

    Args:
        prod_manifest_path: Path to production manifest (used for fallback only)

    Returns:
        Path to dev manifest if exists, None otherwise
    """
    try:
        # PRIORITY 1: Search from current directory upward
        current = Path.cwd()
        for _ in range(5):  # Search up to 5 levels
            dev_manifest = current / 'target' / 'manifest.json'
            if dev_manifest.exists():
                return str(dev_manifest.absolute())
            if current.parent == current:  # Reached filesystem root
                break
            current = current.parent

        # PRIORITY 2: Fallback to production manifest location
        # (for cases where command runs from outside project)
        prod_path = Path(prod_manifest_path)
        project_root = prod_path.parent.parent
        dev_manifest = project_root / 'target' / 'manifest.json'

        if dev_manifest.exists():
            return str(dev_manifest.absolute())

        return None

    except Exception:  # pragma: no cover
        return None


def calculate_dev_schema() -> str:
    """
    Calculate dev schema/dataset name for development tables.

    Environment variables (simplified priority):
    1. DBT_DEV_DATASET - Full dataset name (REQUIRED, e.g., "personal_alice")
    2. Legacy fallback for backward compatibility:
       - DBT_DEV_SCHEMA (alias for DBT_DEV_DATASET)
       - DBT_DEV_SCHEMA_TEMPLATE with {username} placeholder
       - DBT_DEV_SCHEMA_PREFIX + username

    Returns:
        Dev dataset name (e.g., "personal_alice")

    Raises:
        ValueError: If no dev dataset is configured

    Example:
        export DBT_DEV_DATASET="personal_alice"
        meta schema --dev model_name  # → personal_alice.table_name
    """
    # Get username for templates
    username = os.environ.get('DBT_USER') or os.environ.get('USER') or getpass.getuser()
    username = username.replace('.', '_')

    # Primary: DBT_DEV_DATASET (recommended)
    dev_dataset = os.environ.get('DBT_DEV_DATASET')

    if dev_dataset:
        # Validate and return
        return validate_dev_dataset(dev_dataset)

    # Legacy support: DBT_DEV_SCHEMA (deprecated, use DBT_DEV_DATASET)
    dev_schema = os.environ.get('DBT_DEV_SCHEMA')

    if dev_schema:
        print("⚠️  DBT_DEV_SCHEMA is deprecated, use DBT_DEV_DATASET instead", file=sys.stderr)
        return validate_dev_dataset(dev_schema)

    # Legacy template/prefix support (for backward compatibility)
    has_template = 'DBT_DEV_SCHEMA_TEMPLATE' in os.environ
    has_prefix = 'DBT_DEV_SCHEMA_PREFIX' in os.environ

    if has_template:
        template = os.environ.get('DBT_DEV_SCHEMA_TEMPLATE', '')
        print("⚠️  DBT_DEV_SCHEMA_TEMPLATE is deprecated, use DBT_DEV_DATASET instead", file=sys.stderr)
        if template:
            result = template.format(username=username)
            return validate_dev_dataset(result)
        # Empty template - fallback to prefix logic
        has_template = False

    if has_prefix:
        prefix = os.environ.get('DBT_DEV_SCHEMA_PREFIX', '')
        print("⚠️  DBT_DEV_SCHEMA_PREFIX is deprecated, use DBT_DEV_DATASET instead", file=sys.stderr)
        result = f"{prefix}_{username}" if prefix else username
        return validate_dev_dataset(result)

    # No legacy vars set - use default for backward compatibility
    # (This maintains v0.3.0 behavior when no env vars are set)
    dev_dataset = f"personal_{username}"
    return validate_dev_dataset(dev_dataset)


def validate_dev_dataset(dataset: str) -> str:
    """
    Apply BigQuery validation to dev dataset name if enabled.

    Args:
        dataset: Dataset name to validate

    Returns:
        Validated (possibly sanitized) dataset name
    """
    if os.environ.get('DBT_VALIDATE_BIGQUERY', '').lower() in ('true', '1', 'yes'):
        from dbt_meta.utils.bigquery import sanitize_bigquery_name

        sanitized, warnings = sanitize_bigquery_name(dataset, "dataset")
        if warnings:
            for warning in warnings:
                print(f"⚠️  BigQuery validation: {warning}", file=sys.stderr)
        return sanitized
    return dataset


def build_dev_table_name(model: dict, model_name: str) -> str:
    """
    Build dev table name based on DBT_DEV_TABLE_PATTERN.

    Environment variable:
        DBT_DEV_TABLE_PATTERN - Table naming pattern (default: "name")

    Predefined patterns:
        - "name" (default): Use model filename
        - "alias": Use alias (fallback to name)

    Custom patterns with placeholders:
        - {name}: Model filename (e.g., "client_events")
        - {alias}: Model alias from config (fallback to name)
        - {username}: Current user (DBT_USER or $USER)
        - {model_name}: Full model name with __ (e.g., "core_client__events")
        - {folder}: Model folder (e.g., "core_client")
        - {date}: Current date YYYYMMDD (e.g., "20250205")

    Args:
        model: Model data from manifest
        model_name: Original dbt model name (e.g., "core_client__events")

    Returns:
        Table name for dev environment

    Examples:
        # Simple patterns
        DBT_DEV_TABLE_PATTERN="name"
        → "client_events"  (filename, default)

        DBT_DEV_TABLE_PATTERN="alias"
        → "events"  (from config.alias, or filename if no alias)

        # Custom patterns with placeholders
        DBT_DEV_TABLE_PATTERN="{username}_{name}"
        → "pavel_client_events"

        DBT_DEV_TABLE_PATTERN="tmp_{name}"
        → "tmp_client_events"  (temporary dev table)

        DBT_DEV_TABLE_PATTERN="{folder}_{name}"
        → "core_client_client_events"  (avoid name collisions)

        DBT_DEV_TABLE_PATTERN="{name}_{date}"
        → "client_events_20250205"  (date-stamped)

    Use cases:
        - Standard dev: "name" (default)
        - Shared dataset: "{username}_{name}"
        - Temporary work: "tmp_{name}"
        - Avoid collisions: "{folder}_{name}"
        - Time-based: "{name}_{date}"
    """
    pattern = os.environ.get('DBT_DEV_TABLE_PATTERN', 'name')

    # Extract values
    name = model.get('name', model_name)
    alias = model.get('config', {}).get('alias', '')
    username = os.environ.get('DBT_USER') or os.environ.get('USER') or getpass.getuser()
    username = username.replace('.', '_')

    # Extract folder from model_name (e.g., "core_client__events" → "core_client")
    folder = model_name.split('__')[0] if '__' in model_name else ''

    # Current date
    date = datetime.now().strftime('%Y%m%d')

    # Apply pattern
    if pattern == 'name':
        return name
    elif pattern == 'alias':
        return alias if alias else name
    elif '{' in pattern:
        # Custom pattern with placeholders
        try:
            return pattern.format(
                name=name,
                alias=alias if alias else name,
                username=username,
                model_name=model_name,
                folder=folder,
                date=date
            )
        except KeyError as e:
            # Unknown placeholder
            print(f"⚠️  Unknown placeholder in DBT_DEV_TABLE_PATTERN: {e}", file=sys.stderr)
            print(f"⚠️  Available: {{name}}, {{alias}}, {{username}}, {{model_name}}, {{folder}}, {{date}}", file=sys.stderr)
            # Fallback to name
            return name
    else:
        # Treat as literal string
        return pattern


def build_dev_schema_result(model: dict, model_name: str) -> Dict[str, str]:
    """
    Build dev schema result from model data.

    Args:
        model: Model data from manifest
        model_name: Original model name (for fallback)

    Returns:
        Dictionary with schema, table, full_name (dev format)

    Note: Dev tables use pattern from DBT_DEV_TABLE_PATTERN (default: filename)
    """
    dev_schema = calculate_dev_schema()
    table_name = build_dev_table_name(model, model_name)

    return {
        'schema': dev_schema,
        'table': table_name,
        'full_name': f"{dev_schema}.{table_name}"
    }
