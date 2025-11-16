"""
Commands - Model metadata extraction functions

Provides high-level commands for extracting metadata from dbt manifest.
Each command returns formatted data matching bash version output.
"""

import subprocess
import json as json_lib
import sys
import os
import re
import glob
from functools import lru_cache
from typing import Dict, List, Optional, Any
from dbt_meta.manifest.parser import ManifestParser
from dbt_meta.manifest.finder import ManifestFinder


@lru_cache(maxsize=2)
def _get_cached_parser(manifest_path: str) -> ManifestParser:
    """
    Get cached ManifestParser instance

    Uses LRU cache to avoid re-parsing the same manifest.
    Cache size = 2 to cache both production (.dbt-state) and dev (target/) manifests.

    Args:
        manifest_path: Path to manifest.json

    Returns:
        Cached ManifestParser instance
    """
    return ManifestParser(manifest_path)


def is_modified(model_name: str) -> bool:
    """
    Check if model file is modified in git (new or changed).

    Uses git diff to detect if the model's SQL file has uncommitted changes.

    Args:
        model_name: dbt model name (e.g., "core_client__events")

    Returns:
        True if model is new or modified, False otherwise or if git check fails
    """
    try:
        # Extract table name from model_name
        _, table = _infer_table_parts(model_name)

        # Check git diff for modified files
        result = subprocess.run(
            ['git', 'diff', '--name-only', 'HEAD'],
            capture_output=True,
            text=True,
            timeout=5
        )

        if result.returncode == 0:
            # Check if any modified file contains the table name
            modified_files = result.stdout.splitlines()
            for file_path in modified_files:
                if table in file_path and file_path.endswith('.sql'):
                    return True

        # Check git status for new files (untracked)
        result = subprocess.run(
            ['git', 'status', '--porcelain'],
            capture_output=True,
            text=True,
            timeout=5
        )

        if result.returncode == 0:
            # Check for new files (starting with ??)
            status_lines = result.stdout.splitlines()
            for line in status_lines:
                if line.startswith('??') or line.startswith('A '):
                    if table in line and line.endswith('.sql'):
                        return True

        return False

    except (subprocess.TimeoutExpired, FileNotFoundError, Exception):
        # If git check fails, assume not modified (safe default)
        return False


def _check_manifest_git_mismatch(
    model_name: str,
    use_dev: bool,
    dev_manifest_found: Optional[str] = None
) -> List[Dict[str, str]]:
    """
    Internal helper: Check git status and return structured warnings.

    Returns list of warning objects that can be output as JSON (with -j) or text (without -j).

    Warning types:
    - git_mismatch: Model modified in git but querying production
    - dev_without_changes: Using --dev but model not modified
    - dev_manifest_missing: Using --dev but dev manifest not found

    Args:
        model_name: dbt model name (e.g., "core_client__events")
        use_dev: Whether --dev flag was used
        dev_manifest_found: Path to dev manifest if found, None otherwise

    Returns:
        List of warning dictionaries with keys: type, severity, message, suggestion (optional)
    """
    warnings = []
    modified = is_modified(model_name)

    # Case 1: Using --dev but model NOT modified
    if use_dev and not modified:
        warnings.append({
            "type": "dev_without_changes",
            "severity": "warning",
            "message": f"Model '{model_name}' NOT modified in git, but using --dev flag",
            "detail": "Dev table may not exist or may be outdated",
            "suggestion": "Remove --dev flag to query production table"
        })

    # Case 2: NOT using --dev but model IS modified
    elif not use_dev and modified:
        warnings.append({
            "type": "git_mismatch",
            "severity": "warning",
            "message": f"Model '{model_name}' IS modified in git",
            "detail": "Querying production table, but local changes exist",
            "suggestion": "Use --dev flag to query dev table"
        })

    # Case 3: Using --dev but dev manifest not found
    if use_dev and dev_manifest_found is None:
        warnings.append({
            "type": "dev_manifest_missing",
            "severity": "error",
            "message": "Dev manifest (target/manifest.json) not found",
            "detail": "Dev table cannot be queried without manifest",
            "suggestion": f"Run 'defer run --select {model_name}' to build dev table"
        })

    return warnings


def _print_warnings(warnings: List[Dict[str, str]], json_output: bool = False) -> None:
    """
    Print warnings to stderr in JSON or text format.

    Args:
        warnings: List of warning dictionaries from _check_manifest_git_mismatch()
        json_output: If True, print as JSON. If False, print as colored text.
    """
    if not warnings:
        return

    if json_output:
        # Print as JSON for machine parsing (agents)
        print(json_lib.dumps({"warnings": warnings}), file=sys.stderr)
    else:
        # Print as colored text for humans
        for warning in warnings:
            severity = warning["severity"]
            message = warning["message"]
            detail = warning.get("detail", "")
            suggestion = warning.get("suggestion", "")

            # Map severity to icon and color
            if severity == "info":
                severity_icon = "ℹ️"
                color_code = "\033[36m"  # Cyan
                label = "INFO"
            elif severity == "warning":
                severity_icon = "⚠️"
                color_code = "\033[33m"  # Yellow
                label = "WARNING"
            else:  # error
                severity_icon = "❌"
                color_code = "\033[31m"  # Red
                label = "ERROR"

            reset_code = "\033[0m"

            print(f"{color_code}{severity_icon}  {label}: {message}{reset_code}", file=sys.stderr)
            if detail:
                print(f"   {detail}", file=sys.stderr)
            if suggestion:
                print(f"   Suggestion: {suggestion}", file=sys.stderr)


def _find_dev_manifest(prod_manifest_path: str) -> Optional[str]:
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
    from pathlib import Path
    import os

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


def _calculate_dev_schema() -> str:
    """
    Calculate dev schema/dataset name for development tables.

    Environment variables (simplified priority):
    1. DBT_DEV_DATASET - Full dataset name (REQUIRED, e.g., "personal_pavel_filianin")
    2. Legacy fallback for backward compatibility:
       - DBT_DEV_SCHEMA (alias for DBT_DEV_DATASET)
       - DBT_DEV_SCHEMA_TEMPLATE with {username} placeholder
       - DBT_DEV_SCHEMA_PREFIX + username

    Returns:
        Dev dataset name (e.g., "personal_pavel_filianin")

    Raises:
        ValueError: If no dev dataset is configured

    Example:
        export DBT_DEV_DATASET="personal_pavel_filianin"
        meta schema --dev model_name  # → personal_pavel_filianin.table_name
    """
    import os
    import getpass

    # Get username for templates
    username = os.environ.get('DBT_USER') or os.environ.get('USER') or getpass.getuser()
    username = username.replace('.', '_')

    # Primary: DBT_DEV_DATASET (recommended)
    dev_dataset = os.environ.get('DBT_DEV_DATASET')

    if dev_dataset:
        # Validate and return
        return _validate_dev_dataset(dev_dataset)

    # Legacy support: DBT_DEV_SCHEMA (deprecated, use DBT_DEV_DATASET)
    dev_schema = os.environ.get('DBT_DEV_SCHEMA')

    if dev_schema:
        print("⚠️  DBT_DEV_SCHEMA is deprecated, use DBT_DEV_DATASET instead", file=sys.stderr)
        return _validate_dev_dataset(dev_schema)

    # Legacy template/prefix support (for backward compatibility)
    has_template = 'DBT_DEV_SCHEMA_TEMPLATE' in os.environ
    has_prefix = 'DBT_DEV_SCHEMA_PREFIX' in os.environ

    if has_template:
        template = os.environ.get('DBT_DEV_SCHEMA_TEMPLATE', '')
        print("⚠️  DBT_DEV_SCHEMA_TEMPLATE is deprecated, use DBT_DEV_DATASET instead", file=sys.stderr)
        if template:
            result = template.format(username=username)
            return _validate_dev_dataset(result)
        # Empty template - fallback to prefix logic
        has_template = False

    if has_prefix:
        prefix = os.environ.get('DBT_DEV_SCHEMA_PREFIX', '')
        print("⚠️  DBT_DEV_SCHEMA_PREFIX is deprecated, use DBT_DEV_DATASET instead", file=sys.stderr)
        result = f"{prefix}_{username}" if prefix else username
        return _validate_dev_dataset(result)

    # No legacy vars set - use default for backward compatibility
    # (This maintains v0.3.0 behavior when no env vars are set)
    dev_dataset = f"personal_{username}"
    return _validate_dev_dataset(dev_dataset)


def _validate_dev_dataset(dataset: str) -> str:
    """
    Apply BigQuery validation to dev dataset name if enabled.

    Args:
        dataset: Dataset name to validate

    Returns:
        Validated (possibly sanitized) dataset name
    """
    if os.environ.get('DBT_VALIDATE_BIGQUERY', '').lower() in ('true', '1', 'yes'):
        sanitized, warnings = _sanitize_bigquery_name(dataset, "dataset")
        if warnings:
            for warning in warnings:
                print(f"⚠️  BigQuery validation: {warning}", file=sys.stderr)
        return sanitized
    return dataset


def _build_dev_table_name(model: dict, model_name: str) -> str:
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
    import os
    import getpass
    from datetime import datetime

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


def _build_dev_schema_result(model: dict, model_name: str) -> Dict[str, str]:
    """
    Build dev schema result from model data.

    Args:
        model: Model data from manifest
        model_name: Original model name (for fallback)

    Returns:
        Dictionary with schema, table, full_name (dev format)

    Note: Dev tables use pattern from DBT_DEV_TABLE_PATTERN (default: filename)
    """
    dev_schema = _calculate_dev_schema()
    table_name = _build_dev_table_name(model, model_name)

    return {
        'schema': dev_schema,
        'table': table_name,
        'full_name': f"{dev_schema}.{table_name}"
    }


def _sanitize_bigquery_name(name: str, name_type: str = "dataset") -> tuple[str, list[str]]:
    """
    Sanitize name for BigQuery compatibility

    BigQuery naming rules:
    - Dataset names: letters, numbers, underscores, hyphens (max 1024 chars)
    - Cannot contain: dots (except at start for project), @, spaces, other special chars
    - Must start with letter or underscore
    - Case-sensitive but treated as case-insensitive in some contexts

    Args:
        name: Name to sanitize
        name_type: Type of name ("dataset", "table", "column")

    Returns:
        Tuple of (sanitized_name, list of warnings)
    """
    warnings = []
    original = name

    # Check length
    if len(name) > 1024:
        warnings.append(f"Name too long ({len(name)} chars, max 1024)")
        name = name[:1024]

    # Replace invalid characters
    invalid_chars = set()

    # Dots are invalid (except in fully-qualified names with project)
    if '.' in name:
        invalid_chars.add('.')
        name = name.replace('.', '_')

    # @ is invalid
    if '@' in name:
        invalid_chars.add('@')
        name = name.replace('@', '_')

    # Spaces are invalid
    if ' ' in name:  # pragma: no cover
        invalid_chars.add(' ')
        name = name.replace(' ', '_')

    # Other special characters (keep only letters, numbers, underscores, hyphens)
    valid_pattern = re.compile(r'[^a-zA-Z0-9_\-]')
    other_invalid = valid_pattern.findall(name)
    if other_invalid:  # pragma: no cover
        invalid_chars.update(other_invalid)
        name = valid_pattern.sub('_', name)

    # Must start with letter or underscore
    if name and not (name[0].isalpha() or name[0] == '_'):
        warnings.append(f"Name must start with letter or underscore, got '{name[0]}'")
        name = f"_{name}"

    if invalid_chars:
        chars_str = ', '.join(f"'{c}'" for c in sorted(invalid_chars))
        warnings.append(f"Invalid BigQuery characters replaced: {chars_str}")

    if name != original and not warnings:  # pragma: no cover
        warnings.append(f"Name sanitized: '{original}' → '{name}'")

    return name, warnings


def _infer_table_parts(model_name: str) -> tuple[Optional[str], str]:
    """
    Extract dataset and table from dbt model name.

    Examples:
        'core_client__events' → ('core_client', 'events')
        'staging_sugarcrm__accounts' → ('staging_sugarcrm', 'accounts')
        'single_word' → (None, 'single_word')
        'core__client__events' → ('core__client', 'events')

    Args:
        model_name: dbt model name with __ separator

    Returns:
        Tuple of (dataset, table). dataset is None if no __ found.
    """
    if '__' not in model_name:
        return None, model_name

    # Split by __ and take last part as table, everything else as dataset
    parts = model_name.split('__')
    table = parts[-1]
    dataset = '__'.join(parts[:-1])

    return dataset, table


def _fetch_table_metadata_from_bigquery(
    dataset: str,
    table: str,
    database: Optional[str] = None
) -> Optional[dict]:
    """
    Fetch table metadata from BigQuery using bq show.

    Args:
        dataset: BigQuery dataset name (schema)
        table: BigQuery table name
        database: Optional project ID (if None, uses default project)

    Returns:
        Dictionary with BigQuery table metadata:
        {
            'tableReference': {
                'projectId': str,
                'datasetId': str,
                'tableId': str
            },
            'type': 'TABLE' | 'VIEW',
            'timePartitioning': {...},  # Optional
            'clustering': {...}  # Optional
        }
        None if bq command fails or table not found
    """
    # Construct full table name
    if database:
        full_table = f"{database}:{dataset}.{table}"
    else:
        full_table = f"{dataset}.{table}"

    # Execute bq show command
    try:
        result = _run_bq_command(['show', '--format=json', full_table], timeout=10)
        metadata = json_lib.loads(result.stdout)
        return metadata

    except (subprocess.CalledProcessError, subprocess.TimeoutExpired, FileNotFoundError, json_lib.JSONDecodeError):
        return None


def _run_bq_command(args: List[str], timeout: int = 10) -> subprocess.CompletedProcess:
    """
    Run bq command with PYTHONPATH workaround for dwh-pipeline projects.

    This replicates the logic from run_bq.sh to avoid Python module conflicts.

    Args:
        args: Command arguments (e.g., ['show', '--schema', 'table'])
        timeout: Command timeout in seconds

    Returns:
        CompletedProcess result

    Raises:
        subprocess.CalledProcessError, FileNotFoundError, subprocess.TimeoutExpired
    """
    import shutil

    # Find bq command
    bq_cmd = shutil.which('bq')
    if not bq_cmd:
        # Try hardcoded path (common on macOS with Homebrew)
        bq_cmd = '/opt/homebrew/bin/bq'
        if not os.path.exists(bq_cmd):
            bq_cmd = 'bq'  # Let subprocess raise FileNotFoundError

    # Save current PYTHONPATH and clear it (avoid conflicts with local modules)
    old_pythonpath = os.environ.get('PYTHONPATH')
    env = os.environ.copy()
    env['PYTHONPATH'] = ''

    try:
        result = subprocess.run(
            [bq_cmd] + args,
            capture_output=True,
            text=True,
            check=True,
            timeout=timeout,
            env=env
        )
        return result
    finally:
        # Restore PYTHONPATH (optional, process env is isolated anyway)
        if old_pythonpath is not None:
            os.environ['PYTHONPATH'] = old_pythonpath


def _fetch_columns_from_bigquery_direct(
    dataset: str,
    table: str,
    database: Optional[str] = None
) -> Optional[List[Dict[str, str]]]:
    """
    Fetch columns directly from BigQuery without requiring model in manifest.

    Args:
        dataset: BigQuery dataset name (schema)
        table: BigQuery table name
        database: Optional project ID (if None, uses default project)

    Returns:
        List of {name, data_type} dictionaries
        None if BigQuery fetch fails
    """
    # Construct full table name
    if database:
        full_table = f"{database}:{dataset}.{table}"
    else:
        full_table = f"{dataset}.{table}"

    # Check if bq command is available
    try:
        _run_bq_command(['version'], timeout=5)
    except (subprocess.CalledProcessError, FileNotFoundError, subprocess.TimeoutExpired):
        print(f"Error: bq command not found. Install Google Cloud SDK.", file=sys.stderr)
        return None

    # Fetch schema from BigQuery
    try:
        result = _run_bq_command(
            ['show', '--schema', '--format=prettyjson', full_table],
            timeout=10
        )

        # Parse JSON output
        bq_schema = json_lib.loads(result.stdout)

        # Convert to our format
        columns = [
            {
                'name': col['name'],
                'data_type': col['type'].lower()
            }
            for col in bq_schema
        ]

        return columns

    except subprocess.CalledProcessError:
        print(f"Error: Failed to fetch columns from BigQuery for table: {full_table}", file=sys.stderr)
        return None
    except (json_lib.JSONDecodeError, subprocess.TimeoutExpired):
        print(f"Error: Invalid response from BigQuery", file=sys.stderr)
        return None


def _fetch_columns_from_bigquery(manifest_path: str, model_name: str) -> Optional[List[Dict[str, str]]]:
    """
    Fallback: fetch columns from BigQuery when not in manifest

    Args:
        manifest_path: Path to manifest.json
        model_name: Model name

    Returns:
        List of {name, data_type} dictionaries
        None if BigQuery fetch fails

    Note: Prints warning to stderr about fallback
    """
    parser = _get_cached_parser(manifest_path)
    model = parser.get_model(model_name)

    if not model:
        return None

    # Get full table name (database:schema.table format for bq)
    database = model.get('database', '')
    schema_name = model.get('schema', '')
    config = model.get('config', {})
    table_name = config.get('alias', model.get('name', ''))

    full_table = f"{database}:{schema_name}.{table_name}"

    # Print warning to stderr (like bash version)
    print(f"⚠️  No columns documented in manifest, fetching from BigQuery...", file=sys.stderr)

    # Check if bq command is available
    try:
        _run_bq_command(['version'], timeout=5)
    except (subprocess.CalledProcessError, FileNotFoundError, subprocess.TimeoutExpired):
        print(f"Error: bq command not found. Install Google Cloud SDK.", file=sys.stderr)
        return None

    # Fetch schema from BigQuery
    try:
        result = _run_bq_command(
            ['show', '--schema', '--format=prettyjson', full_table],
            timeout=10
        )

        # Parse JSON output
        bq_schema = json_lib.loads(result.stdout)

        # Convert to our format
        columns = [
            {
                'name': col['name'],
                'data_type': col['type'].lower()
            }
            for col in bq_schema
        ]

        return columns

    except subprocess.CalledProcessError:
        print(f"Error: Failed to fetch columns from BigQuery for table: {full_table}", file=sys.stderr)
        return None
    except json_lib.JSONDecodeError:
        print(f"Error: Invalid JSON from BigQuery", file=sys.stderr)
        return None


def info(manifest_path: str, model_name: str, use_dev: bool = False, json_output: bool = False) -> Optional[Dict[str, Any]]:
    """
    Extract basic model information

    Args:
        manifest_path: Path to manifest.json
        model_name: Model name (e.g., "core_client__client_profiles_events")
        use_dev: If True, prioritize dev manifest over production

    Returns:
        Dictionary with:
        - name: Model name
        - database: BigQuery project (empty for dev)
        - schema: BigQuery dataset (dev schema for use_dev=True)
        - table: Table name (filename for dev, alias for prod)
        - full_name: database.schema.table (or schema.table for dev)
        - materialized: Materialization type
        - file: Relative file path
        - tags: List of tags
        - unique_id: Full unique identifier

        Returns None if model not found.

    Behavior with use_dev=True:
        - Searches dev manifest (target/) FIRST
        - Returns dev schema name (e.g., personal_USERNAME)
        - Uses model filename, NOT alias
        - Falls back to BigQuery if not in dev manifest
    """
    # Check git status and collect warnings
    dev_manifest = _find_dev_manifest(manifest_path) if use_dev else None
    warnings = _check_manifest_git_mismatch(model_name, use_dev, dev_manifest)
    _print_warnings(warnings, json_output)

    # Handle --dev flag: prioritize dev manifest
    if use_dev:  # pragma: no cover
        if not dev_manifest:
            dev_manifest = _find_dev_manifest(manifest_path)
        if dev_manifest:
            try:
                parser_dev = _get_cached_parser(dev_manifest)
                model = parser_dev.get_model(model_name)
                if model:
                    # Build dev info result
                    dev_schema = _calculate_dev_schema()
                    table_name = _build_dev_table_name(model, model_name)
                    config = model.get('config', {})

                    return {
                        'name': model_name,
                        'database': '',  # Dev doesn't use database
                        'schema': dev_schema,
                        'table': table_name,
                        'full_name': f"{dev_schema}.{table_name}",
                        'materialized': config.get('materialized', 'table'),
                        'file': model.get('original_file_path', ''),
                        'tags': model.get('tags', []),
                        'unique_id': model.get('unique_id', '')
                    }
            except Exception:  # pragma: no cover
                pass

        # Fallback to BigQuery for dev (if enabled)
        if os.environ.get('DBT_FALLBACK_BIGQUERY', 'true').lower() in ('true', '1', 'yes'):  # pragma: no cover
            # For dev mode, use full model name as table name (not split by __)
            dev_schema = _calculate_dev_schema()
            bq_metadata = _fetch_table_metadata_from_bigquery(dev_schema, model_name)
            if bq_metadata:
                table_ref = bq_metadata.get('tableReference', {})
                table_type = bq_metadata.get('type', 'TABLE')

                return {
                    'name': model_name,
                    'database': '',
                    'schema': dev_schema,
                    'table': model_name,
                    'full_name': f"{dev_schema}.{model_name}",
                    'materialized': 'table' if table_type == 'TABLE' else 'view',
                    'file': '',
                    'tags': [],
                    'unique_id': ''
                }

        return None

    # Default behavior: production first, then fallbacks
    parser = _get_cached_parser(manifest_path)
    model = parser.get_model(model_name)
    fallback_warnings = []

    if not model:
        # LEVEL 2 Fallback: Try dev manifest (target/)
        if os.environ.get('DBT_FALLBACK_TARGET', 'true').lower() in ('true', '1', 'yes'):
            dev_manifest = _find_dev_manifest(manifest_path)
            if dev_manifest:
                try:
                    parser_dev = _get_cached_parser(dev_manifest)
                    model = parser_dev.get_model(model_name)
                    if model:
                        fallback_warnings.append({
                            "type": "dev_manifest_fallback",
                            "severity": "warning",
                            "message": f"Model '{model_name}' not found in production manifest",
                            "detail": "Using dev manifest (target/manifest.json) as fallback",
                            "source": "LEVEL 2"
                        })
                        # Continue with model data processing below
                except Exception:
                    pass  # Fall through to BigQuery fallback

        # LEVEL 3 Fallback: Query BigQuery directly
        if not model and os.environ.get('DBT_FALLBACK_BIGQUERY', 'true').lower() in ('true', '1', 'yes'):  # pragma: no cover
            dataset, table = _infer_table_parts(model_name)
            if dataset:
                bq_metadata = _fetch_table_metadata_from_bigquery(dataset, table)
                if bq_metadata:
                    fallback_warnings.append({
                        "type": "bigquery_fallback",
                        "severity": "warning",
                        "message": f"Model '{model_name}' not in manifest",
                        "detail": "Using BigQuery metadata (missing: file path, tags, unique_id)",
                        "source": "LEVEL 3"
                    })
                    _print_warnings(fallback_warnings, json_output)

                    table_ref = bq_metadata.get('tableReference', {})
                    table_type = bq_metadata.get('type', 'TABLE')

                    return {
                        'name': model_name,
                        'database': table_ref.get('projectId', ''),
                        'schema': table_ref.get('datasetId', ''),
                        'table': table_ref.get('tableId', ''),
                        'full_name': f"{table_ref.get('projectId', '')}.{table_ref.get('datasetId', '')}.{table_ref.get('tableId', '')}",
                        'materialized': 'table' if table_type == 'TABLE' else 'view',
                        'file': '',  # Not available from BigQuery
                        'tags': [],  # Not available from BigQuery
                        'unique_id': ''  # Not available from BigQuery
                    }

        if not model:
            return None

    # Print fallback warnings if any
    if fallback_warnings:
        _print_warnings(fallback_warnings, json_output)

    # Extract config
    config = model.get('config', {})
    database = model.get('database', '')
    schema_name = model.get('schema', '')

    # Table name: use alias if present, otherwise model name
    table_name = config.get('alias', model.get('name', ''))

    return {
        'name': model_name,
        'database': database,
        'schema': schema_name,
        'table': table_name,
        'full_name': f"{database}.{schema_name}.{table_name}",
        'materialized': config.get('materialized', 'table'),
        'file': model.get('original_file_path', ''),
        'tags': model.get('tags', []),
        'unique_id': model.get('unique_id', '')
    }


def schema(manifest_path: str, model_name: str, use_dev: bool = False, json_output: bool = False) -> Optional[Dict[str, str]]:
    """
    Extract schema/table location information

    Args:
        manifest_path: Path to manifest.json
        model_name: Model name
        use_dev: If True, prioritize dev manifest and return dev schema name

    Returns:
        Dictionary with:
        - database: BigQuery project (prod) or empty (dev)
        - schema: BigQuery dataset (prod schema or dev schema like personal_USERNAME)
        - table: Table name (prod: alias or name, dev: filename)
        - full_name: database.schema.table (prod) or schema.table (dev)

        Returns None if model not found.

    Behavior with use_dev=True:
        - Searches dev manifest (target/) FIRST
        - Returns dev schema name (e.g., personal_pavel_filianin)
        - Uses model filename, NOT alias
        - Falls back to BigQuery if not in dev manifest

    Behavior with use_dev=False (default):
        - Searches production manifest (.dbt-state/) first
        - Falls back to dev manifest if DBT_FALLBACK_TARGET=true
        - Falls back to BigQuery if DBT_FALLBACK_BIGQUERY=true

    Environment variables:
        DBT_PROD_TABLE_NAME: Table name resolution strategy (prod only)
            - "alias_or_name" (default): Use alias if present, else name
            - "name": Always use model name
            - "alias": Always use alias (fallback to name)

        DBT_PROD_SCHEMA_SOURCE: Schema/database resolution strategy (prod only)
            - "config_or_model" (default): Use config if present, else model
            - "model": Always use model.schema and model.database
            - "config": Always use config.schema and config.database (fallback to model)

        DBT_DEV_SCHEMA: Full dev schema override
        DBT_DEV_SCHEMA_TEMPLATE: Template with {username} placeholder
        DBT_DEV_SCHEMA_PREFIX: Prefix for dev schema (default: "personal")
        DBT_FALLBACK_TARGET: Enable dev manifest fallback (default: true)
        DBT_FALLBACK_BIGQUERY: Enable BigQuery fallback (default: true)
    """
    # Check git status and collect warnings
    dev_manifest = _find_dev_manifest(manifest_path) if use_dev else None
    warnings = _check_manifest_git_mismatch(model_name, use_dev, dev_manifest)
    _print_warnings(warnings, json_output)

    # Handle --dev flag: prioritize dev manifest
    if use_dev:  # pragma: no cover
        if not dev_manifest:
            dev_manifest = _find_dev_manifest(manifest_path)
        if dev_manifest:
            try:
                parser_dev = _get_cached_parser(dev_manifest)
                model = parser_dev.get_model(model_name)
                if model:
                    return _build_dev_schema_result(model, model_name)
            except Exception:  # pragma: no cover
                pass  # Fall through to BigQuery fallback

        # Fallback to BigQuery for dev
        if os.environ.get('DBT_FALLBACK_BIGQUERY', 'true').lower() in ('true', '1', 'yes'):
            # For dev mode, use full model name as table name (not split by __)
            dev_schema = _calculate_dev_schema()
            full_table = f"{dev_schema}.{model_name}"

            try:
                result = _run_bq_command(['show', '--format=json', full_table], timeout=10)
                print(f"⚠️  Model not in manifest, using BigQuery table: {full_table}",
                      file=sys.stderr)
                return {
                    'database': '',
                    'schema': dev_schema,
                    'table': model_name,
                    'full_name': full_table
                }
            except (subprocess.CalledProcessError, subprocess.TimeoutExpired, FileNotFoundError):
                pass

        return None

    # Default behavior: production first, then fallbacks
    parser = _get_cached_parser(manifest_path)
    model = parser.get_model(model_name)
    fallback_warnings = []

    if not model:
        # LEVEL 2 Fallback: Try dev manifest (target/)
        if os.environ.get('DBT_FALLBACK_TARGET', 'true').lower() in ('true', '1', 'yes'):
            dev_manifest = _find_dev_manifest(manifest_path)
            if dev_manifest:
                try:
                    parser_dev = _get_cached_parser(dev_manifest)
                    model = parser_dev.get_model(model_name)
                    if model:
                        fallback_warnings.append({
                            "type": "dev_manifest_fallback",
                            "severity": "warning",
                            "message": f"Model '{model_name}' not found in production manifest",
                            "detail": "Using dev manifest - table location in DEV schema (personal_USERNAME)",
                            "source": "LEVEL 2"
                        })
                        _print_warnings(fallback_warnings, json_output)
                        # Return DEV schema result immediately
                        return _build_dev_schema_result(model, model_name)
                except Exception:
                    pass  # Fall through to BigQuery fallback

        # LEVEL 3 Fallback: Query BigQuery directly
        if not model and os.environ.get('DBT_FALLBACK_BIGQUERY', 'true').lower() in ('true', '1', 'yes'):  # pragma: no cover
            dataset, table = _infer_table_parts(model_name)
            if dataset:
                bq_metadata = _fetch_table_metadata_from_bigquery(dataset, table)
                if bq_metadata:
                    fallback_warnings.append({
                        "type": "bigquery_fallback",
                        "severity": "warning",
                        "message": f"Model '{model_name}' not in manifest",
                        "detail": f"Using BigQuery table: {dataset}.{table}",
                        "source": "LEVEL 3"
                    })
                    _print_warnings(fallback_warnings, json_output)
                    table_ref = bq_metadata.get('tableReference', {})
                    return {
                        'database': table_ref.get('projectId', ''),
                        'schema': table_ref.get('datasetId', ''),
                        'table': table_ref.get('tableId', ''),
                        'full_name': f"{table_ref.get('projectId', '')}.{table_ref.get('datasetId', '')}.{table_ref.get('tableId', '')}"
                    }

        if not model:
            return None

    # Print fallback warnings if any
    if fallback_warnings:
        _print_warnings(fallback_warnings, json_output)

    # Extract config
    config = model.get('config', {})

    # Schema/database resolution based on environment variable
    schema_source = os.environ.get('DBT_PROD_SCHEMA_SOURCE', 'config_or_model')

    model_database = model.get('database', '')
    model_schema = model.get('schema', '')
    config_database = config.get('database', '')
    config_schema = config.get('schema', '')

    if schema_source == 'model':
        # Always use model values
        database = model_database
        schema_name = model_schema
    elif schema_source == 'config':
        # Prefer config, fallback to model
        database = config_database or model_database
        schema_name = config_schema or model_schema
    else:  # 'config_or_model' (default)
        # Use config if present, otherwise model
        database = config_database or model_database
        schema_name = config_schema or model_schema

    # Table name resolution based on environment variable
    table_name_strategy = os.environ.get('DBT_PROD_TABLE_NAME', 'alias_or_name')

    alias = config.get('alias', '')
    name = model.get('name', '')

    if table_name_strategy == 'name':
        # Prefer model name, fallback to alias if name missing
        table_name = name or alias
    elif table_name_strategy == 'alias':
        # Prefer alias, fallback to name if alias missing
        table_name = alias or name
    else:  # 'alias_or_name' (default)
        # Use alias if present, otherwise model name
        table_name = alias or name

    return {
        'database': database,
        'schema': schema_name,
        'table': table_name,
        'full_name': f"{database}.{schema_name}.{table_name}"
    }


def columns(manifest_path: str, model_name: str, use_dev: bool = False, json_output: bool = False) -> Optional[List[Dict[str, str]]]:
    """
    Extract column list with types

    Args:
        manifest_path: Path to manifest.json
        model_name: Model name
        use_dev: If True, prioritize dev manifest over production

    Returns:
        List of dictionaries with:
        - name: Column name
        - data_type: Column data type

        Returns None if model not found.
        Preserves column order from manifest.

        Falls back to BigQuery if columns not in manifest.
    """
    # Check git status and collect warnings
    dev_manifest = _find_dev_manifest(manifest_path) if use_dev else None
    warnings = _check_manifest_git_mismatch(model_name, use_dev, dev_manifest)
    _print_warnings(warnings, json_output)

    # Handle --dev flag: prioritize dev manifest
    if use_dev:  # pragma: no cover
        if not dev_manifest:
            dev_manifest = _find_dev_manifest(manifest_path)
        if dev_manifest:
            try:
                parser_dev = _get_cached_parser(dev_manifest)
                model = parser_dev.get_model(model_name)
                if model:
                    model_columns = model.get('columns', {})
                    if model_columns:
                        result = []
                        for col_name, col_data in model_columns.items():
                            result.append({
                                'name': col_name,
                                'data_type': col_data.get('data_type', 'unknown')
                            })
                        return result
            except Exception:  # pragma: no cover
                pass

        # Fallback to BigQuery for dev
        if os.environ.get('DBT_FALLBACK_BIGQUERY', 'true').lower() in ('true', '1', 'yes'):
            # For dev mode, use full model name as table name (not split by __)
            dev_schema = _calculate_dev_schema()
            return _fetch_columns_from_bigquery_direct(dev_schema, model_name)

        return None

    # Default behavior: production first, then fallbacks
    parser = _get_cached_parser(manifest_path)
    model = parser.get_model(model_name)
    fallback_warnings = []

    if not model:
        # LEVEL 2 Fallback: Try dev manifest (target/)
        if os.environ.get('DBT_FALLBACK_TARGET', 'true').lower() in ('true', '1', 'yes'):
            dev_manifest = _find_dev_manifest(manifest_path)
            if dev_manifest:
                try:
                    parser_dev = _get_cached_parser(dev_manifest)
                    model = parser_dev.get_model(model_name)
                    if model:
                        fallback_warnings.append({
                            "type": "dev_manifest_fallback",
                            "severity": "warning",
                            "message": f"Model '{model_name}' not found in production manifest",
                            "detail": "Using dev manifest (target/manifest.json) as fallback",
                            "source": "LEVEL 2"
                        })
                        # Continue with model data processing below
                except Exception:
                    pass  # Fall through to BigQuery fallback

        # LEVEL 3 Fallback: Query BigQuery directly
        if not model and os.environ.get('DBT_FALLBACK_BIGQUERY', 'true').lower() in ('true', '1', 'yes'):  # pragma: no cover
            dataset, table = _infer_table_parts(model_name)
            if dataset:
                fallback_warnings.append({
                    "type": "bigquery_fallback",
                    "severity": "info",
                    "message": f"Model '{model_name}' not in manifest",
                    "detail": "Fetching columns from BigQuery",
                    "source": "LEVEL 3"
                })
                _print_warnings(fallback_warnings, json_output)
                # Fetch directly from BigQuery (bypassing the model parameter)
                return _fetch_columns_from_bigquery_direct(dataset, table)

        if not model:
            return None

    # Print fallback warnings if any
    if fallback_warnings:
        _print_warnings(fallback_warnings, json_output)

    # Extract columns from model
    model_columns = model.get('columns', {})

    # If no columns in manifest, fallback to BigQuery
    if not model_columns:
        return _fetch_columns_from_bigquery(manifest_path, model_name)

    # Convert to list format, preserving order
    result = []
    for col_name, col_data in model_columns.items():
        result.append({
            'name': col_name,
            'data_type': col_data.get('data_type', 'string').lower()
        })

    return result


def config(manifest_path: str, model_name: str, use_dev: bool = False, json_output: bool = False) -> Optional[Dict[str, Any]]:
    """
    Extract full dbt config

    Args:
        manifest_path: Path to manifest.json
        model_name: Model name
        use_dev: If True, prioritize dev manifest over production

    Returns:
        Full config dictionary with all 29+ fields:
        materialized, partition_by, cluster_by, unique_key,
        incremental_strategy, on_schema_change, grants, etc.

        Returns None if model not found.

    Behavior with use_dev=True:
        - Searches dev manifest (target/) FIRST
        - Returns dev-specific config
        - Falls back to BigQuery if not in dev manifest
    """
    # Check git status and collect warnings
    dev_manifest = _find_dev_manifest(manifest_path) if use_dev else None
    warnings = _check_manifest_git_mismatch(model_name, use_dev, dev_manifest)
    _print_warnings(warnings, json_output)

    # Handle --dev flag: prioritize dev manifest
    if use_dev:  # pragma: no cover
        if not dev_manifest:
            dev_manifest = _find_dev_manifest(manifest_path)
        if dev_manifest:
            try:
                parser_dev = _get_cached_parser(dev_manifest)
                model = parser_dev.get_model(model_name)
                if model:
                    return model.get('config', {})
            except Exception:  # pragma: no cover
                pass

        # Fallback to BigQuery for dev (if enabled)
        if os.environ.get('DBT_FALLBACK_BIGQUERY', 'true').lower() in ('true', '1', 'yes'):  # pragma: no cover
            # For dev mode, use full model name as table name (not split by __)
            dev_schema = _calculate_dev_schema()
            bq_metadata = _fetch_table_metadata_from_bigquery(dev_schema, model_name)
            if bq_metadata:
                print(f"⚠️  Model not in manifest, using BigQuery config: {dev_schema}.{model_name}",
                      file=sys.stderr)
                print(f"⚠️  Partial config available (dbt-specific settings unavailable)",
                      file=sys.stderr)

                # Map BigQuery → dbt config
                table_type = bq_metadata.get('type', 'TABLE')
                config_result = {
                    'materialized': 'table' if table_type == 'TABLE' else 'view',
                    'partition_by': None,
                    'cluster_by': None,
                    # dbt-specific (not available from BigQuery)
                    'unique_key': None,
                    'incremental_strategy': None,
                    'on_schema_change': None,
                    'grants': {},
                    'tags': [],
                    'meta': {},
                    'enabled': True,
                    'alias': None,
                    'schema': None,
                    'database': None,
                    'pre_hook': [],
                    'post_hook': [],
                    'quoting': {},
                    'column_types': {},
                    'persist_docs': {},
                    'full_refresh': None,
                }

                # Extract partition info
                if 'timePartitioning' in bq_metadata:
                    config_result['partition_by'] = bq_metadata['timePartitioning'].get('field')

                # Extract clustering info
                if 'clustering' in bq_metadata:
                    config_result['cluster_by'] = bq_metadata['clustering'].get('fields', [])

                return config_result

        return None

    # Default behavior: production first, then fallbacks
    parser = _get_cached_parser(manifest_path)
    model = parser.get_model(model_name)
    fallback_warnings = []

    if not model:
        # LEVEL 2 Fallback: Try dev manifest (target/)
        if os.environ.get('DBT_FALLBACK_TARGET', 'true').lower() in ('true', '1', 'yes'):
            dev_manifest = _find_dev_manifest(manifest_path)
            if dev_manifest:
                try:
                    parser_dev = _get_cached_parser(dev_manifest)
                    model = parser_dev.get_model(model_name)
                    if model:
                        fallback_warnings.append({
                            "type": "dev_manifest_fallback",
                            "severity": "warning",
                            "message": f"Model '{model_name}' not found in production manifest",
                            "detail": "Using dev manifest (target/manifest.json) as fallback",
                            "source": "LEVEL 2"
                        })
                        # Continue with model data processing below
                except Exception:
                    pass  # Fall through to BigQuery fallback

        # LEVEL 3 Fallback: Query BigQuery directly
        if not model and os.environ.get('DBT_FALLBACK_BIGQUERY', 'true').lower() in ('true', '1', 'yes'):  # pragma: no cover
            dataset, table = _infer_table_parts(model_name)
            if dataset:
                bq_metadata = _fetch_table_metadata_from_bigquery(dataset, table)
                if bq_metadata:
                    fallback_warnings.append({
                        "type": "bigquery_fallback",
                        "severity": "warning",
                        "message": f"Model '{model_name}' not in manifest",
                        "detail": "Using BigQuery config (dbt-specific settings unavailable)",
                        "source": "LEVEL 3"
                    })
                    _print_warnings(fallback_warnings, json_output)

                    # Map BigQuery → dbt config
                    table_type = bq_metadata.get('type', 'TABLE')
                    config_result = {
                        'materialized': 'table' if table_type == 'TABLE' else 'view',
                        'partition_by': None,
                        'cluster_by': None,
                        # dbt-specific (not available from BigQuery)
                        'unique_key': None,
                        'incremental_strategy': None,
                        'on_schema_change': None,
                        'grants': {},
                        'tags': [],
                        'meta': {},
                        'enabled': True,
                        'alias': None,
                        'schema': None,
                        'database': None,
                        'pre_hook': [],
                        'post_hook': [],
                        'quoting': {},
                        'column_types': {},
                        'persist_docs': {},
                        'full_refresh': None,
                    }

                    # Extract partition info
                    if 'timePartitioning' in bq_metadata:
                        config_result['partition_by'] = bq_metadata['timePartitioning'].get('field')

                    # Extract clustering info
                    if 'clustering' in bq_metadata:
                        config_result['cluster_by'] = bq_metadata['clustering'].get('fields', [])

                    return config_result

        if not model:
            return None

    # Print fallback warnings if any
    if fallback_warnings:
        _print_warnings(fallback_warnings, json_output)

    return model.get('config', {})


def deps(manifest_path: str, model_name: str, use_dev: bool = False, json_output: bool = False) -> Optional[Dict[str, List[str]]]:
    """
    Extract dependencies by type

    Args:
        manifest_path: Path to manifest.json
        model_name: Model name
        use_dev: If True, prioritize dev manifest over production

    Returns:
        Dictionary with:
        - refs: List of model dependencies
        - sources: List of source dependencies
        - macros: List of macro dependencies

        Returns None if model not found.

    Behavior with use_dev=True:
        - Searches dev manifest (target/) FIRST
        - Returns dev-specific dependencies
        - NO BigQuery fallback (lineage is manifest-only)
    """
    # Check git status and collect warnings
    dev_manifest = _find_dev_manifest(manifest_path) if use_dev else None
    warnings = _check_manifest_git_mismatch(model_name, use_dev, dev_manifest)
    _print_warnings(warnings, json_output)

    # Handle --dev flag: prioritize dev manifest
    if use_dev:  # pragma: no cover
        if not dev_manifest:
            dev_manifest = _find_dev_manifest(manifest_path)
        if dev_manifest:
            try:
                parser_dev = _get_cached_parser(dev_manifest)
                result = parser_dev.get_dependencies(model_name)
                if result is not None:
                    return result
            except Exception:  # pragma: no cover
                pass

        # No BigQuery fallback for dependencies (lineage is manifest-only)
        print(f"❌ Dependencies not available for '{model_name}': model not in dev manifest",
              file=sys.stderr)
        print(f"   Dependencies are dbt-specific (refs, sources, macros) and cannot be inferred from BigQuery.",
              file=sys.stderr)
        print(f"   Hint: Run 'defer run --select {model_name}' to add model to manifest",
              file=sys.stderr)
        return None

    # Default behavior: production first
    result = _get_cached_parser(manifest_path).get_dependencies(model_name)

    if result is None:
        # Improved error message
        print(f"❌ Dependencies not available for '{model_name}': model not in manifest",
              file=sys.stderr)
        print(f"   Dependencies are dbt-specific (refs, sources, macros) and cannot be inferred from BigQuery.",
              file=sys.stderr)
        print(f"   Hint: Run 'defer run --select {model_name}' to add model to manifest",
              file=sys.stderr)

    return result


def sql(manifest_path: str, model_name: str, use_dev: bool = False, raw: bool = False, json_output: bool = False) -> Optional[str]:
    """
    Extract SQL code

    Args:
        manifest_path: Path to manifest.json
        model_name: Model name
        use_dev: If True, prioritize dev manifest over production
        raw: If True, return raw SQL with Jinja. If False, return compiled SQL.

    Returns:
        SQL code as string
        Returns None if model not found.

    Behavior with use_dev=True:
        - Searches dev manifest (target/) FIRST
        - Returns dev-specific SQL
        - NO BigQuery fallback (SQL is dbt-specific)
    """
    # Check git status and collect warnings
    dev_manifest = _find_dev_manifest(manifest_path) if use_dev else None
    warnings = _check_manifest_git_mismatch(model_name, use_dev, dev_manifest)
    _print_warnings(warnings, json_output)

    # Handle --dev flag: prioritize dev manifest
    if use_dev:  # pragma: no cover
        if not dev_manifest:
            dev_manifest = _find_dev_manifest(manifest_path)
        if dev_manifest:
            try:
                parser_dev = _get_cached_parser(dev_manifest)
                model = parser_dev.get_model(model_name)
                if model:
                    # Return raw or compiled SQL
                    if raw:
                        return model.get('raw_code', '')
                    else:
                        return model.get('compiled_code', '')
            except Exception:  # pragma: no cover
                pass

        # No BigQuery fallback for SQL (dbt-specific)
        print(f"❌ SQL code not available for '{model_name}': model not in dev manifest",
              file=sys.stderr)
        print(f"   Hint: Use 'meta path {model_name}' to locate source file",
              file=sys.stderr)
        return None

    # Default behavior: production first, then dev fallback
    parser = _get_cached_parser(manifest_path)
    model = parser.get_model(model_name)
    fallback_warnings = []

    if not model:
        # LEVEL 2 Fallback: Try dev manifest (target/)
        if os.environ.get('DBT_FALLBACK_TARGET', 'true').lower() in ('true', '1', 'yes'):
            dev_manifest = _find_dev_manifest(manifest_path)
            if dev_manifest:
                try:
                    parser_dev = _get_cached_parser(dev_manifest)
                    model = parser_dev.get_model(model_name)
                    if model:
                        fallback_warnings.append({
                            "type": "dev_manifest_fallback",
                            "severity": "warning",
                            "message": f"Model '{model_name}' not found in production manifest",
                            "detail": "Using dev manifest (target/manifest.json) as fallback",
                            "source": "LEVEL 2"
                        })
                        _print_warnings(fallback_warnings, json_output)
                        # Return SQL from dev manifest
                        if raw:
                            return model.get('raw_code', '')
                        else:
                            return model.get('compiled_code', '')
                except Exception:
                    pass  # Fall through to error

    if not model:
        # Model not found in production or dev
        print(f"❌ SQL code not available for '{model_name}': model not in manifest",
              file=sys.stderr)
        print(f"   Hint: If new model, run 'defer run --select {model_name}' first",
              file=sys.stderr)
        print(f"   Or use 'meta path {model_name}' to locate source file",
              file=sys.stderr)
        return None

    # Return raw or compiled SQL
    if raw:
        return model.get('raw_code', '')
    else:
        return model.get('compiled_code', '')


def path(manifest_path: str, model_name: str, use_dev: bool = False, json_output: bool = False) -> Optional[str]:
    """
    Get relative file path

    Args:
        manifest_path: Path to manifest.json
        model_name: Model name
        use_dev: If True, prioritize dev manifest over production

    Returns:
        Relative file path (e.g., "models/core/client/model.sql")
        Returns None if model not found.

    Behavior with use_dev=True:
        - Searches dev manifest (target/) FIRST
        - Returns dev-specific file path
        - NO BigQuery fallback (file path is dbt-specific)
    """
    # Check git status and collect warnings
    dev_manifest = _find_dev_manifest(manifest_path) if use_dev else None
    warnings = _check_manifest_git_mismatch(model_name, use_dev, dev_manifest)
    _print_warnings(warnings, json_output)

    # Handle --dev flag: prioritize dev manifest
    if use_dev:  # pragma: no cover
        if not dev_manifest:
            dev_manifest = _find_dev_manifest(manifest_path)
        if dev_manifest:
            try:
                parser_dev = _get_cached_parser(dev_manifest)
                model = parser_dev.get_model(model_name)

                # If not found by model_name and contains dots, try BigQuery format search
                if not model and '.' in model_name:
                    parts = model_name.split('.')
                    if len(parts) >= 2:
                        bq_schema = parts[-2]
                        bq_table = parts[-1]

                        # Get dev table pattern for matching
                        dev_pattern = os.environ.get('DBT_DEV_TABLE_PATTERN', 'name')

                        nodes = parser_dev.manifest.get('nodes', {})
                        for node_id, node_data in nodes.items():
                            if node_data.get('resource_type') != 'model':
                                continue

                            # In dev mode, check both:
                            # 1. Actual dev schema (personal_*)
                            # 2. Config schema (production schema for reference)
                            node_dev_schema = node_data.get('schema', '')
                            node_config_schema = node_data.get('config', {}).get('schema', '')

                            if node_dev_schema != bq_schema and node_config_schema != bq_schema:
                                continue

                            # Build expected dev table name based on pattern
                            node_name = node_data.get('name', '')
                            node_alias = node_data.get('config', {}).get('alias', '')

                            if dev_pattern == 'name':
                                expected_table = node_name
                            elif dev_pattern == 'alias':
                                expected_table = node_alias if node_alias else node_name
                            else:
                                # For custom patterns, try name and alias
                                expected_table = node_name

                            if expected_table == bq_table:
                                model = node_data
                                break

                if model:
                    return model.get('original_file_path', '')
            except Exception:  # pragma: no cover
                pass

        # No BigQuery fallback for path (dbt-specific)
        return None

    # Default behavior: production first, then dev fallback
    parser = _get_cached_parser(manifest_path)
    model = parser.get_model(model_name)
    fallback_warnings = []

    # If model not found and model_name contains dots (BigQuery format),
    # search by schema.table in manifest
    if not model and '.' in model_name:
        parts = model_name.split('.')
        if len(parts) >= 2:
            # Extract schema and table from BigQuery format
            # Format: "project.schema.table" or "schema.table"
            bq_schema = parts[-2]
            bq_table = parts[-1]

            # Search all models for matching schema + alias/name
            nodes = parser.manifest.get('nodes', {})
            for node_id, node_data in nodes.items():
                if node_data.get('resource_type') != 'model':
                    continue

                # Check schema match
                node_schema = node_data.get('schema', '')
                if node_schema != bq_schema:
                    continue

                # Check table match: alias OR name OR filename
                node_alias = node_data.get('alias') or node_data.get('name', '')

                # Extract filename from path (for dev tables)
                node_path = node_data.get('original_file_path', '')
                filename = ''
                if node_path:
                    # Get filename without extension: "models/core/client/file.sql" → "file"
                    filename = node_path.split('/')[-1].replace('.sql', '')

                # Match by alias, name, or filename
                if node_alias == bq_table or filename == bq_table:
                    model = node_data
                    # Found model by BigQuery format - no warning needed
                    break

    if not model:
        # LEVEL 2 Fallback: Try dev manifest (target/)
        if os.environ.get('DBT_FALLBACK_TARGET', 'true').lower() in ('true', '1', 'yes'):
            dev_manifest = _find_dev_manifest(manifest_path)
            if dev_manifest:
                try:
                    parser_dev = _get_cached_parser(dev_manifest)
                    model = parser_dev.get_model(model_name)
                    if model:
                        fallback_warnings.append({
                            "type": "dev_manifest_fallback",
                            "severity": "warning",
                            "message": f"Model '{model_name}' not found in production manifest",
                            "detail": "Using dev manifest (target/manifest.json) as fallback",
                            "source": "LEVEL 2"
                        })
                        _print_warnings(fallback_warnings, json_output)
                        return model.get('original_file_path', '')
                except Exception:
                    pass  # Fall through to filesystem search

    if not model:
        # LEVEL 3 Fallback: Filesystem search
        if os.environ.get('DBT_FALLBACK_BIGQUERY', 'true').lower() in ('true', '1', 'yes'):
            # Extract table name from different formats:
            # - dbt format: "core_client__events" → "events"
            # - BigQuery format: "admirals-bi-dwh.core_client.events" → "events"
            if '.' in model_name:
                # BigQuery format: extract last part after last dot
                table = model_name.split('.')[-1]
            else:
                # dbt format: use _infer_table_parts
                _, table = _infer_table_parts(model_name)

            # Search in models directory
            matches = glob.glob(f"models/**/*{table}*.sql", recursive=True)

            if len(matches) == 1:
                print(f"⚠️  Model '{model_name}' not in manifest, found file by pattern: {matches[0]}",
                      file=sys.stderr)
                return matches[0]
            elif len(matches) > 1:
                print(f"❌ Multiple files match pattern '{table}':", file=sys.stderr)
                for match in matches:
                    print(f"   - {match}", file=sys.stderr)
                return None
            else:
                print(f"❌ No .sql files found matching pattern '{table}'", file=sys.stderr)
                return None
        return None

    # Print fallback warnings if any
    if fallback_warnings:
        _print_warnings(fallback_warnings, json_output)

    return model.get('original_file_path', '')


def list_models(manifest_path: str, pattern: Optional[str] = None) -> List[str]:
    """
    List all models, optionally filtered by pattern

    Args:
        manifest_path: Path to manifest.json
        pattern: Optional filter pattern (substring match, case-insensitive)

    Returns:
        Sorted list of model names
    """
    parser = _get_cached_parser(manifest_path)
    models = parser.get_all_models()

    # Extract and filter model names in one pass
    if pattern:
        pattern_lower = pattern.lower()
        model_names = [
            uid.split('.')[-1]
            for uid in models.keys()
            if pattern_lower in uid.split('.')[-1].lower()
        ]
    else:
        model_names = [uid.split('.')[-1] for uid in models.keys()]

    return sorted(model_names)


def search(manifest_path: str, query: str) -> List[Dict[str, str]]:
    """
    Search models by name or description

    Args:
        manifest_path: Path to manifest.json
        query: Search query (substring match)

    Returns:
        List of dictionaries with:
        - name: Model name
        - description: Model description
    """
    parser = _get_cached_parser(manifest_path)
    results = parser.search_models(query)

    # Format results
    output = []
    for model in results:
        model_name = model['unique_id'].split('.')[-1]
        output.append({
            'name': model_name,
            'description': model.get('description', '')
        })

    return sorted(output, key=lambda x: x['name'])


def _get_all_relations_recursive(
    relation_map: Dict[str, List[str]],
    node_id: str,
    visited: Optional[set] = None
) -> List[str]:
    """
    Recursively get all dependencies (parents or children)

    Generic function that works for both parent_map and child_map.

    Args:
        relation_map: manifest['parent_map'] or manifest['child_map']
        node_id: Starting node unique_id
        visited: Set of already visited nodes (to avoid cycles)

    Returns:
        List of all related unique_ids (maintaining order, removing duplicates)
    """
    if visited is None:
        visited = set()

    if node_id in visited:
        return []

    visited.add(node_id)
    relations = relation_map.get(node_id, [])

    all_relations = list(relations)
    for relation_id in relations:
        all_relations.extend(_get_all_relations_recursive(relation_map, relation_id, visited))

    # Return unique items (preserving order with dict.fromkeys)
    return list(dict.fromkeys(all_relations))


def _count_tree_nodes(tree: List[Dict[str, Any]]) -> int:
    """
    Count total nodes in hierarchical tree

    Args:
        tree: Hierarchical tree structure

    Returns:
        Total count of nodes including all nested children
    """
    count = len(tree)
    for node in tree:
        if 'children' in node and node['children']:
            count += _count_tree_nodes(node['children'])
    return count


def _flatten_tree_to_compact(tree: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Flatten nested tree to compact flat format

    Args:
        tree: Nested tree [{path, table, level, children}, ...]

    Returns:
        Flat array [{"path": "...", "table": "...", "level": 0}, ...]
    """
    result = []
    for node in tree:
        # Add current node without children
        result.append({
            'path': node['path'],
            'table': node['table'],
            'type': node.get('type', ''),
            'level': node['level']
        })
        # Recursively add children
        if node.get('children'):
            result.extend(_flatten_tree_to_compact(node['children']))
    return result


def _build_relation_tree(
    relation_map: Dict[str, List[str]],
    node_id: str,
    nodes: Dict[str, Any],
    sources: Dict[str, Any],
    visited: Optional[set] = None,
    level: int = 0,
    json_mode: bool = False
) -> List[Dict[str, Any]]:
    """
    Build hierarchical tree of relations (parents or children)

    Args:
        relation_map: manifest['parent_map'] or manifest['child_map']
        node_id: Starting node unique_id
        nodes: manifest['nodes']
        sources: manifest['sources']
        visited: Set of already visited nodes (to avoid cycles)
        level: Current depth level
        json_mode: If True, return compact JSON structure for AI agents

    Returns:
        List of dicts with 'node' info and 'children' list

        If json_mode=False (for display):
        [{
            'name': '...',
            'type': '...',
            'level': 0,
            'children': [...]
        }]

        If json_mode=True (for AI agents):
        [{
            'path': 'models/core/client.sql',  # relative path to .sql file
            'table': 'core_client.client',     # schema.table for BigQuery
            'level': 0,
            'children': [...]
        }]
    """
    if visited is None:
        visited = set()

    if node_id in visited:
        return []

    visited.add(node_id)
    relations = relation_map.get(node_id, [])

    result = []
    for relation_id in relations:
        # Get node details
        node = nodes.get(relation_id) or sources.get(relation_id)
        if not node:
            continue

        # Filter out tests
        if node.get('resource_type') == 'test':
            continue

        # Build node info based on mode
        if json_mode:
            # Compact JSON for AI agents (nested structure)
            schema = node.get('schema', '')
            alias = node.get('alias') or node.get('name', '')
            table = f"{schema}.{alias}" if schema else alias

            # Get relative path - remove "models/" prefix to save space
            path = node.get('original_file_path', '')
            if path.startswith('models/'):
                path = path[7:]  # Remove "models/" prefix

            node_info = {
                'path': path,
                'table': table,
                'type': node.get('resource_type', ''),
                'level': level,
                'children': _build_relation_tree(relation_map, relation_id, nodes, sources, visited, level + 1, json_mode=True)
            }
        else:
            # Full info for display
            node_info = {
                'unique_id': relation_id,
                'name': node.get('name', ''),
                'type': node.get('resource_type', ''),
                'database': node.get('database', ''),
                'schema': node.get('schema', ''),
                'level': level,
                'children': _build_relation_tree(relation_map, relation_id, nodes, sources, visited, level + 1, json_mode=False)
            }

        result.append(node_info)

    return result


def parents(manifest_path: str, model_name: str, use_dev: bool = False, recursive: bool = False, json_output: bool = False) -> Optional[List[Dict[str, str]]]:
    """
    Get upstream dependencies (parent models)

    Args:
        manifest_path: Path to manifest.json
        model_name: Model name
        use_dev: If True, prioritize dev manifest over production
        recursive: If True, get all ancestors. If False, only direct parents.
        json_output: If True, return ultra-compact format for AI agents

    Returns:
        If recursive=False (direct parents):
            - Without -j: [{unique_id, name, type, database, schema}, ...]
            - With -j, <= 20: [{path, table}, ...]
            - With -j, > 20: [{path, table, level}, ...]

        If recursive=True and json_output=False (tree for display):
            [{name, type, level, children}, ...]

        If recursive=True and json_output=True:
            - If <= 20 nodes: nested JSON [{path, table, level, children}, ...]
            - If > 20 nodes: flat array [{path, table, level}, ...]

            Flat format (> 20 nodes) saves ~60% tokens vs nested:
            [{"path": "staging/amas/clients.sql", "table": "staging_amas.clients", "level": 0},
             {"path": "sources/amas.yml", "table": "raw_amas.clients", "level": 1}]

        Returns None if model not found.
        Filters out tests (resource_type != "test").

    Behavior with use_dev=True:
        - Searches dev manifest (target/) FIRST
        - Returns dev-specific parent dependencies
        - NO BigQuery fallback (lineage is manifest-only)
    """
    # Check git status and collect warnings
    dev_manifest = _find_dev_manifest(manifest_path) if use_dev else None
    warnings = _check_manifest_git_mismatch(model_name, use_dev, dev_manifest)
    _print_warnings(warnings, json_output)

    # Handle --dev flag: prioritize dev manifest
    if use_dev:  # pragma: no cover
        if not dev_manifest:
            dev_manifest = _find_dev_manifest(manifest_path)
        if dev_manifest:
            try:
                parser_dev = _get_cached_parser(dev_manifest)
                model = parser_dev.get_model(model_name)
                if model:
                    unique_id = model['unique_id']
                    parent_map = parser_dev.manifest.get('parent_map', {})
                    nodes = parser_dev.manifest.get('nodes', {})
                    sources = parser_dev.manifest.get('sources', {})

                    # Get parent details
                    if recursive:
                        # Build hierarchical tree
                        tree = _build_relation_tree(parent_map, unique_id, nodes, sources, json_mode=json_output)
                        # If JSON mode and > 20 nodes, use ultra-compact format
                        if json_output and _count_tree_nodes(tree) > 20:
                            return _flatten_tree_to_compact(tree)
                        return tree
                    else:
                        # Return flat list of direct parents
                        parent_ids = parent_map.get(unique_id, [])
                        parents_details = []

                        for parent_id in parent_ids:
                            # Get from nodes or sources
                            parent_node = nodes.get(parent_id) or sources.get(parent_id)

                            if not parent_node:
                                continue

                            # Filter out tests
                            if parent_node.get('resource_type') == 'test':
                                continue

                            # Use compact format {path, table, type}
                            schema = parent_node.get('schema', '')
                            alias = parent_node.get('alias') or parent_node.get('name', '')
                            table = f"{schema}.{alias}" if schema else alias
                            path = parent_node.get('original_file_path', '')
                            if path.startswith('models/'):
                                path = path[7:]

                            parents_details.append({
                                'path': path,
                                'table': table,
                                'type': parent_node.get('resource_type', '')
                            })

                        # If JSON mode and > 20 nodes, add level field
                        if json_output and len(parents_details) > 20:
                            return [{'path': item['path'], 'table': item['table'], 'type': item['type'], 'level': 0} for item in parents_details]

                        return parents_details
            except Exception:  # pragma: no cover
                pass

        # No BigQuery fallback for lineage (manifest-only)
        print(f"❌ Parent dependencies not available for '{model_name}': model not in dev manifest",
              file=sys.stderr)
        print(f"   Lineage information is stored only in manifest.json",
              file=sys.stderr)
        return None

    # Default behavior: production first
    parser = _get_cached_parser(manifest_path)
    model = parser.get_model(model_name)

    if not model:
        # Improved error message
        print(f"❌ Parent dependencies not available for '{model_name}': model not in manifest",
              file=sys.stderr)
        print(f"   Lineage information is stored only in manifest.json",
              file=sys.stderr)
        return None

    unique_id = model['unique_id']
    parent_map = parser.manifest.get('parent_map', {})
    nodes = parser.manifest.get('nodes', {})
    sources = parser.manifest.get('sources', {})

    # Get parent details
    if recursive:
        # Build hierarchical tree
        tree = _build_relation_tree(parent_map, unique_id, nodes, sources, json_mode=json_output)
        # If JSON mode and > 20 nodes, use ultra-compact format
        if json_output and _count_tree_nodes(tree) > 20:
            return _flatten_tree_to_compact(tree)
        return tree
    else:
        # Return flat list of direct parents
        parent_ids = parent_map.get(unique_id, [])
        parents_details = []

        for parent_id in parent_ids:
            # Get from nodes or sources
            parent_node = nodes.get(parent_id) or sources.get(parent_id)

            if not parent_node:
                continue

            # Filter out tests
            if parent_node.get('resource_type') == 'test':
                continue

            # Use compact format {path, table, type}
            schema = parent_node.get('schema', '')
            alias = parent_node.get('alias') or parent_node.get('name', '')
            table = f"{schema}.{alias}" if schema else alias
            path = parent_node.get('original_file_path', '')
            if path.startswith('models/'):
                path = path[7:]

            parents_details.append({
                'path': path,
                'table': table,
                'type': parent_node.get('resource_type', '')
            })

        # If JSON mode and > 20 nodes, add level field
        if json_output and len(parents_details) > 20:
            return [{'path': item['path'], 'table': item['table'], 'type': item['type'], 'level': 0} for item in parents_details]

        return parents_details


def children(manifest_path: str, model_name: str, use_dev: bool = False, recursive: bool = False, json_output: bool = False) -> Optional[List[Dict[str, str]]]:
    """
    Get downstream dependencies (child models)

    Args:
        manifest_path: Path to manifest.json
        model_name: Model name
        use_dev: If True, prioritize dev manifest over production
        recursive: If True, get all descendants. If False, only direct children.
        json_output: If True, return ultra-compact format for AI agents

    Returns:
        If recursive=False (direct children):
            - Without -j: [{unique_id, name, type, database, schema}, ...]
            - With -j, <= 20: [{path, table}, ...]
            - With -j, > 20: [{path, table, level}, ...]

        If recursive=True and json_output=False (tree for display):
            [{name, type, level, children}, ...]

        If recursive=True and json_output=True:
            - If <= 20 nodes: nested JSON [{path, table, level, children}, ...]
            - If > 20 nodes: flat array [{path, table, level}, ...]

            Flat format (> 20 nodes) saves ~60% tokens vs nested:
            [{"path": "core/client_info.sql", "table": "core_client.client_info", "level": 0},
             {"path": "report/client_report.sql", "table": "report.client_report", "level": 1}]

        Returns None if model not found.
        Filters out tests (resource_type != "test").

    Behavior with use_dev=True:
        - Searches dev manifest (target/) FIRST
        - Returns dev-specific child dependencies
        - NO BigQuery fallback (lineage is manifest-only)
    """
    # Check git status and collect warnings
    dev_manifest = _find_dev_manifest(manifest_path) if use_dev else None
    warnings = _check_manifest_git_mismatch(model_name, use_dev, dev_manifest)
    _print_warnings(warnings, json_output)

    # Handle --dev flag: prioritize dev manifest
    if use_dev:  # pragma: no cover
        if not dev_manifest:
            dev_manifest = _find_dev_manifest(manifest_path)
        if dev_manifest:
            try:
                parser_dev = _get_cached_parser(dev_manifest)
                model = parser_dev.get_model(model_name)
                if model:
                    unique_id = model['unique_id']
                    child_map = parser_dev.manifest.get('child_map', {})
                    nodes = parser_dev.manifest.get('nodes', {})
                    sources = parser_dev.manifest.get('sources', {})

                    # Get child details
                    if recursive:
                        # Build hierarchical tree
                        tree = _build_relation_tree(child_map, unique_id, nodes, sources, json_mode=json_output)
                        # If JSON mode and > 20 nodes, use ultra-compact format
                        if json_output and _count_tree_nodes(tree) > 20:
                            return _flatten_tree_to_compact(tree)
                        return tree
                    else:
                        # Return flat list of direct children
                        child_ids = child_map.get(unique_id, [])
                        children_details = []

                        for child_id in child_ids:
                            # Get from nodes or sources
                            child_node = nodes.get(child_id) or sources.get(child_id)

                            if not child_node:
                                continue

                            # Filter out tests
                            if child_node.get('resource_type') == 'test':
                                continue

                            # Use compact format {path, table, type}
                            schema = child_node.get('schema', '')
                            alias = child_node.get('alias') or child_node.get('name', '')
                            table = f"{schema}.{alias}" if schema else alias
                            path = child_node.get('original_file_path', '')
                            if path.startswith('models/'):
                                path = path[7:]

                            children_details.append({
                                'path': path,
                                'table': table,
                                'type': child_node.get('resource_type', '')
                            })

                        # If JSON mode and > 20 nodes, add level field
                        if json_output and len(children_details) > 20:
                            return [{'path': item['path'], 'table': item['table'], 'type': item['type'], 'level': 0} for item in children_details]

                        return children_details
            except Exception:  # pragma: no cover
                pass

        # No BigQuery fallback for lineage (manifest-only)
        print(f"❌ Child dependencies not available for '{model_name}': model not in dev manifest",
              file=sys.stderr)
        print(f"   Lineage information is stored only in manifest.json",
              file=sys.stderr)
        return None

    # Default behavior: production first
    parser = _get_cached_parser(manifest_path)
    model = parser.get_model(model_name)

    if not model:
        # Improved error message
        print(f"❌ Child dependencies not available for '{model_name}': model not in manifest",
              file=sys.stderr)
        print(f"   Lineage information is stored only in manifest.json",
              file=sys.stderr)
        return None

    unique_id = model['unique_id']
    child_map = parser.manifest.get('child_map', {})
    nodes = parser.manifest.get('nodes', {})
    sources = parser.manifest.get('sources', {})

    # Get child details
    if recursive:
        # Build hierarchical tree
        tree = _build_relation_tree(child_map, unique_id, nodes, sources, json_mode=json_output)
        # If JSON mode and > 20 nodes, use ultra-compact format
        if json_output and _count_tree_nodes(tree) > 20:
            return _flatten_tree_to_compact(tree)
        return tree
    else:
        # Return flat list of direct children
        child_ids = child_map.get(unique_id, [])
        children_details = []

        for child_id in child_ids:
            # Get from nodes or sources
            child_node = nodes.get(child_id) or sources.get(child_id)

            if not child_node:
                continue

            # Filter out tests
            if child_node.get('resource_type') == 'test':
                continue

            # Use compact format {path, table, type}
            schema = child_node.get('schema', '')
            alias = child_node.get('alias') or child_node.get('name', '')
            table = f"{schema}.{alias}" if schema else alias
            path = child_node.get('original_file_path', '')
            if path.startswith('models/'):
                path = path[7:]

            children_details.append({
                'path': path,
                'table': table,
                'type': child_node.get('resource_type', '')
            })

        # If JSON mode and > 20 nodes, add level field
        if json_output and len(children_details) > 20:
            return [{'path': item['path'], 'table': item['table'], 'type': item['type'], 'level': 0} for item in children_details]

        return children_details


def node(manifest_path: str, input_identifier: str) -> Optional[Dict[str, Any]]:
    """
    Get node by unique_id or model name

    Args:
        manifest_path: Path to manifest.json
        input_identifier: unique_id (e.g., "model.project.name") or model name

    Returns:
        Complete node metadata from manifest (all fields)
        Returns None if not found.
    """
    parser = _get_cached_parser(manifest_path)

    # Check if input looks like unique_id (contains dots)
    if '.' in input_identifier:
        # Try as unique_id
        nodes = parser.manifest.get('nodes', {})
        sources = parser.manifest.get('sources', {})

        node_data = nodes.get(input_identifier) or sources.get(input_identifier)

        if not node_data:
            return None

        return node_data
    else:
        # Try as model name
        model = parser.get_model(input_identifier)

        if not model:
            return None

        return model


def refresh() -> None:
    """
    Refresh manifest by running dbt parse

    Executes: dbt parse

    Raises:
        subprocess.CalledProcessError: If dbt parse fails
    """
    print("Refreshing manifest...")
    subprocess.run(['dbt', 'parse'], check=True)
    print("✓ Manifest refreshed")


def docs(manifest_path: str, model_name: str, use_dev: bool = False, json_output: bool = False) -> Optional[List[Dict[str, str]]]:
    """
    Get columns with descriptions

    Args:
        manifest_path: Path to manifest.json
        model_name: Model name
        use_dev: If True, prioritize dev manifest over production

    Returns:
        List of dictionaries with:
        - name: Column name
        - data_type: Column data type
        - description: Column description

        Returns None if model not found.

    Behavior with use_dev=True:
        - Searches dev manifest (target/) FIRST
        - Returns dev-specific column descriptions
        - NO BigQuery fallback (descriptions are manifest-only)
    """
    # Check git status and collect warnings
    dev_manifest = _find_dev_manifest(manifest_path) if use_dev else None
    warnings = _check_manifest_git_mismatch(model_name, use_dev, dev_manifest)
    _print_warnings(warnings, json_output)

    # Handle --dev flag: prioritize dev manifest
    if use_dev:  # pragma: no cover
        if not dev_manifest:
            dev_manifest = _find_dev_manifest(manifest_path)
        if dev_manifest:
            try:
                parser_dev = _get_cached_parser(dev_manifest)
                model = parser_dev.get_model(model_name)
                if model:
                    # Extract columns with descriptions
                    model_columns = model.get('columns', {})

                    result = []
                    for col_name, col_data in model_columns.items():
                        result.append({
                            'name': col_name,
                            'data_type': col_data.get('data_type', 'unknown'),
                            'description': col_data.get('description', '')
                        })

                    return result
            except Exception:  # pragma: no cover
                pass

        # No BigQuery fallback for docs (descriptions are manifest-only)
        return None

    # Default behavior: production first
    parser = _get_cached_parser(manifest_path)
    model = parser.get_model(model_name)

    if not model:
        return None

    # Extract columns with descriptions
    model_columns = model.get('columns', {})

    result = []
    for col_name, col_data in model_columns.items():
        result.append({
            'name': col_name,
            'data_type': col_data.get('data_type', 'unknown'),
            'description': col_data.get('description', '')
        })

    return result
