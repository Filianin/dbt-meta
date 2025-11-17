"""BigQuery utilities for dbt-meta

Handles BigQuery name sanitization, table metadata fetching, and column extraction.
"""

import os
import sys
import re
import subprocess
import json as json_lib
from typing import Optional, List, Dict, Tuple


def sanitize_bigquery_name(name: str, name_type: str = "dataset") -> Tuple[str, List[str]]:
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


def infer_table_parts(model_name: str) -> Tuple[Optional[str], str]:
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


def fetch_table_metadata_from_bigquery(
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
    if database:  # pragma: no cover
        full_table = f"{database}:{dataset}.{table}"
    else:
        full_table = f"{dataset}.{table}"

    # Execute bq show command
    try:
        result = run_bq_command(['show', '--format=json', full_table], timeout=10)
        metadata = json_lib.loads(result.stdout)
        return metadata

    except (subprocess.CalledProcessError, subprocess.TimeoutExpired, FileNotFoundError, json_lib.JSONDecodeError):
        return None


def run_bq_command(args: List[str], timeout: int = 10) -> subprocess.CompletedProcess:
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


def fetch_columns_from_bigquery_direct(
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
    if database:  # pragma: no cover
        full_table = f"{database}:{dataset}.{table}"
    else:
        full_table = f"{dataset}.{table}"

    # Check if bq command is available
    try:
        run_bq_command(['version'], timeout=5)
    except (subprocess.CalledProcessError, FileNotFoundError, subprocess.TimeoutExpired):  # pragma: no cover
        print(f"Error: bq command not found. Install Google Cloud SDK.", file=sys.stderr)
        return None

    # Fetch schema from BigQuery
    try:
        result = run_bq_command(
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

    except subprocess.CalledProcessError:  # pragma: no cover
        print(f"Error: Failed to fetch columns from BigQuery for table: {full_table}", file=sys.stderr)
        return None
    except (json_lib.JSONDecodeError, subprocess.TimeoutExpired):  # pragma: no cover
        print(f"Error: Invalid response from BigQuery", file=sys.stderr)
        return None


def fetch_columns_from_bigquery(manifest_path: str, model_name: str) -> Optional[List[Dict[str, str]]]:
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
    from dbt_meta.utils import get_cached_parser

    parser = get_cached_parser(manifest_path)
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
        run_bq_command(['version'], timeout=5)
    except (subprocess.CalledProcessError, FileNotFoundError, subprocess.TimeoutExpired):  # pragma: no cover
        print(f"Error: bq command not found. Install Google Cloud SDK.", file=sys.stderr)
        return None

    # Fetch schema from BigQuery
    try:
        result = run_bq_command(
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

    except subprocess.CalledProcessError:  # pragma: no cover
        print(f"Error: Failed to fetch columns from BigQuery for table: {full_table}", file=sys.stderr)
        return None
    except (json_lib.JSONDecodeError, subprocess.TimeoutExpired):  # pragma: no cover
        print(f"Error: Invalid response from BigQuery", file=sys.stderr)
        return None
