"""BigQuery utilities for dbt-meta

Handles BigQuery name sanitization, table metadata fetching, and column extraction.
"""

import json as json_lib
import os
import re
import subprocess
import sys
import time
from typing import Optional


def _should_retry(attempt: int, max_retries: int, error_msg: str) -> bool:
    """Handle retry logic with exponential backoff.

    Args:
        attempt: Current attempt number (0-indexed)
        max_retries: Maximum number of retry attempts
        error_msg: Debug message to print during retry

    Returns:
        True if should retry (sleep executed), False if final attempt

    Example:
        >>> if not _should_retry(0, 3, "Query failed"):
        ...     return None  # Was final attempt
        >>> # else: continue to next attempt
    """
    if attempt < max_retries - 1:
        # Retry with exponential backoff (2^0=1s, 2^1=2s, 2^2=4s)
        wait_time = 2 ** attempt
        if os.environ.get('DBT_META_DEBUG'):
            print(f"⚠️  {error_msg}, retrying in {wait_time}s...", file=sys.stderr)
        time.sleep(wait_time)
        return True
    # Final attempt - no retry
    return False


def sanitize_bigquery_name(name: str, name_type: str = "dataset") -> tuple[str, list[str]]:
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


def infer_table_parts(model_name: str) -> tuple[Optional[str], str]:
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
    full_table = f"{database}:{dataset}.{table}" if database else f"{dataset}.{table}"

    # Execute bq show command
    try:
        result = run_bq_command(['show', '--format=json', full_table], timeout=10)
        metadata = json_lib.loads(result.stdout)
        return metadata

    except (subprocess.CalledProcessError, subprocess.TimeoutExpired, FileNotFoundError, json_lib.JSONDecodeError):
        return None


def run_bq_command(args: list[str], timeout: int = 10) -> subprocess.CompletedProcess:
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
            [bq_cmd, *args],
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
    database: Optional[str] = None,
    max_retries: int = 3
) -> Optional[list[dict[str, str]]]:
    """
    Fetch columns directly from BigQuery with retry logic and performance tracking.

    Performance: ~2.5s per query (acceptable for accuracy).

    Retry strategy:
    - Attempt 1: immediate
    - Attempt 2: wait 2s
    - Attempt 3: wait 4s

    Args:
        dataset: BigQuery dataset name (schema)
        table: BigQuery table name
        database: Optional project ID (if None, uses default project)
        max_retries: Maximum number of retry attempts (default: 3)

    Returns:
        List of {name, data_type} dictionaries
        None if BigQuery fetch fails after all retries

    Environment Variables:
        DBT_META_DEBUG: If set, prints performance timing to stderr
    """
    start_time = time.time()

    # Construct full table name
    full_table = f"{database}:{dataset}.{table}" if database else f"{dataset}.{table}"

    # Check if bq command is available (only once, no retry)
    try:
        run_bq_command(['version'], timeout=5)
    except (subprocess.CalledProcessError, FileNotFoundError, subprocess.TimeoutExpired):  # pragma: no cover
        print("Error: bq command not found. Install Google Cloud SDK.", file=sys.stderr)
        return None

    # Fetch schema from BigQuery with retry
    for attempt in range(max_retries):
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

            # Performance tracking (optional)
            elapsed = time.time() - start_time
            if os.environ.get('DBT_META_DEBUG'):
                print(f"🔍 BigQuery query took {elapsed:.2f}s", file=sys.stderr)

            return columns

        except subprocess.CalledProcessError:  # pragma: no cover
            if _should_retry(attempt, max_retries, "BigQuery query failed"):
                continue
            # Final attempt failed
            print(f"Error: Failed to fetch columns from BigQuery for table: {full_table}", file=sys.stderr)
            return None

        except subprocess.TimeoutExpired:  # pragma: no cover
            if _should_retry(attempt, max_retries, "BigQuery timeout"):
                continue
            # Final attempt timed out
            print(f"Error: BigQuery request timed out after {max_retries} attempts", file=sys.stderr)
            return None

        except json_lib.JSONDecodeError:  # pragma: no cover
            # JSON parse error - no point retrying
            print("Error: Invalid JSON response from BigQuery", file=sys.stderr)
            return None

    return None
