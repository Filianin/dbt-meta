"""
Commands - Model metadata extraction functions

Provides high-level commands for extracting metadata from dbt manifest.
Each command returns formatted data matching bash version output.
"""

import subprocess
import json as json_lib
import sys
import re
from functools import lru_cache
from typing import Dict, List, Optional, Any
from dbt_meta.manifest.parser import ManifestParser
from dbt_meta.manifest.finder import ManifestFinder


@lru_cache(maxsize=1)
def _get_cached_parser(manifest_path: str) -> ManifestParser:
    """
    Get cached ManifestParser instance

    Uses LRU cache to avoid re-parsing the same manifest.
    Cache size = 1 since we typically work with one manifest at a time.

    Args:
        manifest_path: Path to manifest.json

    Returns:
        Cached ManifestParser instance
    """
    return ManifestParser(manifest_path)


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
    if ' ' in name:
        invalid_chars.add(' ')
        name = name.replace(' ', '_')

    # Other special characters (keep only letters, numbers, underscores, hyphens)
    valid_pattern = re.compile(r'[^a-zA-Z0-9_\-]')
    other_invalid = valid_pattern.findall(name)
    if other_invalid:
        invalid_chars.update(other_invalid)
        name = valid_pattern.sub('_', name)

    # Must start with letter or underscore
    if name and not (name[0].isalpha() or name[0] == '_'):
        warnings.append(f"Name must start with letter or underscore, got '{name[0]}'")
        name = f"_{name}"

    if invalid_chars:
        chars_str = ', '.join(f"'{c}'" for c in sorted(invalid_chars))
        warnings.append(f"Invalid BigQuery characters replaced: {chars_str}")

    if name != original and not warnings:
        warnings.append(f"Name sanitized: '{original}' → '{name}'")

    return name, warnings


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
        subprocess.run(['bq', 'version'], capture_output=True, check=True)
    except (subprocess.CalledProcessError, FileNotFoundError):
        print(f"Error: bq command not found. Install Google Cloud SDK.", file=sys.stderr)
        return None

    # Fetch schema from BigQuery
    try:
        result = subprocess.run(
            ['bq', 'show', '--schema', '--format=prettyjson', full_table],
            capture_output=True,
            text=True,
            check=True
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


def info(manifest_path: str, model_name: str) -> Optional[Dict[str, Any]]:
    """
    Extract basic model information

    Args:
        manifest_path: Path to manifest.json
        model_name: Model name (e.g., "core_client__client_profiles_events")

    Returns:
        Dictionary with:
        - name: Model name
        - database: BigQuery project
        - schema: BigQuery dataset
        - table: Table name (alias or model name)
        - full_name: database.schema.table
        - materialized: Materialization type
        - file: Relative file path
        - tags: List of tags
        - unique_id: Full unique identifier

        Returns None if model not found.
    """
    parser = _get_cached_parser(manifest_path)
    model = parser.get_model(model_name)

    if not model:
        return None

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


def schema(manifest_path: str, model_name: str) -> Optional[Dict[str, str]]:
    """
    Extract schema/table location information

    Args:
        manifest_path: Path to manifest.json
        model_name: Model name

    Returns:
        Dictionary with:
        - database: BigQuery project
        - schema: BigQuery dataset
        - table: Table name (based on DBT_PROD_TABLE_NAME setting)
        - full_name: database.schema.table

        Returns None if model not found.

    Environment variables:
        DBT_PROD_TABLE_NAME: Table name resolution strategy
            - "alias_or_name" (default): Use alias if present, else name
            - "name": Always use model name
            - "alias": Always use alias (fallback to name)

        DBT_PROD_SCHEMA_SOURCE: Schema/database resolution strategy
            - "config_or_model" (default): Use config if present, else model
            - "model": Always use model.schema and model.database
            - "config": Always use config.schema and config.database (fallback to model)
    """
    import os

    parser = _get_cached_parser(manifest_path)
    model = parser.get_model(model_name)

    if not model:
        return None

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


def columns(manifest_path: str, model_name: str) -> Optional[List[Dict[str, str]]]:
    """
    Extract column list with types

    Args:
        manifest_path: Path to manifest.json
        model_name: Model name

    Returns:
        List of dictionaries with:
        - name: Column name
        - data_type: Column data type

        Returns None if model not found.
        Preserves column order from manifest.

        Falls back to BigQuery if columns not in manifest.
    """
    parser = _get_cached_parser(manifest_path)
    model = parser.get_model(model_name)

    if not model:
        return None

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


def config(manifest_path: str, model_name: str) -> Optional[Dict[str, Any]]:
    """
    Extract full dbt config

    Args:
        manifest_path: Path to manifest.json
        model_name: Model name

    Returns:
        Full config dictionary with all 29+ fields:
        materialized, partition_by, cluster_by, unique_key,
        incremental_strategy, on_schema_change, grants, etc.

        Returns None if model not found.
    """
    parser = _get_cached_parser(manifest_path)
    model = parser.get_model(model_name)

    if not model:
        return None

    return model.get('config', {})


def deps(manifest_path: str, model_name: str) -> Optional[Dict[str, List[str]]]:
    """
    Extract dependencies by type

    Args:
        manifest_path: Path to manifest.json
        model_name: Model name

    Returns:
        Dictionary with:
        - refs: List of model dependencies
        - sources: List of source dependencies
        - macros: List of macro dependencies

        Returns None if model not found.
    """
    return _get_cached_parser(manifest_path).get_dependencies(model_name)


def sql(manifest_path: str, model_name: str, raw: bool = False) -> Optional[str]:
    """
    Extract SQL code

    Args:
        manifest_path: Path to manifest.json
        model_name: Model name
        raw: If True, return raw SQL with Jinja. If False, return compiled SQL.

    Returns:
        SQL code as string
        Returns None if model not found.
    """
    parser = _get_cached_parser(manifest_path)
    model = parser.get_model(model_name)

    if not model:
        return None

    # Return raw or compiled SQL
    if raw:
        return model.get('raw_code', '')
    else:
        return model.get('compiled_code', '')


def path(manifest_path: str, model_name: str) -> Optional[str]:
    """
    Get relative file path

    Args:
        manifest_path: Path to manifest.json
        model_name: Model name

    Returns:
        Relative file path (e.g., "models/core/client/model.sql")
        Returns None if model not found.
    """
    parser = _get_cached_parser(manifest_path)
    model = parser.get_model(model_name)

    if not model:
        return None

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


def parents(manifest_path: str, model_name: str, recursive: bool = False) -> Optional[List[Dict[str, str]]]:
    """
    Get upstream dependencies (parent models)

    Args:
        manifest_path: Path to manifest.json
        model_name: Model name
        recursive: If True, get all ancestors. If False, only direct parents.

    Returns:
        List of parent details:
        [{unique_id, name, type, database, schema}, ...]

        Returns None if model not found.
        Filters out tests (resource_type != "test").
    """
    parser = _get_cached_parser(manifest_path)
    model = parser.get_model(model_name)

    if not model:
        return None

    unique_id = model['unique_id']
    parent_map = parser.manifest.get('parent_map', {})

    # Get parent IDs
    if recursive:
        parent_ids = _get_all_relations_recursive(parent_map, unique_id)
    else:
        parent_ids = parent_map.get(unique_id, [])

    # Get parent details (filter out tests)
    parents_details = []
    nodes = parser.manifest.get('nodes', {})
    sources = parser.manifest.get('sources', {})

    for parent_id in parent_ids:
        # Get from nodes or sources
        parent_node = nodes.get(parent_id) or sources.get(parent_id)

        if not parent_node:
            continue

        # Filter out tests
        if parent_node.get('resource_type') == 'test':
            continue

        parents_details.append({
            'unique_id': parent_id,
            'name': parent_node.get('name', ''),
            'type': parent_node.get('resource_type', ''),
            'database': parent_node.get('database', ''),
            'schema': parent_node.get('schema', '')
        })

    return parents_details


def children(manifest_path: str, model_name: str, recursive: bool = False) -> Optional[List[Dict[str, str]]]:
    """
    Get downstream dependencies (child models)

    Args:
        manifest_path: Path to manifest.json
        model_name: Model name
        recursive: If True, get all descendants. If False, only direct children.

    Returns:
        List of child details:
        [{unique_id, name, type, database, schema}, ...]

        Returns None if model not found.
        Filters out tests (resource_type != "test").
    """
    parser = _get_cached_parser(manifest_path)
    model = parser.get_model(model_name)

    if not model:
        return None

    unique_id = model['unique_id']
    child_map = parser.manifest.get('child_map', {})

    # Get child IDs
    if recursive:
        child_ids = _get_all_relations_recursive(child_map, unique_id)
    else:
        child_ids = child_map.get(unique_id, [])

    # Get child details (filter out tests)
    children_details = []
    nodes = parser.manifest.get('nodes', {})
    sources = parser.manifest.get('sources', {})

    for child_id in child_ids:
        # Get from nodes or sources
        child_node = nodes.get(child_id) or sources.get(child_id)

        if not child_node:
            continue

        # Filter out tests
        if child_node.get('resource_type') == 'test':
            continue

        children_details.append({
            'unique_id': child_id,
            'name': child_node.get('name', ''),
            'type': child_node.get('resource_type', ''),
            'database': child_node.get('database', ''),
            'schema': child_node.get('schema', '')
        })

    return children_details


def schema_dev(manifest_path: str, model_name: str) -> Optional[Dict[str, str]]:
    """
    Get dev table location (development schema)

    Args:
        manifest_path: Path to manifest.json
        model_name: Model name

    Returns:
        Dictionary with:
        - schema: Development schema name
        - table: model name (NOT alias!)
        - full_name: schema.model_name

        Returns None if model not found.

    Note: Dev tables use SQL filename, not config.alias

    Environment variables (in priority order):
        1. DBT_DEV_SCHEMA: Full dev schema name (e.g., "my_dev_schema")
           - Highest priority, bypasses all template logic
        2. DBT_DEV_SCHEMA_TEMPLATE: Schema template with {username} placeholder
           - Example: "dev_{username}" → "dev_pavel_filianin"
           - Example: "{username}_sandbox" → "pavel_filianin_sandbox"
           - Example: "{username}" → "pavel_filianin"
        3. DBT_DEV_SCHEMA_PREFIX: Prefix for schema (becomes "prefix_{username}")
           - Example: "personal" → "personal_pavel_filianin"
           - Empty string "" means no prefix → just "{username}"
        4. Default: "personal_{username}"

    Username priority: $DBT_USER > $USER > getpass.getuser()
    Dots in username are replaced with underscores for BigQuery compatibility
    """
    import os
    import getpass

    parser = _get_cached_parser(manifest_path)
    model = parser.get_model(model_name)

    if not model:
        return None

    # Priority 1: Check if full dev schema is specified
    dev_schema = os.environ.get('DBT_DEV_SCHEMA')

    if not dev_schema:
        # Get username from DBT_USER env var, fallback to system USER, then getpass
        username = os.environ.get('DBT_USER') or os.environ.get('USER') or getpass.getuser()
        # Replace dots with underscores for BigQuery compatibility
        username = username.replace('.', '_')

        # Priority 2: Check for template
        template = os.environ.get('DBT_DEV_SCHEMA_TEMPLATE')
        if template:
            dev_schema = template.format(username=username)
        else:
            # Priority 3: Check for prefix (default: "personal")
            prefix = os.environ.get('DBT_DEV_SCHEMA_PREFIX', 'personal')

            # Combine prefix and username
            if prefix:
                dev_schema = f"{prefix}_{username}"
            else:
                # No prefix, just username
                dev_schema = username

    # Optional: Sanitize schema name for BigQuery compatibility
    # Only if DBT_VALIDATE_BIGQUERY is set (opt-in, not breaking for other DWH)
    if os.environ.get('DBT_VALIDATE_BIGQUERY', '').lower() in ('true', '1', 'yes'):
        sanitized_schema, warnings = _sanitize_bigquery_name(dev_schema, "dataset")
        if warnings:
            for warning in warnings:
                print(f"⚠️  BigQuery validation: {warning}", file=sys.stderr)
        dev_schema = sanitized_schema

    # Dev table name uses model.name (filename), NOT config.alias
    table_name = model.get('name', '')

    return {
        'schema': dev_schema,
        'table': table_name,
        'full_name': f"{dev_schema}.{table_name}"
    }


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


def docs(manifest_path: str, model_name: str) -> Optional[List[Dict[str, str]]]:
    """
    Get columns with descriptions

    Args:
        manifest_path: Path to manifest.json
        model_name: Model name

    Returns:
        List of dictionaries with:
        - name: Column name
        - data_type: Column data type
        - description: Column description

        Returns None if model not found.
    """
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
