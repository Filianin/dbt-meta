"""
Commands - Model metadata extraction functions

Provides high-level commands for extracting metadata from dbt manifest.
Each command returns formatted data matching bash version output.
"""

import subprocess
from pathlib import Path
from typing import Any, Optional

from dbt_meta.command_impl.children import ChildrenCommand
from dbt_meta.command_impl.columns import ColumnsCommand
from dbt_meta.command_impl.config import ConfigCommand
from dbt_meta.command_impl.deps import DepsCommand
from dbt_meta.command_impl.info import InfoCommand
from dbt_meta.command_impl.parents import ParentsCommand
from dbt_meta.command_impl.path import PathCommand
from dbt_meta.command_impl.schema import SchemaCommand
from dbt_meta.command_impl.sql import SqlCommand

# Command classes
from dbt_meta.config import Config
from dbt_meta.errors import DbtMetaError
from dbt_meta.utils import get_cached_parser as _get_cached_parser
from dbt_meta.utils import print_warnings as _print_warnings
from dbt_meta.utils.dev import (
    find_dev_manifest as _find_dev_manifest,
)
from dbt_meta.utils.git import check_manifest_git_mismatch as _check_manifest_git_mismatch

# Dev and BigQuery utility functions are now imported from utils.dev and utils.bigquery


def info(manifest_path: str, model_name: str, use_dev: bool = False, json_output: bool = False) -> Optional[dict[str, Any]]:
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
    config = Config.from_config_or_env()
    command = InfoCommand(config, manifest_path, model_name, use_dev, json_output)
    return command.execute()


def schema(manifest_path: str, model_name: str, use_dev: bool = False, json_output: bool = False) -> Optional[dict[str, str]]:
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
        - Returns dev schema name (e.g., personal_alice)
        - Uses model filename, NOT alias
        - Falls back to BigQuery if not in dev manifest

    Behavior with use_dev=False (default):
        - Searches production manifest (~/dbt-state/) first
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
    config = Config.from_config_or_env()
    command = SchemaCommand(config, manifest_path, model_name, use_dev, json_output)
    return command.execute()


def columns(manifest_path: str, model_name: str, use_dev: bool = False, json_output: bool = False) -> Optional[list[dict[str, str]]]:
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
    config = Config.from_config_or_env()
    command = ColumnsCommand(config, manifest_path, model_name, use_dev, json_output)
    return command.execute()


def config(manifest_path: str, model_name: str, use_dev: bool = False, json_output: bool = False) -> Optional[dict[str, Any]]:
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
    cfg = Config.from_config_or_env()
    command = ConfigCommand(cfg, manifest_path, model_name, use_dev, json_output)
    return command.execute()


def deps(manifest_path: str, model_name: str, use_dev: bool = False, json_output: bool = False) -> Optional[dict[str, list[str]]]:
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
    cfg = Config.from_config_or_env()
    command = DepsCommand(cfg, manifest_path, model_name, use_dev, json_output)
    return command.execute()


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
    cfg = Config.from_config_or_env()
    command = SqlCommand(cfg, manifest_path, model_name, use_dev, json_output, raw=raw)
    return command.execute()



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
    cfg = Config.from_config_or_env()
    command = PathCommand(cfg, manifest_path, model_name, use_dev, json_output)
    return command.execute()

def list_models(manifest_path: str, pattern: Optional[str] = None) -> list[str]:
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
            for uid in models
            if pattern_lower in uid.split('.')[-1].lower()
        ]
    else:
        model_names = [uid.split('.')[-1] for uid in models]

    return sorted(model_names)


def search(manifest_path: str, query: str) -> list[dict[str, str]]:
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
    relation_map: dict[str, list[str]],
    node_id: str,
    visited: Optional[set] = None
) -> list[str]:
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
    if visited is None:  # pragma: no cover
        visited = set()

    if node_id in visited:  # pragma: no cover
        return []

    visited.add(node_id)
    relations = relation_map.get(node_id, [])

    all_relations = list(relations)
    for relation_id in relations:  # pragma: no cover
        all_relations.extend(_get_all_relations_recursive(relation_map, relation_id, visited))

    # Return unique items (preserving order with dict.fromkeys)
    return list(dict.fromkeys(all_relations))


def parents(manifest_path: str, model_name: str, use_dev: bool = False, recursive: bool = False, json_output: bool = False) -> Optional[list[dict[str, str]]]:
    """Get upstream dependencies (parent models).

    Args:
        manifest_path: Path to manifest.json
        model_name: Model name
        use_dev: If True, prioritize dev manifest over production
        recursive: If True, get all ancestors. If False, only direct parents.
        json_output: If True, return ultra-compact format for AI agents

    Returns:
        Parent dependencies list, or None if model not found
    """
    config = Config.from_config_or_env()
    command = ParentsCommand(config, manifest_path, model_name, use_dev, json_output, recursive=recursive)
    return command.execute()


def children(manifest_path: str, model_name: str, use_dev: bool = False, recursive: bool = False, json_output: bool = False) -> Optional[list[dict[str, str]]]:
    """Get downstream dependencies (child models).

    Args:
        manifest_path: Path to manifest.json
        model_name: Model name
        use_dev: If True, prioritize dev manifest over production
        recursive: If True, get all descendants. If False, only direct children.
        json_output: If True, return ultra-compact format for AI agents

    Returns:
        Child dependencies list, or None if model not found
    """
    config = Config.from_config_or_env()
    command = ChildrenCommand(config, manifest_path, model_name, use_dev, json_output, recursive=recursive)
    return command.execute()


def refresh(use_dev: bool = False) -> None:
    """
    Refresh dbt artifacts (manifest.json + catalog.json)

    Args:
        use_dev: If True, parse local project (dbt parse --target dev)
                If False, sync production artifacts from remote storage

    Raises:
        DbtMetaError: If sync script not found or command fails
        subprocess.CalledProcessError: If subprocess fails
    """
    if use_dev:
        # Dev mode: parse local project
        print("Parsing local dbt project...")
        subprocess.run(['dbt', 'parse', '--target', 'dev'], check=True)
        print("✓ Local manifest refreshed (./target/manifest.json)")
    else:
        # Production mode: sync from remote storage with --force
        script_path = Path.home() / '.claude' / 'scripts' / 'sync-artifacts.sh'
        if not script_path.exists():
            raise DbtMetaError(
                f"Sync script not found: {script_path}",
                suggestion="Install sync-artifacts.sh in ~/.claude/scripts/"
            )

        print("Syncing production artifacts from remote storage...")
        subprocess.run([str(script_path), '--force'], check=True)
        print("✓ Production artifacts synced (~/dbt-state/)")


def docs(manifest_path: str, model_name: str, use_dev: bool = False, json_output: bool = False) -> Optional[list[dict[str, str]]]:
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
            except (FileNotFoundError, OSError, KeyError):  # pragma: no cover
                # Dev manifest not available or structure different - continue
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
