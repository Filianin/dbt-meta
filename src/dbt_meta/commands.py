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
        print("✅ Local manifest refreshed (./target/manifest.json)")
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
        print("✅ Production artifacts synced (~/dbt-state/)")


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


def ls(
    manifest_path: str,
    selectors: Optional[list[str]] = None,
    modified: bool = False,
    refresh: bool = False,
    and_logic: bool = False,
    group: bool = False,
    tree_view: bool = False,
    use_dev: bool = False,
    json_output: bool = False
) -> str | list[dict[str, Any]] | dict[str, list[dict[str, Any]]]:
    """
    Filter and list dbt models (replaces dbt ls)

    Args:
        manifest_path: Path to manifest.json
        selectors: List of selectors (tag:name, config.key:value, path:pattern, package:name)
        modified: Show only modified/new models (git-aware)
        refresh: Show models requiring --full-refresh (modified + intermediate + downstream)
        and_logic: Require ALL tags (default: OR - at least one)
        group: Group by tag combinations
        use_dev: Use dev manifest
        json_output: Return metadata dict

    Returns:
        Default text mode: Space-separated model names
        --group text mode: Grouped with headers
        Default JSON mode: List of dicts
        --group JSON mode: Dict of groups

    Selectors:
        - tag:verified - models with 'verified' tag
        - config.materialized:table - models with specific config value
        - path:models/core/ - models in specific path
        - package:dbt_utils - models from specific package

    Examples:
        meta ls tag:verified tag:active           # OR: at least one tag
        meta ls tag:verified tag:active --and     # AND: both tags required
        meta ls tag:verified tag:active --group   # Grouped output
        meta ls config.materialized:incremental
        meta ls --modified                        # Git-modified only
        meta ls --refresh                         # Models needing --full-refresh
    """
    parser = _get_cached_parser(manifest_path)
    models = parser.get_all_models()

    # Extract tag selectors for grouping
    tag_selectors = [s.split(':', 1)[1] for s in (selectors or []) if s.startswith('tag:')]

    # Save modified models for tree view (before refresh expansion)
    modified_models_for_tree = []

    # Filter by refresh requirement (modified + intermediate + downstream)
    if refresh:
        # Get modified models first (for tree view)
        modified_models_for_tree = _filter_modified_models(models, parser)
        filtered_models = _filter_refresh_models(models, parser, manifest_path)
    # Filter by git status only (modified models)
    elif modified:
        filtered_models = _filter_modified_models(models, parser)
    # Filter by selectors
    elif selectors:
        if and_logic and tag_selectors:
            # AND logic - model must have ALL tags
            filtered_models = _filter_by_selectors_and(models, selectors, parser)
        else:
            # OR logic (default) - model needs at least one tag
            filtered_models = _filter_by_selectors_or(models, selectors, parser)
    else:
        # No filters - return all
        filtered_models = list(models.values())

    # Group by tag combinations if requested
    if group and tag_selectors:
        return _format_models_grouped(
            filtered_models,
            tag_selectors,
            parser,
            use_dev,
            json_output
        )

    # Print git warnings for modified/refresh modes
    if modified or refresh:
        if filtered_models:
            warnings = _generate_git_warnings(filtered_models, use_dev)
            _print_warnings(warnings, json_output=json_output)
        else:
            # Empty result - provide helpful warning
            if modified:
                empty_warnings = [{
                    "type": "no_modified_models",
                    "severity": "info",
                    "message": "No modified models found",
                    "detail": "No models changed compared to main/master branch",
                    "suggestion": "All models are in sync with production"
                }]
            else:  # refresh
                empty_warnings = [{
                    "type": "no_refresh_needed",
                    "severity": "info",
                    "message": "No models need refresh",
                    "detail": "No modified models found, so no downstream models to refresh",
                    "suggestion": "All models are in sync with production"
                }]
            _print_warnings(empty_warnings, json_output=json_output)

    # Tree view for --refresh --all (text mode only)
    if tree_view and refresh and not json_output:
        return _format_refresh_tree(filtered_models, modified_models_for_tree, parser, models)

    # Standard format output
    if json_output:
        # Compact format for refresh mode, detailed format otherwise
        if refresh or modified:
            return _format_models_json_compact(filtered_models, parser, use_dev)
        else:
            return _format_models_json(filtered_models, parser, use_dev)
    else:
        # Text mode: add + suffix for refresh mode (for dbt select syntax)
        if refresh:
            return _format_models_text_with_suffix(filtered_models, suffix="+")
        else:
            return _format_models_text(filtered_models)


def _filter_by_selectors_or(models: dict[str, Any], selectors: list[str], parser: Any) -> list[dict[str, Any]]:
    """Filter with OR logic for tags, AND for other selectors"""
    # Separate tag selectors from others
    tag_selectors = [s for s in selectors if s.startswith('tag:')]
    other_selectors = [s for s in selectors if not s.startswith('tag:')]

    filtered = list(models.values())

    # Apply non-tag selectors (AND logic)
    for selector in other_selectors:
        filtered = _apply_selector(filtered, selector)

    # Apply tag selectors (OR logic) - at least one tag
    if tag_selectors:
        tags = [s.split(':', 1)[1] for s in tag_selectors]
        filtered = [m for m in filtered
                   if any(tag in m.get('tags', []) for tag in tags)]

    return filtered


def _filter_by_selectors_and(models: dict[str, Any], selectors: list[str], parser: Any) -> list[dict[str, Any]]:
    """Filter with AND logic for tags"""
    filtered = list(models.values())

    # Apply all selectors with AND logic
    for selector in selectors:
        filtered = _apply_selector(filtered, selector)

    return filtered


def _apply_selector(models: list[dict[str, Any]], selector: str) -> list[dict[str, Any]]:
    """Apply single selector filter"""
    if ':' not in selector:
        return models

    # Special handling for config.key:value format
    if selector.startswith('config.'):
        # Format: config.materialized:incremental
        # Split by last ':' to get config.key and value
        if ':' not in selector[7:]:  # Check if there's a ':' after 'config.'
            return models
        config_part, config_val = selector.rsplit(':', 1)
        config_key = config_part[7:]  # Remove 'config.' prefix
        return [m for m in models
               if m.get('config', {}).get(config_key) == config_val]

    selector_type, selector_value = selector.split(':', 1)

    if selector_type == 'tag':
        return [m for m in models if selector_value in m.get('tags', [])]

    elif selector_type == 'path':
        return [m for m in models
               if m.get('original_file_path', '').startswith(selector_value)]

    elif selector_type == 'package':
        return [m for m in models if m.get('package_name') == selector_value]

    return models


def _generate_git_warnings(models: list[dict[str, Any]], use_dev: bool) -> list[dict[str, str]]:
    """Generate warnings for models with git status metadata

    Returns:
        List of warning dicts for print_warnings()
    """
    warnings = []

    # Count models by status
    uncommitted_models = [m for m in models if m.get('_git_status') == 'uncommitted']
    committed_models = [m for m in models if m.get('_git_status') == 'committed']

    # Create single INFO block with both uncommitted and committed counts
    if uncommitted_models or committed_models:
        parts = []
        if uncommitted_models:
            parts.append(f"{len(uncommitted_models)} uncommitted")
        if committed_models:
            parts.append(f"{len(committed_models)} committed")

        message = f"Found {' and '.join(parts)} model(s) in current branch"

        # Suggestion depends on --dev flag usage
        if use_dev:
            suggestion = "Using dev tables for branch changes"
        else:
            suggestion = "Use --dev flag to query dev tables if needed"

        warnings.append({
            "type": "git_branch_changes",
            "severity": "info",
            "message": message,
            "suggestion": suggestion
        })

    return warnings


def _format_refresh_tree(
    all_models: list[dict[str, Any]],
    modified_models: list[dict[str, Any]],
    parser: Any,
    models_dict: dict[str, Any]
) -> str:
    """Format refresh models as tree view showing lineage from modified to downstream

    Args:
        all_models: All models in refresh set (modified + downstream)
        modified_models: Only modified models (roots of trees)
        parser: Manifest parser
        models_dict: Full models dictionary from manifest

    Returns:
        Tree-formatted string showing modified models and their descendants
    """
    if not modified_models:
        return "No modified models found"

    output_lines = []
    all_model_uids = {m['unique_id'] for m in all_models}

    def print_tree(node_uid: str, prefix: str = "", is_last: bool = True, visited: set[str] | None = None) -> None:
        """Recursively print tree structure"""
        if visited is None:
            visited = set()

        if node_uid in visited:  # Avoid infinite loops
            return
        visited.add(node_uid)

        node_name = node_uid.split('.')[-1]

        # Find direct children in the refresh set
        children_uids = []
        for uid in all_model_uids:
            if uid == node_uid or uid in visited:
                continue
            model = models_dict.get(uid)
            if model:
                depends_on = model.get('depends_on', {}).get('nodes', [])
                if node_uid in depends_on:
                    children_uids.append(uid)

        # Print children recursively
        children_sorted = sorted(children_uids, key=lambda u: u.split('.')[-1])
        for i, child_uid in enumerate(children_sorted):
            is_last_child = (i == len(children_sorted) - 1)
            child_prefix = "└── " if is_last_child else "├── "
            child_name = child_uid.split('.')[-1]
            output_lines.append(f"{prefix}{child_prefix}{child_name}")

            # Recursive call with updated prefix
            new_prefix = prefix + ("    " if is_last_child else "│   ")
            print_tree(child_uid, new_prefix, is_last_child, visited)

    for modified in modified_models:
        mod_uid = modified['unique_id']
        mod_name = mod_uid.split('.')[-1]

        # Get git status indicator
        git_status = modified.get('_git_status', '')
        if git_status == 'uncommitted':
            status_icon = "🔴"  # Uncommitted changes (red circle)
        elif git_status == 'committed':
            status_icon = "✅"  # Committed changes (green checkmark)
        else:
            status_icon = "•"

        output_lines.append(f"{status_icon} {mod_name}")

        # Print full tree starting from this modified model
        print_tree(mod_uid, "  ")

        output_lines.append("")  # Empty line between trees

    return '\n'.join(output_lines)


def _format_models_text(models: list[dict[str, Any]]) -> str:
    """Format as space-separated model names"""
    model_names = [m['unique_id'].split('.')[-1] for m in models]
    return ' '.join(sorted(model_names))


def _format_models_text_with_suffix(models: list[dict[str, Any]], suffix: str = "+") -> str:
    """Format as space-separated model names with suffix (for dbt select syntax)"""
    model_names = [m['unique_id'].split('.')[-1] + suffix for m in models]
    return ' '.join(sorted(model_names))


def _format_models_json_compact(models: list[dict[str, Any]], parser: Any, use_dev: bool) -> dict[str, list[str]]:
    """Format as compact dict with models and tables arrays"""
    model_names = []
    table_names = []

    for model in models:
        model_name = model['unique_id'].split('.')[-1]
        model_names.append(model_name)

        # Get schema info
        schema_name = model.get('schema', '')
        table_name = model.get('alias') or model.get('name', model_name)
        full_table = f"{schema_name}.{table_name}" if schema_name else table_name
        table_names.append(full_table)

    return {
        'models': sorted(model_names),
        'tables': sorted(table_names)
    }


def _format_models_json(models: list[dict[str, Any]], parser: Any, use_dev: bool) -> list[dict[str, Any]]:
    """Format as list of metadata dicts"""
    result = []
    for model in models:
        model_name = model['unique_id'].split('.')[-1]

        # Get schema info
        schema_name = model.get('schema', '')
        table_name = model.get('alias') or model.get('name', model_name)

        model_dict = {
            'model': model_name,
            'table': f"{schema_name}.{table_name}" if schema_name else table_name,
            'tags': model.get('tags', []),
            'materialized': model.get('config', {}).get('materialized', 'view'),
            'path': model.get('original_file_path', '')
        }

        # Add git status if present (for --modified/--refresh)
        if '_git_status' in model:
            model_dict['git_status'] = model['_git_status']

        result.append(model_dict)

    return sorted(result, key=lambda x: x['model'])


def _filter_modified_models(models: dict[str, Any], parser: Any) -> list[dict[str, Any]]:
    """Filter models modified compared to main branch (committed + uncommitted)

    Returns models with additional '_git_status' field:
    - 'uncommitted' - has local changes (unstaged/staged/new)
    - 'committed' - committed in current branch but not in main
    """
    import subprocess

    # Call git commands ONCE for all models (performance optimization)
    try:
        # 1. Get all files changed between main/master and current branch (including commits)
        # Try origin/main first, then origin/master, then local main, then local master
        main_diff_result = None
        for base_branch in ['origin/main', 'origin/master', 'main', 'master']:
            result = subprocess.run(
                ['git', 'diff', f'{base_branch}...HEAD', '--name-only'],
                capture_output=True,
                text=True,
                timeout=5
            )
            if result.returncode == 0:
                main_diff_result = result
                break

        # If no base branch found, return empty (can't detect changes)
        if main_diff_result is None or main_diff_result.returncode != 0:
            return []

        # 2. Get uncommitted changes (unstaged)
        unstaged_result = subprocess.run(
            ['git', 'diff', 'HEAD', '--name-only'],
            capture_output=True,
            text=True,
            timeout=5
        )

        # 3. Get staged changes
        staged_result = subprocess.run(
            ['git', 'diff', '--cached', '--name-only'],
            capture_output=True,
            text=True,
            timeout=5
        )

        # 4. Get new untracked files
        status_result = subprocess.run(
            ['git', 'status', '--porcelain'],
            capture_output=True,
            text=True,
            timeout=5
        )

        # Parse results
        main_diff_files = set(main_diff_result.stdout.splitlines()) if main_diff_result.returncode == 0 else set()
        unstaged_files = set(unstaged_result.stdout.splitlines()) if unstaged_result.returncode == 0 else set()
        staged_files = set(staged_result.stdout.splitlines()) if staged_result.returncode == 0 else set()
        new_files = set(
            line[3:].strip() for line in status_result.stdout.splitlines()
            if line.startswith('??')
        ) if status_result.returncode == 0 else set()

        # All uncommitted changes
        uncommitted_files = unstaged_files | staged_files | new_files

    except (subprocess.TimeoutExpired, FileNotFoundError, subprocess.CalledProcessError, OSError):
        # Git check failed - return empty list (safe default)
        return []

    # Filter models and add git status
    modified = []
    for uid, model in models.items():
        file_path = model.get('original_file_path', '')

        # Check if model is in branch changes (committed) OR has uncommitted changes
        in_branch_diff = any(file_path in changed_file or changed_file.endswith(file_path)
                            for changed_file in main_diff_files)
        has_uncommitted = any(file_path in f or f.endswith(file_path) for f in uncommitted_files)

        # Include model if EITHER condition is true
        if in_branch_diff or has_uncommitted:
            model_copy = model.copy()
            # Priority: uncommitted takes precedence over committed
            if has_uncommitted:
                model_copy['_git_status'] = 'uncommitted'
            else:
                model_copy['_git_status'] = 'committed'
            modified.append(model_copy)

    return modified


def _format_models_grouped(
    models: list[dict[str, Any]],
    tags: list[str],
    parser: Any,
    use_dev: bool,
    json_output: bool
) -> str | dict[str, list[dict[str, Any]]]:
    """Group models by tag combinations"""
    from itertools import combinations

    groups: dict[str, list[dict[str, Any]]] = {}

    # Generate all tag combinations
    # Single tags
    for tag in tags:
        groups[f"tag:{tag}"] = []

    # Tag combinations (only for specified tags)
    for r in range(2, len(tags) + 1):
        for combo in combinations(tags, r):
            groups[" ".join(f"tag:{t}" for t in combo)] = []

    # Assign models to groups
    for model in models:
        model_tags = set(model.get('tags', []))
        matched_tags = [t for t in tags if t in model_tags]

        if not matched_tags:
            continue

        # Find exact tag match group
        if len(matched_tags) == 1:
            group_key = f"tag:{matched_tags[0]}"
        else:
            # Multiple tags - create combination key
            sorted_tags = sorted(matched_tags)
            group_key = " ".join(f"tag:{t}" for t in sorted_tags)

        if group_key in groups:
            groups[group_key].append(model)

    # Format output
    if json_output:
        # JSON: dict of groups with metadata
        result = {}
        for group_key, group_models in groups.items():
            if group_models:  # Only include non-empty groups
                result[group_key] = _format_models_json(group_models, parser, use_dev)
        return result
    else:
        # Text: grouped with headers
        output_lines = []
        for group_key, group_models in groups.items():
            if group_models:  # Only show non-empty groups
                model_names = ' '.join(sorted(m['unique_id'].split('.')[-1] for m in group_models))
                output_lines.append(f"{group_key}:")
                output_lines.append(model_names)
                output_lines.append("")  # Empty line between groups

        return '\n'.join(output_lines).rstrip()


def _get_all_descendants_recursive(node_uid: str, all_models: dict[str, Any]) -> set[str]:
    """Find all descendant models recursively using BFS

    Args:
        node_uid: Starting node unique_id
        all_models: Dict of all models {unique_id: model_dict}

    Returns:
        Set of unique_ids of all descendants
    """
    from collections import deque

    descendants = set()
    queue = deque([node_uid])
    visited = {node_uid}

    while queue:
        current_uid = queue.popleft()

        # Find all models that depend on current model
        for uid, model in all_models.items():
            if uid in visited:
                continue

            # Check if this model depends on current
            depends_on = model.get('depends_on', {}).get('nodes', [])
            if current_uid in depends_on:
                descendants.add(uid)
                visited.add(uid)
                queue.append(uid)

    return descendants


def _filter_refresh_models(models: dict[str, Any], parser: Any, manifest_path: str) -> list[dict[str, Any]]:
    """
    Filter models requiring --full-refresh

    Includes:
    1. All modified/new models (via git)
    2. All downstream dependencies of modified models
    3. Intermediate models between multiple modified models

    Algorithm:
    - Find all modified models M = {m1, m2, ...}
    - For each mi in M: find all descendants D(mi)
    - Find intermediate models I = models on paths between any two modified models
    - Return M ∪ D(m1) ∪ D(m2) ∪ ... ∪ I
    """
    # Step 1: Find all modified models
    modified_models = _filter_modified_models(models, parser)
    if not modified_models:
        return []

    result_set = set(m['unique_id'] for m in modified_models)

    # Step 2: Find all downstream for each modified model
    # Use direct manifest traversal to avoid duplicate warnings from children()
    for modified_model in modified_models:
        uid = modified_model['unique_id']
        # Find all descendants recursively using BFS
        descendants = _get_all_descendants_recursive(uid, models)
        result_set.update(descendants)

    # Step 3: Find intermediate models (if 2+ modified models)
    if len(modified_models) >= 2:
        intermediate = _find_intermediate_models(modified_models, models, parser)
        result_set.update(uid for uid in intermediate)

    # Create dict of modified models with git status
    modified_uid_to_status = {m['unique_id']: m.get('_git_status') for m in modified_models}

    # Convert unique_ids back to model dicts, preserving git status
    result_models = []
    for uid in result_set:
        if uid in models:
            model_copy = models[uid].copy()
            # Add git status if this was a modified model
            if uid in modified_uid_to_status:
                model_copy['_git_status'] = modified_uid_to_status[uid]
            result_models.append(model_copy)

    return result_models


def _find_intermediate_models(
    modified_models: list[dict[str, Any]],
    all_models: dict[str, Any],
    parser: Any
) -> set[str]:
    """
    Find models on paths between modified models

    For each pair of modified models (m1, m2):
    - If m2 is downstream of m1: include all models on path m1 -> m2
    - Build dependency graph and use BFS to find paths
    """
    intermediate = set()

    # Build parent-child relationship map for all models
    parent_map: dict[str, list[str]] = {}  # model_uid -> list of parent unique_ids
    for uid, model in all_models.items():
        parents = model.get('depends_on', {}).get('nodes', [])
        parent_map[uid] = parents

    # For each pair of modified models
    for i, m1 in enumerate(modified_models):
        for m2 in modified_models[i+1:]:
            uid1 = m1['unique_id']
            uid2 = m2['unique_id']

            # Find if there's a path from m1 to m2 (or vice versa)
            path_1_to_2 = _find_path_between(uid1, uid2, parent_map)
            if path_1_to_2:
                intermediate.update(path_1_to_2)

            path_2_to_1 = _find_path_between(uid2, uid1, parent_map)
            if path_2_to_1:
                intermediate.update(path_2_to_1)

    return intermediate


def _find_path_between(
    source: str,
    target: str,
    parent_map: dict[str, list[str]]
) -> list[str]:
    """
    Find models on path from source to target using BFS
    Returns list of unique_ids on the path (excluding source and target)
    """
    from collections import deque

    # BFS to find path
    queue: deque = deque([(source, [source])])
    visited = {source}

    while queue:
        current, path = queue.popleft()

        # Check children of current node
        for node_uid, parents in parent_map.items():
            if current in parents and node_uid not in visited:
                new_path = path + [node_uid]

                if node_uid == target:
                    # Found path! Return intermediate nodes (exclude source and target)
                    return new_path[1:-1]

                visited.add(node_uid)
                queue.append((node_uid, new_path))

    return []  # No path found
