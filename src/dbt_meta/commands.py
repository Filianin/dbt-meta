"""
Commands - Model metadata extraction functions

Provides high-level commands for extracting metadata from dbt manifest.
Each command returns formatted data matching bash version output.
"""

from __future__ import annotations

import subprocess
from pathlib import Path
from typing import Any

from dbt_meta.command_impl.analyze import AnalyzeCommand
from dbt_meta.command_impl.branch import BranchCommand
from dbt_meta.command_impl.children import ChildrenCommand
from dbt_meta.command_impl.columns import ColumnsCommand
from dbt_meta.command_impl.config import ConfigCommand
from dbt_meta.command_impl.hotspots import HotspotsCommand
from dbt_meta.command_impl.info import InfoCommand
from dbt_meta.command_impl.parents import ParentsCommand
from dbt_meta.command_impl.path import PathCommand
from dbt_meta.command_impl.powerbi import PowerBiCommand
from dbt_meta.command_impl.scan import ScanCommand
from dbt_meta.command_impl.schema import SchemaCommand
from dbt_meta.command_impl.sql import SqlCommand
from dbt_meta.command_impl.validate import ValidateCommand

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


def info(manifest_path: str, model_name: str, use_dev: bool = False, json_output: bool = False) -> dict[str, Any] | None:
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


def schema(manifest_path: str, model_name: str, use_dev: bool = False, json_output: bool = False) -> dict[str, str] | None:
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


def columns(manifest_path: str, model_name: str, use_dev: bool = False, json_output: bool = False) -> list[dict[str, str]] | None:
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


def columns_search(
    manifest_path: str,
    pattern: str,
    case_sensitive: bool = False,
) -> list[dict[str, Any]]:
    """Repo-wide column search: which models declare a column matching pattern?

    Searches the manifest's ``columns`` block on every model. Columns are
    declared in dbt schema.yml, so this finds documented columns — not every
    column physically present in the underlying table.

    Args:
        manifest_path: Path to manifest.json
        pattern: Column-name substring to match
        case_sensitive: If True, match case exactly (default: False)

    Returns:
        List of {model, unique_id, column, data_type, description} sorted by
        (model, column). Empty list if nothing matched.
    """
    parser = _get_cached_parser(manifest_path)
    needle = pattern if case_sensitive else pattern.lower()
    out: list[dict[str, Any]] = []
    for uid, model in parser.get_all_models().items():
        model_name = uid.split('.')[-1]
        for col_name, col_data in model.get('columns', {}).items():
            haystack = col_name if case_sensitive else col_name.lower()
            if needle not in haystack:
                continue
            out.append({
                'model': model_name,
                'unique_id': uid,
                'column': col_name,
                'data_type': col_data.get('data_type', '') or '',
                'description': col_data.get('description', '') or '',
            })
    return sorted(out, key=lambda x: (x['model'], x['column']))


def config(manifest_path: str, model_name: str, use_dev: bool = False, json_output: bool = False) -> dict[str, Any] | None:
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


def sql(manifest_path: str, model_name: str, use_dev: bool = False, raw: bool = False, json_output: bool = False) -> str | None:
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


def validate(manifest_path: str, model_name: str, use_dev: bool = False, json_output: bool = False) -> dict[str, Any] | None:
    """
    Validate model SQL syntax using BigQuery dry run.

    Args:
        manifest_path: Path to manifest.json
        model_name: Model name
        use_dev: If True, use dev manifest SQL

    Returns:
        Dictionary with:
        - model: Model name
        - valid: True if SQL is valid
        - error: Error message (None if valid)

        Returns None if model not found.
    """
    cfg = Config.from_config_or_env()
    command = ValidateCommand(cfg, manifest_path, model_name, use_dev, json_output)
    return command.execute()


def scan(manifest_path: str, model_name: str, use_dev: bool = False, json_output: bool = False) -> dict[str, Any] | None:
    """
    Estimate query scan size using BigQuery dry run.

    Args:
        manifest_path: Path to manifest.json
        model_name: Model name
        use_dev: If True, use dev manifest SQL

    Returns:
        Dictionary with:
        - model: Model name
        - bytes: Estimated bytes to scan
        - formatted: Human-readable size (e.g., "1.5 GB")
        - error: Error message if validation failed

        Returns None if model not found.
    """
    cfg = Config.from_config_or_env()
    command = ScanCommand(cfg, manifest_path, model_name, use_dev, json_output)
    return command.execute()


def path(manifest_path: str, model_name: str, use_dev: bool = False, json_output: bool = False) -> str | None:
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

def list_models(manifest_path: str, pattern: str | None = None) -> list[str]:
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


def resolve(manifest_path: str, query: str, limit: int = 5, cutoff: float = 0.6) -> list[dict[str, Any]]:
    """Fuzzy resolve a typo'd model name to the closest manifest matches.

    Uses ``difflib.get_close_matches`` over all model names. Useful when
    a `meta info <name>` failed with "model not found" and the user wants
    a quick "did you mean ...?" hint without running `meta list` and
    grepping the output.

    Args:
        manifest_path: Path to manifest.json
        query: Model name to match (case-insensitive)
        limit: Max number of matches to return (default 5)
        cutoff: difflib similarity cutoff in [0, 1] (default 0.6)

    Returns:
        List of {name, unique_id, score} sorted by score desc.
    """
    import difflib

    parser = _get_cached_parser(manifest_path)
    models = parser.get_all_models()

    # Map lowercase name → (original name, unique_id)
    name_index: dict[str, tuple[str, str]] = {}
    for uid in models:
        name = uid.split('.')[-1]
        name_index.setdefault(name.lower(), (name, uid))

    matches = difflib.get_close_matches(
        query.lower(), list(name_index.keys()), n=limit, cutoff=cutoff
    )

    result = []
    for m in matches:
        name, uid = name_index[m]
        score = difflib.SequenceMatcher(None, query.lower(), m).ratio()
        result.append({'name': name, 'unique_id': uid, 'score': round(score, 3)})
    return result


def find(manifest_path: str, fqn: str) -> list[dict[str, Any]]:
    """Reverse-lookup: physical FQN → dbt model(s).

    Accepts ``table``, ``schema.table``, or ``database.schema.table``.
    Matches against each model's resolved physical name (``config.alias``
    or ``name``) and, when provided, schema/database.

    Args:
        manifest_path: Path to manifest.json
        fqn: Physical reference. One of:
            - ``table``                       (any schema/database)
            - ``schema.table``                (any database)
            - ``database.schema.table``       (exact match)

    Returns:
        List of {name, unique_id, database, schema, table, alias,
                 materialized, file} sorted by unique_id. Empty list
        if nothing matched.
    """
    parts = fqn.split('.')
    if len(parts) == 1:
        want_db, want_schema, want_table = None, None, parts[0]
    elif len(parts) == 2:
        want_db, want_schema, want_table = None, parts[0], parts[1]
    elif len(parts) == 3:
        want_db, want_schema, want_table = parts[0], parts[1], parts[2]
    else:
        raise DbtMetaError(
            f"Invalid FQN: {fqn!r}",
            suggestion="Use 'table', 'schema.table', or 'database.schema.table'",
        )

    parser = _get_cached_parser(manifest_path)
    out: list[dict[str, Any]] = []
    for uid, model in parser.get_all_models().items():
        cfg = model.get('config', {})
        alias = cfg.get('alias')
        physical = alias or model.get('name', '')
        if physical != want_table:
            continue
        m_schema = model.get('schema', '')
        m_db = model.get('database', '')
        if want_schema is not None and m_schema != want_schema:
            continue
        if want_db is not None and m_db != want_db:
            continue
        out.append({
            'name': uid.split('.')[-1],
            'unique_id': uid,
            'database': m_db,
            'schema': m_schema,
            'table': physical,
            'alias': alias,
            'materialized': cfg.get('materialized', 'view'),
            'file': model.get('original_file_path', ''),
        })
    return sorted(out, key=lambda x: x['unique_id'])


def sources(
    manifest_path: str,
    name_filter: str | None = None,
    freshness_only: bool = False,
) -> list[dict[str, Any]]:
    """List sources from manifest with optional freshness metadata.

    Args:
        manifest_path: Path to manifest.json
        name_filter: Substring filter on source unique_id (case-insensitive)
        freshness_only: Only return sources that declare freshness checks

    Returns:
        List of {name, source_name, unique_id, schema, identifier,
                 database, freshness, loaded_at_field}.
    """
    parser = _get_cached_parser(manifest_path)
    sources_map = parser.manifest.get('sources', {})

    out = []
    for uid, src in sources_map.items():
        if not uid.startswith('source.'):
            continue
        if name_filter and name_filter.lower() not in uid.lower():
            continue
        freshness = src.get('freshness')
        if freshness_only and not freshness:
            continue
        out.append({
            'name': src.get('name', ''),
            'source_name': src.get('source_name', ''),
            'unique_id': uid,
            'database': src.get('database', ''),
            'schema': src.get('schema', ''),
            'identifier': src.get('identifier', ''),
            'loaded_at_field': src.get('loaded_at_field'),
            'freshness': freshness,
        })
    return sorted(out, key=lambda x: x['unique_id'])


def _get_all_relations_recursive(
    relation_map: dict[str, list[str]],
    node_id: str,
    visited: set | None = None
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


def parents(manifest_path: str, model_name: str, use_dev: bool = False, recursive: bool = False, json_output: bool = False) -> list[dict[str, str]] | None:
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


def children(
    manifest_path: str,
    model_name: str,
    use_dev: bool = False,
    recursive: bool = False,
    json_output: bool = False,
    source_ref: str | None = None,
) -> list[dict[str, str]] | None:
    """Get downstream dependencies (child models).

    Args:
        manifest_path: Path to manifest.json
        model_name: Model name (or source identifier when source_ref is set)
        use_dev: If True, prioritize dev manifest over production
        recursive: If True, get all descendants. If False, only direct children.
        json_output: If True, return ultra-compact format for AI agents
        source_ref: If set, treat input as a source reference
            ('schema.table' or 'source_name.table' or 'table') and return
            downstream models of that source.

    Returns:
        Child dependencies list, or None if model/source not found
    """
    config = Config.from_config_or_env()
    command = ChildrenCommand(
        config,
        manifest_path,
        model_name,
        use_dev,
        json_output,
        recursive=recursive,
        source_ref=source_ref,
    )
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


def docs(manifest_path: str, model_name: str, use_dev: bool = False, json_output: bool = False) -> list[dict[str, str]] | None:
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
    selectors: list[str] | None = None,
    modified: bool = False,
    and_logic: bool = False,
    group: bool = False,
    use_dev: bool = False,
    json_output: bool = False
) -> str | list[dict[str, Any]] | dict[str, list[dict[str, Any]]]:
    """
    Filter and list dbt models (replaces dbt ls)

    Args:
        manifest_path: Path to manifest.json
        selectors: List of selectors (tag:name, config.key:value, path:pattern, package:name)
        modified: Show only modified/new models (git-aware)
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
        - name:substr - models whose name contains substring (case-insensitive)

    Examples:
        meta ls tag:verified tag:active           # OR: at least one tag
        meta ls tag:verified tag:active --and     # AND: both tags required
        meta ls tag:verified tag:active --group   # Grouped output
        meta ls config.materialized:incremental
        meta ls --modified                        # Git-modified only

    For chain-aware refresh planning, use `meta optimize refresh` instead.
    """
    parser = _get_cached_parser(manifest_path)
    models = parser.get_all_models()

    # Extract tag selectors for grouping
    tag_selectors = [s.split(':', 1)[1] for s in (selectors or []) if s.startswith('tag:')]

    # Filter by git status only (modified models)
    if modified:
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

    # Print git warnings for modified mode
    if modified:
        if filtered_models:
            warnings = _generate_git_warnings(filtered_models, use_dev)
            _print_warnings(warnings, json_output=json_output)
        else:
            _print_warnings(
                [{
                    "type": "no_modified_models",
                    "severity": "info",
                    "message": "No modified models found",
                    "detail": "No models changed compared to main/master branch",
                    "suggestion": "All models are in sync with production",
                }],
                json_output=json_output,
            )

    # Standard format output
    if json_output:
        if modified:
            return _format_models_json_compact(filtered_models, parser, use_dev)
        return _format_models_json(filtered_models, parser, use_dev)
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

    elif selector_type == 'name':
        # Substring match on model name (case-insensitive). Useful when you
        # know fragments of a name but not the exact prefix used in --select.
        needle = selector_value.lower()
        return [
            m for m in models
            if needle in m['unique_id'].split('.')[-1].lower()
        ]

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


def _format_models_text(models: list[dict[str, Any]]) -> str:
    """Format as space-separated model names"""
    model_names = [m['unique_id'].split('.')[-1] for m in models]
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
            'name': model_name,
            'unique_id': model['unique_id'],
            'model': model_name,  # legacy alias for backwards compat
            'table': f"{schema_name}.{table_name}" if schema_name else table_name,
            'tags': model.get('tags', []),
            'materialized': model.get('config', {}).get('materialized', 'view'),
            'path': model.get('original_file_path', '')
        }

        # Add git status if present (for --modified/--refresh)
        if '_git_status' in model:
            model_dict['git_status'] = model['_git_status']

        result.append(model_dict)

    return sorted(result, key=lambda x: x['name'])


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
    for _, model in models.items():
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


# =============================================================================
# Optimization Commands
# =============================================================================


def analyze(manifest_path: str, model_name: str, use_dev: bool = False, json_output: bool = False) -> dict[str, Any] | None:
    """
    Analyze model partitioning/clustering effectiveness.

    Combines manifest metadata with BigQuery monitoring data to provide
    deep analysis of optimization opportunities.

    Args:
        manifest_path: Path to manifest.json
        model_name: Model name
        use_dev: Not used (always analyzes production)
        json_output: If True, suppress warnings

    Returns:
        Dictionary with:
        - model: Model name
        - table: Full table name (schema.table)
        - config: Partition/cluster configuration from manifest
        - storage: Storage metrics from dbt_bigquery_monitoring
        - partitions: Partition statistics
        - usage: Query frequency data
        - recommendations: List of optimization recommendations

        Returns None if model not found.
    """
    config = Config.from_config_or_env()
    command = AnalyzeCommand(config, manifest_path, model_name, use_dev, json_output)
    return command.execute()


def hotspots(manifest_path: str, limit: int = 20, min_gb: float = 1.0, json_output: bool = False) -> dict[str, Any]:
    """
    Find models with highest optimization potential.

    Analyzes all tables and scores them based on partitioning,
    clustering, and query patterns to identify optimization candidates.

    Args:
        manifest_path: Path to manifest.json
        limit: Maximum number of hotspots to return (default: 20)
        min_gb: Minimum table size in GB to consider (default: 1.0)
        json_output: If True, suppress warnings

    Returns:
        Dictionary with:
        - hotspots: List of top optimization candidates, each with:
            - model: dbt model name (if found)
            - table: BigQuery table name
            - score: Optimization score (higher = more potential)
            - reasons: List of reasons for the score
            - potential_savings_gb: Estimated storage savings
        - summary: Overall statistics
    """
    config = Config.from_config_or_env()
    command = HotspotsCommand(config, manifest_path, limit, min_gb, json_output)
    return command.execute()


def branch(manifest_path: str, model_name: str, use_dev: bool = False, json_output: bool = False) -> dict[str, Any] | None:
    """
    Analyze optimization across model branch.

    Examines upstream and downstream models to identify alignment issues
    between partitioning/clustering configurations.

    Args:
        manifest_path: Path to manifest.json
        model_name: Model name
        use_dev: Not used (always analyzes production)
        json_output: If True, suppress warnings

    Returns:
        Dictionary with:
        - root: Root model name
        - root_config: Root model partition/cluster config
        - upstream: List of upstream models with impact analysis
        - downstream: List of downstream models with alignment analysis
        - recommendations: Branch-level optimization recommendations

        Returns None if model not found.
    """
    config = Config.from_config_or_env()
    command = BranchCommand(config, manifest_path, model_name, use_dev, json_output)
    return command.execute()


def powerbi(
    manifest_path: str,
    workspace_id: str | None = None,
    json_output: bool = False,
    show_measures: bool = False,
    show_columns: bool = False,
    show_full: bool = False,
    by_table: bool = False,
) -> dict[str, Any]:
    """
    Extract Power BI dashboard to BigQuery table mappings.

    Scans Power BI workspace to extract datasets, reports, and their
    BigQuery table dependencies. Maps tables to dbt model names.
    Optionally includes measures (DAX) and column schemas.

    Args:
        manifest_path: Path to manifest.json
        workspace_id: Power BI workspace ID (or use first from config)
        json_output: If True, suppress warnings
        show_measures: Include measures with DAX expressions
        show_columns: Include column schemas
        show_full: Include all metadata (measures + columns)
        by_table: Group by tables instead of datasets

    Returns:
        Dictionary with:
        - workspace: Workspace name
        - workspace_id: Workspace ID
        - datasets: List of datasets, each with:
            - name: Dataset name
            - id: Dataset ID
            - reports: List of report names using this dataset
            - tables: List of BigQuery tables with dbt mapping and optionally:
                - measures: List of measures with DAX (if show_measures/show_full/json_output)
                - columns: List of columns with types (if show_columns/show_full/json_output)
            - refresh_count_30d: Number of refreshes in last 30 days
            - last_refresh: Timestamp of last refresh
        - summary: Totals for datasets, reports, tables

    Raises:
        ConfigurationError: If Power BI integration not configured
    """
    config = Config.from_config_or_env()
    command = PowerBiCommand(
        config,
        manifest_path,
        workspace_id,
        json_output,
        show_measures,
        show_columns,
        show_full,
        by_table,
    )
    return command.execute()
