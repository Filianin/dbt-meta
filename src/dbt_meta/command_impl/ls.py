"""List and filter commands for dbt-meta."""

from __future__ import annotations

import subprocess
from itertools import combinations
from typing import Any

from dbt_meta.utils import get_cached_parser as _get_cached_parser
from dbt_meta.utils import print_warnings as _print_warnings


class LsCommand:
    """Filter and list dbt models (replaces dbt ls).

    Selectors:
        tag:name               — filter by tag (OR logic by default)
        config.key:value       — filter by config value
        path:dir/              — filter by file path prefix
        package:name           — filter by package

    Returns:
        Default text mode: space-separated model names
        --group text mode: grouped with headers
        Default JSON mode: list of metadata dicts
        --group JSON mode: dict of groups
    """

    def __init__(
        self,
        manifest_path: str,
        selectors: list[str] | None = None,
        modified: bool = False,
        and_logic: bool = False,
        group: bool = False,
        use_dev: bool = False,
        json_output: bool = False,
    ):
        self.manifest_path = manifest_path
        self.selectors = selectors
        self.modified = modified
        self.and_logic = and_logic
        self.group = group
        self.use_dev = use_dev
        self.json_output = json_output

    def execute(self) -> str | list[dict[str, Any]] | dict[str, list[dict[str, Any]]]:
        parser = _get_cached_parser(self.manifest_path)
        models = parser.get_all_models()

        tag_selectors = [s.split(':', 1)[1] for s in (self.selectors or []) if s.startswith('tag:')]

        if self.modified:
            filtered_models = _filter_modified_models(models, parser)
        elif self.selectors:
            if self.and_logic and tag_selectors:
                filtered_models = _filter_by_selectors_and(models, self.selectors, parser)
            else:
                filtered_models = _filter_by_selectors_or(models, self.selectors, parser)
        else:
            filtered_models = list(models.values())

        if self.group and tag_selectors:
            return _format_models_grouped(filtered_models, tag_selectors, parser, self.use_dev, self.json_output)

        if self.modified:
            if filtered_models:
                warnings = _generate_git_warnings(filtered_models, self.use_dev)
                _print_warnings(warnings, json_output=self.json_output)
            else:
                _print_warnings(
                    [{
                        "type": "no_modified_models",
                        "severity": "info",
                        "message": "No modified models found",
                        "detail": "No models changed compared to main/master branch",
                        "suggestion": "All models are in sync with production",
                    }],
                    json_output=self.json_output,
                )

        if self.json_output:
            if self.modified:
                return _format_models_json_compact(filtered_models, parser, self.use_dev)
            return _format_models_json(filtered_models, parser, self.use_dev)
        return _format_models_text(filtered_models)


class ListModelsCommand:
    """List all models, optionally filtered by substring pattern.

    Args:
        manifest_path: Path to manifest.json
        pattern: Optional filter pattern (case-insensitive substring match)

    Returns:
        Sorted list of model names
    """

    def __init__(self, manifest_path: str, pattern: str | None = None):
        self.manifest_path = manifest_path
        self.pattern = pattern

    def execute(self) -> list[str]:
        parser = _get_cached_parser(self.manifest_path)
        models = parser.get_all_models()

        if self.pattern:
            pattern_lower = self.pattern.lower()
            model_names = [
                uid.split('.')[-1]
                for uid in models
                if pattern_lower in uid.split('.')[-1].lower()
            ]
        else:
            model_names = [uid.split('.')[-1] for uid in models]

        return sorted(model_names)


# ---------------------------------------------------------------------------
# Private helpers
# ---------------------------------------------------------------------------

def _filter_by_selectors_or(models: dict[str, Any], selectors: list[str], parser: Any) -> list[dict[str, Any]]:
    """Filter with OR logic for tags, AND for other selectors."""
    tag_selectors = [s for s in selectors if s.startswith('tag:')]
    other_selectors = [s for s in selectors if not s.startswith('tag:')]

    filtered = list(models.values())

    for selector in other_selectors:
        filtered = _apply_selector(filtered, selector)

    if tag_selectors:
        tags = [s.split(':', 1)[1] for s in tag_selectors]
        filtered = [m for m in filtered if any(tag in m.get('tags', []) for tag in tags)]

    return filtered


def _filter_by_selectors_and(models: dict[str, Any], selectors: list[str], parser: Any) -> list[dict[str, Any]]:
    """Filter with AND logic for all selectors."""
    filtered = list(models.values())
    for selector in selectors:
        filtered = _apply_selector(filtered, selector)
    return filtered


def _apply_selector(models: list[dict[str, Any]], selector: str) -> list[dict[str, Any]]:
    """Apply a single selector filter."""
    if ':' not in selector:
        return models

    if selector.startswith('config.'):
        if ':' not in selector[7:]:
            return models
        config_part, config_val = selector.rsplit(':', 1)
        config_path = config_part[7:].split('.')
        return [m for m in models if _nested_get(m.get('config', {}), config_path) == config_val]

    selector_type, selector_value = selector.split(':', 1)

    if selector_type == 'tag':
        return [m for m in models if selector_value in m.get('tags', [])]
    if selector_type == 'path':
        return [m for m in models if m.get('original_file_path', '').startswith(selector_value)]
    if selector_type == 'package':
        return [m for m in models if m.get('package_name') == selector_value]

    return models


def _nested_get(data: Any, path: list[str]) -> Any:
    """Walk a dotted config path (e.g. ['meta', 'domain']) into nested dicts.

    Returns the value coerced to str for comparison with the string selector
    value, or None if any segment is missing or non-dict.
    """
    current: Any = data
    for key in path:
        if not isinstance(current, dict):
            return None
        current = current.get(key)
    return str(current) if current is not None else None


def _generate_git_warnings(models: list[dict[str, Any]], use_dev: bool) -> list[dict[str, str]]:
    """Generate warnings for models with git status metadata."""
    uncommitted_models = [m for m in models if m.get('_git_status') == 'uncommitted']
    committed_models = [m for m in models if m.get('_git_status') == 'committed']

    if not uncommitted_models and not committed_models:
        return []

    parts = []
    if uncommitted_models:
        parts.append(f"{len(uncommitted_models)} uncommitted")
    if committed_models:
        parts.append(f"{len(committed_models)} committed")

    return [{
        "type": "git_branch_changes",
        "severity": "info",
        "message": f"Found {' and '.join(parts)} model(s) in current branch",
        "suggestion": "Using dev tables for branch changes" if use_dev else "Use --dev flag to query dev tables if needed",
    }]


def _format_models_text(models: list[dict[str, Any]]) -> str:
    """Format as space-separated sorted model names."""
    return ' '.join(sorted(m['unique_id'].split('.')[-1] for m in models))


def _format_models_json_compact(models: list[dict[str, Any]], parser: Any, use_dev: bool) -> dict[str, list[str]]:
    """Format as compact dict with models and tables arrays."""
    model_names = []
    table_names = []
    for model in models:
        model_name = model['unique_id'].split('.')[-1]
        model_names.append(model_name)
        schema_name = model.get('schema', '')
        table_name = model.get('alias') or model.get('name', model_name)
        table_names.append(f"{schema_name}.{table_name}" if schema_name else table_name)
    return {'models': sorted(model_names), 'tables': sorted(table_names)}


def _format_models_json(models: list[dict[str, Any]], parser: Any, use_dev: bool) -> list[dict[str, Any]]:
    """Format as list of metadata dicts."""
    result = []
    for model in models:
        model_name = model['unique_id'].split('.')[-1]
        schema_name = model.get('schema', '')
        table_name = model.get('alias') or model.get('name', model_name)
        model_dict: dict[str, Any] = {
            'model': model_name,
            'table': f"{schema_name}.{table_name}" if schema_name else table_name,
            'tags': model.get('tags', []),
            'materialized': model.get('config', {}).get('materialized', 'view'),
            'path': model.get('original_file_path', ''),
        }
        meta = model.get('config', {}).get('meta') or model.get('meta') or {}
        if meta:
            model_dict['meta'] = meta
        if '_git_status' in model:
            model_dict['git_status'] = model['_git_status']
        result.append(model_dict)
    return sorted(result, key=lambda x: x['model'])


def _filter_modified_models(models: dict[str, Any], parser: Any) -> list[dict[str, Any]]:
    """Filter models modified vs main branch (committed + uncommitted).

    Each returned model has an added '_git_status' key:
      'uncommitted' — local changes (unstaged/staged/new)
      'committed'   — committed in current branch but not in main
    """
    try:
        main_diff_result = None
        for base_branch in ['origin/main', 'origin/master', 'main', 'master']:
            result = subprocess.run(
                ['git', 'diff', f'{base_branch}...HEAD', '--name-only'],
                capture_output=True, text=True, timeout=5,
            )
            if result.returncode == 0:
                main_diff_result = result
                break

        if main_diff_result is None or main_diff_result.returncode != 0:
            return []

        unstaged_result = subprocess.run(
            ['git', 'diff', 'HEAD', '--name-only'], capture_output=True, text=True, timeout=5,
        )
        staged_result = subprocess.run(
            ['git', 'diff', '--cached', '--name-only'], capture_output=True, text=True, timeout=5,
        )
        status_result = subprocess.run(
            ['git', 'status', '--porcelain'], capture_output=True, text=True, timeout=5,
        )

        main_diff_files = set(main_diff_result.stdout.splitlines()) if main_diff_result.returncode == 0 else set()
        unstaged_files = set(unstaged_result.stdout.splitlines()) if unstaged_result.returncode == 0 else set()
        staged_files = set(staged_result.stdout.splitlines()) if staged_result.returncode == 0 else set()
        new_files = set(
            line[3:].strip() for line in status_result.stdout.splitlines() if line.startswith('??')
        ) if status_result.returncode == 0 else set()
        uncommitted_files = unstaged_files | staged_files | new_files

    except (subprocess.TimeoutExpired, FileNotFoundError, subprocess.CalledProcessError, OSError):
        return []

    modified = []
    for _, model in models.items():
        file_path = model.get('original_file_path', '')
        in_branch_diff = any(file_path in f or f.endswith(file_path) for f in main_diff_files)
        has_uncommitted = any(file_path in f or f.endswith(file_path) for f in uncommitted_files)

        if in_branch_diff or has_uncommitted:
            model_copy = model.copy()
            model_copy['_git_status'] = 'uncommitted' if has_uncommitted else 'committed'
            modified.append(model_copy)

    return modified


def _format_models_grouped(
    models: list[dict[str, Any]],
    tags: list[str],
    parser: Any,
    use_dev: bool,
    json_output: bool,
) -> str | dict[str, list[dict[str, Any]]]:
    """Group models by tag combinations."""
    groups: dict[str, list[dict[str, Any]]] = {}

    for tag in tags:
        groups[f"tag:{tag}"] = []
    for r in range(2, len(tags) + 1):
        for combo in combinations(tags, r):
            groups[" ".join(f"tag:{t}" for t in combo)] = []

    for model in models:
        model_tags = set(model.get('tags', []))
        matched_tags = [t for t in tags if t in model_tags]
        if not matched_tags:
            continue
        if len(matched_tags) == 1:
            group_key = f"tag:{matched_tags[0]}"
        else:
            group_key = " ".join(f"tag:{t}" for t in sorted(matched_tags))
        if group_key in groups:
            groups[group_key].append(model)

    if json_output:
        return {k: _format_models_json(v, parser, use_dev) for k, v in groups.items() if v}

    output_lines = []
    for group_key, group_models in groups.items():
        if group_models:
            model_names = ' '.join(sorted(m['unique_id'].split('.')[-1] for m in group_models))
            output_lines.append(f"{group_key}:")
            output_lines.append(model_names)
            output_lines.append("")
    return '\n'.join(output_lines).rstrip()
