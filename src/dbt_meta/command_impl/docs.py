"""Docs command for dbt-meta."""

from __future__ import annotations

from typing import Any

from dbt_meta.utils import get_cached_parser as _get_cached_parser
from dbt_meta.utils import print_warnings as _print_warnings
from dbt_meta.utils.dev import find_dev_manifest as _find_dev_manifest
from dbt_meta.utils.git import check_manifest_git_mismatch as _check_manifest_git_mismatch


class DocsCommand:
    """Get columns with names, types, and descriptions.

    Returns list of dicts with:
      - name: column name
      - data_type: column data type
      - description: column description (from schema.yml)

    Returns None if model not found.

    Behavior with use_dev=True:
      - Searches dev manifest (target/) FIRST
      - Returns dev-specific column descriptions
      - No BigQuery fallback (descriptions are manifest-only)
    """

    def __init__(
        self,
        manifest_path: str,
        model_name: str,
        use_dev: bool = False,
        json_output: bool = False,
    ):
        self.manifest_path = manifest_path
        self.model_name = model_name
        self.use_dev = use_dev
        self.json_output = json_output

    def execute(self) -> list[dict[str, str]] | None:
        dev_manifest = _find_dev_manifest(self.manifest_path) if self.use_dev else None
        warnings = _check_manifest_git_mismatch(self.model_name, self.use_dev, dev_manifest)
        _print_warnings(warnings, self.json_output)

        if self.use_dev:  # pragma: no cover
            if not dev_manifest:
                dev_manifest = _find_dev_manifest(self.manifest_path)
            if dev_manifest:
                try:
                    parser_dev = _get_cached_parser(dev_manifest)
                    model = parser_dev.get_model(self.model_name)
                    if model:
                        return self._extract_columns(model)
                except (FileNotFoundError, OSError, KeyError):  # pragma: no cover
                    pass
            return None

        parser = _get_cached_parser(self.manifest_path)
        model = parser.get_model(self.model_name)
        if not model:
            return None
        return self._extract_columns(model)

    def _extract_columns(self, model: dict[str, Any]) -> list[dict[str, str]]:
        return [
            {
                'name': col_name,
                'data_type': col_data.get('data_type', 'unknown'),
                'description': col_data.get('description', ''),
            }
            for col_name, col_data in model.get('columns', {}).items()
        ]
