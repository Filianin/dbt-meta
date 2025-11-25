"""Utility functions for dbt-meta commands.

This module contains shared utilities used across multiple commands:
- Parser caching (_get_cached_parser)
- Warning formatting and printing (_print_warnings)
"""

import json as json_lib
import sys
from functools import lru_cache

from dbt_meta.manifest.parser import ManifestParser

__all__ = ['get_cached_parser', 'print_warnings']


@lru_cache(maxsize=2)
def get_cached_parser(manifest_path: str) -> ManifestParser:
    """Get cached ManifestParser instance.

    Uses LRU cache to avoid re-parsing the same manifest.
    Cache size = 2 to cache both production (.dbt-state) and dev (target/) manifests.

    Args:
        manifest_path: Path to manifest.json

    Returns:
        Cached ManifestParser instance

    Example:
        >>> parser = get_cached_parser('/path/to/manifest.json')
        >>> model = parser.get_model('core__clients')
    """
    return ManifestParser(manifest_path)


def print_warnings(warnings: list[dict[str, str]], json_output: bool = False) -> None:
    """Print warnings to stderr in JSON or text format.

    Args:
        warnings: List of warning dictionaries with keys:
            - type: Warning type (e.g., 'git_mismatch')
            - severity: 'info', 'warning', or 'error'
            - message: Main warning message
            - detail: Optional additional details
            - suggestion: Optional suggestion for fixing
        json_output: If True, print as JSON. If False, print as colored text.

    Example:
        >>> warnings = [{
        ...     'type': 'git_mismatch',
        ...     'severity': 'warning',
        ...     'message': 'Model modified but using production manifest',
        ...     'detail': 'File: models/core/clients.sql',
        ...     'suggestion': 'Use --dev flag'
        ... }]
        >>> print_warnings(warnings, json_output=False)
        ⚠️  WARNING: Model modified but using production manifest
           File: models/core/clients.sql
           Suggestion: Use --dev flag
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
