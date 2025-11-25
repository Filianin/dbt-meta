"""Parents command - Get upstream dependencies (parent models)."""

import sys
from typing import Any, Optional

from dbt_meta.command_impl.base import BaseCommand
from dbt_meta.command_impl.lineage_utils import (
    build_relation_tree,
    count_tree_nodes,
    flatten_tree_to_compact,
)
from dbt_meta.fallback import FallbackLevel


class ParentsCommand(BaseCommand):
    """Get upstream dependencies (parent models).

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

        Returns None if model not found.
        Filters out tests (resource_type != "test").

    Behavior with use_dev=True:
        - Searches dev manifest (target/) FIRST
        - Returns dev-specific parent dependencies
        - NO BigQuery fallback (lineage is manifest-only)
    """

    SUPPORTS_BIGQUERY = False  # Lineage is manifest-only
    SUPPORTS_DEV = True

    def __init__(self, *args, recursive: bool = False, **kwargs):
        """Initialize parents command.

        Args:
            recursive: If True, get all ancestors. If False, only direct parents.
        """
        super().__init__(*args, **kwargs)
        self.recursive = recursive

    def execute(self) -> Optional[list[dict[str, Any]]]:
        """Execute parents command.

        Returns:
            Parent dependencies list, or None if model not found
        """
        from dbt_meta.utils import get_cached_parser as _get_cached_parser

        model = self.get_model_with_fallback()
        if not model:
            # Print helpful error message
            print(f"❌ Parent dependencies not available for '{self.model_name}': model not in manifest",
                  file=sys.stderr)
            print("   Lineage information is stored only in manifest.json",
                  file=sys.stderr)
            return None

        # Get manifest data for lineage processing
        parser = _get_cached_parser(self.manifest_path)
        parent_map = parser.manifest.get('parent_map', {})
        nodes = parser.manifest.get('nodes', {})
        sources = parser.manifest.get('sources', {})

        return self.process_model(model, parent_map=parent_map, nodes=nodes, sources=sources)

    def process_model(
        self,
        model: dict,
        level: Optional[FallbackLevel] = None,
        parent_map: Optional[dict] = None,
        nodes: Optional[dict] = None,
        sources: Optional[dict] = None
    ) -> Optional[list[dict[str, Any]]]:
        """Process model data and return parent dependencies.

        Args:
            model: Model data from manifest
            level: Fallback level (not used for parents command)
            parent_map: manifest['parent_map']
            nodes: manifest['nodes']
            sources: manifest['sources']

        Returns:
            Parent dependencies list
        """
        unique_id = model['unique_id']

        if self.recursive:
            # Build hierarchical tree
            tree = build_relation_tree(parent_map, unique_id, nodes, sources, json_mode=self.json_output)
            # If JSON mode and > 20 nodes, use ultra-compact format
            if self.json_output and count_tree_nodes(tree) > 20:
                return flatten_tree_to_compact(tree)
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
            if self.json_output and len(parents_details) > 20:
                return [{'path': item['path'], 'table': item['table'], 'type': item['type'], 'level': 0} for item in parents_details]

            return parents_details
