"""Children command - Get downstream dependencies (child models)."""

import sys
from typing import Any, Optional

from dbt_meta.command_impl.base import BaseCommand
from dbt_meta.command_impl.lineage_utils import (
    build_relation_tree,
    count_tree_nodes,
    flatten_tree_to_compact,
)
from dbt_meta.fallback import FallbackLevel


class ChildrenCommand(BaseCommand):
    """Get downstream dependencies (child models).

    Returns:
        If recursive=False (direct children):
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
        - Returns dev-specific child dependencies
        - NO BigQuery fallback (lineage is manifest-only)
    """

    SUPPORTS_BIGQUERY = False  # Lineage is manifest-only
    SUPPORTS_DEV = True

    def __init__(self, *args, recursive: bool = False, **kwargs):
        """Initialize children command.

        Args:
            recursive: If True, get all descendants. If False, only direct children.
        """
        super().__init__(*args, **kwargs)
        self.recursive = recursive

    def execute(self) -> Optional[list[dict[str, Any]]]:
        """Execute children command.

        Returns:
            Child dependencies list, or None if model not found
        """
        from dbt_meta.utils import get_cached_parser as _get_cached_parser

        model = self.get_model_with_fallback()
        if not model:
            # Print helpful error message
            print(f"❌ Child dependencies not available for '{self.model_name}': model not in manifest",
                  file=sys.stderr)
            print("   Lineage information is stored only in manifest.json",
                  file=sys.stderr)
            return None

        # Get manifest data for lineage processing
        parser = _get_cached_parser(self.manifest_path)
        child_map = parser.manifest.get('child_map', {})
        nodes = parser.manifest.get('nodes', {})
        sources = parser.manifest.get('sources', {})

        return self.process_model(model, child_map=child_map, nodes=nodes, sources=sources)

    def process_model(
        self,
        model: dict,
        level: Optional[FallbackLevel] = None,
        child_map: Optional[dict] = None,
        nodes: Optional[dict] = None,
        sources: Optional[dict] = None
    ) -> Optional[list[dict[str, Any]]]:
        """Process model data and return child dependencies.

        Args:
            model: Model data from manifest
            level: Fallback level (not used for children command)
            child_map: manifest['child_map']
            nodes: manifest['nodes']
            sources: manifest['sources']

        Returns:
            Child dependencies list
        """
        unique_id = model['unique_id']

        if self.recursive:
            # Build hierarchical tree
            tree = build_relation_tree(child_map, unique_id, nodes, sources, json_mode=self.json_output)
            # If JSON mode and > 20 nodes, use ultra-compact format
            if self.json_output and count_tree_nodes(tree) > 20:
                return flatten_tree_to_compact(tree)
            return tree
        else:
            # Return flat list of direct children
            child_ids = child_map.get(unique_id, [])
            children_details = []

            for child_id in child_ids:
                # Get from nodes or sources
                child_node = nodes.get(child_id) or sources.get(child_id)

                if not child_node:
                    continue

                # Filter out tests
                if child_node.get('resource_type') == 'test':
                    continue

                # Use compact format {path, table, type}
                schema = child_node.get('schema', '')
                alias = child_node.get('alias') or child_node.get('name', '')
                table = f"{schema}.{alias}" if schema else alias
                path = child_node.get('original_file_path', '')
                if path.startswith('models/'):
                    path = path[7:]

                children_details.append({
                    'path': path,
                    'table': table,
                    'type': child_node.get('resource_type', '')
                })

            # If JSON mode and > 20 nodes, add level field
            if self.json_output and len(children_details) > 20:
                return [{'path': item['path'], 'table': item['table'], 'type': item['type'], 'level': 0} for item in children_details]

            return children_details
