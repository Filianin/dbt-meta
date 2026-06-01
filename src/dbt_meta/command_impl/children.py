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

    def __init__(self, *args, recursive: bool = False, source_ref: Optional[str] = None, **kwargs):
        """Initialize children command.

        Args:
            recursive: If True, get all descendants. If False, only direct children.
            source_ref: If set, treat the input as a source reference
                ('schema.table' or 'table') and list downstream models of
                that source instead of a dbt model.
        """
        super().__init__(*args, **kwargs)
        self.recursive = recursive
        self.source_ref = source_ref

    def execute(self) -> Optional[list[dict[str, Any]]]:
        """Execute children command.

        Returns:
            Child dependencies list, or None if model not found
        """
        from dbt_meta.utils import get_cached_parser as _get_cached_parser

        parser = _get_cached_parser(self.manifest_path)
        child_map = parser.manifest.get('child_map', {})
        nodes = parser.manifest.get('nodes', {})
        sources = parser.manifest.get('sources', {})

        # --source branch: resolve source by 'schema.table' or 'table'
        if self.source_ref:
            source_uid = self._resolve_source_uid(self.source_ref, sources)
            if not source_uid:
                print(
                    f"❌ Source '{self.source_ref}' not found in manifest",
                    file=sys.stderr,
                )
                print(
                    "   Use 'meta sources' to list available sources.",
                    file=sys.stderr,
                )
                return None
            pseudo_model = {'unique_id': source_uid}
            return self.process_model(
                pseudo_model, child_map=child_map, nodes=nodes, sources=sources
            )

        model = self.get_model_with_fallback()
        if not model:
            # Print helpful error message
            print(f"❌ Child dependencies not available for '{self.model_name}': model not in manifest",
                  file=sys.stderr)
            print("   Lineage information is stored only in manifest.json",
                  file=sys.stderr)
            return None

        return self.process_model(model, child_map=child_map, nodes=nodes, sources=sources)

    @staticmethod
    def _resolve_source_uid(ref: str, sources: dict) -> Optional[str]:
        """Resolve 'schema.table', 'source_name.table', or 'table' to a source unique_id."""
        parts = ref.split('.')
        if len(parts) == 1:
            want_qualifier, want_table = None, parts[0]
        elif len(parts) == 2:
            want_qualifier, want_table = parts[0], parts[1]
        else:
            return None

        for uid, src in sources.items():
            if not uid.startswith('source.'):
                continue
            identifier = src.get('identifier') or src.get('name', '')
            if identifier != want_table:
                continue
            if want_qualifier is None:
                return uid
            # Accept match on either physical schema or logical source_name.
            if src.get('schema') == want_qualifier:
                return uid
            if src.get('source_name') == want_qualifier:
                return uid
        return None

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

        # Guard against missing manifest data (defensive defaults)
        child_map = child_map or {}
        nodes = nodes or {}
        sources = sources or {}

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
                    'name': child_node.get('name', ''),
                    'unique_id': child_id,
                    'path': path,
                    'table': table,
                    'type': child_node.get('resource_type', '')
                })

            # If JSON mode and > 20 nodes, add level field
            if self.json_output and len(children_details) > 20:
                return [{**item, 'level': 0} for item in children_details]

            return children_details
