"""Lineage utilities for parents/children commands.

Helper functions for building and processing hierarchical dependency trees.
"""

from typing import Any, Optional


def count_tree_nodes(tree: list[dict[str, Any]]) -> int:
    """Count total nodes in hierarchical tree.

    Args:
        tree: Hierarchical tree structure

    Returns:
        Total count of nodes including all nested children
    """
    count = len(tree)
    for node in tree:  # pragma: no cover
        if node.get('children'):
            count += count_tree_nodes(node['children'])
    return count


def flatten_tree_to_compact(tree: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Flatten nested tree to compact flat format.

    Args:
        tree: Nested tree [{path, table, level, children}, ...]

    Returns:
        Flat array [{"path": "...", "table": "...", "level": 0}, ...]
    """
    result = []
    for node in tree:  # pragma: no cover
        # Add current node without children
        result.append({
            'path': node['path'],
            'table': node['table'],
            'type': node.get('type', ''),
            'level': node['level']
        })
        # Recursively add children
        if node.get('children'):
            result.extend(flatten_tree_to_compact(node['children']))
    return result


def build_relation_tree(
    relation_map: dict[str, list[str]],
    node_id: str,
    nodes: dict[str, Any],
    sources: dict[str, Any],
    visited: Optional[set[str]] = None,
    level: int = 0,
    json_mode: bool = False
) -> list[dict[str, Any]]:
    """Build hierarchical tree of relations (parents or children).

    Args:
        relation_map: manifest['parent_map'] or manifest['child_map']
        node_id: Starting node unique_id
        nodes: manifest['nodes']
        sources: manifest['sources']
        visited: Set of already visited nodes (to avoid cycles)
        level: Current depth level
        json_mode: If True, return compact JSON structure for AI agents

    Returns:
        List of dicts with 'node' info and 'children' list

        If json_mode=False (for display):
        [{
            'name': '...',
            'type': '...',
            'level': 0,
            'children': [...]
        }]

        If json_mode=True (for AI agents):
        [{
            'path': 'models/core/client.sql',  # relative path to .sql file
            'table': 'core_client.client',     # schema.table for BigQuery
            'level': 0,
            'children': [...]
        }]
    """
    if visited is None:
        visited = set()

    if node_id in visited:  # pragma: no cover
        return []

    visited.add(node_id)
    relations = relation_map.get(node_id, [])

    result = []
    for relation_id in relations:
        # Get node details
        node = nodes.get(relation_id) or sources.get(relation_id)
        if not node:  # pragma: no cover
            continue

        # Filter out tests
        if node.get('resource_type') == 'test':  # pragma: no cover
            continue

        # Build node info based on mode
        if json_mode:  # pragma: no cover
            # Compact JSON for AI agents (nested structure)
            schema = node.get('schema', '')
            alias = node.get('alias') or node.get('name', '')
            table = f"{schema}.{alias}" if schema else alias

            # Get relative path - remove "models/" prefix to save space
            path = node.get('original_file_path', '')
            if path.startswith('models/'):
                path = path[7:]  # Remove "models/" prefix

            node_info = {
                'path': path,
                'table': table,
                'type': node.get('resource_type', ''),
                'level': level,
                'children': build_relation_tree(relation_map, relation_id, nodes, sources, visited, level + 1, json_mode=True)
            }
        else:
            # Full info for display
            node_info = {
                'unique_id': relation_id,
                'name': node.get('name', ''),
                'type': node.get('resource_type', ''),
                'database': node.get('database', ''),
                'schema': node.get('schema', ''),
                'level': level,
                'children': build_relation_tree(relation_map, relation_id, nodes, sources, visited, level + 1, json_mode=False)
            }

        result.append(node_info)

    return result
