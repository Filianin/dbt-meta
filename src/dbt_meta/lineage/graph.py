"""LineageGraph — rustworkx-backed column-level dependency graph.

Uses rustworkx.PyDiGraph for native-code BFS / ancestors / descendants and an
external dict[str, int] to map composite "model.column" IDs to integer indices.

Edge direction: upstream → downstream (raw_clients.id → core_clients.client_id).
"""

from __future__ import annotations

from typing import Any

import rustworkx as rx

NODE_SEP = "."


def make_node_id(model: str, column: str) -> str:
    """Build composite node id 'model.column'.

    Examples:
        >>> make_node_id('core_clients', 'client_id')
        'core_clients.client_id'
    """
    return f"{model}{NODE_SEP}{column}"


def split_node_id(node_id: str) -> tuple[str, str]:
    """Split composite id back into (model, column).

    Splits on the LAST separator so column names never collide with model
    names that contain dots (BigQuery schema-qualified names).
    """
    model, _, column = node_id.rpartition(NODE_SEP)
    if not model:
        raise ValueError(f"Invalid node id (missing separator): {node_id!r}")
    return model, column


class LineageGraph:
    """Directed graph of column-level lineage.

    Wraps rustworkx.PyDiGraph and maintains a name↔index map so callers
    can query by string ids instead of opaque integer indices.

    Examples:
        >>> g = LineageGraph()
        >>> g.add_node('raw_clients.id', {'data_type': 'INT64'})
        >>> g.add_node('core_clients.client_id', {'data_type': 'INT64'})
        >>> g.add_edge('raw_clients.id', 'core_clients.client_id', {'transform': 'renamed'})
        >>> g.ancestors('core_clients.client_id')
        ['raw_clients.id']
        >>> g.descendants('raw_clients.id')
        ['core_clients.client_id']
    """

    def __init__(self) -> None:
        self._graph: rx.PyDiGraph = rx.PyDiGraph(check_cycle=False, multigraph=False)
        self._id_to_idx: dict[str, int] = {}

    # ----- mutation -----

    def add_node(self, node_id: str, attrs: dict[str, Any] | None = None) -> int:
        """Add a node, return its index. Idempotent — returns existing index if present."""
        existing = self._id_to_idx.get(node_id)
        if existing is not None:
            if attrs:
                existing_payload: dict[str, Any] = self._graph[existing]
                existing_payload.update(attrs)
            return existing

        payload: dict[str, Any] = {"id": node_id}
        if attrs:
            payload.update(attrs)
        idx = self._graph.add_node(payload)
        self._id_to_idx[node_id] = idx
        return idx

    def add_edge(
        self,
        src: str,
        dst: str,
        attrs: dict[str, Any] | None = None,
    ) -> None:
        """Add a directed edge upstream→downstream. Auto-creates missing nodes."""
        src_idx = self.add_node(src)
        dst_idx = self.add_node(dst)
        # rustworkx allows duplicate edges; dedupe to keep graph small
        if self._graph.has_edge(src_idx, dst_idx):
            return
        self._graph.add_edge(src_idx, dst_idx, attrs or {})

    # ----- queries -----

    def has_node(self, node_id: str) -> bool:
        return node_id in self._id_to_idx

    def get_node(self, node_id: str) -> dict[str, Any] | None:
        idx = self._id_to_idx.get(node_id)
        if idx is None:
            return None
        # PyDiGraph stores payloads keyed by index
        return dict(self._graph[idx])

    def ancestors(self, node_id: str) -> list[str]:
        """All upstream node ids (transitive). Order is unspecified."""
        idx = self._id_to_idx.get(node_id)
        if idx is None:
            return []
        idx_to_id = self._idx_to_id()
        return [idx_to_id[i] for i in rx.ancestors(self._graph, idx)]

    def descendants(self, node_id: str) -> list[str]:
        """All downstream node ids (transitive). Order is unspecified."""
        idx = self._id_to_idx.get(node_id)
        if idx is None:
            return []
        idx_to_id = self._idx_to_id()
        return [idx_to_id[i] for i in rx.descendants(self._graph, idx)]

    def parents(self, node_id: str) -> list[str]:
        """Direct upstream nodes (predecessors)."""
        idx = self._id_to_idx.get(node_id)
        if idx is None:
            return []
        idx_to_id = self._idx_to_id()
        return [idx_to_id[i] for i in self._graph.predecessor_indices(idx)]

    def children(self, node_id: str) -> list[str]:
        """Direct downstream nodes (successors)."""
        idx = self._id_to_idx.get(node_id)
        if idx is None:
            return []
        idx_to_id = self._idx_to_id()
        return [idx_to_id[i] for i in self._graph.successor_indices(idx)]

    # ----- stats -----

    @property
    def node_count(self) -> int:
        return self._graph.num_nodes()

    @property
    def edge_count(self) -> int:
        return self._graph.num_edges()

    def all_nodes(self) -> list[str]:
        return list(self._id_to_idx.keys())

    # ----- (de)serialization helpers -----

    def to_dict(self) -> dict[str, Any]:
        """Serialize graph to JSON-friendly dict.

        Schema:
            {
                "nodes": [{"id": "...", ...attrs}, ...],
                "edges": [{"src": "...", "dst": "...", ...attrs}, ...]
            }
        """
        idx_to_id = self._idx_to_id()
        nodes: list[dict[str, Any]] = []
        for idx in self._graph.node_indexes():
            payload = dict(self._graph[idx])
            payload.setdefault("id", idx_to_id[idx])
            nodes.append(payload)

        edges: list[dict[str, Any]] = []
        for src_idx, dst_idx, attrs in self._graph.weighted_edge_list():
            edge: dict[str, Any] = {
                "src": idx_to_id[src_idx],
                "dst": idx_to_id[dst_idx],
            }
            if isinstance(attrs, dict):
                edge.update(attrs)
            edges.append(edge)

        return {"nodes": nodes, "edges": edges}

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> LineageGraph:
        """Build graph from to_dict()-shaped payload."""
        graph = cls()
        for node in data.get("nodes", []):
            node_id = node["id"]
            attrs = {k: v for k, v in node.items() if k != "id"}
            graph.add_node(node_id, attrs)
        for edge in data.get("edges", []):
            src = edge["src"]
            dst = edge["dst"]
            attrs = {k: v for k, v in edge.items() if k not in ("src", "dst")}
            graph.add_edge(src, dst, attrs)
        return graph

    # ----- internals -----

    def _idx_to_id(self) -> dict[int, str]:
        return {idx: node_id for node_id, idx in self._id_to_idx.items()}
