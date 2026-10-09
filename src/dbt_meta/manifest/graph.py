"""ManifestGraph — the one dependency graph every command walks.

Edges come from ``depends_on.nodes`` only. On real manifests this equals the
inverted ``child_map`` for every model, seed and snapshot, and unlike
``child_map`` it is present on every node of a hand-built test manifest.

``prefixes`` decides which node types are part of the graph; an edge to a
node outside it (source, test, macro, or a type filtered out) is dropped, so
a model-only graph never walks through a seed or snapshot. Closures include
the start node, are iterative (no recursion limit on deep chains), cycle-safe
and memoized per instance; they are returned as ``frozenset`` so a caller
cannot corrupt the cache.

Not built on this graph, on purpose: ``lineage_utils.build_relation_tree``
(display tree that includes sources, uses dbt's own maps) and
``usage._common.direct_downstream`` (one hop over ``child_map``).
"""

from __future__ import annotations

from collections import deque
from typing import Any

DEFAULT_PREFIXES = ("model.", "seed.", "snapshot.")
BUILDABLE_PREFIXES = ("model.", "seed.")


class ManifestGraph:
    """Dependency subgraph of a manifest restricted to ``prefixes``."""

    def __init__(self, manifest: dict[str, Any], prefixes: tuple[str, ...] = DEFAULT_PREFIXES):
        self.prefixes = prefixes
        self.nodes: dict[str, dict[str, Any]] = {
            uid: node for uid, node in manifest.get("nodes", {}).items() if uid.startswith(prefixes)
        }
        self.parents: dict[str, list[str]] = {}
        self.children: dict[str, list[str]] = {uid: [] for uid in self.nodes}
        for uid, node in self.nodes.items():
            deps = [d for d in (node.get("depends_on") or {}).get("nodes") or [] if d in self.nodes]
            self.parents[uid] = deps
            for d in deps:
                self.children[d].append(uid)
        self._by_name: dict[str, list[str]] = {}
        for uid in self.nodes:
            self._by_name.setdefault(uid.split(".")[-1], []).append(uid)
        self._ancestors: dict[str, frozenset[str]] = {}
        self._descendants: dict[str, frozenset[str]] = {}

    def resolve(self, name: str, prefixes: tuple[str, ...] = BUILDABLE_PREFIXES) -> str | None:
        """First node with short ``name`` whose type is in ``prefixes``."""
        for uid in self._by_name.get(name, []):
            if uid.startswith(prefixes):
                return uid
        return None

    @staticmethod
    def _closure(start: str, edges: dict[str, list[str]]) -> frozenset[str]:
        seen = {start}
        stack = [start]
        while stack:
            for nxt in edges.get(stack.pop(), []):
                if nxt not in seen:
                    seen.add(nxt)
                    stack.append(nxt)
        return frozenset(seen)

    def ancestors(self, uid: str) -> frozenset[str]:
        """Ancestors including ``uid`` itself."""
        cached = self._ancestors.get(uid)
        if cached is None:
            cached = self._ancestors[uid] = self._closure(uid, self.parents)
        return cached

    def descendants(self, uid: str) -> frozenset[str]:
        """Descendants including ``uid`` itself."""
        cached = self._descendants.get(uid)
        if cached is None:
            cached = self._descendants[uid] = self._closure(uid, self.children)
        return cached

    def path(self, start: str, end: str, within: set[str] | frozenset[str]) -> list[str]:
        """Shortest downstream path ``start → … → end`` inside ``within``."""
        prev: dict[str, str | None] = {start: None}
        queue = deque([start])
        while queue:
            cur = queue.popleft()
            if cur == end:
                break
            for child in sorted(self.children.get(cur, [])):
                if child in within and child not in prev:
                    prev[child] = cur
                    queue.append(child)
        if end not in prev:
            return []
        out: list[str] = []
        node: str | None = end
        while node is not None:
            out.append(node)
            node = prev[node]
        return out[::-1]

    def topological(self, uids: set[str] | frozenset[str]) -> list[str]:
        """Kahn's order over ``uids``, parents first; ties broken by unique_id.

        Nodes left on a cycle (dbt forbids them, a hand-edited manifest may
        not) are appended sorted rather than dropped.
        """
        indeg = {u: sum(1 for p in self.parents.get(u, []) if p in uids) for u in uids}
        ready = sorted(u for u, d in indeg.items() if d == 0)
        out: list[str] = []
        while ready:
            cur = ready.pop(0)
            out.append(cur)
            for child in self.children.get(cur, []):
                if child in indeg:
                    indeg[child] -= 1
                    if indeg[child] == 0:
                        ready.append(child)
            ready.sort()
        done = set(out)
        out.extend(sorted(u for u in uids if u not in done))
        return out

    def materialized(self, uid: str) -> str:
        if uid.startswith("seed."):
            return "seed"
        return str(self.nodes[uid].get("config", {}).get("materialized", ""))

    def is_ephemeral(self, uid: str) -> bool:
        return self.materialized(uid) == "ephemeral"

    def is_hidden(self, uid: str) -> bool:
        """Traversed but never emitted: ephemeral models and snapshots."""
        return uid.startswith("snapshot.") or self.is_ephemeral(uid)
