"""Column-level lineage for dbt models.

Builds a directed graph of column→column dependencies by parsing compiled
SQL with SQLGlot and stores it as a JSON artifact in ~/dbt-state/lineage.json.

The graph backend is rustworkx for fast traversals (BFS / ancestors / descendants
in O(V+E) with native code).

Public surface:
    LineageGraph  — rustworkx-backed graph with composite "model.column" IDs
    LineageBuilder — builds a LineageGraph from manifest + catalog
    load_artifact / save_artifact — JSON serialization to/from disk
    find_lineage_artifact — locate lineage.json (prod-first priority)
"""

from dbt_meta.lineage.artifact import load_artifact, save_artifact
from dbt_meta.lineage.builder import LineageBuilder
from dbt_meta.lineage.finder import find_lineage_artifact
from dbt_meta.lineage.graph import LineageGraph

__all__ = [
    "LineageBuilder",
    "LineageGraph",
    "find_lineage_artifact",
    "load_artifact",
    "save_artifact",
]
