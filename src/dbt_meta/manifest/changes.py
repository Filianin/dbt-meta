"""Map files changed on a git branch onto manifest nodes.

Single definition of "modified" shared by ``meta run-set`` and
``meta optimize refresh -m``:

* ``.sql`` model / ``.csv`` seed files → the node by ``original_file_path``.
* ``.yml`` files → nodes patched from it, only when their ``config`` differs
  from the prod manifest (description edits never trigger a rebuild); with no
  prod manifest every patched node counts, with a warning.
* changed macros → returned separately, never modified nodes.
* disabled models, changed snapshots, deleted or unparsed model files →
  warnings.
"""

from __future__ import annotations

import os
from typing import Any

from dbt_meta.manifest.graph import BUILDABLE_PREFIXES, ManifestGraph

YAML_SUFFIXES = (".yml", ".yaml")


def short(uid: str) -> str:
    return uid.split(".")[-1]


def _patch_file(node: dict[str, Any]) -> str:
    patch = node.get("patch_path") or ""
    return patch.split("://", 1)[-1]


def detect_modified(
    files: dict[str, str],
    manifest: dict[str, Any],
    prod_manifest: dict[str, Any] | None,
    graph: ManifestGraph,
) -> tuple[dict[str, str], dict[str, str], list[str]]:
    """Map branch-changed files to nodes.

    Returns ``(modified, changed_macros, warnings)``: ``modified`` is
    ``{uid: reason}``, ``changed_macros`` is ``{macro_uid: path}``.
    """
    modified: dict[str, str] = {}
    warnings: list[str] = []

    by_file: dict[str, str] = {}
    by_patch: dict[str, list[str]] = {}
    model_dirs: set[str] = set()
    for uid, node in graph.nodes.items():
        if not uid.startswith(BUILDABLE_PREFIXES):
            continue
        path = node.get("original_file_path", "")
        if path:
            by_file[path] = uid
            if uid.startswith("model."):
                model_dirs.add(path.split("/", 1)[0])
        patch = _patch_file(node)
        if patch:
            by_patch.setdefault(patch, []).append(uid)

    snapshots = {
        node.get("original_file_path", ""): uid
        for uid, node in manifest.get("nodes", {}).items()
        if uid.startswith("snapshot.")
    }
    macros = {
        node.get("original_file_path", ""): uid for uid, node in manifest.get("macros", {}).items()
    }
    disabled = disabled_paths(manifest)
    prod_nodes = (prod_manifest or {}).get("nodes", {})
    changed_macros: dict[str, str] = {}

    for path in sorted(files):
        if path in by_file:
            modified[by_file[path]] = "sql" if path.endswith(".sql") else "seed"
        elif path in macros:
            changed_macros[macros[path]] = path
        elif path in disabled:
            warnings.append(f"{path}: disabled (enabled: false) — not built")
        elif path in snapshots:
            warnings.append(f"snapshot changed: {short(snapshots[path])} — not part of the run set")
        elif path.endswith(YAML_SUFFIXES) and path in by_patch:
            for uid in by_patch[path]:
                if prod_manifest is None:
                    modified.setdefault(uid, "yml (no prod manifest to compare)")
                elif uid not in prod_nodes:
                    modified.setdefault(uid, "yml (new node)")
                elif graph.nodes[uid].get("config") != prod_nodes[uid].get("config"):
                    modified.setdefault(uid, "yml config")
        elif path.endswith(".sql") and path.split("/", 1)[0] in model_dirs:
            if os.path.exists(path):
                warnings.append(f"{path}: not in the dev manifest — run `dbt parse`")
            else:
                warnings.append(f"{path}: deleted on the branch — ignored")

    if prod_manifest is None and any(p.endswith(YAML_SUFFIXES) for p in files):
        warnings.append(
            "prod manifest not found — every node patched by a changed yml counts as modified"
        )
    return modified, changed_macros, warnings


def disabled_paths(manifest: dict[str, Any]) -> set[str]:
    """Files of ``enabled: false`` nodes — dbt keeps them out of ``nodes``."""
    return {
        node.get("original_file_path", "")
        for versions in manifest.get("disabled", {}).values()
        for node in versions
    }
