"""Run-set command for dbt-meta.

Computes the closed ``--select`` list for a ``defer run`` in dev.

Under ``--defer`` (dbt 1.8, no ``--favor-state``) an unselected ``ref()``
reads the dev table when it exists, else prod. A parent left out of the
selection is therefore silently either prod or a stale dev copy. The run set
closes the selection over every branch change that reaches the targets::

    RunSet(T) = T ∪ ⋃_t ⋃_{m ∈ M ∩ anc*(t)} (desc*(m) ∩ anc*(t))

``M`` is the set of nodes modified on the branch (git is the source of
truth, not ``state:modified``), ``anc*`` / ``desc*`` include the node
itself. In words: every target plus every node on a path from a modified
ancestor down to it, including unmodified intermediates. Modified nodes that
are not upstream of any target are left out.

What counts as modified is defined once, in ``manifest/changes.py``
(shared with ``optimize refresh -m``); the graph is ``manifest/graph.py``.

Ephemeral models and snapshots are traversed but never emitted: an
ephemeral is inlined, a snapshot is not rebuilt in dev (it reads its dev copy
if one exists, else prod), so a path through it gets a warning instead.
Seeds are emitted and get their own ``defer seed`` command.

Git, not ``state:modified``, decides ``M``: prod-manifest drift made
``state:modified`` report 17 changes where the branch had 7. Under
``--check``, exit code 1 means "cannot verify" (selector syntax, unknown
model, failed ``dbt parse``) and is never a pass; a stale dev manifest is
re-parsed once, never silently used.
"""

from __future__ import annotations

import os
from typing import Any

from dbt_meta.errors import DbtMetaError
from dbt_meta.manifest.changes import YAML_SUFFIXES, detect_modified, disabled_paths, short
from dbt_meta.manifest.graph import BUILDABLE_PREFIXES, ManifestGraph
from dbt_meta.utils import get_cached_parser
from dbt_meta.utils.compiled_sql import infer_project_root, run_dbt_parse

SELECTOR_MARKERS = (":", "+", "@", ",", "*")


# -- run set ---------------------------------------------------------------


def compute_run_set(
    graph: ManifestGraph,
    targets: list[str],
    modified: set[str],
) -> tuple[set[str], set[str], dict[str, tuple[str, str]]]:
    """Apply the invariant.

    Returns ``(run_set, upstream, reasons)``: the raw closure (ephemerals
    included), the union of target ancestors, and for each non-target member
    the ``(modified, target)`` pair that pulled it in.
    """
    result: set[str] = set(targets)
    upstream: set[str] = set()
    reasons: dict[str, tuple[str, str]] = {}
    for t in targets:
        anc = graph.ancestors(t)
        upstream |= anc
        for m in sorted(modified & anc):
            for x in graph.descendants(m) & anc:
                result.add(x)
                if x not in targets:
                    reasons.setdefault(x, (m, t))
    return result, upstream, reasons


def reason_chain(graph: ManifestGraph, node: str, modified: str, target: str) -> str:
    """``m (modified) → … → node → … → target``."""
    within = graph.ancestors(target)
    head = graph.path(modified, node, within)
    tail = graph.path(node, target, within)[1:]
    names = [short(u) for u in head + tail]
    if names:
        names[0] = f"{names[0]} (modified)"
    return " → ".join(names)


def build_command(seeds: list[str], models: list[str]) -> str:
    parts = []
    if seeds:
        parts.append(f"defer seed -s {' '.join(seeds)}")
    if models:
        parts.append(f"defer run -s {' '.join(models)}")
    return " && ".join(parts)


# -- orchestration ---------------------------------------------------------


def _manifest_stale(manifest_path: str, project_root: str, files: dict[str, str]) -> str | None:
    """Why the dev manifest is older than the project, or ``None`` when fresh."""
    if not os.path.exists(manifest_path):
        return "dev manifest does not exist"
    built = os.path.getmtime(manifest_path)
    models_dir = os.path.join(project_root, "models")
    for root, _dirs, names in os.walk(models_dir):
        for name in names:
            if name.endswith((".sql", *YAML_SUFFIXES)):
                full = os.path.join(root, name)
                if os.path.getmtime(full) > built:
                    return f"{os.path.relpath(full, project_root)} is newer than the manifest"
    parser = get_cached_parser(manifest_path)
    known = {n.get("original_file_path") for n in parser.manifest.get("nodes", {}).values()}
    known |= disabled_paths(parser.manifest)
    for path in sorted(files):
        if (
            path.startswith("models/")
            and path.endswith(".sql")
            and path not in known
            and os.path.exists(os.path.join(project_root, path))
        ):
            return f"{path} is not in the manifest"
    return None


class RunSetCommand:
    """Compute or verify the closed run set for ``defer run``."""

    def __init__(
        self,
        manifest_path: str,
        prod_manifest_path: str | None,
        files: dict[str, str],
        base_branch: str | None = None,
        parse: bool = True,
    ):
        self.manifest_path = manifest_path
        self.prod_manifest_path = prod_manifest_path
        self.files = files
        self.base_branch = base_branch
        self.parse = parse

    def _ensure_fresh(self) -> None:
        if not self.parse:
            return
        project_root = infer_project_root(self.manifest_path)
        if not project_root:
            return
        why = _manifest_stale(self.manifest_path, project_root, self.files)
        if why is None:
            return
        import sys

        sys.stderr.write(f"ℹ️  Running `dbt parse` in {project_root} ({why})…\n")
        ok, err = run_dbt_parse(project_root)
        if not ok:
            tail = "\n      ".join((err or "").splitlines()[-12:]) or "(no output captured)"
            raise DbtMetaError(
                f"`dbt parse` failed — cannot compute the run set on a stale graph.\n      {tail}",
                "Fix the dbt project errors above, or pass --no-parse to use the manifest on disk as-is.",
            )
        get_cached_parser.cache_clear()

    def _load(self) -> tuple[dict[str, Any], dict[str, Any] | None]:
        self._ensure_fresh()
        if not os.path.exists(self.manifest_path):
            raise DbtMetaError(
                f"dev manifest not found: {self.manifest_path}",
                "Run `dbt parse` in the project, or pass --manifest PATH.",
            )
        manifest = get_cached_parser(self.manifest_path).manifest
        prod: dict[str, Any] | None = None
        if self.prod_manifest_path and os.path.exists(self.prod_manifest_path):
            prod = get_cached_parser(self.prod_manifest_path).manifest
        return manifest, prod

    def execute(
        self,
        targets: list[str] | None = None,
        use_all: bool = False,
        check: bool = False,
    ) -> dict[str, Any]:
        names = list(dict.fromkeys(targets or []))
        if check:
            bad = [n for n in names if any(mark in n for mark in SELECTOR_MARKERS)]
            if bad:
                raise DbtMetaError(
                    f"cannot verify selector {', '.join(bad)} — pass explicit model names"
                )

        manifest, prod = self._load()
        graph = get_cached_parser(self.manifest_path).graph()
        modified_map, changed_macros, warnings = detect_modified(self.files, manifest, prod, graph)
        modified = set(modified_map)

        if use_all:
            target_ids = sorted(u for u in modified if u.startswith(BUILDABLE_PREFIXES))
        else:
            unknown = [n for n in names if graph.resolve(n) is None]
            if unknown:
                raise DbtMetaError(
                    f"not in the dev manifest: {', '.join(unknown)}",
                    "Check the names, or run `dbt parse` if the models are new.",
                )
            target_ids = [u for n in names if (u := graph.resolve(n)) is not None]

        raw, upstream, reasons = compute_run_set(graph, target_ids, modified)
        for t in target_ids:
            if graph.is_ephemeral(t):
                warnings.append(f"{short(t)} is ephemeral — inlined into its children, never built")
        members = [u for u in graph.topological(raw) if not graph.is_hidden(u)]
        for uid in sorted(u for u in raw if u.startswith("snapshot.")):
            m, t = reasons[uid]
            warnings.append(
                f"snapshot {short(uid)} is on the path {reason_chain(graph, uid, m, t)} — "
                "it is not rebuilt in dev and reads its dev copy if one exists, else prod, "
                f"so the change in {short(m)} may not reach its children "
                f"(rebuild deliberately with `defer snapshot -s {short(uid)}`)"
            )

        run_set: list[dict[str, Any]] = []
        for uid in members:
            entry: dict[str, Any] = {"model": short(uid)}
            if uid in target_ids:
                entry["role"] = "target"
                entry["modified"] = uid in modified
            elif uid in modified:
                entry["role"] = "modified"
            else:
                entry["role"] = "intermediate"
            entry["resource_type"] = uid.split(".", 1)[0]
            entry["materialized"] = graph.materialized(uid)
            run_set.append(entry)

        seeds = [short(u) for u in members if u.startswith("seed.")]
        models = [short(u) for u in members if u.startswith("model.")]

        missing: list[dict[str, str]] = []
        seed_warnings: list[str] = []
        if check:
            given = set(target_ids)
            for uid in members:
                if uid in given:
                    continue
                m, t = reasons[uid]
                if uid.startswith("seed."):
                    seed_warnings.append(
                        f"{short(uid)}: modified seed upstream — run `defer seed -s {short(uid)}` first"
                    )
                    continue
                missing.append(
                    {"model": short(uid), "reason": f"on path {reason_chain(graph, uid, m, t)}"}
                )

        macro_warnings = []
        for macro_uid in sorted(changed_macros):
            dependents = sorted(
                short(u)
                for u in upstream
                if macro_uid in graph.nodes[u].get("depends_on", {}).get("macros", [])
            )
            macro_warnings.append({"macro": short(macro_uid), "models": dependents})

        return {
            "targets": [short(u) for u in target_ids],
            "modified": sorted(short(u) for u in modified),
            "modified_upstream": sorted(short(u) for u in modified & upstream),
            "ignored_modified": sorted(short(u) for u in modified - upstream),
            "run_set": run_set,
            "missing": missing,
            "closed": not missing,
            "command": build_command(seeds, models),
            "macro_warnings": macro_warnings,
            "seed_warnings": seed_warnings,
            "warnings": warnings,
            "git": {"base_branch": self.base_branch},
        }
