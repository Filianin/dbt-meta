"""Column-aware refresh planner.

Given one or more changed dbt models (and, optionally, the specific changed
columns of each), partition the transitive downstream into three buckets:

    needs_full_refresh  — schema or incremental-key dependency: must rebuild
                          from scratch with --full-refresh
    needs_incremental   — references the changed column(s) but only in
                          read-only WHERE/JOIN/SELECT — incremental run is
                          enough to pick up new data
    can_skip            — does NOT reference any changed column at all;
                          existing data is still valid

The classifier is intentionally conservative: when in doubt, escalate
to ``needs_full_refresh``. False negatives (data inconsistency) are far
more costly than false positives (extra compute).

Usage:
    advisor = RefreshAdvisor(manifest, catalog)
    result = advisor.plan(
        changes={'core_clients': {'client_id', 'country'}},
    )
"""

from __future__ import annotations

import os
import shutil
import subprocess
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterable, Mapping, Optional

from dbt_meta.usage._common import (
    find_target_node,
    references_target,
    select_star_from,
    transitive_downstream,
    upstream_table_aliases,
)
from dbt_meta.usage.extractor import ColumnUsageExtractor

DBT_COMPILE_TIMEOUT = 300  # seconds — bulk compile for many downstream models

# Operators that indicate a value-flowing dependency (not just structural)
_VALUE_FLOW_CLAUSES = {"select", "where", "join", "group_by", "qualify", "partition_by"}


@dataclass
class _RefreshDecision:
    model: str
    bucket: str  # 'full' | 'incremental' | 'skip'
    reasons: list[str] = field(default_factory=list)


@dataclass
class RefreshPlan:
    needs_full_refresh: list[_RefreshDecision] = field(default_factory=list)
    needs_incremental: list[_RefreshDecision] = field(default_factory=list)
    can_skip: list[_RefreshDecision] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        def _ser(d: _RefreshDecision) -> dict[str, Any]:
            return {"model": d.model, "reasons": d.reasons}

        return {
            "summary": {
                "full_refresh": len(self.needs_full_refresh),
                "incremental": len(self.needs_incremental),
                "skip": len(self.can_skip),
            },
            "needs_full_refresh": [_ser(d) for d in self.needs_full_refresh],
            "needs_incremental": [_ser(d) for d in self.needs_incremental],
            "can_skip": [_ser(d) for d in self.can_skip],
            "warnings": self.warnings,
        }


class RefreshAdvisor:
    """Plan minimal full-refresh / incremental / skip set for changed models."""

    def __init__(
        self,
        manifest: dict[str, Any],
        catalog: Optional[dict[str, Any]] = None,
        extractor: Optional[ColumnUsageExtractor] = None,
        manifest_path: Optional[str] = None,
        auto_compile: bool = True,
    ) -> None:
        """Initialize the refresh advisor.

        Args:
            manifest: parsed manifest.json
            catalog: optional parsed catalog.json
            extractor: ColumnUsageExtractor (one is created if omitted)
            manifest_path: file path of the manifest.json — needed to find
                the dbt project root for the disk-compiled fallback and the
                bulk ``dbt compile`` invocation.
            auto_compile: when True (default), the advisor will run
                ``dbt compile --select <downstream models>`` once at the
                start of ``plan()`` if many downstream models are missing
                ``compiled_code`` AND lack disk-compiled SQL.
        """
        self.manifest = manifest
        self.catalog = catalog or {}
        self.extractor = extractor or ColumnUsageExtractor(dialect="bigquery")
        self.manifest_path = manifest_path
        self.auto_compile = auto_compile
        self._project_root: Optional[str] = (
            _infer_project_root(manifest_path) if manifest_path else None
        )
        self._bulk_compile_attempted = False

    def plan(
        self,
        changes: Mapping[str, Optional[set[str]]],
    ) -> RefreshPlan:
        """Build the refresh plan with chain-aware propagation.

        Args:
            changes: ``{changed_model_short_name: {changed_column_lowercased, ...} | None}``.
                Pass ``None`` instead of a column set when the entire model
                changed (e.g., DDL change unknown). The advisor then assumes
                every column is at risk.

        Returns:
            ``RefreshPlan`` with three buckets sorted by model name.

        Algorithm:
            1. Resolve changed models + transitive downstream universe.
            2. Bulk-compile if many downstream models lack ``compiled_code``.
            3. Walk downstream in topological order. For each model M, look
               at its already-affected upstreams (a model is affected if it
               either is in ``changes`` or was marked affected earlier in
               the walk). If M's compiled SQL references any of those
               upstreams' affected columns — OR uses ``SELECT *`` from one
               — M itself becomes affected and propagates the impact to
               *its* downstream. This catches transitive consumers that
               never directly mention the changed model.
            4. Classify every affected model into full/incremental;
               un-affected downstream go to ``can_skip``.
        """
        plan = RefreshPlan()
        nodes = self.manifest.get("nodes", {})

        # 1) Resolve changed models, collect transitive downstream
        resolved: list[tuple[str, dict[str, Any], Optional[set[str]]]] = []
        all_downstream: set[str] = set()
        changed_uids: set[str] = set()
        for changed_short, cols in changes.items():
            target = find_target_node(self.manifest, changed_short)
            if target is None:
                plan.warnings.append(f"changed model '{changed_short}' not in manifest")
                continue
            unique_id, model = target
            resolved.append((unique_id, model, cols))
            changed_uids.add(unique_id)
            all_downstream.update(transitive_downstream(self.manifest, unique_id))

        # 2) Bulk-compile if needed
        if self.auto_compile:
            self._maybe_bulk_compile(all_downstream, plan)

        # 3) Initialise affected-set with the changed models themselves.
        # Value is ``None`` (= "all output cols affected, including via
        # SELECT *") OR a set of specific column names.
        affected_cols: dict[str, Optional[set[str]]] = {}
        affected_via: dict[str, list[str]] = {}  # uid -> human-readable reasons
        for uid, _node, cols in resolved:
            affected_cols[uid] = None if cols is None else set(cols)
            affected_via[uid] = ["changed model itself"]

        # 3a) Topological order: process upstream before downstream.
        ordered = self._topological_order(all_downstream, nodes)

        # 3b) Walk and propagate
        for ds_uid in ordered:
            if ds_uid in changed_uids:
                continue
            ds_node = nodes.get(ds_uid)
            if ds_node is None:
                continue
            parents = (ds_node.get("depends_on") or {}).get("nodes") or []
            new_cols, why = self._propagate(ds_node, parents, affected_cols, nodes)
            if why:
                affected_cols[ds_uid] = new_cols
                affected_via[ds_uid] = why

        # 4) Classify everything
        # Changed models go straight into full_refresh (SQL itself changed)
        for uid, _model, _cols in resolved:
            plan.needs_full_refresh.append(
                _RefreshDecision(
                    model=uid.split(".")[-1],
                    bucket="full",
                    reasons=["changed model itself"],
                )
            )
        for ds_uid in sorted(all_downstream):
            ds_node = nodes.get(ds_uid)
            if ds_node is None:
                continue
            ds_short = ds_uid.split(".")[-1]
            if ds_uid not in affected_cols:
                plan.can_skip.append(
                    _RefreshDecision(
                        model=ds_short, bucket="skip",
                        reasons=["does not reference changed model's columns (chain-aware)"],
                    )
                )
                continue
            decision = self._classify_affected(
                ds_short, ds_node,
                affected_cols=affected_cols[ds_uid],
                reasons=affected_via.get(ds_uid, []),
            )
            if decision.bucket == "full":
                plan.needs_full_refresh.append(decision)
            elif decision.bucket == "incremental":
                plan.needs_incremental.append(decision)
            else:
                plan.can_skip.append(decision)

        for bucket in (plan.needs_full_refresh, plan.needs_incremental, plan.can_skip):
            bucket.sort(key=lambda d: d.model)
        return plan

    # ----- chain propagation -----

    @staticmethod
    def _topological_order(downstream_uids: set[str], nodes: dict[str, Any]) -> list[str]:
        """Return downstream uids in topological order (Kahn's algorithm).

        Order guarantees parents are visited before children, so by the time
        we process a model M every upstream M had a chance to be marked as
        affected.
        """
        in_set = downstream_uids
        # Build subgraph adjacency limited to nodes in our downstream set
        adj: dict[str, list[str]] = {uid: [] for uid in in_set}
        indeg: dict[str, int] = {uid: 0 for uid in in_set}
        for uid in in_set:
            node = nodes.get(uid) or {}
            parents = (node.get("depends_on") or {}).get("nodes") or []
            for p in parents:
                if p in in_set:
                    adj[p].append(uid)
                    indeg[uid] += 1

        # Roots first — models whose only parents (in our subgraph) have been processed
        from collections import deque
        q: deque[str] = deque(uid for uid, d in indeg.items() if d == 0)
        out: list[str] = []
        while q:
            cur = q.popleft()
            out.append(cur)
            for child in adj.get(cur, []):
                indeg[child] -= 1
                if indeg[child] == 0:
                    q.append(child)
        # Append any models we couldn't topologically order (cycles shouldn't
        # happen in dbt, but guard against pathological manifests).
        if len(out) < len(in_set):
            out.extend(uid for uid in in_set if uid not in set(out))
        return out

    def _propagate(
        self,
        ds_node: dict[str, Any],
        parents: list[str],
        affected_cols: dict[str, Optional[set[str]]],
        nodes: dict[str, Any],
    ) -> tuple[Optional[set[str]], list[str]]:
        """Decide whether ``ds_node`` is affected via the chain.

        Returns:
            ``(propagated_cols, reasons)``:
              * ``propagated_cols`` is ``None`` if all of ds's output columns
                should be considered affected (e.g. via SELECT *), a set of
                specific col names when we can be more precise (V1: always
                ``None`` once the model is affected), or ``set()`` when the
                model is not affected.
              * ``reasons`` is empty when the model is not affected.
        """
        ds_short = (ds_node.get("name") or "").lower()
        sql = self._resolve_compiled_sql(ds_node)
        if not sql.strip():
            # No compiled SQL — conservative: treat as affected if any
            # parent is affected, so it ends up in full_refresh.
            for p in parents:
                if p in affected_cols:
                    return None, [
                        "no compiled_code; assumed affected through chain "
                        "(run `dbt compile` for column-level precision)"
                    ]
            return set(), []

        reasons: list[str] = []
        ds_affected: Optional[set[str]] = set()  # local accumulator
        whole_row = False

        for parent_uid in parents:
            if parent_uid not in affected_cols:
                continue
            parent_node = nodes.get(parent_uid) or {}
            parent_short = parent_node.get("name") or parent_uid.split(".")[-1]
            parent_cols = affected_cols[parent_uid]
            aliases = upstream_table_aliases(parent_node)
            if not aliases:
                continue

            # SELECT * — propagates any column change of parent into all of ds
            if select_star_from(sql, aliases):
                reasons.append(f"SELECT * from '{parent_short}' (whole-row propagation)")
                whole_row = True
                continue

            events = self.extractor.extract(sql, ds_short, aliases)
            if parent_cols is None:
                # Whole parent affected — any reference makes ds whole-row
                # affected too (we can't prove which cols of ds are spared).
                if events or references_target(sql, aliases):
                    reasons.append(
                        f"references '{parent_short}' (whole model affected upstream)"
                    )
                    whole_row = True
                continue

            # Parent has specific affected cols — collect names of upstream
            # cols that show up in ds's compiled SQL. These are used as a
            # *proxy* for ds's affected output cols (works when downstream
            # passes columns through with the same name; over-counts on
            # rename-only aliases, which is the safer side).
            for ev in events:
                if ev.column in parent_cols:
                    reasons.append(
                        f"uses '{parent_short}.{ev.column}' (changed) in {ev.clause}"
                    )
                    if ds_affected is not None:
                        ds_affected.add(ev.column)

        if not reasons:
            return set(), []
        if whole_row:
            return None, reasons
        return ds_affected, reasons

    # ----- per-affected-model classification -----

    def _classify_affected(
        self,
        ds_short: str,
        ds_node: dict[str, Any],
        *,
        affected_cols: Optional[set[str]],
        reasons: list[str],
    ) -> _RefreshDecision:
        """Decide full-refresh vs incremental for an already-affected model."""
        is_incremental = (ds_node.get("config") or {}).get("materialized") == "incremental"
        ds_unique_keys = self._unique_keys(ds_node)
        ds_partition_cols = self._partition_cols(ds_node)
        sql = self._resolve_compiled_sql(ds_node)

        if not sql.strip():
            return _RefreshDecision(
                model=ds_short, bucket="full",
                reasons=reasons + ["no compiled_code; cannot prove safety"],
            )

        # Whole-row propagation (SELECT * or unknown column set) is
        # schema-sensitive → full refresh
        if affected_cols is None:
            return _RefreshDecision(
                model=ds_short, bucket="full",
                reasons=reasons + ["whole-row impact propagated through chain"],
            )

        # Refresh is needed; choose full-vs-incremental
        if not is_incremental:
            return _RefreshDecision(
                model=ds_short, bucket="full",
                reasons=reasons + ["materialization is not incremental"],
            )
        # Incremental — check whether incremental keys (unique_key /
        # partition_by) intersect the affected columns of *this* model.
        # If they do, full-refresh is mandatory because incremental writes
        # would corrupt existing partitions / merge keys.
        unique_hit = ds_unique_keys & affected_cols
        partition_hit = ds_partition_cols & affected_cols
        if unique_hit:
            return _RefreshDecision(
                model=ds_short, bucket="full",
                reasons=reasons + [f"unique_key column(s) affected: {sorted(unique_hit)}"],
            )
        if partition_hit:
            return _RefreshDecision(
                model=ds_short, bucket="full",
                reasons=reasons + [f"partition_by column(s) affected: {sorted(partition_hit)}"],
            )
        return _RefreshDecision(
            model=ds_short, bucket="incremental",
            reasons=reasons,
        )

    # ----- model config helpers -----

    @staticmethod
    def _unique_keys(node: dict[str, Any]) -> set[str]:
        cfg = node.get("config") or {}
        uk = cfg.get("unique_key")
        if not uk:
            return set()
        if isinstance(uk, str):
            return {uk.lower()}
        if isinstance(uk, (list, tuple)):
            return {str(x).lower() for x in uk if x}
        return set()

    @staticmethod
    def _partition_cols(node: dict[str, Any]) -> set[str]:
        cfg = node.get("config") or {}
        pb = cfg.get("partition_by")
        if not pb:
            return set()
        if isinstance(pb, str):
            return {pb.lower()}
        if isinstance(pb, dict):
            f = pb.get("field") or pb.get("column") or ""
            return {f.lower()} if f else set()
        if isinstance(pb, (list, tuple)):
            return {str(x).lower() for x in pb if x}
        return set()

    # ----- compiled-SQL resolution -----

    def _resolve_compiled_sql(self, node: dict[str, Any]) -> str:
        """Return compiled SQL for a node: manifest → disk → empty.

        Auto-compile (level 3) is handled in bulk via ``_maybe_bulk_compile``
        BEFORE per-model classification, so this method itself is cheap
        (no subprocess calls).
        """
        sql = (node.get("compiled_code") or "").strip()
        if sql:
            return sql
        if self._project_root:
            disk_sql = _read_disk_compiled(self._project_root, node)
            if disk_sql:
                return disk_sql
        return ""

    def _maybe_bulk_compile(self, downstream_ids: set[str], plan: "RefreshPlan") -> None:
        """Run ``dbt compile`` once for downstream models that lack compiled SQL.

        Scans the ENTIRE downstream set (not a sample) and triggers a single
        ``dbt compile --select <missing models>`` invocation when:

          * project root resolved (we are inside a dbt project, possibly via
            cwd fallback in :func:`_infer_project_root`)
          * ``dbt`` CLI is available on PATH
          * at least one downstream model has neither ``compiled_code`` in the
            manifest nor a ``target/compiled/.../<file>.sql`` on disk

        Only the missing models are passed to ``dbt compile`` — this keeps
        the compile fast even on large downstream chains.
        """
        if self._bulk_compile_attempted or not self._project_root:
            return
        nodes = self.manifest.get("nodes", {})

        missing_ids: list[str] = []
        for uid in downstream_ids:
            node = nodes.get(uid)
            if not node:
                continue
            if (node.get("compiled_code") or "").strip():
                continue
            if _read_disk_compiled(self._project_root, node):
                continue
            missing_ids.append(uid)

        if not missing_ids:
            return  # everything already has compiled SQL

        self._bulk_compile_attempted = True
        target_models = sorted({uid.split(".")[-1] for uid in missing_ids})

        sys.stderr.write(
            f"ℹ️  {len(missing_ids)}/{len(downstream_ids)} downstream models missing compiled SQL — "
            f"running `dbt compile` for {len(target_models)} model(s) "
            f"in {self._project_root} (one-time, may take 1-5 min)…\n"
        )
        ok, err = _run_bulk_dbt_compile(target_models, self._project_root)
        if ok:
            sys.stderr.write("✅ dbt compile succeeded; analysing column usage now.\n")
        else:
            err_short = (err or "").splitlines()[0][:160] if err else "unknown"
            plan.warnings.append(
                f"bulk dbt compile failed; analysis falls back to model-level only: {err_short}"
            )
            sys.stderr.write(
                f"⚠️  dbt compile failed.\n"
                f"   First error: {err_short}\n"
                f"   Workaround: re-run with the production manifest, which already\n"
                f"               has compiled_code:\n"
                f"     meta optimize refresh -m --manifest ~/dbt-state/manifest.json\n"
                f"   Or fix dbt project errors and retry, or use --no-compile to skip\n"
                f"   the auto-compile attempt entirely.\n"
            )


def _infer_project_root(manifest_path: Optional[str]) -> Optional[str]:
    """Find a dbt project root by walking from the manifest path, then cwd.

    Two-step strategy:
      1. Walk up from the manifest path itself (handles dev manifest at
         ``<project>/target/manifest.json``).
      2. Walk up from ``Path.cwd()`` (handles prod manifest at
         ``~/dbt-state/manifest.json`` when the user is invoking ``meta``
         from inside ``reports/`` or ``reports-spare/``). To avoid
         accidentally picking up unrelated ``dbt_project.yml`` files higher
         up the home directory, step 2 additionally requires a ``target/``
         subdirectory next to ``dbt_project.yml`` — that's a strong signal
         the user actually built the project (``dbt parse``/``compile`` /
         ``run`` all create it).

    Without step 2, prod-manifest runs from a real dbt project couldn't
    trigger auto-compile because ``~/dbt-state/`` is not a dbt project,
    so every downstream missing ``compiled_code`` ended up in
    ``full_refresh`` with a "no compiled_code; cannot prove safety" reason.
    """
    if manifest_path:
        try:
            resolved = Path(manifest_path).expanduser().resolve()
        except OSError:
            resolved = None
        if resolved is not None:
            for parent in [resolved.parent, *resolved.parents]:
                if (parent / "dbt_project.yml").exists():
                    return str(parent)

    try:
        cwd = Path.cwd().resolve()
    except OSError:
        return None
    for parent in [cwd, *cwd.parents]:
        if (parent / "dbt_project.yml").exists() and (parent / "target").is_dir():
            return str(parent)
    return None


def _read_disk_compiled(project_root: str, node: dict[str, Any]) -> str:
    """Read compiled SQL from ``target/compiled/{package}/{original_file_path}``.

    Returns empty string when the file is missing, empty, or unreadable.
    """
    package = node.get("package_name") or ""
    if not package:
        # Fallback: derive from unique_id "model.<pkg>.<name>"
        uid = str(node.get("unique_id", ""))
        parts = uid.split(".")
        if len(parts) >= 3 and parts[0] == "model":
            package = parts[1]
    path = node.get("original_file_path") or node.get("path") or ""
    if not (package and path):
        return ""
    compiled_path = Path(project_root) / "target" / "compiled" / package / path
    if not compiled_path.is_file():
        return ""
    try:
        content = compiled_path.read_text()
    except OSError:
        return ""
    return content if content.strip() else ""


def _find_dbt_executable(project_root: Optional[str]) -> Optional[str]:
    """Locate the dbt CLI to use, preferring the project's own venv.

    Order:
      1. ``<project_root>/.venv/bin/dbt`` (most common convention)
      2. ``<project_root>/venv/bin/dbt``
      3. ``$VIRTUAL_ENV/bin/dbt`` if the user has a venv activated
      4. ``shutil.which("dbt")`` — system PATH

    The project-local lookup matters because users frequently install
    ``dbt-core`` inside a project venv while having a different ``dbt`` on
    PATH (e.g. ``dbt-fusion`` preview). Without this, ``meta optimize
    refresh`` would invoke the wrong CLI and either fail or produce a
    differently-parsed manifest.
    """
    if project_root:
        for candidate in (".venv/bin/dbt", "venv/bin/dbt"):
            local = Path(project_root) / candidate
            if local.is_file():
                return str(local)
    venv_env = os.environ.get("VIRTUAL_ENV")
    if venv_env:
        candidate = Path(venv_env) / "bin" / "dbt"
        if candidate.is_file():
            return str(candidate)
    return shutil.which("dbt")


def _run_bulk_dbt_compile(
    model_names: list[str],
    project_root: str,
    timeout: int = DBT_COMPILE_TIMEOUT,
) -> tuple[bool, Optional[str]]:
    """Invoke ``dbt compile --select <models...>`` in the project root.

    Returns (success, error_text). The dbt CLI honours the user's default
    target from ``profiles.yml`` so callers don't need to pass --target.
    """
    dbt_cmd = _find_dbt_executable(project_root)
    if not dbt_cmd:
        return False, "dbt CLI not found in project venv or PATH"
    cmd = [dbt_cmd, "compile", "--select", *model_names]
    try:
        result = subprocess.run(
            cmd,
            cwd=project_root,
            capture_output=True,
            text=True,
            timeout=timeout,
        )
    except subprocess.TimeoutExpired:
        return False, f"dbt compile timed out after {timeout}s"
    except OSError as exc:
        return False, f"failed to launch dbt: {exc}"

    if result.returncode != 0:
        out = ((result.stdout or "") + (result.stderr or "")).strip()
        return False, out or f"dbt compile exited with code {result.returncode}"
    return True, None


def changed_models_from_git(modified_files: Iterable[str], manifest: dict[str, Any]) -> dict[str, None]:
    """Helper: convert a list of git-modified file paths into ``{model: None}``.

    Each value is ``None`` because git only tells us *which file* changed,
    not which columns inside that file were affected. Callers can pass
    column-level diffs separately if they have them.
    """
    out: dict[str, None] = {}
    files = {f for f in modified_files if f}
    for unique_id, node in manifest.get("nodes", {}).items():
        if not unique_id.startswith("model."):
            continue
        path = node.get("original_file_path", "")
        if path in files:
            out[unique_id.split(".")[-1]] = None
    return out
