"""Contour command for dbt-meta.

Checks the trust contour invariant: a model carrying a trust tag must not
stand on an ancestor that has not been reviewed.

The tag lives in ``config.meta.tier``. Values:

``primary``     a significant read point, reviewed and endorsed
``verified``    reviewed and trusted, feeds other models
``review``      queued for review, not yet checked
``deprecating`` reviewed and rejected, still built for its last consumers
``mirror``      an unprocessed 1:1 copy of one staging model; outside the
                ordered rungs, so it is neither trusted nor a violation

Invariant (a violation): no model tagged ``primary`` or ``verified`` has a
transitive model ancestor that is untagged or tagged ``review`` or
``deprecating``. ``staging`` models and ``mirror``
models are exempt — a staging wrapper is reviewed together with the core
model that reads it, and a mirror has nothing in it to review.

Warnings, never violations: a trusted model with no decision doc, or with
fewer than ``--min-tests`` tests referencing it. Without this split the
contour would be empty on day one and an agent would have nowhere to go.
"""

import os
from typing import Any, Optional

from dbt_meta.utils import get_cached_parser
from dbt_meta.utils.compiled_sql import infer_project_root

TRUSTED_TIERS = ("primary", "verified")
UNREVIEWED_TIERS = ("review", "deprecating")
EXEMPT_TIERS = ("mirror",)
EXEMPT_LAYER_PREFIXES = ("staging",)


class ContourCommand:
    """Verify the trust contour invariant across the whole project."""

    SUPPORTS_BIGQUERY = False
    SUPPORTS_DEV = True

    def __init__(
        self,
        config: Any,
        manifest_path: str,
        min_tests: int = 1,
        json_output: bool = False,
    ):
        self.config = config
        self.manifest_path = manifest_path
        self.min_tests = min_tests
        self.json_output = json_output
        self._parser = get_cached_parser(manifest_path)
        self._models = self._parser.get_all_models()
        self._graph = self._parser.graph(('model.',))
        self._test_counts: Optional[dict[str, int]] = None
        self._project_root = infer_project_root(manifest_path)

    # -- graph -------------------------------------------------------------

    def _ancestors(self, unique_id: str) -> frozenset[str]:
        """Transitive model ancestors (self excluded), memoized by the graph."""
        return self._graph.ancestors(unique_id) - {unique_id}

    # -- model facts -------------------------------------------------------

    @staticmethod
    def _tier(node: dict[str, Any]) -> Optional[str]:
        tier = node.get('config', {}).get('meta', {}).get('tier')
        return tier if isinstance(tier, str) else None

    @staticmethod
    def _layer(node: dict[str, Any]) -> str:
        parts = node.get('path', '').split('/')
        return parts[0] if parts else ''

    def _is_exempt(self, node: dict[str, Any]) -> bool:
        if self._layer(node).startswith(EXEMPT_LAYER_PREFIXES):
            return True
        return self._tier(node) in EXEMPT_TIERS

    def _test_count(self, model_name: str) -> int:
        if self._test_counts is None:
            counts: dict[str, int] = {}
            for uid, node in self._parser.manifest.get('nodes', {}).items():
                if not uid.startswith('test.'):
                    continue
                for dep in node.get('depends_on', {}).get('nodes', []):
                    if dep.startswith('model.'):
                        name = dep.split('.')[-1]
                        counts[name] = counts.get(name, 0) + 1
            self._test_counts = counts
        return self._test_counts.get(model_name, 0)

    def _has_decision_doc(self, node: dict[str, Any]) -> bool:
        """A decision doc is ``<model dir>/docs/<model>.md`` next to the SQL."""
        original = node.get('original_file_path', '')
        if not original or not original.endswith('.sql'):
            return False
        directory, filename = os.path.split(original)
        doc = os.path.join(directory, 'docs', filename[: -len('.sql')] + '.md')
        candidates = [doc]
        if self._project_root:
            candidates.append(os.path.join(self._project_root, doc))
        return any(os.path.exists(c) for c in candidates)

    # -- execution ---------------------------------------------------------

    def execute(self) -> dict[str, Any]:
        violations: list[dict[str, Any]] = []
        warnings: list[dict[str, Any]] = []
        counts: dict[str, int] = {}

        for unique_id, node in self._models.items():
            tier = self._tier(node)
            if tier:
                counts[tier] = counts.get(tier, 0) + 1
            if tier not in TRUSTED_TIERS:
                continue

            name = unique_id.split('.')[-1]
            blockers = []
            for ancestor_id in sorted(self._ancestors(unique_id)):
                ancestor = self._models[ancestor_id]
                if self._is_exempt(ancestor):
                    continue
                ancestor_tier = self._tier(ancestor)
                if ancestor_tier in TRUSTED_TIERS:
                    continue
                blockers.append({
                    'model': ancestor_id.split('.')[-1],
                    'tier': ancestor_tier or 'none',
                    'path': ancestor.get('original_file_path', ''),
                })

            if blockers:
                violations.append({
                    'model': name,
                    'tier': tier,
                    'path': node.get('original_file_path', ''),
                    'unreviewed_ancestors': len(blockers),
                    'ancestors': blockers,
                })

            tests = self._test_count(name)
            missing = []
            if not self._has_decision_doc(node):
                missing.append('decision doc')
            if tests < self.min_tests:
                missing.append(f'{tests} tests (min {self.min_tests})')
            if missing:
                warnings.append({
                    'model': name,
                    'tier': tier,
                    'missing': missing,
                })

        violations.sort(key=lambda v: (-v['unreviewed_ancestors'], v['model']))
        warnings.sort(key=lambda w: w['model'])

        return {
            'ok': not violations,
            'summary': {
                'models_checked': sum(counts.get(t, 0) for t in TRUSTED_TIERS),
                'violations': len(violations),
                'warnings': len(warnings),
                'tiers': dict(sorted(counts.items())),
            },
            'violations': violations,
            'warnings': warnings,
        }
