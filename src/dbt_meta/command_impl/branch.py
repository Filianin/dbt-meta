"""Branch command for dbt-meta.

Analyzes optimization impact across upstream and downstream model chains,
identifying alignment issues between partitioning/clustering configurations.
"""

from typing import Any, Optional

from dbt_meta.command_impl.base import BaseCommand
from dbt_meta.config import Config
from dbt_meta.errors import DbtMetaError
from dbt_meta.fallback import FallbackLevel
from dbt_meta.utils.monitoring import fetch_downstream_filter_patterns


class BranchCommand(BaseCommand):
    """Analyze optimization across model branch.

    Examines upstream and downstream models to identify:
    - Partition/cluster alignment between related models
    - Filter patterns in downstream that should inform upstream config
    - Optimization opportunities across the entire branch

    Returns structured analysis with recommendations.
    """

    SUPPORTS_BIGQUERY = True
    SUPPORTS_DEV = False  # Only analyze production data

    def __init__(
        self,
        config: Config,
        manifest_path: str,
        model_name: str,
        use_dev: bool = False,
        json_output: bool = False
    ):
        super().__init__(config, manifest_path, model_name, use_dev, json_output)

    def execute(self) -> Optional[dict]:
        """Execute branch analysis."""
        model = self.get_model_with_fallback()
        if not model:
            return None

        return self.process_model(model)

    def process_model(self, model: dict, level: Optional[FallbackLevel] = None) -> dict:
        """Process model and generate branch analysis."""
        from dbt_meta.command_impl.children import ChildrenCommand
        from dbt_meta.command_impl.parents import ParentsCommand

        # Get root model config
        config = model.get('config', {})
        root_partition = self._extract_partition_field(config.get('partition_by'))
        root_cluster = config.get('cluster_by', [])

        # Get SQL to extract filter patterns
        root_sql = model.get('compiled_code', '')
        root_filters = fetch_downstream_filter_patterns(root_sql) if root_sql else []

        # Analyze upstream models
        upstream_analysis = []
        try:
            parents = ParentsCommand(
                self.config, self.manifest_path, self.model_name,
                use_dev=False, json_output=True, recursive=False,
            ).execute()
            if parents:
                upstream_analysis = self._analyze_upstream(parents, root_filters, root_partition)
        except DbtMetaError:
            pass

        # Analyze downstream models
        downstream_analysis = []
        try:
            children = ChildrenCommand(
                self.config, self.manifest_path, self.model_name,
                use_dev=False, json_output=True, recursive=False,
            ).execute()
            if children:
                downstream_analysis = self._analyze_downstream(children, root_partition, root_cluster)
        except DbtMetaError:
            pass

        # Generate branch-level recommendations
        recommendations = self._generate_branch_recommendations(
            root_partition=root_partition,
            root_cluster=root_cluster,
            upstream=upstream_analysis,
            downstream=downstream_analysis,
        )

        return {
            'root': self.model_name,
            'root_config': {
                'partition_by': root_partition,
                'cluster_by': root_cluster,
            },
            'upstream': upstream_analysis,
            'downstream': downstream_analysis,
            'recommendations': recommendations,
        }

    def _extract_partition_field(self, partition_by: Any) -> Optional[str]:
        """Extract partition field name from config."""
        if isinstance(partition_by, dict):
            return partition_by.get('field')
        elif isinstance(partition_by, str):
            return partition_by
        return None

    def _analyze_upstream(
        self,
        parents: list[dict],
        root_filters: list[str],
        root_partition: Optional[str]
    ) -> list[dict]:
        """Analyze upstream models for optimization alignment."""
        from dbt_meta.command_impl.config import ConfigCommand

        results = []

        for parent in parents[:10]:  # Limit analysis
            parent_path = parent.get('path', '')
            parent_model = parent_path.split('/')[-1].replace('.sql', '') if parent_path else None

            if not parent_model:
                continue

            # Get parent config
            try:
                parent_config = ConfigCommand(
                    self.config, self.manifest_path, parent_model,
                    use_dev=False, json_output=True,
                ).execute()
            except DbtMetaError:
                parent_config = {}

            if not parent_config:
                continue

            parent_partition = self._extract_partition_field(parent_config.get('partition_by'))
            parent_cluster = parent_config.get('cluster_by', [])
            parent_materialized = parent_config.get('materialized', 'table')

            # Determine alignment status
            impact = 'ALIGNED'
            details = []

            # Skip views
            if parent_materialized == 'view':
                impact = 'N/A'
                details.append('View - no partitioning')
            else:
                # Check if root filters on parent's partition field
                if root_partition and not parent_partition:
                    impact = 'HIGH'
                    details.append(f'No partition - root filters by {root_partition}')
                elif root_filters:
                    # Check if root's filter columns are in parent's cluster
                    parent_cluster_set = set(c.lower() for c in parent_cluster) if parent_cluster else set()
                    for f in root_filters[:3]:
                        if f.lower() not in parent_cluster_set and f.lower() != (parent_partition or '').lower():
                            impact = 'MEDIUM' if impact != 'HIGH' else impact
                            details.append(f'Root filters by {f} - not in cluster')

            results.append({
                'model': parent_model,
                'partition_by': parent_partition,
                'cluster_by': parent_cluster,
                'materialized': parent_materialized,
                'impact': impact,
                'details': details if details else ['Configuration aligned'],
            })

        return results

    def _analyze_downstream(
        self,
        children: list[dict],
        root_partition: Optional[str],
        root_cluster: list[str]
    ) -> list[dict]:
        """Analyze downstream models for filter pattern alignment."""
        from dbt_meta.command_impl.sql import SqlCommand

        results = []

        for child in children[:10]:  # Limit analysis
            child_path = child.get('path', '')
            child_model = child_path.split('/')[-1].replace('.sql', '') if child_path else None

            if not child_model:
                continue

            # Get child SQL to analyze filters
            try:
                sql_result = SqlCommand(
                    self.config, self.manifest_path, child_model,
                    use_dev=False, json_output=False, raw=False,
                ).execute()
                child_sql = sql_result if isinstance(sql_result, str) else ''
            except DbtMetaError:
                child_sql = ''

            filters = fetch_downstream_filter_patterns(child_sql) if child_sql else []

            # Check alignment
            alignment = 'GOOD'
            details = []

            root_cluster_set = set(c.lower() for c in root_cluster) if root_cluster else set()

            for f in filters[:5]:
                f_lower = f.lower()
                if root_partition and f_lower == root_partition.lower():
                    details.append(f'Filters by partition: {f}')
                elif f_lower in root_cluster_set:
                    details.append(f'Filters by cluster: {f}')
                else:
                    alignment = 'SUBOPTIMAL' if alignment != 'POOR' else alignment
                    details.append(f'Filters by {f} - not in partition/cluster')
                    if not root_partition and not root_cluster:
                        alignment = 'POOR'

            results.append({
                'model': child_model,
                'filters_used': filters[:5],
                'alignment': alignment,
                'details': details if details else ['No filter patterns detected'],
            })

        return results

    def _generate_branch_recommendations(
        self,
        root_partition: Optional[str],
        root_cluster: list[str],
        upstream: list[dict],
        downstream: list[dict],
    ) -> list[dict]:
        """Generate branch-level optimization recommendations."""
        recs = []

        # Find upstream issues
        high_impact_upstream = [u for u in upstream if u.get('impact') == 'HIGH']
        for u in high_impact_upstream:
            recs.append({
                'model': u['model'],
                'type': 'upstream_partition',
                'action': f"Add partition_by to {u['model']}",
                'impact': 'Reduces scan for root model queries',
                'priority': 'HIGH',
            })

        # Find downstream alignment issues
        poor_downstream = [d for d in downstream if d.get('alignment') in ('POOR', 'SUBOPTIMAL')]

        # Collect common filter columns not in root config
        missing_cluster_cols: dict[str, int] = {}
        for d in poor_downstream:
            for f in d.get('filters_used', []):
                f_lower = f.lower()
                if root_partition and f_lower == root_partition.lower():
                    continue
                if f_lower in (c.lower() for c in root_cluster):
                    continue
                missing_cluster_cols[f] = missing_cluster_cols.get(f, 0) + 1

        # Recommend adding frequently used filter cols to clustering
        for col, count in sorted(missing_cluster_cols.items(), key=lambda x: x[1], reverse=True)[:2]:
            if count >= 2:  # Used in at least 2 downstream models
                recs.append({
                    'model': 'root',
                    'type': 'add_clustering',
                    'action': f"Add '{col}' to cluster_by",
                    'impact': f"Used as filter in {count} downstream models",
                    'priority': 'MEDIUM',
                })

        # If no partitioning and many downstream filters
        if not root_partition and downstream:
            common_filters = {}
            for d in downstream:
                for f in d.get('filters_used', []):
                    common_filters[f] = common_filters.get(f, 0) + 1

            if common_filters:
                top_filter = max(common_filters.items(), key=lambda x: x[1])
                if top_filter[1] >= 2:
                    recs.append({
                        'model': 'root',
                        'type': 'add_partition',
                        'action': f"Consider partition_by: {top_filter[0]}",
                        'impact': f"Most common downstream filter (used in {top_filter[1]} models)",
                        'priority': 'HIGH',
                    })

        return recs
