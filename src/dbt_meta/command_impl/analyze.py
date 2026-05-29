"""Analyze command for dbt-meta.

Provides deep analysis of model partitioning/clustering effectiveness
by combining manifest metadata with BigQuery monitoring data.
"""

from typing import Optional

from dbt_meta.command_impl.base import BaseCommand
from dbt_meta.config import Config
from dbt_meta.errors import DbtMetaError
from dbt_meta.fallback import FallbackLevel
from dbt_meta.utils.monitoring import (
    fetch_column_clustering_info,
    fetch_downstream_filter_patterns,
    fetch_partition_stats,
    fetch_storage_metrics,
    fetch_usage_stats,
)


class AnalyzeCommand(BaseCommand):
    """Analyze model partitioning and clustering effectiveness.

    Combines data from:
    - manifest.json (partition_by, cluster_by, materialized)
    - prod.storage_with_cost (size, cost)
    - prod.partitions_monitoring (partition stats)
    - prod.table_reference_incremental (query frequency)
    - INFORMATION_SCHEMA.COLUMNS (actual partition/cluster columns)

    Generates recommendations for optimization.
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
        self._children_cache: Optional[list] = None

    def execute(self) -> Optional[dict]:
        """Execute analyze command."""
        model = self.get_model_with_fallback()
        if not model:
            return None

        return self.process_model(model)

    def process_model(self, model: dict, level: Optional[FallbackLevel] = None) -> dict:
        """Process model and generate analysis."""
        # Extract schema and table from model
        schema = model.get('schema', '')
        alias = model.get('alias') or model.get('name', '')
        full_table = f"{schema}.{alias}"

        # Get config from manifest
        config = model.get('config', {})
        partition_by = config.get('partition_by')
        cluster_by = config.get('cluster_by', [])
        materialized = config.get('materialized', 'view')

        # Parse partition_by (can be dict or string)
        partition_field = None
        partition_type = None
        if isinstance(partition_by, dict):
            partition_field = partition_by.get('field')
            partition_type = partition_by.get('granularity', 'day').upper()
        elif isinstance(partition_by, str):
            partition_field = partition_by
            partition_type = 'DAY'

        # Fetch monitoring data
        storage = fetch_storage_metrics(schema, alias, monitoring_dataset=self.config.monitoring_dataset)
        partitions = fetch_partition_stats(schema, alias, monitoring_dataset=self.config.monitoring_dataset)
        usage = fetch_usage_stats(schema, alias, monitoring_dataset=self.config.monitoring_dataset)
        bq_columns = fetch_column_clustering_info(schema, alias, monitoring_dataset=self.config.monitoring_dataset)

        # Get downstream models for filter analysis
        downstream_filters = self._analyze_downstream_filters(model)

        # Generate recommendations
        recommendations = self._generate_recommendations(
            config=config,
            storage=storage,
            partitions=partitions,
            usage=usage,
            bq_columns=bq_columns,
            downstream_filters=downstream_filters,
            materialized=materialized,
        )

        return {
            'model': self.model_name,
            'table': full_table,
            'config': {
                'partition_by': partition_field,
                'partition_type': partition_type,
                'cluster_by': cluster_by if cluster_by else [],
                'materialized': materialized,
                'partition_expiration_days': config.get('partition_expiration_days'),
            },
            'storage': storage,
            'partitions': partitions,
            'usage': usage,
            'bq_config': bq_columns,
            'downstream_filters': downstream_filters,
            'recommendations': recommendations,
        }

    def _analyze_downstream_filters(self, model: dict) -> list[str]:
        """Analyze filter patterns in downstream models.

        Returns list of column names commonly used in WHERE clauses
        of models that depend on this one.
        """
        # Import here to avoid circular dependency
        from dbt_meta import commands

        try:
            children = commands.children(
                self.manifest_path,
                self.model_name,
                use_dev=False,
                recursive=False,
                json_output=True
            )
        except DbtMetaError:
            return []

        if not children:
            return []

        # Get SQL for each child and extract filter patterns
        all_filters: dict[str, int] = {}

        for child in children[:10]:  # Limit to first 10 children
            child_name = child.get('model', child.get('path', ''))
            if not child_name:
                continue

            # Extract model name from path if needed
            if '/' in child_name:
                child_name = child_name.split('/')[-1].replace('.sql', '')

            try:
                sql_result = commands.sql(
                    self.manifest_path,
                    child_name,
                    use_dev=False,
                    raw=False
                )
                if sql_result:
                    sql_text = sql_result if isinstance(sql_result, str) else sql_result.get('sql', '')
                    filters = fetch_downstream_filter_patterns(sql_text)
                    for f in filters:
                        all_filters[f] = all_filters.get(f, 0) + 1
            except DbtMetaError:
                continue

        # Return top filters (used in multiple children)
        sorted_filters = sorted(all_filters.items(), key=lambda x: x[1], reverse=True)
        return [f for f, count in sorted_filters[:5]]

    def _generate_recommendations(
        self,
        config: dict,
        storage: Optional[dict],
        partitions: Optional[dict],
        usage: Optional[dict],
        bq_columns: Optional[dict],
        downstream_filters: list[str],
        materialized: str,
    ) -> list[dict]:
        """Generate optimization recommendations."""
        recs = []

        # Skip recommendations for views
        if materialized == 'view':
            return recs

        partition_by = config.get('partition_by')
        cluster_by = config.get('cluster_by', [])
        partition_exp = config.get('partition_expiration_days')

        # Check storage size
        total_gb = storage.get('total_gb', 0) if storage else 0

        # 1. No partitioning on large table
        if not partition_by and total_gb > 10:
            recs.append({
                'type': 'add_partition',
                'priority': 'HIGH',
                'message': f"Add partitioning - {total_gb:.1f} GB unpartitioned table",
                'impact': 'Reduces query scan by filtering on partition column',
            })

        # 2. No clustering on frequently queried table
        query_count = usage.get('query_count', 0) if usage else 0
        if not cluster_by and total_gb > 5 and query_count > 100:
            suggested_cols = downstream_filters[:4] if downstream_filters else []
            msg = "Add clustering"
            if suggested_cols:
                msg += f" - consider: {', '.join(suggested_cols)}"
            recs.append({
                'type': 'add_clustering',
                'priority': 'MEDIUM',
                'message': msg,
                'impact': 'Improves query performance for filtered queries',
            })

        # 3. Partition expiration missing
        if partition_by and not partition_exp and partitions:
            partition_count = partitions.get('count', 0)
            if partition_count > 365:
                # Estimate savings from 90-day retention
                part_gb = partitions.get('total_gb', 0)
                estimated_savings = part_gb * 0.75  # Rough estimate
                recs.append({
                    'type': 'partition_expiration',
                    'priority': 'MEDIUM',
                    'message': f"Add partition expiration - {partition_count} partitions, ~{estimated_savings:.1f} GB reclaimable",
                    'impact': 'Reduces storage cost by removing old partitions',
                })

        # 4. Config mismatch with actual BigQuery config
        if bq_columns:
            bq_partition = bq_columns.get('partition_column', {})

            # Check if manifest partition differs from BigQuery
            if partition_by and bq_partition:
                bq_part_name = bq_partition.get('name', '').lower()
                manifest_part = partition_by.get('field', partition_by) if isinstance(partition_by, dict) else partition_by
                if manifest_part and manifest_part.lower() != bq_part_name:
                    recs.append({
                        'type': 'config_mismatch',
                        'priority': 'LOW',
                        'message': f"Partition mismatch: manifest='{manifest_part}', BigQuery='{bq_part_name}'",
                        'impact': 'Config may be out of sync with actual table',
                    })

            # Check clustering alignment with downstream usage
            if cluster_by and downstream_filters:
                cluster_set = set(c.lower() for c in cluster_by)
                filter_set = set(f.lower() for f in downstream_filters[:3])
                missing = filter_set - cluster_set
                if missing:
                    recs.append({
                        'type': 'clustering_optimization',
                        'priority': 'LOW',
                        'message': f"Consider adding to cluster_by: {', '.join(missing)}",
                        'impact': 'These columns are frequently used in downstream filters',
                    })

        # 5. Potential storage savings
        if storage:
            potential_savings = storage.get('potential_savings_usd', 0)
            if potential_savings > 1:  # More than $1/month
                optimal_model = storage.get('optimal_billing_model', '')
                recs.append({
                    'type': 'billing_model',
                    'priority': 'LOW',
                    'message': f"Switch to {optimal_model} billing - saves ${potential_savings:.2f}/month",
                    'impact': 'Reduces monthly storage cost',
                })

        return recs
