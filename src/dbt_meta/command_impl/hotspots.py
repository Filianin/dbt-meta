"""Hotspots command for dbt-meta.

Identifies models with highest optimization potential by analyzing
storage, partitioning, clustering, and query patterns across all tables.
"""

import math
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Optional

from dbt_meta.config import Config
from dbt_meta.utils import get_cached_parser
from dbt_meta.utils.monitoring import (
    fetch_all_tables_storage,
    fetch_dataset_billing_recommendations,
    fetch_model_metrics,
    fetch_model_query_costs,
    fetch_partition_info_all,
    fetch_read_heavy_tables,
    fetch_table_query_frequency,
    fetch_total_bigquery_costs,
    fetch_unused_tables,
)


class HotspotsCommand:
    """Find models with highest optimization potential.

    Scoring algorithm (v4) - calibrated in cents (€0.01 = 1pt):

    1. query_cost: €0.01/week = 1pt (direct spend)
       Max ~600pts for €6/week

    2. high_scan: bytes_per_query × log2(frequency)
       >10GB/q = 20 base, >1GB = 10, >100MB = 3
       Max 100pts

    3. high_slot: slot_sec_per_query × log2(frequency)
       >10min/q = 15 base, >2min = 8, >30sec = 3
       Max 75pts

    4. no_partition: table_size × log2(frequency)
       >100GB = 15 base, >10GB = 8, >1GB = 3
       Max 75pts (only for optimizable tables)

    5. no_clustering: table_size × log2(frequency)
       >100GB = 10 base, >10GB = 5, >1GB = 2
       Max 50pts (only for optimizable tables)

    6. low_cache: wasted_cost × 100 (if cache_hit < 30%)
       wasted = cost_7d × (70% - cache_hit%)
       Max ~200pts

    7. unused: monthly_storage_cost × 100 (if >30 days unused)
       storage = total_gb × €0.02
       Max 200pts

    Returns scoring_details with recommendations for each triggered criterion.
    """

    def __init__(
        self,
        config: Config,
        manifest_path: str,
        limit: int = 10,
        min_gb: float = 0.1,  # Lowered from 1.0
        json_output: bool = False
    ):
        self.config = config
        self.manifest_path = manifest_path
        self.limit = limit
        self.min_gb = min_gb
        self.json_output = json_output

    def execute(self) -> dict:
        """Execute hotspots analysis."""
        # Fetch all data sources in parallel-ish manner
        all_tables = fetch_all_tables_storage(min_gb=self.min_gb, monitoring_dataset=self.config.monitoring_dataset)
        if not all_tables:
            return {
                'hotspots': [],
                'summary': {
                    'total_tables_analyzed': 0,
                    'tables_with_issues': 0,
                    'total_potential_savings_gb': 0,
                    'total_query_cost_7d': 0,
                },
            }

        # Get manifest parser
        parser = get_cached_parser(self.manifest_path)

        # Build lookup maps in parallel (BigQuery queries are I/O bound)
        reverse_model_map = self._build_reverse_model_lookup(parser)

        with ThreadPoolExecutor(max_workers=8) as executor:
            futures = {
                executor.submit(self._build_query_freq_map): 'query_freq',
                executor.submit(self._build_query_cost_map, parser): 'query_cost',
                executor.submit(self._build_partition_map): 'partition',
                executor.submit(self._build_read_heavy_map): 'read_heavy',
                executor.submit(self._build_unused_map): 'unused',
                executor.submit(self._build_model_metrics_map, parser): 'model_metrics',
                executor.submit(fetch_dataset_billing_recommendations): 'dataset_billing',
                executor.submit(fetch_total_bigquery_costs): 'total_bq_costs',
            }

            results = {}
            for future in as_completed(futures):
                name = futures[future]
                results[name] = future.result()

        query_freq_map = results['query_freq']
        query_cost_map = results['query_cost']
        partition_map = results['partition']
        read_heavy_map = results['read_heavy']
        unused_map = results['unused']
        model_metrics_map = results['model_metrics']
        dataset_billing = results['dataset_billing'] or []
        total_bq_costs = results['total_bq_costs'] or {}

        # Score each table
        scored_tables = []
        total_query_cost = 0.0  # For tables with issues
        all_query_cost = 0.0    # For ALL tables
        all_slot_hours = 0.0    # For ALL tables

        for table in all_tables:
            dataset = table['dataset_id']
            table_name = table['table_id']
            table_key = f"{dataset}.{table_name}"

            # Get model_name from reverse lookup (O(1) instead of O(3*N))
            model_name = reverse_model_map.get(table_key)

            score, scoring_details, metrics = self._score_table(
                model_name=model_name,
                dataset=dataset,
                total_gb=float(table.get('total_gb', 0) or 0),
                partition_count=int(table.get('partition_count', 0) or 0),
                query_freq=query_freq_map.get(table_key, {}),
                query_cost=query_cost_map.get(table_key, {}),
                partition_info=partition_map.get(table_key, {}),
                read_heavy=read_heavy_map.get(table_key, {}),
                unused_info=unused_map.get(table_key, {}),
                model_metrics=model_metrics_map.get(table_key, {}),
                parser=parser,
            )

            # Track totals for ALL tables
            all_query_cost += metrics.get('query_cost', 0)
            all_slot_hours += metrics.get('slot_hours', 0)

            if score > 0:
                scored_tables.append({
                    'model': model_name,
                    'table': table_key,
                    'score': score,
                    'scoring_details': scoring_details,
                    'total_gb': float(table.get('total_gb', 0) or 0),
                    'query_cost_7d': metrics.get('query_cost', 0),
                    'query_count_7d': metrics.get('query_count', 0),
                    'slot_hours_7d': metrics.get('slot_hours', 0),
                    'gb_per_query': metrics.get('gb_per_query', 0),
                    'references_7d': metrics.get('references', 0),
                    'is_incremental': metrics.get('is_incremental', False),
                    'is_partitioned': metrics.get('is_partitioned', False),
                    'is_clustered': metrics.get('is_clustered', False),
                })

                total_query_cost += metrics.get('query_cost', 0)

        # Sort by score descending
        scored_tables.sort(key=lambda x: x['score'], reverse=True)
        top_by_score = scored_tables[:self.limit]

        # Format dataset billing recommendations
        dataset_billing_recs = [
            {
                'dataset': row['dataset_id'],
                'tables_recommend_physical': int(row.get('tables_recommend_physical', 0) or 0),
                'total_tables': int(row.get('total_tables', 0) or 0),
                'net_savings_eur': float(row.get('net_savings_eur', 0) or 0),
                'recommended_billing': row.get('recommended_billing', 'PHYSICAL'),
            }
            for row in dataset_billing[:self.limit]
        ]

        # Calculate summary
        total_billing_savings = sum(r['net_savings_eur'] for r in dataset_billing_recs)
        total_gb = sum(t.get('total_gb', 0) for t in scored_tables)
        total_slot_hours = sum(t.get('slot_hours_7d', 0) for t in scored_tables)

        return {
            'hotspots': top_by_score,
            'dataset_billing_recommendations': dataset_billing_recs,
            'summary': {
                'total_tables_analyzed': len(all_tables),
                'tables_with_issues': len(scored_tables),
                'total_size_gb': round(total_gb, 1),
                # Totals for tables with issues
                'total_slot_hours_7d': round(total_slot_hours, 1),
                'total_query_cost_7d': round(total_query_cost, 2),
                # Totals for dbt models only
                'dbt_query_cost_7d': round(all_query_cost, 2),
                'dbt_slot_hours_7d': round(all_slot_hours, 1),
                # Totals for ALL BigQuery usage (including ad-hoc, BI, etc.)
                'bq_total_cost_7d': total_bq_costs.get('total_cost', 0),
                'bq_total_slot_hours_7d': total_bq_costs.get('total_slot_hours', 0),
                'bq_total_queries_7d': total_bq_costs.get('total_queries', 0),
                'total_billing_savings_eur': round(total_billing_savings, 2),
            },
        }

    def _build_query_freq_map(self) -> dict:
        """Build lookup map for query frequency."""
        result = {}
        data = fetch_table_query_frequency(days=7, monitoring_dataset=self.config.monitoring_dataset)  # 7 days to match monitoring
        if data:
            for row in data:
                key = f"{row['dataset_id']}.{row['table_id']}"
                result[key] = {
                    'active_days': int(row.get('active_days', 0) or 0),
                    'references': int(row.get('total_references', 0) or 0),
                }
        return result

    def _build_query_cost_map(self, parser: Any) -> dict:
        """Build lookup map for query costs from models_costs_incremental.

        Uses manifest to correctly map dbt_model_name to dataset.table format.
        Includes raw metrics for per-query calculations.
        """
        result = {}
        data = fetch_model_query_costs(monitoring_dataset=self.config.monitoring_dataset)
        if data:
            for row in data:
                # dbt_model_name format: admirals_bi_dwh.model_name
                dbt_model_name = row.get('dbt_model_name', '')
                # Remove project prefix to get model name
                model_name = dbt_model_name.replace('admirals_bi_dwh.', '')

                cost_data = {
                    'query_cost': float(row.get('query_cost_usd', 0) or 0),
                    'query_count': int(row.get('query_count', 0) or 0),
                    'total_slot_ms': int(row.get('total_slot_ms', 0) or 0),
                    'bytes_processed': int(row.get('bytes_processed', 0) or 0),
                    'cache_hit_ratio': float(row.get('cache_hit_ratio', 0) or 0),
                }

                # Look up model in manifest to get correct schema.table
                model = parser.get_model(model_name)
                if model:
                    schema = model.get('schema', '')
                    alias = model.get('alias') or model.get('name', '')
                    if schema and alias:
                        key = f"{schema}.{alias}"
                        result[key] = cost_data
                else:
                    # Fallback: try double underscore split
                    if '__' in model_name:
                        parts = model_name.split('__')
                        key = f"{parts[0]}.{'__'.join(parts[1:])}"
                        result[key] = cost_data
        return result

    def _build_partition_map(self) -> dict:
        """Build lookup map for partition info."""
        result = {}
        data = fetch_partition_info_all(monitoring_dataset=self.config.monitoring_dataset)
        if data:
            for row in data:
                key = f"{row['dataset_id']}.{row['table_id']}"
                result[key] = {
                    'partition_type': row.get('partition_type'),
                    'partition_count': int(row.get('partition_count', 0) or 0),
                    'expiration_days': row.get('partition_expiration_days'),
                    'total_gb': float(row.get('total_gb', 0) or 0),
                }
        return result

    def _build_read_heavy_map(self) -> dict:
        """Build lookup map for read-heavy tables."""
        result = {}
        data = fetch_read_heavy_tables(monitoring_dataset=self.config.monitoring_dataset)
        if data:
            for row in data:
                key = f"{row['dataset_id']}.{row['table_id']}"
                result[key] = {
                    'reference_count': int(row.get('reference_count', 0) or 0),
                    'total_partitions': int(row.get('total_partitions', 0) or 0),
                }
        return result

    def _build_unused_map(self) -> dict:
        """Build lookup map for unused tables."""
        result = {}
        data = fetch_unused_tables(days_threshold=30, monitoring_dataset=self.config.monitoring_dataset)
        if data:
            for row in data:
                key = f"{row['dataset_id']}.{row['table_id']}"
                result[key] = {
                    'days_unused': int(row.get('days_unused', 0) or 0),
                    'last_used_date': row.get('last_used_date'),
                }
        return result

    def _build_reverse_model_lookup(self, parser: Any) -> dict:
        """Build reverse lookup map: schema.table → model_name.

        Pre-computes mapping for all models in manifest to avoid
        repeated get_model() calls during scoring loop.
        """
        result = {}
        all_models = parser.get_all_models()  # Returns {unique_id: model_data}

        for unique_id, model in all_models.items():
            # Extract model name from unique_id (format: model.project.model_name)
            model_name = unique_id.split('.')[-1]
            schema = model.get('schema', '')
            alias = model.get('alias') or model.get('name', '')
            if schema and alias:
                key = f"{schema}.{alias}"
                result[key] = model_name

        return result

    def _build_model_metrics_map(self, parser: Any) -> dict:
        """Build lookup map for model execution metrics.

        Uses manifest to correctly map dbt_model_name to dataset.table format.
        """
        result = {}
        data = fetch_model_metrics(monitoring_dataset=self.config.monitoring_dataset)
        if data:
            for row in data:
                dbt_model_name = row.get('dbt_model_name', '')
                model_name = dbt_model_name.replace('admirals_bi_dwh.', '')

                metrics_data = {
                    'failed_runs': int(row.get('failed_runs', 0) or 0),
                    'p90_duration_seconds': float(row.get('p90_duration_seconds', 0) or 0),
                    'avg_duration_seconds': float(row.get('avg_duration_seconds', 0) or 0),
                }

                # Look up model in manifest to get correct schema.table
                model = parser.get_model(model_name)
                if model:
                    schema = model.get('schema', '')
                    alias = model.get('config', {}).get('alias') or model.get('name', '')
                    if schema and alias:
                        key = f"{schema}.{alias}"
                        result[key] = metrics_data
        return result

    def _score_table(
        self,
        model_name: Optional[str],
        dataset: str,
        total_gb: float,
        partition_count: int,
        query_freq: dict,
        query_cost: dict,
        partition_info: dict,
        read_heavy: dict,
        unused_info: dict,
        model_metrics: dict,
        parser: Any,
    ) -> tuple[int, list[dict], dict]:
        """Score a table for optimization potential (v4 algorithm).

        Scoring calibrated in "cents equivalent" (€0.01 = 1pt).
        Uses log2(query_count) as frequency multiplier for structural metrics.

        Args:
            model_name: Pre-resolved model name from reverse lookup (or None)
            dataset: BigQuery dataset name
            total_gb: Table size in GB
            partition_count: Number of partitions
            query_freq: Query frequency data
            query_cost: Query cost data (includes bytes_processed, total_slot_ms)
            partition_info: Partition configuration
            read_heavy: Read frequency data
            unused_info: Usage staleness data
            model_metrics: Model execution metrics
            parser: Manifest parser for config lookup

        Returns:
            Tuple of (score, scoring_details list, metrics dict)
        """
        score = 0
        scoring_details = []

        # Extract raw metrics
        cost_7d = query_cost.get('query_cost', 0)
        query_count = query_cost.get('query_count', 0)
        total_slot_ms = query_cost.get('total_slot_ms', 0)
        bytes_processed = query_cost.get('bytes_processed', 0)
        cache_hit_ratio = query_cost.get('cache_hit_ratio', 1.0)

        # Calculate per-query metrics
        gb_per_query = (bytes_processed / query_count / 1e9) if query_count > 0 else 0
        slot_sec_per_query = (total_slot_ms / query_count / 1000) if query_count > 0 else 0
        slot_hours_total = total_slot_ms / 3600000.0

        # Frequency factor: log2(N+1), capped at 5
        freq_factor = min(math.log2(query_count + 1), 5) if query_count > 0 else 0

        metrics = {
            'query_cost': cost_7d,
            'query_count': query_count,
            'slot_hours': slot_hours_total,
            'gb_per_query': gb_per_query,
            'references': query_freq.get('references', 0),
        }

        # Get model config from manifest
        config = {}
        materialized = 'table'
        has_model_config = False  # Track if we found model in manifest

        if model_name:
            model = parser.get_model(model_name)
            if model:
                config = model.get('config', {})
                materialized = config.get('materialized', 'table')
                has_model_config = True

        # Skip models not in manifest (deleted) or disabled
        if not has_model_config:
            return 0, [], metrics

        is_enabled = config.get('enabled', True)
        if not is_enabled:
            return 0, [], metrics

        # Skip views
        if materialized == 'view':
            return 0, [], metrics

        partition_by = config.get('partition_by')
        cluster_by = config.get('cluster_by', [])
        is_incremental = materialized == 'incremental'

        # Add model config to metrics for output
        metrics['is_incremental'] = is_incremental
        metrics['is_partitioned'] = bool(partition_by)
        metrics['is_clustered'] = bool(cluster_by)

        # Skip datasets we can't optimize (external ingestion tools)
        is_unoptimizable = dataset.startswith(('raw_', 'airbyte_', 'job_logs'))

        # Can suggest structure changes only if:
        # - Model found in manifest (has_model_config)
        # - Not incremental (already optimized by definition)
        # - Not unoptimizable (raw/airbyte tables)
        can_suggest_structure = has_model_config and not is_incremental and not is_unoptimizable

        # === 1. QUERY COST — direct spend in cents ===
        if cost_7d > 0:
            pts = round(cost_7d * 100)  # €0.01 = 1pt
            pts = max(pts, 1)
            score += pts
            scoring_details.append({
                'criterion': 'query_cost',
                'points': pts,
                'value': f"€{cost_7d:.2f}/week",
                'recommendation': f'Query cost €{cost_7d:.2f}/week',
            })

        # === 2. HIGH SLOT USAGE — SQL complexity ===
        if slot_sec_per_query > 30 and query_count > 0:
            if slot_sec_per_query > 600:
                base = 15  # >10min/query
            elif slot_sec_per_query > 120:
                base = 8   # >2min/query
            else:
                base = 3   # >30sec/query

            pts = round(base * freq_factor)
            if pts > 0:
                score += pts
                slot_min = slot_sec_per_query / 60
                scoring_details.append({
                    'criterion': 'high_slot',
                    'points': pts,
                    'value': f"{slot_min:.1f}min/query × {query_count}/week",
                    'recommendation': 'High compute — optimize SQL complexity',
                })

        # === 3. NO PARTITION — structural optimization ===
        # Only suggest if: model in manifest, not incremental, not raw/airbyte, no partition_by yet
        if not partition_by and total_gb > 1 and query_count > 5 and can_suggest_structure:
            if total_gb > 100:
                base = 15  # Huge table
            elif total_gb > 10:
                base = 8   # Large table
            else:
                base = 3   # Medium table

            pts = round(base * freq_factor)
            if pts > 0:
                score += pts
                scoring_details.append({
                    'criterion': 'no_partition',
                    'points': pts,
                    'value': f"{total_gb:.0f}GB × {query_count}/week",
                    'recommendation': 'Add partition_by config',
                })

        # === 4. NO CLUSTERING — scan optimization ===
        # Only suggest if: model in manifest, not incremental, not raw/airbyte, no cluster_by yet
        if not cluster_by and total_gb > 1 and query_count > 5 and can_suggest_structure:
            if total_gb > 100:
                base = 10
            elif total_gb > 10:
                base = 5
            else:
                base = 2

            pts = round(base * freq_factor)
            if pts > 0:
                score += pts
                scoring_details.append({
                    'criterion': 'no_clustering',
                    'points': pts,
                    'value': f"{total_gb:.0f}GB × {query_count}/week",
                    'recommendation': 'Add cluster_by config',
                })

        # === 5. UNUSED TABLE — storage waste ===
        days_unused = unused_info.get('days_unused', 0)
        if days_unused > 30:
            monthly_storage_cost = total_gb * 0.02  # €0.02/GB/month
            pts = round(monthly_storage_cost * 100)  # in cents
            if pts > 0:
                score += pts
                scoring_details.append({
                    'criterion': 'unused',
                    'points': pts,
                    'value': f"{days_unused}d unused, {total_gb:.0f}GB",
                    'recommendation': f'Unused {days_unused}d — €{monthly_storage_cost:.2f}/month storage',
                })

        # === 6. INEFFECTIVE PARTITION — partition pruning not working ===
        # Table has partition but queries scan most of the table (>50%)
        # This often indicates misconfigured incremental update
        if partition_by and total_gb > 1 and query_count > 5 and gb_per_query > 0:
            scan_ratio = gb_per_query / total_gb if total_gb > 0 else 0
            if scan_ratio > 0.5:  # scanning more than 50% of table per query
                # High frequency makes this worse
                base = 10 if scan_ratio > 0.8 else 5
                pts = round(base * freq_factor)
                if pts > 0:
                    score += pts
                    rec = 'Check incremental config' if is_incremental else 'Check WHERE partition filter'
                    scoring_details.append({
                        'criterion': 'ineffective_partition',
                        'points': pts,
                        'value': f"{scan_ratio:.0%} scanned/query ({gb_per_query:.1f}/{total_gb:.0f}GB)",
                        'recommendation': f'Partition pruning ineffective — {rec}',
                    })

        return score, scoring_details, metrics

