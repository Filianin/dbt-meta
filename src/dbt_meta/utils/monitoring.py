"""Monitoring utilities for dbt-meta optimization commands.

Provides functions to query dbt_bigquery_monitoring tables for
storage, partition, and usage statistics.
"""

import json
import os
import shutil
import subprocess
from typing import Any, Optional


def run_monitoring_query(sql: str, timeout: int = 30) -> Optional[list[dict[str, Any]]]:
    """Execute BigQuery query and return results as list of dicts.

    Args:
        sql: SQL query to execute
        timeout: Command timeout in seconds

    Returns:
        List of result rows as dictionaries, or None on error
    """
    bq_cmd = shutil.which('bq')
    if not bq_cmd:
        bq_cmd = '/opt/homebrew/bin/bq'
        if not os.path.exists(bq_cmd):
            return None

    try:
        result = subprocess.run(
            [bq_cmd, 'query', '--use_legacy_sql=false', '--format=json', '--max_rows=5000', sql],
            capture_output=True,
            text=True,
            timeout=timeout
        )

        if result.returncode != 0:
            return None

        output = result.stdout.strip()
        if not output or output == '[]':
            return []

        rows: list[dict[str, Any]] = json.loads(output)
        return rows

    except (subprocess.TimeoutExpired, json.JSONDecodeError, FileNotFoundError):
        return None


def fetch_storage_metrics(dataset: str, table: str, monitoring_dataset: str = "prod") -> Optional[dict[str, Any]]:
    """Fetch storage metrics from prod.storage_with_cost.

    Args:
        dataset: Dataset name (e.g., 'core_client')
        table: Table name (e.g., 'events')

    Returns:
        Dictionary with storage metrics or None if not found
    """
    query = f"""
    SELECT
        ROUND(total_logical_bytes / POW(1024, 3), 2) as total_gb,
        ROUND(active_logical_bytes / POW(1024, 3), 2) as active_gb,
        ROUND(long_term_logical_bytes / POW(1024, 3), 2) as long_term_gb,
        total_partitions as partition_count,
        total_rows as row_count,
        ROUND(cost_monthly_forecast, 2) as cost_monthly_eur,
        ROUND(potential_savings, 2) as potential_savings_usd,
        optimal_storage_billing_model
    FROM `{monitoring_dataset}.storage_with_cost`
    WHERE dataset_id = '{dataset}'
      AND table_id = '{table}'
    """

    results = run_monitoring_query(query)
    if results and len(results) > 0:
        row = results[0]
        return {
            'total_gb': float(row.get('total_gb', 0) or 0),
            'active_gb': float(row.get('active_gb', 0) or 0),
            'long_term_gb': float(row.get('long_term_gb', 0) or 0),
            'partition_count': int(row.get('partition_count', 0) or 0),
            'row_count': int(row.get('row_count', 0) or 0),
            'cost_monthly_eur': float(row.get('cost_monthly_usd', 0) or 0),
            'potential_savings_eur': float(row.get('potential_savings_usd', 0) or 0),
            'optimal_billing_model': row.get('optimal_storage_billing_model', 'LOGICAL'),
        }
    return None


def fetch_partition_stats(dataset: str, table: str, monitoring_dataset: str = "prod") -> Optional[dict[str, Any]]:
    """Fetch partition statistics from prod.partitions_monitoring.

    Args:
        dataset: Dataset name
        table: Table name

    Returns:
        Dictionary with partition stats or None if not found
    """
    query = f"""
    SELECT
        partition_type,
        partition_count,
        CAST(earliest_partition_time AS STRING) as oldest_partition,
        CAST(latest_partition_time AS STRING) as newest_partition,
        partition_expiration_days,
        ROUND(sum_total_logical_bytes / POW(1024, 3), 2) as total_gb
    FROM `{monitoring_dataset}.partitions_monitoring`
    WHERE project_id || '.' || dataset_id || '.' || table_id
          LIKE '%{dataset}.{table}'
    """

    results = run_monitoring_query(query)
    if results and len(results) > 0:
        row = results[0]
        return {
            'type': row.get('partition_type'),
            'count': int(row.get('partition_count', 0) or 0),
            'oldest': row.get('oldest_partition'),
            'newest': row.get('newest_partition'),
            'expiration_days': int(row.get('partition_expiration_days') or 0) if row.get('partition_expiration_days') else None,
            'total_gb': float(row.get('total_gb', 0) or 0),
        }
    return None


def fetch_usage_stats(dataset: str, table: str, days: int = 30, monitoring_dataset: str = "prod") -> Optional[dict[str, Any]]:
    """Fetch query usage statistics from prod.table_reference_incremental.

    Args:
        dataset: Dataset name
        table: Table name
        days: Number of days to look back (default: 30)

    Returns:
        Dictionary with usage stats or None if not found
    """
    query = f"""
    SELECT
        COUNT(*) as query_count,
        SUM(reference_count) as total_references
    FROM `{monitoring_dataset}.table_reference_incremental`
    WHERE dataset_id = '{dataset}'
      AND table_id = '{table}'
      AND day >= DATE_SUB(CURRENT_DATE(), INTERVAL {days} DAY)
    """

    results = run_monitoring_query(query)
    if results and len(results) > 0:
        row = results[0]
        return {
            'query_count': int(row.get('query_count', 0) or 0),
            'total_references': int(row.get('total_references', 0) or 0),
            'period_days': days,
        }
    return None


def fetch_partition_details(dataset: str, table: str, monitoring_dataset: str = "prod") -> Optional[list[dict[str, Any]]]:
    """Fetch detailed partition information from INFORMATION_SCHEMA.

    Args:
        dataset: Dataset name
        table: Table name

    Returns:
        List of partition details or None if not found
    """
    query = f"""
    SELECT
        partition_id,
        total_rows,
        ROUND(total_logical_bytes / POW(1024, 3), 4) as size_gb,
        CAST(last_modified_time AS STRING) as last_modified,
        storage_tier
    FROM `region-EU`.INFORMATION_SCHEMA.PARTITIONS
    WHERE table_schema = '{dataset}'
      AND table_name = '{table}'
    ORDER BY partition_id DESC
    LIMIT 100
    """

    return run_monitoring_query(query, timeout=60)


def fetch_column_clustering_info(dataset: str, table: str, monitoring_dataset: str = "prod") -> Optional[dict[str, Any]]:
    """Fetch partitioning and clustering column info from INFORMATION_SCHEMA.

    Args:
        dataset: Dataset name
        table: Table name

    Returns:
        Dictionary with partition and cluster column info
    """
    query = f"""
    SELECT
        column_name,
        data_type,
        is_partitioning_column,
        clustering_ordinal_position
    FROM `region-EU`.INFORMATION_SCHEMA.COLUMNS
    WHERE table_schema = '{dataset}'
      AND table_name = '{table}'
      AND (is_partitioning_column = 'YES' OR clustering_ordinal_position IS NOT NULL)
    ORDER BY clustering_ordinal_position NULLS LAST
    """

    results = run_monitoring_query(query, timeout=60)
    if results:
        partition_col = None
        cluster_cols = []

        for row in results:
            if row.get('is_partitioning_column') == 'YES':
                partition_col = {
                    'name': row['column_name'],
                    'type': row['data_type'],
                }
            if row.get('clustering_ordinal_position'):
                cluster_cols.append({
                    'name': row['column_name'],
                    'type': row['data_type'],
                    'position': int(row['clustering_ordinal_position']),
                })

        return {
            'partition_column': partition_col,
            'cluster_columns': sorted(cluster_cols, key=lambda x: x['position']),
        }
    return None


def fetch_all_tables_storage(min_gb: float = 1.0, monitoring_dataset: str = "prod") -> Optional[list[dict[str, Any]]]:
    """Fetch storage metrics for all tables above size threshold.

    Used for hotspots analysis.

    Args:
        min_gb: Minimum table size in GB (default: 1.0)
        monitoring_dataset: Dataset containing dbt_bigquery_monitoring tables (default: prod)

    Returns:
        List of tables with storage metrics
    """
    query = f"""
    SELECT
        dataset_id,
        table_id,
        ROUND(total_logical_bytes / POW(1024, 3), 2) as total_gb,
        ROUND(active_logical_bytes / POW(1024, 3), 2) as active_gb,
        total_partitions as partition_count,
        total_rows as row_count,
        ROUND(cost_monthly_forecast, 2) as cost_monthly_eur
    FROM `{monitoring_dataset}.storage_with_cost`
    WHERE total_logical_bytes / POW(1024, 3) >= {min_gb}
    ORDER BY total_logical_bytes DESC
    """

    return run_monitoring_query(query, timeout=120)


def fetch_table_query_frequency(days: int = 30, monitoring_dataset: str = "prod") -> Optional[list[dict[str, Any]]]:
    """Fetch query frequency for all tables.

    Used for hotspots analysis to identify high-usage tables.

    Args:
        days: Number of days to look back

    Returns:
        List of tables with query counts
    """
    query = f"""
    SELECT
        dataset_id,
        table_id,
        COUNT(DISTINCT day) as active_days,
        SUM(reference_count) as total_references
    FROM `{monitoring_dataset}.table_reference_incremental`
    WHERE day >= DATE_SUB(CURRENT_DATE(), INTERVAL {days} DAY)
    GROUP BY dataset_id, table_id
    HAVING total_references > 10
    ORDER BY total_references DESC
    """

    return run_monitoring_query(query, timeout=120)


def fetch_model_query_costs(monitoring_dataset: str = "prod") -> Optional[list[dict[str, Any]]]:
    """Fetch query costs per dbt model from models_costs_incremental.

    Aggregates 7-day query cost data for ALL models (not just top N).
    Uses raw table instead of most_expensive_models view which may be limited.

    Returns:
        List of models with query cost metrics including:
        - query_cost_usd: total cost in USD
        - query_count: number of queries
        - total_slot_ms: raw slot milliseconds (not converted)
        - bytes_processed: total bytes scanned
        - cache_hit_ratio: cache hit percentage
    """
    query = f"""
    SELECT
        dbt_model_name,
        ROUND(SUM(total_query_cost), 4) as query_cost_usd,
        SUM(query_count) as query_count,
        SUM(total_slot_ms) as total_slot_ms,
        SUM(total_bytes_processed) as bytes_processed,
        SAFE_DIVIDE(SUM(cache_hit), SUM(query_count)) as cache_hit_ratio
    FROM `{monitoring_dataset}.models_costs_incremental`
    WHERE hour >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)
    GROUP BY dbt_model_name
    ORDER BY query_cost_usd DESC
    """

    return run_monitoring_query(query, timeout=60)


def fetch_tables_with_savings(min_savings: float = 0.0, monitoring_dataset: str = "prod") -> Optional[list[dict[str, Any]]]:
    """Fetch tables with potential storage savings.

    Args:
        min_savings: Minimum savings in USD (default: 0, show all)

    Returns:
        List of tables with savings potential
    """
    query = f"""
    SELECT
        dataset_id,
        table_id,
        ROUND(total_logical_tb * 1024, 2) as total_gb,
        optimal_storage_billing_model,
        ROUND(potential_savings, 2) as potential_savings_usd
    FROM `{monitoring_dataset}.table_with_potential_savings`
    WHERE potential_savings >= {min_savings}
    ORDER BY potential_savings DESC
    """

    return run_monitoring_query(query, timeout=60)


def fetch_dataset_billing_recommendations(monitoring_dataset: str = "prod") -> Optional[list[dict[str, Any]]]:
    """Fetch aggregated billing recommendations per dataset with net impact.

    Calculates NET savings for switching entire dataset to optimal billing,
    comparing LOGICAL vs PHYSICAL costs for all tables combined.

    Billing model changes apply to entire datasets via:
    ALTER SCHEMA `dataset` SET OPTIONS(storage_billing_model='PHYSICAL')

    Returns:
        List of datasets with billing recommendations:
        - dataset_id: Dataset name
        - tables_recommend_physical: Count of tables that benefit from PHYSICAL
        - total_tables: Total tables in dataset
        - net_savings_eur: Net monthly savings (positive = switch recommended)
        - recommended_billing: PHYSICAL or LOGICAL (whichever is cheaper)
    """
    query = f"""
    SELECT
        dataset_id,
        COUNTIF(optimal_storage_billing_model = 'PHYSICAL') as tables_recommend_physical,
        COUNT(*) as total_tables,
        ROUND(SUM(cost_monthly_forecast) - LEAST(
            SUM(logical_cost_monthly_forecast),
            SUM(physical_cost_monthly_forecast)
        ), 2) as net_savings_eur,
        IF(SUM(physical_cost_monthly_forecast) < SUM(logical_cost_monthly_forecast),
           'PHYSICAL', 'LOGICAL') as recommended_billing
    FROM `{monitoring_dataset}.storage_with_cost`
    WHERE dataset_id IS NOT NULL
    GROUP BY dataset_id
    HAVING net_savings_eur > 0.1
    ORDER BY net_savings_eur DESC
    """

    return run_monitoring_query(query, timeout=60)


def fetch_total_bigquery_costs(days: int = 7, monitoring_dataset: str = "prod") -> Optional[dict[str, Any]]:
    """Fetch total BigQuery costs from all jobs (not just dbt).

    Queries jobs_costs_incremental for aggregate costs across all
    BigQuery usage including ad-hoc queries, BI tools, etc.

    Args:
        days: Number of days to look back (default: 7)

    Returns:
        Dict with total_cost, total_slot_hours, total_queries
        or None on error
    """
    query = f"""
    SELECT
        ROUND(SUM(total_query_cost), 2) as total_cost,
        ROUND(SUM(total_slot_ms) / 3600000, 1) as total_slot_hours,
        SUM(query_count) as total_queries
    FROM prod.jobs_costs_incremental
    WHERE hour >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {days} DAY)
    """

    result = run_monitoring_query(query, timeout=30)
    if result and len(result) > 0:
        row = result[0]
        return {
            'total_cost': float(row.get('total_cost', 0) or 0),
            'total_slot_hours': float(row.get('total_slot_hours', 0) or 0),
            'total_queries': int(row.get('total_queries', 0) or 0),
        }
    return None


def fetch_partition_info_all(monitoring_dataset: str = "prod") -> Optional[list[dict[str, Any]]]:
    """Fetch partition info for all partitioned tables.

    Returns:
        List of tables with partition configuration
    """
    query = f"""
    SELECT
        dataset_id,
        table_id,
        partition_type,
        partition_count,
        partition_expiration_days,
        ROUND(sum_total_logical_bytes / POW(1024, 3), 2) as total_gb
    FROM `{monitoring_dataset}.partitions_monitoring`
    ORDER BY sum_total_logical_bytes DESC
    """

    return run_monitoring_query(query, timeout=120)


def fetch_read_heavy_tables(monitoring_dataset: str = "prod") -> Optional[list[dict[str, Any]]]:
    """Fetch tables with high read frequency.

    Returns:
        List of heavily read tables with storage info
    """
    query = f"""
    SELECT
        dataset_id,
        table_id,
        reference_count,
        ROUND(total_logical_bytes / POW(1024, 3), 2) as total_gb,
        total_partitions
    FROM `{monitoring_dataset}.read_heavy_tables`
    WHERE reference_count > 0
    ORDER BY reference_count DESC
    """

    return run_monitoring_query(query, timeout=60)


def fetch_unused_tables(days_threshold: int = 30, monitoring_dataset: str = "prod") -> Optional[list[dict[str, Any]]]:
    """Fetch tables not used in the last N days.

    Args:
        days_threshold: Days since last use to consider unused (default: 30)

    Returns:
        List of unused tables with last_used_date
    """
    query = f"""
    SELECT
        dataset_id,
        table_id,
        CAST(last_used_date AS STRING) as last_used_date,
        DATE_DIFF(CURRENT_DATE(), last_used_date, DAY) as days_unused
    FROM `{monitoring_dataset}.unused_tables`
    WHERE DATE_DIFF(CURRENT_DATE(), last_used_date, DAY) >= {days_threshold}
    ORDER BY days_unused DESC
    """

    return run_monitoring_query(query, timeout=60)


def fetch_model_metrics(monitoring_dataset: str = "prod") -> Optional[list[dict[str, Any]]]:
    """Fetch model execution metrics from models_costs_incremental.

    Returns 7-day aggregated metrics: failed_runs, p90_duration, avg_duration.

    Returns:
        List of models with execution metrics
    """
    query = f"""
    SELECT
        dbt_model_name,
        SUM(failed_runs) as failed_runs,
        MAX(p90_duration_seconds) as p90_duration_seconds,
        ROUND(AVG(avg_duration_seconds), 2) as avg_duration_seconds,
        SUM(query_count) as query_count
    FROM `{monitoring_dataset}.models_costs_incremental`
    WHERE hour >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)
    GROUP BY dbt_model_name
    """

    return run_monitoring_query(query, timeout=60)


def fetch_downstream_filter_patterns(model_sql: str) -> list[str]:
    """Extract filter columns from compiled SQL.

    Simple regex-based extraction of columns used in WHERE clauses.

    Args:
        model_sql: Compiled SQL of the model

    Returns:
        List of column names found in WHERE clauses
    """
    import re

    # Find WHERE clause
    where_match = re.search(r'\bWHERE\b(.+?)(?:\bGROUP BY\b|\bORDER BY\b|\bLIMIT\b|\bHAVING\b|$)',
                            model_sql, re.IGNORECASE | re.DOTALL)
    if not where_match:
        return []

    where_clause = where_match.group(1)

    # Extract column names (simplified - looks for word.word or just word before operators)
    # Pattern: table_alias.column_name or just column_name before =, <, >, IN, LIKE, BETWEEN
    pattern = r'(?:[\w]+\.)?(\w+)\s*(?:=|<|>|!=|<>|IN\s*\(|LIKE|BETWEEN|IS\s+NULL|IS\s+NOT\s+NULL)'
    matches = re.findall(pattern, where_clause, re.IGNORECASE)

    # Deduplicate and filter out common keywords
    keywords = {'AND', 'OR', 'NOT', 'NULL', 'TRUE', 'FALSE', 'CASE', 'WHEN', 'THEN', 'ELSE', 'END'}
    columns = list(dict.fromkeys(col for col in matches if col.upper() not in keywords))

    return columns
