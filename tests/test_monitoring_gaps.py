"""Tests covering gap branches in utils/monitoring.py.

Targets:
- lines 117-118: fetch_partition_stats populated-result path
- lines 152-153: fetch_usage_stats populated-result path
- lines 171-185: fetch_partition_details
- lines 213-229: fetch_column_clustering_info processing results
- lines 332-344: fetch_tables_with_savings
"""

from __future__ import annotations

from unittest.mock import patch

from dbt_meta.utils.monitoring import (
    fetch_column_clustering_info,
    fetch_partition_details,
    fetch_partition_stats,
    fetch_tables_with_savings,
    fetch_usage_stats,
)


class TestFetchPartitionStats:
    @patch('dbt_meta.utils.monitoring.run_monitoring_query')
    def test_returns_dict_when_rows_present(self, mock_query):
        mock_query.return_value = [{
            'partition_type': 'DAY',
            'partition_count': '42',
            'oldest_partition': '2024-01-01',
            'newest_partition': '2026-03-01',
            'partition_expiration_days': '90',
            'total_gb': '12.5',
        }]

        result = fetch_partition_stats('core', 'events')

        assert result is not None
        assert result['type'] == 'DAY'
        assert result['count'] == 42
        assert result['total_gb'] == 12.5
        assert result['expiration_days'] == 90

    @patch('dbt_meta.utils.monitoring.run_monitoring_query')
    def test_returns_none_when_no_rows(self, mock_query):
        mock_query.return_value = []
        assert fetch_partition_stats('core', 'events') is None

    @patch('dbt_meta.utils.monitoring.run_monitoring_query')
    def test_returns_none_when_query_fails(self, mock_query):
        mock_query.return_value = None
        assert fetch_partition_stats('core', 'events') is None

    @patch('dbt_meta.utils.monitoring.run_monitoring_query')
    def test_handles_null_expiration_days(self, mock_query):
        mock_query.return_value = [{
            'partition_type': 'DAY',
            'partition_count': '1',
            'oldest_partition': None,
            'newest_partition': None,
            'partition_expiration_days': None,
            'total_gb': '0',
        }]

        result = fetch_partition_stats('core', 'events')

        assert result['expiration_days'] is None


class TestFetchUsageStats:
    @patch('dbt_meta.utils.monitoring.run_monitoring_query')
    def test_returns_dict_when_rows_present(self, mock_query):
        mock_query.return_value = [{'query_count': '123', 'total_references': '4567'}]

        result = fetch_usage_stats('core', 'events', days=7)

        assert result is not None
        assert result['query_count'] == 123
        assert result['total_references'] == 4567
        assert result['period_days'] == 7

    @patch('dbt_meta.utils.monitoring.run_monitoring_query')
    def test_returns_none_when_no_results(self, mock_query):
        mock_query.return_value = []
        assert fetch_usage_stats('core', 'events') is None


class TestFetchPartitionDetails:
    @patch('dbt_meta.utils.monitoring.run_monitoring_query')
    def test_returns_query_result_unchanged(self, mock_query):
        fake = [
            {
                'partition_id': '20260101',
                'total_rows': '1000',
                'size_gb': '0.5',
                'last_modified': '2026-01-01',
                'storage_tier': 'ACTIVE',
            }
        ]
        mock_query.return_value = fake

        result = fetch_partition_details('core', 'events')

        assert result == fake
        # verify timeout argument was used
        _, kwargs = mock_query.call_args
        assert kwargs.get('timeout') == 60

    @patch('dbt_meta.utils.monitoring.run_monitoring_query')
    def test_returns_none_when_query_fails(self, mock_query):
        mock_query.return_value = None
        assert fetch_partition_details('core', 'events') is None


class TestFetchColumnClusteringInfo:
    @patch('dbt_meta.utils.monitoring.run_monitoring_query')
    def test_extracts_partition_and_cluster_columns(self, mock_query):
        mock_query.return_value = [
            {
                'column_name': 'event_date',
                'data_type': 'DATE',
                'is_partitioning_column': 'YES',
                'clustering_ordinal_position': None,
            },
            {
                'column_name': 'client_id',
                'data_type': 'INT64',
                'is_partitioning_column': 'NO',
                'clustering_ordinal_position': '1',
            },
            {
                'column_name': 'country',
                'data_type': 'STRING',
                'is_partitioning_column': 'NO',
                'clustering_ordinal_position': '2',
            },
        ]

        result = fetch_column_clustering_info('core', 'events')

        assert result is not None
        assert result['partition_column']['name'] == 'event_date'
        assert result['partition_column']['type'] == 'DATE'
        assert len(result['cluster_columns']) == 2
        # Should be sorted by position
        assert result['cluster_columns'][0]['name'] == 'client_id'
        assert result['cluster_columns'][0]['position'] == 1
        assert result['cluster_columns'][1]['name'] == 'country'
        assert result['cluster_columns'][1]['position'] == 2

    @patch('dbt_meta.utils.monitoring.run_monitoring_query')
    def test_handles_no_partition_column(self, mock_query):
        mock_query.return_value = [
            {
                'column_name': 'id',
                'data_type': 'INT64',
                'is_partitioning_column': 'NO',
                'clustering_ordinal_position': '1',
            },
        ]

        result = fetch_column_clustering_info('core', 'events')

        assert result['partition_column'] is None
        assert len(result['cluster_columns']) == 1

    @patch('dbt_meta.utils.monitoring.run_monitoring_query')
    def test_returns_none_when_no_results(self, mock_query):
        mock_query.return_value = None
        assert fetch_column_clustering_info('core', 'events') is None

    @patch('dbt_meta.utils.monitoring.run_monitoring_query')
    def test_returns_none_when_empty_list(self, mock_query):
        mock_query.return_value = []
        assert fetch_column_clustering_info('core', 'events') is None


class TestFetchTablesWithSavings:
    @patch('dbt_meta.utils.monitoring.run_monitoring_query')
    def test_returns_query_result(self, mock_query):
        fake = [
            {
                'dataset_id': 'core',
                'table_id': 'big_table',
                'total_gb': '250.5',
                'optimal_storage_billing_model': 'PHYSICAL',
                'potential_savings_usd': '12.34',
            },
        ]
        mock_query.return_value = fake

        result = fetch_tables_with_savings(min_savings=5.0)

        assert result == fake

    @patch('dbt_meta.utils.monitoring.run_monitoring_query')
    def test_default_min_savings(self, mock_query):
        mock_query.return_value = []

        fetch_tables_with_savings()

        # Query should be called once with default min_savings=0.0
        assert mock_query.call_count == 1
        call_args = mock_query.call_args
        sql = call_args.args[0] if call_args.args else call_args.kwargs.get('sql', '')
        assert 'potential_savings >= 0.0' in sql

    @patch('dbt_meta.utils.monitoring.run_monitoring_query')
    def test_none_returned_on_failure(self, mock_query):
        mock_query.return_value = None
        assert fetch_tables_with_savings() is None
