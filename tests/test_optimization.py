"""Tests for optimization commands (analyze, hotspots, branch)."""

import pytest
from unittest.mock import patch, MagicMock

from dbt_meta.command_impl.analyze import AnalyzeCommand
from dbt_meta.command_impl.branch import BranchCommand
from dbt_meta.command_impl.hotspots import HotspotsCommand
from dbt_meta.config import Config
from tests.helpers_cmd import analyze, branch, hotspots


class TestAnalyzeCommand:
    """Tests for analyze command."""

    def test_analyze_returns_dict(self, prod_manifest, test_model):
        """Test analyze returns dict with expected keys."""
        result = analyze(prod_manifest, test_model)

        assert result is not None
        assert isinstance(result, dict)
        assert 'model' in result
        assert 'table' in result
        assert 'config' in result
        assert 'recommendations' in result

    def test_analyze_not_found_returns_none(self, prod_manifest):
        """Test analyze returns None for non-existent model."""
        result = analyze(prod_manifest, 'nonexistent_model_xyz')
        assert result is None

    def test_analyze_config_structure(self, prod_manifest, test_model):
        """Test config section has expected fields."""
        result = analyze(prod_manifest, test_model)

        assert result is not None
        config = result.get('config', {})
        assert 'partition_by' in config
        assert 'cluster_by' in config
        assert 'materialized' in config

    def test_analyze_recommendations_is_list(self, prod_manifest, test_model):
        """Test recommendations is always a list."""
        result = analyze(prod_manifest, test_model)

        assert result is not None
        assert isinstance(result.get('recommendations', []), list)

    @patch('dbt_meta.command_impl.analyze.fetch_storage_metrics')
    def test_analyze_handles_missing_storage(self, mock_storage, prod_manifest, test_model):
        """Test analyze handles missing storage data gracefully."""
        mock_storage.return_value = None

        result = analyze(prod_manifest, test_model)

        assert result is not None
        assert result.get('storage') is None

    @patch('dbt_meta.command_impl.analyze.fetch_partition_stats')
    def test_analyze_handles_missing_partitions(self, mock_partitions, prod_manifest, test_model):
        """Test analyze handles missing partition data gracefully."""
        mock_partitions.return_value = None

        result = analyze(prod_manifest, test_model)

        assert result is not None
        assert result.get('partitions') is None


class TestHotspotsCommand:
    """Tests for hotspots command."""

    @patch('dbt_meta.command_impl.hotspots.fetch_total_bigquery_costs')
    @patch('dbt_meta.command_impl.hotspots.fetch_unused_tables')
    @patch('dbt_meta.command_impl.hotspots.fetch_model_metrics')
    @patch('dbt_meta.command_impl.hotspots.fetch_dataset_billing_recommendations')
    @patch('dbt_meta.command_impl.hotspots.fetch_read_heavy_tables')
    @patch('dbt_meta.command_impl.hotspots.fetch_partition_info_all')
    @patch('dbt_meta.command_impl.hotspots.fetch_model_query_costs')
    @patch('dbt_meta.command_impl.hotspots.fetch_all_tables_storage')
    @patch('dbt_meta.command_impl.hotspots.fetch_table_query_frequency')
    def test_hotspots_returns_dict(
        self, mock_freq, mock_storage, mock_costs, mock_parts, mock_heavy,
        mock_billing, mock_metrics, mock_unused, mock_total_bq, prod_manifest
    ):
        """Test hotspots returns dict with expected keys."""
        mock_storage.return_value = [
            {'dataset_id': 'test', 'table_id': 'table1', 'total_gb': 50.0, 'partition_count': 0}
        ]
        mock_freq.return_value = []
        mock_costs.return_value = []
        mock_parts.return_value = []
        mock_heavy.return_value = []
        mock_billing.return_value = []
        mock_metrics.return_value = []
        mock_unused.return_value = []
        mock_total_bq.return_value = {'total_cost': 100.0, 'total_slot_hours': 500.0, 'total_queries': 10000}

        result = hotspots(prod_manifest)

        assert result is not None
        assert isinstance(result, dict)
        assert 'hotspots' in result
        assert 'summary' in result

    @patch('dbt_meta.command_impl.hotspots.fetch_total_bigquery_costs')
    @patch('dbt_meta.command_impl.hotspots.fetch_unused_tables')
    @patch('dbt_meta.command_impl.hotspots.fetch_model_metrics')
    @patch('dbt_meta.command_impl.hotspots.fetch_dataset_billing_recommendations')
    @patch('dbt_meta.command_impl.hotspots.fetch_read_heavy_tables')
    @patch('dbt_meta.command_impl.hotspots.fetch_partition_info_all')
    @patch('dbt_meta.command_impl.hotspots.fetch_model_query_costs')
    @patch('dbt_meta.command_impl.hotspots.fetch_all_tables_storage')
    @patch('dbt_meta.command_impl.hotspots.fetch_table_query_frequency')
    def test_hotspots_empty_when_no_data(
        self, mock_freq, mock_storage, mock_costs, mock_parts, mock_heavy,
        mock_billing, mock_metrics, mock_unused, mock_total_bq, prod_manifest
    ):
        """Test hotspots returns empty list when no data."""
        mock_storage.return_value = []
        mock_freq.return_value = []
        mock_costs.return_value = []
        mock_parts.return_value = []
        mock_heavy.return_value = []
        mock_billing.return_value = []
        mock_metrics.return_value = []
        mock_unused.return_value = []
        mock_total_bq.return_value = None

        result = hotspots(prod_manifest)

        assert result is not None
        assert result['hotspots'] == []
        assert result['summary']['total_tables_analyzed'] == 0

    @patch('dbt_meta.command_impl.hotspots.fetch_total_bigquery_costs')
    @patch('dbt_meta.command_impl.hotspots.fetch_unused_tables')
    @patch('dbt_meta.command_impl.hotspots.fetch_model_metrics')
    @patch('dbt_meta.command_impl.hotspots.fetch_dataset_billing_recommendations')
    @patch('dbt_meta.command_impl.hotspots.fetch_read_heavy_tables')
    @patch('dbt_meta.command_impl.hotspots.fetch_partition_info_all')
    @patch('dbt_meta.command_impl.hotspots.fetch_model_query_costs')
    @patch('dbt_meta.command_impl.hotspots.fetch_all_tables_storage')
    @patch('dbt_meta.command_impl.hotspots.fetch_table_query_frequency')
    def test_hotspots_limit_parameter(
        self, mock_freq, mock_storage, mock_costs, mock_parts, mock_heavy,
        mock_billing, mock_metrics, mock_unused, mock_total_bq, prod_manifest
    ):
        """Test hotspots respects limit parameter."""
        mock_storage.return_value = [
            {'dataset_id': 'test', 'table_id': f'table{i}', 'total_gb': 100.0, 'partition_count': 0}
            for i in range(10)
        ]
        mock_freq.return_value = []
        mock_costs.return_value = []
        mock_parts.return_value = []
        mock_heavy.return_value = []
        mock_billing.return_value = []
        mock_metrics.return_value = []
        mock_unused.return_value = []
        mock_total_bq.return_value = None

        result = hotspots(prod_manifest, limit=3)

        assert result is not None
        assert len(result['hotspots']) <= 3

    @patch('dbt_meta.command_impl.hotspots.fetch_total_bigquery_costs')
    @patch('dbt_meta.command_impl.hotspots.fetch_unused_tables')
    @patch('dbt_meta.command_impl.hotspots.fetch_model_metrics')
    @patch('dbt_meta.command_impl.hotspots.fetch_dataset_billing_recommendations')
    @patch('dbt_meta.command_impl.hotspots.fetch_read_heavy_tables')
    @patch('dbt_meta.command_impl.hotspots.fetch_partition_info_all')
    @patch('dbt_meta.command_impl.hotspots.fetch_model_query_costs')
    @patch('dbt_meta.command_impl.hotspots.fetch_all_tables_storage')
    @patch('dbt_meta.command_impl.hotspots.fetch_table_query_frequency')
    def test_hotspots_scoring(
        self, mock_freq, mock_storage, mock_costs, mock_parts, mock_heavy,
        mock_billing, mock_metrics, mock_unused, mock_total_bq, prod_manifest
    ):
        """Test hotspots assigns scores to tables."""
        mock_storage.return_value = [
            {'dataset_id': 'test', 'table_id': 'big_table', 'total_gb': 150.0, 'partition_count': 0}
        ]
        mock_freq.return_value = [
            {'dataset_id': 'test', 'table_id': 'big_table', 'active_days': 7, 'total_references': 1000}
        ]
        mock_costs.return_value = []
        mock_parts.return_value = []
        mock_heavy.return_value = []
        mock_billing.return_value = []
        mock_metrics.return_value = []
        mock_unused.return_value = []
        mock_total_bq.return_value = None

        result = hotspots(prod_manifest)

        assert result is not None
        if result['hotspots']:
            assert 'score' in result['hotspots'][0]
            assert result['hotspots'][0]['score'] > 0


class TestBranchCommand:
    """Tests for branch command."""

    def test_branch_returns_dict(self, prod_manifest, test_model):
        """Test branch returns dict with expected keys."""
        result = branch(prod_manifest, test_model)

        assert result is not None
        assert isinstance(result, dict)
        assert 'root' in result
        assert 'root_config' in result
        assert 'upstream' in result
        assert 'downstream' in result
        assert 'recommendations' in result

    def test_branch_not_found_returns_none(self, prod_manifest):
        """Test branch returns None for non-existent model."""
        result = branch(prod_manifest, 'nonexistent_model_xyz')
        assert result is None

    def test_branch_root_config_structure(self, prod_manifest, test_model):
        """Test root_config has expected fields."""
        result = branch(prod_manifest, test_model)

        assert result is not None
        root_config = result.get('root_config', {})
        assert 'partition_by' in root_config
        assert 'cluster_by' in root_config

    def test_branch_upstream_is_list(self, prod_manifest, test_model):
        """Test upstream is always a list."""
        result = branch(prod_manifest, test_model)

        assert result is not None
        assert isinstance(result.get('upstream', []), list)

    def test_branch_downstream_is_list(self, prod_manifest, test_model):
        """Test downstream is always a list."""
        result = branch(prod_manifest, test_model)

        assert result is not None
        assert isinstance(result.get('downstream', []), list)

    def test_branch_recommendations_is_list(self, prod_manifest, test_model):
        """Test recommendations is always a list."""
        result = branch(prod_manifest, test_model)

        assert result is not None
        assert isinstance(result.get('recommendations', []), list)


class TestBranchCommandHelpers:
    """Tests for BranchCommand helper methods."""

    def test_extract_partition_field_from_dict(self):
        """Test _extract_partition_field with dict config."""
        from dbt_meta.command_impl.branch import BranchCommand

        cmd = BranchCommand.__new__(BranchCommand)

        result = cmd._extract_partition_field({'field': 'event_date', 'granularity': 'day'})

        assert result == 'event_date'

    def test_extract_partition_field_from_string(self):
        """Test _extract_partition_field with string config."""
        from dbt_meta.command_impl.branch import BranchCommand

        cmd = BranchCommand.__new__(BranchCommand)

        result = cmd._extract_partition_field('created_at')

        assert result == 'created_at'

    def test_extract_partition_field_none(self):
        """Test _extract_partition_field with None."""
        from dbt_meta.command_impl.branch import BranchCommand

        cmd = BranchCommand.__new__(BranchCommand)

        result = cmd._extract_partition_field(None)

        assert result is None

    def test_generate_branch_recommendations_empty(self):
        """Test _generate_branch_recommendations with empty inputs."""
        from dbt_meta.command_impl.branch import BranchCommand

        cmd = BranchCommand.__new__(BranchCommand)

        recs = cmd._generate_branch_recommendations(
            root_partition=None,
            root_cluster=[],
            upstream=[],
            downstream=[],
        )

        assert isinstance(recs, list)

    def test_generate_branch_recommendations_with_misalignment(self):
        """Test _generate_branch_recommendations with partition misalignment."""
        from dbt_meta.command_impl.branch import BranchCommand

        cmd = BranchCommand.__new__(BranchCommand)

        upstream = [{
            'model': 'parent_model',
            'partition_by': 'created_at',
            'cluster_by': ['client_id'],
            'filter_alignment': 'partial',
        }]

        downstream = [{
            'model': 'child_model',
            'partition_by': 'event_date',  # Different partition
            'cluster_by': ['user_id'],
            'filters': ['client_id'],
        }]

        recs = cmd._generate_branch_recommendations(
            root_partition='event_date',
            root_cluster=['client_id'],
            upstream=upstream,
            downstream=downstream,
        )

        assert isinstance(recs, list)


class TestMonitoringUtilities:
    """Tests for monitoring utility functions."""

    @patch('dbt_meta.utils.monitoring.run_monitoring_query')
    def test_fetch_storage_metrics(self, mock_query):
        """Test fetch_storage_metrics returns expected structure."""
        from dbt_meta.utils.monitoring import fetch_storage_metrics

        mock_query.return_value = [{
            'total_gb': '100.5',
            'active_gb': '80.0',
            'long_term_gb': '20.5',
            'partition_count': '365',
            'row_count': '1000000',
            'cost_monthly_usd': '2.50',
            'potential_savings_usd': '0.50',
            'optimal_storage_billing_model': 'PHYSICAL',
        }]

        result = fetch_storage_metrics('test_dataset', 'test_table')

        assert result is not None
        assert result['total_gb'] == 100.5
        assert result['partition_count'] == 365

    @patch('dbt_meta.utils.monitoring.run_monitoring_query')
    def test_fetch_storage_metrics_not_found(self, mock_query):
        """Test fetch_storage_metrics returns None when table not found."""
        from dbt_meta.utils.monitoring import fetch_storage_metrics

        mock_query.return_value = []

        result = fetch_storage_metrics('test_dataset', 'nonexistent_table')

        assert result is None

    def test_fetch_downstream_filter_patterns(self):
        """Test filter pattern extraction from SQL."""
        from dbt_meta.utils.monitoring import fetch_downstream_filter_patterns

        sql = """
        SELECT * FROM table
        WHERE event_date = '2024-01-01'
          AND client_id IN (1, 2, 3)
          AND status = 'active'
        """

        filters = fetch_downstream_filter_patterns(sql)

        assert 'event_date' in filters
        assert 'client_id' in filters
        assert 'status' in filters

    def test_fetch_downstream_filter_patterns_empty_sql(self):
        """Test filter pattern extraction handles empty SQL."""
        from dbt_meta.utils.monitoring import fetch_downstream_filter_patterns

        filters = fetch_downstream_filter_patterns('')

        assert filters == []

    def test_fetch_downstream_filter_patterns_no_where(self):
        """Test filter pattern extraction handles SQL without WHERE."""
        from dbt_meta.utils.monitoring import fetch_downstream_filter_patterns

        sql = "SELECT * FROM table"
        filters = fetch_downstream_filter_patterns(sql)

        assert filters == []


class TestAnalyzeRecommendations:
    """Tests for recommendation generation logic."""

    def test_recommendation_for_large_unpartitioned_table(self):
        """Test recommendation generated for large unpartitioned table."""
        cmd = AnalyzeCommand.__new__(AnalyzeCommand)

        recs = cmd._generate_recommendations(
            config={'materialized': 'table'},
            storage={'total_gb': 50},
            partitions=None,
            usage={'query_count': 100},
            bq_columns=None,
            downstream_filters=[],
            materialized='table',
        )

        assert len(recs) > 0
        assert any(r['type'] == 'add_partition' for r in recs)

    def test_recommendation_for_high_scan_volume(self):
        """Test recommendation for high scan volume."""
        cmd = AnalyzeCommand.__new__(AnalyzeCommand)

        recs = cmd._generate_recommendations(
            config={'materialized': 'table'},
            storage={'total_gb': 100},
            partitions={'partition_count': 30},
            usage={'query_count': 500, 'bytes_scanned_7d': 500000000000},  # 500 GB
            bq_columns=None,
            downstream_filters=['event_date', 'client_id'],
            materialized='table',
        )

        # Should have some recommendations for high scan volume
        assert isinstance(recs, list)

    def test_recommendation_for_clustering(self):
        """Test recommendation for clustering candidate."""
        cmd = AnalyzeCommand.__new__(AnalyzeCommand)

        recs = cmd._generate_recommendations(
            config={'materialized': 'table'},
            storage={'total_gb': 20},
            partitions={'partition_count': 365},
            usage={'query_count': 1000},
            bq_columns={'client_id': {'clustering_rank': None}, 'event_type': {'clustering_rank': None}},
            downstream_filters=['client_id', 'event_type'],
            materialized='table',
        )

        # May recommend clustering based on downstream filters
        assert isinstance(recs, list)

    def test_no_recommendation_for_small_table(self):
        """Test no partition recommendation for small table."""
        cmd = AnalyzeCommand.__new__(AnalyzeCommand)

        recs = cmd._generate_recommendations(
            config={'materialized': 'table'},
            storage={'total_gb': 1},
            partitions=None,
            usage={'query_count': 10},
            bq_columns=None,
            downstream_filters=[],
            materialized='table',
        )

        partition_recs = [r for r in recs if r['type'] == 'add_partition']
        assert len(partition_recs) == 0

    def test_no_recommendation_for_views(self):
        """Test no recommendations for views."""
        cmd = AnalyzeCommand.__new__(AnalyzeCommand)

        recs = cmd._generate_recommendations(
            config={'materialized': 'view'},
            storage={'total_gb': 100},
            partitions=None,
            usage={'query_count': 1000},
            bq_columns=None,
            downstream_filters=[],
            materialized='view',
        )

        assert len(recs) == 0


class TestHotspotsScoringLogic:
    """Tests for HotspotsCommand scoring logic."""

    def test_score_table_query_cost(self):
        """Test scoring for query cost criterion."""
        from dbt_meta.command_impl.hotspots import HotspotsCommand
        from unittest.mock import MagicMock

        cmd = HotspotsCommand.__new__(HotspotsCommand)
        parser = MagicMock()
        parser.get_model.return_value = {
            'config': {'materialized': 'table', 'enabled': True},
            'schema': 'test',
            'alias': 'table1'
        }

        score, details, metrics = cmd._score_table(
            model_name='test_model',
            dataset='test',
            total_gb=10.0,
            partition_count=0,
            query_freq={'active_days': 7, 'references': 100},
            query_cost={'query_cost': 5.0, 'query_count': 100, 'total_slot_ms': 1000000, 'bytes_processed': 5000000000, 'cache_hit_ratio': 0.5},
            partition_info={},
            read_heavy={},
            unused_info={},
            model_metrics={},
            parser=parser,
        )

        assert score > 0
        assert any(d['criterion'] == 'query_cost' for d in details)
        assert metrics['query_cost'] == 5.0

    def test_score_table_no_partition(self):
        """Test scoring for missing partition."""
        from dbt_meta.command_impl.hotspots import HotspotsCommand
        from unittest.mock import MagicMock

        cmd = HotspotsCommand.__new__(HotspotsCommand)
        parser = MagicMock()
        parser.get_model.return_value = {
            'config': {'materialized': 'table', 'enabled': True},
            'schema': 'core',
            'alias': 'big_table'
        }

        score, details, metrics = cmd._score_table(
            model_name='test_model',
            dataset='core',
            total_gb=50.0,
            partition_count=0,
            query_freq={'active_days': 7, 'references': 100},
            query_cost={'query_cost': 1.0, 'query_count': 50, 'total_slot_ms': 500000, 'bytes_processed': 50000000000, 'cache_hit_ratio': 0.8},
            partition_info={},
            read_heavy={},
            unused_info={},
            model_metrics={},
            parser=parser,
        )

        assert score > 0
        assert any(d['criterion'] == 'no_partition' for d in details)

    def test_score_table_unused(self):
        """Test scoring for unused table."""
        from dbt_meta.command_impl.hotspots import HotspotsCommand
        from unittest.mock import MagicMock

        cmd = HotspotsCommand.__new__(HotspotsCommand)
        parser = MagicMock()
        parser.get_model.return_value = {
            'config': {'materialized': 'table', 'enabled': True},
            'schema': 'test',
            'alias': 'unused_table'
        }

        score, details, metrics = cmd._score_table(
            model_name='unused_model',
            dataset='test',
            total_gb=100.0,
            partition_count=0,
            query_freq={},
            query_cost={},
            partition_info={},
            read_heavy={},
            unused_info={'days_unused': 60, 'last_used_date': '2024-01-01'},
            model_metrics={},
            parser=parser,
        )

        assert score > 0
        assert any(d['criterion'] == 'unused' for d in details)

    def test_score_table_view_skipped(self):
        """Test that views are skipped in scoring."""
        from dbt_meta.command_impl.hotspots import HotspotsCommand
        from unittest.mock import MagicMock

        cmd = HotspotsCommand.__new__(HotspotsCommand)
        parser = MagicMock()
        parser.get_model.return_value = {
            'config': {'materialized': 'view', 'enabled': True},
            'schema': 'test',
            'alias': 'my_view'
        }

        score, details, metrics = cmd._score_table(
            model_name='view_model',
            dataset='test',
            total_gb=10.0,
            partition_count=0,
            query_freq={'active_days': 7, 'references': 1000},
            query_cost={'query_cost': 10.0, 'query_count': 100, 'total_slot_ms': 1000000, 'bytes_processed': 10000000000, 'cache_hit_ratio': 0.5},
            partition_info={},
            read_heavy={},
            unused_info={},
            model_metrics={},
            parser=parser,
        )

        assert score == 0
        assert len(details) == 0

    def test_score_table_disabled_skipped(self):
        """Test that disabled models are skipped."""
        from dbt_meta.command_impl.hotspots import HotspotsCommand
        from unittest.mock import MagicMock

        cmd = HotspotsCommand.__new__(HotspotsCommand)
        parser = MagicMock()
        parser.get_model.return_value = {
            'config': {'materialized': 'table', 'enabled': False},
            'schema': 'test',
            'alias': 'disabled_table'
        }

        score, details, metrics = cmd._score_table(
            model_name='disabled_model',
            dataset='test',
            total_gb=100.0,
            partition_count=0,
            query_freq={'active_days': 7, 'references': 1000},
            query_cost={'query_cost': 50.0, 'query_count': 500, 'total_slot_ms': 5000000, 'bytes_processed': 100000000000, 'cache_hit_ratio': 0.1},
            partition_info={},
            read_heavy={},
            unused_info={},
            model_metrics={},
            parser=parser,
        )

        assert score == 0

    def test_score_table_high_slot_usage(self):
        """Test scoring for high slot usage."""
        from dbt_meta.command_impl.hotspots import HotspotsCommand
        from unittest.mock import MagicMock

        cmd = HotspotsCommand.__new__(HotspotsCommand)
        parser = MagicMock()
        parser.get_model.return_value = {
            'config': {'materialized': 'table', 'enabled': True},
            'schema': 'test',
            'alias': 'slow_table'
        }

        # 10 minutes per query (600 seconds * 1000ms * 100 queries)
        score, details, metrics = cmd._score_table(
            model_name='slow_model',
            dataset='test',
            total_gb=5.0,
            partition_count=10,
            query_freq={'active_days': 7, 'references': 100},
            query_cost={'query_cost': 2.0, 'query_count': 100, 'total_slot_ms': 60000000, 'bytes_processed': 5000000000, 'cache_hit_ratio': 0.5},
            partition_info={'partition_type': 'DAY'},
            read_heavy={},
            unused_info={},
            model_metrics={},
            parser=parser,
        )

        assert score > 0
        assert any(d['criterion'] == 'high_slot' for d in details)


class TestMonitoringFunctions:
    """Tests for monitoring utility functions."""

    @patch('dbt_meta.utils.monitoring.run_monitoring_query')
    def test_fetch_total_bigquery_costs(self, mock_query):
        """Test fetch_total_bigquery_costs returns expected structure."""
        from dbt_meta.utils.monitoring import fetch_total_bigquery_costs

        mock_query.return_value = [{
            'total_cost': '207.07',
            'total_slot_hours': '5176.8',
            'total_queries': '218719',
        }]

        result = fetch_total_bigquery_costs(days=7)

        assert result is not None
        assert result['total_cost'] == 207.07
        assert result['total_slot_hours'] == 5176.8
        assert result['total_queries'] == 218719

    @patch('dbt_meta.utils.monitoring.run_monitoring_query')
    def test_fetch_total_bigquery_costs_empty(self, mock_query):
        """Test fetch_total_bigquery_costs returns None on empty result."""
        from dbt_meta.utils.monitoring import fetch_total_bigquery_costs

        mock_query.return_value = []

        result = fetch_total_bigquery_costs(days=7)

        assert result is None

    @patch('dbt_meta.utils.monitoring.run_monitoring_query')
    def test_fetch_total_bigquery_costs_none(self, mock_query):
        """Test fetch_total_bigquery_costs returns None on query failure."""
        from dbt_meta.utils.monitoring import fetch_total_bigquery_costs

        mock_query.return_value = None

        result = fetch_total_bigquery_costs(days=7)

        assert result is None

    @patch('dbt_meta.utils.monitoring.run_monitoring_query')
    def test_fetch_all_tables_storage(self, mock_query):
        """Test fetch_all_tables_storage returns list."""
        from dbt_meta.utils.monitoring import fetch_all_tables_storage

        mock_query.return_value = [
            {'dataset_id': 'test', 'table_id': 'table1', 'total_gb': '10.5', 'partition_count': '100'},
            {'dataset_id': 'test', 'table_id': 'table2', 'total_gb': '5.2', 'partition_count': '50'},
        ]

        result = fetch_all_tables_storage(min_gb=1.0)

        assert result is not None
        assert len(result) == 2

    @patch('dbt_meta.utils.monitoring.run_monitoring_query')
    def test_fetch_table_query_frequency(self, mock_query):
        """Test fetch_table_query_frequency returns list."""
        from dbt_meta.utils.monitoring import fetch_table_query_frequency

        mock_query.return_value = [
            {'dataset_id': 'test', 'table_id': 'table1', 'active_days': '7', 'total_references': '1000'},
        ]

        result = fetch_table_query_frequency(days=7)

        assert result is not None
        assert len(result) == 1

    @patch('dbt_meta.utils.monitoring.run_monitoring_query')
    def test_fetch_unused_tables(self, mock_query):
        """Test fetch_unused_tables returns list."""
        from dbt_meta.utils.monitoring import fetch_unused_tables

        mock_query.return_value = [
            {'dataset_id': 'test', 'table_id': 'old_table', 'days_unused': '90', 'last_used_date': '2024-01-01'},
        ]

        result = fetch_unused_tables(days_threshold=30)

        assert result is not None
        assert len(result) == 1

    @patch('dbt_meta.utils.monitoring.run_monitoring_query')
    def test_fetch_read_heavy_tables(self, mock_query):
        """Test fetch_read_heavy_tables returns list."""
        from dbt_meta.utils.monitoring import fetch_read_heavy_tables

        mock_query.return_value = [
            {'dataset_id': 'test', 'table_id': 'hot_table', 'reference_count': '5000', 'total_partitions': '365'},
        ]

        result = fetch_read_heavy_tables()

        assert result is not None
        assert len(result) == 1

    @patch('dbt_meta.utils.monitoring.run_monitoring_query')
    def test_fetch_model_query_costs(self, mock_query):
        """Test fetch_model_query_costs returns list."""
        from dbt_meta.utils.monitoring import fetch_model_query_costs

        mock_query.return_value = [
            {
                'dataset_id': 'core',
                'table_id': 'events',
                'query_cost_usd': '5.50',
                'query_count': '100',
                'total_slot_ms': '1000000',
                'bytes_processed': '5000000000',
                'cache_hit_ratio': '0.8',
            },
        ]

        result = fetch_model_query_costs()

        assert result is not None
        assert len(result) == 1

    @patch('dbt_meta.utils.monitoring.run_monitoring_query')
    def test_fetch_partition_info_all(self, mock_query):
        """Test fetch_partition_info_all returns list."""
        from dbt_meta.utils.monitoring import fetch_partition_info_all

        mock_query.return_value = [
            {
                'dataset_id': 'core',
                'table_id': 'events',
                'partition_type': 'DAY',
                'partition_count': '365',
                'partition_expiration_days': '90',
                'total_gb': '10.5',
            },
        ]

        result = fetch_partition_info_all()

        assert result is not None
        assert len(result) == 1

    @patch('dbt_meta.utils.monitoring.run_monitoring_query')
    def test_fetch_model_metrics(self, mock_query):
        """Test fetch_model_metrics returns list."""
        from dbt_meta.utils.monitoring import fetch_model_metrics

        mock_query.return_value = [
            {
                'dbt_model_name': 'core__events',
                'p90_duration_seconds': '120',
                'failure_count': '2',
            },
        ]

        result = fetch_model_metrics()

        assert result is not None
        assert len(result) == 1

    @patch('dbt_meta.utils.monitoring.run_monitoring_query')
    def test_fetch_dataset_billing_recommendations(self, mock_query):
        """Test fetch_dataset_billing_recommendations returns list."""
        from dbt_meta.utils.monitoring import fetch_dataset_billing_recommendations

        mock_query.return_value = [
            {
                'dataset_id': 'staging_google_events',
                'tables_recommend_physical': '7',
                'total_tables': '13',
                'net_savings_eur': '0.87',
                'recommended_billing': 'PHYSICAL',
            },
        ]

        result = fetch_dataset_billing_recommendations()

        assert result is not None
        assert len(result) == 1

    @patch('shutil.which')
    def test_run_monitoring_query_bq_not_found(self, mock_which):
        """Test run_monitoring_query returns None when bq not found."""
        from dbt_meta.utils.monitoring import run_monitoring_query

        mock_which.return_value = None

        with patch('os.path.exists', return_value=False):
            result = run_monitoring_query("SELECT 1")

        assert result is None

    @patch('shutil.which')
    @patch('subprocess.run')
    def test_run_monitoring_query_timeout(self, mock_run, mock_which):
        """Test run_monitoring_query handles timeout."""
        import subprocess
        from dbt_meta.utils.monitoring import run_monitoring_query

        mock_which.return_value = '/usr/bin/bq'
        mock_run.side_effect = subprocess.TimeoutExpired(cmd='bq', timeout=30)

        result = run_monitoring_query("SELECT 1", timeout=30)

        assert result is None

    @patch('shutil.which')
    @patch('subprocess.run')
    def test_run_monitoring_query_json_error(self, mock_run, mock_which):
        """Test run_monitoring_query handles invalid JSON."""
        from dbt_meta.utils.monitoring import run_monitoring_query

        mock_which.return_value = '/usr/bin/bq'
        mock_run.return_value = MagicMock(
            returncode=0,
            stdout='invalid json {{{',
        )

        result = run_monitoring_query("SELECT 1")

        assert result is None

    @patch('shutil.which')
    @patch('subprocess.run')
    def test_run_monitoring_query_empty_result(self, mock_run, mock_which):
        """Test run_monitoring_query handles empty result."""
        from dbt_meta.utils.monitoring import run_monitoring_query

        mock_which.return_value = '/usr/bin/bq'
        mock_run.return_value = MagicMock(
            returncode=0,
            stdout='[]',
        )

        result = run_monitoring_query("SELECT 1")

        assert result == []

    @patch('shutil.which')
    @patch('subprocess.run')
    def test_run_monitoring_query_nonzero_exit(self, mock_run, mock_which):
        """Test run_monitoring_query handles non-zero exit code."""
        from dbt_meta.utils.monitoring import run_monitoring_query

        mock_which.return_value = '/usr/bin/bq'
        mock_run.return_value = MagicMock(
            returncode=1,
            stdout='',
            stderr='Error',
        )

        result = run_monitoring_query("SELECT 1")

        assert result is None


class TestHotspotsHelperMethods:
    """Tests for HotspotsCommand helper methods."""

    def test_build_reverse_model_lookup(self):
        """Test _build_reverse_model_lookup creates correct mapping."""
        from dbt_meta.command_impl.hotspots import HotspotsCommand
        from unittest.mock import MagicMock

        cmd = HotspotsCommand.__new__(HotspotsCommand)

        parser = MagicMock()
        parser.get_all_models.return_value = {
            'model.project.core__events': {
                'schema': 'core',
                'alias': 'events',
                'name': 'core__events',
            },
            'model.project.staging__raw': {
                'schema': 'staging',
                'name': 'staging__raw',
            },
        }

        result = cmd._build_reverse_model_lookup(parser)

        assert 'core.events' in result
        assert result['core.events'] == 'core__events'
        assert 'staging.staging__raw' in result

    def test_build_query_freq_map(self):
        """Test _build_query_freq_map creates correct mapping."""
        from dbt_meta.command_impl.hotspots import HotspotsCommand
        from unittest.mock import MagicMock

        cmd = HotspotsCommand.__new__(HotspotsCommand)
        cmd.config = MagicMock()
        cmd.config.monitoring_dataset = 'prod'

        with patch('dbt_meta.command_impl.hotspots.fetch_table_query_frequency') as mock_fetch:
            mock_fetch.return_value = [
                {'dataset_id': 'core', 'table_id': 'events', 'active_days': 7, 'total_references': 500}
            ]

            result = cmd._build_query_freq_map()

        assert 'core.events' in result
        assert result['core.events']['references'] == 500

    def test_build_partition_map(self):
        """Test _build_partition_map creates correct mapping."""
        from dbt_meta.command_impl.hotspots import HotspotsCommand
        from unittest.mock import MagicMock

        cmd = HotspotsCommand.__new__(HotspotsCommand)
        cmd.config = MagicMock()
        cmd.config.monitoring_dataset = 'prod'

        with patch('dbt_meta.command_impl.hotspots.fetch_partition_info_all') as mock_fetch:
            mock_fetch.return_value = [
                {'dataset_id': 'core', 'table_id': 'events', 'partition_type': 'DAY', 'total_gb': 50.0}
            ]

            result = cmd._build_partition_map()

        assert 'core.events' in result
        assert result['core.events']['partition_type'] == 'DAY'

    def test_build_unused_map(self):
        """Test _build_unused_map creates correct mapping."""
        from dbt_meta.command_impl.hotspots import HotspotsCommand
        from unittest.mock import MagicMock

        cmd = HotspotsCommand.__new__(HotspotsCommand)
        cmd.config = MagicMock()
        cmd.config.monitoring_dataset = 'prod'

        with patch('dbt_meta.command_impl.hotspots.fetch_unused_tables') as mock_fetch:
            mock_fetch.return_value = [
                {'dataset_id': 'core', 'table_id': 'old_table', 'days_unused': 60}
            ]

            result = cmd._build_unused_map()

        assert 'core.old_table' in result
        assert result['core.old_table']['days_unused'] == 60

    def test_build_read_heavy_map(self):
        """Test _build_read_heavy_map creates correct mapping."""
        from dbt_meta.command_impl.hotspots import HotspotsCommand
        from unittest.mock import MagicMock

        cmd = HotspotsCommand.__new__(HotspotsCommand)
        cmd.config = MagicMock()
        cmd.config.monitoring_dataset = 'prod'

        with patch('dbt_meta.command_impl.hotspots.fetch_read_heavy_tables') as mock_fetch:
            mock_fetch.return_value = [
                {'dataset_id': 'core', 'table_id': 'hot_table', 'reference_count': 1000}
            ]

            result = cmd._build_read_heavy_map()

        assert 'core.hot_table' in result


class TestBranchCommand:
    """Tests for BranchCommand covering lines 73-74, 87-89, 134, 144-175, 193-243, 258-301."""

    def test_branch_upstream_analysis_with_parent_partition(self):
        """Test _analyze_upstream with partitioned parent."""
        from dbt_meta.command_impl.branch import BranchCommand
        from dbt_meta.config import Config

        config = Config.from_config_or_env()
        cmd = BranchCommand.__new__(BranchCommand)
        cmd.manifest_path = '/test/manifest.json'
        cmd.model_name = 'test_model'
        cmd.config = Config.from_config_or_env()

        parents = [{'path': 'models/staging/stg_events.sql'}]
        root_filters = ['event_date', 'user_id']
        root_partition = 'event_date'

        with patch('dbt_meta.command_impl.config.ConfigCommand.execute') as mock_config:
            mock_config.return_value = {
                'partition_by': {'field': 'event_date'},
                'cluster_by': ['user_id'],
                'materialized': 'incremental',
            }

            result = cmd._analyze_upstream(parents, root_filters, root_partition)

        assert len(result) == 1
        assert result[0]['model'] == 'stg_events'
        assert result[0]['partition_by'] == 'event_date'
        assert result[0]['impact'] == 'ALIGNED'

    def test_branch_upstream_analysis_without_partition(self):
        """Test _analyze_upstream when parent has no partition (HIGH impact)."""
        from dbt_meta.command_impl.branch import BranchCommand

        cmd = BranchCommand.__new__(BranchCommand)
        cmd.manifest_path = '/test/manifest.json'
        cmd.model_name = 'test_model'
        cmd.config = Config.from_config_or_env()

        parents = [{'path': 'models/staging/stg_raw.sql'}]
        root_filters = ['event_date']
        root_partition = 'event_date'

        with patch('dbt_meta.command_impl.config.ConfigCommand.execute') as mock_config:
            mock_config.return_value = {
                'partition_by': None,
                'cluster_by': [],
                'materialized': 'table',
            }

            result = cmd._analyze_upstream(parents, root_filters, root_partition)

        assert len(result) == 1
        assert result[0]['impact'] == 'HIGH'
        assert 'No partition' in result[0]['details'][0]

    def test_branch_upstream_analysis_with_view(self):
        """Test _analyze_upstream with view parent (N/A impact)."""
        from dbt_meta.command_impl.branch import BranchCommand

        cmd = BranchCommand.__new__(BranchCommand)
        cmd.manifest_path = '/test/manifest.json'
        cmd.model_name = 'test_model'
        cmd.config = Config.from_config_or_env()

        parents = [{'path': 'models/staging/stg_view.sql'}]
        root_filters = []
        root_partition = None

        with patch('dbt_meta.command_impl.config.ConfigCommand.execute') as mock_config:
            mock_config.return_value = {
                'partition_by': None,
                'cluster_by': [],
                'materialized': 'view',
            }

            result = cmd._analyze_upstream(parents, root_filters, root_partition)

        assert len(result) == 1
        assert result[0]['impact'] == 'N/A'
        assert 'View' in result[0]['details'][0]

    def test_branch_upstream_analysis_filter_not_in_cluster(self):
        """Test _analyze_upstream when root filters aren't in parent cluster (MEDIUM)."""
        from dbt_meta.command_impl.branch import BranchCommand

        cmd = BranchCommand.__new__(BranchCommand)
        cmd.manifest_path = '/test/manifest.json'
        cmd.model_name = 'test_model'
        cmd.config = Config.from_config_or_env()

        parents = [{'path': 'models/staging/stg_events.sql'}]
        root_filters = ['country_code', 'platform']  # Not in cluster
        root_partition = None

        with patch('dbt_meta.command_impl.config.ConfigCommand.execute') as mock_config:
            mock_config.return_value = {
                'partition_by': {'field': 'event_date'},
                'cluster_by': ['user_id'],  # Doesn't include root filters
                'materialized': 'table',
            }

            result = cmd._analyze_upstream(parents, root_filters, root_partition)

        assert len(result) == 1
        assert result[0]['impact'] == 'MEDIUM'
        assert 'not in cluster' in result[0]['details'][0]

    def test_branch_upstream_exception_handling(self):
        """Test _analyze_upstream handles DbtMetaError."""
        from dbt_meta.command_impl.branch import BranchCommand
        from dbt_meta.errors import ModelNotFoundError

        cmd = BranchCommand.__new__(BranchCommand)
        cmd.manifest_path = '/test/manifest.json'
        cmd.model_name = 'test_model'
        cmd.config = Config.from_config_or_env()

        parents = [{'path': 'models/staging/stg_events.sql'}]

        with patch('dbt_meta.command_impl.config.ConfigCommand.execute') as mock_config:
            mock_config.side_effect = ModelNotFoundError('not_found', ['test'])

            result = cmd._analyze_upstream(parents, [], None)

        assert len(result) == 0

    def test_branch_upstream_empty_path(self):
        """Test _analyze_upstream skips parents without path."""
        from dbt_meta.command_impl.branch import BranchCommand

        cmd = BranchCommand.__new__(BranchCommand)
        cmd.manifest_path = '/test/manifest.json'
        cmd.model_name = 'test_model'
        cmd.config = Config.from_config_or_env()

        parents = [{'path': ''}]  # Empty path

        result = cmd._analyze_upstream(parents, [], None)

        assert len(result) == 0

    def test_branch_downstream_analysis_good_alignment(self):
        """Test _analyze_downstream with good filter alignment."""
        from dbt_meta.command_impl.branch import BranchCommand

        cmd = BranchCommand.__new__(BranchCommand)
        cmd.manifest_path = '/test/manifest.json'
        cmd.model_name = 'test_model'
        cmd.config = Config.from_config_or_env()

        children = [{'path': 'models/core/core_events.sql'}]
        root_partition = 'event_date'
        root_cluster = ['user_id', 'country']

        with patch('dbt_meta.command_impl.sql.SqlCommand.execute') as mock_sql, \
             patch('dbt_meta.utils.monitoring.fetch_downstream_filter_patterns') as mock_filters:
            mock_sql.return_value = 'SELECT * FROM events WHERE event_date > ...'
            mock_filters.return_value = ['event_date', 'user_id']

            result = cmd._analyze_downstream(children, root_partition, root_cluster)

        assert len(result) == 1
        assert result[0]['model'] == 'core_events'
        assert result[0]['alignment'] == 'GOOD'
        assert any('partition' in d for d in result[0]['details'])

    def test_branch_downstream_analysis_suboptimal(self):
        """Test _analyze_downstream with suboptimal filter alignment."""
        from dbt_meta.command_impl.branch import BranchCommand

        cmd = BranchCommand.__new__(BranchCommand)
        cmd.manifest_path = '/test/manifest.json'
        cmd.model_name = 'test_model'
        cmd.config = Config.from_config_or_env()

        children = [{'path': 'models/core/core_events.sql'}]
        root_partition = 'event_date'
        root_cluster = ['user_id']

        with patch('dbt_meta.command_impl.sql.SqlCommand.execute') as mock_sql, \
             patch('dbt_meta.utils.monitoring.fetch_downstream_filter_patterns') as mock_filters:
            mock_sql.return_value = 'SELECT * FROM events WHERE platform = ...'
            mock_filters.return_value = ['platform']  # Not in partition/cluster

            result = cmd._analyze_downstream(children, root_partition, root_cluster)

        assert len(result) == 1
        assert result[0]['alignment'] == 'SUBOPTIMAL'
        assert any('not in partition/cluster' in d for d in result[0]['details'])

    def test_branch_downstream_analysis_poor(self):
        """Test _analyze_downstream with POOR alignment (no partition/cluster)."""
        from dbt_meta.command_impl.branch import BranchCommand

        cmd = BranchCommand.__new__(BranchCommand)
        cmd.manifest_path = '/test/manifest.json'
        cmd.model_name = 'test_model'
        cmd.config = Config.from_config_or_env()

        children = [{'path': 'models/core/core_events.sql'}]
        root_partition = None  # No partition
        root_cluster = []  # No cluster

        with patch('dbt_meta.command_impl.sql.SqlCommand.execute') as mock_sql, \
             patch('dbt_meta.utils.monitoring.fetch_downstream_filter_patterns') as mock_filters:
            mock_sql.return_value = 'SELECT * FROM events WHERE platform = ...'
            mock_filters.return_value = ['platform']

            result = cmd._analyze_downstream(children, root_partition, root_cluster)

        assert len(result) == 1
        assert result[0]['alignment'] == 'POOR'

    def test_branch_downstream_exception_handling(self):
        """Test _analyze_downstream handles DbtMetaError."""
        from dbt_meta.command_impl.branch import BranchCommand
        from dbt_meta.errors import ModelNotFoundError

        cmd = BranchCommand.__new__(BranchCommand)
        cmd.manifest_path = '/test/manifest.json'
        cmd.model_name = 'test_model'
        cmd.config = Config.from_config_or_env()

        children = [{'path': 'models/core/core_events.sql'}]

        with patch('dbt_meta.command_impl.sql.SqlCommand.execute') as mock_sql:
            mock_sql.side_effect = ModelNotFoundError('not_found', ['test'])

            result = cmd._analyze_downstream(children, None, [])

        assert len(result) == 1
        assert result[0]['alignment'] == 'GOOD'  # No filters detected
        assert 'No filter patterns' in result[0]['details'][0]

    def test_branch_recommendations_upstream_partition(self):
        """Test _generate_branch_recommendations for upstream partitioning."""
        from dbt_meta.command_impl.branch import BranchCommand

        cmd = BranchCommand.__new__(BranchCommand)

        upstream = [{'model': 'stg_events', 'impact': 'HIGH'}]
        downstream = []

        recs = cmd._generate_branch_recommendations(
            root_partition='event_date',
            root_cluster=[],
            upstream=upstream,
            downstream=downstream,
        )

        assert len(recs) == 1
        assert recs[0]['type'] == 'upstream_partition'
        assert recs[0]['priority'] == 'HIGH'
        assert 'stg_events' in recs[0]['action']

    def test_branch_recommendations_add_clustering(self):
        """Test _generate_branch_recommendations for adding clustering."""
        from dbt_meta.command_impl.branch import BranchCommand

        cmd = BranchCommand.__new__(BranchCommand)

        upstream = []
        downstream = [
            {'alignment': 'POOR', 'filters_used': ['country']},
            {'alignment': 'SUBOPTIMAL', 'filters_used': ['country', 'platform']},
        ]

        recs = cmd._generate_branch_recommendations(
            root_partition='event_date',
            root_cluster=[],
            upstream=upstream,
            downstream=downstream,
        )

        # Should recommend adding 'country' to cluster_by
        cluster_recs = [r for r in recs if r['type'] == 'add_clustering']
        assert len(cluster_recs) == 1
        assert 'country' in cluster_recs[0]['action']
        assert 'priority' in cluster_recs[0]

    def test_branch_recommendations_add_partition(self):
        """Test _generate_branch_recommendations for adding partition."""
        from dbt_meta.command_impl.branch import BranchCommand

        cmd = BranchCommand.__new__(BranchCommand)

        upstream = []
        downstream = [
            {'alignment': 'POOR', 'filters_used': ['event_date']},
            {'alignment': 'POOR', 'filters_used': ['event_date']},
        ]

        recs = cmd._generate_branch_recommendations(
            root_partition=None,  # No partition
            root_cluster=[],
            upstream=upstream,
            downstream=downstream,
        )

        # Should recommend adding partition
        partition_recs = [r for r in recs if r['type'] == 'add_partition']
        assert len(partition_recs) == 1
        assert 'event_date' in partition_recs[0]['action']
        assert partition_recs[0]['priority'] == 'HIGH'

    def test_branch_execute_with_parents_error(self):
        """Test execute handles DbtMetaError from parents command."""
        from dbt_meta.command_impl.branch import BranchCommand
        from dbt_meta.config import Config
        from dbt_meta.errors import ModelNotFoundError

        config = Config.from_config_or_env()
        cmd = BranchCommand.__new__(BranchCommand)
        cmd.config = config
        cmd.manifest_path = '/test/manifest.json'
        cmd.model_name = 'test_model'
        cmd.use_dev = False
        cmd.json_output = False

        model = {
            'config': {'partition_by': 'event_date', 'cluster_by': []},
            'compiled_code': '',
        }

        with patch('dbt_meta.command_impl.parents.ParentsCommand.execute') as mock_parents, \
             patch('dbt_meta.command_impl.children.ChildrenCommand.execute') as mock_children:
            mock_parents.side_effect = ModelNotFoundError('not_found', ['test'])
            mock_children.return_value = []

            result = cmd.process_model(model)

        assert result is not None
        assert result['upstream'] == []  # Exception handled gracefully

    def test_branch_execute_with_children_error(self):
        """Test execute handles DbtMetaError from children command."""
        from dbt_meta.command_impl.branch import BranchCommand
        from dbt_meta.config import Config
        from dbt_meta.errors import ModelNotFoundError

        config = Config.from_config_or_env()
        cmd = BranchCommand.__new__(BranchCommand)
        cmd.config = config
        cmd.manifest_path = '/test/manifest.json'
        cmd.model_name = 'test_model'
        cmd.use_dev = False
        cmd.json_output = False

        model = {
            'config': {'partition_by': 'event_date', 'cluster_by': []},
            'compiled_code': '',
        }

        with patch('dbt_meta.command_impl.parents.ParentsCommand.execute') as mock_parents, \
             patch('dbt_meta.command_impl.children.ChildrenCommand.execute') as mock_children:
            mock_parents.return_value = []
            mock_children.side_effect = ModelNotFoundError('not_found', ['test'])

            result = cmd.process_model(model)

        assert result is not None
        assert result['downstream'] == []  # Exception handled gracefully


class TestAnalyzeCommandCoverage:
    """Tests for AnalyzeCommand covering lines 76-78, 135-136, 142-170, 224-226, 243, 252-256, 267-268."""

    def test_analyze_partition_as_string(self):
        """Test analyze with partition_by as string (not dict)."""
        from dbt_meta.command_impl.analyze import AnalyzeCommand
        from dbt_meta.config import Config

        config = Config.from_config_or_env()
        cmd = AnalyzeCommand.__new__(AnalyzeCommand)
        cmd.config = config
        cmd.manifest_path = '/test/manifest.json'
        cmd.model_name = 'test_model'
        cmd.use_dev = False
        cmd.json_output = False
        cmd._children_cache = None

        model = {
            'schema': 'core',
            'name': 'events',
            'config': {
                'partition_by': 'event_date',  # String, not dict
                'cluster_by': [],
                'materialized': 'table',
            },
        }

        with patch.object(cmd, '_analyze_downstream_filters', return_value=[]), \
             patch('dbt_meta.utils.monitoring.fetch_storage_metrics', return_value=None), \
             patch('dbt_meta.utils.monitoring.fetch_partition_stats', return_value=None), \
             patch('dbt_meta.utils.monitoring.fetch_usage_stats', return_value=None), \
             patch('dbt_meta.utils.monitoring.fetch_column_clustering_info', return_value=None):

            result = cmd.process_model(model)

        assert result['config']['partition_by'] == 'event_date'
        assert result['config']['partition_type'] == 'DAY'

    def test_analyze_downstream_filters_with_children(self):
        """Test _analyze_downstream_filters with actual children."""
        from dbt_meta.command_impl.analyze import AnalyzeCommand
        from dbt_meta.config import Config

        config = Config.from_config_or_env()
        cmd = AnalyzeCommand.__new__(AnalyzeCommand)
        cmd.config = config
        cmd.manifest_path = '/test/manifest.json'
        cmd.model_name = 'test_model'
        cmd.use_dev = False
        cmd.json_output = False

        model = {'name': 'test_model'}

        with patch('dbt_meta.command_impl.children.ChildrenCommand.execute') as mock_children, \
             patch('dbt_meta.command_impl.sql.SqlCommand.execute') as mock_sql, \
             patch('dbt_meta.utils.monitoring.fetch_downstream_filter_patterns') as mock_filters:
            mock_children.return_value = [
                {'model': 'child1', 'path': 'models/child1.sql'},
                {'path': 'models/child2.sql'},  # Uses path instead of model
            ]
            mock_sql.return_value = 'SELECT * FROM x WHERE event_date > ...'
            # Return different filters for each child
            mock_filters.side_effect = [
                ['event_date', 'user_id'],
                ['event_date', 'country'],
            ]

            result = cmd._analyze_downstream_filters(model)

        assert 'event_date' in result  # Most common filter (in both children)

    def test_analyze_downstream_filters_error_handling(self):
        """Test _analyze_downstream_filters handles DbtMetaError in children."""
        from dbt_meta.command_impl.analyze import AnalyzeCommand
        from dbt_meta.config import Config
        from dbt_meta.errors import ModelNotFoundError

        config = Config.from_config_or_env()
        cmd = AnalyzeCommand.__new__(AnalyzeCommand)
        cmd.config = config
        cmd.manifest_path = '/test/manifest.json'
        cmd.model_name = 'test_model'

        with patch('dbt_meta.command_impl.children.ChildrenCommand.execute') as mock_children:
            mock_children.side_effect = ModelNotFoundError('not_found', ['test'])

            result = cmd._analyze_downstream_filters({})

        assert result == []

    def test_analyze_downstream_filters_sql_error(self):
        """Test _analyze_downstream_filters handles DbtMetaError in sql."""
        from dbt_meta.command_impl.analyze import AnalyzeCommand
        from dbt_meta.config import Config
        from dbt_meta.errors import ModelNotFoundError

        config = Config.from_config_or_env()
        cmd = AnalyzeCommand.__new__(AnalyzeCommand)
        cmd.config = config
        cmd.manifest_path = '/test/manifest.json'
        cmd.model_name = 'test_model'

        with patch('dbt_meta.command_impl.children.ChildrenCommand.execute') as mock_children, \
             patch('dbt_meta.command_impl.sql.SqlCommand.execute') as mock_sql:
            mock_children.return_value = [{'model': 'child1'}]
            mock_sql.side_effect = ModelNotFoundError('not_found', ['test'])

            result = cmd._analyze_downstream_filters({})

        assert result == []

    def test_analyze_recommendation_partition_expiration(self):
        """Test partition expiration recommendation (lines 224-226)."""
        from dbt_meta.command_impl.analyze import AnalyzeCommand

        cmd = AnalyzeCommand.__new__(AnalyzeCommand)

        config = {
            'partition_by': {'field': 'event_date'},
            'cluster_by': [],
            'partition_expiration_days': None,  # No expiration set
        }
        storage = {'total_gb': 100}
        partitions = {'count': 400, 'total_gb': 100}  # > 365 partitions
        usage = None
        bq_columns = None
        downstream_filters = []

        recs = cmd._generate_recommendations(
            config=config,
            storage=storage,
            partitions=partitions,
            usage=usage,
            bq_columns=bq_columns,
            downstream_filters=downstream_filters,
            materialized='table',
        )

        exp_recs = [r for r in recs if r['type'] == 'partition_expiration']
        assert len(exp_recs) == 1
        assert 'partition expiration' in exp_recs[0]['message'].lower()
        assert exp_recs[0]['priority'] == 'MEDIUM'

    def test_analyze_recommendation_config_mismatch(self):
        """Test config mismatch recommendation (line 243)."""
        from dbt_meta.command_impl.analyze import AnalyzeCommand

        cmd = AnalyzeCommand.__new__(AnalyzeCommand)

        config = {
            'partition_by': {'field': 'created_at'},
            'cluster_by': [],
        }
        storage = {'total_gb': 10}
        partitions = None
        usage = None
        bq_columns = {
            'partition_column': {'name': 'event_date'},  # Different from manifest
            'cluster_columns': [],
        }
        downstream_filters = []

        recs = cmd._generate_recommendations(
            config=config,
            storage=storage,
            partitions=partitions,
            usage=usage,
            bq_columns=bq_columns,
            downstream_filters=downstream_filters,
            materialized='table',
        )

        mismatch_recs = [r for r in recs if r['type'] == 'config_mismatch']
        assert len(mismatch_recs) == 1
        assert 'created_at' in mismatch_recs[0]['message']
        assert 'event_date' in mismatch_recs[0]['message']

    def test_analyze_recommendation_clustering_optimization(self):
        """Test clustering optimization recommendation (lines 252-256)."""
        from dbt_meta.command_impl.analyze import AnalyzeCommand

        cmd = AnalyzeCommand.__new__(AnalyzeCommand)

        config = {
            'partition_by': {'field': 'event_date'},
            'cluster_by': ['user_id'],
        }
        storage = {'total_gb': 10}
        partitions = None
        usage = None
        bq_columns = {
            'partition_column': {},
            'cluster_columns': [],
        }
        downstream_filters = ['country', 'platform']  # Not in cluster_by

        recs = cmd._generate_recommendations(
            config=config,
            storage=storage,
            partitions=partitions,
            usage=usage,
            bq_columns=bq_columns,
            downstream_filters=downstream_filters,
            materialized='table',
        )

        cluster_recs = [r for r in recs if r['type'] == 'clustering_optimization']
        assert len(cluster_recs) == 1
        assert 'country' in cluster_recs[0]['message'] or 'platform' in cluster_recs[0]['message']

    def test_analyze_recommendation_billing_model(self):
        """Test billing model recommendation (lines 267-268)."""
        from dbt_meta.command_impl.analyze import AnalyzeCommand

        cmd = AnalyzeCommand.__new__(AnalyzeCommand)

        config = {
            'partition_by': None,
            'cluster_by': [],
        }
        storage = {
            'total_gb': 100,
            'potential_savings_usd': 5.50,  # > $1
            'optimal_billing_model': 'PHYSICAL',
        }
        partitions = None
        usage = None
        bq_columns = None
        downstream_filters = []

        recs = cmd._generate_recommendations(
            config=config,
            storage=storage,
            partitions=partitions,
            usage=usage,
            bq_columns=bq_columns,
            downstream_filters=downstream_filters,
            materialized='table',
        )

        billing_recs = [r for r in recs if r['type'] == 'billing_model']
        assert len(billing_recs) == 1
        assert 'PHYSICAL' in billing_recs[0]['message']
        assert '$5.50' in billing_recs[0]['message']


