"""Tests covering gap branches in command_impl/hotspots.py.

Targets:
- lines 155-171: scored_tables append path inside execute()
- lines 237-264: _build_query_cost_map with model found + double-underscore fallback
- lines 336-353: _build_model_metrics_map path
- lines 474, 478: high_slot scoring tiers (>600 sec and 30-120 sec)
- lines 495, 515: no_partition and no_clustering scoring tiers (>100GB)
- lines 549-557: ineffective_partition criterion
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from dbt_meta.command_impl.hotspots import HotspotsCommand


class TestScoreTableAdditionalTiers:
    """Cover scoring tiers not exercised by existing tests."""

    def _build_parser(self, materialized='table', enabled=True):
        parser = MagicMock()
        parser.get_model.return_value = {
            'config': {'materialized': materialized, 'enabled': enabled},
            'schema': 'core',
            'alias': 'tbl',
        }
        return parser

    def test_high_slot_top_tier_over_600_seconds(self):
        """Line 474: slot_sec_per_query > 600 → base=15."""
        cmd = HotspotsCommand.__new__(HotspotsCommand)
        parser = self._build_parser()

        # 1000 queries, 700 sec/query → 700000 sec = 700_000_000 ms
        score, details, _ = cmd._score_table(
            model_name='slow',
            dataset='core',
            total_gb=5.0,
            partition_count=5,
            query_freq={},
            query_cost={
                'query_cost': 1.0,
                'query_count': 1000,
                'total_slot_ms': 700_000_000_000 // 1000,  # 700_000_000 ms total / 1000 queries = 700s each
                'bytes_processed': 1_000_000_000,
                'cache_hit_ratio': 1.0,
            },
            partition_info={'partition_type': 'DAY'},
            read_heavy={},
            unused_info={},
            model_metrics={},
            parser=parser,
        )

        high_slot = [d for d in details if d['criterion'] == 'high_slot']
        assert high_slot, 'high_slot criterion should be present'
        # Sanity: with base=15 × log2(1000)≈10 → pts close to or capped at 75
        assert high_slot[0]['points'] >= 15

    def test_high_slot_lowest_tier_between_30_and_120(self):
        """Line 478: 30 < slot_sec_per_query <= 120 → base=3."""
        cmd = HotspotsCommand.__new__(HotspotsCommand)
        parser = self._build_parser()

        # 100 queries at 50 sec/query → 5000 sec total = 5_000_000 ms
        score, details, _ = cmd._score_table(
            model_name='mid',
            dataset='core',
            total_gb=5.0,
            partition_count=5,
            query_freq={},
            query_cost={
                'query_cost': 0.1,  # below query_cost threshold
                'query_count': 100,
                'total_slot_ms': 5_000_000,
                'bytes_processed': 1_000_000_000,
                'cache_hit_ratio': 1.0,
            },
            partition_info={'partition_type': 'DAY'},
            read_heavy={},
            unused_info={},
            model_metrics={},
            parser=parser,
        )

        high_slot = [d for d in details if d['criterion'] == 'high_slot']
        assert high_slot, 'high_slot criterion should be present at base=3 tier'

    def test_no_partition_huge_tier_over_100gb(self):
        """Line 495: total_gb > 100 → base=15 in no_partition."""
        cmd = HotspotsCommand.__new__(HotspotsCommand)
        parser = self._build_parser()

        score, details, _ = cmd._score_table(
            model_name='huge',
            dataset='core',
            total_gb=250.0,
            partition_count=0,
            query_freq={},
            query_cost={
                'query_cost': 0.01,
                'query_count': 50,
                'total_slot_ms': 1_000_000,
                'bytes_processed': 1_000_000_000,
                'cache_hit_ratio': 1.0,
            },
            partition_info={},  # no partition_by
            read_heavy={},
            unused_info={},
            model_metrics={},
            parser=parser,
        )

        no_part = [d for d in details if d['criterion'] == 'no_partition']
        assert no_part, 'no_partition should be flagged for huge tables'

    def test_no_clustering_huge_tier_over_100gb(self):
        """Line 515: total_gb > 100 → base=10 in no_clustering."""
        cmd = HotspotsCommand.__new__(HotspotsCommand)
        parser = self._build_parser()

        score, details, _ = cmd._score_table(
            model_name='huge',
            dataset='core',
            total_gb=250.0,
            partition_count=10,
            query_freq={},
            query_cost={
                'query_cost': 0.01,
                'query_count': 50,
                'total_slot_ms': 1_000_000,
                'bytes_processed': 1_000_000_000,
                'cache_hit_ratio': 1.0,
            },
            partition_info={'partition_type': 'DAY'},  # partition ok
            read_heavy={},
            unused_info={},
            model_metrics={},
            parser=parser,
        )

        no_clust = [d for d in details if d['criterion'] == 'no_clustering']
        assert no_clust, 'no_clustering should be flagged for huge tables'

    def test_ineffective_partition_high_scan_ratio(self):
        """Lines 549-557: model has partition_by but gb_per_query/total_gb > 0.5."""
        cmd = HotspotsCommand.__new__(HotspotsCommand)
        # parser returns a model with partition_by set (required to enter branch)
        parser = MagicMock()
        parser.get_model.return_value = {
            'config': {
                'materialized': 'table',
                'enabled': True,
                'partition_by': {'field': 'event_date', 'data_type': 'date'},
            },
            'schema': 'core',
            'alias': 'tbl',
        }

        # 50GB table, 40GB scanned per query → scan_ratio=0.8
        score, details, _ = cmd._score_table(
            model_name='badly_partitioned',
            dataset='core',
            total_gb=50.0,
            partition_count=5,
            query_freq={},
            query_cost={
                'query_cost': 0.01,
                'query_count': 100,
                'total_slot_ms': 1_000_000,
                'bytes_processed': 4_000_000_000_000,  # 40GB / query
                'cache_hit_ratio': 1.0,
            },
            partition_info={'partition_type': 'DAY'},
            read_heavy={},
            unused_info={},
            model_metrics={},
            parser=parser,
        )

        ineffective = [d for d in details if d['criterion'] == 'ineffective_partition']
        assert ineffective, 'ineffective_partition should be flagged'

    def test_ineffective_partition_incremental_variant(self):
        """Lines 549-557: same but model is incremental → recommendation mentions incremental."""
        cmd = HotspotsCommand.__new__(HotspotsCommand)
        parser = MagicMock()
        parser.get_model.return_value = {
            'config': {
                'materialized': 'incremental',
                'enabled': True,
                'partition_by': {'field': 'event_date', 'data_type': 'date'},
            },
            'schema': 'core',
            'alias': 'tbl',
        }

        _, details, _ = cmd._score_table(
            model_name='bad_incremental',
            dataset='core',
            total_gb=50.0,
            partition_count=5,
            query_freq={},
            query_cost={
                'query_cost': 0.01,
                'query_count': 100,
                'total_slot_ms': 1_000_000,
                'bytes_processed': 4_000_000_000_000,
                'cache_hit_ratio': 1.0,
            },
            partition_info={'partition_type': 'DAY'},
            read_heavy={},
            unused_info={},
            model_metrics={},
            parser=parser,
        )

        ineffective = [d for d in details if d['criterion'] == 'ineffective_partition']
        assert ineffective
        assert 'incremental' in ineffective[0]['recommendation'].lower()


class TestBuildQueryCostMap:
    """Cover _build_query_cost_map paths (lines 237-264)."""

    def test_model_found_in_manifest(self):
        cmd = HotspotsCommand.__new__(HotspotsCommand)
        cmd.config = MagicMock()
        cmd.config.monitoring_dataset = 'prod'

        parser = MagicMock()
        parser.get_model.return_value = {
            'schema': 'core',
            'alias': 'events',
            'name': 'core__events',
        }

        with patch('dbt_meta.command_impl.hotspots.fetch_model_query_costs') as mock_fetch:
            mock_fetch.return_value = [
                {
                    'dbt_model_name': 'admirals_bi_dwh.core__events',
                    'query_cost_usd': '1.25',
                    'query_count': '42',
                    'total_slot_ms': '1000',
                    'bytes_processed': '9999',
                    'cache_hit_ratio': '0.3',
                },
            ]
            result = cmd._build_query_cost_map(parser)

        assert 'core.events' in result
        assert result['core.events']['query_cost'] == 1.25
        assert result['core.events']['query_count'] == 42

    def test_falls_back_to_double_underscore_split(self):
        cmd = HotspotsCommand.__new__(HotspotsCommand)
        cmd.config = MagicMock()
        cmd.config.monitoring_dataset = 'prod'

        parser = MagicMock()
        parser.get_model.return_value = None  # model not in manifest

        with patch('dbt_meta.command_impl.hotspots.fetch_model_query_costs') as mock_fetch:
            mock_fetch.return_value = [
                {
                    'dbt_model_name': 'admirals_bi_dwh.staging__raw_events',
                    'query_cost_usd': '0.5',
                    'query_count': '10',
                    'total_slot_ms': '100',
                    'bytes_processed': '100',
                    'cache_hit_ratio': '0.0',
                },
            ]
            result = cmd._build_query_cost_map(parser)

        # Falls back to splitting "staging__raw_events" → "staging.raw_events"
        assert 'staging.raw_events' in result

    def test_model_without_schema_is_skipped(self):
        """When manifest returns a model with missing schema, entry is skipped (no fallback)."""
        cmd = HotspotsCommand.__new__(HotspotsCommand)
        cmd.config = MagicMock()
        cmd.config.monitoring_dataset = 'prod'

        parser = MagicMock()
        parser.get_model.return_value = {
            'schema': '',   # missing
            'alias': 'events',
        }

        with patch('dbt_meta.command_impl.hotspots.fetch_model_query_costs') as mock_fetch:
            mock_fetch.return_value = [
                {
                    'dbt_model_name': 'admirals_bi_dwh.core__events',
                    'query_cost_usd': '1.0',
                    'query_count': '5',
                    'total_slot_ms': '10',
                    'bytes_processed': '10',
                    'cache_hit_ratio': '0',
                },
            ]
            result = cmd._build_query_cost_map(parser)

        # model truthy but schema empty → inner if is False → nothing added
        # (fallback only runs when model is None)
        assert result == {}

    def test_empty_fetch_returns_empty_map(self):
        cmd = HotspotsCommand.__new__(HotspotsCommand)
        cmd.config = MagicMock()
        cmd.config.monitoring_dataset = 'prod'

        parser = MagicMock()

        with patch('dbt_meta.command_impl.hotspots.fetch_model_query_costs') as mock_fetch:
            mock_fetch.return_value = []
            result = cmd._build_query_cost_map(parser)

        assert result == {}


class TestBuildModelMetricsMap:
    """Cover _build_model_metrics_map (lines 336-353)."""

    def test_model_metrics_mapped_via_manifest(self):
        cmd = HotspotsCommand.__new__(HotspotsCommand)
        cmd.config = MagicMock()
        cmd.config.monitoring_dataset = 'prod'

        parser = MagicMock()
        parser.get_model.return_value = {
            'schema': 'core',
            'config': {'alias': 'events'},
            'name': 'core__events',
        }

        with patch('dbt_meta.command_impl.hotspots.fetch_model_metrics') as mock_fetch:
            mock_fetch.return_value = [
                {
                    'dbt_model_name': 'admirals_bi_dwh.core__events',
                    'failed_runs': '2',
                    'p90_duration_seconds': '12.5',
                    'avg_duration_seconds': '7.5',
                },
            ]
            result = cmd._build_model_metrics_map(parser)

        assert 'core.events' in result
        assert result['core.events']['failed_runs'] == 2
        assert result['core.events']['p90_duration_seconds'] == 12.5

    def test_model_metrics_falls_back_to_name_when_no_alias(self):
        cmd = HotspotsCommand.__new__(HotspotsCommand)
        cmd.config = MagicMock()
        cmd.config.monitoring_dataset = 'prod'

        parser = MagicMock()
        parser.get_model.return_value = {
            'schema': 'stg',
            'config': {},   # no alias
            'name': 'stg__raw',
        }

        with patch('dbt_meta.command_impl.hotspots.fetch_model_metrics') as mock_fetch:
            mock_fetch.return_value = [
                {
                    'dbt_model_name': 'admirals_bi_dwh.stg__raw',
                    'failed_runs': '0',
                    'p90_duration_seconds': '1',
                    'avg_duration_seconds': '1',
                },
            ]
            result = cmd._build_model_metrics_map(parser)

        assert 'stg.stg__raw' in result

    def test_empty_fetch_returns_empty_map(self):
        cmd = HotspotsCommand.__new__(HotspotsCommand)
        cmd.config = MagicMock()
        cmd.config.monitoring_dataset = 'prod'

        parser = MagicMock()

        with patch('dbt_meta.command_impl.hotspots.fetch_model_metrics') as mock_fetch:
            mock_fetch.return_value = []
            result = cmd._build_model_metrics_map(parser)

        assert result == {}


class TestHotspotsExecuteIntegration:
    """Cover lines 155-171: scored_tables append path within execute()."""

    def _patch_all_fetchers(self, monkeypatch, tables_data, query_cost_data=None, billing_data=None):
        monkeypatch.setattr(
            'dbt_meta.command_impl.hotspots.fetch_all_tables_storage',
            lambda **kw: tables_data,
        )
        monkeypatch.setattr(
            'dbt_meta.command_impl.hotspots.fetch_table_query_frequency',
            lambda **kw: [],
        )
        monkeypatch.setattr(
            'dbt_meta.command_impl.hotspots.fetch_model_query_costs',
            lambda **kw: query_cost_data or [],
        )
        monkeypatch.setattr(
            'dbt_meta.command_impl.hotspots.fetch_partition_info_all',
            lambda **kw: [],
        )
        monkeypatch.setattr(
            'dbt_meta.command_impl.hotspots.fetch_read_heavy_tables',
            lambda **kw: [],
        )
        monkeypatch.setattr(
            'dbt_meta.command_impl.hotspots.fetch_unused_tables',
            lambda **kw: [],
        )
        monkeypatch.setattr(
            'dbt_meta.command_impl.hotspots.fetch_model_metrics',
            lambda **kw: [],
        )
        monkeypatch.setattr(
            'dbt_meta.command_impl.hotspots.fetch_total_bigquery_costs',
            lambda **kw: {'total_cost': 100.0, 'total_slot_hours': 5.0, 'total_queries': 1000},
        )
        monkeypatch.setattr(
            'dbt_meta.command_impl.hotspots.fetch_dataset_billing_recommendations',
            lambda **kw: billing_data or [],
        )

    def test_execute_collects_scored_tables(self, monkeypatch):
        cmd = HotspotsCommand.__new__(HotspotsCommand)
        cmd.config = MagicMock()
        cmd.config.monitoring_dataset = 'prod'
        cmd.limit = 10
        cmd.min_gb = 0.0
        cmd.manifest_path = '/fake/manifest.json'

        parser = MagicMock()
        parser.get_all_models.return_value = {
            'model.pkg.core__events': {
                'schema': 'core',
                'alias': 'events',
                'name': 'core__events',
                'config': {'materialized': 'table', 'enabled': True},
            }
        }
        parser.get_model.return_value = {
            'schema': 'core',
            'alias': 'events',
            'name': 'core__events',
            'config': {'materialized': 'table', 'enabled': True},
        }

        monkeypatch.setattr(
            'dbt_meta.command_impl.hotspots.get_cached_parser',
            lambda _p: parser,
        )
        self._patch_all_fetchers(
            monkeypatch,
            tables_data=[
                {
                    'dataset_id': 'core',
                    'table_id': 'events',
                    'total_gb': '200',
                    'partition_count': '0',
                },
            ],
            query_cost_data=[
                {
                    'dbt_model_name': 'admirals_bi_dwh.core__events',
                    'query_cost_usd': '5.0',
                    'query_count': '100',
                    'total_slot_ms': '10000000',
                    'bytes_processed': '100000000000',
                    'cache_hit_ratio': '0.5',
                },
            ],
        )

        result = cmd.execute()

        assert result['summary']['total_tables_analyzed'] == 1
        assert result['summary']['tables_with_issues'] >= 1
        assert len(result['hotspots']) >= 1
        assert result['hotspots'][0]['table'] == 'core.events'
        # Collection of fields in the scored_tables entry (lines 155-171)
        hs = result['hotspots'][0]
        for key in (
            'model',
            'table',
            'score',
            'scoring_details',
            'total_gb',
            'query_cost_7d',
            'query_count_7d',
            'slot_hours_7d',
            'gb_per_query',
            'references_7d',
            'is_incremental',
            'is_partitioned',
            'is_clustered',
        ):
            assert key in hs, f'missing {key} in scored hotspot entry'

    def test_execute_formats_dataset_billing_recommendations(self, monkeypatch):
        cmd = HotspotsCommand.__new__(HotspotsCommand)
        cmd.config = MagicMock()
        cmd.config.monitoring_dataset = 'prod'
        cmd.limit = 10
        cmd.min_gb = 0.0
        cmd.manifest_path = '/fake/manifest.json'

        parser = MagicMock()
        parser.get_all_models.return_value = {}
        parser.get_model.return_value = None

        monkeypatch.setattr(
            'dbt_meta.command_impl.hotspots.get_cached_parser',
            lambda _p: parser,
        )
        self._patch_all_fetchers(
            monkeypatch,
            # Need at least one table to bypass the early return at line 79
            tables_data=[{
                'dataset_id': 'core',
                'table_id': 'noise',
                'total_gb': '1',
                'partition_count': '0',
            }],
            billing_data=[
                {
                    'dataset_id': 'core',
                    'tables_recommend_physical': '5',
                    'total_tables': '10',
                    'net_savings_eur': '2.5',
                    'recommended_billing': 'PHYSICAL',
                },
            ],
        )

        result = cmd.execute()

        assert len(result['dataset_billing_recommendations']) == 1
        rec = result['dataset_billing_recommendations'][0]
        assert rec['dataset'] == 'core'
        assert rec['tables_recommend_physical'] == 5
        assert rec['net_savings_eur'] == 2.5
