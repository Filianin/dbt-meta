"""Power BI command for dbt-meta.

Extracts BigQuery table usage from Power BI dashboards and maps
them to dbt models. Shows hierarchy: Workspace -> Dataset -> Reports -> Tables.
"""

from typing import Any, Optional

from dbt_meta.config import Config
from dbt_meta.errors import DbtMetaError
from dbt_meta.utils import get_cached_parser
from dbt_meta.utils.powerbi import (
    build_dataset_to_reports_map,
    extract_columns_from_dataset,
    extract_measures_from_dataset,
    extract_tables_from_expressions,
    fetch_workspace_scan,
    get_powerbi_token,
)


class PowerBiCommand:
    """Extract Power BI dashboard to BigQuery table mappings.

    Scans Power BI workspace to extract:
    - Datasets and their BigQuery table dependencies
    - Reports using each dataset
    - Refresh statistics for each dataset
    - Mapping to dbt model names via manifest

    Output hierarchy: Dataset -> Reports -> Tables
    """

    def __init__(
        self,
        config: Config,
        manifest_path: str,
        workspace_id: Optional[str] = None,
        json_output: bool = False,
        show_measures: bool = False,
        show_columns: bool = False,
        show_full: bool = False,
        by_table: bool = False,
    ):
        """Initialize Power BI command.

        Args:
            config: Configuration with Power BI credentials
            manifest_path: Path to dbt manifest.json
            workspace_id: Power BI workspace ID (or use first from config)
            json_output: Output in JSON format
            show_measures: Include measures with DAX expressions
            show_columns: Include column schemas
            show_full: Include all metadata (measures + columns)
            by_table: Group by tables instead of datasets
        """
        self.config = config
        self.manifest_path = manifest_path
        self.workspace_id = workspace_id
        self.json_output = json_output
        self.by_table = by_table

        # JSON mode always gets full metadata
        self.show_measures = show_measures or show_full or json_output
        self.show_columns = show_columns or show_full or json_output

    def execute(self) -> dict:
        """Execute Power BI analysis.

        Returns:
            Dict with workspace info, datasets, tables, and summary

        Raises:
            DbtMetaError: If Power BI is not configured or disabled
        """
        # Check if Power BI is enabled
        if not self.config.powerbi_enabled:
            raise DbtMetaError(
                "Power BI integration is disabled",
                suggestion="Set powerbi.enabled = true in config or POWERBI_ENABLED=true"
            )

        # Validate credentials
        if not all([
            self.config.powerbi_tenant_id,
            self.config.powerbi_client_id,
            self.config.powerbi_client_secret,
        ]):
            raise DbtMetaError(
                "Power BI credentials not configured",
                suggestion="Set powerbi.tenant_id, client_id, client_secret in config"
            )

        # Resolve workspace ID
        workspace_id = self._resolve_workspace_id()

        # Get OAuth token
        token = get_powerbi_token(
            tenant_id=self.config.powerbi_tenant_id,
            client_id=self.config.powerbi_client_id,
            client_secret=self.config.powerbi_client_secret,
        )

        if not token:
            raise DbtMetaError(
                "Failed to obtain Power BI access token",
                suggestion="Check tenant_id, client_id, client_secret values"
            )

        # Scan workspace
        scan_result = fetch_workspace_scan(token, workspace_id)
        if not scan_result:
            raise DbtMetaError(
                f"Failed to scan workspace {workspace_id}",
                suggestion="Check workspace ID and service principal permissions"
            )

        # Get workspace name
        workspace_name = scan_result.get('name', workspace_id)

        # Extract tables from M-expressions
        dataset_tables = extract_tables_from_expressions(scan_result)

        # Build dataset -> reports mapping
        dataset_to_reports = build_dataset_to_reports_map(scan_result)

        # Build reverse model lookup
        parser = get_cached_parser(self.manifest_path)
        reverse_lookup = self._build_reverse_model_lookup(parser)

        # Build result structure
        datasets_result = []
        total_tables = 0
        tables_in_manifest = 0
        tables_not_in_manifest = 0

        for dataset_name, dataset_info in dataset_tables.items():
            dataset_id = dataset_info['id']
            tables = dataset_info['tables']
            content_provider_type = dataset_info['content_provider_type']

            # Get reports using this dataset
            reports = dataset_to_reports.get(dataset_id, [])

            # Determine mode from contentProviderType
            mode = 'DirectQuery' if 'DirectQuery' in (content_provider_type or '') else 'Import'

            # Parse refresh schedule
            refresh_info = self._parse_refresh_schedule(dataset_info.get('refresh_schedule'))

            # Extract measures if requested
            measures_by_table = {}
            if self.show_measures:
                # Find full dataset object from scan_result
                dataset_obj = next(
                    (ds for ds in scan_result.get('datasets', [])
                     if ds.get('name') == dataset_name),
                    None
                )
                if dataset_obj:
                    measures_by_table = extract_measures_from_dataset(dataset_obj)

            # Extract columns if requested
            columns_by_table = {}
            if self.show_columns:
                dataset_obj = next(
                    (ds for ds in scan_result.get('datasets', [])
                     if ds.get('name') == dataset_name),
                    None
                )
                if dataset_obj:
                    columns_by_table = extract_columns_from_dataset(dataset_obj)

            # Map tables to dbt models
            tables_result = []
            for table in tables:
                dbt_model = reverse_lookup.get(table)
                in_manifest = dbt_model is not None

                table_result = {
                    'bigquery_table': table,
                    'dbt_model': dbt_model,
                    'in_manifest': in_manifest,
                }

                # Add measures if available
                pbi_table_name = self._extract_table_name(table)
                if pbi_table_name in measures_by_table:
                    table_result['measures'] = measures_by_table[pbi_table_name]

                # Add columns if available
                if pbi_table_name in columns_by_table:
                    table_result['columns'] = columns_by_table[pbi_table_name]

                tables_result.append(table_result)

                total_tables += 1
                if in_manifest:
                    tables_in_manifest += 1
                else:
                    tables_not_in_manifest += 1

            datasets_result.append({
                'name': dataset_name,
                'id': dataset_id,
                'reports': reports,
                'tables': tables_result,
                'mode': mode,
                'configured_by': dataset_info.get('configured_by', ''),
                'refresh_schedule': refresh_info,
            })

        # Sort by name for consistent output
        datasets_result.sort(key=lambda x: x['name'])

        # Calculate total reports
        total_reports = sum(len(d['reports']) for d in datasets_result)

        # If by_table mode, aggregate tables usage
        if self.by_table:
            tables_aggregated = self._aggregate_tables_usage(datasets_result, reverse_lookup)
            return {
                'workspace': workspace_name,
                'workspace_id': workspace_id,
                'view': 'by_table',
                'tables': tables_aggregated,
                'summary': {
                    'total_tables': total_tables,
                    'total_reports': total_reports,
                    'total_datasets': len(datasets_result),
                    'tables_in_manifest': tables_in_manifest,
                    'tables_not_in_manifest': tables_not_in_manifest,
                },
            }

        # Default dataset-centric view
        return {
            'workspace': workspace_name,
            'workspace_id': workspace_id,
            'datasets': datasets_result,
            'summary': {
                'total_datasets': len(datasets_result),
                'total_reports': total_reports,
                'total_tables': total_tables,
                'tables_in_manifest': tables_in_manifest,
                'tables_not_in_manifest': tables_not_in_manifest,
            },
        }

    def _resolve_workspace_id(self) -> str:
        """Resolve workspace ID from argument or config.

        Returns:
            Workspace ID

        Raises:
            DbtMetaError: If no workspace ID available
        """
        if self.workspace_id:
            return self.workspace_id

        if self.config.powerbi_workspaces:
            return self.config.powerbi_workspaces[0]

        raise DbtMetaError(
            "No Power BI workspace ID specified",
            suggestion="Provide workspace_id or set powerbi.workspaces in config"
        )

    def _build_reverse_model_lookup(self, parser: Any) -> dict:
        """Build reverse lookup map: schema.table -> model_name.

        Args:
            parser: Manifest parser

        Returns:
            Dict mapping schema.table to dbt model name
        """
        result = {}
        all_models = parser.get_all_models()

        for unique_id, model in all_models.items():
            model_name = unique_id.split('.')[-1]
            schema = model.get('schema', '')
            alias = model.get('alias') or model.get('name', '')
            if schema and alias:
                key = f"{schema}.{alias}"
                result[key] = model_name

        return result

    def _parse_refresh_schedule(self, schedule: Optional[dict]) -> Optional[dict]:
        """Parse refresh schedule from Power BI API response.

        Args:
            schedule: Raw refresh schedule from API

        Returns:
            Parsed schedule with frequency and times, or None if no schedule
        """
        if not schedule:
            return None

        # Power BI refresh schedule structure:
        # {
        #   "enabled": true,
        #   "days": ["Sunday", "Monday", ...],
        #   "times": ["07:00", "19:00"],
        #   "localTimeZoneId": "UTC",
        #   "notifyOption": "NoNotification"
        # }

        enabled = schedule.get('enabled', False)
        if not enabled:
            return {'enabled': False, 'frequency': 'disabled'}

        days = schedule.get('days', [])
        times = schedule.get('times', [])

        # Calculate frequency
        refreshes_per_day = len(times) if times else 0
        days_per_week = len(days) if days else 7  # If no days specified, assume daily

        # Frequency description
        if days_per_week == 7:
            if refreshes_per_day == 1:
                frequency = 'daily'
            elif refreshes_per_day > 1:
                frequency = f'{refreshes_per_day}x daily'
            else:
                frequency = 'daily'
        else:
            frequency = f'{refreshes_per_day}x on {days_per_week} days/week'

        return {
            'enabled': True,
            'frequency': frequency,
            'days': days,
            'times': times,
            'timezone': schedule.get('localTimeZoneId', 'UTC'),
        }

    def _extract_table_name(self, bigquery_table: str) -> str:
        """Extract Power BI table name from BigQuery schema.table

        Args:
            bigquery_table: BigQuery table in schema.table format

        Returns:
            Table name (last part after dot)
        """
        # Simple heuristic: use table part
        # May need refinement based on actual Power BI naming
        return bigquery_table.split('.')[-1]

    def _aggregate_tables_usage(
        self,
        datasets_result: list[dict[str, Any]],
        reverse_lookup: dict[str, str]
    ) -> list[dict[str, Any]]:
        """Aggregate table usage across datasets and reports.

        Args:
            datasets_result: List of datasets with tables and reports
            reverse_lookup: Schema.table -> dbt_model mapping

        Returns:
            List of table usage records sorted by report count DESC:
            [
                {
                    'bigquery_table': 'schema.table',
                    'dbt_model': 'model_name',
                    'in_manifest': bool,
                    'report_count': int,
                    'dataset_count': int,
                    'datasets': [dataset_names],
                    'reports': [report_names]
                }
            ]
        """
        # Build aggregation dict
        table_usage = {}

        for dataset in datasets_result:
            dataset_name = dataset['name']
            reports = dataset['reports']

            for table in dataset['tables']:
                bq_table = table['bigquery_table']

                # Initialize if first occurrence
                if bq_table not in table_usage:
                    table_usage[bq_table] = {
                        'bigquery_table': bq_table,
                        'dbt_model': table['dbt_model'],
                        'in_manifest': table['in_manifest'],
                        'datasets': set(),
                        'reports': set(),
                    }

                # Aggregate datasets and reports
                table_usage[bq_table]['datasets'].add(dataset_name)
                table_usage[bq_table]['reports'].update(reports)

        # Convert to list with counts
        result = []
        for bq_table, usage in table_usage.items():
            result.append({
                'bigquery_table': bq_table,
                'dbt_model': usage['dbt_model'],
                'in_manifest': usage['in_manifest'],
                'report_count': len(usage['reports']),
                'dataset_count': len(usage['datasets']),
                'datasets': sorted(usage['datasets']),
                'reports': sorted(usage['reports']),
            })

        # Sort by report_count DESC, then bigquery_table ASC
        result.sort(key=lambda x: (-x['report_count'], x['bigquery_table']))

        return result
