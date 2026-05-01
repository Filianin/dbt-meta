"""Power BI Admin API integration for dbt-meta.

Provides functions to interact with Power BI Admin API using
Service Principal authentication (client_credentials flow).
"""

import json
import re
import shutil
import subprocess
import time
from typing import Optional


def get_powerbi_token(
    tenant_id: str,
    client_id: str,
    client_secret: str,
    timeout: int = 30
) -> Optional[str]:
    """Get OAuth token via client_credentials flow.

    Args:
        tenant_id: Azure AD tenant ID
        client_id: App registration client ID
        client_secret: App registration client secret
        timeout: Request timeout in seconds

    Returns:
        Access token string or None on error
    """
    curl_cmd = shutil.which('curl')
    if not curl_cmd:
        return None

    token_url = f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token"

    try:
        result = subprocess.run(
            [
                curl_cmd,
                '-s',
                '-X', 'POST',
                token_url,
                '-d', f'client_id={client_id}',
                '-d', f'client_secret={client_secret}',
                '-d', 'scope=https://analysis.windows.net/powerbi/api/.default',
                '-d', 'grant_type=client_credentials',
            ],
            capture_output=True,
            text=True,
            timeout=timeout
        )

        if result.returncode != 0:
            return None

        data = json.loads(result.stdout)
        return data.get('access_token')

    except (subprocess.TimeoutExpired, json.JSONDecodeError, FileNotFoundError):
        return None


def _call_powerbi_api(
    token: str,
    endpoint: str,
    method: str = 'GET',
    data: Optional[dict] = None,
    timeout: int = 30
) -> Optional[dict]:
    """Call Power BI Admin API endpoint.

    Args:
        token: OAuth access token
        endpoint: API endpoint (e.g., '/admin/workspaces/getInfo')
        method: HTTP method (GET or POST)
        data: Request body for POST requests
        timeout: Request timeout in seconds

    Returns:
        JSON response as dict or None on error
    """
    curl_cmd = shutil.which('curl')
    if not curl_cmd:
        return None

    base_url = "https://api.powerbi.com/v1.0/myorg"
    url = f"{base_url}{endpoint}"

    cmd = [
        curl_cmd,
        '-s',
        '-X', method,
        '-H', f'Authorization: Bearer {token}',
        '-H', 'Content-Type: application/json',
        url,
    ]

    if data and method == 'POST':
        cmd.extend(['-d', json.dumps(data)])

    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=timeout
        )

        if result.returncode != 0:
            return None

        if not result.stdout.strip():
            return {}

        return json.loads(result.stdout)

    except (subprocess.TimeoutExpired, json.JSONDecodeError, FileNotFoundError):
        return None


def fetch_workspace_scan(
    token: str,
    workspace_id: str,
    poll_interval: int = 2,
    max_polls: int = 30,
    timeout: int = 30
) -> Optional[dict]:
    """Scan workspace for datasets with M-expressions.

    Uses the Admin Scanner API to get detailed workspace metadata
    including M-expressions (Power Query source definitions).

    Args:
        token: OAuth access token
        workspace_id: Power BI workspace ID
        poll_interval: Seconds between status polls
        max_polls: Maximum number of status polls
        timeout: Request timeout in seconds

    Returns:
        Workspace scan result with datasets, reports, expressions
        or None on error
    """
    # Step 1: Start scan
    # datasetSchema=true is required when using datasetExpressions=true
    endpoint = "/admin/workspaces/getInfo?datasetSchema=true&datasetExpressions=true"
    body = {"workspaces": [workspace_id]}

    response = _call_powerbi_api(token, endpoint, method='POST', data=body, timeout=timeout)
    if not response or 'id' not in response:
        return None

    scan_id = response['id']

    # Step 2: Poll for completion
    status_endpoint = f"/admin/workspaces/scanStatus/{scan_id}"

    for _ in range(max_polls):
        status = _call_powerbi_api(token, status_endpoint, timeout=timeout)
        if not status:
            return None

        if status.get('status') == 'Succeeded':
            break
        elif status.get('status') in ('Failed', 'Cancelled'):
            return None

        time.sleep(poll_interval)
    else:
        # Timeout waiting for scan
        return None

    # Step 3: Get results
    result_endpoint = f"/admin/workspaces/scanResult/{scan_id}"
    result = _call_powerbi_api(token, result_endpoint, timeout=timeout)

    if result and 'workspaces' in result and len(result['workspaces']) > 0:
        return result['workspaces'][0]

    return None


def fetch_dataset_refreshes(
    token: str,
    workspace_id: str,
    dataset_id: str,
    top: int = 30,
    timeout: int = 30
) -> list[dict]:
    """Get dataset refresh history.

    Args:
        token: OAuth access token
        workspace_id: Power BI workspace ID
        dataset_id: Dataset ID
        top: Maximum number of refreshes to return
        timeout: Request timeout in seconds

    Returns:
        List of refresh records (newest first)
    """
    endpoint = f"/admin/groups/{workspace_id}/datasets/{dataset_id}/refreshes?$top={top}"
    result = _call_powerbi_api(token, endpoint, timeout=timeout)

    if result and 'value' in result:
        return result['value']

    return []


def extract_tables_from_expressions(scan_result: dict) -> dict[str, dict]:
    """Extract BigQuery tables and metadata from M-expressions.

    Parses Power Query M-expressions to find BigQuery table references.
    Supports both direct query and import mode patterns.

    Args:
        scan_result: Workspace scan result from fetch_workspace_scan

    Returns:
        Dict mapping dataset_name to:
            {
                'id': dataset_id,
                'tables': [schema.table, ...],
                'content_provider_type': 'PbixInDirectQueryMode' or 'PbixInImportMode',
                'configured_by': user email,
                'created_date': ISO datetime,
                'is_refreshable': bool,
                'refresh_schedule': schedule dict or None,
            }
    """
    result: dict[str, dict] = {}

    datasets = scan_result.get('datasets', [])

    for dataset in datasets:
        dataset_name = dataset.get('name', '')
        dataset_id = dataset.get('id', '')
        content_provider_type = dataset.get('contentProviderType', '')
        tables_found: list[str] = []

        # Extract tables from each table's expression
        for table in dataset.get('tables', []):
            for source in table.get('source', []):
                expression = source.get('expression', '')
                if not expression:
                    continue

                # Extract BigQuery tables from M-expression
                tables = _parse_m_expression(expression)
                tables_found.extend(tables)

        # Deduplicate while preserving order
        unique_tables = list(dict.fromkeys(tables_found))

        if unique_tables:
            # Extract refresh schedule (Import mode)
            refresh_schedule = dataset.get('refreshSchedule')
            # Extract DirectQuery refresh schedule
            dq_schedule = dataset.get('directQueryRefreshSchedule')

            result[dataset_name] = {
                'id': dataset_id,
                'tables': unique_tables,
                'content_provider_type': content_provider_type,
                'configured_by': dataset.get('configuredBy', ''),
                'created_date': dataset.get('createdDate', ''),
                'is_refreshable': dataset.get('isRefreshable', False),
                'refresh_schedule': refresh_schedule or dq_schedule,
            }

    return result


def _parse_m_expression(expression: str) -> list[str]:
    """Parse M-expression to extract BigQuery table references.

    Handles patterns like:
    - GoogleBigQuery.Database()[Schema][Table] navigation
    - #"admirals-bi-dwh"{[Name="schema",Kind="Schema"]}[Data]{[Name="table",Kind="Table/View"]}

    Args:
        expression: Power Query M-expression

    Returns:
        List of schema.table strings
    """
    tables: list[str] = []

    # Pattern 1: [Name="schema",Kind="Schema"]...[Name="table",Kind="Table/View"]
    # Captures: schema name and table name from navigation pattern
    pattern1 = r'\{?\[Name="([^"]+)",Kind="Schema"\]\}?\[Data\].*?\{?\[Name="([^"]+)",Kind="(?:Table|View)"\]'
    matches1 = re.findall(pattern1, expression, re.IGNORECASE | re.DOTALL)

    for schema, table in matches1:
        tables.append(f"{schema}.{table}")

    # Pattern 2: Direct schema.table in FROM clause (for native queries)
    # Looks for: FROM `schema.table` or FROM schema.table
    pattern2 = r'FROM\s+[`"]?(\w+)[.](\w+)[`"]?'
    matches2 = re.findall(pattern2, expression, re.IGNORECASE)

    for schema, table in matches2:
        full_name = f"{schema}.{table}"
        if full_name not in tables:
            tables.append(full_name)

    return tables


def extract_measures_from_dataset(dataset: dict) -> dict[str, list[dict]]:
    """Extract measures from dataset.tables[].measures[]

    Returns:
        Dict mapping table_name to list of measures:
        {
            'table_name': [
                {
                    'name': 'Total Installs',
                    'expression': "SUM('installs'[count])",
                    'description': '...',
                    'format_string': '#,0',
                    'is_hidden': False,
                    'references_columns': ['count'],
                    'references_tables': ['installs']
                },
                ...
            ]
        }
    """
    result: dict[str, list[dict]] = {}

    for table in dataset.get('tables', []):
        table_name = table.get('name', '')
        measures = []

        for measure in table.get('measures', []):
            dax_expr = measure.get('expression', '')
            refs = parse_dax_references(dax_expr)

            measures.append({
                'name': measure.get('name', ''),
                'expression': dax_expr,
                'description': measure.get('description', ''),
                'format_string': measure.get('formatString', ''),
                'is_hidden': measure.get('isHidden', False),
                'references_columns': refs['columns'],
                'references_tables': refs['tables'],
            })

        if measures:
            result[table_name] = measures

    return result


def extract_columns_from_dataset(dataset: dict) -> dict[str, list[dict]]:
    """Extract columns from dataset.tables[].columns[]

    Returns:
        Dict mapping table_name to list of columns:
        {
            'table_name': [
                {
                    'name': 'install_date',
                    'data_type': 'DateTime',
                    'is_hidden': False,
                    'format_string': 'yyyy-MM-dd',
                    'sort_by_column': None,
                    'summarize_by': 'none'
                },
                ...
            ]
        }
    """
    result: dict[str, list[dict]] = {}

    for table in dataset.get('tables', []):
        table_name = table.get('name', '')
        columns = []

        for column in table.get('columns', []):
            columns.append({
                'name': column.get('name', ''),
                'data_type': column.get('dataType', ''),
                'is_hidden': column.get('isHidden', False),
                'format_string': column.get('formatString', ''),
                'sort_by_column': column.get('sortByColumn'),
                'summarize_by': column.get('summarizeBy', 'none'),
            })

        if columns:
            result[table_name] = columns

    return result


def parse_dax_references(dax_expression: str) -> dict:
    """Parse DAX expression to extract table and column references

    Patterns to detect:
    - SUM('table'[column])
    - CALCULATE(..., 'table'[column] > 0)
    - RELATED('table'[column])
    - DISTINCTCOUNT('table'[column])

    Returns:
        {
            'tables': ['table1', 'table2'],
            'columns': ['column1', 'column2'],
            'functions': ['SUM', 'CALCULATE']
        }
    """
    # Pattern: 'table'[column] or "table"[column]
    pattern = r"['\"]([^'\"]+)['\"]\[([^\]]+)\]"
    matches = re.findall(pattern, dax_expression)

    tables = list(dict.fromkeys([m[0] for m in matches]))
    columns = list(dict.fromkeys([m[1] for m in matches]))

    # Extract DAX functions (uppercase words followed by parentheses)
    func_pattern = r'\b([A-Z][A-Z0-9]*)\s*\('
    functions = list(dict.fromkeys(re.findall(func_pattern, dax_expression)))

    return {
        'tables': tables,
        'columns': columns,
        'functions': functions
    }


def build_dataset_to_reports_map(scan_result: dict) -> dict[str, list[str]]:
    """Build mapping from dataset ID to report names.

    Args:
        scan_result: Workspace scan result

    Returns:
        Dict mapping dataset_id to list of report names
    """
    result: dict[str, list[str]] = {}

    for report in scan_result.get('reports', []):
        dataset_id = report.get('datasetId')
        report_name = report.get('name', '')

        if dataset_id:
            if dataset_id not in result:
                result[dataset_id] = []
            result[dataset_id].append(report_name)

    return result


def get_workspace_info(token: str, workspace_id: str, timeout: int = 30) -> Optional[dict]:
    """Get basic workspace information.

    Args:
        token: OAuth access token
        workspace_id: Power BI workspace ID
        timeout: Request timeout in seconds

    Returns:
        Workspace info dict with name, id, etc.
    """
    endpoint = f"/admin/groups/{workspace_id}"
    return _call_powerbi_api(token, endpoint, timeout=timeout)


def list_workspaces(token: str, timeout: int = 30) -> list[dict]:
    """List all accessible workspaces.

    Args:
        token: OAuth access token
        timeout: Request timeout in seconds

    Returns:
        List of workspace info dicts
    """
    endpoint = "/admin/groups?$top=5000"
    result = _call_powerbi_api(token, endpoint, timeout=timeout)

    if result and 'value' in result:
        return result['value']

    return []
