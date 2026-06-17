"""Power BI Admin API low-level boundary for dbt-meta.

Service-Principal auth (client_credentials) and a thin ``curl`` wrapper over the
Admin API. The higher-level Scanner flow lives in
:mod:`dbt_meta.powerbi.scanner`; M-expression / SQL / index logic lives in the
:mod:`dbt_meta.powerbi` package.
"""

import json
import shutil
import subprocess
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
