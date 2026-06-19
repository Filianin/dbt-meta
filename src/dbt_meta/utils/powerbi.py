"""Power BI Admin API low-level boundary for dbt-meta.

Service-Principal auth (client_credentials) and a thin ``curl`` wrapper over the
Admin API. The higher-level Scanner flow lives in
:mod:`dbt_meta.powerbi.scanner`; M-expression / SQL / index logic lives in the
:mod:`dbt_meta.powerbi` package.
"""

import json
import shutil
import subprocess
import urllib.parse
from typing import Any, Optional


def get_powerbi_token(
    tenant_id: str,
    client_id: str,
    client_secret: str,
    timeout: int = 30
) -> Optional[str]:
    """Get OAuth token via client_credentials flow.

    The request body (including ``client_secret``) is piped to curl over stdin
    via ``--data @-`` so the secret never appears in the process argv — argv is
    world-readable through ``ps``.

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
    body = urllib.parse.urlencode(
        {
            'client_id': client_id,
            'client_secret': client_secret,
            'scope': 'https://analysis.windows.net/powerbi/api/.default',
            'grant_type': 'client_credentials',
        }
    )

    try:
        result = subprocess.run(
            [
                curl_cmd,
                '-s',
                '-X', 'POST',
                token_url,
                '--data', '@-',
            ],
            input=body,
            capture_output=True,
            text=True,
            timeout=timeout
        )

        if result.returncode != 0:
            return None

        data: dict[str, Any] = json.loads(result.stdout)
        token: Optional[str] = data.get('access_token')
        return token

    except (subprocess.TimeoutExpired, json.JSONDecodeError, FileNotFoundError):
        return None


def _call_powerbi_api(
    token: str,
    endpoint: str,
    method: str = 'GET',
    data: Optional[dict[str, Any]] = None,
    timeout: int = 30
) -> Optional[dict[str, Any]]:
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

    # The bearer token is the sensitive bit — feed it to curl through a config
    # read from stdin (``-K -``) so it stays out of the process argv (visible via
    # ``ps``). Backslashes and quotes are escaped for the curl config syntax;
    # JWTs don't contain them, but the escaping keeps this safe regardless.
    safe_token = token.replace('\\', '\\\\').replace('"', '\\"')
    curl_config = f'header = "Authorization: Bearer {safe_token}"\n'

    cmd = [
        curl_cmd,
        '-s',
        '-X', method,
        '-H', 'Content-Type: application/json',
        '-K', '-',
        url,
    ]

    if data and method == 'POST':
        cmd.extend(['-d', json.dumps(data)])

    try:
        result = subprocess.run(
            cmd,
            input=curl_config,
            capture_output=True,
            text=True,
            timeout=timeout
        )

        if result.returncode != 0:
            return None

        if not result.stdout.strip():
            return {}

        parsed: dict[str, Any] = json.loads(result.stdout)
        return parsed

    except (subprocess.TimeoutExpired, json.JSONDecodeError, FileNotFoundError):
        return None
