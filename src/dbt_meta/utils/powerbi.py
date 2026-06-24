"""Power BI Admin API low-level boundary for dbt-meta.

Service-Principal auth (client_credentials) and a thin ``curl`` wrapper over the
Admin API. The higher-level Scanner flow lives in
:mod:`dbt_meta.powerbi.scanner`; M-expression / SQL / index logic lives in the
:mod:`dbt_meta.powerbi` package.
"""

import base64
import contextlib
import json
import shutil
import subprocess
import time
import urllib.parse
from typing import Any, Optional

# OAuth scopes. The Scanner Admin API takes the Power BI audience; the Fabric
# data-plane (getDefinition) rejects it with 401 and insists on its own.
_SCOPE_POWERBI = "https://analysis.windows.net/powerbi/api/.default"
_SCOPE_FABRIC = "https://api.fabric.microsoft.com/.default"
_FABRIC_BASE = "https://api.fabric.microsoft.com/v1"

# Marker appended by ``-w`` so the HTTP status code can be split off the body
# of a header-capturing curl call.
_HTTP_MARK = "\n__HTTP_CODE__:"


def _acquire_token(
    tenant_id: str,
    client_id: str,
    client_secret: str,
    scope: str,
    timeout: int = 30,
) -> Optional[str]:
    """client_credentials token for an explicit scope. Secret via stdin (not argv)."""
    curl_cmd = shutil.which('curl')
    if not curl_cmd:
        return None

    token_url = f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token"
    body = urllib.parse.urlencode(
        {
            'client_id': client_id,
            'client_secret': client_secret,
            'scope': scope,
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


def get_powerbi_token(
    tenant_id: str,
    client_id: str,
    client_secret: str,
    timeout: int = 30
) -> Optional[str]:
    """Get a Power BI Admin API OAuth token via client_credentials.

    The request body (including ``client_secret``) is piped to curl over stdin
    via ``--data @-`` so the secret never appears in the process argv — argv is
    world-readable through ``ps``.

    Returns:
        Access token string or None on error
    """
    return _acquire_token(
        tenant_id, client_id, client_secret, _SCOPE_POWERBI, timeout
    )


def get_fabric_token(
    tenant_id: str,
    client_id: str,
    client_secret: str,
    timeout: int = 30,
) -> Optional[str]:
    """Get a Fabric data-plane OAuth token (separate audience from the Scanner).

    Needed for the ``getDefinition`` layout pass; the Power BI Admin token 401s
    against ``api.fabric.microsoft.com``. Secret goes through stdin, not argv.
    """
    return _acquire_token(
        tenant_id, client_id, client_secret, _SCOPE_FABRIC, timeout
    )


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


def _fabric_call(
    token: str,
    url: str,
    method: str = 'GET',
    body: Optional[dict[str, Any]] = None,
    timeout: int = 90,
) -> tuple[int, dict[str, str], str]:
    """Call a Fabric REST endpoint, returning ``(status_code, headers, body)``.

    Unlike :func:`_call_powerbi_api` this captures response headers (``-i``) and
    the status code (``-w``) — the LRO ``Location`` header and ``202`` status
    live there. The bearer token is fed through a curl config on stdin (``-K -``)
    so it stays out of argv.
    """
    curl_cmd = shutil.which('curl')
    if not curl_cmd:
        return 0, {}, ""

    safe_token = token.replace('\\', '\\\\').replace('"', '\\"')
    curl_config = f'header = "Authorization: Bearer {safe_token}"\n'

    cmd = [
        curl_cmd,
        '-s', '-i',
        '-w', _HTTP_MARK + '%{http_code}',
        '-X', method,
        '-H', 'Content-Type: application/json',
        '-K', '-',
        url,
    ]
    if body is not None and method == 'POST':
        cmd.extend(['-d', json.dumps(body)])

    try:
        result = subprocess.run(
            cmd, input=curl_config, capture_output=True, text=True, timeout=timeout
        )
    except (subprocess.TimeoutExpired, FileNotFoundError):
        return 0, {}, ""
    if result.returncode != 0:
        return 0, {}, ""

    out = result.stdout
    code = 0
    if _HTTP_MARK in out:
        out, _, code_str = out.rpartition(_HTTP_MARK)
        code = int(code_str.strip() or 0)

    # Split the (possibly multiple, due to redirects) header blocks from the
    # body; keep the last header block before the body.
    head_part, _, body_part = out.rpartition("\r\n\r\n")
    if not head_part:
        head_part, _, body_part = out.rpartition("\n\n")

    headers: dict[str, str] = {}
    for line in head_part.splitlines():
        if ":" in line and not line.startswith("HTTP/"):
            k, _, v = line.partition(":")
            headers[k.strip().lower()] = v.strip()

    return code, headers, body_part


def _decode_report_json(definition: dict[str, Any]) -> Optional[dict[str, Any]]:
    """Pull the base64 ``report.json`` part out of a getDefinition payload."""
    part = next(
        (
            p
            for p in definition.get("parts", [])
            if (p.get("path") or "").endswith("report.json")
        ),
        None,
    )
    if not part or not part.get("payload"):
        return None
    try:
        raw = base64.b64decode(part["payload"]).decode("utf-8", "replace")
        parsed: dict[str, Any] = json.loads(raw)
        return parsed
    except (ValueError, json.JSONDecodeError):
        return None


def get_report_definition(
    token: str,
    workspace_id: str,
    report_id: str,
    timeout: int = 90,
    max_polls: int = 15,
    poll_interval: int = 8,
) -> Optional[dict[str, Any]]:
    """Fetch a report's PBIR-Legacy ``report.json`` via Fabric getDefinition.

    Drives the long-running operation (``202`` → poll ``Location`` →
    ``/result``) and returns the decoded classic layout dict, or ``None`` on any
    failure (HTTP error, LRO failure/timeout, no ``report.json`` part). Never
    raises — a failed report is a skipped report, not a fatal error, so the
    caller's scan path stays intact.

    ``token`` must carry the Fabric audience (see :func:`get_fabric_token`).
    """
    url = (
        f"{_FABRIC_BASE}/workspaces/{workspace_id}/reports/{report_id}"
        f"/getDefinition?format=PBIR-Legacy"
    )
    code, headers, body = _fabric_call(token, url, method="POST", body={}, timeout=timeout)

    definition: Optional[dict[str, Any]] = None
    if code == 200:
        try:
            definition = json.loads(body or "{}").get("definition")
        except json.JSONDecodeError:
            return None
    elif code == 202:
        location = headers.get("location")
        retry = poll_interval
        with contextlib.suppress(ValueError):
            retry = min(int(headers.get("retry-after", str(poll_interval))), poll_interval)
        if not location:
            return None
        for _ in range(max_polls):
            time.sleep(retry)
            _, _, sbody = _fabric_call(token, location, timeout=timeout)
            try:
                data = json.loads(sbody or "{}")
            except json.JSONDecodeError:
                return None
            status = data.get("status", "?")
            if status == "Succeeded":
                _, _, rbody = _fabric_call(
                    token, location.rstrip("/") + "/result", timeout=timeout
                )
                try:
                    definition = json.loads(rbody or "{}").get("definition")
                except json.JSONDecodeError:
                    return None
                break
            if status == "Failed":
                return None
        else:
            return None
    else:
        return None

    if not definition:
        return None
    return _decode_report_json(definition)
