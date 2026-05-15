import httpx

from app.worker.errors import FatalError, RetryableError

_RETRYABLE_CODES = {429, 500, 502, 503}
_FATAL_CODES = {403, 404, 410}


async def fetch_url(client: httpx.AsyncClient, url: str) -> str:
    try:
        response = await client.get(url, follow_redirects=True)
    except httpx.TimeoutException as e:
        raise RetryableError(f"Timeout: {e}") from e
    except httpx.ConnectError as e:
        raise RetryableError(f"Connection error: {e}") from e

    if response.status_code in _RETRYABLE_CODES:
        raise RetryableError(f"HTTP {response.status_code}")
    if response.status_code in _FATAL_CODES or response.status_code >= 400:
        raise FatalError(f"HTTP {response.status_code}")

    return response.text
