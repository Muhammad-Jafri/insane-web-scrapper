import logging

import httpx

from app.worker.errors import FatalError, RetryableError

logger = logging.getLogger("app.worker.fetch")

_RETRYABLE_CODES = {429, 500, 502, 503}
_FATAL_CODES = {403, 404, 410}


async def fetch_url(client: httpx.AsyncClient, url: str) -> str:
    try:
        response = await client.get(url, follow_redirects=True)
    except httpx.TimeoutException as e:
        logger.warning("fetch timeout", extra={"url": url, "error": str(e)})
        raise RetryableError(f"Timeout: {e}") from e
    except httpx.ConnectError as e:
        logger.warning("fetch connection error", extra={"url": url, "error": str(e)})
        raise RetryableError(f"Connection error: {e}") from e

    if response.status_code in _RETRYABLE_CODES:
        logger.warning(
            "retryable http error",
            extra={"url": url, "status_code": response.status_code},
        )
        raise RetryableError(f"HTTP {response.status_code}")
    if response.status_code in _FATAL_CODES or response.status_code >= 400:
        logger.error(
            "fatal http error",
            extra={"url": url, "status_code": response.status_code},
        )
        raise FatalError(f"HTTP {response.status_code}")

    return response.text
