import asyncio
from collections import deque
from urllib.parse import urldefrag, urljoin, urlparse

from playwright.async_api import async_playwright

# --- config ---
START_URL = "https://www.faysalbank.com"
MAX_PAGES = 10000
TIMEOUT = 30000  # ms per page
# --------------


def normalise(url: str, base: str) -> str | None:
    url, _ = urldefrag(urljoin(base, url))
    parsed = urlparse(url)
    if parsed.scheme not in ("http", "https"):
        return None
    return parsed._replace(query="").geturl()


def same_domain(url: str, origin: str) -> bool:
    return urlparse(url).netloc == urlparse(origin).netloc


async def crawl(start: str, max_pages: int) -> None:
    queue = deque([start])
    visited = {start}

    async with async_playwright() as p:
        browser = await p.chromium.launch()
        page = await browser.new_page()

        while queue and len(visited) <= max_pages:
            url = queue.popleft()
            try:
                await page.goto(url, wait_until="domcontentloaded", timeout=TIMEOUT)
            except Exception as e:
                print(f"# skip {url} — {e}", flush=True)
                continue

            await page.wait_for_timeout(2000)  # let JS render links
            print(url, flush=True)

            links = await page.eval_on_selector_all(
                "a[href]", "els => els.map(e => e.href)"
            )
            for link in links:
                norm = normalise(link, url)
                if norm and same_domain(norm, start) and norm not in visited:
                    visited.add(norm)
                    queue.append(norm)

        await browser.close()


if __name__ == "__main__":
    asyncio.run(crawl(START_URL, MAX_PAGES))
