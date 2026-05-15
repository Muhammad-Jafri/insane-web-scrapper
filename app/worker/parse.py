from urllib.parse import urljoin

from bs4 import BeautifulSoup

from app.worker.errors import FatalError

_IMAGE_CAP = 20


def parse_html(html: str, base_url: str) -> dict:
    try:
        soup = BeautifulSoup(html, "lxml")
    except Exception as e:
        raise FatalError(f"HTML parse failed: {e}") from e

    title = soup.title.string.strip() if soup.title and soup.title.string else ""
    links = [a["href"] for a in soup.find_all("a", href=True)][:100]
    word_count = len(soup.get_text().split())
    image_urls = [
        urljoin(base_url, img["src"])
        for img in soup.find_all("img", src=True)
        if not img["src"].startswith("data:")
    ][:_IMAGE_CAP]

    return {
        "title": title,
        "links": links,
        "word_count": word_count,
        "image_urls": image_urls,
    }
