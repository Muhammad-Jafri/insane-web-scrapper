from bs4 import BeautifulSoup

from app.worker.errors import FatalError


def parse_html(html: str) -> dict:
    try:
        soup = BeautifulSoup(html, "lxml")
    except Exception as e:
        raise FatalError(f"HTML parse failed: {e}") from e

    title = soup.title.string.strip() if soup.title and soup.title.string else ""
    links = [a["href"] for a in soup.find_all("a", href=True)][:100]
    word_count = len(soup.get_text().split())

    return {"title": title, "links": links, "word_count": word_count}
