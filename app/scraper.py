from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, List
import httpx
from bs4 import BeautifulSoup
from urllib.parse import urljoin

from .models import NewsCreate
from .utils import parse_date, html_to_text


DEFAULT_BASE = "https://www.uppsala.se/"
CANDIDATE_LIST_PATHS = [
    "kommun-och-politik/nyheter-och-pressmeddelanden/",
    "kommun-och-politik/press-och-nyheter/nyheter/",
    "om-uppsala/nyheter/",
    "nyheter/",
]


@dataclass
class UppsalaNewsScraper:
    base_url: str = DEFAULT_BASE
    list_paths: list[str] = None

    def __post_init__(self):
        if self.list_paths is None:
            self.list_paths = CANDIDATE_LIST_PATHS

    def fetch(self, url: str) -> str:
        headers = {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0 Safari/537.36"
            ),
            "Accept-Language": "sv-SE,sv;q=0.9,en;q=0.8",
        }
        with httpx.Client(timeout=20.0, follow_redirects=True, headers=headers) as client:
            resp = client.get(url)
            resp.raise_for_status()
            return resp.text

    def resolve_list_url(self) -> str:
        last_error: Exception | None = None
        for path in self.list_paths:
            url = urljoin(self.base_url, path)
            try:
                html = self.fetch(url)
                # quick heuristic: must contain anchor with /nyheter/
                if "/nyheter/" in html or "news" in html.lower():
                    return url
            except Exception as exc:
                last_error = exc
                continue
        if last_error:
            raise last_error
        raise RuntimeError("Could not resolve a working list URL")

    def parse_list(self, html: str) -> List[str]:
        soup = BeautifulSoup(html, "lxml")
        links: List[str] = []

        def is_article_url(href: str) -> bool:
            if not href:
                return False
            full = urljoin(self.base_url, href)
            return "/kommun-och-politik/nyheter-och-pressmeddelanden/" in full or "/nyheter" in full

        for selector in [
            ".c-card a",
            "article a",
            "a.c-article-card__link",
            ".news-list a",
        ]:
            for a in soup.select(selector):
                href = a.get("href")
                if not href:
                    continue
                full = urljoin(self.base_url, href)
                if full not in links and is_article_url(href):
                    links.append(full)
        if not links:
            for a in soup.find_all("a", href=True):
                href = a["href"]
                full = urljoin(self.base_url, href)
                if is_article_url(href) and full not in links:
                    links.append(full)
        return links

    def parse_article(self, url: str, html: str) -> NewsCreate:
        soup = BeautifulSoup(html, "lxml")
        title_el = soup.find("h1") or soup.select_one(".c-article__title, .page-title")
        title = title_el.get_text(strip=True) if title_el else url
        date_text = None
        time_el = soup.find("time") or soup.select_one(".c-article__date, .published, .date")
        if time_el:
            date_text = time_el.get("datetime") or time_el.get_text(strip=True)
        published_at = parse_date(date_text)
        body_container = (
            soup.select_one(".c-rich-text, .c-article__content, article, main") or soup
        )
        for tag in body_container.select("nav, aside, footer, script, style"):
            tag.extract()
        paragraphs = [p.get_text(" ", strip=True) for p in body_container.find_all("p")]
        body_text = "\n\n".join([p for p in paragraphs if p]) or html_to_text(str(body_container))
        summary_el = soup.select_one(".ingress, .lead, .c-article__lead")
        summary = summary_el.get_text(" ", strip=True) if summary_el else None

        return NewsCreate(
            title=title,
            summary=summary,
            body_text=body_text,
            published_at=published_at,
            source_url=url,
            municipality="Uppsala",
        )

    def run(self, limit: int | None = 20) -> Iterable[NewsCreate]:
        list_url = self.resolve_list_url()
        list_html = self.fetch(list_url)
        links = self.parse_list(list_html)
        if limit:
            links = links[:limit]
        results: List[NewsCreate] = []
        for url in links:
            try:
                article_html = self.fetch(url)
                item = self.parse_article(url, article_html)
                results.append(item)
            except Exception:
                continue
        return results