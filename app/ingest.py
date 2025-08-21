from __future__ import annotations

import argparse

from .db import initialize_schema, upsert_news
from .scraper import UppsalaNewsScraper


def main() -> None:
    parser = argparse.ArgumentParser(description="Ingest Uppsala municipality news")
    parser.add_argument("--limit", type=int, default=20, help="Max articles to ingest")
    args = parser.parse_args()

    initialize_schema()
    scraper = UppsalaNewsScraper()
    items = scraper.run(limit=args.limit)
    count = upsert_news(items)
    print(f"Ingested {count} items")


if __name__ == "__main__":
    main()