from __future__ import annotations

from fastapi import FastAPI, Query, BackgroundTasks
from typing import Optional

from .db import initialize_schema, list_news, search_news, upsert_news
from .models import NewsItem, SearchResponse
from .scraper import UppsalaNewsScraper

app = FastAPI(title="Kommun News API", version="0.1.0")


@app.on_event("startup")
def _startup() -> None:
    initialize_schema()


@app.get("/news", response_model=list[NewsItem])
def get_news(limit: int = 20, offset: int = 0):
    return list_news(limit=limit, offset=offset)


@app.get("/search", response_model=SearchResponse)
def get_search(q: str = Query(..., min_length=1), limit: int = 20, offset: int = 0):
    total, items = search_news(q, limit=limit, offset=offset)
    return SearchResponse(total=total, items=items)


@app.post("/ingest")
def post_ingest(background: BackgroundTasks, limit: Optional[int] = 20):
    def job():
        scraper = UppsalaNewsScraper()
        items = scraper.run(limit=limit)
        upsert_news(items)

    background.add_task(job)
    return {"status": "started"}

