from __future__ import annotations

from datetime import datetime
from typing import Optional
from pydantic import BaseModel, HttpUrl, Field


class NewsItem(BaseModel):
    id: str
    title: str
    summary: Optional[str] = None
    body_text: Optional[str] = None
    published_at: Optional[datetime] = None
    source_url: HttpUrl
    municipality: str = Field(default="Uppsala")


class NewsCreate(BaseModel):
    title: str
    summary: Optional[str] = None
    body_text: Optional[str] = None
    published_at: Optional[datetime] = None
    source_url: HttpUrl
    municipality: str = Field(default="Uppsala")


class SearchResponse(BaseModel):
    total: int
    items: list[NewsItem]