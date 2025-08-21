from __future__ import annotations

import os
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timezone
from hashlib import sha256
from typing import Iterable, Optional

from .models import NewsCreate, NewsItem


DB_PATH = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data", "news.db")


def _ensure_parent_dir(file_path: str) -> None:
    parent = os.path.dirname(file_path)
    if parent and not os.path.exists(parent):
        os.makedirs(parent, exist_ok=True)


@contextmanager
def get_conn(db_path: str = DB_PATH):
    _ensure_parent_dir(db_path)
    conn = sqlite3.connect(db_path)
    try:
        conn.row_factory = sqlite3.Row
        yield conn
    finally:
        conn.close()


def initialize_schema(db_path: str = DB_PATH) -> None:
    with get_conn(db_path) as conn:
        cur = conn.cursor()
        cur.execute("PRAGMA journal_mode=WAL;")
        cur.execute("PRAGMA foreign_keys=ON;")
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS news (
                id TEXT PRIMARY KEY,
                title TEXT NOT NULL,
                summary TEXT,
                body_text TEXT,
                published_at TEXT,
                source_url TEXT UNIQUE NOT NULL,
                municipality TEXT NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                raw_html TEXT
            );
            """
        )
        cur.execute(
            """
            CREATE VIRTUAL TABLE IF NOT EXISTS news_fts USING fts5(
                title, summary, body_text, content='news', content_rowid='rowid'
            );
            """
        )
        # Triggers to keep FTS in sync
        cur.execute(
            """
            CREATE TRIGGER IF NOT EXISTS news_ai AFTER INSERT ON news BEGIN
              INSERT INTO news_fts(rowid, title, summary, body_text)
              VALUES (new.rowid, new.title, new.summary, new.body_text);
            END;
            """
        )
        cur.execute(
            """
            CREATE TRIGGER IF NOT EXISTS news_ad AFTER DELETE ON news BEGIN
              INSERT INTO news_fts(news_fts, rowid, title, summary, body_text)
              VALUES('delete', old.rowid, old.title, old.summary, old.body_text);
            END;
            """
        )
        cur.execute(
            """
            CREATE TRIGGER IF NOT EXISTS news_au AFTER UPDATE ON news BEGIN
              INSERT INTO news_fts(news_fts, rowid, title, summary, body_text)
              VALUES('delete', old.rowid, old.title, old.summary, old.body_text);
              INSERT INTO news_fts(rowid, title, summary, body_text)
              VALUES (new.rowid, new.title, new.summary, new.body_text);
            END;
            """
        )
        conn.commit()


def _hash_id_from_url(url: str) -> str:
    return sha256(url.encode("utf-8")).hexdigest()[:24]


def upsert_news(items: Iterable[NewsCreate], db_path: str = DB_PATH) -> int:
    now = datetime.now(timezone.utc).isoformat()
    inserted_or_updated = 0
    with get_conn(db_path) as conn:
        cur = conn.cursor()
        for item in items:
            news_id = _hash_id_from_url(str(item.source_url))
            cur.execute(
                """
                INSERT INTO news (id, title, summary, body_text, published_at, source_url, municipality, created_at, updated_at, raw_html)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, NULL)
                ON CONFLICT(id) DO UPDATE SET
                    title=excluded.title,
                    summary=excluded.summary,
                    body_text=excluded.body_text,
                    published_at=excluded.published_at,
                    municipality=excluded.municipality,
                    updated_at=excluded.updated_at
                ;
                """,
                (
                    news_id,
                    item.title,
                    item.summary,
                    item.body_text,
                    item.published_at.isoformat() if item.published_at else None,
                    str(item.source_url),
                    item.municipality,
                    now,
                    now,
                ),
            )
            inserted_or_updated += 1
        conn.commit()
    return inserted_or_updated


def list_news(limit: int = 20, offset: int = 0, db_path: str = DB_PATH) -> list[NewsItem]:
    with get_conn(db_path) as conn:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT id, title, summary, body_text, published_at, source_url, municipality
            FROM news
            ORDER BY (published_at IS NULL) ASC, published_at DESC, updated_at DESC
            LIMIT ? OFFSET ?
            """,
            (limit, offset),
        )
        rows = cur.fetchall()
        items: list[NewsItem] = []
        for r in rows:
            items.append(
                NewsItem(
                    id=r["id"],
                    title=r["title"],
                    summary=r["summary"],
                    body_text=r["body_text"],
                    published_at=datetime.fromisoformat(r["published_at"]) if r["published_at"] else None,
                    source_url=r["source_url"],
                    municipality=r["municipality"],
                )
            )
        return items


def search_news(query: str, limit: int = 20, offset: int = 0, db_path: str = DB_PATH) -> tuple[int, list[NewsItem]]:
    with get_conn(db_path) as conn:
        cur = conn.cursor()
        # Count
        cur.execute(
            """
            SELECT count(*) FROM news_fts WHERE news_fts MATCH ?
            """,
            (query,),
        )
        total = int(cur.fetchone()[0])
        # Search with rank (avoid alias for FTS table in MATCH/bm25)
        cur.execute(
            """
            SELECT n.id, n.title, n.summary, n.body_text, n.published_at, n.source_url, n.municipality
            FROM news_fts
            JOIN news n ON n.rowid = news_fts.rowid
            WHERE news_fts MATCH ?
            ORDER BY bm25(news_fts)
            LIMIT ? OFFSET ?
            """,
            (query, limit, offset),
        )
        rows = cur.fetchall()
        items: list[NewsItem] = []
        for r in rows:
            items.append(
                NewsItem(
                    id=r["id"],
                    title=r["title"],
                    summary=r["summary"],
                    body_text=r["body_text"],
                    published_at=datetime.fromisoformat(r["published_at"]) if r["published_at"] else None,
                    source_url=r["source_url"],
                    municipality=r["municipality"],
                )
            )
        return total, items