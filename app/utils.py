from __future__ import annotations

from bs4 import BeautifulSoup
from dateutil import parser as date_parser
from datetime import datetime, timezone
from typing import Optional
import re


def parse_date(value: str | None) -> Optional[datetime]:
    if not value:
        return None
    try:
        dt = date_parser.parse(value)
        if not dt.tzinfo:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except Exception:
        return None


def html_to_text(html: str | None) -> Optional[str]:
    if not html:
        return None
    soup = BeautifulSoup(html, "lxml")
    text = soup.get_text(" ")
    text = re.sub(r"\s+", " ", text).strip()
    return text or None