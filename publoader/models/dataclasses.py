from datetime import datetime
from typing import Optional

from pydantic.dataclasses import dataclass


@dataclass()
class Manga:
    manga_id: str
    manga_name: str
    manga_language: str
    manga_url: str


@dataclass()
class Chapter:
    chapter_timestamp: datetime
    chapter_language: str

    chapter_expire: Optional[datetime] = None
    chapter_number: Optional[str] = None
    chapter_title: Optional[str] = None
    chapter_volume: Optional[str] = None
    chapter_id: Optional[str] = None
    chapter_url: Optional[str] = None
    md_chapter_id: Optional[str] = None

    manga_id: Optional[str] = None
    md_manga_id: Optional[str] = None

    manga_name: Optional[str] = None
    manga_url: Optional[str] = None

    extension_name: Optional[str] = None
