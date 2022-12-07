from dataclasses import dataclass, field
from typing import Optional

from .utils import mplus_language_map


@dataclass(order=True)
class Manga:
    manga_id: int
    manga_name: str
    manga_language: str

    def __post_init__(self):
        language = self.manga_language
        try:
            language = int(language)
        except ValueError:
            pass
        else:
            self.manga_language = mplus_language_map.get(str(language), "NULL")


@dataclass()
class Chapter:
    chapter_timestamp: int
    chapter_expire: int
    chapter_title: str
    chapter_number: str
    chapter_language: str
    chapter_volume: Optional[str] = field(default=None)
    chapter_id: Optional[int] = field(default=None)
    md_chapter_id: Optional[str] = field(default=None)
    manga_id: Optional[int] = field(default=None)
    md_manga_id: Optional[str] = field(default=None)
    manga: Optional[Manga] = field(default=None)

    def __post_init__(self):
        language = self.chapter_language
        try:
            language = int(language)
        except ValueError:
            pass
        else:
            self.chapter_language = mplus_language_map.get(str(language), "NULL")
