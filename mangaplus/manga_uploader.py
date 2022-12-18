import logging
import time
from typing import TYPE_CHECKING, Dict, List

from . import Chapter, mplus_language_map
from .chapter_uploader import ChapterUploaderProcess
from .webhook import MPlusBotUpdatesWebhook
from .utils.database import update_expired_chapter_database
from .utils.helpter_functions import fetch_aggregate

if TYPE_CHECKING:
    import sqlite3
    from .chapter_deleter import ChapterDeleterProcess
    from .utils.http import HTTPClient


logger = logging.getLogger("mangaplus")


class MangaUploaderProcess:
    def __init__(
        self,
        database_connection: "sqlite3.Connection",
        http_client: "HTTPClient",
        updated_chapters: List[Chapter],
        all_manga_chapters: List[Chapter],
        mangadex_manga_id: str,
        deleter_process_object: "ChapterDeleterProcess",
        chapters_on_md: List[dict],
        current_uploaded_chapters: List[Chapter],
        same_chapter_dict: Dict[str, List[int]],
        mangadex_manga_data: dict,
        custom_language: Dict[str, str],
        chapters_on_db: List[dict],
        **kwargs,
    ):
        self.database_connection = database_connection
        self.http_client = http_client
        self.updated_chapters = updated_chapters
        self.all_manga_chapters = all_manga_chapters
        self.mangadex_manga_id = mangadex_manga_id
        self.chapters_on_md = chapters_on_md
        self.deleter_process_object = deleter_process_object
        self.posted_md_updates = current_uploaded_chapters
        self.same_chapter_dict = same_chapter_dict
        self.mangadex_manga_data = mangadex_manga_data
        self.custom_language = custom_language
        self.chapters_on_db = chapters_on_db
        self.posted_chapters: List[Chapter] = []
        self.failed_uploads: List[Chapter] = []

        self.get_chapter_volumes()

        if self.chapters_on_md:
            self._delete_extra_chapters()

    def _remove_chapters_not_mplus(self) -> List[dict]:
        """Find chapters on MangaDex not on MangaPlus."""
        md_chapters_not_mplus = [
            c
            for c in self.chapters_on_md
            if c["attributes"]["chapter"]
            not in [x.chapter_number for x in self.all_manga_chapters]
            or c["attributes"]["translatedLanguage"]
            not in list(
                set(
                    list(mplus_language_map.values())
                    + list(self.custom_language.values())
                )
            )
        ]

        logger.info(
            f"{self.__class__.__name__} deleter finder found: {md_chapters_not_mplus}"
        )

        chapters_to_delete = []
        for expired in md_chapters_not_mplus:
            expired_chapter_object = update_expired_chapter_database(
                self.database_connection,
                expired,
                chapters_on_db=self.chapters_on_db,
                md_manga_id=self.mangadex_manga_id,
            )
            chapters_to_delete.append(expired_chapter_object)

        return chapters_to_delete

    def _delete_extra_chapters(self):
        chapters_to_delete = self._remove_chapters_not_mplus()
        if chapters_to_delete:
            self.deleter_process_object.add_more_chapters(chapters_to_delete)

    def get_chapter_volumes(self):
        aggregate_chapters = fetch_aggregate(
            self.http_client,
            self.mangadex_manga_id,
            # **{"translatedLanguage[]": ["en"]},
        )
        if aggregate_chapters is None:
            return

        for chapter in self.updated_chapters:
            for volume in aggregate_chapters:
                if volume == "none":
                    continue

                if isinstance(aggregate_chapters, dict):
                    volume_iter = aggregate_chapters[volume]["chapters"]
                elif isinstance(aggregate_chapters, list):
                    volume_iter = volume["chapters"]

                if isinstance(volume_iter, dict):
                    volume_chapters = volume_iter.keys()

                    if chapter.chapter_number in volume_chapters:
                        volume_str = str(volume).lstrip("0")
                        if volume_str == "" or not volume_str:
                            volume_str = "0"

                        chapter.chapter_volume = volume_str

    def start_manga_uploading_process(self, last_manga: bool):
        self.skipped = 0
        self.edited = 0
        self.skipped_chapter = False
        chapter = self.updated_chapters[0]
        for count, chapter in enumerate(self.updated_chapters, start=1):
            chapter: Chapter = chapter
            chapter.md_manga_id = self.mangadex_manga_id
            self.http_client.login()

            chapter_to_upload_process = ChapterUploaderProcess(
                database_connection=self.database_connection,
                http_client=self.http_client,
                mangadex_manga_id=self.mangadex_manga_id,
                chapter=chapter,
                posted_md_updates=self.posted_md_updates,
                same_chapter_dict=self.same_chapter_dict,
            )

            uploaded = chapter_to_upload_process.start_upload(self.chapters_on_md)
            if uploaded in ("on_md", "session_error", "edited"):
                if uploaded in ("on_md",):
                    self.skipped += 1
                    self.skipped_chapter = True
                elif uploaded in ("edited",):
                    self.edited += 1
                elif uploaded in ("session_error",):
                    self.skipped += 1
                    self.failed_uploads.append(chapter)
                continue

            self.skipped_chapter = False
            self.posted_chapters.append(chapter)
            time.sleep(1)

        if self.skipped != 0:
            skipped_chapters_message = f"Skipped {self.skipped} chapters out of {len(self.updated_chapters)} for manga {chapter.manga.manga_name}: {self.mangadex_manga_id} - {chapter.manga_id}."
            logger.info(skipped_chapters_message)
            print(skipped_chapters_message)

        if self.edited != 0:
            edited_chapters_message = f"Edited {self.edited} chapters out of {len(self.updated_chapters)} for manga {chapter.manga.manga_name}: {self.mangadex_manga_id} - {chapter.manga_id}."
            logger.info(edited_chapters_message)
            print(edited_chapters_message)

        MPlusBotUpdatesWebhook(
            self.mangadex_manga_data,
            self.posted_chapters,
            self.failed_uploads,
            self.skipped,
            self.edited,
        ).main(last_manga)
