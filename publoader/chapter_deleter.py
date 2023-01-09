import logging
from datetime import datetime
from typing import TYPE_CHECKING, List, Optional

from publoader.webhook import PubloaderDeleterWebhook
from publoader.models.http import RequestError
from publoader.utils.config import (
    mangadex_api_url,
)

if TYPE_CHECKING:
    import sqlite3
    from publoader.models.http import HTTPClient


logger = logging.getLogger("publoader")


class ChapterDeleterProcess:
    def __init__(
        self,
        *,
        http_client: "HTTPClient",
        posted_chapters: list = [],
        on_db: bool = True,
        database_connection: "sqlite3.Connection",
    ):
        self.http_client = http_client
        self.on_db = on_db
        self.database_connection = database_connection
        self.posted_chapters = posted_chapters
        self.chapters_to_delete = self.get_chapter_to_delete(
            self.posted_chapters if self.posted_chapters else []
        )

        self.chapter_delete_ratelimit = 8
        self.chapter_delete_process = None

        logger.info(f"Chapters to delete: {self.chapters_to_delete}")

    def _get_all_chapters(self) -> List[dict]:
        """Get all the chapters from the database."""
        return [
            dict(k)
            for k in self.database_connection.execute(
                "SELECT * FROM chapters WHERE chapter_expire IS NOT NULL and chapter_expire <= ?",
                (datetime.now(),),
            ).fetchall()
        ]

    def get_chapter_to_delete(self, database=False) -> List[dict]:
        """Get only the expired chapters from the total chapters list."""
        if database:
            posted_chapters = self.posted_chapters
        else:
            posted_chapters = self._get_all_chapters()

        expired = [
            dict(x)
            for x in posted_chapters
            if x["chapter_expire"] is not None and x["chapter_expire"] <= datetime.now()
        ]

        return [
            chap
            for chap in expired
            if chap["md_chapter_id"]
            not in [cha["md_chapter_id"] for cha in posted_chapters]
        ]

    def _delete_from_database(self, chapter: dict):
        """Move the chapter from the chapters table to the deleted_chapters table."""
        try:
            self.database_connection.execute(
                """INSERT INTO deleted_chapters SELECT * FROM chapters WHERE md_chapter_id=(?)""",
                (chapter["md_chapter_id"],),
            )
        except sqlite3.IntegrityError:
            pass
        self.database_connection.execute(
            """DELETE FROM chapters WHERE md_chapter_id=(?)""",
            (chapter["md_chapter_id"],),
        )
        self.database_connection.commit()

    def _remove_old_chapter(
        self,
        chapter: dict = None,
        to_delete_id: Optional[str] = None,
    ):
        """Check if the chapters expired and remove off mangadex if they are."""
        # If the expiry date of the chapter is less than the current time and
        # the md chapter id is available, try delete
        if chapter is None:
            return

        md_chapter_id: Optional[str] = chapter.get("md_chapter_id", to_delete_id)
        logger.info(
            f"Moving {md_chapter_id} from chapters table to deleted_chapters table."
        )
        manga_id = chapter.get("manga_id", None)
        if manga_id is None:
            manga_id = chapter.get("md_manga_id", None)
        deleted_message = f'{md_chapter_id}: {chapter.get("chapter_id", None)}, manga {manga_id}, chapter {chapter.get("chapter_number", None)}, language {chapter.get("chapter_language", None)}.'

        if md_chapter_id is not None:
            try:
                delete_reponse = self.http_client.delete(
                    f"{mangadex_api_url}/chapter/{md_chapter_id}"
                )
            except RequestError as e:
                logger.error(e)
                return

            if delete_reponse.status_code == 200:
                logger.info(f"Deleted {chapter}.")
                print(f"----Deleted {deleted_message}")
                if self.on_db and chapter:
                    self._delete_from_database(chapter)

                PubloaderDeleterWebhook(chapter.get("extension_name"), chapter).main()
                return

        logger.error(f"Couldn't delete expired chapter {deleted_message}")

    def _delete_expired_chapters(self):
        """Delete expired chapters from mangadex."""
        looped_all = False
        if self.chapters_to_delete:
            logger.info(f"Started deleting expired chapters process.")
            print("Deleting expired chapters.")

        _local_list = self.chapters_to_delete[:]
        for count, chapter_to_delete in enumerate(_local_list, start=1):
            self.http_client.login()
            looped_all = count == len(_local_list)
            self._remove_old_chapter(chapter_to_delete)

            try:
                self.chapters_to_delete.remove(chapter_to_delete)
            except ValueError:
                pass

        if looped_all:
            del self.chapters_to_delete[:]

    def add_more_chapters(self, chapters_to_add: List[dict], on_db: bool = True):
        """Extend the list of chapters to delete with another list."""
        self.on_db = on_db
        chapters_to_extend = [
            chap
            for chap in chapters_to_add
            if chap["md_chapter_id"]
            not in [cha["md_chapter_id"] for cha in self.chapters_to_delete]
        ]
        self.chapters_to_delete.extend(chapters_to_extend)

    def delete(self):
        """Start the chapter deleter process."""
        if self.chapters_to_delete:
            self.http_client.login()
            self._delete_expired_chapters()
