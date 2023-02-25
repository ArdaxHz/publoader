import logging
from datetime import datetime
from typing import TYPE_CHECKING, List, Optional

from publoader.models.dataclasses import Chapter
from publoader.webhook import PubloaderDeleterWebhook, PubloaderWebhook
from publoader.models.http import RequestError
from publoader.utils.config import (
    mangadex_api_url,
)
from publoader.utils.utils import get_current_datetime

if TYPE_CHECKING:
    import sqlite3
    from publoader.models.http import HTTPClient


logger = logging.getLogger("publoader")


class ChapterDeleterProcess:
    def __init__(
        self,
        http_client: "HTTPClient",
        database_connection: "sqlite3.Connection",
    ):
        self.http_client = http_client
        self.database_connection = database_connection
        self.chapters_to_delete = []

    def _get_all_chapters(self) -> List[Chapter]:
        """Get all the chapters from the database."""
        return [
            Chapter(**k)
            for k in self.database_connection.execute(
                "SELECT * FROM chapters WHERE chapter_expire IS NOT NULL and chapter_expire <= ?",
                (get_current_datetime(),),
            ).fetchall()
        ]

    def get_chapter_to_delete(self) -> List[Chapter]:
        """Get only the expired chapters from the total chapters list."""
        posted_chapters = self._get_all_chapters()

        expired = [
            x
            for x in posted_chapters
            if x.chapter_expire is not None
            and x.chapter_expire <= get_current_datetime()
        ]

        return [
            chap
            for chap in expired
            if chap.md_chapter_id not in [cha.md_chapter_id for cha in posted_chapters]
        ]

    def _delete_from_database(self, chapter: Chapter):
        """Move the chapter from the chapters table to the deleted_chapters table."""
        try:
            self.database_connection.execute(
                """INSERT INTO deleted_chapters SELECT * FROM chapters WHERE md_chapter_id=(?)""",
                (chapter.md_chapter_id,),
            )
        except sqlite3.IntegrityError:
            pass
        self.database_connection.execute(
            """DELETE FROM chapters WHERE md_chapter_id=(?)""",
            (chapter.md_chapter_id,),
        )
        self.database_connection.commit()

    def _remove_old_chapter(
        self,
        chapter: Chapter = None,
        to_delete_id: Optional[str] = None,
    ):
        """Check if the chapters expired and remove off mangadex if they are."""
        # If the expiry date of the chapter is less than the current time and
        # the md chapter id is available, try delete
        if chapter is None:
            return

        md_chapter_id: Optional[str] = chapter.md_chapter_id or to_delete_id
        logger.info(
            f"Moving {md_chapter_id} from chapters table to deleted_chapters table."
        )
        manga_id = chapter.manga_id
        if manga_id is None:
            manga_id = chapter.md_manga_id
        deleted_message = f"{md_chapter_id}: {chapter.chapter_id}, manga {manga_id}, chapter {chapter.chapter_number}, language {chapter.chapter_language}."

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
                self._delete_from_database(chapter)

                PubloaderDeleterWebhook(chapter.extension_name, chapter).main()
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

    def add_more_chapters(self, chapters_to_add: List[Chapter]):
        """Extend the list of chapters to delete with another list."""
        chapters_to_extend = [
            chap
            for chap in chapters_to_add
            if chap.md_chapter_id
            not in [cha.md_chapter_id for cha in self.chapters_to_delete]
        ]
        self.chapters_to_delete.extend(chapters_to_extend)

    def delete(self):
        """Start the chapter deleter process."""
        self.add_more_chapters(self.get_chapter_to_delete())
        logger.info(f"Chapters to delete: {self.chapters_to_delete}")

        if self.chapters_to_delete:
            self.http_client.login()
            self._delete_expired_chapters()

            print("Finished deleting expired chapters.")
            PubloaderWebhook(
                "no_extension",
                **{"title": "Finished deleting expired chapters.", "colour": "C43542"},
            ).send()
        else:
            print("No chapters to delete.")
