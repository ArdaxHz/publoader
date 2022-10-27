import logging
import sqlite3
import time
from datetime import datetime
from typing import TYPE_CHECKING, List, Optional

import requests
from .webhook import MPlusBotDeleterWebhook
from . import (
    print_error,
    mangadex_api_url,
    upload_retry,
)

if TYPE_CHECKING:
    from .auth_md import AuthMD

logger = logging.getLogger("mangaplus")


class ChapterDeleterProcess:
    def __init__(
        self,
        *,
        session: requests.Session,
        posted_chapters: list = [],
        md_auth_object: "AuthMD",
        on_db: bool = True,
        database_connection: "sqlite3.Connection",
    ):
        self.session = session
        self.on_db = on_db
        self.database_connection = database_connection
        self.posted_chapters = posted_chapters
        self.md_auth_object = md_auth_object
        self.chapters_to_delete = self.get_chapter_to_delete(
            self.posted_chapters if self.posted_chapters else []
        )

        self.chapter_delete_ratelimit = 8
        self.chapter_delete_process = None
        self.deleter_discord_bot = MPlusBotDeleterWebhook()

        logger.info(f"Chapters to delete: {self.chapters_to_delete}")

    def _get_all_chapters(self) -> List[dict]:
        """Get all the chapters from the database."""
        return [
            dict(k)
            for k in self.database_connection.execute(
                "SELECT * FROM chapters"
            ).fetchall()
        ]

    def get_chapter_to_delete(self, database=False) -> List[dict]:
        """Get only the expired chapters from the total chapters list."""
        if database:
            posted_chapters = self.posted_chapters
        else:
            posted_chapters = self._get_all_chapters()

        return [
            dict(x)
            for x in posted_chapters
            if datetime.fromtimestamp(x["chapter_expire"]) <= datetime.now()
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
        chapter: dict = {},
        to_delete_id: Optional[str] = None,
        looped_all: bool = False,
    ):
        """Check if the chapters expired and remove off mangadex if they are."""
        # If the expiry date of the chapter is less than the current time and
        # the md chapter id is available, try delete
        md_chapter_id: Optional[str] = chapter.get("md_chapter_id", to_delete_id)
        logger.info(
            f"Moving {md_chapter_id} from chapters table to deleted_chapters table."
        )
        manga_id = chapter.get("manga_id", None)
        if manga_id is None:
            manga_id = chapter.get("md_manga_id", None)
        deleted_message = f'{md_chapter_id}: {chapter.get("chapter_id", None)}, manga {manga_id}, chapter {chapter.get("chapter_number", None)}, language {chapter.get("chapter_language", None)}.'

        if md_chapter_id is not None:
            for i in range(upload_retry):
                try:
                    delete_reponse = self.session.delete(
                        f"{mangadex_api_url}/chapter/{md_chapter_id}", verify=False
                    )
                except requests.RequestException:
                    continue

                if delete_reponse.status_code != 200:
                    logger.error(f"Couldn't delete expired chapter {deleted_message}")
                    print_error(delete_reponse, log_error=True)

                    if delete_reponse.status_code == 401:
                        unauthorised_message = (
                            f"You're not logged in to delete this chapter {chapter}."
                        )
                        logger.error(unauthorised_message)
                        print(unauthorised_message)

                        self.md_auth_object.login()
                        continue

                if delete_reponse.status_code == 200:
                    logger.info(f"Deleted {chapter}.")
                    print(f"----Deleted {deleted_message}")
                    self.deleter_discord_bot.main(chapter, looped_all)
                    break

        if self.on_db and chapter:
            self._delete_from_database(chapter)
        time.sleep(self.chapter_delete_ratelimit)

    def _delete_expired_chapters(self):
        """Delete expired chapters from mangadex."""
        looped_all = False
        if self.chapters_to_delete:
            logger.info(f"Started deleting expired chapters process.")
            print("Deleting expired chapters.")

        _local_list = self.chapters_to_delete[:]
        for count, chapter_to_delete in enumerate(_local_list, start=1):
            self.md_auth_object.login()
            looped_all = count == len(_local_list)
            self._remove_old_chapter(chapter_to_delete, looped_all)

            try:
                self.chapters_to_delete.remove(chapter_to_delete)
            except ValueError:
                pass

        if looped_all:
            del self.chapters_to_delete[:]

    def add_more_chapters(self, chapters_to_add: List[dict], on_db: bool = True):
        """Extend the list of chapters to delete with another list."""
        self.on_db = on_db
        self.chapters_to_delete.extend(chapters_to_add)

    def delete(self):
        """Start the chapter deleter process."""
        if self.chapters_to_delete:
            self.md_auth_object.login()
            self._delete_expired_chapters()
