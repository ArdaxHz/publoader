import asyncio
import time

from motor import motor_asyncio
import pymongo

from publoader.models.database import database_connection


import logging
import re
from typing import TYPE_CHECKING, Dict, List, Optional

from publoader.models.database import update_database
from publoader.models.dataclasses import Chapter
from publoader.models.http import RequestError
from publoader.utils.config import (
    md_upload_api_url,
    upload_retry,
    mangadex_api_url,
)
from publoader.utils.misc import find_key_from_list_value, flatten

if TYPE_CHECKING:
    import sqlite3
    from publoader.models.http import HTTPClient


logger = logging.getLogger("publoader")


upload_queue = []


class UploaderProcess:
    def __init__(
        self,
        extension_name: str,
        database_connection: "sqlite3.Connection",
        http_client: "HTTPClient",
        mangadex_manga_id: str,
        mangadex_group_id: str,
        chapter: "Chapter",
        posted_md_updates: List["Chapter"],
        same_chapter_dict: Dict[str, List[str]],
        **kwargs,
    ):
        self.extension_name = extension_name
        self.database_connection = database_connection
        self.http_client = http_client
        self.mangadex_manga_id = mangadex_manga_id
        self.mangadex_group_id = mangadex_group_id
        self.chapter = chapter
        self.posted_md_updates = posted_md_updates
        self.same_chapter_dict = same_chapter_dict

        self.manga_generic_error_message = (
            f"Extension: {self.extension_name}, "
            f"Manga: {self.chapter.manga_name}, "
            f"{self.mangadex_manga_id} - {self.chapter.manga_id}, "
            f"chapter: {self.chapter.chapter_id}, "
            f"number: {self.chapter.chapter_number!r}, "
            f"volume: {self.chapter.chapter_volume!r}, "
            f"language: {self.chapter.chapter_language!r}, "
            f"title: {self.chapter.chapter_title!r}"
        )
        self.upload_retry_total = upload_retry
        self.upload_session_id: Optional[str] = None

    def remove_upload_session(self, session_id: Optional[str] = None):
        """Delete the upload session."""
        if session_id is None:
            session_id = self.upload_session_id

        try:
            self.http_client.delete(
                f"{md_upload_api_url}/{session_id}",
                successful_codes=[404],
            )
        except RequestError as e:
            logger.error(e)
        logger.info(f"Sent {session_id} to be deleted.")

    def _delete_exising_upload_session(self):
        """Remove any exising upload sessions to not error out as mangadex only allows one upload session at a time."""
        logger.debug(
            f"Checking for upload sessions for manga {self.mangadex_manga_id}, chapter {self.chapter}."
        )

        try:
            existing_session = self.http_client.get(
                f"{md_upload_api_url}", successful_codes=[404]
            )
        except RequestError as e:
            logger.error(e)
        else:
            if (
                existing_session.status_code == 200
                and existing_session.data is not None
            ):
                self.remove_upload_session(existing_session.data["data"]["id"])
                return
            elif existing_session.status_code == 404:
                return

        logger.error("Exising upload session not deleted.")
        raise Exception(f"Couldn't delete existing upload session.")

    def _create_upload_session(self) -> Optional[dict]:
        """Try to create an upload session 3 times."""
        for chapter_upload_session_retry in range(self.upload_retry_total):
            # Delete existing upload session if exists
            try:
                self._delete_exising_upload_session()
            except Exception as e:
                logger.error(e)
                continue

            # Start the upload session
            try:
                upload_session_response = self.http_client.post(
                    f"{md_upload_api_url}/begin",
                    json={
                        "manga": self.mangadex_manga_id,
                        "groups": [self.mangadex_group_id],
                    },
                    tries=1,
                )
            except RequestError as e:
                logger.error(e)
                continue

            if upload_session_response.status_code == 200:
                if upload_session_response.data is not None:
                    return upload_session_response.data

                upload_session_response_json_message = f"Couldn't convert successful upload session creation into a json, retrying."
                logger.error(f"{upload_session_response_json_message} {self.chapter}")
                print(
                    f"{upload_session_response_json_message} {self.manga_generic_error_message}."
                )
                continue

        # Couldn't create an upload session, skip the chapter
        upload_session_response_json_message = (
            f"Couldn't create an upload session for {self.manga_generic_error_message}."
        )
        logger.error(f"{upload_session_response_json_message} {self.chapter}")
        print(
            f"{upload_session_response_json_message} {self.manga_generic_error_message}."
        )

    def _commit_chapter(self) -> bool:
        """Try commit the chapter to mangadex."""
        payload = {
            "chapterDraft": {
                "volume": self.chapter.chapter_volume,
                "chapter": self.chapter.chapter_number,
                "title": self.chapter.chapter_title,
                "translatedLanguage": self.chapter.chapter_language,
                "externalUrl": self.chapter.chapter_url,
            },
            "pageOrder": [],
        }

        if self.chapter.chapter_expire is not None:
            payload["chapterDraft"]["publishAt"] = self.chapter.chapter_expire.strftime(
                "%Y-%m-%dT%H:%M:%S"
            )

        logger.info(f"Commit payload: {payload}")

        try:
            chapter_commit_response = self.http_client.post(
                f"{md_upload_api_url}/{self.upload_session_id}/commit",
                json=payload,
            )
        except RequestError as e:
            logger.error(e)
            return False

        if chapter_commit_response.status_code == 200:
            if chapter_commit_response.data is not None:
                successful_upload_id = chapter_commit_response.data["data"]["id"]
                self.chapter.md_chapter_id = successful_upload_id

                successful_upload_message = (
                    f"Committed {successful_upload_id} - {self.chapter.chapter_id} for"
                )
                logger.info(f"{successful_upload_message} {self.chapter}")
                print(f"{successful_upload_message} {self.manga_generic_error_message}")

                update_database(self.database_connection, self.chapter)
            else:
                chapter_commit_response_json_message = f"Couldn't convert successful chapter commit api response into a json"
                logger.warning(
                    f"{chapter_commit_response_json_message} for {self.chapter}"
                )
                print(chapter_commit_response_json_message)
            return True

        logger.error(f"Couldn't commit {self.chapter}")
        print(
            f"Couldn't commit {self.upload_session_id}, manga {self.mangadex_manga_id} - {self.chapter.manga_id} chapter {self.chapter.chapter_number!r} language {self.chapter.chapter_language}."
        )
        self.remove_upload_session()
        return False

    def edit_chapter(self, md_chapter: dict):
        """Update the chapter on mangadex if it is different."""
        md_id = md_chapter["id"]
        chapter_attrs = md_chapter["attributes"]
        data_to_post = {
            "volume": chapter_attrs["volume"],
            "chapter": chapter_attrs["chapter"],
            "title": chapter_attrs["title"],
            "translatedLanguage": chapter_attrs["translatedLanguage"],
            "externalUrl": chapter_attrs["externalUrl"],
            "version": chapter_attrs["version"],
            "groups": [
                g["id"]
                for g in md_chapter["relationships"]
                if g["type"] == "scanlation_group"
            ],
        }
        changed = False

        if str(self.chapter.chapter_id) not in chapter_attrs["externalUrl"]:
            logger.debug(
                f"MD chapter {md_id} {self.extension_name} id {chapter_attrs['externalUrl']} doesn't match id {self.chapter.chapter_id}"
            )
            return False

        if self.chapter.chapter_volume != chapter_attrs["volume"]:
            data_to_post["volume"] = self.chapter.chapter_volume
            changed = True

        if self.chapter.chapter_number != chapter_attrs["chapter"]:
            data_to_post["chapter"] = self.chapter.chapter_number
            changed = True

        if self.chapter.chapter_title != chapter_attrs["title"]:
            data_to_post["title"] = self.chapter.chapter_title
            changed = True

        if changed:
            logger.debug(f"Editing chapter {md_id} with old info {chapter_attrs}")
            logger.info(f"Editing chapter {md_id} with new info {data_to_post}")

            try:
                update_response = self.http_client.put(
                    f"{mangadex_api_url}/chapter/{md_id}",
                    json=data_to_post,
                )
            except RequestError as e:
                logger.error(e)
                return False

            if update_response.status_code == 200:
                logger.info(f"Edited chapter {md_id}")
                print(f"--Edited chapter: {self.manga_generic_error_message}")
                return True
        else:
            logger.info(f"Nothing to edit for chapter {md_id}")
        return False

    def _check_for_duplicate_chapter_md_list(self, manga_chapters: List[dict]) -> str:
        """Check for duplicate chapters on mangadex."""
        for md_chapter in manga_chapters:
            if (
                md_chapter["attributes"]["chapter"] == self.chapter.chapter_number
                and md_chapter["attributes"]["translatedLanguage"]
                == self.chapter.chapter_language
                and md_chapter["attributes"]["externalUrl"] is not None
                and self.chapter.chapter_id in md_chapter["attributes"]["externalUrl"]
                and self.chapter.chapter_id
                not in flatten(list(self.same_chapter_dict.values()))
            ):
                self.chapter.md_chapter_id = md_chapter["id"]
                logger.info(f"Chapter already on MangaDex: {self.chapter}")
                print(
                    f"{self.manga_generic_error_message} already exists on mangadex, skipping."
                )

                edited = self.edit_chapter(md_chapter)
                # Add duplicate chapter to database to avoid checking it again
                # in the future
                update_database(self.database_connection, self.chapter)
                return "edited" if edited else "on_md"
        return "new"

    def _check_already_uploaded_internal_list(self) -> bool:
        """Check if chapter to upload is already in the internal list of uploaded chapters."""
        for chap in self.posted_md_updates:
            if (
                chap.chapter_id == self.chapter.chapter_id
                and chap.chapter_number == self.chapter.chapter_number
                and chap.chapter_language == self.chapter.chapter_language
            ):
                return True
        return False

    def _check_uploaded_different_id(self, manga_chapters: List[dict]) -> bool:
        """Check if chapter id to upload has been uploaded already under a different id."""
        same_chapter_list_md = [
            c["attributes"]["externalUrl"]
            for c in manga_chapters
            if c["attributes"]["chapter"] == self.chapter.chapter_number
            and c["attributes"]["translatedLanguage"] == self.chapter.chapter_language
        ]
        same_chapter_list_posted_ids = [
            str(c.chapter_id) for c in self.posted_md_updates
        ]

        if self.chapter.chapter_id in flatten(list(self.same_chapter_dict.values())):
            master_id = find_key_from_list_value(
                self.same_chapter_dict, self.chapter.chapter_id
            )
            if master_id is not None:
                if (
                    any(
                        [
                            re.search(master_id, search)
                            for search in same_chapter_list_md
                        ]
                    )
                    or master_id in same_chapter_list_posted_ids
                ):
                    return True
        return False

    def start_upload(self, manga_chapters: list) -> str:
        upload_session_response_json = self._create_upload_session()
        if upload_session_response_json is None:
            return "session_error"

        self.upload_session_id = upload_session_response_json["data"]["id"]
        logger.info(
            f"Created upload session: {self.upload_session_id} - {self.chapter}"
        )
        chapter_committed = self._commit_chapter()
        if not chapter_committed:
            self.remove_upload_session()
            return "session_error"

        self.posted_md_updates.append(self.chapter)
        return "uploaded"


import threading
import queue

q = queue.Queue()


def worker():
    while True:
        item = q.get()
        print(f"Working on {item}")
        print(f"Finished")
        q.task_done()


def main():
    # Turn-on the worker thread.
    threading.Thread(target=worker, daemon=True).start()

    print("starting watcher")

    while True:
        try:
            with database_connection["to_upload"].watch(
                [{"$match": {"operationType": "insert"}}]
            ) as stream:
                for change in stream:
                    q.put(change["fullDocument"])
        except pymongo.errors.PyMongoError as e:
            print(e)

    # Block until all tasks are done.
    q.join()
    print("All work completed")
