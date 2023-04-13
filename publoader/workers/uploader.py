import logging
import queue
import threading
from typing import Optional

import pymongo

from publoader.models.database import update_database, database_connection
from publoader.models.dataclasses import Chapter
from publoader.models.http import RequestError
from publoader.utils.config import (
    md_upload_api_url,
    upload_retry,
)
from publoader.models.http import http_client


logger = logging.getLogger("publoader")

upload_queue = queue.Queue()


class UploaderProcess:
    def __init__(
        self,
        upload_chapter: dict,
        **kwargs,
    ):
        self.chapter = Chapter(**upload_chapter)
        self.extension_name = self.chapter.extension_name
        self.mangadex_manga_id = upload_chapter.get("mangadex_manga_id", "")
        self.mangadex_group_id = upload_chapter.get("mangadex_group_id", "")

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
        self.successful_upload_id: Optional[str] = None

    def remove_upload_session(self, session_id: Optional[str] = None):
        """Delete the upload session."""
        if session_id is None:
            session_id = self.upload_session_id

        try:
            http_client.delete(
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
            existing_session = http_client.get(
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
                upload_session_response = http_client.post(
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
            chapter_commit_response = http_client.post(
                f"{md_upload_api_url}/{self.upload_session_id}/commit",
                json=payload,
            )
        except RequestError as e:
            logger.error(e)
            return False

        if chapter_commit_response.status_code == 200:
            if chapter_commit_response.data is not None:
                self.successful_upload_id = chapter_commit_response.data["data"]["id"]
                self.chapter.md_chapter_id = self.successful_upload_id

                successful_upload_message = f"Committed {self.successful_upload_id} - {self.chapter.chapter_id} for"
                logger.info(f"{successful_upload_message} {self.chapter}")
                print(f"{successful_upload_message} {self.manga_generic_error_message}")
                return True
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

    def start_upload(self) -> bool:
        return False
        upload_session_response_json = self._create_upload_session()
        if upload_session_response_json is None:
            return False

        self.upload_session_id = upload_session_response_json["data"]["id"]
        logger.info(
            f"Created upload session: {self.upload_session_id} - {self.chapter}"
        )
        chapter_committed = self._commit_chapter()
        if not chapter_committed:
            self.remove_upload_session()
            return False
        return True


def worker():
    while True:
        item = upload_queue.get()
        print(f"----Working on uploading {item['_id']}----")

        chapter_uploader = UploaderProcess(item)
        uploaded = chapter_uploader.start_upload()
        successful_upload_id = chapter_uploader.successful_upload_id

        item["md_chapter_id"] = successful_upload_id

        if uploaded:
            database_connection["to_upload"].delete_one({"_id": {"$eq": item["_id"]}})
            if successful_upload_id is not None:
                print(successful_upload_id)
                update_database(item)

        upload_queue.task_done()


def main():
    chapters = database_connection["to_upload"].find()
    for chapter in chapters:
        upload_queue.put(chapter)

    # Turn-on the worker thread.
    threading.Thread(target=worker, daemon=True).start()

    print("starting watcher")

    while True:
        try:
            with database_connection["to_upload"].watch(
                [{"$match": {"operationType": "insert"}}]
            ) as stream:
                for change in stream:
                    upload_queue.put(change["fullDocument"])
        except pymongo.errors.PyMongoError as e:
            print(e)

    # Block until all tasks are done.
    upload_queue.join()
    print("All work completed")
