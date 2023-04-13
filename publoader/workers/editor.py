import logging
import queue
import threading
from typing import Optional

import pymongo

from publoader.models.database import update_database, database_connection
from publoader.models.dataclasses import Chapter
from publoader.models.http import RequestError
from publoader.utils.config import (mangadex_api_url, md_upload_api_url, upload_retry, )
from publoader.models.http import http_client


logger = logging.getLogger("publoader")

edit_queue = queue.Queue()


class EditorProcess:
    def __init__(
        self,
        upload_chapter: dict,
        **kwargs,
    ):
        self.upload_chapter = upload_chapter
        self.chapter = Chapter(**self.upload_chapter["chapter"])
        self.payload = self.upload_chapter["payload"]
        self.md_chapter_id = self.upload_chapter["md_chapter_id"]

        self.manga_generic_error_message = (f"Extension: {self.chapter.extension_name}, "
                                            f"Manga: {self.chapter.manga_name}, "
                                            f"{self.chapter.md_manga_id} - "
                                            f"{self.chapter.manga_id}, "
                                            f"chapter: {self.chapter.chapter_id}, "
                                            f"number: {self.chapter.chapter_number!r}, "
                                            f"volume: {self.chapter.chapter_volume!r}, "
                                            f"language: {self.chapter.chapter_language!r}, "
                                            f"title: {self.chapter.chapter_title!r}")

    def start_edit(self) -> bool:
        try:
            update_response = http_client.put(
                f"{mangadex_api_url}/chapter/{self.md_chapter_id}", json=self.payload, )
        except RequestError as e:
            logger.error(e)
            return False

        if update_response.status_code == 200:
            logger.info(f"Edited chapter {self.md_chapter_id}")
            print(f"--Edited chapter: {self.manga_generic_error_message}")
            return True
        return False


def worker():
    while True:
        item = edit_queue.get()
        print(f"----Working on editing {item['_id']}----")

        chapter_editor = EditorProcess(item)
        edited = chapter_editor.start_edit()

        if edited:
            database_connection["to_edit"].delete_one({"_id": {"$eq": item["_id"]}})
            update_database(item)

        edit_queue.task_done()


def main():
    chapters = database_connection["to_edit"].find()
    for chapter in chapters:
        edit_queue.put(chapter)

    # Turn-on the worker thread.
    threading.Thread(target=worker, daemon=True).start()

    print("starting watcher")

    while True:
        try:
            with database_connection["to_edit"].watch(
                [{"$match": {"operationType": "insert"}}]
            ) as stream:
                for change in stream:
                    edit_queue.put(change["fullDocument"])
        except pymongo.errors.PyMongoError as e:
            print(e)

    # Block until all tasks are done.
    edit_queue.join()
    print("All work completed")