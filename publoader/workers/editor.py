import logging
import queue
import threading

import pymongo

from publoader.models.database import database_connection, update_database
from publoader.models.dataclasses import Chapter
from publoader.models.http import RequestError, http_client
from publoader.utils.config import (
    mangadex_api_url,
)
from publoader.webhook import PubloaderQueueWebhook

logger = logging.getLogger("publoader")

edit_queue = queue.Queue()


class EditorProcess:
    def __init__(
        self,
        upload_chapter: dict,
        http_client,
        **kwargs,
    ):
        self.upload_chapter = upload_chapter
        self._id = self.upload_chapter.pop("_id")
        self.http_client = http_client
        self.chapter = Chapter(**self.upload_chapter["chapter"])
        self.payload = self.upload_chapter["payload"]
        self.md_chapter_id = self.upload_chapter["md_chapter_id"]

        self.manga_generic_error_message = (
            f"Extension: {self.chapter.extension_name}, "
            f"Manga: {self.chapter.manga_name}, "
            f"{self.chapter.md_manga_id} - "
            f"{self.chapter.manga_id}, "
            f"chapter: {self.chapter.chapter_id}, "
            f"number: {self.chapter.chapter_number!r}, "
            f"volume: {self.chapter.chapter_volume!r}, "
            f"language: {self.chapter.chapter_language!r}, "
            f"title: {self.chapter.chapter_title!r}"
        )

    def start_edit(self) -> bool:
        try:
            update_response = self.http_client.put(
                f"{mangadex_api_url}/chapter/{self.md_chapter_id}",
                json=self.payload,
            )
        except RequestError as e:
            logger.error(e)
            return False

        if update_response.status_code == 200:
            logger.info(f"Edited chapter {self.md_chapter_id}")
            print(f"--Edited chapter: {self.manga_generic_error_message}")
            return True
        return False


def worker(http_client, queue_webhook, **kwargs):
    while True:
        item = edit_queue.get()
        print(f"----Editor: Working on {item['_id']}----")

        chapter_editor = EditorProcess(item, http_client)
        edited = chapter_editor.start_edit()

        queue_webhook.add_chapter(item["chapter"], processed=edited)
        if edited:
            database_connection["to_edit"].delete_one({"_id": {"$eq": item["_id"]}})
            update_database(item["chapter"])

        edit_queue.task_done()
        if edit_queue.empty():
            queue_webhook.send_queue_finished()


def setup_thread(queue_webhook, *args, **kwargs):
    thread = threading.Thread(
        target=worker, daemon=True, args=(http_client, queue_webhook), kwargs=kwargs
    )
    thread.start()
    return thread


def main():
    queue_webhook = PubloaderQueueWebhook(worker_type="editor", colour="FFF71C")

    chapters = database_connection["to_edit"].find()
    for chapter in chapters:
        edit_queue.put(chapter)

    # Turn-on the worker thread.
    thread = setup_thread(queue_webhook=queue_webhook)
    print(f"Starting Editor watcher.")

    while True:
        try:
            with database_connection["to_edit"].watch(
                [{"$match": {"operationType": "insert"}}]
            ) as stream:
                for change in stream:
                    edit_queue.put(change["fullDocument"])

                print("Restarting Editor Thread")
                if not thread.is_alive():
                    thread = setup_thread(queue_webhook=queue_webhook)
        except pymongo.errors.PyMongoError as e:
            print(e)

    # Block until all tasks are done.
    edit_queue.join()
    print("All work completed")
