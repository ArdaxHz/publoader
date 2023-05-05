import logging
import queue
import threading
from typing import Optional

import pymongo

from publoader.models.database import database_connection
from publoader.models.dataclasses import Chapter
from publoader.models.http import RequestError, http_client
from publoader.utils.config import mangadex_api_url
from publoader.utils.utils import get_current_datetime
from publoader.webhook import PubloaderDeleterWebhook

logger = logging.getLogger("publoader")

delete_queue = queue.Queue()


class DeleteProcess:
    def __init__(
        self,
        upload_chapter: dict,
        http_client,
        **kwargs,
    ):
        self.upload_chapter = upload_chapter
        self.http_client = http_client
        self.chapter = Chapter(**self.upload_chapter)
        self.extension_name = self.chapter.extension_name

    def delete_chapter(
        self,
    ) -> bool:
        """Check if the chapters expired and remove off mangadex if they are."""
        md_chapter_id: Optional[str] = self.chapter.md_chapter_id
        deleted_message = f"{md_chapter_id}: {self.chapter.chapter_id}, manga {self.chapter.manga_id}, chapter {self.chapter.chapter_number}, language {self.chapter.chapter_language}."

        if md_chapter_id is not None:
            try:
                delete_reponse = self.http_client.delete(
                    f"{mangadex_api_url}/chapter/{md_chapter_id}"
                )
            except RequestError as e:
                logger.error(e)
                return False

            if delete_reponse.status_code == 200:
                logger.info(f"Deleted {self.chapter}.")
                print(f"--Deleted {deleted_message}")

                PubloaderDeleterWebhook(
                    self.chapter.extension_name, self.chapter
                ).main()
                return True

        logger.error(f"Couldn't delete expired chapter {deleted_message}")
        print(f"Couldn't delete chapter {deleted_message}")
        return False


def worker(http_client):
    while True:
        item = delete_queue.get()
        print(f"----Deleter: Working on {item['_id']}----")

        chapter_deleter = DeleteProcess(item, http_client)
        deleted = chapter_deleter.delete_chapter()

        if deleted:
            database_connection["to_delete"].delete_one({"_id": {"$eq": item["_id"]}})
            database_connection["uploaded"].delete_one({"_id": {"$eq": item["_id"]}})
            item.pop("_id")
            database_connection["deleted"].insert_one(item)

        delete_queue.task_done()


def setup_thread():
    thread = threading.Thread(target=worker, daemon=True, args=(http_client,))
    thread.start()
    return thread


def main():
    chapters = database_connection["to_delete"].find()
    for chapter in chapters:
        delete_queue.put(chapter)

    chapters = database_connection["uploaded"].find(
        {"chapter_expire": {"$lt": get_current_datetime()}}
    )
    for chapter in chapters:
        delete_queue.put(chapter)

    # Turn-on the worker thread.
    thread = setup_thread()
    print(f"Starting Deleter watcher.")

    while True:
        try:
            with database_connection["to_delete"].watch(
                [{"$match": {"operationType": "insert"}}]
            ) as stream:
                for change in stream:
                    delete_queue.put(change["fullDocument"])

                print("Restarting Deleter Thread")
                if not thread.is_alive():
                    thread = setup_thread()
        except pymongo.errors.PyMongoError as e:
            print(e)

    # Block until all tasks are done.
    delete_queue.join()
    print("All work completed")
