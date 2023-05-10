import logging
import queue
import threading
import traceback
from typing import Optional

import pymongo

from publoader.models.database import database_connection
from publoader.models.dataclasses import Chapter
from publoader.models.http import RequestError, http_client
from publoader.utils.config import mangadex_api_url
from publoader.utils.utils import get_current_datetime
from publoader.webhook import PubloaderQueueWebhook

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
                return True

        logger.error(f"Couldn't delete expired chapter {deleted_message}")
        print(f"Couldn't delete chapter {deleted_message}")
        return False


def worker(http_client, queue_webhook, **kwargs):
    while True:
        try:
            item = delete_queue.get()
            print(f"----Deleter: Working on {item['_id']}----")

            chapter_deleter = DeleteProcess(item, http_client)
            deleted = chapter_deleter.delete_chapter()

            queue_webhook.add_chapter(item, processed=deleted)
            if deleted:
                database_connection["to_delete"].delete_one(
                    {"_id": {"$eq": item["_id"]}}
                )
                database_connection["uploaded"].delete_one(
                    {"_id": {"$eq": item["_id"]}}
                )
                item.pop("_id")
                database_connection["deleted"].insert_one(item)

            delete_queue.task_done()
            if delete_queue.qsize() == 0:
                queue_webhook.send_queue_finished()
        except Exception as e:
            traceback.print_exc()
            logger.exception(f"Deleter raised an error.")


def fetch_data_from_database():
    chapters = database_connection["to_delete"].find()
    for chapter in chapters:
        delete_queue.put(chapter)

    chapters = database_connection["uploaded"].find(
        {"chapter_expire": {"$lt": get_current_datetime()}}
    )
    for chapter in chapters:
        delete_queue.put(chapter)


def setup_thread(queue_webhook, *args, **kwargs):
    with delete_queue.mutex:
        delete_queue.queue.clear()

    fetch_data_from_database()
    thread = threading.Thread(
        target=worker, daemon=True, args=(http_client, queue_webhook), kwargs=kwargs
    )
    thread.start()
    return thread


def main():
    queue_webhook = PubloaderQueueWebhook(worker_type="deleter", colour="C43542")

    # Turn-on the worker thread.
    thread = setup_thread(queue_webhook=queue_webhook)
    print(f"Starting Deleter watcher.")

    while True:
        try:
            with database_connection["to_delete"].watch(
                [{"$match": {"operationType": "insert"}}]
            ) as stream:
                for change in stream:
                    delete_queue.put(change["fullDocument"])

                if not thread.is_alive():
                    print("Restarting Deleter Thread")
                    thread = setup_thread(queue_webhook=queue_webhook)
        except pymongo.errors.PyMongoError as e:
            print(e)

    # Block until all tasks are done.
    delete_queue.join()
    print("All work completed")
