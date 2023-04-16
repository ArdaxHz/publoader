import datetime
import logging
import queue
import threading
from typing import Optional

import pymongo

from publoader.models.database import database_connection
from publoader.models.dataclasses import Chapter
from publoader.models.http import RequestError, http_client
from publoader.utils.config import mangadex_api_url
from publoader.webhook import PubloaderDeleterWebhook

logger = logging.getLogger("publoader")

delete_queue = queue.Queue()


class DeleteProcess:
    def __init__(
        self,
        upload_chapter: dict,
        **kwargs,
    ):
        self.upload_chapter = upload_chapter
        self.chapter = Chapter(**self.upload_chapter)
        self.extension_name = self.chapter.extension_name

    def delete_chapter(
        self,
        chapter: Chapter = None,
        to_delete_id: Optional[str] = None,
    ):
        """Check if the chapters expired and remove off mangadex if they are."""
        # If the expiry date of the chapter is less than the current time and
        # the md chapter id is available, try delete
        if chapter is None:
            return False

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
                delete_reponse = http_client.delete(
                    f"{mangadex_api_url}/chapter/{md_chapter_id}"
                )
            except RequestError as e:
                logger.error(e)
                return False

            if delete_reponse.status_code == 200:
                logger.info(f"Deleted {chapter}.")
                print(f"----Deleted {deleted_message}")

                PubloaderDeleterWebhook(chapter.extension_name, chapter).main()
                return True

        logger.error(f"Couldn't delete expired chapter {deleted_message}")
        return False


def worker():
    while True:
        item = delete_queue.get()
        print(f"----Working on deleting {item['_id']}----")

        chapter_deleter = DeleteProcess(item)
        deleted = chapter_deleter.delete_chapter()

        if deleted:
            database_connection["to_delete"].delete_one({"_id": {"$eq": item["_id"]}})
            database_connection["deleted"].insert_one(item)

        delete_queue.task_done()


def setup_thread():
    thread = threading.Thread(target=worker, daemon=True)
    thread.start()
    return thread


def main():
    chapters = database_connection["to_delete"].find()
    for chapter in chapters:
        delete_queue.put(chapter)

    chapters = database_connection["uploaded"].find(
        {"chapter_expire": {"$lt": datetime.datetime.now(tz=datetime.timezone.utc)}}
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

                if not thread.is_alive():
                    thread = setup_thread()
        except pymongo.errors.PyMongoError as e:
            print(e)

    # Block until all tasks are done.
    delete_queue.join()
    print("All work completed")
