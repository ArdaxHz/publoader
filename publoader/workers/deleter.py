import logging
from typing import Optional

from publoader.http.properties import RequestError
from publoader.models.database import database_connection
from publoader.models.dataclasses import Chapter
from publoader.utils.config import mangadex_api_url
from publoader.utils.utils import get_current_datetime

logger = logging.getLogger("publoader-deleter")


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


def run(item, http_client, queue_webhook, **kwargs):
    chapter_deleter = DeleteProcess(item, http_client)
    deleted = chapter_deleter.delete_chapter()

    queue_webhook.add_chapter(item, processed=deleted)
    if deleted:
        database_connection["to_delete"].delete_one({"_id": {"$eq": item["_id"]}})
        database_connection["uploaded"].delete_one({"_id": {"$eq": item["_id"]}})
        item.pop("_id")
        database_connection["deleted"].insert_one(item)


def fetch_data_from_database():
    chapters = []

    chapters.extend([chap for chap in database_connection["to_delete"].find()])
    chapters.extend(
        [
            chap
            for chap in database_connection["uploaded"].find(
                {"chapter_expire": {"$lte": get_current_datetime()}}
            )
        ]
    )
    return chapters
