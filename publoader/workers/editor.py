import logging

from publoader.models.database import database_connection, update_database
from publoader.models.dataclasses import Chapter
from publoader.models.http import RequestError
from publoader.utils.config import mangadex_api_url

logger = logging.getLogger("publoader-editor")


class EditorProcess:
    def __init__(
        self,
        upload_chapter: dict,
        http_client,
        **kwargs,
    ):
        self.upload_chapter = upload_chapter
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


def run(item, http_client, queue_webhook, **kwargs):
    chapter_editor = EditorProcess(item, http_client)
    edited = chapter_editor.start_edit()

    queue_webhook.add_chapter(item["chapter"], processed=edited)
    database_connection["to_edit"].delete_one({"_id": {"$eq": item["_id"]}})
    if edited:
        update_database(item["chapter"])


def fetch_data_from_database():
    return [chap for chap in database_connection["to_edit"].find()]
