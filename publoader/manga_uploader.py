import itertools
import logging
import re
import time
from typing import Optional, TYPE_CHECKING, Dict, List

from pymongo import InsertOne, UpdateOne

from publoader.chapter_uploader import ChapterUploaderProcess
from publoader.utils.config import mangadex_api_url, md_upload_api_url
from publoader.webhook import PubloaderUpdatesWebhook
from publoader.models.database import (
    database_connection,
    update_expired_chapter_database,
)
from publoader.models.dataclasses import Chapter
from publoader.utils.misc import fetch_aggregate, flatten

if TYPE_CHECKING:
    import sqlite3
    from publoader.chapter_deleter import ChapterDeleterProcess
    from publoader.models.http import HTTPClient


logger = logging.getLogger("publoader")


class MangaUploaderProcess:
    def __init__(
        self,
        extension_name: str,
        http_client: "HTTPClient",
        clean_db: bool,
        updated_chapters: List[Chapter],
        all_manga_chapters: List[Chapter],
        mangadex_manga_id: str,
        mangadex_group_id: str,
        deleter_process_object: "ChapterDeleterProcess",
        chapters_on_md: List[dict],
        current_uploaded_chapters: List[Chapter],
        same_chapter_dict: Dict[str, List[str]],
        mangadex_manga_data: dict,
        custom_language: Dict[str, str],
        chapters_on_db: List[Chapter],
        languages: List[str],
        **kwargs,
    ):
        self.extension_name = extension_name
        self.http_client = http_client
        self.clean_db = clean_db
        self.updated_chapters = updated_chapters
        self.all_manga_chapters = all_manga_chapters
        self.mangadex_manga_id = mangadex_manga_id
        self.mangadex_group_id = mangadex_group_id
        self.chapters_on_md = chapters_on_md
        self.deleter_process_object = deleter_process_object
        self.posted_md_updates = current_uploaded_chapters
        self.same_chapter_dict = same_chapter_dict
        self.mangadex_manga_data = mangadex_manga_data
        self.custom_language = custom_language
        self.chapters_on_db = chapters_on_db
        self.languages = languages

        self.skipped = 0
        self.edited = 0

        self.posted_chapters: List[Chapter] = []
        self.failed_uploads: List[Chapter] = []

        self.get_chapter_volumes()

        # if self.chapters_on_md:
        #     self._delete_extra_chapters()

    def _remove_chapters_not_external(self) -> List[Chapter]:
        """Find chapters on MangaDex not on external."""
        md_chapters_not_external = [
            c
            for c in self.chapters_on_md
            if c["attributes"]["chapter"]
            not in [x.chapter_number for x in self.all_manga_chapters]
            or c["attributes"]["translatedLanguage"]
            not in list(set(self.languages + list(self.custom_language.values())))
        ]

        logger.info(
            f"{self.__class__.__name__} deleter finder for extensions.{self.extension_name} found: {md_chapters_not_external}"
        )

        chapters_to_delete = []
        for expired in md_chapters_not_external:
            expired_chapter_object = update_expired_chapter_database(
                extension_name=self.extension_name,
                md_chapter_obj=expired,
                chapters_on_db=self.chapters_on_db,
                md_manga_id=self.mangadex_manga_id,
            )
            chapters_to_delete.append(expired_chapter_object)

        return chapters_to_delete

    def _delete_extra_chapters(self):
        if not self.all_manga_chapters:
            return

        chapters_to_delete = self._remove_chapters_not_external()
        if chapters_to_delete:
            self.deleter_process_object.add_more_chapters(chapters_to_delete)

    def get_chapter_volumes(self):
        aggregate_chapters = fetch_aggregate(
            self.http_client,
            self.mangadex_manga_id,
            # **{"translatedLanguage[]": ["en"]},
        )
        if aggregate_chapters is None:
            return

        for chapter in self.updated_chapters:
            for volume in aggregate_chapters:
                volume_iter = None
                if volume == "none":
                    continue

                if isinstance(aggregate_chapters, dict):
                    volume_iter = aggregate_chapters[volume]["chapters"]
                elif isinstance(aggregate_chapters, list):
                    volume_iter = volume["chapters"]

                if isinstance(volume_iter, dict):
                    volume_chapters = volume_iter.keys()
                    chapter_number = chapter.chapter_number.split(".", 1)[0]

                    if chapter_number in volume_chapters:
                        volume_str = str(volume).lstrip("0")
                        if volume_str == "" or not volume_str:
                            volume_str = "0"

                        chapter.chapter_volume = volume_str

    def _check_for_duplicate_chapter_md_list(self, chapter) -> Optional[dict]:
        """Check for duplicate chapters on mangadex."""
        for md_chapter in self.chapters_on_md:
            if (
                md_chapter["attributes"]["chapter"] == chapter.chapter_number
                and md_chapter["attributes"]["translatedLanguage"]
                == chapter.chapter_language
                and md_chapter["attributes"]["externalUrl"] is not None
                and re.search(
                    chapter.chapter_id, md_chapter["attributes"]["externalUrl"]
                )
                and chapter.chapter_id
                not in flatten(list(self.same_chapter_dict.values()))
            ):

                return {"md_chapter": md_chapter, "chapter": chapter}
        return

    def edit_chapter(self, dupe_chapter: dict):
        """Update the chapter on mangadex if it is different."""
        chapter = dupe_chapter["chapter"]
        md_chapter = dupe_chapter["md_chapter"]

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

        if str(chapter.chapter_id) not in chapter_attrs["externalUrl"]:
            logger.debug(
                f"MD chapter {md_id} {self.extension_name} id "
                f"{chapter_attrs['externalUrl']} doesn't match id "
                f"{chapter.chapter_id}"
            )
            return False

        if chapter.chapter_volume != chapter_attrs["volume"]:
            data_to_post["volume"] = chapter.chapter_volume
            changed = True

        if chapter.chapter_number != chapter_attrs["chapter"]:
            data_to_post["chapter"] = chapter.chapter_number
            changed = True

        if chapter.chapter_title != chapter_attrs["title"]:
            data_to_post["title"] = chapter.chapter_title
            changed = True

        if changed:
            logger.debug(f"Editing chapter {md_id} with old info {chapter_attrs}")
            logger.info(f"Editing chapter {md_id} with new info {data_to_post}")

            return {
                "route": f"{mangadex_api_url}/chapter/{md_id}",
                "method": "PUT",
                "type": "edit",
                "chapter": vars(chapter),
                "payload": data_to_post,
            }
        else:
            logger.info(f"Nothing to edit for chapter {md_id}")
        return

    def start_manga_uploading_process(self, last_manga: bool):
        chapters_to_upload = [
            chapter
            for chapter in self.updated_chapters
            if not bool(self._check_for_duplicate_chapter_md_list(chapter))
        ]
        dupes = [
            dupe
            for dupe in map(
                self._check_for_duplicate_chapter_md_list, self.updated_chapters
            )
            if dupe is not None
        ]
        chapters_to_update = [vars(dupe["chapter"]) for dupe in dupes]

        chapters_to_edit = [
            dupe for dupe in map(self.edit_chapter, dupes) if dupe is not None
        ]

        chapters_to_insert = [
            {
                "route": md_upload_api_url,
                "method": "POST",
                "type": "upload",
                "chapter": vars(chapter),
                "payload": {
                    "chapterDraft": {
                        "volume": chapter.chapter_volume,
                        "chapter": chapter.chapter_number,
                        "title": chapter.chapter_title,
                        "translatedLanguage": chapter.chapter_language,
                        "externalUrl": chapter.chapter_url,
                    },
                    "pageOrder": [],
                },
            }
            for chapter in chapters_to_upload
        ]

        if chapters_to_insert:
            upload_insertion = database_connection["to_upload"].insert_many(
                chapters_to_insert
            )
            print(upload_insertion.inserted_ids)

        if chapters_to_edit:
            edit_insertion = database_connection["to_edit"].insert_many(
                chapters_to_edit
            )
            print(edit_insertion.inserted_ids)

        database_connection["uploaded"].insert_many(
            [vars(chapter) for chapter in chapters_to_upload]
        )

        if chapters_to_update:
            database_connection["uploaded"].bulk_write(
                [
                    UpdateOne(
                        {"md_chapter_id": {"$eq": chapter["md_chapter_id"]}},
                        chapter,
                        upsert=True,
                    )
                    for chapter in chapters_to_update
                ]
            )

        # {"$or": [{"md_chapter_id": {"$eq": chapter["md_chapter_id"]}},
        #          {"chapter_id": {"$regex": chapter["chapter_url"]}}]}

        return

        # for count, chapter in enumerate(chapters_to_upload, start=1):
        #     chapter.md_manga_id = self.mangadex_manga_id
        #     chapter.extension_name = self.extension_name
        #     self.http_client.login()
        #
        #     chapter_to_upload_process = ChapterUploaderProcess(
        #         extension_name=self.extension_name,
        #         database_connection=self.database_connection,
        #         http_client=self.http_client,
        #         mangadex_manga_id=self.mangadex_manga_id,
        #         mangadex_group_id=self.mangadex_group_id,
        #         chapter=chapter,
        #         posted_md_updates=self.posted_md_updates,
        #         same_chapter_dict=self.same_chapter_dict,
        #     )
        #
        #     uploaded = chapter_to_upload_process.start_upload(self.chapters_on_md)
        #     if uploaded in ("on_md", "session_error", "edited"):
        #         if uploaded in ("on_md",):
        #             self.skipped += 1
        #         elif uploaded in ("edited",):
        #             self.edited += 1
        #         elif uploaded in ("session_error",):
        #             self.skipped += 1
        #             self.failed_uploads.append(chapter)
        #         continue
        #
        #     self.posted_chapters.append(chapter)
        #     time.sleep(1)
        #
        # if self.skipped != 0:
        #     skipped_chapters_message = f"Skipped {self.skipped} chapters out of {len(self.updated_chapters)} for extensions.{self.extension_name} manga {self.mangadex_manga_data['title']}: {self.mangadex_manga_id}."
        #     logger.info(skipped_chapters_message)
        #     print(skipped_chapters_message)
        #
        # if self.edited != 0:
        #     edited_chapters_message = f"Edited {self.edited} chapters out of {len(self.updated_chapters)} for extensions.{self.extension_name} manga {self.mangadex_manga_data['title']}: {self.mangadex_manga_id}."
        #     logger.info(edited_chapters_message)
        #     print(edited_chapters_message)
        #
        # PubloaderUpdatesWebhook(
        #     self.extension_name,
        #     self.mangadex_manga_data,
        #     self.posted_chapters,
        #     self.failed_uploads,
        #     self.skipped,
        #     self.edited,
        #     self.clean_db,
        # ).main(last_manga)
