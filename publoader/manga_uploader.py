import logging
import re
import traceback
from typing import Dict, List, Optional

import pymongo
from pymongo import UpdateOne

from publoader.models.database import (
    database_connection,
    update_database,
    update_expired_chapter_database,
)
from publoader.models.dataclasses import Chapter
from publoader.models.http import http_client
from publoader.utils.misc import fetch_aggregate, find_key_from_list_value, flatten

logger = logging.getLogger("publoader")


class MangaUploaderProcess:
    def __init__(
        self,
        extension_name: str,
        clean_db: bool,
        updated_chapters: List[Chapter],
        all_manga_chapters: List[Chapter],
        mangadex_manga_id: str,
        mangadex_group_id: str,
        chapters_on_md: List[dict],
        current_uploaded_chapters: List[Chapter],
        same_chapter_dict: Dict[str, List[str]],
        mangadex_manga_data: dict,
        custom_language: Dict[str, str],
        chapters_on_db: List[Chapter],
        languages: List[str],
        chapters_for_upload: List[Chapter],
        chapters_for_skipping: List[Chapter],
        chapters_for_editing: List[Chapter],
        **kwargs,
    ):
        self.extension_name = extension_name
        self.clean_db = clean_db
        self.updated_chapters = updated_chapters
        self.all_manga_chapters = all_manga_chapters
        self.mangadex_manga_id = mangadex_manga_id
        self.mangadex_group_id = mangadex_group_id
        self.chapters_on_md = chapters_on_md
        self.posted_md_updates = current_uploaded_chapters
        self.same_chapter_dict = same_chapter_dict
        self.mangadex_manga_data = mangadex_manga_data
        self.custom_language = custom_language
        self.chapters_on_db = chapters_on_db
        self.languages = languages
        self.chapters_for_upload = chapters_for_upload
        self.chapters_for_skipping = chapters_for_skipping
        self.chapters_for_editing = chapters_for_editing
        self.get_chapter_volumes()

        if self.chapters_on_md:
            self._delete_extra_chapters()

    def _remove_chapters_not_external(self):
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

        update_expired_chapter_database(
            extension_name=self.extension_name,
            md_chapter=md_chapters_not_external,
            md_manga_id=self.mangadex_manga_id,
        )

    def _delete_extra_chapters(self):
        if not self.all_manga_chapters:
            return

        self._remove_chapters_not_external()

    def get_chapter_volumes(self):
        aggregate_chapters = fetch_aggregate(
            http_client,
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
                chapter.md_chapter_id = md_chapter["id"]
                return {"md_chapter": md_chapter, "chapter": chapter}
        return

    def _check_uploaded_different_id(self, chapter) -> bool:
        """Check if chapter id to upload has been uploaded already under a different
        id."""
        same_chapter_list_md = [
            c["attributes"]["externalUrl"]
            for c in self.chapters_on_md
            if c["attributes"]["chapter"] == chapter.chapter_number
            and c["attributes"]["translatedLanguage"] == chapter.chapter_language
        ]
        same_chapter_list_posted_ids = [
            str(c.chapter_id) for c in self.posted_md_updates
        ]

        if chapter.chapter_id in flatten(list(self.same_chapter_dict.values())):
            master_id = find_key_from_list_value(
                self.same_chapter_dict, chapter.chapter_id
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
                "md_chapter_id": md_id,
                "md_group_id": self.mangadex_group_id,
                "chapter": vars(chapter),
                "payload": data_to_post,
            }
        else:
            logger.info(f"Nothing to edit for chapter {md_id}")
        return

    def start_manga_uploading_process(self, last_manga: bool):
        chapters_dupe_checker = list(
            map(self._check_for_duplicate_chapter_md_list, self.updated_chapters)
        )

        chapters_to_upload = [
            chapter
            for chapter in self.updated_chapters
            if not bool(self._check_for_duplicate_chapter_md_list(chapter))
            and not self._check_uploaded_different_id(chapter)
        ]
        dupes = [dupe for dupe in chapters_dupe_checker if dupe is not None]

        chapters_to_edit = [
            dupe for dupe in map(self.edit_chapter, dupes) if dupe is not None
        ]
        dupes_for_editing = [Chapter(**dupe["chapter"]) for dupe in chapters_to_edit]

        chapters_skipped = [
            chapter["chapter"]
            for chapter in dupes
            if chapter["chapter"] not in chapters_to_upload
            and chapter["chapter"] not in dupes_for_editing
        ]

        chapters_to_insert = [
            {
                **{
                    "mangadex_manga_id": self.mangadex_manga_id,
                    "mangadex_group_id": self.mangadex_group_id,
                },
                **vars(chapter),
            }
            for chapter in chapters_to_upload
        ]

        print(f"Inserting chapters for manga {self.mangadex_manga_id}")

        if chapters_to_insert:
            try:
                upload_insertion = database_connection["to_upload"].bulk_write(
                    [
                        UpdateOne(
                            {
                                "chapter_number": {"$eq": chap["chapter_number"]},
                                "chapter_language": {"$eq": chap["chapter_language"]},
                                "chapter_id": {"$eq": chap["chapter_id"]},
                            },
                            {
                                "$setOnInsert": chap,
                            },
                            upsert=True,
                        )
                        for chap in chapters_to_insert
                    ]
                )
                logger.info(
                    f"Inserted manga {self.mangadex_manga_id} chapters to upload "
                    f"{upload_insertion.upserted_ids}"
                )
            except pymongo.errors.BulkWriteError as e:
                traceback.print_exc()
                logger.exception(
                    f"{self.start_manga_uploading_process.__name__} raised an error when bulk writing to 'to_upload'."
                )

        if chapters_to_edit:
            try:
                edit_insertion = database_connection["to_edit"].bulk_write(
                    [
                        UpdateOne(
                            {"md_chapter_id": {"$eq": chap["md_chapter_id"]}},
                            {
                                "$setOnInsert": chap,
                            },
                            upsert=True,
                        )
                        for chap in chapters_to_edit
                    ]
                )
                logger.info(
                    f"Inserted manga {self.mangadex_manga_id} chapters to upload "
                    f"{edit_insertion.upserted_ids}"
                )
            except pymongo.errors.BulkWriteError as e:
                traceback.print_exc()
                logger.exception(
                    f"{self.start_manga_uploading_process.__name__} raised an error when bulk writing to 'to_edit'."
                )

        self.chapters_for_upload.extend(chapters_to_upload)
        self.chapters_for_editing.extend(dupes_for_editing)
        self.chapters_for_skipping.extend(chapters_skipped)

        if len(chapters_skipped) != 0:
            skipped_chapters_message = (
                f"Skipped {len(chapters_skipped)} chapters out of "
            )
            f"{len(self.updated_chapters)} for extensions.{self.extension_name} manga "
            f"{self.mangadex_manga_data['title']}: {self.mangadex_manga_id}."
            logger.info(skipped_chapters_message)
            logger.debug(f"Chapters skipped: {chapters_skipped}")
            print(skipped_chapters_message)

        if len(dupes_for_editing) != 0:
            edited_chapters_message = (
                f"Edited {len(dupes_for_editing)} chapters out of "
            )
            f"{len(self.updated_chapters)} for extensions.{self.extension_name} manga "
            f"{self.mangadex_manga_data['title']}: {self.mangadex_manga_id}."
            logger.info(edited_chapters_message)
            logger.debug(f"Chapters to edit: {dupes_for_editing}")
            print(edited_chapters_message)

        update_database(chapter=dupes_for_editing + chapters_skipped)
        return

        # for count, chapter in enumerate(chapters_to_upload, start=1):
        #     chapter.md_manga_id = self.mangadex_manga_id
        #     chapter.extension_name = self.extension_name
        #     http_client.login()
        #
        #     chapter_to_upload_process = ChapterUploaderProcess(
        #         extension_name=self.extension_name,
        #         database_connection=self.database_connection,
        #         http_client=http_client,
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
