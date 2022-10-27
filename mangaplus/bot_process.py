import configparser
import json
import logging
import math
import multiprocessing
import sqlite3
import time
from typing import TYPE_CHECKING, Dict, List

import requests

from .utils.utils import format_title
from .webhook import MPlusBotNotIndexedWebhook
from .manga_uploader import MangaUploaderProcess
from . import (
    convert_json,
    print_error,
    mangadex_api_url,
    ratelimit_time,
    mplus_group_id,
    Chapter,
    update_database,
    components_path,
    get_md_id,
)

if TYPE_CHECKING:
    from .chapter_deleter import ChapterDeleterProcess
    from .auth_md import AuthMD

logger = logging.getLogger("mangaplus")


class BotProcess:
    def __init__(
        self,
        config: configparser.RawConfigParser,
        session: requests.Session,
        updates: List[Chapter],
        all_mplus_chapters: List[Chapter],
        deleter_process_object: "ChapterDeleterProcess",
        md_auth_object: "AuthMD",
        manga_id_map: Dict[str, List[int]],
        database_connection: sqlite3.Connection,
        title_regexes: Dict[str, List[int]],
        clean_db: bool,
        chapters_on_db: List[dict],
        manga_data_local: Dict[str, dict],
    ):
        self.config = config
        self.session = session
        self.updates = updates
        self.all_mplus_chapters = all_mplus_chapters
        self.deleter_process_object = deleter_process_object
        self.md_auth_object = md_auth_object
        self.manga_id_map = manga_id_map
        self.database_connection = database_connection
        self.title_regexes = title_regexes
        self.same_chapter_dict: Dict[str, List[int]] = self.title_regexes.get(
            "same", {}
        )
        self.clean_db = clean_db
        self.chapters_on_db = chapters_on_db
        self.processes: List[multiprocessing.Process] = []
        self.current_uploaded_chapters: List[Chapter] = []
        self.manga_data_local = manga_data_local
        self.chapters_on_md = self._get_mplus_chapters()
        self.manga_untracked = [
            m
            for m in list(self.chapters_on_md.keys())
            if m not in list(self.manga_id_map.keys())
        ]

        self._get_manga_data_md()

        logger.info(f"Manga not tracked but on mangadex: {self.manga_untracked}")

    def _remove_chapters_not_mplus(self) -> List[dict]:
        """Find chapters on MangaDex not on MangaPlus."""
        chapters_to_delete = []

        for manga_id in self.chapters_on_md:
            if manga_id in self.manga_untracked:
                for expired in self.chapters_on_md[manga_id]:
                    md_chapter_id = expired["id"]

                    expired_chapter_object = Chapter(
                        chapter_timestamp=946684799,
                        chapter_expire=946684799,
                        chapter_language=expired["attributes"]["translatedLanguage"],
                        chapter_title=expired["attributes"]["title"],
                        chapter_number=expired["attributes"]["chapter"],
                        md_manga_id=manga_id,
                        md_chapter_id=md_chapter_id,
                    )

                    update_database(
                        self.database_connection, expired_chapter_object, md_chapter_id
                    )
                    chapters_to_delete.append(vars(expired_chapter_object))

        return chapters_to_delete

    def _delete_extra_chapters(self):
        chapters_to_delete = self._remove_chapters_not_mplus()
        logger.info(
            f"{self.__class__.__name__} deleter finder found: {chapters_to_delete}"
        )
        if chapters_to_delete:
            self.deleter_process_object.add_more_chapters(chapters_to_delete)

    def _get_md_api(self, route: str, params: dict) -> List[dict]:
        """Go through each page in the api to get all the chapters/manga."""
        chapters = []
        limit = 100
        offset = 0
        pages = 1
        iteration = 1
        created_at_since_time = "2000-01-01T00:00:00"

        parameters = {}
        parameters.update(params)

        while True:
            # Update the parameters with the new offset
            parameters.update(
                {
                    "limit": limit,
                    "offset": offset,
                    "createdAtSince": created_at_since_time,
                }
            )

            # Call the api and get the json data
            try:
                chapters_response = self.session.get(
                    f"{mangadex_api_url}/{route}", params=parameters, verify=False
                )
            except requests.RequestException as e:
                logger.error(e)
                limit = limit
                offset = offset
                created_at_since_time = created_at_since_time
                parameters = parameters
                continue

            if chapters_response.status_code != 200:
                manga_response_message = f"Couldn't get the {route}s of the group."
                print_error(chapters_response, log_error=True)
                logger.error(manga_response_message)
                continue

            chapters_response_data = convert_json(chapters_response)
            if chapters_response_data is None:
                logger.warning(f"Couldn't convert {route}s data into json, retrying.")
                continue

            chapters.extend(chapters_response_data["data"])
            offset += limit

            # Finds how many pages needed to be called
            if pages == 1:
                chapters_count = chapters_response_data.get("total", 0)

                if not chapters_response_data["data"]:
                    chapters_count = 0

                if chapters_count > limit:
                    pages = math.ceil(chapters_count / limit)

                logger.debug(f"{pages} page(s) for group {route}s.")

            # Wait every 5 pages
            if iteration % 5 == 0 and pages != 5:
                time.sleep(ratelimit_time)

            # End the loop when all the pages have been gone through
            # Offset 10000 is the highest you can go, reset offset and get next
            # 10k batch using the last available chapter's created at date
            if (
                iteration == pages
                or offset >= 10000
                or not chapters_response_data["data"]
            ):
                if chapters_count >= 10000 and offset == 10000:
                    logger.debug(f"Reached 10k {route}s, looping over next 10k.")
                    created_at_since_time = chapters[-1]["attributes"][
                        "createdAt"
                    ].split("+")[0]
                    offset = 0
                    pages = 1
                    iteration = 1
                    time.sleep(5)
                    continue
                break

            iteration += 1

        time.sleep(ratelimit_time)
        return chapters

    def _get_mplus_chapters(self) -> Dict[str, List[dict]]:
        logger.debug("Getting all m+'s uploaded chapters.")
        print("Getting the mangaplus chapters on mangadex.")
        chapters_unsorted = self._get_md_api(
            "chapter",
            params={
                "groups[]": [mplus_group_id],
                "order[createdAt]": "desc",
                "includes[]": ["manga"],
            },
        )

        logger.debug("Sorting the uploaded chapters by MD ID.")
        chapters_sorted = {}
        for chapter in chapters_unsorted:
            manga = [g for g in chapter["relationships"] if g["type"] == "manga"][0]
            manga_id = manga["id"]

            try:
                chapters_sorted[manga_id].append(chapter)
            except (KeyError, ValueError, AttributeError):
                chapters_sorted[manga_id] = [chapter]
        return chapters_sorted

    def _get_manga_data_md(self) -> Dict[str, dict]:
        """Get the manga data from mangadex if needed and sort by manga id."""
        get_manga_data = []

        tracked_manga = self.manga_id_map.keys()
        for tracked in tracked_manga:
            if tracked not in self.manga_data_local.keys():
                get_manga_data.append(tracked)

        if get_manga_data:
            tracked_manga_splice = [
                get_manga_data[l : l + 100] for l in range(0, len(get_manga_data), 100)
            ]

            tracked_manga_data = []

            for manga_splice in tracked_manga_splice:
                tracked_manga_data.extend(
                    self._get_md_api(
                        "manga",
                        params={
                            "ids[]": manga_splice,
                            "order[createdAt]": "desc",
                        },
                    )
                )

            for manga in tracked_manga_data:
                manga_id = manga["id"]
                manga_title = format_title(manga)
                if manga_id not in self.manga_data_local:
                    self.manga_data_local.update(
                        {manga_id: {"id": manga_id, "title": manga_title}}
                    )

            with open(
                components_path.joinpath(self.config["Paths"]["manga_data_path"]),
                "w",
            ) as json_file:
                json.dump(self.manga_data_local, json_file, indent=2)

        return self.manga_data_local

    def _sort_chapters_by_manga(
        self, updates: List[Chapter]
    ) -> Dict[str, List[Chapter]]:
        """Sort the chapters by manga id."""
        chapters_sorted = {}

        for chapter in updates:
            md_id = get_md_id(self.manga_id_map, chapter.manga_id)
            if md_id is None:
                logger.warning(f"No mangadex id found for mplus id {chapter.manga_id}.")
                continue

            try:
                chapters_sorted[md_id].append(chapter)
            except (KeyError, ValueError, AttributeError):
                chapters_sorted[md_id] = [chapter]
        return chapters_sorted

    def _check_all_chapters_uploaded(self):
        """Check if all the chapters uploaded to MangaDex were indexed correctly."""
        logger.info(
            "Checking if all currently uploaded chapters are available on MangaDex."
        )
        print("Checking which chapters weren't indexed.")
        chapters_on_md = []
        chapters_not_on_md = []

        uploaded_chapter_ids = [
            chapter.md_chapter_id
            for chapter in self.current_uploaded_chapters
            if chapter.md_chapter_id is not None
        ]

        # if self.clean_db:
        #     uploaded_chapter_ids.extend(
        #         [
        #             chapter["md_chapter_id"]
        #             for chapter in self.chapters_on_db
        #             if datetime.fromtimestamp(chapter["chapter_expire"])
        #             >= datetime.now()
        #             and chapter["md_chapter_id"] is not None
        #         ]
        #     )

        uploaded_chapter_ids = list(set(uploaded_chapter_ids))
        if uploaded_chapter_ids:
            logger.info(f"Uploaded chapters mangadex ids: {uploaded_chapter_ids}")
            uploaded_chapter_ids_split = [
                uploaded_chapter_ids[l : l + 100]
                for l in range(0, len(uploaded_chapter_ids), 100)
            ]

            for uploaded_ids in uploaded_chapter_ids_split:
                chapters_on_md.extend(
                    self._get_md_api(
                        "chapter",
                        params={
                            "ids[]": uploaded_ids,
                            "order[createdAt]": "desc",
                            "includes[]": ["manga"],
                        },
                    )
                )

            chapters_not_on_md = [
                chapter_id
                for chapter_id in uploaded_chapter_ids
                if chapter_id not in [chapter["id"] for chapter in chapters_on_md]
            ]

            logger.info(f"Chapters not indexed: {chapters_not_on_md}")
            MPlusBotNotIndexedWebhook(chapters_not_on_md).main()
        else:
            logger.info("No uploaded chapter mangadex ids.")

    def upload_chapters(self):
        """Go through each new chapter and upload it to mangadex."""
        # Sort each chapter by manga
        updated_manga_chapters = self._sort_chapters_by_manga(self.updates)
        all_manga_chapters = self._sort_chapters_by_manga(self.all_mplus_chapters)
        self._delete_extra_chapters()

        for index, mangadex_manga_id in enumerate(updated_manga_chapters, start=1):
            self.md_auth_object.login()
            manga_uploader = MangaUploaderProcess(
                database_connection=self.database_connection,
                session=self.session,
                updated_chapters=updated_manga_chapters[mangadex_manga_id],
                all_manga_chapters=all_manga_chapters[mangadex_manga_id],
                mangadex_manga_id=mangadex_manga_id,
                deleter_process_object=self.deleter_process_object,
                md_auth_object=self.md_auth_object,
                chapters_on_md=self.chapters_on_md.get(mangadex_manga_id, []),
                current_uploaded_chapters=self.current_uploaded_chapters,
                same_chapter_dict=self.same_chapter_dict,
                mangadex_manga_data=self.manga_data_local.get(mangadex_manga_id, ""),
                custom_language=self.title_regexes.get("custom_language", {}),
                chapters_on_db=self.chapters_on_db,
            )
            manga_uploader.start_manga_uploading_process(
                index == len(updated_manga_chapters)
            )

        if self.current_uploaded_chapters or self.clean_db:
            time.sleep(ratelimit_time)
            self._check_all_chapters_uploaded()
