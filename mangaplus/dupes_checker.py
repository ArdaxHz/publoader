import logging
import sqlite3
import time
from datetime import datetime
from typing import TYPE_CHECKING, Dict, List, Optional

import requests
from . import (
    convert_json,
    print_error,
    mangadex_api_url,
    ratelimit_time,
    mplus_group_id,
    upload_retry,
    mplus_language_map,
)
from .utils.helpter_functions import get_md_api
from .webhook import MPlusBotDupesWebhook
from .utils.utils import format_title

if TYPE_CHECKING:
    from .chapter_deleter import ChapterDeleterProcess

logger = logging.getLogger("mangaplus")


class DeleteDuplicatesMD:
    def __init__(
        self,
        session: requests.Session,
        manga_id_map: Dict[str, List[int]],
        deleter_process_object: "ChapterDeleterProcess",
        database_connection: sqlite3.Connection,
        manga_data_local: Dict[str, dict],
    ) -> None:
        self.session = session
        self.manga_id_map = manga_id_map
        self.deleter_process_object = deleter_process_object
        self.database_connection = database_connection
        self.manga_data_local = manga_data_local
        self.tracked_mangadex_ids = list(manga_id_map.keys())
        self.languages = list(set(mplus_language_map.values()))
        self.to_delete = []

    def fetch_aggregate(self, manga_id: str) -> Optional[dict]:
        logger.info(f"Getting aggregate info for manga {manga_id} in languages {self.languages}.")

        for i in range(upload_retry):
            try:
                aggregate_response = self.session.get(
                    f"{mangadex_api_url}/manga/{manga_id}/aggregate",
                    params={
                        "translatedLanguage[]": self.languages,
                        "groups[]": [mplus_group_id],
                    },
                    verify=False,
                )
            except requests.RequestException as e:
                logger.error(e)
                continue

            if aggregate_response.status_code in range(200, 300):
                aggregate_response_json = convert_json(aggregate_response)
                if aggregate_response_json is not None:
                    return aggregate_response_json["volumes"]

        error = print_error(aggregate_response)
        logger.error(f"Error returned from aggregate response for manga {manga_id}: {error}")

    def check_count(self, aggregate_chapters: dict) -> List[dict]:
        to_check = []
        for volume in aggregate_chapters:
            if isinstance(aggregate_chapters, dict):
                volume_iter = aggregate_chapters[volume]["chapters"]
            elif isinstance(aggregate_chapters, list):
                volume_iter = volume["chapters"]

            for chapter in volume_iter:
                if isinstance(chapter, str):
                    chapter_iter = volume_iter[chapter]
                elif isinstance(chapter, dict):
                    chapter_iter = chapter

                if chapter_iter["count"] > 1:
                    to_check.append(chapter_iter)
        return to_check

    def fetch_chapters(self, chapters: List[str]) -> Optional[List[dict]]:
        logger.debug(f"Getting chapter data for chapter ids: {chapters}")
        for i in range(upload_retry):
            try:
                chapters_response = self.session.get(
                    f"{mangadex_api_url}/chapter",
                    params={"ids[]": chapters, "limit": 100, "includes[]": ["manga"]},
                    verify=False,
                )
            except requests.RequestException as e:
                logger.error(e)
                continue

            if chapters_response.status_code in range(200, 300):
                chapters_response_json = convert_json(chapters_response)
                if chapters_response_json is not None:
                    return chapters_response_json["data"]

            error = print_error(chapters_response, log_error=True)
        return None

    def sort_manga_data(self, chapters: list):
        chapter = chapters[0]

        manga = [m for m in chapter["relationships"] if m["type"] == "manga"][0]
        manga_id = manga["id"]
        manga_title = format_title(manga)

        return {manga_id: {"id": manga_id, "title": manga_title}}

    def filter_group(self, chapter: dict) -> List[str]:
        return [g["id"] for g in chapter["relationships"] if g["type"] == "scanlation_group"]

    def check_chapters(
        self, chapters: List[dict], dupes_webhook: "MPlusBotDupesWebhook"
    ) -> List[dict]:
        to_check = []

        for chapter in chapters[1:]:
            current_index = chapters.index(chapter)
            previous_chapter = chapters[current_index - 1]

            current_attributes = chapter["attributes"]
            current_groups = self.filter_group(chapter)

            previous_attributes = previous_chapter["attributes"]
            previous_groups = self.filter_group(previous_chapter)

            if mplus_group_id in current_groups and mplus_group_id in previous_groups:
                if (
                    current_attributes["translatedLanguage"]
                    == previous_attributes["translatedLanguage"]
                    and current_attributes["chapter"] == previous_attributes["chapter"]
                    and current_attributes["externalUrl"] == previous_attributes["externalUrl"]
                ):
                    if chapter not in to_check:
                        to_check.append(chapter)

                    if previous_chapter not in to_check:
                        to_check.append(previous_chapter)

        if to_check:
            oldest = to_check[0]
            for chapter in to_check:
                if datetime.strptime(
                    chapter["attributes"]["createdAt"], "%Y-%m-%dT%H:%M:%S%z"
                ) < datetime.strptime(oldest["attributes"]["createdAt"], "%Y-%m-%dT%H:%M:%S%z"):
                    oldest = chapter

            oldest_id = oldest["id"]
            try:
                to_check.remove(oldest)
            except ValueError:
                pass

            if to_check:
                dupes_webhook.add_chapters(oldest, to_check)

                to_return_ids = list(set([c["id"] for c in to_check]))
                try:
                    to_return_ids.remove(oldest_id)
                except ValueError:
                    pass
                print(f"Found dupes of {oldest_id} to delete: {to_return_ids}")
                logger.info(f"Found dupes of {oldest_id} to delete: {to_return_ids}")
        return to_check

    def sort_chapters(self, chapters: list):
        sorted_chapters = {}
        for chapter in chapters:
            chapter_language = chapter["attributes"]["translatedLanguage"]
            if chapter_language not in sorted_chapters:
                sorted_chapters[chapter_language] = [chapter]
            else:
                sorted_chapters[chapter_language].append(chapter)
        return sorted_chapters

    def delete_dupes(self):
        print("Looking for chapter dupes.")

        for mang_index, manga_id in enumerate(self.tracked_mangadex_ids, start=1):
            manga_data = self.manga_data_local.get(manga_id)
            dupes_webhook = MPlusBotDupesWebhook(manga_data)
            dupes_found = False

            aggregate_chapters_all_langs_unchecked = self.fetch_aggregate(manga_id)
            if aggregate_chapters_all_langs_unchecked is None:
                continue

            aggregate_chapters_all_langs_checked = self.check_count(
                aggregate_chapters_all_langs_unchecked
            )

            main_chapters = [chapter["id"] for chapter in aggregate_chapters_all_langs_checked]
            other_chapters = []
            for chapter in aggregate_chapters_all_langs_checked:
                other_chapters.extend(chapter["others"])

            all_chapter_ids_unsorted = [*main_chapters, *other_chapters]
            all_chapter_ids_unsorted_split = [
                all_chapter_ids_unsorted[l : l + 100]
                for l in range(0, len(all_chapter_ids_unsorted), 100)
            ]

            chapters_md_unsorted = []
            for chapter_chunk in all_chapter_ids_unsorted_split:
                chapters_md_unsorted.extend(
                    get_md_api(
                        self.session,
                        "chapter",
                        **{"ids[]": chapter_chunk, "includes[]": ["manga"]},
                    )
                )

            chapters_md_sorted = self.sort_chapters(chapters_md_unsorted)

            if not bool(dupes_webhook.manga):
                manga_data = self.sort_manga_data(chapters_md_unsorted)
                dupes_webhook.init_manga(manga_data)

            for language in chapters_md_sorted:
                chapters_to_delete = self.check_chapters(
                    chapters_md_sorted[language], dupes_webhook
                )
                dupes_found = bool(chapters_to_delete)

                if not dupes_found:
                    continue

                logger.debug(f"Found dupes in manga {manga_id} for language {language}")

                chapters_to_delete_list: List[dict] = [
                    {
                        "md_chapter_id": c["id"],
                        "md_manga_id": manga_id,
                        "chapter_language": c["attributes"]["translatedLanguage"],
                        "chapter_number": c["attributes"]["chapter"],
                        "chapter_timestamp": 946684799,
                        "chapter_expire": 946684799,
                    }
                    for c in chapters_to_delete
                ]

                self.deleter_process_object.add_more_chapters(chapters_to_delete_list)

            if not dupes_found:
                print(f"Didn't find any dupes in manga: {manga_id}")
            else:
                print(f"--Found dupes in manga: {manga_id}")

            dupes_webhook.main()

        print("Finished looking for chapter dupes.")
