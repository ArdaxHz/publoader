import logging
from datetime import datetime
from typing import TYPE_CHECKING, Dict, List, Optional

from publoader.webhook import PubloaderDupesWebhook
from publoader.models.database import update_expired_chapter_database
from publoader.models.http import RequestError, http_client
from publoader.utils.config import mangadex_api_url
from publoader.utils.misc import (
    fetch_aggregate,
    get_md_api,
    iter_aggregate_chapters,
    format_title,
)


logger = logging.getLogger("publoader")


class DeleteDuplicatesMD:
    def __init__(
        self,
        extension_name: str,
        tracked_mangadex_ids: List[str],
        manga_data_local: Dict[str, dict],
        extension_languages: List[str],
        mangadex_group_id: str,
    ) -> None:
        self.extension_name = extension_name
        self.tracked_mangadex_ids = tracked_mangadex_ids
        self.manga_data_local = manga_data_local
        self.languages = list(set(extension_languages))
        self.mangadex_group_id = mangadex_group_id
        self.to_delete = []

    def check_count(self, aggregate_chapters: dict) -> List[dict]:
        to_check = []
        for chapter in iter_aggregate_chapters(aggregate_chapters):
            if chapter["count"] > 1:
                to_check.append(chapter)
        return to_check

    def fetch_chapters(self, chapters: List[str]) -> Optional[List[dict]]:
        logger.debug(f"Getting chapter data for chapter ids: {chapters}")
        try:
            chapters_response = http_client.get(
                f"{mangadex_api_url}/chapter",
                params={"ids[]": chapters, "limit": 100, "includes[]": ["manga"]},
                verify=False,
            )
        except RequestError as e:
            logger.error(e)
            return

        if (
            chapters_response.status_code in range(200, 300)
            and chapters_response.data is not None
        ):
            return chapters_response.data["data"]

    def sort_manga_data(self, chapters: list):
        chapter = chapters[0]

        manga = [m for m in chapter["relationships"] if m["type"] == "manga"][0]
        manga_id = manga["id"]
        manga_title = format_title(manga)

        return {manga_id: {"id": manga_id, "title": manga_title}}

    def filter_group(self, chapter: dict) -> List[str]:
        return [
            g["id"] for g in chapter["relationships"] if g["type"] == "scanlation_group"
        ]

    def check_chapters(
        self, chapters: List[dict], dupes_webhook: "PubloaderDupesWebhook"
    ) -> List[dict]:
        to_check = []

        for chapter in chapters[1:]:
            current_index = chapters.index(chapter)
            previous_chapter = chapters[current_index - 1]

            current_attributes = chapter["attributes"]
            current_groups = self.filter_group(chapter)

            previous_attributes = previous_chapter["attributes"]
            previous_groups = self.filter_group(previous_chapter)

            if (
                self.mangadex_group_id in current_groups
                and self.mangadex_group_id in previous_groups
            ):
                if (
                    current_attributes["translatedLanguage"]
                    == previous_attributes["translatedLanguage"]
                    and current_attributes["chapter"] == previous_attributes["chapter"]
                    and current_attributes["externalUrl"]
                    == previous_attributes["externalUrl"]
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
                ) < datetime.strptime(
                    oldest["attributes"]["createdAt"], "%Y-%m-%dT%H:%M:%S%z"
                ):
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
            dupes_webhook = PubloaderDupesWebhook(self.extension_name, manga_data)
            dupes_found = False

            logger.info(
                f"Getting aggregate info for extensions.{self.extension_name} manga {manga_id} in languages {self.languages}."
            )
            aggregate_chapters_all_langs_unchecked = fetch_aggregate(
                http_client,
                manga_id,
                **{
                    "translatedLanguage[]": self.languages,
                    "groups[]": [self.mangadex_group_id],
                },
            )
            if aggregate_chapters_all_langs_unchecked is None:
                logger.info(
                    f"Aggregate fetching for extensions.{self.extension_name} manga {manga_id} returned null."
                )
                continue

            logger.debug(
                f"Checking which chapters have more than one of the same number chapters."
            )
            aggregate_chapters_all_langs_checked = self.check_count(
                aggregate_chapters_all_langs_unchecked
            )

            main_chapters = [
                chapter["id"] for chapter in aggregate_chapters_all_langs_checked
            ]
            other_chapters = []
            for chapter in aggregate_chapters_all_langs_checked:
                other_chapters.extend(chapter["others"])

            all_chapter_ids_unsorted = [*main_chapters, *other_chapters]
            all_chapter_ids_unsorted_split = [
                all_chapter_ids_unsorted[elem : elem + 100]
                for elem in range(0, len(all_chapter_ids_unsorted), 100)
            ]

            logger.debug(f"Getting chapter data for chapters with more than one count.")

            chapters_md_unsorted = []
            for chapter_chunk in all_chapter_ids_unsorted_split:
                chapters_md_unsorted.extend(
                    get_md_api(
                        "chapter", **{"ids[]": chapter_chunk, "includes[]": ["manga"]}
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

                if not chapters_to_delete:
                    continue

                logger.debug(f"Found dupes in manga {manga_id} for language {language}")

                # update_expired_chapter_database(
                #     extension_name=self.extension_name,
                #     md_chapter=chapters_to_delete,
                #     md_manga_id=manga_id,
                # )

            if not dupes_found:
                print(f"Didn't find any dupes in manga: {manga_id}")
            else:
                print(f"--Found dupes in manga: {manga_id}")

            dupes_webhook.main()

        print("Finished looking for chapter dupes.")
