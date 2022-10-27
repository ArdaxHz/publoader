import aiohttp
import asyncio
import logging
import math
import re
import string
from dataclasses import replace
from datetime import datetime
from typing import Dict, List, Optional, Union


from . import response_pb2 as response_pb
from . import Chapter, Manga, get_md_id
from .webhook import MPlusBotWebhook

logger = logging.getLogger("mangaplus")
logger_debug = logging.getLogger("debug")


class MPlusAPI:
    def __init__(
        self,
        manga_map_mplus_ids: List[int],
        posted_chapters_ids: List[int],
        manga_id_map: Dict[str, List[int]],
        title_regexes: dict,
    ):
        self.tracked_manga = manga_map_mplus_ids
        self.posted_chapters_ids = posted_chapters_ids
        self.manga_id_map = manga_id_map
        self.updated_chapters: List[Chapter] = []
        self.all_mplus_chapters: List[Chapter] = []
        self.untracked_manga: List[Manga] = []
        self.mplus_base_api_url = "https://jumpg-webapi.tokyo-cdn.com"
        self.title_regexes = title_regexes
        self.num2words: Optional[str] = self._get_num2words_string()

        self.get_mplus_updated_manga()
        self.get_mplus_updates()

    def _get_num2words_string(self):
        num2words_list = self.title_regexes.get("num2words")
        if num2words_list is None:
            return

        return "(" + "|".join(self.title_regexes.get("num2words")) + ")"

    def _get_proto_response(self, response_proto: bytes) -> response_pb.Response:
        """Convert api response into readable data."""
        response = response_pb.Response()
        response.ParseFromString(response_proto)
        return response

    def _get_language(self, manga_id: int, language: str):
        if str(manga_id) in self.title_regexes.get("custom_language", {}):
            return self.title_regexes["custom_language"][str(manga_id)]
        return language

    async def _request_from_api(
        self, manga_id: Optional[int] = None, updated: Optional[bool] = False
    ) -> Optional[bytes]:
        """Get manga and chapter details from the api."""
        async with aiohttp.ClientSession() as session:
            try:
                if manga_id is not None:
                    url = "/api/title_detail"
                    params = {"title_id": manga_id}
                elif updated:
                    url = "/api/title_list/updated"
                    params = {}

                async with session.get(
                    self.mplus_base_api_url + url,
                    params=params,
                ) as response:
                    return await response.read()
            except Exception as e:
                logger.error(f"{e}: Couldn't get details from the mangaplus api.")
                print("Request API Error", e)
                return

    def get_mplus_updated_manga(self):
        """Find new untracked mangaplus series."""
        logger.info("Looking for new untracked manga.")
        print("Getting new manga.")

        loop = asyncio.get_event_loop()
        task = self._request_from_api(updated=True)
        updated_manga_response = loop.run_until_complete(task)

        if updated_manga_response is not None:
            updated_manga_response_parsed = self._get_proto_response(
                updated_manga_response
            )
            updated_manga_details = updated_manga_response_parsed.success.updated

            for manga in updated_manga_details.updated_manga_detail:
                if manga.updated_manga.manga_id not in self.tracked_manga:
                    manga_id = manga.updated_manga.manga_id
                    manga_name = manga.updated_manga.manga_name
                    language = self._get_language(
                        manga_id, manga.updated_manga.language
                    )

                    self.untracked_manga.append(
                        Manga(
                            manga_id=manga_id,
                            manga_name=manga_name,
                            manga_language=language,
                        )
                    )
                    logger.info(f"Found untracked manga {manga_id}: {manga_name}.")
                    print(f"Found untracked manga {manga_id}: {manga_name}.")

            if self.untracked_manga:
                untracked_manga_webhook = MPlusBotWebhook(
                    title="Untracked Manga",
                    description="\n".join(
                        [
                            f"`{manga.manga_id}`: `{manga.manga_name}`"
                            for manga in self.untracked_manga
                        ]
                    ),
                )
                untracked_manga_webhook.send()

    def get_mplus_updates(self):
        """Get latest chapter updates."""
        logger.info("Looking for tracked manga new chapters.")
        print("Getting new chapters.")
        tasks = []

        spliced_manga = [
            self.tracked_manga[l : l + 3] for l in range(0, len(self.tracked_manga), 3)
        ]

        loop = asyncio.get_event_loop()
        for mangas in spliced_manga:
            task = self._chapter_updates(mangas)
            tasks.append(task)

        loop.run_until_complete(asyncio.gather(*tasks))

    def _normalise_chapter_object(
        self, chapter_list, manga_object: Manga
    ) -> List[Chapter]:
        """Return a list of chapter objects made from the api chapter lists."""
        return [
            Chapter(
                chapter_id=chapter.chapter_id,
                chapter_timestamp=chapter.start_timestamp,
                chapter_title=chapter.chapter_name,
                chapter_expire=chapter.end_timestamp,
                chapter_number=chapter.chapter_number,
                chapter_language=self._get_language(
                    manga_object.manga_id, manga_object.manga_language
                ),
                manga_id=manga_object.manga_id,
                md_manga_id=get_md_id(self.manga_id_map, manga_object.manga_id),
                manga=manga_object,
            )
            for chapter in chapter_list
        ]

    async def _chapter_updates(self, mangas: list):
        """Get the updated chapters from each manga."""
        for manga in mangas:
            manga_response = await self._request_from_api(manga_id=manga)
            if manga_response is None:
                continue

            manga_response_parsed = self._get_proto_response(manga_response)

            manga_chapters = manga_response_parsed.success.manga_detail
            manga_object = Manga(
                manga_id=manga_chapters.manga.manga_id,
                manga_name=manga_chapters.manga.manga_name,
                manga_language=self._get_language(
                    manga_chapters.manga.manga_id, manga_chapters.manga.language
                ),
            )

            manga_chapters_lists = []
            manga_chapters_lists.append(
                self._normalise_chapter_object(
                    list(manga_chapters.first_chapter_list), manga_object
                )
            )

            if len(manga_chapters.last_chapter_list) > 0:
                manga_chapters_lists.append(
                    self._normalise_chapter_object(
                        list(manga_chapters.last_chapter_list), manga_object
                    )
                )

            all_chapters = self.get_latest_chapters(
                manga_chapters_lists, self.posted_chapters_ids, True
            )
            self.all_mplus_chapters.extend(all_chapters)

            updated_chapters = self.get_latest_chapters(
                manga_chapters_lists, self.posted_chapters_ids
            )
            if updated_chapters:
                logger.info(f"MangaPlus newly updated chapters: {updated_chapters}")

            if updated_chapters:
                print(f"Manga {manga_object.manga_name}: {manga_object.manga_id}.")
                for update in updated_chapters:
                    print(
                        f"--Found {update.chapter_id}, chapter: {update.chapter_number}, language: {update.chapter_language}, title: {update.chapter_title}."
                    )

            self.updated_chapters.extend(
                [
                    chapter
                    for chapter in updated_chapters
                    if chapter.chapter_id not in self.posted_chapters_ids
                    and datetime.fromtimestamp(chapter.chapter_expire) >= datetime.now()
                ]
            )

    def _get_surrounding_chapter(
        self,
        chapters: List[Chapter],
        current_chapter: Chapter,
        next_chapter_search: bool = False,
    ) -> Optional[Chapter]:
        """Find the chapter before or after the current."""
        # Starts from the first chapter before the current
        index_search = reversed(chapters[: chapters.index(current_chapter)])
        if next_chapter_search:
            # Starts from the first chapter after the current
            index_search = chapters[chapters.index(current_chapter) :]

        for chapter in index_search:
            number_match = re.match(
                pattern=r"^#?(\d+)", string=chapter.chapter_number, flags=re.I
            )

            if bool(number_match):
                number = number_match.group(1)
            else:
                number = re.split(
                    r"[\s{}]+".format(re.escape(string.punctuation)),
                    chapter.chapter_number.strip("#"),
                )[0]

            try:
                int(number)
            except ValueError:
                continue
            else:
                return chapter

    def _strip_chapter_number(self, number: Union[str, int]) -> str:
        """Returns the chapter number without the un-needed # or 0."""
        stripped = str(number).strip().strip("#")

        parts = re.split(r"\.|\-", stripped)
        parts[0] = "0" if len(parts[0].lstrip("0")) == 0 else parts[0].lstrip("0")
        stripped = ".".join(parts)

        return stripped

    def _normalise_chapter_number(
        self, chapters: List[Chapter], chapter: Chapter
    ) -> List[Optional[str]]:
        """Rid the extra data from the chapter number for use in ManagDex."""
        current_number = self._strip_chapter_number(chapter.chapter_number)
        chapter_number = chapter.chapter_number
        if chapter_number is not None:
            chapter_number = current_number

        if chapter_number == "ex":
            # Get previous chapter's number for chapter number
            previous_chapter = self._get_surrounding_chapter(chapters, chapter)
            next_chapter_number = None
            previous_chapter_number = None

            if previous_chapter is None:
                # Previous chapter isn't available, use next chapter's number
                # if available
                next_chapter = self._get_surrounding_chapter(
                    chapters, chapter, next_chapter_search=True
                )
                if next_chapter is None:
                    chapter_number = None
                else:
                    next_chapter_number = self._strip_chapter_number(
                        next_chapter.chapter_number
                    )
                    chapter_number = (
                        int(re.split(r"\.|\-|\,", next_chapter_number)[0]) - 1
                    )
                    first_index = next_chapter
                    second_index = chapter
            else:
                previous_chapter_number = self._strip_chapter_number(
                    previous_chapter.chapter_number
                )
                if "," in previous_chapter_number:
                    chapter_number = previous_chapter_number.split(",")[-1]
                else:
                    chapter_number = re.split(r"\.|\-", previous_chapter_number)[0]
                first_index = chapter
                second_index = previous_chapter

            if chapter_number == "ex":
                chapter_number = None

            if chapter_number is not None and current_number != "ex":
                # If difference between current chapter and previous/next
                # chapter is more than 5, use None as chapter_number
                if math.sqrt((int(current_number) - int(chapter_number)) ** 2) >= 5:
                    chapter_number = None

            if chapter_number is not None:
                chapter_decimal = "5"

                # There may be multiple extra chapters before the last numbered chapter
                # Use index difference as decimal to avoid not uploading
                # non-dupes
                try:
                    chapter_difference = chapters.index(first_index) - chapters.index(
                        second_index
                    )
                    if chapter_difference > 1:
                        chapter_decimal = chapter_difference
                except (ValueError, IndexError):
                    pass

                chapter_number = f"{chapter_number}.{chapter_decimal}"
        elif chapter_number.lower() in ("one-shot", "one.shot"):
            chapter_number = None
        elif chapter_number.lower().startswith(("spin-off", "spin.off")):
            chapter_number = re.sub(
                r"(?:spin\-off|spin\.off)\s?", "", chapter_number.lower(), re.I
            ).strip()

        if chapter_number is None:
            chapter_number_split = [chapter_number]
        else:
            chapter_number_split = [
                self._strip_chapter_number(chap_number)
                for chap_number in chapter_number.split(",")
            ]

        chapter_number_split: List[Optional[str]] = chapter_number_split
        return chapter_number_split

    def _normalise_chapter_title(
        self, chapter: Chapter, chapter_number: List[Optional[str]]
    ) -> Optional[str]:
        """Strip away the title prefix."""
        colon_regex = re.compile(
            r"^(?:\S+\s?)?\d+(?:(?:[\,\-\.])\d{0,2})?\s?[\:]\s?", re.I
        )
        no_title_regex = re.compile(r"^\S+\s?\d+(?:(?:[\,\-\.])\d{0,2})?$", re.I)
        hashtag_regex = re.compile(r"^(?:\S+\s?)?#\d+(?:(?:[\,\-\.])\d{0,2})?\s?", re.I)
        period_dash_regex = re.compile(
            r"^(?:\S+\s?)?\d+(?:(?:[\,\-\.])\d{0,2})?\s?[\.\/\-]\s?", re.I
        )
        spaces_regex = re.compile(r"^(?:\S+\s?)?\d+(?:(?:[\,\-\.])\d{0,2})?\s?", re.I)
        final_chapter_regex = re.compile(
            r"^(?:final|last)\s?(?:chapter|ep|episode)\s?[\:\.]\s?", re.I
        )
        word_numbers_regex = None
        if self.num2words is not None:
            word_numbers_regex = re.compile(
                rf"^(?:\S+\s?)\s?{self.num2words}\s?(?:{self.num2words}\s?)?\:\s?", re.I
            )

        original_title = str(chapter.chapter_title).strip()
        normalised_title = original_title
        pattern_to_use: Optional[re.Pattern[str]] = None
        replace_string = ""
        custom_regex = None

        if (
            chapter.manga_id in self.title_regexes.get("empty", [])
            and None not in chapter_number
            or original_title.lower() in ("final chapter",)
        ):
            normalised_title = None
            custom_regex = "Empty Title"
        elif chapter.manga_id in self.title_regexes.get("noformat", []):
            normalised_title = original_title
            custom_regex = "Original Title"
        elif str(chapter.manga_id) in self.title_regexes.get("custom", {}):
            pattern_to_use = re.compile(
                self.title_regexes["custom"][str(chapter.manga_id)], re.I
            )
            custom_regex = "Custom Regex"
        elif final_chapter_regex.match(original_title):
            pattern_to_use = final_chapter_regex
            custom_regex = "Final Chapter Regex"
        elif word_numbers_regex is not None and word_numbers_regex.match(
            original_title
        ):
            pattern_to_use = word_numbers_regex
            custom_regex = "Word Numbers Regex"
        elif colon_regex.match(original_title):
            pattern_to_use = colon_regex
        elif no_title_regex.match(original_title):
            pattern_to_use = no_title_regex
        elif period_dash_regex.match(original_title):
            pattern_to_use = period_dash_regex
        elif hashtag_regex.match(original_title):
            pattern_to_use = hashtag_regex
        elif spaces_regex.match(original_title):
            pattern_to_use = spaces_regex

        if pattern_to_use is not None:
            normalised_title = pattern_to_use.sub(
                repl=replace_string, string=original_title, count=1
            ).strip()

        if normalised_title == "":
            normalised_title = None

        logger_debug.debug(
            f"Chapter title normaliser chapter_id: {chapter.chapter_id}, manga_id: {chapter.manga_id}, {custom_regex=}, regex used: {pattern_to_use!r}, {original_title=}, {normalised_title=}"
        )
        return normalised_title

    def get_latest_chapters(
        self,
        manga_chapters_lists: List[List[Chapter]],
        posted_chapters: List[int],
        all_chapters: bool = False,
    ) -> List[Chapter]:
        """Get the latest un-uploaded chapters."""
        updated_chapters = []

        for chapters in manga_chapters_lists:
            # Go through the last three chapters
            for chapter in chapters:
                if not all_chapters:
                    # Chapter id is not in database or chapter expiry isn't
                    # before now
                    if (
                        chapter.chapter_id in posted_chapters
                        or datetime.fromtimestamp(chapter.chapter_expire)
                        <= datetime.now()
                    ):
                        continue

                chapter_number_split = self._normalise_chapter_number(chapters, chapter)

                chapter_title = self._normalise_chapter_title(
                    chapter, chapter_number_split
                )

                # MPlus sometimes joins two chapters as one, upload to md as
                # two different chapters
                for chap_number in chapter_number_split:
                    changes = {
                        "chapter_number": chap_number,
                        "chapter_title": chapter_title,
                    }
                    chapter_object: Chapter = replace(chapter, **changes)
                    updated_chapters.append(chapter_object)

        return updated_chapters
