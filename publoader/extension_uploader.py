import configparser
import json
import logging
import time
from typing import Dict, List

from publoader.manga_uploader import MangaUploaderProcess
from publoader.models.database import update_expired_chapter_database
from publoader.models.dataclasses import Chapter, Manga
from publoader.utils.config import ratelimit_time, resources_path
from publoader.utils.misc import format_title, get_md_api
from publoader.webhook import PubloaderNotIndexedWebhook, PubloaderWebhook

logger = logging.getLogger("publoader")


class ExtensionUploader:
    def __init__(
        self,
        config: configparser.RawConfigParser,
        extension,
        extension_name: str,
        updates: List[Chapter],
        all_chapters: List[Chapter],
        untracked_manga: List[Manga],
        tracked_mangadex_ids: List[str],
        mangadex_group_id: str,
        custom_regexes: dict,
        extension_languages: list[str],
        clean_db: bool,
        chapters_on_db: List[Chapter],
        manga_data_local: Dict[str, dict],
    ):
        self.config = config
        self.extension = extension
        self.clean_db = clean_db
        self.chapters_on_db = chapters_on_db
        self.manga_data_local = manga_data_local

        self.updates = updates
        self.extension_name = extension_name
        self.all_chapters = all_chapters
        self.untracked_manga = untracked_manga
        self.tracked_mangadex_ids = tracked_mangadex_ids
        self.custom_regexes = custom_regexes
        self.mangadex_group_id = mangadex_group_id
        self.extension_languages = extension_languages

        self.send_begin_extension_uploading()
        self.send_untracked_manga_webhook()

        self.same_chapter_dict: Dict[str, List[str]] = self.custom_regexes.get(
            "same", {}
        )

        self.current_uploaded_chapters: List[Chapter] = []
        self._get_manga_data_md()

        for chapter in self.updates:
            chapter.extension_name = self.extension_name

        self.updated_manga_chapters = self._sort_chapters_by_manga(self.updates)
        self.chapters_on_md = self._get_external_chapters_md()

        self.chapters_for_upload: List[Chapter] = []
        self.chapters_for_skipping: List[Chapter] = []
        self.chapters_for_editing: List[Chapter] = []

        self.manga_untracked = [
            m
            for m in list(self.chapters_on_md.keys())
            if m not in self.tracked_mangadex_ids
        ]

        logger.info(f"Manga not tracked but on mangadex: {self.manga_untracked}")

    def send_begin_extension_uploading(self):
        PubloaderWebhook(
            self.extension_name,
            title=f"Posting updates for extension {self.extension_name}",
        ).send()

    def send_untracked_manga_webhook(self):
        for untracked in self.untracked_manga:
            logger.info(
                f"Found untracked manga {untracked.manga_id}: {untracked.manga_name}."
            )
            print(
                f"Found untracked manga {untracked.manga_id}: {untracked.manga_name}."
            )

        if self.untracked_manga:
            untracked_manga_webhook = PubloaderWebhook(
                extension_name=self.extension_name,
                title="Untracked Manga",
                description="\n".join(
                    [
                        f"**{manga.manga_name}**: [{manga.manga_id}]({manga.manga_url})"
                        for manga in self.untracked_manga
                    ]
                ),
            )
            untracked_manga_webhook.send()

    def _delete_extra_chapters(self):
        """Find chapters on MangaDex not on external."""
        logger.info(
            f"{self.__class__.__name__} deleting chapters that don't exist on external."
        )
        for manga_id in self.chapters_on_md:
            if manga_id in self.manga_untracked:
                update_expired_chapter_database(
                    extension_name=self.extension_name,
                    md_chapter=self.chapters_on_md[manga_id],
                    md_manga_id=manga_id,
                )

    def _get_external_chapters_md(self) -> Dict[str, List[dict]]:
        logger.debug(f"Getting all {self.extension_name}'s uploaded chapters.")
        print(f"Getting the {self.extension_name} chapters on mangadex.")
        chapters_sorted = {}
        for manga_id in set(self.updated_manga_chapters.keys()):
            chapters_sorted[manga_id] = get_md_api(
                "chapter",
                **{
                    "groups[]": [self.mangadex_group_id],
                    "order[createdAt]": "desc",
                    "manga": manga_id,
                },
            )
        return chapters_sorted

    def _get_manga_data_md(self) -> Dict[str, dict]:
        """Get the manga data from mangadex if needed and sort by manga id."""
        get_manga_data = []

        tracked_manga = self.tracked_mangadex_ids
        for tracked in tracked_manga:
            if tracked not in self.manga_data_local.keys():
                get_manga_data.append(tracked)

        if get_manga_data:
            tracked_manga_splice = [
                get_manga_data[elem : elem + 100]
                for elem in range(0, len(get_manga_data), 100)
            ]

            tracked_manga_data = []

            for manga_splice in tracked_manga_splice:
                tracked_manga_data.extend(
                    get_md_api(
                        "manga",
                        **{
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
                resources_path.joinpath(self.config["Paths"]["manga_data_path"]),
                "w",
            ) as json_file:
                json.dump(self.manga_data_local, json_file, indent=2)

        return self.manga_data_local

    def _sort_chapters_by_manga(
        self, updates: List[Chapter]
    ) -> Dict[str, List[Chapter]]:
        """Sort the chapters by manga id."""
        chapters_sorted = {}
        if not updates:
            return {}

        for chapter in updates:
            md_id = chapter.md_manga_id
            if md_id is None:
                logger.warning(
                    f"No mangadex id found for {self.extension_name} id {chapter.manga_id}."
                )
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

        uploaded_chapter_ids = [
            chapter.md_chapter_id
            for chapter in self.current_uploaded_chapters
            if chapter.md_chapter_id is not None
        ]

        # if self.clean_db:
        #     uploaded_chapter_ids.extend(
        #         [
        #             chapter.md_chapter_id
        #             for chapter in self.chapters_on_db
        #             if chapter.chapter_expire
        #             >= get_current_datetime()
        #             and chapter.md_chapter_id. is not None
        #         ]
        #     )

        uploaded_chapter_ids = list(set(uploaded_chapter_ids))
        if uploaded_chapter_ids:
            logger.info(f"Uploaded chapters mangadex ids: {uploaded_chapter_ids}")
            uploaded_chapter_ids_split = [
                uploaded_chapter_ids[elem : elem + 100]
                for elem in range(0, len(uploaded_chapter_ids), 100)
            ]

            time.sleep(ratelimit_time * 3)
            for uploaded_ids in uploaded_chapter_ids_split:
                chapters_on_md.extend(
                    get_md_api(
                        "chapter",
                        **{
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
            PubloaderNotIndexedWebhook(self.extension_name, chapters_not_on_md).main()
        else:
            logger.info("No uploaded chapter mangadex ids.")

    def upload_chapters(self):
        """Go through each new chapter and upload it to mangadex."""
        # Sort each chapter by manga
        all_manga_chapters = self._sort_chapters_by_manga(self.all_chapters)
        self._delete_extra_chapters()

        for index, mangadex_manga_id in enumerate(self.updated_manga_chapters, start=1):
            manga_uploader = MangaUploaderProcess(
                extension_name=self.extension_name,
                clean_db=self.clean_db,
                updated_chapters=self.updated_manga_chapters.get(mangadex_manga_id, []),
                all_manga_chapters=all_manga_chapters.get(mangadex_manga_id, []),
                mangadex_manga_id=mangadex_manga_id,
                mangadex_group_id=self.mangadex_group_id,
                chapters_on_md=self.chapters_on_md.get(mangadex_manga_id, []),
                current_uploaded_chapters=self.current_uploaded_chapters,
                same_chapter_dict=self.same_chapter_dict,
                mangadex_manga_data=self.manga_data_local.get(mangadex_manga_id, {}),
                custom_language=self.custom_regexes.get("custom_language", {}),
                chapters_on_db=self.chapters_on_db,
                languages=self.extension_languages,
                chapters_for_upload=self.chapters_for_upload,
                chapters_for_skipping=self.chapters_for_skipping,
                chapters_for_editing=self.chapters_for_editing,
            )
            manga_uploader.start_manga_uploading_process(
                index == len(self.updated_manga_chapters)
            )
            time.sleep(0.5)

        if self.current_uploaded_chapters:
            self._check_all_chapters_uploaded()

        PubloaderWebhook(
            self.extension_name,
            title=f"{self.extension_name.title()} Updates",
            description=(
                f"To upload: {len(self.chapters_for_upload)}\n"
                f"To edit: {len(self.chapters_for_editing)}\n"
                f"Skipped: {len(self.chapters_for_skipping)}"
            ),
        ).send()
