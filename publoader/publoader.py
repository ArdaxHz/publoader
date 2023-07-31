import argparse
import atexit
import logging
import signal
import sys
import traceback
from typing import List

import requests
from requests.packages.urllib3.exceptions import InsecureRequestWarning

from publoader.webhook import PubloaderWebhook

requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

from publoader.workers import worker
from publoader.dupes_checker import DeleteDuplicatesMD
from publoader.extension_uploader import ExtensionUploader
from publoader.load_extensions import (
    load_extensions,
    run_extensions,
)
from publoader.utils.config import config, resources_path
from publoader.models.database import (
    database_connection,
)
from publoader.models.dataclasses import Chapter
from publoader.utils.utils import get_current_datetime, open_manga_data

logger = logging.getLogger("publoader")


def send_untracked_manga_webhook(extension_name, untracked_manga):
    logger.info(
        f"Found {len(untracked_manga)} untracked manga for {extension_name}: {untracked_manga}."
    )
    for untracked in untracked_manga:
        print(f"Found untracked manga {untracked.manga_id}: {untracked.manga_name}")

    if untracked_manga:
        split_series = [
            untracked_manga[elem : elem + 30]
            for elem in range(0, len(untracked_manga), 30)
        ]

        for count, series_list in enumerate(split_series, start=1):
            PubloaderWebhook(
                extension_name=extension_name,
                title=f"{len(untracked_manga)} Untracked Manga ({count})",
                description="\n".join(
                    [
                        f"**{manga.manga_name}**: [{manga.manga_id}]({manga.manga_url})"
                        for manga in series_list
                    ]
                ),
            ).send()


def run_updates(
    extension_data: dict,
    manga_data_local: dict,
):
    logger.info(f"Getting updates for {extension_data['name']}")

    extension = extension_data["extension"]
    extension_name = extension_data["name"]
    normalised_extension_name = extension_data["normalised_extension_name"]
    updated_chapters = extension_data["updated_chapters"]
    all_chapters = extension_data["all_chapters"]
    untracked_manga = extension_data["untracked_manga"]
    tracked_mangadex_ids = extension_data["tracked_mangadex_ids"]
    mangadex_group_id = extension_data["mangadex_group_id"]
    custom_regexes = extension_data["custom_regexes"]
    extension_languages = extension_data["extension_languages"]
    clean_db = extension_data["clean_db"]

    PubloaderWebhook(
        extension_name,
        title=f"Posting updates for extension {extension_name}",
        add_timestamp=False,
    ).main()

    try:
        send_untracked_manga_webhook(extension_name, untracked_manga)

        if not updated_chapters:
            print(f"No new updates found for {normalised_extension_name}")
            PubloaderWebhook(
                extension_name,
                title=f"No new updates found for {extension_name}",
            ).send()
            return False

        for update in updated_chapters:
            print(
                f"--Found manga {update.manga_name} - {update.manga_id}, "
                f"chapter_id: {update.chapter_id}, "
                f"chapter: {update.chapter_number!r}, "
                f"language: {update.chapter_language!r}, "
                f"title: {update.chapter_title!r}."
            )

        print(f"Found {len(updated_chapters)} chapters for {normalised_extension_name}")
        PubloaderWebhook(
            extension_name,
            title=f"Found {len(updated_chapters)} chapters for {normalised_extension_name}",
        ).send()

        # Get already posted chapters for the extension
        posted_chapters_data = list(
            database_connection["uploaded"].find(
                {"extension_name": {"$eq": extension_name}}
            )
        )

        posted_chapters_data = [Chapter(**data) for data in posted_chapters_data]
        logger.info("Retrieved posted chapters from database.")

        ExtensionUploader(
            config=config,
            extension=extension_data,
            extension_name=extension_name,
            updates=updated_chapters,
            all_chapters=all_chapters,
            untracked_manga=untracked_manga,
            tracked_mangadex_ids=tracked_mangadex_ids,
            mangadex_group_id=mangadex_group_id,
            custom_regexes=custom_regexes,
            extension_languages=extension_languages,
            clean_db=clean_db,
            chapters_on_db=posted_chapters_data,
            manga_data_local=manga_data_local,
        ).upload_chapters()

        print(
            f"Uploaded all chapters for {normalised_extension_name} at {get_current_datetime()}."
        )

        if clean_db:
            dupes_deleter = DeleteDuplicatesMD(
                extension_name=extension_name,
                tracked_mangadex_ids=tracked_mangadex_ids,
                manga_data_local=manga_data_local,
                extension_languages=extension_languages,
                mangadex_group_id=mangadex_group_id,
            )
            dupes_deleter.delete_dupes()
        return True
    except Exception:
        traceback.print_exc()
        logger.exception(f"{normalised_extension_name} raised an error.")
        return False


def open_extensions(
    names: List[str] = None, clean_db: bool = False, general_run: bool = False
):
    """Run multiple extensions."""
    extensions_data = load_extensions(names, clean_db, general_run)
    if not extensions_data:
        return

    if clean_db:
        PubloaderWebhook(
            extension_name=None, title="Bot Clean Run Cycle", colour="256ef5"
        ).send()

    extensions = run_extensions(extensions_data, clean_db)
    if not extensions:
        return

    manga_data_local = open_manga_data(
        resources_path.joinpath(config["Paths"]["manga_data_path"])
    )
    for site in extensions:
        run_updates(
            extensions[site],
            manga_data_local=manga_data_local,
        )


def handle_exit(*args):
    try:
        print(f"{'-'*10}Program Exit{'-'*10}")
        logger.info(f"{'-'*10}Program Exit{'-'*10}")
        sys.exit(0)
    except BaseException as exception:
        print(f"{'-'*10}Error Program Exit{'-'*10}")
        logger.exception(f"{'-'*10}Error Program Exit{'-'*10}")
        sys.exit(1)


atexit.register(handle_exit)
signal.signal(signal.SIGTERM, handle_exit)
signal.signal(signal.SIGINT, handle_exit)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--clean",
        "-c",
        default=False,
        const=True,
        nargs="?",
        help="Clean the database.",
    )
    parser.add_argument(
        "--force",
        "-f",
        default=False,
        const=True,
        nargs="?",
        help="Force run the bot, if extensions is unspecified, run all.",
    )
    parser.add_argument(
        "--extension",
        "-e",
        action="append",
        required=False,
        help="Run a specific extension.",
    )

    vargs = vars(parser.parse_args())

    try:
        worker.main(restart_threads=False)

        if vargs["extension"] is None:
            extension_to_run = None
        else:
            extension_to_run = [
                str(extension).strip() for extension in vargs["extension"]
            ]

        open_extensions(
            names=extension_to_run, clean_db=vargs["clean"], general_run=vargs["force"]
        )
    except KeyboardInterrupt:
        worker.kill()
