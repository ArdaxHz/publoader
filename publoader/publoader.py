import argparse
import logging
import multiprocessing
import traceback

import requests
from requests.packages.urllib3.exceptions import InsecureRequestWarning

requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

from publoader.workers import deleter, editor, uploader
from publoader.dupes_checker import DeleteDuplicatesMD
from publoader.extension_uploader import ExtensionUploader
from publoader.load_extensions import (
    load_extensions,
    run_extensions,
    read_extension,
    run_extension,
)
from publoader.utils.config import config, components_path
from publoader.models.database import (
    database_connection,
)
from publoader.models.dataclasses import Chapter
from publoader.utils.utils import open_manga_data

logger = logging.getLogger("publoader")


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

    try:
        if not updated_chapters:
            print(f"No new updates found for {normalised_extension_name}")
            return False

        for update in updated_chapters:
            print(
                f"--Found manga {update.manga_name} - {update.manga_id}, "
                f"chapter_id: {update.chapter_id}, "
                f"chapter: {update.chapter_number!r}, "
                f"language: {update.chapter_language!r}, "
                f"title: {update.chapter_title!r}."
            )

        print(
            f"Found {len(updated_chapters)} new chapters for {normalised_extension_name}"
        )

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

        print("Uploaded all update(s).")

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


def open_extensions(names=None, clean_db: bool = False, general_run: bool = False):
    """Run multiple extensions."""
    extensions_data = load_extensions(names, clean_db, general_run)
    if not extensions_data:
        return

    extensions = run_extensions(extensions_data, clean_db)
    if not extensions:
        return

    manga_data_local = open_manga_data(
        components_path.joinpath(config["Paths"]["manga_data_path"])
    )
    for site in extensions:
        run_updates(
            extensions[site],
            manga_data_local=manga_data_local,
        )


def open_extension(name: str, clean_db: bool = False):
    """Run a single extension."""
    loaded_extension = read_extension(name, clean_db=clean_db)
    if loaded_extension is None:
        return

    extension_data = run_extension(loaded_extension, clean_db_override=clean_db)
    if not extension_data:
        return

    manga_data_local = open_manga_data(
        components_path.joinpath(config["Paths"]["manga_data_path"])
    )

    run_updates(
        extension_data,
        manga_data_local=manga_data_local,
    )


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
        "--general",
        "-g",
        default=False,
        const=True,
        nargs="?",
        help="General run of the bot.",
    )
    parser.add_argument(
        "--extension",
        "-e",
        action="append",
        required=False,
        help="Run a specific extension.",
    )

    vargs = vars(parser.parse_args())

    process = multiprocessing.Process(target=uploader.main)
    process.start()

    process = multiprocessing.Process(target=editor.main)
    process.start()

    process = multiprocessing.Process(target=deleter.main)
    process.start()

    if vargs["extension"] is None:
        extension_to_run = None
    else:
        extension_to_run = [str(extension).strip() for extension in vargs["extension"]]

    open_extensions(
        names=extension_to_run, clean_db=vargs["clean"], general_run=vargs["general"]
    )
