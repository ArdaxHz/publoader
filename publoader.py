import argparse
import logging
import traceback

import requests
from requests.packages.urllib3.exceptions import InsecureRequestWarning

requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

from publoader import __version__
from publoader.bot_process import BotProcess
from publoader.chapter_deleter import ChapterDeleterProcess
from publoader.dupes_checker import DeleteDuplicatesMD
from publoader.load_extensions import load_extensions
from publoader.webhook import webhook
from publoader.models.http import HTTPClient
from publoader.utils.config import config, components_path
from publoader.models.database import database_name, database_path, open_database
from publoader.models.dataclasses import Chapter
from publoader.utils.utils import open_manga_data

logger = logging.getLogger("publoader")


def main(clean_db=False):
    """Main function for getting the updates."""
    database_connection, fill_backlog = open_database(database_path)
    manga_data_local = open_manga_data(
        components_path.joinpath(config["Paths"]["manga_data_path"])
    )

    if fill_backlog:
        clean_db = True

    http_client = HTTPClient(config, __version__)

    # Start deleting expired chapters
    deleter_process_object = ChapterDeleterProcess(
        http_client=http_client,
        database_connection=database_connection,
    )

    extensions = load_extensions(database_connection, clean_db)
    for site in extensions:
        logger.info(f"Getting updates for {site}")

        extension_data = extensions[site]
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
        posted_chapters_ids = extension_data["posted_chapters_ids"]

        try:
            if not updated_chapters:
                print(f"No new updates found for {normalised_extension_name}")
                continue

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
            posted_chapters_data = database_connection.execute(
                "SELECT * FROM chapters WHERE extension_name=?", (extension_name,)
            ).fetchall()
            posted_chapters_data = [Chapter(**data) for data in posted_chapters_data]
            logger.info("Retrieved posted chapters from database.")

            BotProcess(
                config=config,
                http_client=http_client,
                extension=extension_data,
                extension_name=extension_name,
                updates=updated_chapters,
                all_chapters=all_chapters,
                untracked_manga=untracked_manga,
                tracked_mangadex_ids=tracked_mangadex_ids,
                mangadex_group_id=mangadex_group_id,
                custom_regexes=custom_regexes,
                extension_languages=extension_languages,
                deleter_process_object=deleter_process_object,
                database_connection=database_connection,
                clean_db=clean_db,
                chapters_on_db=posted_chapters_data,
                manga_data_local=manga_data_local,
            ).upload_chapters()

            print("Uploaded all update(s).")

            if clean_db:
                dupes_deleter = DeleteDuplicatesMD(
                    http_client=http_client,
                    extension_name=extension_name,
                    tracked_mangadex_ids=tracked_mangadex_ids,
                    deleter_process_object=deleter_process_object,
                    database_connection=database_connection,
                    manga_data_local=manga_data_local,
                    extension_languages=extension_languages,
                    mangadex_group_id=mangadex_group_id,
                )
                dupes_deleter.delete_dupes()
        except Exception:
            traceback.print_exc()
            logger.exception(
                f"{normalised_extension_name} raised an error."
            )
            continue

    deleter_process_object.delete()
    print("Finished deleting expired chapters.")

    # Save and close database
    database_connection.commit()
    backup_database_connection, _ = open_database(
        components_path.joinpath(database_name).with_suffix(".bak")
    )
    database_connection.backup(backup_database_connection)
    backup_database_connection.close()
    database_connection.close()
    logger.info("Saved and closed database.")


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

    vargs = vars(parser.parse_args())

    if vargs["clean"]:
        main(clean_db=True)
    else:
        main()

    if webhook.embeds:
        webhook.execute(remove_embeds=True)
