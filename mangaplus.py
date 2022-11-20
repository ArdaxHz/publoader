import argparse
import logging
import sqlite3
from typing import Optional

import requests
from requests.packages.urllib3.exceptions import InsecureRequestWarning

requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

from mangaplus.utils import utils
from mangaplus import open_database, components_path
from mangaplus.utils.database import database_name, database_path

from mangaplus.webhook import webhook
from mangaplus.chapter_deleter import ChapterDeleterProcess
from mangaplus.auth_md import AuthMD
from mangaplus.mplus_api import MPlusAPI
from mangaplus.dupes_checker import DeleteDuplicatesMD
from mangaplus.bot_process import BotProcess

__version__ = "2.0.2"

logger = logging.getLogger("mangaplus")

config = utils.config


def main(db_connection: Optional[sqlite3.Connection] = None, clean_db=False):
    """Main function for getting the updates."""
    manga_id_map = utils.open_manga_id_map(
        components_path.joinpath(config["Paths"]["manga_id_map_path"])
    )
    title_regexes = utils.open_title_regex(
        components_path.joinpath(config["Paths"]["title_regex_path"])
    )
    manga_data_local = utils.open_manga_data(
        components_path.joinpath(config["Paths"]["manga_data_path"])
    )

    if db_connection is not None:
        database_connection = db_connection
    else:
        database_connection, _ = open_database(database_path)

    # Get already posted chapters
    posted_chapters_data = database_connection.execute(
        "SELECT * FROM chapters"
    ).fetchall()
    posted_chapters_ids_data = database_connection.execute(
        "SELECT * FROM posted_mplus_ids"
    ).fetchall()
    posted_chapters_ids = (
        [job["chapter_id"] for job in posted_chapters_ids_data] if not clean_db else []
    )
    manga_map_mplus_ids = [
        mplus_id for md_id in manga_id_map for mplus_id in manga_id_map[md_id]
    ]
    logger.info(
        "Retrieved posted chapters from database and got mangaplus ids from manga id map file."
    )

    session = requests.Session()
    session.headers.update({"User-Agent": f"MP-MD_bot/{__version__}"})
    md_auth_object = AuthMD(session, config)

    # Start deleting expired chapters
    deleter_process_object = ChapterDeleterProcess(
        session=session,
        md_auth_object=md_auth_object,
        database_connection=database_connection,
    )

    # Get new manga and chapter updates
    mplus_api = MPlusAPI(
        manga_map_mplus_ids, posted_chapters_ids, manga_id_map, title_regexes
    )
    # updated_manga = mplus_api.untracked_manga
    updates = mplus_api.updated_chapters
    all_mplus_chapters = mplus_api.all_mplus_chapters

    if not updates:
        logger.info("No new updates found.")
        print("No new updates found.")
    else:
        logger.info(f"Found {len(updates)} update(s).")
        print(f"Found {len(updates)} update(s).")
        while True:
            try:
                BotProcess(
                    config=config,
                    session=session,
                    updates=updates,
                    all_mplus_chapters=all_mplus_chapters,
                    deleter_process_object=deleter_process_object,
                    md_auth_object=md_auth_object,
                    manga_id_map=manga_id_map,
                    database_connection=database_connection,
                    title_regexes=title_regexes,
                    clean_db=clean_db,
                    chapters_on_db=posted_chapters_data,
                    manga_data_local=manga_data_local,
                ).upload_chapters()
            except (requests.RequestException, sqlite3.OperationalError) as e:
                logger.error(e)
                continue
            else:
                break
        print("Uploaded all update(s).")

    if clean_db:
        dupes_deleter = DeleteDuplicatesMD(
            session,
            manga_id_map,
            deleter_process_object,
            database_connection,
            manga_data_local,
        )
        dupes_deleter.delete_dupes()

    first_process = deleter_process_object.delete()
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
    database_connection, fill_backlog = open_database(database_path)

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
        main(database_connection, clean_db=True)
    else:
        main(database_connection)

    if webhook.embeds:
        webhook.execute(remove_embeds=True)
