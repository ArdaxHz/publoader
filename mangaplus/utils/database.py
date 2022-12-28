import logging
import sqlite3
from copy import copy
from pathlib import Path
from typing import List, Optional, Union

from . import components_path
from .config import config
from .dataclass_models import Chapter
from .utils import mplus_url_regex, EXPIRE_TIME


logger = logging.getLogger("mangaplus")
logger_debug = logging.getLogger("debug")


def make_tables(database_connection: sqlite3.Connection):
    """Make the database table."""
    logger.info("Creating new tables for database.")
    database_connection.execute(
        """CREATE TABLE IF NOT EXISTS chapters
        (chapter_id         INTEGER,
        chapter_timestamp   INTEGER NOT NULL,
        chapter_expire      INTEGER NOT NULL,
        chapter_language    TEXT NOT NULL,
        chapter_title       TEXT,
        chapter_number      TEXT,
        chapter_volume      TEXT,
        manga_id            INTEGER,
        md_chapter_id       TEXT NOT NULL PRIMARY KEY,
        md_manga_id         TEXT)"""
    )
    database_connection.execute(
        """CREATE TABLE IF NOT EXISTS deleted_chapters
        (chapter_id         INTEGER,
        chapter_timestamp   INTEGER NOT NULL,
        chapter_expire      INTEGER NOT NULL,
        chapter_language    TEXT NOT NULL,
        chapter_title       TEXT,
        chapter_number      TEXT,
        chapter_volume      TEXT,
        manga_id            INTEGER,
        md_chapter_id       TEXT NOT NULL PRIMARY KEY,
        md_manga_id         TEXT)"""
    )
    database_connection.execute(
        """CREATE TABLE IF NOT EXISTS posted_mplus_ids
        (chapter_id         INTEGER NOT NULL)"""
    )
    database_connection.commit()


def check_table_exists(database_connection: sqlite3.Connection) -> bool:
    """Check if the table exists."""
    table_exist = database_connection.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='chapters'"
    )

    fill_backlog = False
    # Table doesn't exist, fill backlog without posting to mangadex
    if not table_exist.fetchall():
        logger.error("Database tables don't exist, making new ones.")
        print("Tables don't exist, making new ones.")
        make_tables(database_connection)
        fill_backlog = True
    return fill_backlog


database_name = config["Paths"]["database_path"]
database_path = components_path.joinpath(database_name)


def open_database(db_path: Path) -> tuple[sqlite3.Connection, bool]:
    database_connection = sqlite3.connect(db_path)
    database_connection.row_factory = sqlite3.Row
    logger.info("Opened database.")
    logger_debug.info("Opened database.")

    fill_backlog = check_table_exists(database_connection)
    return database_connection, fill_backlog


def update_database(
    database_connection: sqlite3.Connection,
    chapter: Union[Chapter, dict],
    mplus_chapter_id: Optional[int] = None,
):
    """Update the database with the new chapter."""
    if isinstance(chapter, Chapter):
        chapter = vars(chapter)

    chapter = copy(chapter)

    if mplus_chapter_id is not None:
        if chapter.get("chapter_id") is None:
            chapter["chapter_id"] = mplus_chapter_id

    if "manga" in chapter:
        del chapter["manga"]

    md_chapter_id = chapter.get("md_chapter_id")
    mplus_chapter_id = chapter.get("chapter_id")

    if md_chapter_id is None:
        logger.error(f"md_chapter_id to update the database with is null: {chapter}")
        return

    chapter_id_exists = database_connection.execute(
        "SELECT * FROM chapters WHERE EXISTS(SELECT 1 FROM chapters WHERE md_chapter_id=(?))",
        (md_chapter_id,),
    )
    chapter_id_exists_dict = chapter_id_exists.fetchone()
    if chapter_id_exists_dict is not None:
        logger.info(f"Updating existing record in the database: {chapter}.")
        update_dict_keys = ""
        for index, key in enumerate(chapter.keys(), start=1):
            update_dict_keys += f"{key}=:{key}"
            if index != len(chapter.keys()):
                update_dict_keys += ","

        logger.debug(f"{update_dict_keys=}")
        database_connection.execute(
            f"UPDATE chapters SET {update_dict_keys} WHERE md_chapter_id=:md_chapter_id",
            chapter,
        )
    else:
        logger.info(f"Adding new chapter to database: {chapter}.")
        database_connection.execute(
            """INSERT INTO chapters (chapter_id, chapter_timestamp, chapter_expire, chapter_language, chapter_title, chapter_number, manga_id, md_chapter_id, md_manga_id) VALUES
                                                            (:chapter_id, :chapter_timestamp, :chapter_expire, :chapter_language, :chapter_title, :chapter_number, :manga_id, :md_chapter_id, :md_manga_id)""",
            chapter,
        )

    database_connection.execute(
        "INSERT OR IGNORE INTO posted_mplus_ids (chapter_id) VALUES (?)",
        (mplus_chapter_id,),
    )

    if chapter_id_exists_dict is None:
        logger.debug(f"Added to database: {chapter.get('md_chapter_id')} - {chapter}")
    database_connection.commit()


def update_expired_chapter_database(
    database_connection: sqlite3.Connection,
    md_chapter_obj: dict,
    md_manga_id: str,
    chapters_on_db: List[dict] = [],
) -> dict:
    """Update a chapter as expired on the database.."""
    md_chapter_id = md_chapter_obj["id"]
    mplus_chapter_url = md_chapter_obj["attributes"]["externalUrl"]
    mplus_chapter_id = None
    mplus_chapter_match = mplus_url_regex.match(mplus_chapter_url)
    if mplus_chapter_url is not None:
        mplus_chapter_id = mplus_chapter_match.group(1)

    found = False
    chapter_to_delete = None

    for db_chapter in chapters_on_db:
        if (
            md_chapter_id == db_chapter["md_chapter_id"]
            or str(db_chapter["chapter_id"]) in mplus_chapter_url
        ):
            found = True
            chapter_to_delete = copy(dict(db_chapter))
            break

    if found and chapter_to_delete is not None:
        logger.info(
            f"Updating chapter on database with expired time. {chapter_to_delete}"
        )
        chapter_to_delete["chapter_expire"] = EXPIRE_TIME
        expired_chapter_object = Chapter(**chapter_to_delete)
    else:
        logger.info(f"Chapter not found on the database, making a new chapter object.")
        expired_chapter_object = Chapter(
            chapter_timestamp=EXPIRE_TIME,
            chapter_expire=EXPIRE_TIME,
            chapter_language=md_chapter_obj["attributes"]["translatedLanguage"],
            chapter_title=md_chapter_obj["attributes"]["title"],
            chapter_number=md_chapter_obj["attributes"]["chapter"],
            md_manga_id=md_manga_id,
            md_chapter_id=md_chapter_id,
            chapter_id=mplus_chapter_id,
        )

    expired_chapter_object = vars(expired_chapter_object)

    # if mplus_chapter_id is not None:
    #     where_clause = "OR chapter_id=:chapter_id"
    # else:
    #     where_clause = ""

    logger.info(f"Deleting all chapters on the database that match {md_chapter_id=}")
    database_connection.execute(
        f"DELETE FROM chapters WHERE md_chapter_id=:md_chapter_id",
        expired_chapter_object,
    )

    logger.info(f"Inserting new object into the database.")
    database_connection.execute(
        """INSERT INTO chapters (chapter_id, chapter_timestamp, chapter_expire, chapter_language, chapter_title, chapter_number, manga_id, md_chapter_id, md_manga_id) VALUES
                                                            (:chapter_id, :chapter_timestamp, :chapter_expire, :chapter_language, :chapter_title, :chapter_number, :manga_id, :md_chapter_id, :md_manga_id)""",
        expired_chapter_object,
    )

    return expired_chapter_object
