import logging
import sqlite3
from copy import copy
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Optional, Union

from publoader.models.dataclasses import Chapter
from publoader.utils.config import config, components_path
from publoader.utils.utils import EXPIRE_TIME


logger = logging.getLogger("publoader")
logger_debug = logging.getLogger("debug")

column_names = [
    "chapter_id",
    "chapter_timestamp",
    "chapter_expire",
    "chapter_language",
    "chapter_title",
    "chapter_number",
    "chapter_volume",
    "chapter_url",
    "manga_id",
    "md_chapter_id",
    "md_manga_id",
    "manga_name",
    "manga_url",
    "extension_name",
]


def make_tables(database_connection: sqlite3.Connection):
    """Make the database table."""
    logger.info("Creating new tables for database.")
    database_connection.execute(
        """CREATE TABLE IF NOT EXISTS chapters
        (chapter_id         TEXT,
        chapter_timestamp   DATETIME NOT NULL,
        chapter_expire      DATETIME,
        chapter_language    TEXT NOT NULL,
        chapter_title       TEXT,
        chapter_number      TEXT,
        chapter_volume      TEXT,
        chapter_url         TEXT,
        manga_id            TEXT,
        md_chapter_id       TEXT NOT NULL PRIMARY KEY,
        md_manga_id         TEXT,
        manga_name          TEXT,
        manga_url           TEXT,
        extension_name      TEXT)"""
    )
    database_connection.execute(
        """CREATE TABLE IF NOT EXISTS deleted_chapters
        (chapter_id         TEXT,
        chapter_timestamp   DATETIME NOT NULL,
        chapter_expire      DATETIME,
        chapter_language    TEXT NOT NULL,
        chapter_title       TEXT,
        chapter_number      TEXT,
        chapter_volume      TEXT,
        chapter_url         TEXT,
        manga_id            TEXT,
        md_chapter_id       TEXT NOT NULL PRIMARY KEY,
        md_manga_id         TEXT,
        manga_name          TEXT,
        manga_url           TEXT,
        extension_name      TEXT)"""
    )
    database_connection.execute(
        """CREATE TABLE IF NOT EXISTS posted_ids
        (chapter_id         TEXT NOT NULL,
        extension_name      TEXT NOT NULL)"""
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


def adapt_datetime_iso(val: datetime):
    """Adapt datetime.datetime to timezone-naive ISO 8601 date."""
    return val.astimezone(tz=timezone.utc).isoformat()


def convert_datetime(val: bytes):
    """Convert ISO 8601 datetime to datetime.datetime object."""
    return datetime.fromisoformat(val.decode()).astimezone(tz=timezone.utc)


def open_database(db_path: Path) -> tuple[sqlite3.Connection, bool]:
    sqlite3.register_converter("datetime", convert_datetime)
    database_connection = sqlite3.connect(db_path, detect_types=sqlite3.PARSE_DECLTYPES)
    database_connection.row_factory = sqlite3.Row
    logger.info("Opened database.")
    logger_debug.info("Opened database.")

    fill_backlog = check_table_exists(database_connection)
    return database_connection, fill_backlog


def update_database(
    database_connection: sqlite3.Connection,
    chapter: Union[Chapter, dict],
    external_chapter_id: Optional[int] = None,
):
    """Update the database with the new chapter."""
    if isinstance(chapter, Chapter):
        chapter = vars(chapter)

    chapter = copy(chapter)
    chapter = {k: v for k, v in chapter.items() if k in column_names}

    if external_chapter_id is not None:
        if chapter.get("chapter_id") is None:
            chapter["chapter_id"] = external_chapter_id

    md_chapter_id = chapter.get("md_chapter_id")
    external_chapter_id = chapter.get("chapter_id")

    if md_chapter_id is None:
        logger.error(f"md_chapter_id to update the database with is null: {chapter}")
        return

    chapter_id_exists = database_connection.execute(
        "SELECT * FROM chapters WHERE md_chapter_id=(?)",
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
            """INSERT INTO chapters (
                    chapter_id,
                    chapter_timestamp,
                    chapter_expire,
                    chapter_language,
                    chapter_title,
                    chapter_number,
                    manga_id,
                    md_chapter_id,
                    md_manga_id,
                    chapter_url,
                    manga_name,
                    manga_url,
                    extension_name
                ) VALUES (
                    :chapter_id,
                    :chapter_timestamp,
                    :chapter_expire,
                    :chapter_language,
                    :chapter_title,
                    :chapter_number,
                    :manga_id,
                    :md_chapter_id,
                    :md_manga_id,
                    :chapter_url,
                    :manga_name,
                    :manga_url,
                    :extension_name)""",
            chapter,
        )

    database_connection.execute(
        "INSERT OR IGNORE INTO posted_ids (chapter_id, extension_name) VALUES (?, ?)",
        (external_chapter_id, chapter.get("extension_name")),
    )

    if chapter_id_exists_dict is None:
        logger.debug(f"Added to database: {chapter.get('md_chapter_id')} - {chapter}")
    database_connection.commit()


def update_expired_chapter_database(
    database_connection: sqlite3.Connection,
    extension_name: str,
    md_chapter_obj: dict,
    md_manga_id: str,
    chapters_on_db: List[Chapter] = None,
) -> Chapter:
    """Update a chapter as expired on the database."""
    md_chapter_id = md_chapter_obj["id"]
    external_chapter_url = md_chapter_obj["attributes"]["externalUrl"]

    found = False
    chapter_to_delete = None

    if chapters_on_db is not None:
        for db_chapter in chapters_on_db:
            if (
                md_chapter_id == db_chapter.md_chapter_id
                or str(db_chapter.chapter_id) in external_chapter_url
            ):
                found = True
                chapter_to_delete = copy(vars(db_chapter))
                break

    if found and chapter_to_delete is not None:
        logger.info(
            f"Updating chapter on database with expired time. {chapter_to_delete}"
        )
        chapter_to_delete["chapter_expire"] = EXPIRE_TIME
        chapter_to_delete["extension_name"] = extension_name
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
            chapter_url=external_chapter_url,
            extension_name=extension_name,
        )

    logger.info(f"Updating database entry with expired entry.")
    update_database(database_connection, expired_chapter_object)
    return expired_chapter_object
