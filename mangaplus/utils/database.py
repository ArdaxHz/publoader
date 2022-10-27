import logging
import sqlite3
from pathlib import Path
from typing import Optional, Union

from .dataclass_models import Chapter
from .utils import config, components_path


logger = logging.getLogger("mangaplus")
logger_debug = logging.getLogger("mangaplus")


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
    succesful_upload_id: Optional[str] = None,
):
    """Update the database with the new chapter."""
    if isinstance(chapter, Chapter):
        chapter = vars(chapter)

    if succesful_upload_id is None:
        succesful_upload_id = chapter.get("md_chapter_id")

    if succesful_upload_id is None:
        logger.error(f"md_chapter_id to update the database with is null: {chapter}")
        return

    mplus_chapter_id = chapter.get("chapter_id")

    chapter_id_exists = database_connection.execute(
        "SELECT * FROM chapters WHERE EXISTS(SELECT 1 FROM chapters WHERE md_chapter_id=(?))",
        (succesful_upload_id,),
    )
    chapter_id_exists_dict = chapter_id_exists.fetchone()
    if chapter_id_exists_dict is not None:
        if (
            dict(chapter_id_exists_dict).get("chapter_id", None) is None
            and chapter.get("chapter_id") is not None
        ):
            print("Updating database with new mangadex and mangaplus chapter ids.")
            logger.info(f"Updating existing record in the database: {chapter}.")
            database_connection.execute(
                "UPDATE chapters SET chapter_id=:mplus_id, md_manga_id=:md_manga_id, chapter_expire=:chapter_expire WHERE md_chapter_id=:md_id",
                {
                    "md_id": succesful_upload_id,
                    "mplus_id": mplus_chapter_id,
                    "md_manga_id": chapter.get("md_manga_id"),
                    "chapter_expire": chapter.get("chapter_expire"),
                },
            )
    else:
        logger.info(f"Adding new chapter to database: {chapter}.")
        database_connection.execute(
            """INSERT INTO chapters (chapter_id, chapter_timestamp, chapter_expire, chapter_language, chapter_title, chapter_number, manga_id, md_chapter_id, md_manga_id) VALUES
                                                            (:chapter_id, :chapter_timestamp, :chapter_expire, :chapter_language, :chapter_title, :chapter_number, :manga_id, :md_chapter_id, :md_manga_id)""",
            {
                "chapter_id": mplus_chapter_id,
                "chapter_timestamp": chapter.get("chapter_timestamp"),
                "chapter_expire": chapter.get("chapter_expire"),
                "chapter_language": chapter.get("chapter_language"),
                "chapter_title": chapter.get("chapter_title"),
                "chapter_number": chapter.get("chapter_number"),
                "manga_id": chapter.get("manga_id"),
                "md_chapter_id": succesful_upload_id,
                "md_manga_id": chapter.get("md_manga_id"),
            },
        )
    database_connection.execute(
        "INSERT OR IGNORE INTO posted_mplus_ids (chapter_id) VALUES (?)",
        (mplus_chapter_id,),
    )

    if chapter_id_exists_dict is None:
        logger.debug(f"Added to database: {succesful_upload_id} - {chapter}")
    database_connection.commit()
