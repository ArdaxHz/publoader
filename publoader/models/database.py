import logging
import traceback
from typing import List, Union

import gridfs
import pymongo
from pymongo import DeleteOne, UpdateOne

from publoader.models.dataclasses import Chapter
from publoader.utils.config import config
from publoader.utils.singleton import Singleton
from publoader.utils.utils import EXPIRE_TIME, get_current_datetime

logger = logging.getLogger("publoader")
logger_debug = logging.getLogger("debug")


class DatabaseConnector(metaclass=Singleton):
    def __init__(self):
        self.database_uri = config["Credentials"]["mongodb_uri"]
        self.database_name = config["Credentials"]["mongodb_db_name"]
        self.database = self.connect_db()
        self.database_connection = self.database[self.database_name]

    def connect_db(self):
        client = pymongo.MongoClient(self.database_uri)
        return client


database = DatabaseConnector()
database_connection = database.database_connection
image_filestream = gridfs.GridFS(database_connection, "images")


def convert_model_dict(chapter):
    if isinstance(chapter, Chapter):
        chapter = vars(chapter)
    return chapter


def update_database(chapter: Union[list, Union[Chapter, dict]], **kwargs):
    """Update the database with the new chapter."""
    if isinstance(chapter, Chapter):
        chapter = vars(chapter)

    chapters = [chapter]

    if isinstance(chapter, list):
        chapters = list(map(convert_model_dict, chapter))

    if not chapters:
        print(f"No chapters to update: {chapters}")
        return

    for chap in chapters:
        if "_id" in chap:
            chap.pop("_id")

    null_chapters = list(filter(lambda x: x.get("md_chapter_id") is None, chapters))
    logger.debug(
        f"Chapters to insert into database but md_chapter_id is null {null_chapters}"
    )
    chapters = list(filter(lambda x: x.get("md_chapter_id") is not None, chapters))
    if not chapters:
        logger.warning("No chapters to add to the database.")
        return

    try:
        result = database_connection["uploaded"].bulk_write(
            [
                UpdateOne(
                    {"md_chapter_id": {"$eq": chap["md_chapter_id"]}},
                    {"$set": chap},
                    upsert=True,
                )
                for chap in chapters
            ]
        )
    except pymongo.errors.BulkWriteError as e:
        traceback.print_exc()
        logger.exception(
            f"{update_database.__name__} raised an error when bulk writing to 'uploaded'."
        )
        return

    logger.info(f"Updated {result.modified_count} chapters on the database.")

    if result.upserted_count > 0:
        logger.info(
            f"Added {result.upserted_count} new chapters to database: {result.upserted_ids}"
        )

    try:
        database_connection["uploaded_ids"].bulk_write(
            [
                UpdateOne(
                    {"chapter_id": {"$eq": chap["chapter_id"]}},
                    {
                        "$setOnInsert": {
                            "chapter_id": chap["chapter_id"],
                            "extension_name": chap["extension_name"],
                            "md_chapter_id": chap["md_chapter_id"],
                        },
                    },
                    upsert=True,
                )
                for chap in chapters
            ]
        )
    except pymongo.errors.BulkWriteError as e:
        traceback.print_exc()
        logger.exception(
            f"{update_database.__name__} raised an error when bulk writing to 'uploaded_ids'."
        )
        return


def update_expired_chapter_database(
    extension_name: str,
    md_manga_id: str,
    md_chapter: Union[List[dict], dict] = None,
    chapter: Union[list, Union[Chapter, dict]] = None,
    **kwargs,
):
    """Update a chapter as expired on the database."""
    if md_chapter is None:
        md_chapter = []

    if chapter is None:
        chapter = []

    if not chapter and not md_chapter:
        logger.info(f"No chapters specified to update expired.")
        return

    if isinstance(chapter, Chapter):
        chapter = vars(chapter)

    chapters = [chapter]

    if isinstance(chapter, list):
        chapters = list(map(convert_model_dict, chapter))

    if isinstance(md_chapter, dict):
        md_chapter = [md_chapter]

    for chap in chapters:
        chap["chapter_expire"] = EXPIRE_TIME
        chap["extension_name"] = extension_name

    if isinstance(md_chapter, list):
        chapters.extend(
            [
                {
                    "chapter_lookup": get_current_datetime(),
                    "chapter_timestamp": EXPIRE_TIME,
                    "chapter_expire": EXPIRE_TIME,
                    "chapter_language": md_chap["attributes"]["translatedLanguage"],
                    "chapter_title": md_chap["attributes"]["title"],
                    "chapter_number": md_chap["attributes"]["chapter"],
                    "md_manga_id": md_manga_id,
                    "md_chapter_id": md_chap["id"],
                    "chapter_url": md_chap["attributes"]["externalUrl"],
                    "extension_name": extension_name,
                }
                for md_chap in md_chapter
            ]
        )

    try:
        result = database_connection["to_delete"].bulk_write(
            [
                UpdateOne(
                    {"md_chapter_id": {"$eq": chap["md_chapter_id"]}},
                    {"$set": chap},
                    upsert=True,
                )
                for chap in chapters
            ]
        )
    except pymongo.errors.BulkWriteError as e:
        traceback.print_exc()
        logger.exception(
            f"{update_expired_chapter_database.__name__} raised an error when bulk writing to 'to_delete'."
        )
        return

    logger.info(f"Updated {result.modified_count} chapters to delete on the database.")

    if result.upserted_count > 0:
        logger.info(
            f"Added {result.upserted_count} chapters to delete: {result.upserted_ids}"
        )
    try:
        deleted_result = database_connection["uploaded"].bulk_write(
            [
                DeleteOne(
                    {"md_chapter_id": {"$eq": chap["md_chapter_id"]}},
                )
                for chap in chapters
            ]
        )
    except pymongo.errors.BulkWriteError as e:
        traceback.print_exc()
        logger.exception(
            f"{update_expired_chapter_database.__name__} raised an error when bulk writing to 'uploaded'."
        )
        return

    logger.info(f"Deleted {deleted_result.deleted_count} from 'uploaded' collection.")
