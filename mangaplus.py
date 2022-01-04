import configparser
import json
import logging
import math
import multiprocessing
import re
import sqlite3
import time
from dataclasses import dataclass, field
from datetime import datetime, date
from pathlib import Path
from typing import Any, Dict, Optional, List, Union
from uuid import UUID

import requests

import proto.response_pb2 as response_pb

mplus_base_api_url = "https://jumpg-webapi.tokyo-cdn.com"
mplus_chapter_url = 'https://mangaplus.shueisha.co.jp/viewer/{}'
mplus_language_map = {
    '0': 'en',
    '1': 'es-la',
    '2': 'fr',
    '3': 'id',
    '4': 'pt-br',
    '5': 'ru',
    '6': 'th'}
mplus_group = '4f1de6a2-f0c5-4ac5-bce5-02c7dbb67deb'

http_error_codes = {
    "400": "Bad request.",
    "401": "Unauthorised.",
    "403": "Forbidden.",
    "404": "Not found.",
    "429": "Too many requests."}

root_path = Path('.')
config_file_path = root_path.joinpath('config').with_suffix('.ini')

log_folder_path = root_path.joinpath('logs')
log_folder_path.mkdir(parents=True, exist_ok=True)

logs_path = log_folder_path.joinpath(
    f'mplus_md_uploader_{str(date.today())}.log')
logging.basicConfig(
    filename=logs_path,
    level=logging.DEBUG,
    format='%(asctime)s,%(msecs)d %(levelname)-8s [%(filename)s:%(lineno)d] %(message)s',
    datefmt='%Y-%m-%d:%H:%M:%S')


def load_config_info(config: configparser.ConfigParser):
    if config["User Set"].get("database_path", '') == '':
        logging.warning('Database path empty, using default.')
        config["User Set"]["database_path"] = 'chapters.db'

    if config["User Set"].get("mangadex_api_url", '') == '':
        logging.warning('Mangadex api path empty, using default.')
        config["User Set"]["mangadex_api_url"] = 'https://api.mangadex.org'

    if config["User Set"].get("manga_id_map_path", '') == '':
        logging.info('Manga id map path empty, using default.')
        config["User Set"]["manga_id_map_path"] = 'manga.json'


def open_config_file() -> configparser.ConfigParser:
    # Open config file and read values
    if config_file_path.exists():
        config = configparser.ConfigParser()
        config.read(config_file_path)
        logging.info('Loaded config file.')
    else:
        logging.critical('Config file not found, exiting.')
        raise FileNotFoundError('Config file not found.')

    load_config_info(config)
    return config


config = open_config_file()

database_path = Path(config["User Set"]["database_path"])
database_connection = database_connection = sqlite3.connect(database_path)
database_connection.row_factory = sqlite3.Row
logging.info('Opened database.')

mangadex_api_url = config["User Set"]["mangadex_api_url"]
md_upload_api_url = f'{mangadex_api_url}/upload'
md_auth_api_url = f'{mangadex_api_url}/auth'

try:
    mangadex_ratelimit_time = int(
        config["User Set"].get(
            "mangadex_ratelimit_time", ''))
except (ValueError, KeyError):
    mangadex_ratelimit_time = 2


@dataclass(order=True)
class Manga:
    manga_id: int
    manga_name: str
    manga_language: str


@dataclass()
class Chapter:
    chapter_timestamp: int
    chapter_expire: int
    chapter_title: str
    chapter_number: str
    chapter_language: str
    chapter_id: int = field(default=None)
    manga_id: int = field(default=None)
    md_chapter_id: str = field(default=None)

    def __post_init__(self):
        try:
            self.chapter_language = int(self.chapter_language)
        except ValueError:
            pass
        else:
            self.chapter_language = mplus_language_map.get(
                str(self.chapter_language), "NULL")


def make_tables(database_connection: sqlite3.Connection):
    """Make the database table."""
    logging.error("Creating new tables for database.")
    database_connection.execute(
        """CREATE TABLE IF NOT EXISTS chapters
        (chapter_id         INTEGER,
        chapter_timestamp   INTEGER NOT NULL,
        chapter_expire      INTEGER NOT NULL,
        chapter_language    TEXT NOT NULL,
        chapter_title       TEXT,
        chapter_number      TEXT,
        manga_id            INTEGER,
        md_chapter_id       TEXT NOT NULL PRIMARY KEY)""")
    database_connection.execute(
        """CREATE TABLE IF NOT EXISTS deleted_chapters
        (chapter_id         INTEGER,
        chapter_timestamp   INTEGER NOT NULL,
        chapter_expire      INTEGER NOT NULL,
        chapter_language    TEXT NOT NULL,
        chapter_title       TEXT,
        chapter_number      TEXT,
        manga_id            INTEGER,
        md_chapter_id       TEXT NOT NULL PRIMARY KEY)""")
    database_connection.execute(
        """CREATE TABLE IF NOT EXISTS posted_mplus_ids
        (chapter_id         INTEGER NOT NULL)""")
    database_connection.commit()


def check_table_exists(database_connection: sqlite3.Connection) -> bool:
    """Check if the table exists."""
    table_exist = database_connection.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='chapters'")

    fill_backlog = False
    # Table doesn't exist, fill backlog without posting to mangadex
    if not table_exist.fetchall():
        logging.error("Database tables don't exist, making new ones.")
        print("Tables don't exist, making new ones.")
        make_tables(database_connection)
        fill_backlog = True
    return fill_backlog


fill_backlog = check_table_exists(database_connection)


def convert_json(response_to_convert: requests.Response) -> Optional[dict]:
    """Convert the api response into a parsable json."""
    critical_decode_error_message = "Couldn't convert mangadex api response into a json."
    try:
        converted_response = response_to_convert.json()
    except json.JSONDecodeError:
        logging.critical(critical_decode_error_message)
        print(critical_decode_error_message)
        return
    except AttributeError:
        logging.critical(
            f"Api response doesn't have load as json method, trying to load as json manually.")
        try:
            converted_response = json.loads(response_to_convert.content)
        except json.JSONDecodeError:
            logging.critical(critical_decode_error_message)
            print(critical_decode_error_message)
            return

    logging.info("Convert api response into json.")
    return converted_response


def print_error(error_response: requests.Response) -> str:
    """Print the errors the site returns."""
    status_code = error_response.status_code
    error_converting_json_log_message = "{} when converting error_response into json."
    error_converting_json_print_message = f"{status_code}: Couldn't convert api reposnse into json."
    error_message = ''

    if status_code == 429:
        error_message = f'429: {http_error_codes.get(str(status_code))}'
        logging.error(error_message)
        print(error_message)
        time.sleep(mangadex_ratelimit_time * 4)
        return error_message

    # Api didn't return json object
    try:
        error_json = error_response.json()
    except json.JSONDecodeError as e:
        logging.error(error_converting_json_log_message.format(e))
        print(error_converting_json_print_message)
        return error_converting_json_print_message
    # Maybe already a json object
    except AttributeError:
        logging.error(f"error_response is already a json.")
        # Try load as a json object
        try:
            error_json = json.loads(error_response.content)
        except json.JSONDecodeError as e:
            logging.error(error_converting_json_log_message.format(e))
            print(error_converting_json_print_message)
            return error_converting_json_print_message

    # Api response doesn't follow the normal api error format
    try:
        errors = [
            f'{e["status"]}: {e["detail"] if e["detail"] is not None else ""}' for e in error_json["errors"]]
        errors = ', '.join(errors)

        if not errors:
            errors = http_error_codes.get(str(status_code), '')

        error_message = f'Error: {errors}.'
        logging.warning(error_message)
        print(error_message)
    except KeyError:
        error_message = f'KeyError {status_code}: {error_json}.'
        logging.warning(error_message)
        print(error_message)

    return error_message


def login_to_md(session: requests.Session, config: configparser.ConfigParser):
    """Login to MangaDex using the credentials found in the env file."""
    username = config["MangaDex Credentials"]["mangadex_username"]
    password = config["MangaDex Credentials"]["mangadex_password"]

    if username == '' or password == '':
        critical_message = 'Login details missing.'
        logging.critical(critical_message)
        raise Exception(critical_message)

    login_response = session.post(
        f'{md_auth_api_url}/login',
        json={
            "username": username,
            "password": password})

    if login_response.status_code != 200:
        login_response_error_message = f"Couldn't login, mangadex returned an error {login_response.status_code}."
        logging.critical(login_response_error_message)
        print_error(login_response)
        raise Exception(login_response_error_message)

    # Update requests session with headers to always be logged in
    login_response_json = convert_json(login_response)
    if login_response_json is None:
        login_response_json_message = "Couldn't convert login api response into a json."
        logging.error(login_response_json_message)
        raise Exception(login_response_json_message)

    session_token = login_response_json["token"]["session"]
    session.headers.update({"Authorization": f"Bearer {session_token}"})
    logging.info(f'Logged into mangadex.')
    print('Logged in.')


def check_logged_in(
        session: requests.Session,
        config: configparser.ConfigParser):
    """Check if still logged into mangadex."""
    auth_check_response = session.get(f'{md_auth_api_url}/check')

    if auth_check_response.status_code == 200:
        auth_data = convert_json(auth_check_response)
        if auth_data is not None:
            if auth_data["isAuthenticated"]:
                return

    logging.warning(
        f"Checking if logged in returned {auth_check_response.status_code}.")
    print_error(auth_check_response)

    logging.info('Login token expired, logging in again.')
    login_to_md(session, config)


def update_database(
        database_connection: sqlite3.Connection,
        chapter: Chapter,
        succesful_upload_id: UUID = None):
    """Update the database with the new chapter."""
    mplus_chapter_id = chapter.chapter_id

    chapter_id_exists = database_connection.execute(
        'SELECT * FROM chapters WHERE EXISTS(SELECT 1 FROM chapters WHERE md_chapter_id=(?))',
        (succesful_upload_id,
         ))
    chapter_id_exists_dict = chapter_id_exists.fetchone()
    if chapter_id_exists_dict is not None:
        if dict(chapter_id_exists_dict).get('chapter_id',
                                            None) is None and chapter.chapter_id is not None:
            print('Updating database with new mangadex and mangaplus chapter ids.')
            logging.info(
                f'Updating existing record in the database: {chapter}.')
            database_connection.execute(
                'UPDATE chapters SET md_chapter_id=:md_id WHERE chapter_id=:mplus_id', {
                    "md_id": succesful_upload_id, "mplus_id": mplus_chapter_id})
    else:
        logging.info(f'Adding new chapter to database: {chapter}.')
        database_connection.execute(
            """INSERT INTO chapters (chapter_id, chapter_timestamp, chapter_expire, chapter_language, chapter_title, chapter_number, manga_id, md_chapter_id) VALUES
                                                            (:chapter_id, :chapter_timestamp, :chapter_expire, :chapter_language, :chapter_title, :chapter_number, :manga_id, :md_chapter_id)""",
            {
                "chapter_id": mplus_chapter_id,
                "chapter_timestamp": chapter.chapter_timestamp,
                "chapter_expire": chapter.chapter_expire,
                "chapter_language": chapter.chapter_language,
                "chapter_title": chapter.chapter_title,
                "chapter_number": chapter.chapter_number,
                "manga_id": chapter.manga_id,
                "md_chapter_id": succesful_upload_id})
    database_connection.execute(
        'INSERT OR IGNORE INTO posted_mplus_ids (chapter_id) VALUES (?)',
        (mplus_chapter_id,
         ))
    database_connection.commit()


def open_manga_id_map(manga_map_path: Path) -> Dict[UUID, List[int]]:
    """Open mangaplus id to mangadex id map."""
    try:
        with open(manga_map_path, 'r') as manga_map_fp:
            manga_map = json.load(manga_map_fp)
        logging.info('Opened manga id map file.')
    except json.JSONDecodeError:
        logging.critical('Manga map file is corrupted.')
        raise json.JSONDecodeError("Manga map file is corrupted.")
    except FileNotFoundError:
        logging.critical('Manga map file is missing.')
        raise FileNotFoundError("Couldn't file manga map file.")
    return manga_map


class ChapterUploaderProcess:

    def __init__(
            self,
            database_connection: sqlite3.Connection,
            session: requests.Session,
            mangadex_manga_id: UUID,
            chapter: Chapter):
        self.database_connection = database_connection
        self.session = session
        self.mangadex_manga_id = mangadex_manga_id
        self.chapter = chapter
        self.chapter_language = self.chapter.chapter_language
        if isinstance(self.chapter_language, int):
            self.chapter_language = mplus_language_map.get(
                str(self.chapter_language), "NULL")

        self.manga_generic_error_message = f'manga {self.mangadex_manga_id}: {self.chapter.manga_id}, chapter {self.chapter.chapter_number}, language {self.chapter_language}'
        self.upload_retry_total = 3

    def _remove_upload_session(self, upload_session_id: UUID):
        """Delete the upload session."""
        self.session.delete(f'{md_upload_api_url}/{upload_session_id}')
        logging.info(f'Sent {upload_session_id} to be deleted.')

    def _delete_exising_upload_session(self):
        """Remove any exising upload sessions to not error out as mangadex only allows one upload session at a time."""
        removal_retry = 0
        while removal_retry < self.upload_retry_total:
            existing_session = self.session.get(f'{md_upload_api_url}')
            if existing_session.status_code == 200:
                existing_session_json = convert_json(existing_session)
                if existing_session_json is None:
                    removal_retry += 1
                    logging.warning(
                        f"Couldn't convert exising upload session response into a json, retrying.")
                else:
                    self._remove_upload_session(
                        existing_session_json["data"]["id"])
                    return
            elif existing_session.status_code == 404:
                logging.warning("No existing upload session found.")
                return
            elif existing_session.status_code == 401:
                logging.warning("Not logged in, logging in and retrying.")
                login_to_md(self.session, config)
                removal_retry += 1
            else:
                removal_retry += 1
                logging.warning(
                    f"Couldn't delete the exising upload session, retrying.")

            time.sleep(mangadex_ratelimit_time)

        logging.error("Exising upload session not deleted.")

    def check_for_duplicate_chapter(self, manga_chapters: List[dict]) -> bool:
        """Check for duplicate chapters on mangadex."""
        # Skip duplicate chapters
        for md_chapter in manga_chapters:
            if md_chapter["attributes"]["chapter"] == self.chapter.chapter_number and md_chapter["attributes"][
                    "translatedLanguage"] == self.chapter_language and md_chapter["attributes"]["externalUrl"] is not None:
                dupe_chapter_message = f'{self.manga_generic_error_message} already exists on mangadex, skipping.'
                logging.info(dupe_chapter_message)
                print(dupe_chapter_message)
                # Add duplicate chapter to database to avoid checking it again
                # in the future
                update_database(
                    self.database_connection,
                    self.chapter,
                    md_chapter["id"])
                return True
        return False

    def create_upload_session(self) -> Optional[dict]:
        """Try create an upload session 3 times."""
        chapter_upload_session_retry = 0
        chapter_upload_session_successful = False
        while chapter_upload_session_retry < self.upload_retry_total:
            self._delete_exising_upload_session()
            time.sleep(mangadex_ratelimit_time)
            # Start the upload session
            upload_session_response = self.session.post(
                f'{md_upload_api_url}/begin',
                json={
                    "manga": self.mangadex_manga_id,
                    "groups": [mplus_group]})

            if upload_session_response.status_code == 401:
                login_to_md(self.session, config)

            elif upload_session_response.status_code != 200:
                print_error(upload_session_response)
                logging.error(
                    f"Couldn't create an upload session for {self.mangadex_manga_id}, chapter {self.chapter.chapter_number}.")
                print("Couldn't create an upload session.")

            if upload_session_response.status_code == 200:
                upload_session_response_json = convert_json(
                    upload_session_response)

                if upload_session_response_json is not None:
                    chapter_upload_session_successful = True
                    chapter_upload_session_retry == self.upload_retry_total
                    return upload_session_response_json
                else:
                    upload_session_response_json_message = f"Couldn't convert successful upload session creation into a json, retrying. {self.manga_generic_error_message}."
                    logging.error(upload_session_response_json_message)
                    print(upload_session_response_json_message)

            chapter_upload_session_retry += 1
            time.sleep(mangadex_ratelimit_time)

        # Couldn't create an upload session, skip the chapter
        if not chapter_upload_session_successful:
            upload_session_response_json_message = f"Couldn't create an upload session for {self.manga_generic_error_message}."
            logging.error(upload_session_response_json_message)
            print(upload_session_response_json_message)
            return

    def commit_chapter(self, upload_session_id: UUID) -> bool:
        """Try commit the chapter to mangadex."""
        commit_retries = 0
        succesful_upload = False
        while commit_retries < self.upload_retry_total:
            chapter_commit_response = self.session.post(
                f'{md_upload_api_url}/{upload_session_id}/commit',
                json={
                    "chapterDraft": {
                        "volume": None,
                        "chapter": self.chapter.chapter_number,
                        "title": self.chapter.chapter_title,
                        "translatedLanguage": self.chapter_language,
                        "externalUrl": mplus_chapter_url.format(
                            self.chapter.chapter_id),
                        "publishAt": datetime.fromtimestamp(self.chapter.chapter_expire).strftime('%Y-%m-%dT%H:%M:%S%z')},
                    "pageOrder": []})

            if chapter_commit_response.status_code == 200:
                succesful_upload = True
                chapter_commit_response_json = convert_json(
                    chapter_commit_response)

                if chapter_commit_response_json is not None:
                    succesful_upload_id = chapter_commit_response_json["data"]["id"]
                    succesful_upload_message = f"Committed {succesful_upload_id}: {self.chapter.chapter_id} for {self.manga_generic_error_message}."
                    logging.info(succesful_upload_message)
                    print(succesful_upload_message)
                    update_database(
                        self.database_connection,
                        self.chapter,
                        succesful_upload_id)
                    commit_retries == self.upload_retry_total
                    return True

                chapter_commit_response_json_message = f"Couldn't convert successful chapter commit api response into a json"
                logging.error(chapter_commit_response_json_message)
                print(chapter_commit_response_json_message)
                return True

            elif chapter_commit_response.status_code == 401:
                login_to_md(self.session, config)

            else:
                logging.warning(
                    f"Failed to commit {upload_session_id}, retrying.")
                print_error(chapter_commit_response)

            commit_retries += 1
            time.sleep(mangadex_ratelimit_time)

        if not succesful_upload:
            error_message = f"Couldn't commit {upload_session_id}, manga {self.mangadex_manga_id}: {self.chapter.manga_id} chapter {self.chapter.chapter_number}."
            logging.error(error_message)
            print(error_message)
            self._remove_upload_session(upload_session_id)
            return False


class ChapterDeleterProcess:

    def __init__(
            self,
            session: requests.Session,
            posted_chapters: List[dict],
            on_db: bool = True):
        self.session = session
        self.on_db = on_db
        self.chapters_to_delete = [
            dict(x) for x in posted_chapters if datetime.fromtimestamp(
                x["chapter_expire"]) <= datetime.now()]
        self.chapter_delete_ratelimit = 8
        self.chapter_delete_process = None

        logging.info(f"Chapters to delete: {self.chapters_to_delete}")

    def _delete_from_database(self, chapter: dict):
        """Move the chapter from the chapters table to the deleted_chapters table."""
        try:
            database_connection.execute(
                """INSERT INTO deleted_chapters SELECT * FROM chapters WHERE md_chapter_id=(?)""",
                (chapter["md_chapter_id"],
                 ))
        except sqlite3.IntegrityError:
            pass
        database_connection.execute(
            """DELETE FROM chapters WHERE md_chapter_id=(?)""", (chapter["md_chapter_id"],))
        database_connection.commit()

    def _remove_old_chapter(self, chapter: dict):
        """Check if the chapters expired and remove off mangadex if they are."""
        # If the expiry date of the chapter is less than the current time and
        # the md chapter id is available, try delete
        logging.info(
            f'Moving {chapter} from chapters table to deleted_chapters table.')
        md_chapter_id = chapter["md_chapter_id"]
        deleted_message = f'{md_chapter_id}: {chapter["chapter_id"]}, manga {chapter["manga_id"]}, chapter {chapter["chapter_number"]}, language {chapter["chapter_language"]}.'

        if md_chapter_id is not None:
            delete_reponse = self.session.delete(
                f'{mangadex_api_url}/chapter/{md_chapter_id}')

            if delete_reponse.status_code != 200:
                logging.error(
                    f"Couldn't delete expired chapter {deleted_message}")
                print_error(delete_reponse)

                if delete_reponse.status_code == 401:
                    unauthorised_message = f"You're not logged in to delete this chapter {chapter}."
                    logging.error(unauthorised_message)
                    print(unauthorised_message)

                    check_logged_in(self.session, config)
                    time.sleep(mangadex_ratelimit_time)

                    self._remove_old_chapter(chapter)

            if delete_reponse.status_code == 200:
                logging.info(f'Deleted {chapter}.')
                print(f'----Deleted {deleted_message}')

        if self.on_db:
            self._delete_from_database(chapter)
        time.sleep(self.chapter_delete_ratelimit)

    def _delete_expired_chapters(self):
        """Delete expired chapters from mangadex."""
        logging.info(f'Started deleting expired chapters process.')
        print('Deleting expired chapters.')

        for count, chapter_to_delete in enumerate(
                self.chapters_to_delete, start=1):
            if count == 1 or count % 3 == 0:
                check_logged_in(self.session, config)
                time.sleep(mangadex_ratelimit_time)

            self._remove_old_chapter(chapter_to_delete)

    def delete(self):
        """Delete chapters non-concurrently."""
        self._delete_expired_chapters()

    def delete_async(self) -> multiprocessing.Process:
        """Delete chapters concurrently."""
        self.chapter_delete_process = multiprocessing.Process(
            target=self._delete_expired_chapters)
        self.chapter_delete_process.start()
        return self.chapter_delete_process


class MangaUploaderProcess:

    def __init__(
            self,
            database_connection: sqlite3.Connection,
            session: requests.Session,
            updated_manga_chapters: list,
            all_manga_chapters: list,
            processes: list,
            mangadex_manga_id: UUID,
            deleter_process_object: Optional['ChapterDeleterProcess']):

        self.database_connection = database_connection
        self.session = session
        self.updated_manga_chapters = updated_manga_chapters
        self.all_manga_chapters = all_manga_chapters
        self.processes = processes
        self.mangadex_manga_id = mangadex_manga_id
        self.deleter_process_object = deleter_process_object

        self.chapters: List[dict] = updated_manga_chapters[self.mangadex_manga_id]
        self.chapters_all: List[dict] = all_manga_chapters[self.mangadex_manga_id]

        self.second_process = None
        self.second_process_object = None

        self.manga_chapters = self._get_mangadex_chapters()
        self._delete_extra_chapters()

    def _remove_chapters_not_mplus(self) -> List[dict]:
        """Find chapters on MangaDex not on MangaPlus."""
        md_chapters_not_mplus = [c for c in self.manga_chapters if
                                 c["attributes"]["chapter"] not in [x.chapter_number for x in self.chapters_all] or
                                 c["attributes"]["translatedLanguage"] not in mplus_language_map.values()]
        chapters_to_delete = []
        for expired in md_chapters_not_mplus:
            md_chapter_id = expired["id"]

            expired_chapter_object = Chapter(
                chapter_timestamp=946684799,
                chapter_expire=946684799,
                chapter_language=expired["attributes"]["translatedLanguage"],
                chapter_title=expired["attributes"]["title"],
                chapter_number=expired["attributes"]["chapter"],
                manga_id=self.mangadex_manga_id,
                md_chapter_id=md_chapter_id)

            # update_database(
            #     self.database_connection,
            #     expired_chapter_object,
            #     md_chapter_id)
            chapters_to_delete.append(vars(expired_chapter_object))

        return chapters_to_delete

    def _get_mangadex_chapters(self):
        return self._get_chapters(params={
            "groups[]": [mplus_group],
            "manga": self.mangadex_manga_id,
            "order[createdAt]": "desc",
        })

    def _delete_extra_chapters(self):
        chapters_to_delete = self._remove_chapters_not_mplus()
        if chapters_to_delete:
            if self.deleter_process_object is not None:
                if not self.deleter_process_object.chapter_delete_process.is_alive():
                    if not self.deleter_process_object.chapters_to_delete:
                        self.deleter_process_object.chapters_to_delete = chapters_to_delete
                    else:
                        self.deleter_process_object.chapters_to_delete.extend(chapters_to_delete)
                    self.deleter_process_object.delete_async()
                else:
                    self.deleter_process_object.chapters_to_delete.extend(chapters_to_delete)
            else:
                self.second_process_object = ChapterDeleterProcess(
                    self.session, chapters_to_delete)
                self.second_process_object.delete()

    def start_manga_uploading_process(self):
        self.skipped = 0
        self.skipped_chapter = False
        for count, chapter in enumerate(self.chapters, start=1):
            chapter: Chapter = chapter
            # Delete existing upload session if exists
            if not self.skipped_chapter and count % 3 == 0:
                check_logged_in(self.session, config)
                time.sleep(mangadex_ratelimit_time)

            chapter_to_upload_process = ChapterUploaderProcess(
                self.database_connection, self.session, self.mangadex_manga_id, chapter)

            duplicate_chapter = chapter_to_upload_process.check_for_duplicate_chapter(
                self.manga_chapters)
            if duplicate_chapter:
                self.skipped += 1
                self.skipped_chapter = True
                continue

            upload_session_response_json = chapter_to_upload_process.create_upload_session()
            if upload_session_response_json is None:
                time.sleep(mangadex_ratelimit_time)
                self.skipped += 1
                self.skipped_chapter = False
                continue

            upload_session_id = upload_session_response_json["data"]["id"]
            chapter_committed = chapter_to_upload_process.commit_chapter(
                upload_session_id)
            if not chapter_committed:
                self.skipped += 1

            self.skipped_chapter = False
            time.sleep(mangadex_ratelimit_time)

        if self.skipped != 0:
            skipped_chapters_message = f'Skipped {self.skipped} chapters out of {len(self.chapters)} for manga {self.mangadex_manga_id}: {chapter.manga_id}.'
            logging.info(skipped_chapters_message)
            print(skipped_chapters_message)

        if self.second_process_object is not None:
            del self.second_process_object
        time.sleep(mangadex_ratelimit_time * 2)

    def _get_chapters(self, params: dict) -> list:
        """Go through each page in the api to get all the chapters."""
        chapters = []
        limit = 100
        offset = 0
        pages = 1
        iteration = 1
        created_at_since_time = '2000-01-01T00:00:00'

        parameters = {}
        parameters.update(params)

        while True:
            # Update the parameters with the new offset
            parameters.update({
                "limit": limit,
                "offset": offset,
                'createdAtSince': created_at_since_time
            })

            # Call the api and get the json data
            chapters_response = self.session.get(
                f'{mangadex_api_url}/chapter', params=parameters)
            if chapters_response.status_code != 200:
                print_error(chapters_response)
                manga_response_message = f"Couldn't get chapters of manga {params['manga']}."
                logging.error(manga_response_message)
                continue

            chapters_response_data = convert_json(chapters_response)
            if chapters_response_data is None:
                logging.warning(
                    f"Couldn't convert chapters data into json, retrying.")
                continue

            chapters.extend(chapters_response_data["data"])
            offset += limit

            # Finds how many pages needed to be called
            if pages == 1:
                chapters_count = chapters_response_data.get('total', 0)

                if not chapters_response_data["data"]:
                    chapters_count = 0

                if chapters_count > limit:
                    pages = math.ceil(chapters_count / limit)

                logging.info(f"{pages} page(s) for manga {params['manga']}.")

            # Wait every 5 pages
            if iteration % 5 == 0 and pages != 5:
                time.sleep(5)

            # End the loop when all the pages have been gone through
            # Offset 10000 is the highest you can go, reset offset and get next
            # 10k batch using the last available chapter's created at date
            if iteration == pages or offset == 10000 or not chapters_response_data["data"]:
                if chapters_count >= 10000 and offset == 10000:
                    logging.debug(
                        'Reached 10k chapters, looping over next 10k.')
                    created_at_since_time = (
                        chapters[-1]["attributes"]["createdAt"].split('+')[0])
                    offset = 0
                    pages = 1
                    iteration = 1
                    time.sleep(5)
                    continue
                break

            iteration += 1

        time.sleep(mangadex_ratelimit_time)
        return chapters


class MPlusAPI:

    def __init__(
            self,
            manga_map_mplus_ids: List[int],
            posted_chapters_ids: List[int]):
        self.tracked_manga = manga_map_mplus_ids
        self.posted_chapters_ids = posted_chapters_ids
        self.updated_chapters: List[Chapter] = []
        self.all_mplus_chapters: List[Chapter] = []
        self.untracked_manga: List[Manga] = []

        self.get_mplus_updated_manga()
        self.get_mplus_updates()

    def _get_proto_response(
            self,
            response_proto: bytes) -> response_pb.Response:
        """Convert api response into readable data."""
        response = response_pb.Response()
        response.ParseFromString(response_proto)
        return response

    def _request_from_api(
            self,
            manga_id: Optional[int] = None,
            updated: Optional[bool] = False) -> Optional[bytes]:
        """Get manga and chapter details from the api."""
        try:
            if manga_id is not None:
                response = requests.get(
                    mplus_base_api_url +
                    "/api/title_detail",
                    params={
                        'title_id': manga_id},
                    stream=True)
            elif updated:
                response = requests.get(
                    mplus_base_api_url +
                    "/api/title_list/updated",
                    params={
                        'lang': 'eng'},
                    stream=True)
        except Exception as e:
            logging.error(f"{e}: Couldn't get details from the mangaplus api.")
            print("Request API Error", e)
            return

        if response.status_code == 200:
            return response.content

    def get_mplus_updated_manga(self):
        """Find new untracked mangaplus series."""
        logging.info('Looking for new untracked manga.')
        print('Getting new manga.')
        updated_manga_response = self._request_from_api(updated=True)
        if updated_manga_response is not None:
            updated_manga_response_parsed = self._get_proto_response(
                updated_manga_response)
            updated_manga_details = updated_manga_response_parsed.success.updated

            for manga in updated_manga_details.updated_manga_detail:
                if manga.updated_manga.manga_id not in self.tracked_manga:
                    manga_id = manga.updated_manga.manga_id
                    manga_name = manga.updated_manga.manga_name
                    language = manga.updated_manga.language
                    self.untracked_manga.append(
                        Manga(
                            manga_id=manga_id,
                            manga_name=manga_name,
                            manga_language=language))
                    logging.info(
                        f"Found untracked manga {manga_id}: {manga_name}.")
                    print(f"Found untracked manga {manga_id}: {manga_name}.")

    def get_mplus_updates(self):
        """Get latest chapter updates."""
        logging.info('Looking for tracked manga new chapters.')
        print('Getting new chapters.')
        processes = []

        spliced_manga = [self.tracked_manga[l:l + 3]
                         for l in range(0, len(self.tracked_manga), 3)]
        self.updated_chapters = multiprocessing_manager.list()
        self.all_mplus_chapters = multiprocessing_manager.list()
        for mangas in spliced_manga:
            process = multiprocessing.Process(
                target=self._chapter_updates, args=(mangas,))
            process.start()
            processes.append(process)

        for process in processes:
            if process is not None:
                process.join()

    def _chapter_updates(self, mangas: list):
        """Get the updated chapters from each manga."""
        for manga in mangas:
            manga_response = self._request_from_api(manga_id=manga)
            if manga_response is not None:
                manga_response_parsed = self._get_proto_response(
                    manga_response)

                manga_chapters = manga_response_parsed.success.manga_detail
                manga_name = manga_chapters.manga.manga_name
                manga_chapters_lists = []

                manga_chapters_lists.append(
                    list(manga_chapters.first_chapter_list))
                if len(manga_chapters.last_chapter_list) > 0:
                    manga_chapters_lists.append(
                        list(manga_chapters.last_chapter_list))

                all_chapters = self.get_latest_chapters(
                    manga_chapters_lists, manga_chapters, self.posted_chapters_ids, True)
                self.all_mplus_chapters.extend(all_chapters)

                updated_chapters = self.get_latest_chapters(
                    manga_chapters_lists, manga_chapters, self.posted_chapters_ids)
                logging.info(updated_chapters)

                if updated_chapters:
                    print(f'Manga {manga_name}: {manga}.')
                for update in updated_chapters:
                    print(
                        f'--Found {update.chapter_id} chapter {update.chapter_number} language {update.chapter_language}.')

                self.updated_chapters.extend(updated_chapters)

    def _get_surrounding_chapter(
            self,
            chapters: list,
            current_chapter,
            next_chapter_search: bool = False) -> Optional[Any]:
        """Find the previous and next chapter to the current."""
        chapters = list(chapters)
        # Starts from the first chapter before the current
        index_search = reversed(chapters[:chapters.index(current_chapter)])
        if next_chapter_search:
            # Starts from the first chapter after the current
            index_search = chapters[chapters.index(current_chapter):]

        for chapter in index_search:
            try:
                int(chapter.chapter_number.strip('#'))
            except ValueError:
                continue
            else:
                return chapter

    def _strip_chapter_number(self, number: Union[str, int]) -> str:
        """Returns the chapter number without the un-needed # or 0."""
        stripped = str(number).strip('#')

        parts = re.split('\.|\-', stripped)
        parts[0] = '0' if len(parts[0].lstrip(
            '0')) == 0 else parts[0].lstrip('0')
        stripped = '.'.join(parts)

        return stripped

    def _normalise_chapter_number(
            self, chapters: list, chapter) -> List[Optional[str]]:
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
                    chapters, chapter, next_chapter_search=True)
                if next_chapter is None:
                    chapter_number = None
                else:
                    next_chapter_number = self._strip_chapter_number(
                        next_chapter.chapter_number)
                    chapter_number = int(next_chapter_number.split(',')[0]) - 1
                    first_index = next_chapter
                    second_index = chapter
            else:
                previous_chapter_number = self._strip_chapter_number(
                    previous_chapter.chapter_number)
                chapter_number = previous_chapter_number.split(',')[-1]
                first_index = chapter
                second_index = previous_chapter

            if chapter_number == 'ex':
                chapter_number = None

            if chapter_number is not None and current_number != 'ex':
                # If difference between current chapter and previous/next
                # chapter is more than 5, use None as chapter_number
                if math.sqrt(
                        (int(current_number) - int(chapter_number))**2) >= 5:
                    chapter_number = None

            if chapter_number is not None:
                chapter_decimal = '5'

                # There may be multiple extra chapters before the last numbered chapter
                # Use index difference as decimal to avoid not uploading
                # non-dupes
                try:
                    chapter_difference = chapters.index(
                        first_index) - chapters.index(second_index)
                    if chapter_difference > 1:
                        chapter_decimal = chapter_difference
                except (ValueError, IndexError):
                    pass

                chapter_number = f"{chapter_number}.{chapter_decimal}"
        elif chapter_number == "One-Shot":
            chapter_number = None

        if chapter_number is None:
            chapter_number_split = [chapter_number]
        else:
            chapter_number_split = [self._strip_chapter_number(
                chap_number) for chap_number in chapter_number.split(',')]
        return chapter_number_split

    def _normalise_chapter_title(self, chapter) -> Optional[str]:
        colon_regex = r'^.+:\s?'
        no_title_regex = r'^\S+\s?\d+(?:\,\d{0,2})?$'
        hashtag_regex = r'^(?:\S+\s?)?#\d+(?:\,\d{0,2})?\s'
        period_regex = r'^(?:\S+\s?)?\d+(?:\,\d{0,2})?\s?[\.\/]\s'
        spaces_regex = r'^(?:\S+\s?)?\d+(?:\,\d{0,2})?\s'

        title = str(chapter.chapter_name)
        normalised_title = title
        pattern_to_use = None
        replace_string = ''

        if ':' in title:
            pattern_to_use = colon_regex
        elif re.match(no_title_regex, title):
            pattern_to_use = no_title_regex
        elif re.match(hashtag_regex, title):
            pattern_to_use = hashtag_regex
        elif re.match(period_regex, title):
            pattern_to_use = period_regex
        elif re.match(spaces_regex, title):
            pattern_to_use = spaces_regex

        if pattern_to_use is not None:
            normalised_title = re.sub(pattern=pattern_to_use, repl=replace_string, string=title, flags=re.I)

        if normalised_title == '':
            normalised_title = None
        return normalised_title

    def get_latest_chapters(
            self,
            manga_chapters_lists: list,
            manga_chapters,
            posted_chapters: List[int],
            all_chapters: bool = False) -> List[Chapter]:
        """Get the latest un-uploaded chapters."""
        updated_chapters = []

        for chapters in manga_chapters_lists:
            # Go through the last three chapters
            for chapter in chapters:
                if not all_chapters:
                    # Chapter id is not in database or chapter expiry isn't
                    # before atm
                    if chapter.chapter_id in posted_chapters or datetime.fromtimestamp(
                            chapter.end_timestamp) <= datetime.now():
                        continue

                chapter_number_split = self._normalise_chapter_number(
                    chapters, chapter)
 
                chapter_title = self._normalise_chapter_title(chapter)

                # MPlus sometimes joins two chapters as one, upload to md as
                # two different chapters
                for chap_number in chapter_number_split:
                    updated_chapters.append(
                        Chapter(
                            chapter_id=chapter.chapter_id,
                            chapter_timestamp=chapter.start_timestamp,
                            chapter_title=chapter_title,
                            chapter_expire=chapter.end_timestamp,
                            chapter_number=chap_number,
                            chapter_language=mplus_language_map.get(
                                str(
                                    manga_chapters.manga.language),
                                "NULL"),
                            manga_id=manga_chapters.manga.manga_id))

        return updated_chapters


class BotProcess:

    def __init__(
            self,
            session: requests.Session,
            updates: list,
            all_mplus_chapters: list,
            first_process_object: Optional['ChapterDeleterProcess']):
        self.session = session
        self.updates = updates
        self.all_mplus_chapters = all_mplus_chapters
        self.first_process_object = first_process_object
        self.processes: List[multiprocessing.Process] = []

    def _get_md_id(self,
                   manga_id_map: Dict[UUID,
                                      List[int]],
                   mangaplus_id: int) -> Optional[UUID]:
        """Get the mangadex id from the mangaplus one."""
        for md_id in manga_id_map:
            if mangaplus_id in manga_id_map[md_id]:
                return md_id

    def _sort_chapters_by_manga(self, updates: list) -> dict:
        """Sort the chapters by manga id."""
        chapters_sorted = {}

        for chapter in updates:
            md_id = self._get_md_id(manga_id_map, chapter.manga_id)
            if md_id is None:
                logging.warning(f'No mangadex id found for mplus id {chapter.manga_id}.')
                continue

            try:
                chapters_sorted[md_id].append(chapter)
            except (KeyError, ValueError, AttributeError):
                chapters_sorted[md_id] = [chapter]
        return chapters_sorted

    def upload_chapters(self):
        """Go through each new chapter and upload it to mangadex."""
        # Sort each chapter by manga
        updated_manga_chapters = self._sort_chapters_by_manga(self.updates)
        all_manga_chapters = self._sort_chapters_by_manga(
            self.all_mplus_chapters)
        processes = []

        for mangadex_manga_id in updated_manga_chapters:
            # Get each manga's uploaded chapters on mangadex
            manga_uploader = MangaUploaderProcess(
                database_connection,
                self.session,
                updated_manga_chapters,
                all_manga_chapters,
                processes,
                mangadex_manga_id,
                self.first_process_object)
            manga_uploader.start_manga_uploading_process()


def main():
    """Main function for getting the updates."""
    # Get already posted chapters
    posted_chapters_data = database_connection.execute(
        "SELECT * FROM chapters").fetchall()
    posted_chapters_ids_data = database_connection.execute(
        "SELECT * FROM posted_mplus_ids").fetchall()
    posted_chapters_ids = [job["chapter_id"]
                           for job in posted_chapters_ids_data] if not fill_backlog else []
    manga_map_mplus_ids = [
        mplus_id for md_id in manga_id_map for mplus_id in manga_id_map[md_id]]
    logging.info(
        'Retrieved posted chapters from database and got mangaplus ids from manga id map file.')

    session = requests.Session()
    login_to_md(session, config)

    # Start deleting expired chapters
    first_process_object = ChapterDeleterProcess(
        session, [dict(k) for k in posted_chapters_data])
    first_process = first_process_object.delete_async()

    # Get new manga and chapter updates
    mplus_api = MPlusAPI(manga_map_mplus_ids, posted_chapters_ids)
    updated_manga = mplus_api.untracked_manga
    updates = mplus_api.updated_chapters
    all_mplus_chapters = mplus_api.all_mplus_chapters

    if not updates:
        logging.info("No new updates found.")
        print("No new updates found.")
    else:
        logging.info(f'Found {len(updates)} update(s).')
        print(f'Found {len(updates)} update(s).')
        BotProcess(session, updates, all_mplus_chapters,
                first_process_object).upload_chapters()

    if first_process is not None:
        first_process.join()


if __name__ == "__main__":

    manga_id_map = open_manga_id_map(
        Path(config["User Set"]["manga_id_map_path"]))
    multiprocessing_manager = multiprocessing.Manager()

    main()

    # Save and close database
    database_connection.commit()
    database_connection.close()
    logging.info('Saved and closed database.')
