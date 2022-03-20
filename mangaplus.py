import argparse
import base64
import configparser
import json
import logging
import math
import multiprocessing
import re
import sqlite3
import string
import time
from dataclasses import dataclass, field, replace
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List, Literal, Optional, Union

import requests
from requests.packages.urllib3.exceptions import InsecureRequestWarning

import response_pb2 as response_pb
from webhook import (
    MPlusBotDeleterWebhook,
    MPlusBotDupesWebhook,
    MPlusBotNotIndexedWebhook,
    MPlusBotUpdatesWebhook,
)
from webhook import webhook as WEBHOOK

requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

__version__ = "1.6.0"

mplus_language_map = {
    "0": "en",
    "1": "es-la",
    "2": "fr",
    "3": "id",
    "4": "pt-br",
    "5": "ru",
    "6": "th",
}

http_error_codes = {
    "400": "Bad request.",
    "401": "Unauthorised.",
    "403": "Forbidden.",
    "404": "Not found.",
    "429": "Too many requests.",
}

root_path = Path(".")
config_file_path = root_path.joinpath("config").with_suffix(".ini")

log_folder_path = root_path.joinpath("logs").joinpath("bot")
log_folder_path.mkdir(parents=True, exist_ok=True)


def setup_logs():
    logs_path = log_folder_path.joinpath(f"mplus_md_uploader_{str(date.today())}.log")
    fileh = logging.FileHandler(logs_path, "a")
    formatter = logging.Formatter(
        "%(asctime)s,%(msecs)d %(levelname)-8s [%(filename)s:%(lineno)d] %(message)s"
    )
    fileh.setFormatter(formatter)

    log = logging.getLogger()  # root logger
    for hdlr in log.handlers[:]:  # remove all old handlers
        if isinstance(hdlr, logging.FileHandler):
            log.removeHandler(hdlr)
    log.addHandler(fileh)
    log.setLevel(logging.DEBUG)


setup_logs()


def load_config_info(config: configparser.RawConfigParser):
    if config["Paths"].get("database_path", "") == "":
        logging.warning("Database path empty, using default.")
        config["Paths"]["database_path"] = "chapters.db"

    if config["Paths"].get("mangadex_api_url", "") == "":
        logging.warning("Mangadex api path empty, using default.")
        config["Paths"]["mangadex_api_url"] = "https://api.mangadex.org"

    if config["Paths"].get("manga_id_map_path", "") == "":
        logging.info("Manga id map path empty, using default.")
        config["Paths"]["manga_id_map_path"] = "manga.json"

    if config["Paths"].get("title_regex_path", "") == "":
        logging.info("Title regex map path empty, using default.")
        config["Paths"]["title_regex_path"] = "title_regex.json"

    if config["Paths"].get("mdauth_path", "") == "":
        logging.info("mdauth path empty, using default.")
        config["Paths"]["mdauth_path"] = ".mdauth"

    if config["Paths"].get("components_path", "") == "":
        logging.info("components path empty, using default.")
        config["Paths"]["components_path"] = "components_path"


def open_config_file() -> configparser.RawConfigParser:
    # Open config file and read values
    if config_file_path.exists():
        config = configparser.RawConfigParser()
        config.read(config_file_path)
        logging.info("Loaded config file.")
    else:
        logging.critical("Config file not found, exiting.")
        raise FileNotFoundError("Config file not found.")

    load_config_info(config)
    return config


config = open_config_file()
components_path = root_path.joinpath(config["Paths"]["components_path"])
components_path.mkdir(parents=True, exist_ok=True)

mangadex_api_url = config["Paths"]["mangadex_api_url"]
md_upload_api_url = f"{mangadex_api_url}/upload"
MPLUS_GROUP_ID = "4f1de6a2-f0c5-4ac5-bce5-02c7dbb67deb"

try:
    RATELIMIT_TIME = int(config["User Set"].get("mangadex_ratelimit_time", ""))
except (ValueError, KeyError):
    RATELIMIT_TIME = 2


try:
    UPLOAD_RETRY = int(config["User Set"].get("upload_retry", ""))
except (ValueError, KeyError):
    UPLOAD_RETRY = 3


def make_tables(database_connection: sqlite3.Connection):
    """Make the database table."""
    logging.info("Creating new tables for database.")
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
        logging.error("Database tables don't exist, making new ones.")
        print("Tables don't exist, making new ones.")
        make_tables(database_connection)
        fill_backlog = True
    return fill_backlog


database_name = config["Paths"]["database_path"]
database_path = components_path.joinpath(database_name)


def open_database(db_path: Path) -> tuple[sqlite3.Connection, bool]:
    database_connection = sqlite3.connect(db_path)
    database_connection.row_factory = sqlite3.Row
    logging.info("Opened database.")

    fill_backlog = check_table_exists(database_connection)
    return database_connection, fill_backlog


database_connection, fill_backlog = open_database(database_path)


@dataclass(order=True)
class Manga:
    manga_id: int
    manga_name: str
    manga_language: str

    def __post_init__(self):
        language = self.manga_language
        try:
            language = int(language)
        except ValueError:
            pass
        else:
            self.manga_language = mplus_language_map.get(str(language), "NULL")


@dataclass()
class Chapter:
    chapter_timestamp: int
    chapter_expire: int
    chapter_title: str
    chapter_number: str
    chapter_language: str
    chapter_id: Optional[int] = field(default=None)
    md_chapter_id: Optional[str] = field(default=None)
    manga_id: Optional[int] = field(default=None)
    md_manga_id: Optional[str] = field(default=None)
    manga: Optional[Manga] = field(default=None)

    def __post_init__(self):
        language = self.chapter_language
        try:
            language = int(language)
        except ValueError:
            pass
        else:
            self.chapter_language = mplus_language_map.get(str(language), "NULL")


def convert_json(response_to_convert: requests.Response) -> Optional[dict]:
    """Convert the api response into a parsable json."""
    critical_decode_error_message = (
        "Couldn't convert mangadex api response into a json."
    )

    logging.debug(
        f"Request id: {response_to_convert.headers.get('x-request-id', None)}"
    )

    try:
        converted_response = response_to_convert.json()
    except json.JSONDecodeError:
        logging.critical(critical_decode_error_message)
        print(critical_decode_error_message)
        return
    except AttributeError:
        logging.critical(
            f"Api response doesn't have load as json method, trying to load as json manually."
        )
        try:
            converted_response = json.loads(response_to_convert.content)
        except json.JSONDecodeError:
            logging.critical(critical_decode_error_message)
            print(critical_decode_error_message)
            return

    logging.debug("Convert api response into json.")
    return converted_response


def print_error(
    error_response: requests.Response,
    *,
    show_error: bool = True,
    log_error: bool = False,
) -> str:
    """Print the errors the site returns."""
    status_code = error_response.status_code
    error_converting_json_log_message = (
        "{} when converting the error response into json."
    )
    error_converting_json_print_message = (
        f"{status_code}: Couldn't convert api response into json."
    )
    error_message = ""

    if status_code == 429:
        error_message = f"429: {http_error_codes.get(str(status_code))}"
        if log_error:
            logging.error(error_message)
        if show_error:
            print(error_message)
        time.sleep(RATELIMIT_TIME * 4)
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
        logging.error(f"Error response is already a json.")
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
            f'{e["status"]}: {e["detail"] if e["detail"] is not None else ""}'
            for e in error_json["errors"]
        ]
        errors = ", ".join(errors)

        if not errors:
            errors = http_error_codes.get(str(status_code), "")

        error_message = f"Error: {errors}"
        if log_error:
            logging.warning(error_message)
        if show_error:
            print(error_message)
    except KeyError:
        error_message = f"KeyError {status_code}: {error_json}."
        if log_error:
            logging.warning(error_message)
        if show_error:
            print(error_message)

    return error_message


def flatten(t: List[list]) -> list:
    """Flatten nested lists into one list."""
    return [item for sublist in t for item in sublist]


def get_md_id(manga_id_map: Dict[str, List[int]], mangaplus_id: int) -> Optional[str]:
    """Get the mangadex id from the mangaplus one."""
    for md_id in manga_id_map:
        if mangaplus_id in manga_id_map[md_id]:
            return md_id


def update_database(
    database_connection: sqlite3.Connection,
    chapter: Chapter,
    succesful_upload_id: Optional[str] = None,
):
    """Update the database with the new chapter."""
    mplus_chapter_id = chapter.chapter_id

    chapter_id_exists = database_connection.execute(
        "SELECT * FROM chapters WHERE EXISTS(SELECT 1 FROM chapters WHERE md_chapter_id=(?))",
        (succesful_upload_id,),
    )
    chapter_id_exists_dict = chapter_id_exists.fetchone()
    if chapter_id_exists_dict is not None:
        if (
            dict(chapter_id_exists_dict).get("chapter_id", None) is None
            and chapter.chapter_id is not None
        ):
            print("Updating database with new mangadex and mangaplus chapter ids.")
            logging.info(f"Updating existing record in the database: {chapter}.")
            database_connection.execute(
                "UPDATE chapters SET chapter_id=:mplus_id, md_manga_id=:md_manga_id WHERE md_chapter_id=:md_id",
                {
                    "md_id": succesful_upload_id,
                    "mplus_id": mplus_chapter_id,
                    "md_manga_id": chapter.md_manga_id,
                },
            )
    else:
        logging.info(f"Adding new chapter to database: {chapter}.")
        database_connection.execute(
            """INSERT INTO chapters (chapter_id, chapter_timestamp, chapter_expire, chapter_language, chapter_title, chapter_number, manga_id, md_chapter_id, md_manga_id) VALUES
                                                            (:chapter_id, :chapter_timestamp, :chapter_expire, :chapter_language, :chapter_title, :chapter_number, :manga_id, :md_chapter_id, :md_manga_id)""",
            {
                "chapter_id": mplus_chapter_id,
                "chapter_timestamp": chapter.chapter_timestamp,
                "chapter_expire": chapter.chapter_expire,
                "chapter_language": chapter.chapter_language,
                "chapter_title": chapter.chapter_title,
                "chapter_number": chapter.chapter_number,
                "manga_id": chapter.manga_id,
                "md_chapter_id": succesful_upload_id,
                "md_manga_id": chapter.md_manga_id,
            },
        )
    database_connection.execute(
        "INSERT OR IGNORE INTO posted_mplus_ids (chapter_id) VALUES (?)",
        (mplus_chapter_id,),
    )

    if chapter_id_exists_dict is None:
        logging.debug(f"Added to database: {succesful_upload_id} - {chapter}")
    database_connection.commit()


def open_manga_id_map(manga_map_path: Path) -> Dict[str, List[int]]:
    """Open mangaplus id to mangadex id map."""
    try:
        with open(manga_map_path, "r") as manga_map_fp:
            manga_map = json.load(manga_map_fp)
        logging.info("Opened manga id map file.")
    except json.JSONDecodeError as e:
        logging.critical("Manga map file is corrupted.")
        raise json.JSONDecodeError(
            msg="Manga map file is corrupted.", doc=e.doc, pos=e.pos
        )
    except FileNotFoundError:
        logging.critical("Manga map file is missing.")
        raise FileNotFoundError("Couldn't file manga map file.")
    return manga_map


def open_title_regex(title_regex_path: Path) -> dict:
    """Open the chapter title regex."""
    try:
        with open(title_regex_path, "r") as title_regex_fp:
            title_regexes = json.load(title_regex_fp)
        logging.info("Opened title regex file.")
    except json.JSONDecodeError as e:
        logging.critical("Title regex file is corrupted.")
        raise json.JSONDecodeError(
            msg="Title regex file is corrupted.", doc=e.doc, pos=e.pos
        )
    except FileNotFoundError:
        logging.critical("Title regex file is missing.")
        raise FileNotFoundError("Couldn't file title regex file.")
    return title_regexes


class AuthMD:
    def __init__(self, session: requests.Session, config: configparser.RawConfigParser):
        self.session = session
        self.config = config
        self.token_file = components_path.joinpath(config["Paths"]["mdauth_path"])
        self.md_auth_api_url = f"{mangadex_api_url}/auth"
        self.file_token = self._open_auth_file()

        self.first_login = True
        self.successful_login = False
        self.session_token = self.file_token.get("session", None)
        self.refresh_token = self.file_token.get("refresh", None)
        self.decoded_session_token = None
        self.decoded_refresh_token = None

    def _open_auth_file(self) -> dict:
        """Open auth file and read saved tokens."""
        try:
            with open(self.token_file, "r") as login_file:
                token = json.load(login_file)
            return token
        except (FileNotFoundError, json.JSONDecodeError):
            logging.error(
                "Couldn't find the file, trying to login using your account details."
            )
            return {}

    def _save_session(self, token: dict):
        """Save the session and refresh tokens."""
        with open(self.token_file, "w") as login_file:
            login_file.write(json.dumps(token, indent=4))
        logging.debug("Saved mdauth file.")

    def _update_headers(self, session_token: str):
        """Update the session headers to include the auth token."""
        self.session.headers = {"Authorization": f"Bearer {session_token}"}

    def _update_token_details(self, token: dict):
        """Update the instance session and refresh tokens, update the header and save the file."""
        self.session_token = token["session"]
        self.refresh_token = token["refresh"]
        self.decoded_session_token = self._decode_token(self.session_token)
        self.decoded_refresh_token = self._decode_token(self.refresh_token)

        self._update_headers(token["session"])
        self._save_session(token)

    def _refresh_token(self) -> bool:
        """Use the refresh token to get a new session token."""
        refreshed = False

        if self.refresh_token is None:
            logging.error(
                f"Refresh token doesn't exist, logging in through account details."
            )
            return self._login_using_details()

        refresh_response = self.session.post(
            f"{self.md_auth_api_url}/refresh",
            json={"token": self.refresh_token},
            verify=False,
        )

        if refresh_response.status_code == 200:
            refresh_response_json = convert_json(refresh_response)
            if refresh_response_json is not None:
                refresh_data = refresh_response_json["token"]

                self._update_token_details(refresh_data)
                refreshed = True
            else:
                refreshed = False
        elif refresh_response.status_code in (401, 403):
            error = print_error(refresh_response)
            logging.warning(
                f"Couldn't login using refresh token, logging in using your account. Error: {error}"
            )
            return self._login_using_details()
        else:
            error = print_error(refresh_response)
            logging.error(f"Couldn't refresh token. Error: {error}")

        return refreshed

    def _check_login(self) -> bool:
        """Try login using saved session token."""
        auth_check_response = self.session.get(
            f"{self.md_auth_api_url}/check", verify=False
        )

        if auth_check_response.status_code == 200:
            auth_data = convert_json(auth_check_response)
            if auth_data is not None:
                if auth_data["isAuthenticated"]:
                    logging.info("Already logged in.")
                    return True

        if self.refresh_token is None:
            return self._login_using_details()
        return self._refresh_token()

    def _login_using_details(self) -> bool:
        """Login using account details."""
        username = self.config["MangaDex Credentials"]["mangadex_username"]
        password = self.config["MangaDex Credentials"]["mangadex_password"]

        if username == "" or password == "":
            critical_message = "Login details missing."
            logging.critical(critical_message)
            raise Exception(critical_message)

        login_response = self.session.post(
            f"{self.md_auth_api_url}/login",
            json={"username": username, "password": password},
            verify=False,
        )

        if login_response.status_code == 200:
            login_response_json = convert_json(login_response)
            if login_response_json is not None:
                login_token = login_response_json["token"]
                self._update_token_details(login_token)
                return True

        error = print_error(login_response)
        logging.error(
            f"Couldn't login to mangadex using the details provided. Error: {error}."
        )
        return False

    def _decode_token(self, token: str) -> dict:
        """Read the payload stored in the json web token."""
        payload = token.split(".")[1]
        padding = len(payload) % 4
        payload += "=" * padding
        try:
            parsed_payload: dict = json.loads(base64.b64decode(payload))
        except (json.JSONDecodeError,):
            parsed_payload = {}
        return parsed_payload

    def _check_token_expiry(self, token: dict) -> bool:
        """Check if the token is expired or will expire in the next two minutes."""
        expiry = datetime.fromtimestamp(
            token.get("exp", 946684799),
            timezone.utc,
        )
        datetime_now = datetime.now(timezone.utc) + timedelta(minutes=2)

        if expiry > datetime_now:
            return False
        return True

    def login(self, check_login=True):
        """Login to MD account using details or saved token."""
        if not check_login and self.successful_login:
            logging.info("Already logged in, not checking for login.")
            return

        if self.first_login:
            logging.info("Trying to login through the .mdauth file.")

        if self.session_token is not None:
            if self.first_login:
                logging.info("Reading the session expiry from the token.")

            if self.decoded_session_token is None:
                logging.debug("Decoding the token into a json object.")
                self.decoded_session_token = self._decode_token(self.session_token)

            expired_session_token = self._check_token_expiry(self.decoded_session_token)
            if expired_session_token:
                logging.warning("Session expired, refreshing using token.")
                logged_in = self._refresh_token()
            else:
                self._update_headers(self.session_token)
                logged_in = True
        else:
            logged_in = self._refresh_token()

        if logged_in:
            self.successful_login = True
            if self.first_login:
                logging.info(f"Logged into mangadex.")
                print("Logged in.")
                self.first_login = False
        else:
            logging.critical("Couldn't login.")
            raise Exception("Couldn't login.")


class ChapterDeleterProcess:
    def __init__(
        self,
        *,
        session: requests.Session,
        posted_chapters: List[dict],
        md_auth_object: AuthMD,
        on_db: bool = True,
    ):
        self.session = session
        self.on_db = on_db
        self.posted_chapters = posted_chapters
        self.md_auth_object = md_auth_object
        self.chapters_to_delete = self.get_chapter_to_delete()
        self.chapter_delete_ratelimit = 8
        self.chapter_delete_process = None

        logging.info(f"Chapters to delete: {self.chapters_to_delete}")

    def _open_database() -> sqlite3.Connection:
        database_connection, _ = open_database(database_path)
        return database_connection

    def get_chapter_to_delete(self) -> List[dict]:
        return [
            dict(x)
            for x in self.posted_chapters
            if datetime.fromtimestamp(x["chapter_expire"], timezone.utc)
            <= datetime.now(timezone.utc)
        ]

    def _delete_from_database(self, chapter: dict):
        """Move the chapter from the chapters table to the deleted_chapters table."""
        try:
            database_connection.execute(
                """INSERT INTO deleted_chapters SELECT * FROM chapters WHERE md_chapter_id=(?)""",
                (chapter["md_chapter_id"],),
            )
        except sqlite3.IntegrityError:
            pass
        database_connection.execute(
            """DELETE FROM chapters WHERE md_chapter_id=(?)""",
            (chapter["md_chapter_id"],),
        )
        database_connection.commit()

    def _remove_old_chapter(
        self, chapter: dict = {}, to_delete_id: Optional[str] = None
    ):
        """Check if the chapters expired and remove off mangadex if they are."""
        # If the expiry date of the chapter is less than the current time and
        # the md chapter id is available, try delete
        logging.info(f"Moving {chapter} from chapters table to deleted_chapters table.")
        md_chapter_id = chapter.get("md_chapter_id", to_delete_id)
        manga_id = chapter.get("manga_id", None)
        if manga_id is None:
            manga_id = chapter.get("md_manga_id", None)
        deleted_message = f'{md_chapter_id}: {chapter.get("chapter_id", None)}, manga {manga_id}, chapter {chapter.get("chapter_number", None)}, language {chapter.get("chapter_language", None)}.'

        if md_chapter_id is not None:
            for i in range(UPLOAD_RETRY):
                try:
                    delete_reponse = self.session.delete(
                        f"{mangadex_api_url}/chapter/{md_chapter_id}", verify=False
                    )
                except requests.RequestException:
                    continue

                if delete_reponse.status_code != 200:
                    logging.error(f"Couldn't delete expired chapter {deleted_message}")
                    print_error(delete_reponse, log_error=True)

                    if delete_reponse.status_code == 401:
                        unauthorised_message = (
                            f"You're not logged in to delete this chapter {chapter}."
                        )
                        logging.error(unauthorised_message)
                        print(unauthorised_message)

                        self.md_auth_object.login()
                        continue

                if delete_reponse.status_code == 200:
                    logging.info(f"Deleted {chapter}.")
                    print(f"----Deleted {deleted_message}")
                    MPlusBotDeleterWebhook(chapter).main()
                    setup_logs()
                    break

        if self.on_db and chapter:
            self._delete_from_database(chapter)
        time.sleep(self.chapter_delete_ratelimit)

    def _delete_expired_chapters(self):
        """Delete expired chapters from mangadex."""
        looped_all = False
        if self.chapters_to_delete:
            logging.info(f"Started deleting expired chapters process.")
            print("Deleting expired chapters.")

        for count, chapter_to_delete in enumerate(self.chapters_to_delete, start=1):
            self.md_auth_object.login()
            self._remove_old_chapter(chapter_to_delete)

            if count == len(self.chapters_to_delete):
                looped_all = True

        if looped_all:
            self.chapters_to_delete = []

    def add_more_chapters(
        self, chapters_to_add: List[dict], on_db: bool = True
    ) -> Optional["ChapterDeleterProcess"]:
        self.on_db = on_db

        if self.chapter_delete_process is None:
            second_process_object = ChapterDeleterProcess(
                session=self.session,
                posted_chapters=chapters_to_add,
                md_auth_object=self.md_auth_object,
            )
            return second_process_object
        else:
            if not self.chapter_delete_process.is_alive():
                if not self.chapters_to_delete:
                    self.chapters_to_delete = chapters_to_add
                else:
                    self.chapters_to_delete.extend(chapters_to_add)
                self.delete_async()
            else:
                self.chapters_to_delete.extend(chapters_to_add)

    def delete(self):
        """Delete chapters non-concurrently."""
        if self.chapters_to_delete:
            self.md_auth_object.login()
            self._delete_expired_chapters()

    def delete_async(self) -> multiprocessing.Process:
        """Delete chapters concurrently."""
        if self.chapters_to_delete:
            self.md_auth_object.login()

        self.chapter_delete_process = multiprocessing.Process(
            target=self._delete_expired_chapters
        )
        self.chapter_delete_process.start()
        return self.chapter_delete_process


class ChapterUploaderProcess:
    def __init__(self, **kwargs):
        self.database_connection: sqlite3.Connection = kwargs["database_connection"]
        self.session: requests.Session = kwargs["session"]
        self.mangadex_manga_id: str = kwargs["mangadex_manga_id"]
        self.chapter: Chapter = kwargs["chapter"]
        self.md_auth_object: AuthMD = kwargs["md_auth_object"]
        self.posted_md_updates: List[Chapter] = kwargs["posted_md_updates"]
        self.same_chapter_dict: Dict[str, List[int]] = kwargs["same_chapter_dict"]

        self.mplus_chapter_url = "https://mangaplus.shueisha.co.jp/viewer/{}"

        self.manga_generic_error_message = (
            f"Manga: {self.chapter.manga.manga_name}, "
            f"{self.mangadex_manga_id} - {self.chapter.manga_id}, "
            f"chapter: {self.chapter.chapter_number}, "
            f"language: {self.chapter.chapter_language}, "
            f"title: {self.chapter.chapter_title}"
        )
        self.upload_retry_total = UPLOAD_RETRY
        self.upload_session_id: Optional[str] = None

    def remove_upload_session(self, session_id: Optional[str] = None):
        """Delete the upload session."""
        if session_id is None:
            session_id = self.upload_session_id

        self.session.delete(f"{md_upload_api_url}/{session_id}", verify=False)
        logging.info(f"Sent {session_id} to be deleted.")

    def _delete_exising_upload_session(
        self, chapter_upload_session_retry: int, json_error=False
    ):
        """Remove any exising upload sessions to not error out as mangadex only allows one upload session at a time."""
        if chapter_upload_session_retry > 0 and not json_error:
            return

        logging.debug(
            f"Checking for upload sessions for manga {self.mangadex_manga_id}, chapter {self.chapter}."
        )
        for removal_retry in range(self.upload_retry_total):
            try:
                existing_session = self.session.get(
                    f"{md_upload_api_url}", verify=False
                )
            except requests.RequestException as e:
                logging.error(e)
                continue

            if existing_session.status_code == 200:
                existing_session_json = convert_json(existing_session)

                if existing_session_json is None:
                    logging.warning(
                        f"Couldn't convert exising upload session response into a json, retrying."
                    )
                else:
                    self.remove_upload_session(existing_session_json["data"]["id"])
                    return
            elif existing_session.status_code == 404:
                logging.info("No existing upload session found.")
                return
            elif existing_session.status_code == 401:
                print_error(existing_session, log_error=True)
                logging.warning("Not logged in, logging in and retrying.")
                self.md_auth_object.login()
                continue
            else:
                logging.warning(
                    f"Couldn't delete the exising upload session, retrying."
                )

            time.sleep(RATELIMIT_TIME)

        logging.error("Exising upload session not deleted.")
        raise Exception(f"Couldn't delete existing upload session.")

    def _create_upload_session(self) -> Optional[dict]:
        """Try create an upload session 3 times."""
        chapter_upload_session_successful = False
        json_error = False
        for chapter_upload_session_retry in range(self.upload_retry_total):
            if chapter_upload_session_retry == 0 or json_error:
                # Delete existing upload session if exists
                self._delete_exising_upload_session(
                    chapter_upload_session_retry, json_error
                )
                time.sleep(RATELIMIT_TIME)

            try:
                # Start the upload session
                upload_session_response = self.session.post(
                    f"{md_upload_api_url}/begin",
                    json={
                        "manga": self.mangadex_manga_id,
                        "groups": [MPLUS_GROUP_ID],
                    },
                    verify=False,
                )
            except requests.RequestException as e:
                logging.error(e)
                continue
            json_error = False

            if upload_session_response.status_code == 200:
                upload_session_response_json = convert_json(upload_session_response)

                if upload_session_response_json is not None:
                    chapter_upload_session_successful = True
                    break
                else:
                    upload_session_response_json_message = f"Couldn't convert successful upload session creation into a json, retrying. {self.manga_generic_error_message}."
                    logging.error(upload_session_response_json_message)
                    print(upload_session_response_json_message)
                    json_error = True
            elif upload_session_response.status_code == 401:
                print_error(upload_session_response, log_error=True)
                self.md_auth_object.login()
                continue
            else:
                print_error(upload_session_response, log_error=True)
                logging.error(
                    f"Couldn't create an upload session for {self.mangadex_manga_id}, chapter {self.chapter.chapter_number}."
                )
                print("Couldn't create an upload session.")

            time.sleep(RATELIMIT_TIME)

        # Couldn't create an upload session, skip the chapter
        if not chapter_upload_session_successful:
            upload_session_response_json_message = f"Couldn't create an upload session for {self.manga_generic_error_message}."
            logging.error(upload_session_response_json_message)
            print(upload_session_response_json_message)
            time.sleep(RATELIMIT_TIME)
            return

        time.sleep(RATELIMIT_TIME)
        return upload_session_response_json

    def _commit_chapter(self) -> bool:
        """Try commit the chapter to mangadex."""
        succesful_upload = False
        for commit_retries in range(self.upload_retry_total):
            try:
                chapter_commit_response = self.session.post(
                    f"{md_upload_api_url}/{self.upload_session_id}/commit",
                    json={
                        "chapterDraft": {
                            "volume": None,
                            "chapter": self.chapter.chapter_number,
                            "title": self.chapter.chapter_title,
                            "translatedLanguage": self.chapter.chapter_language,
                            "externalUrl": self.mplus_chapter_url.format(
                                self.chapter.chapter_id
                            ),
                            "publishAt": datetime.fromtimestamp(
                                self.chapter.chapter_expire, timezone.utc
                            ).strftime("%Y-%m-%dT%H:%M:%S%z"),
                        },
                        "pageOrder": [],
                    },
                    verify=False,
                )
            except requests.RequestException as e:
                logging.error(e)
                continue

            if chapter_commit_response.status_code == 200:
                succesful_upload = True
                chapter_commit_response_json = convert_json(chapter_commit_response)

                if chapter_commit_response_json is not None:
                    succesful_upload_id = chapter_commit_response_json["data"]["id"]
                    succesful_upload_message = f"Committed {succesful_upload_id} - {self.chapter.chapter_id} for {self.manga_generic_error_message}."
                    self.chapter.md_chapter_id = succesful_upload_id
                    logging.info(succesful_upload_message)
                    print(succesful_upload_message)
                    update_database(
                        self.database_connection, self.chapter, succesful_upload_id
                    )
                else:
                    chapter_commit_response_json_message = f"Couldn't convert successful chapter commit api response into a json"
                    logging.error(chapter_commit_response_json_message)
                    print(chapter_commit_response_json_message)
                return True
            elif chapter_commit_response.status_code == 401:
                print_error(chapter_commit_response, log_error=True)
                self.md_auth_object.login()
                continue
            else:
                succesful_upload = False
                logging.warning(f"Failed to commit {self.upload_session_id}, retrying.")
                print_error(chapter_commit_response, log_error=True)

            time.sleep(RATELIMIT_TIME * 2)

        if not succesful_upload:
            error_message = f"Couldn't commit {self.upload_session_id}, manga {self.mangadex_manga_id} - {self.chapter.manga_id} chapter {self.chapter.chapter_number} language {self.chapter.chapter_language}."
            logging.error(error_message)
            print(error_message)
            self.remove_upload_session()
            return False
        return succesful_upload

    def _check_for_duplicate_chapter_md_list(self, manga_chapters: List[dict]) -> bool:
        """Check for duplicate chapters on mangadex."""
        # Skip duplicate chapters
        for md_chapter in manga_chapters:
            if (
                md_chapter["attributes"]["chapter"] == self.chapter.chapter_number
                and md_chapter["attributes"]["translatedLanguage"]
                == self.chapter.chapter_language
                and md_chapter["attributes"]["externalUrl"] is not None
            ):
                dupe_chapter_message = f"{self.manga_generic_error_message} already exists on mangadex, skipping."
                logging.info(dupe_chapter_message)
                print(dupe_chapter_message)
                # Add duplicate chapter to database to avoid checking it again
                # in the future
                update_database(
                    self.database_connection, self.chapter, md_chapter["id"]
                )
                return True
        return False

    def _check_already_uploaded(self) -> bool:
        for chap in self.posted_md_updates:
            if (
                chap.chapter_id == self.chapter.chapter_id
                and chap.chapter_number == self.chapter.chapter_number
                and chap.chapter_language == self.chapter.chapter_language
            ):
                return True
        return False

    def _check_uploaded_same_ids(self, manga_chapters: List[dict]) -> bool:
        same_chapter_list_md = [
            re.sub(
                r"https\:\/\/mangaplus\.shueisha\.co\.jp\/viewer\/",
                "",
                c["attributes"]["externalUrl"],
            )
            for c in manga_chapters
            if c["attributes"]["chapter"] == self.chapter.chapter_number
            and c["attributes"]["translatedLanguage"] == self.chapter.chapter_language
        ]
        same_chapter_list_posted_ids = [
            str(c.chapter_id) for c in self.posted_md_updates
        ]

        if self.chapter.chapter_id in flatten(list(self.same_chapter_dict.values())):
            master_id = get_md_id(self.same_chapter_dict, self.chapter.chapter_id)
            if master_id is not None:
                if (
                    master_id in same_chapter_list_md
                    or master_id in same_chapter_list_posted_ids
                ):
                    return True
        return False

    def start_upload(self, manga_chapters: list) -> Literal[0, 1, 2]:
        if self._check_for_duplicate_chapter_md_list(manga_chapters):
            return 1

        if self._check_already_uploaded():
            return 1

        if self._check_uploaded_same_ids(manga_chapters):
            return 1

        upload_session_response_json = self._create_upload_session()
        if upload_session_response_json is None:
            time.sleep(RATELIMIT_TIME)
            return 2

        self.upload_session_id = upload_session_response_json["data"]["id"]
        logging.info(
            f"Created upload session: {self.upload_session_id} - {self.chapter}"
        )
        chapter_committed = self._commit_chapter()
        if not chapter_committed:
            self.remove_upload_session()
            time.sleep(RATELIMIT_TIME)
            return 2

        self.posted_md_updates.append(self.chapter)
        time.sleep(RATELIMIT_TIME)
        return 0


class MangaUploaderProcess:
    def __init__(self, **kwargs):
        self.database_connection: sqlite3.Connection = kwargs["database_connection"]
        self.session: requests.Session = kwargs["session"]
        self.chapters: List[Chapter] = kwargs["updated_manga_chapters"]
        self.chapters_all: List[Chapter] = kwargs["all_manga_chapters"]
        self.mangadex_manga_id: str = kwargs["mangadex_manga_id"]
        self.manga_chapters = kwargs["mplus_group_chapters"]
        self.deleter_process_object: Optional["ChapterDeleterProcess"] = kwargs[
            "deleter_process_object"
        ]
        self.md_auth_object: AuthMD = kwargs["md_auth_object"]
        self.posted_md_updates: List[Chapter] = kwargs["posted_md_updates"]
        self.same_chapter_dict: Dict[str, List[int]] = kwargs["same_chapter_dict"]
        self.mangadex_manga_data: dict = kwargs["mangadex_manga_data"]
        self.posted_chapters: List[Chapter] = []
        self.failed_uploads: List[Chapter] = []

        self.second_process = None
        self.second_process_object = None

        if self.manga_chapters:
            self._delete_extra_chapters()

    def _remove_chapters_not_mplus(self) -> List[dict]:
        """Find chapters on MangaDex not on MangaPlus."""
        md_chapters_not_mplus = [
            c
            for c in self.manga_chapters
            if c["attributes"]["chapter"]
            not in [x.chapter_number for x in self.chapters_all]
            or c["attributes"]["translatedLanguage"] not in mplus_language_map.values()
        ]
        chapters_to_delete = []
        for expired in md_chapters_not_mplus:
            md_chapter_id = expired["id"]

            expired_chapter_object = Chapter(
                chapter_timestamp=946684799,
                chapter_expire=946684799,
                chapter_language=expired["attributes"]["translatedLanguage"],
                chapter_title=expired["attributes"]["title"],
                chapter_number=expired["attributes"]["chapter"],
                md_manga_id=self.mangadex_manga_id,
                md_chapter_id=md_chapter_id,
            )

            update_database(
                self.database_connection, expired_chapter_object, md_chapter_id
            )
            chapters_to_delete.append(vars(expired_chapter_object))

        return chapters_to_delete

    def _delete_extra_chapters(self):
        chapters_to_delete = self._remove_chapters_not_mplus()
        if chapters_to_delete:
            if self.deleter_process_object is None:
                self.second_process_object = ChapterDeleterProcess(
                    session=self.session,
                    posted_chapters=chapters_to_delete,
                    md_auth_object=self.md_auth_object,
                )
                self.second_process_object.delete()
            else:
                self.second_process_object = (
                    self.deleter_process_object.add_more_chapters(chapters_to_delete)
                )
                if self.second_process_object is not None:
                    self.second_process_object.delete()

    def start_manga_uploading_process(self, last_manga: bool):
        self.skipped = 0
        self.skipped_chapter = False
        chapter = self.chapters[0]
        for count, chapter in enumerate(self.chapters, start=1):
            chapter: Chapter = chapter
            chapter.md_manga_id = self.mangadex_manga_id
            self.md_auth_object.login()

            chapter_to_upload_process = ChapterUploaderProcess(
                **{
                    "database_connection": self.database_connection,
                    "session": self.session,
                    "mangadex_manga_id": self.mangadex_manga_id,
                    "chapter": chapter,
                    "md_auth_object": self.md_auth_object,
                    "posted_md_updates": self.posted_md_updates,
                    "same_chapter_dict": self.same_chapter_dict,
                }
            )

            uploaded = chapter_to_upload_process.start_upload(self.manga_chapters)
            if uploaded in (1, 2):
                self.skipped += 1
                if uploaded in (1,):
                    self.skipped_chapter = True
                if uploaded in (2,):
                    self.failed_uploads.append(chapter)
                continue

            self.skipped_chapter = False
            self.posted_chapters.append(chapter)

        if self.skipped != 0:
            skipped_chapters_message = f"Skipped {self.skipped} chapters out of {len(self.chapters)} for manga {chapter.manga.manga_name}: {self.mangadex_manga_id} - {chapter.manga_id}."
            logging.info(skipped_chapters_message)
            print(skipped_chapters_message)

        MPlusBotUpdatesWebhook(
            self.mangadex_manga_data,
            self.posted_chapters,
            self.failed_uploads,
            self.skipped,
        ).main(last_manga)

        setup_logs()

        if self.second_process_object is not None:
            del self.second_process_object
        time.sleep(RATELIMIT_TIME * 2)


class BotProcess:
    def __init__(
        self,
        session: requests.Session,
        updates: List[Chapter],
        all_mplus_chapters: List[Chapter],
        deleter_process_object: Optional["ChapterDeleterProcess"],
        md_auth_object: AuthMD,
        manga_id_map: Dict[str, List[int]],
        database_connection: sqlite3.Connection,
        same_chapter_dict: Dict[str, List[int]],
        clean_db: bool,
        posted_chapters_data: List[dict],
    ):
        self.session = session
        self.updates = updates
        self.all_mplus_chapters = all_mplus_chapters
        self.deleter_process_object = deleter_process_object
        self.md_auth_object = md_auth_object
        self.manga_id_map = manga_id_map
        self.database_connection = database_connection
        self.same_chapter_dict = same_chapter_dict
        self.clean_db = clean_db
        self.posted_chapters_data = posted_chapters_data
        self.processes: List[multiprocessing.Process] = []
        self.posted_md_updates: List[Chapter] = []
        self.md_manga_objs = {}
        self.mplus_group_chapters = self._get_mplus_chapters()
        self.manga_untracked = [
            m
            for m in list(self.mplus_group_chapters.keys())
            if m not in list(self.manga_id_map.keys())
        ]

        logging.info(f"Manga not tracked but on mangadex: {self.manga_untracked}")

    def _remove_chapters_not_mplus(self) -> List[dict]:
        """Find chapters on MangaDex not on MangaPlus."""
        chapters_to_delete = []

        for manga_id in self.mplus_group_chapters:
            if manga_id in self.manga_untracked:
                for expired in self.mplus_group_chapters[manga_id]:
                    md_chapter_id = expired["id"]

                    expired_chapter_object = Chapter(
                        chapter_timestamp=946684799,
                        chapter_expire=946684799,
                        chapter_language=expired["attributes"]["translatedLanguage"],
                        chapter_title=expired["attributes"]["title"],
                        chapter_number=expired["attributes"]["chapter"],
                        md_manga_id=manga_id,
                        md_chapter_id=md_chapter_id,
                    )

                    update_database(
                        self.database_connection, expired_chapter_object, md_chapter_id
                    )
                    chapters_to_delete.append(vars(expired_chapter_object))

        return chapters_to_delete

    def _delete_extra_chapters(self):
        chapters_to_delete = self._remove_chapters_not_mplus()
        if chapters_to_delete:
            if (
                self.deleter_process_object is None
                or self.deleter_process_object.chapter_delete_process is None
            ):
                self.second_process_object = ChapterDeleterProcess(
                    session=self.session,
                    posted_chapters=chapters_to_delete,
                    md_auth_object=self.md_auth_object,
                )
                self.second_process_object.delete()
            else:
                if not self.deleter_process_object.chapter_delete_process.is_alive():
                    if not self.deleter_process_object.chapters_to_delete:
                        self.deleter_process_object.chapters_to_delete = (
                            chapters_to_delete
                        )
                    else:
                        self.deleter_process_object.chapters_to_delete.extend(
                            chapters_to_delete
                        )
                    self.deleter_process_object.delete_async()
                else:
                    self.deleter_process_object.chapters_to_delete.extend(
                        chapters_to_delete
                    )

    def _get_chapters(self, params: dict) -> List[dict]:
        """Go through each page in the api to get all the chapters."""
        chapters = []
        limit = 100
        offset = 0
        pages = 1
        iteration = 1
        created_at_since_time = "2000-01-01T00:00:00"

        parameters = {}
        parameters.update(params)

        while True:
            # Update the parameters with the new offset
            parameters.update(
                {
                    "limit": limit,
                    "offset": offset,
                    "createdAtSince": created_at_since_time,
                }
            )

            # Call the api and get the json data
            try:
                chapters_response = self.session.get(
                    f"{mangadex_api_url}/chapter", params=parameters, verify=False
                )
            except requests.RequestException as e:
                logging.error(e)
                limit = limit
                offset = offset
                created_at_since_time = created_at_since_time
                parameters = parameters
                continue

            if chapters_response.status_code != 200:
                manga_response_message = f"Couldn't get the chapters of the group."
                print_error(chapters_response, log_error=True)
                logging.error(manga_response_message)
                continue

            chapters_response_data = convert_json(chapters_response)
            if chapters_response_data is None:
                logging.warning(f"Couldn't convert chapters data into json, retrying.")
                continue

            chapters.extend(chapters_response_data["data"])
            offset += limit

            # Finds how many pages needed to be called
            if pages == 1:
                chapters_count = chapters_response_data.get("total", 0)

                if not chapters_response_data["data"]:
                    chapters_count = 0

                if chapters_count > limit:
                    pages = math.ceil(chapters_count / limit)

                logging.debug(f"{pages} page(s) for group chapters.")

            # Wait every 5 pages
            if iteration % 5 == 0 and pages != 5:
                time.sleep(RATELIMIT_TIME)

            # End the loop when all the pages have been gone through
            # Offset 10000 is the highest you can go, reset offset and get next
            # 10k batch using the last available chapter's created at date
            if (
                iteration == pages
                or offset >= 10000
                or not chapters_response_data["data"]
            ):
                if chapters_count >= 10000 and offset == 10000:
                    logging.debug("Reached 10k chapters, looping over next 10k.")
                    created_at_since_time = chapters[-1]["attributes"][
                        "createdAt"
                    ].split("+")[0]
                    offset = 0
                    pages = 1
                    iteration = 1
                    time.sleep(5)
                    continue
                break

            iteration += 1

        time.sleep(RATELIMIT_TIME)
        return chapters

    def _get_mplus_chapters(self) -> Dict[str, List[dict]]:
        logging.debug("Getting all m+'s uploaded chapters.")
        print("Getting the mangaplus chapters on mangadex.")
        chapters_unsorted = self._get_chapters(
            params={
                "groups[]": [MPLUS_GROUP_ID],
                "order[createdAt]": "desc",
                "includes[]": ["manga"],
            }
        )

        logging.debug("Sorting the uploaded chapters by MD ID.")
        chapters_sorted = {}
        for chapter in chapters_unsorted:
            manga = [g for g in chapter["relationships"] if g["type"] == "manga"][0]
            manga_id = manga["id"]

            try:
                chapters_sorted[manga_id].append(chapter)
            except (KeyError, ValueError, AttributeError):
                chapters_sorted[manga_id] = [chapter]

            if manga_id not in self.md_manga_objs:
                self.md_manga_objs.update({manga_id: manga})
        return chapters_sorted

    def _sort_chapters_by_manga(
        self, updates: List[Chapter]
    ) -> Dict[str, List[Chapter]]:
        """Sort the chapters by manga id."""
        chapters_sorted = {}

        for chapter in updates:
            md_id = get_md_id(self.manga_id_map, chapter.manga_id)
            if md_id is None:
                logging.warning(
                    f"No mangadex id found for mplus id {chapter.manga_id}."
                )
                continue

            try:
                chapters_sorted[md_id].append(chapter)
            except (KeyError, ValueError, AttributeError):
                chapters_sorted[md_id] = [chapter]
        return chapters_sorted

    def _check_all_chapters_uploaded(self):
        """Check if all the chapters uploaded to MangaDex were indexed correctly."""
        logging.info(
            "Checking if all currently uploaded chapters are available on MangaDex."
        )
        print("Checking which chapters weren't indexed.")

        uploaded_chapter_ids = [
            chapter.md_chapter_id
            for chapter in self.posted_md_updates
            if chapter.md_chapter_id is not None
        ]

        if self.clean_db:
            uploaded_chapter_ids.extend(
                [
                    chapter["md_chapter_id"]
                    for chapter in self.posted_chapters_data
                    if datetime.fromtimestamp(chapter["chapter_expire"], timezone.utc)
                    >= datetime.now(timezone.utc)
                    and chapter["md_chapter_id"] is not None
                ]
            )

        uploaded_chapter_ids = list(set(uploaded_chapter_ids))
        if uploaded_chapter_ids:
            logging.info(f"Uploaded chapters mangadex ids: {uploaded_chapter_ids}")
            chapters_on_md = []

            uploaded_chapter_ids_split = [
                uploaded_chapter_ids[l : l + 100]
                for l in range(0, len(uploaded_chapter_ids), 100)
            ]

            for uploaded_ids in uploaded_chapter_ids_split:
                chapters_on_md.extend(
                    self._get_chapters(
                        params={
                            "ids[]": uploaded_ids,
                            "order[createdAt]": "desc",
                            "includes[]": ["manga"],
                        }
                    )
                )

            chapters_not_on_md = [
                chapter["id"]
                for chapter in chapters_on_md
                if chapter["id"] not in uploaded_chapter_ids
            ]

            if chapters_not_on_md:
                logging.info(f"Chapters not indexed: {chapters_not_on_md}")
                MPlusBotNotIndexedWebhook(chapters_not_on_md).main()
                setup_logs()
        else:
            logging.info("No uploaded chapter mangadex ids.")

    def upload_chapters(self):
        """Go through each new chapter and upload it to mangadex."""
        # Sort each chapter by manga
        updated_manga_chapters = self._sort_chapters_by_manga(self.updates)
        all_manga_chapters = self._sort_chapters_by_manga(self.all_mplus_chapters)
        self._delete_extra_chapters()

        for index, mangadex_manga_id in enumerate(updated_manga_chapters, start=1):
            self.md_auth_object.login()
            manga_uploader = MangaUploaderProcess(
                **{
                    "database_connection": self.database_connection,
                    "session": self.session,
                    "updated_manga_chapters": updated_manga_chapters[mangadex_manga_id],
                    "all_manga_chapters": all_manga_chapters[mangadex_manga_id],
                    "mangadex_manga_id": mangadex_manga_id,
                    "deleter_process_object": self.deleter_process_object,
                    "md_auth_object": self.md_auth_object,
                    "mplus_group_chapters": self.mplus_group_chapters.get(
                        mangadex_manga_id, []
                    ),
                    "posted_md_updates": self.posted_md_updates,
                    "same_chapter_dict": self.same_chapter_dict,
                    "mangadex_manga_data": self.md_manga_objs.get(
                        mangadex_manga_id, {}
                    ),
                }
            )
            manga_uploader.start_manga_uploading_process(
                index == len(updated_manga_chapters)
            )

        if self.posted_md_updates or self.clean_db:
            time.sleep(RATELIMIT_TIME)
            self._check_all_chapters_uploaded()


class MPlusAPI:
    def __init__(
        self,
        manga_map_mplus_ids: List[int],
        posted_chapters_ids: List[int],
        manga_id_map: Dict[str, List[int]],
        title_regexes: dict,
    ):
        self.tracked_manga = manga_map_mplus_ids
        self.posted_chapters_ids = posted_chapters_ids
        self.manga_id_map = manga_id_map
        self.updated_chapters: List[Chapter] = []
        self.all_mplus_chapters: List[Chapter] = []
        self.untracked_manga: List[Manga] = []
        self.mplus_base_api_url = "https://jumpg-webapi.tokyo-cdn.com"
        self.title_regexes = title_regexes

        self.get_mplus_updated_manga()
        self.get_mplus_updates()

    def _get_proto_response(self, response_proto: bytes) -> response_pb.Response:
        """Convert api response into readable data."""
        response = response_pb.Response()
        response.ParseFromString(response_proto)
        return response

    def _request_from_api(
        self, manga_id: Optional[int] = None, updated: Optional[bool] = False
    ) -> Optional[bytes]:
        """Get manga and chapter details from the api."""
        try:
            if manga_id is not None:
                response = requests.get(
                    self.mplus_base_api_url + "/api/title_detail",
                    params={"title_id": manga_id},
                    stream=True,
                )
            elif updated:
                response = requests.get(
                    self.mplus_base_api_url + "/api/title_list/updated", stream=True
                )
        except Exception as e:
            logging.error(f"{e}: Couldn't get details from the mangaplus api.")
            print("Request API Error", e)
            return

        if response.status_code == 200:
            return response.content

    def get_mplus_updated_manga(self):
        """Find new untracked mangaplus series."""
        logging.info("Looking for new untracked manga.")
        print("Getting new manga.")
        updated_manga_response = self._request_from_api(updated=True)
        if updated_manga_response is not None:
            updated_manga_response_parsed = self._get_proto_response(
                updated_manga_response
            )
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
                            manga_language=language,
                        )
                    )
                    logging.info(f"Found untracked manga {manga_id}: {manga_name}.")
                    print(f"Found untracked manga {manga_id}: {manga_name}.")

    def get_mplus_updates(self):
        """Get latest chapter updates."""
        logging.info("Looking for tracked manga new chapters.")
        print("Getting new chapters.")
        processes = []

        spliced_manga = [
            self.tracked_manga[l : l + 3] for l in range(0, len(self.tracked_manga), 3)
        ]
        self.updated_chapters = multiprocessing_manager.list()
        self.all_mplus_chapters = multiprocessing_manager.list()
        for mangas in spliced_manga:
            process = multiprocessing.Process(
                target=self._chapter_updates, args=(mangas,)
            )
            process.start()
            processes.append(process)

        for process in processes:
            if process is not None:
                process.join()

    def _normalise_chapter_object(
        self, chapter_list, manga_object: Manga
    ) -> List[Chapter]:
        """Return a list of chapter objects made from the api chapter lists."""
        return [
            Chapter(
                chapter_id=chapter.chapter_id,
                chapter_timestamp=chapter.start_timestamp,
                chapter_title=chapter.chapter_name,
                chapter_expire=chapter.end_timestamp,
                chapter_number=chapter.chapter_number,
                chapter_language=manga_object.manga_language,
                manga_id=manga_object.manga_id,
                md_manga_id=get_md_id(self.manga_id_map, manga_object.manga_id),
                manga=manga_object,
            )
            for chapter in chapter_list
        ]

    def _chapter_updates(self, mangas: list):
        """Get the updated chapters from each manga."""
        for manga in mangas:
            manga_response = self._request_from_api(manga_id=manga)
            if manga_response is None:
                continue

            manga_response_parsed = self._get_proto_response(manga_response)

            manga_chapters = manga_response_parsed.success.manga_detail
            manga_object = Manga(
                manga_id=manga_chapters.manga.manga_id,
                manga_name=manga_chapters.manga.manga_name,
                manga_language=manga_chapters.manga.language,
            )

            manga_chapters_lists = []
            manga_chapters_lists.append(
                self._normalise_chapter_object(
                    list(manga_chapters.first_chapter_list), manga_object
                )
            )

            if len(manga_chapters.last_chapter_list) > 0:
                manga_chapters_lists.append(
                    self._normalise_chapter_object(
                        list(manga_chapters.last_chapter_list), manga_object
                    )
                )

            all_chapters = self.get_latest_chapters(
                manga_chapters_lists, self.posted_chapters_ids, True
            )
            self.all_mplus_chapters.extend(all_chapters)

            updated_chapters = self.get_latest_chapters(
                manga_chapters_lists, self.posted_chapters_ids
            )
            logging.info(f"MangaPlus newly updated chapters: {updated_chapters}")

            if updated_chapters:
                print(f"Manga {manga_object.manga_name}: {manga_object.manga_id}.")
                for update in updated_chapters:
                    print(
                        f"--Found {update.chapter_id}, chapter: {update.chapter_number}, language: {update.chapter_language}, title: {update.chapter_title}."
                    )

            self.updated_chapters.extend(
                [
                    chapter
                    for chapter in updated_chapters
                    if chapter.chapter_id not in self.posted_chapters_ids
                    and datetime.fromtimestamp(chapter.chapter_expire, timezone.utc)
                    >= datetime.now(timezone.utc)
                ]
            )

    def _get_surrounding_chapter(
        self,
        chapters: List[Chapter],
        current_chapter: Chapter,
        next_chapter_search: bool = False,
    ) -> Optional[Chapter]:
        """Find the chapter before or after the current."""
        # Starts from the first chapter before the current
        index_search = reversed(chapters[: chapters.index(current_chapter)])
        if next_chapter_search:
            # Starts from the first chapter after the current
            index_search = chapters[chapters.index(current_chapter) :]

        for chapter in index_search:
            number_match = re.match(
                pattern=r"^#?(\d+)", string=chapter.chapter_number, flags=re.I
            )

            if bool(number_match):
                number = number_match.group(1)
            else:
                number = re.split(
                    r"[\s{}]+".format(re.escape(string.punctuation)),
                    chapter.chapter_number.strip("#"),
                )[0]

            try:
                int(number)
            except ValueError:
                continue
            else:
                return chapter

    def _strip_chapter_number(self, number: Union[str, int]) -> str:
        """Returns the chapter number without the un-needed # or 0."""
        stripped = str(number).strip("#")

        parts = re.split(r"\.|\-", stripped)
        parts[0] = "0" if len(parts[0].lstrip("0")) == 0 else parts[0].lstrip("0")
        stripped = ".".join(parts)

        return stripped

    def _normalise_chapter_number(
        self, chapters: List[Chapter], chapter: Chapter
    ) -> List[Optional[str]]:
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
                    chapters, chapter, next_chapter_search=True
                )
                if next_chapter is None:
                    chapter_number = None
                else:
                    next_chapter_number = self._strip_chapter_number(
                        next_chapter.chapter_number
                    )
                    chapter_number = (
                        int(re.split(r"\.|\-|\,", next_chapter_number)[0]) - 1
                    )
                    first_index = next_chapter
                    second_index = chapter
            else:
                previous_chapter_number = self._strip_chapter_number(
                    previous_chapter.chapter_number
                )
                if "," in previous_chapter_number:
                    chapter_number = previous_chapter_number.split(",")[-1]
                else:
                    chapter_number = re.split(r"\.|\-", previous_chapter_number)[0]
                first_index = chapter
                second_index = previous_chapter

            if chapter_number == "ex":
                chapter_number = None

            if chapter_number is not None and current_number != "ex":
                # If difference between current chapter and previous/next
                # chapter is more than 5, use None as chapter_number
                if math.sqrt((int(current_number) - int(chapter_number)) ** 2) >= 5:
                    chapter_number = None

            if chapter_number is not None:
                chapter_decimal = "5"

                # There may be multiple extra chapters before the last numbered chapter
                # Use index difference as decimal to avoid not uploading
                # non-dupes
                try:
                    chapter_difference = chapters.index(first_index) - chapters.index(
                        second_index
                    )
                    if chapter_difference > 1:
                        chapter_decimal = chapter_difference
                except (ValueError, IndexError):
                    pass

                chapter_number = f"{chapter_number}.{chapter_decimal}"
        elif chapter_number.lower() in ("one-shot", "one.shot"):
            chapter_number = None

        if chapter_number is None:
            chapter_number_split = [chapter_number]
        else:
            chapter_number_split = [
                self._strip_chapter_number(chap_number)
                for chap_number in chapter_number.split(",")
            ]

        chapter_number_split: List[Optional[str]] = chapter_number_split
        return chapter_number_split

    def _normalise_chapter_title(
        self, chapter: Chapter, chapter_number: List[Optional[str]]
    ) -> Optional[str]:
        """Strip away the title prefix."""
        colon_regex = re.compile(r"^.+:\s?", re.I)
        no_title_regex = re.compile(r"^\S+\s?\d+(?:(?:\,|\-)\d{0,2})?$", re.I)
        hashtag_regex = re.compile(r"^(?:\S+\s?)?#\d+(?:(?:\,|\-)\d{0,2})?\s?", re.I)
        period_regex = re.compile(
            r"^(?:\S+\s?)?\d+(?:(?:\,|\-)\d{0,2})?\s?[\.\/\-]\s?", re.I
        )
        spaces_regex = re.compile(r"^(?:\S+\s?)?\d+(?:(?:\,|\-)\d{0,2})?\s?", re.I)

        title = str(chapter.chapter_title).strip()
        normalised_title = title
        pattern_to_use: Optional[re.Pattern[str]] = None
        replace_string = ""

        if (
            chapter.manga_id in self.title_regexes.get("empty", [])
            and None not in chapter_number
            or title.lower() in ("final chapter",)
        ):
            normalised_title = None
        elif chapter.manga_id in self.title_regexes.get("noformat", []):
            normalised_title = title
        elif str(chapter.manga_id) in self.title_regexes.get("custom", {}):
            pattern_to_use = re.compile(
                self.title_regexes["custom"][str(chapter.manga_id)], re.I
            )
        elif ":" in title:
            pattern_to_use = colon_regex
        elif no_title_regex.match(title):
            pattern_to_use = no_title_regex
        elif period_regex.match(title):
            pattern_to_use = period_regex
        elif hashtag_regex.match(title):
            pattern_to_use = hashtag_regex
        elif spaces_regex.match(title):
            pattern_to_use = spaces_regex

        if pattern_to_use is not None:
            normalised_title = pattern_to_use.sub(
                repl=replace_string, string=title, count=1
            ).strip()

        if normalised_title == "":
            normalised_title = None
        return normalised_title

    def get_latest_chapters(
        self,
        manga_chapters_lists: List[List[Chapter]],
        posted_chapters: List[int],
        all_chapters: bool = False,
    ) -> List[Chapter]:
        """Get the latest un-uploaded chapters."""
        updated_chapters = []

        for chapters in manga_chapters_lists:
            # Go through the last three chapters
            for chapter in chapters:
                if not all_chapters:
                    # Chapter id is not in database or chapter expiry isn't
                    # before atm
                    if chapter.chapter_id in posted_chapters or datetime.fromtimestamp(
                        chapter.chapter_expire, timezone.utc
                    ) <= datetime.now(timezone.utc):
                        continue

                chapter_number_split = self._normalise_chapter_number(chapters, chapter)

                chapter_title = self._normalise_chapter_title(
                    chapter, chapter_number_split
                )

                # MPlus sometimes joins two chapters as one, upload to md as
                # two different chapters
                for chap_number in chapter_number_split:
                    changes = {
                        "chapter_number": chap_number,
                        "chapter_title": chapter_title,
                    }
                    chapter_object: Chapter = replace(chapter, **changes)
                    updated_chapters.append(chapter_object)

        return updated_chapters


class DeleteDuplicatesMD:
    def __init__(
        self,
        session: requests.Session,
        manga_id_map: Dict[str, List[int]],
        first_process_object: "ChapterDeleterProcess",
    ) -> None:
        self.session = session
        self.manga_id_map = manga_id_map
        self.first_process_object = first_process_object
        self.tracked_mangadex_ids = list(manga_id_map.keys())
        self.languages = list(set(mplus_language_map.values()))
        self.to_delete = []

    def get_aggregate(self, manga_id: str, language: str) -> Optional[dict]:
        logging.info(
            f"Getting aggregate info for manga {manga_id} in language {language}."
        )

        for i in range(UPLOAD_RETRY):
            try:
                aggregate_response = self.session.get(
                    f"{mangadex_api_url}/manga/{manga_id}/aggregate",
                    params={
                        "translatedLanguage[]": [language],
                        "groups[]": [MPLUS_GROUP_ID],
                    },
                    verify=False,
                )
            except requests.RequestException as e:
                logging.error(e)
                continue

            if aggregate_response.status_code in range(200, 300):
                aggregate_response_json = convert_json(aggregate_response)
                if aggregate_response_json is not None:
                    return aggregate_response_json["volumes"]

        error = print_error(aggregate_response)
        logging.error(
            f"Error returned from aggregate response for manga {manga_id}: {error}"
        )

    def check_count(self, aggregate_chapters: dict) -> List[dict]:
        to_check = []
        for volume in aggregate_chapters:
            if isinstance(aggregate_chapters, dict):
                volume_itter = aggregate_chapters[volume]["chapters"]
            elif isinstance(aggregate_chapters, list):
                volume_itter = volume["chapters"]
                for chapter in volume["chapters"]:
                    if chapter["count"] > 1:
                        to_check.append(chapter)

            for chapter in volume_itter:
                if isinstance(chapter, str):
                    chapter_itter = aggregate_chapters[volume]["chapters"][chapter]
                elif isinstance(chapter, (list, dict)):
                    chapter_itter = chapter

                if chapter_itter["count"] > 1:
                    to_check.append(chapter_itter)
        return to_check

    def get_chapters(self, chapters: List[str]) -> Optional[List[dict]]:
        logging.debug(f"Getting chapter data for chapter ids: {chapters}")
        for i in range(UPLOAD_RETRY):
            try:
                chapters_response = self.session.get(
                    f"{mangadex_api_url}/chapter",
                    params={"ids[]": chapters, "limit": 100, "includes[]": ["manga"]},
                    verify=False,
                )
            except requests.RequestException as e:
                logging.error(e)
                continue

            if chapters_response.status_code in range(200, 300):
                chapters_response_json = convert_json(chapters_response)
                if chapters_response_json is not None:
                    return chapters_response_json["data"]

            error = print_error(chapters_response, log_error=True)

    def sort_manga_data(self, chapters: list):
        manga_data = {}
        for chapter in chapters:
            manga = [m for m in chapter["relationships"] if m["type"] == "manga"][0]
            manga_id = manga["id"]

            if manga_id not in manga_data:
                manga_data({manga_id: manga})
        return manga_data

    def filter_group(self, chapter: dict) -> List[str]:
        return [
            g["id"] for g in chapter["relationships"] if g["type"] == "scanlation_group"
        ]

    def check_chapters(
        self, chapters: List[dict], dupes_webhook: "MPlusBotDupesWebhook"
    ) -> List[dict]:
        to_return_ids = []
        to_check = []

        for chapter in chapters[1:]:
            current_index = chapters.index(chapter)
            previous_chapter = chapters[current_index - 1]

            current_attributes = chapter["attributes"]
            current_groups = self.filter_group(chapter)

            previous_attributes = previous_chapter["attributes"]
            previous_groups = self.filter_group(previous_chapter)

            if MPLUS_GROUP_ID in current_groups and MPLUS_GROUP_ID in previous_groups:
                if (
                    current_attributes["translatedLanguage"]
                    == previous_attributes["translatedLanguage"]
                    and current_attributes["chapter"] == previous_attributes["chapter"]
                    and current_attributes["externalUrl"]
                    == previous_attributes["externalUrl"]
                ):
                    if chapter not in to_check:
                        to_check.append(chapter)

                    if previous_chapter not in to_check:
                        to_check.append(previous_chapter)

        if to_check:
            oldest = to_check[0]
            for chapter in to_check:
                if datetime.strptime(
                    chapter["attributes"]["createdAt"], "%Y-%m-%dT%H:%M:%S%z"
                ) < datetime.strptime(
                    oldest["attributes"]["createdAt"], "%Y-%m-%dT%H:%M:%S%z"
                ):
                    oldest = chapter

            oldest_id = oldest["id"]
            to_return_ids = [c["id"] for c in to_check]
            to_return_ids.remove(oldest_id)
            to_check.remove(oldest)

            if to_return_ids:
                dupes_webhook.add_chapters(oldest, to_check)
                print(f"Found dupes of {oldest_id} to delete: {to_return_ids}")
                logging.info(f"Found dupes of {oldest_id} to delete: {to_return_ids}")
        return to_check

    def delete_dupes(self):
        print("Looking for chapter dupes.")

        for mang_index, manga_id in enumerate(self.tracked_mangadex_ids, start=1):
            dupes_webhook = MPlusBotDupesWebhook()

            for lang_index, language in enumerate(self.languages, start=1):
                aggregate_chapters_unchecked = self.get_aggregate(manga_id, language)
                if aggregate_chapters_unchecked is None:
                    continue

                aggregate_chapters = self.check_count(aggregate_chapters_unchecked)
                logging.debug(
                    f"Aggregate chapters with more than one count: {aggregate_chapters}"
                )
                for chapter in aggregate_chapters:
                    chapters_ids = []
                    chapters_ids.append(chapter["id"])
                    chapters_ids.extend(chapter["others"])

                    chapters_md = self.get_chapters(chapters_ids)
                    if chapters_md is None:
                        continue

                    if bool(dupes_webhook.manga):
                        manga_data = self.sort_manga_data(chapters_md)
                        dupes_webhook.init_manga(manga_data)

                    chapters_to_delete = self.check_chapters(chapters_md, dupes_webhook)
                    chapters_to_delete_dict: Dict[str, str] = [
                        {
                            "md_chapter_id": c["id"],
                            "md_manga_id": manga_id,
                            "chapter_language": c["attributes"]["translatedLanguage"],
                            "chapter_number": c["attributes"]["chapter"],
                            "chapter_timestamp": 946684799,
                            "chapter_expire": 946684799,
                        }
                        for c in chapters_to_delete
                    ]

                    self.first_process_object.add_more_chapters(chapters_to_delete_dict)
                if lang_index % 4 == 0:
                    time.sleep(RATELIMIT_TIME)

            dupes_webhook.main()
            setup_logs()

            if mang_index % 4 == 0:
                time.sleep(RATELIMIT_TIME)

        print("Finished looking for chapter dupes.")


def main(db_connection: Optional[sqlite3.Connection] = None, clean_db=False):
    """Main function for getting the updates."""
    setup_logs()
    manga_id_map = open_manga_id_map(
        components_path.joinpath(config["Paths"]["manga_id_map_path"])
    )
    title_regexes = open_title_regex(
        components_path.joinpath(config["Paths"]["title_regex_path"])
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
    logging.info(
        "Retrieved posted chapters from database and got mangaplus ids from manga id map file."
    )

    session = requests.Session()
    session.headers.update({"User-Agent": f"MP_MD_bot/{__version__}"})
    md_auth_object = AuthMD(session, config)

    # Start deleting expired chapters
    first_process_object = ChapterDeleterProcess(
        session=session,
        posted_chapters=[dict(k) for k in posted_chapters_data],
        md_auth_object=md_auth_object,
    )
    first_process = first_process_object.delete_async()

    # Get new manga and chapter updates
    mplus_api = MPlusAPI(
        manga_map_mplus_ids, posted_chapters_ids, manga_id_map, title_regexes
    )
    # updated_manga = mplus_api.untracked_manga
    updates = mplus_api.updated_chapters
    all_mplus_chapters = mplus_api.all_mplus_chapters

    if not updates:
        logging.info("No new updates found.")
        print("No new updates found.")
    else:
        logging.info(f"Found {len(updates)} update(s).")
        print(f"Found {len(updates)} update(s).")
        while True:
            try:
                BotProcess(
                    session,
                    updates,
                    all_mplus_chapters,
                    first_process_object,
                    md_auth_object,
                    manga_id_map,
                    database_connection,
                    title_regexes.get("same", {}),
                    clean_db,
                    posted_chapters_data,
                ).upload_chapters()
            except (requests.RequestException, sqlite3.OperationalError) as e:
                logging.error(e)
                continue
            else:
                break
        print("Uploaded all update(s).")

    if clean_db:
        try:
            dupes_deleter = DeleteDuplicatesMD(
                session, manga_id_map, first_process_object
            )
            dupes_deleter.delete_dupes()
        except (UnboundLocalError, TypeError, IndexError, KeyError, ValueError):
            pass

    if first_process is not None:
        first_process.join()
        print("Finished deleting expired chapters.")

    # Save and close database
    database_connection.commit()
    backup_database_connection, _ = open_database(
        components_path.joinpath(database_name).with_suffix(".bak")
    )
    database_connection.backup(backup_database_connection)
    backup_database_connection.close()
    database_connection.close()
    logging.info("Saved and closed database.")


if __name__ == "__main__":

    multiprocessing_manager = multiprocessing.Manager()
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

    if WEBHOOK.embeds:
        WEBHOOK.execute(remove_embeds=True)
