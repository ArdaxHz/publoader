import configparser
import json
import logging
import math
import multiprocessing
import sqlite3
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Dict, Optional, List
from uuid import UUID

import requests

import proto.response_pb2 as response_pb


mplus_base_api_url = "https://jumpg-webapi.tokyo-cdn.com"
mangaplus_chapter_url = 'https://mangaplus.shueisha.co.jp/viewer/{}'
http_error_codes = {"400": "Bad request.", "401": "Unauthorised.", "403": "Forbidden.", "404": "Not found.", "429": "Too many requests."}
md_upload_api_url = 'https://api.mangadex.org/upload'
md_auth_api_url = 'https://api.mangadex.org/auth'
mplus_language_map = {'0': 'en', '1': 'es-la', '2': 'fr', '3': 'id', '4': 'pt-br', '5': 'ru', '6': 'th'}
mplus_group = '4f1de6a2-f0c5-4ac5-bce5-02c7dbb67deb'
logging.basicConfig(filename='mplus_md_uploader.log', encoding='utf-8', level=logging.DEBUG,
    format='%(asctime)s,%(msecs)d %(levelname)-8s [%(filename)s:%(lineno)d] %(message)s', datefmt='%Y-%m-%d:%H:%M:%S')


MPLUS_LANGUAGE_ID_MAP = """
0: English
1: Spanish
2: French
3: Indonesian
4: Portuguese
5: Russian
6: Thai
"""

if sys.version_info < (3, 9):
    print("This program can only run Python 3.9 or higher.")
    sys.exit()

root_path = Path('.')
database_path = root_path.joinpath('chapters').with_suffix('.db')
config_file_path = root_path.joinpath('config').with_suffix('.ini')

database_connection = database_connection = sqlite3.connect(database_path)
database_connection.row_factory = sqlite3.Row
logging.info('Opened database.')

# Open config file and read values
config = configparser.ConfigParser()
config.read(config_file_path)

try:
    mangadex_ratelimit_time = int(config["User Set"]["mangadex_ratelimit_time"])
except ValueError:
    mangadex_ratelimit_time = 3



@dataclass(order=True)
class Manga:
    manga_id:int
    manga_name:str
    manga_language:int



@dataclass()
class Chapter:
    chapter_id:int
    chapter_timestamp:int
    chapter_expire:int
    chapter_title:str
    chapter_number:str
    chapter_language: int
    manga_id:int = field(default=None)


def make_tables(database_connection: sqlite3.Connection):
    """Make the database table."""
    logging.warning("Creating new tables for database.")
    database_connection.execute('''CREATE TABLE IF NOT EXISTS chapters
                (chapter_id         INTEGER NOT NULL PRIMARY KEY,
                timestamp           INTEGER NOT NULL,
                chapter_expire      INTEGER NOT NULL,
                chapter_language    INTEGER NOT NULL,
                chapter_title       TEXT NULL,
                chapter_number      TEXT NOT NULL,
                mplus_manga_id      INTEGER NOT NULL,
                md_chapter_id       TEXT NULL)''')
    database_connection.execute('''CREATE TABLE IF NOT EXISTS deleted_chapters
                (chapter_id         INTEGER NOT NULL PRIMARY KEY,
                timestamp           INTEGER NOT NULL,
                chapter_expire      INTEGER NOT NULL,
                chapter_language    INTEGER NOT NULL,
                chapter_title       TEXT NULL,
                chapter_number      TEXT NOT NULL,
                mplus_manga_id      INTEGER NOT NULL,
                md_chapter_id       TEXT NULL)''')
    database_connection.commit()


def check_table_exists(database_connection: sqlite3.Connection) -> bool:
    """Check if the table exists."""
    table_exist = database_connection.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='chapters'")

    fill_backlog = False
    # Table doesn't exist, fill backlog without posting to mangadex
    if not table_exist.fetchall():
        logging.error("Database table doesn't exist, making new one.")
        print("Table doesn't exist, making new one.")
        make_tables(database_connection)
        fill_backlog = True
    return fill_backlog

fill_backlog = check_table_exists(database_connection)


def convert_json(response_to_convert: requests.Response) -> Optional[Dict]:
    """Convert the api response into a parsable json."""
    critical_decode_error_message = "Couldn't convert mangadex api response into a json."
    try:
        converted_response = response_to_convert.json()
    except json.JSONDecodeError:
        logging.critical(critical_decode_error_message)        
        print(critical_decode_error_message)
        return
    except AttributeError:
        logging.critical(f"Api response doesn't have load as json method, trying to load as json manually.")
        try:
            converted_response = json.loads(response_to_convert.content)
        except json.JSONDecodeError:
            logging.critical(critical_decode_error_message)        
            print(critical_decode_error_message)
            return

    logging.info("Convert api response into json.")
    return converted_response


def print_error(error_response: requests.Response):
    """Print the errors the site returns."""
    error_converting_json_log_message = "{} when converting error_response into json."
    error_converting_json_print_message = f"{error_response.status_code}: Couldn't convert api reposnse into json."

    # Api didn't return json object
    try:
        error_json = error_response.json()
    except json.JSONDecodeError as e:
        logging.warning(error_converting_json_log_message.format(e))
        print(error_converting_json_print_message)
        return
    # Maybe already a json object
    except AttributeError:
        logging.warning(f"error_response is already a json.")
        # Try load as a json object
        try:
            error_json = json.loads(error_response.content)
        except json.JSONDecodeError as e:
            logging.warning(error_converting_json_log_message.format(e))
            print(error_converting_json_print_message)
            return

    # Api response doesn't follow the normal api error format
    try:
        errors = [f'{e["status"]}: {e["detail"] if e["detail"] is not None else ""}' for e in error_json["errors"]]
        errors = ', '.join(errors)

        if not errors:
            errors = http_error_codes.get(str(error_response.status_code), '')

        logging.warning(f'Error: {errors}.')
        print(f'Error: {errors}.')
    except KeyError:
        keyerror_message = f'KeyError: {error_response.status_code}.'
        logging.warning(keyerror_message)
        print(keyerror_message)


def remove_upload_session(session: requests.Session, upload_session_id: str):
    """Delete the upload session."""
    session.delete(f'{md_upload_api_url}/{upload_session_id}')
    logging.info(f'Sent {upload_session_id} to be deleted.')


def delete_exising_upload_session(session: requests.Session):
    """Remove any exising upload sessions to not error out as mangadex only allows one upload session at a time."""
    removal_retry = 0
    while removal_retry < upload_retry_total:
        existing_session = session.get(f'{md_upload_api_url}')
        if existing_session.status_code == 200:
            existing_session_json = convert_json(existing_session)
            if existing_session_json is None:
                removal_retry += 1
                logging.warning(f"Couldn't convert exising upload session response into a json, retrying.")
            else:
                remove_upload_session(session, existing_session_json["data"]["id"])
                return
        elif existing_session.status_code == 404:
            logging.info("No existing upload session found.")
            return
        elif existing_session.status_code == 401:
            logging.info("Not logged in, logging in and retrying.")
            login_to_md(session, config)
            removal_retry += 1
        else:
            removal_retry += 1
            logging.warning(f"Couldn't delete the exising upload session, retrying.")

        time.sleep(mangadex_ratelimit_time)

    logging.error("Exising upload session not deleted.")


def get_md_id(manga_id_map: Dict[str, List[int]], mangaplus_id: int) -> str:
    """Get the mangadex id from the mangaplus one."""
    for md_id in manga_id_map:
        if mangaplus_id in manga_id_map[md_id]:
            return md_id


def get_chapters(session: requests.Session, **params) -> list:
    """Go through each page in the api to get all the chapters."""
    chapters = []
    limit = 100
    offset = 0
    pages = 1
    iteration = 1

    parameters = {}
    parameters.update(params)

    while True:
        # Update the parameters with the new offset
        parameters.update({
            "limit": limit,
            "offset": offset
        })

        # Call the api and get the json data
        chapters_response = session.get(f'https://api.mangadex.org/chapter', params=parameters)
        if chapters_response.status_code != 200:
            print_error(chapters_response)
            manga_response_message = f"Couldn't get chapters of manga {params['manga']}."
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
            chapters_count = chapters_response_data.get('total', 0)

            if not chapters_response_data["data"]:
                chapters_count = 0

            if chapters_count > limit:
                pages = math.ceil(chapters_count / limit)

            if chapters_count >= 10000:
                print('Due to api limits, a maximum of 10000 chapters can be downloaded.')

            logging.info(f"{pages} page(s) for manga {params['manga']}.")

        # Wait every 5 pages
        if iteration % 5 == 0 and pages != 5:
            time.sleep(5)

        # End the loop when all the pages have been gone through
        # Offset 10000 is the highest you can go, any higher returns an error
        if iteration == pages or offset == 10000 or not chapters_response_data["data"]:
            break

        iteration += 1

    print('Finished going through the pages.')
    time.sleep(mangadex_ratelimit_time)
    return chapters


def get_previous_chapter(chapters: list, current_chapter):
    """Find the previous chapter to the current."""
    chapters = list(chapters)
    for chapter in reversed(chapters[:chapters.index(current_chapter)]):
        try:
            int(chapter.chapter_number.strip('#'))
        except ValueError:
            continue
        else:
            return chapter


def get_latest_chapters(manga_response: response_pb.Response, posted_chapters: List[int], last_run: int) -> List[Chapter]:
    """Get the latest unuploaded chapters."""
    manga_chapters = manga_response.success.manga_detail
    updated_chapters = []

    if len(manga_chapters.last_chapter_list) > 0:
        chapters = manga_chapters.last_chapter_list
    else:
        chapters = manga_chapters.first_chapter_list

    # Go through the last three chapters
    for chapter in chapters:
        # Chapter id is not in database and chapter release isn't before last run time
        chapter_timestamp = datetime.fromtimestamp(chapter.start_timestamp)
        if chapter.chapter_id not in posted_chapters and datetime.fromtimestamp(chapter.start_timestamp) <= datetime.now() and datetime.fromtimestamp(chapter.end_timestamp) >= datetime.now():
            previous_chapter = get_previous_chapter(chapters, chapter)
            chapter_number = chapter.chapter_number
            if chapter_number is not None:
                chapter_number = chapter.chapter_number.strip('#')
            if chapter_number == "ex":
                if previous_chapter is None:
                    continue

                chapter_decimal = '5'
                previous_chapter_number = str(previous_chapter.chapter_number)

                # There may be multiple extra chapters before the last numbered chapter
                # Use index difference as decimal to avoid not uploading non-dupes
                try:
                    chapter_difference = list(chapters).index(chapter) - list(chapters).index(previous_chapter)
                    if chapter_difference > 1:
                        chapter_decimal = chapter_difference
                except (ValueError, IndexError):
                    pass

                chapter_number = f"{previous_chapter_number.lstrip('#').lstrip('0')}.{chapter_decimal}"
            elif chapter_number == "One-Shot":
                chapter_number = None

            chapter_number_split = chapter_number.split(',')
            chapter_number = chapter_number_split[-1]
            chapter_number = str(chapter_number.lstrip('#')).lstrip('0')

            updated_chapters.append(Chapter(chapter_id=chapter.chapter_id, chapter_timestamp=chapter.start_timestamp,
                chapter_title=chapter.chapter_name, chapter_expire=chapter.end_timestamp, chapter_number=chapter_number,
                chapter_language=mplus_language_map.get(str(manga_chapters.manga.language), "NULL"), manga_id=manga_chapters.manga.manga_id))

    return updated_chapters


def get_updated_manga(updated_manga_response_parsed: response_pb.Response, manga_re_edtion_ids: List[int]) -> List[Manga]:
    """return new found manga ids"""
    updated_manga_details = updated_manga_response_parsed.success.updated
    updated_manga = []

    for manga in updated_manga_details.updated_manga_detail:
        if manga.updated_manga.manga_id not in manga_re_edtion_ids:
            manga_id = manga.updated_manga.manga_id
            manga_name = manga.updated_manga.manga_name
            language = manga.updated_manga.language
            updated_manga.append(Manga(manga_id=manga_id, manga_name=manga_name, manga_language=language))
            logging.info(f"Found untracked manga {manga_id}: {manga_name}.")
            print(f"Found untracked manga {manga_id}: {manga_name}.")

    return updated_manga


def get_proto_response(response_proto: bytes) -> response_pb.Response:
    """Convert api response into readable data."""
    response = response_pb.Response()
    response.ParseFromString(response_proto)
    return response


def request_from_api(manga_id: Optional[int]=None, updated: bool=False) -> Optional[bytes]:
    """Get manga and chapter details from the api."""
    try:
        if manga_id is not None:
            response = requests.get(mplus_base_api_url+"/api/title_detail", params={
                                    'lang': 'eng', 'title_id': manga_id}, stream=True)
        elif updated:
            response = requests.get(
                mplus_base_api_url+"/api/title_list/updated", params={'lang': 'eng'}, stream=True)
    except Exception as e:
        logging.error(f"{e}: Couldn't get details from the mangaplus api.")
        print("Request API Error", e)

    if response.status_code == 200:
        return response.content
    return


def delete_from_database(chapter: dict):
    """Move the chapter from the chapters table to the deleted_chapters table."""
    database_connection.execute("""INSERT INTO deleted_chapters SELECT * FROM chapters WHERE chapter_id=(?)""", (chapter["chapter_id"],))
    database_connection.execute("""DELETE FROM chapters WHERE chapter_id=(?)""", (chapter["chapter_id"],))
    database_connection.commit()


def remove_old_chapters(session: requests.Session, chapter: Dict[int, Optional[str]]):
    """Check if the chapters expired and remove off mangadex if they are."""
    # If the expiry date of the chapter is less than the current time and the md chapter id is available, try delete
    if datetime.fromtimestamp(chapter["chapter_expire"]) <= datetime.now():
        logging.info(f'Moving {chapter} from chapters table to deleted_chapters table.')

        if chapter["md_chapter_id"] is not None:
            logging.info(f'{chapter["md_chapter_id"]} expired, deleting.')
            delete_reponse = session.delete(f'https://api.mangadex.org/chapter/{chapter["md_chapter_id"]}')
            if delete_reponse.status_code == 404:
                notfound_message = f"{chapter['md_chapter_id']} already deleted."
                logging.info(notfound_message)
                print(notfound_message)
            elif delete_reponse.status_code == 403:
                unauthorised_message = f"You're not authorised to delete {chapter['md_chapter_id']}."
                logging.info(unauthorised_message)
                print(unauthorised_message)
            elif delete_reponse.status_code != 200:
                logging.warning(f"Couldn't delete expired chapter {chapter['md_chapter_id']}.")
                print_error(delete_reponse)
                time.sleep(mangadex_ratelimit_time*2)
                return

            if delete_reponse.status_code == 200:
                delete_chapter_message = f'Deleted {chapter["md_chapter_id"]}.'
                logging.info(delete_chapter_message)
                print(delete_chapter_message)

        delete_from_database(chapter)
        time.sleep(mangadex_ratelimit_time*2)
        return


def delete_expired_chapters(posted_chapters: List[Dict[str, int]], session: requests.Session):
    """Delete expired chapters from mangadex."""
    chapter_delete_processes = []
    logging.info(f'Started deleting exired chapters process.')
    print('Deleting expired chapters.')
    for chapter_to_delete in posted_chapters:
        process = remove_old_chapters(session, dict(chapter_to_delete))
        chapter_delete_processes.append(process)


def get_mplus_updated_manga(tracked_manga: List[int]) -> List[Manga]:
    """Find new untracked mangaplus series."""
    updates = []

    logging.info('Looking for new untracked manga.')
    print('Getting new manga.')
    updated_manga_response = request_from_api(updated=True)
    if updated_manga_response is not None:
        updated_manga_response_parsed = get_proto_response(updated_manga_response)
        updated_manga = get_updated_manga(updated_manga_response_parsed, tracked_manga)
        updates.extend(updated_manga)
    return updates


def get_mplus_updates(manga_series: List[int], posted_chapters_ids: List[int], last_run: int) -> List[Chapter]:
    """Get latest chapter updates."""
    updates = []

    logging.info('Looking for tracked manga new chapters.')
    print('Getting new chapters.')
    for manga in manga_series:
        manga_response = request_from_api(manga_id=manga)
        if manga_response is not None:
            manga_response_parsed = get_proto_response(manga_response)
            updated_chapters = get_latest_chapters(manga_response_parsed, posted_chapters_ids, last_run)
            logging.info(updated_chapters)
            # print(updated_chapters)
            updates.extend(updated_chapters)
    return updates


def login_to_md(session: requests.Session, config: Dict[str, Dict[str, str]]):
    """Login to MangaDex using the credentials found in the env file."""
    username = config["MangaDex Credentials"]["mangadex_username"]
    password = config["MangaDex Credentials"]["mangadex_password"]

    print(username, password)

    if username == '' or password == '':
        critical_message = 'Login details missing.'
        logging.critical(critical_message)
        raise Exception(critical_message)

    login_response = session.post(f'{md_auth_api_url}/login', json={"username": username, "password": password})

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
    


def check_logged_in(session: requests.Session, config: Dict[str, Dict[str, str]]):
    """Check if still logged into mangadex."""
    auth_check_response = session.get(f'{md_auth_api_url}/check')

    if auth_check_response.status_code != 200:
        logging.warning(f"Checking if logged in returned {auth_check_response.status_code}.")
        print_error(auth_check_response)
        return

    auth_data = convert_json(auth_check_response)
    if auth_data is None:
        return

    if auth_data["isAuthenticated"]:
        return

    logging.info('Login token expired, logging in again.')
    login_to_md(session, config)


def update_database(database_connection: sqlite3.Connection, chapter: Chapter, succesful_upload_id: Optional[str]=None):
    """Update the database with the new chapter."""
    print('Updating database.')
    chapter_id = chapter.chapter_id

    chapter_id_exists = database_connection.execute('SELECT * FROM chapters WHERE EXISTS(SELECT 1 FROM chapters WHERE chapter_id=(?))', (chapter_id,))
    chapter_id_exists_dict = chapter_id_exists.fetchone()
    if chapter_id_exists_dict is not None:
        chap_lang = chapter_id_exists_dict["chapter_language"]
        if dict(chapter_id_exists_dict).get('md_chapter_id', None) is None:
            print('Updating database with new mangadex chapter id.')
            logging.info(f'Updating existing record in the database: {chapter}.')
            database_connection.execute('UPDATE chapters SET md_chapter_id=:md_id, chapter_language=:language WHERE chapter_id=:mplus_id',
                                {"md_id": succesful_upload_id, "mplus_id": chapter_id,
                                "language": mplus_language_map.get(str(chap_lang), "NULL") if isinstance(chap_lang, int) else chap_lang})
    else:
        logging.info(f'Adding new chapter to database: {chapter}.')
        database_connection.execute('''INSERT INTO chapters (chapter_id, timestamp, chapter_expire, chapter_language, chapter_title, chapter_number, mplus_manga_id, md_chapter_id) VALUES
                                                            (:chapter_id, :timestamp, :chapter_expire, :chapter_language, :chapter_title, :chapter_number, :mplus_manga_id, :md_chapter_id)''',
                    {"chapter_id": chapter_id, "timestamp": chapter.chapter_timestamp, "chapter_expire": chapter.chapter_expire,
                    "chapter_language": chapter.chapter_language, "chapter_title": chapter.chapter_title, "chapter_number": chapter.chapter_number,
                    "mplus_manga_id": chapter.manga_id, "md_chapter_id": succesful_upload_id})
    database_connection.commit()
    print('Updated database.')


def check_last_run(last_run_path: Path) -> int:
    """Open last run file, if not exists, use current time."""
    if last_run_path.exists():
        with open(last_run_path, 'r') as last_run_fp:
            last_run = int(last_run_fp.readline().strip('\n'))
        logging.info(f'Opened last run file, last run: {last_run}.')
    else:
        logging.warning('Last run file not found, using now as the last_run time.')
        last_run = int(datetime.timestamp(datetime.now()))
    return last_run


def open_manga_id_map(manga_map_path: Path) -> Optional[Dict[str, List[int]]]:
    """Open mangaplus id to mangadex id map."""
    try:
        with open(manga_map_path, 'r') as manga_map_fp:
            manga_map = json.load(manga_map_fp)
        logging.info('Opened manga id map file.')
    except json.JSONDecodeError:
        logging.critical('Manga map file is corrupted.')
        raise Exception("Manga map file is corrupted.")
        manga_map = {}
    except FileNotFoundError:
        logging.critical('Manga map file is missing.')
        raise Exception("Couldn't file manga map file.")
        manga_map = {}
    return manga_map


def check_for_duplicate_chapter(database_connection: sqlite3.Connection, manga_chapters: List[Dict], chapter, mangadex_manga_id: UUID, mplus_manga_id: int, chapter_number: str, chapter_language: str) -> bool:
    """Check for duplicate chapters on mangadex."""
    # Skip duplicate chapters
    for md_chapter in manga_chapters:
        if md_chapter["attributes"]["chapter"] == chapter_number and md_chapter["attributes"]["translatedLanguage"] == chapter_language and md_chapter["attributes"]["externalUrl"] is not None:
            dupe_chapter_message = f'Manga: {mangadex_manga_id}: {mplus_manga_id}, chapter: {chapter_number}, language: {chapter_language} already exists on mangadex, skipping.'
            logging.info(dupe_chapter_message)
            print(dupe_chapter_message)
            # Add duplicate chapter to database to avoid checking it again in the future
            update_database(database_connection, chapter, md_chapter["id"])
            return True
    return False


def create_upload_session(mangadex_manga_id: UUID, chapter_number: str, manga_generic_error_message: str) -> Optional[Dict]:
    """Try create an upload session 3 times."""
    chapter_upload_session_retry = 0
    chapter_upload_session_successful = False
    while chapter_upload_session_retry < upload_retry_total:
        delete_exising_upload_session(session)
        time.sleep(mangadex_ratelimit_time)
        # Start the upload session
        upload_session_response = session.post(f'{md_upload_api_url}/begin', json={"manga": mangadex_manga_id, "groups": [mplus_group]})
        if upload_session_response.status_code == 401:
            login_to_md(session, config)            
        elif upload_session_response.status_code != 200:
            print_error(upload_session_response)
            logging.error(f"Couldn't create an upload session for {mangadex_manga_id}, chapter {chapter_number}.")
            print("Couldn't create an upload session.")

        if upload_session_response.status_code == 200:
            upload_session_response_json = convert_json(upload_session_response)
            if upload_session_response_json is not None:
                chapter_upload_session_successful = True
                chapter_upload_session_retry == upload_retry_total
                return upload_session_response_json
            else:
                upload_session_response_json_message = f"Couldn't convert successful upload session creation into a json, retrying. {manga_generic_error_message}."
                logging.error(upload_session_response_json_message)
                print(upload_session_response_json_message)

        chapter_upload_session_retry += 1
        time.sleep(mangadex_ratelimit_time)

    # Couldn't create an upload session, skip the chapter
    if not chapter_upload_session_successful:
        upload_session_response_json_message = f"Couldn't create an upload session for {manga_generic_error_message}."
        logging.error(upload_session_response_json_message)
        print(upload_session_response_json_message)
        return


def commit_chapter(chapter, upload_session_id: UUID, mangadex_manga_id: UUID, mplus_manga_id: int, manga_generic_error_message: str, chapter_number: str, chapter_language: str) -> bool:
    """Try commit the chapter to mangadex."""
    commit_retries = 0
    succesful_upload = False
    while commit_retries < upload_retry_total:
        chapter_commit_response = session.post(f'{md_upload_api_url}/{upload_session_id}/commit',
            json={"chapterDraft":
                {"volume": None, "chapter": chapter_number, "title": chapter.chapter_title, "translatedLanguage": chapter_language, "externalUrl": mangaplus_chapter_url.format(chapter.chapter_id)}, "pageOrder": []
            })

        if chapter_commit_response.status_code == 200:
            succesful_upload = True
            chapter_commit_response_json = convert_json(chapter_commit_response)
            if chapter_commit_response_json is not None:
                succesful_upload_id = chapter_commit_response_json["data"]["id"]
                succesful_upload_message = f"Committed {succesful_upload_id} for {manga_generic_error_message}."
                logging.info(succesful_upload_message)
                print(succesful_upload_message)
                update_database(database_connection, chapter, succesful_upload_id)
                commit_retries == upload_retry_total
                return True

            chapter_commit_response_json_message = f"Couldn't convert successful chapter commit api response into a json"
            logging.error(chapter_commit_response_json_message)
            print(chapter_commit_response_json_message)
            return True
        elif chapter_commit_response.status_code == 401:
            login_to_md(session, config)
        else:
            logging.warning(f"Failed to commit {upload_session_id}, retrying.")
            print_error(chapter_commit_response)

        commit_retries += 1
        time.sleep(mangadex_ratelimit_time)

    if not succesful_upload:
        error_message = f"Couldn't commit {upload_session_id}, manga {mangadex_manga_id}: {mplus_manga_id} chapter {chapter_number}."
        logging.error(error_message)
        print(error_message)
        remove_upload_session(session, upload_session_id)
        return False


def upload_chapters():
    """Go through each new chapter and upload it to mangadex."""
    # Sort each chapter by manga
    updated_manga_chapters = {}
    for chapter in updates:
        md_id = get_md_id(manga_id_map, chapter.manga_id)
        try:
            updated_manga_chapters[md_id].append(chapter)
        except (KeyError, ValueError, AttributeError):
            updated_manga_chapters[md_id] = [chapter]

    for mangadex_manga_id in updated_manga_chapters:
        # Get each manga's uploaded chapters on mangadex
        chapters = updated_manga_chapters[mangadex_manga_id]
        manga_chapters = get_chapters(session, **{
            "groups[]": [mplus_group],
            "manga": mangadex_manga_id,
            "order[createdAt]": "desc",
        })

        skipped = 0
        for chapter in chapters:
            # Delete existing upload session if exists
            check_logged_in(session, config)
            time.sleep(mangadex_ratelimit_time)
            mplus_manga_id = chapter.manga_id
            chapter_number = chapter.chapter_number
            chapter_language = chapter.chapter_language
            if isinstance(chapter_language, int):
                chapter_language = mplus_language_map.get(str(chapter_language), "NULL")

            manga_generic_error_message = f'manga {mangadex_manga_id}: {mplus_manga_id}, chapter {chapter_number}, language {chapter_language}'

            duplicate_chapter = check_for_duplicate_chapter(database_connection, manga_chapters, chapter, mangadex_manga_id, mplus_manga_id, chapter_number, chapter_language)
            if duplicate_chapter:
                skipped += 1
                continue           

            upload_session_response_json = create_upload_session(mangadex_manga_id, chapter_number, manga_generic_error_message)
            if upload_session_response_json is None:
                time.sleep(mangadex_ratelimit_time)
                skipped += 1
                continue

            upload_session_id = upload_session_response_json["data"]["id"]
            chapter_committed = commit_chapter(chapter, upload_session_id, mangadex_manga_id, mplus_manga_id, manga_generic_error_message, chapter_number, chapter_language)
            if not chapter_committed:
                skipped += 1

            time.sleep(mangadex_ratelimit_time)

        skipped_chapters_message = f'Skipped {skipped} chapters out of {len(chapters)} for manga {mangadex_manga_id}: {mplus_manga_id}.'
        logging.info(skipped_chapters_message)
        print(skipped_chapters_message)
        time.sleep(mangadex_ratelimit_time)


if __name__ == '__main__':

    last_run_path = root_path.joinpath('last_run').with_suffix('.txt')
    manga_map_path = root_path.joinpath('manga').with_suffix('.json')

    # Open required files
    last_run = check_last_run(last_run_path)
    manga_id_map = open_manga_id_map(manga_map_path)
    uploader_account_id = config["MangaDex Credentials"]["mangadex_userid"]
    upload_retry_total = 3

    # Get already posted chapters
    posted_chapters = database_connection.execute("SELECT * FROM chapters").fetchall()
    posted_chapters_ids = [job["chapter_id"] for job in posted_chapters] if not fill_backlog else []
    manga_map_mplus_ids = [mplus_id for md_id in manga_id_map for mplus_id in manga_id_map[md_id]]
    logging.info('Retrieved posted chapters from database and got mangaplus ids from manga id map file.')

    session = requests.Session()
    login_to_md(session, config)

    # Start deleting expired chapters
    chapter_delete_processes = None
    if not fill_backlog:
        try:
            chapter_delete_processes = multiprocessing.Process(target=delete_expired_chapters, args=([dict(k) for k in posted_chapters], session))
            chapter_delete_processes.start()
        except:
            pass

    # Get new manga and chapter updates
    updated_manga = get_mplus_updated_manga(manga_map_mplus_ids)
    updates = get_mplus_updates(manga_map_mplus_ids, posted_chapters_ids, last_run)

    if not updates:
        logging.info("No new updates found.")
        print("No new updates found.")
    else:
        upload_chapters()

    if chapter_delete_processes is not None:
        chapter_delete_processes.join(6)

    # Save and close database
    database_connection.commit()
    database_connection.close()
    logging.info('Saved and closed database.')

    # Save last run as now
    with open(last_run_path, 'w') as last_run_fp:
        last_run_path.write_text(str(int(datetime.timestamp(datetime.now()))))
    logging.info('Saved last run time.')
