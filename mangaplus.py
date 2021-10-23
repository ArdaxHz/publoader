import json
import multiprocessing
import sqlite3
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Optional, List, Union

import requests

import proto.response_pb2 as response_pb


mplus_base_api_url = "https://jumpg-webapi.tokyo-cdn.com"
mangaplus_chapter_url = 'https://mangaplus.shueisha.co.jp/viewer/{}'
http_error_codes = {"400": "Bad request.", "401": "Unauthorised.", "403": "Forbidden.", "404": "Not found.", "429": "Too many requests."}
md_upload_api_url = 'https://api.mangadex.org/upload'
mplus_language_map = {'0': 'en', '1': 'es-la', '2': 'fr', '3': 'id', '4': 'pt-br', '5': 'ru', '6': 'th'}
mplus_group = '4f1de6a2-f0c5-4ac5-bce5-02c7dbb67deb'

MPLUS_LANGUAGE_ID_MAP = """
0: English
1: Spanish
2: French
3: Indonesian
4: Portuguese
5: Russian
6: Thai
"""



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



def print_error(error_response: requests.Response):
    """Print the errors the site returns."""
    # Api didn't return json object
    try:
        error_json = error_response.json()
    except json.JSONDecodeError:
        print(error_response.status_code)
        return
    # Maybe already a json object
    except AttributeError:
        # Try load as a json object
        try:
            error_json = json.loads(error_response)
        except json.JSONDecodeError:
            print(error_response.status_code)
            return

    # Api response doesn't follow the normal api error format
    try:
        errors = [f'{e["status"]}: {e["detail"]}' for e in error_json["errors"] if e["detail"] is not None and e["status"] is not None]
        errors = ', '.join(errors)

        if not errors:
            errors = http_error_codes.get(str(error_response.status_code), '')

        print(f'Error: {errors}')
    except KeyError:
        print(error_response.status_code)


def login_to_md(session: requests.Session):
    """Login to MangaDex using the credentials found in the env file."""
    username = ''
    password = ''
    login_response = session.post('https://api.mangadex.org/auth/login', json={"username": username, "password": password})

    if login_response.status_code != 200:
        print_error(login_response)
        raise Exception("Couldn't login.")

    # Update requests session with headers to always be logged in
    session_token = login_response.json()["token"]["session"]
    session.headers.update({"Authorization": f"Bearer {session_token}"})


def remove_upload_session(session: requests.Session, upload_session_id: str):
    """Delete the upload session."""
    session.delete(f'{md_upload_api_url}/{upload_session_id}')


def get_md_id(manga_id_map: Dict[str, List[int]], mangaplus_id: int) -> str:
    """Get the mangadex id from the mangaplus one."""
    for md_id in manga_id_map:
        if mangaplus_id in manga_id_map[md_id]:
            return md_id


def get_previous_chapter(chapters: list, current_chapter):
    """Find the previous chapter to the current."""
    for chapter in reversed(list(chapters)[:list(chapters).index(current_chapter)]):
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
    for chapter in chapters[-3:]:
        # Chapter id is not posted and the 
        if chapter.chapter_id not in posted_chapters and (datetime.fromtimestamp(chapter.start_timestamp) + timedelta(hours=24)) <= datetime.fromtimestamp(last_run):
            previous_chapter = get_previous_chapter(chapters, chapter)
            if chapter.chapter_number == "ex":
                previous_chapter_number = str(previous_chapter.chapter_number)
                chapter_number = f"{previous_chapter_number.lstrip('#').lstrip('0')}.5"
            elif chapter.chapter_number == "One-Shot":
                chapter_number = None
            else:
                chapter_number = str(chapter.chapter_number.lstrip('#')).lstrip('0')
            
            updated_chapters.append(Chapter(chapter_id=chapter.chapter_id, chapter_timestamp=chapter.start_timestamp,
                chapter_title=chapter.chapter_name, chapter_expire=chapter.end_timestamp, chapter_number=chapter_number, chapter_language=manga_chapters.manga.language, manga_id=manga_chapters.manga.manga_id))

    return updated_chapters


def get_updated_manga(updated_manga_response_parsed: response_pb.Response, manga_re_edtion_ids: List[int]) -> List[Manga]:
    """return new found manga ids"""
    updated_manga_details = updated_manga_response_parsed.success.updated
    updated_manga = []

    for manga in updated_manga_details.updated_manga_detail:
        if str(manga.updated_manga.manga_id) not in manga_re_edtion_ids:
            manga_id = manga.updated_manga.manga_id
            manga_name = manga.updated_manga.manga_name
            language = manga.updated_manga.language
            updated_manga.append(Manga(manga_id=manga_id, manga_name=manga_name, manga_language=language))
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
        print("Request API Error", e)

    if response.status_code == 200:
        return response.content
    return None


def remove_old_chapters(session: requests.Session, databse_connection: sqlite3.Connection, chapter: Dict[int, Optional[str]]):
    """Check if the chapters expired and remove off mangadex if they are."""
    if datetime.fromtimestamp(chapter["chapter_expire"]) <= datetime.now() and chapter["md_chapter_id"] is not None:
        delete_reponse = session.delete(f'https://api.mangadex.org/chapter/{chapter["md_chapter_id"]}')
        if delete_reponse.status_code != 200:
            print_error(delete_reponse)
            return
        # databse_connection.execute('DELETE FROM chapters WHERE chapter_id=(?)', (chapter["chapter_id"]))
        # databse_connection.commit()


def delete_expired_chapters(posted_chapters: List[Dict[str, int]], session: requests.Session, database_connection: sqlite3.Connection) -> List[multiprocessing.Process]:
    """Delete expired chapters from mangadex and the database."""
    chapter_delete_processes = []
    for chapter_to_delete in posted_chapters:
        print('Deleting expired chapters.')
        process = multiprocessing.Process(target=remove_old_chapters, args=(session, database_connection, chapter_to_delete))
        process.start()
        chapter_delete_processes.append(process)
    return chapter_delete_processes


def get_mplus_updated_manga(tracked_manga: List[int]) -> List[Manga]:
    """Find new untracked mangaplus series."""
    updates = []

    print('Getting new manga.')
    updated_manga_response = request_from_api(updated=True)
    if updated_manga_response is not None:
        updated_manga_response_parsed = get_proto_response(updated_manga_response)
        updated_manga = get_updated_manga(updated_manga_response_parsed, tracked_manga)
        updates.extend(updated_manga)
    return updates


def get_mplus_updates(manga_series: List[int], posted_chapters: List[int], last_run: int) -> List[Chapter]:
    """Get latest chapter updates."""
    updates = []

    print('Getting manga updates.')
    for manga in manga_series:
        manga_response = request_from_api(manga_id=manga)
        if manga_response is not None:
            manga_response_parsed = get_proto_response(manga_response)
            updated_chapters = get_latest_chapters(manga_response_parsed, posted_chapters, last_run)
            print(updated_chapters)
            updates.extend(updated_chapters)
    return updates


def update_database(database_connection: sqlite3.Connection, chapter: Chapter, succesful_upload_id: Optional[str]=None):
    """Update the database with the new chapter."""
    print('Updating database.')
    database_connection.execute('INSERT INTO chapters (chapter_id, timestamp, chapter_expire, chapter_language, chapter_title, chapter_number, mplus_manga_id, md_chapter_id) VALUES (?,?,?,?,?,?,?,?)',
                (chapter.chapter_id, chapter.chapter_timestamp, chapter.chapter_expire, chapter.chapter_language, chapter.chapter_title, chapter.chapter_number, chapter.manga_id, succesful_upload_id))
    database_connection.commit()


def make_tables(database_connection: sqlite3.Connection):
    """Make the database table."""
    # con.execute('''CREATE TABLE IF NOT EXISTS manga
    #             (manga_id INTEGER NOT NULL PRIMARY KEY, manga_name TEXT NULL, md_manga_id TEXT NOT NULL)''')
    database_connection.execute('''CREATE TABLE IF NOT EXISTS chapters
                (chapter_id INTEGER NOT NULL PRIMARY KEY, timestamp INTEGER NOT NULL, chapter_expire INTEGER NOT NULL, chapter_language INTEGER NOT NULL, chapter_title TEXT NULL, chapter_number TEXT NOT NULL, mplus_manga_id INTEGER NOT NULL, md_chapter_id TEXT NULL)''')
                # FOREIGN KEY (mplus_manga_id) REFERENCES manga(manga_id))
    database_connection.commit()


def check_last_run(last_run_path: Path) -> int:
    """Open last run file, if not exists, use current time."""
    if last_run_path.exists():
        with open(last_run_path, 'r') as last_run_fp:
            last_run = int(last_run_fp.readline().strip('\n'))
    else:
        last_run = int(datetime.timestamp(datetime.now()))
    return last_run


def open_manga_id_map(manga_map_path: Path) -> Optional[Dict[str, List[int]]]:
    """Open mangaplus id to mangadex id map."""
    try:
        with open(manga_map_path, 'r') as manga_map_fp:
            manga_map = json.load(manga_map_fp)
    except json.JSONDecodeError:
        raise Exception("Manga map file is corrupted.")
        manga_map = {}
    except FileNotFoundError:
        raise Exception("Couldn't file manga map file.")
        manga_map = {}
    return manga_map


def check_table_exists(database_connection: sqlite3.Connection) -> bool:
    """Check if the table exists."""
    table_exist = database_connection.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='chapters'")

    fill_backlog = False
    # Table doesn't exist, fill backlog without posting to mangadex
    if not table_exist.fetchall():
        print("Table doesn't exist, making new one.")
        make_tables(database_connection)
        fill_backlog = True
    return fill_backlog


def open_database(database_path: Path) -> sqlite3.Connection:
    """Open the database."""
    database_connection = sqlite3.connect(database_path)
    database_connection.row_factory = sqlite3.Row
    return database_connection


if __name__ == '__main__':

    root_path = Path('.')
    database_path = root_path.joinpath('chapters').with_suffix('.db')
    last_run_path = root_path.joinpath('last_run').with_suffix('.txt')
    manga_map_path = root_path.joinpath('manga').with_suffix('.json')

    # Open required files
    last_run = check_last_run(last_run_path)
    manga_id_map = open_manga_id_map(manga_map_path)
    database_connection = open_database(database_path)
    fill_backlog = check_table_exists(database_connection)

    # Get already posted chapters
    posted_chapters = database_connection.execute("SELECT * FROM chapters").fetchall()
    posted_chapters_ids = [job["chapter_id"] for job in posted_chapters] if not fill_backlog else []
    manga_map_mplus_ids = [mplus_id for md_id in manga_id_map for mplus_id in manga_id_map[md_id]]

    # Get new manga and chapter updates
    updated_manga = get_mplus_updated_manga(manga_map_mplus_ids)
    updates = get_mplus_updates(manga_map_mplus_ids, posted_chapters_ids, last_run)

    session = requests.Session()
    login_to_md(session)

    # Start deleting expired chapters
    if not fill_backlog:
        chapter_delete_processes = delete_expired_chapters(posted_chapters, session, database_connection)

    for chapter in updates:
        # Remove any exising upload sessions to not error out
        existing_session = session.get(f'{md_upload_api_url}')
        if existing_session.status_code == 200:
            remove_upload_session(session, existing_session.json()["data"]["id"])

        # Start the upload session
        upload_session_response = session.post(f'{md_upload_api_url}/begin', json={"manga": get_md_id(manga_id_map, chapter.manga_id), "groups": [mplus_group]})
        if upload_session_response.status_code != 200:
            print_error(upload_session_response)
            print("Couldn't create an upload session.")
            continue

        # chapter_title = chapter.chapter_title.strip()
        upload_session_id = upload_session_response.json()["data"]["id"]
        commit_retries = 0
        succesful_upload = False
        while commit_retries < 5:
            chapter_commit_response = session.post(f'{md_upload_api_url}/{upload_session_id}/commit',
                json={"chapterDraft":
                    {"volume": None, "chapter": chapter.chapter_number, "title": chapter.chapter_title, "translatedLanguage": mplus_language_map[str(chapter.chapter_language)], "externalUrl": mangaplus_chapter_url.format(chapter.chapter_id)}, "pageOrder": []
                })

            if chapter_commit_response.status_code == 200:
                succesful_upload = True
                succesful_upload_id = chapter_commit_response.json()["data"]["id"]
                print(f"Uploaded {succesful_upload_id}")
                update_database(database_connection, chapter, succesful_upload_id)
                commit_retries == 5
                break

            commit_retries += 1
            time.sleep(1)

        if not succesful_upload:
            print(f"Couldn't commit {upload_session_id}, manga {chapter.manga_id} chapter {chapter.chapter_number}")
            remove_upload_session(session, upload_session_id)

    # Make sure background process of deleting expired chapters is finished
    if not fill_backlog:
        for process in chapter_delete_processes:
            process.join()

    # Save and close database
    database_connection.commit()
    database_connection.close()

    # Save last run as now
    with open(last_run_path, 'w') as last_run_fp:
        last_run_path.write_text(str(datetime.timestamp(datetime.now())))
