import logging
import re
import sqlite3
import time
from datetime import datetime
from typing import TYPE_CHECKING, Dict, List, Literal, Optional

import requests
from . import (
    convert_json,
    print_error,
    md_upload_api_url,
    ratelimit_time,
    mplus_group_id,
    upload_retry,
    Chapter,
    get_md_id,
    flatten,
    update_database,
    mangadex_api_url,
)


if TYPE_CHECKING:
    from .auth_md import AuthMD

logger = logging.getLogger("mangaplus")


class ChapterUploaderProcess:
    def __init__(
        self,
        database_connection: sqlite3.Connection,
        session: requests.Session,
        mangadex_manga_id: str,
        chapter: "Chapter",
        md_auth_object: "AuthMD",
        posted_md_updates: List["Chapter"],
        same_chapter_dict: Dict[str, List[int]],
        **kwargs,
    ):
        self.database_connection = database_connection
        self.session = session
        self.mangadex_manga_id = mangadex_manga_id
        self.chapter = chapter
        self.md_auth_object = md_auth_object
        self.posted_md_updates = posted_md_updates
        self.same_chapter_dict = same_chapter_dict

        self.mplus_chapter_url = "https://mangaplus.shueisha.co.jp/viewer/{}"

        self.manga_generic_error_message = (
            f"Manga: {self.chapter.manga.manga_name}, "
            f"{self.mangadex_manga_id} - {self.chapter.manga_id}, "
            f"chapter: {self.chapter.chapter_number}, "
            f"volume: {self.chapter.chapter_volume}, "
            f"language: {self.chapter.chapter_language}, "
            f"title: {self.chapter.chapter_title}"
        )
        self.upload_retry_total = upload_retry
        self.upload_session_id: Optional[str] = None

    def remove_upload_session(self, session_id: Optional[str] = None):
        """Delete the upload session."""
        if session_id is None:
            session_id = self.upload_session_id

        self.session.delete(f"{md_upload_api_url}/{session_id}", verify=False)
        logger.info(f"Sent {session_id} to be deleted.")

    def _delete_exising_upload_session(
        self, chapter_upload_session_retry: int, json_error=False
    ):
        """Remove any exising upload sessions to not error out as mangadex only allows one upload session at a time."""
        if chapter_upload_session_retry > 0 and not json_error:
            return

        logger.debug(
            f"Checking for upload sessions for manga {self.mangadex_manga_id}, chapter {self.chapter}."
        )
        for removal_retry in range(self.upload_retry_total):
            try:
                existing_session = self.session.get(
                    f"{md_upload_api_url}", verify=False
                )
            except requests.RequestException as e:
                logger.error(e)
                continue

            if existing_session.status_code == 200:
                existing_session_json = convert_json(existing_session)

                if existing_session_json is None:
                    logger.warning(
                        f"Couldn't convert exising upload session response into a json, retrying."
                    )
                else:
                    self.remove_upload_session(existing_session_json["data"]["id"])
                    return
            elif existing_session.status_code == 404:
                logger.info("No existing upload session found.")
                return
            elif existing_session.status_code == 401:
                print_error(existing_session, log_error=True)
                logger.warning("Not logged in, logging in and retrying.")
                self.md_auth_object.login()
                continue
            else:
                logger.warning(f"Couldn't delete the exising upload session, retrying.")

            time.sleep(ratelimit_time)

        logger.error("Exising upload session not deleted.")
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
                time.sleep(ratelimit_time)

            try:
                # Start the upload session
                upload_session_response = self.session.post(
                    f"{md_upload_api_url}/begin",
                    json={
                        "manga": self.mangadex_manga_id,
                        "groups": [mplus_group_id],
                    },
                    verify=False,
                )
            except requests.RequestException as e:
                logger.error(e)
                continue
            json_error = False

            if upload_session_response.status_code == 200:
                upload_session_response_json = convert_json(upload_session_response)

                if upload_session_response_json is not None:
                    chapter_upload_session_successful = True
                    break
                else:
                    upload_session_response_json_message = f"Couldn't convert successful upload session creation into a json, retrying. {self.manga_generic_error_message}."
                    logger.error(upload_session_response_json_message)
                    print(upload_session_response_json_message)
                    json_error = True
            elif upload_session_response.status_code == 401:
                print_error(upload_session_response, log_error=True)
                self.md_auth_object.login()
                continue
            else:
                print_error(upload_session_response, log_error=True)
                logger.error(
                    f"Couldn't create an upload session for {self.mangadex_manga_id}, chapter {self.chapter.chapter_number}."
                )
                print("Couldn't create an upload session.")

            time.sleep(ratelimit_time)

        # Couldn't create an upload session, skip the chapter
        if not chapter_upload_session_successful:
            upload_session_response_json_message = f"Couldn't create an upload session for {self.manga_generic_error_message}."
            logger.error(upload_session_response_json_message)
            print(upload_session_response_json_message)
            time.sleep(ratelimit_time)
            return

        time.sleep(ratelimit_time)
        return upload_session_response_json

    def _commit_chapter(self) -> bool:
        """Try commit the chapter to mangadex."""
        succesful_upload = False

        payload = {
            "chapterDraft": {
                "volume": self.chapter.chapter_volume,
                "chapter": self.chapter.chapter_number,
                "title": self.chapter.chapter_title,
                "translatedLanguage": self.chapter.chapter_language,
                "externalUrl": self.mplus_chapter_url.format(self.chapter.chapter_id),
                "publishAt": datetime.fromtimestamp(
                    self.chapter.chapter_expire
                ).strftime("%Y-%m-%dT%H:%M:%S%z"),
            },
            "pageOrder": [],
        }

        logger.info(f"Commit payload: {payload}")

        for commit_retries in range(self.upload_retry_total):
            try:
                chapter_commit_response = self.session.post(
                    f"{md_upload_api_url}/{self.upload_session_id}/commit",
                    json=payload,
                    verify=False,
                )
            except requests.RequestException as e:
                logger.error(e)
                continue

            if chapter_commit_response.status_code == 200:
                succesful_upload = True
                chapter_commit_response_json = convert_json(chapter_commit_response)

                if chapter_commit_response_json is not None:
                    succesful_upload_id = chapter_commit_response_json["data"]["id"]
                    succesful_upload_message = f"Committed {succesful_upload_id} - {self.chapter.chapter_id} for {self.manga_generic_error_message}."
                    self.chapter.md_chapter_id = succesful_upload_id
                    logger.info(succesful_upload_message)
                    print(succesful_upload_message)
                    update_database(
                        self.database_connection, self.chapter, succesful_upload_id
                    )
                else:
                    chapter_commit_response_json_message = f"Couldn't convert successful chapter commit api response into a json"
                    logger.error(chapter_commit_response_json_message)
                    print(chapter_commit_response_json_message)
                return True
            elif chapter_commit_response.status_code == 401:
                print_error(chapter_commit_response, log_error=True)
                self.md_auth_object.login()
                continue
            else:
                succesful_upload = False
                logger.warning(f"Failed to commit {self.upload_session_id}, retrying.")
                print_error(chapter_commit_response, log_error=True)

            time.sleep(ratelimit_time * 2)

        if not succesful_upload:
            error_message = f"Couldn't commit {self.upload_session_id}, manga {self.mangadex_manga_id} - {self.chapter.manga_id} chapter {self.chapter.chapter_number} language {self.chapter.chapter_language}."
            logger.error(error_message)
            print(error_message)
            self.remove_upload_session()
            return False
        return succesful_upload

    def edit_chapter(self, md_chapter: dict):
        """Update the chapter on mangadex if it is different."""
        md_id = md_chapter["id"]
        chapter_attrs = md_chapter["attributes"]
        data_to_post = {
            "volume": chapter_attrs["volume"],
            "chapter": chapter_attrs["chapter"],
            "title": chapter_attrs["title"],
            "translatedLanguage": chapter_attrs["translatedLanguage"],
            "externalUrl": chapter_attrs["externalUrl"],
            "version": chapter_attrs["version"],
            "groups": [
                g["id"]
                for g in md_chapter["relationships"]
                if g["type"] == "scanlation_group"
            ],
        }
        changed = False

        if str(self.chapter.chapter_id) not in chapter_attrs["externalUrl"]:
            logger.debug(
                f"MD chapter {md_id} mangaplus id {chapter_attrs['externalUrl']} doesn't match id {self.chapter.chapter_id}"
            )
            return False

        logger.info(f"Editing {md_id} with old info {chapter_attrs}")

        if self.chapter.chapter_volume != chapter_attrs["volume"]:
            data_to_post["volume"] = self.chapter.chapter_volume
            changed = True

        if self.chapter.chapter_number != chapter_attrs["chapter"]:
            data_to_post["chapter"] = self.chapter.chapter_number
            changed = True

        if self.chapter.chapter_title != chapter_attrs["title"]:
            data_to_post["title"] = self.chapter.chapter_title
            changed = True

        if changed:
            logger.info(f"Editing chapter {md_id} with new info {data_to_post}")

            for commit_retries in range(self.upload_retry_total):
                try:
                    update_response = self.session.put(
                        f"{mangadex_api_url}/chapter/{md_id}",
                        json=data_to_post,
                        verify=False,
                    )
                except requests.RequestException as e:
                    logger.error(e)
                    continue

                if update_response.status_code == 200:
                    logger.info(f"Edited chapter {md_id}")
                    print(f"Edited chapter:  {self.manga_generic_error_message}")
                    time.sleep(ratelimit_time * 3)
                    return True
                elif update_response.status_code == 401:
                    print_error(update_response, log_error=True)
                    self.md_auth_object.login()
                    continue
                elif update_response.status_code == 404:
                    print_error(update_response, log_error=True)
                    time.sleep(ratelimit_time * 3)
                    return False
                else:
                    logger.warning(f"Failed to edit {md_id}, retrying.")
                    print_error(update_response, log_error=True)

                time.sleep(ratelimit_time * 3)
        else:
            logger.info(f"Nothing to edit for chapter {md_id}")
        return False

    def _check_for_duplicate_chapter_md_list(self, manga_chapters: List[dict]) -> bool:
        """Check for duplicate chapters on mangadex."""
        for md_chapter in manga_chapters:
            if (
                md_chapter["attributes"]["chapter"] == self.chapter.chapter_number
                and md_chapter["attributes"]["translatedLanguage"]
                == self.chapter.chapter_language
                and md_chapter["attributes"]["externalUrl"] is not None
            ):
                dupe_chapter_message = f"{self.manga_generic_error_message} already exists on mangadex, skipping."
                logger.info(dupe_chapter_message)
                print(dupe_chapter_message)

                edited = self.edit_chapter(md_chapter)
                # Add duplicate chapter to database to avoid checking it again
                # in the future
                update_database(
                    self.database_connection, self.chapter, md_chapter["id"]
                )
                return "edited" if edited else "dupe"
        return "dupe"

    def _check_already_uploaded_internal_list(self) -> bool:
        """Check if chapter to upload is already in the internal list of uploaded chapters."""
        for chap in self.posted_md_updates:
            if (
                chap.chapter_id == self.chapter.chapter_id
                and chap.chapter_number == self.chapter.chapter_number
                and chap.chapter_language == self.chapter.chapter_language
            ):
                return True
        return False

    def _check_uploaded_different_id(self, manga_chapters: List[dict]) -> bool:
        """Check if chapter id to upload has been uploaded already under a different id."""
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
        if dupe_response := self._check_for_duplicate_chapter_md_list(manga_chapters):
            return dupe_response if dupe_response in ("edited",) else "on_md"

        if self._check_already_uploaded_internal_list():
            return "on_md"

        if self._check_uploaded_different_id(manga_chapters):
            return "on_md"

        upload_session_response_json = self._create_upload_session()
        if upload_session_response_json is None:
            time.sleep(ratelimit_time)
            return "session_error"

        self.upload_session_id = upload_session_response_json["data"]["id"]
        logger.info(
            f"Created upload session: {self.upload_session_id} - {self.chapter}"
        )
        chapter_committed = self._commit_chapter()
        if not chapter_committed:
            self.remove_upload_session()
            time.sleep(ratelimit_time)
            return "session_error"

        self.posted_md_updates.append(self.chapter)
        time.sleep(ratelimit_time)
        return "uploaded"
