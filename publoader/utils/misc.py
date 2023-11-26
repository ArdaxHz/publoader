import asyncio
import logging
import math
from datetime import datetime
from typing import Dict, List, Optional

from publoader.http import http_client
from publoader.http.properties import RequestError
from publoader.utils.config import mangadex_api_url, upload_retry

logger = logging.getLogger("publoader")


def get_md_api(route: str, **params: dict) -> List[dict]:
    """Go through each page in the api to get all the chapters/manga."""
    chapters = []
    limit = 100
    offset = 0
    iteration = 0
    retry = 0
    created_at_since_time = "2000-01-01T00:00:00"

    parameters = {}
    parameters.update(params)

    while retry < upload_retry:
        # Update the parameters with the new offset
        parameters.update(
            {
                "limit": limit,
                "offset": offset,
                "createdAtSince": created_at_since_time,
            }
        )

        logger.debug(f"Request parameters: {parameters}")

        # Call the api and get the json data
        try:
            chapters_response = http_client.get(
                f"{mangadex_api_url}/{route}", params=parameters, verify=False
            )
        except RequestError as e:
            logger.error(e)
            retry += 1
            continue

        if chapters_response.status_code != 200:
            manga_response_message = f"Couldn't get the {route}s of the group."
            logger.error(manga_response_message)
            retry += 1
            continue

        if chapters_response.data is None:
            logger.warning(f"Couldn't convert {route}s data into json, retrying.")
            retry += 1
            continue

        chapters.extend(chapters_response.data["data"])
        offset += limit

        if iteration == 0:
            # Finds how many pages needed to be called
            pages = math.ceil(chapters_response.data.get("total", 0) / limit)
            logger.debug(f"{pages} page(s) for group {route}s.")

        # End the loop when all the pages have been gone through
        # Offset 10000 is the highest you can go, reset offset and get next
        # 10k batch using the last available chapter's created at date
        if (
            len(chapters_response.data["data"]) == 0
            or not chapters_response.data["data"]
        ):
            break

        if offset >= 10000:
            logger.debug(f"Reached 10k {route}s, looping over next 10k.")
            created_at_since_time = chapters[-1]["attributes"]["createdAt"].split("+")[
                0
            ]
            offset = 0
            retry = 0
            iteration = 0
            continue

        iteration += 1
        retry = 0

    return sorted(
        chapters,
        key=lambda chap_timestamp: datetime.strptime(
            chap_timestamp["attributes"]["createdAt"], "%Y-%m-%dT%H:%M:%S%z"
        ),
    )


def iter_aggregate_chapters(aggregate_chapters: dict):
    """Return a generator for each chapter object in the aggregate response."""
    for volume in aggregate_chapters:
        if isinstance(aggregate_chapters, dict):
            volume_iter = aggregate_chapters[volume]["chapters"]
        elif isinstance(aggregate_chapters, list):
            volume_iter = volume["chapters"]

        for chapter in volume_iter:
            if isinstance(chapter, str):
                chapter_iter = volume_iter[chapter]
            elif isinstance(chapter, dict):
                chapter_iter = chapter

            yield chapter_iter


def fetch_aggregate(http_client, manga_id: str, **params) -> Optional[dict]:
    """Call the mangadex api to get the volumes of each chapter."""
    try:
        aggregate_response = http_client.get(
            f"{mangadex_api_url}/manga/{manga_id}/aggregate",
            params=params,
        )
    except RequestError as e:
        return

    if (
        aggregate_response.status_code in range(200, 300)
        and aggregate_response.data is not None
    ):
        return aggregate_response.data["volumes"]

    logger.error(f"Error returned from aggregate response for manga {manga_id}")


def flatten(t: List[list]) -> list:
    """Flatten nested lists into one list."""
    return [item for sublist in t for item in sublist]


def find_key_from_list_value(
    dict_to_search: Dict[str, List[str]], list_element: str
) -> Optional[str]:
    """Get the key from the list value one."""
    for key in dict_to_search:
        if list_element in dict_to_search[key]:
            return key


def format_title(manga_data: dict) -> str:
    """Get the MD title from the manga data."""
    attributes = manga_data.get("attributes", None)
    if attributes is None:
        return manga_data["id"]

    manga_title = attributes["title"].get("en")
    if manga_title is None:
        key = next(iter(attributes["title"]))
        manga_title = attributes["title"].get(
            attributes["originalLanguage"], attributes["title"][key]
        )
    return manga_title


def create_new_event_loop():
    """Return the event loop, create one if not there is not one running."""
    try:
        return asyncio.get_event_loop()
    except RuntimeError as e:
        if str(e).startswith("There is no current event loop in thread"):
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            return loop
        else:
            raise
