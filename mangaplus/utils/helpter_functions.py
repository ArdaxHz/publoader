import logging
import math
import time
from typing import List

import requests

from .utils import mangadex_api_url, ratelimit_time, upload_retry
from .http_client import print_error, convert_json

logger = logging.getLogger("mangaplus")


def get_md_api(session: requests.Session, route: str, **params: dict) -> List[dict]:
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
            chapters_response = session.get(
                f"{mangadex_api_url}/{route}", params=parameters, verify=False
            )
            logger.info(f"Request url {chapters_response.url}")
        except requests.RequestException as e:
            logger.error(e)
            retry += 1
            continue

        if chapters_response.status_code != 200:
            manga_response_message = f"Couldn't get the {route}s of the group."
            print_error(chapters_response, log_error=True)
            logger.error(manga_response_message)
            retry += 1
            continue

        chapters_response_data = convert_json(chapters_response)
        if chapters_response_data is None:
            logger.warning(f"Couldn't convert {route}s data into json, retrying.")
            retry += 1
            continue

        chapters.extend(chapters_response_data["data"])
        offset += limit

        if iteration == 0:
            # Finds how many pages needed to be called
            pages = math.ceil(chapters_response_data.get("total", 0) / limit)
            logger.debug(f"{pages} page(s) for group {route}s.")

        # Wait every 5 pages
        if iteration % 5 == 0:
            time.sleep(ratelimit_time)

        # End the loop when all the pages have been gone through
        # Offset 10000 is the highest you can go, reset offset and get next
        # 10k batch using the last available chapter's created at date
        if len(chapters_response_data["data"]) == 0 or not chapters_response_data["data"]:
            break

        if offset >= 10000:
            logger.debug(f"Reached 10k {route}s, looping over next 10k.")
            created_at_since_time = chapters[-1]["attributes"]["createdAt"].split("+")[0]
            offset = 0
            retry = 0
            iteration = 0
            time.sleep(5)
            continue

        iteration += 1
        retry = 0

    time.sleep(ratelimit_time)
    return chapters