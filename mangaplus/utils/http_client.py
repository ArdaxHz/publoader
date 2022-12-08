import json
import logging
import time
from typing import Optional

import requests

from .utils import http_error_codes, ratelimit_time


logger = logging.getLogger("mangaplus")
logger_debug = logging.getLogger("debug")


def convert_json(response_to_convert: requests.Response) -> Optional[dict]:
    """Convert the api response into a parsable json."""
    critical_decode_error_message = (
        "Couldn't convert mangadex api response into a json."
    )

    logger.debug(f"Request id: {response_to_convert.headers.get('x-request-id', None)}")

    try:
        converted_response = response_to_convert.json()
    except json.JSONDecodeError:
        logger.critical(critical_decode_error_message)
        print(critical_decode_error_message)
        return
    except AttributeError:
        logger.critical(
            f"Api response doesn't have load as json method, trying to load as json manually."
        )
        try:
            converted_response = json.loads(response_to_convert.content)
        except json.JSONDecodeError:
            logger.critical(critical_decode_error_message)
            print(critical_decode_error_message)
            return

    logger.debug("Convert api response into json.")
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

    # logger.error(f"Error response: {error_response.raw}")

    if status_code == 429:
        error_message = f"429: {http_error_codes.get(str(status_code))}"
        if log_error:
            logger.error(error_message)
        if show_error:
            print(error_message)
        time.sleep(ratelimit_time * 6)
        return error_message

    # Api didn't return json object
    try:
        error_json = error_response.json()
    except json.JSONDecodeError as e:
        logger.error(error_converting_json_log_message.format(e))
        print(error_converting_json_print_message)
        return error_converting_json_print_message
    # Maybe already a json object
    except AttributeError:
        logger.error(f"Error response is already a json.")
        # Try load as a json object
        try:
            error_json = json.loads(error_response.content)
        except json.JSONDecodeError as e:
            logger.error(error_converting_json_log_message.format(e))
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
            logger.warning(error_message)
        if show_error:
            print(error_message)
    except KeyError:
        error_message = f"KeyError {status_code}: {error_json}."
        if log_error:
            logger.warning(error_message)
        if show_error:
            print(error_message)

    return error_message
