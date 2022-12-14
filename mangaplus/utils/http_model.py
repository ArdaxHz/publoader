import json
import logging
import time
from datetime import datetime
from typing import Optional

import requests

from .utils import max_requests, upload_retry


logger = logging.getLogger("mangaplus")
logger_debug = logging.getLogger("debug")
http_error_codes = {
    "400": "Bad request.",
    "401": "Unauthorised.",
    "403": "Forbidden.",
    "404": "Not found.",
    "429": "Too many requests.",
}


class RequestError(Exception):
    def __init__(self, message: str) -> None:
        super().__init__(message)


class HTTPResponse:
    def __init__(self, response: requests.Response) -> None:
        self.response = response
        self.data = self.json()

    @property
    def status_code(self):
        return self.response.status_code

    @property
    def status(self):
        return self.response.status_code

    def json(self) -> Optional[dict]:
        """Convert the api response into a parsable json."""
        critical_decode_error_message = (
            "Couldn't convert mangadex api response into a json."
        )

        logger.debug(f"Request id: {self.response.headers.get('x-request-id', None)}")

        try:
            converted_response = self.response.json()
            return converted_response
        except json.JSONDecodeError:
            logger.critical(critical_decode_error_message)
            print(critical_decode_error_message)
            return
        except AttributeError:
            logger.critical(
                f"Api response doesn't have load as json method, trying to load as json manually."
            )
            try:
                converted_response = json.loads(self.response.content)
            except json.JSONDecodeError:
                logger.critical(critical_decode_error_message)
                print(critical_decode_error_message)
                return

    def print_error(
        self,
        show_error: bool = False,
        log_error: bool = True,
    ) -> str:
        """Print the errors the site returns."""
        error_message = f"Error: {self.status_code}"
        error_json = self.json()

        if error_json is not None:
            # Api response doesn't follow the normal api error format
            try:
                errors = [
                    f'{e["status"]}: {e["detail"] if e["detail"] is not None else ""}'
                    for e in error_json["errors"]
                ]
                errors = ", ".join(errors)

                if not errors:
                    errors = http_error_codes.get(str(self.status_code), "")

                error_message = f"Error: {errors}"
                if log_error:
                    logger.warning(error_message)
                if show_error:
                    print(error_message)
            except KeyError:
                error_message = f"KeyError {self.status_code}: {error_json}."
                if log_error:
                    logger.warning(error_message)
                if show_error:
                    print(error_message)

        return error_message


class HTTPModel:
    def __init__(self, version) -> None:
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": f"MP-MD_bot/{version}"})

        self.upload_retry_total = upload_retry
        self.max_requests = max_requests
        self.number_of_requests = 0
        self.total_requests = 0

    def calculate_sleep_time(self, status_code: int, headers={}):
        self.number_of_requests += 1
        self.total_requests += 1
        loop = False

        limit = int(headers.get("x-ratelimit-limit", self.max_requests))
        logger.debug("limit is: %s", limit)
        remaining = int(
            headers.get(
                "x-ratelimit-remaining", self.max_requests - self.number_of_requests
            )
        )
        logger.debug("remaining is: %s", remaining)
        retry_after = headers.get("x-ratelimit-retry-after", None)

        logger.debug("retry is: %s", retry_after)

        delta = self.max_requests
        sleep = delta / limit
        if status_code == 429:
            error_message = f"429: {http_error_codes.get('429')}"
            logger.warning(error_message)
            sleep = 60
            loop = True

        if retry_after is not None:
            retry = datetime.fromtimestamp(int(retry_after))
            now = datetime.now()
            if retry > now:
                difference = retry - now
            else:
                difference = now - now

            delta = difference.total_seconds() + 1
            if remaining == 0:
                sleep = delta
                loop = True
            else:
                sleep = delta / remaining

        logger.debug("delta is: %s", delta)

        if remaining == 0 or retry_after is not None:
            self.number_of_requests = 0
            logger.debug(f"Sleeping {sleep} seconds")
            time.sleep(sleep)
        return loop

    def format_request_log(
        self,
        method: str,
        route: str,
        params: dict = None,
        json: dict = None,
        data=None,
        successful_codes: list = [],
    ):
        return f'"{method}": {route} {params=} {json=} {data=} {successful_codes=}'

    def _request(
        self,
        method: str,
        route: str,
        params: dict = None,
        json: dict = None,
        data=None,
        successful_codes: list = [],
        **kwargs,
    ):
        retry = self.upload_retry_total
        tries = kwargs.get("tries", self.upload_retry_total)

        formatted_request_string = self.format_request_log(
            method=method,
            route=route,
            params=params,
            json=json,
            data=data,
            successful_codes=successful_codes,
        )

        logger.debug(formatted_request_string)

        while retry > 0:
            try:
                response = self.session.request(
                    method, route, json=json, params=params, data=data
                )
                loop = self.calculate_sleep_time(
                    status_code=response.status_code, headers=response.headers
                )
                if loop:
                    continue
            except requests.RequestException as e:
                logger.error(e)
                retry -= 1
                continue

            response_obj = HTTPResponse(response)
            if response_obj.data is None and tries > 1:
                retry -= 1
                continue

            if (successful_codes and response.status_code in successful_codes) or (
                response.status_code in range(200, 300)
            ):
                return response_obj

            if response.status_code == 401:
                error_message = f"401: Not logged in."
                logger.warning(error_message)
                print(error_message)
                try:
                    self.login()
                except Exception as e:
                    logger.error(e)
                    retry = self.upload_retry_total
                    continue
            elif response.status_code == 429:
                error_message = f"429: {http_error_codes.get('429')}"
                logger.warning(error_message)
                print(error_message)
                time.sleep(60)
                retry = self.upload_retry_total
                continue
            else:
                if tries == 1:
                    retry = 0

                response_obj.print_error(
                    show_error=kwargs.get("show_error", True),
                    log_error=kwargs.get("show_error", True),
                )
                retry -= 1
                continue

        raise RequestError(formatted_request_string)

    def request(
        self,
        method: str,
        route: str,
        params: dict = None,
        json: dict = None,
        data=None,
        successful_codes: list = [],
        **kwargs,
    ):
        return self._request(
            method=method,
            route=route,
            params=params,
            json=json,
            data=data,
            successful_codes=successful_codes,
            **kwargs,
        )

    def post(
        self,
        route: str,
        json: dict = None,
        data=None,
        successful_codes: list = [],
        **kwargs,
    ):
        return self._request(
            method="POST",
            route=route,
            json=json,
            data=data,
            successful_codes=successful_codes,
            **kwargs,
        )

    def get(
        self,
        route: str,
        params: dict = None,
        successful_codes: list = [],
        **kwargs,
    ):
        return self._request(
            method="GET",
            route=route,
            params=params,
            successful_codes=successful_codes,
            **kwargs,
        )

    def put(
        self,
        route: str,
        json: dict = None,
        data=None,
        successful_codes: list = [],
        **kwargs,
    ):
        return self._request(
            method="PUT",
            route=route,
            json=json,
            data=data,
            successful_codes=successful_codes,
            **kwargs,
        )

    def update(
        self,
        route: str,
        json: dict = None,
        data=None,
        successful_codes: list = [],
        **kwargs,
    ):
        return self.put(
            route=route,
            json=json,
            data=data,
            successful_codes=successful_codes,
            **kwargs,
        )

    def delete(
        self,
        route: str,
        params: dict = None,
        json: dict = None,
        data=None,
        successful_codes: list = [],
        **kwargs,
    ):
        return self._request(
            method="DELETE",
            route=route,
            params=params,
            json=json,
            data=data,
            successful_codes=successful_codes,
            **kwargs,
        )
