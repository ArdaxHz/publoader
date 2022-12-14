import configparser
import json
import logging

from . import mangadex_api_url, components_path
from .utils.http_model import HTTPModel
from .utils import RequestError


logger = logging.getLogger("mangaplus")


class HTTPClient(HTTPModel):
    def __init__(self, config: configparser.RawConfigParser, version):
        super().__init__(version)

        self._config = config
        self._token_file = components_path.joinpath(config["Paths"]["mdauth_path"])
        self._md_auth_api_url = f"{mangadex_api_url}/auth"
        self._file_token = self._open_auth_file()

        self._first_login = True
        self._successful_login = False
        self._session_token = self._file_token.get("session", None)
        self._refresh_token = self._file_token.get("refresh", None)

    def _open_auth_file(self) -> dict:
        """Open auth file and read saved tokens."""
        try:
            with open(self._token_file, "r") as login_file:
                token = json.load(login_file)
            return token
        except (FileNotFoundError, json.JSONDecodeError):
            logger.error(
                "Couldn't find the file, trying to login using your account details."
            )
            return {}

    def _save_session(self, token: dict):
        """Save the session and refresh tokens."""
        with open(self._token_file, "w") as login_file:
            login_file.write(json.dumps(token, indent=4))
        logger.debug("Saved mdauth file.")

    def _update_headers(self, session_token: str):
        """Update the session headers to include the auth token."""
        self.session.headers.update({"Authorization": f"Bearer {session_token}"})

    def _update_token_details(self, token: dict):
        """Update the instance session and refresh tokens, update the header and save the file."""
        self._session_token = token["session"]
        self._refresh_token = token["refresh"]

        self._update_headers(token["session"])
        self._save_session(token)

    def _refresh_token_md(self) -> bool:
        """Use the refresh token to get a new session token."""
        refreshed = False

        if self._refresh_token is None:
            logger.error(
                f"Refresh token doesn't exist, logging in through account details."
            )
            return self._login_using_details()

        try:
            refresh_response = self.post(
                f"{self._md_auth_api_url}/refresh",
                json={"token": self._refresh_token},
                successful_codes=[401, 403],
            )
        except RequestError as e:
            logger.error(e)
            return False

        if refresh_response.status_code == 200 and refresh_response.data is not None:
            refresh_data = refresh_response.data["token"]
            self._update_token_details(refresh_data)
            return True
        elif refresh_response.status_code in (401, 403):
            logger.warning(
                f"Couldn't login using refresh token, logging in using your account."
            )
            return self._login_using_details()

        logger.error(f"Couldn't refresh token.")
        return refreshed

    def _check_login(self) -> bool:
        """Try login using saved session token."""
        try:
            auth_check_response = self.get(
                f"{self._md_auth_api_url}/check",
            )
        except RequestError as e:
            logger.error(e)
        else:
            if (
                auth_check_response.status_code == 200
                and auth_check_response.data is not None
            ):
                if auth_check_response.data["isAuthenticated"]:
                    logger.info("Already logged in.")
                    return True

        if self._refresh_token is None:
            return self._login_using_details()
        return self._refresh_token_md()

    def _login_using_details(self) -> bool:
        """Login using account details."""
        username = self._config["MangaDex Credentials"]["mangadex_username"]
        password = self._config["MangaDex Credentials"]["mangadex_password"]

        if username == "" or password == "":
            critical_message = "Login details missing."
            logger.critical(critical_message)
            raise Exception(critical_message)

        try:
            login_response = self.post(
                f"{self._md_auth_api_url}/login",
                json={"username": username, "password": password},
            )
        except RequestError as e:
            logger.error(e)
            return False

        if login_response.status_code == 200 and login_response.data is not None:
            login_token = login_response.data["token"]
            self._update_token_details(login_token)
            return True

        logger.error(f"Couldn't login to mangadex using the details provided.")
        return False

    def login(self, check_login=True):
        """Login to MD account using details or saved token."""
        if not check_login and self._successful_login:
            logger.info("Already logged in, not checking for login.")
            return

        if self._first_login:
            logger.info("Trying to login through the .mdauth file.")

        if self._session_token is not None:
            logged_in = self._check_login()
        else:
            logged_in = self._refresh_token_md()

        if logged_in:
            self._successful_login = True
            if self._first_login:
                logger.info(f"Logged into mangadex.")
                print("Logged in.")
                self._first_login = False
        else:
            logger.critical("Couldn't login.")
            raise Exception("Couldn't login.")
