import base64
import configparser
import json
import logging
from datetime import datetime, timedelta

import requests
from . import convert_json, print_error, mangadex_api_url, components_path

logger = logging.getLogger("mangaplus")


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
            logger.error(
                "Couldn't find the file, trying to login using your account details."
            )
            return {}

    def _save_session(self, token: dict):
        """Save the session and refresh tokens."""
        with open(self.token_file, "w") as login_file:
            login_file.write(json.dumps(token, indent=4))
        logger.debug("Saved mdauth file.")

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
            logger.error(
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
            logger.warning(
                f"Couldn't login using refresh token, logging in using your account. Error: {error}"
            )
            return self._login_using_details()
        else:
            error = print_error(refresh_response)
            logger.error(f"Couldn't refresh token. Error: {error}")

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
                    logger.info("Already logged in.")
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
            logger.critical(critical_message)
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
        logger.error(
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
        expiry = datetime.fromtimestamp(token.get("exp", 946684799))
        datetime_now = datetime.now() + timedelta(minutes=2)

        if expiry > datetime_now:
            return False
        return True

    def login(self, check_login=True):
        """Login to MD account using details or saved token."""
        if not check_login and self.successful_login:
            logger.info("Already logged in, not checking for login.")
            return

        if self.first_login:
            logger.info("Trying to login through the .mdauth file.")

        if self.session_token is not None:
            if self.first_login:
                logger.info("Reading the session expiry from the token.")

            if self.decoded_session_token is None:
                logger.debug("Decoding the token into a json object.")
                self.decoded_session_token = self._decode_token(self.session_token)

            expired_session_token = self._check_token_expiry(self.decoded_session_token)
            if expired_session_token:
                logger.warning("Session expired, refreshing using token.")
                logged_in = self._refresh_token()
            else:
                self._update_headers(self.session_token)
                logged_in = True
        else:
            logged_in = self._refresh_token()

        if logged_in:
            self.successful_login = True
            if self.first_login:
                logger.info(f"Logged into mangadex.")
                print("Logged in.")
                self.first_login = False
        else:
            logger.critical("Couldn't login.")
            raise Exception("Couldn't login.")
