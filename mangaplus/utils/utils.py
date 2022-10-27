import configparser
import json
import logging
from datetime import date
from pathlib import Path
from typing import Dict, List, Optional


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


def format_log_dir_path(directory_name: str):
    log_folder_path = root_path.joinpath("logs").joinpath(directory_name)
    log_folder_path.mkdir(parents=True, exist_ok=True)
    return log_folder_path


log_folder_path = format_log_dir_path("bot")
webhook_log_folder_path = format_log_dir_path("webhook")


def setup_logs(
    logger_name: str = "mangaplus",
    path: Path = log_folder_path,
    logger_filename: str = "mplus_md_uploader",
):
    if logger_name == "mangaplus":
        add_to = ""
    else:
        add_to = f"{logger_name}_"
    filename = f"{logger_filename}_{add_to}{str(date.today())}.log"

    logs_path = path.joinpath(filename)
    fileh = logging.FileHandler(logs_path, "a")
    formatter = logging.Formatter(
        "%(asctime)s,%(msecs)d %(levelname)-8s [%(filename)s:%(lineno)d] %(message)s"
    )
    fileh.setFormatter(formatter)

    log = logging.getLogger(logger_name)  # root logger
    # for hdlr in log.handlers[:]:  # remove all old handlers
    #     if isinstance(hdlr, logging.FileHandler):
    #         log.removeHandler(hdlr)
    log.addHandler(fileh)
    log.setLevel(logging.DEBUG)


setup_logs()
logger = logging.getLogger("mangaplus")
setup_logs("debug")
logger_debug = logging.getLogger("mangaplus")
setup_logs(
    logger_name="webhook", path=webhook_log_folder_path, logger_filename="webhook"
)


def load_config_info(config: configparser.RawConfigParser):
    if config["Paths"].get("database_path", "") == "":
        logger.warning("Database path empty, using default.")
        config["Paths"]["database_path"] = "chapters.db"

    if config["Paths"].get("mangadex_api_url", "") == "":
        logger.warning("Mangadex api path empty, using default.")
        config["Paths"]["mangadex_api_url"] = "https://api.mangadex.org"

    if config["Paths"].get("manga_id_map_path", "") == "":
        logger.info("Manga id map path empty, using default.")
        config["Paths"]["manga_id_map_path"] = "manga.json"

    if config["Paths"].get("title_regex_path", "") == "":
        logger.info("Title regex map path empty, using default.")
        config["Paths"]["title_regex_path"] = "title_regex.json"

    if config["Paths"].get("mdauth_path", "") == "":
        logger.info("mdauth path empty, using default.")
        config["Paths"]["mdauth_path"] = ".mdauth"

    if config["Paths"].get("components_path", "") == "":
        logger.info("components path empty, using default.")
        config["Paths"]["components_path"] = "components_path"

    if config["Paths"].get("manga_data_path", "") == "":
        logger.info("Manga data path empty, using default.")
        config["Paths"]["manga_data_path"] = "manga_data.json"


def open_config_file() -> configparser.RawConfigParser:
    # Open config file and read values
    if config_file_path.exists():
        config = configparser.RawConfigParser()
        config.read(config_file_path)
        logger.info("Loaded config file.")
    else:
        logger.critical("Config file not found, exiting.")
        raise FileNotFoundError("Config file not found.")

    load_config_info(config)
    return config


def open_manga_id_map(manga_map_path: Path) -> Dict[str, List[int]]:
    """Open mangaplus id to mangadex id map."""
    try:
        with open(manga_map_path, "r") as manga_map_fp:
            manga_map = json.load(manga_map_fp)
        logger.info("Opened manga id map file.")
    except json.JSONDecodeError as e:
        logger.critical("Manga map file is corrupted.")
        raise json.JSONDecodeError(
            msg="Manga map file is corrupted.", doc=e.doc, pos=e.pos
        )
    except FileNotFoundError:
        logger.critical("Manga map file is missing.")
        raise FileNotFoundError("Couldn't file manga map file.")
    return manga_map


def open_title_regex(title_regex_path: Path) -> dict:
    """Open the chapter title regex."""
    try:
        with open(title_regex_path, "r") as title_regex_fp:
            title_regexes = json.load(title_regex_fp)
        logger.info("Opened title regex file.")
    except json.JSONDecodeError as e:
        logger.critical("Title regex file is corrupted.")
        raise json.JSONDecodeError(
            msg="Title regex file is corrupted.", doc=e.doc, pos=e.pos
        )
    except FileNotFoundError:
        logger.critical("Title regex file is missing.")
        raise FileNotFoundError("Couldn't file title regex file.")
    return title_regexes


def open_manga_data(manga_data_path: Path) -> Dict[str, dict]:
    """Open mangaplus id to mangadex id map."""
    manga_data = {}
    try:
        with open(manga_data_path, "r") as manga_data_fp:
            manga_data = json.load(manga_data_fp)
        logger.info("Opened manga data file.")
    except json.JSONDecodeError as e:
        logger.error("Manga data file is corrupted.")
    except FileNotFoundError:
        logger.error("Manga data file is missing.")
    return manga_data


config = open_config_file()
components_path = root_path.joinpath(config["Paths"]["components_path"])
components_path.mkdir(parents=True, exist_ok=True)

mangadex_api_url = config["Paths"]["mangadex_api_url"]
md_upload_api_url = f"{mangadex_api_url}/upload"
mplus_group_id = "4f1de6a2-f0c5-4ac5-bce5-02c7dbb67deb"

try:
    ratelimit_time = int(config["User Set"].get("mangadex_ratelimit_time", ""))
except (ValueError, KeyError):
    ratelimit_time = 2


try:
    upload_retry = int(config["User Set"].get("upload_retry", ""))
except (ValueError, KeyError):
    upload_retry = 3


def flatten(t: List[list]) -> list:
    """Flatten nested lists into one list."""
    return [item for sublist in t for item in sublist]


def get_md_id(manga_id_map: Dict[str, List[int]], mangaplus_id: int) -> Optional[str]:
    """Get the mangadex id from the mangaplus one."""
    for md_id in manga_id_map:
        if mangaplus_id in manga_id_map[md_id]:
            return md_id


def format_title(manga_data: dict) -> str:
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
