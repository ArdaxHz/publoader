import configparser
import logging
from calendar import WEDNESDAY
from datetime import time

from publoader.utils.utils import root_path

logger = logging.getLogger("publoader")


def load_config_info(config: configparser.RawConfigParser):
    if config["Paths"].get("database_path", "") == "":
        logger.warning("Database path empty, using default.")
        config["Paths"]["database_path"] = "chapters.db"

    if config["Paths"].get("mangadex_api_url", "") == "":
        logger.warning("Mangadex api path empty, using default.")
        config["Paths"]["mangadex_api_url"] = "https://api.mangadex.org"

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


config_file_path = root_path.joinpath("config").with_suffix(".ini")
config = open_config_file()
components_path = root_path.joinpath(config["Paths"]["components_path"])
components_path.mkdir(parents=True, exist_ok=True)

mangadex_api_url = config["Paths"]["mangadex_api_url"]
md_upload_api_url = f"{mangadex_api_url}/upload"

try:
    ratelimit_time = int(config["User Set"].get("mangadex_ratelimit_time", ""))
except (ValueError, KeyError):
    ratelimit_time = 2


try:
    upload_retry = int(config["User Set"].get("upload_retry", ""))
except (ValueError, KeyError):
    upload_retry = 3

try:
    max_requests = int(config["User Set"].get("max_requests", ""))
except (ValueError, KeyError):
    max_requests = 5


try:
    max_log_days = int(config["User Set"].get("max_log_days", ""))
except (ValueError, KeyError):
    max_log_days = 30


try:
    daily_run_time_daily_hour = int(
        config["User Set"].get("bot_run_time_daily", "").split(":")[0]
    )
except (ValueError, KeyError):
    daily_run_time_daily_hour = 15

try:
    daily_run_time_daily_minute = int(
        config["User Set"].get("bot_run_time_daily", "").split(":")[1]
    )
except (ValueError, KeyError):
    daily_run_time_daily_minute = 0

try:
    daily_run_time_checks_hour = int(
        config["User Set"].get("bot_run_time_checks", "").split(":")[0]
    )
except (ValueError, KeyError):
    daily_run_time_checks_hour = 0

try:
    daily_run_time_checks_minute = int(
        config["User Set"].get("bot_run_time_checks", "").split(":")[1]
    )
except (ValueError, KeyError):
    daily_run_time_checks_minute = 0

DEFAULT_TIME = time(hour=daily_run_time_daily_hour, minute=daily_run_time_daily_minute)
CLEAN_TIME = time(hour=daily_run_time_checks_hour, minute=daily_run_time_checks_minute)
DEFAULT_CLEAN_DAY = WEDNESDAY
ALL_DAYS = range(7)
