import datetime
import json
import logging
import re
from pathlib import Path
from typing import Dict, List

logger = logging.getLogger("publoader")

root_path = Path(".")


def open_manga_id_map(manga_map_path: Path) -> Dict[str, List[int]]:
    """Open external id to mangadex id map."""
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
        return {}
    except FileNotFoundError:
        logger.critical("Title regex file is missing.")
        return {}
    return title_regexes


def open_manga_data(manga_data_path: Path) -> Dict[str, dict]:
    """Open MangaDex titles data."""
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


chapter_number_regex = re.compile(r"^(0|[1-9]\d*)((\.\d+){1,2})?[a-z]?$", re.I)
EXPIRE_TIME = datetime.datetime(year=1990, month=1, day=1)