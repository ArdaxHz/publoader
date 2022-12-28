import json
import logging
import re
from pathlib import Path
from typing import Dict, List

logger = logging.getLogger("mangaplus")

root_path = Path(".")
mplus_language_map = {
    "0": "en",
    "1": "es-la",
    "2": "fr",
    "3": "id",
    "4": "pt-br",
    "5": "ru",
    "6": "th",
}


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


mplus_group_id = "4f1de6a2-f0c5-4ac5-bce5-02c7dbb67deb"
mplus_url_regex = re.compile(
    r"(?:https\:\/\/mangaplus\.shueisha\.co\.jp\/viewer\/)(\d+)", re.I
)
chapter_number_regex = re.compile(r"^(0|[1-9]\d*)((\.\d+){1,2})?[a-z]?$", re.I)
EXPIRE_TIME = 946684799
