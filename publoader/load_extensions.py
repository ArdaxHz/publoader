import datetime
import importlib.util
import logging
import string
import sys
import traceback
from datetime import time, timezone, timedelta
from pathlib import Path
from typing import TYPE_CHECKING, List

from publoader.models.dataclasses import Chapter, Manga
from publoader.utils.config import DEFAULT_TIME, DEFAULT_CLEAN_DAY, ALL_DAYS, CLEAN_TIME
from publoader.utils.utils import root_path, get_current_datetime

if TYPE_CHECKING:
    import sqlite3

logger = logging.getLogger("publoader")
extensions_folder = root_path.joinpath("publoader", "extensions")


def validate_list_chapters(list_to_validate, list_elements_type):
    """Check if variable is a list and the contents of the list are of the specified type."""
    if not isinstance(list_to_validate, list):
        raise TypeError("Specified list is not a list.")

    list_elements_correct = [
        item for item in list_to_validate if isinstance(item, list_elements_type)
    ]
    list_elements_wrong = [
        item for item in list_to_validate if not isinstance(item, list_elements_type)
    ]

    length_correct_elements = len(list_elements_correct)
    total_list_elements = len(list_to_validate)

    if length_correct_elements != total_list_elements:
        logger.debug(
            f"{length_correct_elements} out of {total_list_elements} elements in the correct format."
        )

    if list_elements_wrong:
        logger.warning(f"Skipping wrong type elements: {list_elements_wrong}")
    return list_elements_correct


def check_class_has_attribute(
    extension_name: str, extension_class, attribute: str, default=None
):
    """Check if the class has the attribute and return default if not."""
    attribute_class = getattr(extension_class, attribute, None)
    if attribute_class is not None:
        return attribute_class

    logger.error(
        f"{extension_name} class doesn't have the {attribute} attribute, using default value."
    )
    return default


def check_class_has_method(
    extension_name: str, extension_class, method: str, default=None, run=True, **kwargs
):
    """Check if the class has the method and return default if not."""
    method_class = getattr(extension_class, method, None)
    if method_class is not None:
        if callable(method_class):
            if run:
                return method_class(**kwargs)
            else:
                return method_class

    logger.info(
        f"{extension_name} doesn't have the {method} method, using default return value."
    )
    return default


def convert_chapters_datetimes(chapters: List[Chapter]):
    """Convert all the chapter objects to be timezone-aware."""
    for chapter in chapters:
        chapter.chapter_timestamp = chapter.chapter_timestamp.astimezone(
            tz=timezone.utc
        )
        if chapter.chapter_expire is not None:
            chapter.chapter_expire = chapter.chapter_expire.astimezone(tz=timezone.utc)


def check_run_in_range(time_to_run):
    """Return true if time_to_run is in the range +- 5 minutes from now."""
    now = get_current_datetime()
    start = (now - timedelta(minutes=5)).time()
    end = (now + timedelta(minutes=5)).time()
    return (time_to_run.hour == now.hour) and (start <= time_to_run <= end)


def check_extension_run(
    extension_name, extension_class, clean_db: bool, general_run: bool
):
    """Check if an extension is scheduled to run."""
    current_time = get_current_datetime()
    current_day = get_current_datetime().weekday()
    days_to_run = []
    time_to_run = check_class_has_method(extension_name, extension_class, "run_at")
    daily_check_run = check_class_has_method(
        extension_name, extension_class, "daily_check_run", default=False
    )

    if not isinstance(time_to_run, time):
        time_to_run = DEFAULT_TIME

    time_to_run_datetime = current_time.replace(
        hour=time_to_run.hour, minute=time_to_run.minute, tzinfo=time_to_run.tzinfo
    )
    time_to_run_datetime.astimezone(tz=timezone.utc)
    time_to_run = time_to_run_datetime.time()

    days_to_clean_unsanitised = check_class_has_method(
        extension_name, extension_class, "clean_at"
    )

    if not isinstance(days_to_clean_unsanitised, (list, type(None))):
        days_to_clean_unsanitised = None

    # if days_to_clean_unsanitised is None:
    #     days_to_run.append(DEFAULT_CLEAN_DAY)

    cleaned_list = []
    if isinstance(days_to_clean_unsanitised, list):
        for elem in days_to_clean_unsanitised:
            try:
                cleaned_list.append(int(elem))
            except ValueError:
                pass

    if not cleaned_list:
        days_to_run.append(DEFAULT_CLEAN_DAY)
    else:
        days_to_run.extend(cleaned_list)

    run_extension = check_run_in_range(time_to_run)

    day_to_run = current_day in days_to_run
    time_to_clean = check_run_in_range(CLEAN_TIME)
    clean = time_to_clean and day_to_run

    if time_to_clean:
        logger.info(
            f"Time to clean: Status {clean=} and {daily_check_run=} for {extension_name}"
        )
        if daily_check_run:
            run_extension = True

    if clean:
        run_extension = True

    if general_run:
        run_extension = True

    if clean_db:
        run_extension = True
        clean = True

    return run_extension, clean, time_to_run


def load_extension(extension: Path, clean_db: bool = False, general_run: bool = False):
    """Load the extension."""
    extension_mainfile = extension.joinpath(f"{extension.name}.py")
    if not extension_mainfile.exists():
        logger.error(f"{extension.name} main file does not exist, skipping.")
        return

    extension_name = f"extensions.{extension.name}"
    print(f"------Loading {extension_name}------")

    try:
        spec = importlib.util.spec_from_file_location(
            extension_name, extension_mainfile
        )
        foo = importlib.util.module_from_spec(spec)
        sys.modules[extension_name] = foo
        spec.loader.exec_module(foo)

        try:
            extension_class = foo.Extension(extension)
        except NameError:
            logger.error(f"{extension_name} doesn't have the Extension class")
            return

        run_extension, clean_db, run_at = check_extension_run(
            extension_name, extension_class, clean_db, general_run
        )
        if not run_extension and not clean_db:
            print(
                f"{extension_name} is not scheduled to run now: {datetime.datetime.now()}"
            )
            return

        print(f"{clean_db=} for {extension_name}")
        return {
            "extension": extension_class,
            "clean_db": clean_db,
            "extension_name": extension_name,
            "run_at": run_at,
        }
    except Exception:
        traceback.print_exc()
        logger.exception(f"------{extension_name} raised an error.")
        return


def load_extensions(names=None, clean_db: bool = False, general_run: bool = False):
    """Load all the extensions in the extensions folder."""
    updates = {}

    for extension in [
        f for f in extensions_folder.iterdir() if f.is_dir() and f.name != "__pycache__"
    ]:
        if names is not None:
            if extension.name not in names:
                continue
            else:
                general_run = True

        data = load_extension(extension, clean_db=clean_db, general_run=general_run)
        if data is not None and data:
            updates[data["extension_name"]] = data
    return updates


def read_extension(name: str, clean_db: bool = False):
    """Load a specific extension."""
    extension_folder = extensions_folder.joinpath(name)
    if not extension_folder.exists():
        raise FileNotFoundError(f"extensions.{name} not found.")

    return load_extension(extension_folder, clean_db=clean_db, general_run=True)


def run_extension(
    extension: dict,
    database_connection: "sqlite3.Connection",
    clean_db_override: bool = False,
):
    """Run a single extension."""
    extension_class = extension["extension"]
    clean_db = extension["clean_db"]
    extension_name = extension["extension_name"]

    if clean_db_override:
        clean_db = True

    try:
        logger.info(f"Running {extension_name}.")

        name = check_class_has_attribute(extension_name, extension_class, "name")
        if name is None:
            return
        else:
            name = str(name)

        if any(x in string.punctuation for x in name) or " " in name:
            logger.error(f"{name} contains either punctuation or a space.")
            print(f"{name} contains either punctuation or a space.")
            return

        posted_chapters_ids = database_connection[f"uploaded"].find(
            {"extension": {"$eq": name}}, ["chapter_id"]
        )

        # posted_chapters_ids = posted_chapters_ids_data.to_list(length=None)

        update_posted_chapter_ids = check_class_has_method(
            extension_name, extension_class, "update_posted_chapter_ids", run=False
        )
        if update_posted_chapter_ids is None:
            logger.info(
                f"{extension_name} update_posted_chapter_ids method does not exist, not providing already, uploaded values."
            )
        else:
            update_posted_chapter_ids(posted_chapters_ids)

        normalised_extension_name = f"extensions.{name}"
        updated_chapters = check_class_has_method(
            extension_name, extension_class, "get_updated_chapters", default=[]
        )
        all_chapters = check_class_has_method(
            extension_name, extension_class, "get_all_chapters", default=[]
        )
        untracked_manga = check_class_has_method(
            extension_name, extension_class, "get_updated_manga", default=[]
        )
        tracked_mangadex_ids = check_class_has_attribute(
            extension_name, extension_class, "tracked_mangadex_ids", default=[]
        )
        mangadex_group_id = check_class_has_attribute(
            extension_name, extension_class, "mangadex_group_id"
        )
        custom_regexes = check_class_has_attribute(
            extension_name, extension_class, "custom_regexes", default={}
        )
        extension_languages = check_class_has_attribute(
            extension_name, extension_class, "extension_languages", default=[]
        )

        if mangadex_group_id is not None:
            mangadex_group_id = str(mangadex_group_id)

        try:
            updated_chapters = validate_list_chapters(updated_chapters, Chapter)
        except TypeError:
            logger.error(
                f"{normalised_extension_name} updated chapters is not a list, skipping."
            )
            return

        convert_chapters_datetimes(updated_chapters)

        try:
            all_chapters = validate_list_chapters(all_chapters, Chapter)
        except TypeError:
            logger.error(
                f"{normalised_extension_name} all chapters is not a list, initialising list as empty."
            )
            all_chapters = []

        convert_chapters_datetimes(all_chapters)

        try:
            untracked_manga = validate_list_chapters(untracked_manga, Manga)
        except TypeError:
            logger.error(
                f"{normalised_extension_name} untracked manga is not a list, initialising list as empty."
            )
            untracked_manga = []

        try:
            tracked_mangadex_ids = validate_list_chapters(tracked_mangadex_ids, str)
        except TypeError:
            logger.error(
                f"{normalised_extension_name} tracked mangadex ids is not a list, skipping."
            )
            return

        try:
            extension_languages = validate_list_chapters(extension_languages, str)
        except TypeError:
            logger.error(
                f"{normalised_extension_name} extension languages is not a list, skipping."
            )
            return

        if not isinstance(custom_regexes, dict):
            logger.error(
                f"{normalised_extension_name} custom regexes is not a dict, initialising as dict."
            )
            custom_regexes = {}

        return {
            "extension": extension_class,
            "name": name,
            "normalised_extension_name": normalised_extension_name,
            "updated_chapters": updated_chapters,
            "all_chapters": all_chapters,
            "untracked_manga": untracked_manga,
            "tracked_mangadex_ids": tracked_mangadex_ids,
            "mangadex_group_id": mangadex_group_id,
            "custom_regexes": custom_regexes,
            "extension_languages": extension_languages,
            "posted_chapters_ids": posted_chapters_ids,
            "clean_db": clean_db,
        }
    except Exception:
        traceback.print_exc()
        logger.exception(f"------{extension_name} raised an error.")
        return


def run_extensions(
    extensions: dict, database_connection: "sqlite3.Connection", clean_db_override: bool
):
    """Run the extensions to get the updates."""
    updates = {}
    for site in extensions:
        extension = extensions[site]
        data = run_extension(
            extension,
            database_connection=database_connection,
            clean_db_override=clean_db_override,
        )

        if data is not None and data:
            updates[site] = data

    return updates
