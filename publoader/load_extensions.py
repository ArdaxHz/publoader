import importlib.util
import logging
import sys
import traceback
from datetime import time, datetime, timezone
from typing import TYPE_CHECKING, List

from publoader.models.dataclasses import Chapter, Manga
from publoader.utils.config import DEFAULT_TIME, DEFAULT_CLEAN_DAY, ALL_DAYS, CLEAN_TIME
from publoader.utils.utils import root_path

if TYPE_CHECKING:
    import sqlite3

logger = logging.getLogger("publoader")


def validate_list_chapters(list_to_validate, list_elements_type):
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

    logger.debug(
        f"{length_correct_elements} out of {total_list_elements} elements in the correct format."
    )
    if list_elements_wrong:
        logger.warning(f"Skipping wrong type elements: {list_elements_wrong}")
    return list_elements_correct


def check_class_has_attribute(
    extension_name: str, extension_class, attribute: str, default=None
):
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
    for chapter in chapters:
        chapter.chapter_timestamp = chapter.chapter_timestamp.astimezone(
            tz=timezone.utc
        )
        if chapter.chapter_expire is not None:
            chapter.chapter_expire = chapter.chapter_expire.astimezone(tz=timezone.utc)


def load_extensions(
    database_connection: "sqlite3.Connection", clean_db: bool, general_run: bool
):
    updates = {}

    extensions_folder = root_path.joinpath("publoader", "extensions")
    for extension in [
        f for f in extensions_folder.iterdir() if f.is_dir() and f.name != "__pycache__"
    ]:
        extension_mainfile = extension.joinpath(f"{extension.name}.py")
        if not extension_mainfile.exists():
            logger.error(f"{extension.name} main file does not exist, skipping.")
            continue

        extension_name = f"extensions.{extension.name}"
        print(f"------Loading {extension_name}------")

        try:
            logger.info(f"------Loading {extension_name}------")

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
                continue

            run_extension, clean_db = check_extension_run(
                extension_name, extension_class, clean_db, general_run
            )
            if not run_extension:
                logger.info(
                    f"{extension_name} is not scheduled to run now: {datetime.now().isoformat()}."
                )
                print(
                    f"{extension_name} is not scheduled to run now: {datetime.now().isoformat()}."
                )
                continue

            name = check_class_has_attribute(extension_name, extension_class, "name")
            if name is None:
                continue
            else:
                name = str(name)

            posted_chapters_ids_data = database_connection.execute(
                "SELECT chapter_id FROM posted_ids  WHERE chapter_id IS NOT NULL AND extension_name = ?",
                (name,),
            )
            posted_chapters_ids = (
                [str(job["chapter_id"]) for job in posted_chapters_ids_data]
                if not clean_db
                else []
            )

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
                continue

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
                continue

            try:
                extension_languages = validate_list_chapters(extension_languages, str)
            except TypeError:
                logger.error(
                    f"{normalised_extension_name} extension languages is not a list, skipping."
                )
                continue

            if not isinstance(custom_regexes, dict):
                logger.error(
                    f"{normalised_extension_name} custom regexes is not a dict, initialising as dict."
                )
                custom_regexes = {}

            updates[extension_name] = {
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
            continue

    return updates


def check_extension_run(
    extension_name, extension_class, clean_db: bool, general_run: bool
):
    current_time = datetime.now()
    current_day = datetime.now().weekday()
    days_to_run = []
    time_to_run = check_class_has_method(extension_name, extension_class, "run_at")

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

    if days_to_clean_unsanitised is None:
        days_to_run.append(DEFAULT_CLEAN_DAY)

    cleaned_list = []
    if isinstance(days_to_clean_unsanitised, list):
        for elem in days_to_clean_unsanitised:
            try:
                cleaned_list.append(int(elem))
            except ValueError:
                pass

    if not cleaned_list:
        if days_to_clean_unsanitised:
            days_to_run.append(DEFAULT_CLEAN_DAY)
        else:
            days_to_run.extend(ALL_DAYS)
    else:
        days_to_run.extend(cleaned_list)

    run_extension = current_time.hour == time_to_run.hour
    day_to_run = current_day in days_to_run
    clean = (current_time.hour == CLEAN_TIME.hour) and day_to_run

    if clean:
        run_extension = clean

    if general_run:
        run_extension = general_run

    if clean_db:
        run_extension = clean_db
        clean = clean_db

    return run_extension, clean
