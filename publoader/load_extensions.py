import importlib.util
import logging
import sys
import traceback
from typing import TYPE_CHECKING

from publoader.models.dataclasses import Chapter, Manga
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


def load_extensions(database_connection: "sqlite3.Connection", clean_db: bool):
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
        logger.info(f"------Loading {extension_name}------")

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
                continue
            else:
                posted_chapters_ids_data = database_connection.execute(
                    "SELECT chapter_id FROM posted_ids  WHERE chapter_id IS NOT NULL AND extension_name = ?",
                    (extension_class.name,),
                )
                posted_chapters_ids = (
                    [str(job["chapter_id"]) for job in posted_chapters_ids_data]
                    if not clean_db
                    else []
                )
                extension_class.update_posted_chapter_ids(posted_chapters_ids)

                name = str(extension_class.name)
                normalised_extension_name = f"extensions.{name}"
                updated_chapters = extension_class.get_updated_chapters()
                all_chapters = extension_class.get_all_chapters()
                untracked_manga = extension_class.get_updated_manga()
                tracked_mangadex_ids = extension_class.tracked_mangadex_ids
                mangadex_group_id = str(extension_class.mangadex_group_id)
                custom_regexes = extension_class.custom_regexes
                extension_languages = extension_class.extension_languages

                try:
                    updated_chapters = validate_list_chapters(updated_chapters, Chapter)
                except TypeError:
                    logger.error(
                        f"{normalised_extension_name} updated chapters is not a list, skipping."
                    )
                    continue

                try:
                    all_chapters = validate_list_chapters(all_chapters, Chapter)
                except TypeError:
                    logger.error(
                        f"{normalised_extension_name} all chapters is not a list, initialising list as empty."
                    )
                    all_chapters = []

                try:
                    untracked_manga = validate_list_chapters(untracked_manga, Manga)
                except TypeError:
                    logger.error(
                        f"{normalised_extension_name} untracked manga is not a list, initialising list as empty."
                    )
                    untracked_manga = []

                try:
                    tracked_mangadex_ids = validate_list_chapters(
                        tracked_mangadex_ids, str
                    )
                except TypeError:
                    logger.error(
                        f"{normalised_extension_name} tracked mangadex ids is not a list, skipping."
                    )
                    continue

                try:
                    extension_languages = validate_list_chapters(
                        extension_languages, str
                    )
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
                }
        except Exception:
            traceback.print_exc()
            logger.exception(
                f"------{extension_name} raised an error."
            )
            continue

    return updates
