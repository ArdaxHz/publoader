# Contributing

This guide have some instructions and tips on how to create a new publisher extension. Please **read it carefully** if you're a new contributor or don't have any experience on the required languages and knowledges.

This guide is not definitive, and it's being updated over time. If you find any issue on it, feel free to open an issue or fix it directly yourself by opening a PR.

# Prerequisites

Before you start, please note that the ability to use following technologies is **required** and that existing contributors will not actively teach them to you.

- [Python 3.9+](https://www.python.org/)
- Any other scraper you need to use.

# Writing an extension

The quickest way to get started is to copy an existing extension's folder structure and renaming it as needed. We also recommend reading through a few existing extensions' code before you start.

**You are responsible for implementing rate-limiting for your extension yourself.**

## Setting up your extension directory

Each extension should reside in `/publoader/extensions/<extension_name>`.

## Extension directory structure

The simplest extension structure looks like this:

```
/publoader/extensions/<extension_name>
├── <extension_name>.py
├── manga_id_map.json
├── custom_regexes.json
├── requirements.txt
└── <any_file_or_dir_you_want>
```

#### <extension_name>.py
This is the entry point for your extension. The name of the file should match the name of the extension directory.

#### manga_id_map.json
Can be any name. The MangaDex id to the publisher site's manga ids, or whatever id you will use to associate a chapter to a manga.
The structure of the file can be whatever you want, however you need to provide a list of tracked MangaDex manga ids.

#### custom_regexes.json
Can be any name and is not necessary. Your implementation should sanitise chapter titles to conform to MangaDex's rules.

You can use any means necessary to sanitise the chapter titles. If you use generic, catch-all regexes for the titles and a series' title does not conform to the specified regexes, this file can be used for custom regexes for select series.

_**The bot only accesses the `same` and `custom_language` fields, all other fields can be named differently.**_

If you want to include this file, use the structure as follows:

```json
{
    "empty": [],
    "noformat": [],
    "custom": {"series_id": "regex"},
    "same": {"chapter_to_keep_id": ["other_chapter_id"]},
    "custom_language": {}
}
```
- `"empty": [],` An array of manga ids for chapters that will never have a title (null).
- `"noformat": [],` For titles that you do not want your titles regex to format.
- `"custom": {},` For series you want to use custom regex for. If not, the dictionary should be empty.
- `"same": {},` Chapters that are the same, but uploaded under different ids. Chapters that are part of the dictionary's values are not uploaded and only the dictionary's keys are. The dictionary should be empty if this field is not applicable.
- `"custom_language": {}` For series that have languages that are not documented or follow your site's language specification.

## Dependencies

You can use whatever modules you want to, but remember to include a `requirements.txt` in your extension directory.

## Scheduling the extension for running
Add the time to run the extension in the file `/components/schedule.json`.
The dict should extend to the current file and should follow the format:
```
<extension_name>: {
  "hour": <24_hour_clock_int>,
  "minute": <int>,
}
```
The extension's name should be the same as the extension directory name and mainfile.

***This timings defined here ignore the `run_at` method defined in the extension.***

## Extension main class
The class that is used to read the chapter data from. This class **must** be named `Extension` and your extension will not run if this class is not available.

```python
class Extension:
    def __init__(self, extension_dirpath: Path, **kwargs):
        pass
```

---

### Main class key variables

| Field                  | Type        | Description                                                                                           |
|------------------------|-------------|-------------------------------------------------------------------------------------------------------|
| `name`                 | `str`       | Name used in the database and in the logs. Can contain `-` or `_`. *This name should not be changed.* |
| `mangadex_group_id`    | `str`       | MangaDex id of the group to upload to.                                                                |
| `custom_regexes`       | `dict`      | Your custom regexes file after being opened and read. If not used, return an empty dict `{}`.         |
| `extension_languages`  | `List[str]` | A list of languages supported by the extension.                                                       |
| `tracked_mangadex_ids` | `List[str]` | A list of MangaDex manga ids the extension uploads to.                                                |
| `disabled`             | `bool`      | If the extension is active to run or skipped. *If missing this will be True.*                         |

---

### Main class key methods
#### None of the following methods called by the bot should accept parameters.

- `get_updated_chapters(self) -> List[Chapter]` Returns a list of newly released chapters.
- `get_all_chapters(self) -> List[Chapter]` Returns all the chapters available for a series, uploaded or not uploaded. *If the site does not support retrieving all the available chapters for a series, this should return an empty array.*
- `get_updated_manga(self) -> List[Manga]` Returns a list of untracked newly added series.
- `run_at(self) -> datetime.time` A time object of when you want the extension to be run. As the bot is run hourly, having the minute set as anything other than zero will not run the extension. 
- `clean_at(self) -> Optional[List[int]]` The days you want to run the extension as if it is a fresh run. This allows the bot to check for duplicate chapters, chapters not uploaded and chapters needing to be deleted. Allowed values: `None` to disable this, `[]` for the default day (wednesday), an int value in the range 0-6 (inclusive) for the day of the week, e.g. `[0, 3]` for mondays and thursdays.
- `daily_check_run(self) -> bool` If you want the bot to run daily at 1am to catch any chapters that may have not been uploaded.

***If the chapter and manga methods do not return the correct type, the extension run will be skipped.*** 

#### The following methods should accept the parameters specified. Your implementation of the parameters is to your discretion.

- `update_external_data(self, posted_chapter_ids: List[str], fetch_all_chapters: bool, **kwargs) -> None` Provides data to use before starting the fetch of chapters. `posted_chapter_ids` provides the ids of chapters already uploaded. `fetch_all_chapters` is `True` if the bot is going through the clean cycle. *****kwargs needs to be implemented.***  


The list of chapters returned must be of the `Chapter` class. The chapter class is provided in the package `publoader.models.dataclasses`.
The chapter class contains the following fields:

Fields with `Optional[]` can be left as null, fields without must be populated.

- `chapter_timestamp: datetime.datetime`. Datetime object of when the chapter was published. This is updated to be timezone-aware.
- `chapter_expire: Optional[datetime.datetime]`. Datetime object of when the chapter expires, if the chapter does not expire, this can be null. This is updated to be timezone-aware.
- `chapter_title: Optional[str]`. Chapter title.
- `chapter_number: Optional[str]`. Chapter number, must follow the MangaDex chapter number regex.
- `chapter_language: str`. ISO-639-2 code.
- `chapter_volume: Optional[str]`. Chapter volume. If the series uses seasons, use this field. Keep empty if the chapter does not have a volume.
- `chapter_id: str`. Chapter id.
- `chapter_url: str`. Chapter link.
- `manga_id: str`. The publisher's series id.
- `md_manga_id: str`. The MangaDex manga id to upload the chapter to.
- `manga_name: str`. The series name.
- `manga_url: str`. The series link.

---

### Extension module key variables

`__version__` must be provided to track the extension's version.

**The logger must be used.** Use the `setup_logs` function to set up your logger.

```python
from publoader.utils.logs import setup_extension_logs

setup_extension_logs(
    logger_name="extension_name",
    logger_filename="extension_name",
)
```

---

### Functions provided for use

```python
from publoader.utils.utils import open_manga_id_map, open_title_regex

manga_id_map = open_manga_id_map(file_path: Path)
custom_regexes = open_title_regex(file_path: Path)
```

```python
from publoader.utils.misc import find_key_from_list_value

dictionary_key = find_key_from_list_value(dict_to_search: Dict[str, List[str]], list_element: str)
```
This function returns the dictionary key after lookup in the dictionary values' arrays.

### Variables provided for use

```python
from publoader.utils.utils import chapter_number_regex

chapter_number_regex.match("string")
```
provides the pattern used by MangaDex to validate the chapter number. 

---

# Submitting your extension
Open a PR from your repo to the Publoader master branch with your extension. Format the code using the [Black](https://pypi.org/project/black/) formatter with the default args. You must ensure your extension works, as erroneous extensions will be skipped.
