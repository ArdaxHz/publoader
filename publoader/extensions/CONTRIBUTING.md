# Contributing

This guide have some instructions and tips on how to create a new publisher extension. Please **read it carefully** if you're a new contributor or don't have any experience on the required languages and knowledges.

This guide is not definitive, and it's being updated over time. If you find any issue on it, feel free to open an issue or fix it directly yourself by opening a PR.

# Prerequisites

Before you start, please note that the ability to use following technologies is **required** and that existing contributors will not actively teach them to you.

- [Python 3.9+](https://www.python.org/)
- Any other scraper you need to use.

# Writing an extension

The quickest way to get started is to copy an existing extension's folder structure and renaming it as needed. We also recommend reading through a few existing extensions' code before you start.

## Setting up your extension directory

Each extension should reside in `publoader/extensions/<extension_name>`.

## Extension directory structure

The simplest extension structure looks like this:

```
publoader/extensions/<extension_name>
├── <extension_name>.py
├── manga_id_map.json
├── custom_regexes.json
├── requirements.txt
└── <any_file_or_dir_you_want>
```

#### <extension_name>.py
This is the entry point for your extension. The name of the file should match the name of the extension directory name.

#### manga_id_map.json
Can be any name. The MangaDex id to the publisher site's manga ids, or whatever id you will use to associate a chapter to a manga.
The structure of the file can be whatever you want, however you need to provide a list of tracked MangaDex ids.

#### custom_regexes.json
Can be any name and is not necessary. Your implementation should clean chapter titles to conform to MangaDex's rules. The regexes can be part of your code. For manga that has chapter titles that do not follow your regex implementation, you can use this file for custom title regexes.

Ids used are in this file are the series' id. Contains custom regexes for titles, and list of the same chapters uploaded on the publisher's site under different ids.
If you want to include this file, use the structure as follows:

```json
{
    "empty": [],
    "noformat": [],
    "custom": {},
    "same": {},
    "custom_language": {}
}
```
- `"empty": [],` Empty here, is an array of manga ids for chapters that will never have a title (null).
- `"noformat": [],` For titles that you do not want your titles regex to format.
- `"custom": {},` For series you want to use custom regex for. Should follow 
`"custom": {<series_id>: <regex>},`.
- `"same": {},` Chapters that are the same, but uploaded under different ids. Should follow `"same": {<chapter_to_keep_id>: [<other_chapter_id>]},`.
- `"custom_language": {}` For series that have languages that are not documented or follow your site's language specification.

## Dependencies

You can use whatever modules you want to, but remember to include a `requirements.txt` in your extension directory.

## Extension main class
The class that is used to read the chapter data from.

```python
class Extension:
    def __init__(self, extension_dirpath: Path):
        pass
```

---

### Main class key variables

| Field                  | Type        | Description                                                                                                     |
|------------------------|-------------|-----------------------------------------------------------------------------------------------------------------|
| `name`                 | `str`       | Name used in the database and in the logs. *This name should not be changed between versions.* |
| `mangadex_group_id`    | `str`       | MangaDex id of the group to upload to.                                                                          |
| `custom_regexes`       | `dict`      | Your custom regexes file after being opened and read.                                                           |
| `extension_languages`  | `List[str]` | A list of languages supported by the extension.                                                                 |
| `tracked_mangadex_ids` | `List[str]` | A list of MangaDex ids the extension tracks.                                                                    |

---

### Main class key methods
#### None of the following methods called by the bot should accept parameters.

- `get_updated_chapters(self) -> List[Chapter]` Returns a list of newly released chapters.
- `get_all_chapters(self) -> List[Chapter]` Returns all the chapters available for a series, uploaded or not uploaded. *If the site does not support retrieving all the available chapters for a series, this should return an empty array.*
- `get_updated_manga(self) -> List[Manga]` Returns a list of untracked newly added series.

***If these methods return anything other than a list of the `Chapter` class or the `Manga` class, they will be skipped.***

#### The following methods should accept the parameters specified. Your implementation of the parameters is to your discretion.

- `update_posted_chapter_ids(self, posted_chapter_ids: List[str]) -> None` Provides a list of chapter ids (as strings) already uploaded. You can use this list to retrieve the updated chapters list.

The list of chapters returned must be of the `Chapter` class. The chapter class is provided in the package `publoader.models.dataclasses`.
The chapter class **must** be initialised with the following values:

- `chapter_timestamp: datetime.datetime`. Datetime object of when the chapter was published.
- `chapter_expire: Optional[datetime.datetime]`. Datetime object of when the chapter expires.
- `chapter_title: Optional[str]`. Chapter title.
- `chapter_number: Optional[str]`. Chapter number, must follow the MangaDex chapter number regex.
- `chapter_language: str`. ISO-639-2 code.
- `chapter_volume: Optional[str]`. Chapter volume, null if the chapter has no volume.
- `chapter_id: str`. Chapter id.
- `chapter_url: str`. External chapter url.
- `manga_id: str`. The publisher's series id.
- `md_manga_id: str`. The MangaDex id to upload the chapter to.
- `manga_name: str`. The series name.
- `manga_url: str`. The series link.

---

### Extension module key variables

`__version__` must be provided to track the extension's version.

**The logger must be used.** Use the `setup_logs` function to set up your logger.

```python
from publoader.utils.logs import setup_logs, extensions_logs_folder_path

setup_logs(
    logger_name="extension_name",
    path=extensions_logs_folder_path.joinpath("extension_name"),
    logger_filename="extension_name",
)
```
You **must** use the `extensions_logs_folder_path` for the logs folder path.

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

```
from publoader.utils.utils import chapter_number_regex

chapter_number_regex.match("string")
```
provides the pattern used by MangaDex to validate the chapter number. 

---

# Submitting your extension
Open a PR from your repo to the Publoader master branch with your extension. Format the code using the [Black](https://pypi.org/project/black/) formatter with the default args. You must ensure your extension works, as erroneous extensions will be skipped.