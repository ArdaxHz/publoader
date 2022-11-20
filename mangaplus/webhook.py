import datetime
import logging
import math
import time
from enum import Enum
from json import JSONDecodeError
from typing import TYPE_CHECKING, Dict, List, Optional, Union

from discord_webhook import DiscordEmbed, DiscordWebhook

from .utils.utils import config

if TYPE_CHECKING:
    from mangaplus import Chapter

logger = logging.getLogger("webhook")


def make_webhook():
    return DiscordWebhook(url=config["Paths"]["webhook_url"], rate_limit_retry=True)


webhook = make_webhook()


class LinkToFormatType(Enum):
    MANGADEX_MANGA = "MangaDex manga"
    MANGADEX_CHAPTER = "MangaDex chapter"
    MANGAPLUS_MANGA = "MangaPlus manga"
    MANGAPLUS_CHAPTER = "MangaPlus chapter"


class WebhookHelper:
    def __init__(self) -> None:
        self.colour = "B86F8C"
        self.mangadex_chapter_url = "https://mangadex.org/chapter/{}"
        self.mangaplus_manga_url = "https://mangaplus.shueisha.co.jp/titles/{}"
        self.mangaplus_chapter_url = "https://mangaplus.shueisha.co.jp/viewer/{}"
        self.mangadex_manga_url = "https://mangadex.org/manga/{}"

    def _format_link(
        self,
        type: LinkToFormatType,
        id_to_use: Optional[str],
        skip_chapter_id: bool = False,
    ):
        url = ""
        name = type.value

        if skip_chapter_id or id_to_use is None:
            return ""

        if type == LinkToFormatType.MANGADEX_MANGA:
            url = self.mangadex_manga_url.format(id_to_use)
        elif type == LinkToFormatType.MANGADEX_CHAPTER:
            url = self.mangadex_chapter_url.format(id_to_use)
        elif type == LinkToFormatType.MANGAPLUS_MANGA:
            url = self.mangaplus_manga_url.format(id_to_use)
        else:
            url = self.mangaplus_chapter_url.format(id_to_use)

        return f"{name} link: [here]({url})\n"

    def normalise_chapter(
        self,
        chapter: Union["Chapter", dict],
        failed_upload: bool = False,
        inline: bool = True,
    ) -> Dict[str, str]:

        if not isinstance(chapter, dict):
            chapter = vars(chapter)

        name = f"Chapter: {chapter.get('chapter_number')}\nLanguage: {chapter.get('chapter_language')}"
        value = (
            f"{self._format_link(LinkToFormatType.MANGADEX_CHAPTER, chapter.get('md_chapter_id'), failed_upload)}"
            f"{self._format_link(LinkToFormatType.MANGAPLUS_CHAPTER, chapter.get('chapter_id'))}"
            f"Chapter title: `{chapter.get('chapter_title')}`\n"
            f"Chapter expiry: `{datetime.datetime.fromtimestamp(chapter.get('chapter_expire', 946684799)).isoformat()}`\n"
            f"{self._format_link(LinkToFormatType.MANGADEX_MANGA, chapter.get('md_manga_id'), failed_upload)}"
            f"{self._format_link(LinkToFormatType.MANGAPLUS_MANGA, chapter.get('manga_id'))}"
        )

        return {"name": name, "value": value, "inline": inline}

    def normalise_chapters(self, chapters, failed_upload: bool = False):
        normalised_chapters = [
            self.normalise_chapter(chapter, failed_upload) for chapter in chapters
        ]
        return [
            normalised_chapters[l : l + 25]
            for l in range(0, len(normalised_chapters), 25)
        ]

    def _calculate_embed_size(self, embed: Union[DiscordEmbed, dict]):
        if isinstance(embed, DiscordEmbed):
            embed_dict = embed.__dict__
        else:
            embed_dict = embed

        embed_len = len(embed_dict.get("title", "") or "")
        embed_len += len(embed_dict.get("description", "") or "")
        if embed_dict.get("footer") is not None:
            embed_len += len(embed_dict["footer"].get("text", "") or "")

        fields = embed_dict["fields"]
        embed_len += sum(
            [
                len(field.get("name", "") or "") + len(field.get("value", "") or "")
                for field in fields
            ]
        )
        return embed_len

    def _make_multiple_embeds(self, embed: dict, list_fields: List[List[dict]]):
        new_embeds = []
        for fields in list_fields:
            new_embed = DiscordEmbed()
            new_embed.__dict__.update(embed)
            new_embed.fields = fields

            embed_len = self._calculate_embed_size(new_embed)
            new_embed_split = self._check_embed_length(new_embed, embed_len)
            if new_embed_split is None:
                new_embeds.append(new_embed)
            else:
                new_embeds.extend(new_embed_split)
        return new_embeds

    def _check_embed_length(self, embed, embed_len):
        if embed_len >= 6000:
            num_fields = len(embed["fields"])
            splitter = math.cail(len(num_fields) / 2)
            fields_split = [
                num_fields[l : l + splitter]
                for l in range(0, len(num_fields), splitter)
            ]

            split_embeds = self._make_multiple_embeds(embed, fields_split)
            return split_embeds
        return None

    def check_embeds_size(self, local_webhook: DiscordWebhook):
        embeds = local_webhook.get_embeds()
        for index, embed in enumerate(embeds):
            embed_len = self._calculate_embed_size(embed)
            split_embeds = self._check_embed_length(embed, embed_len)

            if split_embeds is not None:
                local_webhook.embeds.pop(index)
                local_webhook.embeds[index:index] = split_embeds

    def send_webhook(self, local_webhook: DiscordWebhook = webhook):
        if local_webhook.embeds:
            self.check_embeds_size(local_webhook)

            embeds_split = [
                local_webhook.embeds[l : l + 10]
                for l in range(0, len(local_webhook.embeds), 10)
            ]
            local_webhook.embeds.clear()

            for count, embed in enumerate(embeds_split, start=1):
                local_webhook.embeds = embed
                response = local_webhook.execute(remove_embeds=True)
                try:
                    if isinstance(response, list):
                        status_codes = [r.status_code for r in response]
                        messages = [r.json() for r in response]
                        logger.info(f"Discord API returned: {status_codes}, {messages}")
                    else:
                        logger.info(
                            f"Discord API returned: {response.status_code}, {response.json()}"
                        )
                except (JSONDecodeError, AttributeError, KeyError) as e:
                    logger.error(e)

                if count < len(embeds_split):
                    time.sleep(1)


class WebhookBase(WebhookHelper):
    def __init__(
        self,
        manga: dict,
    ) -> None:
        super().__init__()
        self.manga = manga
        logger.debug(f"Making embed for manga {self.manga}")
        self.manga_id = manga["id"]
        self.manga_title = manga["title"]
        self.mangadex_manga_url = self.mangadex_manga_url.format(self.manga_id)

    def make_embed(self, embed_data: Optional[dict] = None) -> DiscordEmbed:
        # if embed_data is None:
        #     embed_data = self.normalised_manga

        embed = DiscordEmbed(**embed_data)
        embed.set_title(embed_data.get("title", None))
        embed.set_description(embed_data.get("description", None))
        logger.debug(f"Made embed: {embed.title}, {embed.description}")
        return embed

    def add_fields_to_embed(
        self, embed: "DiscordEmbed", normalised_chapters: List[dict]
    ):
        logger.debug(f"Adding chapters to embed {embed.title}: {normalised_chapters}")
        for c in normalised_chapters:
            embed.add_embed_field(**c)


class MPlusBotUpdatesWebhook(WebhookBase):
    def __init__(
        self,
        manga: dict,
        chapters: List["Chapter"],
        failed_chapters: List["Chapter"],
        skipped: int,
    ) -> None:
        super().__init__(manga)

        self.chapters: List["Chapter"] = chapters
        self.failed_chapters = failed_chapters

        self.uploaded = len(chapters)
        self.failed = len(self.failed_chapters)
        self.skipped = skipped

        self.normalised_manga = self.normalise_manga(
            self.uploaded, self.failed, self.skipped
        )
        self.normalised_chapters = self.normalise_chapters(self.chapters)
        self.normalised_failed_chapters = self.normalise_chapters(
            self.failed_chapters, failed_upload=True
        )

    def normalise_manga(
        self, chapter_count: int, failed: int, skipped: int
    ) -> Dict[str, str]:
        return {
            "title": f"{self.manga_title}",
            "description": f"MangaDex manga link: [here]({self.mangadex_manga_url})\n"
            f"Uploaded: {chapter_count}\n"
            f"Failed: {failed}\n"
            f"Skipped: {skipped}",
            "timestamp": datetime.datetime.now().isoformat(),
            "color": self.colour,
        }

    def format_embed(self, chapters_to_use: List[List[dict]]):
        for list in chapters_to_use:
            embed = self.make_embed(self.normalised_manga)
            self.add_fields_to_embed(embed, list)

            if list:
                webhook.add_embed(embed)

            if len(webhook.embeds) >= 10 or len(embed.fields) >= 5:
                self.send_webhook()

    def main(self, last_manga: bool = True):
        if self.uploaded > 0 or self.failed > 0:
            self.send_webhook()

        if self.chapters:
            self.format_embed(self.normalised_chapters)
        if self.failed_chapters:
            self.format_embed(self.normalised_failed_chapters)

        if not self.chapters and not self.failed_chapters:
            embed = self.make_embed(self.normalised_manga)
            webhook.add_embed(embed)

        if last_manga:
            embed = self.make_embed(
                {"title": "Finished Getting all chapter updates.", "color": self.colour}
            )
            webhook.add_embed(embed)

        if self.uploaded > 0 or self.failed > 0:
            self.send_webhook()
        else:
            if len(webhook.embeds) >= 10:
                webhook_embeds = [
                    webhook.embeds[l : l + 10]
                    for l in range(0, len(webhook.embeds), 10)
                ]
                for embed_list in webhook_embeds:
                    webhook.embeds = embed_list
                    if len(webhook.embeds) >= 10:
                        self.send_webhook()

            if last_manga:
                self.send_webhook()


class MPlusBotDupesWebhook(WebhookBase):
    def __init__(self, manga: Optional[dict] = None) -> None:
        self.normalised_manga = None
        self.manga = manga
        if manga is not None:
            self.init_manga(manga)

        self.chapters = []

    def init_manga(self, manga: Optional[dict]):
        if manga is not None:
            super().__init__(manga)
            self.colour = "C8AA69"
            self.normalised_manga = self.normalise_manga()

    def normalise_manga(self) -> Dict[str, str]:
        return {
            "title": f"Dupes in: {self.manga_title}",
            "description": f"""MangaDex manga link: [here]({self.mangadex_manga_url})""",
            "timestamp": datetime.datetime.now().isoformat(),
            "color": self.colour,
        }

    def add_chapters(self, main_chapter: dict, chapters: List[dict]):
        self.chapters.append(
            {
                "name": f"Dupes of chapter: {main_chapter['id']}\n"
                f"Chapter Number: {main_chapter['attributes']['chapter']}\n"
                f"Chapter Language: {main_chapter['attributes']['translatedLanguage']}",
                "value": self.normalise_chapters(chapters),
            }
        )

    def normalise_chapters(self, chapters: List[dict]) -> str:
        return "\n".join([f'`{chapter["id"]}`' for chapter in chapters])

    def main(self):
        if self.normalised_manga is not None:
            logger.info(self.normalised_manga)
            embed = self.make_embed(self.normalised_manga)
            self.add_fields_to_embed(embed, self.chapters)
            logger.info(self.chapters)

            if self.chapters:
                webhook.add_embed(embed)
                self.send_webhook()


class MPlusBotDeleterWebhook(WebhookHelper):
    def __init__(self, chapter: dict) -> None:
        super().__init__()
        self.colour = "C43542"
        self.chapter = chapter
        self.webhook = make_webhook()
        self.normalised_chapter = self.normalise_chapter(self.chapter)

    def make_embed(self):
        embed = DiscordEmbed(
            title=f"Deleted chapter {self.chapter['md_chapter_id']}",
            description=f"{self.normalised_chapter['name']}\n{self.normalised_chapter['value']}",
            **{
                "color": self.colour,
                "timestamp": datetime.datetime.now().isoformat(),
            },
        )

        logger.debug(f"Made embed: {embed.title}, {embed.description}")
        return embed

    def main(self):
        embed = self.make_embed()
        self.webhook.add_embed(embed)
        self.send_webhook(self.webhook)


class MPlusBotNotIndexedWebhook(WebhookHelper):
    def __init__(self, chapter_ids: List[str]) -> None:
        super().__init__()
        self.chapter_ids = chapter_ids
        self.colour = "45539B"

    def make_embed(self, **embed_data):
        embed = DiscordEmbed(
            title=embed_data["title"],
            description=embed_data["description"],
            **{
                "color": self.colour,
                "timestamp": datetime.datetime.now().isoformat(),
            },
        )

        logger.debug(f"Made embed: {embed.title}, {embed.description}")
        return embed

    def main(self):
        title = (
            f"Chapter ids not indexed:" if self.chapter_ids else f"All chapters indexed"
        )
        description = (
            "\n".join([f"`{chapter_id}`" for chapter_id in self.chapter_ids])
            if self.chapter_ids
            else None
        )

        embed = self.make_embed(title=title, description=description)
        webhook.add_embed(embed)
        self.send_webhook()


class MPlusBotWebhook(WebhookHelper):
    def __init__(self, **kwargs) -> None:
        super().__init__()
        self.embed_title = kwargs.get("title")
        self.embed_description = kwargs.get("description")
        self.embed_colour = kwargs.get("colour")

    def main(self, **kwargs):
        self.embed = DiscordEmbed(
            title=self.embed_title,
            description=self.embed_description,
            timestamp=datetime.datetime.now().isoformat(),
            color=self.embed_colour or self.colour,
        )
        webhook.add_embed(self.embed)

    def send(self, **kwargs):
        self.main()
        self.send_webhook()


if __name__ == "__main__":
    print("Please run this file through the bot.")
