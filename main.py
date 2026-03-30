import os
import asyncio
import logging
import shutil
from urllib.parse import urlparse, parse_qs

import aiofiles
import aiofiles.os
from dotenv import load_dotenv
from tqdm import tqdm
from telethon import TelegramClient, functions, events
from telethon.sessions import StringSession
from helpers.download_helper import TgFileDownloader
from helpers.extract_helper import ArchiveExtractor
from helpers.ulp_helper import StealerLogParser
from helpers.database_helper import DatabaseSQL

logger = logging.getLogger("ulp")
load_dotenv(override=True)
_api_id = os.getenv("API_ID")
_api_hash = os.getenv("API_HASH")
_string_session = os.getenv("STRING_SESSION")
_bot_token = os.getenv("BOT_TOKEN")
_boxed_id = int(os.getenv("BOXED_ID"))
_log_chat_id = int(os.getenv("LOG_CHAT_ID"))
_cwd = os.getcwd()
_dl_folder = os.path.join(_cwd, "download")
_ex_folder = os.path.join(_cwd, "extract")
os.makedirs(_dl_folder, exist_ok=True)
os.makedirs(_ex_folder, exist_ok=True)


class BoxedULParser:
    """Orchestrates the Boxed ULP pipeline: intercept, download, extract, parse, and store."""

    def __init__(self) -> None:
        self._usr: TelegramClient = None
        self._bot: TelegramClient = None
        self.__bot_username = None
        self.__last_boxbot = None
        self._queue: asyncio.Queue = asyncio.Queue()

    async def start_clients(self) -> None:
        """Initialize and connect both the user client and the bot client."""
        logger.info("Starting Telegram clients...")
        self._usr = TelegramClient(StringSession(_string_session), _api_id, _api_hash)
        await self._usr.connect()
        self._bot = TelegramClient("bot", _api_id, _api_hash)
        await self._bot.start(bot_token=_bot_token)
        self.__bot_username = (await self._bot.get_me()).username
        logger.info("Clients connected (bot: @%s)", self.__bot_username)

    async def tg_log(self, message: str) -> None:
        """Logs a message to telegram log chat_id"""
        await self._bot.send_message(_log_chat_id, message)

    async def _boxed2bot(self, event: events.NewMessage.Event) -> None:
        """Forward Boxed channel messages to the intermediate bot."""
        if event.chat_id == _boxed_id and event.message.buttons:
            logger.info("New Boxed channel message detected (ID: %s)", event.message.id)
            try:
                url = urlparse(event.message.buttons[0][0].url)
                bot = await self._usr.get_entity(f"@{url.path.lstrip('/')}")
                code = parse_qs(url.query)["start"][0]
            except (KeyError, AttributeError, TypeError):
                logger.debug(
                    "Skipping message: not a valid boxed button", exc_info=True
                )
                return
            await self._usr(functions.messages.StartBotRequest(bot, bot, code))
            self.__last_boxbot = bot
        elif (
            self.__last_boxbot
            and event.chat_id == self.__last_boxbot.id
            and event.document
        ):
            await (await event.message.forward_to(f"@{self.__bot_username}")).delete()

    async def _bot2dump(self, event: events.NewMessage.Event) -> None:
        """Queue incoming bot messages for sequential processing."""
        if event.message.document:
            await self._queue.put(event)

    async def _process_queue(self) -> None:
        """Process queued messages one at a time to prevent interleaved output."""
        while True:
            event = await self._queue.get()
            try:
                await self._process_dump(event)
            except Exception as e:  # pylint: disable=broad-exception-caught
                logger.error(
                    "Pipeline failed for message %s", event.message.id, exc_info=True
                )
                await self.tg_log(
                    f"Exception ⚠️\nMsg: {repr(e)}\nID: {event.message.id}"
                )
            finally:
                self._queue.task_done()

    async def _process_dump(self, event: events.NewMessage.Event) -> None:
        """Download, extract, parse, and store credentials from a bot message."""
        logger.info(
            "Processing message %s: %s", event.message.id, event.message.file.name
        )
        archive = await TgFileDownloader().download(event.message, _dl_folder)
        dest_folder = os.path.join(_ex_folder, f"{event.message.id}")
        password = event.message.message.split(".pass:", 1)[1].split("\n", 1)[0].strip()
        if await ArchiveExtractor().try_to_extract(archive, dest_folder, password):
            combolist = await StealerLogParser().ulp_dump(dest_folder)
            insert_count = DatabaseSQL().insert_combos(combolist)
            await self.tg_log(
                f"Done ✅\nFile: {event.message.file.name}\nInserted: {insert_count}"
            )
            tqdm.write(f"Done: {event.message.id} (inserted: {insert_count})")
        else:
            await self.tg_log(
                f"Extraction failed\nFile: {event.message.file.name}\nID: {event.message.id}"
            )
        if os.path.isfile(archive):
            await aiofiles.os.remove(archive)
        if os.path.isdir(dest_folder):
            await asyncio.to_thread(shutil.rmtree, dest_folder)

    async def start(self) -> None:
        """Start both clients and begin listening for events."""
        await self.start_clients()
        self._usr.add_event_handler(self._boxed2bot, events.NewMessage)
        self._bot.add_event_handler(self._bot2dump, events.NewMessage)
        asyncio.create_task(self._process_queue())
        logger.info("Event handlers registered, listening for messages...")
        await self._usr.run_until_disconnected()


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        handlers=[
            logging.FileHandler("boxedulp.log", encoding="utf-8"),
            logging.StreamHandler(),
        ],
    )
    logging.getLogger("telethon").setLevel(logging.WARNING)
    box = BoxedULParser()
    try:
        asyncio.run(box.start())
    except KeyboardInterrupt:
        pass
