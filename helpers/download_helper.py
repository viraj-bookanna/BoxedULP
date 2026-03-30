import os
import logging
from typing import Optional

from telethon.tl.custom.message import Message
from tqdm import tqdm
from FastTelethon import download_file


class TgFileDownloader:
    """Downloads Telegram file attachments with a tqdm progress bar."""

    def __init__(self) -> None:
        self.current = 0
        self.last = 0
        self.pbar = None

    def _update(self, current: int, total: int) -> None:
        """Progress callback invoked by FastTelethon during download."""
        if not isinstance(total, int) or total == 0:
            return
        if not self.pbar:
            self.pbar = tqdm(
                total=total,
                unit="iB",
                unit_scale=True,
                unit_divisor=1024,
                desc="├─ Downloading",
            )
        self.pbar.update(current - self.last)
        self.last = current
        return

    async def download(self, message: Message, download_dir: str) -> Optional[str]:
        """Download the document attached to a message.

        Args:
            message: Telegram message containing a document.
            download_dir: Directory to save the downloaded file.

        Returns:
            Path to the downloaded file, or None if no document.
        """
        if not message.document:
            logging.warning("No document in message %s, skipping download", message.id)
            return
        out_file = os.path.join(
            download_dir,
            message.file.name,
        )
        logging.info("Downloading %s (ID: %s)", message.file.name, message.id)
        tqdm.write(f"┌ Processing: {message.file.name} (ID: {message.id})")
        try:
            with open(out_file, "wb") as out:
                await download_file(
                    message.client,
                    message.document,
                    out,
                    progress_callback=self._update,
                )
        except Exception:
            logging.error("Download failed for %s (ID: %s)", message.file.name, message.id, exc_info=True)
            raise
        finally:
            if self.pbar:
                self.pbar.close()
        logging.info("Download complete: %s", out_file)
        return out_file
