import os
import asyncio
import logging
import zipfile
from typing import Optional

import rarfile
import py7zr
import aiofiles
import aiofiles.os
import multivolumefile
from tqdm import tqdm

_UNRAR_PATH = os.getenv("UNRAR_PATH", "")
if _UNRAR_PATH:
    rarfile.UNRAR_TOOL = _UNRAR_PATH


class Pbar7z(py7zr.callbacks.ExtractCallback, tqdm):
    """Progress bar adapter for py7zr extraction callbacks."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def report_start_preparation(self):
        pass

    def report_start(self, processing_file_path, processing_bytes):
        pass

    def report_update(self, decompressed_bytes):
        pass

    def report_end(self, processing_file_path, wrote_bytes):
        self.update(int(wrote_bytes))

    def report_postprocess(self):
        pass

    def report_warning(self, message):
        pass


class ArchiveExtractor:
    """Extracts zip, rar, and 7z archives with progress reporting."""

    def _extract_zip_or_rar(self, archive_ref, output_folder: str) -> None:
        """Extract entries from a zip or rar archive with a progress bar."""
        for info in tqdm(archive_ref.infolist(), desc="├─ Extracting"):
            try:
                archive_ref.extract(info, path=output_folder)
            except (OSError, KeyError):
                logging.debug("Failed to extract %s", info, exc_info=True)
                continue

    def _extract_7z(self, seven_zip_ref: py7zr.SevenZipFile, output_folder: str) -> None:
        """Extract a 7z archive with a byte-level progress bar."""
        with Pbar7z(
            total=seven_zip_ref.archiveinfo().uncompressed,
            unit="iB",
            unit_scale=True,
            unit_divisor=1024,
            desc="├─ Extracting",
        ) as progress:
            seven_zip_ref.extractall(path=output_folder, callback=progress)

    def extract_file(
        self, input_file: str, output_folder: str, password: Optional[str] = None
    ) -> None:
        """Extract an archive file based on its extension.

        Supports .zip, .rar, .7z, and multi-volume .7z archives.

        Raises:
            Exception: If the file format is not recognized.
        """
        if input_file.lower().endswith(".zip"):
            with zipfile.ZipFile(input_file, "r") as zip_ref:
                if password:
                    zip_ref.setpassword(password.encode("utf-8"))
                self._extract_zip_or_rar(zip_ref, output_folder)
        elif input_file.lower().endswith(".rar"):
            with rarfile.RarFile(input_file, "r") as rar_ref:
                if password:
                    rar_ref.setpassword(password)
                self._extract_zip_or_rar(rar_ref, output_folder)
        elif input_file.lower().endswith(".7z"):
            with py7zr.SevenZipFile(input_file, "r", password=password) as seven_zip_ref:
                self._extract_7z(seven_zip_ref, output_folder)
        elif input_file.lower().endswith((".7z.001", ".7z.0001")):
            with multivolumefile.open(
                input_file.rsplit(".7z", 1)[0] + ".7z", mode="rb"
            ) as target_archive:
                with py7zr.SevenZipFile(
                    target_archive, "r", password=password
                ) as seven_zip_ref:
                    self._extract_7z(seven_zip_ref, output_folder)
        else:
            raise ValueError(f"Unknown file format: {input_file}")

    async def try_to_extract(
        self, file: str, dest_folder: str, password: Optional[str] = None, level: int = 0
    ) -> bool:
        """Attempt to extract an archive, recursing once for nested archives."""
        try:
            await aiofiles.os.makedirs(dest_folder, exist_ok=True)
            await asyncio.to_thread(self.extract_file, file, dest_folder, password)
            ex_files = await aiofiles.os.listdir(dest_folder)
            if level == 0 and len(ex_files) == 1:
                efile = os.path.join(dest_folder, ex_files[0])
                if await aiofiles.os.path.isfile(efile) and ex_files[
                    0
                ].lower().endswith((".rar", ".zip", ".7z")):
                    res = await self.try_to_extract(
                        efile, dest_folder, password, level + 1
                    )
                    await aiofiles.os.remove(efile)
                    return res
        except (OSError, ValueError, rarfile.Error, zipfile.BadZipFile, py7zr.Bad7zFile):
            logging.error("Extraction failed for %s", file, exc_info=True)
            return False
        return True
