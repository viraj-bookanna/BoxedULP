import os
import asyncio
import logging
import shutil
import subprocess
import threading
import zipfile
from typing import Optional
from dotenv import load_dotenv

import rarfile
import py7zr
import aiofiles
import aiofiles.os
import multivolumefile
from tqdm import tqdm

logger = logging.getLogger("ulp")
load_dotenv(override=True)
_UNRAR_PATH = os.getenv("UNRAR_PATH", "")
if _UNRAR_PATH:
    rarfile.UNRAR_TOOL = _UNRAR_PATH
_7Z_PATH = os.getenv("SEVEN_ZIP_PATH", "")
if not _7Z_PATH:
    _7Z_PATH = shutil.which("7z")

if not _7Z_PATH:
    logger.warning(
        "Native 7z not found; falling back to py7zr (slower). Set SEVEN_ZIP_PATH or install 7z to PATH."
    )


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

    @staticmethod
    def _monitored_extract(
        extract_fn, output_folder: str, total: Optional[int] = None
    ) -> None:
        """Run extract_fn while monitoring output_folder for file-count progress."""
        done = threading.Event()

        def _monitor():
            count = 0
            with tqdm(total=total, desc="├─ Extracting", mininterval=0.5) as pbar:
                while not done.wait(1.5):
                    new_count = sum(len(f) for _, _, f in os.walk(output_folder))
                    pbar.update(new_count - count)
                    count = new_count
                new_count = sum(len(f) for _, _, f in os.walk(output_folder))
                pbar.update(new_count - count)

        t = threading.Thread(target=_monitor, daemon=True)
        t.start()
        try:
            extract_fn()
        finally:
            done.set()
            t.join()

    def _extract_zip(self, archive_ref, output_folder: str) -> None:
        """Extract entries from a zip archive with a progress bar."""
        for info in tqdm(archive_ref.infolist(), desc="├─ Extracting", mininterval=0.5):
            try:
                archive_ref.extract(info, path=output_folder)
            except (OSError, KeyError):
                logger.debug("Failed to extract %s", info, exc_info=True)

    def _extract_rar(self, rar_ref: rarfile.RarFile, output_folder: str) -> None:
        """Extract RAR via single unrar process with directory-monitoring progress."""
        total = sum(1 for i in rar_ref.infolist() if not i.is_dir())
        self._monitored_extract(
            lambda: rar_ref.extractall(path=output_folder), output_folder, total
        )

    def _extract_7z_py(
        self, seven_zip_ref: py7zr.SevenZipFile, output_folder: str
    ) -> None:
        """Extract a 7z archive with a byte-level progress bar (py7zr fallback)."""
        with Pbar7z(
            total=seven_zip_ref.archiveinfo().uncompressed,
            unit="iB",
            unit_scale=True,
            unit_divisor=1024,
            desc="├─ Extracting",
        ) as progress:
            seven_zip_ref.extractall(path=output_folder, callback=progress)

    def _extract_7z_native(
        self, input_file: str, output_folder: str, password: Optional[str] = None
    ) -> None:
        """Extract 7z via native 7z binary with directory-monitoring progress."""
        list_cmd = [_7Z_PATH, "l", input_file, "-bso0", "-bsp0", "-slt"]
        if password:
            list_cmd.append(f"-p{password}")
        list_out = subprocess.run(list_cmd, capture_output=True, text=True, check=False)
        total = list_out.stdout.count("\nPath = ") - 1
        if total < 1:
            total = None

        cmd = [_7Z_PATH, "x", input_file, f"-o{output_folder}", "-y", "-bso0", "-bsp0"]
        if password:
            cmd.append(f"-p{password}")

        def _run():
            result = subprocess.run(cmd, capture_output=True, text=True, check=False)
            if result.returncode != 0:
                raise OSError(
                    f"7z exited with code {result.returncode}: {result.stderr.strip()}"
                )

        self._monitored_extract(_run, output_folder, total)

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
                self._extract_zip(zip_ref, output_folder)
        elif input_file.lower().endswith(".rar"):
            with rarfile.RarFile(input_file, "r") as rar_ref:
                if password:
                    rar_ref.setpassword(password)
                self._extract_rar(rar_ref, output_folder)
        elif input_file.lower().endswith(".7z"):
            if _7Z_PATH:
                self._extract_7z_native(input_file, output_folder, password)
            else:
                with py7zr.SevenZipFile(
                    input_file, "r", password=password
                ) as seven_zip_ref:
                    self._extract_7z_py(seven_zip_ref, output_folder)
        elif input_file.lower().endswith((".7z.001", ".7z.0001")):
            if _7Z_PATH:
                self._extract_7z_native(input_file, output_folder, password)
            else:
                with multivolumefile.open(
                    input_file.rsplit(".7z", 1)[0] + ".7z", mode="rb"
                ) as target_archive:
                    with py7zr.SevenZipFile(
                        target_archive, "r", password=password
                    ) as seven_zip_ref:
                        self._extract_7z_py(seven_zip_ref, output_folder)
        else:
            raise ValueError(f"Unknown file format: {input_file}")

    async def try_to_extract(
        self,
        file: str,
        dest_folder: str,
        password: Optional[str] = None,
        level: int = 0,
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
                    logger.info("Nested archive detected, extracting: %s", efile)
                    res = await self.try_to_extract(
                        efile, dest_folder, password, level + 1
                    )
                    await aiofiles.os.remove(efile)
                    return res
        except (
            OSError,
            ValueError,
            rarfile.Error,
            zipfile.BadZipFile,
            py7zr.Bad7zFile,
        ):
            logger.error("Extraction failed for %s", file, exc_info=True)
            return False
        return True
