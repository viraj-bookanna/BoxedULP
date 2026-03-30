import os
import logging
from typing import AsyncGenerator, Optional

import aiofiles
import aiofiles.os
from tqdm import tqdm


class StealerLogParser:
    """Parses stealer log directories into normalized (username, password, url) tuples."""

    def __init__(self) -> None:
        self.__replacers = {
            "a_username": ["USER LOGIN:", "Login:", "Username:", "USER:", "U53RN4M3:"],
            "b_password": ["USER PASSWORD:", "Password:", "PASS:", "P455W0RD:"],
            "c_url": ["Host:", "Hostname:", "URL:", "UR1:", "Url:"],
        }

    def _str_replace(self, needle, rep: str, haystack: str) -> str:
        """Recursively replace all needle variants in haystack with rep."""
        if isinstance(needle, (list, tuple)):
            for n in needle:
                haystack = self._str_replace(n, rep, haystack)
            return haystack
        return haystack.replace(needle, rep)

    async def _parse_ulp(self, redline_txt: str) -> AsyncGenerator[tuple, None]:
        """Parse a single password file, yielding (username, password, url) tuples."""
        keys = self.__replacers.keys()
        try:
            async with aiofiles.open(redline_txt, "r", encoding="utf-8") as f:
                obj = {}
                async for line in f:
                    for k in keys:
                        line = self._str_replace(self.__replacers[k], f"{k}:", line)
                    line = [x.strip() for x in line.split(":", 1)]
                    if len(line) != 2:
                        continue
                    if line[0] in obj:
                        obj = {line[0]: line[1]}
                    elif line[0] in keys:
                        obj[line[0]] = line[1]
                    if len(obj) == len(keys):
                        yield tuple(dict(sorted(obj.items())).values())
                        obj = {}
        except (OSError, UnicodeDecodeError):
            logging.debug("Failed to read password file: %s", redline_txt, exc_info=True)

    def _find_passwords_file(self, extract_path: str) -> Optional[tuple[str, str]]:
        """Walk the directory tree to locate the first password file."""
        for dirpath, _dirnames, filenames in os.walk(extract_path):
            for file in filenames:
                if "password" in file.lower() and file.lower().endswith(".txt"):
                    return os.path.dirname(dirpath), file
        return None

    async def ulp_dump(self, extract_path: str) -> list[tuple]:
        """Scan extracted log directories and return all parsed credentials.

        Args:
            extract_path: Root directory of extracted stealer logs.

        Returns:
            List of (username, password, url) tuples.
        """
        result = self._find_passwords_file(extract_path)
        if not result:
            logging.warning("No password file found in %s", extract_path)
            return
        extract_path, file_name = result
        try:
            dir_entries = await aiofiles.os.listdir(extract_path)
        except (OSError, PermissionError) as e:
            logging.error("Cannot access directory %s: %s", extract_path, e)
            return
        log_dirs = []
        for name in dir_entries:
            logdir = os.path.join(extract_path, name)
            combofile = os.path.join(logdir, file_name)
            if await aiofiles.os.path.isdir(logdir) and await aiofiles.os.path.isfile(
                combofile
            ):
                log_dirs.append(combofile)
        combos = []
        for combofile in tqdm(log_dirs, desc="└─ Parsing"):
            async for line in self._parse_ulp(combofile):
                combos.append(line)
        logging.info("Parsed %d credentials from %d directories", len(combos), len(log_dirs))
        return combos
