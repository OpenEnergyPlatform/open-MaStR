#!/usr/bin/env python3


from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Any, Optional
from zoneinfo import ZoneInfo
import logging
import os
import shutil
import sys
import tempfile

from open_mastr.mastr import _get_fallback_xsd
from open_mastr.utils.xsd_tables import _iterate_xsd_files
from open_mastr.xml_download.utils_download_bulk import download_documentation


def update_fallback_xsd() -> None:
    with tempfile.TemporaryDirectory() as tmpdir:
        new_fallback_xsd = _make_new_fallback_xsd(Path(tmpdir))

        current_fallback_xsd = _get_fallback_xsd()
        logging.info(
            f"Comparing old {current_fallback_xsd} with new {new_fallback_xsd}"
        )
        diff = _directory_diff(current_fallback_xsd, new_fallback_xsd)
        if not diff:
            logging.info("No difference between current and new XSD. Not updating.")
            sys.exit(1)

        logging.info(
            "New fallback XSD is different. Replacing current fallback XSD."
            f"\n    Diff: {diff}"
        )

        resources_dir = Path(__file__).parent.parent / "open_mastr/resources/"
        for old_dir in resources_dir.glob("fallback-xsd-*"):
            logging.info(f"Removing {old_dir}")
            shutil.rmtree(old_dir)

        new_fallback_xsd_in_resources = resources_dir / new_fallback_xsd.name
        logging.info(f"Creating {new_fallback_xsd_in_resources}")
        shutil.copytree(new_fallback_xsd, new_fallback_xsd_in_resources)


def _make_new_fallback_xsd(top_dir: Path) -> Path:
    today = datetime.now(tz=ZoneInfo("Europe/Berlin")).date()
    today_str = today.strftime("%Y%m%d")

    new_fallback_dir = top_dir / f"fallback-xsd-{today_str}"
    logging.info("Making new fallback XSD in {new_fallback_dir}")
    new_fallback_dir.mkdir()

    zipped_docs_path = top_dir / "docs.zip"
    logging.info(f"Downloading docs to {zipped_docs_path}")
    download_documentation(zipped_docs_path, bulk_date_string=today_str)
    logging.info(f"Copying XSD files to {new_fallback_dir}")
    for filename, xsd_file in _iterate_xsd_files(zipped_docs_path):
        with xsd_file:
            with open(new_fallback_dir / filename, "wb") as new_file:
                new_file.write(xsd_file.read())
    return new_fallback_dir


def _directory_diff(
    left: str | os.PathLike[str], right: str | os.PathLike[str]
) -> Optional[dict[str, list[Any]]]:
    """
    Recursively compare filenames, entry types, and file contents.

    Returns:
        {
            "only_in_left": ["path/to/file", ...],
            "only_in_right": ["path/to/file", ...],
            "different": [
                {"path": "file.txt", "reason": "content"},
                {"path": "item", "reason": "type", "left": "file", "right": "directory"},
                ...
            ],
        }
    """
    left = Path(left)
    right = Path(right)

    if not left.is_dir() or not right.is_dir():
        raise ValueError("Both paths must be existing directories")

    def entries(root: Path) -> dict[str, tuple[str, Path]]:
        result: dict[str, tuple[str, Path]] = {}

        for current, dirs, files in os.walk(root, followlinks=False):
            current_path = Path(current)

            # Treat symlinked directories as symlinks, rather than traversing them.
            for name in dirs[:]:
                path = current_path / name
                relative = str(path.relative_to(root))
                if path.is_symlink():
                    result[relative] = ("symlink", path)
                    dirs.remove(name)
                else:
                    result[relative] = ("directory", path)

            for name in files:
                path = current_path / name
                relative = str(path.relative_to(root))
                result[relative] = (
                    "symlink" if path.is_symlink() else "file",
                    path,
                )

        return result

    def files_equal(first: Path, second: Path, chunk_size: int = 1024 * 1024) -> bool:
        if first.stat().st_size != second.stat().st_size:
            return False

        with first.open("rb") as first_file, second.open("rb") as second_file:
            while True:
                first_chunk = first_file.read(chunk_size)
                second_chunk = second_file.read(chunk_size)
                if first_chunk != second_chunk:
                    return False
                if not first_chunk:
                    return True

    left_entries = entries(left)
    right_entries = entries(right)

    only_in_left = sorted(left_entries.keys() - right_entries.keys())
    only_in_right = sorted(right_entries.keys() - left_entries.keys())
    different = []

    for relative in sorted(left_entries.keys() & right_entries.keys()):
        left_type, left_path = left_entries[relative]
        right_type, right_path = right_entries[relative]

        if left_type != right_type:
            different.append(
                {
                    "path": relative,
                    "reason": "type",
                    "left": left_type,
                    "right": right_type,
                }
            )
        elif left_type == "file" and not files_equal(left_path, right_path):
            different.append({"path": relative, "reason": "content"})
        elif left_type == "symlink" and os.readlink(left_path) != os.readlink(
            right_path
        ):
            different.append({"path": relative, "reason": "symlink_target"})

    if not only_in_left and not only_in_right and not different:
        return None

    return {
        "only_in_left": only_in_left,
        "only_in_right": only_in_right,
        "different": different,
    }


if __name__ == "__main__":
    update_fallback_xsd()
