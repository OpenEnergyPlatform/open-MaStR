import os
import shutil
from datetime import date
from pathlib import Path
from typing import Optional

import pytest

from open_mastr import Mastr
from open_mastr.utils.config import get_project_home_dir


_data_dir = Path(get_project_home_dir()) / "data"

EXISTING_XML_ZIP: Optional[Path] = None
_xml_data_dir = _data_dir / "xml_download"
if _xml_data_dir.is_dir():
    for entry in os.scandir(_xml_data_dir):
        if "Gesamtdatenexport" in entry.name and entry.name.endswith(".zip"):
            EXISTING_XML_ZIP = Path(entry.path)
            # We don't break here in case there are multiple files.
            # We take the last matching entry in the hope of getting the most recent file.

EXISTING_DOCS_ZIP: Optional[Path] = None
_docs_data_dir = _data_dir / "docs_download"
if _docs_data_dir.is_dir():
    for entry in os.scandir(_docs_data_dir):
        if "Dokumentation MaStR Gesamtdatenexport" in entry.name and entry.name.endswith(".zip"):
            EXISTING_DOCS_ZIP = Path(entry.path)
            # We don't break here in case there are multiple files.
            # We take the last matching entry in the hope of getting the most recent file.


@pytest.fixture
def output_dir(tmp_path: Path) -> Path:
    dir_ = tmp_path / "output"
    yield dir_
    # Clean up so we don't leave this huge file lying around in some tmp dir.
    shutil.rmtree(dir_)


@pytest.fixture
def mastr(output_dir: Path) -> Mastr:
    return Mastr(output_dir=str(output_dir))


@pytest.fixture
def existing_xml_zip_in_output_dir(output_dir: Path) -> Path:
    if not EXISTING_XML_ZIP:
        raise ValueError(
            "There is no existing XML ZIP file to copy to the output dir."
            " This indicates faulty test setup. This fixture must only be used"
            " when EXISTING_XML_ZIP is not None."
        )

    xml_dir = output_dir / "data" / "xml_download"
    xml_dir.mkdir(parents=True, exist_ok=True)
    # We pretend that this file is from "today".
    dest_path = xml_dir / f"Gesamtdatenexport_{date.today().strftime('%Y%m%d')}.zip"

    # The XML file is pretty large, making this copy operation a bit costly for a unit test.
    # So, use this fixture sparingly.
    # Would be nice to have a hard link with copy-on-write semantics. Is there such a thing?
    shutil.copy(EXISTING_XML_ZIP, dest_path)
    return dest_path


@pytest.fixture
def existing_docs_zip_in_output_dir(output_dir: Path) -> Path:
    if not EXISTING_DOCS_ZIP:
        raise ValueError(
            "There is no existing docs ZIP file to copy to the output dir."
            " This indicates faulty test setup. This fixture must only be used"
            " when EXISTING_DOCS_ZIP is not None."
        )

    docs_dir = output_dir / "data" / "docs_download"
    docs_dir.mkdir(parents=True, exist_ok=True)
    # We pretend that this file is from "today".
    dest_path = docs_dir / f"Dokumentation MaStR Gesamtdatenexport_{date.today().strftime('%Y%m%d')}.zip"

    shutil.copy(EXISTING_DOCS_ZIP, dest_path)
    return dest_path
