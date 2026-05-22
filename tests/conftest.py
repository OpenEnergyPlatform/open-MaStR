import shutil
from datetime import date
from pathlib import Path

import pytest

from open_mastr import Mastr
from open_mastr.mastr import FALLBACK_DOCS_PATH

MOCKUP_XML_ZIP = Path(__file__).parent / "data" / "Gesamtdatenexport_mockup.zip"
NUMBER_ROWS_IN_MOCK_XML_FILES = 1000


@pytest.fixture
def output_dir(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Create a temporary output directory and set MASTR_PROJECT_HOME_DIR to it.

    The directory is cleaned up after the test.
    """
    dir_ = tmp_path / "output"
    dir_.mkdir()
    monkeypatch.setenv("MASTR_PROJECT_HOME_DIR", str(dir_))
    yield dir_
    shutil.rmtree(dir_, ignore_errors=True)


@pytest.fixture
def mastr(output_dir: Path) -> Mastr:
    """Create a Mastr instance with the project home dir set to output_dir."""
    return Mastr()


@pytest.fixture
def mockup_xml_zip_in_output_dir(output_dir: Path) -> Path:
    """Copy the mockup XML zip into the output directory and return its path."""
    xml_dir = output_dir / "data" / "xml_download"
    xml_dir.mkdir(parents=True, exist_ok=True)
    dest = xml_dir / f"Gesamtdatenexport_{date.today().strftime('%Y%m%d')}.zip"
    shutil.copy(MOCKUP_XML_ZIP, dest)
    return dest


@pytest.fixture
def mockup_docs_zip_in_output_dir(output_dir: Path) -> Path:
    """Copy the mockup docs zip into the output directory and return its path."""
    docs_dir = output_dir / "data" / "docs_download"
    docs_dir.mkdir(parents=True, exist_ok=True)
    dest = (
        docs_dir
        / f"Dokumentation MaStR Gesamtdatenexport_{date.today().strftime('%Y%m%d')}.zip"
    )
    shutil.copy(FALLBACK_DOCS_PATH, dest)
    return dest
