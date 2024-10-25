import sys
import sqlite3
from os.path import expanduser
import os
import pandas as pd
import numpy as np
import pytest

from open_mastr.xml_download.utils_cleansing_bulk import (
    create_katalogwerte_from_bulk_download,
    replace_mastr_katalogeintraege,
)

# Check if xml file exists
_xml_file_exists = False
_xml_folder_path = os.path.join(expanduser("~"), ".open-MaStR", "data", "xml_download")
if os.path.isdir(_xml_folder_path):
    for entry in os.scandir(path=_xml_folder_path):
        if "Gesamtdatenexport" in entry.name:
            _xml_file_exists = True

_sqlite_folder_path = os.path.join(expanduser("~"), ".open-MaStR", "data", "sqlite")
_sqlite_file_path = os.path.join(_sqlite_folder_path, "open-mastr.db")
_sqlite_db_exists = bool(os.path.exists(_sqlite_file_path))

# Silence ValueError caused by logger https://github.com/pytest-dev/pytest/issues/5502
@pytest.fixture(autouse=True)
def capture_wrap():
    sys.stderr.close = lambda *args: None
    sys.stdout.close = lambda *args: None
    yield


@pytest.fixture(scope="module")
def con():
    con = sqlite3.connect(_sqlite_file_path)
    yield con
    con.close()


@pytest.fixture(scope="module")
def zipped_xml_file_path():
    zipped_xml_file_path = None
    for entry in os.scandir(path=_xml_folder_path):
        if "Gesamtdatenexport" in entry.name:
            zipped_xml_file_path = os.path.join(_xml_folder_path, entry.name)

    return zipped_xml_file_path


@pytest.mark.skipif(
    not _xml_file_exists, reason="The zipped xml file could not be found."
)
def test_replace_mastr_katalogeintraege(zipped_xml_file_path):
    df_raw = pd.DataFrame({"ID": [0, 1, 2], "Bundesland": [335, 335, 336]})
    df_replaced = pd.DataFrame(
        {"ID": [0, 1, 2], "Bundesland": ["Bayern", "Bayern", "Bremen"]}
    )
    pd.testing.assert_frame_equal(
        df_replaced, replace_mastr_katalogeintraege(zipped_xml_file_path, df_raw)
    )


@pytest.mark.skipif(
    not _xml_file_exists, reason="The zipped xml file could not be found."
)
def test_create_katalogwerte_from_bulk_download(zipped_xml_file_path):
    katalogwerte = create_katalogwerte_from_bulk_download(
        zipped_xml_file_path=zipped_xml_file_path
    )
    assert type(katalogwerte) == dict
    assert len(katalogwerte) > 1000
    assert type(list(katalogwerte.keys())[0]) == int
