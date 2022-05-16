import sys

from open_mastr.xml_download.utils_sqlite_bulk_cleansing import (
    create_katalogwerte_from_sqlite,
    replace_mastr_katalogeintraege,
    date_columns_to_datetime,
)
import sqlite3
from os.path import expanduser
import os
import pandas as pd
import numpy as np
from open_mastr.xml_download.colums_to_replace import columns_replace_list
import pytest
from datetime import datetime

# Check if xml file exists
_xml_file_exists = False
_xml_folder_path = os.path.join(expanduser("~"), ".open-MaStR", "data", "xml_download")
if os.path.isdir(_xml_folder_path):
    for entry in os.scandir(path=_xml_folder_path):
        if "Gesamtdatenexport" in entry.name:
            _xml_file_exists = True

# Check if open-mastr.db exists
_sqlite_db_exists = False
_sqlite_folder_path = os.path.join(expanduser("~"), ".open-MaStR", "data", "sqlite")
_sqlite_file_path = os.path.join(_sqlite_folder_path, "open-mastr.db")
if os.path.exists(_sqlite_file_path):
    _sqlite_db_exists = True

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
def test_create_katalogwerte_from_sqlite(zipped_xml_file_path):
    katalogwerte = create_katalogwerte_from_sqlite(
        zipped_xml_file_path=zipped_xml_file_path
    )
    assert type(katalogwerte) == dict
    assert len(katalogwerte) > 1000
    assert type(list(katalogwerte.keys())[0]) == int


def test_date_columns_to_datetime():
    df_raw = pd.DataFrame(
        {
            "ID": [0, 1, 2],
            "Registrierungsdatum": ["2022-03-22", "2020-01-02", "2022-03-35"],
        }
    )
    df_replaced = pd.DataFrame(
        {
            "ID": [0, 1, 2],
            "Registrierungsdatum": [
                datetime(2022, 3, 22),
                datetime(2020, 1, 2),
                np.datetime64("nat"),
            ],
        }
    )

    pd.testing.assert_frame_equal(
        df_replaced, date_columns_to_datetime("anlageneegwasser", df_raw)
    )
