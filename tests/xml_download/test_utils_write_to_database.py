import sys
from zipfile import ZipFile

from open_mastr.utils import orm
from open_mastr.xml_download.utils_cleansing_bulk import (
    replace_mastr_katalogeintraege,
)
from open_mastr.xml_download.utils_write_to_database import (
    cast_date_columns_to_datetime,
    preprocess_table_for_writing_to_database,
    add_table_to_database,
    add_zero_as_first_character_for_too_short_string,
    correct_ordering_of_filelist,
)
import os
from os.path import expanduser
import sqlite3
from sqlalchemy import create_engine
import pandas as pd
import pytest
import numpy as np
from datetime import datetime

# Check if xml file exists
_xml_file_exists = False
_xml_folder_path = os.path.join(expanduser("~"), ".open-MaStR", "data", "xml_download")
if os.path.isdir(_xml_folder_path):
    for entry in os.scandir(path=_xml_folder_path):
        if "Gesamtdatenexport" in entry.name:
            _xml_file_exists = True


# Silence ValueError caused by logger https://github.com/pytest-dev/pytest/issues/5502
@pytest.fixture(autouse=True)
def capture_wrap():
    sys.stderr.close = lambda *args: None
    sys.stdout.close = lambda *args: None
    yield


@pytest.fixture(scope="module")
def zipped_xml_file_path():
    zipped_xml_file_path = None
    for entry in os.scandir(path=_xml_folder_path):
        if "Gesamtdatenexport" in entry.name:
            zipped_xml_file_path = os.path.join(_xml_folder_path, entry.name)

    return zipped_xml_file_path


@pytest.fixture(scope="module")
def con_testdb():
    testdb_file_path = os.path.join(
        expanduser("~"), ".open-MaStR", "data", "sqlite", "test-open-mastr.db"
    )
    # Create testdb
    con_testdb = sqlite3.connect(testdb_file_path)
    yield con_testdb
    con_testdb.close()
    # Remove testdb
    os.remove(testdb_file_path)


@pytest.fixture(scope="module")
def engine_testdb():
    testdb_file_path = os.path.join(
        expanduser("~"), ".open-MaStR", "data", "sqlite", "test-open-mastr.db"
    )
    testdb_url = f"sqlite:///{testdb_file_path}"
    yield create_engine(testdb_url)


@pytest.mark.skipif(
    not _xml_file_exists, reason="The zipped xml file could not be found."
)
def test_preprocess_table_for_writing_to_database(zipped_xml_file_path):
    # Prepare
    file_name = "EinheitenKernkraft.xml"
    xml_tablename = "einheitenkernkraft"
    bulk_download_date = zipped_xml_file_path.split("_")[-1].replace(".zip", "")
    # Check if bulk_download_date is derived correctly like 20220323
    assert len(bulk_download_date) == 8

    with ZipFile(zipped_xml_file_path, "r") as f:
        # Act
        df = preprocess_table_for_writing_to_database(
            f=f,
            file_name=file_name,
            xml_tablename=xml_tablename,
            bulk_download_date=bulk_download_date,
        )

    # Assert
    assert df["DatenQuelle"].unique().tolist() == ["bulk"]
    assert df["DatumDownload"].unique().tolist() == [bulk_download_date]
    assert df["Gemeindeschluessel"].apply(len).unique().tolist() == [8]
    assert df["Postleitzahl"].apply(len).unique().tolist() == [5]


@pytest.mark.skipif(
    not _xml_file_exists, reason="The zipped xml file could not be found."
)
def test_add_table_to_database(zipped_xml_file_path, engine_testdb):
    # Prepare
    orm.Base.metadata.create_all(engine_testdb)
    file_name = "EinheitenKernkraft.xml"
    xml_tablename = "einheitenkernkraft"
    sql_tablename = "nuclear_extended"
    bulk_download_date = zipped_xml_file_path.split("_")[-1].replace(".zip", "")
    # Check if bulk_download_date is derived correctly like 20220323
    assert len(bulk_download_date) == 8
    with ZipFile(zipped_xml_file_path, "r") as f:
        df_write = preprocess_table_for_writing_to_database(
            f=f,
            file_name=file_name,
            xml_tablename=xml_tablename,
            bulk_download_date=bulk_download_date,
        )

    # Convert date and datetime columns into the datatype datetime
    df_write = cast_date_columns_to_datetime(xml_tablename, df_write)

    # Katalogeintraege: int -> string value
    df_write = replace_mastr_katalogeintraege(
        zipped_xml_file_path=zipped_xml_file_path, df=df_write
    )

    # Act
    add_table_to_database(
        df=df_write,
        xml_tablename=xml_tablename,
        sql_tablename=sql_tablename,
        if_exists="replace",
        engine=engine_testdb,
    )
    with engine_testdb.connect() as con:
        with con.begin():
            df_read = pd.read_sql_table(table_name=sql_tablename, con=con)
    # Drop the empty columns which come from orm and are not filled by bulk download
    df_read.dropna(how="all", axis=1, inplace=True)
    # Rename LokationMaStRNummer -> LokationMastrNummer unresolved error
    df_write.rename(
        columns={"LokationMaStRNummer": "LokationMastrNummer"}, inplace=True
    )
    # Reorder columns of the df_write since the order of columns doesn't play a role
    df_read = df_read[df_write.columns.tolist()]
    # Cast the dtypes of df_write like df_read dtypes
    # Datatypes are changed during inserting via sqlalchemy and orm
    df_read = df_read.astype(dict(zip(df_write.columns.tolist(), df_write.dtypes)))
    # Assert
    pd.testing.assert_frame_equal(
        df_write.select_dtypes(exclude="datetime"),
        df_read.select_dtypes(exclude="datetime"),
    )


def test_add_zero_as_first_character_for_too_short_string():
    # Prepare
    df_raw = pd.DataFrame(
        {"ID": [0, 1, 2], "Gemeindeschluessel": [9162000, np.nan, 19123456]}
    )
    df_correct = pd.DataFrame(
        {"ID": [0, 1, 2], "Gemeindeschluessel": ["09162000", np.nan, "19123456"]}
    )

    # Act
    df_edited = add_zero_as_first_character_for_too_short_string(df_raw)
    # Assert
    pd.testing.assert_frame_equal(df_edited, df_correct)


def test_correct_ordering_of_filelist():
    filelist = [
        "Solar_1.xml",
        "Solar_10.xml",
        "Solar_11.xml",
        "Solar_2.xml",
        "Solar_3.xml",
        "Solar_4.xml",
        "Solar_5.xml",
        "Solar_6.xml",
        "Solar_7.xml",
        "Solar_8.xml",
        "Solar_9.xml",
        "Wind_1.xml",
        "Wind_2.xml",
        "Wind_3.xml",
    ]
    filelist_2 = ["Solar_01.xml", "Solar_02.xml", "Solar_10.xml", "Wind_01.xml"]

    filelist_corrected = correct_ordering_of_filelist(filelist)
    filelist_2_corrected = correct_ordering_of_filelist(filelist_2)

    assert filelist_corrected == [
        "Solar_1.xml",
        "Solar_2.xml",
        "Solar_3.xml",
        "Solar_4.xml",
        "Solar_5.xml",
        "Solar_6.xml",
        "Solar_7.xml",
        "Solar_8.xml",
        "Solar_9.xml",
        "Solar_10.xml",
        "Solar_11.xml",
        "Wind_1.xml",
        "Wind_2.xml",
        "Wind_3.xml",
    ]
    assert filelist_2_corrected == [
        "Solar_01.xml",
        "Solar_02.xml",
        "Solar_10.xml",
        "Wind_01.xml",
    ]


def test_cast_date_columns_to_datetime():
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
        df_replaced, cast_date_columns_to_datetime("anlageneegwasser", df_raw)
    )
