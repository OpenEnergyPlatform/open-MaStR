from zipfile import ZipFile

from open_mastr import orm
from open_mastr.xml_download.utils_sqlite_bulk_cleansing import (
    date_columns_to_datetime,
    replace_mastr_katalogeintraege,
)
from open_mastr.xml_download.utils_write_sqlite import (
    convert_mastr_xml_to_sqlite,
    prepare_table_to_sqlite_database,
    add_table_to_sqlite_database,
)
import os
from os.path import expanduser
import sqlite3
from sqlalchemy import create_engine
import pandas as pd
import pytest

# Check if xml file exists
_xml_file_exists = False
_xml_folder_path = os.path.join(expanduser("~"), ".open-MaStR", "data", "xml_download")
for entry in os.scandir(path=_xml_folder_path):
    if "Gesamtdatenexport" in entry.name:
        _xml_file_exists = True


@pytest.fixture(scope="module")
def zipped_xml_file_path():
    zipped_xml_file_path = None
    for entry in os.scandir(path=_xml_folder_path):
        if "Gesamtdatenexport" in entry.name:
            zipped_xml_file_path = os.path.join(_xml_folder_path, entry.name)

    return zipped_xml_file_path


@pytest.fixture(scope="module")
def testdb_con():
    testdb_file_path = os.path.join(
        expanduser("~"), ".open-MaStR", "data", "sqlite", "test-open-mastr.db"
    )
    # Create testdb
    testdb_con = sqlite3.connect(testdb_file_path)
    yield testdb_con
    testdb_con.close()
    # Remove testdb
    os.remove(testdb_file_path)


@pytest.fixture(scope="module")
def testdb_engine():
    testdb_file_path = os.path.join(
        expanduser("~"), ".open-MaStR", "data", "sqlite", "test-open-mastr.db"
    )
    testdb_url = f"sqlite:///{testdb_file_path}"
    testdb_engine = create_engine(testdb_url)
    yield testdb_engine


def test_convert_mastr_xml_to_sqlite():
    _sqlite_folder_path = os.path.join(expanduser("~"), ".open-MaStR", "data", "sqlite")
    con = sqlite3.connect(os.path.join(_sqlite_folder_path, "bulksqlite.db"))
    today_string = "20220103"

    zipped_xml_file_path = os.path.join(
        expanduser("~"),
        ".open-MaStR",
        "data",
        "xml_download",
        "Gesamtdatenexport_%s.zip" % today_string,
    )
    include_tables = ["einheitenbiomasse"]
    convert_mastr_xml_to_sqlite(
        con=con,
        zipped_xml_file_path=zipped_xml_file_path,
        include_tables=include_tables,
    )

    df = pd.read_sql(f"SELECT * FROM {include_tables[0]}", con=con)
    assert len(df) > 100
    assert len(df.iloc[0]) > 5
    con.close()


@pytest.mark.skipif(
    not _xml_file_exists, reason="The zipped xml file could not be found."
)
def test_prepare_table_to_sqlite_database(zipped_xml_file_path):
    # Prepare
    file_name = "EinheitenKernkraft.xml"
    xml_tablename = "einheitenkernkraft"
    bulk_download_date = zipped_xml_file_path.split("_")[-1].replace(".zip", "")
    # Check if bulk_download_date is derived correctly like 20220323
    assert len(bulk_download_date) == 8

    with ZipFile(zipped_xml_file_path, "r") as f:
        # Act
        df = prepare_table_to_sqlite_database(
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
def test_add_table_to_sqlite_database(zipped_xml_file_path, testdb_con, testdb_engine):
    # Prepare
    orm.Base.metadata.create_all(testdb_engine)
    file_name = "EinheitenKernkraft.xml"
    xml_tablename = "einheitenkernkraft"
    sql_tablename = "nuclear_extended"
    bulk_download_date = zipped_xml_file_path.split("_")[-1].replace(".zip", "")
    # Check if bulk_download_date is derived correctly like 20220323
    assert len(bulk_download_date) == 8
    with ZipFile(zipped_xml_file_path, "r") as f:
        df_write = prepare_table_to_sqlite_database(
            f=f,
            file_name=file_name,
            xml_tablename=xml_tablename,
            bulk_download_date=bulk_download_date,
        )

    # Convert date and datetime columns into the datatype datetime
    df_write = date_columns_to_datetime(xml_tablename, df_write)

    # Katalogeintraege: int -> string value
    df_write = replace_mastr_katalogeintraege(
        zipped_xml_file_path=zipped_xml_file_path, df=df_write
    )

    # Act
    add_table_to_sqlite_database(
        df=df_write,
        xml_tablename=xml_tablename,
        sql_tablename=sql_tablename,
        if_exists="append",
        con=testdb_con,
        engine=testdb_engine,
    )

    df_read = pd.read_sql_table(table_name=sql_tablename, con=testdb_engine)
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
        df_read.select_dtypes(exclude="datetime")
    )
