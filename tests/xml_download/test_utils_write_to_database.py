import os
import sqlite3
import sys
from datetime import datetime
from os.path import expanduser
from zipfile import ZipFile

import numpy as np
import pandas as pd
import pytest
from sqlalchemy import (
    Boolean,
    Column,
    create_engine,
    Date,
    DateTime,
    Double,
    Integer,
    MetaData,
    String,
    Table,
)


from open_mastr.xml_download.utils_write_to_database import (
    add_missing_columns_to_table,
    add_zero_as_first_character_for_too_short_string,
    cast_date_columns_to_string,
    correct_ordering_of_filelist,
    extract_sql_table_name,
    extract_xml_table_name,
    is_date_column,
    is_first_file,
    process_table_before_insertion,
    read_xml_file,
    add_table_to_non_sqlite_database,
    add_table_to_sqlite_database,
    interleave_files,
)

# Check if xml file exists
_xml_file_exists = False
_xml_folder_path = os.path.join(expanduser("~"), ".open-MaStR", "data", "xml_download")
if os.path.isdir(_xml_folder_path):
    for entry in os.scandir(path=_xml_folder_path):
        if "Gesamtdatenexport" in entry.name and entry.name.endswith(".zip"):
            _xml_file_exists = True


# Silence ValueError caused by logger https://github.com/pytest-dev/pytest/issues/5502
@pytest.fixture(autouse=True)
def capture_wrap():
    sys.stderr.close = lambda *args: None
    sys.stdout.close = lambda *args: None
    yield


@pytest.fixture(scope="module")
def zipped_xml_file_path():
    # TODO: Remove this
    return "/home/gorgor/.open-MaStR/data/Gesamtdatenexport_20251228.zip"
    zipped_xml_file_path = None
    for entry in os.scandir(path=_xml_folder_path):
        if "Gesamtdatenexport" in entry.name and entry.name.endswith(".zip"):
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


def test_extract_xml_table_name():
    file_name = "Netzanschlusspunkte_31.xml"
    assert extract_xml_table_name(file_name) == "netzanschlusspunkte"


def text_extract_sql_table_name():
    xml_table_name = "netzanschlusspunkte"
    assert extract_sql_table_name(xml_table_name) == "network_connection_points"


def test_is_first_file():
    assert is_first_file("EinheitenKernkraft.xml") is True
    assert is_first_file("EinheitenKernkraft_1.xml") is True
    assert is_first_file("EinheitenKernkraft_2.xml") is False


def test_cast_date_columns_to_string():
    table = Table(
        "anlageneegwasser",
        MetaData(),
        Column("EegMastrNummer", String, primary_key=True),
        Column("Registrierungsdatum", Date),
        Column("DatumLetzteAktualisierung", DateTime),
    )
    initial_df = pd.DataFrame(
        {
            "EegMastrNummer": ["1", "2", "3"],
            "Registrierungsdatum": [
                datetime(2024, 3, 11).date(),
                datetime(1999, 2, 1).date(),
                np.datetime64("nat"),
            ],
            "DatumLetzteAktualisierung": [
                datetime(2022, 3, 22),
                datetime(2020, 1, 2, 10, 12, 46),
                np.datetime64("nat"),
            ],
        }
    )
    expected_df = pd.DataFrame(
        {
            "EegMastrNummer": ["1", "2", "3"],
            "Registrierungsdatum": ["2024-03-11", "1999-02-01", np.nan],
            "DatumLetzteAktualisierung": [
                "2022-03-22 00:00:00.000000",
                "2020-01-02 10:12:46.000000",
                np.nan,
            ],
        }
    )

    pd.testing.assert_frame_equal(
        expected_df, cast_date_columns_to_string(table, initial_df)
    )


def test_is_date_column():
    assert is_date_column(Column("Id", Integer, primary_key=True)) is False
    assert is_date_column(Column("DatumLetzteAktualisierung", DateTime)) is True
    assert is_date_column(Column("WiederinbetriebnahmeDatum", Date)) is True


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


@pytest.mark.skipif(
    not _xml_file_exists, reason="The zipped xml file could not be found."
)
def test_read_xml_file(zipped_xml_file_path):
    file_name = "EinheitenStromVerbraucher"
    with ZipFile(zipped_xml_file_path, "r") as f:
        df = read_xml_file(f, f"{file_name}.xml")

    assert df.shape[0] > 0


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


# TODO: Do we want to keep this kind of renaming?
@pytest.mark.skip
def test_change_column_names_to_orm_format():
    initial_df = pd.DataFrame(
        {
            "VerknuepfteEinheitenMaStRNummern": ["test1", "test2"],
            "NetzanschlusspunkteMaStRNummern": [1, 2],
        }
    )
    expected_df = pd.DataFrame(
        {
            "VerknuepfteEinheiten": ["test1", "test2"],
            "Netzanschlusspunkte": [1, 2],
        }
    )

    pd.testing.assert_frame_equal(
        expected_df, change_column_names_to_orm_format(initial_df, "lokationen")
    )


@pytest.mark.skipif(
    not _xml_file_exists, reason="The zipped xml file could not be found."
)
def test_process_table_before_insertion(zipped_xml_file_path):
    bulk_download_date = datetime.now().date().strftime("%Y%m%d")
    initial_df = pd.DataFrame(
        {
            "Gemeindeschluessel": [9162000, 19123456],
            "Postleitzahl": [1234, 54321],
            "NameKraftwerk": ["test1", "test2"],
            "LokationMaStRNummer": ["test3", "test4"],
        }
    )
    expected_df = pd.DataFrame(
        {
            "Gemeindeschluessel": ["09162000", "19123456"],
            "Postleitzahl": ["01234", "54321"],
            "NameKraftwerk": ["test1", "test2"],
            "LokationMastrNummer": ["test3", "test4"],
            "DatenQuelle": ["bulk", "bulk"],
            "DatumDownload": [bulk_download_date, bulk_download_date],
        }
    )

    pd.testing.assert_frame_equal(
        expected_df,
        process_table_before_insertion(
            initial_df,
            "einheitenkernkraft",
            zipped_xml_file_path,
            bulk_download_date,
            bulk_cleansing=False,
        ),
    )


def test_add_missing_columns_to_table(engine_testdb):
    table = Table(
        "einheitengasverbraucher",
        MetaData(),
        Column("EinheitMastrNummer", String, primary_key=True),
        Column("DatumLetzteAktualisierung", DateTime),
    )
    # We must recreate the table to be sure that the new column is not present.
    table.drop(engine_testdb, checkfirst=True)
    table.create(engine_testdb)
    with engine_testdb.connect() as con:
        with con.begin():
            initial_data_in_db = pd.DataFrame(
                {
                    "EinheitMastrNummer": ["id1"],
                    "DatumLetzteAktualisierung": [datetime(2022, 2, 2)],
                }
            )
            initial_data_in_db.to_sql(
                table.name, con=con, if_exists="append", index=False
            )

    add_missing_columns_to_table(
        engine_testdb, table, ["NewColumn"]
    )

    expected_df = pd.DataFrame(
        {
            "EinheitMastrNummer": ["id1"],
            "DatumLetzteAktualisierung": [datetime(2022, 2, 2)],
            "NewColumn": [None],
        }
    )
    with engine_testdb.connect() as con:
        with con.begin():
            actual_df = pd.read_sql_table(table.name, con=con)
            # The actual_df will contain more columns than the expected_df, so we can't use assert_frame_equal.
            assert expected_df.index.isin(actual_df.index).all()


@pytest.mark.parametrize(
    "add_table_to_database_function",
    [add_table_to_sqlite_database, add_table_to_non_sqlite_database],
)
def test_add_table_to_sqlite_database(engine_testdb, add_table_to_database_function):
    table = Table(
        "anlageneeggeothermiegrubengasdruckentspannung",
        MetaData(),
        Column("EegMastrNummer", String, primary_key=True),
        Column("InstallierteLeistung", Double),
        Column("AnlageBetriebsstatus", String),
        Column("Registrierungsdatum", Date),
        Column("Meldedatum", DateTime),
        Column("DatumLetzteAktualisierung", DateTime),
        Column("EegInbetriebnahmedatum", DateTime),
        Column("VerknuepfteEinheit", String),
        Column("AnlagenschluesselEeg", String),
        Column("AusschreibungZuschlag", Boolean),
        Column("AnlagenkennzifferAnlagenregister", String),
        Column("AnlagenkennzifferAnlagenregister_nv", String),
        Column("Netzbetreiberzuordnungen", String),
        Column("DatenQuelle", String),
        Column("DatumDownload", DateTime),
    )
    # We must recreate the table to be sure that no other data is present.
    table.drop(engine_testdb, checkfirst=True)
    table.create(engine_testdb)

    df = pd.DataFrame(
        {
            "Registrierungsdatum": ["2022-02-02", "2024-03-20"],
            "EegMastrNummer": ["id1", "id2"],
            "DatumLetzteAktualisierung": [
                "2022-12-02 10:10:10.000300",
                "2024-10-10 00:00:00.000000",
            ],
            "AusschreibungZuschlag": [True, False],
            "Netzbetreiberzuordnungen": ["test1", "test2"],
            "InstallierteLeistung": [1.0, 100.4],
        }
    )
    expected_df = pd.DataFrame(
        {
            "EegMastrNummer": ["id1", "id2"],
            "InstallierteLeistung": [1.0, 100.4],
            "AnlageBetriebsstatus": [None, None],
            "Registrierungsdatum": [datetime(2022, 2, 2), datetime(2024, 3, 20)],
            "Meldedatum": [np.datetime64("NaT"), np.datetime64("NaT")],
            "DatumLetzteAktualisierung": [
                datetime(2022, 12, 2, 10, 10, 10, 300),
                datetime(2024, 10, 10),
            ],
            "EegInbetriebnahmedatum": [np.datetime64("NaT"), np.datetime64("NaT")],
            "VerknuepfteEinheit": [None, None],
            "AnlagenschluesselEeg": [None, None],
            "AusschreibungZuschlag": [True, False],
            "AnlagenkennzifferAnlagenregister": [None, None],
            "AnlagenkennzifferAnlagenregister_nv": [None, None],
            "Netzbetreiberzuordnungen": ["test1", "test2"],
            "DatenQuelle": [None, None],
            "DatumDownload": [np.datetime64("NaT"), np.datetime64("NaT")],
        }
    )

    add_table_to_database_function(
        df, table, engine_testdb
    )
    with engine_testdb.connect() as con:
        with con.begin():
            pd.testing.assert_frame_equal(
                expected_df, pd.read_sql_table(table.name, con=con)
            )


def test_interleave_files():
    input_data = [
        ("AnlagenEegBiomasse.xml", "anlageneegbiomasse", "anlageneegbiomasse"),
        ("AnlagenEegSolar_1.xml", "anlageneegsolar", "anlageneegsolar"),
        ("AnlagenEegSolar_2.xml", "anlageneegsolar", "anlageneegsolar"),
        ("AnlagenEegWind_1.xml", "anlageneegwind", "anlageneegwind"),
        ("AnlagenEegWind_2.xml", "anlageneegwind", "anlageneegwind"),
        ("AnlagenEegWind_3.xml", "anlageneegwind", "anlageneegwind"),
        ("Bilanzierungsgebiete.xml", "bilanzierungsgebiete", "bilanzierungsgebiete"),
    ]
    assert interleave_files(input_data) == [
        ("AnlagenEegBiomasse.xml", "anlageneegbiomasse", "anlageneegbiomasse"),
        ("AnlagenEegSolar_1.xml", "anlageneegsolar", "anlageneegsolar"),
        ("AnlagenEegWind_1.xml", "anlageneegwind", "anlageneegwind"),
        ("Bilanzierungsgebiete.xml", "bilanzierungsgebiete", "bilanzierungsgebiete"),
        ("AnlagenEegSolar_2.xml", "anlageneegsolar", "anlageneegsolar"),
        ("AnlagenEegWind_2.xml", "anlageneegwind", "anlageneegwind"),
        ("AnlagenEegWind_3.xml", "anlageneegwind", "anlageneegwind"),
    ]
