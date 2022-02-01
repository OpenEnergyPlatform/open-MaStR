from open_mastr.xml_download.utils_sqlite_bulk_cleansing import (
    create_katalogwerte_from_sqlite,
    replace_katalogeintraege_in_single_table,
)
import sqlite3
from os.path import expanduser
import os
from open_mastr.xml_download.colums_to_replace import columns_replace_list
import pytest


@pytest.fixture(scope="module")
def con():
    _sqlite_folder_path = os.path.join(expanduser("~"), ".open-MaStR", "data", "sqlite")
    con = sqlite3.connect(os.path.join(_sqlite_folder_path, "bulksqlite.db"))
    yield con
    con.close()


def test_create_katalogwerte_from_sqlite(con):
    katalogwerte = create_katalogwerte_from_sqlite(con)
    assert type(katalogwerte) == dict
    assert len(katalogwerte) > 1000
    assert type(list(katalogwerte.keys())[0]) == int


def test_replace_katalogeintraege_in_single_table(con):
    table_name = "einheitenwasser"
    katalogwerte = create_katalogwerte_from_sqlite(con)
    replace_katalogeintraege_in_single_table(
        con=con,
        table_name=table_name,
        katalogwerte=katalogwerte,
        columns_replace_list=columns_replace_list,
    )

    new_table_name = table_name + "_cleansed"
    cur = con.cursor()
    query = f"SELECT count(name) FROM sqlite_master WHERE type='table' AND name='{new_table_name}'"
    cur.execute(query)

    assert cur.fetchone()[0] == 1
    cur.close()
