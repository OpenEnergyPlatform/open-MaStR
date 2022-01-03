from open_mastr.xml_download.utils_sqlite_bulk_cleansing import (
    create_katalogwerte_from_sqlite,
    replace_katalogeintraege_in_single_table,
)
import sqlite3
from os.path import expanduser
import os
from open_mastr.xml_download.colums_to_replace import columns_replace_list


def test_create_katalogwerte_from_sqlite():
    _sqlite_folder_path = os.path.join(expanduser("~"), ".open-MaStR", "data", "sqlite")
    database_con = sqlite3.connect(os.path.join(_sqlite_folder_path, "bulksqlite.db"))
    katalogwerte = create_katalogwerte_from_sqlite(database_con)
    assert type(katalogwerte) == dict
    assert len(katalogwerte) > 1000
    assert type(list(katalogwerte.keys())[0]) == int


def test_replace_katalogeintraege_in_single_table():
    _sqlite_folder_path = os.path.join(expanduser("~"), ".open-MaStR", "data", "sqlite")
    con = sqlite3.connect(os.path.join(_sqlite_folder_path, "bulksqlite.db"))
    table_name = "einheitenwasser"
    katalogwerte = create_katalogwerte_from_sqlite(con)

    replace_katalogeintraege_in_single_table(
        con=con,
        table_name=table_name,
        katalogwerte=katalogwerte,
        columns_replace_list=columns_replace_list,
    )

    new_table_name = table_name + "_cleansed"

    c = con.cursor()
    query = f"SELECT count(name) FROM sqlite_master WHERE type='table' AND name='{table_name}'"
    c.execute(query)

    assert c.fetchone()[0] == 1

    c.close()
    con.close()
