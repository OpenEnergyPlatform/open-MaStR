from open_mastr.xml_download.utils_sqlite_bulk_cleansing import (
    create_katalogwerte_from_sqlite,
)
import sqlite3
from open_mastr.mastr import Mastr
from os.path import expanduser
import os


def test_create_katalogwerte_from_sqlite():
    _sqlite_folder_path = os.path.join(expanduser("~"), ".open-MaStR", "data", "sqlite")
    database_con = sqlite3.connect(os.path.join(_sqlite_folder_path, "bulksqlite.db"))
    katalogwerte = create_katalogwerte_from_sqlite(database_con)
    assert type(katalogwerte) == dict
    assert len(katalogwerte) > 1000
    assert type(list(katalogwerte.keys())[0]) == int
