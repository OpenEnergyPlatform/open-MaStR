from open_mastr.xml_download.utils_write_sqlite import convert_mastr_xml_to_sqlite
from open_mastr.mastr import Mastr
import os
from os.path import expanduser
import sqlite3
import pandas as pd


def test_convert_mastr_xml_to_sqlite():
    date_string = "20220103"
    mastr = Mastr(date_string=date_string)
    con = mastr._bulk_sql_connection
    zipped_xml_file_path = mastr._zipped_xml_file_path
    include_tables = ["einheitenbiomasse"]
    convert_mastr_xml_to_sqlite(
        con=con,
        zipped_xml_file_path=zipped_xml_file_path,
        include_tables=include_tables,
    )

    _sqlite_folder_path = os.path.join(expanduser("~"), ".open-MaStR", "data", "sqlite")
    database_con = sqlite3.connect(os.path.join(_sqlite_folder_path, "bulksqlite.db"))
    df = pd.read_sql(f"SELECT * FROM {include_tables[0]}", con=database_con)
    assert len(df) > 100
    assert len(df.iloc[0]) > 5
