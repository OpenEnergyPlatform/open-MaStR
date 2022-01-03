from open_mastr.xml_download.utils_write_sqlite import convert_mastr_xml_to_sqlite
import os
from os.path import expanduser
import sqlite3
import pandas as pd


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
