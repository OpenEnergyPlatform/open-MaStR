from datetime import date
from open_mastr.mastr import Mastr
import os
import sqlalchemy
import sqlite3

def test_Mastr_init():
    db = Mastr()
    # test if folder structure exists
    assert os.path.exists(db._xml_folder_path)
    assert os.path.exists(db._sqlite_folder_path)

    # test if engine and connection were created
    assert type(db._engine) == sqlalchemy.engine.base.Engine
    assert type(db._sql_connection) == sqlite3.Connection


#def test_Mastr_download():
#    date_string = "20211206"
#    mastr = Mastr(date_string=date_string)
#    assert mastr._today_string == date_string

    #mastr = Mastr()
    #assert len(mastr._today_string) == 8

    #include_tables=["einheitenwind"]
    #mastr.download(method="bulk", include_tables=include_tables)
    #assert os.path.getsize(mastr._zipped_xml_file_path) > 900000000


