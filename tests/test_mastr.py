from datetime import date
from open_mastr.mastr import Mastr
import os

def test_Mastr_init():
    mastr = Mastr()
    assert os.path.exists(mastr._zipped_xml_file_path) 
    assert os.path.exists(mastr._sqlite_folder_path) 


def test_Mastr_download():
    date_string = "20211206"
    mastr = Mastr(date_string=date_string)
    assert mastr._today_string == date_string

    mastr = Mastr()
    assert len(mastr._today_string) == 8

    include_tables=["einheitenwind"]
    mastr.download(method="bulk", include_tables=include_tables)
    assert os.path.getsize(mastr._zipped_xml_file_path) > 900000000


