from open_mastr.mastr import Mastr
import os

def test_Mastr_download():
    mastr = Mastr()
    mastr.download(method="bulk")
    assert os.path.exists(mastr._zipped_xml_file_path) 
    assert os.path.getsize(mastr._zipped_xml_file_path) > 900000000