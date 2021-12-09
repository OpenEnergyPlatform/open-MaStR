import tempfile
import os
from open_mastr.xml_parser.utils_download_bulk import download_xml_Mastr, get_url_from_Mastr_website

def test_get_url_from_Mastr_website():
    url = get_url_from_Mastr_website
    assert "marktstammdatenregister" in url

def test_download_xml_Mastr():
    url = "https://jsonplaceholder.typicode.com/todos/1"
    save_path = os.path.join(tempfile.gettempdir(),"tempjson.txt") 
    download_xml_Mastr(url=url, save_path=save_path)
    with open(save_path, "r") as f:
        data = f.read()
        assert len(data) > 10

    

