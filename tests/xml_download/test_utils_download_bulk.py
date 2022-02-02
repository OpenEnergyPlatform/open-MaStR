import tempfile
import os

from open_mastr.xml_download.utils_download_bulk import (
    get_url_from_Mastr_website,
    download_xml_Mastr,
)


def test_get_url_from_Mastr_website():
    url = get_url_from_Mastr_website()
    assert len(url) > 10
    assert type(url) == str
    assert "marktstammdaten" in url


#def test_download_xml_Mastr():
#    save_path = os.path.join(tempfile.gettempdir(), "tempjson.txt")
#    download_xml_Mastr(save_path=save_path)
#    with open(save_path, "r") as f:
#        data = f.read()
#        assert len(data) > 10
