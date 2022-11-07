from open_mastr.xml_download.utils_download_bulk import get_url_from_Mastr_website


def test_get_url_from_Mastr_website():
    url = get_url_from_Mastr_website()
    assert len(url) > 10
    assert type(url) == str
    assert "marktstammdaten" in url
