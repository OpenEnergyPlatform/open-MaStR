import time
from open_mastr.xml_download.utils_download_bulk import gen_url

def test_gen_url():
    when = time.strptime("2024-01-01", "%Y-%m-%d")
    url = gen_url(when)
    assert type(url) == str
    assert url == "https://download.marktstammdatenregister.de/Gesamtdatenexport_20240101_23.2.zip"

    when = time.strptime("2024-04-01", "%Y-%m-%d")
    url = gen_url(when)
    assert type(url) == str
    assert url == "https://download.marktstammdatenregister.de/Gesamtdatenexport_20240401_23.2.zip"

    when = time.strptime("2024-04-02", "%Y-%m-%d")
    url = gen_url(when)
    assert type(url) == str
    assert url == "https://download.marktstammdatenregister.de/Gesamtdatenexport_20240402_24.1.zip"

    when = time.strptime("2024-10-01", "%Y-%m-%d")
    url = gen_url(when)
    assert type(url) == str
    assert url == "https://download.marktstammdatenregister.de/Gesamtdatenexport_20241001_24.1.zip"

    when = time.strptime("2024-10-02", "%Y-%m-%d")
    url = gen_url(when)
    assert type(url) == str
    assert url == "https://download.marktstammdatenregister.de/Gesamtdatenexport_20241002_24.2.zip"

    when = time.strptime("2024-12-31", "%Y-%m-%d")
    url = gen_url(when)
    assert type(url) == str
    assert url == "https://download.marktstammdatenregister.de/Gesamtdatenexport_20241231_24.2.zip"
