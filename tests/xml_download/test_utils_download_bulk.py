import time
from open_mastr.xml_download.utils_download_bulk import (
    gen_url,
    delete_xml_files_not_from_given_date,
)
import os
import shutil


def test_gen_url():
    when = time.strptime("2024-01-01", "%Y-%m-%d")
    url = gen_url(when)
    assert type(url) == str
    assert (
        url
        == "https://download.marktstammdatenregister.de/Gesamtdatenexport_20240101_23.2.zip"
    )

    when = time.strptime("2024-04-01", "%Y-%m-%d")
    url = gen_url(when)
    assert type(url) == str
    assert (
        url
        == "https://download.marktstammdatenregister.de/Gesamtdatenexport_20240401_23.2.zip"
    )

    when = time.strptime("2024-04-02", "%Y-%m-%d")
    url = gen_url(when)
    assert type(url) == str
    assert (
        url
        == "https://download.marktstammdatenregister.de/Gesamtdatenexport_20240402_24.1.zip"
    )

    when = time.strptime("2024-10-01", "%Y-%m-%d")
    url = gen_url(when)
    assert type(url) == str
    assert (
        url
        == "https://download.marktstammdatenregister.de/Gesamtdatenexport_20241001_24.1.zip"
    )

    when = time.strptime("2024-10-02", "%Y-%m-%d")
    url = gen_url(when)
    assert type(url) == str
    assert (
        url
        == "https://download.marktstammdatenregister.de/Gesamtdatenexport_20241002_24.2.zip"
    )

    when = time.strptime("2024-12-31", "%Y-%m-%d")
    url = gen_url(when)
    assert type(url) == str
    assert (
        url
        == "https://download.marktstammdatenregister.de/Gesamtdatenexport_20241231_24.2.zip"
    )

    # Tests for use_version parameter

    when = time.strptime("2024-12-31", "%Y-%m-%d")
    url = gen_url(when, use_version="before")
    assert type(url) == str
    assert (
        url
        == "https://download.marktstammdatenregister.de/Gesamtdatenexport_20241231_24.1.zip"
    )

    when = time.strptime("2024-12-31", "%Y-%m-%d")
    url = gen_url(when, use_version="after")
    assert type(url) == str
    assert (
        url
        == "https://download.marktstammdatenregister.de/Gesamtdatenexport_20241231_25.1.zip"
    )

    when = time.strptime("2024-04-02", "%Y-%m-%d")
    url = gen_url(when, use_version="before")
    assert type(url) == str
    assert (
        url
        == "https://download.marktstammdatenregister.de/Gesamtdatenexport_20240402_23.2.zip"
    )

    when = time.strptime("2024-04-02", "%Y-%m-%d")
    url = gen_url(when, use_version="after")
    assert type(url) == str
    assert (
        url
        == "https://download.marktstammdatenregister.de/Gesamtdatenexport_20240402_24.2.zip"
    )


def test_delete_xml_files_not_from_given_date():
    xml_folder_path = os.path.join("tests", "test_utils_download")
    expected_file = os.path.join(xml_folder_path, "20250102.txt")
    os.makedirs(xml_folder_path)

    # Case where expected file exists
    open(expected_file, "w").close()
    delete_xml_files_not_from_given_date(
        save_path=expected_file, xml_folder_path=xml_folder_path
    )
    assert os.path.exists(expected_file)
    os.remove(expected_file)

    # Case where old date is deleted
    path_old_file = os.path.join(xml_folder_path, "20250101.txt")
    open(path_old_file, "w").close()
    delete_xml_files_not_from_given_date(
        save_path=expected_file, xml_folder_path=xml_folder_path
    )
    assert not os.path.exists(path_old_file)
    # clean up test folder
    shutil.rmtree(xml_folder_path)
