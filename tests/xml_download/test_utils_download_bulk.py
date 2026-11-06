import os
import shutil
import time
from pathlib import Path
from types import SimpleNamespace
from zipfile import ZipFile

import pytest

from open_mastr.utils.constants import BULK_INCLUDE_TABLES_MAP
from open_mastr.xml_download import utils_download_bulk
from open_mastr.xml_download.utils_download_bulk import (
    _find_missing_tables,
    delete_xml_files_not_from_given_date,
    gen_xml_download_url,
    partial_download_with_unzip_http,
)


def test_gen_xml_download_url():
    when = time.strptime("2024-01-01", "%Y-%m-%d")
    url = gen_xml_download_url(when)
    assert isinstance(url, str)
    assert (
        url
        == "https://download.marktstammdatenregister.de/Gesamtdatenexport_20240101_23.2.zip"
    )

    when = time.strptime("2024-04-01", "%Y-%m-%d")
    url = gen_xml_download_url(when)
    assert isinstance(url, str)
    assert (
        url
        == "https://download.marktstammdatenregister.de/Gesamtdatenexport_20240401_23.2.zip"
    )

    when = time.strptime("2024-04-02", "%Y-%m-%d")
    url = gen_xml_download_url(when)
    assert isinstance(url, str)
    assert (
        url
        == "https://download.marktstammdatenregister.de/Gesamtdatenexport_20240402_24.1.zip"
    )

    when = time.strptime("2024-10-01", "%Y-%m-%d")
    url = gen_xml_download_url(when)
    assert isinstance(url, str)
    assert (
        url
        == "https://download.marktstammdatenregister.de/Gesamtdatenexport_20241001_24.1.zip"
    )

    when = time.strptime("2024-10-02", "%Y-%m-%d")
    url = gen_xml_download_url(when)
    assert isinstance(url, str)
    assert (
        url
        == "https://download.marktstammdatenregister.de/Gesamtdatenexport_20241002_24.2.zip"
    )

    when = time.strptime("2024-12-31", "%Y-%m-%d")
    url = gen_xml_download_url(when)
    assert isinstance(url, str)
    assert (
        url
        == "https://download.marktstammdatenregister.de/Gesamtdatenexport_20241231_24.2.zip"
    )

    # Tests for use_version parameter

    when = time.strptime("2024-12-31", "%Y-%m-%d")
    url = gen_xml_download_url(when, use_version="before")
    assert isinstance(url, str)
    assert (
        url
        == "https://download.marktstammdatenregister.de/Gesamtdatenexport_20241231_24.1.zip"
    )

    when = time.strptime("2024-12-31", "%Y-%m-%d")
    url = gen_xml_download_url(when, use_version="after")
    assert isinstance(url, str)
    assert (
        url
        == "https://download.marktstammdatenregister.de/Gesamtdatenexport_20241231_25.1.zip"
    )

    when = time.strptime("2024-04-02", "%Y-%m-%d")
    url = gen_xml_download_url(when, use_version="before")
    assert isinstance(url, str)
    assert (
        url
        == "https://download.marktstammdatenregister.de/Gesamtdatenexport_20240402_23.2.zip"
    )

    when = time.strptime("2024-04-02", "%Y-%m-%d")
    url = gen_xml_download_url(when, use_version="after")
    assert isinstance(url, str)
    assert (
        url
        == "https://download.marktstammdatenregister.de/Gesamtdatenexport_20240402_24.2.zip"
    )


def test_find_missing_tables_is_full_download(tmp_path: Path) -> None:
    # Requesting only some categories on a missing file -> partial download
    save_path = str(tmp_path / "mastr.zip")
    missing, is_full = _find_missing_tables(save_path, ["wind", "solar"])
    assert len(missing) > 1
    assert not is_full

    # Whole-zip request on a missing file -> full download
    all_bulk = list(BULK_INCLUDE_TABLES_MAP.keys())
    missing, is_full = _find_missing_tables(save_path, all_bulk)
    # assert that resolved list is longer than key word list of download
    # tables
    assert len(missing) > len(all_bulk)
    assert is_full

    # Zip present with some tables -> not a full download, only missing returned
    with ZipFile(save_path, "w") as z:
        z.writestr("AnlagenEegWind_foo.xml", "x")
    missing, is_full = _find_missing_tables(save_path, all_bulk)
    assert is_full is False
    assert "anlageneegwind" not in missing


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


class _FakeRemoteZipFile:
    """Stands in for unzip_http.RemoteZipFile; extractzip appends like the real one."""

    members = {
        "EinheitenSolar_1.xml": b"a" * 1000,
        "EinheitenSolar_2.xml": b"b" * 1000,
        "Katalogwerte.xml": b"k" * 10,
    }
    interrupt_in = None

    def __init__(self, url):
        pass

    def namelist(self):
        return list(self.members)

    @property
    def files(self):
        return {
            name: SimpleNamespace(file_size=len(data))
            for name, data in self.members.items()
        }

    def extractzip(self, member, path=None, pwd=None):
        with ZipFile(path, "a") as zout:
            with zout.open(member, "w") as fpout:
                fpout.write(self.members[member][:100])
                if member == self.interrupt_in:
                    raise KeyboardInterrupt
                fpout.write(self.members[member][100:])


@pytest.fixture
def fake_remote_zip(monkeypatch):
    monkeypatch.setattr(
        utils_download_bulk.unzip_http, "RemoteZipFile", _FakeRemoteZipFile
    )
    yield _FakeRemoteZipFile
    _FakeRemoteZipFile.interrupt_in = None


def test_partial_download_interrupted_leaves_no_truncated_member(
    tmp_path: Path, fake_remote_zip
) -> None:
    save_path = str(tmp_path / "Gesamtdatenexport_20261017.zip")
    with ZipFile(save_path, "w") as z:
        z.writestr("AnlagenEegSolar_1.xml", "x")

    fake_remote_zip.interrupt_in = "EinheitenSolar_2.xml"
    with pytest.raises(KeyboardInterrupt):
        partial_download_with_unzip_http(
            save_path, "http://example.invalid", {"einheitensolar", "katalogwerte"}
        )

    with ZipFile(save_path) as z:
        assert z.namelist() == ["AnlagenEegSolar_1.xml"]
    assert not os.path.exists(save_path + ".part")
    missing, _ = _find_missing_tables(save_path, ["solar"])
    assert "einheitensolar" in missing


def test_partial_download_complete_replaces_zip(
    tmp_path: Path, fake_remote_zip
) -> None:
    save_path = str(tmp_path / "Gesamtdatenexport_20261017.zip")
    with ZipFile(save_path, "w") as z:
        z.writestr("AnlagenEegSolar_1.xml", "x")
    # leftover from a run that was killed before it could clean up
    with open(save_path + ".part", "wb") as f:
        f.write(b"stale")

    partial_download_with_unzip_http(
        save_path, "http://example.invalid", {"einheitensolar", "katalogwerte"}
    )

    with ZipFile(save_path) as z:
        assert z.testzip() is None
        assert sorted(z.namelist()) == [
            "AnlagenEegSolar_1.xml",
            "EinheitenSolar_1.xml",
            "EinheitenSolar_2.xml",
            "Katalogwerte.xml",
        ]
        assert z.getinfo("EinheitenSolar_2.xml").file_size == 1000
    assert not os.path.exists(save_path + ".part")
