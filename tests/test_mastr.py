import shutil
from pathlib import Path
from typing import Optional

from open_mastr.mastr import Mastr
import os
import re
import sqlalchemy
import pytest
from os.path import expanduser
import pandas as pd
from datetime import date, timedelta

_xml_file_exists = False
_xml_folder_path = os.path.join(expanduser("~"), ".open-MaStR", "data", "xml_download")
if os.path.isdir(_xml_folder_path):
    for entry in os.scandir(path=_xml_folder_path):
        if "Gesamtdatenexport" in entry.name and entry.name.endswith(".zip"):
            _xml_file_exists = True


@pytest.fixture(scope="module")
def zipped_xml_file_path() -> Optional[str]:
    zipped_xml_file_path = None
    for entry in os.scandir(path=_xml_folder_path):
        if "Gesamtdatenexport" in entry.name and entry.name.endswith(".zip"):
            zipped_xml_file_path = os.path.join(_xml_folder_path, entry.name)

    return zipped_xml_file_path


@pytest.fixture
def mastr(tmp_path: Path) -> Mastr:
    output_dir = tmp_path / "output_dir"
    return Mastr(output_dir=output_dir)


def test_mastr_init(mastr: Mastr) -> None:
    # test if folder structure exists
    assert os.path.exists(mastr.home_directory)
    assert os.path.exists(mastr._sqlite_folder_path)
    # test if engine and connection were created
    assert type(mastr.engine) == sqlalchemy.engine.Engine


@pytest.mark.dependency(name="bulk_downloaded")
@pytest.mark.skipif(
    not _xml_file_exists, reason="The zipped xml file could not be found."
)
def test_mastr_download(mastr: Mastr) -> None:
    mastr.download(data="wind")
    df_wind = pd.read_sql("EinheitenWind", con=mastr.engine)
    assert len(df_wind) > 10000

    mastr.download(data="biomass")
    df_biomass = pd.read_sql("EinheitenBiomasse", con=mastr.engine)
    assert len(df_wind) > 10000
    assert len(df_biomass) > 10000

    mastr.download(data=["wind", "nuclear"])
    df_wind = pd.read_sql("EinheitenWind", con=mastr.engine)
    df_biomass = pd.read_sql("EinheitenBiomasse", con=mastr.engine)
    df_nuclear = pd.read_sql("EinheitenKernkraft", con=mastr.engine)
    assert len(df_wind) > 10000
    assert len(df_biomass) > 10000
    assert len(df_nuclear) > 1


@pytest.mark.dependency(depends=["bulk_downloaded"])
def test_mastr_download_keep_old_files(mastr: Mastr, zipped_xml_file_path: Optional[str]) -> None:
    file_today = zipped_xml_file_path
    yesterday = (date.today() - timedelta(days=1)).strftime("%Y%m%d")
    file_old = re.sub(r"\d{8}", yesterday, os.path.basename(file_today))
    file_old = os.path.join(os.path.dirname(zipped_xml_file_path), file_old)
    shutil.copy(file_today, file_old)
    mastr.download(data="gsgk", keep_old_files=True)

    assert os.path.exists(file_old)
