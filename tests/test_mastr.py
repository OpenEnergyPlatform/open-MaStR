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

from tests.conftest import EXISTING_DOCS_ZIP, EXISTING_XML_ZIP


def test_mastr_init(mastr: Mastr) -> None:
    # test if folder structure exists
    assert os.path.exists(mastr._sqlite_folder_path)
    # test if engine and connection were created
    assert type(mastr.engine) == sqlalchemy.engine.Engine


@pytest.mark.dependency(name="bulk_downloaded")
@pytest.mark.skipif(
    not EXISTING_XML_ZIP or not EXISTING_DOCS_ZIP,
    reason="The zipped XML or docs could not be found."
)
def test_mastr_download_latest_real_xml(
    mastr: Mastr,
    existing_xml_zip_in_output_dir: Path,
    existing_docs_zip_in_output_dir: Path,
) -> None:
    mastr.download(data="wind")
    df_wind = pd.read_sql("EinheitenWind", con=mastr.engine)
    assert len(df_wind) > 10000

    mastr.download(data="biomass")
    df_biomass = pd.read_sql("EinheitenBiomasse", con=mastr.engine)
    # Test that old biomass data is not deleted.
    assert len(df_wind) > 10000
    assert len(df_biomass) > 10000

    # Test that we can pass a list of data.
    mastr.download(data=["wind", "nuclear"])
    df_wind = pd.read_sql("EinheitenWind", con=mastr.engine)
    df_biomass = pd.read_sql("EinheitenBiomasse", con=mastr.engine)
    df_nuclear = pd.read_sql("EinheitenKernkraft", con=mastr.engine)
    assert len(df_wind) > 10000
    assert len(df_biomass) > 10000
    assert len(df_nuclear) > 1


@pytest.mark.dependency(depends=["bulk_downloaded"])
@pytest.mark.skipif(
    not EXISTING_XML_ZIP or not EXISTING_DOCS_ZIP,
    reason="The zipped XML or docs could not be found."
)
def test_mastr_download_keep_old_downloads(
    mastr: Mastr,
    existing_xml_zip_in_output_dir: Path,
    existing_docs_zip_in_output_dir: Path,
) -> None:
    file_today = existing_xml_zip_in_output_dir
    if not file_today:
        raise ValueError(
            "Zip file is missing. This should never happen and indicates a faulty test."
            " The file has somehow been deleted between test discovery time and this test"
            " being started."
        )
    yesterday = (date.today() - timedelta(days=1)).strftime("%Y%m%d")
    file_old_basename = re.sub(r"\d{8}", yesterday, os.path.basename(file_today))
    file_old = os.path.join(os.path.dirname(existing_xml_zip_in_output_dir), file_old_basename)
    shutil.copy(file_today, file_old)
    mastr.download(data="gsgk", keep_old_downloads=True)

    assert os.path.exists(file_old)

