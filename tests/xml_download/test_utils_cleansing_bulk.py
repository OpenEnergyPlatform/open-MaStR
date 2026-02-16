import os
import pandas as pd
import pytest
from os.path import expanduser
from pathlib import Path

from open_mastr.xml_download.utils_cleansing_bulk import (
    cleanse_bulk_data,
    create_katalogwerte_from_bulk_download,
    replace_mastr_katalogeintraege,
)

from tests.conftest import EXISTING_XML_ZIP

# Check if xml file exists
_xml_file_exists = False
_xml_folder_path = os.path.join(expanduser("~"), ".open-MaStR", "data", "xml_download")
if os.path.isdir(_xml_folder_path):
    for entry in os.scandir(path=_xml_folder_path):
        if "Gesamtdatenexport" in entry.name and entry.name.endswith(".zip"):
            _xml_file_exists = True


@pytest.mark.skipif(
    not EXISTING_XML_ZIP,
    reason="The zipped XML could not be found."
)
def test_cleanse_bulk_data(existing_xml_zip_in_output_dir: Path) -> None:
    df_raw = pd.DataFrame(
        {
            "ID": [0, 1, 2],
            "Bundesland": [335, 335, 336],
            "Einheittyp": [1, 8, 5],
        }
    )
    df_replaced = pd.DataFrame(
        {
            "ID": [0, 1, 2],
            "Bundesland": ["Bayern", "Bayern", "Bremen"],
            "Einheittyp": ["Solareinheit", "Stromspeichereinheit", "Geothermie"],
        }
    )

    pd.testing.assert_frame_equal(
        cleanse_bulk_data(
            df=df_raw, zipped_xml_file_path=str(existing_xml_zip_in_output_dir), catalog_columns={"Bundesland", "Einheittyp"},
        ),
        df_replaced,
    )


@pytest.mark.skipif(
    not EXISTING_XML_ZIP,
    reason="The zipped XML could not be found."
)
def test_replace_mastr_katalogeintraege(existing_xml_zip_in_output_dir: Path) -> None:
    df_raw = pd.DataFrame({"ID": [0, 1, 2], "Bundesland": [335, 335, 336]})
    df_replaced = pd.DataFrame(
        {"ID": [0, 1, 2], "Bundesland": ["Bayern", "Bayern", "Bremen"]}
    )
    pd.testing.assert_frame_equal(
        replace_mastr_katalogeintraege(
            zipped_xml_file_path=str(existing_xml_zip_in_output_dir), df=df_raw, catalog_columns={"Bundesland", "Einheittyp"},
        ),
        df_replaced,
    )


@pytest.mark.skipif(
    not EXISTING_XML_ZIP,
    reason="The zipped XML could not be found."
)
def test_create_katalogwerte_from_bulk_download(existing_xml_zip_in_output_dir: Path) -> None:
    katalogwerte = create_katalogwerte_from_bulk_download(
        zipped_xml_file_path=existing_xml_zip_in_output_dir
    )
    assert type(katalogwerte) == dict
    assert len(katalogwerte) > 1000
    assert type(list(katalogwerte.keys())[0]) == int
