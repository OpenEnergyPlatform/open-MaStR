import pandas as pd
import pytest
from pathlib import Path

from open_mastr.xml_download.utils_cleansing_bulk import (
    cleanse_bulk_data,
    create_katalogwerte_from_bulk_download,
    replace_mastr_katalogeintraege,
)


def test_cleanse_bulk_data(mockup_xml_zip_in_output_dir: Path) -> None:
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
            df=df_raw,
            zipped_xml_file_path=str(mockup_xml_zip_in_output_dir),
            catalog_columns={"Bundesland", "Einheittyp"},
        ),
        df_replaced,
        check_dtype=False,
    )


def test_replace_mastr_katalogeintraege(mockup_xml_zip_in_output_dir: Path) -> None:
    df_raw = pd.DataFrame({"ID": [0, 1, 2], "Bundesland": [335, 335, 336]})
    df_replaced = pd.DataFrame(
        {"ID": [0, 1, 2], "Bundesland": ["Bayern", "Bayern", "Bremen"]}
    )
    pd.testing.assert_frame_equal(
        replace_mastr_katalogeintraege(
            zipped_xml_file_path=str(mockup_xml_zip_in_output_dir),
            df=df_raw,
            catalog_columns={"Bundesland", "Einheittyp"},
        ),
        df_replaced,
    )


def test_create_katalogwerte_from_bulk_download(
    mockup_xml_zip_in_output_dir: Path,
) -> None:
    katalogwerte = create_katalogwerte_from_bulk_download(
        zipped_xml_file_path=mockup_xml_zip_in_output_dir
    )
    assert isinstance(katalogwerte, dict)
    assert len(katalogwerte) > 1000
    assert isinstance(list(katalogwerte.keys())[0], int)
