import shutil
from pathlib import Path
from typing import Optional

import os
import re
import sqlalchemy
import pytest
from os.path import expanduser
import pandas as pd
from datetime import date, timedelta
from open_mastr.mastr import Mastr
from open_mastr.utils.constants import TABLE_TRANSLATIONS
from open_mastr.utils.sqlalchemy_tables import CatalogString

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

    with mastr.engine.connect() as con:
        query = sqlalchemy.text(
            "SELECT Gemeinde, Bruttoleistung, WindAnlandOderAufSee"
            " FROM EinheitenWind"
            " WHERE EinheitMastrNummer = 'SEE909443729526'"
        )
        result = con.execute(query)
        rows = result.all()
        assert rows == [("Badbergen", 6800.0, "Windkraft an Land")]


@pytest.mark.skipif(
    not EXISTING_XML_ZIP or not EXISTING_DOCS_ZIP,
    reason="The zipped XML or docs could not be found."
)
def test_mastr_download_latest_real_xml_english(
    mastr: Mastr,
    existing_xml_zip_in_output_dir: Path,
    existing_docs_zip_in_output_dir: Path,
) -> None:
    mastr.download(data="wind", english=True)
    df_wind = pd.read_sql("wind_extended", con=mastr.engine)
    assert len(df_wind) > 10000

    with mastr.engine.connect() as con:
        query = sqlalchemy.text(
            "SELECT municipality, grossCapacity, WindAnlandOderAufSee"
            " FROM wind_extended"
            " WHERE unitMastrNumber = 'SEE909443729526'"
        )
        result = con.execute(query)
        rows = result.all()
        assert rows == [("Badbergen", 6800.0, "Windkraft an Land")]


@pytest.mark.skipif(
    not EXISTING_XML_ZIP,
    reason="The zipped XML or docs could not be found."
)
def test_download_latest_without_altering_tables(
    mastr: Mastr,
    existing_xml_zip_in_output_dir: Path,
) -> None:
    db_table = sqlalchemy.Table(
        'balancing_area',
        sqlalchemy.MetaData(),
        sqlalchemy.Column('id', sqlalchemy.Integer(), primary_key=True, nullable=False, info={'original_name': 'Id', 'normalized_name': 'Id', 'english_name': 'id'}),
        sqlalchemy.Column('yeic', sqlalchemy.String(), info={'original_name': 'Yeic', 'normalized_name': 'Yeic', 'english_name': 'yeic'}),
        sqlalchemy.Column('accountingAreaNetworkConnectionPoint', sqlalchemy.String(), info={'original_name': 'BilanzierungsgebietNetzanschlusspunkt', 'normalized_name': 'BilanzierungsgebietNetzanschlusspunkt', 'english_name': 'accountingAreaNetworkConnectionPoint'}),
        sqlalchemy.Column('dataSource', sqlalchemy.String(), info={'normalized_name': 'DatenQuelle', 'english_name': 'dataSource'}),
        sqlalchemy.Column('downloadDate', sqlalchemy.String(), info={'normalized_name': 'DatumDownload', 'english_name': 'downloadDate'}),
        info={'original_name': 'Bilanzierungsgebiete', 'english_name': 'balancing_area'},
    )
    db_table.create(mastr.engine)

    mastr.download(
        data="balancing_area",
        english=True,
        mastr_table_to_db_table={"Bilanzierungsgebiete": db_table},
        alter_database_tables=False,
    )

    # Check that column RegelzoneNetzanschlusspunkt/controlZoneNetworkConnectionPoint has not been created.
    db_column_names = {column["name"] for column in sqlalchemy.inspect(mastr.engine).get_columns(db_table.name)}
    assert db_column_names == {'id', 'yeic', 'accountingAreaNetworkConnectionPoint', 'dataSource', 'downloadDate'}

    # Check that data has been imported.
    with mastr.engine.connect() as con:
        row_count_query = sqlalchemy.text("SELECT COUNT(*) FROM balancing_area")
        row_count = con.scalar(row_count_query)
        assert row_count > 1000
        query = sqlalchemy.text(
            "SELECT id, yeic, accountingAreaNetworkConnectionPoint"
            " FROM balancing_area"
            " WHERE yeic = '11YW-FREUDENST-L'"
        )
        result = con.execute(query)
        rows = result.all()
        assert rows == [(428, "11YW-FREUDENST-L", "Stromnetz Freudenstadt")]


@pytest.mark.skipif(
    not EXISTING_DOCS_ZIP,
    reason="The zipped docs could not be found."
)
def test_mastr_generate_data_model(
    mastr: Mastr,
    existing_docs_zip_in_output_dir: Path,
) -> None:
    mastr_table_to_db_table = mastr.generate_data_model()
    expected_keys = set(TABLE_TRANSLATIONS.keys()) - {
        # A couple of tables we do not create.
        "Einheitentypen",
        "Katalogkategorien",
        "Katalogwerte",
        "Lokationstypen",
    }
    assert set(mastr_table_to_db_table.keys()) == expected_keys
    # Check some samples
    solar_table = mastr_table_to_db_table["EinheitenSolar"]
    assert solar_table.name == "EinheitenSolar"
    solar_table.info == {"original_name": "EinheitenSolar", "english_name": "solar_extended"}
    # Check a couple of columns
    assert solar_table.c.EinheitMastrNummer.primary_key is True
    assert isinstance(solar_table.c.EinheitMastrNummer.type, sqlalchemy.String)
    assert isinstance(solar_table.c.Bruttoleistung.type, sqlalchemy.Float)
    assert isinstance(solar_table.c.Hauptausrichtung.type, CatalogString)
    assert isinstance(solar_table.c.EinheitlicheAusrichtungUndNeigungswinkel.type, sqlalchemy.Boolean)

    changed_dso_assignment_table = mastr_table_to_db_table["EinheitenAenderungNetzbetreiberzuordnungen"]
    assert changed_dso_assignment_table.c.OpenMastrId.primary_key is True
    assert isinstance(changed_dso_assignment_table.c.OpenMastrId.type, sqlalchemy.Integer)


@pytest.mark.skipif(
    not EXISTING_DOCS_ZIP,
    reason="The zipped docs could not be found."
)
def test_mastr_generate_data_model_english(
    mastr: Mastr,
    existing_docs_zip_in_output_dir: Path,
) -> None:
    mastr_table_to_db_table = mastr.generate_data_model(english=True)
    expected_keys = set(TABLE_TRANSLATIONS.keys()) - {
        # A couple of tables we do not create.
        "Einheitentypen",
        "Katalogkategorien",
        "Katalogwerte",
        "Lokationstypen",
    }
    assert set(mastr_table_to_db_table.keys()) == expected_keys
    # Check some samples
    solar_table = mastr_table_to_db_table["EinheitenSolar"]
    assert solar_table.name == "solar_extended"
    solar_table.info == {"original_name": "EinheitenSolar", "english_name": "solar_extended"}
    # Check a couple of columns
    assert solar_table.c.unitMastrNumber.primary_key is True
    assert isinstance(solar_table.c.unitMastrNumber.type, sqlalchemy.String)
    assert isinstance(solar_table.c.grossCapacity.type, sqlalchemy.Float)
    assert isinstance(solar_table.c.mainOrientation.type, CatalogString)
    assert isinstance(solar_table.c.uniformOrientationAndTiltAngle.type, sqlalchemy.Boolean)

    changed_dso_assignment_table = mastr_table_to_db_table["EinheitenAenderungNetzbetreiberzuordnungen"]
    assert changed_dso_assignment_table.c.OpenMastrId.primary_key is True
    assert isinstance(changed_dso_assignment_table.c.OpenMastrId.type, sqlalchemy.Integer)


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

