import io
import zipfile
from pathlib import Path

import os
import sqlalchemy
import pytest
import responses
import pandas as pd
from open_mastr.mastr import Mastr
from open_mastr.utils.constants import TABLE_TRANSLATIONS
from open_mastr.utils.sqlalchemy_tables import CatalogString
from .conftest import NUMBER_ROWS_IN_MOCK_XML_FILES


def test_mastr_init(mastr: Mastr) -> None:
    assert os.path.exists(mastr._sqlite_folder_path)
    assert isinstance(mastr.engine, sqlalchemy.engine.Engine)


def test_download_wind(
    mastr: Mastr,
    mockup_xml_zip_in_output_dir: Path,
    mockup_docs_zip_in_output_dir: Path,
) -> None:
    mastr.download(data="wind")
    df = pd.read_sql("EinheitenWind", con=mastr.engine)
    assert 0 < len(df) <= NUMBER_ROWS_IN_MOCK_XML_FILES


def test_download_multiple_technologies(
    mastr: Mastr,
    mockup_xml_zip_in_output_dir: Path,
    mockup_docs_zip_in_output_dir: Path,
) -> None:
    mastr.download(data=["biomass", "hydro"])
    df_biomass = pd.read_sql("EinheitenBiomasse", con=mastr.engine)
    df_hydro = pd.read_sql("EinheitenWasser", con=mastr.engine)
    assert 0 < len(df_biomass) <= NUMBER_ROWS_IN_MOCK_XML_FILES
    assert 0 < len(df_hydro) <= NUMBER_ROWS_IN_MOCK_XML_FILES


def test_download_accumulates_across_calls(
    mastr: Mastr,
    mockup_xml_zip_in_output_dir: Path,
    mockup_docs_zip_in_output_dir: Path,
) -> None:
    mastr.download(data="wind")
    mastr.download(data="biomass")
    df_wind = pd.read_sql("EinheitenWind", con=mastr.engine)
    df_biomass = pd.read_sql("EinheitenBiomasse", con=mastr.engine)
    assert 0 < len(df_wind) <= NUMBER_ROWS_IN_MOCK_XML_FILES
    assert 0 < len(df_biomass) <= NUMBER_ROWS_IN_MOCK_XML_FILES


def test_download_english_table_names(
    mastr: Mastr,
    mockup_xml_zip_in_output_dir: Path,
    mockup_docs_zip_in_output_dir: Path,
) -> None:
    mastr.download(data="wind", english=True)
    df = pd.read_sql("units_wind", con=mastr.engine)
    assert 0 < len(df) <= NUMBER_ROWS_IN_MOCK_XML_FILES
    # TODO: Add assertion of english names in tables / columns


# TODO add a test that checks the VIEW creation


def test_download_no_cleansing(
    mastr: Mastr,
    mockup_xml_zip_in_output_dir: Path,
    mockup_docs_zip_in_output_dir: Path,
) -> None:
    mastr.download(data="wind", bulk_cleansing=False)
    df = pd.read_sql("EinheitenWind", con=mastr.engine)
    assert 0 < len(df) <= NUMBER_ROWS_IN_MOCK_XML_FILES
    # TODO: Add assertion of non cleansed data


def test_download_keep_old_downloads(
    mastr: Mastr,
    mockup_xml_zip_in_output_dir: Path,
    mockup_docs_zip_in_output_dir: Path,
) -> None:
    old_zip = mockup_xml_zip_in_output_dir
    mastr.download(data="wind", keep_old_downloads=True)
    assert old_zip.exists()
    # TODO: Check that this really tests the functionality here
    # not sure if the mockup works to test the keep downloads


def test_download_no_alter_tables(
    mastr: Mastr,
    mockup_xml_zip_in_output_dir: Path,
) -> None:
    db_table = sqlalchemy.Table(
        "wind_minimal",
        sqlalchemy.MetaData(),
        sqlalchemy.Column(
            "EinheitMastrNummer",
            sqlalchemy.String(),
            primary_key=True,
            info={
                "original_name": "EinheitMastrNummer",
                "normalized_name": "EinheitMastrNummer",
                "english_name": "unitMastrNumber",
            },
        ),
        sqlalchemy.Column(
            "DatenQuelle",
            sqlalchemy.String(),
            info={"normalized_name": "DatenQuelle", "english_name": "dataSource"},
        ),
        sqlalchemy.Column(
            "DatumDownload",
            sqlalchemy.String(),
            info={"normalized_name": "DatumDownload", "english_name": "downloadDate"},
        ),
        info={"original_name": "EinheitenWind", "english_name": "units_wind"},
    )
    db_table.create(mastr.engine)

    mastr.download(
        data="wind",
        mastr_table_to_db_table={"EinheitenWind": db_table},
        alter_database_tables=False,
    )

    db_column_names = {
        col["name"]
        for col in sqlalchemy.inspect(mastr.engine).get_columns(db_table.name)
    }

    assert "Bruttoleistung" not in db_column_names
    assert db_column_names == {"EinheitMastrNummer", "DatenQuelle", "DatumDownload"}

    with mastr.engine.connect() as con:
        row_count = con.scalar(sqlalchemy.text("SELECT COUNT(*) FROM wind_minimal"))
    assert row_count == NUMBER_ROWS_IN_MOCK_XML_FILES


def test_mastr_generate_data_model(
    mastr: Mastr,
    mockup_docs_zip_in_output_dir: Path,
) -> None:
    mastr_table_to_db_table = mastr.generate_data_model()
    expected_keys = set(TABLE_TRANSLATIONS.keys()) - {
        # A couple of tables with meta information we do not create.
        "Einheitentypen",
        "Katalogkategorien",
        "Katalogwerte",
        "Lokationstypen",
        "Marktrollen",
        "Marktfunktionen",
    }
    assert set(mastr_table_to_db_table.keys()) == expected_keys
    # Check some samples
    solar_table = mastr_table_to_db_table["EinheitenSolar"]
    assert solar_table.name == "EinheitenSolar"
    assert solar_table.info == {
        "original_name": "EinheitenSolar",
        "english_name": "units_solar",
    }
    # Check a couple of columns
    assert solar_table.c.EinheitMastrNummer.primary_key is True
    assert isinstance(solar_table.c.EinheitMastrNummer.type, sqlalchemy.String)
    assert isinstance(solar_table.c.Bruttoleistung.type, sqlalchemy.Float)
    assert isinstance(solar_table.c.Hauptausrichtung.type, CatalogString)
    assert isinstance(
        solar_table.c.EinheitlicheAusrichtungUndNeigungswinkel.type, sqlalchemy.Boolean
    )
    changed_dso_assignment_table = mastr_table_to_db_table[
        "EinheitenAenderungNetzbetreiberzuordnungen"
    ]
    assert changed_dso_assignment_table.c.OpenMastrId.primary_key is True
    assert isinstance(
        changed_dso_assignment_table.c.OpenMastrId.type, sqlalchemy.Integer
    )


def test_mastr_generate_data_model_english(
    mastr: Mastr,
    mockup_docs_zip_in_output_dir: Path,
) -> None:
    mastr_table_to_db_table = mastr.generate_data_model(english=True)
    expected_keys = set(TABLE_TRANSLATIONS.keys()) - {
        # A couple of tables with meta information we do not create.
        "Einheitentypen",
        "Katalogkategorien",
        "Katalogwerte",
        "Lokationstypen",
        "Marktrollen",
        "Marktfunktionen",
    }
    assert set(mastr_table_to_db_table.keys()) == expected_keys
    # Check some samples
    solar_table = mastr_table_to_db_table["EinheitenSolar"]
    assert solar_table.name == "units_solar"
    assert solar_table.info == {
        "original_name": "EinheitenSolar",
        "english_name": "units_solar",
    }
    # Check a couple of columns
    assert solar_table.c.unitMastrNumber.primary_key is True
    assert isinstance(solar_table.c.unitMastrNumber.type, sqlalchemy.String)
    assert isinstance(solar_table.c.grossCapacity.type, sqlalchemy.Float)
    assert isinstance(solar_table.c.mainOrientation.type, CatalogString)
    assert isinstance(
        solar_table.c.uniformOrientationAndTiltAngle.type, sqlalchemy.Boolean
    )
    changed_dso_assignment_table = mastr_table_to_db_table[
        "EinheitenAenderungNetzbetreiberzuordnungen"
    ]
    assert changed_dso_assignment_table.c.OpenMastrId.primary_key is True
    assert isinstance(
        changed_dso_assignment_table.c.OpenMastrId.type, sqlalchemy.Integer
    )


def test_mastr_generate_data_model_fallback_to_included_docs(
    mastr: Mastr,
    output_dir: Path,
    responses: responses.RequestsMock,
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level("ERROR")

    invalid_xsd = """<?xml version="1.0" encoding="UTF-8"?>
    <xs:schema attributeFormDefault="unqualified" elementFormDefault="qualified" xmlns:xs="http://www.w3.org/2001/XMLSchema">
      <xs:element name="Netze">
        <xs:complexType>
          <xs:sequence>
            <xs:element name="Netz" maxOccurs="unbounded" minOccurs="0">
              <xs:complexType>
                <xs:sequence>
                  <!-- Duplicate element produces an error in xmlschema -->
                  <xs:element type="xs:string" name="MastrNummer"/>
                  <xs:element type="xs:string" name="MastrNummer"/>
                </xs:sequence>
              </xs:complexType>
            </xs:element>
          </xs:sequence>
        </xs:complexType>
      </xs:element>
    </xs:schema>
    """
    # Create inner ZIP file xsd.zip containing Netze.xsd
    inner_zip_content = io.BytesIO()
    with zipfile.ZipFile(inner_zip_content, "w") as inner_zip:
        inner_zip.writestr("Netze.xsd", invalid_xsd)
    inner_zip_content.seek(0)

    # Create outer ZIP file containing xsd.zip
    outer_zip_content = io.BytesIO()
    with zipfile.ZipFile(outer_zip_content, "w") as outer_zip:
        outer_zip.writestr("xsd.zip", inner_zip_content.getvalue())
    outer_zip_content.seek(0)

    expected_url = (
        "https://download.marktstammdatenregister.de/Stichtag/"
        "Dokumentation%20MaStR%20Gesamtdatenexport%2001-03-2026.zip"
    )
    # Mock the GET request
    responses.add(
        responses.GET,
        expected_url,
        body=outer_zip_content.getvalue(),
        content_type="application/zip",
    )

    mastr_table_to_db_table = mastr.generate_data_model(date="20260301")
    # We expect that reading the invalid XSD Netze.xsd fails, triggering the fallback.
    assert len(caplog.messages) == 1
    assert "Falling back to stored docs" in caplog.messages[0]

    expected_keys = set(TABLE_TRANSLATIONS.keys()) - {
        # A couple of tables with meta information we do not create.
        "Einheitentypen",
        "Katalogkategorien",
        "Katalogwerte",
        "Lokationstypen",
        "Marktrollen",
        "Marktfunktionen",
    }
    assert set(mastr_table_to_db_table.keys()) == expected_keys
    # Check some samples
    solar_table = mastr_table_to_db_table["EinheitenSolar"]
    assert solar_table.name == "EinheitenSolar"
    assert solar_table.info == {
        "original_name": "EinheitenSolar",
        "english_name": "units_solar",
    }
    # Check a couple of columns
    assert solar_table.c.EinheitMastrNummer.primary_key is True
    assert isinstance(solar_table.c.EinheitMastrNummer.type, sqlalchemy.String)
    assert isinstance(solar_table.c.Bruttoleistung.type, sqlalchemy.Float)
    assert isinstance(solar_table.c.Hauptausrichtung.type, CatalogString)
    assert isinstance(
        solar_table.c.EinheitlicheAusrichtungUndNeigungswinkel.type, sqlalchemy.Boolean
    )
    changed_dso_assignment_table = mastr_table_to_db_table[
        "EinheitenAenderungNetzbetreiberzuordnungen"
    ]
    assert changed_dso_assignment_table.c.OpenMastrId.primary_key is True
    assert isinstance(
        changed_dso_assignment_table.c.OpenMastrId.type, sqlalchemy.Integer
    )
