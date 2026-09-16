import tempfile
import zipfile
from pathlib import Path

from open_mastr.utils.xsd_tables import (
    MastrColumnType,
    read_mastr_table_descriptions_from_xsd,
    _iterate_xsd_files,
)
from open_mastr.mastr import _get_fallback_xsd


MINIMAL_XSD = """<?xml version="1.0" encoding="UTF-8"?>
<xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema">
  <xs:element name="Root">
    <xs:complexType>
      <xs:sequence>
        <xs:element name="Instance">
          <xs:complexType>
            <xs:sequence>
              <xs:element name="ColumnA" type="xs:string"/>
              <xs:element name="ColumnB" type="xs:int"/>
            </xs:sequence>
          </xs:complexType>
        </xs:element>
      </xs:sequence>
    </xs:complexType>
  </xs:element>
</xs:schema>"""


def test_iterate_xsd_files_from_directory():
    with tempfile.TemporaryDirectory() as tmpdir:
        xsd_dir = Path(tmpdir) / "xsd"
        xsd_dir.mkdir()
        (xsd_dir / "anlageneegwind.xsd").write_text(MINIMAL_XSD)

        names = [name for name, _ in _iterate_xsd_files(xsd_dir)]
        assert names == ["anlageneegwind.xsd"]


def test_iterate_xsd_files_from_zip_with_nested_xsd_zip():
    with tempfile.TemporaryDirectory() as tmpdir:
        docs_zip_path = Path(tmpdir) / "docs.zip"
        xsd_zip_path = Path(tmpdir) / "xsd.zip"

        with zipfile.ZipFile(xsd_zip_path, "w") as xsd_z:
            xsd_z.writestr("anlageneegwind.xsd", MINIMAL_XSD)

        with zipfile.ZipFile(docs_zip_path, "w") as docs_z:
            docs_z.write(xsd_zip_path, "xsd.zip")

        names = [name for name, _ in _iterate_xsd_files(docs_zip_path)]
        assert names == ["anlageneegwind.xsd"]


def test_iterate_xsd_files_from_zip_with_unzipped_xsd_folder():
    with tempfile.TemporaryDirectory() as tmpdir:
        docs_zip_path = Path(tmpdir) / "docs.zip"

        with zipfile.ZipFile(docs_zip_path, "w") as docs_z:
            docs_z.writestr("xsd/anlageneegwind.xsd", MINIMAL_XSD)

        names = [name for name, _ in _iterate_xsd_files(docs_zip_path)]
        assert names == ["anlageneegwind.xsd"]


def test_read_mastr_table_descriptions_from_xsd():
    with tempfile.TemporaryDirectory() as tmpdir:
        xsd_dir = Path(tmpdir) / "xsd"
        xsd_dir.mkdir()
        (xsd_dir / "anlageneegwind.xsd").write_text(MINIMAL_XSD)

        result = read_mastr_table_descriptions_from_xsd(xsd_dir, ["wind"])
        assert len(result) == 1
        table = next(iter(result))
        assert table.original_table_name == "Root"
        assert table.instance_name == "Instance"
        assert len(table.columns) == 2


def test_catalog_columns_missing_xsd_restriction_are_catalog_values():
    fallback_xsd_dir = _get_fallback_xsd()
    table_descriptions = read_mastr_table_descriptions_from_xsd(
        fallback_xsd_dir, ["combustion", "grid", "market", "wind"]
    )
    table_to_column_types = {
        table.original_table_name: {
            column.normalized_name: column.type for column in table.columns
        }
        for table in table_descriptions
    }

    assert {
        "Marktgebiet": MastrColumnType.CATALOG_VALUE,
        "Bundesland": MastrColumnType.CATALOG_VALUE,
        "Sparte": MastrColumnType.CATALOG_VALUE,
    }.items() <= table_to_column_types["Netze"].items()
    assert (
        table_to_column_types["EinheitenVerbrennung"]["WeitereBrennstoffe"]
        is MastrColumnType.CATALOG_VALUE
    )
    assert (
        table_to_column_types["EinheitenWind"]["Hersteller"]
        is MastrColumnType.CATALOG_VALUE
    )
    assert (
        table_to_column_types["Marktakteure"]["Rechtsform"]
        is MastrColumnType.CATALOG_VALUE
    )
    assert (
        table_to_column_types["Marktakteure"]["Registergericht"]
        is MastrColumnType.CATALOG_VALUE
    )
    assert table_to_column_types["Netze"]["Bezeichnung"] is MastrColumnType.STRING
