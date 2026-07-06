import logging
import os
from enum import auto, Enum
from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Union
from zipfile import ZipFile
import xmlschema
from xmlschema.validators.simple_types import XsdAtomicBuiltin, XsdAtomicRestriction
from xmlschema.validators.exceptions import XMLSchemaModelError

from open_mastr.utils.helpers import data_to_include_tables
from open_mastr.utils.constants import COLUMN_TRANSLATIONS, TABLE_TRANSLATIONS

_XML_SCHEMA_PREFIX = "{http://www.w3.org/2001/XMLSchema}"

log = logging.getLogger("open-MaStR")


def normalize_mastr_name(original_mastr_name: str) -> str:
    """Normalize original MaStR column name.

    BNetzA sometimes has "MaStR", other times "Mastr". We normalize that.
    Also, in case the column names in the XSD contain äöüß, we replace them.
    This is probably a BNetzA oversight, but has happened at least once.
    """
    return (
        original_mastr_name.replace("MaStR", "Mastr")
        .replace("ä", "ae")
        .replace("ö", "oe")
        .replace("ü", "ue")
        .replace("ß", "ss")
        .strip()
    )


def translate_mastr_column_name(normalized_mastr_column_name: str) -> Optional[str]:
    translated = COLUMN_TRANSLATIONS.get(normalized_mastr_column_name)
    if not translated:
        log.warning(
            f"No translation available for column {normalized_mastr_column_name!r}"
        )
    return translated


def translate_mastr_table_name(normalized_mastr_table_name: str) -> Optional[str]:
    translated = TABLE_TRANSLATIONS.get(normalized_mastr_table_name)
    if not translated:
        log.warning(
            f"No translation available for table {normalized_mastr_table_name!r}"
        )
    return translated


class MastrColumnType(Enum):
    STRING = auto()
    INTEGER = auto()
    FLOAT = auto()
    DATE = auto()
    DATETIME = auto()
    BOOLEAN = auto()
    CATALOG_VALUE = auto()

    @classmethod
    def from_xsd_type(
        cls, xsd_type: Union[XsdAtomicBuiltin, XsdAtomicRestriction]
    ) -> "MastrColumnDescription":
        xsd_type_to_mastr_column_type = {
            f"{_XML_SCHEMA_PREFIX}string": cls.STRING,
            f"{_XML_SCHEMA_PREFIX}decimal": cls.INTEGER,
            f"{_XML_SCHEMA_PREFIX}int": cls.INTEGER,
            f"{_XML_SCHEMA_PREFIX}short": cls.INTEGER,
            f"{_XML_SCHEMA_PREFIX}byte": cls.INTEGER,
            f"{_XML_SCHEMA_PREFIX}float": cls.FLOAT,
            f"{_XML_SCHEMA_PREFIX}date": cls.DATE,
            f"{_XML_SCHEMA_PREFIX}dateTime": cls.DATETIME,
        }
        if xsd_type.is_restriction():
            if enumeration := xsd_type.enumeration:
                if set(enumeration) == {0, 1}:
                    return cls.BOOLEAN
                return cls.CATALOG_VALUE
            # Ertuechtigungen.xsd has some normal types defined as restrictions for some reason.
            # We cope with that by extracting the primitive type it's restricted to.
            inner_xsd_type = xsd_type.primitive_type
            if mastr_column_type := xsd_type_to_mastr_column_type.get(
                inner_xsd_type.name
            ):
                return mastr_column_type

        if mastr_column_type := xsd_type_to_mastr_column_type.get(xsd_type.name):
            return mastr_column_type

        raise ValueError(
            f"Could not determine MastrColumnType from XSD type {xsd_type!r}"
        )


@dataclass(frozen=True)
class MastrColumnDescription:
    original_name: str
    normalized_name: str
    english_name: Optional[str]
    type: MastrColumnType

    @classmethod
    def from_xsd_element(
        cls, xsd_element: xmlschema.XsdElement
    ) -> "MastrColumnDescription":
        normalized_name = normalize_mastr_name(xsd_element.name)
        return cls(
            original_name=xsd_element.name,
            normalized_name=normalized_name,
            english_name=translate_mastr_column_name(normalized_name),
            type=MastrColumnType.from_xsd_type(xsd_element.type),
        )


@dataclass(frozen=True)
class MastrTableDescription:
    original_table_name: str
    english_table_name: Optional[str]
    instance_name: str
    columns: tuple[MastrColumnDescription]

    @classmethod
    def from_xml_schema(cls, schema: xmlschema.XMLSchema) -> "MastrTableDescription":
        if len(schema.root_elements) != 1:
            raise ValueError(
                "XML schema must have exactly one root element,"
                f" but has {len(schema.root_elements)} ({schema.root_elements!r})"
            )
        root = schema.root_elements[0]

        try:
            main_element = root.content.content[0]
            column_elements = main_element.content.content
        except (AttributeError, IndexError, TypeError) as e:
            raise ValueError(f"Could not find columns in XML schema {schema!r}") from e

        columns = tuple(
            MastrColumnDescription.from_xsd_element(element)
            for element in column_elements
        )

        # We don't normalize the table name because
        # - it would introduce too much complexity to have two German table names
        # - the normalization would leave the table name as is (at least as of Feb 2026)
        original_table_name = root.name
        english_table_name = translate_mastr_table_name(original_table_name)

        return cls(
            original_table_name=original_table_name,
            english_table_name=english_table_name,
            instance_name=main_element.name,
            columns=columns,
        )


class InvalidXmlSchemaError(Exception):
    pass


def _iterate_xsd_files(source: Union[Path, str]):
    """Iterate over .xsd files in either a zip containing xsd or xsd.zip.

    Yields tuples of (name, file_object) where file_object is a context manager.
    """
    source_path = Path(source)

    if source_path.is_dir():
        # Case: source is already an unzipped directory containing .xsd files
        for xsd_file in source_path.glob("**/*.xsd"):
            yield xsd_file.name, xsd_file.open("rb")
        return

    with ZipFile(source_path, "r") as docs_z:
        xsd_folder_entries = [
            entry
            for entry in docs_z.namelist()
            if entry.endswith(".xsd")
            and os.path.basename(os.path.dirname(entry)) == "xsd"
        ]
        if xsd_folder_entries:
            # Case: plain xsd/ folder inside the docs zip
            for entry in xsd_folder_entries:
                yield os.path.basename(entry), docs_z.open(entry)
            return

        xsd_zip_name = next(
            (name for name in docs_z.namelist() if os.path.basename(name) == "xsd.zip"),
            None,
        )
        if xsd_zip_name is None:
            raise RuntimeError(
                "Did not find XSD files in the form of an 'xsd' folder or an"
                f" 'xsd.zip' file in the documentation ZIP file {source_path!r}"
            )

        # Case: xsd.zip nested inside the docs zip
        with ZipFile(docs_z.open(xsd_zip_name)) as xsd_z:
            for entry in xsd_z.namelist():
                if entry.endswith(".xsd"):
                    yield os.path.basename(entry), xsd_z.open(entry)


def read_mastr_table_descriptions_from_xsd(
    zipped_docs_file_path: Union[Path, str], data: list[str]
) -> set[MastrTableDescription]:
    include_tables = data_to_include_tables(data)

    mastr_table_descriptions = set()
    for name, xsd_file in _iterate_xsd_files(zipped_docs_file_path):
        with xsd_file:
            normalized_name = name.removesuffix(".xsd").lower()
            if normalized_name in include_tables:
                try:
                    schema = xmlschema.XMLSchema(xsd_file)
                except XMLSchemaModelError as e:
                    raise InvalidXmlSchemaError(f"Invalid XML Schema in {name}") from e
                mastr_table_description = MastrTableDescription.from_xml_schema(schema)
                mastr_table_descriptions.add(mastr_table_description)

    return mastr_table_descriptions
