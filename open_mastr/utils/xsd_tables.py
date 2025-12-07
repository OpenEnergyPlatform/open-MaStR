import re
from enum import auto, Enum
from dataclasses import dataclass
from typing import Union

import xmlschema
from xmlschema.validators.simple_types import XsdAtomicBuiltin, XsdAtomicRestriction

_XML_SCHEMA_PREFIX = "{http://www.w3.org/2001/XMLSchema}"


def normalize_column_name(original_mastr_column_name: str) -> str:
    return original_mastr_column_name.replace("MaStR", "Mastr")


class MastrColumnType(Enum):
    STRING = auto()
    INTEGER = auto()
    FLOAT = auto()
    DATE = auto()
    DATETIME = auto()
    BOOLEAN = auto()
    CATALOG_VALUE = auto()

    @classmethod
    def from_xsd_type(cls, xsd_type: Union[XsdAtomicBuiltin, XsdAtomicRestriction]) -> "MastrColumnDescription":
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
                if set(xsd_type.enumeration) == {0, 1}:
                    return cls.BOOLEAN
                else:
                    return cls.CATALOG_VALUE
            # Ertuechtigungen.xsd has some normal types defined as restrictions for some reason.
            # We cope with that by extracting the primitive type it's restricted to.
            inner_xsd_type = xsd_type.primitive_type
            if mastr_column_type := xsd_type_to_mastr_column_type.get(inner_xsd_type.name):
                return mastr_column_type

        if mastr_column_type := xsd_type_to_mastr_column_type.get(xsd_type.name):
            return mastr_column_type

        raise ValueError(f"Could not determine MastrColumnType from XSD type {xsd_type!r}")


@dataclass
class MastrColumnDescription:
    name: str
    type: MastrColumnType

    @classmethod
    def from_xsd_element(cls, xsd_element: xmlschema.XsdElement) -> "MastrColumnDescription":
        name = normalize_column_name(xsd_element.name)
        return cls(
            name=name,
            type=MastrColumnType.from_xsd_type(xsd_element.type)
        )


@dataclass
class MastrTableDescription:
    table_name: str
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

        return cls(
            table_name=root.name,
            instance_name=main_element.name,
            columns=columns,
        )
