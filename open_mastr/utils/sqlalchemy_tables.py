import datetime
import logging
from dataclasses import dataclass
from typing import Any, Optional, Type, TypeVar, Union
from sqlalchemy import Column, Integer, String, Float, Boolean, Date, DateTime
from sqlalchemy.orm import DeclarativeBase, mapped_column, Mapped

import xmlschema
from xmlschema.validators.simple_types import XsdAtomicBuiltin, XsdAtomicRestriction
from open_mastr.utils.xsd_tables import MastrColumnType, MastrTableDescription, translate_mastr_column_name

log = logging.getLogger("open-MaStR")

# Potential hierarchy
# Id -> MastrNummer -> EinheitMastrNummer
# -> EegMastrNummer -> KwkMastrNummer -> GenMastrNummer
# -> MarktakteurMastrNummer -> NetzanschlusspunktMastrNummer
# in case we want to auto-detect the primary key.
MASTR_TABLE_NAME_TO_PRIMARY_KEY_COLUMNS = {
    "AnlagenEegBiomasse": {"EegMastrNummer"},
    "AnlagenEegGeothermieGrubengasDruckentspannung": {"EegMastrNummer"},
    "AnlagenEegSolar": {"EegMastrNummer"},
    "AnlagenEegSpeicher": {"EegMastrNummer"},
    "AnlagenEegWasser": {"EegMastrNummer"},
    "AnlagenEegWind": {"EegMastrNummer"},
    "AnlagenGasSpeicher": {"MastrNummer"},
    "AnlagenKwk": {"KwkMastrNummer"},
    "AnlagenStromSpeicher": {"MastrNummer"},
    "Bilanzierungsgebiete": {"Id"},

    # There is no unique key for this table. So we will have to insert one.
    # Check for example the entries for SEE990510388975. We could use
    # EinheitMastrNummer + RegistrierungsdatumNetzbetreiberzuordnungsaenderung,
    # but the MaStR docs say that RegistrierungsdatumNetzbetreiberzuordnungsaenderung
    # can be NULL.
    "EinheitenAenderungNetzbetreiberzuordnungen": None,

    "EnheitenBiomasse": {"EinheitMastrNummer"},
    "EinheitenGasErzeuger": {"EinheitMastrNummer"},
    "EinheitenGasSpeicher": {"EinheitMastrNummer"},
    "EinheitenGasverbraucher": {"EinheitMastrNummer"},
    "EinheitenGenehmigung": {"GenMastrNummer"},
    "EinheitenGeothermieGrubengasDruckentspannung": {"EinheitMastrNummer"},
    "EinheitenKernkraft": {"EinheitMastrNummer"},
    "EinheitenSolar": {"EinheitMastrNummer"},
    "EinheitenStromSpeicher": {"EinheitMastrNummer"},
    "EinheitenStromVerbraucher": {"EinheitMastrNummer"},
    "Einheitentypen": {"Id"},
    "EinheitenVerbrennung": {"EinheitMastrNummer"},
    "EinheitenWasser": {"EinheitMastrNummer"},
    "EinheitenWind": {"EinheitMastrNummer"},
    "Ertuechtigungen": {"Id"},
    "GeloeschteUndDeaktivierteEinheiten": {"EinheitMastrNummer"},
    "GeloeschteUndDeaktivierteMarktakteure": {"MarktakteurMastrNummer"},
    "Katalogkategorien": {"Id"},
    "Katalogwerte": {"Id"},
    "Lokationen": {"MastrNummer"},
    "Lokationstypen": {"Id"},
    "MarktakteureUndRollen": {"MastrNummer"},
    "Marktakteure": {"MastrNummer"},
    "Marktfunktionen": {"Id"},
    "Marktrollen": {"Id"},
    "Netzanschlusspunkte": {"NetzanschlusspunktMastrNummer"},
    "Netze": {"MastrNummer"},
}


DeclarativeBase_T = TypeVar("DeclarativeBase_T", bound=DeclarativeBase)


def make_sqlalchemy_model_from_mastr_table_description(
    table_description: MastrTableDescription,
    catalog_value_as_str: bool,
    base: Type[DeclarativeBase_T],
    english: bool = False,
    mixins: tuple[type, ...] = tuple(),
    include_download_metadata: bool = True,
) -> Type[DeclarativeBase_T]:
    if english:
        if table_description.english_table_name:
            table_name = table_description.english_table_name
        else:
            table_name = table_description.original_table_name
            english = False
            log.warning(
                f"English table name not available for {table_name}."
                " Using German for the whole table."
            )
    else:
        table_name = table_description.original_table_name

    column_name_to_column_type = {
        (
            column.english_name or column.normalized_name
            if english
            else column.normalized_name
        ): _get_sqlalchemy_type_for_mastr_column_type(
            mastr_column_type=column.type,
            catalog_value_as_str=catalog_value_as_str,
        )
        for column in table_description.columns
    }
    column_kwargs = {
        (
            column.english_name or column.normalized_name
            if english
            else column.normalized_name
        ): {
            "info": {
                "original_name": column.original_name,
                "normalized_name": column.normalized_name,
                "english_name": column.english_name,
            }
        }
        for column in table_description.columns
    }

    primary_key_columns = MASTR_TABLE_NAME_TO_PRIMARY_KEY_COLUMNS.get(
        table_description.original_table_name
    )
    if primary_key_columns and english:
        primary_key_columns = [translate_mastr_column_name(column) for column in primary_key_columns]
    if not primary_key_columns or any(
        column not in column_name_to_column_type
        for column in primary_key_columns
    ):
        id_col_name = "OpenMastrId"
        log.info(
            f"Found no primary key column for table {table_description.original_table_name}."
            f" Inserting custom ID column {id_col_name!r}"
        )
        # The integer column will be autoincrement by default since we make it a primary key.
        column_name_to_column_type[id_col_name] = Integer
        primary_key_columns = {id_col_name}

    if include_download_metadata:
        data_source_col_info = {
            "normalized_name": "DatenQuelle",
            "english_name": "dataSource",
        }
        data_source_col_name = data_source_col_info["english_name" if english else "normalized_name"]
        column_kwargs[data_source_col_name] = {"info": data_source_col_info}
        column_name_to_column_type[data_source_col_name] = String
        download_date_col_info = {
            "normalized_name": "DatumDownload",
            "english_name": "downloadDate",
        }
        download_date_col_name = download_date_col_info["english_name" if english else "normalized_name"]
        column_kwargs[download_date_col_name] = {"info": download_date_col_info}
        column_name_to_column_type[download_date_col_name] = String

    return _make_sqlalchemy_model(
        class_name=table_description.instance_name,
        table_name=table_name,
        column_name_to_column_type=column_name_to_column_type,
        primary_key_columns=primary_key_columns,
        base=base,
        mixins=mixins,
        table_kwargs={
            "info": {
                "original_name": table_description.original_table_name,
                "english_name": table_description.english_table_name,
            }
        },
        column_kwargs=column_kwargs,
    )


def _make_sqlalchemy_model(
    class_name: str,
    table_name: str,
    column_name_to_column_type: dict[str, Any],
    primary_key_columns: set[str],
    base: Type[DeclarativeBase_T],
    mixins: tuple[type, ...] = tuple(),
    table_kwargs: Optional[dict[str, Any]] = None,
    column_kwargs: Optional[dict[str, dict[str, Any]]] = None,
) -> Type[DeclarativeBase_T]:  # TODO: Is there a way to say that the returned model is a sub-type of DeclarativeBase_T?
    namespace = {
        "__tablename__": table_name,
        "__annotations__": {},
    }
    if table_kwargs:
        namespace["__table_args__"] = table_kwargs

    for column_name, column_type in column_name_to_column_type.items():
        kwargs = column_kwargs.get(column_name, {}) if column_kwargs else {}
        if column_name in primary_key_columns:
            kwargs.setdefault("primary_key", True)
        else:
            kwargs.setdefault("nullable", True)
        namespace[column_name] = mapped_column(column_type, **kwargs)

    bases = (base,) + mixins
    return type(class_name, bases, namespace)


_MASTR_COLUMN_TYPE_TO_SQLALCHEMY_TYPE = {
    MastrColumnType.STRING: String,
    MastrColumnType.INTEGER: Integer,
    MastrColumnType.FLOAT: Float,
    MastrColumnType.DATE: Date,
    MastrColumnType.DATETIME: DateTime(timezone=True),
    MastrColumnType.BOOLEAN: Boolean,
}


# We're creating special column types for the catalog columns here so that
# we can identify the catalog columns later when processing the XML files.
class CatalogInteger(Integer):
    pass


class CatalogString(String):
    pass


def _get_sqlalchemy_type_for_mastr_column_type(
    mastr_column_type: MastrColumnType, catalog_value_as_str: bool,
) -> Union[Type[String], Type[Integer], Type[Float], Type[Date], Type[DateTime], Type[Boolean]]:
    if mastr_column_type is MastrColumnType.CATALOG_VALUE:
        return CatalogString if catalog_value_as_str else CatalogInteger
    return _MASTR_COLUMN_TYPE_TO_SQLALCHEMY_TYPE[mastr_column_type]

