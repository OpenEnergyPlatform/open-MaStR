import datetime
from dataclasses import dataclass
from typing import Any, Union, Type, TypeVar
from sqlalchemy import Column, Integer, String, Float, Boolean, Date, DateTime
from sqlalchemy.orm import DeclarativeBase, mapped_column, Mapped

import xmlschema
from xmlschema.validators.simple_types import XsdAtomicBuiltin, XsdAtomicRestriction
from open_mastr.utils.xsd_tables import MastrColumnType, MastrTableDescription


# Potential hierarchy
# Id -> MastrNummer -> EinheitMastrNummer
# -> EegMastrNummer -> KwkMastrNummer -> GenMastrNummer
# -> MarktakteurMastrNummer -> NetzanschlusspunktMastrNummer
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
    "EinheitenAenderungNetzbetreiberzuordnungen": {"EinheitMastrNummer"},  # TODO: May not be a primary key on its own. Check this.
    "EinheitenBiomasse": {"EinheitMastrNummer"},
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
    "MarktakteureUndRollen": {"MarktakteurMastrNummer"},
    "Marktakteure": {"MastrNummer"},
    "Marktfunktionen": {"Id"},
    "Marktrollen": {"Id"},
    "Netzanschlusspunkte": {"NetzanschlusspunktMastrNummer"},
    "Netze": {"MastrNummer"},
}


class ParentAllTables(object):
    DatenQuelle: Mapped[str] = mapped_column(String)
    DatumDownload: Mapped[datetime.date] = mapped_column(Date)


DeclarativeBase_T = TypeVar("DeclarativeBase_T", bound=DeclarativeBase)


def make_sqlalchemy_model_from_mastr_table_description(
    table_description: MastrTableDescription,
    catalog_value_as_str: bool,
    base: Type[DeclarativeBase_T],
    mixins: tuple[type, ...] = (ParentAllTables,),
) -> Type[DeclarativeBase_T]:
    return _make_sqlalchemy_model(
        class_name=table_description.instance_name,
        table_name=table_description.table_name,
        column_name_to_column_type={
            column.name: _get_sqlalchemy_type_for_mastr_column_type(
                mastr_column_type=column.type,
                catalog_value_as_str=catalog_value_as_str,
            )
            for column in table_description.columns
        },
        primary_key_columns=MASTR_TABLE_NAME_TO_PRIMARY_KEY_COLUMNS[table_description.table_name],
        base=base,
        mixins=(ParentAllTables,)
    )


def _make_sqlalchemy_model(
    class_name: str,
    table_name: str,
    column_name_to_column_type: dict[str, Any],
    primary_key_columns: set[str],
    base: Type[DeclarativeBase_T],
    mixins: tuple[type, ...] = tuple(),
) -> Type[DeclarativeBase_T]:  # TODO: Is there a way to say that the returned model is a sub-type of DeclarativeBase_T?
    namespace = {
        "__tablename__": table_name,
        "__annotations__": {},
    }

    for column_name, column_type in column_name_to_column_type.items():
        kwargs = {"primary_key": True} if column_name in primary_key_columns else {"nullable": True}
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



# TODO: Remove this or make it useful for outsiders.
if __name__ == "__main__":
    import os
    import sys
    from sqlalchemy import create_engine
    import traceback
    import xmlschema

    print("Parsing XSD files")
    xsd_path = sys.argv[1]
    for xsd_path in sys.argv[1:]:
        schema = xmlschema.XMLSchema(xsd_path)
        try:
            table_description = MastrTableDescription.from_xml_schema(schema)
        except ValueError:
            traceback.print_exc()
            print("Failed for ", xsd_path)
            sys.exit(1)

        model = make_sqlalchemy_model_from_mastr_table_description(
            table_description=table_description,
        )

    db_path = os.path.join(os.getcwd(), "test.db")
    print(f"Creating SQLite database at {db_path}")
    engine = create_engine(f"sqlite:///{db_path}")
    Base.metadata.create_all(engine)
