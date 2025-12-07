import datetime
from dataclasses import dataclass
from typing import Any, Union
from sqlalchemy import Column, Integer, String, Float, Boolean, Date, DateTime
from sqlalchemy.orm import DeclarativeBase, mapped_column, Mapped

import xmlschema
from xmlschema.validators.simple_types import XsdAtomicBuiltin, XsdAtomicRestriction
from open_mastr.utils.xsd_tables import MastrColumnType, MastrTableDescription


MASTR_COLUMN_TYPE_TO_SQLALCHEMY_TYPE = {
    MastrColumnType.STRING: String,
    MastrColumnType.INTEGER: Integer,
    MastrColumnType.FLOAT: Float,
    MastrColumnType.DATE: Date,
    MastrColumnType.DATETIME: DateTime(timezone=True),
    MastrColumnType.BOOLEAN: Boolean,
    MastrColumnType.CATALOG_VALUE: Integer,  # TODO: Think about how to deal with mapping catalog values
}

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
    "EinheitenAenderungNetzbetreiberzuordnungen": {"EinheitMastrNummer"},  # TODO: Is not a primary key on its own!
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


class Base(DeclarativeBase):
    pass


class ParentAllTables(object):
    DatenQuelle: Mapped[str] = mapped_column(String)
    DatumDownload: Mapped[datetime.date] = mapped_column(Date)


def make_sqlalchemy_model_from_mastr_table_description(
    table_description: MastrTableDescription,
    base: DeclarativeBase = Base,
    mixins: tuple[type, ...] = (ParentAllTables,),
):
    return _make_sqlalchemy_model(
        class_name=table_description.instance_name,
        table_name=table_description.table_name,
        column_name_to_column_type={
            column.name: MASTR_COLUMN_TYPE_TO_SQLALCHEMY_TYPE[column.type]
            for column in table_description.columns
        },
        primary_key_columns=MASTR_TABLE_NAME_TO_PRIMARY_KEY_COLUMNS[table_description.table_name],
        base=Base,
        mixins=(ParentAllTables,)
    )


def _make_sqlalchemy_model(
    class_name: str,
    table_name: str,
    column_name_to_column_type: dict[str, Any],
    primary_key_columns: set[str],
    base: DeclarativeBase,
    mixins: tuple[type, ...] = tuple(),
):
    namespace = {
        "__tablename__": table_name,
        "__annotations__": {},
    }

    for column_name, column_type in column_name_to_column_type.items():
        kwargs = {"primary_key": True} if column_name in primary_key_columns else {"nullable": True}
        namespace[column_name] = mapped_column(column_type, **kwargs)

    bases = (base,) + mixins
    return type(class_name, bases, namespace)


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
