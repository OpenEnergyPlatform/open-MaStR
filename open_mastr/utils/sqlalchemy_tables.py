import logging
from typing import Any, Type, Union
from sqlalchemy import Column, Integer, String, Float, Boolean, Date, DateTime, Table, MetaData

from open_mastr.utils.xsd_tables import (
    MastrColumnType,
    MastrTableDescription,
    translate_mastr_column_name,
)

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
    # Plus, SEE904666329300 would additionally need Netzbetreiberzuordnungsaenderungsdatum
    # to make it unique. But Netzbetreiberzuordnungsaenderungsdatum actually has NULL values for
    # some rows, so it cannot be used in a composite primary key because it must be nullable.
    # (Nullable columns in a primary key are OK in SQLite, but not in PostgreSQL & MySQL.)
    "EinheitenAenderungNetzbetreiberzuordnungen": None,

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
    "MarktakteureUndRollen": {"MastrNummer"},
    "Marktakteure": {"MastrNummer"},
    "Marktfunktionen": {"Id"},
    "Marktrollen": {"Id"},
    "Netzanschlusspunkte": {"NetzanschlusspunktMastrNummer"},
    "Netze": {"MastrNummer"},
}


def make_sqlalchemy_table_from_mastr_table_description(
    table_description: MastrTableDescription,
    catalog_value_as_str: bool,
    metadata: MetaData,
    english: bool = False,
    mixins: tuple[type, ...] = tuple(),
    include_download_metadata: bool = True,
) -> Table:
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

    try:
        primary_key_columns = MASTR_TABLE_NAME_TO_PRIMARY_KEY_COLUMNS[
            table_description.original_table_name
        ] or set()
        artificial_primary_key_name = "OpenMastrId"
    except KeyError:
        # This table is not yet known to open-mastr. We insert a temporary
        # primary key, but make that clear in the name so that users don't
        # rely on it.
        primary_key_columns = set()
        artificial_primary_key_name = "TempOpenMastrIdForUnknownTable"

    if primary_key_columns and english:
        primary_key_columns = {translate_mastr_column_name(column) for column in primary_key_columns}

    db_column_kwargs = []
    for mastr_column in table_description.columns:
        column_name = (
            mastr_column.english_name or mastr_column.normalized_name
            if english
            else mastr_column.normalized_name

        )
        column_type = _get_sqlalchemy_type_for_mastr_column_type(
            mastr_column_type=mastr_column.type,
            catalog_value_as_str=catalog_value_as_str,
        )
        kwargs = (
            {"primary_key": True}
            if column_name in primary_key_columns
            else {"nullable": True}
        )
        db_column_kwargs.append(
            {
                "name": column_name,
                "type_": column_type,
                "info": {
                    "original_name": mastr_column.original_name,
                    "normalized_name": mastr_column.normalized_name,
                    "english_name": mastr_column.english_name,
                },
                **kwargs,
            }
        )
    db_column_kwargs = _prepend_primary_key_if_missing(
        expected_primary_key_columns=primary_key_columns,
        db_column_kwargs=db_column_kwargs,
        table_name=table_description.original_table_name,
        new_primary_key_name=artificial_primary_key_name,
    )

    if include_download_metadata:
        data_source_col_info = {
            "normalized_name": "DatenQuelle",
            "english_name": "dataSource",
        }
        data_source_col_name = data_source_col_info["english_name" if english else "normalized_name"]
        db_column_kwargs.append(
            {
                "name": data_source_col_name,
                "type_": String,
                "info": data_source_col_info,
            }
        )
        download_date_col_info = {
            "normalized_name": "DatumDownload",
            "english_name": "downloadDate",
        }
        download_date_col_name = download_date_col_info["english_name" if english else "normalized_name"]
        db_column_kwargs.append(
            {
                "name": download_date_col_name,
                "type_": String,
                "info": download_date_col_info,
            }
        )

    db_columns = [Column(**kwargs) for kwargs in db_column_kwargs]

    return Table(
        table_name,
        metadata,
        *db_columns,
        info={
            "original_name": table_description.original_table_name,
            "english_name": table_description.english_table_name,
        },
    )


def _prepend_primary_key_if_missing(
    expected_primary_key_columns: set[str],
    db_column_kwargs: list[dict[str, Any]],
    table_name: str,
    new_primary_key_name: str,
) -> list[dict[str, Any]]:
    realized_primary_key_columns = {
        kwargs["name"]
        for kwargs in db_column_kwargs
        if kwargs["name"] in expected_primary_key_columns
    }
    if expected_primary_key_columns and (
        realized_primary_key_columns == expected_primary_key_columns
    ):
        return db_column_kwargs.copy()

    id_column_name = "OpenMastrId"
    log.info(
        f"Missing primary key column for table {table_name}."
        f" Inserting custom ID column {new_primary_key_name!r}"
    )
    return [
        {"name": new_primary_key_name, "type_": Integer, "primary_key": True, "autoincrement": True}
    ] + [
        kwargs | {"primary_key": False, "nullable": True}
        for kwargs in db_column_kwargs
    ]


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


def format_sqlalchemy_column(column: Column) -> str:
    """Format SQLAlchemy column

    This is an almost exact copy of sqlalchemy.Column.__repr__ with the difference
    that "info" is also formatted.
    """
    kwarg = []
    if column.key != column.name:
        kwarg.append("key")
    if column.primary_key:
        kwarg.append("primary_key")
    if not column.nullable:
        kwarg.append("nullable")
    if column.onupdate:
        kwarg.append("onupdate")
    if column.default:
        kwarg.append("default")
    if column.server_default:
        kwarg.append("server_default")
    if column.comment:
        kwarg.append("comment")
    if column.info:
        kwarg.append("info")
    return "Column(%s)" % ", ".join(
        [repr(column.name)]
        + [repr(column.type)]
        + [repr(x) for x in column.foreign_keys if x is not None]
        + [repr(x) for x in column.constraints]
        + [
            (
                column.table is not None
                and "table=<%s>" % column.table.description
                or "table=None"
            )
        ]
        + ["%s=%s" % (k, repr(getattr(column, k))) for k in kwarg]
    )


def format_sqlalchemy_table(table: Table) -> str:
    """Format SQLAlchemy column

    This is an almost exact copy of sqlalchemy.Table.__repr__ with two differences:
    - "info" is also formatted
    - more whitespace (especially linebreaks) to make it more easily readable
    """
    return "Table(\n    %s\n)" % ",\n    ".join(
        [repr(table.name)]
        + [repr(table.metadata)]
        + [format_sqlalchemy_column(x) for x in table.columns]
        + ["%s=%s" % (k, repr(getattr(table, k))) for k in ["info", "schema"]]
    )


def format_mastr_table_to_db_table(mastr_table_to_db_table: dict[str, Table]) -> str:
    """Format mapping from MaStR table to SQLAlchemy table.

    Parameters
    ----------
    mastr_table_to_db_table : Mapping from MaStR table name (str) to SQLALchemy Table

    Returns
    -------
    str
        The formatted mapping as a string.
    """
    parts = []
    for mastr_table, db_table in mastr_table_to_db_table.items():
        parts.append(f"{mastr_table}: {format_sqlalchemy_table(db_table)}")
    return "\n\n".join(parts)
