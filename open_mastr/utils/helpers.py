import os
import json
import sys
from contextlib import contextmanager
from datetime import date, datetime
from warnings import warn

import dateutil
import sqlalchemy
from sqlalchemy.sql import insert, literal_column, text
from dateutil.parser import parse
from sqlalchemy import create_engine
from sqlalchemy.orm import Query, sessionmaker

import pandas as pd
from tqdm import tqdm
from open_mastr.soap_api.metadata.create import create_datapackage_meta_json
from open_mastr.utils import orm
from open_mastr.utils.config import (
    get_filenames,
    get_data_version_dir,
    column_renaming,
)

from open_mastr.soap_api.download import MaStRAPI, log
from open_mastr.utils.constants import (
    BULK_DATA,
    TECHNOLOGIES,
    API_DATA,
    API_DATA_TYPES,
    API_LOCATION_TYPES,
    BULK_INCLUDE_TABLES_MAP,
    BULK_ADDITIONAL_TABLES_CSV_EXPORT_MAP,
    ORM_MAP,
    UNIT_TYPE_MAP,
    ADDITIONAL_TABLES,
    TRANSLATIONS,
)


def chunks(lst, n):
    """Yield successive n-sized chunks from lst.

    `Credits
    <https://stackoverflow.com/questions/312443/how-do-you-split-a-list-into-evenly-sized-chunks>`_
    """
    length = lst.count() if isinstance(lst, Query) else len(lst)
    for i in range(0, length, n):
        yield lst[i : i + n]


def create_database_engine(engine, sqlite_db_path) -> sqlalchemy.engine.Engine:
    if engine == "sqlite":
        sqlite_database_path = os.environ.get(
            "SQLITE_DATABASE_PATH",
            os.path.join(sqlite_db_path, "open-mastr.db"),
        )
        db_url = f"sqlite:///{sqlite_database_path}"
        return create_engine(db_url)

    if type(engine) == sqlalchemy.engine.Engine:
        return engine


def parse_date_string(bulk_date_string: str) -> str:
    if bulk_date_string == "today":
        return date.today().strftime("%Y%m%d")
    else:
        return parse(bulk_date_string).strftime("%Y%m%d")


def validate_parameter_format_for_mastr_init(engine) -> None:
    if engine not in ["sqlite"] and not isinstance(engine, sqlalchemy.engine.Engine):
        raise ValueError(
            "parameter engine has to be either 'sqlite' "
            "or an sqlalchemy.engine.Engine object."
        )


def validate_parameter_format_for_download_method(
    method,
    data,
    date,
    bulk_cleansing,
    api_processes,
    api_limit,
    api_chunksize,
    api_data_types,
    api_location_types,
    **kwargs,
) -> None:
    if "technology" in kwargs:
        data = kwargs["technology"]
        warn("'technology' parameter is deprecated. Use 'data' instead")
    if "bulk_date" in kwargs:
        date = kwargs["bulk_date"]
        warn("'bulk_date' parameter is deprecated. Use 'date' instead")
    if "api_date" in kwargs:
        date = kwargs["api_date"]
        warn("'api_date' parameter is deprecated. Use 'date' instead")

    validate_parameter_method(method)
    validate_parameter_data(method, data)
    validate_parameter_date(method, date)
    validate_parameter_bulk_cleansing(bulk_cleansing)
    validate_parameter_api_processes(api_processes)
    validate_parameter_api_limit(api_limit)
    validate_parameter_api_chunksize(api_chunksize)
    validate_parameter_api_data_types(api_data_types)
    validate_parameter_api_location_types(api_location_types)

    raise_warning_for_invalid_parameter_combinations(
        method,
        bulk_cleansing,
        date,
        api_processes,
        api_data_types,
        api_location_types,
        api_limit,
        api_chunksize,
    )


def validate_parameter_method(method) -> None:
    if method not in ["bulk", "API"]:
        raise ValueError("parameter method has to be either 'bulk' or 'API'.")


def validate_parameter_api_location_types(api_location_types) -> None:
    if not isinstance(api_location_types, list) and api_location_types is not None:
        raise ValueError("parameter api_location_types has to be a list or 'None'.")

    if isinstance(api_location_types, list):
        if not api_location_types:  # api_location_types == []
            raise ValueError("parameter api_location_types cannot be an empty list!")
        for value in api_location_types:
            if value not in API_LOCATION_TYPES:
                raise ValueError(
                    f"list entries of api_data_types have to be in {API_LOCATION_TYPES}."
                )


def validate_parameter_api_data_types(api_data_types) -> None:
    if not isinstance(api_data_types, list) and api_data_types is not None:
        raise ValueError("parameter api_data_types has to be a list or 'None'.")

    if isinstance(api_data_types, list):
        if not api_data_types:  # api_data_types == []
            raise ValueError("parameter api_data_types cannot be an empty list!")
        for value in api_data_types:
            if value not in API_DATA_TYPES:
                raise ValueError(
                    f"list entries of api_data_types have to be in {API_DATA_TYPES}."
                )


def validate_parameter_api_chunksize(api_chunksize) -> None:
    if not isinstance(api_chunksize, int) and api_chunksize is not None:
        raise ValueError("parameter api_chunksize has to be an integer or 'None'.")


def validate_parameter_bulk_cleansing(bulk_cleansing) -> None:
    if type(bulk_cleansing) != bool:
        raise ValueError("parameter bulk_cleansing has to be boolean")


def validate_parameter_api_limit(api_limit) -> None:
    if not isinstance(api_limit, int) and api_limit is not None:
        raise ValueError("parameter api_limit has to be an integer or 'None'.")


def validate_parameter_date(method, date) -> None:
    if date is None:  # default
        return
    if method == "bulk":
        if date not in ["today", "existing"]:
            try:
                _ = parse(date)
            except (dateutil.parser._parser.ParserError, TypeError) as e:
                raise ValueError(
                    "Parameter date has to be a proper date in the format yyyymmdd"
                    "or 'today' for bulk method."
                ) from e
    elif method == "API":
        if not isinstance(date, datetime) and date != "latest":
            raise ValueError(
                "parameter api_date has to be 'latest' or a datetime object or 'None'"
                " for API method."
            )


def validate_parameter_api_processes(api_processes) -> None:
    if api_processes not in ["max", None] and not isinstance(api_processes, int):
        raise ValueError(
            "parameter api_processes has to be 'max' or an integer or 'None'"
        )
    if api_processes == "max" or isinstance(api_processes, int):
        system = sys.platform
        if system not in ["linux2", "linux"]:
            raise ValueError(
                "The functionality of multiprocessing only works on Linux based systems. "
                "On your system, the parameter api_processes has to be 'None'."
            )


def validate_parameter_data(method, data) -> None:
    if not isinstance(data, (str, list)) and data is not None:
        raise ValueError("parameter data has to be a string, list, or None")
    if isinstance(data, str):
        data = [data]
    if isinstance(data, list):
        if not data:  # data == []
            raise ValueError("parameter data cannot be an empty list!")
        for value in data:
            if method == "bulk" and value not in BULK_DATA:
                raise ValueError(
                    f"Allowed values for parameter data with bulk method are {BULK_DATA}"
                )
            if method == "API" and value not in API_DATA:
                raise ValueError(
                    f"Allowed values for parameter data with API method are {API_DATA}"
                )
            if method == "csv_export" and value not in TECHNOLOGIES + ADDITIONAL_TABLES:
                raise ValueError(
                    "Allowed values for CSV export are "
                    f"{TECHNOLOGIES} or {ADDITIONAL_TABLES}"
                )


def raise_warning_for_invalid_parameter_combinations(
    method,
    bulk_cleansing,
    date,
    api_processes,
    api_data_types,
    api_location_types,
    api_limit,
    api_chunksize,
):
    if method == "API" and bulk_cleansing is not True:
        warn(
            "For method = 'API', bulk download related parameters "
            "(with prefix bulk_) are ignored."
        )

    if method == "bulk" and (
        (
            any(
                parameter is not None
                for parameter in [
                    api_processes,
                    api_data_types,
                    api_location_types,
                ]
            )
            or api_limit != 50
            or api_chunksize != 1000
        )
    ):
        warn(
            "For method = 'bulk', API related parameters (with prefix api_) are ignored."
        )


def transform_data_parameter(
    method, data, api_data_types, api_location_types, **kwargs
):
    """
    Parse input parameters related to data as lists. Harmonize variables for later use.
    Data output depends on the possible data types of chosen method.
    """
    data = kwargs.get("technology", data)
    # parse parameters as list
    if isinstance(data, str):
        data = [data]
    elif data is None:
        data = BULK_DATA if method == "bulk" else API_DATA
    if api_data_types is None:
        api_data_types = API_DATA_TYPES
    if api_location_types is None:
        api_location_types = API_LOCATION_TYPES

    # data input harmonisation
    harmonisation_log = []
    if method == "API" and "permit" in data:
        data.remove("permit")
        api_data_types.append(
            "permit_data"
        ) if "permit_data" not in api_data_types else api_data_types
        harmonisation_log.append("permit")

    if method == "API" and "location" in data:
        data.remove("location")
        api_location_types = API_LOCATION_TYPES
        harmonisation_log.append("location")

    return data, api_data_types, api_location_types, harmonisation_log


def transform_date_parameter(self, method, date, **kwargs):
    if method == "bulk":
        date = kwargs.get("bulk_date", date)
        date = "today" if date is None else date
        if date == "existing":
            existing_files_list = os.listdir(
                os.path.join(self.output_dir, "data", "xml_download")
            )
            if not existing_files_list:
                date = "today"
                print(
                    "By choosing `date`='existing' you want to use an existing "
                    "xml download."
                    "However no xml_files were downloaded yet. The parameter `date` is"
                    "therefore set to 'today'."
                )
            # we assume that there is only one file in the folder which is the
            # zipped xml folder
            date = existing_files_list[0].split("_")[1].split(".")[0]
    elif method == "API":
        date = kwargs.get("api_date", date)

    return date


@contextmanager
def session_scope(engine):
    """Provide a transactional scope around a series of operations."""
    Session = sessionmaker(bind=engine)
    session = Session()
    try:
        yield session
        session.commit()
    except:
        session.rollback()
        raise
    finally:
        session.close()


def print_api_settings(
    harmonisation_log,
    data,
    date,
    api_data_types,
    api_chunksize,
    api_limit,
    api_processes,
    api_location_types,
):
    print(
        f"Downloading with soap_API.\n\n   -- API settings --  \nunits after date: "
        f"{date}\nunit download limit per data: "
        f"{api_limit}\nparallel_processes: {api_processes}\nchunksize: "
        f"{api_chunksize}\ndata_api: {data}"
    )
    if "permit" in harmonisation_log:
        print(
            f"data_types: {api_data_types}" "\033[31m",
            "Attention, 'permit_data' was automatically set in api_data_types, "
            "as you defined 'permit' in parameter data_api.",
            "\033[m",
        )

    else:
        print(f"data_types: {api_data_types}")

    if "location" in harmonisation_log:
        print(
            "location_types:",
            "\033[31m",
            "Attention, 'location' is in parameter data. location_types are set to",
            "\033[m",
            f"{api_location_types}"
            "\n                 If you want to change location_types, please remove 'location' "
            "from data_api and specify api_location_types."
            "\n   ------------------  \n",
        )

    else:
        print(
            f"location_types: {api_location_types}",
            "\n   ------------------  \n",
        )


def validate_api_credentials() -> None:
    mastr_api = MaStRAPI()
    assert mastr_api.GetAktuellerStandTageskontingent()["Ergebniscode"] == "OK"


def data_to_include_tables(data: list, mapping: str = None) -> list:
    """
    Convert user input 'data' to the list 'include_tables'.
    It contains file names from zipped bulk download, if mapping="write_xml".
    It contains database table names, if mapping="export_db_tables".
    Parameters
    ----------
    data: list
        The user input for data selection
    mapping: str
        Specify the mapping dict for the function and thus the list output.
    Returns
    -------
    list
        List of file names | List of database table names
    -------

    """
    if mapping == "write_xml":
        # Map data selection to include tables in xml
        include_tables = [
            table for tech in data for table in BULK_INCLUDE_TABLES_MAP[tech]
        ]
        return include_tables

    if mapping == "export_db_tables":
        # Map data selection to include tables for csv export
        include_tables = [
            table
            for possible_data_bulk in data
            for table in BULK_ADDITIONAL_TABLES_CSV_EXPORT_MAP[possible_data_bulk]
        ]
        return include_tables

    raise NotImplementedError(
        "This function is only implemented for 'write_xml' and 'export_db_tables', "
        "please specify when calling the function."
    )


def reverse_unit_type_map():
    return {v: k for k, v in UNIT_TYPE_MAP.items()}


# EXPORT RELEVANT FUNCTIONS


def create_db_query(
    tech=None,
    additional_table=None,
    additional_data=["unit_data", "eeg_data", "kwk_data", "permit_data"],
    limit=None,
    engine=None,
):
    """
    Create a database query to export a snapshot MaStR data from database to CSV.

    For technologies, during the query creation, additional available data is joined on
    list of basic units. A query for a single technology is created separately because
    of multiple non-overlapping columns. Duplicate columns for a single technology
    (a results on data from different sources) are suffixed.

    The data in the database probably has duplicates because
    of the history how data was collected in the
    Marktstammdatenregister.

    Along with the data, metadata is saved in the file `datapackage.json`.

    Parameters
    ----------
    technology: `list` of `str`
        See list of available technologies in
        `open_mastr.utils.constants.TECHNOLOGIES`
    additional_table: `list` of `str`
        See list of available technologies or additional tables in
        `open_mastr.utils.constants.ADDITIONAL_TABLES`
    engine: <class 'sqlalchemy.engine.base.Engine'>
        User-defined database engine.
    limit: int
        Limit number of rows.
    additional_data: `list`
        Defaults to "export all available additional data" which is:
        `["unit_data", "eeg_data", "kwk_data", "permit_data"]`.
    chunksize: int or None
        Defines the chunksize of the tables export. Default to 500.000 which is roughly 2.5 GB.
    """

    renaming = column_renaming()

    unit_type_map_reversed = reverse_unit_type_map()

    with session_scope(engine=engine) as session:

        if tech:

            # Select orm tables for specified additional_data.
            orm_tables = {
                f"{dat}": getattr(orm, ORM_MAP[tech].get(dat, "KeyNotAvailable"), None)
                for dat in additional_data
            }

            # Filter for possible orm-additional_data combinations (not None)
            orm_tables = {k: v for k, v in orm_tables.items() if v is not None}

            # Build query based on available tables for tech and user input; always use basic units
            subtables = partially_suffixed_columns(
                orm.BasicUnit,
                renaming["basic_data"]["columns"],
                renaming["basic_data"]["suffix"],
            )

            # Extend table with columns from selected additional_data orm
            for addit_data_type, addit_data_orm in orm_tables.items():
                subtables.extend(
                    partially_suffixed_columns(
                        addit_data_orm,
                        renaming[addit_data_type]["columns"],
                        renaming[addit_data_type]["suffix"],
                    )
                )

            query_tech = Query(subtables, session=session)

            # Define joins based on available tables for data and user input
            if "unit_data" in orm_tables:
                query_tech = query_tech.join(
                    orm_tables["unit_data"],
                    orm.BasicUnit.EinheitMastrNummer
                    == orm_tables["unit_data"].EinheitMastrNummer,
                    isouter=True,
                )
            if "eeg_data" in orm_tables:
                query_tech = query_tech.join(
                    orm_tables["eeg_data"],
                    orm.BasicUnit.EegMastrNummer
                    == orm_tables["eeg_data"].EegMastrNummer,
                    isouter=True,
                )
            if "kwk_data" in orm_tables:
                query_tech = query_tech.join(
                    orm_tables["kwk_data"],
                    orm.BasicUnit.KwkMastrNummer
                    == orm_tables["kwk_data"].KwkMastrNummer,
                    isouter=True,
                )
            if "permit_data" in orm_tables:
                query_tech = query_tech.join(
                    orm_tables["permit_data"],
                    orm.BasicUnit.GenMastrNummer
                    == orm_tables["permit_data"].GenMastrNummer,
                    isouter=True,
                )

            # Restricted to technology
            query_tech = query_tech.filter(
                orm.BasicUnit.Einheittyp == unit_type_map_reversed[tech]
            )

            # Limit returned rows of query
            if limit:
                query_tech = query_tech.limit(limit)

            return query_tech

        if additional_table:

            orm_table = getattr(orm, ORM_MAP[additional_table], None)

            query_additional_tables = Query(orm_table, session=session)

            # Limit returned rows of query
            if limit:
                query_additional_tables = query_additional_tables.limit(limit)

            return query_additional_tables


def save_metadata(data: list = None, engine=None) -> None:
    """
    Save metadata during csv export.

    Parameters
    ----------
    data: list
        List of exported technologies for which metadata is needed.
    engine: <class 'sqlalchemy.engine.base.Engine'>
        User-defined database engine.

    Returns
    -------

    """
    data_path = get_data_version_dir()
    filenames = get_filenames()
    metadata_file = os.path.join(data_path, filenames["metadata"])
    unit_type_map_reversed = reverse_unit_type_map()

    with session_scope(engine=engine) as session:
        # check for latest db entry for exported technologies
        mastr_technologies = [unit_type_map_reversed[tech] for tech in data]
        newest_date = (
            session.query(orm.BasicUnit.DatumLetzteAktualisierung)
            .filter(orm.BasicUnit.Einheittyp.in_(mastr_technologies))
            .order_by(orm.BasicUnit.DatumLetzteAktualisierung.desc())
            .first()[0]
        )

    metadata = create_datapackage_meta_json(newest_date, data, json_serialize=False)

    with open(metadata_file, "w", encoding="utf-8") as f:
        json.dump(metadata, f, ensure_ascii=False, indent=4)

    log.info("Saved metadata")


def reverse_fill_basic_units(technology=None, engine=None):
    """
    The basic_units table is empty after bulk download.
    To enable csv export, the table is filled from extended
    tables reversely.

    .. warning::
    The basic_units table will be dropped and then recreated.
    Returns -------

    Parameters
    ----------
    technology: list of str
        Available technologies are in open_mastr.Mastr.to_csv()
    """

    with session_scope(engine=engine) as session:
        # Empty the basic_units table, because it will be filled entirely from extended tables
        session.query(getattr(orm, "BasicUnit", None)).delete()

        for tech in tqdm(technology, desc="Performing reverse fill of basic units: "):
            # Get the class of extended table
            unit_data_orm = getattr(orm, ORM_MAP[tech]["unit_data"], None)
            basic_unit_column_names = [
                column.name
                for column in getattr(orm, "BasicUnit", None).__mapper__.columns
            ]

            unit_columns_to_reverse_fill = [
                column
                for column in unit_data_orm.__mapper__.columns
                if column.name in basic_unit_column_names
            ]
            unit_column_names_to_reverse_fill = [
                column.name for column in unit_columns_to_reverse_fill
            ]

            unit_type_map_reversed = reverse_unit_type_map()

            # Add Einheittyp artificially
            unit_typ = "'" + unit_type_map_reversed.get(tech, None) + "'"
            unit_columns_to_reverse_fill.append(
                literal_column(unit_typ).label("Einheittyp")
            )
            unit_column_names_to_reverse_fill.append("Einheittyp")

            # Build query
            query = Query(unit_columns_to_reverse_fill, session=session)
            insert_query = insert(orm.BasicUnit).from_select(
                unit_column_names_to_reverse_fill, query
            )

            session.execute(insert_query)


def partially_suffixed_columns(mapper, column_names, suffix):
    """
    Add a suffix to a subset of ORM map tables for a query

    Parameters
    ----------
    mapper:
        SQLAlchemy ORM table mapper
    column_names: list
        Names of columns to be suffixed
    suffix: str
        Suffix that is append like + "_" + suffix

    Returns
    -------
    list
        List of ORM table mapper instance
    """
    columns = list(mapper.__mapper__.columns)
    return [
        _.label(f"{_.name}_{suffix}") if _.name in column_names else _ for _ in columns
    ]


def db_query_to_csv(db_query, data_table: str, chunksize: int) -> None:
    """
    Export database query to CSV file

    Save CSV files to the respective data version directory, see
    :meth:`open_mastr.utils.config.get_data_version_dir`.

    Parameters
    ----------
    db_query: <class 'sqlalchemy.orm.query.Query'>
        QueryORM-level SQL construction object that holds tables for export.
    data_table: str
        See list of available technologies or additional tables in
        `open_mastr.utils.constants.TECHNOLOGIES` and
        `open_mastr.utils.constants.ADDITIONAL_TABLES`
    chunksize: int
        Defines the size of the chunks that are saved to csv.
        Useful when export fails due to memory issues.
    """
    data_path = get_data_version_dir()
    filenames = get_filenames()

    # Set export settings per table type
    if data_table in TECHNOLOGIES:
        index = True
        index_col = "EinheitMastrNummer"
        index_label = "EinheitMastrNummer"
        csv_file = os.path.join(data_path, filenames["raw"][data_table]["joined"])
    if data_table in ADDITIONAL_TABLES:
        index = False
        index_col = None
        index_label = None
        csv_file = os.path.join(
            data_path, filenames["raw"]["additional_table"][data_table]
        )

    with db_query.session.bind.connect() as con:
        with con.begin():
            # Read data into pandas.DataFrame in chunks of max. 500000 rows of ~2.5 GB RAM
            for chunk_number, chunk_df in enumerate(
                pd.read_sql(
                    sql=db_query.statement,
                    con=con,
                    index_col=index_col,
                    chunksize=chunksize,
                )
            ):
                # For debugging purposes, check RAM usage of chunk_df
                # chunk_df.info(memory_usage='deep')

                # Make sure no duplicate column names exist
                assert not any(chunk_df.columns.duplicated())

                # Remove newline statements from certain strings
                if data_table in TECHNOLOGIES:
                    for col in ["Aktenzeichen", "Behoerde"]:
                        chunk_df[col] = chunk_df[col].str.replace("\r", "")

                if not chunk_df.empty:

                    if chunk_number == 0:
                        chunk_df.to_csv(
                            csv_file,
                            index=index,
                            index_label=index_label,
                            encoding="utf-8",
                        )
                        log.info(f"Created csv: {csv_file.split('/')[-1:]} ")
                    else:
                        chunk_df.to_csv(
                            csv_file,
                            mode="a",
                            header=False,
                            index=index,
                            index_label=index_label,
                            encoding="utf-8",
                        )
                        log.info(
                            f"Appended {len(chunk_df)} rows to: {csv_file.split('/')[-1:]}"
                        )


def rename_table(table, columns, engine) -> None:
    """
    Rename table based on translation dictionary.
    """
    alter_statements = []

    for column in columns:
        column = column["name"]

        if column in TRANSLATIONS:
            alter_statement = text(
                f"ALTER TABLE {table} RENAME COLUMN {column} TO {TRANSLATIONS[column]}"
            )
            alter_statements.append(alter_statement)

    with engine.connect() as connection:
        for statement in alter_statements:
            try:
                connection.execute(statement)
            except sqlalchemy.exc.OperationalError:
                continue


def create_translated_database_engine(engine, folder_path) -> sqlalchemy.engine.Engine:
    """
    Check if translated version of the database, as defined with engine parameter, exists.
    Return sqlite engine connected with the translated database.
    """

    if engine == "sqlite":
        db_path = os.path.join(folder_path, "open-mastr-translated.db")
    else:
        if "sqlite" not in engine.dialect.name:
            raise ValueError("engine has to be of type 'sqlite'")

        prev_path = r"{}".format(engine.url.database)
        engine.dispose()
        db_path = prev_path[:-3] + "-translated.db"

    if not os.path.exists(db_path):
        raise FileNotFoundError(
            f"no database at {db_path} found.\n"
            "make sure the database has been translated before with translate()"
        )

    return create_engine(f"sqlite:///{db_path}")
