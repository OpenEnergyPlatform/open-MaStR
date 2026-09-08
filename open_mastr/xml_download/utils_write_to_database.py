import os
from collections.abc import Collection, Mapping
from concurrent.futures import ProcessPoolExecutor, wait
from io import StringIO
from multiprocessing import cpu_count
from shutil import Error
from zipfile import ZipFile

import re
import lxml
import numpy as np
import pandas as pd
import sqlalchemy
from sqlalchemy import Column, Engine, Table, delete, select, create_engine
from sqlalchemy.sql import text
from sqlalchemy.sql.sqltypes import Date, DateTime

from open_mastr.utils.config import setup_logger
from open_mastr.utils.helpers import data_to_include_tables
from open_mastr.utils.xsd_tables import (
    normalize_mastr_name,
    translate_mastr_column_name,
)
from open_mastr.utils.sqlalchemy_tables import CatalogInteger, CatalogString
from open_mastr.xml_download.utils_cleansing_bulk import cleanse_bulk_data

log = setup_logger()


def write_mastr_xml_to_database(
    engine: sqlalchemy.engine.Engine,
    zipped_xml_file_path: str,
    data: list,
    bulk_cleansing: bool,
    bulk_download_date: str,
    mastr_table_to_db_table: Mapping[str, Table],
    alter_database_tables: bool,
) -> None:
    """Write the Mastr in xml format into a database defined by the engine parameter."""
    log.info("Starting bulk download...")

    include_tables = data_to_include_tables(data)
    threads_data = []
    lower_mastr_table_to_db_table = {
        db_table.info.get("original_name", mastr_table_name).lower(): db_table
        for mastr_table_name, db_table in mastr_table_to_db_table.items()
    }

    with ZipFile(zipped_xml_file_path, "r") as f:
        files_list = correct_ordering_of_filelist(f.namelist())

        for file_name in files_list:
            xml_table_name = extract_xml_table_name(file_name)

            if xml_table_name not in include_tables:
                continue

            db_table = lower_mastr_table_to_db_table.get(xml_table_name)
            if db_table is None:
                log.warning(
                    f"Skipping MaStR file {file_name!r} because no database table was found for {xml_table_name=}"
                )
                continue

            threads_data.append(
                (
                    file_name,
                    db_table,
                    str(engine.url),
                    engine.url.password,
                    zipped_xml_file_path,
                    bulk_download_date,
                    bulk_cleansing,
                    alter_database_tables,
                )
            )

    # Clear each target table exactly once in the parent process
    db_tables_to_clear = {db_table.name: db_table for _, db_table, *_ in threads_data}
    for db_table in db_tables_to_clear.values():
        delete_all_existing_rows(db_table=db_table, engine=engine)

    interleaved_files = interleave_files(threads_data)
    number_of_processes = get_number_of_processes()

    if number_of_processes > 0:
        with ProcessPoolExecutor(max_workers=number_of_processes) as executor:
            futures = [
                executor.submit(process_xml_file, *item) for item in interleaved_files
            ]
            for future in futures:
                future.result()
            wait(futures)
    else:
        for item in interleaved_files:
            process_xml_file(*item)

    log.info("Bulk download was successful.")


def get_number_of_processes():
    """Get the number of processes to use for the bulk download. Returns -1 if the user has not opted for the
    parallelized implementation. Otherwise, we recommend using the number of available CPUs - 1. If the user wants to
    use more processes, they can set the custom environment variable."""
    if "NUMBER_OF_PROCESSES" in os.environ:
        try:
            number_of_processes = int(os.environ.get("NUMBER_OF_PROCESSES"))
        except ValueError:
            log.warning("Invalid value for NUMBER_OF_PROCESSES. Fallback to 1.")
            return 1
        if number_of_processes >= cpu_count():
            log.warning(
                f"Your system supports {cpu_count()} CPUs. Using "
                f"more processes than available CPUs may cause excessive "
                f"context-switching overhead."
            )
        return number_of_processes
    if "USE_RECOMMENDED_NUMBER_OF_PROCESSES" in os.environ:
        return int(min(cpu_count() - 1, 4))
    return -1


def process_xml_file(
    file_name: str,
    db_table: Table,
    connection_url: str,
    password: str,
    zipped_xml_file_path: str,
    bulk_download_date: str,
    bulk_cleansing: bool,
    alter_database_tables: bool,
) -> None:
    """Process a single xml file and write it to the database."""
    try:
        # If set, the connection url obfuscates the password. We must replace the masked password with the actual password.
        if password:
            connection_url = re.sub(
                r"://([^:]+):\*+@", r"://\1:" + password + "@", connection_url
            )

        # Each process will create its own engine to ensure isolation and efficient resource management.
        # The connection url obfuscates the password. We must replace the masked password with the actual password.
        engine = create_efficient_engine(connection_url)
        with ZipFile(zipped_xml_file_path, "r") as f:
            log.info(f"Processing file '{file_name}'...")
            df = read_xml_file(f, file_name)
            df = process_table_before_insertion(
                df=df,
                db_table=db_table,
                zipped_xml_file_path=zipped_xml_file_path,
                bulk_download_date=bulk_download_date,
                bulk_cleansing=bulk_cleansing,
            )
            df = check_for_column_mismatch_and_try_to_solve_it(
                df=df,
                db_table=db_table,
                engine=engine,
                alter_database_tables=alter_database_tables,
            )
            if engine.dialect.name == "sqlite":
                add_table_to_sqlite_database(
                    df=df,
                    db_table=db_table,
                    engine=engine,
                )
            else:
                add_table_to_non_sqlite_database(
                    df=df,
                    db_table=db_table,
                    engine=engine,
                )
    except Exception as e:
        log.error(f"Error processing file '{file_name}': '{e}'")


def delete_all_existing_rows(db_table: Table, engine: Engine) -> None:
    with engine.begin() as con:
        con.execute(delete(db_table))


def check_for_column_mismatch_and_try_to_solve_it(
    df: pd.DataFrame,
    db_table: Table,
    engine: Engine,
    alter_database_tables: bool,
) -> pd.DataFrame:
    df_column_names = set(df.columns)
    db_column_names = {column.name for column in db_table.columns}

    if additional_db_column_names := db_column_names - df_column_names:
        # Many columns are optional and it's perfectly normal to have and XML file / a dataframe that doesn't have
        # a column that is present in the database. So this is only worth a debug message.
        log.debug(
            f"Database table {db_table.name} has some columns that weren't found in the XML file."
            f" Proceeding and trying to insert anyway. Additional DB columns:"
            f" {', '.join(additional_db_column_names)}"
        )

    if additional_df_column_names := df_column_names - db_column_names:
        if alter_database_tables:
            log.warning(
                f"XML file has some columns that aren't present in the database table {db_table.name}."
                f" Trying to add the columns to the table. Additional XML columns:"
                f" {', '.join(additional_df_column_names)}"
            )
            # TODO: What if we can add some columns and not others? We should then return the columns for which we succeeded.
            try:
                add_missing_columns_to_table(
                    engine=engine,
                    db_table=db_table,
                    missing_columns=additional_df_column_names,
                )
            except Exception:
                log.exception(
                    "Could not add at least some columns to the database. Ignoring the columns from the XML file instead."
                )
                df = df.drop(columns=additional_df_column_names)
        else:
            log.warning(
                f"XML file has some columns that aren't present in the database table {db_table.name}."
                f" Ignoring those columns since you asked not to alter tables. Additional XML columns:"
                f" {', '.join(additional_df_column_names)}"
            )
            df = df.drop(columns=additional_df_column_names)

    return df


def create_efficient_engine(connection_url: str) -> sqlalchemy.engine.Engine:
    """Create an efficient engine for the SQLite database."""
    is_sqlite = connection_url.startswith("sqlite://")

    connect_args = {}

    if is_sqlite:
        # Wait for max 5 minutes before timing out.
        connect_args["timeout"] = 300
        # Lock the database only it is necessary to improve concurrency and performance.
        connect_args["isolation_level"] = "DEFERRED"
        # Allow multiple threads to access the database.
        connect_args["check_same_thread"] = False
    else:
        # Wait for max 5 minutes before timing out.
        connect_args["connect_timeout"] = 300

    return create_engine(
        connection_url,
        connect_args=connect_args,
        # Before returning a connection from the pool, check if the connection is still valid.
        pool_pre_ping=True,
        # Max number of connections in the pool.
        pool_size=10,
        # Create up to 20 more connections when the demand for connections is high.
        max_overflow=20,
        # Recycle inactive connections after 180 seconds to prevent stale connections.
        pool_recycle=180,
        # Wait for 30 seconds before raising an exception when the pool is full.
        pool_timeout=30,
    )


def interleave_files(threads_data: list):
    """
    Multiple threads will process different files at once. If the files target the same table, the risk of a
    "database lock" error (i.e., 2 threads attempting to modify the same table at the same time) is increased.
    To reduce this probability, we can "interleave" the files based on the table they belong to.
    Example:
        Initial order: AnlagenEegSolar_1, AnlagenEegSolar_2, ..., AnlagenEegSpeicher_1, AnlagenEegSpeicher_2, ...
        Interleaved order: AnlagenEegSolar_1, AnlagenEegSpeicher_1, ..., AnlagenEegSolar_2, AnlagenEegSpeicher_2, ...
    """
    files_grouped_by_table = {}

    for item in threads_data:
        db_table = item[1]
        table_name = db_table.name
        if table_name not in files_grouped_by_table:
            files_grouped_by_table[table_name] = []
        files_grouped_by_table[table_name].append(item)

    sorted_threads_data = []
    max_no_files_per_table = max(
        len(group) for group in files_grouped_by_table.values()
    )

    for idx in range(max_no_files_per_table):
        for table_name in files_grouped_by_table.keys():
            files = files_grouped_by_table[table_name]
            if idx < len(files):
                sorted_threads_data.append(files[idx])

    return sorted_threads_data


def extract_xml_table_name(file_name: str) -> str:
    """Extract the XML table name from the file name."""
    return file_name.split("_")[0].split(".")[0].lower()


def cast_date_columns_to_datetime(db_table: Table, df: pd.DataFrame) -> pd.DataFrame:
    for column in db_table.columns:
        if is_date_column(column) and column.name in df.columns:
            # Convert column to datetime64, invalid string -> NaT
            df[column.name] = pd.to_datetime(df[column.name], errors="coerce")
    return df


def cast_date_columns_to_string(db_table: Table, df: pd.DataFrame) -> pd.DataFrame:
    for column in db_table.columns:
        if not is_date_column(column) or column.name not in df.columns:
            continue

        df[column.name] = pd.to_datetime(df[column.name], errors="coerce")

        if type(column.type) is Date:
            mask = df[column.name].notna()
            df[column.name] = df[column.name].dt.strftime("%Y-%m-%d")
            df.loc[mask, column.name] = df.loc[mask, column.name].str.zfill(10)
            df[column.name] = df[column.name].replace("NaT", None)
        elif type(column.type) is DateTime:
            df[column.name] = (
                df[column.name].dt.strftime("%Y-%m-%d %H:%M:%S.%f").replace("NaT", None)
            )
    return df


def is_date_column(column: Column) -> bool:
    return type(column.type) in [Date, DateTime]


def correct_ordering_of_filelist(files_list: list) -> list:
    """Files that end with a single digit number get a 0 prefixed to this number
    to correct the list ordering. Afterwards the 0 is deleted again."""
    files_list_ordered = []
    count_if_zeros_are_prefixed = 0
    for file_name in files_list:
        if len(file_name.split(".")[0].split("_")[-1]) == 1:
            file_name = file_name.split("_")[0] + "_0" + file_name.split("_")[1]
            count_if_zeros_are_prefixed += 1
        files_list_ordered.append(file_name)

    files_list_ordered.sort()
    # the list is now in right order, but the 0 has to be deleted
    files_list_correct = []
    for file_name in files_list_ordered:
        if file_name.split(".")[0].split("_")[-1][0] == "0":
            file_name = file_name.split("_")[0] + "_" + file_name.split("_0")[-1]
        files_list_correct.append(file_name)

    if count_if_zeros_are_prefixed >= 5:
        # check if file names from marktstammdatenregister have no prefixed 0 already
        files_list = files_list_correct

    return files_list


def read_xml_file(f: ZipFile, file_name: str) -> pd.DataFrame:
    """Read the xml file from the zip file and return it as a DataFrame."""
    with f.open(file_name) as xml_file:
        try:
            return pd.read_xml(xml_file, encoding="UTF-16", parser="etree")
        except lxml.etree.XMLSyntaxError as error:
            return handle_xml_syntax_error(xml_file.read().decode("utf-16"), error)


def add_table_to_non_sqlite_database(
    df: pd.DataFrame,
    db_table: Table,
    engine: sqlalchemy.engine.Engine,
) -> None:
    # get a dictionary for the data types
    dtypes_for_writing_sql = {
        column.name: column.type
        for column in db_table.columns
        if column.name in df.columns
    }

    # Convert date and datetime columns into the datatype datetime.
    df = cast_date_columns_to_datetime(db_table, df)

    for _ in range(10000):
        try:
            with engine.connect() as con:
                with con.begin():
                    df.to_sql(
                        db_table.name,
                        con=con,
                        index=False,
                        if_exists="append",
                        dtype=dtypes_for_writing_sql,
                    )
                    break

        except sqlalchemy.exc.DataError as err:
            delete_wrong_xml_entry(err, df)

        except sqlalchemy.exc.IntegrityError:
            # error resulting from Unique constraint failed
            # FIXME: This error can also indicate other problems than non-unique.
            # We should differentiate more and show it to the user for cases we cannot solve.
            df = write_single_entries_until_not_unique_comes_up(df, db_table, engine)


def add_zero_as_first_character_for_too_short_string(df: pd.DataFrame) -> pd.DataFrame:
    """Some columns are read as integer even though they are actually strings starting with
    a 0. This function converts those columns back to strings and adds a 0 as first character.
    """

    dict_of_columns_and_string_length = {
        "Gemeindeschluessel": 8,
        "Postleitzahl": 5,
    }
    for column_name, string_length in dict_of_columns_and_string_length.items():
        if column_name not in df.columns:
            continue
        try:
            df[column_name] = df[column_name].astype("Int64").astype(str)
        except (ValueError, TypeError):
            # some Plz are in the format DK-9999 for danish Postleitzahl
            # or A-9999 for austrian PLz
            # They cannot be converted to integer
            df[column_name] = df[column_name].astype(str)
            continue
        df[column_name] = df[column_name].where(
            cond=-df[column_name].isin(["None", "<NA>"]), other=None
        )

        string_adding_series = pd.Series(["0"] * len(df))
        string_adding_series = string_adding_series.where(
            cond=df[column_name].str.len() == string_length - 1, other=""
        )
        df[column_name] = string_adding_series + df[column_name]
    return df


def write_single_entries_until_not_unique_comes_up(
    df: pd.DataFrame, db_table: Table, engine: sqlalchemy.engine.Engine
) -> pd.DataFrame:
    """
    Remove from dataframe these rows, which are already existing in the database table
    Parameters
    ----------
    df
    xml_table_name
    engine

    Returns
    -------
    Filtered dataframe
    """
    # TODO: Check if we need to support composite primary keys for the MaStR changes table.
    # Because this here assumes single-column primary keys.
    primary_key = next(c for c in db_table.columns if c.primary_key)

    with engine.begin() as con:
        key_list = (
            pd.read_sql(sql=select(primary_key), con=con).values.squeeze().tolist()
        )

    len_df_before = len(df)
    df = df.drop_duplicates(
        subset=[primary_key.name]
    )  # drop all entries with duplicated primary keys in the dataframe
    df = df.set_index(primary_key.name)

    df = df.drop(
        labels=key_list, errors="ignore"
    )  # drop primary keys that already exist in the table
    df = df.reset_index()
    log.warning(f"{len_df_before - len(df)} entries already existed in the database.")

    return df


def add_missing_columns_to_table(
    engine: sqlalchemy.engine.Engine,
    db_table: Table,
    missing_columns: Collection[str],
) -> None:
    """
    Some files introduce new columns for existing tables.
    If the pandas dataframe contains columns that do not
    exist in the database, they are added to the database.
    Parameters
    ----------
    engine
    xml_table_name
    column_list

    Returns
    -------

    """
    table_name = db_table.name
    for column_name in missing_columns:
        alter_query = 'ALTER TABLE %s ADD "%s" VARCHAR NULL;' % (
            table_name,
            column_name,
        )
        try:
            with engine.connect().execution_options(autocommit=True) as con:
                with con.begin():
                    con.execute(text(alter_query).execution_options(autocommit=True))
        except sqlalchemy.exc.OperationalError as err:
            # If the column already exists, we can ignore the error.
            if "duplicate column name" not in str(err):
                raise err
    log.info(
        f"Added the following columns to database table {table_name}:"
        f" {', '.join(missing_columns)}"
    )


def delete_wrong_xml_entry(err: Error, df: pd.DataFrame) -> pd.DataFrame:
    delete_entry = str(err).split("«")[0].split("»")[1]
    log.warning(f"The entry {delete_entry} was deleted due to its false data type.")
    return df.replace(delete_entry, np.nan)


def handle_xml_syntax_error(data: str, err: Error) -> pd.DataFrame:
    """Deletes entries that cause an xml syntax error and produces DataFrame.

    Parameters
    -----------
    data : str
        Decoded xml file as one string
    err : ErrorMessage
        Error message that appeared when trying to use pd.read_xml on invalid xml file.

    Returns
    ----------
    df : pandas.DataFrame
        DataFrame which is read from the changed xml data.
    """

    def find_nearest_brackets(xml_string: str, position: int) -> tuple[int, int]:
        left_bracket_position = xml_string.rfind(">", 0, position)
        right_bracket_position = xml_string.find("<", position)
        return left_bracket_position, right_bracket_position

    data = data.splitlines()

    for _ in range(100):
        # check for maximum of 100 syntax errors, otherwise return an error
        wrong_char_row, wrong_char_column = err.position
        row_with_error = data[wrong_char_row - 1]

        left_bracket, right_bracket = find_nearest_brackets(
            row_with_error, wrong_char_column
        )
        data[wrong_char_row - 1] = (
            row_with_error[: left_bracket + 1] + row_with_error[right_bracket:]
        )
        try:
            log.warning("One invalid xml expression was deleted.")
            df = pd.read_xml(StringIO("\n".join(data)))
            return df
        except lxml.etree.XMLSyntaxError as e:
            err = e
            continue

    raise Error("An error occured when parsing the xml file. Maybe it is corrupted?")


def process_table_before_insertion(
    df: pd.DataFrame,
    db_table: Table,
    zipped_xml_file_path: str,
    bulk_download_date: str,
    bulk_cleansing: bool,
) -> pd.DataFrame:
    df = add_zero_as_first_character_for_too_short_string(df)

    # Add Column that refers to the source of the data
    df["DatenQuelle"] = "bulk"
    df["DatumDownload"] = bulk_download_date

    df = align_df_column_names_to_db_column_names(df=df, db_table=db_table)

    if bulk_cleansing:
        catalog_columns = {
            column.name
            for column in db_table.columns
            if isinstance(column.type, (CatalogInteger, CatalogString))
        }
        df = cleanse_bulk_data(
            df=df,
            catalog_columns=catalog_columns,
            zipped_xml_file_path=zipped_xml_file_path,
        )
    return df


def align_df_column_names_to_db_column_names(
    df: pd.DataFrame,
    db_table: Table,
) -> pd.DataFrame:
    old_column_name_to_new_column_name = {
        column_name: normalize_mastr_name(column_name) for column_name in df.columns
    }
    if db_table.name == db_table.info.get("english_name"):
        # Database is in English. We must translate the df columns
        english_updates = {}
        for (
            old_column_name,
            normalized_column_name,
        ) in old_column_name_to_new_column_name.items():
            if english_column_name := translate_mastr_column_name(
                normalized_column_name
            ):
                english_updates[old_column_name] = english_column_name
        old_column_name_to_new_column_name.update(english_updates)

    renamed_df = df.rename(columns=old_column_name_to_new_column_name)
    return renamed_df


def add_table_to_sqlite_database(
    df: pd.DataFrame,
    db_table: Table,
    engine: sqlalchemy.engine.Engine,
) -> None:
    column_list = df.columns.tolist()

    # Convert NaNs to None.
    df = df.where(pd.notnull(df), None)

    # Convert date columns to strings. Dates are not supported directly by SQLite.
    df = cast_date_columns_to_string(db_table, df)

    # Create SQL statement for bulk insert. ON CONFLICT DO NOTHING prevents duplicates.
    insert_stmt = f"INSERT INTO {db_table.name} ({','.join(column_list)}) VALUES ({','.join(['?' for _ in column_list])}) ON CONFLICT DO NOTHING"

    for _ in range(10000):
        try:
            with engine.begin() as con:
                con.connection.executemany(insert_stmt, df.to_numpy())
                break
        except sqlalchemy.exc.DataError as err:
            delete_wrong_xml_entry(err, df)
        except sqlalchemy.exc.IntegrityError:
            # error resulting from Unique constraint failed
            # FIXME: This error can also indicate other problems than non-unique.
            # We should differentiate more and show it to the user for cases we cannot solve.
            df = write_single_entries_until_not_unique_comes_up(df, db_table, engine)
        except Exception:
            # If any unexpected error occurs, we'll switch back to the non-SQLite method.
            add_table_to_non_sqlite_database(df, db_table, engine)
            break
