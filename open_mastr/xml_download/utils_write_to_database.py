from shutil import Error
from zipfile import ZipFile
import lxml
import numpy as np
import pandas as pd
import sqlite3
import sqlalchemy
from sqlalchemy import select
from sqlalchemy.sql import text

from open_mastr.utils.helpers import data_to_include_tables
from open_mastr.utils.orm import tablename_mapping
from open_mastr.xml_download.utils_cleansing_bulk import cleanse_bulk_data
from open_mastr.utils.config import setup_logger


def write_mastr_xml_to_database(
    engine: sqlalchemy.engine.Engine,
    zipped_xml_file_path: str,
    data: list,
    bulk_cleansing: bool,
    bulk_download_date: str,
) -> None:
    """Write the Mastr in xml format into a database defined by the engine parameter."""
    include_tables = data_to_include_tables(data, mapping="write_xml")

    with ZipFile(zipped_xml_file_path, "r") as f:
        files_list = f.namelist()
        files_list = correct_ordering_of_filelist(files_list)
        for file_name in files_list:
            # xml_tablename is the beginning of the filename without the number in lowercase
            xml_tablename = file_name.split("_")[0].split(".")[0].lower()

            if is_table_relevant(
                xml_tablename=xml_tablename, include_tables=include_tables
            ):
                sql_tablename = tablename_mapping[xml_tablename]["__name__"]

                if is_first_file(file_name):
                    create_database_table(engine=engine, xml_tablename=xml_tablename)
                    print(
                        f"Table '{sql_tablename}' is filled with data '{xml_tablename}' "
                        "from the bulk download."
                    )
                print(f"File '{file_name}' is parsed.")

                df = preprocess_table_for_writing_to_database(
                    f=f,
                    file_name=file_name,
                    xml_tablename=xml_tablename,
                    bulk_download_date=bulk_download_date,
                )

                # Convert date and datetime columns into the datatype datetime
                df = cast_date_columns_to_datetime(xml_tablename, df)

                if bulk_cleansing:
                    df = cleanse_bulk_data(df, zipped_xml_file_path)

                add_table_to_database(
                    df=df,
                    xml_tablename=xml_tablename,
                    sql_tablename=sql_tablename,
                    if_exists="append",
                    engine=engine,
                )
    print("Bulk download and data cleansing were successful.")


def is_table_relevant(xml_tablename: str, include_tables: list) -> bool:
    """Checks if the table contains relevant data and if the user wants to
    have it in the database."""
    # few tables are only needed for data cleansing of the xml files and contain no
    # information of relevance
    boolean_write_table_to_sql_database = (
        tablename_mapping[xml_tablename]["__class__"] is not None
    )
    # check if the table should be written to sql database (depends on user input)
    include_count = include_tables.count(xml_tablename)

    return include_count == 1 and boolean_write_table_to_sql_database


def create_database_table(engine: sqlalchemy.engine.Engine, xml_tablename: str) -> None:
    orm_class = tablename_mapping[xml_tablename]["__class__"]
    # drop the content from table
    orm_class.__table__.drop(engine, checkfirst=True)
    # create table schema
    orm_class.__table__.create(engine)


def is_first_file(file_name: str) -> bool:
    """check if the file name indicates that it is the first file from the table"""
    return (
        file_name.split(".")[0].split("_")[-1] == "1"
        or len(file_name.split(".")[0].split("_")) == 1
    )


def cast_date_columns_to_datetime(xml_tablename: str, df: pd.DataFrame) -> pd.DataFrame:

    sqlalchemy_columnlist = tablename_mapping[xml_tablename][
        "__class__"
    ].__table__.columns.items()
    for column in sqlalchemy_columnlist:
        column_name = column[0]
        if is_date_column(column, df):
            # Convert column to datetime64, invalid string -> NaT
            df[column_name] = pd.to_datetime(df[column_name], errors="coerce")
    return df


def is_date_column(column, df: pd.DataFrame) -> bool:
    return (
        type(column[1].type)
        in [
            sqlalchemy.sql.sqltypes.Date,
            sqlalchemy.sql.sqltypes.DateTime,
        ]
        and column[0] in df.columns
    )


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


def preprocess_table_for_writing_to_database(
    f: ZipFile,
    file_name: str,
    xml_tablename: str,
    bulk_download_date: str,
) -> pd.DataFrame:
    data = f.read(file_name)
    try:
        df = pd.read_xml(data, encoding="UTF-16", compression="zip")
    except lxml.etree.XMLSyntaxError as err:
        df = handle_xml_syntax_error(data, err)

    df = add_zero_as_first_character_for_too_short_string(df)
    df = change_column_names_to_orm_format(df, xml_tablename)

    # Add Column that refers to the source of the data
    df["DatenQuelle"] = "bulk"
    df["DatumDownload"] = bulk_download_date
    return df


def change_column_names_to_orm_format(
    df: pd.DataFrame, xml_tablename: str
) -> pd.DataFrame:
    if tablename_mapping[xml_tablename]["replace_column_names"]:
        df.rename(
            columns=tablename_mapping[xml_tablename]["replace_column_names"],
            inplace=True,
        )
    return df


def add_table_to_database(
    df: pd.DataFrame,
    xml_tablename: str,
    sql_tablename: str,
    if_exists: str,
    engine: sqlalchemy.engine.Engine,
) -> None:

    # get a dictionary for the data types

    table_columns_list = list(
        tablename_mapping[xml_tablename]["__class__"].__table__.columns
    )
    dtypes_for_writing_sql = {
        column.name: column.type
        for column in table_columns_list
        if column.name in df.columns
    }

    continueloop = True
    while continueloop:
        try:
            with engine.connect() as con:
                with con.begin():
                    df.to_sql(
                        sql_tablename,
                        con=con,
                        index=False,
                        if_exists=if_exists,
                        dtype=dtypes_for_writing_sql,
                    )
                    continueloop = False
        except sqlalchemy.exc.OperationalError as err:
            add_missing_column_to_table(err, engine, xml_tablename)

        except sqlalchemy.exc.ProgrammingError as err:
            add_missing_column_to_table(err, engine, xml_tablename)

        except sqlite3.OperationalError as err:
            add_missing_column_to_table(err, engine, xml_tablename)

        except sqlalchemy.exc.DataError as err:
            delete_wrong_xml_entry(err, df)

        except sqlalchemy.exc.IntegrityError as err:
            # error resulting from Unique constraint failed
            df = write_single_entries_until_not_unique_comes_up(
                df=df, xml_tablename=xml_tablename, engine=engine
            )


def add_zero_as_first_character_for_too_short_string(df: pd.DataFrame) -> pd.DataFrame:
    """Some columns are read as integer even though they are actually strings starting with
    a 0. This function converts those columns back to strings and adds a 0 as first character."""

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
    df: pd.DataFrame, xml_tablename: str, engine: sqlalchemy.engine.Engine
) -> pd.DataFrame:
    """
    Remove from dataframe these rows, which are already existing in the database table
    Parameters
    ----------
    df
    xml_tablename
    engine

    Returns
    -------
    Filtered dataframe
    """

    table = tablename_mapping[xml_tablename]["__class__"].__table__
    primary_key = next(c for c in table.columns if c.primary_key)

    with engine.connect() as con:
        with con.begin():
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
    print(f"{len_df_before-len(df)} entries already existed in the database.")

    return df


def add_missing_column_to_table(
    err: Error, engine: sqlalchemy.engine.Engine, xml_tablename: str
) -> None:
    """
    Some files introduce new columns for existing tables.
    If this happens, the error from writing entries into
    non-existing columns is caught and the column is created.
    Parameters
    ----------
    err
    engine
    xml_tablename

    Returns
    -------

    """
    log = setup_logger()

    if engine.name == "postgresql":
        missing_column = err.args[0].split("»")[1].split("«")[0]
    elif engine.name == "sqlite":
        missing_column = err.args[0].split()[-1]
    else:
        # only a guess, can fail with other db systems
        missing_column = err.args[0].split()[-1]
    table = tablename_mapping[xml_tablename]["__class__"].__table__

    alter_query = 'ALTER TABLE %s ADD "%s" VARCHAR NULL;' % (
        table.name,
        missing_column,
    )
    with engine.connect().execution_options(autocommit=True) as con:
        with con.begin():
            con.execute(text(alter_query).execution_options(autocommit=True))
    log.info(
        "From the downloaded xml files following new attribute was "
        f"introduced: {table.name}.{missing_column}"
    )


def delete_wrong_xml_entry(err: Error, df: pd.DataFrame) -> None:
    delete_entry = str(err).split("«")[0].split("»")[1]
    print(f"The entry {delete_entry} was deleted due to its false data type.")
    df = df.replace(delete_entry, np.nan)


def handle_xml_syntax_error(data: bytes, err: Error) -> pd.DataFrame:
    """Deletes entries that cause an xml syntax error and produces DataFrame.

    Parameters
    -----------
    data : bytes
        Unzipped xml data
    err : ErrorMessage
        Error message that appeared when trying to use pd.read_xml on invalid xml file.

    Returns
    ----------
    df : pandas.DataFrame
        DataFrame which is read from the changed xml data.
    """
    wrong_char_position = int(str(err).split()[-4])
    decoded_data = data.decode("utf-16")
    loop_condition = True

    shift = 0
    while loop_condition:
        evaluated_string = decoded_data[wrong_char_position + shift]
        if evaluated_string == ">":
            start_char = wrong_char_position + shift + 1
            break
        else:
            shift -= 1
    loop_condition_2 = True
    while loop_condition_2:
        evaluated_string = decoded_data[start_char]
        if evaluated_string == "<":
            break
        else:
            decoded_data = decoded_data[:start_char] + decoded_data[start_char + 1 :]
    df = pd.read_xml(decoded_data)
    print("One invalid xml expression was deleted.")
    return df
