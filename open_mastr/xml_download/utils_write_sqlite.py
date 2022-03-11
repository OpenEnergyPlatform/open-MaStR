from shutil import Error
from zipfile import ZipFile
import lxml
import numpy as np
import pandas as pd
import sqlalchemy
import sqlite3
from open_mastr.orm import tablename_mapping
from open_mastr.xml_download.colums_to_replace import system_catalog
from open_mastr.xml_download.utils_sqlite_bulk_cleansing import (
    replace_mastr_katalogeintraege,
    date_columns_to_datetime,
)
from sqlalchemy import (
    Integer,
    String,
    Float,
    DateTime,
    Boolean,
    Date,
    JSON,
)


dtypes_mapping = {
    String: "string",
    Integer: "int",
    Float: "float",
    DateTime: "datetime64[s]",
    Boolean: "bool",
    Date: "datetime64[s]",
    JSON: "string",
}


def convert_mastr_xml_to_sqlite(
    con: sqlite3.Connection,
    engine,
    zipped_xml_file_path: str,
    include_tables: list,
    bulk_cleansing: bool,
) -> None:
    """Converts the Mastr in xml format into a sqlite database."""
    """Writes the local zipped MaStR to a PostgreSQL database.

    Parameters
    ------------
    include_tables : list, default None
        List of tables from the Marktstammdatenregister that
        should be written into the database. Elements of
        include_tables are lower case strings without "_" and index.
        It is possible to include any table from the zipped
        local MaStR folder (see MaStR.initialize()).
        Example: If you do want to write the data from
        files "AnlagenEegSolar_*.xml" to a table in your
        database (where * is any number >=1), write the
        element "anlageneegsolar" into the include_tables list.
        Elements of the list that cannot be matched to tables of
        the MaStR are ignored. If include_tables is given,
        only the tables listed here are written to the database.


    """

    with ZipFile(zipped_xml_file_path, "r") as f:
        for file_name in f.namelist():
            # xml_tablename is the beginning of the filename without the number in lowercase
            xml_tablename = file_name.split("_")[0].split(".")[0].lower()

            # few tables are only needed for data cleansing of the xml files and contain no
            # information of relevance
            boolean_write_table_to_sql_database = False
            if tablename_mapping[xml_tablename]["__class__"] is not None:
                boolean_write_table_to_sql_database = True

            # check if the table should be written to sql database (depends on user input)
            include_count = include_tables.count(xml_tablename)

            if include_count == 1 and boolean_write_table_to_sql_database:

                sql_tablename = tablename_mapping[xml_tablename]["__name__"]
                # check if the file name indicates that it is the first file from the
                # table
                if (
                    file_name.split(".")[0].split("_")[-1] == "1"
                    or len(file_name.split(".")[0].split("_")) == 1
                ):
                    orm_class = tablename_mapping[xml_tablename]["__class__"]
                    # drop the content from table
                    orm_class.__table__.drop(engine, checkfirst=True)
                    # create table schema
                    orm_class.__table__.create(engine)

                    print(
                        f"Table '{sql_tablename}' is filled with data '{xml_tablename}' "
                        "from the bulk download."
                    )
                    index_for_printed_message = 1
                else:
                    print(
                        f"File {index_for_printed_message} from '{xml_tablename}' is parsed."
                    )
                    index_for_printed_message += 1

                df = prepare_table_to_sqlite_database(f, file_name, xml_tablename)

                if bulk_cleansing:
                    print("Data cleansing started.")

                    # Convert date and datetime columns into the datatype datetime
                    df = date_columns_to_datetime(xml_tablename, df)

                    # Katalogeintraege: int -> string value
                    df = replace_mastr_katalogeintraege(
                        sql_tablename=sql_tablename,
                        zipped_xml_file_path=zipped_xml_file_path,
                        df=df,
                    )

                add_table_to_sqlite_database(
                    df=df,
                    xml_tablename=xml_tablename,
                    sql_tablename=sql_tablename,
                    if_exists="append",
                    con=con,
                    engine=engine,
                )


def prepare_table_to_sqlite_database(
    f: ZipFile,
    file_name: str,
    xml_tablename: str,
) -> pd.DataFrame:
    data = f.read(file_name)
    try:
        df = pd.read_xml(data, encoding="UTF-16", compression="zip")
    except lxml.etree.XMLSyntaxError as err:
        df = handle_xml_syntax_error(data, err)

    # Change data types in dataframe
    dict_of_tables_and_string_length = {
        "Gemeindeschluessel": 8,
        "Postleitzahl": 5,
    }
    for table_name in dict_of_tables_and_string_length.keys():
        if table_name in df.columns:
            string_length = dict_of_tables_and_string_length[table_name]
            df = add_zero_as_first_character_for_too_short_string(
                df, table_name, string_length
            )

    # Replace IDs with names from system_catalogue
    for table_name in system_catalog.keys():
        if table_name in df.columns:
            df[table_name] = df[table_name].replace(system_catalog[table_name])

    # Change column names according to orm data model
    if tablename_mapping[xml_tablename]["replace_column_names"]:

        df = df.rename(columns=tablename_mapping[xml_tablename]["replace_column_names"])

    # Add Column that refers to the source of the data
    df["Quelle"] = "bulk"
    return df


def add_table_to_sqlite_database(
    df: pd.DataFrame,
    xml_tablename: str,
    sql_tablename: str,
    if_exists: str,
    con: sqlite3.Connection,
    engine,
) -> None:

    # get a dictionary for the data types
    dtypes_for_writing_sql = {}

    for column in list(tablename_mapping[xml_tablename]["__class__"].__table__.columns):
        if column.name in df.columns:
            dtypes_for_writing_sql[column.name] = column.type

    continueloop = True
    while continueloop:
        try:
            df.to_sql(
                sql_tablename,
                con=engine,
                index=False,
                if_exists=if_exists,
                dtype=dtypes_for_writing_sql,
            )
            continueloop = False
        except sqlalchemy.exc.OperationalError as err:
            add_missing_column_to_table(err, con, sql_tablename)

        except sqlite3.OperationalError as err:
            add_missing_column_to_table(err, con, sql_tablename)

        except sqlalchemy.exc.DataError as err:
            delete_wrong_xml_entry(err, df)

        except sqlalchemy.exc.IntegrityError:
            # error resulting from Unique constraint failed
            df = write_single_entries_until_not_unique_comes_up(
                df=df, sql_tablename=sql_tablename, engine=engine
            )


def add_zero_as_first_character_for_too_short_string(df, table_name, string_length):
    # Gemeindeschluessel or PLZ are read as float, but they are actually strings
    # if they start with a 0 this gets lost
    try:
        df[table_name] = df[table_name].astype("Int64").astype(str)
    except ValueError:
        # some Plz are in the format DK-9999 for danish Postleitzahl
        # They cannot be converted to integer
        df[table_name] = df[table_name].astype(str)

    df[table_name] = df[table_name].where(cond=df[table_name] != "None", other=None)
    df[table_name] = df[table_name].where(cond=df[table_name] != "<NA>", other=None)

    string_adding_series = pd.Series(["0"] * len(df))
    string_adding_series = string_adding_series.where(
        cond=df[table_name].str.len() == string_length - 1, other=""
    )
    df[table_name] = string_adding_series + df[table_name]
    return df


def write_single_entries_until_not_unique_comes_up(
    df: pd.DataFrame, sql_tablename: str, engine: sqlalchemy.engine.Engine
) -> None:
    for index, row in df.iterrows():
        try:
            row = row.to_frame().transpose()
            row.to_sql(name=sql_tablename, con=engine, if_exists="append", index=False)
            df = df.drop(labels=index, axis=0)
        except sqlalchemy.exc.IntegrityError:
            df = df.drop(labels=index, axis=0)
            return df


def add_missing_column_to_table(
    err: Error, con: sqlite3.Connection, sql_tablename: str
) -> None:
    """Some files introduce new columns for existing tables.
    If this happens, the error from writing entries into
    non-existing columns is caught and the column is created."""

    # Needed for sqlite3 error message
    # missing_column = str(err).split("no column named ")[1]

    # Needed for sqlalchemy error message
    missing_column = str(err).split("has no column named ")[1].split("\n[SQL: ")[0]

    cursor = con.cursor()
    execute_message = 'ALTER TABLE %s ADD "%s" VARCHAR NULL;' % (
        sql_tablename,
        missing_column,
    )
    cursor.execute(execute_message)
    con.commit()
    cursor.close()


def delete_wrong_xml_entry(err: Error, df: pd.DataFrame) -> None:
    delete_entry = str(err).split("«")[0].split("»")[1]
    print(f"The entry {delete_entry} was deleteted due to its false data type.")
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
