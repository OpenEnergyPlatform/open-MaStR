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
    engine: sqlalchemy.engine.Engine,
    zipped_xml_file_path: str,
    include_tables: list,
    bulk_cleansing: bool,
    bulk_download_date: str,
) -> None:
    """Converts the Mastr in xml format into a sqlite database."""

    with ZipFile(zipped_xml_file_path, "r") as f:
        files_list = f.namelist()
        files_list = correct_ordering_of_filelist(files_list)
        for file_name in files_list:
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
                        "from the bulk download. \n"
                        f"File '{file_name}' is parsed."
                    )
                else:
                    print(f"File '{file_name}' is parsed.")

                df = prepare_table_to_sqlite_database(
                    f=f,
                    file_name=file_name,
                    xml_tablename=xml_tablename,
                    bulk_download_date=bulk_download_date,
                )

                # Convert date and datetime columns into the datatype datetime
                df = date_columns_to_datetime(xml_tablename, df)

                if bulk_cleansing:
                    print("Data is cleansed.")
                    # Katalogeintraege: int -> string value
                    df = replace_mastr_katalogeintraege(
                        zipped_xml_file_path=zipped_xml_file_path, df=df
                    )

                add_table_to_sqlite_database(
                    df=df,
                    xml_tablename=xml_tablename,
                    sql_tablename=sql_tablename,
                    if_exists="append",
                    engine=engine,
                )


def correct_ordering_of_filelist(files_list: list) -> list:
    """Files that end with a single digit number get a 0 prefixed to this number
    to correct the list ordering. Afterwards the 0 is deleted again."""
    files_list_ordered = []
    count_if_zeros_are_prefixed = 0
    for file_name in files_list:
        if len(file_name.split(".")[0].split("_")[-1]) == 1:
            file_name = file_name.split("_")[0]+"_0"+file_name.split("_")[1]
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


def prepare_table_to_sqlite_database(
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

    # Change data types in dataframe
    dict_of_columns_and_string_length = {
        "Gemeindeschluessel": 8,
        "Postleitzahl": 5,
    }
    for column_name in dict_of_columns_and_string_length.keys():
        if column_name in df.columns:
            string_length = dict_of_columns_and_string_length[column_name]
            df = add_zero_as_first_character_for_too_short_string(
                df, column_name, string_length
            )

    # Replace IDs with names from system_catalogue
    for column_name in system_catalog.keys():
        if column_name in df.columns:
            df[column_name] = df[column_name].replace(system_catalog[column_name])

    # Change column names according to orm data model
    if tablename_mapping[xml_tablename]["replace_column_names"]:

        df = df.rename(columns=tablename_mapping[xml_tablename]["replace_column_names"])

    # Add Column that refers to the source of the data
    df["DatenQuelle"] = "bulk"
    df["DatumDownload"] = bulk_download_date
    return df


def add_table_to_sqlite_database(
    df: pd.DataFrame,
    xml_tablename: str,
    sql_tablename: str,
    if_exists: str,
    engine: sqlalchemy.engine.Engine,
) -> None:

    # get a dictionary for the data types
    dtypes_for_writing_sql = {}

    for column in list(tablename_mapping[xml_tablename]["__class__"].__table__.columns):
        if column.name in df.columns:
            dtypes_for_writing_sql[column.name] = column.type

    with engine.begin() as con:
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

            except sqlalchemy.exc.IntegrityError as err:
                # error resulting from Unique constraint failed
                df = write_single_entries_until_not_unique_comes_up(
                    df=df, sql_tablename=sql_tablename, engine=engine, err=err
                )


def add_zero_as_first_character_for_too_short_string(df, column_name, string_length):
    # Gemeindeschluessel or PLZ are read as float, but they are actually strings
    # if they start with a 0 this gets lost
    try:
        df[column_name] = df[column_name].astype("Int64").astype(str)
    except ValueError:
        # some Plz are in the format DK-9999 for danish Postleitzahl
        # They cannot be converted to integer
        df[column_name] = df[column_name].astype(str)

    df[column_name] = df[column_name].where(cond=df[column_name] != "None", other=None)
    df[column_name] = df[column_name].where(cond=df[column_name] != "<NA>", other=None)

    string_adding_series = pd.Series(["0"] * len(df))
    string_adding_series = string_adding_series.where(
        cond=df[column_name].str.len() == string_length - 1, other=""
    )
    df[column_name] = string_adding_series + df[column_name]
    return df


def write_single_entries_until_not_unique_comes_up(
    df: pd.DataFrame, sql_tablename: str, engine: sqlalchemy.engine.Engine, err: str
) -> None:

    key_column = (
        str(err)
        .split("\n[SQL: INSERT INTO")[0]
        .split("UNIQUE constraint failed:")[1]
        .split(".")[1]
    )
    key_list = (
        pd.read_sql(sql=f"SELECT {key_column} FROM {sql_tablename};", con=engine)
        .values.squeeze()
        .tolist()
    )
    df = df.set_index(key_column)
    len_df_before = len(df)
    df = df.drop(labels=key_list, errors="ignore")
    df = df.reset_index()
    print(f"{len_df_before-len(df)} entries already existed in the database.")

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
            decoded_data = decoded_data[:start_char] + decoded_data[start_char + 1:]
    df = pd.read_xml(decoded_data)
    print("One invalid xml expression was deleted.")
    return df
