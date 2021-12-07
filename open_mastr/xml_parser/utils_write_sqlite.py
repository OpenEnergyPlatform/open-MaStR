from shutil import Error
from typing import Tuple
from zipfile import ZipFile
import os
from os.path import expanduser
import lxml
import numpy as np
import pandas as pd
import sqlalchemy
import sqlite3
import pdb


def convert_mastr_xml_to_sqlite(
    con: sqlite3.Connection,
    zipped_xml_file_path: str,
    include_tables: list,
    exclude_tables: list,
) -> None:
    """Converts the Mastr in xml format into a sqlite database."""
    """Writes the local zipped MaStR to a PostgreSQL database.
        
    Parameters
    ------------
    include_tables : list, default None
        List of tables from the Marktstammdatenregister that should be written into
        the database. Elements of include_tables are lower case strings without "_" and index. 
        It is possible to include any table from the zipped local MaStR folder (see MaStR.initialize()). 
        Example: If you do want to write the data from files "AnlagenEegSolar_*.xml" to a table 
        in your database (where * is any number >=1), write the element "anlageneegsolar" into the 
        include_tables list. Elements of the list that cannot be matched to tables of the MaStR are ignored.
        If include_tables is given, only the tables listed here are written to the database.

    exclude_tables : list, default None
        List of tables from the Marktstammdatenregister that should NOT be written into
        the database. Elements of exclude_tables are lower case strings without "_" and index. 
        It is possible to exclude any table from the zipped local MaStR folder (see MaStR.initialize()). 
        Example: If you do not want to write the data from files "AnlagenEegSolar_*.xml" to a table 
        in your database (where * is any number >=1), write the element "anlageneegsolar" into the 
        exclude_tables list. Elements of the list that cannot be matched to tables of the MaStR are ignored.

    """

    tables_reference_list, count_reference = make_reference_list_and_count(
        include_tables, exclude_tables
    )

    with ZipFile(zipped_xml_file_path, "r") as f:
        for file_name in f.namelist():
            # sql tablename is the beginning of the filename without the number in lowercase
            sql_tablename = file_name.split("_")[0].split(".")[0].lower()

            # check whether the table exists with current data and append new data or whether to overwrite the existing table

            exclude_count = tables_reference_list.count(sql_tablename)
            if exclude_count == count_reference:

                if (
                    file_name.split(".")[0].split("_")[-1] == "1"
                    or len(file_name.split(".")[0].split("_")) == 1
                ):
                    if_exists = "replace"
                    print("New table %s is created in the database." % sql_tablename)
                    index_for_printed_message = 1
                else:
                    if_exists = "append"
                    print(
                        f"File {index_for_printed_message} from {sql_tablename} is parsed."
                    )
                    index_for_printed_message += 1

                add_table_to_sqlite_database(
                    f, file_name, sql_tablename, if_exists, con
                )


def make_reference_list_and_count(
    include_tables: list, exclude_tables: list
) -> Tuple[list, int]:
    """
    count_reference and tables_reference_list are important for checking which files are written to the SQL database
    if count_reference is 1, all files from the tables_reference_list are included to be written to the databse,
    if it is 0, all files from the tables_reference_list are excluded.
    """
    if include_tables:
        count_reference = 1
        tables_reference_list = include_tables
    elif exclude_tables:
        count_reference = 0
        tables_reference_list = exclude_tables
    else:
        count_reference = 0
        tables_reference_list = []
    return tables_reference_list, count_reference


def add_table_to_sqlite_database(
    f: ZipFile,
    file_name: str,
    sql_tablename: str,
    if_exists: bool,
    con: sqlite3.Connection,
) -> None:
    data = f.read(file_name)
    try:
        df = pd.read_xml(data, encoding="UTF-16", compression="zip")
    except lxml.etree.XMLSyntaxError as err:
        df = handle_xml_syntax_error(data, err)

    continueloop = True
    while continueloop:
        try:
            df.to_sql(
                sql_tablename,
                con,
                if_exists=if_exists,
            )
            continueloop = False
        except sqlite3.OperationalError as err:
            add_missing_column_to_table(err, con, sql_tablename)

        except sqlalchemy.exc.DataError as err:
            delete_wrong_xml_entry(err, df)


def add_missing_column_to_table(
    err: Error, con: sqlite3.Connection, sql_tablename: str
) -> None:
    """Some files introduce new columns for existing tables.
    If this happens, the error from writing entries into non-existing columns is caught and the column is created."""
    missing_column = str(err).split("no column named ")[1]
    cursor = con.cursor()
    execute_message = 'ALTER TABLE %s ADD "%s" text NULL;' % (
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

    # Actually it is unclear if there are still invalid xml files in the recent MaStR.

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
