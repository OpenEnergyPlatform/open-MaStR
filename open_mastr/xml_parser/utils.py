import requests
from clint.textui import progress
import time
import sqlalchemy
import pandas as pd
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from bs4 import BeautifulSoup
import pdb
import dateutil
from mastrsql.metadata import metadata_dict
import sqlite3
from zipfile import ZipFile


def getURLFromMastrWebsite():
    """Get the url of the latest MaStR file from markstammdatenregister.de.

    The file and the corresponding url are updated once per day.
    The url has a randomly generated string appended, so it has to be
    grabbed from the marktstammdatenregister.de homepage.
    For further details visit https://www.marktstammdatenregister.de/MaStR/Datendownload
    """
    html = requests.get("https://www.marktstammdatenregister.de/MaStR/Datendownload")
    soup = BeautifulSoup(html.text, "lxml")
    # find the download button element on the website
    element = soup.find_all("a", "btn btn-primary text-right")[0]
    # extract the url from the html element
    url = str(element).split('href="')[1].split('" title')[0]
    return url


def downloadXMLMastr(url, save_path):
    """Downloads the zipped MaStR.

    Parameters
    -----------
    url : str
        The url where the MaStR can be downloaded.
    save_path: str
        The path where the downloaded MaStR zipped folder will be saved.
    """
    print("Download has started, this can take several minutes.")
    time_a = time.perf_counter()
    r = requests.get(url, stream=True)
    with open(save_path, "wb") as zfile:
        total_length = int(7400 * 1024 * 1024)
        for chunk in progress.bar(
            r.iter_content(chunk_size=1024 * 1024),
            # chunk size of 1024 * 1024 needs 9min 11 sek = 551sek
            # chunk size of 1024 needs 9min 11 sek as well
            expected_size=(total_length / 1024 / 1024),
        ):
            if chunk:
                zfile.write(chunk)
                zfile.flush()
    time_b = time.perf_counter()
    print("Download is finished. It took %s seconds." % (time_b - time_a))

def MastrxmlToSQLite(conn,zippedXMLFilePath,includeTables,excludeTables):
    """Converts the Mastr in xml format into a sqlite database.
    """
    """Writes the local zipped MaStR to a PostgreSQL database.
        
    Parameters
    ------------
    includeTables : list, default None
        List of tables from the Marktstammdatenregister that should be written into
        the database. Elements of includeTables are lower case strings without "_" and index. 
        It is possible to include any table from the zipped local MaStR folder (see MaStR.initialize()). 
        Example: If you do want to write the data from files "AnlagenEegSolar_*.xml" to a table 
        in your database (where * is any number >=1), write the element "anlageneegsolar" into the 
        includeTables list. Elements of the list that cannot be matched to tables of the MaStR are ignored.
        If includeTables is given, only the tables listed here are written to the database.

    excludeTables : list, default None
        List of tables from the Marktstammdatenregister that should NOT be written into
        the database. Elements of excludeTables are lower case strings without "_" and index. 
        It is possible to exclude any table from the zipped local MaStR folder (see MaStR.initialize()). 
        Example: If you do not want to write the data from files "AnlagenEegSolar_*.xml" to a table 
        in your database (where * is any number >=1), write the element "anlageneegsolar" into the 
        excludeTables list. Elements of the list that cannot be matched to tables of the MaStR are ignored.

    """

    # exclude_count_reference is important for checking which files are written to the SQL database
    # if it is 1, all files from the excludeTables_reference are included to be written to the databse,
    # if it is 0, all files from the excludeTables_reference are excluded.

    if includeTables:
        exclude_count_reference = 1
        excludeTables_reference=includeTables
    elif excludeTables:
        exclude_count_reference = 0
        excludeTables_reference=excludeTables
    else:
        exclude_count_reference = 0
        excludeTables_reference=[]


    with ZipFile(self.save_zip_path, "r") as f:
        for name in f.namelist():
            # sql tablename is the beginning of the filename without the number in lowercase
            sql_tablename = name.split("_")[0].split(".")[0].lower()
            
            # check whether the table exists with current data and append new data or whether to overwrite the existing table
            

            exclude_count = excludeTables_reference.count(sql_tablename)
            if exclude_count == exclude_count_reference:
                
                if (
                name.split(".")[0].split("_")[-1] == "1"
                or len(name.split(".")[0].split("_")) == 1
                ):
                    if_exists = "replace"
                    print("New table %s is created in the PostgreSQL database." %sql_tablename)
                    index_for_printed_message=1
                else:
                    if_exists = "append"
                    print(f"File {index_for_printed_message} from {sql_tablename} is parsed.")
                    index_for_printed_message+=1
                
                data = f.read(name)
                save_path_metadata = os.path.join(expanduser("~"),".mastrsql","metadata")
                try:
                    df = pd.read_xml(data, encoding="UTF-16", compression="zip")
                    df, sql_dtype_dict = correction_of_metadata(
                        df, sql_tablename
                    )

                except lxml.etree.XMLSyntaxError as err:
                    df = handle_xml_syntax_error(data, err)
                    df, sql_dtype_dict = correction_of_metadata(
                        df, sql_tablename, save_path_metadata
                    )
                continueloop = True

                """Some files introduce new columns for existing tables. 
                If this happens, the error from writing entries into non-existing columns is caught and the column is created."""
                while continueloop:
                    try:
                        df.to_sql(
                            sql_tablename,
                            self.engine,
                            if_exists=if_exists,
                            dtype=sql_dtype_dict,
                        )
                        continueloop = False
                    except sqlalchemy.exc.ProgrammingError as err:
                        missing_column = str(err).split("«")[0].split("»")[1]
                        con = psycopg2.connect(
                            "dbname=mastrsql user=postgres password='postgres'"
                        )
                        cursor = con.cursor()
                        execute_message = 'ALTER TABLE %s ADD "%s" text NULL;' % (
                            sql_tablename,
                            missing_column,
                        )
                        cursor.execute(execute_message)
                        con.commit()
                        cursor.close()
                        con.close()
                    except sqlalchemy.exc.DataError as err:
                        delete_entry = str(err).split("«")[0].split("»")[1]
                        print(f"The entry {delete_entry} was deleteted due to its false data type.")
                        df = df.replace(delete_entry, np.nan)


    pass


def correction_of_metadata(df, sql_tablename):
    """Changes data types of Dataframe columns according to predefined metadata.

    Parameters
    -----------
    df : pandas.DataFrame
        DataFrame of MaStR tables read from xml files
    sql_tablename : str
        Name of the table in the PostrgreSQL database

    Returns
    --------
    df : pandas.DataFrame
        DataFrame of MaStR with corrected metadata
    sql_dtype_dict : dict
        Dictionary of column name / data type pairs that are needed
        for the sql dump of the DataFrame
    """

    transform_dtypes_sql = {
        "str": sqlalchemy.types.Text,
        "int": sqlalchemy.types.Integer,
        "float": sqlalchemy.types.Float,
        "datetime64": sqlalchemy.types.DateTime,
        "datetime64[D]": sqlalchemy.types.Date,
    }

    dtype_dict = metadata_dict[sql_tablename]
    # The loaded dtype_dict usually specifies more columns than there are
    # columns in the df. We have to create a dtype_dict_for_df that only
    # has keys that are columns of the list.
    dtype_dict_for_df = {}
    sql_dtype_dict = {}
    for column in df.columns:
        dtype_dict_for_df[column] = dtype_dict[column]
        sql_dtype_dict[column] = transform_dtypes_sql[dtype_dict[column]]
    try:
        df = df.astype(dtype_dict_for_df)
    except pd._libs.tslibs.np_datetime.OutOfBoundsDatetime:
        # Some datetimes ly outside the interval of 677-09-21 and
        # 2262-04-11, which causes an error.
        # With pd.to_datetime, those values are set to NaT
        for key in dtype_dict_for_df:
            if (
                dtype_dict_for_df[key] == "datetime64[D]"
                or dtype_dict_for_df[key] == "datetime64"
            ):
                df[key] = pd.to_datetime(df[key], errors="coerce")
    except (pd.errors.IntCastingNaNError, dateutil.parser._parser.ParserError):
        # Errors not related to datetimes are ignored.
        # Then the column where the error occured will not be changed,
        # but all other columns change their data type
        df = df.astype(dtype_dict_for_df, errors="ignore")
    return df, sql_dtype_dict



def initialize_database(user_credentials, postgres_standard_credentials = {}):
    """Create new PostgreSQL database if it doesn't exist yet.

    Parameters
    ------------
    user_credentials : dict
        Dictionary of credentials for the database.

    postgres_standard_credentials : dict, default {}
        Dictionary of credentials for the database initially created
        when installing PostgresSQL. Possible keys are
        "dbname" default "postgres", "user" default "postgres",
        "password" default "postgres", "host" default "localhost",
        "port" default "5432". The given default values are insterted
        into the postgres_standard_credentials if they are not given
        by the user.

    """
    
    postgres_standard_credentials_temp = {
        "dbname": "postgres",
        "user": "postgres",
        "password": "postgres",
        "host": "localhost",
        "port": "5432",
        }

    postgres_standard_credentials = {**postgres_standard_credentials_temp, **postgres_standard_credentials}    

    try:
        con = psycopg2.connect(
            dbname=postgres_standard_credentials["dbname"],
            user=postgres_standard_credentials["user"],
            password=postgres_standard_credentials["password"],
            host=postgres_standard_credentials["host"],
            port=postgres_standard_credentials["port"],
        )
        con.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cursor = con.cursor()
        name_database = user_credentials["dbname"]
        cursor.execute(f"CREATE DATABASE {name_database};")
        cursor.close()
        con.close()
    except psycopg2.errors.DuplicateDatabase:
        print(f"Using existing {name_database} database in PostgreSQL.")


def handle_xml_syntax_error(data, err):
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
