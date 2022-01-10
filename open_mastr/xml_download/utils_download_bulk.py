import requests
from clint.textui import progress
import time
from bs4 import BeautifulSoup
import pdb



def get_url_from_Mastr_website() -> str:
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


def download_xml_Mastr(url: str, save_path: str) -> None:
    """Downloads the zipped MaStR.

    Parameters
    -----------
    url : str
        The url where the MaStR can be downloaded.
    save_path: str
        The path where the downloaded MaStR zipped folder will be saved.
    """
    print(
        "Download has started, this can take several minutes. The download bar is only a rough estimate."
    )
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


'''
def correction_of_metadata(df: pd.DataFrame, sql_tablename: str) -> Tuple[pd.DataFrame, dict]:
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
'''


'''
def initialize_database(user_credentials, postgres_standard_credentials={}):
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

    postgres_standard_credentials = {
        **postgres_standard_credentials_temp,
        **postgres_standard_credentials,
    }

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
'''
