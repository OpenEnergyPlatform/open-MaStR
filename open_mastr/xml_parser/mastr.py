import numpy as np
import os
from os.path import expanduser
from zipfile import ZipFile
import sqlalchemy
from sqlalchemy import create_engine
import pandas as pd
import pdb
import psycopg2
import lxml
import shutil
from mastrsql.utils import (
    get_url,
    download_from_url,
    correction_of_metadata,
    handle_xml_syntax_error,
    initialize_database,
)
from datetime import date


class Mastr:
    """Mirrors the MaStR (Marktstammdatenregister) to a PostrgreSQL data base.

    Parameters
    ------------
    user_credentials : dict, default {}
        Dictionary of credentials for the MaStR-database. Possible keys are
        "dbname", "user", "password", "host", "port". Key-Value pairs that are not
        explicitly given will be set to 
        "dbname" : "mastrsql",
        "user": "postgres",
        "password": "postgres",
        "host": "localhost",
        "port": "5432"

    postgres_standard_credentials : dict, default {}
        Dictionary of credentials for the first log in to postgres. Possible keys are
        "dbname", "user", "password", "host", "port". Key-Value pairs that are not
        explicitly given will be set to 
        "dbname" : "postgres",
        "user": "postgres",
        "password": "postgres",
        "host": "localhost",
        "port": "5432"

    Returns
    --------
    out : mastrsql.mastr.Mastr
        An Mastr object that represents the MaStR database.
    """


    def __init__(self, user_credentials={}, postgres_standard_credentials={}):
        assert type(user_credentials) == dict, "Variable user_credentials has to be a python dictionary!"
        
        #self.today = date.today().strftime("%Y%m%d")
        self.today = "20211026"
        self.url = get_url()
        self.save_path = os.path.join(
            expanduser("~"),
            ".mastrsql",
            "data",
        )

        standard_credentials = {
        "dbname": "mastrsql",
        "user": "postgres",
        "password": "postgres",
        "host": "localhost",
        "port": "5432",
        }
        self.user_credentials = {**standard_credentials, **user_credentials}
    
    
        self.save_zip_path = os.path.join(
            self.save_path, "Gesamtdatenexport_%s.zip" % self.today
        )

        initialize_database(self.user_credentials,postgres_standard_credentials=postgres_standard_credentials)
        create_engine_string=f"postgresql+psycopg2://postgres:{self.user_credentials['user']}@{self.user_credentials['host']}:{self.user_credentials['port']}/{self.user_credentials['dbname']}"
        self.engine = create_engine(create_engine_string)

    def download(self, delete_old=True):
        """Downloads the latest MaStR zipped file to HOME/.mastrsql/data
        
        Parameters
        ------------
        delete_old : boolean, default True
            If delete_old is True, older versions of the downloaded zipped
            MaStR will be deleted if found in self.save_zip_path
        """
        if os.path.exists(self.save_zip_path):
            print("MaStR already downloaded.")
        else:
            if delete_old:
                shutil.rmtree(self.save_path)
            print("MaStR is downloaded to %s" % self.save_path)
            os.makedirs(self.save_path, exist_ok=True)
            # download data from url
            download_from_url(self.url, self.save_zip_path)

    def to_sql(self,include_tables=None,exclude_tables=None):
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

        # exclude_count_reference is important for checking which files are written to the SQL database
        # if it is 1, all files from the exclude_tables_reference are included to be written to the databse,
        # if it is 0, all files from the exclude_tables_reference are excluded.

        if include_tables:
            exclude_count_reference = 1
            exclude_tables_reference=include_tables
        elif exclude_tables:
            exclude_count_reference = 0
            exclude_tables_reference=exclude_tables
        else:
            exclude_count_reference = 0
            exclude_tables_reference=[]


        with ZipFile(self.save_zip_path, "r") as f:
            for name in f.namelist():
                # sql tablename is the beginning of the filename without the number in lowercase
                sql_tablename = name.split("_")[0].split(".")[0].lower()
                
                # check whether the table exists with current data and append new data or whether to overwrite the existing table
                

                exclude_count = exclude_tables_reference.count(sql_tablename)
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



if __name__ == "__main__":
    database = Mastr()
    #database.download()
    exclude_tables=["anlageneegsolar", "einheitensolar", "lokationen", "marktakteure", "netzanschlusspunkte"]
    database.to_sql(exclude_tables=exclude_tables)
