from datetime import date
from dateutil.parser import isoparse
import dateutil
import os
import shutil
import sqlite3
from zipfile import ZipFile
import numpy as np
import open_mastr.settings as settings

# import xml dependencies
from os.path import expanduser
from open_mastr.xml_download.utils_download_bulk import (
    download_xml_Mastr,
)
from open_mastr.xml_download.utils_write_sqlite import convert_mastr_xml_to_sqlite
from open_mastr.xml_download.utils_sqlite_bulk_cleansing import (
    cleansing_sqlite_database_from_bulkdownload,
)

# import soap_API dependencies
from open_mastr.soap_api.mirror import MaStRMirror

# import initialize_database dependencies
from open_mastr.utils.helpers import db_engine
import open_mastr.orm as orm
from sqlalchemy.schema import CreateSchema


class Mastr:
    """
    Mirror the MaStR database and keep it up-to-date.

    A sqlite database is used to mirror the MaStR database. It can be filled with
    data either from the MaStR-bulk download or from the MaStR-API.

    .. code-block:: python

       from open_mastr import Mastr

       db = Mastr()
       db.download()
    """

    def __init__(self, empty_schema=False) -> None:

        # Define the paths for the zipped xml download and the sql databases
        self._xml_folder_path = os.path.join(
            expanduser("~"), ".open-MaStR", "data", "xml_download"
        )
        self._sqlite_folder_path = os.path.join(
            expanduser("~"), ".open-MaStR", "data", "sqlite"
        )
        os.makedirs(self._xml_folder_path, exist_ok=True)
        os.makedirs(self._sqlite_folder_path, exist_ok=True)

        # setup database engine and connection
        self.DB_ENGINE = os.environ.get("DB_ENGINE", "sqlite")
        self._engine = db_engine()
        SQLITE_DATABASE_PATH = settings.SQLITE_DATABASE_PATH
        self._sql_connection = sqlite3.connect(SQLITE_DATABASE_PATH)

        # Initialize database structure
        self.empty_schema = empty_schema
        self._initialize_database(empty_schema)

    def download(
        self,
        method="bulk",
        bulk_date_string="today",
        bulk_cleansing=True,
        processes=None,
        technology=None,
        limit=None,
        api_date=None,
        chunksize=None,
        data_types=None,
        location_types=None,
        initialize_db=None,
    ) -> None:
        # TODO: To increase clarity discuss whether to rename API-related arguments with
        # TODO: Group variables in dicts for API and BULK
        # prefix "api", i.e. api_processes instead of processes; api_limit instead of limit; ...
        """
        Download the MaStR either via the bulk download or via the MaStR API and write it to a
        sqlite database.
        Parameters
        ----------
        method: {"bulk", "API"}
            Determines whether the data is downloaded via the zipped bulk download or via the
            MaStR API. The latter requires an account from marktstammdatenregister.de,
            (see :ref:`configuration <Configuration>`).

        """
        if method != "bulk" and method != "API":
            raise Exception("method has to be either 'bulk' or 'API'.")
        if method == "bulk":

            # Find the name of the zipped xml folder
            if bulk_date_string == "today":
                bulk_date_folder_extension = date.today().strftime("%Y%m%d")
            else:
                try:
                    bulk_date_folder_extension = isoparse(bulk_date_string).strftime(
                        "%Y%m%d"
                    )
                except dateutil.parser.ParserError:
                    print("date_string has to be a proper date in the format yyyymmdd.")
                    raise

            _zipped_xml_file_path = os.path.join(
                self._xml_folder_path,
                f"Gesamtdatenexport_{bulk_date_folder_extension}.zip",
            )

            if os.path.exists(_zipped_xml_file_path):
                print("MaStR already downloaded.")
            else:
                if bulk_date_string != "today":
                    raise Exception(
                        "There exists no file for given date. MaStR can only be downloaded "
                        "from the website if today's date is given."
                    )
                shutil.rmtree(self._xml_folder_path, ignore_errors=True)
                os.makedirs(self._xml_folder_path, exist_ok=True)
                download_xml_Mastr(_zipped_xml_file_path)
                print(f"MaStR was successfully downloaded to {self._xml_folder_path}.")

            # Map technology input to the .xml file names like 'einheitensolar'
            bulk_include_tables = orm.technology_to_include_tables(technology)

            convert_mastr_xml_to_sqlite(
                con=self._sql_connection,
                engine=self._engine,
                zipped_xml_file_path=_zipped_xml_file_path,
                include_tables=bulk_include_tables,
            )
            if bulk_cleansing:
                print("Data cleansing started.")
                cleansing_sqlite_database_from_bulkdownload(
                    con=self._sql_connection,
                    engine=self._engine,
                    include_tables=bulk_include_tables,
                    zipped_xml_file_path=_zipped_xml_file_path,
                )
                print("Data cleansing done successfully.")

        if method == "API":
            print(
                f"Downloading with soap_API.\n\n   -- Settings --  \nunits after date: "
                f"{api_date}\nunit download limit per technology: "
                f"{limit}\nparallel_processes: {processes}\nchunksize: "
                f"{chunksize}\ntechnologies: {technology}\ndata_types: "
                f"{data_types}\nlocation_types: {location_types}"
            )
            mastr_mirror = MaStRMirror(
                empty_schema=False,
                parallel_processes=processes,
                initialize_db=initialize_db,
                restore_dump=None,
            )
            # Download basic unit data
            mastr_mirror.backfill_basic(technology, limit=limit, date=api_date)

            # Download additional unit data
            for tech in technology:
                # mastr_mirror.create_additional_data_requests(tech)
                for data_type in data_types:
                    mastr_mirror.retrieve_additional_data(
                        tech, data_type, chunksize=chunksize, limit=limit
                    )

            # Download basic location data
            mastr_mirror.backfill_locations_basic(limit=limit, date="latest")

            # Download extended location data
            for location_type in location_types:
                mastr_mirror.retrieve_additional_location_data(
                    location_type, limit=limit
                )

            return

    def _initialize_database(self, empty_schema) -> None:
        engine = self._engine
        with engine.connect().execution_options(autocommit=True) as con:
            if empty_schema:
                con.execute(
                    f"DROP SCHEMA IF EXISTS {orm.Base.metadata.schema} CASCADE;"
                )
            # con.dialect.has_schema(con, {orm.Base.metadata.schema})
            # con.execute('CREATE SCHEMA IF NOT EXISTS (?);', (orm.Base.metadata.schema))
            if self.DB_ENGINE == "docker":
                engine.execute(CreateSchema(orm.Base.metadata.schema))
        orm.Base.metadata.create_all(engine)
