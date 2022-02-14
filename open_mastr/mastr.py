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

# import initialize_databse dependencies
from open_mastr.utils.helpers import db_engine
import open_mastr.orm as orm
from sqlalchemy.schema import CreateSchema


class Mastr:
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
        bulk_include_tables=None,
        bulk_exclude_tables=None,
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
        # prefix "api", i.e. api_processes instaed of processes; api_limit instead of limit; ...
        """
        method in {bulk, API}

        if method==bulk:
            - download folder
            - write to sqlite database
            - existing functions from earlier development are taken from a new bulk_download folder
            - start bulk cleansing functions



        if method==API:
            - download data via API
            - write to sqlite database
            - existing functions from earlier development are taken from soap_api folder
            - start API cleansing functions


        At the end, both methods give us a sqlite database, which should be (almost) identical.

        """
        empty_schema = self.empty_schema
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

            if bulk_include_tables and bulk_exclude_tables:
                raise Exception(
                    "Either include_tables or exclude_tables has to be None."
                )

            if os.path.exists(_zipped_xml_file_path):
                print("MaStR already downloaded.")
            else:
                shutil.rmtree(self._xml_folder_path, ignore_errors=True)
                os.makedirs(self._xml_folder_path, exist_ok=True)
                print("MaStR is downloaded to %s" % self._xml_folder_path)
                download_xml_Mastr(_zipped_xml_file_path)

            with ZipFile(_zipped_xml_file_path, "r") as f:
                full_list_of_files = [
                    entry.split(".")[0].split("_")[0].lower() for entry in f.namelist()
                ]

                full_list_of_files = list(np.unique(np.array(full_list_of_files)))
                # full_list_of_files now inlcudes all table_names in the format
                # einheitensolar instead of einheitensolar_12.xml
            if bulk_exclude_tables:
                bulk_include_tables = [
                    entry
                    for entry in full_list_of_files
                    if entry not in bulk_exclude_tables
                ]
            if not bulk_include_tables and not bulk_exclude_tables:
                bulk_include_tables = full_list_of_files

            convert_mastr_xml_to_sqlite(
                con=self._sql_connection,
                zipped_xml_file_path=_zipped_xml_file_path,
                include_tables=bulk_include_tables,
                engine=self._engine
            )
            if bulk_cleansing:
                print("Data cleansing started.")
                cleansing_sqlite_database_from_bulkdownload(
                    con=self._sql_connection,
                    include_tables=bulk_include_tables,
                    zipped_xml_file_path = _zipped_xml_file_path
                )

        if method == "API":
            print(
                f"Downloading with soap_API.\n\n   -- Settings --  \nunits after date: "
                f"{api_date}\nunit donwnload limit per technology: "
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
            # # Download basic unit data
            # mastr_mirror.backfill_basic(technology, limit=limit, date=api_date)
            #
            # # Download additional unit data
            # for tech in technology:
            #     # mastr_mirror.create_additional_data_requests(tech)
            #     for data_type in data_types:
            #         mastr_mirror.retrieve_additional_data(tech, data_type, chunksize=chunksize, limit=limit)
            #
            # # Download basic location data
            # mastr_mirror.backfill_locations_basic(
            #     limit=limit,
            #     date="latest"
            # )
            #
            # # Download extended location data
            # for location_type in location_types:
            #     mastr_mirror.retrieve_additional_location_data(location_type, limit=limit)
            #
            # return

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

    def to_docker():
        """

        - check whether docker is installed, if it can be initialized or already exists
        - transfer the database into a docker container postgres database
        """

    def postprocess(method="all"):
        """
        if method == all, all postprocessing functions are run.
        Otherwise single functions can be selected manually (?)
        The functions themselfs are collected in the postprocessing folder.
        """

        pass
