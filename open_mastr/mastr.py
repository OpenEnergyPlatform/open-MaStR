from datetime import date
from dateutil.parser import isoparse
import dateutil
import os
import shutil
import sqlite3
from zipfile import ZipFile
import numpy as np

# import xml dependencies
from os.path import expanduser
from open_mastr.xml_download.utils_download_bulk import (
    get_url_from_Mastr_website,
    download_xml_Mastr,
)
from open_mastr.xml_download.utils_write_sqlite import convert_mastr_xml_to_sqlite
from open_mastr.xml_download.utils_sqlite_bulk_cleansing import (
    cleansing_sqlite_database_from_bulkdownload,
)

# import soap_API dependencies
from open_mastr.soap_api.mirror import MaStRMirror


class Mastr:
    def __init__(self, date_string="today") -> None:
        # TODO: Should parameter date_string be defined here or later in the download function?
        if date_string == "today":
            self._today_string = date.today().strftime("%Y%m%d")
        else:
            try:
                self._today_string = isoparse(date_string).strftime("%Y%m%d")
            except dateutil.parser.ParserError:
                print("date_string has to be a proper date in the format yyyymmdd.")
                raise

        self._xml_download_url = get_url_from_Mastr_website()
        self._xml_folder_path = os.path.join(
            expanduser("~"), ".open-MaStR", "data", "xml_download"
        )

        self._zipped_xml_file_path = os.path.join(
            self._xml_folder_path, "Gesamtdatenexport_%s.zip" % self._today_string
        )
        self._sqlite_folder_path = os.path.join(
            expanduser("~"), ".open-MaStR", "data", "sqlite"
        )
        os.makedirs(self._xml_folder_path, exist_ok=True)
        os.makedirs(self._sqlite_folder_path, exist_ok=True)
        self._bulk_sql_connection = sqlite3.connect(
            os.path.join(self._sqlite_folder_path, "bulksqlite.db")
        )

    def download(
        self,
        method=None,
        include_tables=None,
        exclude_tables=None,
        cleansing=True,
        processes=None,
        technology=None,
        limit=None,
        date=None,
        chunksize=None,
        data_types=None,
        location_types=None,
        initialize_db=None,
        empty_schema=None,
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
        if method == "bulk":
            if include_tables and exclude_tables:
                raise Exception(
                    "Either include_tables or exclude_tables has to be None."
                )

            if os.path.exists(self._zipped_xml_file_path):
                print("MaStR already downloaded.")
            else:
                shutil.rmtree(self._xml_folder_path, ignore_errors=True)
                os.makedirs(self._xml_folder_path, exist_ok=True)
                print("MaStR is downloaded to %s" % self._xml_folder_path)
                download_xml_Mastr(self._xml_download_url, self._zipped_xml_file_path)

            with ZipFile(self._zipped_xml_file_path, "r") as f:
                full_list_of_files = [
                    entry.split(".")[0].split("_")[0].lower() for entry in f.namelist()
                ]

                full_list_of_files = list(np.unique(np.array(full_list_of_files)))
                # full_list_of_files now inlcudes all table_names in the format
                # einheitensolar instead of einheitensolar_12.xml
            if exclude_tables:
                include_tables = [
                    entry for entry in full_list_of_files if entry not in exclude_tables
                ]
            if not include_tables and not exclude_tables:
                include_tables = full_list_of_files

            convert_mastr_xml_to_sqlite(
                con=self._bulk_sql_connection,
                zipped_xml_file_path=self._zipped_xml_file_path,
                include_tables=include_tables,
            )
            if cleansing:
                cleansing_sqlite_database_from_bulkdownload(
                    con=self._bulk_sql_connection,
                    include_tables=include_tables,
                )

        if method == "API":
            print(
                f"Downloading with soap_API.\n\n   -- Settings --  \nunits after date: {date}\nunit donwnload limit per technology: {limit}\nparallel_processes: {processes}\nchunksize: {chunksize}\ntechnologies: {technology}\ndata_types: {data_types}\nlocation_types: {location_types}"
            )
            mastr_mirror = MaStRMirror(
                empty_schema=False,
                parallel_processes=processes,
                initialize_db=initialize_db,
                restore_dump=None,
            )
            # # Download basic unit data
            # mastr_mirror.backfill_basic(technology, limit=limit, date=date)
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
