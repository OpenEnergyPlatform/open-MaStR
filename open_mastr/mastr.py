from datetime import date
import os
from os.path import expanduser
from open_mastr.xml_parser.utils import get_url_from_Mastr_website, download_xml_Mastr, convert_mastr_xml_to_sqlite
import shutil
import sqlite3


class Mastr:
    def __init__(self) -> None:
        #self._today_string = date.today().strftime("%Y%m%d")
        self._today_string = "20211130"
        self._xml_download_url = get_url_from_Mastr_website()
        self._xml_folder_path = os.path.join(
            expanduser("~"), ".open-MaStR", "data", "xml_download"
        )

        self._zipped_xml_file_path = os.path.join(
            self._xml_folder_path, "Gesamtdatenexport_%s.zip" % self._today_string
        )
        self._sqlite_folder_path=os.path.join(
            expanduser("~"), ".open-MaStR", "data", "sqlite"
        )
        
        os.makedirs(self._sqlite_folder_path, exist_ok=True)
        self._bulk_sql_connection = sqlite3.connect(os.path.join(self._sqlite_folder_path,'bulksqlite.db'))


    def download(self, method="bulk") -> None:
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
            if os.path.exists(self._zipped_xml_file_path):
                print("MaStR already downloaded.")

            else:
                shutil.rmtree(self._xml_folder_path,ignore_errors=True)
                os.makedirs(self._xml_folder_path, exist_ok=True)
                print("MaStR is downloaded to %s" % self._xml_folder_path)
                download_xml_Mastr(self._xml_download_url, self._zipped_xml_file_path)

            #convert_mastr_xml_to_sqlite(con=self._bulk_sql_connection)

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
