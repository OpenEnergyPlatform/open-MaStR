from _typeshed import Self
from datetime import date
import os
from os.path import expanduser
from xml_parser.utils import getURLFromMastrWebsite, downloadXMLMastr, MastrxmlToSQLite
import shutil
import sqlite3


class Mastr:
    def __init__(self) -> None:
        """ """
        self.todayString = date.today().strftime("%Y%m%d")
        self.xmlDownloadURL = getURLFromMastrWebsite()
        self.xmlFolderPath = os.path.join(
            expanduser("~"), ".open-MaStR", "data", "xml_download"
        )

        self.zippedXMLFilePath = os.path.join(
            self.xmlFolderPath, "Gesamtdatenexport_%s.zip" % self.todayString
        )
        self.sqliteFolderPath=os.path.join(
            expanduser("~"), ".open-MaStR", "data", "sqlite"
        )
        self.BulkSQLConnection = sqlite3.connect(os.path.join(self.sqliteFolderPath,'bulksqlite.db'))


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
            if os.path.exists(self.zippedXMLFilePath):
                print("MaStR already downloaded.")

            else:
                shutil.rmtree(self.save_path)
                print("MaStR is downloaded to %s" % self.save_path)
                os.makedirs(self.save_path, exist_ok=True)
                downloadXMLMastr(self.url, self.save_zip_path)

            MastrxmlToSQLite()

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
