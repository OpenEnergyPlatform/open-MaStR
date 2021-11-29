from datetime import date
import os
from os.path import expanduser
from xml_parser.utils import get_url

class Mastr():
    def __init__(self) -> None:
        """
        
        
        """
        self.today = date.today().strftime("%Y%m%d")
        self.url = get_url()
        self.save_path = os.path.join(
            expanduser("~"),
            ".open-MaStR",
            "data",
            "xml_download"
        )



        pass

    def download(method="bulk") -> None:
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
        if method=="bulk":

            pass

    def to_docker():
        """
        
        - check whether docker is installed, if it can be initialized or already exists
        - transfer the database into a docker container postgres database
        """

    def postprocess(method = "all"):
        """
        if method == all, all postprocessing functions are run. 
        Otherwise single functions can be selected manually (?)
        The functions themselfs are collected in the postprocessing folder.
        """

        pass
