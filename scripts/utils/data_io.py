import datetime
import json
import os
import pandas as pd
import pathlib
import pynodo

from open_mastr.utils.config import (
    get_filenames,
    get_data_version_dir,
)
from open_mastr.soap_api.metadata.create import create_datapackage_meta_json
from open_mastr.utils.constants import TECHNOLOGIES
from open_mastr.utils.credentials import get_zenodo_token


dtypes = {"Postleitzahl": str, "Gemeindeschluessel": str, "Bruttoleistung": float}


def filter(df):
    """
    Filter unit data based on characters of index labels

    Data is filtered by unit category. Only directly in MaStR entered data is considered; migrated data is filtered out.
    # TODO: Link to data description about migrated, duplicates update process

    Parameters
    ----------
    df. pandas.DataFrame
        Unit data

    Returns
    -------
    pandas.DataFrame
        Filtered dataframe without migrated data
    """
    return df[df.index.str.match("^[S,G][E,S,V][E,L]")]


def convert_datetime(row):
    """
    String to datetime conversion considering different string formats

    Parameters
    ----------
    row: str
        String to convert or pandas.DataFrame cell

    Returns
    -------
    pd.Timestamp
        Pandas timestamp is returned which bases on datetime
    """
    try:
        datetime_date = pd.to_datetime(row, format="%Y-%m-%d %H:%M:%S.%f%z")
    except ValueError:
        datetime_date = pd.to_datetime(row, format="%Y-%m-%d %H:%M:%S", errors="coerce")
    return datetime_date


def read_csv_data(data_stage):
    """Read raw open-MaStR data from project home dir

    Parameters
    ----------
    data_stage: str
        Select which data is loaded from CSV files

        * "raw": Raw MaStR data is read
        * "cleaned": Cleaned MaStR data is read

    """

    data_dir = get_data_version_dir()
    filenames = get_filenames()

    data = {}
    for technology, filename in filenames[data_stage].items():
        if technology in TECHNOLOGIES:
            if data_stage == "raw":
                filename = filename["joined"]
            data_file = os.path.join(data_dir, filename)
            if os.path.isfile(data_file):
                data[technology] = pd.read_csv(
                    data_file,
                    parse_dates=["Inbetriebnahmedatum", "DatumLetzteAktualisierung"],
                    index_col="EinheitMastrNummer",
                    sep=",",
                    date_parser=convert_datetime,
                    dtype=dtypes,
                )
            else:
                pass

    return data


def save_cleaned_data(data):
    """
    Save cleaned open-MaStR to CSV files in desired location in project home dir

    Parameters
    ----------
    data: dict of pandas.DataFrame
        Cleaned open-MaStR data in a dict keyed by technology
    """
    data_dir = get_data_version_dir()
    filenames = get_filenames()

    newest_date = datetime.datetime(1900, 1, 1, 0, 0, 0, tzinfo=datetime.timezone.utc)

    # Iterate through dict and save individual dataframes to CSV
    for tech, dat in data.items():
        data_file = os.path.join(data_dir, filenames["cleaned"][tech])
        dat.to_csv(
            data_file, index=True, index_label="EinheitMastrNummer", encoding="utf-8"
        )

        newest_date = dat["DatumLetzteAktualisierung"].max()

    # Save metadata along with data
    metadata_file = os.path.join(data_dir, filenames["metadata"])
    metadata = create_datapackage_meta_json(
        newest_date, data.keys(), data=["raw", "cleaned"], json_serialize=False
    )

    with open(metadata_file, "w", encoding="utf-8") as f:
        json.dump(metadata, f, ensure_ascii=False, indent=4)


def zenodo_upload(
    data_stages=["raw", "cleaned", "postprocessed"], zenodo_token=None, sandbox=True
):
    """
    Upload MaStR data to a new Zenodo deposit

    Metadata stored in `datapackage.json` is also uploaded.

    Parameters
    ----------
    data_stages: list, optional
        Data is available at three stages: 'raw', 'cleaned' and 'processed'. The data upload only includes data stages
        specified here. Defaults to all data.
    zenodo_token: str, optional
        Uploading to Zenodo requires authentication. Either provide your token here or store it in the credentials.cfg
        file. See also in :ref:`Zenodo token`.
    sandbox: bool
        Flag to toggle between `Zenodo production <https://www.zenodo.org/>`_ and
        `sandbox instance <https://sandbox.zenodo.org>`_. If True, the sandbox instance will be used.
        Specify False, to upload a publication ready dataset of the production instance.
    """

    data_dir = get_data_version_dir()
    filenames = get_filenames()
    metadata_file = os.path.join(data_dir, filenames["metadata"])

    if not zenodo_token:
        # If no token for Zenodo is given, read from credentials.yml
        zenodo_token = get_zenodo_token()
    # If neither a Zenodo token exists in config, raise Error
    if not zenodo_token:
        raise ValueError("No Zenodo token provided. Can't upload.")

    # Prepare metadata for Zenodo deposit
    with open(metadata_file, "r") as read_file:
        metadata = json.load(read_file)
    zenodo_required_metadata = {
        "upload_type": "dataset",
        "publication_date": metadata["created"].split(" ")[0],
        "creators": [
            {"name": contributor["title"], "affiliation": contributor["organization"]}
            for contributor in metadata["contributors"]
        ],
        "access_right": "open",
        "license": "other-at",
        "title": metadata["title"],
        "description": metadata["description"],
        "version": metadata["version"],
    }

    # Create deposit
    zen = pynodo.Depositions(access_token=zenodo_token, sandbox=sandbox)
    deposit = zen.create(data={"metadata": zenodo_required_metadata})

    # Access deposit files for uploading files
    zen_files = pynodo.DepositionFiles(
        deposition=deposit.id,
        access_token=zenodo_token,
        sandbox=True,
    )

    # Upload LICENSE file
    zen_files.upload(
        os.path.join(
            pathlib.Path(__file__).parent.absolute(), "soap_api", "metadata", "LICENSE"
        )
    )

    # Upload actual data
    for data_stage in data_stages:
        for tech, file_specs in filenames[data_stage].items():
            if data_stage == "raw" and tech in TECHNOLOGIES:
                zen_files.upload(os.path.join(data_dir, file_specs["joined"]))
            else:
                zen_files.upload(os.path.join(data_dir, file_specs))

    # Upload datapackage.json
    zen_files.upload(os.path.join(data_dir, filenames["metadata"]))


def cleaned_data(save_csv=True):
    """
    Cleanes raw data while preserving columns and its names

    Cleaning includes:

    * Removal of duplicates originating from migrated and directly entered data in MaStR
    which describes the same unit

    Parameters
    ----------
    save_csv: bool
        If :obj:`True`, cleaned data will be saved to CSV files.

    Returns
    -------
    dict
        Cleaned open-MaStR unit data
    """

    # Read raw data
    raw = read_csv_data("raw")

    # Filter data to remove duplicates originating from migrated and directly entered data
    raw_filtered = {k: filter(df) for k, df in raw.items()}

    if save_csv:
        save_cleaned_data(raw_filtered)

    return raw_filtered
