import datetime
import json
import os
import pandas as pd

from open_mastr.soap_api.config import get_filenames, get_data_version_dir
from open_mastr.soap_api.metadata.create import datapackage_meta_json

dtypes = {
    "Postleitzahl": str,
    "Gemeindeschluessel": str,
    "Bruttoleistung": float
}


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
        datetime_date = pd.to_datetime(row, format='%Y-%m-%d %H:%M:%S.%f%z')
    except ValueError:
        datetime_date = pd.to_datetime(row, format='%Y-%m-%d %H:%M:%S', errors='coerce')
    return datetime_date


def read_raw_data():
    """Read raw open-MaStR data from project home dir"""

    data_dir = get_data_version_dir()
    filenames = get_filenames()

    raw_data = {}
    for technology, filename in filenames["raw"].items():
        data_file = os.path.join(data_dir, filename["joined"])
        if os.path.isfile(data_file):
            raw_data[technology] = pd.read_csv(data_file,
                                               parse_dates=["Inbetriebnahmedatum",
                                                            "DatumLetzteAktualisierung"],
                                               index_col="EinheitMastrNummer",
                                               sep=",",
                                               date_parser=convert_datetime,
                                               dtype=dtypes
                                               )

    return raw_data


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
        dat.to_csv(data_file, index=True, index_label="EinheitMastrNummer", encoding='utf-8')

        if dat["DatumLetzteAktualisierung"].max() > newest_date:
            newest_date = dat["DatumLetzteAktualisierung"].max()

    # Save metadata along with data
    metadata_file = os.path.join(data_dir, filenames["metadata"])
    metadata = datapackage_meta_json(newest_date, data.keys(), data=["raw", "cleaned"], json_serialize=False)

    with open(metadata_file, 'w', encoding='utf-8') as f:
        json.dump(metadata, f, ensure_ascii=False, indent=4)
