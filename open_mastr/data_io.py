import os
import pandas as pd

from open_mastr.soap_api.config import get_filenames, get_data_version_dir


dtypes = {
    "Postleitzahl": str,
    "Gemeindeschluessel": str,
    "Bruttoleistung": float
}


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

