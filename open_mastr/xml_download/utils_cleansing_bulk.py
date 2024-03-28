import pandas as pd
import numpy as np
from open_mastr.xml_download.colums_to_replace import (
    system_catalog,
    columns_replace_list,
)
from zipfile import ZipFile


def cleanse_bulk_data(df: pd.DataFrame, zipped_xml_file_path: str) -> pd.DataFrame:
    print("Data is cleansed.")
    df = replace_ids_with_names(df, system_catalog)
    # Katalogeintraege: int -> string value
    df = replace_mastr_katalogeintraege(
        zipped_xml_file_path=zipped_xml_file_path, df=df
    )
    return df


def replace_ids_with_names(df: pd.DataFrame, system_catalog: dict) -> pd.DataFrame:
    """Replaces ids with names according to the system catalog. This is
    necessary since the data from the bulk download encodes columns with
    IDs instead of the actual values."""
    for column_name, name_mapping_dictionary in system_catalog.items():
        if column_name in df.columns:
            df[column_name] = df[column_name].replace(name_mapping_dictionary)
    return df


def replace_mastr_katalogeintraege(
    zipped_xml_file_path: str,
    df: pd.DataFrame,
) -> pd.DataFrame:
    """Replaces the IDs from the mastr database by its mapped string values from
    the table katalogwerte"""
    katalogwerte = create_katalogwerte_from_bulk_download(zipped_xml_file_path)
    for column_name in df.columns:
        if column_name in columns_replace_list:
            if df[column_name].dtype == "O":
                # Handle comma seperated strings from catalog values
                df[column_name] = (
                    df[column_name]
                    .str.split(",", expand=True)
                    .apply(lambda x: x.str.strip())
                    .replace("", None)
                    .astype("Int64")
                    .map(katalogwerte.get)
                    .agg(lambda d: ",".join(i for i in d if isinstance(i, str)), axis=1)
                    .replace("", None)
                )
            else:
                df[column_name] = (
                    df[column_name].astype("float").astype("Int64").map(katalogwerte)
                )

    return df


def create_katalogwerte_from_bulk_download(zipped_xml_file_path) -> dict:
    """Creates a dictionary from the id -> value mapping defined in the table
    katalogwerte from MaStR."""
    with ZipFile(zipped_xml_file_path, "r") as f:
        data = f.read("Katalogwerte.xml")
        df_katalogwerte = pd.read_xml(data, encoding="UTF-16", compression="zip")
    katalogwerte_array = np.array(df_katalogwerte[["Id", "Wert"]])
    katalogwerte = dict(
        (katalogwerte_array[n][0], katalogwerte_array[n][1])
        for n in range(len(katalogwerte_array))
    )
    return katalogwerte
