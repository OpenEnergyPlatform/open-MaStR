import pathlib
from collections import ChainMap
import sqlite3
from typing import List, Dict, Tuple
from defusedxml.ElementTree import parse
from zipfile import ZipFile
import pandas as pd
import pdb
import re
import numpy as np
from open_mastr.xml_parser.colums_to_replace import columns_replace_list

from pandas.core.frame import DataFrame


def cleansing_sqlite_database_from_bulkdownload(
    con: sqlite3.Connection,
    include_tables: list,
) -> None:
    """The cleansing of the bulk download data consists of the following parts:
    - replace the katalogeintraege
    """

    for sql_tablename in include_tables:
        replace_mastr_katalogeintraege(
            con, sql_tablename,
        )


def replace_mastr_katalogeintraege(
    con: sqlite3.Connection,
    sql_tablename: str,
) -> None:
    katalogwerte = create_katalogwerte_from_sqlite(con)
    pattern = re.compile(r"cleansed|katalog")
    if not re.search(pattern, sql_tablename):
        replace_katalogeintraege_in_single_table(con, sql_tablename, katalogwerte, columns_replace_list)


def create_katalogwerte_from_sqlite(con):
     df_katalogwerte = pd.read_sql("SELECT * from katalogwerte", con)
     katalogwerte_array = np.array(df_katalogwerte[["Id","Wert"]])
     katalogwerte = dict((katalogwerte_array[n][0],katalogwerte_array[n][1]) for n in range(len(katalogwerte_array)))
     return katalogwerte


def replace_katalogeintraege_in_single_table(
    con: sqlite3.Connection, table_name: str, katalogwerte: np.ndarray, columns_replace_list: list
) -> None:
    df = pd.read_sql(f"SELECT * FROM {table_name};", con)
    for column_name in df.columns:
        if column_name in columns_replace_list:
            df[column_name] = df[column_name].astype("Int64").map(katalogwerte)
    new_tablename = table_name + "_cleansed"
    df.to_sql(new_tablename,con, index=False, if_exists="replace")
    print(f"Data in table {table_name} was sucessfully cleansed.")



#def replace_katalogeintraege_in_single_table(
#    con: sqlite3.Connection, table_name: str, catalog: dict, df_katalogwerte: pd.DataFrame
#) -> None:
#    df = pd.read_sql(f"SELECT * FROM {table_name};", con)
#    for column_name in df.columns:
#        if column_name in catalog.keys():
#            df[column_name] = df[column_name].astype("Int64").map(catalog[column_name])
#    cleansed_table_name = table_name + "_cleansed"
#    df.to_sql(cleansed_table_name, con, index=False, if_exists="replace")
#    print(f"Data in table {table_name} was sucessfully cleansed.")

"""
def make_catalog_from_mastr_xml_files(
    zipped_xml_file_path: str, xml_folder_path: str
) -> dict:
    CATALOG_MAPPING = {
        "NACEGruppeAbschnitt": "HauptwirtdschaftszweigAbschnitt",
        "NACEGruppeAbteilung": "HauptwirtdschaftszweigAbteilung",
        "NACEGruppe": "HauptwirtdschaftszweigGruppe",
        "RegisterGericht": "Registergericht",
    }  # Achtung: Falsche Schreibweise liegt am MaStR-Datensatz!
    CATALOG_MISSING = {"Marktfunktion": {2: "Anlagenbetreiber"}}
    catalog = dict(
        {
            CATALOG_MAPPING.get(category, category): get_values_for_category(
                category_id, zipped_xml_file_path, xml_folder_path
            )
            for category, category_id in get_categories(
                zipped_xml_file_path, xml_folder_path
            )
        }
    )
    catalog = catalog | CATALOG_MISSING

    return catalog


def get_categories(zipped_xml_file_path: str, xml_folder_path: str) -> Tuple[str, int]:
    with ZipFile(zipped_xml_file_path, "r") as f:
        for file_name in f.namelist():
            if file_name == "Katalogkategorien.xml":
                categories_extracted_filepath = f.extract(
                    file_name, path=xml_folder_path
                )

                if not categories_extracted_filepath:
                    raise FileNotFoundError(
                        f"Kann die Datei '{file_name}' aus dem MaStR-Datensatz nicht finden."
                    )
                tree = parse(categories_extracted_filepath)
                root = tree.getroot()
                for category in root:
                    attributes = {
                        attribute.tag: attribute.text for attribute in category
                    }
                    yield attributes["Name"], attributes["Id"]


def get_values_for_category(
    category_id: int, zipped_xml_file_path: str, xml_folder_path: str
) -> dict:
    with ZipFile(zipped_xml_file_path, "r") as f:
        for file_name in f.namelist():
            if file_name == "Katalogwerte.xml":
                values_extracted_filepath = f.extract(file_name, path=xml_folder_path)
    if not values_extracted_filepath:
        raise FileNotFoundError(
            f"Kann die Datei '{file_name}' aus dem MaStR-Datensatz nicht finden."
        )
    tree = parse(values_extracted_filepath)
    root = tree.getroot()
    values = {}
    for category in root:
        attributes = {attribute.tag: attribute.text for attribute in category}
        if attributes["KatalogKategorieId"] == category_id:
            values[int(attributes["Id"])] = attributes["Wert"]
    return values
"""