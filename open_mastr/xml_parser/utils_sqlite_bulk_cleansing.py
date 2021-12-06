import pathlib
from collections import ChainMap
from typing import List, Dict, Tuple
from defusedxml.ElementTree import parse
from zipfile import ZipFile
import pandas as pd
import pdb
import re


def cleansing_sqlite_database_from_bulkdownload(
    con, zipped_xml_file_path, xml_folder_path
):
    """The cleansing of the bulk download data consists of the following parts:
    - replace the katalogeintraege
    """
    replace_mastr_katalogeintraege(con, zipped_xml_file_path, xml_folder_path)


def replace_mastr_katalogeintraege(con, zipped_xml_file_path, xml_folder_path):
    catalog = make_catalog_from_mastr_xml_files(zipped_xml_file_path, xml_folder_path)
    execute_message = "SELECT name FROM sqlite_master WHERE TYPE='table';"
    cursor = con.cursor()
    cursor.execute(execute_message)
    tables_list = cursor.fetchall()
    pattern = re.compile(r'cleansed|katalog')
    for table_name_tuple in tables_list:
        table_name = table_name_tuple[0]
        if not re.search(pattern,table_name):
            replace_katalogeintraege_in_single_table(con, table_name, catalog)


def replace_katalogeintraege_in_single_table(con, table_name, catalog):
    """This still seems to have errors."""
    df = pd.read_sql(f"SELECT * FROM {table_name};", con)
    for column_name in df.columns:
        if column_name in catalog.keys():
            df[column_name] = df[column_name].astype("Int64").map(catalog[column_name])
    cleansed_table_name=table_name + "_cleansed"
    df.to_sql(cleansed_table_name,con,index=False,if_exists='replace')
    print(f"Data in table {table_name} was sucessfully cleansed.")


def make_catalog_from_mastr_xml_files(zipped_xml_file_path, xml_folder_path):
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


def get_categories(zipped_xml_file_path, xml_folder_path):
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


def get_values_for_category(category_id, zipped_xml_file_path,xml_folder_path):
    with ZipFile(zipped_xml_file_path, "r") as f:
        for file_name in f.namelist():
            if file_name == "Katalogwerte.xml":
                values_extracted_filepath = f.extract(
                    file_name, path=xml_folder_path
                )
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
