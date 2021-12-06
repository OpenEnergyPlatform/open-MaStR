import pathlib
from collections import ChainMap
from typing import List, Dict, Tuple
from defusedxml.ElementTree import parse
from zipfile import ZipFile
import pandas as pd
import pdb


def cleansing_sqlite_database_from_bulkdownload(con,zipped_xml_file_path):
    """The cleansing of the bulk download data consists of the following parts:
    - replace the katalogeintraege
    """
    replace_mastr_katalogeintraege(con,zipped_xml_file_path)



def replace_mastr_katalogeintraege(con,zipped_xml_file_path):
    catalog = make_catalog_from_mastr_xml_files(zipped_xml_file_path)
    execute_message = "SELECT name FROM sqlite_master WHERE TYPE='table';"
    cursor = con.cursor()
    cursor.execute(execute_message)
    tables_list=cursor.fetchall()
    for table_name_tuple in tables_list:
        table_name = table_name_tuple[0]
        if not table_name in ['katalogwerte','katalogkategorien']:
            replace_katalogeintraege_in_single_table(table_name,catalog)
            

def replace_katalogeintraege_in_single_table(con,table_name,catalog):
    """This still seems to have errors."""
    df = pd.read_sql(table_name,con)
    for column_name in df.columns:
        if column_name in catalog.keys():
            df[column_name]=df[column_name].astype('Int64').map(catalog[column_name])
    
    return df




def make_catalog_from_mastr_xml_files(zipped_xml_file_path):
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
                category_id,
                zipped_xml_file_path
            )
            for category, category_id in get_categories(zipped_xml_file_path)
        })
    catalog = catalog | CATALOG_MISSING

    return catalog

def get_categories(zipped_xml_file_path):
    with ZipFile(zipped_xml_file_path, "r") as f:
        for file_name in f.namelist():
            if file_name=="Katalogkategorien.xml":
                categories_file = f.read(file_name)
                if not categories_file:
                    raise FileNotFoundError(
                        f"Kann die Datei '{categories_file}' aus dem MaStR-Datensatz nicht finden."
                    )
                pdb.set_trace()
                tree = parse(categories_file)
                root = tree.getroot()
                for category in root:
                    attributes = {attribute.tag: attribute.text for attribute in category}
                    yield attributes["Name"], attributes["Id"]


def get_values_for_category(category_id,zipped_xml_file_path):
    with ZipFile(zipped_xml_file_path, "r") as f:
        for file_name in f.namelist():
            if file_name=="Katalogwerte.xml":
                values_file = f.read(file_name)
    if not (values_file.exists()):
        raise FileNotFoundError(
            f"Kann die Datei '{values_file}' aus dem MaStR-Datensatz nicht finden."
        )
    tree = parse(values_file)
    root = tree.getroot()
    values = {}
    for category in root:
        attributes = {attribute.tag: attribute.text for attribute in category}
        if attributes["KatalogKategorieId"] == category_id:
            values[int(attributes["Id"])] = attributes["Wert"]
    return values
