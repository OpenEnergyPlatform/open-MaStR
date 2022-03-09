import pdb
import sqlite3
from xmlrpc.client import Boolean
import pandas as pd
import re
import numpy as np
from open_mastr.xml_download.colums_to_replace import columns_replace_list
from zipfile import ZipFile
from open_mastr.orm import tablename_mapping
from open_mastr.xml_download.utils_write_sqlite import add_table_to_sqlite_database
from open_mastr.xml_download.utils_write_sqlite import date_columns_to_datetime


def cleansing_sqlite_database_from_bulkdownload(
    con: sqlite3.Connection,
    engine,
    include_tables: list,
    zipped_xml_file_path: str,
) -> None:
    """The cleansing of the bulk download data consists of the following parts:
    - replace the katalogeintraege
    """

    for xml_tablename in include_tables:
        sql_tablename = tablename_mapping[xml_tablename]["__name__"]
        replace_mastr_katalogeintraege(
            con, engine, xml_tablename, sql_tablename, zipped_xml_file_path
        )


def replace_mastr_katalogeintraege(
    con: sqlite3.Connection,
    engine,
    xml_tablename: str,
    sql_tablename: str,
    zipped_xml_file_path: str,
) -> None:
    katalogwerte = create_katalogwerte_from_sqlite(zipped_xml_file_path)
    pattern = re.compile(r"cleansed|katalog|typen")
    if not re.search(pattern, sql_tablename):
        replace_katalogeintraege_in_single_table(
            con=con,
            engine=engine,
            xml_tablename=xml_tablename,
            table_name=sql_tablename,
            katalogwerte=katalogwerte,
            columns_replace_list=columns_replace_list,
        )


def create_katalogwerte_from_sqlite(zipped_xml_file_path) -> dict:
    with ZipFile(zipped_xml_file_path, "r") as f:
        data = f.read("Katalogwerte.xml")
        df_katalogwerte = pd.read_xml(data, encoding="UTF-16", compression="zip")
    katalogwerte_array = np.array(df_katalogwerte[["Id", "Wert"]])
    katalogwerte = dict(
        (katalogwerte_array[n][0], katalogwerte_array[n][1])
        for n in range(len(katalogwerte_array))
    )
    return katalogwerte


def replace_katalogeintraege_in_single_table(
    con: sqlite3.Connection,
    engine,
    xml_tablename: str,
    table_name: str,
    katalogwerte: dict,
    columns_replace_list: list,
) -> None:
    df = pd.read_sql(f"SELECT * FROM {table_name};", con)
    for column_name in df.columns:
        if column_name in columns_replace_list:
            df[column_name] = (
                df[column_name].astype("float").astype("Int64").map(katalogwerte)
            )
    df = date_columns_to_datetime(xml_tablename, df)

    add_table_to_sqlite_database(
        df=df,
        xml_tablename=xml_tablename,
        sql_tablename=table_name,
        if_exists="replace",
        con=con,
        engine=engine,
    )
    print(f"Data in table {table_name} was successfully cleansed.")


# def replace_katalogeintraege_in_single_table(
#    con: sqlite3.Connection, table_name: str, catalog: dict, df_katalogwerte: pd.DataFrame
# ) -> None:
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
