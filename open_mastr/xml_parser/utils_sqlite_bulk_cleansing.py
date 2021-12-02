import pathlib
from collections import ChainMap
from typing import List, Dict, Tuple
from defusedxml.ElementTree import parse


def cleansing_sqlite_database_from_bulkdownload(con):
    """The cleansing of the bulk download data consists of the following parts:
    - replace the katalogeintraege
    """
    replace_mastr_katalogeintraege(con)



def replace_mastr_katalogeintraege(con):
    catalog = make_catalog_from_mastr_xml_files()

def make_catalog_from_mastr_xml_files():
    CATALOG_MAPPING = {
    "NACEGruppeAbschnitt": "HauptwirtdschaftszweigAbschnitt",
    "NACEGruppeAbteilung": "HauptwirtdschaftszweigAbteilung",
    "NACEGruppe": "HauptwirtdschaftszweigGruppe",
    "RegisterGericht": "Registergericht",
    }  # Achtung: Falsche Schreibweise liegt am MaStR-Datensatz!
    CATALOG_MISSING = {"Marktfunktion": {"2": "Anlagenbetreiber"}}
    catalog = ChainMap(
        {
            CATALOG_MAPPING.get(category, category): get_values_for_category(
                category_id
            )
            for category, category_id in get_categories()
        },
        CATALOG_MISSING,
    )

def get_categories():
    categories_file = pathlib.Path(MASTR_FOLDER) / "Katalogkategorien.xml"
    if not (categories_file.exists()):
        raise FileNotFoundError(
            f"Kann die Datei '{categories_file}' aus dem MaStR-Datensatz nicht finden."
        )
    tree = parse(categories_file)
    root = tree.getroot()
    for category in root:
        attributes = {attribute.tag: attribute.text for attribute in category}
        yield attributes["Name"], attributes["Id"]


def get_values_for_category(category_id,):
    values_file = pathlib.Path(MASTR_FOLDER) / "Katalogwerte.xml"
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
            values[attributes["Id"]] = attributes["Wert"]
    return values
