import csv
import pathlib
from collections import ChainMap
from typing import List, Dict, Tuple
from defusedxml.ElementTree import parse

MASTR_FOLDER = "mastr"
OUTPUT_FOLDER = "output"
CSV_CHUNK_SIZE = 10000

CATALOG_MAPPING = {
    "NACEGruppeAbschnitt": "HauptwirtdschaftszweigAbschnitt",
    "NACEGruppeAbteilung": "HauptwirtdschaftszweigAbteilung",
    "NACEGruppe": "HauptwirtdschaftszweigGruppe",
    "RegisterGericht": "Registergericht",
}  # Achtung: Falsche Schreibweise liegt am MaStR-Datensatz!
CATALOG_MISSING = {"Marktfunktion": {"2": "Anlagenbetreiber"}}


def main():
    folder = pathlib.Path(MASTR_FOLDER)

    for file in folder.iterdir():
        print(f"Current file: {file.name}")
        headers, data = get_data_from_xml_file(file)
        export_file(file.stem, headers, data)


def get_data_from_xml_file(
    file: pathlib.Path,
) -> Tuple[List[str], List[Dict[str, str]]]:
    tree = parse(file)
    root = tree.getroot()
    data = []
    headers = set()
    for entry in root:
        item = {attribute.tag: attribute.text for attribute in entry}
        item = apply_catalog(item)
        data.append(item)
        headers = headers.union(list(item.keys()))
    return list(headers), data


def export_file(filename: str, headers: List[str], data: List[Dict[str, str]]):
    with open(
        f"{OUTPUT_FOLDER}/{filename}.csv", "w", encoding="utf-8", newline=""
    ) as csv_file:
        csv_writer = csv.DictWriter(csv_file, headers, delimiter=";")
        csv_writer.writeheader()
        for item in data:
            csv_writer.writerow(item)


def apply_catalog(player):
    return {
        attribute: catalog[attribute].get(value, value)
        if attribute in catalog
        else value
        for attribute, value in player.items()
    }


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


def get_values_for_category(category_id):
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


if __name__ == "__main__":
    catalog = ChainMap(
        {
            CATALOG_MAPPING.get(category, category): get_values_for_category(
                category_id
            )
            for category, category_id in get_categories()
        },
        CATALOG_MISSING,
    )
    main()
