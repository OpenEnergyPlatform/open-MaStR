import datetime
import uuid

from open_mastr.soap_api.metadata.description import DataDescription
from open_mastr.soap_api.config import get_data_config, get_filenames
from open_mastr.soap_api.download import MaStRDownload


def datapackag_base(reference_date, publication_date=None, statistik_flag=None):

    if not publication_date:
        publication_date = datetime.datetime.now()

    publication_date = publication_date.strftime("%Y-%m-%d %H:%M:%S")
    data_version = get_data_config()["data_version"]

    # Add a note to the description if this is filtered data or if it the complete data including potential duplicates
    if statistik_flag == "B":
        description_extra = "The original MaStR data is filtered by `StatistikFlag == 'B'`. It includes data that " \
                            "was migrated to the Martstammdatenregister + newly registered units with commissioning "\
                            "date after 31.01.2019 Thus, it is suitable for " \
                            "statistical analysis as is does not contain duplicates.\n"
    elif statistik_flag == "A":
        description_extra = "The original MaStR data is filtered by `StatistikFlag == 'A'`. "\
                            "It includes newly registered units with commissioning date before 31.01.2019. " \
                            "Data may contain duplicates.\n"
    else:
        description_extra = "All data from the Marktstammdatenregister is included. There are duplicates included.\n"
    description_extra += "For further information read in the documentation of the original data source: " \
                         "https://www.marktstammdatenregister.de/MaStRHilfe/subpages/statistik.html"

    datapackage_meta = {
        "name": "open-mastr_raw",
        "title": "open-MaStR power unit registry",
        "id": str(uuid.uuid4()),
        "description": f"Raw data download Marktstammdatenregister (MaStR) data using the webservice.\n\n{description_extra}",
        "language": ["en-GB", "de-DE"],
        "keywords": ["powerplants", "renewables"],
        "created": publication_date,
        "version": data_version,
        "context": {
            "homepage": "https://www.marktstammdatenregister.de/MaStR/",
            "documentation": "https://www.marktstammdatenregister.de/MaStRHilfe/index.html",
            "sourceCode": None,
            "contact": "https://www.marktstammdatenregister.de/MaStR/Startseite/Kontakt",
            "grantNo": None,
            "fundingAgency": "Bundesnetzagentur für Elektrizität, Gas, Telekommunikation, Post und Eisenbahnen",
            "fundingAgencyLogo": "https://www.bundesnetzagentur.de/DE/Home/home_node.html",
            "publisherLogo": "https://www.bundesnetzagentur.de/DE/Sachgebiete/ElektrizitaetundGas/Unternehmen_Institutionen/MaStR/MaStR_logo_long.svg?__blob=normal&v=4",
        },
        "spatial": {"location": None, "extent": "Germany", "resolution": "vector"},
        "temporal": {
            "referenceDate": reference_date.strftime("%Y-%m-%d %H:%M:%S"),
            "timeseries": {
                "start": None,
                "end": None,
                "resolution": None,
                "alignment": None,
                "aggregationType": None,
            },
        },
        "sources": [
            {
                "title": "Bundesnetzagentur - Marktstammdatenregister",
                "description": "Das Marktstammdatenregister ist das Register für den deutschen Strom- und Gasmarkt. Es wird MaStR abgekürzt. Im MaStR sind vor allem die Stammdaten zu Strom- und Gaserzeugungsanlagen zu registrieren. Außerdem sind die Stammdaten von Marktakteuren wie Anlagenbetreibern, Netzbetreibern und Energielieferanten zu registrieren. Das MaStR wird von der Bundesnetzagentur geführt.",
                "path": "https://www.marktstammdatenregister.de/MaStR/",
                "licenses": [
                    {
                        "name": "dl-de/by-2-0",
                        "title": "Datenlizenz Deutschland – Namensnennung – Version 2.0",
                        "path": "https://www.govdata.de/dl-de/by-2-0",
                        "instruction": "You are free: To Share, To Create, To Adapt; As long as you: Attribute",
                        "attribution": f"© Marktstammdatenregister {datetime.date.today().year} | dl-de/by-2-0",
                    }
                ],
            },
            {
                "title": "RLI - open_MaStR",
                "description": "Scripts to download, process and publish the MaStR data set.",
                "path": "https://github.com/OpenEnergyPlatform/open-MaStR",
                "licenses": [
                    {
                        "name": "AGPL-3.0",
                        "title": "GNU Affero General Public License v3.0",
                        "path": "https://www.gnu.org/licenses/agpl-3.0.en.html",
                        "instruction": "You are free: To Share, To Create, To Adapt; As long as you: Attribute, Share-Alike, Keep open!",
                        "attribution": "open_MaStR © Reiner Lemoine Institut | AGPL-3.0",
                    }
                ],
            },
        ],
        "licenses": [
            {
                "name": "dl-de/by-2-0",
                "title": "Datenlizenz Deutschland – Namensnennung – Version 2.0",
                "path": "https://www.govdata.de/dl-de/by-2-0",
                "instruction": "Die bereitgestellten Daten und Metadaten dürfen für die kommerzielle und nicht kommerzielle Nutzung verwendet werden. Bei der Nutzung ist sicherzustellen, dass Angaben als Quellenvermerk enthalten sind.",
                "attribution": "© Marktstammdatenregister 2019 | dl-de/by-2-0",
            }
        ],
        "contributors": [
            {
                "title": "Ludee",
                "email": None,
                "path": "https://github.com/ludee",
                "role": "maintainer",
                "organization": "Reiner Lemoine Institut gGmbH",
            },
            {
                "title": "Guido Pleßmann",
                "email": None,
                "path": "https://gplssm.de",
                "role": "maintainer",
                "organization": "Reiner Lemoine Institut gGmbH",
            },
            {
                "title": "oakca",
                "email": None,
                "path": "https://github.com/oakca",
                "role": "contributor",
                "organization": "Reiner Lemoine Institut gGmbH",
            },
        ],
        "review": {"path": None, "badge": None},
        "metaMetadata": {
            "metadataVersion": "OEP-1.4.0",
            "metadataLicense": {
                "name": "CC0-1.0",
                "title": "Creative Commons Zero v1.0 Universal",
                "path": "https://creativecommons.org/publicdomain/zero/1.0/",
            },
        },
        "_comment": {
            "metadata": "Metadata documentation and explanation (https://github.com/OpenEnergyPlatform/organisation/wiki/metadata)",
            "dates": "Dates and time must follow the ISO8601 including time zone (YYYY-MM-DD or YYYY-MM-DDThh:mm:ss±hh)",
            "units": "Use a space between numbers and units (100 m)",
            "languages": "Languages must follow the IETF (BCP47) format (en-GB, en-US, de-DE)",
            "licenses": "License name must follow the SPDX License List (https://spdx.org/licenses/)",
            "review": "Following the OEP Data Review (https://github.com/OpenEnergyPlatform/data-preprocessing/wiki)",
            "null": "If not applicable use (null)",
        },
    }

    return datapackage_meta


def datapackage_meta_json(reference_date, statistik_flag=None):
    datapackage_base_dict = datapackag_base(reference_date, statistik_flag=statistik_flag)

    table_columns = DataDescription().functions_data_documentation()
    mastr_dl = MaStRDownload()

    unit_data_specs = {
        k: v
        for k, v in mastr_dl._unit_data_specs.items()
        if k
        not in ["consumer"] + [f"gas_{t}" for t in ["consumer", "producer", "storage"]]
    }

    filenames = get_filenames()

    resources_meta = {"resources": []}
    for tech, specs in unit_data_specs.items():
        fields = []

        for data_type in ["unit_data", "eeg_data", "kwk_data", "permit_data"]:
            if data_type in specs.keys():
                for name, specification in table_columns[specs[data_type]].items():
                    fields.append({"name": name, **specification, "unit": None})

        resource = {
            "profile": "tabular-data-resource",
            "name": f"open-mastr_{tech}_raw",
            "title": f"open-MaStR {tech} units (raw)",
            "path": filenames["raw"][tech]["joined"],
            "format": "csv",
            "encoding": "UTF-8",
            "mediatype": "text/csv",
            "schema": {
                "fields": fields,
                "primaryKey": ["EinheitMastrNummer"],
                "foreignKeys": [
                    {
                        "fields": None,
                        "reference": {"resource": None, "fields": None},
                    }
                ],
            },
            "dialect": {"delimiter": ",", "decimalSeparator": "."},
        }

        resources_meta["resources"].append(resource)

    datapackage_dict = {**datapackage_base_dict, **resources_meta}

    return datapackage_dict

