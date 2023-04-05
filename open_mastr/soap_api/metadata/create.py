import csv
import datetime
import json
import os
import uuid

from open_mastr.soap_api.metadata.description import DataDescription
from open_mastr.utils.config import get_data_config, get_filenames, column_renaming
from open_mastr.soap_api.download import MaStRDownload


# TODO: We should not describe the data in both metadata folder and orm.py


def datapackag_base(reference_date, publication_date=None, statistik_flag=None):
    """
    Create datapackage metadata base information

    Includes all information about the data except for key `resources` which describes the actual data files and its
    columns.

    Parameters
    ----------
    reference_date: datetime.datetime
        Reference date for data
    publication_date: datetime.datetime
        Date of data being published. Defaults to `datetime.datetime.now()`, the time when this metadata is being
        created.
    statistik_flag: str or None
        Describe if filtering is applied during CSV export of data. Read in
        :meth:`~.open_mastr.soap_api.mirror.MaStRMirror.to_csv()` for more details.

    Returns
    -------
    dict
        Datapackage metadata
    """

    if not publication_date:
        publication_date = datetime.datetime.now()

    publication_date = publication_date.strftime("%Y-%m-%d %H:%M:%S")
    data_version = get_data_config()

    # Add a note to the description if this is filtered data or if it the complete data including potential duplicates
    if statistik_flag == "B":
        description_extra = (
            "The original MaStR data is filtered by `StatistikFlag == 'B'`. It includes data that "
            "was migrated to the Martstammdatenregister + newly registered units with commissioning "
            "date after 31.01.2019 Thus, it is suitable for "
            "statistical analysis as is does not contain duplicates.\n"
        )
    elif statistik_flag == "A":
        description_extra = (
            "The original MaStR data is filtered by `StatistikFlag == 'A'`. "
            "It includes newly registered units with commissioning date before 31.01.2019. "
            "Data may contain duplicates.\n"
        )
    else:
        description_extra = "All data from the Marktstammdatenregister is included. There are duplicates included.\n"
    description_extra += (
        "For further information read in the documentation of the original data source: "
        "https://www.marktstammdatenregister.de/MaStRHilfe/subpages/statistik.html"
    )

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


def create_datapackage_meta_json(
    reference_date,
    technologies=None,
    data=["raw"],
    statistik_flag=None,
    json_serialize=True,
):
    """
    Create a frictionless data conform metadata description

    Parameters
    ----------
    reference_date: datetime.datetime
        Reference date for data
    technologies: list
        Only consider specified technologies in metadata JSON string. If not provided, metadata is created for data
        of all technologies.
    data: list, optional
        Specify which data should be documented by this metadata.
    statistik_flag: str or None
        Describe if filtering is applied during CSV export of data. Read in
        :meth:`~.open_mastr.soap_api.mirror.MaStRMirror.to_csv()` for more details.
    json_serialize: bool
        Toggle between returning a JSON string or a dict

        * True: return JSON string
        * False: return dict

    Returns
    -------
    dict or str
        Set `json_serialize` to False, for returning a dict instead of JSON str.
    """

    datapackage_base_dict = datapackag_base(
        reference_date, statistik_flag=statistik_flag
    )

    table_columns = DataDescription().functions_data_documentation()
    mastr_dl = MaStRDownload()

    # Filter specified technologies
    if technologies:
        unit_data_specs = {
            k: v for k, v in mastr_dl._unit_data_specs.items() if k in technologies
        }

    filenames = get_filenames()

    renaming = column_renaming()

    resources_meta = {"resources": []}

    # Add resources of raw data files
    for tech, specs in unit_data_specs.items():
        raw_fields = []
        specs["basic_data"] = "GetListeAlleEinheiten"

        for data_type in [
            "basic_data",
            "unit_data",
            "eeg_data",
            "kwk_data",
            "permit_data",
        ]:
            if data_type in specs.keys():
                for name, specification in table_columns[specs[data_type]].items():
                    if name in renaming[data_type]["columns"]:
                        name = f"{name}_{renaming[data_type]['suffix']}"
                    raw_fields.append({"name": name, **specification, "unit": None})

        if "raw" in data:
            resource = {
                "profile": "tabular-data-resource",
                "name": f"bnetza_mastr_{tech}_raw",
                "title": f"open-MaStR {tech} units (raw)",
                "path": filenames["raw"][tech]["joined"],
                "scheme": "file",
                "encoding": "utf-8",
                "mediatype": "text/csv",
                "schema": {
                    "fields": raw_fields,
                    "primaryKey": ["EinheitMastrNummer"],
                },
                "dialect": {"delimiter": ","},
            }

            resources_meta["resources"].append(resource)
        if "cleaned" in data:
            resource = {
                "profile": "tabular-data-resource",
                "name": f"bnetza_mastr_{tech}_cleaned",
                "title": f"open-MaStR {tech} units (cleaned)",
                "path": filenames["cleaned"][tech],
                "scheme": "file",
                "encoding": "utf-8",
                "mediatype": "text/csv",
                "schema": {
                    "fields": raw_fields,
                    "primaryKey": ["EinheitMastrNummer"],
                },
                "dialect": {"delimiter": ","},
            }

            resources_meta["resources"].append(resource)
        if "postprocessed" in data:
            processed_fields = [
                {
                    "name": "geom",
                    "unit": None,
                    "type": "str",
                    "desciption": "Standort der Anlage als Punktgeometrie im WKB Format",
                    "examples": "0101000020e610000071fbe59315131c40a2b437f8c20e4a40",
                },
                {
                    "name": "comment",
                    "unit": None,
                    "type": "str",
                    "desciption": "Information about data post-processing",
                    "examples": "has_geom; outside_vg250",
                },
            ]
            if tech == "wind":
                processed_fields.append(
                    {
                        "name": "tags",
                        "unit": None,
                        "type": "json",
                        "desciption": "Data insights and report about post-processing steps",
                        "examples": {
                            "plz_check": False,
                            "processed": True,
                            "inside_germany": True,
                        },
                    }
                )
                processed_fields.append(
                    {
                        "name": "geom",
                        "unit": None,
                        "type": "str",
                        "desciption": "Standort der Anlage als Punktgeometrie im WKB Format (EPSG 3035)",
                        "examples": "0101000020e610000071fbe59315131c40a2b437f8c20e4a40",
                    }
                )
            resource = {
                "profile": "tabular-data-resource",
                "name": f"bnetza_mastr_{tech}",
                "title": f"open-MaStR {tech} units",
                "path": filenames["postprocessed"][tech],
                "scheme": "file",
                "encoding": "utf-8",
                "mediatype": "text/csv",
                "schema": {
                    "fields": raw_fields + processed_fields,
                    "primaryKey": ["EinheitMastrNummer"],
                },
                "dialect": {"delimiter": ","},
            }

            resources_meta["resources"].append(resource)

    datapackage_dict = {**datapackage_base_dict, **resources_meta}

    if json_serialize:
        return json.dumps(datapackage_dict, ensure_ascii=False)
    else:
        return datapackage_dict


def column_docs_csv(technologies, base_path):
    metadata = create_datapackage_meta_json(
        datetime.datetime.now(), technologies, json_serialize=False
    )

    filenames = []

    # Sort filenames according to technologies
    raw_data_filenames = get_filenames()["raw"]
    metadata_resources = []

    for tech in technologies:
        for metadata_resource in metadata["resources"]:
            if metadata_resource["path"] == raw_data_filenames[tech]["joined"]:
                metadata_resources.append(metadata_resource)

    for table in metadata_resources:
        csv_header = ["Column", "Description", "Type", "Example"]

        os.makedirs(base_path, exist_ok=True)
        filename_base = table["name"] + ".csv"
        filename = os.path.join(base_path, filename_base)
        filenames.append(filename_base)
        with open(filename, "w", encoding="utf-8", newline="") as fh:
            fh_writer = csv.writer(fh)
            fh_writer.writerow(csv_header)
            for column in table["schema"]["fields"]:
                csv_row = [
                    column["name"],
                    column["description"],
                    column["type"],
                    column["example"],
                ]
                fh_writer.writerow(csv_row)

    return filenames
