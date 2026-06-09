import datetime
import uuid

from open_mastr.utils.config import get_data_config


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
