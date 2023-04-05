#!/usr/bin/env python
# -*- coding: utf-8 -*-



"""
Service functions for logging

Configure console and file logging; Create and handle config file for api keys;

SPDX-License-Identifier: AGPL-3.0-or-later
"""

__copyright__ = "© Reiner Lemoine Institut"
__license__ = "GNU Affero General Public License Version 3 (AGPL-3.0)"
__url__ = "https://www.gnu.org/licenses/agpl-3.0.en.html"
__author__ = "Ludee; christian-rli"
__issue__ = "https://github.com/OpenEnergyPlatform/examples/issues/52"
__version__ = "v0.10.0"

import os
import yaml
import shutil
import pathlib
from datetime import date

import logging
import logging.config
from open_mastr.utils.constants import TECHNOLOGIES, API_LOCATION_TYPES, ADDITIONAL_TABLES


log = logging.getLogger(__name__)


def get_project_home_dir():
    """Get root dir of project data

    On linux this path equals `$HOME/.open-MaStR/`, respectively `~/.open-MaStR/`
    which is also called `PROJECTHOME`.

    Returns
    -------
    path-like object
        Absolute path to root dir of open-MaStR project home
    """

    return os.path.join(os.path.expanduser("~"), ".open-MaStR")


def get_output_dir():
    """Get output directory for csv data, xml file and database. Defaults to get_project_home_dir()

    Returns
    -------
    path-like object
        Absolute path to output path
    """

    if "OUTPUT_PATH" in os.environ:
        return os.environ.get('OUTPUT_PATH')

    return get_project_home_dir()


def get_data_version_dir():
    """
    Subdirectory of data/ in PROJECTHOME

    See :ref:`docs <Project directory>` for configuration of data version.

    Returns
    -------
    path-like object
        Absolute path to `PROJECTHOME/data/<data-version>/`
    """
    data_version = get_data_config()

    if "OUTPUT_PATH" in os.environ:
        return os.path.join(os.environ.get('OUTPUT_PATH'), "data", data_version)

    return os.path.join(get_project_home_dir(), "data", data_version)


def get_filenames():
    """
    Get file names defined in config

    Returns
    -------
    dict
        File names used in open-MaStR
    """
    with open(
        os.path.join(get_project_home_dir(), "config", "filenames.yml")
    ) as filename_fh:
        filenames = yaml.safe_load(filename_fh)

    return filenames


def get_data_config():
    """
    Get data version

    Returns
    -------
    str
        dataversion
    """

    today = date.today()

    data_config = f'dataversion-{today.strftime("%Y-%m-%d")}'

    return data_config


def create_project_home_dir():
    """Create directory structure of PROJECTHOME"""
    project_home = get_project_home_dir()

    # Create root project home path
    if not os.path.isdir(project_home):
        # Create project home
        log.info(f"Create {project_home} used for config, parameters and data.")
        os.mkdir(project_home)

    # Create project home subdirs
    subdirs = [os.path.join(project_home, s) for s in ["config", "logs", "data"]]
    for subdir in subdirs:
        if not os.path.isdir(subdir):
            os.mkdir(subdir)

    # copy default config files
    config_path = os.path.join(get_project_home_dir(), "config")
    log.info(f"I will create a default set of config files in {config_path}")

    internal_config_dir = os.path.join(
        pathlib.Path(__file__).parent.absolute(), "config"
    )
    files = ["logging.yml"]

    for file in files:
        if file not in os.listdir(config_path):
            shutil.copy(
                os.path.join(internal_config_dir, file),
                os.path.join(config_path, os.path.basename(file)),
            )


def create_data_dir():
    """
    Create direct for current data version

    The directory that is created for this fata version can
    be returned by :func:`~.get_data_version_dir`.
    """

    os.makedirs(get_data_version_dir(), exist_ok=True)


def _filenames_generator():
    """Write default file names .yml to project home dir"""

    filenames_file = os.path.join(get_project_home_dir(), "config", "filenames.yml")

    # How files are prefixed
    prefix = "bnetza_mastr"

    # Additional data available for certain technologies
    type_specific_data = {
        "eeg": ["wind", "hydro", "solar", "biomass", "gsgk", "storage"],
        "kwk": ["combustion", "biomass", "gsgk"],
        "permit": [
            "wind",
            "biomass",
            "combustion",
            "gsgk",
            "nuclear",
            "solar",
            "hydro",
        ],
    }

    # Template for file names
    filenames_template = {
        "raw": {
            "joined": "{prefix}_{technology}_raw.csv",
            "basic": "{prefix}_{technology}_basic.csv",  # power-unit
            "extended": "{prefix}_{technology}_extended.csv",  # unit
            "eeg": "{prefix}_{technology}_eeg.csv",
            "kwk": "{prefix}_{technology}_kwk.csv",
            "permit": "{prefix}_{technology}_permit.csv",
            "extended_fail": "{prefix}_{technology}_extended_fail.csv",
            "eeg_fail": "{prefix}_{technology}_eeg_fail.csv",
            "kwk_fail": "{prefix}_{technology}_kwk_fail.csv",
            "permit_fail": "{prefix}_{technology}_permit_fail.csv",
        }
    }

    filenames = {}

    # Define filenames .yml with a dict
    for section, section_filenames in filenames_template.items():
        filenames[section] = {}
        for tech in TECHNOLOGIES:

            # Files for all technologies
            files = ["joined", "basic", "extended", "extended_fail"]

            # Additional file for some technologies
            for addit_data_type, techs in type_specific_data.items():
                if tech in techs:
                    files.append(addit_data_type)
                    files.append(addit_data_type + "_fail")

                # Create filename dictionary for one technologies
                tmp = {
                    k: v.format(prefix=prefix, technology=tech)
                    for k, v in section_filenames.items()
                    if k in files
                }

                # Collect file names for all technologies
                filenames[section].update({tech: tmp})

    # Add file names of cleaned data
    filenames["cleaned"] = {
        tech: f"{prefix}_{tech}_cleaned.csv" for tech in TECHNOLOGIES
    }

    # Add file names of processed data
    filenames["postprocessed"] = {
        tech: f"{prefix}_{tech}.csv" for tech in TECHNOLOGIES
    }

    # Add filenames for location data
    filenames["raw"].update(
        {loc: f"{prefix}_{loc}_raw.csv" for loc in API_LOCATION_TYPES}
    )

    # Add filenames for additional tables
    filenames["raw"].update({"additional_table":
        {addit_table: f"{prefix}_{addit_table}_raw.csv" for addit_table in ADDITIONAL_TABLES}}
    )

    # Add metadata file
    filenames["metadata"] = "datapackage.json"

    with open(filenames_file, "w") as outfile:
        yaml.dump(filenames, outfile)
    log.info(f"File names configuration saved to {filenames_file}")


def setup_project_home():
    """Create open-MaStR project home directory structure

    Create PROJECTHOME returned by :func:`~.get_project_home_dir`.
    In addition, default config files are copied to `PROJECTHOME/config/`.
    """

    # Create directory structure of project home dir
    create_project_home_dir()

    # Save default file names
    _filenames_generator()


def setup_logger():
    """Configure logging in console and log file.

    Returns
    -------
    logging.Logger
        Logger with two handlers: console and file.
    """

    # Read logging config
    with open(
        os.path.join(get_project_home_dir(), "config", "logging.yml")
    ) as filename_fh:
        logging_config = yaml.safe_load(filename_fh)

    # Add logfile location
    logging_config["handlers"]["file"]["filename"] = os.path.join(
        get_project_home_dir(), "logs", "open_mastr.log"
    )

    logging.config.dictConfig(logging_config)
    return logging.getLogger("open-MaStR")


def column_renaming():
    """
    Column renaming for CSV export of raw data

    Helps to export duplicate columns from different data sources.

    Returns
    -------
    dict
        Suffix and column to be suffixed keyed by data type.
    """
    return {
        "basic_data": {
            "columns": ["BestandsanlageMastrNummer"],
            "suffix": "basic",
        },
        "unit_data": {
            "columns": [
                "EinheitMastrNummer",
                "EegMastrNummer",
                "KwkMastrNummer",
                "GenMastrNummer",
                "SpeMastrNummer",
                "EinheitBetriebsstatus",
                "NichtVorhandenInMigriertenEinheiten",
                "Bruttoleistung",
            ],
            "suffix": "extended",
        },
        "eeg_data": {
            "columns": ["EegMastrNummer", "DatumLetzteAktualisierung", "Meldedatum"],
            "suffix": "eeg",
        },
        "kwk_data": {
            "columns": [
                "KwkMastrNummer",
                "Meldedatum",
                "Inbetriebnahmedatum",
                "DatumLetzteAktualisierung",
                "AnlageBetriebsstatus",
                "VerknuepfteEinheiten",
                "AusschreibungZuschlag",
            ],
            "suffix": "kwk",
        },
        "permit_data": {
            "columns": ["GenMastrNummer", "DatumLetzteAktualisierung", "Meldedatum"],
            "suffix": "permit",
        },
    }
