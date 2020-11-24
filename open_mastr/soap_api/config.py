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
import configparser as cp
import yaml
import shutil
import pathlib

import logging
log = logging.getLogger(__name__)


def get_project_home_dir():
    """Get root dir of project data"""

    project_home = os.path.join(os.path.expanduser('~'), ".open-MaStR")

    return project_home


def get_data_version_dir():
    data_version = get_data_config()["data_version"]
    data_path = os.path.join(get_project_home_dir(), "data", data_version)
    return data_path


def get_filenames():
    """
    Get file names define in config

    Returns
    -------
    dict
        File names used in open-MaStR
    """
    with open(os.path.join(get_project_home_dir(), "config", "filenames.yml")) as filename_fh:
        filenames = yaml.safe_load(filename_fh)

    return filenames


def get_data_config():
    """
    Get data version and more parameters

    Returns
    -------
    dict
        File names used in open-MaStR
    """
    with open(os.path.join(get_project_home_dir(), "config", "data.yml")) as data_fh:
        data_config = yaml.safe_load(data_fh)

    return data_config


def get_db_tables():
    with open(os.path.join(get_project_home_dir(), "config", "tables.yml")) as data_fh:
        db_tables = yaml.safe_load(data_fh)

    return db_tables


def create_project_home_dir():
    """Create data root path, if necessary"""
    project_home = get_project_home_dir()

    # Create root project home path
    if not os.path.isdir(project_home):
        # Create project home
        log.info('Create {} used for config, parameters and data.'.format(project_home))
        os.mkdir(project_home)

    # Create project home subdirs
    subdirs = [os.path.join(project_home, s) for s in ['config', 'logs', 'data']]
    for subdir in subdirs:
        if not os.path.isdir(subdir):
            os.mkdir(subdir)

    # copy default config files
    config_path = os.path.join(get_project_home_dir(), "config")
    log.info(f'I will create a default set of config files in {config_path}')

    internal_config_dir = os.path.join(pathlib.Path(__file__).parent.absolute(), 'config')
    files = ["data.yml", "tables.yml"]

    for file in files:
        if not file in os.listdir(config_path):
            shutil.copy(os.path.join(internal_config_dir, file),
                        os.path.join(config_path,
                                     os.path.basename(file)))


def create_data_dir():
    os.makedirs(get_data_version_dir(), exist_ok=True)


def get_power_unit_types():
    return ["wind", "hydro", "solar", "biomass", "combustion", "nuclear", "gsgk", "storage"]


def _filenames_generator():
    """Write default file names .yml to project home dir"""

    filenames_file = os.path.join(get_project_home_dir(), "config", "filenames.yml")

    if not os.path.isfile(filenames_file):

        # How files are prefixed
        prefix = "bnetza_mastr"

        # Additional data available for certain technologies
        type_specific_data = {
            "eeg": ["wind", "hydro", "solar", "biomass", "gsgk", "storage"],
            "kwk": ["combustion", "biomass", "gsgk"],
            "permit": ["wind", "biomass", "combustion", "gsgk", "nuclear", "solar", "hydro"]
        }

        # Template for file names
        filenames_template = {
            "raw": {
                "joined": "{prefix}_{technology}_raw.csv",
                "basic": "{prefix}_{technology}_basic.csv", # power-unit
                "extended": "{prefix}_{technology}_extended.csv", # unit
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
            for tech in get_power_unit_types():

                # Files for all technologies
                files = ["joined", "basic", "extended", "extended_fail"]

                # Additional file for some technologies
                for t, techs in type_specific_data.items():
                    if tech in techs:
                        files.append(t)
                        files.append(t + "_fail")

                # Create filename dictionary for one technologies
                    tmp = {
                        k: v.format(prefix=prefix, technology=tech) for k, v in section_filenames.items() if k in files}

                    # Collect file names for all technologies
                    filenames[section].update({tech: tmp})

        with open(filenames_file, 'w') as outfile:
            yaml.dump(filenames, outfile)
        log.info("File names configuration saved to {}".format(filenames_file))


def setup_project_home():
    """Create open-MaStR home directory structure and save default config files"""

    # Create directory structure of project home dir
    create_project_home_dir()

    # Save default file names
    _filenames_generator()


def setup_logger(log_level=logging.INFO):
    """Configure logging in console and log file.
    
    Returns
    -------
    rl : logger
        Logging in console (ch) and file (fh).
    """

    rl = logging.getLogger()
    rl.setLevel(log_level)
    rl.propagate = False

    # set format
    formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s',
                                  datefmt='%Y-%m-%d %H:%M:%S')

    # console handler (ch)
    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)
    ch.setFormatter(formatter)

    # file handler (fh)
    fh = logging.FileHandler(os.path.join(get_project_home_dir(), "logs", 'open_mastr.log'))
    fh.setLevel(logging.INFO)
    fh.setFormatter(formatter)

    rl.handlers = [ch, fh]

    return rl