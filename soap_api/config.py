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

import logging
log = logging.getLogger(__name__)


cfg = cp.RawConfigParser()

_loaded = False


def get_project_home_dir():
    """Get root dir of project data"""

    project_home = os.path.join(os.path.expanduser('~'), ".open-MaStR")

    return project_home


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
    # config_path = os.path.join(root_path, get('user_dirs', 'config_dir'))
    # log.info(f'I will create a default set of config files in {config_path}')
    # internal_config_dir = os.path.join(package_path, 'config')
    # for file in glob(os.path.join(internal_config_dir, '*.cfg')):
    #     shutil.copy(file,
    #                 os.path.join(config_path,
    #                              os.path.basename(file)
    #                              .replace('_default', '')))


def get_power_unit_types():
    return ["wind", "hydro", "solar", "biomass", "combustion", "nuclear", "gsgk", "storage"]


def _filenames_generator():

    # How files are prefixed
    prefix = "bnetza_mastr"

    # Additional data available for certain technologies
    type_specific_data = {
        "eeg": ["wind", "hydro", "solar", "biomass", "gsgk", "storage"],
        "kwk": ["combustion"],
        "permit": ["wind"]
    }

    # Template for file names
    filenames_template = {
        "raw": {
            "joined": "{prefix}_{technology}_raw",
            "basic": "{prefix}_{technology}_basic", # power-unit
            "extended": "{prefix}_{technology}_extended", # unit
            "eeg": "{prefix}_{technology}_eeg",
            "kwk": "{prefix}_{technology}_kwk",
            "permit": "{prefix}_{technology}_permit",
            "extended_fail": "{prefix}_{technology}_extended_fail",
            "eeg_fail": "{prefix}_{technology}_eeg_fail",
            "kwk_fail": "{prefix}_{technology}_kwk_fail",
            "permit_fail": "{prefix}_{technology}_permit_fail",
        }
    }

    filenames = {}

    # Define filenames .yml with a dict
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
            k: v.format(prefix=prefix, technology=tech) for k, v in filenames_template["raw"].items() if k in files}

        # Collect file names for all technologies
        filenames.update({tech: tmp})

    filenames_file = os.path.join(get_project_home_dir(), "config", "filenames.yml")

    with open(filenames_file, 'w') as outfile:
        yaml.dump(filenames, outfile)


def setup_project_home():
    create_project_home_dir()

    _filenames_generator()


def setup_logger():
    """Configure logging in console and log file.
    
    Returns
    -------
    rl : logger
        Logging in console (ch) and file (fh).
    """

    rl = logging.getLogger()
    rl.setLevel(logging.INFO)
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


def config_section_set(config_section, key, value):
    """Create a config file.

    Sets input values to a [db_section] key - pair.

    Parameters
    ----------
    config_section : str
        Section in config file.
    key : str
        The username.
    value : str
        The pw.
    """

    with open(config_file, 'w') as config:  # save
        if not cfg.has_section(config_section):
            cfg.add_section(config_section)
            cfg.set(config_section, 'token', key)
            cfg.set(config_section, 'user', value)
            cfg.write(config)


def config_file_load():
    """Load the username and pw from config file."""

    if os.path.isfile(config_file):
        config_file_init()
    else:
        config_file_not_found_message()


def config_file_init():
    """Read config file."""

    try:
        # print('Load ' + config_file)
        cfg.read(config_file)
        global _loaded
        _loaded = True
    except FileNotFoundError:
        config_file_not_found_message()


def config_file_get(config_section, key):
    """Read data from config file.

    Parameters
    ----------
    config_section : str
        Section in config file.
    key : str
        Config entries.
    """

    if not _loaded:
        config_file_init()
    try:
        return cfg.getfloat(config_section, key)
    except Exception:
        try:
            return cfg.getint(config_section, key)
        except:
            try:
                return cfg.getboolean(config_section, key)
            except:
                return cfg.get(config_section, key)


def config_file_not_found_message():
    """Show error message if file not found."""

    print(f'The config file "{config_file}" could not be found')


# Define variable to be imported in other files
config_file = os.path.join(get_project_home_dir(), 'config', 'credentials.cfg')