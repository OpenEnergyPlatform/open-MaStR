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
__version__ = "v0.9.0"

import os
import configparser as cp
from open_mastr.utils.tools import open_mastr_home

import logging
log = logging.getLogger(__name__) 

"""parameter"""
cfg = cp.RawConfigParser()
config_file = os.path.join(open_mastr_home, 'config.ini')
log_file = os.path.join(open_mastr_home, 'open_mastr.log')


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
    fh = logging.FileHandler(log_file)
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