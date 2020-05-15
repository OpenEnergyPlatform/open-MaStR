"""
Read and write user config file for data base credentials

The config file storing the MaStR user name is located in
`~/.open-MaStR/config.ini`.

SPDX-License-Identifier: AGPL-3.0-or-later
"""

__copyright__ = "© Reiner Lemoine Institut"
__license__ = "GNU Affero General Public License Version 3 (AGPL-3.0)"
__url__ = "https://www.gnu.org/licenses/agpl-3.0.en.html"
__author__ = "gplssm"
__issue__ = "https://github.com/OpenEnergyPlatform/examples/issues/83"
__version__ = "v0.9.0"

import os
import configparser as cp
from open_mastr.utils.tools import open_mastr_home, create_open_mastr_dir
import keyring
import getpass

import logging
log = logging.getLogger(__name__)

config_file = os.path.join(open_mastr_home, 'config.ini')


def _load_config_file():
    cfg = cp.ConfigParser()

    if not os.path.isdir(open_mastr_home):
        create_open_mastr_dir(open_mastr_home)

    if os.path.isfile(config_file):
        # return cfg.read(config_file)
        cfg.read(config_file)
        return cfg
    else:
        with open(config_file, 'w') as configfile:
            cfg.write(configfile)
        return cfg


def get_mastr_user():
    """
    Read MaStR user name (MaStR Nummer/ MaStR Akteursnummer) from config file

    If no user name is available or the config file doesn't exist, it's
    created ad-hoc and the user is prompted for input.

    Returns
    -------
    str : MaStR user name (MaStR Nummer/ MaStR Akteursnummer)
    """
    cfg = _load_config_file()

    try:
        user = cfg.get("MaStR", "user")
    except (cp.NoSectionError, cp.NoOptionError):
        user = input('Cannot not find a MaStR user name in {config_file}.\n\n'
                     'Please enter MaStR-ID (pattern: SOM123456789012): '
                     ''.format(config_file=config_file))
        cfg["MaStR"] = {"user": user}
        with open(config_file, 'w') as configfile:
            cfg.write(configfile)
    return user


def get_mastr_token(user):
    """
    Get MaStR token of an associated role from keyring

    If the token/password is not available in the keyring, the user is asked
    to provide the token which is then stored in the keyring

    Parameters
    ----------
    user : str
        MaStR user name (MaStR Nummer/ MaStR Akteursnummer)

    Returns
    -------
    str : Token (password)
    """

    keyring.get_keyring()

    password = keyring.get_password("MaStR", user)
    if not password:
        password = getpass.getpass(
            "No token found for MaStR with user {}\n\n"
            "Please enter a valid access token of a role (Benutzerrolle) "
            "associated to the above user.\n"
            "The token might look like: "
            "koo5eixeiQuoi'w8deighai8ahsh1Ha3eib3coqu7ceeg%ies...\n"
        )
        keyring.set_password("MaStR", user, password)

    return password
