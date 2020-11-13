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
__version__ = "v0.10.0"

import os
import configparser as cp
# from open_mastr.utils.tools import open_mastr_home, create_open_mastr_dir
from open_mastr.soap_api.config import config_file
import keyring
import getpass

import logging
log = logging.getLogger(__name__)


def _load_config_file():
    cfg = cp.ConfigParser()

    # if not os.path.isdir(open_mastr_home):
    #     create_open_mastr_dir(open_mastr_home)

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

    # Try to get password from keyring
    keyring.get_keyring()
    password = keyring.get_password("MaStR", user)

    # No password stored in keyring, try config file
    if not password:
        cfg = _load_config_file()
        try:
            password = cfg.get("MaStR", "token")
        except (cp.NoSectionError, cp.NoOptionError):
            # If also no password in config file, ask the user to input password
            # Two options: (1) storing in keyring; (2) storing in config file
            password = input('Cannot not find a MaStR password, neither in keyring nor in {config_file}.\n\n'
                             "Please enter a valid access token of a role (Benutzerrolle) "
                             "associated to the user {user}.\n"
                             "The token might look like: "
                             "koo5eixeiQuoi'w8deighai8ahsh1Ha3eib3coqu7ceeg%ies...\n".format(
                config_file=config_file,
                user=user))

            # let the user decide where to store the password
            # (1) keyring
            # (2) credentials.cfg
            # (0) don't store, abort
            # Wait for correct input
            while True:
                choice = int(input("Where do you want to store your password?\n"
                                "\t(1) Keyring (default, hit ENTER to select)\n"
                                "\t(2) Config file (credendials.cfg)\n"
                                "\t(0) Abort. Don't store password\n") or "1\n")
                # check if choice is valid input
                if choice in [0, 1, 2]:
                    break

            # Do action according to input
            if choice == 0:
                pass
            elif choice == 1:
                keyring.set_password("MaStR", user, password)
            elif choice == 2:
                cfg["MaStR"] = {"user": user, "token": password}
                with open(config_file, 'w') as configfile:
                    cfg.write(configfile)
            else:
                log.error("No clue what happened here!?")

    return password