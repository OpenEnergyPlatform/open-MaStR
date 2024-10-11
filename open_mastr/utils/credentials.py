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
from open_mastr.utils.config import get_project_home_dir
import keyring

import logging

log = logging.getLogger(__name__)


def _load_config_file():

    config_file = os.path.join(get_project_home_dir(), "config", "credentials.cfg")
    cfg = cp.ConfigParser()

    # if not os.path.isdir(open_mastr_home):
    #     create_open_mastr_dir(open_mastr_home)

    if os.path.isfile(config_file):
        cfg.read(config_file)
        return cfg
    else:
        with open(config_file, "w") as configfile:
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
    section = "MaStR"
    cfg_path = os.path.join(get_project_home_dir(), "config", "credentials.cfg")

    try:
        user = cfg.get(section, "user")
        return user
    except (cp.NoSectionError, cp.NoOptionError):
        # Actually, an error should be raised here. But this conflicts
        # with automatic calls from MaStRDownloadFactory
        # except cp.NoSectionError:
        #     raise cp.Error(f"Section {section} not found in {cfg_path}")
        # except cp.NoOptionError:
        #     raise cp.Error(f"The option 'user' could not by found in the section "
        #                    f"{section} in file {cfg_path}.")
        log.warning(
            f"The option 'user' could not by found in the section "
            f"{section} in file {cfg_path}. "
            f"You might run into trouble when downloading data via the MaStR API."
            f"\n Bulk download works without option 'user'."
        )
        return None


def check_and_set_mastr_user():
    """Checks if MaStR user is stored, otherwise asks for it."""

    user = get_mastr_user()

    if not user:
        credentials_file = os.path.join(
            get_project_home_dir(), "config", "credentials.cfg"
        )
        cfg = _load_config_file()

        user = input(
            "\n\nCannot not find a MaStR user name in {config_file}.\n\n"
            "Please enter MaStR-ID (pattern: SOM123456789012): "
            "".format(config_file=credentials_file)
        )
        cfg["MaStR"] = {"user": user}

        with open(credentials_file, "w") as configfile:
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
    # Retrieving password from keyring does currently fail on headless systems
    # Prevent from breaking program execution with following try/except clause
    section = "MaStR"
    cfg_path = os.path.join(get_project_home_dir(), "config", "credentials.cfg")
    try:
        password = keyring.get_password(section, user)
    except:
        password = None

    # No password stored in keyring, try config file
    if not password:
        cfg = _load_config_file()
        try:
            password = cfg.get(section, "token")
        except (cp.NoSectionError, cp.NoOptionError):
            log.warning(
                f"The option 'token' could not by found in the section "
                f"{section} in file {cfg_path}. "
                f"You might run into trouble when downloading data via the MaStR API."
                f"\n Bulk download works without option 'token'."
            )
            password = None
    return password


def check_and_set_mastr_token(user):
    """Checks if MaStR token is stored, otherwise asks for it."""

    password = get_mastr_token(user)

    if not password:
        cfg = _load_config_file()
        credentials_file = os.path.join(
            get_project_home_dir(), "config", "credentials.cfg"
        )

        # If also no password in credentials file, ask the user to input password
        # Two options: (1) storing in keyring; (2) storing in config file
        password = input(
            "\n\nCannot not find a MaStR password, neither in keyring nor in {config_file}.\n\n"
            "Please enter a valid access token of a role (Benutzerrolle) "
            "associated to the user {user}.\n"
            "The token might look like: "
            "koo5eixeiQuoi'w8deighai8ahsh1Ha3eib3coqu7ceeg%ies...\n".format(
                config_file=credentials_file, user=user
            )
        )

        # let the user decide where to store the password
        # (1) keyring
        # (2) credentials.cfg
        # (0) don't store, abort
        # Wait for correct input
        while True:
            choice = int(
                input(
                    "Where do you want to store your password?\n"
                    "\t(1) Keyring (default, hit ENTER to select)\n"
                    "\t(2) Config file (credendials.cfg)\n"
                    "\t(0) Abort. Don't store password\n"
                )
                or "1\n"
            )
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
            with open(credentials_file, "w") as configfile:
                cfg.write(configfile)
        else:
            log.error("No clue what happened here!?")

    return password


def get_zenodo_token():
    """
    Read Zenodo token from config file (credentials.cfg)

    Returns
    -------
    str
        Zenodo token
    """
    cfg = _load_config_file()
    section = "Zenodo"

    try:
        user = cfg.get(section, "token")
        return user
    except (cp.NoSectionError, cp.NoOptionError):
        return None
