#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Service functions for OEP logging

Read data from MaStR API, process, and write to OEP

SPDX-License-Identifier: AGPL-3.0-or-later
"""

__copyright__ = "© Reiner Lemoine Institut"
__license__ = "GNU Affero General Public License Version 3 (AGPL-3.0)"
__url__ = "https://www.gnu.org/licenses/agpl-3.0.en.html"
__author__ = "Ludee; christian-rli"
__issue__ = "https://github.com/OpenEnergyPlatform/examples/issues/52"
__version__ = "v0.5.0"


import config as lc

# import getpass
import os
import sqlalchemy as sa
from collections import namedtuple

from zeep import Client, Settings
from zeep.cache import SqliteCache
from zeep.transports import Transport

UserToken = namedtuple('UserToken', ['user', 'token'])


def oep_config():
    """Access config.ini.

    Returns
    -------
    UserToken : namedtuple
        API token (key) and user name (value).
    """
    config_section = 'OEP'

    # username
    try:
        lc.config_file_load()
        user = lc.config_file_get(config_section, 'user')
        print(f'Hello {user}, welcome back')
    except:
        user = input('Please provide your OEP username (default surname_name):')
        print(f'Hello {user}')

    # token
    try:
        token = lc.config_file_get(config_section, 'token')
        print(f'Load API token')
    except:
        import sys
        token = input('Token:')
        # token = getpass.getpass(prompt = 'Token:',
        #                         stream = sys.stdin)
        lc.config_section_set(config_section, value=user, key=token)
        print('Config file created')
    return UserToken(user, token)

# a = oep_config()
# a.user
# a.token

def oep_session():
    """SQLAlchemy session object with valid connection to database.

    Returns
    -------
    metadata : SQLAlchemy object
        Database connection object.
    """
    user, token = oep_config()
    # user = input('Enter OEP-username:')
    # token = getpass.getpass('Token:')

    # engine
    try:
        oep_url = 'openenergy-platform.org'  # 'oep.iks.cs.ovgu.de'
        oed_string = f'postgresql+oedialect://{user}:{token}@{oep_url}'
        engine = sa.create_engine(oed_string)
        metadata = sa.MetaData(bind = engine)

        print(f'OEP connection established: /n {metadata}')
        return metadata

    except:
        print('Password authentication failed for user: "{}"'.format(user))
        try:
            os.remove(lc.config_file)
            print('Existing config file deleted! /n Restart script and try again!')
        except OSError:
            print('Cannot delete file! /n Please check login parameters in config file!')


def mastr_config():
    """Access config.ini.

    Returns
    -------
    user : str
        marktakteurMastrNummer (value).
    token : str
        API token (key).
    """
    config_section = 'MaStR'

    # user
    try:
        lc.config_file_load()
        user = lc.config_file_get(config_section, 'user')
        print('Hello ' + user)
    except:
        user = input('Please provide your MaStR Nummer:')

    # token
    try:
        from config import config_file_get
        token = config_file_get(config_section, 'token')
    except:
        import sys
        token = input('Token:')
        # token = getpass.getpass(prompt='apiKey: ',
        #                            stream=sys.stderr)
        lc.config_section_set(config_section, value = user, key = token)
        print('Config file created.')
    return user, token


def mastr_session():
    """MaStR SOAP session using Zeep Client.

    Returns
    -------
    client : SOAP client
        API connection.
    client_bind : SOAP client bind
        bind API connection.
    token : str
        API key.
    user : str
        marktakteurMastrNummer.
    """
    user, token = mastr_config()

    wsdl = 'https://www.marktstammdatenregister.de/MaStRAPI/wsdl/mastr.wsdl'
    transport = Transport(cache=SqliteCache())
    settings = Settings(strict=False, xml_huge_tree=True)
    client = Client(wsdl=wsdl, transport=transport, settings=settings)
    client_bind = client.bind('Marktstammdatenregister', 'Anlage')

    print('MaStR API connection established.')
    return client, client_bind, token, user
