#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
BNetzA - MaStR Download - General

Read data from MaStR API and write to CSV files.

SPDX-License-Identifier: AGPL-3.0-or-later
"""

__copyright__ = "© Reiner Lemoine Institut"
__license__ = "GNU Affero General Public License Version 3 (AGPL-3.0)"
__url__ = "https://www.gnu.org/licenses/agpl-3.0.en.html"
__author__ = "Ludee"
__issue__ = "https://github.com/OpenEnergyPlatform/examples/issues/52"
__version__ = "v0.10.0"

from open_mastr.soap_api.sessions import mastr_session
from open_mastr.soap_api.utils import get_data_version

import pandas as pd
import datetime
from zeep.helpers import serialize_object

import logging

log = logging.getLogger(__name__)

"""SOAP API"""
client, client_bind, token, user = mastr_session()
api_key = token
my_mastr = user

def get_mastr_time():
    """Return current time from MaStR.

        Returns
    -------
    mastr_time : DataFrame
        LokaleUhrzeit.
    """
    data_version = get_data_version()
    try:
        c = client.service.GetLokaleUhrzeit()
        s = serialize_object(c)
        df = pd.DataFrame(list(s.items()), )
        mastr_time = df.set_index(list(df.columns.values)[0]).transpose()
        mastr_time['version'] = data_version
        mastr_time['timestamp'] = str(datetime.datetime.now())
        mastr_time_str = mastr_time['LokaleUhrzeit'].values[0]
        log.info(f'MaStR-Time: {mastr_time_str}')
        return mastr_time, mastr_time_str
    except Exception as e:
        log.info(f'Failed to get time.')

def get_mastr_time_auth():
    """Return current time with authentication from MaStR.

        Returns
    -------
    mastr_time_auth_str : Str
        LokaleUhrzeitMitAuthentifizierung.
    """
    data_version = get_data_version()
    try:
        c = client.service.GetLokaleUhrzeitMitAuthentifizierung(apiKey=api_key)
        s = serialize_object(c)
        df = pd.DataFrame(list(s.items()), )
        mastr_time_auth = df.set_index(list(df.columns.values)[0]).transpose()
        mastr_time_auth['version'] = data_version
        mastr_time_auth['timestamp'] = str(datetime.datetime.now())
        mastr_time_auth_str = mastr_time_auth['LokaleUhrzeitMitAuthentifizierung'].values[0]
        log.info(f'MaStR-Time-Auth: {mastr_time_auth_str}')
        return mastr_time_auth
    except Exception as e:
        log.info(f'Failed to get time with authentication.')


def get_daily_contingent():
    """Return current contingent status.

        Returns
    -------
    daily_contingent_limit : DataFrame
        AktuellesLimitTageskontingent.
    daily_contingent_status : DataFrame
        AktuellerStandTageskontingent.
    """
    data_version = get_data_version()
    try:
        c = client.service.GetAktuellerStandTageskontingent(apiKey=api_key,
                                                         marktakteurMastrNummer=my_mastr)
        s = serialize_object(c)
        df = pd.DataFrame(list(s.items()), )
        daily_contingent = df.set_index(list(df.columns.values)[0]).transpose()
        daily_contingent['version'] = data_version
        daily_contingent['timestamp'] = str(datetime.datetime.now())
        daily_contingent_limit = daily_contingent['AktuellesLimitTageskontingent'].values[0]
        daily_contingent_status = daily_contingent['AktuellerStandTageskontingent'].values[0]
        log.info(f'Daily contigent: {daily_contingent_status} from daily Limit: {daily_contingent_limit}')
        return daily_contingent, daily_contingent_limit, daily_contingent_status
    except Exception as e:
        log.info(f'Failed to get daily contingent.')
