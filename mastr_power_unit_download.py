#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
BNetzA - MaStR Download - Wind

Read data from MaStR API and write to CSV files.

SPDX-License-Identifier: AGPL-3.0-or-later
"""

__copyright__ = "© Reiner Lemoine Institut"
__license__ = "GNU Affero General Public License Version 3 (AGPL-3.0)"
__url__ = "https://www.gnu.org/licenses/agpl-3.0.en.html"
__author__ = "Ludee; christian-rli"
__issue__ = "https://github.com/OpenEnergyPlatform/examples/issues/52"
__version__ = "v0.7.0"

from config import get_data_version, write_to_csv
from sessions import mastr_session

import pandas as pd
import datetime
from zeep.helpers import serialize_object

import logging
log = logging.getLogger(__name__)

"""SOAP API"""
client, client_bind, token, user = mastr_session()
api_key = token
my_mastr = user


def get_power_unit(start_from, limit=2000):
    """Get Stromerzeugungseinheit from API using GetGefilterteListeStromErzeuger.

    Parameters
    ----------
    start_from : int
        Skip first entries.
    limit : int
        Number of entries to get (default: 2000)
    """
    data_version = get_data_version()
    status = 'None'                 # 8.1.2 AnlagenBetriebsStatusEnum::string
    energy_carrier = 'None'         # 8.1.36 EnergietraegerEnum::string
    c = client_bind.GetGefilterteListeStromErzeuger(
        apiKey=api_key,
        marktakteurMastrNummer=my_mastr,
        einheitBetriebsstatus=status,
        energietraeger=energy_carrier,
        startAb=start_from,
        limit=limit)  # Limit of API.
    s = serialize_object(c)
    power_unit = pd.DataFrame(s['Einheiten'])
    power_unit.index.names = ['lid']
    power_unit['version'] = data_version
    power_unit['timestamp'] = str(datetime.datetime.now())

    # remove double quotes from column
    power_unit['Standort'] = power_unit['Standort'].str.replace('"', '')

    return power_unit


def download_power_unit(power_unit_list_len=2020220, limit=2000):
    """Download StromErzeuger.

    Arguments
    ---------
    power_unit_list_len : None|int
        Maximum number of units to get. Check MaStR portal for current number.
    limit : int
        Number of units to get per call to API (limited to 2000).

    Existing units:
    1822000 (2019-02-10)
    1844882 (2019-02-15)
    1847117 (2019-02-17)
    1864103 (2019-02-23)
    1887270 (2019-03-03)
    1965200 (2019-04-11)
    2018480 (2019-05-15)
    2020220 (2019-05-16)
    """

    data_version = get_data_version()
    csv_see = f'data/bnetza_mastr_{data_version}_power-unit.csv'
    log.info('Download MaStR Power Unit')
    log.info(f'Number of expected power_unit: {power_unit_list_len}')

    for start_from in range(0, power_unit_list_len, limit):
        try:
            power_unit = get_power_unit(start_from, limit)
            write_to_csv(csv_see, power_unit)

            power_unit_len = len(power_unit)
            log.info(f'Download power_unit from {start_from}-{start_from + power_unit_len}')
        except:
            log.exception(f'Download failed power_unit from {start_from}')


def read_power_units(csv_name):
    """Read Stromerzeugungseinheit from CSV file.

    Parameters
    ----------
    csv_name : str
        Name of file.

    Returns
    -------
    power_unit : DataFrame
        Stromerzeugungseinheit.
    """
    # log.info(f'Read data from {csv_name}')
    power_unit = pd.read_csv(csv_name, header=0, sep=';', index_col=False, encoding='utf-8',
                             dtype={'id': int,
                                    'lid': int,
                                    'EinheitMastrNummer': str,
                                    'Name': str,
                                    'Einheitart': str,
                                    'Einheittyp': str,
                                    'Standort': str,
                                    'Bruttoleistung': str,
                                    'Erzeugungsleistung': str,
                                    'EinheitBetriebsstatus': str,
                                    'Anlagenbetreiber': str,
                                    'EegMastrNummer': str,
                                    'KwkMastrNummer': str,
                                    'SpeMastrNummer': str,
                                    'GenMastrNummer': str,
                                    'BestandsanlageMastrNummer': str,
                                    'NichtVorhandenInMigriertenEinheiten': str,
                                    'version': str,
                                    'timestamp': str})

    # log.info(f'Finished reading data from {csv_name}')
    return power_unit
