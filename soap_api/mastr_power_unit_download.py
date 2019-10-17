#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
BNetzA - MaStR Download - Power Unit

Read data from MaStR API and write to CSV files.

SPDX-License-Identifier: AGPL-3.0-or-later
"""

__copyright__ = "© Reiner Lemoine Institut"
__license__ = "GNU Affero General Public License Version 3 (AGPL-3.0)"
__url__ = "https://www.gnu.org/licenses/agpl-3.0.en.html"
__author__ = "Ludee; christian-rli"
__issue__ = "https://github.com/OpenEnergyPlatform/examples/issues/52"
__version__ = "v0.8.0"

from soap_api.sessions import mastr_session, API_MAX_DEMANDS

import time
import math
import multiprocessing as mp
from multiprocessing.pool import ThreadPool 
from functools import partial
import pandas as pd
import datetime
from zeep.helpers import serialize_object
import logging
log = logging.getLogger(__name__)
from soap_api.utils import split_to_sublists, get_filename_csv_see, set_filename_csv_see, write_to_csv, remove_csv, get_data_version
import math
''' GLOBAL VAR IMPORT '''
from soap_api.utils import csv_see

"""SOAP API"""
client, client_bind, token, user = mastr_session()
api_key = token
my_mastr = user


def get_power_unit(start_from, limit=API_MAX_DEMANDS):
    """Get Stromerzeugungseinheit from API using GetGefilterteListeStromErzeuger.

    Parameters
    ----------
    start_from : int
        Skip first entries.
    limit : int
        Number of entries to get (default: 2000)
    """ 
    status = 'InBetrieb'
    try:
        c = client_bind.GetGefilterteListeStromErzeuger(
            apiKey=api_key,
            marktakteurMastrNummer=my_mastr,
            # einheitBetriebsstatus=status,
            startAb=start_from,
            limit=limit  # Limit of API.
        )
        s = serialize_object(c)
        power_unit = pd.DataFrame(s['Einheiten'])
        power_unit.index.names = ['lid']
        power_unit['version'] = get_data_version()
        power_unit['timestamp'] = str(datetime.datetime.now())
    except Exception as e:
        log.info('Download failed, retrying for %s', start_from)
    # remove double quotes from column
    #power_unit['Standort'] = power_unit['Standort'].str.replace('"', '')
    return power_unit


def get_all_units(start_from, limit=API_MAX_DEMANDS):
    
    try:
        c = client_bind.GetListeAlleEinheiten(
        apiKey=api_key,
        marktakteurMastrNummer=my_mastr,
        startAb=start_from,
        limit=limit)  # Limit of API.  
        s = serialize_object(c)

        power_unit = pd.DataFrame(s['Einheiten'])
        power_unit.index.names = ['lid']
        power_unit['version'] = get_data_version()
        power_unit['timestamp'] = str(datetime.datetime.now())
    except Exception as e:
        log.info('Download failed, retrying for %s', start_from)
        log.debug(e)
        #log.error(e)
    # remove double quotes from column
    return power_unit


def download_power_unit(
        power_unit_list_len=2363200,
        limit=API_MAX_DEMANDS,
        ofname=None
):
    """Download StromErzeuger.

    Arguments
    ---------
    power_unit_list_len : None|int
        Maximum number of units to get. Check MaStR portal for current number.
    limit : int
        Number of units to get per call to API (limited to 2000).
    ofname : string
        Path to save the downloaded files.

    Existing units:
    1822000 (2019-02-10)
    1844882 (2019-02-15)
    1847117 (2019-02-17)
    1864103 (2019-02-23)
    1887270 (2019-03-03)
    1965200 (2019-04-11)
    2328576 (2019-09-30)
    2331651 (2019-10-01)
    2359365 (2019-10-15)
    2363200 (2019-10-17)
    """
    log.info('Download MaStR Power Unit')
    log.info(f'Number of expected power units: {power_unit_list_len}')

    if ofname is None:
        ofname = csv_see

    log.info(f'Write to : {ofname}')

    # if the list size is smaller than the limit
    if limit > power_unit_list_len:
        limit = power_unit_list_len

    for start_from in range(0, power_unit_list_len, limit):
        try:
            power_unit = get_power_unit(start_from, limit)
            write_to_csv(ofname, power_unit)
            power_unit_len = len(power_unit)
            log.info(f'Download power_unit from {start_from}-{start_from + power_unit_len}')
        except:
            log.exception(f'Download failed power_unit from {start_from}')


''' split power_unit_list_len into batches of 20.000, this number can be changed and was decided on empirically -- 
a higher batch size number means less threads but more retries
each batch is processed by a thread pool, where for each subbatch of 2000 (API limit) a new thread is created  '''
def download_parallel_power_unit(power_unit_list_len=2359365, limit=2000, batch_size=10000, start_from=0, overwrite=True, all_units=False):
    global csv_see
    power_unit_list = list()
    log.info('Download MaStR Power Unit')
    if batch_size < API_MAX_DEMANDS:
        limit = batch_size

    # if the list size is smaller than the limit
    if limit > power_unit_list_len:
        limit = power_unit_list_len

    if power_unit_list_len+start_from > 2359365:
        deficit = (power_unit_list_len+start_from)-2359365
        power_unit_list_len = power_unit_list_len-deficit
        if power_unit_list_len <= 0:
            log.info('No entries to download. Decrease index size.')
            return 0
    end_at = power_unit_list_len+start_from
    if ofname is None:
        ofname = set_filename_csv_see('power_units', overwrite)

    if overwrite:
        remove_csv(ofname)

    if power_unit_list_len < limit:
        log.info(f'Number of expected power units: {limit}')
    else:
        log.info(f'Number of expected power units: {power_unit_list_len}')
    log.info(f'Starting at index: {start_from}')
    t = time.time()
    # assert lists with size < api limit
    if power_unit_list_len < limit:
        limit = power_unit_list_len

    partial(get_power_unit, limit)
    partial(get_all_units, limit)
    start_from_list = list(range(start_from, end_at, limit))
    length = len(start_from_list)
    num = math.ceil(power_unit_list_len/batch_size)
    assert num >= 1
    sublists = split_to_sublists(start_from_list, length, num)
    log.info('Number of batches to process: %s', num)
    summe = 0
    length = len(sublists)
    while sublists:
        try:
            pool = mp.Pool(processes=len(sublists[0]))
            if all_units:
                result= pool.map(get_all_units,sublists[0])
            else:
                result = pool.map(get_power_unit, sublists[0])
            summe += 1
            progress = math.floor((summe/length)*100)
            print('\r[{0}{1}] %'.format('#'*(int(math.floor(progress/10))), '-'*int(math.floor((100-progress)/10))))
            sublists.pop(0)
            if result:
                for mylist in result:
                    write_to_csv(ofname, pd.DataFrame(mylist))
                    pool.close()
                    pool.join()
        except Exception as e:
            log.error(e)
    log.info('Power Unit Download executed in: {0:.2f}'.format(time.time()-t))



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
                                    'StatisikFlag': str,
                                    'version': str,
                                    'timestamp': str})

    log.info(f'Finished reading data from {csv_name}')
    #log.info(power_unit[1:4])
    return power_unit