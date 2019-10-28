#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
BNetzA - MaStR Download - Wind

Read data from MaStR API and write to CSV files.

SPDX-License-Identifier: AGPL-3.0-or-later
"""

__copyright__ = "\xc2 Reiner Lemoine Institut"
__license__ = "GNU Affero General Public License Version 3 (AGPL-3.0)"
__url__ = "https://www.gnu.org/licenses/agpl-3.0.en.html"
__author__ = "Ludee; christian-rli"
__issue__ = "https://github.com/OpenEnergyPlatform/examples/issues/52"
__version__ = "v0.8.0"


import time
from datetime import datetime as dt
import math
import logging
import multiprocessing as mp
from functools import partial
import pandas as pd
import numpy as np
from datetime import datetime
from zeep.helpers import serialize_object

from soap_api.sessions import mastr_session, API_MAX_DEMANDS
from soap_api.utils import split_to_sublists, write_to_csv, remove_csv, get_data_version
from mastr_wind_processing import do_wind

import math

log = logging.getLogger(__name__)
''' VAR IMPORT '''
from utils import fname_all_units, fname_wind_unit, read_timestamp

"""SOAP API"""
client, client_bind, token, user = mastr_session()
api_key = token
my_mastr = user


def get_power_unit(start_from, wind=False, datum='Null', limit=API_MAX_DEMANDS):
    """Get Stromerzeugungseinheit from API using GetGefilterteListeStromErzeuger.

    Parameters
    ----------
    start_from : int
        Skip first entries.
    limit : int
        Number of entries to get (default: 2000)
    """
    status = 'InBetrieb'
    source = 'Wind'
    if wind==False:
        source = 'None'
    try:
        c = client_bind.GetGefilterteListeStromErzeuger(
            apiKey=api_key,
            marktakteurMastrNummer=my_mastr,
            # einheitBetriebsstatus=status,
            startAb=start_from,
            energietraeger=source,
            limit=limit,  # Limit of API.
            datumAb = datum
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


def download_power_unit(
        power_unit_list_len=2363200,
        limit=API_MAX_DEMANDS,
        wind=False
):
    """Download StromErzeuger.

    Arguments
    ---------
    power_unit_list_len : None|int
        Maximum number of units to get. Check MaStR portal for current number.
    limit : int
        Number of units to get per call to API (limited to 2000).
    energietraeger: string
        None, AndereGase, Biomasse, Braunkohle, Erdgas, Geothermie, Grubengas, Kernenergie,
        Klaerschlamm, Mineraloelprodukte, NichtBiogenerAbfall, SolareStrahlungsenergie, Solarthermie,
        Speicher, Steinkohle, Waerme, Wind, Wasser
    wind : bool
        Wether only wind data but all wind data (wind power unit, wind, (wind eeg), wind permit, wind all)
        should be downloaded and processed

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

    log.info(f'Write to : {fname_all_units}')

    # if the list size is smaller than the limit
    if limit > power_unit_list_len:
        limit = power_unit_list_len

    for start_from in range(0, power_unit_list_len, limit):
        try:
            power_unit = get_power_unit(start_from, wind, limit)
            write_to_csv(fname_all_units, power_unit)
            power_unit_len = len(power_unit)
            log.info(f'Download power_unit from {start_from}-{start_from + power_unit_len}')
        except:
            log.exception(f'Download failed power_unit from {start_from}')


def download_parallel_power_unit(
        power_unit_list_len=2359365,
        limit=API_MAX_DEMANDS,
        batch_size=10000,
        start_from=0,
        overwrite=False, 
        wind=False,
        eeg=False,
        update=False
):
    """Download StromErzeuger with parallel process


    Arguments
    ---------
    power_unit_list_len : None|int
        Maximum number of units to get. Check MaStR portal for current number.
    limit : int
        Number of units to get per call to API (limited to 2000).
    batch_size : int
        Number of elements in a batch.
        Units from the list will be split into n batches of batch_size.
        A higher batch size number means less threads but more retries.
    start_from : int
        Start index in the power_unit_list.
    overwrite : bool
        Whether or not the data file should be overwritten.
    wind : bool
        Wether only wind data but all wind data (wind power unit, wind, (wind eeg), wind permit, wind all)
        should be downloaded and processed
        current max entries: 42748 / 27.10.2019
    eeg : bool
        Wether eeg data should be downloaded,too
    """
    if wind==True:
        power_unit_list_len=42748

    if update==True:
        datum = get_update_date(wind)
    else:
        datum = 'NULL'


    log.info('Download MaStR Power Unit')
    if batch_size < API_MAX_DEMANDS:
        limit = batch_size

    if power_unit_list_len+start_from > 2359365:
        deficit = (power_unit_list_len+start_from)-2359365
        power_unit_list_len = power_unit_list_len-deficit
        if power_unit_list_len <= 0:
            log.info('No entries to download. Decrease index size.')
            return 0
    end_at = power_unit_list_len + start_from

    if overwrite:
        remove_csv(fname_all_units)

    if power_unit_list_len < limit:
        log.info(f'Number of expected power units: {limit}')
    else:
        log.info(f'Number of expected power units: {power_unit_list_len}')
    log.info(f'Starting at index: {start_from}')
    t = time.time()
    # assert lists with size < api limit
    if power_unit_list_len < limit:
        limit = power_unit_list_len

    start_from_list = list(range(start_from, end_at, limit + 1))
    length = len(start_from_list)
    num = math.ceil(power_unit_list_len/batch_size)
    assert num >= 1
    sublists = split_to_sublists(start_from_list, length, num)
    log.info('Number of batches to process: %s', num)
    summe = 0
    length = len(sublists)

    almost_end_of_list = False
    end_of_list = False

    while len(sublists) > 0:
        if len(sublists) == 1:
            if almost_end_of_list is False:
                almost_end_of_list = True
            else:
                # compute the rest of the integer division by API_MAX_DEMANDS
                limit = np.mod(end_at - start_from, API_MAX_DEMANDS)
                if limit == 0:
                    # number of download is an integer number of API_MAX_DEMANDS
                    limit = API_MAX_DEMANDS
                end_of_list = True
        try:
            sublist = sublists.pop(0)
            ndownload = len(sublist)
            pool = mp.Pool(processes=ndownload)
            if almost_end_of_list is False:
                result = pool.map(partial(get_power_unit, limit=limit, wind=wind, datum=datum), sublist)
            else:
                if end_of_list is False:
                    # The last list might not be an integer number of API_MAX_DEMANDS
                    result = pool.map(partial(get_power_unit, limit=limit, wind=wind, datum=datum), sublist[:-1])
                    # Evaluate the last item separately
                    sublists.append([sublist[-1]])
                else:
                    result = pool.map(partial(get_power_unit, limit=limit, wind=wind, datum=datum), sublist)

            summe += 1
            progress = math.floor((summe/length)*100)
            print('\r[{0}{1}] %'.format('#'*(int(math.floor(progress/10))), '-'*int(math.floor((100-progress)/10))))

            if result:
                for mylist in result:
                    if wind==False:
                        write_to_csv(fname_all_units, pd.DataFrame(mylist))
                    else:
                        write_to_csv(fname_wind_unit, pd.DataFrame(mylist))
                    pool.close()
                    pool.join()
        except Exception as e:
            log.error(e)
    log.info('Power Unit Download executed in: {0:.2f}'.format(time.time()-t))
    do_wind(eeg=eeg)

""" check for new entries since TIMESTAMP """
def get_update_date(wind=False):

    dateTime = datetime.now()
    date = dateTime.date()

    ts = read_timestamp(wind)
    if not ts==False:
        ts = dt.strptime(ts, '%Y-%m-%d %H:%M:%S.%f')
        ts_date = ts.date()

        if ts_date is date:
            log.info("No updates available. Try again on another day.")
            return TIMESTAMP
        else:
            log.info(f"checking database for updates since {ts_date}")
            return ts
    return 'NULL'