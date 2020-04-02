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
__author__ = "Ludee; christian-rli; Bachibouzouk; solar-c"
__issue__ = "https://github.com/OpenEnergyPlatform/examples/issues/52"
__version__ = "v0.9.0"


import time
from datetime import datetime as dt
import logging
import multiprocessing as mp
from multiprocessing import get_context
from multiprocessing.pool import ThreadPool 
from functools import partial
import pandas as pd
import numpy as np
from datetime import datetime
from zeep.helpers import serialize_object

from soap_api.sessions import mastr_session, API_MAX_DEMANDS
from soap_api.utils import split_to_sublists, write_to_csv, remove_csv, get_data_version, read_timestamp, TOTAL_POWER_UNITS
# from soap_api.mastr_wind_processing import do_wind

log = logging.getLogger(__name__)
''' VAR IMPORT '''
from soap_api.utils import fname_power_unit, fname_wind_unit, TIMESTAMP


"""SOAP API"""
client, client_bind, token, user = mastr_session()
api_key = token
my_mastr = user


def get_power_unit(start_from, wind=False, datum='1900-01-01 00:00:00.00000', limit=API_MAX_DEMANDS):
    """Get Stromerzeugungseinheit from API using GetGefilterteListeStromErzeuger.

    Parameters
    ----------
    start_from : int
        Skip first entries.
    wind : bool
        Wether only wind data should be retrieved
    datum: String
        the starting datestring to retrieve data, can be used for updating a data set
    limit : int
        Number of entries to get (default: 2000)
    """
    power_unit = pd.DataFrame()
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
            limit=limit  # Limit of API.
            #datumAb = datum
        )
        s = serialize_object(c)
        power_unit = pd.DataFrame(s['Einheiten'])
        power_unit.index.names = ['lid']
        power_unit['version'] = get_data_version()
        power_unit['timestamp'] = str(datetime.now())
    except Exception as e:
        log.info(e)
    return [start_from, power_unit]


def download_power_unit(
        power_unit_list_len=TOTAL_POWER_UNITS,
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
    2468804 (2019-11-28)
    2487585 (2019-12-05) data-release/2.2.0
    2791367 (2020-03-21) data-release/2.2.1
    2812372 (2020-03-28) data-release/2.4.0
    """
    log.info('Download MaStR Power Unit')
    log.info(f'Number of expected power units: {power_unit_list_len}')

    log.info(f'Write to : {fname_power_unit}')

    # if the list size is smaller than the limit
    if limit > power_unit_list_len:
        limit = power_unit_list_len

    for start_from in range(0, power_unit_list_len, limit):
        try:
            power_unit = get_power_unit(start_from, wind, limit)
            write_to_csv(fname_power_unit, power_unit)
            power_unit_len = len(power_unit)
            log.info(f'Download power_unit from {start_from}-{start_from + power_unit_len}')
        except:
            log.exception(f'Download failed power_unit from {start_from}')


def download_parallel_power_unit(
        power_unit_list_len=TOTAL_POWER_UNITS,
        limit=API_MAX_DEMANDS,
        batch_size=10000,
        start_from=0,
        overwrite=False, 
        wind=False,
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
        current max entries:
        42748 (2019-10-27)
        42481 (2019-11-28)
    eeg : bool
        Wether eeg data should be downloaded,too
    update: bool
        Wether a dataset should only be updated
    """
    if wind==True:
        power_unit_list_len=42748
    update = TIMESTAMP
    if update==True:
        datum = get_update_date(wind)

    log.info('Download MaStR Power Unit')
    if batch_size < API_MAX_DEMANDS:
        limit = batch_size

    if power_unit_list_len+start_from > TOTAL_POWER_UNITS:
        deficit = (power_unit_list_len+start_from)-TOTAL_POWER_UNITS
        power_unit_list_len = power_unit_list_len-deficit
        if power_unit_list_len <= 0:
            log.info('No entries to download. Decrease index size.')
            return 0
    end_at = power_unit_list_len + start_from

    if overwrite:
        remove_csv(fname_power_unit)

    if power_unit_list_len < limit:
        # less than one batch is to be downloaded
        limit = power_unit_list_len

    log.info(f'Number of expected power units: {power_unit_list_len}')

    log.info(f'Starting at index: {start_from}')
    t = time.time()

    # set some params
    start_from_list = list(range(start_from, end_at, limit + 1))
    length = len(start_from_list)
    num_batches = int(np.ceil(power_unit_list_len/batch_size))
    assert num_batches >= 1
    sublists = split_to_sublists(start_from_list, num_batches)
    log.info('Number of batches to process: %s', num_batches)
    summe = 0
    length = len(sublists)
    almost_end_of_list = False
    end_of_list = False
    failed_downloads = []
    counter = 0
    
    # while there are still batches, try to download batch in parallel
    while len(sublists) > 0:
        if len(sublists) == 1:
            # check wether the 1st round is done and if any downloads are in the failed_downloads list
            if counter < 1 and len(failed_downloads) > 0:
                    sublists = sublists[0]+failed_downloads
                    num = int(np.ceil((len(failed_downloads)*2000)/batch_size))
                    sublists = split_to_sublists(sublists, num)
                    counter = counter+1
                    summe = 0
                    length = len(sublists)
                    failed_downloads = []
                    log.info('Starting second round with failed downloads')
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
            if len(sublists) > 1:
                almost_end_of_list = False
                end_of_list = False
            if len(sublists) == 1:
                end_of_list = False
            
            ndownload = len(sublist)
            #pool = ThreadPool(processes=ndownload)

            # create pool and map all indices in batch sublist to one instance of get_power_unit
            # use partial to partially preset some variables from get_power_unit
            pool = mp.get_context("spawn").Pool(processes=3)
            if almost_end_of_list is False:
                result = pool.map(partial(get_power_unit, limit=limit, wind=wind), sublist)
            else:
                if end_of_list is False:
                    # The last list might not be an integer number of API_MAX_DEMANDS
                    result = pool.map(partial(get_power_unit, limit=limit, wind=wind), sublist[:-1])
                    # Evaluate the last item separately
                    sublists.append([sublist[-1]])
                else:
                    result = pool.map(partial(get_power_unit, limit=limit, wind=wind), sublist)
            # print progression        
            summe += 1
            progress = np.floor((summe/length)*100)
            print('\r[{0}{1}] %'.format('#'*(int(np.floor(progress/10))), '-'*int(np.floor((100-progress)/10))))
            # check for failed downloads and add indices of failed downloads to failed list
            indices_list = []
            if not len(result)==0:
                for ind, mylist in result:
                    if mylist.empty:
                        failed_downloads.append(ind)
                    mylist['timestamp'] = datetime.now()
                    if wind==False:
                        write_to_csv(fname_power_unit, pd.DataFrame(mylist))
                    else:
                        write_to_csv(fname_wind_unit, pd.DataFrame(mylist))
                log.info('Failed downloads: %s', len(failed_downloads))
                pool.close()
                pool.terminate()
                pool.join()
            else:
                failed_downloads = failed_downloads+sublist
                log.info('Download failed, retrying later')
        except Exception as e:
            log.error(e)
    log.info('Power Unit Download executed in: {0:.2f}'.format(time.time()-t))

""" check for new entries since TIMESTAMP """
def get_update_date(wind=False):
    """ Retrieve timestamp to use as update baseline for powerunits - from timestamp until now.

    Parameters
    ----------
    wind : bool
        Wether a wind data timestamp should be retrieved or a general powerunit timestamp
    """
    dateTime = datetime.now()
    date = dateTime.date()

    # retrieve last timestamp from file powerunits or wind units if wind=True
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
