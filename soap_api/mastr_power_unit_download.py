#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
BNetzA - MaStR Download - Power Unit

Read data from MaStR API and write to CSV files.

SPDX-License-Identifier: AGPL-3.0-or-later
"""

__copyright__ = "\xc2 Reiner Lemoine Institut"
__license__ = "GNU Affero General Public License Version 3 (AGPL-3.0)"
__url__ = "https://www.gnu.org/licenses/agpl-3.0.en.html"
__author__ = "Ludee; christian-rli; Bachibouzouk; solar-c"
__issue__ = "https://github.com/OpenEnergyPlatform/examples/issues/52"
__version__ = "v0.10.0"


import time
from datetime import datetime as dt
import logging
import pandas as pd
import numpy as np
from datetime import datetime
from zeep.helpers import serialize_object

from soap_api.sessions import mastr_session, API_MAX_DEMANDS
from soap_api.utils import split_to_sublists, write_to_csv, remove_csv, get_data_version, read_timestamp, TOTAL_POWER_UNITS
from soap_api.parallel import parallel_download
from soap_api.utils import fname_power_unit
# from soap_api.mastr_wind_processing import do_wind

log = logging.getLogger(__name__)
''' VAR IMPORT '''
from soap_api.utils import fname_power_unit, \
    fname_wind_unit, \
    fname_power_unit_wind, \
    fname_power_unit_hydro, \
    fname_power_unit_biomass, \
    fname_power_unit_solar, \
    fname_power_unit_nuclear,  \
    fname_power_unit_storage,  \
    fname_power_unit_gsgk,  \
    fname_power_unit_combustion,  \
    TIMESTAMP


"""SOAP API"""
client, client_bind, token, user = mastr_session()
api_key = token
my_mastr = user


def get_power_unit(start_from, energy_carrier, datum='1900-01-01 00:00:00.00000', limit=API_MAX_DEMANDS):
    """Get Stromerzeugungseinheit from API using GetGefilterteListeStromErzeuger.

    Parameters
    ----------
    start_from : int
        Skip first entries.
    datum: String
        the starting datestring to retrieve data, can be used for updating a data set
    limit : int
        Number of power unit to get (default: 2000)
    """
    power_unit = pd.DataFrame()
    status = 'InBetrieb'
    # power = 30

    try:
        c = client_bind.GetGefilterteListeStromErzeuger(
            apiKey=api_key,
            marktakteurMastrNummer=my_mastr,
            # einheitBetriebsstatus=status,
            startAb=start_from,
            energietraeger=energy_carrier,
            limit=limit
            #bruttoleistungGroesser=power
            #datumAb = datum
        )
        s = serialize_object(c)
        power_unit = pd.DataFrame(s['Einheiten'])
        power_unit.index.names = ['lid']
        power_unit['db_offset'] = [i for i in range(start_from, start_from+len(power_unit))]
        power_unit['version'] = get_data_version()
        power_unit['timestamp'] = str(datetime.now())
        return power_unit
    except Exception as e:
        log.info(e)
    # from an old branch:
    # log.info('Download failed, retrying for %s', start_from)
    # power_unit = pd.DataFrame()
    # remove double quotes from column
    # power_unit['Standort'] = power_unit['Standort'].str.replace('"', '')
    # return power_unit


def download_power_unit(
        power_unit_list_len=TOTAL_POWER_UNITS,
        pu_limit=API_MAX_DEMANDS,
        energy_carrier='None'
):
    """Download StromErzeuger.

    Arguments
    ---------
    power_unit_list_len : None|int
        Maximum number of units to get. Check MaStR portal for current number.
    pu_limit : int
        Number of units to get per call to API (limited to 2000).
    energy_carrier: string
        Energieträger: None, AndereGase, Biomasse, Braunkohle, Erdgas, Geothermie, Grubengas, Kernenergie,
        Klaerschlamm, Mineraloelprodukte, NichtBiogenerAbfall, SolareStrahlungsenergie, Solarthermie,
        Speicher, Steinkohle, Waerme, Wind, Wasser

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
    3197769 (2020-08-17) data-release/2.5.0
    3200862 (2020-08-18) data-release/2.5.1
    3203715 (2020-08-19) data-release/2.5.2
    3204000 (2020-08-20) data-release/2.5.5
    3233056 (2020-08-20) data-release/2.7.0
    """
    log.info(f'Download MaStR power unit for energy carrier: {energy_carrier}')
    log.info(f'Number of expected power units: {power_unit_list_len}')

    if energy_carrier == 'Kernenergie':
        filename = fname_power_unit_nuclear
    elif energy_carrier == 'Wind':
        filename = fname_power_unit_wind
    elif energy_carrier == 'Wasser':
        filename = fname_power_unit_hydro
    elif energy_carrier == 'Biomasse':
        filename = fname_power_unit_biomass
    elif energy_carrier == 'SolareStrahlungsenergie':
        filename = fname_power_unit_solar
    elif energy_carrier == 'Speicher':
        filename = fname_power_unit_storage
    elif energy_carrier == 'Geothermie' or energy_carrier == 'Solarthermie' or energy_carrier == 'Grubengas' or energy_carrier == 'Klaerschlamm':
        filename = fname_power_unit_gsgk
    elif energy_carrier == 'AndereGase' or 'Braunkohle' or 'Erdgas' or 'NichtBiogenerAbfall' or 'Steinkohle' or 'Waerme':
        filename = fname_power_unit_combustion
    else:
        filename = fname_power_unit

    log.info(f'Write to: {filename}')

    # if the list size is smaller than the limit
    if pu_limit > power_unit_list_len:
        pu_limit = power_unit_list_len

    for start_from in range(0, power_unit_list_len, pu_limit):
        try:
            start_from, power_unit = get_power_unit(start_from, energy_carrier, pu_limit)
            write_to_csv(filename, pd.DataFrame(power_unit))
            power_unit_len = len(power_unit)
            log.info(f'Download power_unit from {start_from}-{start_from + power_unit_len}')
        except:
            log.exception(f'Download failed power_unit from {start_from}')


# Helper function to pass more than one argument in parallel function call (multiprocess allows only a single argument to be passed, so we pass a tuple and destructure its contents here)
def get_power_unit_helper(arg):
    start_from, limit, wind = arg
    return get_power_unit(start_from, limit=limit, wind=wind)

def download_parallel_power_unit(
        power_unit_list_len=TOTAL_POWER_UNITS,
        limit=API_MAX_DEMANDS,
        start_from=0,
        threads=4,
        timeout=10,
        time_blacklist=True,
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
    start_from : int
        Start index in the power_unit_list.
    threads : int
        number of parallel threads to download with
    timeout : int
        retry for this amount of minutes after the last successful
        query before stopping
    time_blacklist : bool
        exit as soon as current time is blacklisted
    overwrite : bool
        Whether or not the data file should be overwritten.
    wind : bool
        Wether only wind data but all wind data (wind power unit, wind, (wind eeg), wind permit, wind all)
        should be downloaded and processed
        current max entries:
        42748 (2019-10-27)
        42481 (2019-11-28)
    update: bool
        Wether a dataset should only be updated
    """

    # TODO: Not sure what this does
    # TODO: [LH] This is legacy code and donwloads only the latest updates (I think). It was coded by solar-c without any documentation
    #if wind is True:
    #    power_unit_list_len=42748
    #update = TIMESTAMP
    #if update is True:
    #    datum = get_update_date(wind)

    log.info('Download MaStR Power Unit')

    #if overwrite is True:
    #    remove_csv(fname_power_unit)

    # Create a list of tuples with (start_from, limit, wind) to pass to thread. If list is not evenly divisible, the limit value of the last element is adjusted to be below the database limit
    def unit_list(start, num, size):
        items = num-start
        l = [(i, size, wind) for i in range(start,num,size)]
        if items % size != 0:
            idx, size = l.pop()
            l.append((idx, items%size, wind))
        return l

    units = unit_list(start_from, power_unit_list_len, limit)

    parallel_download(units, get_power_unit_helper, fname_power_unit, threads=threads, timeout=timeout, time_blacklist=time_blacklist)

    

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
  