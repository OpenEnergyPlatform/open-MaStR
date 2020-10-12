#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
BNetzA - MaStR Download - Solar

Read data from MaStR API and write to CSV files.

SPDX-License-Identifier: AGPL-3.0-or-later
"""

__copyright__ = "© Reiner Lemoine Institut"
__license__ = "GNU Affero General Public License Version 3 (AGPL-3.0)"
__url__ = "https://www.gnu.org/licenses/agpl-3.0.en.html"
__author__ = "Ludee; christian-rli; Bachibouzouk; solar-c"
__issue__ = "https://github.com/OpenEnergyPlatform/examples/issues/52"
__version__ = "v0.10.0"

from soap_api.sessions import mastr_session
from soap_api.utils import split_to_sublists,get_data_version, write_to_csv, remove_csv, read_power_units

from zeep.helpers import serialize_object
from functools import partial
import pandas as pd
import multiprocessing as mp
from multiprocessing.pool import ThreadPool 
import os
import time
import datetime
import re
import requests
from xml.etree import ElementTree
import time
import logging
log = logging.getLogger(__name__)

""" import variables """
from soap_api.utils import fname_power_unit, fname_storage, fname_storage_unit

"""SOAP API"""
client, client_bind, token, user = mastr_session()
api_key = token
my_mastr = user

def download_parallel_unit_storage(start_from=0, end_at=20, overwrite=True):
    storage_units = setup_storage_units(overwrite)
    storage_units_nr = storage_units['EinheitMastrNummer'].values.tolist()
    eeg_nr = storage_units['EegMastrNummer'].values.tolist()
    storage_len = len(storage_units_nr)
    eeg_len = len(eeg_nr)
    log.info(f'Download MaStR Storage')
    log.info(f'Number of storage units: {storage_len}')
    if storage_len <= 0:
        log.info('No storages to retrieve')
        return 
    cpu = parts=mp.cpu_count()
    split_storage_list =  split_to_sublists(storage_units_nr, cpu)
    process_pool = mp.Pool(processes=mp.cpu_count())
    storage = pd.DataFrame()
    try:
        storage_units = process_pool.map(split_to_threads,split_storage_list)
        process_pool.close()
        process_pool.join()
    except Exception as e:
        log.info(e)


def download_unit_storage(start_from=0, end_at=20, overwrite=True):
    storage_units = setup_storage_units(overwrite)
    storage_units_nr = storage_units['EinheitMastrNummer'].values.tolist()
    eeg_nr = storage_units['EegMastrNummer'].values.tolist()
    storage_len = len(storage_units_nr)
    eeg_len = len(eeg_nr)
    log.info(f'Download MaStR Storage')
    log.info(f'Number of storage units: {storage_len}')
    cpu = parts=mp.cpu_count()
    for x in range(len(storage_units_nr)):
        get_unit_storage(storage_units_nr[x])


def split_to_threads(sublist):
    pool = ThreadPool(processes=4)
    results = pool.map(get_unit_storage, sublist)
    pool.close()
    pool.join()
    return results


''' starting batch num /current 1st speichereinheit 5th Aug 2019:    1220000 '''
def setup_storage_units(overwrite=True):   
    data_version = get_data_version()
    if overwrite: 
        if os.path.isfile(fname_storage):
            remove_csv(fname_storage)
    if os.path.isfile(fname_power_unit):
        power_unit = read_power_units(fname_power_unit)
        if not power_unit.empty:
            power_unit = power_unit.drop_duplicates()
            power_unit_storage = power_unit[power_unit.Einheittyp == 'Stromspeichereinheit']
            power_unit_storage.index.names = ['see_id']
            power_unit_storage.reset_index()
            power_unit_storage.index.names = ['id']
            if not power_unit_storage.empty:
                write_to_csv(fname_storage_unit, power_unit_storage)    
            else:
                log.info('No storage units in this dataset. Storage units can be found starting at index: approx. 1 220 000')   
            power_unit.iloc[0:0]
            return power_unit_storage
        else:
            log.info('no storageunits found')
            return pd.DataFrame()
    else:
        power_unit_solar = read_power_units(name_storage)
    return power_unit_solar


def get_unit_storage(mastr_unit_storage):
    """Get Solareinheit from API using GetEinheitStromspeicher."""
    data_version = get_data_version()
    try:
        c = client_bind.GetEinheitStromSpeicher(apiKey=api_key,
                                    marktakteurMastrNummer=my_mastr,
                                    einheitMastrNummer=mastr_unit_storage)
        s = serialize_object(c)
        df = pd.DataFrame(list(s.items()), )
        unit_storage = df.set_index(list(df.columns.values)[0]).transpose()
        unit_storage.reset_index()
        unit_storage.index.names = ['lid']
        unit_storage['version'] = data_version
        unit_storage['timestamp'] = str(datetime.datetime.now())
        write_to_csv(fname_storage, unit_storage)
    except Exception as e:
        return
    return unit_storage