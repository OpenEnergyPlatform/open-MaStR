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
__author__ = "Ludee; christian-rli"
__issue__ = "https://github.com/OpenEnergyPlatform/examples/issues/52"
__version__ = "v0.7.0"

from sessions import mastr_session
from mastr_power_unit_download import read_power_units
from utils import split_to_sublists,get_data_version, write_to_csv, get_filename_csv_see, set_filename_csv_see, get_correct_filepath, set_corrected_path, remove_csv

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
    cpu = parts=mp.cpu_count()
    split_storage_list =  split_to_sublists(storage_units_nr,storage_len,cpu)
    process_pool = mp.Pool(processes=mp.cpu_count())
    storage = pd.DataFrame()
    try:
        storage_units = process_pool.map(split_to_threads,split_storage_list)
        process_pool.close()
        process_pool.join()
    except Exception as e:
        log.info(e)

def download_unit_storage(start_from=0, end_at=20, overwrite=False):
    storage_units = setup_storage_units(overwrite)
    from utils import csv_see_storage
    storage_units_nr = storage_units['EinheitMastrNummer'].values.tolist()
    eeg_nr = storage_units['EegMastrNummer'].values.tolist()
    storage_len = len(storage_units_nr)
    eeg_len = len(eeg_nr)
    log.info(f'Download MaStR Storage')
    log.info(f'Number of storage units: {storage_len}')
    cpu = parts=mp.cpu_count()
    split_storage_list =  split_to_sublists(storage_units_nr,storage_len,cpu)
    for x in range(len(storage_units_nr)):
        get_unit_storage(storage_units_nr[x])


def split_to_threads(sublist):
    pool = ThreadPool(processes=10)
    results = pool.map(get_unit_storage, sublist)
    pool.close()
    pool.join()
    return results


''' starting batch num /current 1st speichereinheit 5th Aug 2019:    1220000 '''
def setup_storage_units(overwrite=True):   
    data_version = get_data_version()
    csv_see = get_correct_filepath()
    set_corrected_path(csv_see)
    csv_see_storage = set_filename_csv_see('storage_units', overwrite)
    if overwrite: 
        if os.path.isfile(csv_see_storage):
            remove_csv(csv_see_storage)
    if os.path.isfile(csv_see):
        power_unit = read_power_units(csv_see)
        if not power_unit.empty:
            power_unit = power_unit.drop_duplicates()
            power_unit_storage = power_unit[power_unit.Einheittyp == 'Stromspeichereinheit']
            power_unit_storage.index.names = ['see_id']
            power_unit_storage.reset_index()
            power_unit_storage.index.names = ['id']
            if not power_unit_storage.empty:
                write_to_csv(csv_see_storage, power_unit_storage)    
            else:
                log.info('No storage units in this dataset. Storage units can be found starting at index: approx. 1 220 000')   
            power_unit.iloc[0:0]
            return power_unit_storage
        else:
            log.info('no storageunits found')
            return pd.DataFrame()
    else:
        power_unit_solar = read_power_units(csv_see_storage)
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
        write_to_csv(f'data/bnetza_mastr_{data_version}_storage_units.csv', unit_storage)
    except Exception as e:
        return
    return unit_storage


def prepare_data(units_speicher, units_solar):
    storage_postal = []
    solar_postal = []
    for x in range(len(units_speicher)):
        postal = re.findall(r'\b[0-9]{5}', units_speicher[x][0])
        if postal and units_speicher[x][1]:
            storage_postal.append([postal[0], units_speicher[x][1]])
    for y in range(len(units_solar)):
        postal = re.findall(r'\b[0-9]{5}', units_solar[y][0])
        if postal and postal != None and units_solar[y][1] and units_solar[y][1] != None:
            solar_postal.append([postal[0], units_solar[y][1]])
    return [pd.DataFrame(storage_postal), pd.DataFrame(solar_postal)]


def get_solarunit_storages(overwrite=True):
    data_version = get_data_version()
    csv_see = get_correct_filepath()
    set_corrected_path(csv_see)
    csv_see_postal = set_filename_csv_see('postal', overwrite)
    csv_see_address = set_filename_csv_see('address', overwrite)
    if overwrite:
        remove_csv(csv_see_postal)
        remove_csv(csv_see_address)
    if os.path.isfile(csv_see):
        power_unit = read_power_units(csv_see)
        units_solar = power_unit[power_unit.Einheittyp == 'Solareinheit'][['Standort', 'EinheitMastrNummer']].values.tolist()
        units_speicher = power_unit[power_unit.Einheittyp=='Stromspeichereinheit'][['Standort', 'EinheitMastrNummer']].values.tolist()
        units_solar_add = pd.DataFrame(units_solar)
        units_solar_add.columns= ['Standort', 'EinheitMastrNummer solar']
        units_speicher_add = pd.DataFrame(units_speicher) 
        units_speicher_add.columns = ['Standort', 'EinheitMastrNummer storage']

        all_units_add = pd.merge(units_solar_add, units_speicher_add, on='Standort')
        all_units_add.columns = ['Standort', 'EinheitMastrNummer storage', 'EinheitMastrNummer solar']
        all_units_add =  all_units_add.groupby( ['Standort','EinheitMastrNummer solar'])['EinheitMastrNummer storage'].apply(list)

        storage_postal, solar_postal = prepare_data(units_speicher, units_solar)
        storage_postal.columns=['postal', 'EinheitMastrNummer storage']
        storage_postal = storage_postal.groupby('postal')['EinheitMastrNummer storage'].apply(list)
        solar_postal.columns=['postal', 'EinheitMastrNummer solar']
        solar_postal = solar_postal.groupby('postal')['EinheitMastrNummer solar'].apply(list)

        all_postals = pd.merge(solar_postal, storage_postal, on='postal')
        dv = get_data_version()
        write_to_csv(f'data/{dv}_postal_solar_storage.csv', all_postals)
        write_to_csv(f'data/{dv}_address_solar_storage.csv', all_units_add)


# load and prepare data, geocode
def get_geocode_address():
    csv_see = get_correct_filepath()
    if os.path.isfile(csv_see):
        power_unit = read_power_units(csv_see)
        units_solar = pd.DataFrame(power_unit[power_unit.Einheittyp == 'Solareinheit'][['Standort', 'EinheitMastrNummer']].values.tolist())
        units_speicher = pd.DataFrame(power_unit[power_unit.Einheittyp=='Stromspeichereinheit'][['Standort', 'EinheitMastrNummer']].values.tolist())
    
    units_solar.columns = ['Standort', 'MaStR']
    units_speicher.columns = ['Standort', 'MaStR']
    request_geo_loc(units_solar, '_solar')
    request_geo_loc(units_speicher, '_speicher')


# use nominatim to geocode address as [lat, long], index with MaStR Nr
def request_geo_loc(liste, string):
    dv = get_data_version()
    basic_url = 'https://nominatim.openstreetmap.org/search?q='
    end_url = '&format=xml&polygon=1&addressdetails=1'
    empty = False
    add = ""
    for i,r in liste.iterrows():
        if empty:
            break
        num = [int(i) for i in r.Standort.split() if i.isdigit()] 
        street_pos = [(i,c) for i,c in enumerate(r.Standort.split()) if c.isdigit()]
        try:
            street = r.Standort[:street_pos[0][0]]
        except Exception as e:
            continue
        if num[0] < 1000:
            try:
                index = (r.Standort).find(str(num[0]))
                street = (r.Standort)[0:index-1]
                index = (r.Standort).find(str(num[1]))
                length = len(str(num[1]))
                ort = (r.Standort)[(index+int(length)+1):]
                add = str(num[0])+"+"+street+","+ort
            except Exception as e:
                continue
        if add!="":
            res = requests.get(basic_url+add+end_url)
            root = ElementTree.fromstring(res.content)
            for child in root:
                elem = pd.DataFrame()
                if child.attrib != {}:
                    elem =elem.append({'MaStR':r.MaStR, 'lat':child.attrib['lat'], 'lon':child.attrib['lon']}, ignore_index=True)
                    print(r.MaStR + " " + child.attrib['lat'])
                    write_to_csv(f'data/{dv}_geocoding'+string+'.csv', elem)
                    time.sleep(2)
                continue