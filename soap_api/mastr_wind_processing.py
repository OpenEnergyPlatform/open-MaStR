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
__version__ = "v0.8.0"

from soap_api.sessions import mastr_session
#from mastr_power_unit_download import read_power_units
from soap_api.utils import write_to_csv, get_data_version
from soap_api.mastr_wind_process import make_wind

import pandas as pd
import numpy as np
import datetime
import os
import re
from zeep.helpers import serialize_object
from functools import partial
from collections import OrderedDict 
import logging
log = logging.getLogger(__name__)

import multiprocessing as mp 

""" import variables """
from soap_api.utils import fname_all_units, fname_wind, fname_wind_unit, fname_wind_eeg, fname_wind_eeg_unit, fname_wind_permit, remove_csv, read_power_units, split_to_sublists

"""SOAP API"""
client, client_bind, token, user = mastr_session()
api_key = token
my_mastr = user

def do_wind(eeg,start_from=0):
  if os.path.isfile(fname_wind_unit):
    wind_unit = read_power_units(fname_wind_unit)
    wind_unit = wind_unit.drop_duplicates()
    wind_unit.index.names = ['see_id']
    wind_unit.reset_index()
    wind_unit.index.names = ['id']
    wind_unit.iloc[0:0]

    if wind_unit.empty:
      log.info('no windunits found')
      return pd.DataFrame()
  else:
    log.info('windunit file not found')
  download_wind(units=wind_unit, start_from=start_from, eeg=eeg)
  download_wind_permit(units=wind_unit, start_from=start_from)
  log.info("Finished download wind")
  make_wind(eeg)
  log.info("DONE :)")

def download_wind(units, start_from, eeg=False):
    retry_max = 0
    wind_list_len = len(units)
    log.info('Download MaStR Wind')
    log.info(f'Number of unit_wind: {wind_list_len}')

    sublists = split_to_sublists(units, len(units), mp.cpu_count()*2)
    pool = mp.Pool(processes=mp.cpu_count()*2)
    pool.map(partial(process_partionier, eeg=eeg), sublists)
    pool.close()
    pool.join()

def process_partionier(units, eeg=False):
    wind_list = units['EinheitMastrNummer'].values.tolist()
    if eeg==True:
      wind_list_eeg = units['EegMastrNummer'].values.tolist()
    else:
      wind_list_eeg = wind_list
    max_units = len(units)
    for i in range(1, max_units, 1):
      try:
          unit_wind = get_power_unit_wind(mastr_unit_wind=wind_list[i], mastr_unit_eeg=wind_list_eeg[i],eeg=eeg)
          write_to_csv(fname_wind, unit_wind[0])
          if not len(unit_wind[1])==0:
            write_to_csv(fname_wind_eeg, unit_wind[1])
      except:
          log.exception(f'Download failed unit_wind ({i}): {wind_list[i]}')


def get_power_unit_wind(mastr_unit_wind, mastr_unit_eeg, eeg=False):
    """Get Windeinheit from API using GetEinheitWind.

    Parameters
    ----------
    mastr_unit_wind : object
        Wind from EinheitMastrNummerId.

    Returns
    -------
    unit_wind : DataFrame
        Windeinheit.
    """
    data_version = get_data_version()
    unit_wind = pd.DataFrame()
    unit_wind_eeg = pd.DataFrame()
    if eeg== True:
      try:
        c = client_bind.GetAnlageEegWind(apiKey=api_key,
                                     marktakteurMastrNummer=my_mastr,
                                     eegMastrNummer=mastr_unit_eeg)
        s = serialize_object(c)
        dicts = list((s['VerknuepfteEinheit'][0]).items())[0]
        s['VerknuepfteEinheit']= dicts[1]
        s['EinheitMastrNummer'] = s.pop('VerknuepfteEinheit')

        """for i in s.items():
            if isinstance(i[1], OrderedDict):  
              title = re.findall('[A-Z][^A-Z]*', str(i[0]))          
              if len(i[1].keys()) == len(title):"""


              #s[i[0]] = 
        df = pd.DataFrame(list(s.items()), )
        unit_wind_eeg = df.set_index(list(df.columns.values)[0]).transpose()
        unit_wind_eeg.reset_index()
        unit_wind_eeg.index.names = ['lid']
        unit_wind_eeg["version"] = data_version
        unit_wind_eeg["timestamp"] = str(datetime.datetime.now())
      except Exception as e:
          log.info('Download eeg failed for %s', mastr_unit_wind)

    try:
      c = client_bind.GetEinheitWind(apiKey=api_key,
                                       marktakteurMastrNummer=my_mastr,
                                       einheitMastrNummer=mastr_unit_wind)
      c = disentangle_manufacturer(c)
      del c['Hersteller']
      s = serialize_object(c)
      df = pd.DataFrame(list(s.items()), )
      unit_wind = df.set_index(list(df.columns.values)[0]).transpose()
      unit_wind.reset_index()
      unit_wind.index.names = ['lid']
      unit_wind['version'] = data_version
      unit_wind['timestamp'] = str(datetime.datetime.now())
    except Exception as e:
          log.info('Download failed for %s', mastr_unit_wind)

    return unit_wind, unit_wind_eeg

def download_wind_permit(units, start_from=0, overwrite=False):
        """Download unit_wind_permit using GetEinheitGenehmigung request."""
        df_all = pd.DataFrame()
        unit_wind_list = units['GenMastrNummer'].values.tolist()
        unit_wind_list_len = len(unit_wind_list)
        for i in range(start_from, unit_wind_list_len, 1):
          if not pd.isna(unit_wind_list[i]):
            try:
                unit_wind_permit = get_unit_wind_permit(unit_wind_list[i])
                for k,v in unit_wind_permit.VerknuepfteEinheiten.items():
                  df_new = pd.DataFrame.from_dict(v)
                  df = pd.DataFrame()
                  gennr = df_new.size * [unit_wind_permit.GenMastrNummer.iloc[0]]
                  dates = df_new.size * [unit_wind_permit.Datum.iloc[0]]
                  types = df_new.size * [unit_wind_permit.Art.iloc[0]]
                  authority = df_new.size * [(unit_wind_permit.Behoerde.iloc[0]).translate({ord(','):None})]
                  file_num = df_new.size * [unit_wind_permit.Aktenzeichen.iloc[0]]
                  frist = df_new.size * [unit_wind_permit.Frist.iloc[0]['Wert']]
                  water_num = df_new.size * [unit_wind_permit.WasserrechtsNummer.iloc[0]]
                  water_date = df_new.size * [unit_wind_permit.WasserrechtAblaufdatum.iloc[0]['Wert']]
                  reporting_date = df_new.size * [unit_wind_permit.Meldedatum.iloc[0]]
                  df = pd.DataFrame(
                      {
                      'GenMastrNummer':gennr,
                      'Datum': dates,
                      'Art': types,
                      'Behoerde': authority,
                      'Aktenzeichen': file_num,
                      'Frist': frist,
                      'WasserrechtsNummer': water_num,
                      'WasserrechtAblaufdatum': water_date,
                      'Meldedatum': reporting_date
                      })
                  df_all = pd.concat([df_new, df.reindex(df_new.index)], axis=1)
                  df_all = df_all.rename({'MaStRNummer':'EinheitMastrNummer'}, axis=1) 
                  #df_all.set_index(['MaStRNummer'], inplace=True)
                  write_to_csv(fname_wind_permit,df_all)
            except:
                log.exception(f'Download failed unit_wind_permit ({i}): {unit_wind_list[i]}')

def get_unit_wind_permit(mastr_wind_permit):
    """Get Genehmigung-Wind-Wind from API using GetEinheitGenehmigung.

    Parameters
    ----------
    mastr_wind_permit : str
        Genehmigungnummer MaStR

    Returns
    -------
    unit_wind_permit : DataFrame
        Genehmigung-Einheit-Wind.
    """
    data_version = get_data_version()
    c = client_bind.GetEinheitGenehmigung(apiKey=api_key,
                                     marktakteurMastrNummer=my_mastr,
                                     genMastrNummer=mastr_wind_permit)
    s = serialize_object(c)
    df = pd.DataFrame(list(s.items()), )
    unit_wind_permit = df.set_index(list(df.columns.values)[0]).transpose()
    unit_wind_permit.reset_index()
    unit_wind_permit.index.names = ['lid']
    unit_wind_permit["version"] = data_version
    unit_wind_permit["timestamp"] = str(datetime.datetime.now())
    unit_wind_permit = unit_wind_permit.replace('\r', '', regex=True)
    return unit_wind_permit

def disentangle_manufacturer(wind_unit):
    wu = wind_unit
    try:
        wu['HerstellerID'] = wind_unit['Hersteller']['Id']
        wu['HerstellerName'] = wind_unit['Hersteller']['Wert']
        return(wu)
    except:
        print("This wind_unit contains no OrderedDict for 'Hersteller'")
        return(wind_unit)