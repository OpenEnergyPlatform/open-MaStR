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

import pandas as pd
import numpy as np
import datetime
import os
from zeep.helpers import serialize_object

import logging
log = logging.getLogger(__name__)

""" import variables """
from utils import fname_all_units, fname_wind, fname_wind_unit, fname_wind_eeg, fname_wind_eeg_unit, fname_wind_permit, remove_csv, read_power_units

"""SOAP API"""
client, client_bind, token, user = mastr_session()
api_key = token
my_mastr = user

def do_wind(start_from, eeg):
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

  wind = download_wind(units=wind_unit, start_from=start_from, eeg=False)
  if not wind.iloc[0].empty:
    write_to_csv(fname_wind, wind.iloc[0])
  elif not wind.iloc[1].empty:
    write_to_csv(fname_wind_eeg, wind.iloc[1])


def download_wind(units, start_from, eeg=False):
    wind_list = units['EinheitMastrNummer'].values.tolist()
    wind_list_len = len(wind_list)
    log.info('Download MaStR Wind')
    log.info(f'Number of unit_wind: {wind_list_len}')

    for i in range(start_from, wind_list_len, 1):
        try:
            unit_wind = get_power_unit_wind(mastr_unit_wind=wind_list[i], eeg=eeg)
            write_to_csv(fname_wind, unit_wind[0])
            if not unit_wind[1].empty:
              log.info(" wind unit found xy")
            #if not isempty(unit_wind[1]):
            #  write_to_csv(fname_wind_eeg, unit_wind[1])
        except:
            log.exception(f'Download failed unit_wind ({i}): {wind_list[i]}')


def get_power_unit_wind(mastr_unit_wind, eeg=False):
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
                                     eegMastrNummer=mastr_wind_eeg)
        s = serialize_object(c)
        df = pd.DataFrame(list(s.items()), )
        unit_wind_eeg = df.set_index(list(df.columns.values)[0]).transpose()
        unit_wind_eeg.reset_index()
        unit_wind_eeg.index.names = ['lid']
        unit_wind_eeg["version"] = data_version
        unit_wind_eeg["timestamp"] = str(datetime.datetime.now())
        log.info("succss")
      except Exception as e:
          log.info('Download failed for %s', mastr_unit_wind)

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
      log.info("success")
    except Exception as e:
          log.info('Download failed for %s', mastr_unit_wind)

    return unit_wind, unit_wind_eeg


def disentangle_manufacturer(wind_unit):
    wu = wind_unit
    try:
        wu['HerstellerID'] = wind_unit['Hersteller']['Id']
        wu['HerstellerName'] = wind_unit['Hersteller']['Wert']
        return(wu)
    except:
        print("This wind_unit contains no OrderedDict for 'Hersteller'")
        return(wind_unit)