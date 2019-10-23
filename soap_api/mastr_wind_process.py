#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
BNetzA - MaStR Processing Wind

Process MaStR data from CSV

SPDX-License-Identifier: AGPL-3.0-or-later
"""

__copyright__ = "© Reiner Lemoine Institut"
__license__ = "GNU Affero General Public License Version 3 (AGPL-3.0)"
__url__ = "https://www.gnu.org/licenses/agpl-3.0.en.html"
__author__ = "Ludee; christian-rli"
__issue__ = "https://github.com/OpenEnergyPlatform/examples/issues/52"
__version__ = "v0.8.0"

from mastr_wind_download import *
from utils import fname_wind, fname_wind_unit, fname_wind_eeg, fname_wind_eeg_unit, fname_wind_permit, read_power_units
import pandas as pd 

import logging
log = logging.getLogger(__name__)


def make_wind(eeg=False):
    """Read wind data from CSV files. Join data and write to file."""
    data_version = get_data_version()
    csv_wind = f'data/bnetza_mastr_{data_version}_wind_all.csv'

    power_unit_wind = read_power_units(fname_wind_unit)
    unit_wind = read_unit_wind(fname_wind)
    if eeg==True:
        unit_wind_eeg = read_unit_wind_eeg(fname_wind_eeg)
    unit_wind_permit = read_unit_wind_permit(fname_wind_permit)
    log.info("Joining tables...")
    table_wind = power_unit_wind \
    .merge(unit_wind_permit.set_index('EinheitMastrNummer'), on= ['Einheitart', 'Einheittyp', 'GenMastrNummer','EinheitMastrNummer'], how='left') \
    .merge(unit_wind_eeg.set_index('EegMastrNummer'), on=['EegMastrNummer'], how='left') \

    """table_wind = power_unit_wind \
    .join(unit_wind_permit.set_index('GenMastrNummer'),
              on='GenMastrNummer', how='left', rsuffix='_p') \
    .join(unit_wind.set_index('EinheitMastrNummer'),
             on='EinheitMastrNummer', how='left', rsuffix='_w')
    if eeg==True:
        table_wind = table_wind.join(unit_wind_eeg.set_index('EegMastrNummer'),
              on='EegMastrNummer', how='left', rsuffix='_e') \
"""
    write_to_csv(csv_wind, table_wind)
    log.info(f'Join Wind to: {csv_wind}')
