#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
BNetzA - MaStR Processing

Process MaStR data from CSV

SPDX-License-Identifier: AGPL-3.0-or-later
"""

__copyright__ = "© Reiner Lemoine Institut"
__license__ = "GNU Affero General Public License Version 3 (AGPL-3.0)"
__url__ = "https://www.gnu.org/licenses/agpl-3.0.en.html"
__author__ = "Ludee; christian-rli"
__issue__ = "https://github.com/OpenEnergyPlatform/examples/issues/52"
__version__ = "v0.5.0"

from mastr_wind_download import *

import logging
log = logging.getLogger(__name__)


def make_wind():
    """Read wind data from CSV files. Join data and write to file."""
    data_version = get_data_version()
    # csv_see = f'data/bnetza_mastr_{data_version}_stromerzeuger.csv'
    csv_see_wind = f'data/bnetza_mastr_{data_version}_stromerzeuger_wind.csv'
    csv_unit_wind = f'data/bnetza_mastr_{data_version}_windeinheit.csv'
    csv_unit_wind_eeg = f'data/bnetza_mastr_{data_version}_windeeg.csv'
    csv_wind = f'data/bnetza_mastr_{data_version}_wind.csv'

    # power_unit = read_power_units(csv_see)
    power_unit_wind = read_power_units(csv_see_wind)
    unit_wind = read_unit_wind(csv_unit_wind)
    unit_wind_eeg = read_unit_wind_eeg(csv_unit_wind_eeg)

    table_wind = power_unit_wind.set_index('EinheitMastrNummer') \
        .join(unit_wind.set_index('EinheitMastrNummer'),
              on='EinheitMastrNummer', how='left', rsuffix='_w') \
        .join(unit_wind_eeg.set_index('EegMastrNummer'),
              on='EegMastrNummer', how='left', rsuffix='_e')
    log.info(f'Join data.')

    write_to_csv(csv_wind, table_wind)
    log.info(f'Process MaStR Wind to: {csv_wind}.')
