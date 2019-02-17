#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
BNetzA - MaStR Processing Solar

Process MaStR data from CSV

SPDX-License-Identifier: AGPL-3.0-or-later
"""

__copyright__ = "© Reiner Lemoine Institut"
__license__ = "GNU Affero General Public License Version 3 (AGPL-3.0)"
__url__ = "https://www.gnu.org/licenses/agpl-3.0.en.html"
__author__ = "Ludee; christian-rli"
__issue__ = "https://github.com/OpenEnergyPlatform/examples/issues/52"
__version__ = "v0.6.0"

from mastr_solar_download import *

import logging
log = logging.getLogger(__name__)


def make_solar():
    """Read solar data from CSV files. Join data and write to file."""
    data_version = get_data_version()
    csv_see_solar = f'data/bnetza_mastr_{data_version}_power-unit-solar.csv'
    csv_unit_solar = f'data/bnetza_mastr_{data_version}_unit-solar.csv'
    csv_unit_solar_eeg = f'data/bnetza_mastr_{data_version}_unit-solar-eeg.csv'
    csv_solar = f'data/bnetza_mastr_{data_version}_solar.csv'
    log.info('Join Solar')

    power_unit_solar = read_power_units(csv_see_solar)
    unit_solar = read_unit_solar(csv_unit_solar)
    unit_solar_eeg = read_unit_solar_eeg(csv_unit_solar_eeg)

    table_solar = power_unit_solar.set_index('EinheitMastrNummer') \
        .join(unit_solar.set_index('EinheitMastrNummer'),
              on='EinheitMastrNummer', how='left', rsuffix='_w') \
        .join(unit_solar_eeg.set_index('EegMastrNummer'),
              on='EegMastrNummer', how='left', rsuffix='_e')

    write_to_csv(csv_solar, table_solar)
    log.info(f'Join Solar to: {csv_solar}')
