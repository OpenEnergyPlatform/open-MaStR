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
__version__ = "v0.10.0"

from soap_api.mastr_solar_download import *
from soap_api.utils import fname_power_unit_solar, fname_solar_unit, fname_solar_eeg, fname_solar

import logging
log = logging.getLogger(__name__)


def make_solar():
    """Read solar data from CSV files. Join data and write to file."""
    data_version = get_data_version()
    csv_solar = f'data/bnetza_mastr_{data_version}_solar.csv'

    power_unit_solar = read_power_units(fname_power_unit_solar)
    unit_solar = read_unit_solar(fname_solar_unit)
    power_unit_eeg = read_power_units(fname_solar_eeg)
    unit_solar_eeg = read_unit_solar_eeg(fname_solar_eeg)

    table_solar = power_unit_solar.set_index('EinheitMastrNummer') \
        .join(unit_solar.set_index('EinheitMastrNummer'),
              on='EinheitMastrNummer', how='left', rsuffix='_w') \
        .join(unit_solar_eeg.set_index('EegMastrNummer'),
              on='EegMastrNummer', how='left', rsuffix='_e')

    write_to_csv(csv_solar, table_solar)
    log.info(f'Join Solar to: {fname_solar}')
