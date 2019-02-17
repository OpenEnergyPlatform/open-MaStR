#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
BNetzA - MaStR Processing Hydro

Process MaStR data from CSV

SPDX-License-Identifier: AGPL-3.0-or-later
"""

__copyright__ = "© Reiner Lemoine Institut"
__license__ = "GNU Affero General Public License Version 3 (AGPL-3.0)"
__url__ = "https://www.gnu.org/licenses/agpl-3.0.en.html"
__author__ = "Ludee; christian-rli"
__issue__ = "https://github.com/OpenEnergyPlatform/examples/issues/52"
__version__ = "v0.7.0"

from mastr_hydro_download import *

import logging
log = logging.getLogger(__name__)


def make_hydro():
    """Read hydro data from CSV files. Join data and write to file."""
    data_version = get_data_version()
    csv_see_hydro = f'data/bnetza_mastr_{data_version}_power-unit-hydro.csv'
    csv_unit_hydro = f'data/bnetza_mastr_{data_version}_unit-hydro.csv'
    csv_unit_hydro_eeg = f'data/bnetza_mastr_{data_version}_unit-hydro-eeg.csv'
    csv_hydro = f'data/bnetza_mastr_{data_version}_hydro.csv'
    log.info('Join Hydro')

    power_unit_hydro = read_power_units(csv_see_hydro)
    unit_hydro = read_unit_hydro(csv_unit_hydro)
    unit_hydro_eeg = read_unit_hydro_eeg(csv_unit_hydro_eeg)

    table_hydro = power_unit_hydro.set_index('EinheitMastrNummer') \
        .join(unit_hydro.set_index('EinheitMastrNummer'),
              on='EinheitMastrNummer', how='left', rsuffix='_w') \
        .join(unit_hydro_eeg.set_index('EegMastrNummer'),
              on='EegMastrNummer', how='left', rsuffix='_e')

    write_to_csv(csv_hydro, table_hydro)
    log.info(f'Join Hydro to: {csv_hydro}')
