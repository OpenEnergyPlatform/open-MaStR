#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
BNetzA - MaStR Processing Biomass

Process MaStR data from CSV

SPDX-License-Identifier: AGPL-3.0-or-later
"""

__copyright__ = "© Reiner Lemoine Institut"
__license__ = "GNU Affero General Public License Version 3 (AGPL-3.0)"
__url__ = "https://www.gnu.org/licenses/agpl-3.0.en.html"
__author__ = "Ludee; christian-rli"
__issue__ = "https://github.com/OpenEnergyPlatform/examples/issues/52"
__version__ = "v0.7.0"

from mastr_biomass_download import *

import logging
log = logging.getLogger(__name__)


def make_biomass():
    """Read biomass data from CSV files. Join data and write to file."""
    data_version = get_data_version()
    csv_see_biomass = f'data/bnetza_mastr_{data_version}_power-unit-biomass.csv'
    csv_unit_biomass = f'data/bnetza_mastr_{data_version}_unit-biomass.csv'
    csv_unit_biomass_eeg = f'data/bnetza_mastr_{data_version}_unit-biomass-eeg.csv'
    csv_biomass = f'data/bnetza_mastr_{data_version}_biomass.csv'

    power_unit_biomass = read_power_units(csv_see_biomass)
    unit_biomass = read_unit_biomass(csv_unit_biomass)
    unit_biomass_eeg = read_unit_biomass_eeg(csv_unit_biomass_eeg)

    table_biomass = power_unit_biomass.set_index('EinheitMastrNummer') \
        .join(unit_biomass.set_index('EinheitMastrNummer'),
              on='EinheitMastrNummer', how='left', rsuffix='_w') \
        .join(unit_biomass_eeg.set_index('EegMastrNummer'),
              on='EegMastrNummer', how='left', rsuffix='_e')

    write_to_csv(csv_biomass, table_biomass)
    log.info(f'Join Biomass to: {csv_biomass}')
