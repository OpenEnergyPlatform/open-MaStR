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
__author__ = "Ludee; christian-rli; Bachibouzouk; solar-c"
__issue__ = "https://github.com/OpenEnergyPlatform/examples/issues/52"
__version__ = "v0.10.0"

from open_mastr.soap_api.mastr_combustion_download import *
from open_mastr.soap_api.utils import fname_power_unit_combustion, fname_combustion_unit, fname_combustion_kwk, fname_combustion

import logging

log = logging.getLogger(__name__)


def make_combustion():
    """Read combustion data from CSV files. Join data and write to file."""

    power_unit_combustion = read_power_units(fname_power_unit_combustion)
    unit_combustion = read_unit_combustion(fname_combustion_unit)
    unit_combustion_eeg = read_unit_combustion_kwk(fname_combustion_kwk)

    table_combustion = power_unit_combustion.set_index('EinheitMastrNummer') \
        .join(unit_combustion.set_index('EinheitMastrNummer'),
            on='EinheitMastrNummer', how='left', rsuffix='_w') \
        .join(unit_combustion_eeg.set_index('KwkMastrNummer'),
            on='KwkMastrNummer', how='left', rsuffix='_e')

    write_to_csv(fname_combustion, table_combustion)
    log.info(f'Join Biomass to: {fname_combustion}')
