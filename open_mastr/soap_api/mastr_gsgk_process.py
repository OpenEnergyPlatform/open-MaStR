#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
BNetzA - MaStR Processing GeoSolarthermieGrubenKlaerschlamm

Process MaStR data from CSV

SPDX-License-Identifier: AGPL-3.0-or-later
"""

__copyright__ = "© Reiner Lemoine Institut"
__license__ = "GNU Affero General Public License Version 3 (AGPL-3.0)"
__url__ = "https://www.gnu.org/licenses/agpl-3.0.en.html"
__author__ = "Ludee; christian-rli; Bachibouzouk; solar-c"
__issue__ = "https://github.com/OpenEnergyPlatform/examples/issues/52"
__version__ = "v0.10.0"

from open_mastr.soap_api.mastr_gsgk_download import *
from open_mastr.soap_api.utils import fname_power_unit_gsgk, fname_gsgk_unit, fname_gsgk_eeg, fname_gsgk

import logging

log = logging.getLogger(__name__)


def make_gsgk():
    """Read gsgk data from CSV files. Join data and write to file."""

    power_unit_gsgk = read_power_units(fname_power_unit_gsgk)
    unit_gsgk = read_unit_gsgk(fname_gsgk_unit)
    unit_gsgk_eeg = read_unit_gsgk_eeg(fname_gsgk_eeg)

    table_gsgk = power_unit_gsgk.set_index('EinheitMastrNummer') \
        .join(unit_gsgk.set_index('EinheitMastrNummer'),
            on='EinheitMastrNummer', how='left', rsuffix='_w') \
        .join(unit_gsgk_eeg.set_index('EegMastrNummer'),
            on='EegMastrNummer', how='left', rsuffix='_e')

    write_to_csv(fname_gsgk, table_gsgk)
    log.info(f'Join GeoSolarthermieGrubenKlaerschlamm to: {fname_gsgk}')
