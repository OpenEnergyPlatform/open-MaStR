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
__author__ = "Ludee; christian-rli; Bachibouzouk; solar-c"
__issue__ = "https://github.com/OpenEnergyPlatform/examples/issues/52"
__version__ = "v0.9.0"

import logging
from soap_api.mastr_hydro_download import read_unit_hydro, read_unit_hydro_eeg
from soap_api.utils import (
    write_to_csv,
    fname_power_unit_hydro,
    fname_hydro_unit,
    fname_hydro_unit_eeg,
    fname_hydro_all
)

log = logging.getLogger(__name__)


def make_hydro():
    """Read hydro data from CSV files. Join data and write to file."""

    power_unit_hydro = read_power_units(fname_power_unit_hydro)
    unit_hydro = read_unit_hydro(fname_hydro_unit)
    unit_hydro_eeg = read_unit_hydro_eeg(fname_hydro_unit_eeg)

    table_hydro = power_unit_hydro.set_index('EinheitMastrNummer') \
        .join(unit_hydro.set_index('EinheitMastrNummer'),
              on='EinheitMastrNummer', how='left', rsuffix='_w') \
        .join(unit_hydro_eeg.set_index('EegMastrNummer'),
              on='EegMastrNummer', how='left', rsuffix='_e')

    write_to_csv(fname_hydro_all, table_hydro)
    log.info(f'Join Hydro to: {fname_hydro_all}')
