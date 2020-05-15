#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
BNetzA - MaStR Processing Kernenergie

Process MaStR data from CSV

SPDX-License-Identifier: AGPL-3.0-or-later
"""

__copyright__ = "© Reiner Lemoine Institut"
__license__ = "GNU Affero General Public License Version 3 (AGPL-3.0)"
__url__ = "https://www.gnu.org/licenses/agpl-3.0.en.html"
__author__ = "Ludee"
__issue__ = "https://github.com/OpenEnergyPlatform/examples/issues/52"
__version__ = "v0.9.0"

from soap_api.mastr_nuclear_download import *
from soap_api.utils import fname_power_unit_nuclear, fname_nuclear_unit, fname_nuclear

import logging

log = logging.getLogger(__name__)


def make_nuclear():
    """Read nuclear data from CSV files. Join data and write to file."""

    power_unit_nuclear = read_power_units(fname_power_unit_nuclear)
    unit_nuclear = read_unit_nuclear(fname_nuclear_unit)

    table_nuclear = power_unit_nuclear.set_index('EinheitMastrNummer') \
        .join(unit_nuclear.set_index('EinheitMastrNummer'),
            on='EinheitMastrNummer', how='left', rsuffix='_w')

    write_to_csv(fname_nuclear, table_nuclear)
    log.info(f'Join Kernenergie to: {fname_nuclear}')
