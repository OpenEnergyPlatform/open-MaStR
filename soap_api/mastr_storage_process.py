#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
BNetzA - MaStR Processing Storage

Process MaStR data from CSV

SPDX-License-Identifier: AGPL-3.0-or-later
"""

__copyright__ = "© Reiner Lemoine Institut"
__license__ = "GNU Affero General Public License Version 3 (AGPL-3.0)"
__url__ = "https://www.gnu.org/licenses/agpl-3.0.en.html"
__author__ = "Ludee; christian-rli; Bachibouzouk; solar-c"
__issue__ = "https://github.com/OpenEnergyPlatform/examples/issues/52"
__version__ = "v0.9.0"

from soap_api.mastr_storage_download import *
from soap_api.utils import fname_power_unit_storage, fname_storage_unit, fname_storage_eeg, fname_storage

import logging

log = logging.getLogger(__name__)


def make_storage():
    """Read storage data from CSV files. Join data and write to file."""

    power_unit_storage = read_power_units(fname_power_unit_storage)
    unit_storage = read_unit_storage(fname_storage_unit)
    unit_storage_eeg = read_unit_storage_eeg(fname_storage_eeg)

    table_storage = power_unit_storage.set_index('EinheitMastrNummer') \
        .join(unit_storage.set_index('EinheitMastrNummer'),
            on='EinheitMastrNummer', how='left', rsuffix='_w') \
        .join(unit_storage_eeg.set_index('EegMastrNummer'),
            on='EegMastrNummer', how='left', rsuffix='_e')

    write_to_csv(fname_storage, table_storage)
    log.info(f'Join Storage to: {fname_storage}')
