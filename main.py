#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
OEP MaStR Processing

Read data from MaStR API, process, and write to file and OEP

SPDX-License-Identifier: AGPL-3.0-or-later
"""

__copyright__ = "© Reiner Lemoine Institut"
__license__ = "GNU Affero General Public License Version 3 (AGPL-3.0)"
__url__ = "https://www.gnu.org/licenses/agpl-3.0.en.html"
__author__ = "Ludee; christian-rli"
__issue__ = "https://github.com/OpenEnergyPlatform/examples/issues/52"
__version__ = "v0.5.0"

from config import setup_logger
from mastr_wind_download import download_power_unit, download_unit_wind, download_unit_wind_eeg
from mastr_wind_process import make_wind

import time

"""version"""
DATA_VERSION = '0.8'

"""logging"""
log = setup_logger()
start_time = time.time()
log.info(f'Script started with data version: {DATA_VERSION}.')

"""OEP"""
# metadata = oep_session()

"""MaStR Wind"""
download_power_unit()
download_unit_wind()
download_unit_wind_eeg()
make_wind()

"""close"""
log.info('...Script successfully executed in {:.2f} seconds.'
         .format(time.time() - start_time))
log.info('...Script finished. Goodbye!')
