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

from soap_api.mastr_hydro_download import *
from soap_api.utils import fname_power_unit_hydro, fname_hydro_unit, fname_hydro_eeg, fname_hydro

import logging

log = logging.getLogger(__name__)


def make_hydro():
    """Read hydro data from CSV files. Join data and write to file."""

    data_version = get_data_version()
    power_unit_hydro = read_power_unit_hydro(fname_power_unit_hydro)
    power_unit_hydro = power_unit_hydro.drop_duplicates(subset=['EinheitMastrNummer'])
    power_unit_hydro_cnt = power_unit_hydro['EinheitMastrNummer'].count()
    log.info(f'Filter {power_unit_hydro_cnt} unique power-unit_hydro')

    unit_hydro = read_unit_hydro(fname_hydro_unit)
    unit_hydro = unit_hydro.drop_duplicates(subset=['EinheitMastrNummer'])
    unit_hydro_cnt = unit_hydro['EinheitMastrNummer'].count()
    log.info(f'Filter {unit_hydro_cnt} unique unit-hydro')

    unit_hydro_eeg = read_unit_hydro_eeg(fname_hydro_eeg)
    unit_hydro_eeg = unit_hydro_eeg.drop_duplicates(subset=['EegMastrNummer'])
    unit_hydro_eeg_cnt = unit_hydro_eeg['EegMastrNummer'].count()
    log.info(f'Filter {unit_hydro_eeg_cnt} unique unit-hydro-eeg')

    log.info("Joining tables...")
    df_hydro = power_unit_hydro.join(unit_hydro.set_index('EinheitMastrNummer'),
                                                                   on='EinheitMastrNummer', how='left', lsuffix='', rsuffix='_w')
    df_hydro_cnt = df_hydro['timestamp'].count()
    log.info(f'Join {df_hydro_cnt} hydro (puh - uh)')

    df_hydro = df_hydro.join(unit_hydro_eeg.set_index('EegMastrNummer'),
                                                       on='EegMastrNummer', how='left', lsuffix='', rsuffix='_e')
    df_hydro_cnt = df_hydro['timestamp'].count()
    log.info(f'Join {df_hydro_cnt} hydro (puh - uh - uhe)')

    df_hydro.reset_index()
    df_hydro.index.name = 'h-id'
    df_hydro['version_m'] = data_version
    df_hydro['timestamp_m'] = str(datetime.datetime.now())

    write_to_csv(fname_hydro, df_hydro)
    df_hydro_cnt = df_hydro['timestamp_m'].count()
    log.info(f'Write {df_hydro_cnt} hydro to {fname_hydro}')
