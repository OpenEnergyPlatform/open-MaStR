#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
BNetzA - MaStR Processing Wind

Process MaStR data from CSV

SPDX-License-Identifier: AGPL-3.0-or-later
"""

__copyright__ = "© Reiner Lemoine Institut"
__license__ = "GNU Affero General Public License Version 3 (AGPL-3.0)"
__url__ = "https://www.gnu.org/licenses/agpl-3.0.en.html"
__author__ = "Ludee; christian-rli; Bachibouzouk; solar-c"
__issue__ = "https://github.com/OpenEnergyPlatform/examples/issues/52"
__version__ = "v0.10.0"

from soap_api.mastr_wind_download import *
from soap_api.utils import fname_power_unit_wind, fname_wind_unit, fname_wind_eeg, fname_wind_permit, fname_wind

import logging

log = logging.getLogger(__name__)


def make_wind():
    """Read wind data from CSV files. Join data and write to file.

    Parameters
    ----------

    Returns
    -------

    """

    data_version = get_data_version()
    power_unit_wind = read_power_unit_wind(fname_power_unit_wind)

    unit_wind = read_unit_wind(fname_wind_unit)
    unit_wind = unit_wind.drop_duplicates(subset=['EinheitMastrNummer'])
    unit_wind_cnt = unit_wind['EinheitMastrNummer'].count()
    log.info(f'Filter {unit_wind_cnt} unique unit-wind')

    unit_wind_eeg = read_unit_wind_eeg(fname_wind_eeg)

    unit_wind_permit = read_unit_wind_permit(fname_wind_permit)
    unit_wind_permit = unit_wind_permit.drop_duplicates(subset=['GenMastrNummer'])
    unit_wind_permit_cnt = unit_wind['GenMastrNummer'].count()
    log.info(f'Filter {unit_wind_permit_cnt} unique unit-wind-permit')

    log.info("Joining tables...")

    df_wind = power_unit_wind.join(unit_wind.set_index('EinheitMastrNummer'),
                                                                   on='EinheitMastrNummer', how='left', lsuffix='', rsuffix='_w')
    df_wind_cnt = df_wind['timestamp'].count()
    log.info(f'Join {df_wind_cnt} wind (puw - uw)')

    df_wind = df_wind.join(unit_wind_eeg.set_index('EegMastrNummer'),
                                                       on='EegMastrNummer', how='left', lsuffix='', rsuffix='_e')
    df_wind_cnt = df_wind['timestamp'].count()
    log.info(f'Join {df_wind_cnt} wind (puw - uw - uwe)')

    df_wind = df_wind.join(unit_wind_permit.set_index('GenMastrNummer'),
                                                       on='GenMastrNummer', how='left', lsuffix='', rsuffix='_p')
    df_wind_cnt = df_wind['timestamp'].count()
    log.info(f'Join {df_wind_cnt} wind (puw - uw - uwe - uwp)')

    df_wind.reset_index()
    df_wind.index.name = 'w-id'
    df_wind['version_m'] = data_version
    df_wind['timestamp_m'] = str(datetime.datetime.now())

    write_to_csv(fname_wind, df_wind)
    df_wind_cnt = df_wind['timestamp_m'].count()
    log.info(f'Write {df_wind_cnt} Wind to {fname_wind}')