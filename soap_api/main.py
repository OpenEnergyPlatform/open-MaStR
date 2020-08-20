#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
OEP MaStR Processing

Read data from MaStR API, process, and write to file and OEP
The data will be downloaded to the folder /data.

SPDX-License-Identifier: AGPL-3.0-or-later
"""

__copyright__ = "\xa9 Reiner Lemoine Institut"
__license__ = "GNU Affero General Public License Version 3 (AGPL-3.0)"
__url__ = "https://www.gnu.org/licenses/agpl-3.0.en.html"
__author__ = "Ludee; christian-rli"
__issue__ = "https://github.com/OpenEnergyPlatform/examples/issues/52"
__version__ = "v0.9.0"

from soap_api.config import setup_logger
from soap_api.mastr_general_download import get_mastr_time, get_mastr_time_auth, get_daily_contingent
from soap_api.mastr_power_unit_download import download_parallel_power_unit, download_power_unit
from soap_api.mastr_wind_download import setup_power_unit_wind, retry_download_unit_wind, retry_download_unit_wind_eeg, download_unit_wind, download_unit_wind_eeg, download_unit_wind_permit
from soap_api.mastr_wind_process import make_wind
from soap_api.mastr_hydro_download import setup_power_unit_hydro, retry_download_unit_hydro, retry_download_unit_hydro_eeg, download_unit_hydro, download_unit_hydro_eeg
from soap_api.mastr_hydro_process import make_hydro
from soap_api.mastr_biomass_download import setup_power_unit_biomass, retry_download_unit_biomass, retry_download_unit_biomass_eeg, download_unit_biomass, download_unit_biomass_eeg
from soap_api.mastr_biomass_process import make_biomass
from soap_api.mastr_solar_download import setup_power_unit_solar, download_unit_solar, download_parallel_unit_solar, download_unit_solar_eeg, download_parallel_unit_solar_eeg
from soap_api.mastr_solar_process import make_solar
from soap_api.mastr_storage_units_download import download_unit_storage, download_parallel_unit_storage
from soap_api.mastr_nuclear_download import setup_power_unit_nuclear, download_unit_nuclear
from soap_api.mastr_nuclear_process import make_nuclear
# from soap_api.mastr_wind_processing import do_wind
import time


if __name__ == "__main__":
    from soap_api.utils import DATA_VERSION
    """logging"""
    log = setup_logger()
    start_time = time.time()
    log.info(f'MaStR script started with data version: {DATA_VERSION}')
    get_mastr_time()
    get_mastr_time_auth()
    get_daily_contingent()
    """OEP"""
    #metadata = oep_session()

    """MaStR Einheiten"""
    '''Get maximum number from the web interface and setup utils.py'''
    # download_power_unit()

    '''WARNING: Batch download may cause a database error. Extended limit required!'''
    '''DEFAULT PARAMS: batch_size=20000, limit=2000, start_from=0'''
    # download_parallel_power_unit(
    #    batch_size=10000,
    #    limit=2000,
    #    wind=False,
    #    update=False,
    #    overwrite=False,
    #    start_from=0)

    """Wind"""
    # setup_power_unit_wind()
    # download_unit_wind()
    # download_unit_wind_eeg()
    # download_unit_wind_permit()
    # retry_download_unit_wind()
    # retry_download_unit_wind_eeg()
    # make_wind()

    """Hydro"""
    # setup_power_unit_hydro()
    # download_unit_hydro()
    # download_unit_hydro_eeg()
    # retry_download_unit_hydro()
    # retry_download_unit_hydro_eeg()
    # make_hydro()

    """Biomass"""
    # setup_power_unit_biomass()
    # download_unit_biomass()
    # download_unit_biomass_eeg()
    # retry_download_unit_biomass()
    # retry_download_unit_biomass_eeg()
    # make_biomass()

    """Solar"""
    # setup_power_unit_solar()
    # ''' DEFAULT PARAMS: start_from=0, n_entries=1, parallelism=12 '''
    # download_parallel_unit_solar(
    #     start_from=0,
    #     n_entries=1,
    #     parallelism=6)
    # download_parallel_unit_solar_eeg(
    #    start_from=0,
    #    n_entries=1,
    #    parallelism=6)
    # make_solar()

    """Storages"""
    # get_solarunit_storages()
    # download_parallel_unit_storage()

    """Nuclear"""
    # setup_power_unit_nuclear()
    # download_unit_nuclear()
    # make_nuclear()

    """close"""
    log.info('MaSTR script successfully executed in {:.2f} seconds'
             .format(time.time() - start_time))
