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

from open_mastr.soap_api.config import setup_logger
from open_mastr.soap_api.mastr_general_download import get_mastr_time, get_mastr_time_auth, get_daily_contingent
from open_mastr.soap_api.utils import is_time_blacklisted, fname_solar_unit
from open_mastr.soap_api.mastr_power_unit_download import download_parallel_power_unit, download_power_unit
from open_mastr.soap_api.mastr_wind_download import setup_power_unit_wind, retry_download_unit_wind, retry_download_unit_wind_eeg, download_unit_wind, download_unit_wind_eeg, download_unit_wind_permit
from open_mastr.soap_api.mastr_wind_process import make_wind
from open_mastr.soap_api.mastr_hydro_download import setup_power_unit_hydro, retry_download_unit_hydro, retry_download_unit_hydro_eeg, download_unit_hydro, download_unit_hydro_eeg
from open_mastr.soap_api.mastr_hydro_process import make_hydro
from open_mastr.soap_api.mastr_biomass_download import setup_power_unit_biomass, retry_download_unit_biomass, retry_download_unit_biomass_eeg, download_unit_biomass, download_unit_biomass_eeg
from open_mastr.soap_api.mastr_biomass_process import make_biomass
from open_mastr.soap_api.mastr_gsgk_download import download_unit_gsgk, download_unit_gsgk_eeg
from open_mastr.soap_api.mastr_gsgk_process import make_gsgk
from open_mastr.soap_api.mastr_solar_download import setup_power_unit_solar, download_unit_solar, download_parallel_unit_solar, download_unit_solar_eeg, download_parallel_unit_solar_eeg, read_unit_solar
from open_mastr.soap_api.mastr_solar_process import make_solar
from open_mastr.soap_api.mastr_storage_download import setup_power_unit_storage, retry_download_unit_storage, retry_download_unit_storage_eeg, download_unit_storage, download_unit_storage_eeg
from open_mastr.soap_api.mastr_storage_process import make_storage
#from open_mastr.soap_api.mastr_storage_units_download import download_unit_storage, download_parallel_unit_storage
from open_mastr.soap_api.mastr_nuclear_download import setup_power_unit_nuclear, download_unit_nuclear
from open_mastr.soap_api.mastr_nuclear_process import make_nuclear
from open_mastr.soap_api.mastr_combustion_download import setup_power_unit_combustion, download_unit_combustion, download_unit_combustion_kwk
from open_mastr.soap_api.mastr_combustion_process import make_combustion

import datetime
import time


if __name__ == "__main__":
    from open_mastr.soap_api.utils import DATA_VERSION
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
    # download_power_unit(power_unit_list_len=1000)

    MAX_UNITS = 2

    """Nuclear"""
    # API_MAX_DEMANDS: 5 MAX_UNITS = 2
    setup_power_unit_nuclear()  # Extract from all power units
    download_power_unit(energy_carrier='Kernenergie', power_unit_list_len=MAX_UNITS)
    download_unit_nuclear()
    make_nuclear()

    """Wind"""
    # API_MAX_DEMANDS: 50 MAX_UNITS = 2
    # setup_power_unit_wind()   # Extract from all power units
    # download_power_unit(energy_carrier='Wind', power_unit_list_len=MAX_UNITS)
    # download_unit_wind()
    # download_unit_wind_eeg()
    # download_unit_wind_permit()
    # retry_download_unit_wind()
    # retry_download_unit_wind_eeg()
    # make_wind()

    """Hydro"""
    # API_MAX_DEMANDS: 5 MAX_UNITS = 2
    setup_power_unit_hydro()
    download_power_unit(energy_carrier='Wasser', power_unit_list_len=MAX_UNITS)
    download_unit_hydro()
    download_unit_hydro_eeg()
    make_hydro()

    # """Biomass"""
    # API_MAX_DEMANDS: 5 MAX_UNITS = 2
    setup_power_unit_biomass()  # Extract from all power units
    download_power_unit(energy_carrier='Biomasse', power_unit_list_len=MAX_UNITS)
    download_unit_biomass()
    download_unit_biomass_eeg()
    retry_download_unit_biomass()
    retry_download_unit_biomass_eeg()
    make_biomass()

    """Solar"""
    # setup_power_unit_solar()
    # download_power_unit(energy_carrier='SolareStrahlungsenergie', power_unit_list_len=MAX_UNITS)
    # ''' DEFAULT PARAMS: start_from=0, n_entries=1, parallelism= '''
    # download_parallel_unit_solar(threads=8)
    # downloaded_units = read_unit_solar(fname_solar_unit)[
    #     'EegMastrNummer']
    # download_parallel_unit_solar_eeg(downloaded_units, threads=8)
    # make_solar()

    """Combustion"""
    download_power_unit(energy_carrier='AndereGase', power_unit_list_len=MAX_UNITS)
    download_power_unit(energy_carrier='Braunkohle', power_unit_list_len=MAX_UNITS)
    download_power_unit(energy_carrier='Erdgas', power_unit_list_len=MAX_UNITS)
    download_power_unit(energy_carrier='Mineraloelprodukte', power_unit_list_len=MAX_UNITS)
    download_power_unit(energy_carrier='NichtBiogenerAbfall', power_unit_list_len=MAX_UNITS)
    download_power_unit(energy_carrier='Steinkohle', power_unit_list_len=MAX_UNITS)
    download_power_unit(energy_carrier='Waerme', power_unit_list_len=MAX_UNITS)
    download_unit_combustion()
    download_unit_combustion_kwk()
    make_combustion()
