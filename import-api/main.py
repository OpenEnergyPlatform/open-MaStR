
#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
OEP MaStR Processing

Read data from MaStR API, process, and write to file and OEP
The data will be downloaded to the folder /data.

SPDX-License-Identifier: AGPL-3.0-or-later
"""

__copyright__ = "© Reiner Lemoine Institut"
__license__ = "GNU Affero General Public License Version 3 (AGPL-3.0)"
__url__ = "https://www.gnu.org/licenses/agpl-3.0.en.html"
__author__ = "Ludee; christian-rli"
__issue__ = "https://github.com/OpenEnergyPlatform/examples/issues/52"
__version__ = "v0.7.0"

from config import setup_logger
from mastr_power_unit_download import download_parallel_power_unit
from mastr_wind_download import download_unit_wind, download_unit_wind_eeg, download_unit_wind_permit
from mastr_wind_process import make_wind
from mastr_hydro_download import download_unit_hydro, download_unit_hydro_eeg
from mastr_hydro_process import make_hydro
from mastr_biomass_download import download_unit_biomass, download_unit_biomass_eeg
from mastr_biomass_process import make_biomass
from mastr_solar_download import download_unit_solar, download_parallel_unit_solar, download_unit_solar_eeg, download_parallel_unit_solar_eeg
from mastr_solar_process import make_solar
from mastr_storage_units_download import get_storage_groups_by_address_or_postal, download_unit_storage, download_parallel_unit_storage

import time


if __name__ == "__main__":
    from utils import DATA_VERSION
    """logging"""
    log = setup_logger()
    start_time = time.time()
    log.info(f'MaStR script started with data version: {DATA_VERSION}')

    """OEP"""
    #metadata = oep_session()

    """MaStR Einheiten"""

    ''' DEFAULT PARAMS: power_unit_list_len=100000, limit=2000, batch_size=20000, start_from=0, overwrite=False '''
    ''' CURRENT MAX INDEX FOR VAR start_from and power_unit_list_len: 1814000 '''
    download_parallel_power_unit(start_from=1220000,power_unit_list_len=4000000, batch_size=20000,overwrite=False, all_units=True)



    """Wind"""
    download_unit_wind()
    download_unit_wind_eeg()
    download_unit_wind_permit()
    make_wind()

    """Hydro"""
    download_unit_hydro()
    download_unit_hydro_eeg()
    make_hydro()

    """Biomass"""
    download_unit_biomass()
    download_unit_biomass_eeg()
    make_biomass()

    """Solar"""
    ''' DEFAULT PARAMS: start_from=0, n_entries=1, parallelism=300, cpu_factor=1, overwrite=False '''
    download_parallel_unit_solar_eeg(overwrite=True, n_entries=10000)
    download_parallel_unit_solar(overwrite=True, n_entries=500000)
        
    """ Storages"""
    download_parallel_unit_storage()
    download_unit_storage(overwrite=True)
    get_storage_groups_by_address_or_postal()

    """close"""
    log.info('MaSTR script successfully executed in {:.2f} seconds'
             .format(time.time() - start_time))
