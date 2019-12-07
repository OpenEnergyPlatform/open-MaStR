__copyright__ = "© Reiner Lemoine Institut"
__license__ = "GNU Affero General Public License Version 3 (AGPL-3.0)"
__url__ = "https://www.gnu.org/licenses/agpl-3.0.en.html"
__author__ = "Bachibouzouk; solar-c"
__issue__ = "https://github.com/OpenEnergyPlatform/examples/issues/52"
__version__ = "v0.9.0"


import os
import pandas as pd
from datetime import datetime as dt
import logging
log = logging.getLogger(__name__)

DATA_VERSION = 'rli_v2.2.0'
""" Total Count of Power Units (TOTAL_POWER_UNITS) on date (UPDATE_TIMESTAMP) """
TOTAL_POWER_UNITS = 2487585
""" 01.03.2019 """
TIMESTAMP = "1900-01-01 00:00:00.000000"
""" test string: "2019-10-20 00:00:00.000000" """
""" dummy stump for other file names """
fname_template = f'data/bnetza_mastr_{DATA_VERSION}'
ts_path = f'data/update/bnetza_mastr_ts.csv'

""" list of specific power unit file names"""
fname_power_unit = f'{fname_template}_power-unit.csv'

fname_power_unit_wind = f'{fname_template}_power-unit_wind.csv'
fname_wind_unit = f'{fname_template}_unit-wind.csv'
fname_wind_eeg = f'{fname_template}_unit-wind-eeg.csv'
fname_wind_permit = f'{fname_template}_unit-wind-permit.csv'
fname_wind = f'{fname_template}_wind.csv'
fname_wind_fail = f'{fname_template}_wind_fail.csv'

fname_power_unit_hydro = f'{fname_template}_power-unit_hydro.csv'
fname_hydro_unit = f'{fname_template}_unit-hydro.csv'
fname_hydro_eeg = f'{fname_template}_unit-hydro-eeg.csv'
fname_hydro = f'{fname_template}_hydro.csv'
fname_hydro_fail_u = f'{fname_template}_hydro_fail_u.csv'
fname_hydro_fail_e = f'{fname_template}_hydro_fail_e.csv'

fname_power_unit_biomass = f'{fname_template}_power-unit_biomass.csv'
fname_biomass_unit = f'{fname_template}_unit-biomass.csv'
fname_biomass_eeg = f'{fname_template}_unit-biomass-eeg.csv'
fname_biomass = f'{fname_template}_biomass.csv'
fname_biomass_fail = f'{fname_template}_biomass_fail.csv'

fname_solar = f'{fname_template}_power-unit_solar.csv'
fname_solar_unit = f'{fname_template}_unit-solar.csv'
fname_solar_eeg = f'{fname_template}_solar-eeg.csv'
fname_solar_eeg_unit = f'{fname_template}_unit-solar-eeg.csv'

fname_storage = f'{fname_template}_storage.csv'
fname_storage_unit = f'{fname_template}_unit-storage.csv'


def get_data_version():
    """Return current data version """
    return DATA_VERSION


def split_to_sublists(mylist, length, parts):
    """Read data from config file.

    Parameters
    ----------
    mylist : list
        list to split
    length : int
        length of mylist
    parts : int
        number of desired sublists
    """
    s, r = divmod(length, parts)
    k = s+1
    return [mylist[i:i+k] for i in range(0, r*k, k)] + [mylist[i:i+s] for i in range(r*k, length, s)]


def write_to_csv(csv_name, df):
    """Create CSV file or append data to it.

    Parameters
    ----------
    csv_name : str
        Name of file.
    df : DataFrame
        Sata saved to file.
    append : bool
        If False create a new CSV file (default), else append to it.
    """
    #if os.path.exists(os.path.dirname(csv_name)):
    #    os.remove(os.path.dirname(csv_name))

    if not os.path.exists(os.path.dirname(csv_name)):
        os.makedirs(os.path.dirname(csv_name))

    with open(csv_name, mode='a', encoding='utf-8') as file:
        df.to_csv(file, sep=';',
                  mode='a',
                  header=file.tell() == 0,
                  line_terminator='\n',
                  encoding='utf-8')


def remove_csv(csv_name):
    """Remove csv file with given filename.

    Parameters
    ----------
    csv_name : String
        name of csv file to remove
    """
    if os.path.isfile(csv_name):
        os.remove(csv_name)

def read_power_units(csv_name):
    """Read Stromerzeugungseinheit from CSV file.

    Parameters
    ----------
    csv_name : str
        Name of file.

    Returns
    -------
    power_unit : DataFrame
        Stromerzeugungseinheit.
    """

    if os.path.isfile(csv_name):
        power_unit = pd.read_csv(
            csv_name,
            header=0,
            sep=';',
            index_col=False,
            encoding='utf-8',
            dtype={
                'id': str,
                'lid': str,
                'EinheitMastrNummer': str,
                'Name': str,
                'Einheitart': str,
                'Einheittyp': str,
                'Standort': str,
                'Bruttoleistung': str,
                'Erzeugungsleistung': str,
                'EinheitBetriebsstatus': str,
                'Anlagenbetreiber': str,
                'EegMastrNummer': str,
                'KwkMastrNummer': str,
                'SpeMastrNummer': str,
                'GenMastrNummer': str,
                'BestandsanlageMastrNummer': str,
                'NichtVorhandenInMigriertenEinheiten': str,
                'StatisikFlag' : str,
                'version': str,
                'timestamp': str})

        power_unit_cnt = power_unit['timestamp'].count()
        log.info(f'Read {power_unit_cnt} power-unit from {csv_name}')
        return power_unit

    else:
        log.info(f'Error reading {csv_name}')


def read_timestamp(wind=False):
    """Read latest timestamp from powerunit file.

    Parameters
    ----------
    wind : bool
        determines which powerunit file should be used : wind or all units.
    """
    if wind == True:
        if os.path.isfile(fname_wind_unit):
            """ get timestamp wind """
            ts = pd.read_csv(fname_wind_unit,header=0, index_col=0)
            ts = ts.timestamp.iloc[-1]
            return ts
    else:
        if os.path.isfile(fname_power_unit):
            """ get timestamp """
            ts = pd.read_csv(fname_power_unit, header=0, index_col=0)
            ts = ts.timestamp.iloc[-1]
            return ts
    return False