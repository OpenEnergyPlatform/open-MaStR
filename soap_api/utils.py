__copyright__ = "© Reiner Lemoine Institut"
__license__ = "GNU Affero General Public License Version 3 (AGPL-3.0)"
__url__ = "https://www.gnu.org/licenses/agpl-3.0.en.html"
__author__ = "Ludee; Bachibouzouk; solar-c"
__issue__ = "https://github.com/OpenEnergyPlatform/examples/issues/52"
__version__ = "v0.10.0"


import os
import pandas as pd
import logging
import datetime
from soap_api.config import get_filenames, get_project_home_dir, get_data_config


log = logging.getLogger(__name__)

DATA_VERSION = get_data_config()["data_version"]

""" Total Count of Power Units (TOTAL_POWER_UNITS) on date (UPDATE_TIMESTAMP) """
TOTAL_POWER_UNITS = get_data_config()["total_power_units"]

DATA_PATH = os.path.join(get_project_home_dir(), "data", DATA_VERSION)


TIMESTAMP = "1900-01-01 00:00:00.00000"

""" list of specific power unit file names"""
fname_template = f'data/bnetza_mastr_{DATA_VERSION}'
fname_power_unit = f'{fname_template}_power-unit.csv'
filenames = get_filenames()


# Wind
fname_power_unit_wind = os.path.join(DATA_PATH, filenames["raw"]["wind"]["basic"])
fname_wind_unit = os.path.join(DATA_PATH, filenames["raw"]["wind"]["extended"])
fname_wind_eeg = os.path.join(DATA_PATH, filenames["raw"]["wind"]["eeg"])
fname_wind_permit = os.path.join(DATA_PATH, filenames["raw"]["wind"]["permit"])
fname_wind = os.path.join(DATA_PATH, filenames["raw"]["wind"]["joined"])
fname_wind_fail_u = os.path.join(DATA_PATH, filenames["raw"]["wind"]["extended_fail"])
fname_wind_fail_e = os.path.join(DATA_PATH, filenames["raw"]["wind"]["eeg_fail"])
fname_wind_fail_p = os.path.join(DATA_PATH, filenames["raw"]["wind"]["permit_fail"])

# Wasser
fname_power_unit_hydro = os.path.join(DATA_PATH, filenames["raw"]["hydro"]["basic"])
fname_hydro_unit = os.path.join(DATA_PATH, filenames["raw"]["hydro"]["extended"])
fname_hydro_eeg = os.path.join(DATA_PATH, filenames["raw"]["hydro"]["eeg"])
fname_hydro = os.path.join(DATA_PATH, filenames["raw"]["hydro"]["joined"])
fname_hydro_fail_u = os.path.join(DATA_PATH, filenames["raw"]["hydro"]["extended_fail"])
fname_hydro_fail_e = os.path.join(DATA_PATH, filenames["raw"]["hydro"]["eeg_fail"])

# Biomasse
fname_power_unit_biomass = os.path.join(DATA_PATH, filenames["raw"]["biomass"]["basic"])
fname_biomass_unit = os.path.join(DATA_PATH, filenames["raw"]["biomass"]["extended"])
fname_biomass_eeg = os.path.join(DATA_PATH, filenames["raw"]["biomass"]["eeg"])
fname_biomass = os.path.join(DATA_PATH, filenames["raw"]["biomass"]["joined"])
fname_biomass_fail_u = os.path.join(DATA_PATH, filenames["raw"]["biomass"]["extended_fail"])
fname_biomass_fail_e = os.path.join(DATA_PATH, filenames["raw"]["biomass"]["eeg_fail"])

# SolareStrahlungsenergie
fname_power_unit_solar = os.path.join(DATA_PATH, filenames["raw"]["solar"]["basic"])
fname_solar_unit = os.path.join(DATA_PATH, filenames["raw"]["solar"]["extended"])
fname_solar_eeg = os.path.join(DATA_PATH, filenames["raw"]["solar"]["eeg"])
fname_solar = os.path.join(DATA_PATH, filenames["raw"]["solar"]["joined"])
fname_solar_fail_u = os.path.join(DATA_PATH, filenames["raw"]["solar"]["extended_fail"])
fname_solar_fail_e = os.path.join(DATA_PATH, filenames["raw"]["solar"]["eeg_fail"])

# Speicher
fname_power_unit_storage = os.path.join(DATA_PATH, filenames["raw"]["storage"]["basic"])
fname_storage_unit = os.path.join(DATA_PATH, filenames["raw"]["storage"]["extended"])
fname_storage_eeg = os.path.join(DATA_PATH, filenames["raw"]["storage"]["eeg"])
fname_storage = os.path.join(DATA_PATH, filenames["raw"]["storage"]["joined"])
fname_storage_fail_u = os.path.join(DATA_PATH, filenames["raw"]["storage"]["extended_fail"])
fname_storage_fail_e = os.path.join(DATA_PATH, filenames["raw"]["storage"]["eeg_fail"])

# Kernenergie
fname_power_unit_nuclear = os.path.join(DATA_PATH, filenames["raw"]["nuclear"]["basic"])
fname_nuclear_unit = os.path.join(DATA_PATH, filenames["raw"]["nuclear"]["extended"])
fname_nuclear = os.path.join(DATA_PATH, filenames["raw"]["nuclear"]["joined"])

# Geothermie Solarthermie Grubengas Klaerschlamm (AKA GeoSolarthermieGrubenKlaerschlamm)
fname_power_unit_gsgk = os.path.join(DATA_PATH, filenames["raw"]["gsgk"]["basic"])
fname_gsgk_unit = os.path.join(DATA_PATH, filenames["raw"]["gsgk"]["extended"])
fname_gsgk_eeg = os.path.join(DATA_PATH, filenames["raw"]["gsgk"]["eeg"])
fname_gsgk = os.path.join(DATA_PATH, filenames["raw"]["gsgk"]["joined"])
fname_gsgk_fail_u = os.path.join(DATA_PATH, filenames["raw"]["gsgk"]["extended_fail"])
fname_gsgk_fail_e = os.path.join(DATA_PATH, filenames["raw"]["gsgk"]["eeg_fail"])

# AndereGase Braunkohle Erdgas Mineraloelprodukte NichtBiogenerAbfall Steinkohle Waerme (AKA Verbrennung)
fname_power_unit_combustion = os.path.join(DATA_PATH, filenames["raw"]["combustion"]["basic"])
fname_combustion_unit = os.path.join(DATA_PATH, filenames["raw"]["combustion"]["extended"])
fname_combustion_kwk = os.path.join(DATA_PATH, filenames["raw"]["combustion"]["kwk"])
fname_combustion = os.path.join(DATA_PATH, filenames["raw"]["combustion"]["joined"])
fname_combustion_fail_u = os.path.join(DATA_PATH, filenames["raw"]["combustion"]["extended_fail"])
fname_combustion_fail_e = os.path.join(DATA_PATH, filenames["raw"]["combustion"]["kwk_fail"])


def get_data_version():
    """Return current data version. """
    return DATA_VERSION

def split_to_sublists(mylist, num_sublists):
    """Split elements in a list into num_sublists.

    Parameters
    ----------
    mylist : list
        list to split
    num_sublists : int
        number of desired sublists
    """
    length = len(mylist)
    s, r = divmod(length, num_sublists)
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
    # if os.path.exists(os.path.dirname(csv_name)):
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
                'pu-id': int,
                'lid': int,
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
                'StatisikFlag': str,
                'version': str,
                'timestamp': str})

        power_unit_cnt = power_unit['timestamp'].count()
        log.info(f'Read {power_unit_cnt} Stromerzeugungseinheiten from {csv_name}')
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
            ts = pd.read_csv(fname_wind_unit, header=0, index_col=0)
            ts = ts.timestamp.iloc[-1]
            return ts
    else:
        if os.path.isfile(fname_power_unit):
            """ get timestamp """
            ts = pd.read_csv(fname_power_unit, header=0, index_col=0)
            ts = ts.timestamp.iloc[-1]
            return ts
    return False


def is_time_blacklisted(time):
    times_blacklist = [
        ('8:00', '18:00'),  # BNetzA Business hours
        ('23:30', '00:10'),  # Daily database cronjob
        # Add more if needed...
    ]

    # check if time is in a given interval between upper and lower
    def in_interval(lower, upper):
        # Convert str to datatime object
        def parse_time(t): 
            return datetime.datetime.strptime(t, "%H:%M").time()
        lower = parse_time(lower)
        upper = parse_time(upper)

        # Handle interval that spans over midnight (i.e. 23:30-0:30)
        if lower > upper:
            return (time <= upper or time >= lower)
        # Handle all other intevals
        return (lower <= time and upper >= time)

    # check if time is in interval for each interval in the blacklist
    in_interval = [in_interval(lower, upper) for lower, upper in times_blacklist]
    return any(in_interval)
