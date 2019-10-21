__copyright__ = "© Reiner Lemoine Institut"
__license__ = "GNU Affero General Public License Version 3 (AGPL-3.0)"
__url__ = "https://www.gnu.org/licenses/agpl-3.0.en.html"
__author__ = "Ludee; christian-rli"
__issue__ = "https://github.com/OpenEnergyPlatform/examples/issues/52"
__version__ = "v0.8.0"


import os
import logging
log = logging.getLogger(__name__)

DATA_VERSION = 'rli_v2.0.3'

""" dummy stump for other file names """
fname_dummy = f'data/bnetza_mastr_{DATA_VERSION}'

""" list of specific power unit file names"""
fname_all_units = f'{fname_dummy}_all_units.csv'

fname_hydro_eeg_unit = f'{fname_dummy}_unit-hydro-eeg.csv'
fname_hydro_unit = f'{fname_dummy}_unit-hydro.csv'
fname_hydro = f'{fname_dummy}_hydro.csv'
fname_hydro_eeg = f'{fname_dummy}_hydro-eeg.csv'

fname_solar = f'{fname_dummy}_solar.csv'
fname_solar_unit = f'{fname_dummy}_unit-solar.csv'
fname_solar_eeg = f'{fname_dummy}_solar-eeg.csv'
fname_solar_eeg_unit = f'{fname_dummy}_unit-solar-eeg.csv'

fname_wind = f'{fname_dummy}_wind.csv'
fname_wind_eeg = f'{fname_dummy}_wind-eeg.csv'
fname_wind_unit = f'{fname_dummy}_unit-wind.csv'
fname_wind_eeg_unit = f'{fname_dummy}_unit-wind-eeg.csv'
fname_wind_permit = f'{fname_dummy}_unit-wind-permit.csv'

fname_biomass = f'{fname_dummy}_biomass.csv'
fname_biomass_unit = f'{fname_dummy}_unit-biomass.csv'
fname_biomass_eeg = f'{fname_dummy}_biomass-eeg.csv'
fname_biomass_eeg_unit = f'{fname_dummy}_unit-biomass-eeg.csv'

fname_storage = f'{fname_dummy}_storage.csv'
fname_storage_unit = f'{fname_dummy}_unit-storage.csv'


def get_data_version():
    return DATA_VERSION


def split_to_sublists(mylist, length, parts):
        s, r = divmod(length, parts)
        k = s+1
        return [mylist[i:i+k] for i in range(0, r*k, k)] + [mylist[i:i+s] for i in range(r*k, length, s)]

def get_filename_csv_see():
    return csv_see


def set_filename_csv_see(types, overwrite=True):
    global csv_see, csv_see_hydro, csv_see_solar, csv_see_biomass, csv_see_wind, csv_see_storage, csv_see_postal, csv_see_address
    myinput = ""
    return_type = ""
    if not overwrite:
        print('CAUTION! Define a file name or press enter for default. If the file name exists, the file will not be overwritten. Data will be appended at the end of the existing file.')
        myinput = input()

    if types is 'power_units':
        csv_see = csv_see_dummy+'_power-unit'+myinput+'.csv'
        return_type = csv_see
    elif types is 'solar_units':
        return_type = csv_see_dummy+'_solar-units'+myinput+'.csv'
    elif types is 'hydro_units':
        return_type = csv_see_dummy+'_hydro-units'+myinput+'.csv'
    elif types is 'biomass_units':
        return_type = csv_see_dummy+'_biomass-units'+myinput+'.csv'
    elif types is 'eeg_units':
        return_type = csv_see_dummy+'_eeg_units'+myinput+'.csv'
    elif types is 'wind_units':
        return_type = csv_see_dummy+'_wind'+myinput+'.csv'
    elif types is 'storage_units':
        return_type = csv_see_dummy+'_storage-units'+myinput+'.csv'
    elif types is 'postal':
        return_type = csv_see_dummy+'_postal'+myinput+'.csv'
    elif types is 'address':
        return_type = csv_see_dummy+'_address'+myinput+'.csv'
    return return_type


def set_corrected_path(mypath):
    global csv_see
    csv_see = mypath


def get_correct_filepath():
    check = False
    while not check:
        log.info(f'CAUTION!! If your general power units file name DIFFERS from {csv_see} please ENTER NOW the complete path -- ELSE PRESS ENTER')
        csv_path = input()
        if os.path.isfile(csv_path):
            check = True
    return csv_path

def get_correct_solar_filepath():
    log.info(f'CAUTION!! If your SOLAR power units file name DIFFERS from {csv_see_solar} please ENTER NOW the complete path -- ELSE PRESS ENTER')
    csv_path = input()
    if not csv_path:
        return csv_see_solar
    return csv_path 


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
    if os.path.isfile(csv_name):
        os.remove(csv_name)
