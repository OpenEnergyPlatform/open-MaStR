__copyright__ = "© Reiner Lemoine Institut"
__license__ = "GNU Affero General Public License Version 3 (AGPL-3.0)"
__url__ = "https://www.gnu.org/licenses/agpl-3.0.en.html"
__author__ = "Ludee; christian-rli"
__issue__ = "https://github.com/OpenEnergyPlatform/examples/issues/52"
__version__ = "v0.8.0"


DATA_VERSION = 'rli_v2.0.3'


import os
import logging
log = logging.getLogger(__name__)
import csv

csv_see_dummy = f'data/bnetza_mastr_{DATA_VERSION}'
csv_see = f'data/bnetza_mastr_{DATA_VERSION}_power-unit.csv'

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