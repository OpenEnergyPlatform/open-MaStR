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


log = logging.getLogger(__name__)


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
