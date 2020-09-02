import datetime
import multiprocessing
import logging

from soap_api.utils import write_to_csv
from soap_api.utils import is_time_blacklisted
log = logging.getLogger(__name__)

def parallel_download(unit_list, func, filename, threads=4, timeout=10, time_blacklist=True):
    """Download a list of units using a pool of threads

    Maps a download function for a single unit onto a list of
    candidate units that are downloaded in parallel.

    Arguments
    ---------
    unit_list : Iterable of 'EinheitMastrNummer' 
        of units to download
    func : callable function
        Function to download an individual unit from the list,
        i.e. get_power_unit_xxx()
    filename : str
        CSV file to write retrieved units to
    threads : int
        number of threads to download with
    timeout : int
        retry for this amount of minutes after the last successful
        query before stopping
    time_blacklist : bool
        exit as soon as current time is blacklisted
    """

    with multiprocessing.Pool(threads) as pool:
        last_successful = datetime.datetime.now()
        for unit in pool.imap_unordered(func, unit_list):
            now = datetime.datetime.now()
            # Check if data was retrieved successfully
            if unit is not None:
                last_successful = now
                # TODO Check if low-level access can be done for all subfunctions
                log.info('Unit {} sucessfully retrieved.'.format(unit.loc[1, 'EinheitMastrNummer']))
                write_to_csv(filename, unit)
            # Last successful execution was more than 10 minutes ago, so stop execution
            if last_successful + datetime.timedelta(minutes=timeout) < datetime.datetime.now():
                log.error('No response from server in the last {} minutes. Stopping execution.'.format(timeout))
                raise ConnectionAbortedError
            # Stop execution if current system time is in blacklist
            if time_blacklist and is_time_blacklisted(now.time()):
                log.info('Current time is in blacklist. Halting.')
                break
