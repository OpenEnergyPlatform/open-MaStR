import datetime
import multiprocessing
import logging
import tqdm

from open_mastr.soap_api.utils import write_to_csv
from open_mastr.soap_api.utils import is_time_blacklisted
log = logging.getLogger(__name__)

last_successful_download = datetime.datetime.now()

# Check if worker threads need to be killed


def _stop_execution(time_blacklist, timeout):
    # Last successful execution was more than 10 minutes ago. Server seems
    # unresponsive, so stop execution permanently by raising an error
    if last_successful_download + datetime.timedelta(minutes=timeout) < datetime.datetime.now():
        log.error('No response from server in the last {} minutes. Stopping execution.'.format(timeout))
        raise ConnectionAbortedError
    # Stop execution smoothly if current system time is in blacklist by
    # returning. Calling function can decide to continue running later if
    # needed
    if time_blacklist and is_time_blacklisted(last_successful_download.time()):
        log.info('Current time is in blacklist. Halting.')
        return True
    # ... Add more checks here if needed ...
    return False


def _reset_timeout():
    last_successful_download = datetime.datetime.now()


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

    _reset_timeout()
    with multiprocessing.Pool(threads) as pool:
        for unit in tqdm.tqdm(pool.imap_unordered(func, unit_list), total=len(unit_list)):
            # Check if data was retrieved successfully
            if unit is not None:
                _reset_timeout()
                write_to_csv(filename, unit)
            if _stop_execution(time_blacklist, timeout) is True:
                break
