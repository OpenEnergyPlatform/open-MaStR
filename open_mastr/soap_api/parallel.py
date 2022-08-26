# import datetime
# import multiprocessing
# import logging
# import tqdm
#
# from open_mastr.soap_api.utils import is_time_blacklisted
# log = logging.getLogger(__name__)
#
# last_successful_download = datetime.datetime.now()
#
# # Check if worker threads need to be killed
#
#
# def _stop_execution(time_blacklist, timeout):
#     # Last successful execution was more than 10 minutes ago. Server seems
#     # unresponsive, so stop execution permanently by raising an error
#     if last_successful_download + datetime.timedelta(minutes=timeout) < datetime.datetime.now():
#         log.error('No response from server in the last {} minutes. Stopping execution.'.format(timeout))
#         raise ConnectionAbortedError
#     # Stop execution smoothly if current system time is in blacklist by
#     # returning. Calling function can decide to continue running later if
#     # needed
#     if time_blacklist and is_time_blacklisted(last_successful_download.time()):
#         log.info('Current time is in blacklist. Halting.')
#         return True
#     # ... Add more checks here if needed ...
#     return False
#
