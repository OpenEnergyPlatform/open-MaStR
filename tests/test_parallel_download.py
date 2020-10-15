# __copyright__ = "© Reiner Lemoine Institut"
# __license__ = "GNU Affero General Public License Version 3 (AGPL-3.0)"
# __url__ = "https://www.gnu.org/licenses/agpl-3.0.en.html"
# __author__ = "Bachibouzouk"
# __version__ = "v0.10.0"
#
# import time
# import pandas as pd
# import os
#
# from soap_api.sessions import API_MAX_DEMANDS
#
# from soap_api.mastr_power_unit_download import (
#     get_power_unit,
#     download_power_unit,
#     download_parallel_power_unit
# )
#
# TEST_DATA = 'tests/data/bnetza_mastr_test_power-unit.csv'
#
#
# def remove_test_data(fname=TEST_DATA):
#     time.sleep(0.1)
#     os.remove(fname)
#     time.sleep(0.1)
#     try:
#         os.rmdir(os.path.dirname(fname))
#     except OSError:
#         print('could not delete {}'.format(os.path.dirname(fname)))
#
#
# def test_get_power_unit_return_data_frame():
#     power_units = get_power_unit(0, 10)
#     assert isinstance(power_units, pd.DataFrame)
#
#
# def test_get_power_unit_return_correct_number():
#     n = 13
#     power_units = get_power_unit(0, n)
#     assert len(power_units.index) == n
#
#
# def test_get_power_unit_starting_not_from_zero():
#     n = 13
#     power_units = get_power_unit(1000, n)
#     assert len(power_units.index) == n
#
#
# def test_get_power_unit_largest_limit():
#     power_units = get_power_unit(0, API_MAX_DEMANDS)
#     assert len(power_units.index) == API_MAX_DEMANDS
#
#
# def test_download_power_unit_unique_mastr_number():
#     n = 100
#     # download the data
#     download_power_unit(n, ofname=TEST_DATA)
#     # load the data
#     df = pd.read_csv(TEST_DATA, sep=';')
#     # remove the data
#     remove_test_data()
#     assert len(df.EinheitMastrNummer.unique()) == n
#     assert len(df.EegMastrNummer.unique()) == n
#
#
# def test_parallel_download_power_unit_unique_mastr_number_case1():
#     """Number is exactly the same as the API_MAX_DEMAND"""
#     n = API_MAX_DEMANDS
#     # download the data
#     download_parallel_power_unit(n, ofname=TEST_DATA)
#     # load the data
#     df = pd.read_csv(TEST_DATA, sep=';')
#     # remove the data
#     remove_test_data()
#     assert len(df.EinheitMastrNummer.unique()) == n
#     assert len(df.EegMastrNummer.unique()) == n
#
#
# def test_parallel_download_power_unit_unique_mastr_number_case2():
#     """Number is an integer number of the API_MAX_DEMAND"""
#
#     n = 2 * API_MAX_DEMANDS
#     # download the data
#     download_parallel_power_unit(n, ofname=TEST_DATA)
#     # load the data
#     df = pd.read_csv(TEST_DATA, sep=';')
#     # remove the data
#     remove_test_data()
#     assert len(df.EinheitMastrNummer.unique()) == n
#     assert len(df.EegMastrNummer.unique()) == n
#
#
# def test_parallel_download_power_unit_unique_mastr_number_case3():
#     """Number is larger than the API_MAX_DEMAND"""
#
#     n = int(1.5 * API_MAX_DEMANDS)
#     # download the data
#     download_parallel_power_unit(n, ofname=TEST_DATA)
#     # load the data
#     df = pd.read_csv(TEST_DATA, sep=';')
#     # remove the data
#     remove_test_data()
#     assert len(df.EinheitMastrNummer.unique()) == n
#     assert len(df.EegMastrNummer.unique()) == n
#
#
# def test_parallel_download_power_unit_unique_mastr_number_case4():
#     """Number is larger than the API_MAX_DEMAND and batch_size"""
#
#     n = 6 * API_MAX_DEMANDS
#     # download the data
#     download_parallel_power_unit(n, ofname=TEST_DATA)
#     # load the data
#     df = pd.read_csv(TEST_DATA, sep=';')
#     # remove the data
#     remove_test_data()
#     assert len(df.EinheitMastrNummer.unique()) == n
#     assert len(df.EegMastrNummer.unique()) == n