__copyright__ = "© Reiner Lemoine Institut"
__license__ = "GNU Affero General Public License Version 3 (AGPL-3.0)"
__url__ = "https://www.gnu.org/licenses/agpl-3.0.en.html"
__author__ = "Bachibouzouk"
__version__ = "v0.8.0"


import pandas as pd
import os

from soap_api.mastr_power_unit_download import (
    get_power_unit,
    download_power_unit,
)

TEST_DATA = 'tests/data/bnetza_mastr_test_power-unit.csv'

def remove_test_data(fname=TEST_DATA):
    os.remove(fname)
    os.rmdir(os.path.dirname(fname))


def test_get_power_unit_return_data_frame():
    power_units = get_power_unit(0, 10)
    assert isinstance(power_units, pd.DataFrame)


def test_get_power_unit_return_correct_number():
    n = 13
    power_units = get_power_unit(0, n)
    assert len(power_units.index) == n


def test_download_power_unit_unique_mastr_number():
    n = 100
    # download the data
    download_power_unit(n, ofname=TEST_DATA)
    # load the data
    df = pd.read_csv(TEST_DATA, sep=';')
    # remove the data
    remove_test_data()
    assert len(df.EinheitMastrNummer.unique()) == n
    assert len(df.EegMastrNummer.unique()) == n


