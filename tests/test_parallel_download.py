__copyright__ = "© Reiner Lemoine Institut"
__license__ = "GNU Affero General Public License Version 3 (AGPL-3.0)"
__url__ = "https://www.gnu.org/licenses/agpl-3.0.en.html"
__author__ = "Bachibouzouk"
__version__ = "v0.8.0"


import pandas as pd

from soap_api.mastr_power_unit_download import (
    get_power_unit,
)


def test_get_power_unit_return_data_frame():
    power_units = get_power_unit(0, 10)
    assert isinstance(power_units, pd.DataFrame)


def test_get_power_unit_return_correct_number():
    n = 13
    power_units = get_power_unit(0, n)
    assert len(power_units.index) == n
