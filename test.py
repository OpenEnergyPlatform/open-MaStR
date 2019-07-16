__copyright__ = "© Reiner Lemoine Institut"
__license__ = "GNU Affero General Public License Version 3 (AGPL-3.0)"
__url__ = "https://www.gnu.org/licenses/agpl-3.0.en.html"
__author__ = "Ludee; christian-rli"
__issue__ = "https://github.com/OpenEnergyPlatform/examples/issues/52"
__version__ = "v0.7.0"


from config import setup_logger
from mastr_power_unit_download import download_power_unit, download_parallel_power_unit
from mastr_wind_download import download_unit_wind, download_unit_wind_eeg, download_unit_wind_permit
from mastr_wind_process import make_wind
from mastr_hydro_download import download_unit_hydro, download_unit_hydro_eeg
from mastr_hydro_process import make_hydro
from mastr_biomass_download import download_unit_biomass, download_unit_biomass_eeg
from mastr_biomass_process import make_biomass
from mastr_solar_download import download_parallel_unit_solar, download_unit_solar_eeg
from mastr_solar_process import make_solar

log = setup_logger()
#metadata = oep_session()

download_parallel_power_unit(power_unit_list_len=50)
download_parallel_unit_solar()
