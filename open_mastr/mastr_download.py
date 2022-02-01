from open_mastr.mastr import Mastr
from open_mastr.settings import DB_ENGINE

# instantiate Mastr() class
mastr = Mastr()

# configure settings for API download
date='latest' # TODO: initiate_string -> maybe centralize function
limit = None
processes = 12
chunksize = 1000
technology = ["wind", "biomass", "combustion", "gsgk", "hydro", "nuclear", "storage", "solar"]
data_types = ["unit_data", "eeg_data", "kwk_data", "permit_data"]
location_types = ["location_elec_generation", "location_elec_consumption", "location_gas_generation",
                  "location_gas_consumption"]

# call download functionality
mastr.download(method='API', processes=processes, limit=limit, chunksize=chunksize, technology=technology, date=date, data_types=data_types, location_types=location_types, initialize_db=DB_ENGINE)