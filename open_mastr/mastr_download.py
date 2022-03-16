from open_mastr.mastr import Mastr
from open_mastr.settings import DB_ENGINE

# instantiate Mastr() class
mastr = Mastr()

# configure settings for API download
api_date = "latest"  # TODO: initiate_string -> maybe centralize function
api_limit = 1
api_processes = False
api_chunksize = 1000
technology = [
    "wind",
    "biomass",
    "combustion",
    "gsgk",
    "hydro",
    "nuclear",
    "storage",
    "solar",
]
api_data_types = ["unit_data", "eeg_data", "kwk_data", "permit_data"]  #
api_location_types = [
    "location_elec_generation",
    "location_elec_consumption",
    "location_gas_generation",
    "location_gas_consumption",
]

# call download functionality
mastr.download(
    method="API",
    technology=technology,
    api_processes=api_processes,
    api_limit=api_limit,
    api_chunksize=api_chunksize,
    api_date=api_date,
    api_data_types=api_data_types,
    api_location_types=api_location_types,
    initialize_db=DB_ENGINE,
)
