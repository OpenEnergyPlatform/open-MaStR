from open_mastr.soap_api.mirror import MaStRMirror
import datetime

limit = None
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
data_types = ["unit_data", "eeg_data", "kwk_data", "permit_data"]
location_types = [
    "location_elec_generation",
    "location_elec_consumption",
    "location_gas_generation",
    "location_gas_consumption",
]
processes = 12

mastr_mirror = MaStRMirror(
    empty_schema=False, parallel_processes=processes, restore_dump=None
)

# Download basic unit data
mastr_mirror.backfill_basic(technology, limit=limit, date="latest")

# Download additional unit data
for tech in technology:
    # mastr_mirror.create_additional_data_requests(tech)
    for data_type in data_types:
        mastr_mirror.retrieve_additional_data(
            tech, data_type, chunksize=1000, limit=limit
        )

# Download basic location data
mastr_mirror.backfill_locations_basic(limit=limit, date="latest")

# Download extended location data
for location_type in location_types:
    mastr_mirror.retrieve_additional_location_data(location_type, limit=limit)
