from open_mastr.utils.helpers import reverse_fill_basic_units, create_db_query


technology = [
    "solar",
    "wind",
    "biomass",
    "combustion",
    "gsgk",
    "hydro",
    "nuclear",
    "storage",
]

data_types = ["unit_data", "eeg_data", "kwk_data", "permit_data"]
location_types = [
    "location_elec_generation",
    "location_elec_consumption",
    "location_gas_generation",
    "location_gas_consumption",
]

# Fill the basic_units table from extended tables
reverse_fill_basic_units()

# to csv per tech
create_db_query(technology=technology, additional_data=data_types, limit=None)
