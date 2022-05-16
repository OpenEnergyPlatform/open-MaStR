from open_mastr.soap_api.mirror import MaStRMirror


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

mastr_refl = MaStRMirror()

# Fill the basic_units table from extended tables
mastr_refl.reverse_fill_basic_units()

# to csv per tech
mastr_refl.to_csv(
    technology=technology, additional_data=data_types, statistic_flag=None, limit=None
)
