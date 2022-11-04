# Possible values for parameter 'data' with bulk download method
BULK_DATA = [
    "wind",
    "solar",
    "biomass",
    "hydro",
    "gsgk",
    "combustion",
    "nuclear",
    "gas",
    "storage",
    "electricity_consumer",
    "location",
    "market",
    "grid",
    "balancing_area",
    "permit",
    "deleted_units",
]

# Possible values for parameter 'data' with API download method
API_DATA = [
    "wind",
    "solar",
    "biomass",
    "hydro",
    "gsgk",
    "combustion",
    "nuclear",
    "storage",
    "location",
    "permit",
]

# Technology related values of parameter 'data'
# Methods like Mastr.to_csv() must separate these from additional tables
TECHNOLOGIES = [
    "wind",
    "solar",
    "biomass",
    "hydro",
    "gsgk",
    "combustion",
    "nuclear",
    "storage",
]

# Data which are not related to a specific technology
ADDITIONAL_TABLES = [
    "balancing_area",
    "electricity_consumer",
    "gas_consumer",
    "gas_producer",
    "gas_storage",
    "gas_storage_extended",
    "grid_connections",
    "grids",
    "locations_extended",
    "market_actors",
    "market_roles",
]

# Possible data types for API download
API_DATA_TYPES = ["unit_data", "eeg_data", "kwk_data", "permit_data"]

# Possible location types for API download
API_LOCATION_TYPES = [
    "location_elec_generation",
    "location_elec_consumption",
    "location_gas_generation",
    "location_gas_consumption",
]

# Map bulk data to bulk download tables (xml file names)
BULK_INCLUDE_TABLES_MAP = {
    "wind": ["anlageneegwind", "einheitenwind"],
    "solar": ["anlageneegsolar", "einheitensolar"],
    "biomass": ["anlageneegbiomasse", "einheitenbiomasse"],
    "hydro": ["anlageneegwasser", "einheitenwasser"],
    "gsgk": [
        "anlageneeggeosolarthermiegrubenklaerschlammdruckentspannung",
        "einheitengeosolarthermiegrubenklaerschlammdruckentspannung",
    ],
    "combustion": ["anlagenkwk", "einheitenverbrennung"],
    "nuclear": ["einheitenkernkraft"],
    "storage": ["anlageneegspeicher", "anlagenstromspeicher", "einheitenstromspeicher"],
    "gas": [
        "anlagengasspeicher",
        "einheitengaserzeuger",
        "einheitengasspeicher",
        "einheitengasverbraucher",
    ],
    "electricity_consumer": ["einheitenstromverbraucher"],
    "location": ["lokationen"],
    "market": ["marktakteure", "marktrollen"],
    "grid": ["netzanschlusspunkte", "netze"],
    "balancing_area": ["bilanzierungsgebiete"],
    "permit": ["einheitengenehmigung"],
    "deleted_units": ["geloeschteunddeaktivierteeinheiten"],
}
