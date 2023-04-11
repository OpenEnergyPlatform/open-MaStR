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
    "retrofit_units",
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
    "permit",
    "deleted_units",
    "retrofit_units",
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
    "retrofit_units": ["ertuechtigungen"],
}

# Map bulk data to database table names, for csv export
BULK_ADDITIONAL_TABLES_CSV_EXPORT_MAP = {
    "gas": [
        "gas_consumer",
        "gas_producer",
        "gas_storage",
        "gas_storage_extended",
    ],
    "electricity_consumer": ["electricity_consumer"],
    "location": ["locations_extended"],
    "market": ["market_actors", "market_roles"],
    "grid": ["grid_connections", "grids"],
    "balancing_area": ["balancing_area"],
    "permit": ["permit"],
    "deleted_units": ["deleted_units"],
    "retrofit_units": ["retrofit_units"],
}

# used to map the parameter options in open-mastr to the exact table class names in orm.py
ORM_MAP = {
    "wind": {
        "unit_data": "WindExtended",
        "eeg_data": "WindEeg",
        "permit_data": "Permit",
    },
    "solar": {
        "unit_data": "SolarExtended",
        "eeg_data": "SolarEeg",
        "permit_data": "Permit",
    },
    "biomass": {
        "unit_data": "BiomassExtended",
        "eeg_data": "BiomassEeg",
        "kwk_data": "Kwk",
        "permit_data": "Permit",
    },
    "combustion": {
        "unit_data": "CombustionExtended",
        "kwk_data": "Kwk",
        "permit_data": "Permit",
    },
    "gsgk": {
        "unit_data": "GsgkExtended",
        "eeg_data": "GsgkEeg",
        "kwk_data": "Kwk",
        "permit_data": "Permit",
    },
    "hydro": {
        "unit_data": "HydroExtended",
        "eeg_data": "HydroEeg",
        "permit_data": "Permit",
    },
    "nuclear": {"unit_data": "NuclearExtended", "permit_data": "Permit"},
    "storage": {
        "unit_data": "StorageExtended",
        "eeg_data": "StorageEeg",
        "permit_data": "Permit",
    },
    "gas_consumer": "GasConsumer",
    "gas_producer": "GasProducer",
    "gas_storage": "GasStorage",
    "gas_storage_extended": "GasStorageExtended",
    "electricity_consumer": "ElectricityConsumer",
    "locations_extended": "LocationExtended",
    "market_actors": "MarketActors",
    "market_roles": "MarketRoles",
    "grid_connections": "GridConnections",
    "grids": "Grids",
    "balancing_area": "BalancingArea",
    "permit": "Permit",
    "deleted_units": "DeletedUnits",
    "retrofit_units": "RetrofitUnits",
}


UNIT_TYPE_MAP = {
    "Windeinheit": "wind",
    "Solareinheit": "solar",
    "Biomasse": "biomass",
    "Wasser": "hydro",
    "Geothermie": "gsgk",
    "Verbrennung": "combustion",
    "Kernenergie": "nuclear",
    "Stromspeichereinheit": "storage",
    "Gasspeichereinheit": "gas_storage",
    "Gasverbrauchseinheit": "gas_consumer",
    "Stromverbrauchseinheit": "electricity_consumer",
    "Gaserzeugungseinheit": "gas_producer",
    "Stromerzeugungslokation": "location_elec_generation",
    "Stromverbrauchslokation": "location_elec_consumption",
    "Gaserzeugungslokation": "location_gas_generation",
    "Gasverbrauchslokation": "location_gas_consumption",
}
