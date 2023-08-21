import os

DBNAME = ""
USER = "postgres"
HOST = "localhost"
PORT = "5432"
PASSWORD = ""

CSV_RESULT_PATH = os.path.join("results", "csv")

COMPULSORY_COLUMNS = [
    "solar_extended.manval_relevant",
    "solar_extended.manval_location",
    "solar_extended.manval_size",
    "solar_extended.manval_orientation",
    "solar_extended.geom",
    "wind_extended.manval_relevant",
    "wind_extended.manval_location",
    "wind_extended.geom",
    "biomass_extended.manval_relevant",
    "biomass_extended.manval_location",
    "biomass_extended.geom",
]

TOLERANCE_RADUIS_TRUE = 10  # Meter
TOLERANCE_RADUIS_PARTLY_TRUE = (
    50  # Meter, wenn die Anlage in 50 m Radius liegt und eindeutig zuordenbar ist
)

# Analysis related constants

MANVAL_CATALOG = {
    "manval_location": {1: "True", 2: "Partly true", 3: "False"},
    "manval_size": {1: "True", 2: "False"},
    "manval_orientation": {1: "True", 2: "False"},
}

FIGURE_LABEL_MAPPING = {
    "manval_location": "Location",
    "manval_size": "Size",
    "manval_orientation": "Orientation",
}
