import pandas as pd
from sqlalchemy import create_engine
from geoalchemy2 import Geometry
import os
from urllib.request import urlretrieve
from soap_api.utils import DATA_VERSION
import soap_api.utils as soap_utils


BKG_VG250 = {
    "schema": "boundaries",
    "table": "bkg_vg250_1_sta_union_mview"
}

OSM_PLZ = {
    "schema": "boundaries",
    "table": "osm_postcode"
}

OEP_QUERY_PATTERN = "https://openenergy-platform.org/api/v0/schema/{schema}/tables/{table}/rows?form=csv"

DATA_BASE_PATH = "data"

MASTR_RAW_SCHEMA = "model_draft"
OPEN_MASTR_SCHEMA = "model_draft"

TECHNOLOGIES = ["nuclear"] #["combustion", "wind", "hydro", "solar", "biomass", "gsgk", "storage", "nuclear"]


def engine_local_db():
    return create_engine('postgresql+psycopg2://open-mastr:open-mastr@localhost:55443/open-mastr', echo=False)


ENGINE_LOCAL = engine_local_db()


def table_name_from_file(filename):

    table_name = filename.replace(DATA_BASE_PATH + "/", "").replace("_" + DATA_VERSION, "").replace(".csv", "")
    return table_name


def get_csv_db_mapping(keys=TECHNOLOGIES):

    MASTR_CSV_DB_MAPPING = {}

    for k in keys:
        defined_filename = getattr(soap_utils, "fname_" + k)
        MASTR_CSV_DB_MAPPING.update({
            k: {
                "table": table_name_from_file(defined_filename),
                "file": defined_filename}})

    return MASTR_CSV_DB_MAPPING


def wkb_hexer(line):
    return line.wkb_hex


def read_csv_to_db(csv_file, table, schema, index_col, conn, geom_col="geom", sep=","):
    # Create schemas
    query = "CREATE SCHEMA IF NOT EXISTS {schema}".format(schema=schema)
    conn.execute(query)

    # CSV file
    csv_data = pd.read_csv(csv_file,
                           index_col=index_col,
                           sep=sep)

    # Insert data into database
    csv_data.to_sql(table,
                    con=conn,
                    schema=schema,
                    dtype={geom_col: Geometry()},
                    if_exists="replace")


def import_boundary_data_csv(schema, table, index_col="id"):

    csv_file = "{schema}_{table}.csv".format(schema=schema, table=table)

    # Check if file is locally available
    csv_file_exists = os.path.isfile(csv_file)

    with ENGINE_LOCAL.connect() as con:

        # Check if table already exists
        table_query = "SELECT to_regclass('{schema}.{table}');".format(schema=schema, table=table)
        table_name = "{schema}.{table}".format(schema=schema, table=table)
        table_exists = table_name in con.execute(table_query).first().values()

        if not table_exists:
            # Download CSV file if it does not exist
            if not csv_file_exists:
                urlretrieve(
                    OEP_QUERY_PATTERN.format(schema=schema, table=table),
                    csv_file)
            else:
                print("CSV file exists")

            # Read CSV and insert to db
            read_csv_to_db(csv_file.format(schema=schema, table=table), table, schema, index_col, con)
        else:
            print("Table '{schema}.{table}' already exists in local database".format(schema=schema, table=table))


def import_bnetz_mastr_csv():
    csv_db_mapping = get_csv_db_mapping()

    with ENGINE_LOCAL.connect() as con:
        for k, d in csv_db_mapping.items():
            read_csv_to_db("soap_api/" + d["file"],
                           d["table"],
                           MASTR_RAW_SCHEMA,
                           "lid",
                           con,
                           sep=";")


if __name__ == "__main__":

    import_boundary_data_csv(BKG_VG250["schema"], BKG_VG250["table"])
    import_boundary_data_csv(OSM_PLZ["schema"], OSM_PLZ["table"])

    import_bnetz_mastr_csv()

    # TODO: replace prints with logger
    # TODO: make entire workflow also work on OEP (for Ludwig)
