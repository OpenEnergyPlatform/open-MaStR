import pandas as pd
from sqlalchemy import create_engine, text
from geoalchemy2 import Geometry, WKTElement
import os
from urllib.request import urlretrieve
from soap_api.utils import DATA_VERSION
import soap_api.utils as soap_utils
import geopandas as gpd
from shapely.wkb import loads as wkb_loads


BKG_VG250 = {
    "schema": "boundaries",
    "table": "bkg_vg250_1_sta_union_mview"
}

OSM_PLZ = {
    "schema": "boundaries",
    "table": "osm_postcode"
}

OFFSHORE = {
    "schema": "model_draft",
    "table": "rli_boundaries_offshore"
}

OSM_WINDPOWER = {
    "schema": "model_draft",
    "table": "mastr_osm_deu_point_windpower"
}

OEP_QUERY_PATTERN = "https://openenergy-platform.org/api/v0/schema/{schema}/tables/{table}/rows?form=csv"

DATA_BASE_PATH = "data"

MASTR_RAW_SCHEMA = "model_draft"
OPEN_MASTR_SCHEMA = "model_draft"

TECHNOLOGIES = ["wind", "hydro", "solar", "biomass", "combustion"] #, "gsgk", "storage"]


def engine_local_db():
    return create_engine('postgresql+psycopg2://open-mastr:open-mastr@localhost:55443/open-mastr', echo=False)


ENGINE_LOCAL = engine_local_db()


def table_name_from_file(filename):

    table_name = filename.replace(DATA_BASE_PATH + "/", "").replace("_" + DATA_VERSION, "").replace(".csv", "") + "_clean"
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


def table_to_db(csv_data, table, schema, conn, geom_col="geom"):
    # Create schemas
    query = "CREATE SCHEMA IF NOT EXISTS {schema}".format(schema=schema)
    conn.execute(query)

    csv_data.to_sql(table,
                    con=conn,
                    schema=schema,
                    # dtype={geom_col: Geometry("POINT", srid=4326)},
                    dtype={geom_col: Geometry(srid=4326)},
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
                print("CSV file {} exists".format(csv_file))

            # Read CSV file
            csv_data = pd.read_csv(csv_file,
                                   index_col=index_col)

            # Prepare geom data for DB upload
            csv_data["geom"] = csv_data["geom"].apply(lambda x: WKTElement(wkb_loads(x, hex=True).wkt, srid=4326))

            # Insert to db
            table_to_db(csv_data, table, schema, con)
        else:
            print("Table '{schema}.{table}' already exists in local database".format(schema=schema, table=table))


def import_bnetz_mastr_csv():
    csv_db_mapping = get_csv_db_mapping()

    with ENGINE_LOCAL.connect() as con:
        for k, d in csv_db_mapping.items():
            # Read CSV file
            csv_data = pd.read_csv("soap_api/" + d["file"].replace("data", "data/saved"),
                                   index_col="lid",
                                   sep=";")

            # Create 'geom' column from lat/lon
            gdf = add_geom_col(csv_data)

            # Import to local database
            table_to_db(gdf,
                        d["table"],
                        MASTR_RAW_SCHEMA,
                        con,
                        geom_col="geom")


def add_geom_col(df, lat_col="Breitengrad", lon_col="Laengengrad", srid=4326):

    df_with_coords = df.loc[~(df["Breitengrad"].isna() | df["Laengengrad"].isna())]
    df_no_coords = df.loc[(df["Breitengrad"].isna() | df["Laengengrad"].isna())]

    gdf = gpd.GeoDataFrame(
        df_with_coords, geometry=gpd.points_from_xy(df_with_coords[lon_col], df_with_coords[lat_col]),
        crs="EPSG:{}".format(srid))
    gdf["geom"] = gdf["geometry"].apply(lambda x: WKTElement(x.wkt, srid=srid))
    gdf.drop(columns=["geometry"], inplace=True)

    gdf = gdf.append(df_no_coords)

    return gdf


def run_sql_postprocessing():

    with ENGINE_LOCAL.connect().execution_options(autocommit=True) as con:

        for tech_name in ["wind"]: #TECHNOLOGIES:
            # Read SQL query from file
            with open("postprocessing/db-cleansing/rli-mastr-{tech_name}-cleansing.sql".format(tech_name=tech_name)) as file:
                escaped_sql = text(file.read())

            # Execute query
            con.execute(escaped_sql)


if __name__ == "__main__":

    import_boundary_data_csv(BKG_VG250["schema"], BKG_VG250["table"])
    import_boundary_data_csv(OSM_PLZ["schema"], OSM_PLZ["table"])
    import_boundary_data_csv(OFFSHORE["schema"], OFFSHORE["table"])
    import_boundary_data_csv(OSM_WINDPOWER["schema"], OSM_WINDPOWER["table"])

    import_bnetz_mastr_csv()

    run_sql_postprocessing()



    # TODO: replace prints with logger
    # TODO: make entire workflow also work on OEP (for Ludwig)
    # TODO: grant to oeuser: make this optional
    # TODO: Sort functions, rename, find right location in repository
    # TODO: Move commented SQL code into issues
