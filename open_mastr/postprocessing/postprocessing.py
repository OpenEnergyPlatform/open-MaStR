import pandas as pd
from sqlalchemy import create_engine, text, String
from geoalchemy2 import Geometry, WKTElement
import os
from urllib.request import urlretrieve
from open_mastr.soap_api.config import setup_logger, get_db_tables, get_filenames, get_data_version_dir
import geopandas as gpd
from shapely.wkb import loads as wkb_loads


log = setup_logger()

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

TECHNOLOGIES = ["wind", "hydro", "solar", "biomass", "combustion", "nuclear", "gsgk", "storage"]

ENGINE_LOCAL = create_engine('postgresql+psycopg2://open-mastr:open-mastr@localhost:55443/open-mastr', echo=False)


def get_csv_db_mapping(keys=TECHNOLOGIES):
    """
    Retrieve raw data file and desired table name for each technology

    Parameters
    ----------
    keys : list
        List of technologies

    Returns
    -------
    dict
        Table name and file name for each technology, keyed by items of `keys`
    """

    db_tables = get_db_tables()
    defined_filenames = get_filenames()

    MASTR_CSV_DB_MAPPING = {}

    for k in keys:
        MASTR_CSV_DB_MAPPING.update({
            k: {
                "table": db_tables[k]["raw"],
                "file": defined_filenames["raw"][k]["joined"]}})

    return MASTR_CSV_DB_MAPPING


def wkb_hexer(line):
    """
    Convert WKB to hex format

    Parameters
    ----------
    line : shapely
        Shapely Well Known Binary (WKB) representation

    Returns
    -------
    shapely.wkb_hex
        Hex representation of geo data

    """
    return line.wkb_hex


def table_to_db(csv_data, table, schema, conn, geom_col="geom", srid=4326):
    """
    Import data table into PostgreSQL database

    Parameters
    ----------
    csv_data : pandas.DataFrame
        The table data
    table : str
        Table name
    schema : str
        Schema name
    conn : sqlalchemy.engine.Connection
        Database connection
    geom_col : str
        Name of column which is expected to hold geom data (defaults to 'geom')
    """

    # Create schemas
    query = "CREATE SCHEMA IF NOT EXISTS {schema}".format(schema=schema)
    conn.execute(query)

    csv_data.to_sql(table,
                    con=conn,
                    schema=schema,
                    dtype={
                        geom_col: Geometry(srid=srid),
                        "plz": String(),
                    },
                    chunksize=100000,
                    if_exists="replace")


def import_boundary_data_csv(schema, table, index_col="id", srid=4326):
    """
    Import additional data for post-processing

    Parameters
    ----------
    schema : str
        Schema of `table`
    table : str
        Table name
    index_col : str
        Column used as index (defaults to 'id')
    """

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
                log.info("Downloading table {schema}.{table} from OEP".format(schema=schema, table=table))
                urlretrieve(
                    OEP_QUERY_PATTERN.format(schema=schema, table=table),
                    csv_file)
            else:
                log.info("Found {} locally.".format(csv_file))

            # Read CSV file
            csv_data = pd.read_csv(csv_file,
                                   index_col=index_col)

            # Prepare geom data for DB upload
            csv_data["geom"] = csv_data["geom"].apply(lambda x: WKTElement(wkb_loads(x, hex=True).wkt, srid=srid))

            # Insert to db
            table_to_db(csv_data, table, schema, con, srid=srid)
            log.info("Data from {} successfully imported to database.".format(csv_file))
        else:
            log.info("Table '{schema}.{table}' already exists in local database".format(schema=schema, table=table))


def add_geom_col(df, lat_col="Breitengrad", lon_col="Laengengrad", srid=4326):
    """
    Creates a geometry column based on lat/long

    Parameters
    ----------
    df : pandas.DataFrame
        Data
    lat_col : str
        Column name with lat coordinate
    lon_col : str
        Column name with long coordinate
    srid : int
        Defines the spatial reference system of geo data (defaults to 4326)

    Returns
    -------
    GeoDataFrame
        Read MaStR raw data with added geom column
    """

    # Split data into with and without coordinates
    df_with_coords = df.loc[~(df["Breitengrad"].isna() | df["Laengengrad"].isna())]

    # Just select data with lat/lon in range [(-90,90), (-180,180)]
    df_with_coords = df_with_coords[~((df_with_coords["Breitengrad"] < -90)
                                      | (df_with_coords["Breitengrad"] > 90)
                                      | (df_with_coords["Laengengrad"] < -180)
                                      | (df_with_coords["Laengengrad"] > 180))
    ]
    df_no_coords = df.loc[~df.index.isin(df_with_coords.index)]


    gdf = gpd.GeoDataFrame(
        df_with_coords, geometry=gpd.points_from_xy(df_with_coords[lon_col], df_with_coords[lat_col]),
        crs="EPSG:{}".format(srid))
    gdf["geom"] = gdf["geometry"].apply(lambda x: WKTElement(x.wkt, srid=srid))
    gdf.drop(columns=["geometry"], inplace=True)

    gdf = gdf.append(df_no_coords)

    return gdf


def import_bnetz_mastr_csv():
    """
    Import MaStR raw data and create geom column
    """
    csv_db_mapping = get_csv_db_mapping()

    with ENGINE_LOCAL.connect() as con:
        for k, d in csv_db_mapping.items():

            csv_file = os.path.abspath(
                os.path.join(get_data_version_dir(), d["file"]))

            if os.path.isfile(csv_file):
                # Read CSV file
                log.info(f"Read raw data from {csv_file}")
                csv_data = pd.read_csv(csv_file,
                                       index_col="EinheitMastrNummer",
                                       sep=",",
                                       dtype={"Postleitzahl": str})

                # Create 'geom' column from lat/lon
                gdf = add_geom_col(csv_data)

                # Import to local database
                log.info(f"Import data to database for {k}")
                table_to_db(gdf,
                            d["table"],
                            MASTR_RAW_SCHEMA,
                            con,
                            geom_col="geom")
                log.info("Data from {} successfully imported to database.".format(csv_file))
            else:
                log.warning("No raw data found for {}, cannot find {},".format(k, csv_file))


def run_sql_postprocessing():
    """
    Execute SQL scripts ins `db-cleansing/`
    """

    with ENGINE_LOCAL.connect().execution_options(autocommit=True) as con:

        for tech_name in TECHNOLOGIES:
            if tech_name not in ["gsgk", "storage", "nuclear"]:
                # Read SQL query from file
                with open(os.path.join(os.path.dirname(__file__),
                                       "db-cleansing",
                                       "rli-mastr-{tech_name}-cleansing.sql".format(tech_name=tech_name))) as file:
                    escaped_sql = text(file.read())

                # Execute query
                con.execute(escaped_sql)


def postprocess():
    """
    Run post-processing

    Import raw MaStR data to PostgreSQL database, retrieve additional data for post-processing and clean, enrich, and
    prepare data for further analysis.
    """
    import_boundary_data_csv(BKG_VG250["schema"], BKG_VG250["table"], srid=3035)
    import_boundary_data_csv(OSM_PLZ["schema"], OSM_PLZ["table"])
    import_boundary_data_csv(OFFSHORE["schema"], OFFSHORE["table"])
    import_boundary_data_csv(OSM_WINDPOWER["schema"], OSM_WINDPOWER["table"])

    import_bnetz_mastr_csv()

    run_sql_postprocessing()


if __name__ == "__main__":

    pass
