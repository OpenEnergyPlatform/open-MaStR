import datetime
import json
import pandas as pd
from sqlalchemy import text, String
from geoalchemy2 import Geometry, WKTElement
import numpy as np
import os
from urllib.request import urlretrieve
from open_mastr.postprocessing import orm
from open_mastr.utils.config import setup_logger, get_filenames, get_data_version_dir
from open_mastr.soap_api.metadata.create import create_datapackage_meta_json
from open_mastr.utils.helpers import chunks, session_scope, db_engine
import geopandas as gpd
from shapely.wkb import loads as wkb_loads


log = setup_logger()

BKG_VG250 = {"schema": "boundaries", "table": "bkg_vg250_1_sta_union_mview"}

OSM_PLZ = {"schema": "boundaries", "table": "osm_postcode"}

OFFSHORE = {"schema": "model_draft", "table": "rli_boundaries_offshore"}

OSM_WINDPOWER = {"schema": "model_draft", "table": "mastr_osm_deu_point_windpower"}

OEP_QUERY_PATTERN = "https://openenergy-platform.org/api/v0/schema/{schema}/tables/{table}/rows?form=csv"

DATA_BASE_PATH = "data"

MASTR_RAW_SCHEMA = "model_draft"
OPEN_MASTR_SCHEMA = "model_draft"

TECHNOLOGIES = [
    "wind",
    "hydro",
    "solar",
    "biomass",
    "combustion",
    "nuclear",
    "gsgk",
    "storage",
]

orm_map = {
    "wind": {
        "cleaned": "WindCleaned",
    },
    "solar": {
        "cleaned": "SolarCleaned",
    },
    "biomass": {
        "cleaned": "BiomassCleaned",
    },
    "combustion": {
        "cleaned": "CombustionCleaned",
    },
    "gsgk": {
        "cleaned": "GsgkCleaned",
    },
    "hydro": {
        "cleaned": "HydroCleaned",
    },
    "nuclear": {
        "cleaned": "NuclearCleaned",
    },
    "storage": {
        "cleaned": "StorageCleaned",
    },
}


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

    csv_data.to_sql(
        table,
        con=conn,
        schema=schema,
        dtype={
            geom_col: Geometry(srid=srid),
            "plz": String(),
        },
        chunksize=100000,
        if_exists="replace",
    )


def table_to_db_orm(mapper, data, chunksize=10000):
    """
    Import data table into PostgreSQL database using ORM bulk insert

    Parameters
    ----------
    mapper: SQLAlchemy decarative mapping
        Table ORM
    data: pandas.DataFrame
        Tabular data for one technology
    chunksize: int, optional
        Size of data chunks for database insertation. Data gets inserted in chunks of size `chunksize`.
    """

    with session_scope() as session:
        for chunk in chunks(data.reset_index().to_dict(orient="records"), chunksize):
            session.bulk_insert_mappings(mapper, chunk)
            # Commit each chunk separately
            session.commit()


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

    with db_engine().connect() as con:

        # Check if table already exists
        table_query = "SELECT to_regclass('{schema}.{table}');".format(
            schema=schema, table=table
        )
        table_name = "{schema}.{table}".format(schema=schema, table=table)
        table_exists = table_name in con.execute(table_query).first().values()

        if not table_exists:
            # Download CSV file if it does not exist
            if not csv_file_exists:
                log.info(
                    "Downloading table {schema}.{table} from OEP".format(
                        schema=schema, table=table
                    )
                )
                urlretrieve(
                    OEP_QUERY_PATTERN.format(schema=schema, table=table), csv_file
                )
            else:
                log.info("Found {} locally.".format(csv_file))

            # Read CSV file
            csv_data = pd.read_csv(csv_file, index_col=index_col)

            # Prepare geom data for DB upload
            csv_data["geom"] = csv_data["geom"].apply(
                lambda x: WKTElement(wkb_loads(x, hex=True).wkt, srid=srid)
            )

            # Insert to db
            table_to_db(csv_data, table, schema, con, srid=srid)
            log.info("Data from {} successfully imported to database.".format(csv_file))
        else:
            log.info(
                "Table '{schema}.{table}' already exists in local database".format(
                    schema=schema, table=table
                )
            )


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
    df_with_coords = df_with_coords[
        ~(
            (df_with_coords["Breitengrad"] < -90)
            | (df_with_coords["Breitengrad"] > 90)
            | (df_with_coords["Laengengrad"] < -180)
            | (df_with_coords["Laengengrad"] > 180)
        )
    ]
    df_no_coords = df.loc[~df.index.isin(df_with_coords.index)]

    gdf = gpd.GeoDataFrame(
        df_with_coords,
        geometry=gpd.points_from_xy(df_with_coords[lon_col], df_with_coords[lat_col]),
        crs="EPSG:{}".format(srid),
    )
    gdf["geom"] = gdf["geometry"].apply(lambda x: WKTElement(x.wkt, srid=srid))
    gdf.drop(columns=["geometry"], inplace=True)

    gdf = gdf.append(df_no_coords)

    return gdf


def import_bnetz_mastr_csv(mastr_cleaned):
    """
    Import MaStR raw data and create geom column

    Parameters
    ----------
    mastr_cleaned: dict of pandas.DataFrame
        Cleaned MaStR data in a dictionary of dataframes keyed by technology.
    """

    for k, d in mastr_cleaned.items():

        # Create 'geom' column from lat/lon
        gdf = add_geom_col(d)

        # Import to local database
        log.info(f"Import data to database for {k}")
        mapper = getattr(orm, orm_map[k]["cleaned"])
        table_to_db_orm(mapper, gdf.replace({np.nan: None}), chunksize=100000)
        log.info(f"Data for {k} successfully imported to database.")


def run_sql_postprocessing():
    """
    Execute SQL scripts ins `db-cleansing/`
    """

    with db_engine().connect().execution_options(autocommit=True) as con:

        for tech_name in TECHNOLOGIES:
            if tech_name not in ["gsgk", "storage", "nuclear"]:
                log.info(f"Run post-processing on {tech_name} data")
                # Read SQL query from file
                with open(
                    os.path.join(
                        os.path.dirname(__file__),
                        "db-cleansing",
                        "rli-mastr-{tech_name}-cleansing.sql".format(
                            tech_name=tech_name
                        ),
                    )
                ) as file:
                    escaped_sql = text(file.read())

                # Execute query
                con.execute(escaped_sql)


def postprocess(mastr_cleaned):
    """
    Run post-processing

    Import cleaned MaStR data to PostgreSQL database, retrieve additional data for post-processing, enrich, and
    prepare data for further analysis.

    Parameters
    ----------
    mastr_cleaned: dict of pandas.DataFrame
        Cleaned MaStR data in a dictionary of dataframes keyed by technology.
    """
    # Create cleaned tables
    engine = db_engine()
    with engine.connect().execution_options(autocommit=True) as con:
        con.execute(f"DROP SCHEMA IF EXISTS {orm.Base.metadata.schema} CASCADE;")
        con.execute(f"CREATE SCHEMA {orm.Base.metadata.schema};")
    orm.Base.metadata.create_all(engine)

    # Import external data (most boundaries)
    import_boundary_data_csv(BKG_VG250["schema"], BKG_VG250["table"], srid=3035)
    import_boundary_data_csv(OSM_PLZ["schema"], OSM_PLZ["table"])
    import_boundary_data_csv(OFFSHORE["schema"], OFFSHORE["table"])
    import_boundary_data_csv(OSM_WINDPOWER["schema"], OSM_WINDPOWER["table"])

    # Import MaStR raw data in o database
    import_bnetz_mastr_csv(mastr_cleaned)

    # Process data
    run_sql_postprocessing()


def to_csv(limit=None):
    """
    Write post-processed MaStR data to CSV files

    Write structurally similar data as :meth:`~.open_mastr.soap_api.mirror.MaStRMirror.to_csv`, but with additional
    columns and post-processed data.

    Metadata gets created decribing post-processed and raw data as one datapackage.

    Parameters
    ----------
    limit: int, optional
        Limit number of rows exported to CSV file. This applied independently for each file/technology.
    """
    data_path = get_data_version_dir()
    filenames = get_filenames()
    newest_date = datetime.datetime(1900, 1, 1, 0, 0, 0, tzinfo=datetime.timezone.utc)

    for tech in TECHNOLOGIES:
        with session_scope() as session:
            orm_tech = getattr(orm, orm_map[tech]["cleaned"])
            query = session.query(orm_tech).limit(limit)
            df = pd.read_sql(
                query.statement, query.session.bind, index_col="EinheitMastrNummer"
            )

        csv_file = os.path.join(data_path, filenames["postprocessed"][tech])

        df.to_csv(
            csv_file, index=True, index_label="EinheitMastrNummer", encoding="utf-8"
        )

        if df["DatumLetzteAktualisierung"].max() > newest_date:
            newest_date = df["DatumLetzteAktualisierung"].max()

    # Save metadata along with data
    metadata_file = os.path.join(data_path, filenames["metadata"])
    metadata = create_datapackage_meta_json(
        newest_date,
        TECHNOLOGIES,
        data=["raw", "cleaned", "postprocessed"],
        json_serialize=False,
    )

    with open(metadata_file, "w", encoding="utf-8") as f:
        json.dump(metadata, f, ensure_ascii=False, indent=4)


if __name__ == "__main__":

    pass
