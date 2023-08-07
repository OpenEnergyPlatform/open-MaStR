import warnings
import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import create_engine

from config import *


class MapCheckerEngine:
    def __init__(self):
        self.db_path = f"postgresql+psycopg2://{USER}:{PASSWORD}@{HOST}:{PORT}/{DBNAME}"
        self.db_engine = create_engine(self.db_path)
        self.db_engine.connect()
        if not self.check_if_schema_fits():
            self.add_columns_for_manual_validation()

    def fetch_random_unit(self, technology: str):

        if technology == "solar":
            query = """ SELECT "EinheitMastrNummer", "Laengengrad", 
             "Breitengrad", "Bruttoleistung", "AnzahlModule", "Lage", 
             "Hauptausrichtung", "HauptausrichtungNeigungswinkel",
              "Nebenausrichtung", 
              "Inbetriebnahmedatum", "Registrierungsdatum" 
            FROM public.solar_extended 
            WHERE "manval_relevant" IS TRUE
            ORDER BY RANDOM() 
            LIMIT 1
            """
        elif technology == "wind":
            query = """ SELECT "EinheitMastrNummer", "Laengengrad", "Breitengrad","Bruttoleistung",
                        "NameStromerzeugungseinheit", "Inbetriebnahmedatum"
                        FROM public.wind_extended 
                        WHERE "manval_relevant" IS TRUE
                        ORDER BY RANDOM() 
                        LIMIT 1 
                        """
        elif technology == "biomass":
            query = """ SELECT "EinheitMastrNummer", "Laengengrad", "Breitengrad","Bruttoleistung",
                        "NameStromerzeugungseinheit", "Inbetriebnahmedatum"
                        FROM public.biomass_extended 
                        WHERE "manval_relevant" IS TRUE
                        ORDER BY RANDOM() 
                        LIMIT 1 
                        """
        else:
            warnings.warn("Wrong technology parameter!")
            return

        return pd.read_sql(sql=query, con=self.db_engine)

    def set_validity(
        self, mastr_nummer, technology, manval_location, manval_size, manval_orientation
    ):
        if technology == "solar":

            query = text(
                """UPDATE public.solar_extended
            SET "manval_location" =:l,
            "manval_size" = :s,
            "manval_orientation" = :o
            WHERE "EinheitMastrNummer" =:m """
            )
        elif technology in {"wind", "biomass"}:
            query = text(
                f"""UPDATE public.{technology}_extended
                        SET "manval_location" =:l
                        WHERE "EinheitMastrNummer" =:m """
            )

        with self.db_engine.connect() as con:
            con.execute(
                query,
                l=manval_location,
                s=manval_size,
                o=manval_orientation,
                m=mastr_nummer,
            )
            print(f"1 INSERT {mastr_nummer}")

    def fetch_neighbors(self, unit_id, technology):
        query = f"""SELECT "EinheitMastrNummer", "Bruttoleistung", "Laengengrad", "Breitengrad" 
                FROM {technology}_extended
                WHERE ST_DWithin("geom", 
                    (SELECT "geom" FROM {technology}_extended WHERE "EinheitMastrNummer" = '{unit_id}'), 0.005) 
                    AND "EinheitMastrNummer" <> '{unit_id}'"""
        return pd.read_sql(sql=query, con=self.db_engine)

    def check_if_schema_fits(self):
        query = """SELECT "table_name" || '.' || "column_name" AS column_name
            FROM information_schema.columns
            WHERE "table_schema" = 'public' 
            AND "table_name" IN ('solar_extended', 'wind_extended', 'biomass_extended')"""

        df = pd.read_sql(sql=query, con=self.db_engine)
        column_name_list = df.column_name.to_list()
        return all(
            column_name in column_name_list for column_name in COMPULSORY_COLUMNS
        )

    def add_columns_for_manual_validation(self):
        with self.db_engine.connect() as con:
            # Geom Queries
            for tech in ["solar", "wind", "biomass"]:
                add_query = f"SELECT AddGeometryColumn('public','{tech}_extended','geom',4326,'POINT',2);"
                update_query = f"""UPDATE {tech}_extended SET geom = ST_SetSRID(
                                                            ST_MakePoint("Laengengrad", "Breitengrad"),4326)
                                    WHERE "Breitengrad" IS NOT NULL AND "Laengengrad" IS NOT NULL AND "geom" IS NULL"""
                index_query = f"""CREATE INDEX {tech}_point_idx
                  ON solar_extended
                  USING GIST (geom);"""

                con.execute(add_query)
                con.execute(update_query)
                con.execute(index_query)

            # Manual validation columns
            add_query_solar = """ALTER TABLE solar_extended ADD COLUMN IF NOT EXISTS manval_relevant BOOLEAN,
                                    ADD COLUMN IF NOT EXISTS manval_location INTEGER,
                                    ADD COLUMN IF NOT EXISTS manval_size INTEGER,
                                    ADD COLUMN IF NOT EXISTS manval_orientation INTEGER;"""
            add_query_wind = """ALTER TABLE wind_extended ADD COLUMN IF NOT EXISTS manval_relevant BOOLEAN, 
                                    ADD COLUMN IF NOT EXISTS manval_location INTEGER;"""
            add_query_biomass = """ALTER TABLE biomass_extended ADD COLUMN IF NOT EXISTS manval_relevant BOOLEAN, 
                                    ADD COLUMN IF NOT EXISTS manval_location INTEGER;"""

            update_query_solar = """UPDATE solar_extended
            SET manval_relevant = TRUE
            WHERE "geom" IS NOT NULL 
            AND "EinheitBetriebsstatus" = 'In Betrieb'"""
            update_query_wind = """UPDATE wind_extended 
            SET manval_relevant = TRUE 
            WHERE "geom" IS NOT NULL 
            AND "EinheitBetriebsstatus" = 'In Betrieb' 
            AND "Lage" = 'Windkraft an Land' """
            update_query_biomass = """UPDATE biomass_extended 
            SET manval_relevant = TRUE 
            WHERE "geom" IS NOT NULL 
            AND "EinheitBetriebsstatus" = 'In Betrieb' """

            con.execute(add_query_solar)
            con.execute(add_query_wind)
            con.execute(add_query_biomass)
            con.execute(update_query_solar)
            con.execute(update_query_wind)
            con.execute(update_query_biomass)
