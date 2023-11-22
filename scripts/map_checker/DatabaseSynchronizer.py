import csv
from pathlib import Path
import pandas as pd
from sqlalchemy.engine import create_engine
from config import *


class DatabaseSynchronizer:
    def __init__(self):
        self.results_path = Path(CSV_RESULT_PATH)
        self.results_path.mkdir(parents=True, exist_ok=True)
        self.db_path = f"postgresql+psycopg2://{USER}:{PASSWORD}@{HOST}:{PORT}/{DBNAME}"
        self.db_engine = create_engine(self.db_path)
        self.db_engine.connect()

    def export_labels_to_csv(self):
        """
        Fetches the labeled units from database into csv file per technology.
        Currently, for solar and wind.
        :return:
        """
        # Solar
        read_query = """SELECT "EinheitMastrNummer", "manval_location", "manval_size", "manval_orientation"
                        FROM solar_extended
                        WHERE "manval_relevant" IS True AND "manval_location" IS NOT NULL"""

        solar_df = pd.read_sql(sql=read_query, con=self.db_engine)
        solar_csv_path = os.path.join(self.results_path, "labeled_solar_units.csv")
        solar_df.to_csv(path_or_buf=solar_csv_path, index=False)

        # Wind
        read_query = """SELECT "EinheitMastrNummer", "manval_location"
                        FROM wind_extended
                        WHERE "manval_relevant" IS True AND "manval_location" IS NOT NULL"""

        wind_df = pd.read_sql(sql=read_query, con=self.db_engine)
        wind_csv_path = os.path.join(self.results_path, "labeled_wind_units.csv")
        wind_df.to_csv(path_or_buf=wind_csv_path, index=False)

        # Biomass
        read_query = """SELECT "EinheitMastrNummer", "manval_location"
                        FROM biomass_extended
                        WHERE "manval_relevant" IS True AND "manval_location" IS NOT NULL"""

        biomass_df = pd.read_sql(sql=read_query, con=self.db_engine)
        biomass_csv_path = os.path.join(self.results_path, "labeled_biomass_units.csv")
        biomass_df.to_csv(path_or_buf=biomass_csv_path, index=False)

        print(f"Labeled units are exported as CSV to {self.results_path}")

    def import_csv_to_db(self):

        solar_csv_path = os.path.join(self.results_path, "labeled_solar_units.csv")
        with open(solar_csv_path, "r") as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                self.db_engine.execute(
                    f"""UPDATE solar_extended SET 
                                    "manval_location" = {row["manval_location"]}, 
                                    "manval_size" = {row["manval_size"]}, 
                                    "manval_orientation" = {row["manval_orientation"]} 
                                    WHERE "EinheitMastrNummer" = '{row["EinheitMastrNummer"]}' """
                )

        wind_csv_path = os.path.join(self.results_path, "labeled_wind_units.csv")
        with open(wind_csv_path, "r") as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                self.db_engine.execute(
                    f"""UPDATE wind_extended SET 
                                    "manval_location" = {row["manval_location"]} 
                                    WHERE "EinheitMastrNummer" = '{row["EinheitMastrNummer"]}' """
                )

        biomass_csv_path = os.path.join(self.results_path, "labeled_biomass_units.csv")
        with open(biomass_csv_path, "r") as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                self.db_engine.execute(
                    f"""UPDATE biomass_extended SET
                                   "manval_location" = {row["manval_location"]}
                                   WHERE "EinheitMastrNummer" = '{row["EinheitMastrNummer"]}' """
                )

        print("CSV files are imported to the database.")


if __name__ == "__main__":
    dbs = DatabaseSynchronizer()
    dbs.export_labels_to_csv()
    # dbs.import_csv_to_db()
