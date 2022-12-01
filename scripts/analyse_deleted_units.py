import pandas as pd
from open_mastr.utils.config import get_project_home_dir
from open_mastr import Mastr
import os
import pdb
from open_mastr.utils.orm import tablename_mapping
from pandas import Timestamp

project_home_dir = get_project_home_dir()
logs_dir = os.path.join(project_home_dir, "logs")

db = Mastr()

primary_column = {
    "anlageneegsolar.csv": "EegMastrNummer",
    "einheitensolar.csv": "EinheitMastrNummer",
    "einheitenstromspeicher.csv": "EinheitMastrNummer",
    "anlagenstromspeicher.csv": "MastrNummer",
}

compare_timestamps_general = [
    "DatumLetzteAktualisierung",
    "EegInbetriebnahmedatum",
    "Registrierungsdatum",
    "NetzbetreiberpruefungDatum",
    "Inbetriebnahmedatum",
    "DatumDesBetreiberwechsels",
]
compare_values_general = [
    "InstallierteLeistung",
    "NetzbetreiberpruefungStatus",
    "EinheitBetriebsstatus",
    "AnlagenbetreiberMastrNummer",
    "EinheitSystemstatus",
    "EinheitBetriebsstatus",
    "SpeMastrNummer",
    "EegMastrNummer",
    "GenMastrNummer",
]

for csv_file in os.listdir(logs_dir):
    if csv_file.split(".")[1] != "csv":
        # if csv_file != "einheitensolar.csv":
        continue
    file_path = os.path.join(logs_dir, csv_file)
    df_csv = pd.read_csv(file_path)
    sql_tablename = tablename_mapping[csv_file.split(".")[0]]["__name__"]

    primary_key_name = primary_column[csv_file]

    df_sql = pd.read_sql(sql=sql_tablename, con=db.engine).set_index(primary_key_name)

    primary_keys = df_csv[primary_key_name].to_list()

    df_csv = df_csv.set_index(primary_key_name)

    compare_timestamps_specific = [
        item for item in compare_timestamps_general if item in df_sql
    ]
    compare_values_specific = [
        item for item in compare_values_general if item in df_sql
    ]

    for counter, index in enumerate(primary_keys):
        for column in compare_timestamps_specific:
            try:
                comparison_bool = df_sql.loc[index][column].round(
                    freq="S"
                ) != Timestamp(df_csv.loc[index][column]).round(freq="S")

                if comparison_bool and not (
                    pd.isnull(df_sql.loc[index][column])
                    and pd.isnull(df_csv.loc[index][column])
                ):
                    print(f"Column: {column}, Index: {index}")
                    print(f"From sql: {df_sql.loc[index][column]}")
                    print(f"From csv: {df_csv.loc[index][column]}")
                    pdb.set_trace()
            except KeyError:
                print(f"{counter}: Key {index} did not exist")
            except:
                pdb.set_trace()
            if counter % 100 == 0:
                print(counter)
        for column in compare_values_specific:
            try:
                comparison_bool = df_sql.loc[index][column] != df_csv.loc[index][column]

                if comparison_bool and not (
                    pd.isnull(df_sql.loc[index][column])
                    and pd.isnull(df_csv.loc[index][column])
                ):
                    print(f"Column: {column}, Index: {index}")
                    print(f"From sql: {df_sql.loc[index][column]}")
                    print(f"From csv: {df_csv.loc[index][column]}")
                    pdb.set_trace()
            except KeyError:
                print(f"{counter}: Key {index} did not exist")
            if counter % 100 == 0:
                print(counter)
