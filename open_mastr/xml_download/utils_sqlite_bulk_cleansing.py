import pandas as pd
import numpy as np
import sqlalchemy

from open_mastr.xml_download.colums_to_replace import columns_replace_list
from zipfile import ZipFile
from open_mastr.orm import tablename_mapping


def replace_mastr_katalogeintraege(
    zipped_xml_file_path: str,
    df: pd.DataFrame,
) -> pd.DataFrame:

    katalogwerte = create_katalogwerte_from_sqlite(zipped_xml_file_path)

    for column_name in df.columns:
        if column_name in columns_replace_list:
            df[column_name] = (
                df[column_name].astype("float").astype("Int64").map(katalogwerte)
            )

    return df


def create_katalogwerte_from_sqlite(zipped_xml_file_path) -> dict:
    with ZipFile(zipped_xml_file_path, "r") as f:
        data = f.read("Katalogwerte.xml")
        df_katalogwerte = pd.read_xml(data, encoding="UTF-16", compression="zip")
    katalogwerte_array = np.array(df_katalogwerte[["Id", "Wert"]])
    katalogwerte = dict(
        (katalogwerte_array[n][0], katalogwerte_array[n][1])
        for n in range(len(katalogwerte_array))
    )
    return katalogwerte


def date_columns_to_datetime(xml_tablename: str, df: pd.DataFrame) -> pd.DataFrame:

    sqlalchemy_columnlist = tablename_mapping[xml_tablename][
        "__class__"
    ].__table__.columns.items()

    # Iterate over all columns from the orm data class
    # If the column has data type = date or datetime, the function
    # pd.to_datetime is applied
    for column in sqlalchemy_columnlist:
        if (
            type(column[1].type) == sqlalchemy.sql.sqltypes.Date
            or type(column[1].type) == sqlalchemy.sql.sqltypes.DateTime
        ):
            column_name = column[0]
            if column_name in df.columns:
                # Convert column to datetime64, invalid string -> NaT
                df[column_name] = pd.to_datetime(df[column_name], errors="coerce")
    return df
