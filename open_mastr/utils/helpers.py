import os
from contextlib import contextmanager
import datetime
from warnings import warn
from typing import Literal, Optional, Union
from zipfile import BadZipfile, ZipFile
from zoneinfo import ZoneInfo

import dateutil
import sqlalchemy
from dateutil.parser import parse
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker


from open_mastr.soap_api.download import log
from open_mastr.utils.constants import (
    BULK_DATA,
    TECHNOLOGIES,
    BULK_INCLUDE_TABLES_MAP,
    ADDITIONAL_TABLES,
)

MASTR_TIMEZONE = ZoneInfo("Europe/Berlin")


def create_database_engine(
    engine: Union[Literal["sqlite"] | sqlalchemy.engine.Engine],
    sqlite_db_path: Optional[str],
) -> sqlalchemy.engine.Engine:
    if isinstance(engine, sqlalchemy.engine.Engine):
        return engine
    if engine != "sqlite":
        log.warning(
            "engine parameter is neither 'sqlite' nor an SQLALchemy engine."
            " Creating SQLite engine."
        )

    sqlite_database_path = os.environ.get(
        "SQLITE_DATABASE_PATH",
        os.path.join(sqlite_db_path, "open-mastr.db"),
    )
    db_url = f"sqlite:///{sqlite_database_path}"
    return create_engine(db_url)


def parse_date_string(bulk_date_string: str) -> str:
    if bulk_date_string == "today":
        return datetime.datetime.now(tz=MASTR_TIMEZONE).strftime("%Y%m%d")
    else:
        return parse(bulk_date_string).strftime("%Y%m%d")


def validate_parameter_format_for_mastr_init(engine) -> None:
    if engine not in ["sqlite"] and not isinstance(engine, sqlalchemy.engine.Engine):
        raise ValueError(
            "parameter engine has to be either 'sqlite' "
            "or an sqlalchemy.engine.Engine object."
        )


def validate_parameter_format_for_download_method(
    method,
    data,
    date,
    bulk_cleansing,
    **kwargs,
) -> None:
    if "technology" in kwargs:
        data = kwargs["technology"]
        warn("'technology' parameter is deprecated. Use 'data' instead")
    if "bulk_date" in kwargs:
        date = kwargs["bulk_date"]
        warn("'bulk_date' parameter is deprecated. Use 'date' instead")
    if "api_date" in kwargs:
        date = kwargs["api_date"]
        warn("'api_date' parameter is deprecated. Use 'date' instead")

    validate_parameter_method(method)
    validate_parameter_data(method, data)
    validate_parameter_date(method, date)
    validate_parameter_bulk_cleansing(bulk_cleansing)


def validate_parameter_method(method) -> None:
    if method != "bulk":
        raise ValueError("parameter method has to be 'bulk'.")


def validate_parameter_bulk_cleansing(bulk_cleansing) -> None:
    if type(bulk_cleansing) != bool:
        raise ValueError("parameter bulk_cleansing has to be boolean")


def validate_parameter_date(method, date) -> None:
    if date is None:  # default
        return
    if method == "bulk":
        if date not in ["today", "existing"]:
            try:
                _ = parse(date)
            except (dateutil.parser._parser.ParserError, TypeError) as e:
                raise ValueError(
                    "Parameter date has to be a proper date in the format yyyymmdd"
                    "or 'today' for bulk method."
                ) from e


def validate_parameter_data(method, data) -> None:
    if not isinstance(data, (str, list)) and data is not None:
        raise ValueError("parameter data has to be a string, list, or None")
    if isinstance(data, str):
        data = [data]
    if isinstance(data, list):
        if not data:  # data == []
            raise ValueError("parameter data cannot be an empty list!")
        for value in data:
            if method == "bulk" and value not in BULK_DATA:
                raise ValueError(
                    f"Allowed values for parameter data with bulk method are {BULK_DATA}"
                )
            if method == "csv_export" and value not in TECHNOLOGIES + ADDITIONAL_TABLES:
                raise ValueError(
                    "Allowed values for CSV export are "
                    f"{TECHNOLOGIES} or {ADDITIONAL_TABLES}"
                )


def transform_data_parameter(data, **kwargs):
    """
    Parse input parameters related to data as lists. Harmonize variables for later use.
    Data output depends on the possible data types of chosen method.
    """

    # data was named technology in an early version of open-mastr
    data = kwargs.get("technology", data)

    # parse parameters as list
    if isinstance(data, str):
        data = [data]
    elif data is None:
        # TODO: This should be adapted so that all tables are downloaded if no data is given.
        # Right now, it would skip new tables.
        data = BULK_DATA

    return data


def transform_date_parameter(date: Union[datetime.date, Literal["today"]], **kwargs: Optional[str]) -> str:
    date = kwargs.get("bulk_date", date)
    date = "today" if date is None else date
    if date == "existing":
        log.warning(
            """
        The date parameter 'existing' is deprecated and will be removed in the future.
        The date parameter is set to `today`.

        If this change causes problems for you, please comment in this issue on github:
        https://github.com/OpenEnergyPlatform/open-MaStR/issues/616#issuecomment-3089377062

        """
        )
        date = "today"

    return date


@contextmanager
def session_scope(engine):
    """Provide a transactional scope around a series of operations."""
    Session = sessionmaker(bind=engine)
    session = Session()
    try:
        yield session
        session.commit()
    except:
        session.rollback()
        raise
    finally:
        session.close()


def data_to_include_tables(data: list[str]) -> set[str]:
    """
    Convert user input 'data' to the set 'include_tables'.
    It contains file names from zipped bulk download.
    Parameters
    ----------
    data: list
        The user input for data selection
    Returns
    -------
    set
        Set of file names
    """
    if "storage_units" in data:
        log.warning(
            "The data parameter 'storage_units' is deprecated and will be removed in the future."
            " Please use 'storage' instead, which now also includes AnlagenStromSpeicher."
        )

    # Map data selection to include tables in xml
    include_tables = {
        table for tech in data for table in BULK_INCLUDE_TABLES_MAP[tech]
    }
    return include_tables


def delete_zip_file_if_corrupted(save_path: str):
    """
    Check if existing zip file is corrupted and if yes, delete it, if no, zipfile exists.
    """
    if os.path.exists(save_path):
        try:
            with ZipFile(save_path) as _:
                pass
        except BadZipfile:
            log.info(f"Bad Zip file is deleted: {save_path}")
            os.remove(save_path)
