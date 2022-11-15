import os
import subprocess
import sys
from contextlib import contextmanager
from datetime import date, datetime
from warnings import warn

import dateutil
import sqlalchemy
from dateutil.parser import parse
from sqlalchemy import create_engine
from sqlalchemy.orm import Query, sessionmaker

from open_mastr.soap_api.download import MaStRAPI
from open_mastr.utils.constants import (
    BULK_DATA,
    API_DATA,
    API_DATA_TYPES,
    API_LOCATION_TYPES,
)


def chunks(lst, n):
    """Yield successive n-sized chunks from lst.

    `Credits
    <https://stackoverflow.com/questions/312443/how-do-you-split-a-list-into-evenly-sized-chunks>`_
    """
    length = lst.count() if isinstance(lst, Query) else len(lst)
    for i in range(0, length, n):
        yield lst[i : i + n]


def create_database_engine(engine, home_directory) -> sqlalchemy.engine.Engine:
    if engine == "sqlite":
        sqlite_database_path = os.environ.get(
            "SQLITE_DATABASE_PATH",
            os.path.join(home_directory, "data", "sqlite", "open-mastr.db"),
        )
        db_url = f"sqlite:///{sqlite_database_path}"
        return create_engine(db_url)

    if engine == "docker-postgres":
        db_url = (
            "postgresql+psycopg2://open-mastr:open-mastr@localhost:55443/open-mastr"
        )
        setup_docker()
        return create_engine(db_url)

    if type(engine) == sqlalchemy.engine.Engine:
        return engine


def setup_docker():
    """Initialize a PostgreSQL database with docker-compose"""

    conf_file_path = os.path.abspath(os.path.dirname(__file__))
    print(conf_file_path)
    subprocess.run(
        ["docker-compose", "up", "-d"],
        cwd=conf_file_path,
    )


def parse_date_string(bulk_date_string: str) -> str:
    if bulk_date_string == "today":
        return date.today().strftime("%Y%m%d")
    else:
        return parse(bulk_date_string).strftime("%Y%m%d")


def validate_parameter_format_for_mastr_init(engine) -> None:
    if engine not in ["sqlite", "docker-postgres"] and not isinstance(
        engine, sqlalchemy.engine.Engine
    ):
        raise ValueError(
            "parameter engine has to be either 'sqlite' "
            "or 'docker-postgres' or an sqlalchemy.engine.Engine object."
        )


def validate_parameter_format_for_download_method(
    method,
    data,
    date,
    bulk_cleansing,
    api_processes,
    api_limit,
    api_chunksize,
    api_data_types,
    api_location_types,
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
    validate_parameter_api_processes(api_processes)
    validate_parameter_api_limit(api_limit)
    validate_parameter_api_chunksize(api_chunksize)
    validate_parameter_api_data_types(api_data_types)
    validate_parameter_api_location_types(api_location_types)

    raise_warning_for_invalid_parameter_combinations(
        method,
        bulk_cleansing,
        date,
        api_processes,
        api_data_types,
        api_location_types,
        api_limit,
        api_chunksize,
    )


def validate_parameter_method(method) -> None:
    if method not in ["bulk", "API"]:
        raise ValueError("parameter method has to be either 'bulk' or 'API'.")


def validate_parameter_api_location_types(api_location_types) -> None:
    if not isinstance(api_location_types, list) and api_location_types is not None:
        raise ValueError("parameter api_location_types has to be a list or 'None'.")

    if isinstance(api_location_types, list):
        if not api_location_types:  # api_location_types == []
            raise ValueError("parameter api_location_types cannot be an empty list!")
        for value in api_location_types:
            if value not in API_LOCATION_TYPES:
                raise ValueError(
                    f"list entries of api_data_types have to be in {API_LOCATION_TYPES}."
                )


def validate_parameter_api_data_types(api_data_types) -> None:
    if not isinstance(api_data_types, list) and api_data_types is not None:
        raise ValueError("parameter api_data_types has to be a list or 'None'.")

    if isinstance(api_data_types, list):
        if not api_data_types:  # api_data_types == []
            raise ValueError("parameter api_data_types cannot be an empty list!")
        for value in api_data_types:
            if value not in API_DATA_TYPES:
                raise ValueError(
                    f"list entries of api_data_types have to be in {API_DATA_TYPES}."
                )


def validate_parameter_api_chunksize(api_chunksize) -> None:
    if not isinstance(api_chunksize, int) and api_chunksize is not None:
        raise ValueError("parameter api_chunksize has to be an integer or 'None'.")


def validate_parameter_bulk_cleansing(bulk_cleansing) -> None:
    if type(bulk_cleansing) != bool:
        raise ValueError("parameter bulk_cleansing has to be boolean")


def validate_parameter_api_limit(api_limit) -> None:
    if not isinstance(api_limit, int) and api_limit is not None:
        raise ValueError("parameter api_limit has to be an integer or 'None'.")


def validate_parameter_date(method, date) -> None:
    if date is None:  # default
        return
    if method == "bulk":
        if date != "today":
            try:
                _ = parse(date)
            except (dateutil.parser._parser.ParserError, TypeError) as e:
                raise ValueError(
                    "Parameter date has to be a proper date in the format yyyymmdd"
                    "or 'today' for bulk method."
                ) from e
    elif method == "API":
        if not isinstance(date, datetime) and date != "latest":
            raise ValueError(
                "parameter api_date has to be 'latest' or a datetime object or 'None' for API method."
            )


def validate_parameter_api_processes(api_processes) -> None:
    if api_processes not in ["max", None] and not isinstance(api_processes, int):
        raise ValueError(
            "parameter api_processes has to be 'max' or an integer or 'None'"
        )
    if api_processes == "max" or isinstance(api_processes, int):
        system = sys.platform
        if system not in ["linux2", "linux"]:
            raise ValueError(
                "The functionality of multiprocessing only works on Linux based systems. "
                "On your system, the parameter api_processes has to be 'None'."
            )


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
            if method == "API" and value not in API_DATA:
                raise ValueError(
                    f"Allowed values for parameter data with API method are {API_DATA}"
                )


def raise_warning_for_invalid_parameter_combinations(
    method,
    bulk_cleansing,
    date,
    api_processes,
    api_data_types,
    api_location_types,
    api_limit,
    api_chunksize,
):
    if method == "API" and bulk_cleansing is not True:
        warn(
            "For method = 'API', bulk download related parameters "
            "(with prefix bulk_) are ignored."
        )

    if method == "bulk" and (
        (
            any(
                parameter is not None
                for parameter in [
                    api_processes,
                    api_data_types,
                    api_location_types,
                ]
            )
            or api_limit != 50
            or api_chunksize != 1000
        )
    ):
        warn(
            "For method = 'bulk', API related parameters (with prefix api_) are ignored."
        )


def transform_data_parameter(
    method, data, api_data_types, api_location_types, **kwargs
):
    """
    Parse input parameters related to data as lists. Harmonize variables for later use.
    Data output depends on the possible data types of chosen method.
    """
    data = kwargs.get("technology", data)
    # parse parameters as list
    if isinstance(data, str):
        data = [data]
    elif data is None:
        data = BULK_DATA if method == "bulk" else API_DATA
    if api_data_types is None:
        api_data_types = API_DATA_TYPES
    if api_location_types is None:
        api_location_types = API_LOCATION_TYPES

    # data input harmonisation
    harmonisation_log = []
    if method == "API" and "permit" in data:
        data.remove("permit")
        api_data_types.append(
            "permit_data"
        ) if "permit_data" not in api_data_types else api_data_types
        harmonisation_log.append("permit")

    if method == "API" and "location" in data:
        data.remove("location")
        api_location_types = API_LOCATION_TYPES
        harmonisation_log.append("location")

    return data, api_data_types, api_location_types, harmonisation_log


def transform_date_parameter(method, date, **kwargs):

    if method == "bulk":
        date = kwargs.get("bulk_date", date)
        date = "today" if date is None else date
    elif method == "API":
        date = kwargs.get("api_date", date)

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


def print_api_settings(
    harmonisation_log,
    data,
    date,
    api_data_types,
    api_chunksize,
    api_limit,
    api_processes,
    api_location_types,
):

    print(
        f"Downloading with soap_API.\n\n   -- API settings --  \nunits after date: "
        f"{date}\nunit download limit per data: "
        f"{api_limit}\nparallel_processes: {api_processes}\nchunksize: "
        f"{api_chunksize}\ndata_api: {data}"
    )
    if "permit" in harmonisation_log:
        print(
            f"data_types: {api_data_types}" "\033[31m",
            "Attention, 'permit_data' was automatically set in api_data_types, as you defined 'permit' in parameter data_api.",
            "\033[m",
        )

    else:
        print(f"data_types: {api_data_types}")

    if "location" in harmonisation_log:
        print(
            "location_types:",
            "\033[31m",
            f"Attention, 'location' is in parameter data. location_types are set to",
            "\033[m",
            f"{api_location_types}"
            "\n                 If you want to change location_types, please remove 'location' from data_api and specify api_location_types."
            "\n   ------------------  \n",
        )

    else:
        print(
            f"location_types: {api_location_types}",
            "\n   ------------------  \n",
        )


def validate_api_credentials() -> None:
    mastr_api = MaStRAPI()
    assert mastr_api.GetAktuellerStandTageskontingent()["Ergebniscode"] == "OK"
