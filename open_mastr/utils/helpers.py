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
    technology,
    bulk_date_string,
    bulk_cleansing,
    api_processes,
    api_limit,
    api_date,
    api_chunksize,
    api_data_types,
    api_location_types,
) -> None:

    validate_parameter_method(method)
    validate_parameter_technology(technology)
    validate_parameter_bulk_date_string(bulk_date_string)
    validate_parameter_bulk_cleansing(bulk_cleansing)
    validate_parameter_api_processes(api_processes)
    validate_parameter_api_limit(api_limit)
    validate_parameter_api_date(api_date)
    validate_parameter_api_chunksize(api_chunksize)
    validate_parameter_api_data_types(api_data_types)
    validate_parameter_api_location_types(api_location_types)

    raise_warning_for_invalid_parameter_combinations(
        method,
        bulk_cleansing,
        bulk_date_string,
        api_processes,
        api_date,
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
        for value in api_location_types:
            if value not in [
                "location_elec_generation",
                "location_elec_consumption",
                "location_gas_generation",
                "location_gas_consumption",
                None,
            ]:
                raise ValueError(
                    'list entries of api_data_types have to be "location_elec_generation",'
                    '"location_elec_consumption", "location_gas_generation" or'
                    ' "location_gas_consumption".'
                )


def validate_parameter_api_data_types(api_data_types) -> None:
    if not isinstance(api_data_types, list) and api_data_types is not None:
        raise ValueError("parameter api_data_types has to be a list or 'None'.")

    if isinstance(api_data_types, list):
        for value in api_data_types:
            if value not in [
                "unit_data",
                "eeg_data",
                "kwk_data",
                "permit_data",
                None,
            ]:
                raise ValueError(
                    'list entries of api_data_types have to be "unit_data", '
                    '"eeg_data", "kwk_data" '
                    'or "permit_data".'
                )


def validate_parameter_api_chunksize(api_chunksize) -> None:
    if not isinstance(api_chunksize, int) and api_chunksize is not None:
        raise ValueError("parameter api_chunksize has to be an integer or 'None'.")


def validate_parameter_bulk_cleansing(bulk_cleansing) -> None:
    if type(bulk_cleansing) != bool:
        raise ValueError("parameter bulk_cleansing has to be boolean")


def validate_parameter_api_date(api_date) -> None:
    if not isinstance(api_date, datetime) and api_date not in ["latest", None]:
        raise ValueError(
            "parameter api_date has to be 'latest' or a datetime object or 'None'."
        )


def validate_parameter_api_limit(api_limit) -> None:
    if not isinstance(api_limit, int) and api_limit is not None:
        raise ValueError("parameter api_limit has to be an integer or 'None'.")


def validate_parameter_bulk_date_string(bulk_date_string) -> None:
    if bulk_date_string != "today":
        try:
            _ = parse(bulk_date_string)
        except (dateutil.parser._parser.ParserError, TypeError) as e:
            raise ValueError(
                "parameter bulk_date_string has to be a proper date in the format yyyymmdd"
                "or 'today'."
            ) from e


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


def validate_parameter_technology(technology) -> None:
    if not isinstance(technology, (str, list)) and technology is not None:
        raise ValueError("parameter technology has to be a string, list, or None")
    if isinstance(technology, str):
        technology = [technology]
    if isinstance(technology, list):
        bulk_technologies = [
            "wind",
            "solar",
            "biomass",
            "hydro",
            "gsgk",
            "combustion",
            "nuclear",
            "gas",
            "storage",
            "electricity_consumer",
            "location",
            "market",
            "grid",
            "balancing_area",
            "permit",
        ]
        for value in technology:
            if value not in bulk_technologies:
                raise ValueError(
                    'Allowed values for parameter technology are "wind", "solar",'
                    '"biomass", "hydro", "gsgk", "combustion", "nuclear", "gas", '
                    '"storage", "electricity_consumer", "location", "market", '
                    '"grid", "balancing_area" or "permit"'
                )


def raise_warning_for_invalid_parameter_combinations(
    method,
    bulk_cleansing,
    bulk_date_string,
    api_processes,
    api_date,
    api_data_types,
    api_location_types,
    api_limit,
    api_chunksize,
):
    if method == "API" and (bulk_cleansing is not True or bulk_date_string != "today"):
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
                    api_date,
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


def technology_input_harmonisation(technology, api_data_types, api_location_types):
    harmonisation_log = []

    if "permit" in technology:
        technology.remove("permit")
        api_data_types.append(
            "permit_data"
        ) if "permit_data" not in api_data_types else api_data_types
        harmonisation_log.append("permit")

    if "location" in technology:
        technology.remove("location")
        api_location_types = [
            "location_elec_generation",
            "location_elec_consumption",
            "location_gas_generation",
            "location_gas_consumption",
        ]
        harmonisation_log.append("location")
        # return changed api_location_types only if "location" in technology, else None

    return harmonisation_log, api_data_types, api_location_types


def print_api_settings(
    harmonisation_log,
    technology,
    api_date,
    api_data_types,
    api_chunksize,
    api_limit,
    api_processes,
    api_location_types,
):

    print(
        f"Downloading with soap_API.\n\n   -- API settings --  \nunits after date: "
        f"{api_date}\nunit download limit per technology: "
        f"{api_limit}\nparallel_processes: {api_processes}\nchunksize: "
        f"{api_chunksize}\ntechnology_api: {technology}"
    )
    if "permit" in harmonisation_log:
        print(
            f"data_types: {api_data_types}" "\033[31m",
            f"Attention, 'permit_data' was automatically set in api_data_types, as you defined 'permit' in parameter technology_api.",
            "\033[m",
        )

    else:
        print(f"data_types: {api_data_types}")

    if "location" in harmonisation_log:
        print(
            "location_types:",
            "\033[31m",
            f"Attention, 'location' is in parameter technology_api. location_types are set to",
            "\033[m",
            f"{api_location_types}"
            "\n                 If you want to change location_types, please remove 'location' from technology_api and specify api_location_types."
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
