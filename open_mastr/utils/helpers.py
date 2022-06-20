from contextlib import contextmanager
from sqlalchemy import create_engine
import sqlalchemy
from sqlalchemy.orm import Query, sessionmaker
import os
from os.path import expanduser
from warnings import warn
from dateutil.parser import parse
import dateutil
from datetime import datetime
import sys
import subprocess


def chunks(lst, n):
    """Yield successive n-sized chunks from lst.

    `Credits <https://stackoverflow.com/questions/312443/how-do-you-split-a-list-into-evenly-sized-chunks>`_
    """
    if isinstance(lst, Query):
        length = lst.count()
    else:
        length = len(lst)
    for i in range(0, length, n):
        yield lst[i : i + n]


def create_database_engine(engine) -> sqlalchemy.engine.Engine:
    if engine == "sqlite":
        sqlite_database_path = os.environ.get(
            "SQLITE_DATABASE_PATH",
            os.path.join(
                expanduser("~"), ".open-MaStR", "data", "sqlite", "open-mastr.db"
            ),
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


def validate_parameter_format_for_mastr_init(engine) -> None:
    if engine not in ["sqlite", "docker-postgres"]:
        if not isinstance(engine, sqlalchemy.engine.Engine):
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
    # method parameter

    if method != "bulk" and method != "API":
        raise ValueError("parameter method has to be either 'bulk' or 'API'.")

    if method == "API":
        if bulk_cleansing is not True or bulk_date_string != "today":
            warn(
                "For method = 'API', bulk download related parameters "
                "(with prefix bulk_) are ignored."
            )

    if method == "bulk":
        if (
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
        ):
            warn(
                "For method = 'bulk', API related parameters (with prefix api_) are ignored."
            )

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

    if bulk_date_string != "today":
        try:
            _ = parse(bulk_date_string)
        except (dateutil.parser._parser.ParserError, TypeError):
            raise ValueError(
                "parameter bulk_date_string has to be a proper date in the format yyyymmdd"
                "or 'today'."
            )

    if type(bulk_cleansing) != bool:
        raise ValueError("parameter bulk_cleansing has to be boolean")

    if (
        api_processes != "max"
        and not isinstance(api_processes, int)
        and api_processes is not None
    ):
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

    if not isinstance(api_limit, int) and api_limit is not None:
        raise ValueError("parameter api_limit has to be an integer or 'None'.")

    if (
        not isinstance(api_date, datetime)
        and api_date != "latest"
        and api_date is not None
    ):
        raise ValueError(
            "parameter api_date has to be 'latest' or a datetime object or 'None'."
        )

    if not isinstance(api_chunksize, int) and api_chunksize is not None:
        raise ValueError("parameter api_chunksize has to be an integer or 'None'.")

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
