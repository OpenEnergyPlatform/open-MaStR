from contextlib import contextmanager
from sqlalchemy import create_engine
from sqlalchemy.orm import Query, sessionmaker

from open_mastr.settings import DB_URL


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


def db_engine():  # TODO: Include in _create_database in MaStR() class
    return create_engine(DB_URL)


@contextmanager
def session_scope():
    """Provide a transactional scope around a series of operations."""
    Session = sessionmaker(bind=db_engine())
    session = Session()
    try:
        yield session
        session.commit()
    except:
        session.rollback()
        raise
    finally:
        session.close()


def technology_input_harmonisation(
    technology, api_date, api_data_types, api_chunksize, api_limit, api_processes
):

    print(
        f"Downloading with soap_API.\n\n   -- API settings --  \nunits after date: "
        f"{api_date}\nunit download limit per technology: "
        f"{api_limit}\nparallel_processes: {api_processes}\nchunksize: "
        f"{api_chunksize}\ntechnology_api: {technology}\ndata_types: {api_data_types}"
    )

    if "location" in technology:
        technology.remove("location")

        api_location_types = [
            "location_elec_generation",
            "location_elec_consumption",
            "location_gas_generation",
            "location_gas_consumption",
        ]

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

