from contextlib import contextmanager
from sqlalchemy import create_engine
import sqlalchemy
from sqlalchemy.orm import Query, sessionmaker
import os
from os.path import expanduser

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
        yield lst[i: i + n]


def db_engine():  # TODO: Delete this function and merge functionality to create_database_engine
    return create_engine(DB_URL)


def create_database_engine(engine):
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
        return create_engine(db_url)

    if type(engine) == sqlalchemy.engine.Engine:
        return engine


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
