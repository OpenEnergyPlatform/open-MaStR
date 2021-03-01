from contextlib import contextmanager
from sqlalchemy import create_engine
from sqlalchemy.orm import Query, sessionmaker


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


def db_engine():
    return create_engine(
        "postgresql+psycopg2://open-mastr:open-mastr@localhost:55443/open-mastr", echo=False
    )


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
