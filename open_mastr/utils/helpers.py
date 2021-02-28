from contextlib import contextmanager
from sqlalchemy import create_engine
from sqlalchemy.orm import Query, sessionmaker


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
