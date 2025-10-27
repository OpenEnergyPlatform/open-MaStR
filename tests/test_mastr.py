from open_mastr.mastr import Mastr
import os
import sqlalchemy
import pytest
import pandas as pd
from open_mastr.utils.constants import TRANSLATIONS


@pytest.fixture
def db_path():
    return os.path.join(
        os.path.expanduser("~"), ".open-MaStR", "data", "sqlite", "mastr-test.db"
    )


@pytest.fixture
def db():
    path = os.path.join(
        os.path.expanduser("~"), ".open-MaStR", "data", "sqlite", "mastr-test.db"
    )
    db = Mastr(engine=sqlalchemy.create_engine(f"sqlite:///{path}"))
    db.download(data="electricity_consumer")
    return db


def test_Mastr_init(db):
    # test if folder structure exists
    assert os.path.exists(db.home_directory)
    assert os.path.exists(db._sqlite_folder_path)

    # test if engine and connection were created
    assert type(db.engine) == sqlalchemy.engine.Engine


def test_Mastr_translate(db, db_path):
    db.translate()
    # test if database was renamed correctly
    transl_path = db_path[:-3] + "-translated.db"
    assert os.path.exists(transl_path)
    # test if columns got translated
    inspector = sqlalchemy.inspect(db.engine)
    table_names = inspector.get_table_names()
    for table in table_names:
        for column in inspector.get_columns(table):
            column = column["name"]
            assert column in TRANSLATIONS.values() or column not in TRANSLATIONS.keys()


def test_mastr_download(db):
    df_consumer = pd.read_sql("electricity_consumer", con=db.engine)
    assert len(df_consumer) > 500
