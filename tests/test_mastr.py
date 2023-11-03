from open_mastr.mastr import Mastr
import os
import sqlalchemy
import pytest
import pandas as pd
from open_mastr.utils.constants import TRANSLATIONS


@pytest.fixture
def db():
    return Mastr()


@pytest.fixture
def db_path():
    return os.path.join(
        os.path.expanduser("~"), ".open-MaStR", "data", "sqlite", "mastr-test.db"
    )


@pytest.fixture
def db_translated(db_path):
    engine = sqlalchemy.create_engine(f"sqlite:///{db_path}")
    db_api = Mastr(engine=engine)

    db_api.download(method="API", api_limit=10)
    db_api.translate()

    return db_api


def test_Mastr_init(db):
    # test if folder structure exists
    assert os.path.exists(db.home_directory)
    assert os.path.exists(db._sqlite_folder_path)

    # test if engine and connection were created
    assert type(db.engine) == sqlalchemy.engine.Engine


def test_Mastr_translate(db_translated, db_path):
    # test if database was renamed correctly
    transl_path = db_path[:-3] + "-translated.db"
    assert os.path.exists(transl_path)

    # test if columns got translated
    inspector = sqlalchemy.inspect(db_translated.engine)
    table_names = inspector.get_table_names()

    for table in table_names:
        for column in inspector.get_columns(table):
            column = column["name"]
            assert column in TRANSLATIONS.values() or column not in TRANSLATIONS.keys()

    # test if new translated version replaces previous one
    db_translated.engine.dispose()
    engine = sqlalchemy.create_engine(f"sqlite:///{db_path}")
    db_empty = Mastr(engine=engine)
    db_empty.translate()

    for table in table_names:
        assert pd.read_sql(sql=table, con=db_empty.engine).shape[0] == 0
