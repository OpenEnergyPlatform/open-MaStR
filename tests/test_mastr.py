from open_mastr.mastr import Mastr
import os
import sqlalchemy
import pytest


@pytest.fixture
def db():
    return Mastr()


def test_Mastr_init(db):
    # test if folder structure exists
    assert os.path.exists(db.home_directory)
    assert os.path.exists(db._sqlite_folder_path)

    # test if engine and connection were created
    assert type(db.engine) == sqlalchemy.engine.Engine

