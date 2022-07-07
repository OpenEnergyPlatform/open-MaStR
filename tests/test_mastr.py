from open_mastr.mastr import Mastr
import os
import sqlalchemy
import pytest


@pytest.fixture
def db():
    return Mastr()


def test_Mastr_init(db):
    # test if folder structure exists
    assert os.path.exists(db._xml_folder_path)
    assert os.path.exists(db._sqlite_folder_path)

    # test if engine and connection were created
    assert type(db._engine) == sqlalchemy.engine.base.Engine


def test_technology_to_include_tables(db):
    # Prepare
    include_tables_list = [
        "anlageneegwind",
        "einheitenwind",
        "anlageneegwasser",
        "einheitenwasser",
    ]
    include_tables_str = ["einheitenstromverbraucher"]

    # Assert
    assert include_tables_list == db._technology_to_include_tables(
        technology=["wind", "hydro"]
    )
    assert include_tables_str == db._technology_to_include_tables(
        technology="electricity_consumer"
    )
    assert "anlageneegwind" in db._technology_to_include_tables(technology=None)
    assert 28 == len(db._technology_to_include_tables(technology=None))
