import pytest
import os
from os.path import expanduser
import itertools

import random
from os.path import join

import pandas as pd
from open_mastr import Mastr

from open_mastr.utils import orm
from open_mastr.utils.constants import TECHNOLOGIES, ADDITIONAL_TABLES, BULK_DATA
from open_mastr.utils.config import get_data_version_dir, create_data_dir
from open_mastr.utils.helpers import (
    validate_parameter_format_for_download_method,
    validate_parameter_format_for_mastr_init,
    transform_data_parameter,
    data_to_include_tables,
    session_scope,
    create_db_query,
    db_query_to_csv,
    reverse_unit_type_map,
)


# Check if db is empty
_db_exists = False
_db_folder_path = os.path.join(
    expanduser("~"), ".open-MaStR", "data", "sqlite"
)  # FIXME: use path in tmpdir when implemented
if os.path.isdir(_db_folder_path):
    for entry in os.scandir(path=_db_folder_path):
        _db_path = os.path.join(_db_folder_path, "open-mastr.db")
        if os.path.getsize(_db_path) > 1000000:  # empty db = 327.7kB < 1 MB
            _db_exists = True


@pytest.fixture
def db():
    return Mastr()


def test_Mastr_validate_working_parameter():
    valid_params = {
        "method": ["bulk"],
        "data": [
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
            "deleted_units",
            "deleted_market_actors",
            "retrofit_units",
            None,
            ["wind", "solar"],
        ],
        "date": ["today", "20200108", "existing"],
        "bulk_cleansing": [True, False],
    }
    method_vals = valid_params["method"]
    data_vals = valid_params["data"]
    date_vals = valid_params["date"]
    bulk_cleansing_vals = valid_params["bulk_cleansing"]
    combinations = list(
        itertools.product(method_vals, data_vals, date_vals, bulk_cleansing_vals)
    )

    # working parameters
    for method, data, date, bulk_cleansing in combinations:
        assert (
            validate_parameter_format_for_download_method(
                method,
                data,
                date,
                bulk_cleansing,
            )
            is None
        )


def test_Mastr_validate_not_working_parameter():
    invalid_params = {
        "method": [5, "BULK", "api"],
        "data": ["wint", "Solar", "biomasse", 5, []],
        "date": [124, "heute", 123],
        "bulk_cleansing": ["cleansing", 4, None],
    }

    for key, invalid_values in invalid_params.items():
        for invalid_value in invalid_values:
            params = {
                "method": "bulk",
                "data": "wind",
                "date": "today",
                "bulk_cleansing": True,
            }
            params[key] = invalid_value

            with pytest.raises(ValueError):
                validate_parameter_format_for_download_method(
                    params["method"],
                    params["data"],
                    params["date"],
                    params["bulk_cleansing"],
                )


def get_parameters_from_parameter_dict(parameter_dict):
    method = parameter_dict["method"]
    data = parameter_dict["data"]
    date = parameter_dict["date"]
    bulk_cleansing = parameter_dict["bulk_cleansing"]
    return (
        method,
        data,
        date,
        bulk_cleansing,
    )


def test_validate_parameter_format_for_mastr_init(db):
    engine_list_working = ["sqlite", db.engine]
    engine_list_failing = ["HI", 12]

    for engine in engine_list_working:
        assert validate_parameter_format_for_mastr_init(engine) is None

    for engine in engine_list_failing:
        with pytest.raises(ValueError):
            validate_parameter_format_for_mastr_init(engine)


def test_transform_data_parameter():
    data_first = transform_data_parameter(
        data="wind",
    )
    assert data_first == ["wind"]

    data_second = transform_data_parameter(
        data=None,
    )
    assert data_second == BULK_DATA


def test_data_to_include_tables():
    # Prepare
    include_tables_list = [
        "anlageneegwind",
        "einheitenwind",
        "anlageneegwasser",
        "einheitenwasser",
    ]
    include_tables_str = ["einheitenstromverbraucher"]

    map_to_db_table_list = ["market_actors", "market_roles"]
    map_to_db_table_str = ["locations_extended"]

    # Assert
    assert include_tables_list == data_to_include_tables(
        data=["wind", "hydro"], mapping="write_xml"
    )
    assert include_tables_str == data_to_include_tables(
        data=["electricity_consumer"], mapping="write_xml"
    )
    assert map_to_db_table_list == data_to_include_tables(
        data=["market"], mapping="export_db_tables"
    )
    assert map_to_db_table_str == data_to_include_tables(
        data=["location"], mapping="export_db_tables"
    )


def test_data_to_include_tables_error():
    # test for non-existent 'mapping' parameter input
    with pytest.raises(
        NotImplementedError,
        match="This function is only implemented for 'write_xml' and 'export_db_tables',"
        " please specify when calling the function.",
    ):
        data_to_include_tables(data=["wind", "hydro"], mapping="X32J_22")


@pytest.mark.skipif(
    not _db_exists,
    reason="The database is smaller than 1 MB, thus suspected to be empty or non-existent.",
)
def test_db_query_to_csv(tmpdir, engine):
    """
    The test checks for 2 random tech and 2 random additional tables:
    1. If csv's have been created and encoded in 'utf-8' and are not empty
    2. For techs, if limited (limit=60) EinheitMastrNummer in basic_units are included in CSV file
    3. For additional_tables, if csv is not empty

    Parameters
    ----------
    tmpdir - temporary directory to test export
    engine - sqlite engine from conftest.py

    Returns
    -------

    """
    unit_type_map_reversed = reverse_unit_type_map()

    # FIXME: Define path to tmpdir (pytest internal temporary dir)
    # to not delete actual data export, when test is run locally
    #  Use the parameter that will be implemented in #394
    # create data dir
    create_data_dir()

    techs = random.sample(TECHNOLOGIES, k=2)
    addit_tables = random.sample(ADDITIONAL_TABLES, k=2)

    with session_scope(engine=engine) as session:
        for tech in techs:
            db_query_to_csv(
                db_query=create_db_query(tech=tech, limit=60, engine=engine),
                data_table=tech,
                chunksize=20,
            )

            # Test if LIMITED EinheitMastrNummer in basic_units are included in CSV file
            csv_path = join(
                get_data_version_dir(),
                f"bnetza_mastr_{tech}_raw.csv",
            )
            # check if table has been created and encoding is correct
            df_tech = pd.read_csv(
                csv_path, index_col="EinheitMastrNummer", encoding="utf-8"
            )

            # check whether table is empty (returns True if it is)
            assert False == df_tech.empty

            units = session.query(orm.BasicUnit.EinheitMastrNummer).filter(
                orm.BasicUnit.Einheittyp == unit_type_map_reversed[tech]
            )
            set_MastrNummer = {unit.EinheitMastrNummer for unit in units}
            for idx in df_tech.index:
                assert idx in set_MastrNummer

            # FIXME: delete when tmpdir is implemented
            # remove file in data folder
            os.remove(csv_path)

        for addit_table in addit_tables:
            csv_path = join(
                get_data_version_dir(),
                f"bnetza_mastr_{addit_table}_raw.csv",
            )

            db_query_to_csv(
                db_query=create_db_query(
                    additional_table=addit_table, limit=60, engine=engine
                ),
                data_table=addit_table,
                chunksize=20,
            )

            # check if table has been created and encoding is correct
            df_at = pd.read_csv(csv_path, encoding="utf-8")

            # check if table is empty (returns True if it is)
            assert False == df_at.empty

            # FIXME: delete when tmpdir is implemented
            # remove file in data folder
            os.remove(csv_path)

    # FIXME: delete when tmpdir is implemented
    # delete empty data dir
    os.rmdir(get_data_version_dir())
