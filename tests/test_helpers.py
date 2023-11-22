import pytest
import os
from os.path import expanduser
import sys
import random
from os.path import join
from datetime import datetime
import pandas as pd
from open_mastr import Mastr

from open_mastr.utils import orm
from open_mastr.utils.constants import (
    API_LOCATION_TYPES,
    TECHNOLOGIES,
    ADDITIONAL_TABLES,
)
from open_mastr.utils.config import get_data_version_dir, create_data_dir
from open_mastr.utils.helpers import (
    validate_parameter_format_for_download_method,
    validate_parameter_format_for_mastr_init,
    validate_api_credentials,
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


@pytest.fixture
def parameter_dict_working_list():
    parameter_dict_bulk = {
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
            "retrofit_units",
            None,
            ["wind", "solar"],
        ],
        "date": ["today", "20200108", "existing"],
        "bulk_cleansing": [True, False],
        "api_processes": [None],
        "api_limit": [50],
        "api_chunksize": [1000],
        "api_data_types": [None],
        "api_location_types": [None],
    }

    parameter_dict_API = {
        "method": ["API"],
        "data": [
            "wind",
            "solar",
            "biomass",
            "hydro",
            "gsgk",
            "combustion",
            "nuclear",
            "storage",
            "location",
            "permit",
            None,
            ["wind", "solar"],
        ],
        "date": [None, datetime(2022, 2, 2), "latest"],
        "bulk_cleansing": [True],
        "api_processes": [None]
        if sys.platform not in ["linux2", "linux"]
        else [2, 20, None, "max"],
        "api_limit": [15, None],
        "api_chunksize": [20],
        "api_data_types": [
            ["unit_data", "eeg_data", "kwk_data", "permit_data"],
            ["unit_data"],
            None,
        ],
        "api_location_types": [
            ["location_elec_generation", "location_elec_consumption"],
            [
                "location_elec_generation",
                "location_elec_consumption",
                "location_gas_generation",
                "location_gas_consumption",
            ],
            None,
        ],
    }
    return [parameter_dict_bulk, parameter_dict_API]


@pytest.fixture
def parameter_dict_not_working():
    parameter_dict = {
        "method": [5, "BULK", "api"],
        "data": ["wint", "Solar", "biomasse", 5, []],
        "date": [124, "heute", 123],
        "bulk_cleansing": ["cleansing", 4, None],
        "api_processes": ["20", "None"],
        "api_limit": ["15", "None"],
        "api_chunksize": ["20"],
        "api_data_types": ["unite_data", 5, []],
        "api_location_types": ["locatione_elec_generation", 5, []],
    }
    return parameter_dict


def test_Mastr_validate_working_parameter(parameter_dict_working_list):
    for parameter_dict_working in parameter_dict_working_list:
        parameter_dict = {
            key: parameter_dict_working[key][0] for key in parameter_dict_working
        }

        # working parameters
        for key in list(parameter_dict_working.keys()):
            for value in parameter_dict_working[key]:
                parameter_dict[key] = value
                (
                    method,
                    data,
                    date,
                    bulk_cleansing,
                    api_processes,
                    api_limit,
                    api_chunksize,
                    api_data_types,
                    api_location_types,
                ) = get_parameters_from_parameter_dict(parameter_dict)

                assert (
                    validate_parameter_format_for_download_method(
                        method,
                        data,
                        date,
                        bulk_cleansing,
                        api_processes,
                        api_limit,
                        api_chunksize,
                        api_data_types,
                        api_location_types,
                    )
                    is None
                )


def test_Mastr_validate_not_working_parameter(
    parameter_dict_working_list, parameter_dict_not_working
):
    for parameter_dict_working in parameter_dict_working_list:
        parameter_dict_initial = {
            key: parameter_dict_working[key][0] for key in parameter_dict_working
        }

        # not working parameters
        for key in list(parameter_dict_not_working.keys()):
            for value in parameter_dict_not_working[key]:
                # reset parameter_dict so that all parameters are working except one
                parameter_dict = parameter_dict_initial.copy()
                parameter_dict[key] = value
                (
                    method,
                    data,
                    date,
                    bulk_cleansing,
                    api_processes,
                    api_limit,
                    api_chunksize,
                    api_data_types,
                    api_location_types,
                ) = get_parameters_from_parameter_dict(parameter_dict)
                with pytest.raises(ValueError):
                    validate_parameter_format_for_download_method(
                        method,
                        data,
                        date,
                        bulk_cleansing,
                        api_processes,
                        api_limit,
                        api_chunksize,
                        api_data_types,
                        api_location_types,
                    )


def get_parameters_from_parameter_dict(parameter_dict):
    method = parameter_dict["method"]
    data = parameter_dict["data"]
    date = parameter_dict["date"]
    bulk_cleansing = parameter_dict["bulk_cleansing"]
    api_processes = parameter_dict["api_processes"]
    api_limit = parameter_dict["api_limit"]
    api_chunksize = parameter_dict["api_chunksize"]
    api_data_types = parameter_dict["api_data_types"]
    api_location_types = parameter_dict["api_location_types"]
    return (
        method,
        data,
        date,
        bulk_cleansing,
        api_processes,
        api_limit,
        api_chunksize,
        api_data_types,
        api_location_types,
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
    (data, api_data_types, api_location_types, harm_log,) = transform_data_parameter(
        method="API",
        data=["wind", "location"],
        api_data_types=["eeg_data"],
        api_location_types=None,
    )

    assert data == ["wind"]
    assert api_data_types == ["eeg_data"]
    assert api_location_types == API_LOCATION_TYPES
    assert harm_log == ["location"]


def test_validate_api_credentials():
    validate_api_credentials()


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


def test_save_metadata():
    # FIXME: implement in #386
    pass
