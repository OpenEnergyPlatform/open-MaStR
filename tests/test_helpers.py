from open_mastr.utils.constants import API_LOCATION_TYPES
from open_mastr.utils.helpers import (
    validate_parameter_format_for_download_method,
    validate_parameter_format_for_mastr_init,
    validate_api_credentials,
    transform_data_parameter,
    data_to_include_tables,
)
import pytest
import sys
from datetime import datetime
from open_mastr import Mastr


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
            None,
            ["wind", "solar"],
        ],
        "date": ["today", "20200108"],
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
    engine_list_working = ["sqlite", "docker-postgres", db.engine]
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
        match="This function is only implemented for 'write_xml' and 'export_db_tables', please specify when calling the function.",
    ):
        data_to_include_tables(data=["wind", "hydro"], mapping="X32J_22")
