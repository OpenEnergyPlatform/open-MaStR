from datetime import datetime
from open_mastr.mastr import Mastr
import os
import sqlalchemy
import sqlite3
import pytest


@pytest.fixture
def db():
    return Mastr()


@pytest.fixture
def parameter_dict_working():
    parameter_dict = {
        "method": ["API", "bulk"],
        "technology": [
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
            ["wind","solar"]
        ],
        "bulk_date_string": ["today", "20200108"],
        "bulk_cleansing": [True, False],
        "api_processes": [2, 20, None, "max"],
        "api_limit": [15, None],
        "api_date": [None, datetime(2022, 2, 2), "latest"],
        "api_chunksize": [20],
        "api_data_types": ["unit_data", "eeg_data", "kwk_data", "permit_data"],
        "api_location_types": [
            "location_elec_generation",
            "location_elec_consumption",
            "location_gas_generation",
            "location_gas_consumption",
        ],
    }
    return parameter_dict


@pytest.fixture
def parameter_dict_not_working():
    parameter_dict = {
        "method": [5, "BULK", "api"],
        "technology": [
            "wint",
            "Solar",
            "biomasse",
            5,
        ],
        "bulk_date_string": [124, "heute", 123],
        "bulk_cleansing": ["cleansing", 4, None],
        "api_processes": ["20", "None"],
        "api_limit": ["15", "None"],
        "api_date": ["None", "20220202"],
        "api_chunksize": [None, "20"],
        "api_data_types": ["unite_data", 5, None],
        "api_location_types": ["locatione_elec_generation", 5, None],
    }
    return parameter_dict


def test_Mastr_init(db):
    # test if folder structure exists
    assert os.path.exists(db._xml_folder_path)
    assert os.path.exists(db._sqlite_folder_path)

    # test if engine and connection were created
    assert type(db._engine) == sqlalchemy.engine.base.Engine
    assert type(db._sql_connection) == sqlite3.Connection


def test_Mastr_validate_working_parameter(db, parameter_dict_working):
    parameter_dict = {}
    for key in list(parameter_dict_working.keys()):
        parameter_dict[key] = parameter_dict_working[key][0]

    # working parameters
    for key in list(parameter_dict_working.keys()):
        for value in parameter_dict_working[key]:
            parameter_dict[key] = value
            assert (
                db._validate_parameter_format_for_download_method(parameter_dict)
                is None
            )


def test_Mastr_validate_not_working_parameter(
    db, parameter_dict_working, parameter_dict_not_working
):
    parameter_dict_initial = {}
    for key in list(parameter_dict_working.keys()):
        parameter_dict_initial[key] = parameter_dict_working[key][0]

    # not working parameters
    for key in list(parameter_dict_not_working.keys()):
        for value in parameter_dict_not_working[key]:
            # reset parameter_dict so that all parameters are working except one
            parameter_dict = parameter_dict_initial.copy()
            parameter_dict[key] = value
            with pytest.raises(ValueError):
                db._validate_parameter_format_for_download_method(parameter_dict)
