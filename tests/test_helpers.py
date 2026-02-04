import pytest
import os
from os.path import expanduser
from pathlib import Path
import itertools

import random
from os.path import join

import pandas as pd
from open_mastr import Mastr
from zipfile import ZipFile

from open_mastr.utils.constants import BULK_DATA
from open_mastr.utils.helpers import (
    validate_parameter_format_for_download_method,
    validate_parameter_format_for_mastr_init,
    transform_data_parameter,
    data_to_include_tables,
    delete_zip_file_if_corrupted,
)


@pytest.fixture
def mastr(tmp_path: Path):
    output_dir = tmp_path / "output_dir"
    return Mastr(output_dir=output_dir)


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


def test_validate_parameter_format_for_mastr_init(mastr):
    engine_list_working = ["sqlite", mastr.engine]
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
    include_tables_list = {
        "anlageneegwind",
        "einheitenwind",
        "anlageneegwasser",
        "einheitenwasser",
    }
    include_tables_str = {"einheitenstromverbraucher"}

    map_to_db_table_list = {"market_actors", "market_roles"}
    map_to_db_table_str = {"locations_extended"}

    # Assert
    assert include_tables_list == data_to_include_tables(
        data={"wind", "hydro"}, mapping="write_xml"
    )
    assert include_tables_str == data_to_include_tables(
        data={"electricity_consumer"}, mapping="write_xml"
    )
    assert map_to_db_table_list == data_to_include_tables(
        data={"market"}, mapping="export_db_tables"
    )
    assert map_to_db_table_str == data_to_include_tables(
        data={"location"}, mapping="export_db_tables"
    )


def test_data_to_include_tables_error():
    # test for non-existent 'mapping' parameter input
    with pytest.raises(
        NotImplementedError,
        match="This function is only implemented for 'write_xml' and 'export_db_tables',"
        " please specify when calling the function.",
    ):
        data_to_include_tables(data=["wind", "hydro"], mapping="X32J_22")


def test_delete_zip_file_if_corrupted():
    test_zip_path = os.path.join("tests", "test.zip")
    with ZipFile(test_zip_path, "w") as zf:
        zf.writestr(os.path.join("tests", "file.txt"), "Hello, world!")
    with open(test_zip_path, "wb+") as f:
        f.seek(10)
        f.write(b"\xff\xff\xff\xff")

    delete_zip_file_if_corrupted(test_zip_path)
    assert not os.path.exists(test_zip_path)
