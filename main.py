#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
open-MaStR - Main file

Bulk: Download XML-Dump and fill in local SQLite database.
API: Download latest entries using the SOAP-API.

SPDX-License-Identifier: AGPL-3.0-or-later
"""

from open_mastr import Mastr
import os

## specify download parameter

# set custom output path for: csv-export, database, xml-export.
# os.environ['OUTPUT_PATH'] = r"/your/custom/output_path"

# optimize bulk downloads and use the recommended number of processes
# os.environ['USE_RECOMMENDED_NUMBER_OF_PROCESSES'] = "True"

# set up your own number of processes for bulk download
# os.environ['NUMBER_OF_PROCESSES'] = "your_number"

# bulk download
bulk_date = "today"
bulk_cleansing = True
data_bulk = [
    "biomass",
    "combustion",
    "gsgk",
    "hydro",
    "nuclear",
    "solar",
    "storage",
    "wind",
    "balancing_area",
    "electricity_consumer",
    "gas",
    "grid",
    "location",
    "market",
    "permit",
]

# API download
# for parameter explanation see: https://open-mastr.readthedocs.io/en/latest/advanced/#soap-api-download

api_date = "latest"
api_chunksize = 10
api_limit = 10
api_processes = None

data_api = [
    "biomass",
    "combustion",
    "gsgk",
    "hydro",
    "nuclear",
    "solar",
    "storage",
    "wind",
]

api_data_types = ["unit_data", "eeg_data", "kwk_data", "permit_data"]

api_location_types = [
    "location_elec_generation",
    "location_elec_consumption",
    "location_gas_generation",
    "location_gas_consumption",
]

if __name__ == "__main__":
    # instantiate Mastr class
    db = Mastr()

    ## download Markstammdatenregister
    # bulk download
    db.download(method="bulk", data=data_bulk, date=bulk_date, bulk_cleansing=True)

    # API download
    db.download(
        method="API",
        data=data_api,
        date=api_date,
        api_processes=api_processes,
        api_limit=api_limit,
        api_chunksize=api_chunksize,
        api_data_types=api_data_types,
        api_location_types=api_location_types,
    )

    ## export to csv
    """
    Technology-related tables are exported as joined, whereas additional tables
    are duplicated as they are in the database. 
    """
    db.to_csv()
