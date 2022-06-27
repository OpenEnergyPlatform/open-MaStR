#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
open-MaStR - Main file

Bulk: Download XML-Dump and fill in local SQLite database.
API: Download latest entries using the SOAP-API.

SPDX-License-Identifier: AGPL-3.0-or-later
"""

from open_mastr import Mastr
from open_mastr.soap_api.mirror import MaStRMirror
from postprocessing.cleaning import cleaned_data


## specify download parameter

# bulk download

technology_bulk = ["biomass",
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
                   "permit"
                   ]

# API download
# for parameter explanation see: https://open-mastr.readthedocs.io/en/latest/getting_started.html#api-download

api_date = 'latest'
api_chunksize = 1000
api_limit = 10
api_processes = 'max'

technology_api = ["biomass",
                  "combustion",
                  "gsgk",
                  "hydro",
                  "nuclear",
                  "solar",
                  "storage",
                  "wind", ]

api_data_types = [
    "unit_data",
    "eeg_data",
    "kwk_data",
    "permit_data"
]

api_location_types = [
    "location_elec_generation",
    "location_elec_consumption",
    "location_gas_generation",
    "location_gas_consumption",
]

# instantiate Mastr class
db = Mastr()

if __name__ == "__main__":

    ## download Markstammdatenregister
    # bulk download
    db.download(method="bulk",
                technology=technology_bulk,
                )

    # API download
    db.download(method="API",
                api_date=api_date,
                api_chunksize=api_chunksize,
                api_limit=api_limit,
                api_processes=api_processes,
                technology=technology_api,
                api_data_types=api_data_types,
                api_location_types=api_location_types)

    ## export to csv
    '''
    Currently two export options exist.
    First: Export exact copy of database tables (referred to as: database dublicate)
    Second: Export technology-specific tables (referred to as: API export)
    '''
    # database dublicate
    # export copy of each database table, if respective tables are not empty
    db.to_csv()

    # API export
    # API export joins technology-specific tables from database and exports them to one csv per technology

    # instantiate MaStRMirror class
    api_export = MaStRMirror(engine=db.engine)

    # fill basic unit table, after downloading with method = 'bulk' to use API export functions
    api_export.reverse_fill_basic_units()

    # export to csv per technology
    # the csv files are exported unmodified and labeled as "raw".
    api_export.to_csv(
       technology=technology_api, additional_data=api_data_types, statistic_flag=None, limit=None
    )

    # clean raw csv's and create cleaned csv's
    cleaned = cleaned_data()

    # export location types
    for location_type in api_location_types:
        api_export.locations_to_csv(location_type=location_type, limit=None)
