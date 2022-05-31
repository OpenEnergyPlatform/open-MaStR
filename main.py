#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
open-MaStR - Main file

Bulk: Download XML-Dump and fill in local SQLite database.
API: Download latest entries using the SOAP-API.

SPDX-License-Identifier: AGPL-3.0-or-later
"""

from open_mastr import Mastr

# from open_mastr.postprocessing.cleaning import cleaned_data
# from postprocessing.postprocessing import postprocess
# from postprocessing.postprocessing import to_csv

db = Mastr()

## Download
technology_bulk = ["biomass",
                   "combustion",
                   "gas",
                   "gsgk",
                   "hydro",
                   "nuclear",
                   "solar",
                   "storage",
                   "wind",

                   "balancing_area",
                   "electricity_consumer",
                   "grid",
                   "location",
                   "market",
                   "permit"
                   ]

technology_api = ["biomass",
                  "combustion",
                  "gas",
                  "gsgk",
                  "hydro",
                  "nuclear",
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

if __name__ == "__main__":

    # Bulk download
    db.download(method="bulk",
                technology=technology_bulk,
                api_data_types=api_data_types)

    # API download
    db.download(method="API",
                api_date='latest',
                api_chunksize=1000,
                api_limit=10,
                api_processes='max',
                technology=technology_api,
                api_data_types=api_data_types,
                api_location_types=api_location_types)

    db.to_csv()


    ## Postprocessing

    #cleaned = cleaned_data()
    #postprocess(cleaned)

    #to_csv()
