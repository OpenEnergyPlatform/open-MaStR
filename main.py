from open_mastr import Mastr

# from open_mastr.postprocessing.cleaning import cleaned_data
# from postprocessing.postprocessing import postprocess
# from postprocessing.postprocessing import to_csv

db = Mastr()

## Download
technology = ["wind",
              "biomass",
              "combustion",
              "gsgk",
              "hydro",
              "nuclear",
              "storage"]
# excluded: "solar"

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

db.download(method="bulk",
            technology=technology,
            api_data_types=api_data_types)

db.download(method="API",
            api_date='latest',
            api_limit=10,
            technology=technology,
            api_data_types=api_data_types,
            api_location_types=api_location_types)

db.to_csv()


## Postprocessing

#cleaned = cleaned_data()
#postprocess(cleaned)

#to_csv()
