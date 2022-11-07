from utils.data_io import zenodo_upload, read_csv_data
from open_mastr.postprocessing.cleaning import cleaned_data
from open_mastr.postprocessing.postprocessing import postprocess
from open_mastr.postprocessing.postprocessing import to_csv

cleaned = cleaned_data()
zenodo_upload(data_stages=["raw", "cleaned"])
cleaned = read_csv_data("cleaned")
postprocess(cleaned)
to_csv()
