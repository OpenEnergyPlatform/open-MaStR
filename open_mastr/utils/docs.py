import os

from open_mastr.soap_api.metadata.create import column_docs_csv


def generate_data_docs():
    technologies = [
        "solar",
        "wind",
        "biomass",
        "combustion",
        "gsgk",
        "hydro",
        "nuclear",
        "storage",
    ]
    raw_data_doc_files = column_docs_csv(technologies, "_data/raw/")

    raw_data_string = "Raw data\n========\n\nRaw data retrieved from MaStR database is structured as follows\n\n"

    for tech, data_table_doc in zip(technologies, raw_data_doc_files):
        section = f"{tech}\n-------\n\n"

        csv_include = (
            f".. csv-table::\n"
            f"   :file: {os.path.join('raw', data_table_doc)}\n"
            "   :widths: 20, 35, 15, 15\n"
            "   :header-rows: 1\n\n\n"
        )

        raw_data_string += section + csv_include
    with open("_data/raw_data.rst", "w") as raw_data_fh:
        raw_data_fh.write(raw_data_string)
