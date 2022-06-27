import os
import pandas as pd

# import xml dependencies
from open_mastr.xml_download.utils_download_bulk import download_xml_Mastr
from open_mastr.xml_download.utils_write_to_database import (
    write_mastr_xml_to_database,
    )

# import soap_API dependencies
from open_mastr.soap_api.mirror import MaStRMirror

# import initialize_database dependencies
from open_mastr.utils.helpers import (
    create_database_engine,
    validate_parameter_format_for_download_method,
    validate_parameter_format_for_mastr_init,
    parse_date_string,
)
import open_mastr.orm as orm
from sqlalchemy.schema import CreateSchema
from open_mastr.utils.config import get_project_home_dir


class Mastr:
    """
    :class:`.Mastr` is used to download the MaStR database and keep it up-to-date.

    A sql database is used to mirror the MaStR database. It can be filled with
    data either from the MaStR-bulk download or from the MaStR-API.

    .. code-block:: python

       from open_mastr import Mastr

       db = Mastr()
       db.download()

    Parameters
    ------------
        engine: {'sqlite', 'docker-postgres', sqlalchemy.engine.Engine}, optional
            Defines the engine of the database where the MaStR is mirrored to. Default is 'sqlite'.
    """

    def __init__(self, engine="sqlite") -> None:

        validate_parameter_format_for_mastr_init(engine)

        self.home_directory = get_project_home_dir()
        self._sqlite_folder_path = os.path.join(self.home_directory, "data", "sqlite")
        os.makedirs(self._sqlite_folder_path, exist_ok=True)

        self.engine = create_database_engine(engine, self.home_directory)

        self._initialize_database()

    def download(
        self,
        method="bulk",
        technology=None,
        bulk_date_string="today",
        bulk_cleansing=True,
        api_processes=None,
        api_limit=50,
        api_date=None,
        api_chunksize=1000,
        api_data_types=None,
        api_location_types=None,
    ) -> None:
        """
        Download the MaStR either via the bulk download or via the MaStR API and write it to a
        sqlite database.

        Parameters
        ------------
        method: {'API', 'bulk'}, optional
            Either "API" or "bulk". Determines whether the data is downloaded via the
            zipped bulk download or via the MaStR API. The latter requires an account
            from marktstammdatenregister.de,
            (see :ref:`Configuration <Configuration>`). Default to 'bulk'.
        technology: str or list or None, optional
            Determines which technologies are written to the database. If None, all technologies are
            used. If it is a list, possible entries are "wind", "solar", "biomass", "hydro", "gsgk",
            "combustion", "nuclear", "gas", "storage", "electricity_consumer", "location", "market",
            "grid", "balancing_area" or "permit". If only one technology is of interest, this can be
            given as a string.  Default to None, where all technologies are included.
        bulk_date_string: str, optional
            Either "today" if the newest data dump should be downloaded from the MaStR website. If
            an already downloaded dump should be used, state the date of the download in the format
            "yyyymmdd". Default to "today".
        bulk_cleansing: bool, optional
            If True, data cleansing is applied after the download (which is recommended). Default
            to True.
        api_processes: int or None or "max", optional
            Number of parallel processes used to download additional data.
            Defaults to `None`. If set to "max", the maximum number of possible processes
            is used.
        api_limit: int or None, optional
            Limit number of units that data is download for. Defaults to `None` which refers
            to query data for existing data requests, for example created by
            :meth:`~.create_additional_data_requests`. Note: There is a limited number of
            requests you are allowed to have per day, so setting api_limit to a value is
            recommended.
        api_date: None or :class:`datetime.datetime` or str, optional
            Specify backfill date from which on data is retrieved

            Only data with modification time stamp greater that `date` is retrieved.

            * `datetime.datetime(2020, 11, 27)`: Retrieve data which is is newer than this
              time stamp
            * 'latest': Retrieve data which is newer than the newest data already in the table.

              .. warning::

                 Don't use "latest" in combination with "api_limit". This might lead to
                 unexpected results.

            * `None`: Complete backfill

            Defaults to `None`.
        api_chunksize: int or None, optional
            Data is downloaded and inserted into the database in chunks of `chunksize`.
            Defaults to 1000.
        api_data_types: list or None, optional
            Select type of additional data that should be retrieved. Choose from
            "unit_data", "eeg_data", "kwk_data", "permit_data".
        api_location_types: list or None, optional
            Select type of location that should be retrieved. Choose from
            "location_elec_generation", "location_elec_consumption", "location_gas_generation",
            "location_gas_consumption".
        """

        validate_parameter_format_for_download_method(
            method=method,
            technology=technology,
            bulk_date_string=bulk_date_string,
            bulk_cleansing=bulk_cleansing,
            api_processes=api_processes,
            api_limit=api_limit,
            api_date=api_date,
            api_chunksize=api_chunksize,
            api_data_types=api_data_types,
            api_location_types=api_location_types,
        )

        if method == "bulk":

            # Find the name of the zipped xml folder
            bulk_download_date = parse_date_string(bulk_date_string)
            xml_folder_path = os.path.join(self.home_directory, "data", "xml_download")
            os.makedirs(xml_folder_path, exist_ok=True)
            zipped_xml_file_path = os.path.join(
                xml_folder_path,
                f"Gesamtdatenexport_{bulk_download_date}.zip",
            )
            download_xml_Mastr(zipped_xml_file_path, bulk_date_string, xml_folder_path)

            write_mastr_xml_to_database(
                engine=self.engine,
                zipped_xml_file_path=zipped_xml_file_path,
                technology=technology,
                bulk_cleansing=bulk_cleansing,
                bulk_download_date=bulk_download_date,
            )

        if method == "API":
            print(
                f"Downloading with soap_API.\n\n   -- Settings --  \nunits after date: "
                f"{api_date}\nunit download limit per technology: "
                f"{api_limit}\nparallel_processes: {api_processes}\nchunksize: "
                f"{api_chunksize}\ntechnologies: {technology}\ndata_types: "
                f"{api_data_types}\nlocation_types: {api_location_types}"
            )
            mastr_mirror = MaStRMirror(
                engine=self.engine,
                parallel_processes=api_processes,
                restore_dump=None,
            )
            # Download basic unit data
            mastr_mirror.backfill_basic(technology, limit=api_limit, date=api_date)

            # Download additional unit data
            for tech in technology:
                # mastr_mirror.create_additional_data_requests(tech)
                for data_type in api_data_types:
                    mastr_mirror.retrieve_additional_data(
                        tech, data_type, chunksize=api_chunksize, limit=api_limit
                    )

            # Download basic location data
            mastr_mirror.backfill_locations_basic(limit=api_limit, date="latest")

            # Download extended location data
            if api_location_types:
                for location_type in api_location_types:
                    mastr_mirror.retrieve_additional_location_data(
                        location_type, limit=api_limit
                    )

    def to_csv(self, path=None) -> None:
        """
        Save the database as csv files.

        Parameters
        ------------
        path: str or None
            The path where the csv files are saved.
            Default is None, in which case they are saved at
            HOME/.open-MaStR/data/csv
        """

        if path:
            if not os.path.exists(path):
                raise ValueError("parameter path is not a valid path")

        else:
            path = os.path.join(self.home_directory, "data", "csv")
        os.makedirs(path, exist_ok=True)

        engine = self.engine
        table_list = engine.table_names()
        with engine.connect().execution_options(autocommit=True) as con:
            print(f"csv files are saved to: {path}")
            for table in table_list:
                df = pd.read_sql(table, con=con)
                if not df.empty:
                    path_of_table = os.path.join(path, f"{table}.csv")
                    df.to_csv(path_or_buf=path_of_table, encoding="utf-16")
                    print(f"{table} saved as csv file.")

    def _initialize_database(self) -> None:
        engine = self.engine
        if engine.name == "postgresql":
            engine.execute(CreateSchema(orm.Base.metadata.schema))
        orm.Base.metadata.create_all(engine)
