import os
import pandas as pd

# import xml dependencies
from open_mastr.xml_download.utils_download_bulk import download_xml_Mastr
from open_mastr.xml_download.utils_write_to_database import (
    write_mastr_xml_to_database,
)

# import soap_API dependencies
from open_mastr.soap_api.mirror import MaStRMirror

from open_mastr.utils.helpers import (
    technology_input_harmonisation,
    print_api_settings,
    validate_api_credentials,
)
from open_mastr.utils.config import create_data_dir, get_data_version_dir
from open_mastr.utils.data_io import cleaned_data


# import initialize_database dependencies
from open_mastr.utils.helpers import (
    create_database_engine,
    validate_parameter_format_for_download_method,
    validate_parameter_format_for_mastr_init,
    parse_date_string,
)
import open_mastr.utils.orm as orm
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

        orm.Base.metadata.create_all(self.engine)

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

            .. warning::

                The implementation of parallel processes is currently under construction.
                Please let the argument `api_processes` at the default value `None`.

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
            validate_api_credentials()
            if isinstance(technology, str):
                technology = [technology]
            elif technology is None:
                technology = [
                    "wind",
                    "biomass",
                    "combustion",
                    "gsgk",
                    "hydro",
                    "nuclear",
                    "storage",
                    "solar",
                ]
            (
                harm_log,
                api_data_types,
                api_location_types,
            ) = technology_input_harmonisation(
                technology=technology,
                api_data_types=api_data_types,
                api_location_types=api_location_types,
            )

            # Set api_processes to None in order to avoid the malfunctioning usage
            if api_processes:
                api_processes = None
                print(
                    "Warning: The implementation of parallel processes is currently under construction. Please let "
                    "the argument api_processes at the default value None."
                )

            print_api_settings(
                harmonisation_log=harm_log,
                technology=technology,
                api_date=api_date,
                api_data_types=api_data_types,
                api_chunksize=api_chunksize,
                api_limit=api_limit,
                api_processes=api_processes,
                api_location_types=api_location_types,
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

    def to_csv(
        self, tables: list = None, chunksize: int = 500000, limit: int = None
    ) -> None:
        """
        Save the database as csv files along with the metadata file.
        If 'tables=None' all possible tables will be exported.

        Parameters
        ------------
        tables: None or list
            For exporting selected tables choose from:
                ["wind","solar","biomass","hydro","gsgk","combustion","nuclear","storage",
                "balancing_area", "electricity_consumer", "gas_consumer", "gas_producer",
                "gas_storage", "gas_storage_extended",
                "grid_connections", "grids", "market_actors", "market_roles",
                "location_elec_generation","location_elec_consumption","location_gas_generation",
                "location_gas_consumption"]
        chunksize: int
            Defines the chunksize of the tables export. Default value is 500.000.
        limit: None or int
            Limits the number of exported technology and location units.
        """

        create_data_dir()
        data_path = get_data_version_dir()

        # All possible tables to export
        all_technologies = [
            "wind",
            "solar",
            "biomass",
            "hydro",
            "gsgk",
            "combustion",
            "nuclear",
            "storage",
        ]
        all_additional_tables = [
            "balancing_area",
            "electricity_consumer",
            "gas_consumer",
            "gas_producer",
            "gas_storage",
            "gas_storage_extended",
            "grid_connections",
            "grids",
            "locations_extended",
            "market_actors",
            "market_roles",
        ]

        api_location_types = [
            "location_elec_generation",
            "location_elec_consumption",
            "location_gas_generation",
            "location_gas_consumption",
        ]

        # Determine tables to export
        technologies_to_export = []
        additional_tables_to_export = []
        if isinstance(tables, str):
            # str to list
            tables = [tables]
        if tables is None:
            technologies_to_export = all_technologies
            additional_tables_to_export = all_additional_tables
            print(f"Tables: {technologies_to_export}, {additional_tables_to_export}")
        elif isinstance(tables, list):
            for table in tables:
                if table in all_technologies:
                    technologies_to_export.append(table)
                elif table in all_additional_tables:
                    additional_tables_to_export.append(table)
                else:
                    raise ValueError("Tables parameter has an invalid string!")

        if technologies_to_export:
            print(f"\nTechnology tables: {technologies_to_export}")
        if additional_tables_to_export:
            print(f"\nAdditional tables: {additional_tables_to_export}")

        print(f"are saved to: {data_path}")

        api_export = MaStRMirror(engine=self.engine)

        if technologies_to_export:
            # fill basic unit table, after downloading with method = 'bulk' to use API export functions
            api_export.reverse_fill_basic_units(technology=technologies_to_export)
            # export to csv per technology
            api_export.to_csv(
                technology=technologies_to_export,
                statistic_flag=None,
                limit=limit,
                chunksize=chunksize,
            )

        # Export additional tables mirrored via pd.DataFrame.to_csv()
        for table in additional_tables_to_export:
            try:
                df = pd.read_sql(table, con=self.engine)
            except ValueError as e:
                print(
                    f"While reading table '{table}', the following error occured: {e}"
                )
                continue
            if not df.empty:
                path_of_table = os.path.join(data_path, f"bnetza_mastr_{table}_raw.csv")
                df.to_csv(path_or_buf=path_of_table, encoding="utf-16")
