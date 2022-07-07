from datetime import date
from dateutil.parser import parse
import os
import shutil

import pandas as pd

# import xml dependencies
from os.path import expanduser
from open_mastr.xml_download.utils_download_bulk import download_xml_Mastr
from open_mastr.xml_download.utils_write_sqlite import convert_mastr_xml_to_sqlite

# import soap_API dependencies
from open_mastr.soap_api.mirror import MaStRMirror
from open_mastr.soap_api.config import create_data_dir, get_data_version_dir
from postprocessing.cleaning import cleaned_data

# import initialize_database dependencies
from open_mastr.utils.helpers import (
    create_database_engine,
    validate_parameter_format_for_download_method,
    validate_parameter_format_for_mastr_init,
)
import open_mastr.orm as orm
from sqlalchemy.schema import CreateSchema


class Mastr:
    """
    :class:`.Mastr` is used to download the MaStR database and keep it up-to-date.

    A sqlite database is used to mirror the MaStR database. It can be filled with
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

        # Define the paths for the zipped xml download and the sql databases
        self._xml_folder_path = os.path.join(
            expanduser("~"), ".open-MaStR", "data", "xml_download"
        )
        self._sqlite_folder_path = os.path.join(
            expanduser("~"), ".open-MaStR", "data", "sqlite"
        )
        os.makedirs(self._xml_folder_path, exist_ok=True)
        os.makedirs(self._sqlite_folder_path, exist_ok=True)

        # setup database engine and connection
        self._engine = create_database_engine(engine)

        # Initialize database structure
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
            if bulk_date_string == "today":
                bulk_download_date = date.today().strftime("%Y%m%d")
            else:
                # proper format already tested in
                # validate_parameter_format_for_download_method
                bulk_download_date = parse(bulk_date_string).strftime("%Y%m%d")

            _zipped_xml_file_path = os.path.join(
                self._xml_folder_path,
                f"Gesamtdatenexport_{bulk_download_date}.zip",
            )

            if os.path.exists(_zipped_xml_file_path):
                print("MaStR already downloaded.")
            else:
                if bulk_date_string != "today":
                    raise Exception(
                        "There exists no file for given date. MaStR can only be downloaded "
                        "from the website if today's date is given."
                    )
                shutil.rmtree(self._xml_folder_path, ignore_errors=True)
                os.makedirs(self._xml_folder_path, exist_ok=True)
                download_xml_Mastr(_zipped_xml_file_path)
                print(f"MaStR was successfully downloaded to {self._xml_folder_path}.")

            # Map technology input to the .xml file names like 'einheitensolar'
            bulk_include_tables = self._technology_to_include_tables(
                technology=technology
            )

            convert_mastr_xml_to_sqlite(
                engine=self._engine,
                zipped_xml_file_path=_zipped_xml_file_path,
                include_tables=bulk_include_tables,
                bulk_cleansing=bulk_cleansing,
                bulk_download_date=bulk_download_date,
            )
            print("Bulk download and data cleansing was successful.")

        if method == "API":
            print(
                f"Downloading with soap_API.\n\n   -- Settings --  \nunits after date: "
                f"{api_date}\nunit download limit per technology: "
                f"{api_limit}\nparallel_processes: {api_processes}\nchunksize: "
                f"{api_chunksize}\ntechnologies: {technology}\ndata_types: "
                f"{api_data_types}\nlocation_types: {api_location_types}"
            )
            mastr_mirror = MaStRMirror(
                engine=self._engine,
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

    def to_csv(self, tables=None, limit=None) -> None:
        """
        Save the database as csv files along with the metadata file.
        If 'tables=None' all possible tables will be exported.

        Parameters
        ------------
        tables: None or list
            For exporting selected tables choose from: [
                "wind","solar","biomass","hydro","gsgk","combustion","nuclear","storage",
                "balancing_area", "electricity_consumer", "gas_consumer", "gas_producer", "gas_storage", "gas_storage_extended",
                "grid_connections", "grids", "market_actors", "market_roles",
                "location_elec_generation","location_elec_consumption","location_gas_generation","location_gas_consumption"]
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
        locations_to_export = []
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
                    print(f"Technology tables: {technologies_to_export}\n")
                elif table in all_additional_tables:
                    additional_tables_to_export.append(table)
                    print(f"Additional tables: {additional_tables_to_export}\n")
                elif table in api_location_types:
                    locations_to_export.append(table)
                    print(f"Location tables: {locations_to_export}\n")
                else:
                    raise ValueError("Tables parameter has an invalid string!")
        print(f"are saved to: {data_path}")

        api_export = MaStRMirror(engine=self._engine)

        if technologies_to_export:
            # fill basic unit table, after downloading with method = 'bulk' to use API export functions
            api_export.reverse_fill_basic_units()
            # export to csv per technology
            api_export.to_csv(
                technology=technologies_to_export, statistic_flag=None, limit=limit
            )

        if locations_to_export:
            for location_type in api_location_types:
                api_export.locations_to_csv(location_type=location_type, limit=limit)

        # Export additional tables mirrored via pd.DataFrame.to_csv()
        exported_additional_tables = []
        for table in additional_tables_to_export:
            df = pd.read_sql(table, con=self._engine)
            if not df.empty:
                path_of_table = os.path.join(data_path, f"bnetza_mastr_{table}_raw.csv")
                df.to_csv(path_or_buf=path_of_table, encoding="utf-16")
                exported_additional_tables.append(table)

        # clean raw csv's and create cleaned csv's
        cleaned_data()

    def _initialize_database(self) -> None:
        engine = self._engine
        if engine == "docker-postgres":
            engine.execute(CreateSchema(orm.Base.metadata.schema))
        orm.Base.metadata.create_all(engine)

    def _technology_to_include_tables(
        self,
        technology,
        all_technologies=orm.bulk_technologies,
        tables_map=orm.bulk_include_tables_map,
    ) -> list:
        """
        Check the user input 'technology' and convert it to the list 'include_tables' which contains
        file names from zipped
        bulk download.
        Parameters
        ----------
        technology: None, str, list
            The user input for technology selection
            * `None`: All technologies (default)
            * `str`: One technology
            * `list`: List of technologies
        all_technologies: list
            All possible selections
        tables_map: dict
            Dictionary that maps the technologies to the file names in the zipped bulk
            download folder
        Returns
        -------
        list
            List of file names
        -------

        """
        # Convert technology input into a standard list
        chosen_technologies = []
        if technology is None:
            # All technologies are to be chosen
            chosen_technologies = all_technologies
        elif isinstance(technology, str):
            # Only one technology is chosen
            chosen_technologies = [technology]
        elif isinstance(technology, list):
            # list of technologies is given
            chosen_technologies = technology

        # Check if given technologies match with the valid options from 'orm.bulk_technologies'
        for tech in chosen_technologies:
            if tech not in all_technologies:
                raise Exception(
                    f"The input technology = {technology} does not match with the "
                    f"possible technology options. Only following technology options are available "
                    f"bulk_technologies = {all_technologies}"
                )

        # Map technologies to include tables
        include_tables = []
        for tech in chosen_technologies:
            # Append table names to the include_tables list respectively
            include_tables += tables_map[tech]

        return include_tables
