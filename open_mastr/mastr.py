import os

# import xml dependencies
from open_mastr.xml_download.utils_download_bulk import download_xml_Mastr
from open_mastr.xml_download.utils_write_to_database import (
    write_mastr_xml_to_database,
)

# import soap_API dependencies
from open_mastr.soap_api.mirror import MaStRMirror

from open_mastr.utils.helpers import (
    print_api_settings,
    validate_api_credentials,
    validate_parameter_format_for_download_method,
    validate_parameter_format_for_mastr_init,
    validate_parameter_data,
    transform_data_parameter,
    parse_date_string,
    transform_date_parameter,
    data_to_include_tables,
    create_db_query,
    db_query_to_csv,
    reverse_fill_basic_units,
)
from open_mastr.utils.config import (
    create_data_dir,
    get_data_version_dir,
    get_project_home_dir,
    setup_logger,
)
import open_mastr.utils.orm as orm

# import initialize_database dependencies
from open_mastr.utils.helpers import (
    create_database_engine,
)

# constants
from open_mastr.utils.constants import TECHNOLOGIES, ADDITIONAL_TABLES

# setup logger
log = setup_logger()


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
        engine: {'sqlite', sqlalchemy.engine.Engine}, optional
            Defines the engine of the database where the MaStR is mirrored to. Default is 'sqlite'.
    """

    def __init__(self, engine="sqlite") -> None:

        validate_parameter_format_for_mastr_init(engine)

        self.home_directory = get_project_home_dir()
        self._sqlite_folder_path = os.path.join(self.home_directory, "data", "sqlite")
        os.makedirs(self._sqlite_folder_path, exist_ok=True)

        self.engine = create_database_engine(engine, self.home_directory)

        print(
            f"Data will be written to the following database: {self.engine.url}\n"
            "If you run into problems, try to "
            "delete the database and update the package by running "
            "'pip install --upgrade open-mastr'\n"
        )

        orm.Base.metadata.create_all(self.engine)

    def download(
        self,
        method="bulk",
        data=None,
        date=None,
        bulk_cleansing=True,
        api_processes=None,
        api_limit=50,
        api_chunksize=1000,
        api_data_types=None,
        api_location_types=None,
        **kwargs,
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
        data: str or list or None, optional
            Determines which types of data are written to the database. If None, all data is
            used. If it is a list, possible entries are listed at the table
            below with respect to the download method. Missing categories are
            being developed. If only one data is of interest, this can be
            given as a string. Default to None, where all data is included.

            .. csv-table:: Values for data parameter
                :header-rows: 1
                :widths: 5 5 5

                "Data", "Bulk", "API"
                "wind", "Yes", "Yes"
                "solar", "Yes", "Yes"
                "biomass", "Yes", "Yes"
                "hydro", "Yes", "Yes"
                "gsgk", "Yes", "Yes"
                "combustion", "Yes", "Yes"
                "nuclear", "Yes", "Yes"
                "gas", "Yes", "Yes"
                "storage", "Yes", "Yes"
                "electricity_consumer", "Yes", "No"
                "location", "Yes", "Yes"
                "market", "Yes", "No"
                "grid", "Yes", "No"
                "balancing_area", "Yes", "No"
                "permit", "Yes", "Yes"
                "deleted_units", "Yes", "No"
                "retrofit_units", "Yes", "No"

        date: None or :class:`datetime.datetime` or str, optional
            For bulk method:

            Either "today" or None if the newest data dump should be downloaded
            rom the MaStR website. If an already downloaded dump should be used,
            state the date of the download in the format
            "yyyymmdd". Defaults to None.

            For API method:

            Specify backfill date from which on data is retrieved

            Only data with modification time stamp greater than `date` is retrieved.

            * `datetime.datetime(2020, 11, 27)`: Retrieve data which is newer than this
              time stamp
            * 'latest': Retrieve data which is newer than the newest data already in the table.

              .. warning::

                 Don't use "latest" in combination with "api_limit". This might lead to
                 unexpected results.

            * `None`: Complete backfill

            Defaults to `None`.
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
        api_chunksize: int or None, optional
            Data is downloaded and inserted into the database in chunks of `chunksize`.
            Defaults to 1000.
        api_data_types: list or None, optional
            Select type of additional data that should be retrieved. Choose from
            "unit_data", "eeg_data", "kwk_data", "permit_data".  Defaults to all.
        api_location_types: list or None, optional
            Select type of location that should be retrieved. Choose from
            "location_elec_generation", "location_elec_consumption", "location_gas_generation",
            "location_gas_consumption". Defaults to all.
        """

        validate_parameter_format_for_download_method(
            method=method,
            data=data,
            date=date,
            bulk_cleansing=bulk_cleansing,
            api_processes=api_processes,
            api_limit=api_limit,
            api_chunksize=api_chunksize,
            api_data_types=api_data_types,
            api_location_types=api_location_types,
            **kwargs,
        )
        (
            data,
            api_data_types,
            api_location_types,
            harm_log,
        ) = transform_data_parameter(
            method, data, api_data_types, api_location_types, **kwargs
        )

        date = transform_date_parameter(method, date, **kwargs)

        if method == "bulk":

            # Find the name of the zipped xml folder
            bulk_download_date = parse_date_string(date)
            xml_folder_path = os.path.join(self.home_directory, "data", "xml_download")
            os.makedirs(xml_folder_path, exist_ok=True)
            zipped_xml_file_path = os.path.join(
                xml_folder_path,
                f"Gesamtdatenexport_{bulk_download_date}.zip",
            )
            download_xml_Mastr(zipped_xml_file_path, date, xml_folder_path)

            write_mastr_xml_to_database(
                engine=self.engine,
                zipped_xml_file_path=zipped_xml_file_path,
                data=data,
                bulk_cleansing=bulk_cleansing,
                bulk_download_date=bulk_download_date,
            )

        if method == "API":
            validate_api_credentials()

            # Set api_processes to None in order to avoid the malfunctioning usage
            if api_processes:
                api_processes = None
                print(
                    "Warning: The implementation of parallel processes "
                    "is currently under construction. Please let "
                    "the argument api_processes at the default value None."
                )

            print_api_settings(
                harmonisation_log=harm_log,
                data=data,
                date=date,
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
            mastr_mirror.backfill_basic(data, limit=api_limit, date=date)

            # Download additional unit data
            for tech in data:
                # mastr_mirror.create_additional_data_requests(data)
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
                "locations_extended, 'permit', 'deleted_units' ]
        chunksize: int
            Defines the chunksize of the tables export.
            Default value is 500.000 rows to include in each chunk.
        limit: None or int
            Limits the number of exported data rows.
        """
        log.info("Starting csv-export")

        data_path = get_data_version_dir()

        create_data_dir()

        # Validate and parse tables parameter
        validate_parameter_data(method="csv_export", data=tables)
        (
            data,
            api_data_types,
            api_location_types,
            harm_log,
        ) = transform_data_parameter(
            method="bulk", data=tables, api_data_types=None, api_location_types=None
        )

        # Determine tables to export
        technologies_to_export = []
        additional_tables_to_export = []
        for table in data:
            if table in TECHNOLOGIES:
                technologies_to_export.append(table)
            elif table in ADDITIONAL_TABLES:
                additional_tables_to_export.append(table)
            else:
                additional_tables_to_export.extend(
                    data_to_include_tables([table], mapping="export_db_tables")
                )

        if technologies_to_export:
            log.info(f"Technology tables: {technologies_to_export}")
        if additional_tables_to_export:
            log.info(f"Additional tables: {additional_tables_to_export}")

        log.info(f"Tables are saved to: {data_path}")

        reverse_fill_basic_units(technology=technologies_to_export, engine=self.engine)

        # Export technologies to csv
        for tech in technologies_to_export:

            db_query_to_csv(
                db_query=create_db_query(tech=tech, limit=limit, engine=self.engine),
                data_table=tech,
                chunksize=chunksize,
            )
        # Export additional tables to csv
        for addit_table in additional_tables_to_export:

            db_query_to_csv(
                db_query=create_db_query(
                    additional_table=addit_table, limit=limit, engine=self.engine
                ),
                data_table=addit_table,
                chunksize=chunksize,
            )

        # FIXME: Currently metadata is only created for technology data, Fix in #386
        # Configure and save data package metadata file along with data
        # save_metadata(data=technologies_to_export, engine=self.engine)
