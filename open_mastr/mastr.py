import os
from sqlalchemy import inspect, create_engine

# import xml dependencies
from open_mastr.xml_download.utils_download_bulk import (
    download_xml_Mastr,
    delete_xml_files_not_from_given_date,
)
from open_mastr.xml_download.utils_write_to_database import (
    write_mastr_xml_to_database,
)

from open_mastr.utils.helpers import (
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
    delete_zip_file_if_corrupted,
    create_database_engine,
    rename_table,
    create_translated_database_engine,
)
from open_mastr.utils.config import (
    create_data_dir,
    get_data_version_dir,
    get_project_home_dir,
    get_output_dir,
    setup_logger,
)
import open_mastr.utils.orm as orm

# constants
from open_mastr.utils.constants import TECHNOLOGIES, ADDITIONAL_TABLES

# setup logger
log = setup_logger()


class Mastr:
    """
    `Mastr` is used to download the MaStR database and keep it up-to-date.

    An SQL database is used to mirror the MaStR database. It is filled by
    downloading and parsing the MaStR via bulk download.

    !!! example

        ```python
        from open_mastr import Mastr

        db = Mastr()
        db.download()
        ```

    Parameters
    ----------
    engine : {'sqlite', sqlalchemy.engine.Engine}, optional
        Defines the engine of the database where the MaStR is mirrored to.
        Default is 'sqlite'.
    connect_to_translated_db: boolean, optional
            Allows connection to an existing translated database. Default is 'False'.
            Only for 'sqlite'-type engines.



    """

    def __init__(self, engine="sqlite", connect_to_translated_db=False) -> None:
        validate_parameter_format_for_mastr_init(engine)

        self.output_dir = get_output_dir()
        self.home_directory = get_project_home_dir()
        self._sqlite_folder_path = os.path.join(self.output_dir, "data", "sqlite")
        os.makedirs(self._sqlite_folder_path, exist_ok=True)

        self.is_translated = connect_to_translated_db
        if connect_to_translated_db:
            self.engine = create_translated_database_engine(
                engine, self._sqlite_folder_path
            )
        else:
            self.engine = create_database_engine(engine, self._sqlite_folder_path)

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
        **kwargs,
    ) -> None:
        """
        Downloads the MaStR registry and writes it to a local database.

        Parameters
        ----------
        method : 'bulk', optional
            Only "bulk" is a valid value. The download via the MaStR SOAP API is deprecated.
            Default to 'bulk'.
        data : str or list or None, optional
            Specifies which tables to download.

            **Possible values:**
            - "wind"
            - "solar"
            - "biomass"
            - "hydro"
            - "gsgk"
            - "combustion"
            - "nuclear"
            - "gas"
            - "storage"
            - "storage_units"
            - "electricity_consumer"
            - "location"
            - "market"
            - "grid"
            - "balancing_area"
            - "permit"
            - "deleted_units"
            - "deleted_market_actors"
            - "retrofit_units"

            **Usage:**
            - If `None`, all data is downloaded.
            - If a string, only the specified table is downloaded (e.g., `"wind"`).
            - If a list, multiple tables are downloaded (e.g., `["wind", "solar"]`).
        date : None or `datetime.datetime` or str, optional

            | date                  | description |
            |-----------------------|------|
            | "today"                | latest files are downloaded from marktstammdatenregister.de  |
            | "20230101"      | If file from this date exists locally, it is used. Otherwise it throws an error (You can only receive todays data from the server)  |
            | "existing"               | Deprecated since 0.16, see [#616](https://github.com/OpenEnergyPlatform/open-MaStR/issues/616#issuecomment-3089377062) |
            | None      | set date="today"  |

            Default to `None`.
        bulk_cleansing : bool, optional
            If set to True, data cleansing is applied after the download (which is recommended).
            In its original format, many entries in the MaStR are encoded with IDs. Columns like
            `state` or `fueltype` do not contain entries such as "Hessen" or "Braunkohle", but instead
            only contain IDs. Cleansing replaces these IDs with their corresponding original entries.
        """

        if self.is_translated:
            raise TypeError(
                "You are currently connected to a translated database.\n"
                "A translated database cannot be further processed."
            )

        if method == "API":
            log.warning(
                "Downloading the whole registry via the MaStR SOAP-API is deprecated. "
                "You can still use the open_mastr.soap_api.download.MaStRAPI class "
                "to construct single calls."
            )
            log.warning("Attention: method='API' changed to method='bulk'.")
            method = "bulk"

        validate_parameter_format_for_download_method(
            method=method,
            data=data,
            date=date,
            bulk_cleansing=bulk_cleansing,
            **kwargs,
        )
        data = transform_data_parameter(data, **kwargs)

        date = transform_date_parameter(self, date, **kwargs)

        # Find the name of the zipped xml folder
        bulk_download_date = parse_date_string(date)
        xml_folder_path = os.path.join(self.output_dir, "data", "xml_download")
        os.makedirs(xml_folder_path, exist_ok=True)
        zipped_xml_file_path = os.path.join(
            xml_folder_path,
            f"Gesamtdatenexport_{bulk_download_date}.zip",
        )

        delete_zip_file_if_corrupted(zipped_xml_file_path)
        delete_xml_files_not_from_given_date(zipped_xml_file_path, xml_folder_path)

        download_xml_Mastr(zipped_xml_file_path, date, data, xml_folder_path)

        print(
            "\nWould you like to speed up the creation of your MaStR database?\n"
            "Try our new parallelized processing by setting os.environ['USE_RECOMMENDED_NUMBER_OF_PROCESSES'] = True "
            "or configure your own number of processes via os.environ['NUMBER_OF_PROCESSES'] = your_number\n"
        )

        write_mastr_xml_to_database(
            engine=self.engine,
            zipped_xml_file_path=zipped_xml_file_path,
            data=data,
            bulk_cleansing=bulk_cleansing,
            bulk_download_date=bulk_download_date,
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
                ["wind", "solar", "biomass", "hydro", "gsgk", "combustion", "nuclear", "storage",
                "balancing_area", "electricity_consumer", "gas_consumer", "gas_producer",
                "gas_storage", "gas_storage_extended",
                "grid_connections", "grids", "market_actors", "market_roles",
                "locations_extended", "permit", "deleted_units", "storage_units"]
        chunksize: int
            Defines the chunksize of the tables export.
            Default value is 500.000 rows to include in each chunk.
        limit: None or int
            Limits the number of exported data rows.
        """

        if self.is_translated:
            raise TypeError(
                "You are currently connected to a translated database.\n"
                "A translated database cannot be used for the csv export."
            )

        log.info("Starting csv-export")

        data_path = get_data_version_dir()

        create_data_dir()

        # Validate and parse tables parameter
        validate_parameter_data(method="csv_export", data=tables)
        data = transform_data_parameter(
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

    def translate(self) -> None:
        """
        A database can be translated only once.

        Deletes translated versions of the currently connected database.

        Translates currently connected database,renames it with '-translated'
        suffix and updates self.engine's path accordingly.

        !!! example
            ```python

            from open_mastr import Mastr
            import pandas as pd

            db = Mastr()
            db.download(data='biomass')
            db.translate()

            df = pd.read_sql(sql='biomass_extended', con=db.engine)
            print(df.head(10))
            ```

        """

        if "sqlite" not in self.engine.dialect.name:
            raise ValueError("engine has to be of type 'sqlite'")
        if self.is_translated:
            raise TypeError("The currently connected database is already translated.")

        inspector = inspect(self.engine)
        old_path = r"{}".format(self.engine.url.database)
        new_path = old_path[:-3] + "-translated.db"

        if os.path.exists(new_path):
            try:
                os.remove(new_path)
            except Exception as e:
                print(f"An error occurred: {e}")

            print("Replacing previous version of the translated database...")

        for table in inspector.get_table_names():
            rename_table(table, inspector.get_columns(table), self.engine)

        self.engine.dispose()

        try:
            os.rename(old_path, new_path)
            print(f"Database '{old_path}' changed to '{new_path}'")
        except Exception as e:
            print(f"An error occurred: {e}")

        self.engine = create_engine(f"sqlite:///{new_path}")
        self.is_translated = True
