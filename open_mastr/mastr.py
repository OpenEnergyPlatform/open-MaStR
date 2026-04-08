import os
from pathlib import Path
from typing import Any, Literal, Optional, Union
from collections.abc import Iterable, Mapping

import pandas as pd
from sqlalchemy import inspect, Engine, Table, MetaData

# import xml dependencies
from open_mastr.xml_download.utils_download_bulk import (
    download_documentation,
    download_xml_Mastr,
    select_download_date,
    delete_xml_files_not_from_given_date,
    list_available_downloads,
    get_date_from_docs_url,
)
from open_mastr.xml_download.utils_write_to_database import (
    write_mastr_xml_to_database,
)
from open_mastr.utils.xsd_tables import (
    read_mastr_table_descriptions_from_xsd,
)

from open_mastr.utils.helpers import (
    validate_parameter_format_for_download_method,
    validate_parameter_format_for_mastr_init,
    transform_data_parameter,
    parse_date_string,
    transform_date_parameter,
    delete_zip_file_if_corrupted,
    create_database_engine,
)
from open_mastr.utils.config import (
    get_data_version_dir,
    get_output_dir,
    setup_logger,
)
from open_mastr.utils.sqlalchemy_tables import make_sqlalchemy_table_from_mastr_table_description
from open_mastr.utils.sqlalchemy_views import create_views


# setup logger
log = setup_logger()

FALLBACK_DOCS_PATH = Path(__file__).parent / "resources" / "Dokumentation-MaStR-Gesamtdatenexport-20260216-Fallback.zip"


class Mastr:
    """`Mastr` is used to download the MaStR database.

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
    output_dir : Top-level directory of produced output (downloaded files, created database)
        Default is the content of env var OUTPUT_PATH, else ~/.open-MaStR
    """

    def __init__(
        self,
        engine: Union[Engine, Literal["sqlite"]] = "sqlite",
        output_dir: Optional[Union[str, Path]] = None,
    ) -> None:
        validate_parameter_format_for_mastr_init(engine)

        self.output_dir = output_dir or get_output_dir()

        self._sqlite_folder_path = os.path.join(self.output_dir, "data", "sqlite")

        os.makedirs(self._sqlite_folder_path, exist_ok=True)

        self.engine = create_database_engine(engine, self._sqlite_folder_path)

        log.info(
            "\n==================================================\n"
            "--------->      open-MaStR started      <---------\n"
            "==================================================\n"
            f"Data will be written to the following database: {self.engine.url}\n"
            "If you run into problems, try to "
            "delete the open-mastr directory & database and update the package by running "
            "'pip install --upgrade open-mastr'\n"
        )

    def generate_data_model(
        self,
        data: Optional[list[str]] = None,
        date: Optional[str] = None,
        url: Optional[str] = None,
        metadata: Optional[MetaData] = None,
        catalog_value_as_str: bool = True,
        english: bool = False,
    ) -> dict[str, Table]:
        """Generate data model from MaStR documentation.

        Download the MaStR documentation, extract the XSD files that describe the format of the
        MaStR XML files, and generate SQLAlchemy tables from those XSD files. The tables are not
        created in the database.

        You can use this method to create a mapping from the MaStR data model to your own data model.
        This mapping should then be passed to the `Mastr.download` method.

        !!! example

            ```python
            from open_mastr import Mastr, format_mastr_table_to_db_table
            db = Mastr()
            mapping = db.generate_data_model()
            # Print the tables so that you can see what was generated.
            print(format_mastr_table_to_db_table(mapping))
            # edit your mapping here
            db.download(mastr_table_to_db_table=mapping)
            ```

        Parameters
        ----------
        data : str or list or None, optional
            Specifies which tables to generate. See the `download` method for details.

        date : None or `datetime.datetime` or str, optional
            Specifies date for the docs download. See the `download` method for details.

        url : str or None, optional
            The URL to download the MaStR documentation from. If this is given, the `date` parameter
            is not used.

        metadata : SQLAlchemy MetaData or None, optional
            SQLAlchemy MetaData object to use for the tables. If not given, a new MetaData object
            is created.

        catalog_value_as_str : bool, optional
            If set to True, columns which contain values from the MaStR catalog (Katalogwerte) are
            generated as string/VARCHAR columns. If set to False, they are generated as int columns.
            This should usually be set to True if you want to use the `bulk_cleansing` option of
            the `download` method to convert the catalog IDs to their values.
            Defaults to True.

        english: bool, optional
            If set to True, table and column names are translated from their MaStR name to an
            English name if open-mastr already has a translation stored for that table/column.

            The English name and original MaStR name are always stored in the table's/column's
            `info` attribute.

            Defaults to False.

        Returns
        -------
        dict from str to SQLAlchemy Table
            Dict mapping original MaStR table name to SQLAlchemy table
            Example: {"EinheitenWind": Table(...), "EinheitenSolar": Table(...), ...}
        """
        data = transform_data_parameter(data)
        date = parse_date_string(transform_date_parameter(date))
        if url:
            # This is awkward. We want to give the option to call this function with just a URL.
            # But in our download file path, we want to have the date. So we need to get the date
            # from the URL now.
            if parsed_date := get_date_from_docs_url(url):
                date = parsed_date

        docs_folder_path = os.path.join(self.output_dir, "data", "docs_download")
        os.makedirs(docs_folder_path, exist_ok=True)
        zipped_docs_file_path = os.path.join(
            docs_folder_path,
            f"Dokumentation MaStR Gesamtdatenexport_{date}.zip"
        )
        try:
            download_documentation(zipped_docs_file_path, bulk_date_string=date, url=url)
            return _generate_data_model_from_downloaded_docs(
                zipped_docs_file_path=zipped_docs_file_path,
                data=data,
                catalog_value_as_str=catalog_value_as_str,
                metadata=metadata,
                english=english,
            )
        except Exception as e:
            log.exception(
                f"Encountered {e!r} when downloading or processing MaStR documentation."
                f" Falling back to stored docs at {FALLBACK_DOCS_PATH}"
            )
            return _generate_data_model_from_downloaded_docs(
                zipped_docs_file_path=FALLBACK_DOCS_PATH,
                data=data,
                catalog_value_as_str=catalog_value_as_str,
                metadata=metadata,
                english=english,
            )

    def download(
        self,
        method: Literal["bulk"] = "bulk",
        data=None,
        date=None,
        select_date_interactively: bool = False,
        bulk_cleansing: bool = True,
        keep_old_downloads: bool = False,
        mastr_table_to_db_table: Optional[Mapping[str, Table]] = None,
        alter_database_tables: bool = True,
        english: bool = False,
        add_views_for_old_table_names: bool = True,
        **kwargs: Any,
    ) -> None:
        """Download the MaStR registry and write it to a local database.

        Parameters
        ----------
        method : 'bulk', optional
            Only "bulk" is a valid value. The download via the MaStR SOAP API has been removed.
            Defaults to 'bulk'.
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

            | date       | description |
            |------------|-------------|
            | "20230101" | If file from this date exists locally, it is used. Otherwise, it tries to get it from markstammdatenregister.de |
            | "today"    | Shorthand for specify today's date in YYYYMMDD format |
            | None       | set date="today" |
            | "existing" | Deprecated since 0.16, see [#616](https://github.com/OpenEnergyPlatform/open-MaStR/issues/616#issuecomment-3089377062) |

            Defaults to `None`.

        select_date_interactively : bool, optional
            If set to True, the user will be presented with a list of available download dates
            from the MaStR website and can interactively select which date to download.
            This allows downloading historical data instead of just the latest available data.
            When True, the `date` parameter is ignored. Defaults to False.

        bulk_cleansing : bool, optional
            If set to True, data cleansing is applied after the download (which is recommended).
            In its original format, many entries in the MaStR are encoded with IDs. Columns like
            `state` or `fueltype` do not contain entries such as "Hessen" or "Braunkohle", but instead
            only contain IDs. Cleansing replaces these IDs with their corresponding original entries.
            Defaults to True.

        keep_old_downloads : bool, optional
            If set to True, prior downloaded MaStR zip files will be kept. Defaults to False.

        mastr_table_to_db_table : Mapping from MaStR table name (str) to SQLALchemy Table, or None, optional
            If given, downloaded data from a MaStR file will be stored in the SQLAlchemy table
            associated with that file. The tables must exist already; they are not created.
            Example: {"EinheitenWind": Table(...), "EinheitenSolar": Table(...), ...}

            If None / not given, the mapping will be generated by calling
            `Mastr.generate_data_model` (and thus downloading the MaStR documentation). In this
            case, the tables will be created.

            You can pass this parameter to use your own mapping.

            Defaults to None.

        alter_database_tables : bool, optional
            If set to True, ALTER statememts to add database columns will be issued if there are
            fields in the downloaded XML that are not yet present in the database tables that have
            been generated or given. If set to False and such unexpected fields are found, those
            fields are not imported.

            Defaults to True.

        english : bool = False, optional,
            If set to True and no `mastr_table_to_db_table` mapping is given, the generated tables
            will have English names and English columns. (Some untranslated German names may remain
            if they haven't been added to open-mastr's translation info yet.)

            Defaults to False.

        add_views_for_old_table_names : bool = True, optional,
            If set to True, database views will be generated for tables renamed in version 1.0 so that
            the previous table names still work. Only has an effect if mastr_table_to_db_table is not
            given.

            Defaults to True.
        """
        if method == "API":
            log.warning(
                "Downloading the whole registry via the MaStR SOAP-API has been removed. "
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

        date = transform_date_parameter(date, **kwargs)

        # Handle interactive date selection if requested
        if select_date_interactively:
            log.info(
                "Interactive date selection enabled. Fetching available downloads..."
            )
            selected_link = select_download_date()

            if selected_link is None:
                log.info("Download cancelled by user or no download links found.")
                return

            # Update the date and use the selected URL
            bulk_download_date = selected_link["date"]
            custom_xml_url = selected_link["url"]
            custom_docs_url = selected_link["docs_url"]
        else:
            # Find the name of the zipped xml folder
            bulk_download_date = parse_date_string(date)
            custom_xml_url = None
            custom_docs_url = None

        if not mastr_table_to_db_table:
            mastr_table_to_db_table = self.generate_data_model(
                data=data,
                date=bulk_download_date,
                url=custom_docs_url,
                catalog_value_as_str=bulk_cleansing,
                english=english,
            )
            log.info(
                "Ensuring database tables for MaStR are present:"
                " Dropping old tables if existing and creating new ones."
            )
            for db_table in mastr_table_to_db_table.values():
                db_table.drop(self.engine, checkfirst=True)
                db_table.create(self.engine)

        if add_views_for_old_table_names:
            create_views(engine=self.engine, mastr_table_to_db_table=mastr_table_to_db_table)

        data = transform_data_parameter(data, **kwargs)

        xml_folder_path = os.path.join(self.output_dir, "data", "xml_download")
        os.makedirs(xml_folder_path, exist_ok=True)
        zipped_xml_file_path = os.path.join(
            xml_folder_path,
            f"Gesamtdatenexport_{bulk_download_date}.zip",
        )

        delete_zip_file_if_corrupted(zipped_xml_file_path)
        if not keep_old_downloads:
            delete_xml_files_not_from_given_date(zipped_xml_file_path, xml_folder_path)

        download_xml_Mastr(
            zipped_xml_file_path, bulk_download_date, data, xml_folder_path, custom_xml_url
        )
        log.info(
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
            mastr_table_to_db_table=mastr_table_to_db_table,
            alter_database_tables=alter_database_tables,
        )

    def to_csv(
        self,
        db_table_names: Iterable[str] = None,
        chunksize: int = 500000,
    ) -> None:
        """Export tables from existing database to CSV.

        Parameters
        ----------
        db_table_names : Iterable of str or None, optional
            The names of the database tables to export. If None, all tables in the database will be
            exported. Defaults to None.

        chunksize : int, optional
            Number of rows to retrieve from the database before dumping them to the CSV file.
            Defaults to 500000.
        """
        data_path = get_data_version_dir()
        os.makedirs(data_path, exist_ok=True)

        inspector = inspect(self.engine)
        existing_table_names = set(inspector.get_table_names())
        if db_table_names is None:
            db_table_names = existing_table_names

        log.info(f"Exporting the following database tables to CSV: {', '.join(db_table_names)}")
        with self.engine.connect() as conn:
            for requested_table_name in db_table_names:
                if requested_table_name not in existing_table_names:
                    log.warning(f"Table {requested_table_name} does not exist. Skipping.")
                    continue
                csv_path = os.path.join(data_path, f"{requested_table_name}.csv")
                if os.path.exists(csv_path):
                    log.info(f"Deleting existing file {csv_path}")
                    os.unlink(csv_path)
                for i, chunk in enumerate(
                    pd.read_sql_table(requested_table_name, conn, chunksize=chunksize)
                ):
                    chunk.to_csv(csv_path, mode="a", index=False, header=i == 0)

    def browse_available_downloads(self) -> list[dict[str, Optional[str]]]:
        """Browse available MaStR downloads from the website without starting the download.

        This method fetches and displays all available download dates from the MaStR website,
        allowing users to see what historical data is available before deciding to download.

        Returns
        -------
        list of dict
            List of available downloads with date, version, and type information.

        Examples
        --------
        >>> from open_mastr import Mastr
        >>> db = Mastr()
        >>> available_downloads = db.browse_available_downloads()
        """
        log.info("Browsing available MaStR downloads...")
        return list_available_downloads()

    def translate(self) -> None:
        """
        The translate method has been removed. You can use the `english` option
        in the `Mastr.download` method to get English table and column names.
        """
        raise NotImplementedError(
            "The translate method has been removed. You can use the `english` option"
            " in the `Mastr.download` method to get English table and column names."
        )

def _generate_data_model_from_downloaded_docs(
    zipped_docs_file_path: Path,
    data: list[str], catalog_value_as_str: bool = True,
    metadata: Optional[MetaData] = None,
    english: bool = False,
) -> dict[str, Table]:
    if metadata is None:
        metadata = MetaData()

    mastr_table_descriptions = read_mastr_table_descriptions_from_xsd(
        zipped_docs_file_path=zipped_docs_file_path, data=data
    )
    mastr_table_to_db_table = {}
    for mastr_table_description in mastr_table_descriptions:
        sqlalchemy_model = make_sqlalchemy_table_from_mastr_table_description(
            table_description=mastr_table_description,
            catalog_value_as_str=catalog_value_as_str,
            metadata=metadata,
            english=english,
        )
        mastr_table_to_db_table[mastr_table_description.original_table_name] = sqlalchemy_model

    return mastr_table_to_db_table
