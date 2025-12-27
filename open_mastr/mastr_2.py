import os
from pathlib import Path
from sqlalchemy import inspect, create_engine, Engine
from sqlalchemy.orm import DeclarativeBase
from typing import Literal, Optional, Type, TypeVar, Union
from collections.abc import Mapping

# import xml dependencies
from open_mastr.xml_download.utils_download_bulk import (
    download_documentation,
    download_xml_Mastr,
    delete_xml_files_not_from_given_date,
)
from open_mastr.xml_download.utils_write_to_database import (
    write_mastr_xml_to_database,
)
from open_mastr.utils.xsd_tables import MastrTableDescription, read_mastr_table_descriptions_from_xsd

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
from open_mastr.utils.sqlalchemy_tables import (
    make_sqlalchemy_model_from_mastr_table_description,
    MastrBase
)

# constants
from open_mastr.utils.constants import TECHNOLOGIES, ADDITIONAL_TABLES

# setup logger
log = setup_logger()

# TODO: Repeating Type[DeclarativeBase_T] in function signatures is strange. There must be a better option.
DeclarativeBase_T = TypeVar("DeclarativeBase_T", bound=DeclarativeBase)


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

    def __init__(
        self,
        engine: Union[Engine, Literal["sqlite"]] = "sqlite",
        mastr_table_to_db_table_name: Optional[dict[str, str]] = None,
        output_dir: Optional[Union[str, Path]] = None,
        home_dir: Optional[Union[str, Path]] = None,
    ) -> None:
        validate_parameter_format_for_mastr_init(engine)

        self.output_dir = output_dir or get_output_dir()
        self.home_directory = home_dir or get_project_home_dir()

        self._sqlite_folder_path = os.path.join(self.output_dir, "data", "sqlite")

        os.makedirs(self._sqlite_folder_path, exist_ok=True)

        self.engine = create_database_engine(engine, self._sqlite_folder_path)

        log.info(
            "\n==================================================\n"
            "--------->      open-MaStR started      <---------\n"
            "==================================================\n"
            f"Data will be written to the following database: {self.engine.url}\n"
            "If you run into problems, try to "
            "delete the database and update the package by running "
            "'pip install --upgrade open-mastr'\n"
        )

    def generate_data_model(
        self,
        data: Optional[list[str]] = None,
        catalog_value_as_str: bool = True,
        base: Type[DeclarativeBase_T] = MastrBase,
    ) -> dict[str, Type[DeclarativeBase_T]]:
        data = transform_data_parameter(data)

        docs_folder_path = os.path.join(self.output_dir, "data", "docs_download")
        os.makedirs(docs_folder_path, exist_ok=True)
        zipped_docs_file_path = os.path.join(
            docs_folder_path,
            "Dokumentation MaStR Gesamtdatenexport.zip"
        )
        download_documentation(zipped_docs_file_path)

        mastr_table_descriptions = read_mastr_table_descriptions_from_xsd(
            zipped_docs_file_path=zipped_docs_file_path, data=data
        )
        mastr_table_to_db_model: dict[str, DeclarativeBase_T] = {}
        for mastr_table_description in mastr_table_descriptions:
            sqlalchemy_model = make_sqlalchemy_model_from_mastr_table_description(
                table_description=mastr_table_description,
                catalog_value_as_str=catalog_value_as_str,
                base=base
            )
            mastr_table_to_db_model[mastr_table_description.table_name] = sqlalchemy_model

        return mastr_table_to_db_model

    def download(
        self,
        method="bulk",
        data=None,
        date=None,
        bulk_cleansing=True,
        keep_old_downloads: bool = False,
        mastr_table_to_db_model: Optional[Mapping[str, Type[DeclarativeBase_T]]] = None,
        create_and_alter_database_tables: bool = True,
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
        keep_old_downloads: bool
            If set to True, prior downloaded MaStR zip files will be kept.
        """
        if method == "API":
            log.warning(
                "Downloading the whole registry via the MaStR SOAP-API is deprecated. "
                "You can still use the open_mastr.soap_api.download.MaStRAPI class "
                "to construct single calls."
            )
            log.warning("Attention: method='API' changed to method='bulk'.")
            method = "bulk"

        if not mastr_table_to_db_model:
            mastr_table_to_db_model = self.generate_data_model(data=data, catalog_value_as_str=bulk_cleansing)
            log.info("Ensuring database tables for MaStR are present")
            for db_model in mastr_table_to_db_model.values():
                db_model.__table__.drop(self.engine, checkfirst=True)
                db_model.__table__.create(self.engine)

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
            f"Gesamtdatenexport_{bulk_download_date.strftime('%Y%m%d')}.zip",
        )

        delete_zip_file_if_corrupted(zipped_xml_file_path)
        if not keep_old_downloads:
            delete_xml_files_not_from_given_date(zipped_xml_file_path, xml_folder_path)

        download_xml_Mastr(zipped_xml_file_path, bulk_download_date, data, xml_folder_path)

        log.info(
            "\nWould you like to speed up the creation of your MaStR database?\n"
            "Try our new parallelized processing by setting os.environ['USE_RECOMMENDED_NUMBER_OF_PROCESSES'] = True "
            "or configure your own number of processes via os.environ['NUMBER_OF_PROCESSES'] = your_number\n"
        )

        delete_zip_file_if_corrupted(zipped_xml_file_path)
        delete_xml_files_not_from_given_date(zipped_xml_file_path, xml_folder_path)

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
            mastr_table_to_db_model=mastr_table_to_db_model,
        )

    def to_csv(
        self, tables: list = None, chunksize: int = 500000, limit: int = None
    ) -> None:
        pass
        # TODO: Think about this.

