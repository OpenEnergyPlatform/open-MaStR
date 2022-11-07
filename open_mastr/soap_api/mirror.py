import datetime
import json
import os
import pandas as pd
from sqlalchemy.orm import Query
from sqlalchemy import and_, func
from sqlalchemy.sql import exists, insert, literal_column
import shlex
import subprocess
from datetime import date

from open_mastr.utils.config import (
    setup_logger,
    create_data_dir,
    get_filenames,
    get_data_version_dir,
    column_renaming,
)
from open_mastr.soap_api.download import MaStRDownload, flatten_dict, to_csv
from open_mastr.utils import orm
from open_mastr.soap_api.metadata.create import datapackage_meta_json
from open_mastr.utils.helpers import session_scope

log = setup_logger()


class MaStRMirror:
    """
    Mirror the Marktstammdatenregister database and keep it up-to-date

    A PostgreSQL database is used to mirror the MaStR database. It builds
    on functionality for bulk data download
    provided by :class:`open_mastr.soap_api.download.MaStRDownload`.

    A rough overview is given by the following schema on the example of wind power units.

    .. figure:: /images/MaStR_Mirror.svg
       :width: 70%
       :align: center

    Initially, basic unit data gets backfilled with :meth:`~.backfill_basic`
    (downloads basic unit data for 2,000
    units of type 'solar').

    .. code-block:: python

       from open_mastr.soap_api.prototype_mastr_reflected import MaStRMirror

       mastr_mirror = MaStRMirror()
       mastr_mirror.backfill_basic("solar", limit=2000)

    Based on this, requests for
    additional data are created. This happens during backfilling basic data.
    But it is also possible to (re-)create
    requests for remaining additional data using :meth:`~.create_additional_data_requests`.

    .. code-block:: python

       mastr_mirror.create_additional_data_requests("solar")

    Additional unit data, in the case of wind power this is extended data,
    EEG data and permit data, can be
    retrieved subsequently by :meth:`~.retrieve_additional_data`.

    .. code-block:: python

       mastr_mirror.retrieve_additional_data("solar", ["unit_data"])


    The data can be joined to one table for each data type and exported to
    CSV files using :meth:`~.to_csv`.

    Also consider to use :meth:`~.dump` and :meth:`~.restore` for specific purposes.

    """

    def __init__(
        self,
        engine,
        restore_dump=None,
        parallel_processes=None,
    ):
        """
        Parameters
        ----------
        engine: sqlalchemy.engine.Engine
            database engine
        restore_dump: str or path-like, optional
            Save path of SQL dump file including filename.
            The database is restored from the SQL dump.
            Defaults to `None` which means nothing gets restored.
            Should be used in combination with `empty_schema=True`.
        parallel_processes: int
            Number of parallel processes used to download additional data.
            Defaults to `None`.
        """
        self._engine = engine

        # Associate downloader
        self.mastr_dl = MaStRDownload(parallel_processes=parallel_processes)

        # Restore database from a dump
        if restore_dump:
            self.restore(restore_dump)

        # Map technologies on ORMs
        self.orm_map = {
            "wind": {
                "unit_data": "WindExtended",
                "eeg_data": "WindEeg",
                "permit_data": "Permit",
            },
            "solar": {
                "unit_data": "SolarExtended",
                "eeg_data": "SolarEeg",
                "permit_data": "Permit",
            },
            "biomass": {
                "unit_data": "BiomassExtended",
                "eeg_data": "BiomassEeg",
                "kwk_data": "Kwk",
                "permit_data": "Permit",
            },
            "combustion": {
                "unit_data": "CombustionExtended",
                "kwk_data": "Kwk",
                "permit_data": "Permit",
            },
            "gsgk": {
                "unit_data": "GsgkExtended",
                "eeg_data": "GsgkEeg",
                "kwk_data": "Kwk",
                "permit_data": "Permit",
            },
            "hydro": {
                "unit_data": "HydroExtended",
                "eeg_data": "HydroEeg",
                "permit_data": "Permit",
            },
            "nuclear": {"unit_data": "NuclearExtended", "permit_data": "Permit"},
            "storage": {
                "unit_data": "StorageExtended",
                "eeg_data": "StorageEeg",
                "permit_data": "Permit",
            },
        }

        # Map data and MaStR unit type
        # Map technologies on ORMs
        self.unit_type_map = {
            "Windeinheit": "wind",
            "Solareinheit": "solar",
            "Biomasse": "biomass",
            "Wasser": "hydro",
            "Geothermie": "gsgk",
            "Verbrennung": "combustion",
            "Kernenergie": "nuclear",
            "Stromspeichereinheit": "storage",
            "Gasspeichereinheit": "gas_storage",
            "Gasverbrauchseinheit": "gas_consumer",
            "Stromverbrauchseinheit": "electricity_consumer",
            "Gaserzeugungseinheit": "gas_producer",
            "Stromerzeugungslokation": "location_elec_generation",
            "Stromverbrauchslokation": "location_elec_consumption",
            "Gaserzeugungslokation": "location_gas_generation",
            "Gasverbrauchslokation": "location_gas_consumption",
        }
        self.unit_type_map_reversed = {v: k for k, v in self.unit_type_map.items()}

    def backfill_basic(self, data=None, date=None, limit=10 ** 8) -> None:
        """Backfill basic unit data.

        Fill database table 'basic_units' with data. It allows specification
        of which data should be retrieved via
        the described parameter options.

        Under the hood, :meth:`open_mastr.soap_api.download.MaStRDownload.basic_unit_data` is used.

        Parameters
        ----------
        data: list
            Specify data types for which data should be backfilled.

            * ['solar']: Backfill data for a single data type.
            * ['solar', 'wind'] (`list`):  Backfill data for multiple technologies given in a list.

        date: None, :class:`datetime.datetime`, str
            Specify backfill date from which on data is retrieved

            Only data with modification time stamp greater that `date` is retrieved.

            * `datetime.datetime(2020, 11, 27)`: Retrieve data which is newer
            than this time stamp
            * 'latest': Retrieve data which is newer than the newest data
            already in the table.
              It is aware of a different 'latest date' for each data.
              Hence, it works in combination with
              `data=None` and `data=["wind", "solar"]` for example.

              .. warning::

                 Don't use 'latest' in combination with `limit`. This might
                 lead to unexpected results.
            * `None`: Complete backfill

            Defaults to `None`.
        limit: int
            Maximum number of units.
            Defaults to the large number of 10**8 which means
            all available data is queried. Use with care!
        """

        dates = self._get_list_of_dates(date, data)

        for data_type, date in zip(data, dates):
            self._write_basic_data_for_one_data_type_to_db(data_type, date, limit)

    def backfill_locations_basic(
        self, limit=10 ** 7, date=None, delete_additional_data_requests=True
    ):
        """
        Backfill basic location data.

        Fill database table 'locations_basic' with data. It allows specification
        of which data should be retrieved via
        the described parameter options.

        Under the hood, :meth:`open_mastr.soap_api.download.MaStRDownload.basic_location_data`
        is used.

        Parameters
        ----------
        date: None, :class:`datetime.datetime`, str
            Specify backfill date from which on data is retrieved

            Only data with modification time stamp greater that `date` is retrieved.

            * `datetime.datetime(2020, 11, 27)`: Retrieve data which is newer than
            this time stamp
            * 'latest': Retrieve data which is newer than the newest data already in the table.
              .. warning::

                 Don't use 'latest' in combination with `limit`. This might lead to
                 unexpected results.
            * `None`: Complete backfill

            Defaults to `None`.
        limit: int
            Maximum number of locations to download.
            Defaults to `None` which means no limit is set and all available data is queried.
            Use with care!
        delete_additional_data_requests: bool
            Useful to speed up download of data. Ignores existence of already created requests
            for additional data and
            skips deletion these.
        """

        date = self._get_date(date, technology_list=None)
        locations_basic = self.mastr_dl.basic_location_data(limit, date_from=date)

        for locations_chunk in locations_basic:

            # Remove duplicates returned from API
            locations_chunk_unique = [
                location
                for n, location in enumerate(locations_chunk)
                if location["LokationMastrNummer"]
                not in [_["LokationMastrNummer"] for _ in locations_chunk[n + 1 :]]
            ]
            locations_unique_ids = [
                _["LokationMastrNummer"] for _ in locations_chunk_unique
            ]

            with session_scope(engine=self._engine) as session:

                # Find units that are already in the DB
                common_ids = [
                    _.LokationMastrNummer
                    for _ in session.query(
                        orm.LocationBasic.LokationMastrNummer
                    ).filter(
                        orm.LocationBasic.LokationMastrNummer.in_(locations_unique_ids)
                    )
                ]
                inserted_and_updated = self._create_inserted_and_updated_list(
                    "locations", session, locations_chunk_unique, common_ids
                )

                # Create data requests for all newly inserted and updated locations
                new_requests = [
                    {
                        "LokationMastrNummer": location["LokationMastrNummer"],
                        "location_type": self.unit_type_map[location["Lokationtyp"]],
                        "request_date": datetime.datetime.now(tz=datetime.timezone.utc),
                    }
                    for location in inserted_and_updated
                ]

                # Delete old data requests
                if delete_additional_data_requests:
                    ids_to_delete = [
                        _["LokationMastrNummer"] for _ in inserted_and_updated
                    ]
                    session.query(orm.AdditionalLocationsRequested).filter(
                        orm.AdditionalLocationsRequested.LokationMastrNummer.in_(
                            ids_to_delete
                        )
                    ).filter(
                        orm.AdditionalLocationsRequested.request_date
                        < datetime.datetime.now(tz=datetime.timezone.utc)
                    ).delete(
                        synchronize_session="fetch"
                    )
                    session.commit()

                # Do bulk insert of new data requests
                session.bulk_insert_mappings(
                    orm.AdditionalLocationsRequested, new_requests
                )

    def retrieve_additional_data(self, data, data_type, limit=10 ** 8, chunksize=1000):
        """
        Retrieve additional unit data

        Execute additional data requests stored in
        :class:`open_mastr.soap_api.orm.AdditionalDataRequested`.
        See also docs of :meth:`open_mastr.soap_api.download.py.MaStRDownload.additional_data`
        for more information on how data is downloaded.

        Parameters
        ----------
        data: `str`
            See list of available technologies in
            :meth:`open_mastr.soap_api.download.py.MaStRDownload.download_power_plants`.
        data_type: `str`
            Select type of additional data that is to be retrieved. Choose from
            "unit_data", "eeg_data", "kwk_data", "permit_data".
        limit: int
            Limit number of units that data is download for. Defaults to the very large number 10**8
            which refers to query data for existing data requests, for example created by
            :meth:`~.create_additional_data_requests`.
        chunksize: int
            Data is downloaded and inserted into the database in chunks of `chunksize`.
            Defaults to 1000.
        """

        # Mapping of download from MaStRDownload
        download_functions = {
            "unit_data": "extended_unit_data",
            "eeg_data": "eeg_unit_data",
            "kwk_data": "kwk_unit_data",
            "permit_data": "permit_unit_data",
        }

        if chunksize > limit:
            chunksize = limit

        number_units_queried = 0
        while number_units_queried < limit:

            with session_scope(engine=self._engine) as session:
                (
                    requested_chunk,
                    requested_ids,
                ) = self._get_additional_data_requests_from_db(
                    table_identifier="additional_data",
                    session=session,
                    data_request_type=data_type,
                    data=data,
                    chunksize=chunksize,
                )

                if not requested_ids:
                    log.info("No further data is requested")
                    break

                # Retrieve data
                unit_data, missed_units = self.mastr_dl.additional_data(
                    data, requested_ids, download_functions[data_type]
                )

                unit_data = flatten_dict(unit_data)
                number_units_merged = 0

                # Prepare data and add to database table
                for unit_dat in unit_data:
                    unit = self._preprocess_additional_data_entry(
                        unit_dat, data, data_type
                    )
                    session.merge(unit)
                    number_units_merged += 1
                session.commit()

                log.info(
                    f"Downloaded data for {len(unit_data)} units ({len(requested_ids)} requested). "
                )
                self._delete_missed_data_from_request_table(
                    table_identifier="additional_data",
                    session=session,
                    missed_requests=missed_units,
                    requested_chunk=requested_chunk,
                )
                # Update while iteration condition
                number_units_queried += len(requested_ids)
            # Emergency break out: if now new data gets inserted/update, don't retrieve any
            # further data
            if number_units_merged == 0:
                log.info("No further data is requested")
                break

    def retrieve_additional_location_data(
        self, location_type, limit=10 ** 8, chunksize=1000
    ):
        """
        Retrieve extended location data

        Execute additional data requests stored in
        :class:`open_mastr.soap_api.orm.AdditionalLocationsRequested`.
        See also docs of :meth:`open_mastr.soap_api.download.py.MaStRDownload.additional_data`
        for more information on how data is downloaded.

        Parameters
        ----------
        location_type: `str`
            Select type of location that is to be retrieved. Choose from
            "location_elec_generation", "location_elec_consumption", "location_gas_generation",
            "location_gas_consumption".
        limit: int
            Limit number of locations that data is download for. Defaults large number 10**8
            which refers to query data for existing data requests.
        chunksize: int
            Data is downloaded and inserted into the database in chunks of `chunksize`.
            Defaults to 1000.
        """

        # Process arguments
        if chunksize > limit:
            chunksize = limit

        locations_queried = 0
        while locations_queried < limit:

            with session_scope(engine=self._engine) as session:
                # Get a chunk
                (
                    requested_chunk,
                    requested_ids,
                ) = self._get_additional_data_requests_from_db(
                    table_identifier="additional_location_data",
                    session=session,
                    data_request_type=location_type,
                    data=None,
                    chunksize=chunksize,
                )

                if not requested_ids:
                    log.info("No further data is requested")
                    break

                # Reset number of locations inserted or updated for this chunk
                number_locations_merged = 0

                # Retrieve data
                location_data, missed_locations = self.mastr_dl.additional_data(
                    location_type, requested_ids, "location_data"
                )

                # Prepare data and add to database table
                location_data = flatten_dict(location_data)
                for location_dat in location_data:
                    location_dat = self._add_data_source_and_download_date(location_dat)
                    # Remove query status information from response
                    for exclude in [
                        "Ergebniscode",
                        "AufrufVeraltet",
                        "AufrufVersion",
                        "AufrufLebenszeitEnde",
                    ]:
                        del location_dat[exclude]

                    # Make data types JSON serializable
                    location_dat["DatumLetzteAktualisierung"] = location_dat[
                        "DatumLetzteAktualisierung"
                    ].isoformat()

                    if type(location_dat["DatumLetzteAktualisierung"]) == str:
                        location_dat[
                            "DatumLetzteAktualisierung"
                        ] = datetime.datetime.strptime(
                            location_dat["DatumLetzteAktualisierung"],
                            "%Y-%m-%dT%H:%M:%S.%f",
                        )

                    # Create new instance and update potentially existing one
                    location = orm.LocationExtended(**location_dat)
                    session.merge(location)
                    number_locations_merged += 1

                    session.commit()
                    # Log locations where data retrieval was not successful
                    log.info(f"Downloaded data for {len(location_data)} ")
                    self._delete_missed_data_from_request_table(
                        table_identifier="additional_location_data",
                        session=session,
                        missed_requests=missed_locations,
                        requested_chunk=requested_chunk,
                    )
                    # Update while iteration condition
                    locations_queried += len(requested_ids)

            # Emergency break out: if now new data gets inserted/update,
            # don't retrieve any further data
            if number_locations_merged == 0:
                log.info("No further data is requested")
                break

    # def create_additional_data_requests(
    #    self,
    #    technology,
    #    data_types=["unit_data", "eeg_data", "kwk_data", "permit_data"],
    #    delete_existing=True,
    # ):
    #    """
    #    Create new requests for additional unit data
    #
    #    For units that exist in basic_units but not in the table for additional
    #    data of `data_type`, a new data request
    #    is submitted.
    #
    #    Parameters
    #    ----------
    #    technology: str
    #        Specify technology additional data should be requested for.
    #    data_types: list
    #        Select type of additional data that is to be requested.
    #        Defaults to all data that is available for a
    #        technology.
    #    delete_existing: bool
    #        Toggle deletion of already existing requests for additional data.
    #        Defaults to True.
    #    """
    #
    #    data_requests = []
    #
    #    with session_scope(engine=self._engine) as session:
    #        # Check which additional data is missing
    #        for data_type in data_types:
    #            if data_type_available := self.orm_map[technology].get(data_type, None):
    #                log.info(
    #                    f"Create requests for additional data of type {data_type} for {technology}"
    #                )
    #
    #                # Get ORM for additional data by technology and data_type
    #                additional_data_orm = getattr(orm, data_type_available)
    #
    #                # Delete prior additional data requests for this technology and data_type
    #                if delete_existing:
    #                    session.query(orm.AdditionalDataRequested).filter(
    #                        orm.AdditionalDataRequested.technology == technology,
    #                        orm.AdditionalDataRequested.data_type == data_type,
    #                    ).delete()
    #                    session.commit()
    #
    #                # Query database for missing additional data
    #                units_for_request = self._get_units_for_request(
    #                    data_type, session, additional_data_orm, technology
    #                )
    #
    #                # Prepare data for additional data request
    #                for basic_unit in units_for_request:
    #                    data_request = {
    #                        "EinheitMastrNummer": basic_unit.EinheitMastrNummer,
    #                        "technology": self.unit_type_map[basic_unit.Einheittyp],
    #                        "data_type": data_type,
    #                        "request_date": datetime.datetime.now(
    #                            tz=datetime.timezone.utc
    #                        ),
    #                    }
    #                    if data_type == "unit_data":
    #                        data_request[
    #                            "additional_data_id"
    #                        ] = basic_unit.EinheitMastrNummer
    #                    elif data_type == "eeg_data":
    #                        data_request[
    #                            "additional_data_id"
    #                        ] = basic_unit.EegMastrNummer
    #                    elif data_type == "kwk_data":
    #                        data_request[
    #                            "additional_data_id"
    #                        ] = basic_unit.KwkMastrNummer
    #                    elif data_type == "permit_data":
    #                        data_request[
    #                            "additional_data_id"
    #                        ] = basic_unit.GenMastrNummer
    #                    data_requests.append(data_request)
    #
    #        # Insert new requests for additional data into database
    #        session.bulk_insert_mappings(orm.AdditionalDataRequested, data_requests)


    def _add_data_source_and_download_date(self, entry: dict) -> dict:
        """Adds DatenQuelle = 'APT' and DatumDownload = date.today"""
        entry["DatenQuelle"] = "API"
        entry["DatumDownload"] = date.today()
        return entry

    def _create_data_list_from_basic_units(self, session, basic_units_chunk):
        # Make sure that no duplicates get inserted into database
        # (would result in an error)
        # Only new data gets inserted or data with newer modification date gets updated

        # Remove duplicates returned from API
        basic_units_chunk_unique = [
            unit
            for n, unit in enumerate(basic_units_chunk)
            if unit["EinheitMastrNummer"]
            not in [_["EinheitMastrNummer"] for _ in basic_units_chunk[n + 1 :]]
        ]
        basic_units_chunk_unique_ids = [
            _["EinheitMastrNummer"] for _ in basic_units_chunk_unique
        ]

        # Find units that are already in the DB
        common_ids = [
            _.EinheitMastrNummer
            for _ in session.query(orm.BasicUnit.EinheitMastrNummer).filter(
                orm.BasicUnit.EinheitMastrNummer.in_(basic_units_chunk_unique_ids)
            )
        ]
        basic_units_chunk_unique = self._correct_typo_in_column_name(
            basic_units_chunk_unique
        )

        inserted_and_updated = self._create_inserted_and_updated_list(
            "basic_units", session, basic_units_chunk_unique, common_ids
        )

        # Submit additional data requests

        extended_data = []
        eeg_data = []
        kwk_data = []
        permit_data = []

        for basic_unit in inserted_and_updated:
            extended_data = self._append_additional_data_from_basic_unit(
                extended_data, basic_unit, "EinheitMastrNummer", "unit_data"
            )
            eeg_data = self._append_additional_data_from_basic_unit(
                eeg_data, basic_unit, "EegMastrNummer", "eeg_data"
            )
            kwk_data = self._append_additional_data_from_basic_unit(
                kwk_data, basic_unit, "KwkMastrNummer", "kwk_data"
            )
            permit_data = self._append_additional_data_from_basic_unit(
                permit_data, basic_unit, "GenMastrNummer", "permit_data"
            )
        return extended_data, eeg_data, kwk_data, permit_data, inserted_and_updated

    def _correct_typo_in_column_name(self, basic_units_chunk_unique: list) -> list:
        """
        Corrects the typo DatumLetzeAktualisierung -> DatumLetzteAktualisierung
        (missing t in Letzte) in the column name.
        """
        basic_units_chunk_unique_correct = []
        for unit in basic_units_chunk_unique:
            # Rename the typo in column DatumLetzeAktualisierung
            if "DatumLetzteAktualisierung" not in unit:
                unit["DatumLetzteAktualisierung"] = unit.pop(
                    "DatumLetzeAktualisierung", None
                )
            basic_units_chunk_unique_correct.append(unit)
        return basic_units_chunk_unique_correct

    def _append_additional_data_from_basic_unit(
        self,
        data_list: list,
        basic_unit: dict,
        basic_unit_identifier: str,
        data_type: str,
    ) -> list:
        """Appends a new entry from basic units to an existing list of unit IDs. This list is
        used when requesting additional data from the MaStR API."""
        if basic_unit[basic_unit_identifier]:
            data_list.append(
                {
                    "EinheitMastrNummer": basic_unit["EinheitMastrNummer"],
                    "additional_data_id": basic_unit[basic_unit_identifier],
                    "technology": self.unit_type_map[basic_unit["Einheittyp"]],
                    "data_type": data_type,
                    "request_date": datetime.datetime.now(tz=datetime.timezone.utc),
                }
            )
        return data_list

    def _create_inserted_and_updated_list(
        self, table_identifier, session, list_chunk_unique, common_ids
    ) -> list:
        """Creates the insert and update list and saves it to the BasicTable.
        This method is called both in backfill_basics and backfill_location_basics."""
        if table_identifier == "locations":
            mastr_number_identifier = "LokationMastrNummer"
            table_class = orm.LocationBasic
        elif table_identifier == "basic_units":
            mastr_number_identifier = "EinheitMastrNummer"
            table_class = orm.BasicUnit

        insert = []
        updated = []
        for entry in list_chunk_unique:
            # In case data for the unit already exists, only update if new data is newer
            if entry[mastr_number_identifier] in common_ids:
                query_filter = (
                    and_(
                        orm.BasicUnit.EinheitMastrNummer == entry["EinheitMastrNummer"],
                        orm.BasicUnit.DatumLetzteAktualisierung
                        < entry["DatumLetzteAktualisierung"],
                    )
                    if table_identifier == "basic_units"
                    else orm.LocationBasic.LokationMastrNummer
                    == entry["LokationMastrNummer"]
                )
                if session.query(exists().where(query_filter)).scalar():
                    updated.append(entry)
                    session.merge(table_class(**entry))
            # In case of new data, just insert
            else:
                insert.append(entry)
        session.bulk_save_objects([table_class(**u) for u in insert])
        session.commit()
        return insert + updated

    def _write_basic_data_for_one_data_type_to_db(self, data, date, limit) -> None:
        log.info(f"Backfill data for data type {data}")

        # Catch weird MaStR SOAP response
        basic_units = self.mastr_dl.basic_unit_data(data, limit, date_from=date)

        with session_scope(engine=self._engine) as session:
            log.info(
                "Insert basic unit data into DB and submit additional data requests"
            )
            for basic_units_chunk in basic_units:
                # Insert basic data into database
                (
                    extended_data,
                    eeg_data,
                    kwk_data,
                    permit_data,
                    inserted_and_updated,
                ) = self._create_data_list_from_basic_units(session, basic_units_chunk)

                # Delete old entries for additional data requests
                additional_data_table = orm.AdditionalDataRequested.__table__
                ids_to_delete = [_["EinheitMastrNummer"] for _ in inserted_and_updated]
                session.execute(
                    additional_data_table.delete()
                    .where(
                        additional_data_table.c.EinheitMastrNummer.in_(ids_to_delete)
                    )
                    .where(additional_data_table.c.technology == "wind")
                    .where(
                        additional_data_table.c.request_date
                        < datetime.datetime.now(tz=datetime.timezone.utc)
                    )
                )

                # Flush delete statements to database
                session.commit()

                # Insert new requests for additional data
                session.bulk_insert_mappings(orm.AdditionalDataRequested, extended_data)
                session.bulk_insert_mappings(orm.AdditionalDataRequested, eeg_data)
                session.bulk_insert_mappings(orm.AdditionalDataRequested, kwk_data)
                session.bulk_insert_mappings(orm.AdditionalDataRequested, permit_data)

            log.info("Backfill successfully finished")

    def _get_date(self, date, technology_list):
        """Parses 'latest' to the latest date in the database, else returns the given date."""
        if technology_list:
            return self._get_list_of_dates(date, technology_list)
        else:
            return self._get_single_date(date)

    def _get_single_date(self, date):
        if date != "latest":
            return date

        with session_scope(engine=self._engine) as session:
            if date_queried := (
                session.query(orm.LocationExtended.DatumLetzteAktualisierung)
                .order_by(orm.LocationExtended.DatumLetzteAktualisierung.desc())
                .first()
            ):
                return date_queried[0]
            else:
                return None

    def _get_list_of_dates(self, date, technology_list) -> list:
        if date != "latest":
            return [date] * len(technology_list)

        dates = []
        for tech in technology_list:
            if tech:
                # In case technologies are specified, latest data date
                # gets queried per data
                with session_scope(engine=self._engine) as session:
                    newest_date = (
                        session.query(orm.BasicUnit.DatumLetzteAktualisierung)
                        .filter(
                            orm.BasicUnit.Einheittyp
                            == self.unit_type_map_reversed[tech]
                        )
                        .order_by(orm.BasicUnit.DatumLetzteAktualisierung.desc())
                        .first()
                    )
            else:
                # If technologies aren't defined ([None]) latest date per data
                #  is queried in query
                # This also leads that the remainder of the loop body is skipped
                with session_scope(engine=self._engine) as session:
                    subquery = session.query(
                        orm.BasicUnit.Einheittyp,
                        func.max(orm.BasicUnit.DatumLetzteAktualisierung).label(
                            "maxdate"
                        ),
                    ).group_by(orm.BasicUnit.Einheittyp)
                    dates = [s[1] for s in subquery]
                    technology_list = [self.unit_type_map[s[0]] for s in subquery]
                    # Break the for loop over data here, because we
                    # write technology_list and dates at once
                    break

            # Add date to dates list
            if newest_date:
                dates.append(newest_date[0])
            # Cover the case where no data is in the database and latest is still used
            else:
                dates.append(None)

        return dates

    def _delete_missed_data_from_request_table(
        self, table_identifier, session, missed_requests, requested_chunk
    ):
        if table_identifier == "additional_data":
            id_attribute = "additional_data_id"
        elif table_identifier == "additional_location_data":
            id_attribute = "LokationMastrNummer"

        missed_entry_ids = [e[0] for e in missed_requests]
        for missed_req in missed_requests:
            missed = (
                orm.MissedAdditionalData(
                    additional_data_id=missed_req[0], reason=missed_req[1]
                )
                if table_identifier == "additional_data"
                else orm.MissedExtendedLocation(
                    LokationMastrNummer=missed_req[0],
                    reason=missed_req[1],
                )
            )
            session.add(missed)
        session.commit()
        # Remove entries from additional data request table if additional data
        # was retrieved
        deleted_entries = []
        for requested_entry in requested_chunk:
            if getattr(requested_entry, id_attribute) not in missed_entry_ids:
                session.delete(requested_entry)
                deleted_entries.append(getattr(requested_entry, id_attribute))
        log.info(
            f"Missed requests: {len(missed_requests)}. "
            f"Deleted requests: {len(deleted_entries)}."
        )
        session.commit()

    def _preprocess_additional_data_entry(self, unit_dat, technology, data_type):
        unit_dat = self._add_data_source_and_download_date(unit_dat)
        # Remove query status information from response
        for exclude in [
            "Ergebniscode",
            "AufrufVeraltet",
            "AufrufVersion",
            "AufrufLebenszeitEnde",
        ]:
            del unit_dat[exclude]

        # Pre-serialize dates/datetimes and decimal in hydro Ertuechtigung
        # This is required because sqlalchemy does not know how serialize
        # dates/decimal of a JSON
        if "Ertuechtigung" in unit_dat:
            for ertuechtigung in unit_dat["Ertuechtigung"]:
                if ertuechtigung["DatumWiederinbetriebnahme"]:
                    ertuechtigung["DatumWiederinbetriebnahme"] = ertuechtigung[
                        "DatumWiederinbetriebnahme"
                    ].isoformat()
                ertuechtigung["ProzentualeErhoehungDesLv"] = float(
                    ertuechtigung["ProzentualeErhoehungDesLv"]
                )
        # The NetzbetreiberMastrNummer is handed over as type:list, hence
        # non-compatible with sqlite)
        # This replaces the list with the first (string)element in the list
        # to make it sqlite compatible
        if (
            "NetzbetreiberMastrNummer" in unit_dat
            and type(unit_dat["NetzbetreiberMastrNummer"]) == list
        ):
            if len(unit_dat["NetzbetreiberMastrNummer"]) > 0:
                unit_dat["NetzbetreiberMastrNummer"] = unit_dat[
                    "NetzbetreiberMastrNummer"
                ][0]
            else:
                unit_dat["NetzbetreiberMastrNummer"] = None

        # Rename the typo in column zugeordneteWirkleistungWechselrichter
        if "zugeordneteWirkleistungWechselrichter" in unit_dat.keys():
            unit_dat["ZugeordneteWirkleistungWechselrichter"] = unit_dat.pop(
                "zugeordneteWirkleistungWechselrichter"
            )

        # Create new instance and update potentially existing one
        return getattr(orm, self.orm_map[technology][data_type])(**unit_dat)

    def _get_additional_data_requests_from_db(
        self, table_identifier, session, data_request_type, data, chunksize
    ):
        """Retrieves the data that is requested from the database table AdditionalDataRequested."""
        if table_identifier == "additional_data":
            requested_chunk = (
                session.query(orm.AdditionalDataRequested)
                .filter(
                    and_(
                        orm.AdditionalDataRequested.data_type == data_request_type,
                        orm.AdditionalDataRequested.technology == data,
                    )
                )
                .limit(chunksize)
            )

            ids = [_.additional_data_id for _ in requested_chunk]
        if table_identifier == "additional_location_data":
            requested_chunk = (
                session.query(orm.AdditionalLocationsRequested)
                .filter(
                    orm.AdditionalLocationsRequested.location_type == data_request_type
                )
                .limit(chunksize)
            )
            ids = [_.LokationMastrNummer for _ in requested_chunk]
        return requested_chunk, ids

    def _get_units_for_request(
        self, data_type, session, additional_data_orm, technology
    ):
        if data_type == "unit_data":
            units_for_request = (
                session.query(orm.BasicUnit)
                .outerjoin(
                    additional_data_orm,
                    orm.BasicUnit.EinheitMastrNummer
                    == additional_data_orm.EinheitMastrNummer,
                )
                .filter(
                    orm.BasicUnit.Einheittyp == self.unit_type_map_reversed[technology]
                )
                .filter(additional_data_orm.EinheitMastrNummer.is_(None))
                .filter(orm.BasicUnit.EinheitMastrNummer.isnot(None))
            )
        elif data_type == "eeg_data":
            units_for_request = (
                session.query(orm.BasicUnit)
                .outerjoin(
                    additional_data_orm,
                    orm.BasicUnit.EegMastrNummer == additional_data_orm.EegMastrNummer,
                )
                .filter(
                    orm.BasicUnit.Einheittyp == self.unit_type_map_reversed[technology]
                )
                .filter(additional_data_orm.EegMastrNummer.is_(None))
                .filter(orm.BasicUnit.EegMastrNummer.isnot(None))
            )
        elif data_type == "kwk_data":
            units_for_request = (
                session.query(orm.BasicUnit)
                .outerjoin(
                    additional_data_orm,
                    orm.BasicUnit.KwkMastrNummer == additional_data_orm.KwkMastrNummer,
                )
                .filter(
                    orm.BasicUnit.Einheittyp == self.unit_type_map_reversed[technology]
                )
                .filter(additional_data_orm.KwkMastrNummer.is_(None))
                .filter(orm.BasicUnit.KwkMastrNummer.isnot(None))
            )
        elif data_type == "permit_data":
            units_for_request = (
                session.query(orm.BasicUnit)
                .outerjoin(
                    additional_data_orm,
                    orm.BasicUnit.GenMastrNummer == additional_data_orm.GenMastrNummer,
                )
                .filter(
                    orm.BasicUnit.Einheittyp == self.unit_type_map_reversed[technology]
                )
                .filter(additional_data_orm.GenMastrNummer.is_(None))
                .filter(orm.BasicUnit.GenMastrNummer.isnot(None))
            )
        else:
            raise ValueError(f"Data type {data_type} is not a valid option.")

        return units_for_request

    def dump(self, dumpfile="open-mastr-continuous-update.backup"):
        """
        Dump MaStR database.

        Parameters
        ----------
        dumpfile : str or path-like, optional
            Save path for dump including filename. When only a filename is given,
            the dump is saved to CWD.
        """
        dump_cmd = (
            f"pg_dump -Fc "
            f"-f {dumpfile} "
            f"-n mastr_mirrored "
            f"-h localhost "
            f"-U open-mastr "
            f"-p 55443 "
            f"open-mastr"
        )

        proc = subprocess.Popen(dump_cmd, shell=True, env={"PGPASSWORD": "open-mastr"})
        proc.wait()

    def restore(self, dumpfile):
        """
        Restore the MaStR database from an SQL dump.

        Parameters
        ----------
        dumpfile : str or path-like, optional
            Save path for dump including filename. When only a filename is given, the
            dump is restored from CWD.


        Warnings
        --------
        If tables that are restored from the dump contain data, restore doesn't work!

        """
        # Interpret file name and path
        dump_file_dir, dump_file = os.path.split(dumpfile)
        cwd = os.path.abspath(os.path.dirname(dump_file_dir))

        # Define import of SQL dump with pg_restore
        restore_cmd = (
            f"pg_restore -h localhost -U open-mastr -p 55443 -d open-mastr {dump_file}"
        )
        restore_cmd = shlex.split(restore_cmd)

        # Execute restore command
        proc = subprocess.Popen(
            restore_cmd,
            shell=False,
            env={"PGPASSWORD": "open-mastr"},
            cwd=cwd,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        proc.wait()

    def to_csv(
        self,
        technology=None,
        limit=None,
        additional_data=["unit_data", "eeg_data", "kwk_data", "permit_data"],
        statistic_flag="B",
        chunksize=500000,
    ):
        """
        Export a snapshot MaStR data from mirrored database to CSV

        During the export, additional available data is joined on list of basic units.
        A CSV file for each technology is
        created separately because of multiple non-overlapping columns.
        Duplicate columns for a single technology (a results on data from
        different sources) are suffixed.

        The data in the database probably has duplicates because
        of the history how data was collected in the
        Marktstammdatenregister. Consider using the parameter
        `statistic_flag`. Read more in the
        `documentation <https://www.marktstammdatenregister.de/MaStRHilfe/subpages/statistik.html>`_
        of the original data source.

        Along with the CSV files, metadata is saved in the file `datapackage.json`.

        Parameters
        ----------
        technology: `str` or `list` of `str`
            See list of available technologies in
            :meth:`open_mastr.soap_api.download.py.MaStRDownload.download_power_plants`
        limit: int
            Limit number of rows
        additional_data: `list`
            Defaults to "export all available additional data" which is described by
            `["unit_data", "eeg_data", "kwk_data", "permit_data"]`.
        statistic_flag: `str`
            Choose between 'A' or 'B' (default) to select a subset of the data for the
            export to CSV.

            * 'B': Migrated that was migrated to the Marktstammdatenregister +
              newly registered units with commissioning
              date after 31.01.2019 (recommended for statistical purposes).
            * 'A':  Newly registered units with commissioning date before 31.01.2019
            * None: Export all data
        chunksize: int or None
            Defines the chunksize of the tables export. Default to 500.000 which is roughly 2.5 GB.
        """

        create_data_dir()

        renaming = column_renaming()

        with session_scope(engine=self._engine) as session:

            for tech in technology:
                unit_data_orm = getattr(orm, self.orm_map[tech]["unit_data"], None)
                eeg_data_orm = getattr(
                    orm, self.orm_map[tech].get("eeg_data", "KeyNotAvailable"), None
                )
                kwk_data_orm = getattr(
                    orm, self.orm_map[tech].get("kwk_data", "KeyNotAvailable"), None
                )
                permit_data_orm = getattr(
                    orm, self.orm_map[tech].get("permit_data", "KeyNotAvailable"), None
                )

                # Define query based on available tables for tech and user input
                subtables = partially_suffixed_columns(
                    orm.BasicUnit,
                    renaming["basic_data"]["columns"],
                    renaming["basic_data"]["suffix"],
                )
                if unit_data_orm and "unit_data" in additional_data:
                    subtables.extend(
                        partially_suffixed_columns(
                            unit_data_orm,
                            renaming["unit_data"]["columns"],
                            renaming["unit_data"]["suffix"],
                        )
                    )
                if eeg_data_orm and "eeg_data" in additional_data:
                    subtables.extend(
                        partially_suffixed_columns(
                            eeg_data_orm,
                            renaming["eeg_data"]["columns"],
                            renaming["eeg_data"]["suffix"],
                        )
                    )
                if kwk_data_orm and "kwk_data" in additional_data:
                    subtables.extend(
                        partially_suffixed_columns(
                            kwk_data_orm,
                            renaming["kwk_data"]["columns"],
                            renaming["kwk_data"]["suffix"],
                        )
                    )
                if permit_data_orm and "permit_data" in additional_data:
                    subtables.extend(
                        partially_suffixed_columns(
                            permit_data_orm,
                            renaming["permit_data"]["columns"],
                            renaming["permit_data"]["suffix"],
                        )
                    )
                query = Query(subtables, session=session)

                # Define joins based on available tables for data and user input
                if unit_data_orm and "unit_data" in additional_data:
                    query = query.join(
                        unit_data_orm,
                        orm.BasicUnit.EinheitMastrNummer
                        == unit_data_orm.EinheitMastrNummer,
                        isouter=True,
                    )
                if eeg_data_orm and "eeg_data" in additional_data:
                    query = query.join(
                        eeg_data_orm,
                        orm.BasicUnit.EegMastrNummer == eeg_data_orm.EegMastrNummer,
                        isouter=True,
                    )
                if kwk_data_orm and "kwk_data" in additional_data:
                    query = query.join(
                        kwk_data_orm,
                        orm.BasicUnit.KwkMastrNummer == kwk_data_orm.KwkMastrNummer,
                        isouter=True,
                    )
                if permit_data_orm and "permit_data" in additional_data:
                    query = query.join(
                        permit_data_orm,
                        orm.BasicUnit.GenMastrNummer == permit_data_orm.GenMastrNummer,
                        isouter=True,
                    )

                # Restricted to technology
                query = query.filter(
                    orm.BasicUnit.Einheittyp == self.unit_type_map_reversed[tech]
                )

                # Decide if migrated data or data of newly registered units or both is selected
                if statistic_flag and "unit_data" in additional_data:
                    query = query.filter(unit_data_orm.StatisikFlag == statistic_flag)

                # Limit returned rows of query
                if limit:
                    query = query.limit(limit)

                # Read data into pandas.DataFrame in chunks of max. 500000 rows of ~2.5 GB RAM
                for chunck_number, chunk_df in enumerate(
                    pd.read_sql(
                        query.statement,
                        query.session.bind,
                        index_col="EinheitMastrNummer",
                        chunksize=chunksize,
                    )
                ):
                    # For debugging purposes, check RAM usage of chunk_df
                    # chunk_df.info(memory_usage='deep')

                    # Make sure no duplicate column names exist
                    assert not any(chunk_df.columns.duplicated())

                    # Remove newline statements from certain strings
                    for col in ["Aktenzeichen", "Behoerde"]:
                        chunk_df[col] = chunk_df[col].str.replace("\r", "")

                    to_csv(df=chunk_df, technology=tech, chunk_number=chunck_number)

            # Create and save data package metadata file along with data
            data_path = get_data_version_dir()
            filenames = get_filenames()
            metadata_file = os.path.join(data_path, filenames["metadata"])

            mastr_technologies = [
                self.unit_type_map_reversed[tech] for tech in technology
            ]
            newest_date = (
                session.query(orm.BasicUnit.DatumLetzteAktualisierung)
                .filter(orm.BasicUnit.Einheittyp.in_(mastr_technologies))
                .order_by(orm.BasicUnit.DatumLetzteAktualisierung.desc())
                .first()[0]
            )
        metadata = datapackage_meta_json(newest_date, technology, json_serialize=False)

        with open(metadata_file, "w", encoding="utf-8") as f:
            json.dump(metadata, f, ensure_ascii=False, indent=4)

    def reverse_fill_basic_units(self, technology):
        """
        The basic_units table is empty after bulk download.
        To enable csv export, the table is filled from extended
        tables reversely.

        .. warning::
        The basic_units table will be dropped and then recreated.
        Returns -------

        Parameters
        ----------
        technology: list of str
            Available technologies are in open_mastr.Mastr.to_csv()
        """

        with session_scope(engine=self._engine) as session:
            # Empty the basic_units table, because it will be filled entirely from extended tables
            session.query(getattr(orm, "BasicUnit", None)).delete()

            for tech in technology:
                # Get the class of extended table
                unit_data_orm = getattr(orm, self.orm_map[tech]["unit_data"], None)
                basic_unit_column_names = [
                    column.name
                    for column in getattr(orm, "BasicUnit", None).__mapper__.columns
                ]

                unit_columns_to_reverse_fill = [
                    column
                    for column in unit_data_orm.__mapper__.columns
                    if column.name in basic_unit_column_names
                ]
                unit_column_names_to_reverse_fill = [
                    column.name for column in unit_columns_to_reverse_fill
                ]

                # Add Einheittyp artificially
                unit_typ = "'" + self.unit_type_map_reversed.get(tech, None) + "'"
                unit_columns_to_reverse_fill.append(
                    literal_column(unit_typ).label("Einheittyp")
                )
                unit_column_names_to_reverse_fill.append("Einheittyp")

                # Build query
                query = Query(unit_columns_to_reverse_fill, session=session)
                insert_query = insert(orm.BasicUnit).from_select(
                    unit_column_names_to_reverse_fill, query
                )

                session.execute(insert_query)


def list_of_dicts_to_columns(row) -> pd.Series:
    """
    Expand data stored in dict to spearate columns

    Parameters
    ----------
    row: list of dict
        Usually applied using apply on a column of a pandas DataFrame,
        hence, a Series. This column of the
        DataFrame should consist of a single-level dict with an
        arbitrary number of columns. Each key is
        transformed into a new column, while data from
        each dict inside the list is concatenated by key. Such
        that the data is stored into a list for each key/column.

    Returns
    -------
    pd.Series
        Pandas Series with keys as columns and values concatenated to a list for each key.
    """
    columns = {k: [] for dic in row for k, _ in dic.items()}
    for dic in row:
        for k, v in dic.items():
            columns[k].append(v)

    return pd.Series(columns)


def partially_suffixed_columns(mapper, column_names, suffix):
    """
    Add a suffix to a subset of ORM map tables for a query

    Parameters
    ----------
    mapper:
        SQLAlchemy ORM table mapper
    column_names: list
        Names of columns to be suffixed
    suffix: str
        Suffix that is append like + "_" + suffix

    Returns
    -------
    list
        List of ORM table mapper instance
    """
    columns = list(mapper.__mapper__.columns)
    return [
        _.label(f"{_.name}_{suffix}") if _.name in column_names else _ for _ in columns
    ]
