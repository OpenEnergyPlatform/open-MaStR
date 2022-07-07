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

from open_mastr.soap_api.config import (
    setup_logger,
    create_data_dir,
    get_filenames,
    get_data_version_dir,
    column_renaming,
)
from open_mastr.soap_api.download import MaStRDownload, flatten_dict, to_csv
from open_mastr import orm
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


    The data can be joined to one table for each technology and exported to
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

        # Map technology and MaStR unit type
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
            "Stromverbrauchseinheit": "consumer",
            "Gaserzeugungseinheit": "gas_producer",
            "Stromerzeugungslokation": "location_elec_generation",
            "Stromverbrauchslokation": "location_elec_consumption",
            "Gaserzeugungslokation": "location_gas_generation",
            "Gasverbrauchslokation": "location_gas_consumption",
        }
        self.unit_type_map_reversed = {v: k for k, v in self.unit_type_map.items()}

    def backfill_basic(self, technology=None, date=None, limit=None):
        """Backfill basic unit data.

        Fill database table 'basic_units' with data. It allows specification
        of which data should be retrieved via
        the described parameter options.

        Under the hood, :meth:`open_mastr.soap_api.download.MaStRDownload.basic_unit_data` is used.

        Parameters
        ----------
        technology: str or list
            Specify technologies for which data should be backfilled.

            * 'solar' (`str`): Backfill data for a single technology.
            * ['solar', 'wind'] (`list`):  Backfill data for multiple technologies given in a list.
            * `None`: Backfill data for all technologies

            Defaults to `None` which is passed to
            :meth:`open_mastr.soap_api.download.MaStRDownload.basic_unit_data`.
        date: None, :class:`datetime.datetime`, str
            Specify backfill date from which on data is retrieved

            Only data with modification time stamp greater that `date` is retrieved.

            * `datetime.datetime(2020, 11, 27)`: Retrieve data which is is newer
            than this time stamp
            * 'latest': Retrieve data which is newer than the newest data
            already in the table.
              It is aware of a different 'latest date' for each technology.
              Hence, it works in combination with
              `technology=None` and `technology=["wind", "solar"]` for example.

              .. warning::

                 Don't use 'latest' in combination with `limit`. This might
                 lead to unexpected results.
            * `None`: Complete backfill

            Defaults to `None`.
        limit: int
            Maximum number of units.
            Defaults to `None` which means no limit is set and all available data is queried.
            Use with care!
        """

        # Create list of technologies to backfill
        if isinstance(technology, str):
            technology_list = [technology]
        elif technology is None:
            technology_list = [None]
        elif isinstance(technology, list):
            technology_list = technology

        # Set limit to a number >> number of units of technology with most units
        if limit is None:
            limit = 10 ** 8

        if date == "latest":
            dates = []
            for tech in technology_list:
                if tech:
                    # In case technologies are specified, latest data date
                    # gets queried per technology
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
                    # If technologies aren't defined ([None]) latest date per technology
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
                        # Break the for loop over technology here, because we
                        # write technology_list and dates at once
                        break

                # Add date to dates list
                if newest_date:
                    dates.append(newest_date[0])
                # Cover the case where no data is in the database and latest is still used
                else:
                    dates.append(None)
        else:
            dates = [date] * len(technology_list)

        # Retrieve data for each technology separately
        for tech, date in zip(technology_list, dates):
            log.info(f"Backfill data for technology {tech}")

            # Catch weird MaStR SOAP response
            basic_units = self.mastr_dl.basic_unit_data(tech, limit, date_from=date)

            with session_scope(engine=self._engine) as session:

                # Insert basic data into database
                log.info(
                    "Insert basic unit data into DB and submit additional data requests"
                )
                for basic_units_chunk in basic_units:
                    # Make sure that no duplicates get inserted into database
                    # (would result in an error)
                    # Only new data gets inserted or data with newer modification date gets updated

                    # Remove duplicates returned from API
                    basic_units_chunk_unique = [
                        unit
                        for n, unit in enumerate(basic_units_chunk)
                        if unit["EinheitMastrNummer"]
                        not in [
                            _["EinheitMastrNummer"] for _ in basic_units_chunk[n + 1:]
                        ]
                    ]
                    basic_units_chunk_unique_ids = [
                        _["EinheitMastrNummer"] for _ in basic_units_chunk_unique
                    ]

                    # Find units that are already in the DB
                    common_ids = [
                        _.EinheitMastrNummer
                        for _ in session.query(orm.BasicUnit.EinheitMastrNummer).filter(
                            orm.BasicUnit.EinheitMastrNummer.in_(
                                basic_units_chunk_unique_ids
                            )
                        )
                    ]

                    # Create instances for new data and for updated data
                    insert = []
                    updated = []
                    for unit in basic_units_chunk_unique:

                        # Rename the typo in column DatumLetzeAktualisierung
                        if "DatumLetzteAktualisierung" not in unit.keys():
                            unit["DatumLetzteAktualisierung"] = unit.pop(
                                "DatumLetzeAktualisierung", None
                            )

                        # In case data for the unit already exists, only update if new data is newer
                        if unit["EinheitMastrNummer"] in common_ids:
                            if session.query(
                                exists().where(
                                    and_(
                                        orm.BasicUnit.EinheitMastrNummer
                                        == unit["EinheitMastrNummer"],
                                        orm.BasicUnit.DatumLetzteAktualisierung
                                        < unit["DatumLetzteAktualisierung"],
                                    )
                                )
                            ).scalar():
                                updated.append(unit)
                                session.merge(orm.BasicUnit(**unit))
                        # In case of new data, just insert
                        else:
                            insert.append(unit)
                    session.bulk_save_objects([orm.BasicUnit(**u) for u in insert])
                    inserted_and_updated = insert + updated

                    # Submit additional data requests
                    extended_data = []
                    eeg_data = []
                    kwk_data = []
                    permit_data = []

                    for basic_unit in inserted_and_updated:
                        # Extended unit data
                        extended_data.append(
                            {
                                "EinheitMastrNummer": basic_unit["EinheitMastrNummer"],
                                "additional_data_id": basic_unit["EinheitMastrNummer"],
                                "technology": self.unit_type_map[
                                    basic_unit["Einheittyp"]
                                ],
                                "data_type": "unit_data",
                                "request_date": datetime.datetime.now(
                                    tz=datetime.timezone.utc
                                ),
                            }
                        )

                        # EEG unit data
                        if basic_unit["EegMastrNummer"]:
                            eeg_data.append(
                                {
                                    "EinheitMastrNummer": basic_unit[
                                        "EinheitMastrNummer"
                                    ],
                                    "additional_data_id": basic_unit["EegMastrNummer"],
                                    "technology": self.unit_type_map[
                                        basic_unit["Einheittyp"]
                                    ],
                                    "data_type": "eeg_data",
                                    "request_date": datetime.datetime.now(
                                        tz=datetime.timezone.utc
                                    ),
                                }
                            )

                        # KWK unit data
                        if basic_unit["KwkMastrNummer"]:
                            kwk_data.append(
                                {
                                    "EinheitMastrNummer": basic_unit[
                                        "EinheitMastrNummer"
                                    ],
                                    "additional_data_id": basic_unit["KwkMastrNummer"],
                                    "technology": self.unit_type_map[
                                        basic_unit["Einheittyp"]
                                    ],
                                    "data_type": "kwk_data",
                                    "request_date": datetime.datetime.now(
                                        tz=datetime.timezone.utc
                                    ),
                                }
                            )

                        # Permit unit data
                        if basic_unit["GenMastrNummer"]:
                            permit_data.append(
                                {
                                    "EinheitMastrNummer": basic_unit[
                                        "EinheitMastrNummer"
                                    ],
                                    "additional_data_id": basic_unit["GenMastrNummer"],
                                    "technology": self.unit_type_map[
                                        basic_unit["Einheittyp"]
                                    ],
                                    "data_type": "permit_data",
                                    "request_date": datetime.datetime.now(
                                        tz=datetime.timezone.utc
                                    ),
                                }
                            )

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

    def backfill_locations_basic(
        self, limit=None, date=None, delete_additional_data_requests=True
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

            * `datetime.datetime(2020, 11, 27)`: Retrieve data which is is newer than
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

        # Set limit to a number >> number of locations
        if not limit:
            limit = 10 ** 7

        # Find newest data date if date="latest"
        if date == "latest":
            with session_scope(engine=self._engine) as session:
                date_queried = (
                    session.query(orm.LocationExtended.DatumLetzteAktualisierung)
                    .order_by(orm.LocationExtended.DatumLetzteAktualisierung.desc())
                    .first()
                )
                if date_queried:
                    date = date_queried[0]
                else:
                    date = None

        locations_basic = self.mastr_dl.basic_location_data(limit, date_from=date)

        for locations_chunk in locations_basic:

            # Remove duplicates returned from API
            locations_chunk_unique = [
                location
                for n, location in enumerate(locations_chunk)
                if location["LokationMastrNummer"]
                not in [_["LokationMastrNummer"] for _ in locations_chunk[n + 1:]]
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

                # Create instances for new data and for updated data
                insert = []
                updated = []
                for location in locations_chunk_unique:
                    # In case data for the unit already exists, only update if new data is newer
                    if location["LokationMastrNummer"] in common_ids:
                        if session.query(
                            exists().where(
                                orm.LocationBasic.LokationMastrNummer
                                == location["LokationMastrNummer"]
                            )
                        ).scalar():
                            updated.append(location)
                            session.merge(orm.LocationBasic(**location))
                    # In case of new data, just insert
                    else:
                        insert.append(location)
                session.bulk_save_objects([orm.LocationBasic(**u) for u in insert])
                inserted_and_updated = insert + updated

                # Create data requests for all newly inserted and updated locations
                new_requests = []
                for location in inserted_and_updated:
                    new_requests.append(
                        {
                            "LokationMastrNummer": location["LokationMastrNummer"],
                            "location_type": self.unit_type_map[
                                location["Lokationtyp"]
                            ],
                            "request_date": datetime.datetime.now(
                                tz=datetime.timezone.utc
                            ),
                        }
                    )

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

    def retrieve_additional_data(
        self, technology, data_type, limit=None, chunksize=1000
    ):
        """
        Retrieve additional unit data

        Execute additional data requests stored in
        :class:`open_mastr.soap_api.orm.AdditionalDataRequested`.
        See also docs of :meth:`open_mastr.soap_api.download.py.MaStRDownload.additional_data`
        for more information on how data is downloaded.

        Parameters
        ----------
        technology: `str` or `list` of `str`
            See list of available technologies in
            :meth:`open_mastr.soap_api.download.py.MaStRDownload.download_power_plants`.
        data_type: `str`
            Select type of additional data that is to be retrieved. Choose from
            "unit_data", "eeg_data", "kwk_data", "permit_data".
        limit: int
            Limit number of units that data is download for. Defaults to `None` which refers
            to query data for existing data requests, for example created by
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

        if not limit:
            limit = 10 ** 8
        if chunksize > limit:
            chunksize = limit

        units_queried = 0
        while units_queried < limit:

            with session_scope(engine=self._engine) as session:

                requested_chunk = (
                    session.query(orm.AdditionalDataRequested)
                    .filter(
                        and_(
                            orm.AdditionalDataRequested.data_type == data_type,
                            orm.AdditionalDataRequested.technology == technology,
                        )
                    )
                    .limit(chunksize)
                )

                ids = [_.additional_data_id for _ in requested_chunk]

                number_units_merged = 0
                deleted_units = []
                if ids:
                    # Retrieve data
                    unit_data, missed_units = self.mastr_dl.additional_data(
                        technology, ids, download_functions[data_type]
                    )
                    missed_units_ids = [u[0] for u in missed_units]
                    unit_data = flatten_dict(unit_data)

                    # Prepare data and add to database table
                    for unit_dat in unit_data:
                        unit_dat["DatenQuelle"] = "API"
                        unit_dat["DatumDownload"] = date.today()
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
                        if "Ertuechtigung" in unit_dat.keys():
                            for ertuechtigung in unit_dat["Ertuechtigung"]:
                                if ertuechtigung["DatumWiederinbetriebnahme"]:
                                    ertuechtigung[
                                        "DatumWiederinbetriebnahme"
                                    ] = ertuechtigung[
                                        "DatumWiederinbetriebnahme"
                                    ].isoformat()
                                ertuechtigung["ProzentualeErhoehungDesLv"] = float(
                                    ertuechtigung["ProzentualeErhoehungDesLv"]
                                )
                        # The NetzbetreiberMastrNummer is handed over as type:list, hence
                        # non-compatible with sqlite)
                        # This replaces the list with the first (string)element in the list
                        # to make it sqlite compatible
                        if "NetzbetreiberMastrNummer" in unit_dat.keys():
                            if type(unit_dat["NetzbetreiberMastrNummer"]) == list:
                                if len(unit_dat["NetzbetreiberMastrNummer"]) > 0:
                                    unit_dat["NetzbetreiberMastrNummer"] = unit_dat[
                                        "NetzbetreiberMastrNummer"
                                    ][0]
                                else:
                                    unit_dat["NetzbetreiberMastrNummer"] = None

                        # Create new instance and update potentially existing one
                        unit = getattr(orm, self.orm_map[technology][data_type])(
                            **unit_dat
                        )
                        session.merge(unit)
                        number_units_merged += 1

                    session.commit()
                    # Log units where data retrieval was not successful
                    for missed_unit in missed_units:
                        missed = orm.MissedAdditionalData(
                            additional_data_id=missed_unit[0], reason=missed_unit[1]
                        )
                        session.add(missed)

                    # Remove units from additional data request table if additional data
                    # was retrieved
                    for requested_unit in requested_chunk:
                        if requested_unit.additional_data_id not in missed_units_ids:
                            session.delete(requested_unit)
                            deleted_units.append(requested_unit.additional_data_id)

                    # Update while iteration condition
                    units_queried += len(ids)

                    log.info(
                        f"Downloaded data for {len(unit_data)} units ({len(ids)} requested). "
                        f"Missed units: {len(missed_units)}. "
                        f"Deleted requests: {len(deleted_units)}."
                    )

            # Emergency break out: if now new data gets inserted/update, don't retrieve any
            # further data
            if number_units_merged == 0:
                log.info("No further data is requested")
                break

    def retrieve_additional_location_data(
        self, location_type, limit=None, chunksize=1000
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
            Limit number of locations that data is download for. Defaults to `None` which refers
            to query data for existing data requests.
        chunksize: int
            Data is downloaded and inserted into the database in chunks of `chunksize`.
            Defaults to 1000.
        """

        # Process arguments
        if not limit:
            limit = 10 ** 8
        if chunksize > limit:
            chunksize = limit

        locations_queried = 0
        while locations_queried < limit:

            with session_scope(engine=self._engine) as session:
                # Get a chunk
                requested_chunk = (
                    session.query(orm.AdditionalLocationsRequested)
                    .filter(
                        orm.AdditionalLocationsRequested.location_type == location_type
                    )
                    .limit(chunksize)
                )
                ids = [_.LokationMastrNummer for _ in requested_chunk]

                # Reset number of locations inserted or updated for this chunk
                number_locations_merged = 0
                deleted_locations = []
                if ids:
                    # Retrieve data
                    location_data, missed_locations = self.mastr_dl.additional_data(
                        location_type, ids, "location_data"
                    )
                    missed_locations_ids = [loc[0] for loc in missed_locations]

                    # Prepare data and add to database table
                    for location_dat in location_data:
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
                        for grid_connection in location_dat["Netzanschlusspunkte"]:
                            grid_connection["letzteAenderung"] = grid_connection[
                                "letzteAenderung"
                            ].isoformat()
                            for field_name in [
                                "MaximaleEinspeiseleistung",
                                "MaximaleAusspeiseleistung",
                                "Nettoengpassleistung",
                                "Netzanschlusskapazitaet",
                            ]:
                                if field_name in grid_connection.keys():
                                    if grid_connection[field_name]:
                                        grid_connection[field_name] = float(
                                            grid_connection[field_name]
                                        )
                        # This converts dates of type:string to type:datetime to match
                        # column data types in orm.py
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
                    for missed_location in missed_locations:
                        missed = orm.MissedExtendedLocation(
                            LokationMastrNummer=missed_location[0],
                            reason=missed_location[1],
                        )
                        session.add(missed)

                    # Remove locations from additional data request table
                    # if additional data was retrieved
                    for requested_location in requested_chunk:
                        if (
                            requested_location.LokationMastrNummer
                            not in missed_locations_ids
                        ):
                            session.delete(requested_location)
                            deleted_locations.append(
                                requested_location.LokationMastrNummer
                            )

                    # Update while iteration condition
                    locations_queried += len(ids)

                    log.info(
                        f"Downloaded data for {len(location_data)} "
                        f"locations ({len(ids)} requested). "
                        f"Missed locations: {len(missed_locations_ids)}. Deleted requests: "
                        f"{len(deleted_locations)}."
                    )

            # Emergency break out: if now new data gets inserted/update,
            # don't retrieve any further data
            if number_locations_merged == 0:
                log.info("No further data is requested")
                break

    def create_additional_data_requests(
        self,
        technology,
        data_types=["unit_data", "eeg_data", "kwk_data", "permit_data"],
        delete_existing=True,
    ):
        """
        Create new requests for additional unit data

        For units that exist in basic_units but not in the table for additional
        data of `data_type`, a new data request
        is submitted.

        Parameters
        ----------
        technology: str
            Specify technology additional data should be requested for.
        data_types: list
            Select type of additional data that is to be requested.
            Defaults to all data that is available for a
            technology.
        delete_existing: bool
            Toggle deletion of already existing requests for additional data.
            Defaults to True.
        """

        data_requests = []

        with session_scope(engine=self._engine) as session:
            # Check which additional data is missing
            for data_type in data_types:
                data_type_available = self.orm_map[technology].get(data_type, None)

                # Only proceed if this data type is available for this technology
                if data_type_available:
                    log.info(
                        f"Create requests for additional data of type {data_type} for {technology}"
                    )

                    # Get ORM for additional data by technology and data_type
                    additional_data_orm = getattr(orm, data_type_available)

                    # Delete prior additional data requests for this technology and data_type
                    if delete_existing:
                        session.query(orm.AdditionalDataRequested).filter(
                            orm.AdditionalDataRequested.technology == technology,
                            orm.AdditionalDataRequested.data_type == data_type,
                        ).delete()
                        session.commit()

                    # Query database for missing additional data
                    if data_type == "unit_data":
                        units_for_request = (
                            session.query(orm.BasicUnit)
                            .outerjoin(
                                additional_data_orm,
                                orm.BasicUnit.EinheitMastrNummer
                                == additional_data_orm.EinheitMastrNummer,
                            )
                            .filter(
                                orm.BasicUnit.Einheittyp
                                == self.unit_type_map_reversed[technology]
                            )
                            .filter(additional_data_orm.EinheitMastrNummer.is_(None))
                            .filter(orm.BasicUnit.EinheitMastrNummer.isnot(None))
                        )
                    elif data_type == "eeg_data":
                        units_for_request = (
                            session.query(orm.BasicUnit)
                            .outerjoin(
                                additional_data_orm,
                                orm.BasicUnit.EegMastrNummer
                                == additional_data_orm.EegMastrNummer,
                            )
                            .filter(
                                orm.BasicUnit.Einheittyp
                                == self.unit_type_map_reversed[technology]
                            )
                            .filter(additional_data_orm.EegMastrNummer.is_(None))
                            .filter(orm.BasicUnit.EegMastrNummer.isnot(None))
                        )
                    elif data_type == "kwk_data":
                        units_for_request = (
                            session.query(orm.BasicUnit)
                            .outerjoin(
                                additional_data_orm,
                                orm.BasicUnit.KwkMastrNummer
                                == additional_data_orm.KwkMastrNummer,
                            )
                            .filter(
                                orm.BasicUnit.Einheittyp
                                == self.unit_type_map_reversed[technology]
                            )
                            .filter(additional_data_orm.KwkMastrNummer.is_(None))
                            .filter(orm.BasicUnit.KwkMastrNummer.isnot(None))
                        )
                    elif data_type == "permit_data":
                        units_for_request = (
                            session.query(orm.BasicUnit)
                            .outerjoin(
                                additional_data_orm,
                                orm.BasicUnit.GenMastrNummer
                                == additional_data_orm.GenMastrNummer,
                            )
                            .filter(
                                orm.BasicUnit.Einheittyp
                                == self.unit_type_map_reversed[technology]
                            )
                            .filter(additional_data_orm.GenMastrNummer.is_(None))
                            .filter(orm.BasicUnit.GenMastrNummer.isnot(None))
                        )
                    else:
                        raise ValueError(
                            f"Data type {data_type} is not a valid option."
                        )

                    # Prepare data for additional data request
                    for basic_unit in units_for_request:
                        data_request = {
                            "EinheitMastrNummer": basic_unit.EinheitMastrNummer,
                            "technology": self.unit_type_map[basic_unit.Einheittyp],
                            "data_type": data_type,
                            "request_date": datetime.datetime.now(
                                tz=datetime.timezone.utc
                            ),
                        }
                        if data_type == "unit_data":
                            data_request[
                                "additional_data_id"
                            ] = basic_unit.EinheitMastrNummer
                        elif data_type == "eeg_data":
                            data_request[
                                "additional_data_id"
                            ] = basic_unit.EegMastrNummer
                        elif data_type == "kwk_data":
                            data_request[
                                "additional_data_id"
                            ] = basic_unit.KwkMastrNummer
                        elif data_type == "permit_data":
                            data_request[
                                "additional_data_id"
                            ] = basic_unit.GenMastrNummer
                        data_requests.append(data_request)

            # Insert new requests for additional data into database
            session.bulk_insert_mappings(orm.AdditionalDataRequested, data_requests)

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
        """

        create_data_dir()

        # Make sure input in either str or list
        if isinstance(technology, str):
            technology = [technology]
        elif not isinstance(technology, (list, None)):
            raise TypeError("Parameter technology must be of type `str` or `list`")

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

                # Define joins based on available tables for tech and user input
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

                # Read data into pandas.DataFrame
                df = pd.read_sql(
                    query.statement, query.session.bind, index_col="EinheitMastrNummer"
                )

                # Remove newline statements from certain strings
                for col in ["Aktenzeichen", "Behoerde"]:
                    df[col] = df[col].str.replace("\r", "")

                # Make sure no duplicate column names exist
                assert not any(df.columns.duplicated())

                # Save to CSV
                to_csv(df, tech)

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

    def reverse_fill_basic_units(self):
        """
        The basic_units table is empty after bulk download.
        To enable csv export, the table is filled from extended
        tables reversely.
        .. warning::
        The basic_units table will be dropped and then recreated.
        Returns -------

        """

        technology = [
            "solar",
            "wind",
            "biomass",
            "combustion",
            "gsgk",
            "hydro",
            "nuclear",
            "storage",
        ]

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

    def locations_to_csv(self, location_type, limit=None):
        """
        Save location raw data to CSV file

        During data export to CSV file, data is reshaped to tabular format.
        Data stored in JSON types is flattened and
        concated to separate rows.

        Parameters
        ----------
        location_type: `str`
            Select type of location that is to be retrieved. Choose from
            "location_elec_generation", "location_elec_consumption", "location_gas_generation",
            "location_gas_consumption".
        limit: int
            Limit number of rows. Defaults to None which implies all rows are selected.
        """
        location_type_shorthand = {
            "location_elec_generation": "SEL",
            "location_elec_consumption": "SVL",
            "location_gas_generation": "GEL",
            "location_gas_consumption": "GVL",
        }

        with session_scope(engine=self._engine) as session:
            # Load basic and extended location data into DataFrame
            locations_extended = (
                session.query(
                    *[
                        c
                        for c in orm.LocationBasic.__table__.columns
                        if c.name not in ["NameDerTechnischenLokation"]
                    ],
                    *[
                        c
                        for c in orm.LocationExtended.__table__.columns
                        if c.name not in ["MaStRNummer"]
                    ],
                )
                .join(
                    orm.LocationExtended,
                    orm.LocationBasic.LokationMastrNummer
                    == orm.LocationExtended.MastrNummer,
                )
                .filter(
                    orm.LocationBasic.LokationMastrNummer.startswith(
                        location_type_shorthand[location_type]
                    )
                )
                .limit(limit)
            )

            df = pd.read_sql(locations_extended.statement, session.bind)

        # Expand data about grid connection points from dict into separate columns
        df_expanded = pd.concat(
            [pd.DataFrame(x) for x in df["Netzanschlusspunkte"]], keys=df.index
        ).reset_index(level=1, drop=True)
        df = (
            df.drop("Netzanschlusspunkte", axis=1)
            .join(df_expanded)
            .reset_index(drop=True)
        )

        # Expand data about related units into separate columns (with lists of related units)
        df = df.drop("VerknuepfteEinheiten", axis=1).join(
            df["VerknuepfteEinheiten"].apply(list_of_dicts_to_columns)
        )

        # Save to file
        create_data_dir()
        data_path = get_data_version_dir()
        filenames = get_filenames()
        csv_file = os.path.join(data_path, filenames["raw"][location_type])
        df.to_csv(csv_file, index=False, encoding="utf-8")


def list_of_dicts_to_columns(row):
    """
    Expand data stored in dict to spearate columns

    Parameters
    ----------
    row: list of dict
        Usually apllied using apply on a column of a pandas DataFrame,
        hence, a Series. This column of the
        DataFrame should comprise of a single-level dict with an
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

    new_cols_df = pd.Series(columns)
    return new_cols_df


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
    columns = [_ for _ in mapper.__mapper__.columns]
    columns_renamed = [
        _.label(f"{_.name}_{suffix}") if _.name in column_names else _ for _ in columns
    ]
    return columns_renamed
