import datetime
import json
import os
import pandas as pd
from sqlalchemy.orm import sessionmaker, Query, defer
from sqlalchemy import and_, create_engine, func
from sqlalchemy.sql import exists
import shlex
import subprocess

from open_mastr.soap_api.config import setup_logger, create_data_dir, get_filenames, get_data_version_dir
from open_mastr.soap_api.download import MaStRDownload, flatten_dict, to_csv
from open_mastr.soap_api import orm
from open_mastr.soap_api.metadata.create import datapackage_meta_json


log = setup_logger()

engine = create_engine(
    "postgresql+psycopg2://open-mastr:open-mastr@localhost:55443/open-mastr", echo=False
)
Session = sessionmaker(bind=engine)
session = Session()


def chunks(lst, n):
    """Yield successive n-sized chunks from lst.

    `Credits <https://stackoverflow.com/questions/312443/how-do-you-split-a-list-into-evenly-sized-chunks>`_
    """
    if isinstance(lst, Query):
        length = lst.count()
    else:
        length = len(lst)
    for i in range(0, length, n):
        yield lst[i: i + n]


class MaStRMirror:
    """
    Mirror the Marktstammdatenregister database and keep it up-to-date
    
    A PostgreSQL database is used to mirror the MaStR database. It builds on functionality for bulk data download 
    provided by :class:`open_mastr.soap_api.download.MaStRDownload`.
    
    A rough overview is given by the following schema on the example of wind power units.
    
    .. figure:: /images/MaStR_Mirror.svg
       :width: 70%
       :align: center
    
    Initially, basic unit data gets backfilled with :meth:`~.backfill_basic` (downloads basic unit data for 2,000 
    units of type 'solar'). 
    
    .. code-block:: python
    
       from open_mastr.soap_api.prototype_mastr_reflected import MaStRMirror
       
       mastr_mirror = MaStRMirror()
       mastr_mirror.backfill_basic("solar", limit=2000)
       
    Based on this, requests for 
    additional data are created. This happens during backfilling basic data. But it is also possible to (re-)create 
    requests for remaining additional data using :meth:`~.create_additional_data_requests`.
    
    .. code-block:: python
       
       mastr_mirror.create_additional_data_requests("solar")
    
    Additional unit data, in the case of wind power this is extended data, EEG data and permit data, can be
    retrieved subsequently by :meth:`~.retrieve_additional_data`.
    
    .. code-block:: python
       
       mastr_mirror.retrieve_additional_data("solar", ["unit_data"])
    
    
    The data can be joined to one table for each technology and exported to CSV files using :meth:`~.to_csv`.
    
    Also consider to use :meth:`~.dump` and :meth:`~.restore` for specific purposes.
    
    """

    def __init__(self, empty_schema=False, restore_dump=None, initialize_db=True, parallel_processes=None):
        """
        Parameters
        ----------
        empty_schema: boolean
            Remove all data from the MaStR mirror database. Deletes the entire schema and recreates it 
            including all tables. Be careful!
            Defaults to False **not** deleting anything.
        restore_dump: str or path-like, optional
            Save path of SQL dump file including filename. The database is restored from the SQL dump.
            Defaults to `None` which means nothing gets restored.
            Should be used in combination with `empty_schema=True`.
        initialize_db: boolean
            Start a PostgreSQL database in a docker container. This calls :meth:`~.initdb` during instantiation.
            Defaults to `True`.
        parallel_processes: int
            Number of parallel processes used to download additional data.
            Defaults to `None`.
        """

        # Spin up database container
        if initialize_db:
            self.initdb()

        # Create database tables
        with engine.connect().execution_options(autocommit=True) as con:
            if empty_schema:
                con.execute(f"DROP SCHEMA IF EXISTS {orm.Base.metadata.schema} CASCADE;")
            con.execute(f"CREATE SCHEMA IF NOT EXISTS {orm.Base.metadata.schema};")
        orm.Base.metadata.create_all(engine)

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
                "permit_data": "Permit"
            },
            "solar": {
                "unit_data": "SolarExtended",
                "eeg_data": "SolarEeg",
                "permit_data": "Permit"
            },
            "biomass": {
                "unit_data": "BiomassExtended",
                "eeg_data": "BiomassEeg",
                "kwk_data": "Kwk",
                "permit_data": "Permit"
            },
            "combustion": {
                "unit_data": "CombustionExtended",
                "kwk_data": "Kwk",
                "permit_data": "Permit"
            },
            "gsgk": {
                "unit_data": "GsgkExtended",
                "eeg_data": "GsgkEeg",
                "kwk_data": "Kwk",
                "permit_data": "Permit"
            },
            "hydro": {
                "unit_data": "HydroExtended",
                "eeg_data": "HydroEeg",
                "permit_data": "Permit"
            },
            "nuclear": {
                "unit_data": "NuclearExtended",
                "permit_data": "Permit"
            },
            "storage": {
                "unit_data": "StorageExtended",
                "eeg_data": "StorageEeg",
                "permit_data": "Permit"
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
        }
        self.unit_type_map_reversed = {v: k for k, v in self.unit_type_map.items()}

    def initdb(self):
        """ Initialize a PostgreSQL database with docker-compose
        """
        conf_file_path = os.path.abspath(
            os.path.join(os.path.dirname(__file__), "..", "..")
        )

        subprocess.run(
            ["docker-compose", "up", "-d"],
            cwd=conf_file_path,
        )

    def backfill_basic(self, technology=None, date=None, limit=None):
        """Backfill basic unit data.

        Fill database table 'basic_units' with data. It allows specification of which data should be retrieved via
        the described parameter options.
        
        Under the hood, :meth:`open_mastr.soap_api.download.MaStRDownload.basic_unit_data` is used.

        Parameters
        ----------
        technology: str or list
            Specify technologies for which data should be backfilled.

            * 'solar' (`str`): Backfill data for a single technology.
            * ['solar', 'wind'] (`list`):  Backfill data for multiple technologies given in a list.
            * `None`: Backfill data for all technologies

            Defaults to `None` which is passed to :meth:`open_mastr.soap_api.download.MaStRDownload.basic_unit_data`.
        date: None, :class:`datetime.datetime`, str
            Specify backfill date from which on data is retrieved

            Only data with modification time stamp greater that `date` is retrieved.

            * `datetime.datetime(2020, 11, 27)`: Retrieve data which is is newer than this time stamp
            * 'latest': Retrieve data which is newer than the newest data already in the table.
              It is aware of a different 'latest date' for each technology. Hence, it works in combination with
              `technology=None` and `technology=["wind", "solar"]` for example.

              .. warning::

                 Don't use 'latest' in combination with `limit`. This might lead to unexpected results.
            * `None`: Complete backfill

            Defaults to `None`.
        limit: int
            Maximum number of units.
            Defaults to `None` which means no limit is set and all available data is queried. Use with care!
        """

        # Create list of technologies to backfill
        if isinstance(technology, str):
            technology_list = [technology]
        elif technology == None:
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
                    # In case technologies are specified, latest data date gets queried per technology
                    newest_date = session.query(orm.BasicUnit.DatumLetzeAktualisierung).filter(
                        orm.BasicUnit.Einheittyp == self.unit_type_map_reversed[tech]).order_by(
                        orm.BasicUnit.DatumLetzeAktualisierung.desc()).first()
                else:
                    # If technologies aren't defined ([None]) latest date per technology is queried in query
                    # This also leads that the remainder of the loop body is skipped
                    subquery = session.query(orm.BasicUnit.Einheittyp,
                                             func.max(orm.BasicUnit.DatumLetzeAktualisierung).label("maxdate")).group_by(
                        orm.BasicUnit.Einheittyp)
                    dates = [s[1] for s in subquery]
                    technology_list = [self.unit_type_map[s[0]] for s in subquery]
                    # Break the for loop over technology here, because we write technology_list and dates at once
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

            # Insert basic data into databse
            log.info("Insert basic unit data into DB and submit additional data requests")
            for basic_units_chunk in basic_units:
                # Make sure that no duplicates get inserted into database (would result in an error)
                # Only new data gets inserted or data with newer modification date gets updated

                # Remove duplicates returned from API
                basic_units_chunk_unique = [
                    unit
                    for n, unit in enumerate(basic_units_chunk)
                    if unit["EinheitMastrNummer"]
                       not in [_["EinheitMastrNummer"] for _ in basic_units_chunk[n + 1:]]
                ]
                basic_units_chunk_unique_ids = [_["EinheitMastrNummer"] for _ in basic_units_chunk_unique]

                # Find units that are already in the DB
                common_ids = [_.EinheitMastrNummer for _ in session.query(orm.BasicUnit.EinheitMastrNummer).filter(
                    orm.BasicUnit.EinheitMastrNummer.in_(basic_units_chunk_unique_ids))]

                # Create instances for new data and for updated data
                insert = []
                updated = []
                for unit in basic_units_chunk_unique:
                    # In case data for the unit already exists, only update if new data is newer
                    if unit["EinheitMastrNummer"] in common_ids:
                        if session.query(exists().where(
                                and_(orm.BasicUnit.EinheitMastrNummer == unit["EinheitMastrNummer"],
                                     orm.BasicUnit.DatumLetzeAktualisierung < unit[
                                         "DatumLetzeAktualisierung"]))).scalar():
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
                            "technology": self.unit_type_map[basic_unit["Einheittyp"]],
                            "data_type": "unit_data",
                            "request_date": datetime.datetime.now(tz=datetime.timezone.utc),
                        }
                    )

                    # EEG unit data
                    if basic_unit["EegMastrNummer"]:
                        eeg_data.append(
                            {
                                "EinheitMastrNummer": basic_unit["EinheitMastrNummer"],
                                "additional_data_id": basic_unit["EegMastrNummer"],
                                "technology": self.unit_type_map[basic_unit["Einheittyp"]],
                                "data_type": "eeg_data",
                                "request_date": datetime.datetime.now(tz=datetime.timezone.utc),
                            }
                        )

                    # KWK unit data
                    if basic_unit["KwkMastrNummer"]:
                        kwk_data.append(
                            {
                                "EinheitMastrNummer": basic_unit["EinheitMastrNummer"],
                                "additional_data_id": basic_unit["KwkMastrNummer"],
                                "technology": self.unit_type_map[basic_unit["Einheittyp"]],
                                "data_type": "kwk_data",
                                "request_date": datetime.datetime.now(tz=datetime.timezone.utc),
                            }
                        )

                    # Permit unit data
                    if basic_unit["GenMastrNummer"]:
                        permit_data.append(
                            {
                                "EinheitMastrNummer": basic_unit["EinheitMastrNummer"],
                                "additional_data_id": basic_unit["GenMastrNummer"],
                                "technology": self.unit_type_map[basic_unit["Einheittyp"]],
                                "data_type": "permit_data",
                                "request_date": datetime.datetime.now(tz=datetime.timezone.utc),
                            }
                        )

                # Delete old entries for additional data requests
                additional_data_table = orm.AdditionalDataRequested.__table__
                ids_to_delete = [_["EinheitMastrNummer"] for _ in inserted_and_updated]
                session.execute(
                    additional_data_table.delete().where(
                        additional_data_table.c.EinheitMastrNummer.in_(ids_to_delete)).where(
                        additional_data_table.c.technology == "wind").where(
                        additional_data_table.c.request_date < datetime.datetime.now(tz=datetime.timezone.utc)
                               )
                )

                # Flush delete statements to database
                session.commit()

                # Insert new requests for additional data
                session.bulk_insert_mappings(orm.AdditionalDataRequested, extended_data)
                session.bulk_insert_mappings(orm.AdditionalDataRequested, eeg_data)
                session.bulk_insert_mappings(orm.AdditionalDataRequested, kwk_data)
                session.bulk_insert_mappings(orm.AdditionalDataRequested, permit_data)

                session.commit()
            session.close()
            log.info("Backfill successfully finished")

    def retrieve_additional_data(self, technology, data_type, limit=None, chunksize=1000):
        """
        Retrieve additional unit data
        
        Execute additional data requests stored in :class:`open_mastr.soap_api.orm.AdditionalDataRequested`.
        See also docs of :meth:`open_mastr.soap_api.download.py.MaStRDownload.additional_data` for more 
        information on how data is downloaded.
        
        Parameters
        ----------
        technology: `str` or `list` of `str`
            See list of available technologies in
            :meth:`open_mastr.soap_api.download.py.MaStRDownload.download_power_plants`.
        data_type: `list`
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

        if limit:
            if chunksize > limit:
                chunksize = limit

        units_queried = 0
        while units_queried < limit:

            requested_chunk = session.query(orm.AdditionalDataRequested).filter(
                and_(orm.AdditionalDataRequested.data_type == data_type,
                     orm.AdditionalDataRequested.technology == technology)).limit(chunksize)

            ids = [_.additional_data_id for _ in requested_chunk]

            number_units_merged = 0
            if ids:
                # Retrieve data
                unit_data, missed_units = self.mastr_dl.additional_data(technology, ids, download_functions[data_type])
                unit_data = flatten_dict(unit_data)

                # Prepare data and add to database table
                for unit_dat in unit_data:
                    # Remove query status information from response
                    for exclude in ["Ergebniscode", "AufrufVeraltet", "AufrufVersion", "AufrufLebenszeitEnde"]:
                        del unit_dat[exclude]

                    # Pre-serialize dates/datetimes and decimal in hydro Ertuechtigung
                    # This is required because sqlalchemy does not know how serialize dates/decimal of a JSON
                    if "Ertuechtigung" in unit_dat.keys():
                        for ertuechtigung in unit_dat['Ertuechtigung']:
                            if ertuechtigung['DatumWiederinbetriebnahme']:
                                ertuechtigung['DatumWiederinbetriebnahme'] = ertuechtigung[
                                    'DatumWiederinbetriebnahme'].isoformat()
                            ertuechtigung['ProzentualeErhoehungDesLv'] = float(ertuechtigung['ProzentualeErhoehungDesLv'])

                    # Create new instance and update potentially existing one
                    unit = getattr(orm, self.orm_map[technology][data_type])(**unit_dat)
                    session.merge(unit)
                    number_units_merged += 1

                session.commit()
                # Log units where data retrieval was not successful
                for missed_unit in missed_units:
                    missed = orm.MissedAdditionalData(additional_data_id=missed_unit)
                    session.add(missed)

                # Remove units from additional data request table if additional data was retrieved
                for requested_unit in requested_chunk:
                    if requested_unit.additional_data_id not in missed_units:
                        session.delete(requested_unit)

                # Send to database complete transactions
                session.commit()

                # Update while iteration condition
                units_queried += len(ids)

                # Report on chunk
                deleted_units = [requested_unit.additional_data_id for requested_unit in requested_chunk
                                 if requested_unit.additional_data_id not in missed_units]
                log.info(f"Downloaded data for {len(unit_data)} units ({len(ids)} requested). "
                         f"Missed units: {len(missed_units)}. Deleted requests: {len(deleted_units)}.")

            # Emergency break out: if now new data gets inserted/update, don't retrieve any further data
            if number_units_merged == 0:
                log.info("No further data is requested")
                break

    def create_additional_data_requests(self, technology,
                                        data_types=["unit_data", "eeg_data", "kwk_data", "permit_data"],
                                        delete_existing=True):
        """
        Create new requests for additional unit data

        For units that exist in basic_units but not in the table for additional data of `data_type`, a new data request
        is submitted.

        Parameters
        ----------
        technology: str
            Specify technology additional data should be requested for.
        data_types: list
            Select type of additional data that is to be requested. Defaults to all data that is available for a
            technology.
        delete_existing: bool
            Toggle deletion of already existing requests for additional data.
            Defaults to True.
        """

        data_requests = []

        # Check which additional data is missing
        for data_type in data_types:
            data_type_available = self.orm_map[technology].get(data_type, None)

            # Only proceed if this data type is available for this technology
            if data_type_available:
                log.info(f"Create requests for additional data of type {data_type} for {technology}")

                # Get ORM for additional data by technology and data_type
                additional_data_orm = getattr(orm, data_type_available)

                # Delete prior additional data requests for this technology and data_type
                if delete_existing:
                    session.query(orm.AdditionalDataRequested).filter(
                        orm.AdditionalDataRequested.technology == technology,
                        orm.AdditionalDataRequested.data_type == data_type).delete()
                    session.commit()

                # Query database for missing additional data
                if data_type == "unit_data":
                    units_for_request = session.query(orm.BasicUnit).outerjoin(
                        additional_data_orm,
                        orm.BasicUnit.EinheitMastrNummer == additional_data_orm.EinheitMastrNummer).filter(
                        orm.BasicUnit.Einheittyp == self.unit_type_map_reversed[technology]).filter(
                        additional_data_orm.EinheitMastrNummer.is_(None)).filter(
                        orm.BasicUnit.EinheitMastrNummer.isnot(None))
                elif data_type == "eeg_data":
                    units_for_request = session.query(orm.BasicUnit).outerjoin(
                        additional_data_orm,
                        orm.BasicUnit.EegMastrNummer == additional_data_orm.EegMastrNummer).filter(
                        orm.BasicUnit.Einheittyp == self.unit_type_map_reversed[technology]).filter(
                        additional_data_orm.EegMastrNummer.is_(None)).filter(orm.BasicUnit.EegMastrNummer.isnot(None))
                elif data_type == "kwk_data":
                    units_for_request = session.query(orm.BasicUnit).outerjoin(
                        additional_data_orm,
                        orm.BasicUnit.KwkMastrNummer == additional_data_orm.KwkMastrNummer).filter(
                        orm.BasicUnit.Einheittyp == self.unit_type_map_reversed[technology]).filter(
                        additional_data_orm.KwkMastrNummer.is_(None)).filter(orm.BasicUnit.KwkMastrNummer.isnot(None))
                elif data_type == "permit_data":
                    units_for_request = session.query(orm.BasicUnit).outerjoin(
                        additional_data_orm,
                        orm.BasicUnit.GenMastrNummer == additional_data_orm.GenMastrNummer).filter(
                        orm.BasicUnit.Einheittyp == self.unit_type_map_reversed[technology]).filter(
                        additional_data_orm.GenMastrNummer.is_(None)).filter(orm.BasicUnit.GenMastrNummer.isnot(None))
                else:
                    raise ValueError(f"Data type {data_type} is not a valid option.")

                # Prepare data for additional data request
                for basic_unit in units_for_request:
                    data_request = {
                        "EinheitMastrNummer": basic_unit.EinheitMastrNummer,
                        "technology": self.unit_type_map[basic_unit.Einheittyp],
                        "data_type": data_type,
                        "request_date": datetime.datetime.now(tz=datetime.timezone.utc),
                    }
                    if data_type == "unit_data":
                        data_request["additional_data_id"] = basic_unit.EinheitMastrNummer
                    elif data_type == "eeg_data":
                        data_request["additional_data_id"] = basic_unit.EegMastrNummer
                    elif data_type == "kwk_data":
                        data_request["additional_data_id"] = basic_unit.KwkMastrNummer
                    elif data_type == "permit_data":
                        data_request["additional_data_id"] = basic_unit.GenMastrNummer
                    data_requests.append(data_request)

        # Insert new requests for additional data into database
        session.bulk_insert_mappings(orm.AdditionalDataRequested, data_requests)
        session.commit()
        session.close()

    def dump(self, dumpfile="open-mastr-continuous-update.backup"):
        """
        Dump MaStR database.

        Parameters
        ----------
        dumpfile : str or path-like, optional
            Save path for dump including filename. When only a filename is given, the dump is saved to CWD.
        """
        dump_cmd = f"pg_dump -Fc " \
            f"-f {dumpfile} " \
            f"-n mastr_mirrored " \
            f"-h localhost " \
            f"-U open-mastr " \
            f"-p 55443 " \
            f"open-mastr"

        proc = subprocess.Popen(dump_cmd, shell=True, env={
            'PGPASSWORD': "open-mastr"
        })
        proc.wait()

    def restore(self, dumpfile):
        """
        Restore the MaStR database from an SQL dump.

        Parameters
        ----------
        dumpfile : str or path-like, optional
            Save path for dump including filename. When only a filename is given, the dump is restored from CWD.


        Warnings
        --------
        If tables that are restored from the dump contain data, restore doesn't work!

        """
        # Interpret file name and path
        dump_file_dir, dump_file = os.path.split(dumpfile)
        cwd = os.path.abspath(os.path.dirname(dump_file_dir))


        # Define import of SQL dump with pg_restore
        restore_cmd = f"pg_restore -h localhost -U open-mastr -p 55443 -d open-mastr {dump_file}"
        restore_cmd = shlex.split(restore_cmd)

        # Execute restore command
        proc = subprocess.Popen(restore_cmd,
                                shell=False,
                                env={'PGPASSWORD': "open-mastr"},
                                cwd=cwd,
                                stdin=subprocess.PIPE,
                                stdout=subprocess.PIPE,
                                stderr=subprocess.PIPE,
                                )
        proc.wait()

    def to_csv(self,
               technology=None,
               limit=None,
               additional_data=["unit_data", "eeg_data", "kwk_data", "permit_data"],
               statistic_flag="B",
               ):
        """
        Export a snapshot MaStR data from mirrored database to CSV

        During the export, additional available data is joined on list of basic units. A CSV file for each technology is
        created separately because of multiple non-overlapping columns.

        The data in the database probably has duplicates because of the history how data was collected in the
        Marktstammdatenregister. Consider to use the parameter `statistic_flag`. Read more in the
        `documentation <https://www.marktstammdatenregister.de/MaStRHilfe/subpages/statistik.html>`_ of the original
        data source.

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
            Choose between 'A' or 'B' (default) to select a subset of the data for the export to CSV.

            * 'B': Migrated that was migrated to the Martstammdatenregister + newly registered units with commissioning
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

        for tech in technology:
            unit_data_orm = getattr(orm, self.orm_map[tech]["unit_data"], None)
            eeg_data_orm = getattr(orm, self.orm_map[tech].get("eeg_data", "KeyNotAvailable"), None)
            kwk_data_orm = getattr(orm, self.orm_map[tech].get("kwk_data", "KeyNotAvailable"), None)
            permit_data_orm = getattr(orm, self.orm_map[tech].get("permit_data", "KeyNotAvailable"), None)

            # Define query based on available tables for tech and user input
            subtables = [orm.BasicUnit]
            if unit_data_orm and "unit_data" in additional_data:
                subtables.append(unit_data_orm)
            if eeg_data_orm and "eeg_data" in additional_data:
                subtables.append(eeg_data_orm)
            if kwk_data_orm and "kwk_data" in additional_data:
                subtables.append(kwk_data_orm)
            if permit_data_orm and "permit_data" in additional_data:
                subtables.append(permit_data_orm)
            query = Query(subtables, session=session)

            # Define joins based on available tables for tech and user input
            duplicates_exclude = [
                # TODO: for most of the columns it needs to be decided newly which one is dropped
                # TODO: therefore it should be checked where is more data included (the other one(s) should be dropped)

                # TODO: change dropping of columns to consequently suffixing them. Whereever, also in a table for a single technology, a duplicate occurs, use a suffix for this column in all tables
                orm.BasicUnit.EegMastrNummer,
                orm.BasicUnit.BestandsanlageMastrNummer, # TODO: needs check
                orm.BasicUnit.StatisikFlag,
            ]
            if unit_data_orm and "unit_data" in additional_data:
                query = query.join(
                    unit_data_orm,
                    orm.BasicUnit.EinheitMastrNummer == unit_data_orm.EinheitMastrNummer,
                    isouter=True
                )
                duplicates_exclude += [
                    unit_data_orm.EinheitMastrNummer,
                    unit_data_orm.GenMastrNummer,
                    unit_data_orm.EinheitBetriebsstatus,  # TODO: needs check
                    unit_data_orm.NichtVorhandenInMigriertenEinheiten,  # TODO: needs check
                    unit_data_orm.Bruttoleistung,  # TODO: needs check
                    unit_data_orm.download_date,
                ]
                if "KwkMastrNummer" in  unit_data_orm.__table__.columns:
                    duplicates_exclude.append(unit_data_orm.KwkMastrNummer)
                if "SpeMastrNummer" in unit_data_orm.__table__.columns:
                    duplicates_exclude.append(unit_data_orm.SpeMastrNummer)
            if eeg_data_orm and "eeg_data" in additional_data:
                query = query.join(
                    eeg_data_orm,
                    orm.BasicUnit.EegMastrNummer == eeg_data_orm.EegMastrNummer,
                    isouter=True
                )
                duplicates_exclude += [
                    eeg_data_orm.EegMastrNummer,
                    eeg_data_orm.DatumLetzteAktualisierung,  # TODO: maybe re-include with suffix
                    eeg_data_orm.Meldedatum,  # TODO: maybe re-include with suffix
                ]
            if kwk_data_orm and "kwk_data" in additional_data:
                query = query.join(
                    kwk_data_orm,
                    orm.BasicUnit.KwkMastrNummer == kwk_data_orm.KwkMastrNummer,
                    isouter=True
                )
                duplicates_exclude += [
                    kwk_data_orm.KwkMastrNummer,
                    kwk_data_orm.Meldedatum,
                    kwk_data_orm.Inbetriebnahmedatum,
                    kwk_data_orm.DatumLetzteAktualisierung,
                    kwk_data_orm.AnlageBetriebsstatus,
                    kwk_data_orm.VerknuepfteEinheiten,
                    kwk_data_orm.AusschreibungZuschlag,
                ]
            if permit_data_orm and "permit_data" in additional_data:
                query = query.join(
                    permit_data_orm,
                    orm.BasicUnit.GenMastrNummer == permit_data_orm.GenMastrNummer,
                    isouter=True
                )
                duplicates_exclude += [
                    permit_data_orm.GenMastrNummer,
                    permit_data_orm.DatumLetzteAktualisierung,  # TODO: maybe re-include with suffix
                    permit_data_orm.Meldedatum,  # TODO: maybe re-include with suffix
                ]

            # Restricted to technology
            query = query.filter(orm.BasicUnit.Einheittyp == self.unit_type_map_reversed[tech])

            # Decide if migrated data or data of newly registered units or both is selected
            if statistic_flag and "unit_data" in additional_data:
                query = query.filter(unit_data_orm.StatisikFlag == statistic_flag)

            # Exclude certain columns that are duplicated
            for excl in duplicates_exclude:
                query = query.options(defer(excl))

            # Limit returned rows of query
            if limit:
                query = query.limit(limit)

            # Read data into pandas.DataFrame
            df = pd.read_sql(query.statement, query.session.bind, index_col="EinheitMastrNummer")

            # Make sure no duplicate column names exist
            assert not any(df.columns.duplicated())

            # Save to CSV
            to_csv(df, tech)

        # Create and save data package metadata file along with data
        data_path = get_data_version_dir()
        filenames = get_filenames()
        metadata_file = os.path.join(data_path, filenames["metadata"])

        mastr_technologies = [self.unit_type_map_reversed[tech] for tech in technology]
        newest_date = session.query(orm.BasicUnit.DatumLetzeAktualisierung).filter(
                        orm.BasicUnit.Einheittyp.in_(mastr_technologies)).order_by(
                        orm.BasicUnit.DatumLetzeAktualisierung.desc()).first()[0]
        metadata = datapackage_meta_json(newest_date, json_serialize=False)

        with open(metadata_file, 'w', encoding='utf-8') as f:
            json.dump(metadata, f, ensure_ascii=False, indent=4)
