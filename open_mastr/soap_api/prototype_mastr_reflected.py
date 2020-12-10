import datetime
import json
import os
from sqlalchemy.orm import sessionmaker, Query
from sqlalchemy import and_, create_engine
import shlex
import subprocess

from open_mastr.soap_api.download import MaStRDownload, MaStRAPI
import open_mastr.soap_api.db_models as db


engine = create_engine(
    "postgresql+psycopg2://open-mastr:open-mastr@localhost:55443/open-mastr", echo=False
)
Session = sessionmaker(bind=engine)
session = Session()

# Create datadb.Base table
# with engine.connect().execution_options(autocommit=True) as con:
#     con.execute(f"CREATE SCHEMA IF NOT EXISTS {db.Base.metadata.schema}")
# db.Base.metadata.create_all(engine)
# db.BasicUnit.metadata.create_all(engine)


def chunks(lst, n):
    """Yield successive n-sized chunks from lst.

    `Credits <https://stackoverflow.com/questions/312443/how-do-you-split-a-list-into-evenly-sized-chunks>`_
    """
    if isinstance(lst, Query):
        length = lst.count()
    else:
        length = len(lst)
    for i in range(0, length, n):
        yield lst[i:i + n]


class MaStRReflected:
    def __init__(self, empty_schema=False, restore_dump=None):

        # Create datadb.Base tables
        with engine.connect().execution_options(autocommit=True) as con:
            if empty_schema:
                con.execute(f"DROP SCHEMA IF EXISTS {db.Base.metadata.schema} CASCADE;")
            con.execute(f"CREATE SCHEMA IF NOT EXISTS {db.Base.metadata.schema};")
        db.Base.metadata.create_all(engine)

        # Associate downloader
        self.mastr_dl = MaStRDownload()

        # Restore datadb.Base from a dump
        if restore_dump:
            self.restore(restore_dump)

        # Map technologies on ORMs
        self.orm_map = {
            "wind": {
                "unit_data": "WindExtended"
            },
            "solar": {
                "unit_data": "SolarExtended"
            },
        }

    def initdb(self):
        """ Initialize the local datadb.Base used for data processing."""
        conf_file_path = os.path.abspath(
            os.path.join(os.path.dirname(__file__), "..", "..")
        )

        subprocess.run(
            ["docker-compose", "up", "-d"],
            cwd=conf_file_path,
        )

    def backfill_basic(self, technology, date=None, limit=None):
        """Loads basic unit information for all units until `date`.

        Parameters
        ----------
        technology: str or list
            Specify technologies for which data should be backfilled
        date: None, :class:`datetime.datetime`, str
            Specify backfiill date to which data is retrieved

            * `datetime.datetime(2020, 11, 27)`: Retrieve data which is is newer than this time stamp
            * 'latest': Retrieve data which is newer than the newest data already in the table
            * `None`: Complete backfill

            Defaults to `None`.
        """
        # TODO: add keyword argument overwrite. If true, queried data overwrites existing without checking
        # TODO: Default is False, which refers to only inserting new or updated data (with newer timestamp than existing)

        # TODO: add option to only consider units with StatistikFlag=="B"
        # Process arguments
        if isinstance(technology, str):
            technology = [technology]
        # Set limit to a number >> number of units of technology with most units
        if limit is None:
            limit = 10 ** 8
        if date == "latest":
            raise NotImplementedError

        # Retrieve data for each technology separately
        for tech in technology:

            basic_units = self.mastr_dl._basic_unit_data(tech, limit, date_from=date)

            # Remove duplicates
            basic_units = [
                unit
                for n, unit in enumerate(basic_units)
                if unit["EinheitMastrNummer"]
                not in [_["EinheitMastrNummer"] for _ in basic_units[n + 1 :]]
            ]

            # Store data in database
            for basic_unit in basic_units:
                # Add basic unit information
                basic = db.BasicUnit(**basic_unit)
                session.add(basic)

                # Save unit identifiers for later additional data request
                data_request = db.AdditionalDataRequested(
                    EinheitMastrNummer=basic.EinheitMastrNummer,
                    additional_data_id=basic.EinheitMastrNummer,
                    technology=tech,
                    data_type="unit_data",
                    request_date=datetime.datetime.now(tz=datetime.timezone.utc),
                )
                session.add(data_request)
                if basic.EegMastrNummer:
                    data_request = db.AdditionalDataRequested(
                        EinheitMastrNummer=basic.EinheitMastrNummer,
                        additional_data_id=basic.EegMastrNummer,
                        technology=tech,
                        data_type="eeg_data",
                        request_date=datetime.datetime.now(tz=datetime.timezone.utc),
                    )
                    session.add(data_request)
                if basic.KwkMastrNummer:
                    data_request = db.AdditionalDataRequested(
                        EinheitMastrNummer=basic.EinheitMastrNummer,
                        additional_data_id=basic.KwkMastrNummer,
                        technology=tech,
                        data_type="kwk_data",
                        request_date=datetime.datetime.now(tz=datetime.timezone.utc),
                    )
                    session.add(data_request)
                if basic.GenMastrNummer:
                    data_request = db.AdditionalDataRequested(
                        EinheitMastrNummer=basic.EinheitMastrNummer,
                        additional_data_id=basic.GenMastrNummer,
                        technology=tech,
                        data_type="permit_data",
                        request_date=datetime.datetime.now(tz=datetime.timezone.utc),
                    )
                    session.add(data_request)
            session.commit()

    def retrieve_additional_data(self, technology, data_type, limit=None, chunksize=1000):

        # TODO: flatten dictionary
        # Mapping of download from MaStRDownload
        download_functions = {
            "unit_data": "_extended_unit_data",
            "eeg_data": "_eeg_unit_data",
            "kwk_data": "_kwk_unit_data",
            "permit_data": "_permit_unit_data",
        }

        # Get list of IDs
        requested = session.query(db.AdditionalDataRequested).filter(
            and_(db.AdditionalDataRequested.data_type == data_type,
                 db.AdditionalDataRequested.technology == technology)).limit(limit)

        if limit:
            if chunksize > limit:
                chunksize = limit

        for requested_chunk in list(chunks(requested, chunksize)):

            ids = [_.additional_data_id for _ in requested_chunk]

            if ids:
                # Retrieve data
                unit_data, missed_units = self.mastr_dl._additional_data(technology, ids, download_functions[data_type])

                # Prepare data and add to database table
                for unit_dat in unit_data:
                    # Remove query status information from response
                    for exclude in ["Ergebniscode", "AufrufVeraltet", "AufrufVersion", "AufrufLebenszeitEnde"]:
                        del unit_dat[exclude]

                    # A temporary hack to store dicts in the database. Will be replaced by dict flattening
                    for k, v in unit_dat.items():
                        if isinstance(v, dict):
                            unit_dat[k] = json.dumps(v)

                    # Create new instance and update potentially existing one
                    unit = getattr(db, self.orm_map[technology][data_type])(**unit_dat)
                    session.merge(unit)

                # Log units where data retrieval was not successful
                for missed_unit in missed_units:
                    missed = db.MissedAdditionalData(additional_data_id=missed_unit)
                    session.add(missed)

                # Remove units from additional data request table if additional data was retrieved
                for requested_unit in requested_chunk:
                    if requested_unit.additional_data_id not in missed_units:
                        session.delete(requested_unit)

                # Send to datadb.Base complete transactions
                session.commit()

    def dump(self, dumpfile="open-mastr-continuous-update.backup"):
        """
        Dump MaStR datadb.Base.

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
        Restore the MaStR datadb.Base from an SQL dump.

        Parameters
        ----------
        dumpfile : str or path-like, optional
            Save path for dump including filename. When only a filename is given, the dump is restored from CWD.
        """
        # Interpret file name and path
        dump_file_dir, dump_file = os.path.split(dumpfile)
        if dump_file_dir:
            cwd = dump_file_dir
        else:
            cwd = os.path.dirname(__file__)

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

limit = 150000

mastr_refl = MaStRReflected(empty_schema=True)
# mastr_refl.backfill_basic("solar", datetime.datetime(2020, 11, 27, 22, 0, 0), limit=10)
mastr_refl.initdb()
mastr_refl.backfill_basic("wind", limit=limit)

# Dump + restore data
dump_file = "open-mastr-continuous-update_wind-120000.backup"
# mastr_refl.restore(dump_file)

# Download additional unit data
mastr_refl.retrieve_additional_data("wind", "unit_data", limit=limit)

# Dump
mastr_refl.dump(dump_file)
