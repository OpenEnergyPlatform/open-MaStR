#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Service functions for OEP logging

Read data from MaStR API, process, and write to OEP

SPDX-License-Identifier: AGPL-3.0-or-later
"""

__copyright__ = "© Reiner Lemoine Institut"
__license__ = "GNU Affero General Public License Version 3 (AGPL-3.0)"
__url__ = "https://www.gnu.org/licenses/agpl-3.0.en.html"
__author__ = "Ludee; christian-rli; Bachibouzouk; solar-c"
__issue__ = "https://github.com/OpenEnergyPlatform/examples/issues/52"
__version__ = "v0.8.0"

import config as lc

# import getpass
import os
import sqlalchemy as sa
from collections import namedtuple
import requests
import urllib3

from zeep import Client, Settings
from zeep.cache import SqliteCache
from zeep.transports import Transport

UserToken = namedtuple('UserToken', ['user', 'token'])

import logging
log = logging.getLogger(__name__)


class PowerUnit(Base):
    __tablename__ = 'tablename',
    id = Column(Integer, primary_key=True),
    name = Column(String(2455))

class Wind(Base):
    EinheitMastrNummer = Column(String(256))
    Id  = Column(Integer, primary_key=True)
    lid = Column(Integer)
    Name = Column(String(256))
    Einheitart = Column(String(256))
    Einheittyp = Column(String(256))
    Standort = Column(String(256))
    Bruttoleistung = Column(Float)
    Erzeugungsleistung = Column(Float)
    EinheitBetriebsstatus = Column(String(256))
    Anlagenbetreiber = Column(String(256))
    EegMastrNummer = Column(String(256))
    KwkMastrNummer = Column(String(256))
    SpeMastrNummer = Column(String(256))
    GenMastrNummer = Column(String(256))
    BestandsanlageMastrNummer = Column(String(256))
    NichtVorhandenInMigriertenEinheiten = Column(String(256))
    version = Column(String(256))
    timestamp = Column(Timestamp)
    lid_w = Column(Integer)
    Ergebniscode = Column(String(256))
    AufrufVeraltet = Column(Boolean)
    AufrufLebenszeitEnde = Column(String(256))
    AufrufVersion = Column(String(256))
    DatumLetzteAktualisierung = Column(String(256))
    LokationMastrNummer = Column(String(256))
    NetzbetreiberpruefungStatus = Column(String(256))
    NetzbetreiberpruefungDatum = Column(String(256))
    NetzbetreiberMastrNummer = Column(String(256))
    Land = Column(String(256))
    Bundesland = Column(String(256))
    Landkreis = Column(String(256))
    Gemeinde = Column(String(256))
    Gemeindeschluessel = Column(String(256))
    Postleitzahl = Column(String(256))
    Gemarkung = Column(String(256))
    FlurFlurstuecknummern = Column(String(256))
    Strasse = Column(String(256))
    StrasseNichtGefunden = Column(Boolean)
    Hausnummer = Column(String(256))
    HausnummerNichtGefunden = Column(Boolean)
    Adresszusatz = Column(String(256))
    Ort = Column(String(256))
    Laengengrad = Column(String(256))
    Breitengrad = Column(String(256))
    UtmZonenwert = Column(String(256))
    UtmEast = Column(String(256))
    UtmNorth = Column(String(256))
    GaussKruegerHoch = Column(String(256))
    GaussKruegerRechts = Column(String(256))
    Meldedatum = Column(String(256))
    GeplantesInbetriebnahmedatum = Column(String(256))
    Inbetriebnahmedatum = Column(String(256))
    DatumEndgueltigeStilllegung = Column(String(256))
    DatumBeginnVoruebergehendeStilllegung = Column(String(256))
    DatumWiederaufnahmeBetrieb = Column(String(256))
    EinheitBetriebsstatus_w = Column(String(256))
    BestandsanlageMastrNummer_w = Column(String(256))
    NichtVorhandenInMigriertenEinheiten_w = Column(String(256))
    NameStromerzeugungseinheit = Column(String(256))
    Weic = Column(String(256))
    WeicDisplayName = Column(String(256))
    Kraftwerksnummer = Column(String(256))
    Energietraeger = Column(String(256))
    Bruttoleistung_w = Column(Float)
    Nettonennleistung = Column(String(256))
    AnschlussAnHoechstOderHochSpannung = Column(String(256))
    Schwarzstartfaehigkeit = Column(String(256))
    Inselbetriebsfaehigkeit = Column(String(256))
    Einsatzverantwortlicher = Column(String(256))
    FernsteuerbarkeitNb = Column(String(256))
    FernsteuerbarkeitDv = Column(String(256))
    FernsteuerbarkeitDr = Column(String(256))
    Einspeisungsart = Column(String(256))
    PraequalifiziertFuerRegelenergie = Column(String(256))
    GenMastrNummer_w = Column(String(256))
    NameWindpark = Column(String(256))
    Lage = Column(String(256))
    Seelage = Column(String(256))
    ClusterOstsee = Column(String(256))
    ClusterNordsee = Column(String(256))
    Technologie = Column(String(256))
    Typenbezeichnung = Column(String(256))
    Nabenhoehe = Column(String(256))
    Rotordurchmesser = Column(String(256))
    AuflageAbschaltungLeistungsbegrenzung = Column(String(256))
    Wassertiefe = Column(String(256))
    Kuestenentfernung = Column(String(256))
    EegMastrNummer_w = Column(String(256))
    HerstellerID = Column(String(256))
    HerstellerName = Column(String(256))
    version_w = Column(String(256))
    timestamp_w = Column(Timestamp)
    lid_e = Column(Integer)
    Ergebniscode_e = Column(String(256))
    AufrufVeraltet_e = Column(Boolean)
    AufrufLebenszeitEnde_e = Column(String(256))
    AufrufVersion_e = Column(String(256))
    Meldedatum_e = Column(String(256))
    DatumLetzteAktualisierung_e = Column(String(256))
    EegInbetriebnahmedatum = Column(String(256))
    AnlagenkennzifferAnlagenregister = Column(String(256))
    AnlagenschluesselEeg = Column(String(256))
    PrototypAnlage = Column(String(256))
    PilotAnlage = Column(String(256))
    InstallierteLeistung = Column(String(256))
    VerhaeltnisReferenzertragErtrag5Jahre = Column(String(256))
    VerhaeltnisReferenzertragErtrag10Jahre = Column(String(256))
    VerhaeltnisReferenzertragErtrag15Jahre = Column(String(256))
    AusschreibungZuschlag = Column(String(256))
    Zuschlagsnummer = Column(String(256))
    AnlageBetriebsstatus = Column(String(256))
    VerknuepfteEinheit = Column(String(256))
    version_e = Column(String(256))
    timestamp_e = Column(String(256))


class Solar(Base):

class Biomass(Base):

class Hydro(Base):


def oep_config():
    """Access config.ini.

    Returns
    -------
    UserToken : namedtuple
        API token (key) and user name (value).
    """
    config_section = 'OEP'

    # username
    try:
        lc.config_file_load()
        user = lc.config_file_get(config_section, 'user')
        log.info(f'Hello {user}, welcome back')
    except FileNotFoundError:
        user = input('Please provide your OEP username (default surname_name):')
        log.info(f'Hello {user}')

    # token
    try:
        token = lc.config_file_get(config_section, 'token')
        print(f'Load API token')
    except:
        import sys
        token = input('Token:')
        # token = getpass.getpass(prompt = 'Token:',
        #                         stream = sys.stdin)
        lc.config_section_set(config_section, value=user, key=token)
        log.info('Config file created')
    return UserToken(user, token)


# a = oep_config()
# a.user
# a.token

def oep_session():
    """SQLAlchemy session object with valid connection to database.

    Returns
    -------
    metadata : SQLAlchemy object
        Database connection object.
    """
    user, token = oep_config()
    # user = input('Enter OEP-username:')
    # token = getpass.getpass('Token:')

    # engine
    try:
        oep_url = 'openenergy-platform.org'  # 'oep.iks.cs.ovgu.de'
        oed_string = f'postgresql+oedialect://{user}:{token}@{oep_url}'
        engine = sa.create_engine(oed_string)
        metadata = sa.MetaData(bind=engine)

        print(f'OEP connection established: /n {metadata}')
        return metadata

    except:
        print('Password authentication failed for user: "{}"'.format(user))
        try:
            os.remove(lc.config_file)
            print('Existing config file deleted! /n Restart script and try again!')
        except OSError:
            print('Cannot delete file! /n Please check login parameters in config file!')


def get_table_powerunits(metadata):
    metadata = metadata.reflect()
    pu_table = metadata.tables['table name powerunits']
    return pu_table


def insert_data(engine, datalength):
    session = Session(bind=engine)
    session.bulk_insert_mappings(
        PowerUnit,
        [
            dict(
                id="asdhguio %d" % i,
                name = "fgasduo %d" %i,
                )
            for i in range(datalength)
        ],
        )
    session.commit()



def mastr_config():
    """Access config.ini.

    Returns
    -------
    user : str
        marktakteurMastrNummer (value).
    token : str
        API token (key).
    """
    config_section = 'MaStR'

    # user
    try:
        lc.config_file_load()
        user = lc.config_file_get(config_section, 'user')
        # print('Hello ' + user)
    except:
        user = input('Please provide your MaStR Nummer:')

    # token
    try:
        from config import config_file_get
        token = config_file_get(config_section, 'token')
    except:
        import sys
        token = input('Token:')
        # token = getpass.getpass(prompt='apiKey: ',
        #                            stream=sys.stderr)
        lc.config_section_set(config_section, value=user, key=token)
        print('Config file created.')
    return user, token


def mastr_session():
    """MaStR SOAP session using Zeep Client.

    Returns
    -------
    client : SOAP client
        API connection.
    client_bind : SOAP client bind
        bind API connection.
    token : str
        API key.
    user : str
        marktakteurMastrNummer.
    """
    user, token = mastr_config()

    wsdl = 'https://www.marktstammdatenregister.de/MaStRAPI/wsdl/mastr.wsdl'
    session = requests.Session()
    session.max_redirects = 30
    a = requests.adapters.HTTPAdapter(max_retries=3, pool_connections=2000, pool_maxsize=2000)
    session.mount('https://',a)
    session.mount('http://',a)
    transport = Transport(cache=SqliteCache(), timeout=600, session=session)
    settings = Settings(strict=True, xml_huge_tree=True)
    client = Client(wsdl=wsdl, transport=transport, settings=settings)
    client_bind = client.bind('Marktstammdatenregister', 'Anlage')

    mastr_suppress_parsing_errors(['parse-time-second'])

    # print(f'MaStR API connection established for user {user}')
    return client, client_bind, token, user


def mastr_suppress_parsing_errors(which_errors):
    """
    Install logging filters into zeep type parsing modules to suppress

    Arguments
    ---------
    which_errors : [str]
        Names of errors defined in `error_filters` to set up.
        Currently one of ('parse-time-second').

    NOTE
    ----
    zeep and mastr don't seem to agree on the correct time format. Instead of
    suppressing the error, we should fix the parsing error, or they should :).
    """

    class FilterExceptions(logging.Filter):
        def __init__(self, name, klass, msg):
            super().__init__(name)

            self.klass = klass
            self.msg = msg

        def filter(self, record):
            if record.exc_info is None:
                return 1

            kl, inst, tb = record.exc_info
            return 0 if isinstance(inst, self.klass) and inst.args[0] == self.msg else 1

    # Definition of available filters
    error_filters = [FilterExceptions('parse-time-second', ValueError, 'second must be in 0..59')]

    # Install filters selected by `which_errors`
    zplogger = logging.getLogger('zeep.xsd.types.simple')
    zplogger.filters = ([f for f in zplogger.filters if not isinstance(f, FilterExceptions)] +
                        [f for f in error_filters if f.name in which_errors])
