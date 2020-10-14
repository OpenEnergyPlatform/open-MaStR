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
__author__ = "Ludee; christian-rli"
__issue__ = "https://github.com/OpenEnergyPlatform/examples/issues/52"
__version__ = "v0.10.0"


# import getpass
import os
import sys
import sqlalchemy as sa
import requests
import urllib3

import open_mastr.soap_api.config as lc

from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, Float, DateTime, Boolean
from collections import namedtuple

from zeep import Client, Settings
from zeep.cache import SqliteCache
from zeep.transports import Transport

UserToken = namedtuple('UserToken', ['user', 'token'])
API_MAX_DEMANDS = lc.get_data_config()["max_api_demands"]

import logging
log = logging.getLogger(__name__)
Base = declarative_base()

on_rtd = os.environ.get('READTHEDOCS', None) == 'True'


class Wind(Base):
    __tablename__ = 'mastr_wind'
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
    timestamp = Column(DateTime)
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
    timestamp_w = Column(DateTime)
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
    __tablename__ = 'mastr_solar'
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
    timestamp = Column(DateTime)
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
    timestamp_w = Column(DateTime)
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

class Biomass(Base):
    __tablename__ = 'mastr_biomass'
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
    timestamp = Column(DateTime)
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
    timestamp_w = Column(DateTime)
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

class Hydro(Base):
    __tablename__ = 'mastr_hydro'
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
    timestamp = Column(DateTime)
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
    timestamp_w = Column(DateTime)
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


def oep_upload():

    files = [(fname_hydro_all, 'hydro'), (fname_wind_all, 'wind'), (fname_solar_all, 'solar'), (fname_biomass_all, 'biomass')]

    # 1st, start a new oep connection
    [metadata, engine] = oep_session()
    # 2nd, check which powerunit files are 'maked'
    for file in files:
        if os.path.exists(os.path.dirname(file[0])):
            # 3rd, insert data into tables
            insert_data(engine, data, metadata, file[1])

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
        return [metadata, engine]

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


def insert_data(engine, data, metadata, unittype):
    # create tables if not exist 
    Base.metadata.create_all(bind=engine, checkfirst=True)
    # create session
    session = Session(bind=engine)
    # iterate over data to insert
    dicts = []
    for i in range(0, len(data)):
        dicts.append(
                dict(
                    id=str(data[i][0]),
                    name = str(data[i][1]),
                    EinheitMastrNummer = str(data[i][2]),
                    lid = str(data[i][3]),
                    Name = str(data[i][4]),
                    Einheitart = str(data[i][5]),
                    Einheittyp = str(data[i][6]),
                    Standort = str(data[i][7]),
                    Bruttoleistung = str(data[i][8]),
                    Erzeugungsleistung = str(data[i][9]),
                    EinheitBetriebsstatus = str(data[i][10]),
                    Anlagenbetreiber = str(data[i][11]),
                    EegMastrNummer = str(data[i][12]),
                    KwkMastrNummer = str(data[i][13]),
                    SpeMastrNummer = str(data[i][14]),
                    GenMastrNummer = str(data[i][15]),
                    BestandsanlageMastrNummer = str(data[i][16]),
                    NichtVorhandenInMigriertenEinheiten = str(data[i][17]),
                    version = str(data[i][18]),
                    timestamp = str(data[i][19]), 
                    lid_w = str(data[i][20]),
                    Ergebniscode = str(data[i][21]),
                    AufrufVeraltet = str(data[i][22]),
                    AufrufLebenszeitEnde = str(data[i][23]),
                    AufrufVersion = str(data[i][24]),
                    DatumLetzteAktualisierung = str(data[i][25]),
                    LokationMastrNummer = str(data[i][26]),
                    NetzbetreiberpruefungStatus = str(data[i][27]),
                    NetzbetreiberpruefungDatum = str(data[i][28]),
                    NetzbetreiberMastrNummer = str(data[i][29]),
                    Land = str(data[i][30]),
                    Bundesland = str(data[i][31]),
                    Landkreis = str(data[i][32]),
                    Gemeinde = str(data[i][33]),
                    Gemeindeschluessel = str(data[i][34]),
                    Postleitzahl = str(data[i][35]),
                    Gemarkung = str(data[i][36]),
                    FlurFlurstuecknummern = str(data[i][37]),
                    Strasse = str(data[i][38]),
                    StrasseNichtGefunden = str(data[i][39]),
                    Hausnummer = str(data[i][40]),
                    HausnummerNichtGefunden = str(data[i][41]),
                    Adresszusatz = str(data[i][42]),
                    Ort = str(data[i][43]),
                    Laengengrad = str(data[i][44]),
                    Breitengrad = str(data[i][45]),
                    UtmZonenwert = str(data[i][46]),
                    UtmEast = str(data[i][47]),
                    UtmNorth = str(data[i][48]),
                    GaussKruegerHoch = str(data[i][49]),
                    GaussKruegerRechts = str(data[i][50]),
                    Meldedatum = str(data[i][51]),
                    GeplantesInbetriebnahmedatum = str(data[i][52]),
                    Inbetriebnahmedatum = str(data[i][53]),
                    DatumEndgueltigeStilllegung =str(data[i][54]),
                    DatumBeginnVoruebergehendeStilllegung = str(data[i][55]),
                    DatumWiederaufnahmeBetrieb = str(data[i][56]),
                    EinheitBetriebsstatus_w = str(data[i][57]),
                    BestandsanlageMastrNummer_w = str(data[i][58]),
                    NichtVorhandenInMigriertenEinheiten_w = str(data[i][59]),
                    NameStromerzeugungseinheit = str(data[i][60]),
                    Weic = str(data[i][61]),
                    WeicDisplayName = str(data[i][62]),
                    Kraftwerksnummer = str(data[i][63]),
                    Energietraeger = str(data[i][64]),
                    Bruttoleistung_w = str(data[i][65]),
                    Nettonennleistung = str(data[i][66]),
                    AnschlussAnHoechstOderHochSpannung = str(data[i][67]),
                    Schwarzstartfaehigkeit = str(data[i][68]),
                    Inselbetriebsfaehigkeit = str(data[i][69]),
                    Einsatzverantwortlicher = str(data[i][70]),
                    FernsteuerbarkeitNb = str(data[i][71]),
                    FernsteuerbarkeitDv = str(data[i][72]),
                    FernsteuerbarkeitDr = str(data[i][73]),
                    Einspeisungsart = str(data[i][74]),
                    PraequalifiziertFuerRegelenergie = str(data[i][75]),
                    GenMastrNummer_w = str(data[i][76]),
                    NameWindpark = str(data[i][77]),
                    Lage = str(data[i][78]),
                    Seelage = str(data[i][79]),
                    ClusterOstsee = str(data[i][80]),
                    ClusterNordsee = str(data[i][81]),
                    Technologie = str(data[i][82]),
                    Typenbezeichnung = str(data[i][83]),
                    Nabenhoehe = str(data[i][84]),
                    Rotordurchmesser = str(data[i][85]),
                    AuflageAbschaltungLeistungsbegrenzung = str(data[i][86]),
                    Wassertiefe = str(data[i][87]),
                    Kuestenentfernung = str(data[i][88]),
                    EegMastrNummer_w = str(data[i][89]),
                    HerstellerID = str(data[i][90]),
                    HerstellerName = str(data[i][91]),
                    version_w = str(data[i][92]),
                    timestamp_w = str(data[i][93]),
                    lid_e = str(data[i][94]),
                    Ergebniscode_e = str(data[i][95]),
                    AufrufVeraltet_e = str(data[i][96]),
                    AufrufLebenszeitEnde_e = str(data[i][97]),
                    AufrufVersion_e = str(data[i][98]),
                    Meldedatum_e = str(data[i][99]),
                    DatumLetzteAktualisierung_e = str(data[i][100]),
                    EegInbetriebnahmedatum = str(data[i][101]),
                    AnlagenkennzifferAnlagenregister = str(data[i][102]),
                    AnlagenschluesselEeg = str(data[i][103]),
                    PrototypAnlage = str(data[i][104]),
                    PilotAnlage = str(data[i][105]),
                    InstallierteLeistung = str(data[i][106]),
                    VerhaeltnisReferenzertragErtrag5Jahre = str(data[i][107]),
                    VerhaeltnisReferenzertragErtrag10Jahre = str(data[i][108]),
                    VerhaeltnisReferenzertragErtrag15Jahre = str(data[i][109]),
                    AusschreibungZuschlag = str(data[i][110]),
                    Zuschlagsnummer = str(data[i][111]),
                    AnlageBetriebsstatus = str(data[i][112]),
                    VerknuepfteEinheit = str(data[i][113]),
                    version_e = str(data[i][114]),
                    timestamp_e = str(data[i][115]),
                    )
                )

    # decide which database to chose
    if unittype=='wind':
        session.bulk_insert_mappings(Wind, dicts)
    elif unittype=='hydro':
        session.bulk_insert_mappings(Hydro, dicts)
    elif unittype=='solar':
        session.bulk_insert_mappings(Solar, dicts)
    elif unittype=='biomass':
        session.bulk_insert_mappings(Biomass, dicts)

    # insert data into db
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
        token = lc.config_file_get(config_section, 'token')
    except:
        # import sys
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

    if on_rtd:
        user, token = (None, None)
    else:
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
