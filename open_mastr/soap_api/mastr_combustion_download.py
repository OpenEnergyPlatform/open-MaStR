#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
BNetzA - MaStR Download - Combustion

Read data from MaStR API and write to CSV files.

SPDX-License-Identifier: AGPL-3.0-or-later
"""

__copyright__ = "© Reiner Lemoine Institut"
__license__ = "GNU Affero General Public License Version 3 (AGPL-3.0)"
__url__ = "https://www.gnu.org/licenses/agpl-3.0.en.html"
__author__ = "Ludee; christian-rli; Bachibouzouk; solar-c"
__issue__ = "https://github.com/OpenEnergyPlatform/examples/issues/52"
__version__ = "v0.10.0"

from open_mastr.soap_api.sessions import mastr_session
from open_mastr.soap_api.utils import write_to_csv, get_data_version, read_power_units
from open_mastr.soap_api.utils import (fname_power_unit,
                            fname_power_unit_combustion,
                            fname_combustion_unit,
                            fname_combustion_kwk,
                            fname_combustion_fail_u,
                            fname_combustion_fail_e)

import pandas as pd
import datetime
import os
from zeep.helpers import serialize_object

import logging

log = logging.getLogger(__name__)

"""SOAP API"""
client, client_bind, token, user = mastr_session()
api_key = token
my_mastr = user


# Setup power-unit
def setup_power_unit_combustion():
    """Setup file for Stromerzeugungseinheit-Verbrennung (power-unit_combustion).

    Check if file with Stromerzeugungseinheit (power-unit) exists.
    Read Stromerzeugungseinheit and filter Stromerzeugungseinheit-Verbrennung.
    Remove duplicates and write to file.

    Returns
    -------
    fname_power_unit_combustion : csv
        Write Stromerzeugungseinheit-Verbrennung to csv file.
    """
    if os.path.isfile(fname_power_unit_combustion):
        log.info(f'Skip setup for Stromerzeugungseinheit-Verbrennung')

    else:
        if os.path.isfile(fname_power_unit):
            power_unit = read_power_units(fname_power_unit)

            power_unit_combustion = power_unit[power_unit.Einheittyp == 'Verbrennung']
            power_unit_combustion = power_unit_combustion.drop_duplicates(subset=['EinheitMastrNummer',
                                                                                  'Name',
                                                                                  'Einheitart',
                                                                                  'Einheittyp',
                                                                                  'Standort',
                                                                                  'Bruttoleistung',
                                                                                  'Erzeugungsleistung',
                                                                                  'EinheitBetriebsstatus',
                                                                                  'Anlagenbetreiber',
                                                                                  'EegMastrNummer',
                                                                                  'KwkMastrNummer',
                                                                                  'SpeMastrNummer',
                                                                                  'GenMastrNummer'])
            log.info(f'Filter power-unit for combustion and remove duplicates')
            power_unit_combustion.reset_index()
            power_unit_combustion.index.name = 'pu-id'

            write_to_csv(fname_power_unit_combustion, power_unit_combustion)
            power_unit_combustion_cnt = power_unit_combustion['timestamp'].count()
            log.info(f'Write {power_unit_combustion_cnt} power-unit_combustion to {fname_power_unit_combustion}')

        else:
            log.info(f'Error reading power-unit from {fname_power_unit}')


def read_power_unit_combustion(csv_name):
    """Read Stromerzeugungseinheit-Verbrennung (power-unit_combustion) from CSV file.

    Parameters
    ----------
    csv_name : str
        Name of file.

    Returns
    -------
    power_unit_combustion : DataFrame
        Stromerzeugungseinheit-Verbrennung.
    """
    if os.path.isfile(csv_name):
        power_unit_combustion = pd.read_csv(csv_name, header=0, encoding='utf-8', sep=';', index_col=False,
                                            dtype={
                                                'pu-id': str,
                                                'lid': str,
                                                'EinheitMastrNummer': str,
                                                'Name': str,
                                                'Einheitart': str,
                                                'Einheittyp': str,
                                                'Standort': str,
                                                'Bruttoleistung': str,
                                                'Erzeugungsleistung': str,
                                                'EinheitBetriebsstatus': str,
                                                'Anlagenbetreiber': str,
                                                'EegMastrNummer': str,
                                                'KwkMastrNummer': str,
                                                'SpeMastrNummer': str,
                                                'GenMastrNummer': str,
                                                'BestandsanlageMastrNummer': str,
                                                'NichtVorhandenInMigriertenEinheiten': str,
                                                'StatisikFlag': str,
                                                'version': str,
                                                'timestamp': str})
        power_unit_combustion_cnt = power_unit_combustion['timestamp'].count()
        log.info(f'Read {power_unit_combustion_cnt} Stromerzeugungseinheit-Verbrennung from {csv_name}')
        return power_unit_combustion

    else:
        log.info(f'Error reading {csv_name}')


# Download unit-combustion
def download_unit_combustion():
    """Download Verbrennungeinheit. Write results to csv file.


    ofname : string
        Path to save the downloaded files.

    Existing units: 31543 (2019-02-10)
    """
    start_from = 0
    setup_power_unit_combustion()

    power_unit_combustion = read_power_unit_combustion(fname_power_unit_combustion)
    power_unit_combustion = power_unit_combustion['EinheitMastrNummer']
    mastr_list = power_unit_combustion.values.tolist()
    mastr_list = list(dict.fromkeys(mastr_list))
    mastr_list_len = len(mastr_list)
    log.info(f'Download {mastr_list_len} Verbrennungeinheit')

    for i in range(start_from, mastr_list_len, 1):
        unit_combustion = get_power_unit_combustion(mastr_list[i])  # First download
        if unit_combustion is not None:
            write_to_csv(fname_combustion_unit, unit_combustion)
        else:
            log.exception(f'First download failed unit_combustion ({i}): {mastr_list[i]}', exc_info=False)
            unit_combustion = get_power_unit_combustion(mastr_list[i])  # Second download
            if unit_combustion is not None:
                write_to_csv(fname_combustion_unit, unit_combustion)
            else:
                mastr_fail = {'EinheitMastrNummer': [mastr_list[i]]}
                log.exception(f'Second download failed unit_combustion ({i}): {mastr_list[i]}', exc_info=False)
                unit_fail = pd.DataFrame(mastr_fail)
                unit_fail['timestamp'] = str(datetime.datetime.now())
                unit_fail['comment'] = 'Second fail'
                write_to_csv(fname_combustion_fail_u, unit_fail)

    retry_download_unit_combustion()


def retry_download_unit_combustion():
    """Download Verbrennungeinheit (unit-combustion) from list.

    Read list of failed downloads from csv.
    Remove duplicates and retry download.
    Write download to file.

    Returns
    -------
    fname_combustion_unit : csv
        Write Verbrennungeinheit to csv file.
    """
    start_from = 0
    if os.path.isfile(os.path.dirname(fname_combustion_fail_u)):
        unit_fail_csv = pd.read_csv(fname_combustion_fail_u, delimiter=';')
        unit_fail = unit_fail_csv['EinheitMastrNummer']
        unit_fail_list = unit_fail.values.tolist()
        unit_fail_list = list(dict.fromkeys(unit_fail_list))
        unit_fail_list_len = len(unit_fail_list)
        log.info(f'Retry download {unit_fail_list_len} failed Verbrennungeinheit')

        for i in range(start_from, unit_fail_list_len, 1):
            unit_wind = get_power_unit_combustion(unit_fail_list[i])  # Third download
            if unit_wind is not None:
                write_to_csv(fname_combustion_unit, unit_wind)
            else:
                unit_fail_unit = {'EinheitMastrNummer': [unit_fail_list[i]]}
                log.exception(f'Third download failed unit_wind: {unit_fail_list[i]}', exc_info=False)
                unit_fail_third = pd.DataFrame(unit_fail_unit)
                unit_fail_third['timestamp'] = str(datetime.datetime.now())
                unit_fail_third['comment'] = 'Third fail'
                write_to_csv(fname_combustion_fail_u, unit_fail_third)
    else:
        log.info('No failed downloads for Verbrennungeinheit')


def get_power_unit_combustion(mastr_unit_combustion):
    """Get Verbrennungeinheit from API using GetEinheitVerbrennung.

    Parameters
    ----------
    mastr_unit_combustion : object
        Combustion from EinheitMastrNummerId.

    Returns
    -------
    unit_combustion : DataFrame
        Verbrennungeinheit.
    """

    data_version = get_data_version()
    try:
        c = client_bind.GetEinheitVerbrennung(apiKey=api_key,
                                              marktakteurMastrNummer=my_mastr,
                                              einheitMastrNummer=mastr_unit_combustion)
        s = serialize_object(c)
        df = pd.DataFrame(list(s.items()), )
        unit_combustion = df.set_index(list(df.columns.values)[0]).transpose()
        unit_combustion.reset_index()
        unit_combustion.index.names = ['lid']
        unit_combustion['version'] = data_version
        unit_combustion['timestamp'] = str(datetime.datetime.now())
        return unit_combustion
    except Exception as e:
        # log.info('Download failed for %s', mastr_unit_combustion)
        pass


def read_unit_combustion(csv_name):
    """Read Verbrennungeinheit from CSV file.

    Parameters
    ----------
    csv_name : str
        Name of file.

    Returns
    -------
    unit_combustion : DataFrame
        Verbrennungeinheit.
    """
    if os.path.isfile(csv_name):
        unit_combustion = pd.read_csv(csv_name, header=0, encoding='utf-8', sep=';', index_col=False,
                                      dtype={'lid': int,
                                             'Ergebniscode': str,
                                             'AufrufVeraltet': str,
                                             'AufrufLebenszeitEnde': str,
                                             'AufrufVersion': str,
                                             'EinheitMastrNummer': str,
                                             'DatumLetzteAktualisierung': str,
                                             'LokationMastrNummer': str,
                                             'NetzbetreiberpruefungStatus': str,
                                             'NetzbetreiberpruefungDatum': str,
                                             'AnlagenbetreiberMastrNummer': str,
                                             'Land': str,
                                             'Bundesland': str,
                                             'Landkreis': str,
                                             'Gemeinde': str,
                                             'Gemeindeschluessel': str,
                                             'Postleitzahl': str,
                                             'Gemarkung': str,
                                             'FlurFlurstuecknummern': str,
                                             'Strasse': str,
                                             'StrasseNichtGefunden': str,
                                             'Hausnummer': str,
                                             'HausnummerNichtGefunden': str,
                                             'Adresszusatz': str,
                                             'Ort': str,
                                             'Laengengrad': str,
                                             'Breitengrad': str,
                                             'UtmZonenwert': str,
                                             'UtmEast': str,
                                             'UtmNorth': str,
                                             'GaussKruegerHoch': str,
                                             'GaussKruegerRechts': str,
                                             'Meldedatum': str,
                                             'GeplantesInbetriebnahmedatum': str,
                                             'Inbetriebnahmedatum': str,
                                             'DatumEndgueltigeStilllegung': str,
                                             'DatumBeginnVoruebergehendeStilllegung': str,
                                             'DatumWiederaufnahmeBetrieb': str,
                                             'EinheitBetriebsstatus': str,
                                             'BestandsanlageMastrNummer': str,
                                             'NichtVorhandenInMigriertenEinheiten': str,
                                             'AltAnlagenbetreiberMastrNummer': str,
                                             'DatumDesBetreiberwechsels': str,
                                             'DatumRegistrierungDesBetreiberwechsels': str,
                                             'StatisikFlag': str,
                                             'NameStromerzeugungseinheit': str,
                                             'Weic': str,
                                             'WeicDisplayName': str,
                                             'Kraftwerksnummer': str,
                                             'Energietraeger': str,
                                             'Bruttoleistung': float,
                                             'Nettonennleistung': float,
                                             'AnschlussAnHoechstOderHochSpannung': str,
                                             'Schwarzstartfaehigkeit': str,
                                             'Inselbetriebsfaehigkeit': str,
                                             'Einsatzverantwortlicher': str,
                                             'FernsteuerbarkeitNb': str,
                                             'FernsteuerbarkeitDv': str,
                                             'FernsteuerbarkeitDr': str,
                                             'Einspeisungsart': str,
                                             'PraequalifiziertFuerRegelenergie': str,
                                             'GenMastrNummer': str,
                                             'NameKraftwerk': str,
                                             'NameKraftwerksblock': str,
                                             'DatumBaubeginn': str,
                                             'AnzeigeEinerStilllegung': str,
                                             'ArtDerStilllegung': str,
                                             'DatumBeginnVorlaeufigenOderEndgueltigenStilllegung': str,
                                             'SteigerungNettonennleistungKombibetrieb': str,
                                             'AnlageIstImKombibetrieb': str,
                                             'MastrNummernKombibetrieb': str,
                                             'NetzreserveAbDatum': str,
                                             'SicherheitsbereitschaftAbDatum': str,
                                             'Hauptbrennstoff': str,
                                             'WeitererHauptbrennstoff': str,
                                             'WeitereBrennstoffe': str,
                                             'VerknuepfteErzeugungseinheiten': str,
                                             'BestandteilGrenzkraftwerk': str,
                                             'NettonennleistungDeutschland': str,
                                             'AnteiligNutzungsberechtigte': str,
                                             'Notstromaggregat': str,
                                             'Einsatzort': str,
                                             'KwkMastrNummer': str,
                                             'Technologie': str,
                                             'version': str,
                                             'timestamp': str})
        unit_combustion_cnt = unit_combustion['timestamp'].count()
        log.info(f'Read {unit_combustion_cnt} Verbrennungseinheit from {csv_name}')
        return unit_combustion

    else:
        log.info(f'Error reading {csv_name}')


# Download unit-combustion-kwk
def download_unit_combustion_kwk():
    """Download Verbrennungeinheit-KWK (unit-combustion-kwk) using GetAnlageKwk request.

    Filter KwkMastrNummer from Stromerzeugungseinheit-Verbrennung.
    Remove duplicates and count.
    Loop over list and write download to file.

    Returns
    -------
    fname_combustion_kwk : csv
        Write Verbrennungeinheit-KWK to csv file.
    """
    start_from = 0
    setup_power_unit_combustion()

    power_unit_combustion = read_power_unit_combustion(fname_power_unit_combustion)
    power_unit_combustion = power_unit_combustion['KwkMastrNummer']
    mastr_list = power_unit_combustion.values.tolist()
    mastr_list = list(dict.fromkeys(mastr_list))
    mastr_list = [x for x in mastr_list if str(x) != 'nan']
    mastr_list_len = len(mastr_list)
    log.info(f'Download {mastr_list_len} Verbrennungeinheit-KWK')

    for i in range(start_from, mastr_list_len, 1):
        unit_combustion_kwk = get_unit_combustion_kwk(mastr_list[i])  # First download
        if unit_combustion_kwk is not None:
            write_to_csv(fname_combustion_kwk, unit_combustion_kwk)
        else:
            log.exception(f'First download failed unit_combustion_kwk ({i}): {mastr_list[i]}', exc_info=False)
            unit_combustion_kwk = get_unit_combustion_kwk(mastr_list[i])  # Second download
            if unit_combustion_kwk is not None:
                write_to_csv(fname_combustion_kwk, unit_combustion_kwk)
            else:
                kwk_fail = {'EegMastrNummer': [mastr_list[i]]}
                log.exception(f'Second download failed unit_combustion_kwk ({i}): {mastr_list[i]}', exc_info=False)
                unit_fail = pd.DataFrame(kwk_fail)
                unit_fail['timestamp'] = str(datetime.datetime.now())
                unit_fail['comment'] = 'Second fail'
                write_to_csv(fname_combustion_fail_e, unit_fail)

    retry_download_unit_combustion_kwk()


def retry_download_unit_combustion_kwk():
    """Download Verbrennungeinheit-KWK (unit-combustion-kwk) from list.

    Read list of failed downloads from csv.
    Remove duplicates and retry download.
    Write download to file.

    Returns
    -------
    fname_combustion_kwk : csv
        Write Verbrennungeinheit-KWK to csv file.
    """
    start_from = 0
    if os.path.isfile(os.path.dirname(fname_combustion_fail_e)):
        unit_fail_csv = pd.read_csv(fname_combustion_fail_e, delimiter=';')
        unit_fail = unit_fail_csv['EegMastrNummer']
        unit_fail_list = unit_fail.values.tolist()
        unit_fail_list = list(dict.fromkeys(unit_fail_list))
        unit_fail_list_len = len(unit_fail_list)
        log.info(f'Retry download {unit_fail_list_len} failed Verbrennungeinheit-KWK')

        for i in range(start_from, unit_fail_list_len, 1):
            unit_wind = get_unit_combustion_kwk(unit_fail_list[i])
            if unit_wind is not None:
                write_to_csv(fname_combustion_kwk, unit_wind)
            else:
                unit_fail_kwk = {'EegMastrNummer': [unit_fail_list[i]]}
                log.exception(f'Third download failed unit_combustion_kwk: {unit_fail_list[i]}', exc_info=False)
                unit_fail_third = pd.DataFrame(unit_fail_kwk)
                unit_fail_third['timestamp'] = str(datetime.datetime.now())
                unit_fail_third['comment'] = 'Third fail'
                write_to_csv(fname_combustion_fail_e, unit_fail_third)
    else:
        log.info('No failed downloads for Verbrennungeinheit-KWK')


def get_unit_combustion_kwk(mastr_combustion_kwk):
    """Get KWK-Anlage-Verbrennung from API using GetAnlageKwk.

    Parameters
    ----------
    mastr_combustion_kwk : str
        MaStR KWK Nr.

    Returns
    -------
    unit_combustion_kwk : DataFrame
        KWK-Anlage-Verbrennung.
    """
    data_version = get_data_version()
    try:
        c = client_bind.GetAnlageKwk(
            apiKey=api_key,
            marktakteurMastrNummer=my_mastr,
            kwkMastrNummer=mastr_combustion_kwk
        )
        s = serialize_object(c)
        df = pd.DataFrame(list(s.items()), )
        unit_combustion_kwk = df.set_index(list(df.columns.values)[0]).transpose()
        unit_combustion_kwk.reset_index()
        unit_combustion_kwk.index.names = ['lid']
        unit_combustion_kwk["version"] = data_version
        unit_combustion_kwk["timestamp"] = str(datetime.datetime.now())
        return unit_combustion_kwk
    except Exception as e:
        # log.info('Download failed for %s', mastr_combustion_kwk)
        pass


def read_unit_combustion_kwk(csv_name):
    """
    Encode and read KWK-Anlage-Biomass from CSV file.

    Parameters
    ----------
    csv_name : str
        Name of file.

    Returns
    -------
    unit_combustion_kwk : DataFrame
        KWK-Anlage-Biomass
    """

    if os.path.isfile(csv_name):
        unit_combustion_kwk = pd.read_csv(csv_name, header=0, sep=';', index_col=False, encoding='utf-8',
                                          dtype={'lid': int,
                                                 'Ergebniscode': str,
                                                 'AufrufVeraltet': str,
                                                 'AufrufLebenszeitEnde': str,
                                                 'AufrufVersion': str,
                                                 'KwkMastrNummer': str,
                                                 'AusschreibungZuschlag': str,
                                                 'Zuschlagnummer': str,
                                                 'DatumLetzteAktualisierung': str,
                                                 'Inbetriebnahmedatum': str,
                                                 'Meldedatum': str,
                                                 'ThermischeNutzleistung': str,
                                                 'ElektrischeKwkLeistung': str,
                                                 'VerknuepfteEinheiten': str,
                                                 'AnlageBetriebsstatus': str,
                                                 'version': str,
                                                 'timestamp': str})
        unit_combustion_kwk_cnt = unit_combustion_kwk['timestamp'].count()
        log.info(f'Read {unit_combustion_kwk_cnt} Verbrennungeinheit-KWK from {csv_name}')
        return unit_combustion_kwk

    else:
        log.info(f'Error reading {csv_name}')
