#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
BNetzA - MaStR Download - GeoSolarthermieGrubenKlaerschlamm

Read data from MaStR API and write to CSV files.

SPDX-License-Identifier: AGPL-3.0-or-later
"""

__copyright__ = "© Reiner Lemoine Institut"
__license__ = "GNU Affero General Public License Version 3 (AGPL-3.0)"
__url__ = "https://www.gnu.org/licenses/agpl-3.0.en.html"
__author__ = "Ludee"
__issue__ = "https://github.com/OpenEnergyPlatform/examples/issues/52"
__version__ = "v0.9.0"

from soap_api.sessions import mastr_session
from soap_api.utils import write_to_csv, get_data_version, read_power_units
from soap_api.utils import (fname_power_unit,
                            fname_power_unit_gsgk,
                            fname_gsgk_unit,
                            fname_gsgk_eeg,
                            fname_gsgk_fail_u,
                            fname_gsgk_fail_e)

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
def setup_power_unit_gsgk():
    """Setup file for Stromerzeugungseinheit-GeoSolarthermieGrubenKlaerschlamm (power-unit_gsgk).

    Check if file with Stromerzeugungseinheit (power-unit) exists.
    Read Stromerzeugungseinheit and filter Stromerzeugungseinheit-GeoSolarthermieGrubenKlaerschlamm.
    Remove duplicates and write to file.

    Returns
    -------
    fname_power_unit_gsgk : csv
        Write Stromerzeugungseinheit-GeoSolarthermieGrubenKlaerschlamm to csv file.
    """
    if os.path.isfile(fname_power_unit_gsgk):
        log.info(f'Skip setup for Stromerzeugungseinheit-GeoSolarthermieGrubenKlaerschlamm')

    else:
        if os.path.isfile(fname_power_unit):
            power_unit = read_power_units(fname_power_unit)

            power_unit_gsgk = power_unit[power_unit.Einheittyp == 'GeoSolarthermieGrubenKlaerschlamm']
            power_unit_gsgk = power_unit_gsgk.drop_duplicates(subset=['EinheitMastrNummer',
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
            log.info(f'Filter power-unit for gsgk and remove duplicates')
            power_unit_gsgk.reset_index()
            power_unit_gsgk.index.name = 'pu-id'

            write_to_csv(fname_power_unit_gsgk, power_unit_gsgk)
            power_unit_gsgk_cnt = power_unit_gsgk['timestamp'].count()
            log.info(f'Write {power_unit_gsgk_cnt} power-unit_gsgk to {fname_power_unit_gsgk}')

        else:
            log.info(f'Error reading power-unit from {fname_power_unit}')


def read_power_unit_gsgk(csv_name):
    """Read Stromerzeugungseinheit-GeoSolarthermieGrubenKlaerschlamm (power-unit_gsgk) from CSV file.

    Parameters
    ----------
    csv_name : str
        Name of file.

    Returns
    -------
    power_unit_gsgk : DataFrame
        Stromerzeugungseinheit-GeoSolarthermieGrubenKlaerschlamm.
    """
    if os.path.isfile(csv_name):
        power_unit_gsgk = pd.read_csv(csv_name, header=0, encoding='utf-8', sep=';', index_col=False,
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
        power_unit_gsgk_cnt = power_unit_gsgk['timestamp'].count()
        log.info(f'Read {power_unit_gsgk_cnt} Stromerzeugungseinheit-GeoSolarthermieGrubenKlaerschlamm from {csv_name}')
        return power_unit_gsgk

    else:
        log.info(f'Error reading {csv_name}')


# Download unit-gsgk
def download_unit_gsgk():
    """Download GeoSolarthermieGrubenKlaerschlammeinheit. Write results to csv file.


    ofname : string
        Path to save the downloaded files.

    Existing units: 31543 (2019-02-10)
    """
    start_from = 0
    setup_power_unit_gsgk()

    power_unit_gsgk = read_power_unit_gsgk(fname_power_unit_gsgk)
    power_unit_gsgk = power_unit_gsgk['EinheitMastrNummer']
    mastr_list = power_unit_gsgk.values.tolist()
    mastr_list = list(dict.fromkeys(mastr_list))
    mastr_list_len = len(mastr_list)
    log.info(f'Download {mastr_list_len} GeoSolarthermieGrubenKlaerschlammeinheit')

    for i in range(start_from, mastr_list_len, 1):
        unit_gsgk = get_power_unit_gsgk(mastr_list[i])  # First download
        if unit_gsgk is not None:
            write_to_csv(fname_gsgk_unit, unit_gsgk)
        else:
            log.exception(f'First download failed unit_gsgk ({i}): {mastr_list[i]}', exc_info=False)
            unit_gsgk = get_power_unit_gsgk(mastr_list[i])  # Second download
            if unit_gsgk is not None:
                write_to_csv(fname_gsgk_unit, unit_gsgk)
            else:
                mastr_fail = {'EinheitMastrNummer': [mastr_list[i]]}
                log.exception(f'Second download failed unit_gsgk ({i}): {mastr_list[i]}', exc_info=False)
                unit_fail = pd.DataFrame(mastr_fail)
                unit_fail['timestamp'] = str(datetime.datetime.now())
                unit_fail['comment'] = 'Second fail'
                write_to_csv(fname_gsgk_fail_u, unit_fail)

    retry_download_unit_gsgk()


def retry_download_unit_gsgk():
    """Download GeoSolarthermieGrubenKlaerschlammeinheit (unit-gsgk) from list.

    Read list of failed downloads from csv.
    Remove duplicates and retry download.
    Write download to file.

    Returns
    -------
    fname_gsgk_unit : csv
        Write GeoSolarthermieGrubenKlaerschlammeinheit to csv file.
    """
    start_from = 0
    if os.path.isfile(os.path.dirname(fname_gsgk_fail_u)):
        unit_fail_csv = pd.read_csv(fname_gsgk_fail_u, delimiter=';')
        unit_fail = unit_fail_csv['EinheitMastrNummer']
        unit_fail_list = unit_fail.values.tolist()
        unit_fail_list = list(dict.fromkeys(unit_fail_list))
        unit_fail_list_len = len(unit_fail_list)
        log.info(f'Retry download {unit_fail_list_len} failed GeoSolarthermieGrubenKlaerschlammeinheit')

        for i in range(start_from, unit_fail_list_len, 1):
            unit_wind = get_power_unit_gsgk(unit_fail_list[i])  # Third download
            if unit_wind is not None:
                write_to_csv(fname_gsgk_unit, unit_wind)
            else:
                unit_fail_unit = {'EinheitMastrNummer': [unit_fail_list[i]]}
                log.exception(f'Third download failed unit_wind: {unit_fail_list[i]}', exc_info=False)
                unit_fail_third = pd.DataFrame(unit_fail_unit)
                unit_fail_third['timestamp'] = str(datetime.datetime.now())
                unit_fail_third['comment'] = 'Third fail'
                write_to_csv(fname_gsgk_fail_u, unit_fail_third)
    else:
        log.info('No failed downloads for GeoSolarthermieGrubenKlaerschlammeinheit')


def get_power_unit_gsgk(mastr_unit_gsgk):
    """Get GeoSolarthermieGrubenKlaerschlammeinheit from API using GetEinheitGeoSolarthermieGrubenKlaerschlamm.

    Parameters
    ----------
    mastr_unit_gsgk : object
        Biomass from EinheitMastrNummerId.

    Returns
    -------
    unit_gsgk : DataFrame
        GeoSolarthermieGrubenKlaerschlammeinheit.
    """

    data_version = get_data_version()
    try:
        c = client_bind.GetEinheitGeoSolarthermieGrubenKlaerschlamm(apiKey=api_key,
                                           marktakteurMastrNummer=my_mastr,
                                           einheitMastrNummer=mastr_unit_gsgk)
        s = serialize_object(c)
        df = pd.DataFrame(list(s.items()), )
        unit_gsgk = df.set_index(list(df.columns.values)[0]).transpose()
        unit_gsgk.reset_index()
        unit_gsgk.index.names = ['lid']
        unit_gsgk['version'] = data_version
        unit_gsgk['timestamp'] = str(datetime.datetime.now())
        return unit_gsgk
    except Exception as e:
        # log.info('Download failed for %s', mastr_unit_gsgk)
        pass


def read_unit_gsgk(csv_name):
    """Read GeoSolarthermieGrubenKlaerschlammeinheit from CSV file.

    Parameters
    ----------
    csv_name : str
        Name of file.

    Returns
    -------
    unit_gsgk : DataFrame
        GeoSolarthermieGrubenKlaerschlammeinheit.
    """
    if os.path.isfile(csv_name):
        unit_gsgk = pd.read_csv(csv_name, header=0, encoding='utf-8', sep=';', index_col=False,
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
                                          'Hauptbrennstoff': str,
                                          'Biomasseart': str,
                                          'Technologie': str,
                                          'EegMastrNummer': str,
                                          'KwkMastrNummer': str,
                                          'version': str,
                                          'timestamp': str})
        unit_gsgk_cnt = unit_gsgk['timestamp'].count()
        log.info(f'Read {unit_gsgk_cnt} Biomassseeinheit from {csv_name}')
        return unit_gsgk

    else:
        log.info(f'Error reading {csv_name}')


# Download unit-gsgk-eeg
def download_unit_gsgk_eeg():
    """Download GeoSolarthermieGrubenKlaerschlammeinheit-EEG (unit-gsgk-eeg) using GetAnlageEegGeoSolarthermieGrubenKlaerschlamm request.

    Filter EegMastrNummer from Stromerzeugungseinheit-GeoSolarthermieGrubenKlaerschlamm.
    Remove duplicates and count.
    Loop over list and write download to file.

    Returns
    -------
    fname_gsgk_eeg : csv
        Write GeoSolarthermieGrubenKlaerschlammeinheit-EEG to csv file.
    """
    start_from = 0
    setup_power_unit_gsgk()

    power_unit_gsgk = read_power_unit_gsgk(fname_power_unit_gsgk)
    power_unit_gsgk = power_unit_gsgk['EegMastrNummer']
    mastr_list = power_unit_gsgk.values.tolist()
    mastr_list = list(dict.fromkeys(mastr_list))
    mastr_list = [x for x in mastr_list if str(x) != 'nan']
    mastr_list_len = len(mastr_list)
    log.info(f'Download {mastr_list_len} GeoSolarthermieGrubenKlaerschlammeinheit-EEG')

    for i in range(start_from, mastr_list_len, 1):
        unit_gsgk_eeg = get_unit_gsgk_eeg(mastr_list[i])  # First download
        if unit_gsgk_eeg is not None:
            write_to_csv(fname_gsgk_eeg, unit_gsgk_eeg)
        else:
            log.exception(f'First download failed unit_gsgk_eeg ({i}): {mastr_list[i]}', exc_info=False)
            unit_wind_eeg = get_unit_gsgk_eeg(mastr_list[i])  # Second download
            if unit_wind_eeg is not None:
                write_to_csv(fname_gsgk_eeg, unit_wind_eeg)
            else:
                eeg_fail = {'EegMastrNummer': [mastr_list[i]]}
                log.exception(f'Second download failed unit_gsgk_eeg ({i}): {mastr_list[i]}', exc_info=False)
                unit_fail = pd.DataFrame(eeg_fail)
                unit_fail['timestamp'] = str(datetime.datetime.now())
                unit_fail['comment'] = 'Second fail'
                write_to_csv(fname_gsgk_fail_e, unit_fail)

    retry_download_unit_gsgk_eeg()


def retry_download_unit_gsgk_eeg():
    """Download GeoSolarthermieGrubenKlaerschlammeinheit-EEG (unit-gsgk-eeg) from list.

    Read list of failed downloads from csv.
    Remove duplicates and retry download.
    Write download to file.

    Returns
    -------
    fname_gsgk_eeg : csv
        Write GeoSolarthermieGrubenKlaerschlammeinheit-EEG to csv file.
    """
    start_from = 0
    if os.path.isfile(os.path.dirname(fname_gsgk_fail_e)):
        unit_fail_csv = pd.read_csv(fname_gsgk_fail_e, delimiter=';')
        unit_fail = unit_fail_csv['EegMastrNummer']
        unit_fail_list = unit_fail.values.tolist()
        unit_fail_list = list(dict.fromkeys(unit_fail_list))
        unit_fail_list_len = len(unit_fail_list)
        log.info(f'Retry download {unit_fail_list_len} failed GeoSolarthermieGrubenKlaerschlammeinheit-EEG')

        for i in range(start_from, unit_fail_list_len, 1):
            unit_wind = get_unit_gsgk_eeg(unit_fail_list[i])
            if unit_wind is not None:
                write_to_csv(fname_gsgk_eeg, unit_wind)
            else:
                unit_fail_eeg = {'EegMastrNummer': [unit_fail_list[i]]}
                log.exception(f'Third download failed unit_gsgk_eeg: {unit_fail_list[i]}', exc_info=False)
                unit_fail_third = pd.DataFrame(unit_fail_eeg)
                unit_fail_third['timestamp'] = str(datetime.datetime.now())
                unit_fail_third['comment'] = 'Third fail'
                write_to_csv(fname_gsgk_fail_e, unit_fail_third)
    else:
        log.info('No failed downloads for GeoSolarthermieGrubenKlaerschlammeinheit-EEG')


def get_unit_gsgk_eeg(mastr_gsgk_eeg):
    """Get EEG-Anlage-GeoSolarthermieGrubenKlaerschlamm from API using GetAnlageEegGeoSolarthermieGrubenKlaerschlamm.

    Parameters
    ----------
    mastr_gsgk_eeg : str
        MaStR EEG Nr.

    Returns
    -------
    unit_gsgk_eeg : DataFrame
        EEG-Anlage-GeoSolarthermieGrubenKlaerschlamm.
    """
    data_version = get_data_version()
    try:
        c = client_bind.GetAnlageEegGeoSolarthermieGrubenKlaerschlamm(
            apiKey=api_key,
            marktakteurMastrNummer=my_mastr,
            eegMastrNummer=mastr_gsgk_eeg
        )
        s = serialize_object(c)
        df = pd.DataFrame(list(s.items()), )
        unit_gsgk_eeg = df.set_index(list(df.columns.values)[0]).transpose()
        unit_gsgk_eeg.reset_index()
        unit_gsgk_eeg.index.names = ['lid']
        unit_gsgk_eeg["version"] = data_version
        unit_gsgk_eeg["timestamp"] = str(datetime.datetime.now())
        return unit_gsgk_eeg
    except Exception as e:
        # log.info('Download failed for %s', mastr_gsgk_eeg)
        pass


def read_unit_gsgk_eeg(csv_name):
    """
    Encode and read EEG-Anlage-Biomass from CSV file.

    Parameters
    ----------
    csv_name : str
        Name of file.

    Returns
    -------
    unit_gsgk_eeg : DataFrame
        EEG-Anlage-Biomass
    """

    if os.path.isfile(csv_name):
        unit_gsgk_eeg = pd.read_csv(csv_name, header=0, sep=';', index_col=False, encoding='utf-8',
                                       dtype={'lid': int,
                                              'Ergebniscode': str,
                                              'AufrufVeraltet': str,
                                              'AufrufLebenszeitEnde': str,
                                              'AufrufVersion': str,
                                              'Meldedatum': str,
                                              'DatumLetzteAktualisierung': str,
                                              'EegInbetriebnahmedatum': str,
                                              'EegMastrNummer': str,
                                              'AnlagenschluesselEeg': str,
                                              'AnlagenkennzifferAnlagenregister': str,
                                              'InstallierteLeistung': str,
                                              'AusschliesslicheVerwendungBiomasse': str,
                                              'AusschreibungZuschlag': str,
                                              'Zuschlagsnummer': str,
                                              'BiogasInanspruchnahmeFlexiPraemie': str,
                                              'BiogasDatumInanspruchnahmeFlexiPraemie': str,
                                              'BiogasLeistungserhoehung': str,
                                              'BiogasDatumLeistungserhoehung': str,
                                              'BiogasUmfangLeistungserhoehung': str,
                                              'BiogasGaserzeugungskapazitaet': str,
                                              'BiogasHoechstbemessungsleistung': str,
                                              'BiomethanErstmaligerEinsatz': str,
                                              'AnlageBetriebsstatus': str,
                                              'VerknuepfteEinheit': str,
                                              'version': str,
                                              'timestamp': str})
        unit_gsgk_eeg_cnt = unit_gsgk_eeg['timestamp'].count()
        log.info(f'Read {unit_gsgk_eeg_cnt} GeoSolarthermieGrubenKlaerschlammeinheit-EEG from {csv_name}')
        return unit_gsgk_eeg

    else:
        log.info(f'Error reading {csv_name}')
