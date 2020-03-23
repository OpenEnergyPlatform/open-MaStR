#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
BNetzA - MaStR Download - Hydro

Read data from MaStR API and write to CSV files.

SPDX-License-Identifier: AGPL-3.0-or-later
"""

__copyright__ = "© Reiner Lemoine Institut"
__license__ = "GNU Affero General Public License Version 3 (AGPL-3.0)"
__url__ = "https://www.gnu.org/licenses/agpl-3.0.en.html"
__author__ = "Ludee; christian-rli; Bachibouzouk; solar-c"
__issue__ = "https://github.com/OpenEnergyPlatform/examples/issues/52"
__version__ = "v0.9.0"

from soap_api.sessions import mastr_session
from soap_api.utils import write_to_csv, get_data_version, read_power_units
from soap_api.utils import (fname_power_unit,
                            fname_power_unit_hydro,
                            fname_hydro_unit,
                            fname_hydro_eeg,
                            fname_hydro_fail_u,
                            fname_hydro_fail_e)

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
def setup_power_unit_hydro():
    """Setup file for Stromerzeugungseinheit-Wasser (power-unit_hydro).

    Check if file with Stromerzeugungseinheit (power-unit) exists.
    Read Stromerzeugungseinheit and filter Stromerzeugungseinheit-Wasser.
    Remove duplicates and write to file.

    Returns
    -------
    fname_power_unit_hydro : csv
        Write Stromerzeugungseinheit-Wasser to csv file.
    """
    if os.path.isfile(fname_power_unit_hydro):
        log.info(f'Skip setup for Stromerzeugungseinheit-Wasser')

    else:
        if os.path.isfile(fname_power_unit):
            power_unit = read_power_units(fname_power_unit)

            power_unit_hydro = power_unit[power_unit.Einheittyp == 'Wasser']
            power_unit_hydro = power_unit_hydro.drop_duplicates(subset=['EinheitMastrNummer',
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
            log.info(f'Filter power-unit for hydro and remove duplicates')
            power_unit_hydro.reset_index()
            power_unit_hydro.index.name = 'pu-id'

            write_to_csv(fname_power_unit_hydro, power_unit_hydro)
            power_unit_hydro_cnt = power_unit_hydro['timestamp'].count()
            log.info(f'Write {power_unit_hydro_cnt} power-unit_hydro to {fname_power_unit_hydro}')

        else:
            log.info(f'Error reading power-unit from {fname_power_unit}')


def read_power_unit_hydro(csv_name):
    """Encode and read Stromerzeugungseinheit-Wasser (power-unit_hydro) from CSV file.

    Parameters
    ----------
    csv_name : str
        Name of file.

    Returns
    -------
    power_unit_hydro : DataFrame
        Stromerzeugungseinheit-Wasser.
    """
    if os.path.isfile(csv_name):
        power_unit_hydro = pd.read_csv(csv_name, header=0, encoding='utf-8', sep=';', index_col=False,
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
        power_unit_hydro_cnt = power_unit_hydro['timestamp'].count()
        log.info(f'Read {power_unit_hydro_cnt} Stromerzeugungseinheit-Wasser from {csv_name}')
        return power_unit_hydro

    else:
        log.info(f'Error reading {csv_name}')


# Download unit-hydro
def download_unit_hydro():
    """Download Wassereinheit (unit-hydro).

    Filter EinheitMastrNummer from Stromerzeugungseinheit-Wasser.
    Remove duplicates and count.
    Loop over list and write download to file.

    Existing units: 31543 (2019-02-10)

    Returns
    -------
    fname_hydro_unit : csv
        Write Wassereinheit to csv file.
    """
    start_from = 0
    setup_power_unit_hydro()

    power_unit_hydro = read_power_unit_hydro(fname_power_unit_hydro)
    power_unit_hydro = power_unit_hydro['EinheitMastrNummer']
    mastr_list = power_unit_hydro.values.tolist()
    mastr_list = list(dict.fromkeys(mastr_list))
    mastr_list_len = len(mastr_list)
    log.info(f'Download {mastr_list_len} Wassereinheit')

    for i in range(start_from, mastr_list_len, 1):
        unit_hydro = get_power_unit_hydro(mastr_list[i])  # First download
        if unit_hydro is not None:
            write_to_csv(fname_hydro_unit, unit_hydro)
        else:
            log.exception(f'Download failed unit_hydro ({i}): {mastr_list[i]} - Second download', exc_info=False)
            unit_hydro = get_power_unit_hydro(mastr_list[i])  # Second download
            if unit_hydro is not None:
                write_to_csv(fname_hydro_unit, unit_hydro)
            else:
                mastr_fail = {'EinheitMastrNummer': [mastr_list[i]]}
                log.exception(f'Second Download failed unit_wind ({i}): {mastr_list[i]} - Write to list', exc_info=False)
                unit_hydro_fail = pd.DataFrame(mastr_fail)
                unit_hydro_fail['timestamp'] = str(datetime.datetime.now())
                write_to_csv(fname_hydro_fail_u, unit_hydro_fail)

    retry_download_unit_hydro()


def retry_download_unit_hydro():
    """Download Wassereinheit (unit-hydro) from list.

    Read list of failed downloads from csv.
    Remove duplicates and retry download.
    Write download to file.

    Returns
    -------
    fname_hydro_unit : csv
        Write Wassereinheit to csv file.
    """
    start_from = 0
    if os.path.exists(os.path.dirname(fname_hydro_fail_u)):
        unit_fail_csv = pd.read_csv(fname_hydro_fail_u, delimiter=';')
        unit_fail = unit_fail_csv['EinheitMastrNummer']
        unit_fail_list = unit_fail.values.tolist()
        unit_fail_list = list(dict.fromkeys(unit_fail_list))
        unit_fail_list_len = len(unit_fail_list)
        log.info(f'Retry download {unit_fail_list_len} failed Wassereinheit')

        for i in range(start_from, unit_fail_list_len, 1):
            unit_wind = get_power_unit_hydro(unit_fail_list[i])  # Third download
            if unit_wind is not None:
                write_to_csv(fname_hydro_unit, unit_wind)
            else:
                unit_fail_unit = {'EinheitMastrNummer': [unit_fail_list[i]]}
                log.exception(f'Third download failed unit_wind: {unit_fail_list[i]}', exc_info=False)
                unit_fail_third = pd.DataFrame(unit_fail_unit)
                unit_fail_third['timestamp'] = str(datetime.datetime.now())
                unit_fail_third['comment'] = 'Third fail'
                write_to_csv(fname_hydro_fail_u, unit_fail_third)
    else:
        log.info('No failed downloads for Wassereinheit')


def get_power_unit_hydro(mastr_unit_hydro):
    """Get Wassereinheit (unit-hydro) from API using GetEinheitWasser.

    Parameters
    ----------
    mastr_unit_hydro : object
        Wasser from EinheitMastrNummerId.

    Returns
    -------
    unit_hydro : DataFrame
        Wassereinheit.
    """
    data_version = get_data_version()
    try:
        c = client_bind.GetEinheitWasser(apiKey=api_key,
                                       marktakteurMastrNummer=my_mastr,
                                       einheitMastrNummer=mastr_unit_hydro)
        s = serialize_object(c)
        df = pd.DataFrame(list(s.items()), )
        unit_hydro = df.set_index(list(df.columns.values)[0]).transpose()
        unit_hydro.reset_index()
        unit_hydro.index.names = ['lid']
        unit_hydro['version'] = data_version
        unit_hydro['timestamp'] = str(datetime.datetime.now())
        return unit_hydro
    except Exception as e:
        log.info('Download failed for %s', mastr_unit_hydro)


def read_unit_hydro(csv_name):
    """Encode and read Wassereinheit from CSV file.

    Parameters
    ----------
    csv_name : str
        Name of file.

    Returns
    -------
    unit_hydro : DataFrame
        Wassereinheit.
    """
    if os.path.isfile(csv_name):
        unit_hydro = pd.read_csv(csv_name, header=0, encoding='utf-8', sep=';', index_col=False,
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
                                        'ArtDerWasserkraftanlage': str,
                                        'AnzeigeEinerStilllegung': str,
                                        'ArtDerStilllegung': str,
                                        'DatumBeginnVorlaeufigenOderEndgueltigenStilllegung': str,
                                        'MinderungStromerzeugung': str,
                                        'BestandteilGrenzkraftwerk': str,
                                        'NettonennleistungDeutschland': str,
                                        'ArtDesZuflusses': str,
                                        'EegMastrNummer': str,
                                        'version': str,
                                        'timestamp': str})
        unit_hydro_cnt = unit_hydro['timestamp'].count()
        log.info(f'Read {unit_hydro_cnt} Wassereinheit from {csv_name}')
        return unit_hydro

    else:
        log.info(f'Error reading {csv_name}')


# Download unit-hydro-eeg
def download_unit_hydro_eeg():
    """Download Wassereinheit-EEG (unit-hydro-eeg) using GetAnlageEegWasser request.

    Filter EegMastrNummer from Stromerzeugungseinheit-Wasser.
    Remove duplicates and count.
    Loop over list and write download to file.

    Returns
    -------
    fname_hydro_eeg : csv
        Write Wassereinheit-EEG to csv file.
    """
    start_from = 0
    setup_power_unit_hydro()

    power_unit_hydro = read_power_unit_hydro(fname_power_unit_hydro)
    power_unit_hydro = power_unit_hydro['EegMastrNummer']
    mastr_list = power_unit_hydro.values.tolist()
    mastr_list = list(dict.fromkeys(mastr_list))
    mastr_list_len = len(mastr_list)
    log.info(f'Download {mastr_list_len} Wassereinheit-EEG')

    for i in range(0, mastr_list_len, 1):
        unit_hydro_eeg = get_unit_hydro_eeg(mastr_list[i])  # First download
        if unit_hydro_eeg is not None:
            write_to_csv(fname_hydro_eeg, unit_hydro_eeg)
        else:
            log.exception(f'Download failed unit_hydro_eeg ({i}): {mastr_list[i]}', exc_info=False)
            unit_wind_eeg = get_unit_hydro_eeg(mastr_list[i])  # Second download
            if unit_wind_eeg is not None:
                write_to_csv(fname_hydro_eeg, unit_wind_eeg)
            else:
                eeg_fail = {'EegMastrNummer': [mastr_list[i]]}
                log.exception(f'Second Download failed unit_hydro_eeg ({i}): {eeg_fail} - Write to list', exc_info=False)
                unit_hydro_fail = pd.DataFrame(eeg_fail)
                unit_hydro_fail['timestamp'] = str(datetime.datetime.now())
                write_to_csv(fname_hydro_fail_e, unit_hydro_fail)

    retry_download_unit_hydro_eeg()


def retry_download_unit_hydro_eeg():
    """Download Wassereinheit-EEG (unit-hydro-eeg) from list.

    Read list of failed downloads from csv.
    Remove duplicates and retry download.
    Write download to file.

    Returns
    -------
    fname_hydro_eeg : csv
        Write Wassereinheit-EEG to csv file.
    """
    start_from = 0
    if os.path.exists(os.path.dirname(fname_hydro_fail_e)):
        unit_fail_csv = pd.read_csv(fname_hydro_fail_e, delimiter=';')
        unit_fail = unit_fail_csv['EegMastrNummer']
        unit_fail_list = unit_fail.values.tolist()
        unit_fail_list = list(dict.fromkeys(unit_fail_list))
        unit_fail_list_len = len(unit_fail_list)
        log.info(f'Retry download {unit_fail_list_len} failed Wassereinheit-EEG')

        for i in range(start_from, unit_fail_list_len, 1):
            unit_wind = get_unit_hydro_eeg(unit_fail_list[i])
            if unit_wind is not None:
                write_to_csv(fname_hydro_eeg, unit_wind)
            else:
                unit_fail_eeg = {'EegMastrNummer': [unit_fail_list[i]]}
                log.exception(f'Third Download failed unit_hydro_eeg: {unit_fail_list[i]}', exc_info=False)
                unit_fail_third = pd.DataFrame(unit_fail_eeg)
                csv_input = pd.read_csv(fname_hydro_fail_e)
                csv_input['Retry'] = unit_fail_third
                csv_input.to_csv(fname_hydro_fail_e)
    else:
        log.info('No failed downloads for Wassereinheit-EEG')

def get_unit_hydro_eeg(mastr_hydro_eeg):
    """Get EEG-Anlage-Wasser from API using GetAnlageEegWasser.

    Parameters
    ----------
    mastr_hydro_eeg : str
        MaStR EEG Nr.

    Returns
    -------
    unit_hydro_eeg : DataFrame
        EEG-Anlage-Wasser.
    """
    data_version = get_data_version()
    try:
        c = client_bind.GetAnlageEegWasser(apiKey=api_key,
                                           marktakteurMastrNummer=my_mastr,
                                           eegMastrNummer=mastr_hydro_eeg)
        # c['VerknuepfteEinheit'] = c['MaStR']['VerknuepfteEinheit']
        # del c['MaStR']
        s = serialize_object(c)
        df = pd.DataFrame(list(s.items()), )
        unit_hydro_eeg = df.set_index(list(df.columns.values)[0]).transpose()
        unit_hydro_eeg.reset_index()
        unit_hydro_eeg.index.names = ['lid']
        unit_hydro_eeg["version"] = data_version
        unit_hydro_eeg["timestamp"] = str(datetime.datetime.now())
        return unit_hydro_eeg
    except Exception as e:
        log.info('Download failed for %s', mastr_hydro_eeg)


def read_unit_hydro_eeg(csv_name):
    """Encode and read EEG-Anlage-Wasser (unit_hydro_eeg) from CSV file.

    Parameters
    ----------
    csv_name : str
        Name of file.

    Returns
    -------
    unit_hydro_eeg : DataFrame
        EEG-Anlage-Wasser
    """
    if os.path.isfile(csv_name):
        unit_hydro_eeg = pd.read_csv(csv_name, header=0, sep=';', index_col=False, encoding='utf-8',
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
                                            'AnlageBetriebsstatus': str,
                                            'Ertuechtigung': str,
                                            'VerknuepfteEinheit': str,
                                            'version': str,
                                            'timestamp': str})
        unit_hydro_eeg_cnt = unit_hydro_eeg['timestamp'].count()
        log.info(f'Read {unit_hydro_eeg_cnt} Windeinheit-EEG from {csv_name}')
        return unit_hydro_eeg

    else:
        log.info(f'Error reading {csv_name}')
