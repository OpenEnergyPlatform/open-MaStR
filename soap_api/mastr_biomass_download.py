#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
BNetzA - MaStR Download - Biomass

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
                            fname_power_unit_biomass,
                            fname_biomass_unit,
                            fname_biomass_eeg,
                            fname_biomass_fail_u,
                            fname_biomass_fail_e)

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
def setup_power_unit_biomass():
    """Setup file for Stromerzeugungseinheit-Biomasse.

    Check if file with Stromerzeugungseinheit exists.
    Read Stromerzeugungseinheit and filter Stromerzeugungseinheit-Biomasse.
    Remove duplicates and write to file.

    Returns
    -------
    power_unit_biomasse : DataFrame
        Stromerzeugungseinheit-Biomasse.
    """

    if os.path.isfile(fname_power_unit_biomass):
        log.info(f'Skip setup for Stromerzeugungseinheit-Biomasse')

    else:
        if os.path.isfile(fname_power_unit):
            power_unit = read_power_units(fname_power_unit)

            power_unit_biomass = power_unit[power_unit.Einheittyp == 'Biomasse']
            power_unit_biomass = power_unit_biomass.drop_duplicates(subset=['EinheitMastrNummer',
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
            log.info(f'Filter power-unit for biomass and remove duplicates')
            power_unit_biomass.reset_index()
            power_unit_biomass.index.name = 'pu-id'

            write_to_csv(fname_power_unit_biomass, power_unit_biomass)
            power_unit_biomass_cnt = power_unit_biomass['timestamp'].count()
            log.info(f'Write {power_unit_biomass_cnt} power-unit_hydro to {fname_power_unit_biomass}')

        else:
            log.info(f'Error reading power-unit from {fname_power_unit}')


def read_power_unit_biomass(csv_name):
    """Read Stromerzeugungseinheit-Biomasse from CSV file.

    Parameters
    ----------
    csv_name : str
        Name of file.

    Returns
    -------
    power_unit_biomass : DataFrame
        Stromerzeugungseinheit-Biomasse.
    """

    if os.path.isfile(csv_name):
        power_unit_biomass = pd.read_csv(csv_name, header=0, encoding='utf-8', sep=';', index_col=False,
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
        power_unit_biomass_cnt = power_unit_biomass['timestamp'].count()
        log.info(f'Read {power_unit_biomass_cnt} Stromerzeugungseinheit-Biomasse from {csv_name}')
        return power_unit_biomass

    else:
        log.info(f'Error reading {csv_name}')


# Download unit-biomass
def download_unit_biomass():
    """Download Biomasseeinheit. Write results to csv file.


    ofname : string
        Path to save the downloaded files.

    Existing units: 31543 (2019-02-10)
    """

    start_from = 0
    setup_power_unit_biomass()
    power_unit_biomass = read_power_unit_biomass(fname_power_unit_biomass)
    power_unit_biomass = power_unit_biomass['EinheitMastrNummer']
    mastr_list = power_unit_biomass.values.tolist()
    mastr_list = list(dict.fromkeys(mastr_list))
    mastr_list_len = len(mastr_list)
    log.info(f'Download {mastr_list_len} Biomasseeinheit')

    for i in range(start_from, mastr_list_len, 1):
        unit_biomass = get_power_unit_biomass(mastr_list[i])
        if unit_biomass is not None:
            write_to_csv(fname_biomass_unit, unit_biomass)
        else:
            log.exception(f'Download failed unit_biomass ({i}): {mastr_list[i]}')
            mastr_fail = {'EinheitMastrNummer': [mastr_list[i]]}
            unit_biomass_fail = pd.DataFrame(mastr_fail)
            write_to_csv(fname_biomass_fail_u, unit_biomass_fail)


def get_power_unit_biomass(mastr_unit_biomass):
    """Get Biomasseeinheit from API using GetEinheitBiomasse.

    Parameters
    ----------
    mastr_unit_biomass : object
        Biomass from EinheitMastrNummerId.

    Returns
    -------
    unit_biomass : DataFrame
        Biomasseeinheit.
    """

    data_version = get_data_version()
    try:
        c = client_bind.GetEinheitBiomasse(apiKey=api_key,
                                           marktakteurMastrNummer=my_mastr,
                                           einheitMastrNummer=mastr_unit_biomass)
        s = serialize_object(c)
        df = pd.DataFrame(list(s.items()), )
        unit_biomass = df.set_index(list(df.columns.values)[0]).transpose()
        unit_biomass.reset_index()
        unit_biomass.index.names = ['lid']
        unit_biomass['version'] = data_version
        unit_biomass['timestamp'] = str(datetime.datetime.now())
        return unit_biomass
    except Exception as e:
        log.info('Download failed for %s', mastr_unit_biomass)


def read_unit_biomass(csv_name):
    """Read Biomasseeinheit from CSV file.

    Parameters
    ----------
    csv_name : str
        Name of file.

    Returns
    -------
    unit_biomass : DataFrame
        Biomasseeinheit.
    """

    if os.path.isfile(csv_name):
        unit_biomass = pd.read_csv(csv_name, header=0, encoding='utf-8', sep=';', index_col=False,
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
        unit_biomass_cnt = unit_biomass['timestamp'].count()
        log.info(f'Read {unit_biomass_cnt} Biomassseeinheit from {csv_name}')
        return unit_biomass

    else:
        log.info(f'Error reading {csv_name}')


# Download unit-biomass-eeg
def download_unit_biomass_eeg():
    """Download unit_biomass_eeg using GetAnlageEegBiomasse request.

    Parameters
    ----------

    Returns
    -------

    """

    setup_power_unit_biomass()
    power_unit_biomass = read_power_unit_biomass(fname_power_unit_biomass)
    power_unit_biomass = power_unit_biomass['EegMastrNummer']
    mastr_list = power_unit_biomass.values.tolist()
    mastr_list = list(dict.fromkeys(mastr_list))
    mastr_list_len = len(mastr_list)
    log.info(f'Download {mastr_list_len} Biomasseinheit EEG')

    for i in range(0, mastr_list_len, 1):
        unit_biomass_eeg = get_unit_biomass_eeg(mastr_list[i])
        if unit_biomass_eeg is not None:
            write_to_csv(fname_biomass_eeg, unit_biomass_eeg)
        else:
            log.exception(f'Download failed unit_biomass_eeg ({i}): {mastr_list[i]}')
            mastr_fail = {'EegMastrNummer': [mastr_list[i]]}
            unit_biomass_fail = pd.DataFrame(mastr_fail)
            write_to_csv(fname_biomass_fail_e, unit_biomass_fail)


def get_unit_biomass_eeg(mastr_biomass_eeg):
    """Get EEG-Anlage-Biomasse from API using GetAnlageEegBiomasse.

    Parameters
    ----------
    mastr_biomass_eeg : str
        MaStR EEG Nr.

    Returns
    -------
    unit_biomass_eeg : DataFrame
        EEG-Anlage-Biomasse.
    """
    data_version = get_data_version()
    try:
        c = client_bind.GetAnlageEegBiomasse(
            apiKey=api_key,
            marktakteurMastrNummer=my_mastr,
            eegMastrNummer=mastr_biomass_eeg
        )
        s = serialize_object(c)
        df = pd.DataFrame(list(s.items()), )
        unit_biomass_eeg = df.set_index(list(df.columns.values)[0]).transpose()
        unit_biomass_eeg.reset_index()
        unit_biomass_eeg.index.names = ['lid']
        unit_biomass_eeg["version"] = data_version
        unit_biomass_eeg["timestamp"] = str(datetime.datetime.now())
        return unit_biomass_eeg
    except Exception as e:
        log.info('Download failed for %s', mastr_biomass_eeg)


def read_unit_biomass_eeg(csv_name):
    """
    Encode and read EEG-Anlage-Biomass from CSV file.

    Parameters
    ----------
    csv_name : str
        Name of file.

    Returns
    -------
    unit_biomass_eeg : DataFrame
        EEG-Anlage-Biomass
    """

    if os.path.isfile(csv_name):
        unit_biomass_eeg = pd.read_csv(csv_name, header=0, sep=';', index_col=False, encoding='utf-8',
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
        unit_biomass_eeg_cnt = unit_biomass_eeg['timestamp'].count()
        log.info(f'Read {unit_biomass_eeg_cnt} Biomasseeinheit-EEG from {csv_name}')
        return unit_biomass_eeg

    else:
        log.info(f'Error reading {csv_name}')
